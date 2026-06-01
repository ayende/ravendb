using System;
using System.Collections.Generic;
using System.Diagnostics;
using Corax.Querying.Planning;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

internal static partial class QueryPlanBuilder
{
    private sealed class PlanEmitter
    {
        private readonly List<PlanOp> _ops = [];
        private int _nextRangeIdx; // index for ctx.InRangeCounts[that idx] at runtime.
        private int _matchIndex;

        // Slot 0 is the live accumulator; EphemeralBitmap stages "build a set then merge into slot 0"
        // (IN/AllIn). Its value never outlives one leaf emission, so a single fixed slot is reusable at
        // any nesting depth and never counts against the scratch high-water mark; save-stack starts above it.
        private const int EphemeralBitmap = 1;
        private int _nextScratch = EphemeralBitmap + 1;
        private int _maxScratchUsed = EphemeralBitmap;

        public static (PlanOp[] Ops, int RequiredBitmaps) Emit(PlanTemplate template, List<ClauseExecution> executions, PlanParameters planParams, ScanPredicateInfo?[] perClause)
        {
            if (executions.Count is 0) // exec.QueryWillReturnNoResults was checked by caller, so that means all results, not none at all
                return (BuildAllEntriesPlan(), 2);

            var emitter = new PlanEmitter();
            var (ops, bitmaps) = template.IsOr ? emitter.EmitOrPlan(executions) : emitter.EmitAndPlan(executions, perClause);
            if (planParams.HasBoost)
            {
                // we require query match for boost, because the other options cannot compute it
                for (int i = 0; i < ops.Length; i++)
                {
                    ops[i].Kind = ToMatchVariant(ops[i].Kind);
                }
            }
            return (ops, bitmaps);
        }

        private (PlanOp[] Ops, int RequiredBitmaps) Complete()
        {
            _ops.Add(new PlanOp { Kind = PlanOpKind.GotoDone });
            return (_ops.ToArray(), Math.Max(2, _maxScratchUsed + 1));
        }

        /// <summary>Reserve a fresh scratch bitmap slot for save/restore of slot 0 around a
        /// nested build. The returned scope releases the slot on Dispose — use with
        /// <c>using var _ = AllocateScratchSlot(out var slot);</c>. The high-water mark
        /// feeds <see cref="Complete"/>'s RequiredBitmaps so the runtime allocates enough bitmaps.</summary>
        private ScratchSlotScope AllocateScratchSlot(out int slot)
        {
            slot = _nextScratch++;
            if (slot > _maxScratchUsed)
                _maxScratchUsed = slot;
            return new ScratchSlotScope(this);
        }

        private readonly struct ScratchSlotScope(PlanEmitter emitter) : IDisposable
        {
            public void Dispose() => emitter._nextScratch--;
        }

        private (PlanOp[] Ops, int RequiredBitmaps) EmitOrPlan(List<ClauseExecution> executions)
        {
            Debug.Assert(executions.Count > 0);

            if (CanFoldNegatedOr(executions))
                return EmitFoldedNegatedOr(executions);

            EmitClauseInto(executions[0],MergeKind.Fill, executions[0].Cardinality, suppressEarlyExit: true);
            for (int i = 1; i < executions.Count; i++)
            {
                EmitClauseInto(executions[i], MergeKind.OrInto, executions[i].Cardinality, suppressEarlyExit: true);
            }
            return Complete();
        }

        /// <summary>An OR chain whose members are ALL negations folds by De Morgan into a single
        /// complement: <c>¬A ∨ ¬B ∨ … = ¬(A ∧ B ∧ …)</c>. Without the fold each negated member emits its
        /// own <see cref="PlanOpKind.FillAllEntries"/> + AndNot (N full-universe scans); folded, we
        /// intersect the (typically selective) positive forms once and take a single complement.
        /// Requires ≥2 members (a lone negated leaf already emits exactly one Fill+AndNot) and only
        /// leaf members — a group would not round-trip through <see cref="EmitPositiveForm"/>'s single-match
        /// default. A mixed chain (e.g. <c>¬A ∨ B</c>) cannot fold because B's positive matches must still OR in.</summary>
        private static bool CanFoldNegatedOr(List<ClauseExecution> executions)
        {
            if (executions.Count < 2)
                return false;

            foreach (var e in executions)
            {
                if (e.Clause.IsOrChainNotEquals == false)
                    return false;
                if (e.ClauseType is ClauseType.OrGroup or ClauseType.AndGroup)
                    return false;
            }

            return true;
        }

        private (PlanOp[] Ops, int RequiredBitmaps) EmitFoldedNegatedOr(List<ClauseExecution> executions)
        {
            // Build the positive intersection A ∧ B ∧ … in slot 0. Only Fill (first) and AndInto (rest, with
            // early-exit suppressed) are used here — neither emits a premature jump to the done label on slot 0,
            // so a partially-built or empty intersection cannot short-circuit before the complement is taken.
            EmitPositiveForm(executions[0], MergeKind.Fill, executions[0].Cardinality, suppressEarlyExit: true);
            for (int i = 1; i < executions.Count; i++)
            {
                EmitPositiveForm(executions[i], MergeKind.AndInto, executions[i].Cardinality, suppressEarlyExit: true);
            }

            // slot 0 = (A ∧ B ∧ …). Park it in a scratch slot, fill the universe, take ONE complement:
            // slot 0 = ALL \ (A ∧ B ∧ …). A doc missing any field is absent from the intersection and so lands
            // in the result — identical null/non-existing semantics to the per-member FillAllEntries + AndNot.
            using var _ = AllocateScratchSlot(out int saveSlot);
            _ops.Add(new PlanOp { Kind = PlanOpKind.SwapBitmaps, BitmapLocal = 0, ParamIndex2 = saveSlot });
            _ops.Add(new PlanOp { Kind = PlanOpKind.FillAllEntries, EstimatedCardinality = long.MaxValue });
            _ops.Add(new PlanOp { Kind = PlanOpKind.AndNotBitmaps, BitmapLocal = 0, ParamIndex2 = saveSlot });
            return Complete();
        }

        private (PlanOp[] Ops, int RequiredBitmaps) EmitAndPlan(List<ClauseExecution> executions, ScanPredicateInfo?[] perClause)
        {
            var e0 = executions[0];
            if (e0.IsNegated)
                _ops.Add(new PlanOp { Kind = PlanOpKind.FillAllEntries, EstimatedCardinality = long.MaxValue });

            EmitClauseInto(e0, e0.IsNegated ? MergeKind.AndNotInto : MergeKind.Fill, e0.Cardinality, suppressEarlyExit: false);

            // check if we have any clause after the first that we cannot scan on, we don't bother with the first since we always run it normally
            bool allScanEligible = perClause.AsSpan()[1..].Contains(null) is false;

            for (int i = 1; i < executions.Count; i++)
            {
                if (allScanEligible) // if we can, check if we can move to entry scan after the first check
                {
                    _ops.Add(new PlanOp
                    {
                        Kind = PlanOpKind.MaybeEntryScan,
                        ParamIndex = _matchIndex
                    });
                }

                var cur = executions[i];
                MergeKind merge = cur.IsNegated ? MergeKind.AndNotInto : MergeKind.AndInto;

                EmitClauseInto(cur, merge, cur.Cardinality, suppressEarlyExit: false);
                if (cur.IsNegated is false) // when we have 0 results, early exit 
                {
                    _ops.Add(new PlanOp { Kind = PlanOpKind.GotoDoneIfEmpty, BitmapLocal = 0 });
                }
            }

            return Complete();
        }

        private void EmitClauseInto(ClauseExecution exec, MergeKind merge, long cardinality, bool suppressEarlyExit)
        {
            if (exec.Clause.IsOrChainNotEquals)
            {
                EmitNegatedLeafInto(exec, merge, cardinality);
                return;
            }

            EmitPositiveForm(exec, merge, cardinality, suppressEarlyExit);
        }

        /// <summary>Emit a clause's POSITIVE form (no negation rewrite) merged into slot 0 with the given
        /// <paramref name="merge"/>. This is the body of <see cref="EmitClauseInto"/> minus the
        /// <see cref="ClauseInfo.IsOrChainNotEquals"/> routing, so the De Morgan fold can build the positive
        /// intersection of negated members without re-triggering complement emission.</summary>
        private void EmitPositiveForm(ClauseExecution exec, MergeKind merge, long cardinality, bool suppressEarlyExit)
        {
            switch (exec.ClauseType)
            {
                case ClauseType.OrGroup or ClauseType.AndGroup when exec.Clause.SubClauses is { Count: > 0 }:
                    EmitGroupInto(exec, exec.SubExecutions, merge, suppressEarlyExit);
                    break;
                case ClauseType.In:
                    EmitInLeaf(exec, cardinality, merge);
                    break;
                case ClauseType.AllIn:
                    EmitAllInLeaf(exec, cardinality, merge, suppressEarlyExit);
                    break;
                default:
                    _ops.Add(new PlanOp
                    {
                        Kind = ToPlanOpKind(merge, GetDispatch(exec)),
                        ParamIndex = _matchIndex++,
                        BitmapLocal = 0,
                        EstimatedCardinality = cardinality,
                        SkipEarlyExit = merge == MergeKind.AndInto && suppressEarlyExit
                    });
                    break;
            }
        }

        private void EmitGroupInto(ClauseExecution exec, List<ClauseExecution> subExecs, MergeKind merge, bool suppressEarlyExit)
        {
            if (merge == MergeKind.Fill)
            {
                EmitGroupContentsInSlot0(exec, subExecs, suppressEarlyExit);
                return;
            }

            using var _ = AllocateScratchSlot(out int saveSlot);

            _ops.Add(new PlanOp { Kind = PlanOpKind.SwapBitmaps, BitmapLocal = 0, ParamIndex2 = saveSlot });

            EmitGroupContentsInSlot0(exec, subExecs, suppressEarlyExit: true); // AndFrom*/AndRangeFrom* MUST NOT early-exit to doneLabel.

            switch (merge)
            {
                case MergeKind.OrInto:
                    _ops.Add(new PlanOp { Kind = PlanOpKind.LazyOrBitmaps, BitmapLocal = 0, ParamIndex2 = saveSlot });
                    break;
                case MergeKind.AndInto:
                    _ops.Add(new PlanOp { Kind = PlanOpKind.AndBitmaps, BitmapLocal = 0, ParamIndex2 = saveSlot });
                    break;
                case MergeKind.AndNotInto:
                    // AndNotBitmaps[0, saveSlot] = slot 0 \ saveSlot. After build, slot 0 = group result, saveSlot = original accumulator, swap them first.
                    _ops.Add(new PlanOp { Kind = PlanOpKind.SwapBitmaps, BitmapLocal = 0, ParamIndex2 = saveSlot });
                    _ops.Add(new PlanOp { Kind = PlanOpKind.AndNotBitmaps, BitmapLocal = 0, ParamIndex2 = saveSlot });
                    break;
            }
        }

        private void EmitGroupContentsInSlot0(ClauseExecution exec, List<ClauseExecution> subExecs, bool suppressEarlyExit)
        {
            bool isOr = exec.ClauseType != ClauseType.OrGroup;
            bool firstNegated = isOr && subExecs[0].IsNegated;
            if (firstNegated)
                _ops.Add(new PlanOp { Kind = PlanOpKind.FillAllEntries, EstimatedCardinality = long.MaxValue });
            var followupAction = isOr ? MergeKind.AndInto : MergeKind.OrInto;
            EmitClauseInto(subExecs[0], firstNegated ? MergeKind.AndNotInto : MergeKind.Fill, subExecs[0].Cardinality, suppressEarlyExit);
            for (int i = 1; i < subExecs.Count; i++)
            {
                MergeKind kind = isOr && subExecs[i].IsNegated ? MergeKind.AndNotInto : followupAction;
                EmitClauseInto(subExecs[i], kind, subExecs[i].Cardinality, suppressEarlyExit);
            }
        }

        private static PlanOpKind ToPlanOpKind(MergeKind merge, MatchDispatch dispatch) => (merge, dispatch) switch
        {
            (MergeKind.Fill, MatchDispatch.PostingList)       => PlanOpKind.FillFromPostingSource,
            (MergeKind.Fill, MatchDispatch.TreeScan)          => PlanOpKind.FillFromTreeScan,
            (MergeKind.Fill, _)                               => PlanOpKind.FillFromMatch,

            (MergeKind.OrInto, MatchDispatch.PostingList)     => PlanOpKind.OrFromPostingSource,
            (MergeKind.OrInto, MatchDispatch.TreeScan)        => PlanOpKind.OrFromTreeScan,
            (MergeKind.OrInto, _)                             => PlanOpKind.OrFromMatch,

            (MergeKind.AndInto, MatchDispatch.PostingList)    => PlanOpKind.AndFromPostingSource,
            (MergeKind.AndInto, MatchDispatch.TreeScan)       => PlanOpKind.AndFromTreeScan,
            (MergeKind.AndInto, _)                            => PlanOpKind.AndFromMatch,

            (MergeKind.AndNotInto, MatchDispatch.PostingList) => PlanOpKind.AndNotFromPostingSource,
            (MergeKind.AndNotInto, MatchDispatch.TreeScan)    => PlanOpKind.AndNotFromTreeScan,
            (MergeKind.AndNotInto, _)                         => PlanOpKind.AndNotFromMatch,

            _ => throw new InvalidOperationException($"Unhandled MergeKind: {merge} / {dispatch}")
        };

        private static PlanOpKind ToMatchVariant(PlanOpKind kind) => kind switch
        {
            PlanOpKind.FillFromPostingSource or PlanOpKind.FillFromTreeScan => PlanOpKind.FillFromMatch,
            PlanOpKind.AndFromPostingSource or PlanOpKind.AndFromTreeScan => PlanOpKind.AndFromMatch,
            PlanOpKind.OrFromPostingSource or PlanOpKind.OrFromTreeScan => PlanOpKind.OrFromMatch,
            PlanOpKind.AndNotFromPostingSource or PlanOpKind.AndNotFromTreeScan => PlanOpKind.AndNotFromMatch,
            PlanOpKind.OrRangeFromPostingSource => PlanOpKind.OrRangeFromMatch,
            PlanOpKind.AndRangeFromPostingSource => PlanOpKind.AndRangeFromMatch,
            _ => kind
        };

        /// <summary>IN clause leaf — logically (term0 ∪ term1 ∪ … ∪ termN).</summary>
        private void EmitInLeaf(ClauseExecution exec, long cardinality, MergeKind merge)
        {
            if (merge is MergeKind.Fill or MergeKind.OrInto)
            {
                var firstKind = merge == MergeKind.Fill ? PlanOpKind.FillFromPostingSource : PlanOpKind.OrFromPostingSource;
                EmitCommonInOps(exec.InTermCount, cardinality, bitmapLocal: 0, firstKind, PlanOpKind.OrRangeFromPostingSource, suppressEarlyExit: false);
                return;
            }
            EmitCommonInOps(exec.InTermCount, cardinality, EphemeralBitmap, PlanOpKind.FillFromPostingSource, PlanOpKind.OrRangeFromPostingSource, suppressEarlyExit: false);
            _ops.Add(new PlanOp
            {
                Kind = merge == MergeKind.AndInto ? PlanOpKind.AndBitmaps : PlanOpKind.AndNotBitmaps,
                BitmapLocal = 0,
                ParamIndex2 = EphemeralBitmap
            });
        }

        /// <summary>AllIn clause leaf — logically (term0 ∩ term1 ∩ … ∩ termN).</summary>
        private void EmitAllInLeaf(ClauseExecution exec, long cardinality, MergeKind merge, bool suppressEarlyExit)
        {
            if (merge == MergeKind.Fill)
            {
                EmitCommonInOps(exec.InTermCount, cardinality, 0, PlanOpKind.FillFromPostingSource, PlanOpKind.AndRangeFromPostingSource, suppressEarlyExit);
                return;
            }

            using var _ = AllocateScratchSlot(out int saveSlot);

            // Build (term0 ∩ term1 ∩ …) in its own scratch slot, then merge it into the slot-0
            // accumulator. AndRange honors its destination slot (using slot 1 as scratch), so the
            // intersection lands in saveSlot directly — no slot-0 staging dance needed.
            // suppressEarlyExit so AndRange doesn't jump to doneLabel mid-intersection and skip that merge.
            EmitCommonInOps(exec.InTermCount, cardinality, saveSlot, PlanOpKind.FillFromPostingSource, PlanOpKind.AndRangeFromPostingSource, suppressEarlyExit: true);

            switch (merge)
            {
                case MergeKind.OrInto:
                    _ops.Add(new PlanOp { Kind = PlanOpKind.LazyOrBitmaps, BitmapLocal = 0, ParamIndex2 = saveSlot });
                    break;
                case MergeKind.AndInto:
                    _ops.Add(new PlanOp { Kind = PlanOpKind.AndBitmaps, BitmapLocal = 0, ParamIndex2 = saveSlot });
                    break;
                case MergeKind.AndNotInto:
                    _ops.Add(new PlanOp { Kind = PlanOpKind.AndNotBitmaps, BitmapLocal = 0, ParamIndex2 = saveSlot });
                    break;
            }
        }

        private void EmitNegatedLeafInto(ClauseExecution exec, MergeKind merge, long cardinality)
        {
            Debug.Assert(merge is MergeKind.Fill or MergeKind.OrInto,
                $"IsOrChainNotEquals only appears in OR chains; got merge={merge}");

            if (merge == MergeKind.Fill)
            {
                _ops.Add(new PlanOp { Kind = PlanOpKind.FillAllEntries, EstimatedCardinality = long.MaxValue });
                EmitComplementBody(exec, cardinality);
                return;
            }

            using var _ = AllocateScratchSlot(out int saveSlot);

            _ops.Add(new PlanOp { Kind = PlanOpKind.SwapBitmaps, BitmapLocal = 0, ParamIndex2 = saveSlot });
            _ops.Add(new PlanOp { Kind = PlanOpKind.FillAllEntries, EstimatedCardinality = long.MaxValue });
            EmitComplementBody(exec, cardinality);
            _ops.Add(new PlanOp { Kind = PlanOpKind.LazyOrBitmaps, BitmapLocal = 0, ParamIndex2 = saveSlot });
        }

        private void EmitComplementBody(ClauseExecution exec, long cardinality)
        {
            switch (exec.ClauseType)
            {
                case ClauseType.In:
                    EmitCommonInOps(exec.InTermCount, cardinality, EphemeralBitmap, PlanOpKind.FillFromPostingSource, PlanOpKind.OrRangeFromPostingSource, suppressEarlyExit: false);
                    _ops.Add(new PlanOp { Kind = PlanOpKind.AndNotBitmaps, BitmapLocal = 0, ParamIndex2 = EphemeralBitmap });
                    return;
                case ClauseType.AllIn:
                {
                    // Build the positive set (term0 ∩ term1 ∩ …) in a dedicated scratch slot, then subtract
                    // it from the AllEntries seed already in slot 0: slot0 = AllEntries \ (term0 ∩ term1 ∩ …).
                    // AndRange honors its destination slot (using slot 1 as scratch), so the intersection
                    // stages cleanly outside slot 0 — the scratch must not be the AND scratch (slot 1), which
                    // AllocateScratchSlot guarantees (it never hands out slot 0 or 1).
                    using var _ = AllocateScratchSlot(out int positiveSlot);
                    EmitCommonInOps(exec.InTermCount, cardinality, positiveSlot, PlanOpKind.FillFromPostingSource, PlanOpKind.AndRangeFromPostingSource, suppressEarlyExit: true);
                    _ops.Add(new PlanOp { Kind = PlanOpKind.AndNotBitmaps, BitmapLocal = 0, ParamIndex2 = positiveSlot });
                    return;
                }
                default:
                    _ops.Add(new PlanOp
                    {
                        Kind = ToPlanOpKind(MergeKind.AndNotInto, GetDispatch(exec)),
                        ParamIndex = _matchIndex++,
                        BitmapLocal = 0,
                        EstimatedCardinality = cardinality
                    });
                    break;
            }
        }

        private void EmitCommonInOps(int inTermCount, long cardinality, int bitmapLocal, PlanOpKind firstKind, PlanOpKind secondKind, bool suppressEarlyExit)
        {
            int totalSlots = inTermCount + 1;
            _ops.Add(new PlanOp
            {
                Kind = firstKind,
                ParamIndex = _matchIndex,
                BitmapLocal = bitmapLocal,
                EstimatedCardinality = Math.Max(1, cardinality / totalSlots)
            });

            _ops.Add(new PlanOp
            {
                Kind = secondKind,
                ParamIndex = _matchIndex + 1,
                ParamIndex2 = _nextRangeIdx++,
                BitmapLocal = bitmapLocal,
                EstimatedCardinality = cardinality,
                SkipEarlyExit = suppressEarlyExit // Defaults to false for EmitInOps
            });

            _matchIndex += totalSlots;
        }

        private static PlanOp[] BuildAllEntriesPlan()
        {
            // No bitmap needed — AllEntries already implements IQueryMatch.Fill(),
            // so we iterate it directly without materializing into a bitmap first.
            return [new PlanOp { Kind = PlanOpKind.FillFromMatch, ParamIndex = 0 }];
        }
    }
}
