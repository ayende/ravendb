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

        public static (PlanOp[] Ops, int RequiredBitmaps) Emit(PlanTemplate template, List<ClauseExecution> executions, PlanParameters planParams)
        {
            if (executions.Count is 0) // exec.QueryWillReturnNoResults was checked by caller, so that means all results, not none at all
                return (BuildAllEntriesPlan(), 2);

            var emitter = new PlanEmitter();
            var (ops, bitmaps) = template.IsOr ? emitter.EmitOrPlan(executions) : emitter.EmitAndPlan(executions);
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
            
            EmitClauseInto(executions[0],MergeKind.Fill, executions[0].Cardinality, suppressEarlyExit: true);
            for (int i = 1; i < executions.Count; i++)
            {
                EmitClauseInto(executions[i], MergeKind.OrInto, executions[i].Cardinality, suppressEarlyExit: true);
            }
            return Complete();
        }

        private (PlanOp[] Ops, int RequiredBitmaps) EmitAndPlan(List<ClauseExecution> executions)
        {
            var e0 = executions[0];
            switch (executions.Count)
            {
                case 1 when e0.ClauseType == ClauseType.Equals && e0.IsNegated is false:
                    _ops.Add(new PlanOp
                    {
                        Kind = ToPlanOpKind(MergeKind.Fill, GetDispatch(e0.Clause)),
                        ParamIndex = 0,
                        EstimatedCardinality = e0.Cardinality
                    });
                    return (_ops.ToArray(), 2);
                case 1 when e0.ClauseType == ClauseType.NotEquals
                            || (e0.ClauseType == ClauseType.Equals && e0.IsNegated):
                    _ops.Add(new PlanOp
                    {
                        Kind = PlanOpKind.FillAllEntries,
                        EstimatedCardinality = long.MaxValue
                    });
                    _ops.Add(new PlanOp
                    {
                        Kind = ToPlanOpKind(MergeKind.AndNotInto, GetDispatch(e0.Clause)),
                        EstimatedCardinality = e0.Cardinality
                    });
                    _ops.Add(new PlanOp { Kind = PlanOpKind.GotoDone });
                    return (_ops.ToArray(), 2);
            }

            // AND chain: Fill the smallest non-negated, then AndWith/AndNotWith the rest.
            // If the first clause is negated (cardinality sort puts negated clauses last,
            // so first-negated ⇒ all-negated) we seed with FillAllEntries instead and
            // AndNot every clause. FillAllEntries calls indexSearcher.AllEntries() directly,
            // avoiding the structural-vs-runtime slot-index mismatch that bites IN with a
            // parameter-bound array of different length.
            bool firstIsNegated = e0.IsNegated;
            int startIndex;

            if (firstIsNegated)
            {
                _ops.Add(new PlanOp { Kind = PlanOpKind.FillAllEntries, EstimatedCardinality = long.MaxValue });
                startIndex = 0;
            }
            else
            {
                EmitClauseInto(e0, MergeKind.Fill, e0.Cardinality, suppressEarlyExit: false);
                startIndex = 1;
            }

            // Precheck: can every remaining clause be evaluated by an entry-scan predicate?
            bool allScanEligible = AreAllScanEligible(executions, startIndex);

            for (int i = startIndex; i < executions.Count; i++)
            {
                // MaybeEntryScan emits a runtime branch into the entry-scan
                // fallback when bitmap[0] is small relative to remaining IQueryMatch
                // counts. Only safe to emit when AreAllScanEligible reports every
                // remaining clause has a scan predicate.
                if (allScanEligible)
                {
                    _ops.Add(new PlanOp
                    {
                        Kind = PlanOpKind.MaybeEntryScan,
                        ParamIndex = _matchIndex
                    });
                }

                var execI = executions[i];
                bool stepNegated = execI.IsNegated;
                MergeKind merge = stepNegated ? MergeKind.AndNotInto : MergeKind.AndInto;

                EmitClauseInto(execI, merge, execI.Cardinality, suppressEarlyExit: false);

                // GotoDoneIfEmpty: short-circuit when slot 0 became empty after a positive
                // intersection. Negated steps don't justify the check — they remove
                // entries, so an empty result still represents a valid finished plan.
                if (!stepNegated)
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
                    EmitLeafMergeOp(merge, cardinality, GetDispatch(exec.Clause), suppressEarlyExit);
                    _matchIndex++;
                    break;
            }
        }

        /// <summary>Emit a group (OrGroup or AndGroup) merged into slot 0. For Fill merge,
        /// build directly in slot 0. For non-Fill, save slot 0 to a scratch slot, build
        /// the group fresh in slot 0, then merge with the saved accumulator via the
        /// matching bitmap-pair op.</summary>
        private void EmitGroupInto(ClauseExecution exec, List<ClauseExecution> subExecs, MergeKind merge, bool suppressEarlyExit)
        {
            if (merge == MergeKind.Fill)
            {
                EmitGroupContentsInSlot0(exec, subExecs, suppressEarlyExit);
                return;
            }

            using var _ = AllocateScratchSlot(out int saveSlot);

            _ops.Add(new PlanOp { Kind = PlanOpKind.ClearBitmap, BitmapLocal = saveSlot });
            _ops.Add(new PlanOp { Kind = PlanOpKind.SwapBitmaps, BitmapLocal = 0, ParamIndex2 = saveSlot });

            // Inside the saved context, AndFrom*/AndRangeFrom* MUST NOT early-exit to
            // doneLabel — that would skip the merge-back below and leak the saved value.
            EmitGroupContentsInSlot0(exec, subExecs, suppressEarlyExit: true);

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

            _ops.Add(new PlanOp { Kind = PlanOpKind.ClearBitmap, BitmapLocal = saveSlot });
        }

        /// <summary>Build a group's complete result in slot 0 (slot 0 must be empty/usable
        /// on entry; the caller arranges this either by being the seed Fill or by swapping
        /// the live accumulator out). OrGroup: Fill first sub, OR rest. AndGroup: Fill
        /// first sub (or FillAllEntries if first is negated), AND/ANDNOT rest.</summary>
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

        /// <summary>Emit one PlanOp for a simple leaf clause according to <paramref name="merge"/>.
        /// Sets SkipEarlyExit on the AndFrom* op when inside a saved-swap context.</summary>
        private void EmitLeafMergeOp(MergeKind merge, long cardinality, MatchDispatch dispatch, bool suppressEarlyExit)
        {
            _ops.Add(new PlanOp
            {
                Kind = ToPlanOpKind(merge, dispatch),
                ParamIndex = _matchIndex,
                BitmapLocal = 0,
                EstimatedCardinality = cardinality,
                SkipEarlyExit = merge == MergeKind.AndInto && suppressEarlyExit
            });
        }

        /// <summary>Fold a (merge-shape, dispatch-source) pair into the concrete leaf
        /// <see cref="PlanOpKind"/>. Replaces the runtime <c>PlanOp.Dispatch</c> field:
        /// the source is now baked into the op kind itself.</summary>
        private static PlanOpKind ToPlanOpKind(MergeKind merge, MatchDispatch dispatch) => merge switch
        {
            MergeKind.Fill => dispatch switch
            {
                MatchDispatch.PostingList => PlanOpKind.FillFromPostingSource,
                MatchDispatch.TreeScan => PlanOpKind.FillFromTreeScan,
                _ => PlanOpKind.FillFromMatch
            },
            MergeKind.OrInto => dispatch switch
            {
                MatchDispatch.PostingList => PlanOpKind.OrFromPostingSource,
                MatchDispatch.TreeScan => PlanOpKind.OrFromTreeScan,
                _ => PlanOpKind.OrFromMatch
            },
            MergeKind.AndInto => dispatch switch
            {
                MatchDispatch.PostingList => PlanOpKind.AndFromPostingSource,
                MatchDispatch.TreeScan => PlanOpKind.AndFromTreeScan,
                _ => PlanOpKind.AndFromMatch
            },
            MergeKind.AndNotInto => dispatch switch
            {
                MatchDispatch.PostingList => PlanOpKind.AndNotFromPostingSource,
                MatchDispatch.TreeScan => PlanOpKind.AndNotFromTreeScan,
                _ => PlanOpKind.AndNotFromMatch
            },
            _ => throw new InvalidOperationException($"Unhandled MergeKind: {merge}")
        };

        /// <summary>Promote a leaf/range op to its IQueryMatch-dispatch variant. Used by the
        /// boost post-pass: boosted plans must score via IQueryMatch, so every PostingSource /
        /// TreeScan leaf op is rewritten to read <c>ctx.ResolvedMatches</c>. Non-leaf ops
        /// (bitmap merges, FillAllEntries, control flow) are returned unchanged.</summary>
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
                EmitInOps(exec.InTermCount, cardinality, bitmapLocal: 0, isSeed: merge == MergeKind.Fill);
                return;
            }
            // for AND, use the ephemeral bitmap to hold this, then merge with slot 0.
            EmitInOps(exec.InTermCount, cardinality, bitmapLocal: EphemeralBitmap, isSeed: true);
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
                EmitAllInOps(exec.InTermCount, cardinality, bitmapLocal: 0, suppressEarlyExit);
                return;
            }

            using var _ = AllocateScratchSlot(out int saveSlot);

            _ops.Add(new PlanOp { Kind = PlanOpKind.ClearBitmap, BitmapLocal = saveSlot });
            _ops.Add(new PlanOp { Kind = PlanOpKind.SwapBitmaps, BitmapLocal = 0, ParamIndex2 = saveSlot });
            EmitAllInOps(exec.InTermCount, cardinality, bitmapLocal: 0, suppressEarlyExit: true);

            switch (merge)
            {
                case MergeKind.OrInto:
                    _ops.Add(new PlanOp { Kind = PlanOpKind.LazyOrBitmaps, BitmapLocal = 0, ParamIndex2 = saveSlot });
                    break;
                case MergeKind.AndInto:
                    _ops.Add(new PlanOp { Kind = PlanOpKind.AndBitmaps, BitmapLocal = 0, ParamIndex2 = saveSlot });
                    break;
                case MergeKind.AndNotInto:
                    _ops.Add(new PlanOp { Kind = PlanOpKind.SwapBitmaps, BitmapLocal = 0, ParamIndex2 = saveSlot });
                    _ops.Add(new PlanOp { Kind = PlanOpKind.AndNotBitmaps, BitmapLocal = 0, ParamIndex2 = saveSlot });
                    break;
            }
            _ops.Add(new PlanOp { Kind = PlanOpKind.ClearBitmap, BitmapLocal = saveSlot });
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

            // OrInto: save the accumulator out, build a fresh complement in slot 0, OR back.
            // Mirrors the save-swap pattern in EmitGroupInto / EmitAllInLeaf.
            using var _ = AllocateScratchSlot(out int saveSlot);

            _ops.Add(new PlanOp { Kind = PlanOpKind.ClearBitmap, BitmapLocal = saveSlot });
            _ops.Add(new PlanOp { Kind = PlanOpKind.SwapBitmaps, BitmapLocal = 0, ParamIndex2 = saveSlot });

            _ops.Add(new PlanOp { Kind = PlanOpKind.FillAllEntries, EstimatedCardinality = long.MaxValue });
            EmitComplementBody(exec, cardinality);

            _ops.Add(new PlanOp { Kind = PlanOpKind.LazyOrBitmaps, BitmapLocal = 0, ParamIndex2 = saveSlot });
            _ops.Add(new PlanOp { Kind = PlanOpKind.ClearBitmap, BitmapLocal = saveSlot });
        }

        /// <summary>Turn slot 0 (currently <see cref="PlanOpKind.FillAllEntries"/>) into the
        /// complement of <paramref name="exec"/>'s positive form. IN unions the terms into the
        /// ephemeral bitmap then AndNotBitmaps(0, ephemeral); AllIn intersects there then AndNotBitmaps.
        /// Scalar / Exists / Range clauses use an AndNotFrom* op directly (the source family
        /// follows <see cref="GetDispatch"/> for the positive form).
        /// Advances <see cref="_matchIndex"/> past the clause's slot footprint.</summary>
        private void EmitComplementBody(ClauseExecution exec, long cardinality)
        {
            if (exec.ClauseType is ClauseType.In)
            {
                // isSeed:true so FillFromPostingSource overwrites the ephemeral bitmap — no ClearBitmap needed.
                EmitInOps(exec.InTermCount, cardinality, bitmapLocal: EphemeralBitmap, isSeed: true);
                _ops.Add(new PlanOp { Kind = PlanOpKind.AndNotBitmaps, BitmapLocal = 0, ParamIndex2 = EphemeralBitmap });
                return;
            }

            if (exec.ClauseType is ClauseType.AllIn)
            {
                EmitAllInOps(exec.InTermCount, cardinality, bitmapLocal: EphemeralBitmap, suppressEarlyExit: true);
                _ops.Add(new PlanOp { Kind = PlanOpKind.AndNotBitmaps, BitmapLocal = 0, ParamIndex2 = EphemeralBitmap });
                return;
            }

            // Single-term positive form (Equals/NotEquals/Exists/StartsWith/range/...).
            // The AndNotFrom* op reads matchIndex from its source family and removes those entries.
            _ops.Add(new PlanOp
            {
                Kind = ToPlanOpKind(MergeKind.AndNotInto, GetDispatch(exec.Clause)),
                ParamIndex = _matchIndex,
                BitmapLocal = 0,
                EstimatedCardinality = cardinality
            });
            _matchIndex++;
        }

        private void EmitInOps(int inTermCount, long cardinality, int bitmapLocal, bool isSeed)
        {
            int totalSlots = inTermCount + 1; // inTermCount non-null terms + 1 null-term slot

            _ops.Add(new PlanOp
            {
                Kind = isSeed ? PlanOpKind.FillFromPostingSource : PlanOpKind.OrFromPostingSource,
                ParamIndex = _matchIndex,
                BitmapLocal = bitmapLocal,
                EstimatedCardinality = Math.Max(1, cardinality / totalSlots)
            });
            _ops.Add(new PlanOp
            {
                Kind = PlanOpKind.OrRangeFromPostingSource,
                ParamIndex = _matchIndex + 1,
                ParamIndex2 = _nextRangeIdx++, // where to find the count of terms for this in ctx.InRangeCounts[...]
                BitmapLocal = bitmapLocal,
                EstimatedCardinality = cardinality
            });
            _matchIndex += totalSlots;
        }

        /// <summary>Emit ops for an AllIn clause (as a seed): Fill slot 0 + AndRange for the rest.
        /// Same fixed shape rationale as <see cref="EmitInOps"/> — the count of remaining
        /// terms lives in <c>ctx.InRangeCounts</c> rather than the op shape itself.
        /// <paramref name="inTermCount"/> must match <c>exec.InTermCount</c> so the slot
        /// layout agrees with the resolver walk.</summary>
        private void EmitAllInOps(int inTermCount, long cardinality, int bitmapLocal, bool suppressEarlyExit)
        {
            int totalSlots = inTermCount + 1; // inTermCount non-null terms + 1 null-term slot
            // Fill consumes slot 0, AndRange iterates the rest. The range count
            // covers all slots after slot 0 (including the null-term slot).
            // The value at this index is filled in at FinalizePlan time by the
            // fused CardinalityArrayBuilder.Build walk.
            int rangeIdx = _nextRangeIdx++;

            _ops.Add(new PlanOp
            {
                Kind = PlanOpKind.FillFromPostingSource,
                ParamIndex = _matchIndex,
                BitmapLocal = bitmapLocal,
                EstimatedCardinality = Math.Max(1, cardinality / totalSlots)
            });
            _ops.Add(new PlanOp
            {
                Kind = PlanOpKind.AndRangeFromPostingSource,
                ParamIndex = _matchIndex + 1,
                ParamIndex2 = rangeIdx,
                BitmapLocal = bitmapLocal,
                EstimatedCardinality = cardinality,
                SkipEarlyExit = suppressEarlyExit
            });
            _matchIndex += totalSlots;
        }

        private static PlanOp[] BuildAllEntriesPlan()
        {
            // No bitmap needed — AllEntries already implements IQueryMatch.Fill(),
            // so we iterate it directly without materializing into a bitmap first.
            return [new PlanOp { Kind = PlanOpKind.FillFromMatch, ParamIndex = 0 }];
        }

        private static bool AreAllScanEligible(List<ClauseExecution> executions, int startIndex)
        {
            // If any clause (In, AllIn, Spatial, Vector, Search, etc.) can't be scanned, we must not emit MaybeEntryScan — entry scan would skip them entirely.
            for (int j = startIndex; j < executions.Count; j++)
            {
                if (IsScanEligible(executions[j]) == false)
                    return false;
            }

            return true;
        }
    }
}
