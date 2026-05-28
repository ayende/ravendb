using System;
using System.Collections.Generic;
using System.Diagnostics;
using Corax.Querying.Planning;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

internal static partial class QueryPlanBuilder
{
    /// <summary>Builds the structural PlanOp sequence for a (cache-miss) query.
    /// Pulls the previously-static EmitXxx methods together so the emission
    /// state (ops buffer, range-count buffer, match cursor, scratch-slot counters)
    /// lives on fields instead of being threaded through every signature.</summary>
    private sealed class PlanEmitter
    {
        private readonly List<PlanOp> _ops = [];
        // Index counter for IN/AllIn range slots. Each EmitInOps/EmitAllInOps consumes
        // one slot; the values themselves are computed at FinalizePlan time by
        // CardinalityArrayBuilder.Build from the same execution-tree walk that produces
        // cardinalities, so PlanEmitter only needs the running index to bake into
        // PlanOp.ParamIndex2 — IL reads ctx.InRangeCounts[that idx] at runtime.
        private int _nextRangeIdx;
        private int _matchIndex;
        private int _nextScratch = 2;
        private int _maxScratchUsed = 1;

        /// <summary>Emits the structural plan for a (cache-miss) query. Stays parameter-blind:
        /// when <see cref="QueryExecution.QueryWillReturnNoResults"/> is set the caller has already
        /// short-circuited with <c>return default</c> (see <see cref="Build"/>) so this method
        /// never has to encode "guaranteed zero results" — it works off the structural execution
        /// list alone. Empty-IN clauses (InTermCount=0, HasNullTerm=false) are NOT compacted out
        /// here: the runtime handles them via <c>ctx.InRangeCounts[i]=0</c> (range loop becomes
        /// no-op) and the slot-0 op reading <c>TryGetPostingListForNull</c> (empty posting). Skipping
        /// a clause based on per-execution parameter shape would poison the cached plan for
        /// subsequent executions with different IN-array sizes — the cache key does not encode
        /// IN array length.</summary>
        public static (PlanOp[] Ops, int RequiredBitmaps) Emit(PlanTemplate template, List<ClauseExecution> executions, PlanParameters planParams)
        {
            if (executions.Count is 0)
                return (BuildAllEntriesPlan(), 2);

            var emitter = new PlanEmitter();
            var (ops, bitmaps) = template.IsOr ? emitter.EmitOrPlan(executions) : emitter.EmitAndPlan(executions);
            if (planParams.HasBoost)
            {
                // we require query match for boost, because the other options cannot compute it
                for (int i = 0; i < ops.Length; i++)
                {
                    ops[i].Dispatch = MatchDispatch.QueryMatch;
                }
            }
            return (ops, bitmaps);
        }

        private (PlanOp[] Ops, int RequiredBitmaps) Complete()
        {
            _ops.Add(new PlanOp { Kind = PlanOpKind.GotoDone });
            return (_ops.ToArray(), Math.Max(2, _maxScratchUsed + 1));
        }

        /// <summary>Emit the PlanOp sequence for an OR chain. All clauses are merged into
        /// slot 0: the first via Fill, the rest via OrInto. Groups recurse through
        /// <see cref="EmitClauseInto"/>, allocating scratch slots on demand. SkipEarlyExit
        /// is forced on every AND-step because remaining OR terms may still match.
        /// Returns RequiredBitmaps = max(2, deepest scratch slot used + 1).</summary>
        private (PlanOp[] Ops, int RequiredBitmaps) EmitOrPlan(List<ClauseExecution> executions)
        {
            for (int i = 0; i < executions.Count; i++)
            {
                var exec = executions[i];
                EmitClauseInto(exec,
                    i == 0 ? MergeKind.Fill : MergeKind.OrInto,
                    exec.Cardinality, suppressEarlyExit: true);
            }
            return Complete();
        }

        /// <summary>Emit the PlanOp sequence for an AND chain. Single-clause Equals/NotEquals
        /// retain their specialised plans (FillFromLeaf, FillAllEntries+AndNot). Otherwise
        /// the first non-negated clause seeds slot 0 (Fill) and each subsequent clause is
        /// merged via AndInto or AndNotInto through <see cref="EmitClauseInto"/>. When every
        /// clause is negated we seed with FillAllEntries instead and AndNot all of them.
        /// MaybeEntryScan is emitted before each iteration when remaining clauses
        /// are scan-eligible; GotoDoneIfEmpty follows each non-negated step.</summary>
        private (PlanOp[] Ops, int RequiredBitmaps) EmitAndPlan(List<ClauseExecution> executions)
        {
            var e0 = executions[0];
            switch (executions.Count)
            {
                case 1 when e0.ClauseType == ClauseType.Equals && e0.IsNegated is false:
                    _ops.Add(new PlanOp
                    {
                        Kind = PlanOpKind.FillFromLeaf,
                        ParamIndex = 0,
                        EstimatedCardinality = e0.Cardinality,
                        Dispatch = GetDispatch(e0.Clause)
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
                        Kind = PlanOpKind.AndNotWithLeaf,
                        EstimatedCardinality = e0.Cardinality,
                        Dispatch = GetDispatch(e0.Clause)
                    });
                    _ops.Add(new PlanOp { Kind = PlanOpKind.GotoDone });

                    // Mark clause as negated so ResolveMatches/ResolveTermSources
                    // produce [AllEntries, TermMatch].
                    if (!e0.IsNegated)
                    {
                        e0.IsNegated = true;
                    }

                    return (_ops.ToArray(), 2);
            }

            // AND chain: Fill the smallest non-negated, then AndWith/AndNotWith the rest.
            // If the first clause is negated (cardinality sort puts negated clauses last,
            // so first-negated ⇒ all-negated) we seed with FillAllEntries instead and
            // AndNot every clause. FillAllEntries calls indexSearcher.AllEntries() directly,
            // avoiding the structural-vs-runtime slot-index mismatch that bites IN with a
            // parameter-bound array of different length.
            bool firstIsNegated = e0.IsNegated || e0.ClauseType == ClauseType.NotEquals;
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
                bool stepNegated = execI.IsNegated || execI.ClauseType == ClauseType.NotEquals;
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
            // Negated leaf of an OR chain. Build the complement at IL time via FillAllEntries +
            // AndNot of the positive form (single term, IN union, or AllIn intersection). The slot
            // footprint follows the POSITIVE form's layout — CountClauseLeaves and
            // ResolveClauseLeavesInto agree. Cancellation and timing come for free from the per-term
            // cursor machinery.
            //
            // Boost on a negated leaf is intentionally a no-op (matches Lucene): boosting is
            // scoring for a match, and a negation produces a complement, not a match — there's
            // nothing to score. The BoostFactor on such a clause is silently ignored.
            if (exec.Clause.IsOrChainNotEquals)
            {
                EmitNegatedLeafInto(exec, merge, cardinality);
                return;
            }

            if (TryGetGroupFanOut(exec.Clause, exec, out _, out var subExecs))
            {
                EmitGroupInto(exec, subExecs, merge, suppressEarlyExit);
                return;
            }

            if (exec.ClauseType is ClauseType.In)
            {
                EmitInLeaf(exec, cardinality, merge);
                return;
            }

            if (exec.ClauseType is ClauseType.AllIn)
            {
                EmitAllInLeaf(exec, cardinality, merge, suppressEarlyExit);
                return;
            }

            EmitLeafMergeOp(merge, cardinality, GetDispatch(exec.Clause), suppressEarlyExit);
            _matchIndex++;
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

            int saveSlot = _nextScratch++;
            if (saveSlot > _maxScratchUsed) _maxScratchUsed = saveSlot;

            _ops.Add(new PlanOp { Kind = PlanOpKind.ClearBitmap, BitmapLocal = saveSlot });
            _ops.Add(new PlanOp { Kind = PlanOpKind.SwapBitmaps, BitmapLocal = 0, ParamIndex2 = saveSlot });

            // Inside the saved context, AndWithLeaf/AndLeafRange MUST NOT early-exit to
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
                    // AndNotBitmaps[0, saveSlot] = slot 0 \ saveSlot. After build,
                    // slot 0 = group result, saveSlot = original accumulator. We
                    // want (orig \ group) so swap operands back first.
                    _ops.Add(new PlanOp { Kind = PlanOpKind.SwapBitmaps, BitmapLocal = 0, ParamIndex2 = saveSlot });
                    _ops.Add(new PlanOp { Kind = PlanOpKind.AndNotBitmaps, BitmapLocal = 0, ParamIndex2 = saveSlot });
                    break;
            }

            _ops.Add(new PlanOp { Kind = PlanOpKind.ClearBitmap, BitmapLocal = saveSlot });
            _nextScratch--;
        }

        /// <summary>Build a group's complete result in slot 0 (slot 0 must be empty/usable
        /// on entry; the caller arranges this either by being the seed Fill or by swapping
        /// the live accumulator out). OrGroup: Fill first sub, OR rest. AndGroup: Fill
        /// first sub (or FillAllEntries if first is negated), AND/ANDNOT rest.</summary>
        private void EmitGroupContentsInSlot0(ClauseExecution exec, List<ClauseExecution> subExecs, bool suppressEarlyExit)
        {
            int subCount = subExecs.Count;
            long subCard = exec.Cardinality / Math.Max(1, subCount);
            bool isOr = exec.ClauseType == ClauseType.OrGroup;

            if (isOr)
            {
                for (int si = 0; si < subCount; si++)
                {
                    EmitClauseInto(subExecs[si],
                        si == 0 ? MergeKind.Fill : MergeKind.OrInto,
                        subCard, suppressEarlyExit);
                }
                return;
            }

            // AndGroup
            bool firstIsNeg = subExecs[0].IsNegated || subExecs[0].ClauseType == ClauseType.NotEquals;
            int start;
            if (firstIsNeg)
            {
                _ops.Add(new PlanOp { Kind = PlanOpKind.FillAllEntries, EstimatedCardinality = long.MaxValue });
                start = 0;
            }
            else
            {
                EmitClauseInto(subExecs[0], MergeKind.Fill, subCard, suppressEarlyExit);
                start = 1;
            }
            for (int si = start; si < subCount; si++)
            {
                bool subNeg = subExecs[si].IsNegated || subExecs[si].ClauseType == ClauseType.NotEquals;
                EmitClauseInto(subExecs[si],
                    subNeg ? MergeKind.AndNotInto : MergeKind.AndInto,
                    subCard, suppressEarlyExit);
            }
        }

        /// <summary>Emit one PlanOp for a simple leaf clause according to <paramref name="merge"/>.
        /// Sets SkipEarlyExit on AndWithLeaf when inside a saved-swap context.</summary>
        private void EmitLeafMergeOp(MergeKind merge, long cardinality, MatchDispatch dispatch, bool suppressEarlyExit)
        {
            PlanOpKind kind = merge switch
            {
                MergeKind.Fill => PlanOpKind.FillFromLeaf,
                MergeKind.OrInto => PlanOpKind.OrWithLeaf,
                MergeKind.AndInto => PlanOpKind.AndWithLeaf,
                MergeKind.AndNotInto => PlanOpKind.AndNotWithLeaf,
                _ => throw new InvalidOperationException($"Unhandled MergeKind: {merge}")
            };
            _ops.Add(new PlanOp
            {
                Kind = kind,
                ParamIndex = _matchIndex,
                BitmapLocal = 0,
                EstimatedCardinality = cardinality,
                Dispatch = dispatch,
                SkipEarlyExit = kind == PlanOpKind.AndWithLeaf && suppressEarlyExit
            });
        }

        /// <summary>IN clause leaf — logically (term0 ∪ term1 ∪ … ∪ termN). For Fill/OrInto
        /// merges, build directly in slot 0 via EmitInOps. For AndInto/AndNotInto, build
        /// the union in slot 1 (slot 1 is freshly cleared first), then merge into slot 0
        /// via AndBitmaps/AndNotBitmaps. OrRange ignores SkipEarlyExit so suppression
        /// doesn't need to propagate here.</summary>
        private void EmitInLeaf(ClauseExecution exec, long cardinality, MergeKind merge)
        {
            int inTermCount = exec.InTermCount;
            if (merge is MergeKind.Fill or MergeKind.OrInto)
            {
                EmitInOps(inTermCount, cardinality, bitmapLocal: 0, isSeed: merge == MergeKind.Fill);
                return;
            }

            // AndInto / AndNotInto: union IN terms in slot 1, then merge with slot 0.
            if (1 > _maxScratchUsed) _maxScratchUsed = 1;
            _ops.Add(new PlanOp { Kind = PlanOpKind.ClearBitmap, BitmapLocal = 1 });
            EmitInOps(inTermCount, cardinality, bitmapLocal: 1, isSeed: false);
            _ops.Add(new PlanOp
            {
                Kind = merge == MergeKind.AndInto ? PlanOpKind.AndBitmaps : PlanOpKind.AndNotBitmaps,
                BitmapLocal = 0,
                ParamIndex2 = 1
            });
        }

        /// <summary>AllIn clause leaf — logically (term0 ∩ term1 ∩ … ∩ termN). For Fill merge,
        /// build directly in slot 0. For OrInto/AndInto/AndNotInto, save slot 0 to a scratch
        /// slot, build the intersection in slot 0 (Fill + AndRange), then merge back. The
        /// AndRange op honors SkipEarlyExit; in a saved context we must set it so the loop
        /// doesn't jump to doneLabel mid-intersection.</summary>
        private void EmitAllInLeaf(ClauseExecution exec, long cardinality, MergeKind merge, bool suppressEarlyExit)
        {
            int inTermCount = exec.InTermCount;
            if (merge == MergeKind.Fill)
            {
                EmitAllInOps(inTermCount, cardinality, bitmapLocal: 0);
                if (suppressEarlyExit)
                    SetLastAndRangeSkipEarlyExit();
                return;
            }

            int saveSlot = _nextScratch++;
            if (saveSlot > _maxScratchUsed) _maxScratchUsed = saveSlot;

            _ops.Add(new PlanOp { Kind = PlanOpKind.ClearBitmap, BitmapLocal = saveSlot });
            _ops.Add(new PlanOp { Kind = PlanOpKind.SwapBitmaps, BitmapLocal = 0, ParamIndex2 = saveSlot });
            EmitAllInOps(inTermCount, cardinality, bitmapLocal: 0);
            // Inside save-swap: AndRange must not jump to doneLabel — that would skip
            // the merge-back and leak the saved accumulator.
            SetLastAndRangeSkipEarlyExit();

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
            _nextScratch--;
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
            int saveSlot = _nextScratch++;
            if (saveSlot > _maxScratchUsed) _maxScratchUsed = saveSlot;

            _ops.Add(new PlanOp { Kind = PlanOpKind.ClearBitmap, BitmapLocal = saveSlot });
            _ops.Add(new PlanOp { Kind = PlanOpKind.SwapBitmaps, BitmapLocal = 0, ParamIndex2 = saveSlot });

            _ops.Add(new PlanOp { Kind = PlanOpKind.FillAllEntries, EstimatedCardinality = long.MaxValue });
            EmitComplementBody(exec, cardinality);

            _ops.Add(new PlanOp { Kind = PlanOpKind.LazyOrBitmaps, BitmapLocal = 0, ParamIndex2 = saveSlot });
            _ops.Add(new PlanOp { Kind = PlanOpKind.ClearBitmap, BitmapLocal = saveSlot });
            _nextScratch--;
        }

        /// <summary>Turn slot 0 (currently <see cref="PlanOpKind.FillAllEntries"/>) into the
        /// complement of <paramref name="exec"/>'s positive form. IN unions the terms into slot 1
        /// then AndNotBitmaps(0, 1); AllIn intersects into slot 1 then AndNotBitmaps(0, 1).
        /// Scalar / Exists / Range clauses use AndNotWithLeaf directly (the
        /// <see cref="PlanOp.Dispatch"/> follows <see cref="GetDispatch"/> for the positive form).
        /// Advances <see cref="_matchIndex"/> past the clause's slot footprint.</summary>
        private void EmitComplementBody(ClauseExecution exec, long cardinality)
        {
            if (exec.ClauseType is ClauseType.In)
            {
                if (1 > _maxScratchUsed) _maxScratchUsed = 1;
                // isSeed:true so FillFromLeaf overwrites slot 1 — no ClearBitmap needed.
                EmitInOps(exec.InTermCount, cardinality, bitmapLocal: 1, isSeed: true);
                _ops.Add(new PlanOp { Kind = PlanOpKind.AndNotBitmaps, BitmapLocal = 0, ParamIndex2 = 1 });
                return;
            }

            if (exec.ClauseType is ClauseType.AllIn)
            {
                if (1 > _maxScratchUsed) _maxScratchUsed = 1;
                EmitAllInOps(exec.InTermCount, cardinality, bitmapLocal: 1);
                // AndRange would early-exit to doneLabel if slot 1 empties mid-intersection,
                // skipping our AndNotBitmaps and the rest of the OR chain. Suppress it.
                SetLastAndRangeSkipEarlyExit();
                _ops.Add(new PlanOp { Kind = PlanOpKind.AndNotBitmaps, BitmapLocal = 0, ParamIndex2 = 1 });
                return;
            }

            // Single-term positive form (Equals/NotEquals/Exists/StartsWith/range/...).
            // AndNotWithLeaf reads matchIndex per Dispatch and removes those entries.
            _ops.Add(new PlanOp
            {
                Kind = PlanOpKind.AndNotWithLeaf,
                ParamIndex = _matchIndex,
                BitmapLocal = 0,
                EstimatedCardinality = cardinality,
                Dispatch = GetDispatch(exec.Clause)
            });
            _matchIndex++;
        }

        private void EmitInOps(int inTermCount, long cardinality, int bitmapLocal, bool isSeed)
        {
            int totalSlots = inTermCount + 1; // inTermCount non-null terms + 1 null-term slot
            // Range iterates over the slots AFTER slot 0 (which Fill handles). When the parameter
            // list has no null, the trailing null slot is Empty — ORing with Empty is a no-op, so
            // we can safely include it (rangeCount = totalSlots - 1). When the list HAS a null
            // term, that slot is non-empty and we want to OR it in. Both cases use the same range.
            // The value stored at this index is filled in at FinalizePlan time by the
            // fused CardinalityArrayBuilder.Build walk.
            int rangeIdx = _nextRangeIdx++;

            _ops.Add(new PlanOp
            {
                Kind = isSeed ? PlanOpKind.FillFromLeaf : PlanOpKind.OrWithLeaf,
                ParamIndex = _matchIndex,
                BitmapLocal = bitmapLocal,
                EstimatedCardinality = Math.Max(1, cardinality / totalSlots),
                Dispatch = MatchDispatch.PostingList
            });
            _ops.Add(new PlanOp
            {
                Kind = PlanOpKind.OrLeafRange,
                ParamIndex = _matchIndex + 1,
                ParamIndex2 = rangeIdx,
                BitmapLocal = bitmapLocal,
                EstimatedCardinality = cardinality,
                Dispatch = MatchDispatch.PostingList
            });
            _matchIndex += totalSlots;
        }

        /// <summary>Emit ops for an AllIn clause (as a seed): Fill slot 0 + AndRange for the rest.
        /// Same fixed shape rationale as <see cref="EmitInOps"/> — the count of remaining
        /// terms lives in <c>ctx.InRangeCounts</c> rather than the op shape itself.
        /// <paramref name="inTermCount"/> must match <c>exec.InTermCount</c> so the slot
        /// layout agrees with the resolver walk.</summary>
        private void EmitAllInOps(int inTermCount, long cardinality, int bitmapLocal)
        {
            int totalSlots = inTermCount + 1; // inTermCount non-null terms + 1 null-term slot
            // Fill consumes slot 0, AndRange iterates the rest. The range count
            // covers all slots after slot 0 (including the null-term slot).
            // The value at this index is filled in at FinalizePlan time by the
            // fused CardinalityArrayBuilder.Build walk.
            int rangeIdx = _nextRangeIdx++;

            _ops.Add(new PlanOp
            {
                Kind = PlanOpKind.FillFromLeaf,
                ParamIndex = _matchIndex,
                BitmapLocal = bitmapLocal,
                EstimatedCardinality = Math.Max(1, cardinality / totalSlots),
                Dispatch = MatchDispatch.PostingList
            });
            _ops.Add(new PlanOp
            {
                Kind = PlanOpKind.AndLeafRange,
                ParamIndex = _matchIndex + 1,
                ParamIndex2 = rangeIdx,
                BitmapLocal = bitmapLocal,
                EstimatedCardinality = cardinality,
                Dispatch = MatchDispatch.PostingList
            });
            _matchIndex += totalSlots;
        }

        /// <summary>Set <see cref="PlanOp.SkipEarlyExit"/>=true on the most recent
        /// <see cref="PlanOpKind.AndLeafRange"/> in <see cref="_ops"/>. Used by
        /// <see cref="EmitComplementBody"/> after emitting the AllIn pair so that
        /// an empty intersection doesn't early-exit out of the surrounding negation.</summary>
        private void SetLastAndRangeSkipEarlyExit()
        {
            for (int i = _ops.Count - 1; i >= 0; i--)
            {
                if (_ops[i].Kind == PlanOpKind.AndLeafRange)
                {
                    var op = _ops[i];
                    op.SkipEarlyExit = true;
                    _ops[i] = op;
                    return;
                }
            }
        }

        private static PlanOp[] BuildAllEntriesPlan()
        {
            // No bitmap needed — AllEntries already implements IQueryMatch.Fill(),
            // so we iterate it directly without materializing into a bitmap first.
            return [new PlanOp { Kind = PlanOpKind.FillFromLeaf, ParamIndex = 0 }];
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
