using System;
using System.Collections.Generic;
using System.Diagnostics;
using Corax.Querying.Planning;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

internal sealed class PlanEmitter
{
    private readonly List<PlanOp> _ops = [];
    private int _nextRangeIdx; // index for ctx.InRangeCounts[that idx] at runtime.
    private int _matchIndex;

    // Slot 0 is the live accumulator; EphemeralBitmap stages "build a set then merge into slot 0"
    // (IN/AllIn). Its value never outlives one leaf emission, so a single fixed slot is reusable at
    // any nesting depth and never counts against the scratch high-water mark; save-stack starts above it.
    // Bound to the Corax primitive's AND scratch slot so the two layers share one constant: the AND
    // primitives clobber this slot, so it must never be a durable accumulator on either side.
    private const int EphemeralBitmap = global::Corax.Querying.Primitives.QueryPrimitives.AndScratchBitmapSlot;
    private int _nextScratch = EphemeralBitmap + 1;
    private int _maxScratchUsed = EphemeralBitmap;

    public static (PlanOp[] Ops, int RequiredBitmaps) Emit(PlanTemplate template, List<ClauseExecution> executions, PlanParameters planParams, ScanPredicateInfo?[] perClause)
    {
        if (executions.Count is 0) // a genuinely clause-less query (no WHERE) — match every doc.
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

        // ClauseExecution.CompareTo sorts negated clauses last, so foldable negated leaves arrive as a
        // contiguous suffix. We fold any contiguous run of ≥2 foldable negated leaves by De Morgan into a single
        // complement; positives and lone negations emit normally. This handles mixed chains such as
        // (C = 4 OR A NOT IN (…) OR B NOT IN (…)) — the positive ORs in, the two negations fold together.
        bool first = true;
        int i = 0;
        while (i < executions.Count)
        {
            int runEnd = FoldableNegatedRunEnd(i);
            if (runEnd - i >= 2) // ≥2 foldable negations: collapse N FillAllEntries into one complement
            {
                EmitFoldedNegatedRun(executions, i, runEnd, first);
                first = false;
                i = runEnd;
                continue;
            }

            EmitClauseInto(executions[i], first ? MergeKind.Fill : MergeKind.OrInto, executions[i].Cardinality, suppressEarlyExit: true);
            first = false;
            i++;
        }

        return Complete();
            
        int FoldableNegatedRunEnd(int j)
        {
            while (j < executions.Count && IsFoldableNegatedLeaf(executions[j]))
                j++;
            return j;
        }
    }

    /// <summary>A negated OR-chain member that can take part in a De Morgan fold: a real leaf (not a
    /// collapse sentinel, not an Or/And sub-group — a group would not round-trip through
    /// <see cref="EmitPositiveForm"/>'s single-match default) carrying
    /// <see cref="ClauseInfo.IsOrChainNotEquals"/>.</summary>
    private static bool IsFoldableNegatedLeaf(ClauseExecution e)
    {
        if (e.IsSentinel) // a collapse sentinel has no positive form to intersect; never fold it
            return false;
        if (e.Clause.IsOrChainNotEquals == false)
            return false;
        return e.ClauseType is not (ClauseType.OrGroup or ClauseType.AndGroup);
    }

    /// <summary>An OR chain whose members are ALL foldable negations (used by the nested-group fold at
    /// <see cref="EmitGroupInto"/>). Requires ≥2 members — a lone negated leaf already emits exactly one
    /// Fill+AndNot, so there is nothing to collapse.</summary>
    private static bool CanFoldNegatedOr(List<ClauseExecution> executions)
    {
        if (executions.Count < 2)
            return false;

        foreach (var e in executions)
        {
            if (IsFoldableNegatedLeaf(e) == false)
                return false;
        }

        return true;
    }

    /// <summary>Fold a contiguous run <c>[from, to)</c> of foldable negated leaves by De Morgan into a
    /// single complement: <c>¬A ∨ ¬B ∨ … = ¬(A ∧ B ∧ …)</c>. Without the fold each member emits its own
    /// <see cref="PlanOpKind.FillAllEntries"/> + AndNot (one full-universe scan apiece); folded, we
    /// intersect the (typically selective) positive forms once and take a single complement. The run's
    /// members are emitted in list order so <c>_matchIndex</c> stays aligned with leaf resolution.
    /// <paramref name="isFirst"/> selects whether the complement seeds slot 0 directly or ORs into an
    /// existing accumulator.</summary>
    private void EmitFoldedNegatedRun(List<ClauseExecution> executions, int from, int to, bool isFirst)
    {
        using var _ = AllocateScratchSlot(out int saveSlot);
        
        EmitPositiveIntersection();
        if (isFirst is true)
            return;

        using var __ = AllocateScratchSlot(out int interSlot);
        _ops.Add(new PlanOp { Kind = PlanOpKind.SwapBitmaps, BitmapLocal = 0, ParamIndex2 = interSlot }); // park A ∧ B ∧ …
        _ops.Add(new PlanOp { Kind = PlanOpKind.FillAllEntries, EstimatedCardinality = long.MaxValue });
        _ops.Add(new PlanOp { Kind = PlanOpKind.LazyOrBitmaps, BitmapLocal = 0, ParamIndex2 = interSlot });

        void EmitPositiveIntersection()
        {
            EmitPositiveForm(executions[from], MergeKind.Fill, executions[from].Cardinality, suppressEarlyExit: true);
            for (int i = from + 1; i < to; i++)
                EmitPositiveForm(executions[i], MergeKind.AndInto, executions[i].Cardinality, suppressEarlyExit: true);

            _ops.Add(new PlanOp { Kind = PlanOpKind.SwapBitmaps, BitmapLocal = 0, ParamIndex2 = saveSlot });
            _ops.Add(new PlanOp { Kind = PlanOpKind.FillAllEntries, EstimatedCardinality = long.MaxValue });
            _ops.Add(new PlanOp { Kind = PlanOpKind.AndNotBitmaps, BitmapLocal = 0, ParamIndex2 = saveSlot });
        }
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

            // Suppress the leaf's built-in empty-check: a plain AndFrom* leaf would otherwise emit its own
            // "if (bitmap[0].IsEmpty) goto Done" AND we'd add the explicit GotoDoneIfEmpty below — two
            // identical checks back-to-back. Merge leaves (In/AllIn/group → AndBitmaps) don't self-guard,
            // so the explicit op is the single uniform empty-check for every clause shape.
            EmitClauseInto(cur, merge, cur.Cardinality, suppressEarlyExit: true);
            if (cur.IsNegated is false) // when we have 0 results, early exit
            {
                _ops.Add(new PlanOp { Kind = PlanOpKind.GotoDoneIfEmpty, BitmapLocal = 0 });
            }
        }

        return Complete();
    }

    private void EmitClauseInto(ClauseExecution exec, MergeKind merge, long cardinality, bool suppressEarlyExit)
    {
        // A collapse sentinel consumes no leaf and bakes straight into bitmap algebra; it must be
        // intercepted before the IsOrChainNotEquals routing (the underlying clause may have been a
        // negated leaf before it was stamped) and before _matchIndex is ever touched.
        if (exec.IsSentinel)
        {
            EmitSentinelInto(exec, merge);
            return;
        }

        if (exec.Clause.IsOrChainNotEquals)
        {
            EmitNegatedLeafInto(exec, merge, cardinality);
            return;
        }

        EmitPositiveForm(exec, merge, cardinality, suppressEarlyExit);
    }

    /// <summary>Bake a <see cref="ClauseType.MatchAll"/> / <see cref="ClauseType.MatchNothing"/> sentinel
    /// directly into the slot-0 bitmap. No match leaf is consumed, so <c>_matchIndex</c> is NOT advanced —
    /// keeping the emitter's leaf cursor aligned with leaf resolution and the cardinality array. The merge
    /// algebra IS the boolean simplification: MatchAll is the universe (x∨ALL=ALL, x∧ALL=x), MatchNothing is
    /// the empty set (x∨∅=x, x∧∅=∅).</summary>
    private void EmitSentinelInto(ClauseExecution exec, MergeKind merge)
    {
        switch (exec.ClauseType, merge)
        {
            case (ClauseType.MatchAll, MergeKind.Fill):
            case (ClauseType.MatchAll, MergeKind.OrInto):           // x ∨ ALL = ALL
                _ops.Add(new PlanOp { Kind = PlanOpKind.FillAllEntries, EstimatedCardinality = long.MaxValue });
                break;

            case (ClauseType.MatchAll, MergeKind.AndNotInto):       // x \ ALL = ∅ — defensive; MatchAll is never negated.
            case (ClauseType.MatchNothing, MergeKind.Fill):         // empty seed
            case (ClauseType.MatchNothing, MergeKind.AndInto):      // x ∧ ∅ = ∅
                _ops.Add(new PlanOp { Kind = PlanOpKind.ClearBitmap, BitmapLocal = 0 });
                break;
            
            case (ClauseType.MatchAll, MergeKind.AndInto):          // x ∧ ALL = x
            case (ClauseType.MatchNothing, MergeKind.OrInto):       // x ∨ ∅ = x
            case (ClauseType.MatchNothing, MergeKind.AndNotInto):   // x \ ∅ = x
                break;
        }
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
                    Kind = ToPlanOpKind(merge, QueryPlanBuilder.GetDispatch(exec)),
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

        // De Morgan in an AND context: an all-negated OR sub-group is ¬(A ∧ B ∧ …). When it is
        // combined with an existing accumulator via AND / ANDNOT the universe complement collapses
        // out — acc AND ¬X = acc \ X, acc ANDNOT ¬X = acc ∩ X — so we only build the (typically
        // selective) positive intersection X and never touch FillAllEntries. Without this the nested
        // group would emit one FillAllEntries + complement per member (the #4867 N-fill problem) and
        // then AND the near-universe result into the accumulator.
        if (exec.ClauseType == ClauseType.OrGroup && merge is MergeKind.AndInto or MergeKind.AndNotInto && CanFoldNegatedOr(subExecs))
        {
            EmitFoldedNegatedOrGroupIntoAccumulator(subExecs, merge);
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

    /// <summary>Fold an all-negated OR sub-group (<c>¬A ∨ ¬B ∨ … = ¬(A ∧ B ∧ …)</c>) directly into the
    /// slot-0 accumulator without materializing the universe. The accumulator parks in a scratch slot
    /// while the positive intersection X = A ∧ B ∧ … is built in slot 0 (Fill + AndInto, early-exit
    /// suppressed so a partial/empty intersection can't short-circuit), then the accumulator returns to
    /// slot 0 and X is combined: <see cref="MergeKind.AndInto"/> ⇒ <c>acc \ X</c> (AndNot),
    /// <see cref="MergeKind.AndNotInto"/> ⇒ <c>acc ∩ X</c> (And). Null/missing-field semantics are
    /// identical to the per-member FillAllEntries + AndNot path: a doc missing any field is absent from
    /// X, so AndNot keeps it and And drops it — same as taking the complement against the universe.</summary>
    private void EmitFoldedNegatedOrGroupIntoAccumulator(List<ClauseExecution> subExecs, MergeKind merge)
    {
        using var _ = AllocateScratchSlot(out int saveSlot);

        _ops.Add(new PlanOp { Kind = PlanOpKind.SwapBitmaps, BitmapLocal = 0, ParamIndex2 = saveSlot }); // park accumulator, free slot 0 to build X

        EmitPositiveForm(subExecs[0], MergeKind.Fill, subExecs[0].Cardinality, suppressEarlyExit: true);
        for (int i = 1; i < subExecs.Count; i++)
            EmitPositiveForm(subExecs[i], MergeKind.AndInto, subExecs[i].Cardinality, suppressEarlyExit: true);

        _ops.Add(new PlanOp { Kind = PlanOpKind.SwapBitmaps, BitmapLocal = 0, ParamIndex2 = saveSlot }); // slot 0 = accumulator, saveSlot = X
        _ops.Add(new PlanOp
        {
            Kind = merge == MergeKind.AndInto ? PlanOpKind.AndNotBitmaps : PlanOpKind.AndBitmaps,
            BitmapLocal = 0,
            ParamIndex2 = saveSlot
        });
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
                using var _ = AllocateScratchSlot(out int positiveSlot);
                EmitCommonInOps(exec.InTermCount, cardinality, positiveSlot, PlanOpKind.FillFromPostingSource, PlanOpKind.AndRangeFromPostingSource, suppressEarlyExit: true);
                _ops.Add(new PlanOp { Kind = PlanOpKind.AndNotBitmaps, BitmapLocal = 0, ParamIndex2 = positiveSlot });
                return;
            }
            default:
                _ops.Add(new PlanOp
                {
                    Kind = ToPlanOpKind(MergeKind.AndNotInto, QueryPlanBuilder.GetDispatch(exec)),
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
