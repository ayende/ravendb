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

            EmitClauseInto(executions[i], first ? MergeKind.Fill : MergeKind.OrInto, suppressEarlyExit: true, destSlot: 0);
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
        if (isFirst is true)
        {
            // slot 0 is empty: build the complement straight into it.
            EmitComplementOfIntersection( destSlot: 0);
            return;
        }

        // slot 0 holds the running OR accumulator. Build the complement in a fresh scratch slot
        // (so the complement's leading FillAllEntries can't clobber the accumulator), then OR it in.
        using var _ = AllocateScratchSlot(out int compSlot);
        EmitComplementOfIntersection(compSlot);
        _ops.Add(new PlanOp { Kind = PlanOpKind.LazyOrBitmaps, BitmapLocal = 0, ParamIndex2 = compSlot });
        
        void EmitComplementOfIntersection(int destSlot)
        {
            using var _ = AllocateScratchSlot(out int xSlot);
            EmitPositiveForm(executions[from], MergeKind.Fill, suppressEarlyExit: true, xSlot);
            for (int i = from + 1; i < to; i++)
                EmitPositiveForm(executions[i], MergeKind.AndInto, suppressEarlyExit: true, xSlot);

            _ops.Add(new PlanOp { Kind = PlanOpKind.FillAllEntries, BitmapLocal = destSlot });
            _ops.Add(new PlanOp { Kind = PlanOpKind.AndNotBitmaps, BitmapLocal = destSlot, ParamIndex2 = xSlot });
        }
    }


    private (PlanOp[] Ops, int RequiredBitmaps) EmitAndPlan(List<ClauseExecution> executions, ScanPredicateInfo?[] perClause)
    {
        var e0 = executions[0];
        if (e0.IsNegated)
            _ops.Add(new PlanOp { Kind = PlanOpKind.FillAllEntries });

        EmitClauseInto(e0, e0.IsNegated ? MergeKind.AndNotInto : MergeKind.Fill, suppressEarlyExit: false, destSlot: 0);

        // check if we have any clause after the first that we cannot scan on, we don't bother with the first since we always run it normally
        bool allScanEligible = perClause.AsSpan()[1..].Contains(null) is false;

        for (int i = 1; i < executions.Count; i++)
        {
            var cur = executions[i];

            // Only switch to entry scan before a clause that actually consumes a leaf. A sentinel
            // (MatchAll/MatchNothing) consumes no leaf and never advances the runtime cursor, so the cursor
            // has already moved past it onto the following real leaves; emitting a MaybeEntryScan here would
            // read Cardinalities[cursor] out of bounds (the cursor points past the leaf-indexed arrays).
            // The sentinel's bitmap algebra is still baked by EmitClauseInto below.
            if (allScanEligible && cur.IsSentinel == false) // if we can, check if we can move to entry scan after the first check
            {
                _ops.Add(new PlanOp
                {
                    Kind = PlanOpKind.MaybeEntryScan,
                    ParamIndex = _matchIndex
                });
            }

            MergeKind merge = cur.IsNegated ? MergeKind.AndNotInto : MergeKind.AndInto;

            // Suppress the leaf's built-in empty-check: a plain AndFrom* leaf would otherwise emit its own
            // "if (bitmap[0].IsEmpty) goto Done" AND we'd add the explicit GotoDoneIfEmpty below — two
            // identical checks back-to-back. Merge leaves (In/AllIn/group → AndBitmaps) don't self-guard,
            // so the explicit op is the single uniform empty-check for every clause shape.
            EmitClauseInto(cur, merge, suppressEarlyExit: true, destSlot: 0);
            if (cur.IsNegated is false) // when we have 0 results, early exit
            {
                _ops.Add(new PlanOp { Kind = PlanOpKind.GotoDoneIfEmpty, BitmapLocal = 0 });
            }
        }

        return Complete();
    }

    private void EmitClauseInto(ClauseExecution exec, MergeKind merge, bool suppressEarlyExit, int destSlot)
    {
        // A collapse sentinel consumes no leaf and bakes straight into bitmap algebra; it must be
        // intercepted before the IsOrChainNotEquals routing (the underlying clause may have been a
        // negated leaf before it was stamped) and before _matchIndex is ever touched.
        if (exec.IsSentinel)
        {
            EmitSentinelInto(exec, merge, destSlot);
            return;
        }

        if (exec.Clause.IsOrChainNotEquals)
        {
            EmitNegatedLeafInto(exec, merge, destSlot);
            return;
        }

        EmitPositiveForm(exec, merge, suppressEarlyExit, destSlot);
    }

    /// <summary>Bake a <see cref="ClauseType.MatchAll"/> / <see cref="ClauseType.MatchNothing"/> sentinel
    /// directly into the slot-0 bitmap. No match leaf is consumed, so <c>_matchIndex</c> is NOT advanced —
    /// keeping the emitter's leaf cursor aligned with leaf resolution and the cardinality array. The merge
    /// algebra IS the boolean simplification: MatchAll is the universe (x∨ALL=ALL, x∧ALL=x), MatchNothing is
    /// the empty set (x∨∅=x, x∧∅=∅).</summary>
    private void EmitSentinelInto(ClauseExecution exec, MergeKind merge, int destSlot)
    {
        switch (exec.ClauseType, merge)
        {
            case (ClauseType.MatchAll, MergeKind.Fill):
            case (ClauseType.MatchAll, MergeKind.OrInto):           // x ∨ ALL = ALL
                _ops.Add(new PlanOp { Kind = PlanOpKind.FillAllEntries, BitmapLocal = destSlot });
                break;

            case (ClauseType.MatchAll, MergeKind.AndNotInto):       // x \ ALL = ∅ — defensive; MatchAll is never negated.
            case (ClauseType.MatchNothing, MergeKind.Fill):         // empty seed
            case (ClauseType.MatchNothing, MergeKind.AndInto):      // x ∧ ∅ = ∅
                _ops.Add(new PlanOp { Kind = PlanOpKind.ClearBitmap, BitmapLocal = destSlot });
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
    private void EmitPositiveForm(ClauseExecution exec, MergeKind merge, bool suppressEarlyExit, int destSlot)
    {
        switch (exec.ClauseType)
        {
            case ClauseType.OrGroup or ClauseType.AndGroup when exec.Clause.SubClauses is { Count: > 0 }:
                EmitGroupInto(exec, exec.SubExecutions, merge, suppressEarlyExit, destSlot);
                break;
            case ClauseType.In:
                EmitInLeaf(exec, merge, destSlot);
                break;
            case ClauseType.AllIn:
                EmitAllInLeaf(exec, merge, suppressEarlyExit, destSlot);
                break;
            default:
                _ops.Add(new PlanOp
                {
                    Kind = ToPlanOpKind(merge, QueryPlanBuilder.GetDispatch(exec)),
                    ParamIndex = _matchIndex++,
                    BitmapLocal = destSlot,
                    SkipEarlyExit = merge == MergeKind.AndInto && suppressEarlyExit,
                    DebugLabel = Label(exec)
                });
                break;
        }
    }

    private void EmitGroupInto(ClauseExecution exec, List<ClauseExecution> subExecs, MergeKind merge, bool suppressEarlyExit, int destSlot)
    {
        if (merge == MergeKind.Fill)
        {
            EmitGroupContents(exec, subExecs, suppressEarlyExit, destSlot);
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
            EmitFoldedNegatedOrGroupIntoAccumulator(subExecs, merge, destSlot);
            return;
        }

        // Build the group into a fresh scratch slot, then merge it into destSlot directly — no parking
        // swap is needed because destSlot already holds the accumulator. AndFrom*/AndRangeFrom* inside the
        // group MUST NOT early-exit to doneLabel, hence suppressEarlyExit: true.
        using var _ = AllocateScratchSlot(out int groupSlot);
        EmitGroupContents(exec, subExecs, suppressEarlyExit: true, groupSlot);

        switch (merge)
        {
            case MergeKind.OrInto:
                _ops.Add(new PlanOp { Kind = PlanOpKind.LazyOrBitmaps, BitmapLocal = destSlot, ParamIndex2 = groupSlot });
                break;
            case MergeKind.AndInto:
                _ops.Add(new PlanOp { Kind = PlanOpKind.AndBitmaps, BitmapLocal = destSlot, ParamIndex2 = groupSlot });
                break;
            case MergeKind.AndNotInto:
                // destSlot \ groupSlot — accumulator stays put, group is the subtrahend, so no operand swap.
                _ops.Add(new PlanOp { Kind = PlanOpKind.AndNotBitmaps, BitmapLocal = destSlot, ParamIndex2 = groupSlot });
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
    private void EmitFoldedNegatedOrGroupIntoAccumulator(List<ClauseExecution> subExecs, MergeKind merge, int destSlot)
    {
        // Build the positive intersection X = A ∧ B ∧ … in a scratch slot (Fill + AndInto, early-exit
        // suppressed so a partial/empty intersection can't short-circuit), then combine it into destSlot:
        // the accumulator never leaves destSlot, so no parking swap is needed.
        using var _ = AllocateScratchSlot(out int xSlot);

        EmitPositiveForm(subExecs[0], MergeKind.Fill, suppressEarlyExit: true, xSlot);
        for (int i = 1; i < subExecs.Count; i++)
            EmitPositiveForm(subExecs[i], MergeKind.AndInto, suppressEarlyExit: true, xSlot);

        _ops.Add(new PlanOp
        {
            Kind = merge == MergeKind.AndInto ? PlanOpKind.AndNotBitmaps : PlanOpKind.AndBitmaps,
            BitmapLocal = destSlot,
            ParamIndex2 = xSlot
        });
    }

    private void EmitGroupContents(ClauseExecution exec, List<ClauseExecution> subExecs, bool suppressEarlyExit, int destSlot)
    {
        bool isOr = exec.ClauseType != ClauseType.OrGroup;
        bool firstNegated = isOr && subExecs[0].IsNegated;
        if (firstNegated)
            _ops.Add(new PlanOp { Kind = PlanOpKind.FillAllEntries, BitmapLocal = destSlot });
        var followupAction = isOr ? MergeKind.AndInto : MergeKind.OrInto;
        EmitClauseInto(subExecs[0], firstNegated ? MergeKind.AndNotInto : MergeKind.Fill, suppressEarlyExit, destSlot);
        for (int i = 1; i < subExecs.Count; i++)
        {
            MergeKind kind = isOr && subExecs[i].IsNegated ? MergeKind.AndNotInto : followupAction;
            EmitClauseInto(subExecs[i], kind, suppressEarlyExit, destSlot);
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
    private void EmitInLeaf(ClauseExecution exec, MergeKind merge, int destSlot)
    {
        if (merge is MergeKind.Fill or MergeKind.OrInto)
        {
            var firstKind = merge == MergeKind.Fill ? PlanOpKind.FillFromPostingSource : PlanOpKind.OrFromPostingSource;
            EmitCommonInOps(exec.InTermCount, destSlot, firstKind, PlanOpKind.OrRangeFromPostingSource, suppressEarlyExit: false, Label(exec));
            return;
        }
        EmitCommonInOps(exec.InTermCount, EphemeralBitmap, PlanOpKind.FillFromPostingSource, PlanOpKind.OrRangeFromPostingSource, suppressEarlyExit: false, Label(exec));
        _ops.Add(new PlanOp
        {
            Kind = merge == MergeKind.AndInto ? PlanOpKind.AndBitmaps : PlanOpKind.AndNotBitmaps,
            BitmapLocal = destSlot,
            ParamIndex2 = EphemeralBitmap
        });
    }

    /// <summary>AllIn clause leaf — logically (term0 ∩ term1 ∩ … ∩ termN).</summary>
    private void EmitAllInLeaf(ClauseExecution exec, MergeKind merge, bool suppressEarlyExit, int destSlot)
    {
        if (merge == MergeKind.Fill)
        {
            EmitCommonInOps(exec.InTermCount, destSlot, PlanOpKind.FillFromPostingSource, PlanOpKind.AndRangeFromPostingSource, suppressEarlyExit, Label(exec));
            return;
        }

        using var _ = AllocateScratchSlot(out int saveSlot);

        EmitCommonInOps(exec.InTermCount, saveSlot, PlanOpKind.FillFromPostingSource, PlanOpKind.AndRangeFromPostingSource, suppressEarlyExit: true, Label(exec));

        switch (merge)
        {
            case MergeKind.OrInto:
                _ops.Add(new PlanOp { Kind = PlanOpKind.LazyOrBitmaps, BitmapLocal = destSlot, ParamIndex2 = saveSlot });
                break;
            case MergeKind.AndInto:
                _ops.Add(new PlanOp { Kind = PlanOpKind.AndBitmaps, BitmapLocal = destSlot, ParamIndex2 = saveSlot });
                break;
            case MergeKind.AndNotInto:
                _ops.Add(new PlanOp { Kind = PlanOpKind.AndNotBitmaps, BitmapLocal = destSlot, ParamIndex2 = saveSlot });
                break;
        }
    }

    private void EmitNegatedLeafInto(ClauseExecution exec, MergeKind merge, int destSlot)
    {
        Debug.Assert(merge is MergeKind.Fill or MergeKind.OrInto,
            $"IsOrChainNotEquals only appears in OR chains; got merge={merge}");

        if (merge == MergeKind.Fill)
        {
            _ops.Add(new PlanOp { Kind = PlanOpKind.FillAllEntries, BitmapLocal = destSlot });
            EmitComplementBody(exec, destSlot);
            return;
        }

        // OR into an existing accumulator: build the complement (ALL \ positive) in a fresh scratch slot
        // so its leading FillAllEntries can't clobber the accumulator, then OR it into destSlot.
        using var _ = AllocateScratchSlot(out int compSlot);

        _ops.Add(new PlanOp { Kind = PlanOpKind.FillAllEntries, BitmapLocal = compSlot });
        EmitComplementBody(exec, compSlot);
        _ops.Add(new PlanOp { Kind = PlanOpKind.LazyOrBitmaps, BitmapLocal = destSlot, ParamIndex2 = compSlot });
    }

    /// <summary>Subtract the clause's positive form from <paramref name="destSlot"/> (which must already hold
    /// the universe): <c>destSlot = destSlot \ positive</c>.</summary>
    private void EmitComplementBody(ClauseExecution exec, int destSlot)
    {
        switch (exec.ClauseType)
        {
            case ClauseType.In:
                EmitCommonInOps(exec.InTermCount, EphemeralBitmap, PlanOpKind.FillFromPostingSource, PlanOpKind.OrRangeFromPostingSource, suppressEarlyExit: false, Label(exec));
                _ops.Add(new PlanOp { Kind = PlanOpKind.AndNotBitmaps, BitmapLocal = destSlot, ParamIndex2 = EphemeralBitmap });
                return;
            case ClauseType.AllIn:
            {
                using var _ = AllocateScratchSlot(out int positiveSlot);
                EmitCommonInOps(exec.InTermCount, positiveSlot, PlanOpKind.FillFromPostingSource, PlanOpKind.AndRangeFromPostingSource, suppressEarlyExit: true, Label(exec));
                _ops.Add(new PlanOp { Kind = PlanOpKind.AndNotBitmaps, BitmapLocal = destSlot, ParamIndex2 = positiveSlot });
                return;
            }
            default:
                _ops.Add(new PlanOp
                {
                    Kind = ToPlanOpKind(MergeKind.AndNotInto, QueryPlanBuilder.GetDispatch(exec)),
                    ParamIndex = _matchIndex++,
                    BitmapLocal = destSlot,
                    DebugLabel = Label(exec)
                });
                break;
        }
    }

    private void EmitCommonInOps(int inTermCount, int bitmapLocal, PlanOpKind firstKind, PlanOpKind secondKind, bool suppressEarlyExit, string label)
    {
        int totalSlots = inTermCount + 1;
        _ops.Add(new PlanOp
        {
            Kind = firstKind,
            ParamIndex = _matchIndex,
            BitmapLocal = bitmapLocal,
            DebugLabel = label
        });

        _ops.Add(new PlanOp
        {
            Kind = secondKind,
            ParamIndex = _matchIndex + 1,
            ParamIndex2 = _nextRangeIdx++,
            BitmapLocal = bitmapLocal,
            SkipEarlyExit = suppressEarlyExit, // Defaults to false for EmitInOps
            DebugLabel = label
        });

        _matchIndex += totalSlots;
    }

    /// <summary>Human label for the clause an op reads, surfaced as a comment in the generated C#
    /// mirror (e.g. "Name [Equals]"). Build-time only; never read at execution time.</summary>
    private static string Label(ClauseExecution exec)
    {
        string field = exec.Clause?.FieldName;
        return field is null ? exec.ClauseType.ToString() : $"{field} [{exec.ClauseType}]";
    }

    private static PlanOp[] BuildAllEntriesPlan()
    {
        // No bitmap needed — AllEntries already implements IQueryMatch.Fill(),
        // so we iterate it directly without materializing into a bitmap first.
        return [new PlanOp { Kind = PlanOpKind.FillFromMatch, ParamIndex = 0 }];
    }
}
