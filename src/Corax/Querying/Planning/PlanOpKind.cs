namespace Corax.Querying.Planning;

public enum PlanOpKind : byte
{
    /// <summary>Seed bitmap[0] from one leaf operand (the operand form is chosen by
    /// <see cref="PlanOp.Dispatch"/>). Also the whole plan for a single Equals clause.
    /// <code>QueryPrimitives.CtxFillFromTreeScan(ctx, cursor); cursor++;</code></summary>
    FillFromLeaf,

    /// <summary>Seed bitmap[0] with every entry via <c>Searcher.AllEntries()</c> — used for
    /// all-negated AND chains and match-all. No slot lookup (sidesteps IN's structural-vs-runtime
    /// slot-index mismatch).
    /// <code>QueryPrimitives.CtxFillAllEntries(ctx);</code></summary>
    FillAllEntries,

    /// <summary>Intersect bitmap[0] with one leaf (scratch = bitmap[1]); stop the plan if the
    /// result is empty, unless <see cref="PlanOp.SkipEarlyExit"/> is set (inside an OR sub-chain).
    /// <code>QueryPrimitives.CtxAndFromPostingSource(ctx, cursor); cursor++; if (ctx.Bitmaps[0].IsEmpty) goto done;</code></summary>
    AndWithLeaf,

    /// <summary>Union one leaf into bitmap[BitmapLocal]. When the target is slot 0, stop once the
    /// page limit is reached.
    /// <code>QueryPrimitives.CtxOrFillFromPostingSource(ctx, cursor, 0); cursor++; if ((long)ctx.Bitmaps[0].Count >= ctx.Limit) goto done;</code></summary>
    OrWithLeaf,

    /// <summary>Subtract one leaf from bitmap[0] (scratch = bitmap[1]).
    /// <code>QueryPrimitives.CtxAndNotFromTreeScan(ctx, cursor); cursor++;</code></summary>
    AndNotWithLeaf,

    /// <summary>Union a contiguous run of leaves (an expanded IN) into bitmap[BitmapLocal].
    /// ParamIndex = first slot, ParamIndex2 = index into ctx.InRangeCounts for the runtime count.
    /// <code>for (j = cursor; j &lt; cursor + ctx.InRangeCounts[r]; j++) QueryPrimitives.CtxOrFillFromPostingSource(ctx, j, slot);</code></summary>
    OrLeafRange,

    /// <summary>Intersect a contiguous run of leaves (an AllIn) with bitmap[0], stopping early on
    /// an empty result unless <see cref="PlanOp.SkipEarlyExit"/> is set.
    /// <code>for (j = cursor; j &lt; cursor + ctx.InRangeCounts[r]; j++) { QueryPrimitives.CtxAndFromPostingSource(ctx, j); if (ctx.Bitmaps[0].IsEmpty) goto done; }</code></summary>
    AndLeafRange,

    /// <summary><code>ctx.Bitmaps[slot].Clear();</code></summary>
    ClearBitmap,

    /// <summary>Intersect two bitmap slots. BitmapLocal = target, ParamIndex2 = source.
    /// <code>ctx.Bitmaps[target].AndWith(ref ctx.Bitmaps[source]);</code></summary>
    AndBitmaps,

    /// <summary>Subtract the source slot from the target slot. BitmapLocal = target, ParamIndex2 = source.
    /// <code>ctx.Bitmaps[target].AndNotWith(ref ctx.Bitmaps[source]);</code></summary>
    AndNotBitmaps,

    /// <summary>Lazy-union two bitmap slots — defers container merging for speed, so the result
    /// bitmap is repaired once at the done label before it can be iterated. BitmapLocal = target,
    /// ParamIndex2 = source.
    /// <code>ctx.Bitmaps[target].LazyOrWith(ref ctx.Bitmaps[source]);</code></summary>
    LazyOrBitmaps,

    /// <summary>Swap the contents of two bitmap slots. BitmapLocal = slot A, ParamIndex2 = slot B.
    /// <code>ctx.Bitmaps[a].SwapContents(ref ctx.Bitmaps[b]);</code></summary>
    SwapBitmaps,

    /// <summary>Short-circuit the plan when a bitmap slot is empty.
    /// <code>if (ctx.Bitmaps[slot].IsEmpty) goto done;</code></summary>
    GotoDoneIfEmpty,

    /// <summary>Switch to the entry-scan tail when bitmap[0] is small relative to the next clause's
    /// cardinality (cheaper to scan the surviving entries than to keep intersecting).
    /// <code>if (QueryPrimitives.ShouldSwitchToEntryScan((long)ctx.Bitmaps[0].Count, ctx.Cardinalities[cursor])) goto EntryScan;</code></summary>
    MaybeEntryScan,

    /// <summary>Terminal op: jump to the done label that ends the bitmap pipeline.
    /// <code>goto done;</code></summary>
    GotoDone,
}
