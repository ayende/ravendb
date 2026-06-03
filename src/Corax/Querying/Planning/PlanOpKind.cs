namespace Corax.Querying.Planning;

/// <summary>The compiled pipeline op. Leaf-merge ops carry the operand source in the
/// kind itself (<c>…From{PostingSource,TreeScan,Match}</c>) — there is no separate
/// dispatch field. PostingSource / TreeScan operands are resolved lazily from
/// <see cref="CompiledQueryMatch.Leaves"/>; Match operands read
/// <see cref="CompiledQueryMatch.ResolvedMatches"/>.</summary>
public enum PlanOpKind : byte
{
    // ── Fill bitmap[0] from one leaf ────────────────────────────────
    /// <summary>Seed bitmap[0] from a native posting-list leaf.
    /// <code>QueryPrimitives.CtxFillFromPostingSource(ctx, cursor); cursor++;</code></summary>
    FillFromPostingSource,

    /// <summary>Seed bitmap[0] from a CompactTree-scan leaf.
    /// <code>QueryPrimitives.CtxFillFromTreeScan(ctx, cursor); cursor++;</code></summary>
    FillFromTreeScan,

    /// <summary>Seed bitmap[0] from an IQueryMatch leaf (spatial / vector / search / boosted,
    /// and the match-all plan).
    /// <code>QueryPrimitives.CtxOrWithMatch(ctx, cursor); cursor++;</code></summary>
    FillFromMatch,

    /// <summary>Seed <c>bitmap[BitmapLocal]</c> with every entry via <c>Searcher.AllEntries()</c> — used for
    /// all-negated AND chains, match-all, and complement builds (universe seeded into a scratch slot, then the
    /// positive form is subtracted). No term-slot lookup (sidesteps IN's structural-vs-runtime slot-index mismatch).
    /// <code>QueryPrimitives.CtxFillAllEntries(ctx, BitmapLocal);</code></summary>
    FillAllEntries,

    // ── Intersect bitmap[0] with one leaf ───────────────────────────
    /// <summary>Intersect bitmap[0] with a posting-list leaf (scratch = bitmap[1]); stop the plan
    /// if the result is empty, unless <see cref="PlanOp.SkipEarlyExit"/> is set.
    /// <code>QueryPrimitives.CtxAndFromPostingSource(ctx, cursor); cursor++; if (ctx.Bitmaps[0].IsEmpty) goto done;</code></summary>
    AndFromPostingSource,

    /// <summary>Intersect bitmap[0] with a tree-scan leaf. <see cref="AndFromPostingSource"/> semantics.</summary>
    AndFromTreeScan,

    /// <summary>Intersect bitmap[0] with an IQueryMatch leaf. <see cref="AndFromPostingSource"/> semantics.</summary>
    AndFromMatch,

    // ── Union one leaf into bitmap[BitmapLocal] ─────────────────────
    /// <summary>Union a posting-list leaf into bitmap[BitmapLocal]. When the target is slot 0,
    /// stop once the page limit is reached.
    /// <code>QueryPrimitives.CtxOrFillFromPostingSource(ctx, cursor, 0); cursor++; if ((long)ctx.Bitmaps[0].Count >= ctx.Limit) goto done;</code></summary>
    OrFromPostingSource,

    /// <summary>Union a tree-scan leaf into bitmap[BitmapLocal]. <see cref="OrFromPostingSource"/> semantics.</summary>
    OrFromTreeScan,

    /// <summary>Union an IQueryMatch leaf into bitmap[BitmapLocal]. <see cref="OrFromPostingSource"/> semantics.</summary>
    OrFromMatch,

    // ── Subtract one leaf from bitmap[0] ────────────────────────────
    /// <summary>Subtract a posting-list leaf from bitmap[0] (scratch = bitmap[1]).
    /// <code>QueryPrimitives.CtxAndNotFromPostingSource(ctx, cursor); cursor++;</code></summary>
    AndNotFromPostingSource,

    /// <summary>Subtract a tree-scan leaf from bitmap[0]. <see cref="AndNotFromPostingSource"/> semantics.</summary>
    AndNotFromTreeScan,

    /// <summary>Subtract an IQueryMatch leaf from bitmap[0]. <see cref="AndNotFromPostingSource"/> semantics.</summary>
    AndNotFromMatch,

    // ── Range loops over IN-expanded term slots ─────────────────────
    /// <summary>Union a contiguous run of posting-list leaves (an expanded IN) into bitmap[BitmapLocal].
    /// ParamIndex2 = index into ctx.InRangeCounts for the runtime count.</summary>
    OrRangeFromPostingSource,

    /// <summary>Union a contiguous run of IQueryMatch leaves (a boosted IN) into bitmap[BitmapLocal].</summary>
    OrRangeFromMatch,

    /// <summary>Intersect a contiguous run of posting-list leaves (an AllIn) with bitmap[0],
    /// stopping early on an empty result unless <see cref="PlanOp.SkipEarlyExit"/> is set.</summary>
    AndRangeFromPostingSource,

    /// <summary>Intersect a contiguous run of IQueryMatch leaves (a boosted AllIn) with bitmap[0].</summary>
    AndRangeFromMatch,

    // ── Source-agnostic ops ─────────────────────────────────────────
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
    /// No longer emitted by the planner — the build tree now dest-addresses scratch slots instead of
    /// parking slot 0 around a nested build — so the inspection renderer's AND-Group detection (which
    /// keys off this op) no longer triggers. Op handler and enum value are kept for now.
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
