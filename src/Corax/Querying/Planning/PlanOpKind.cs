namespace Corax.Querying.Planning;

public enum PlanOpKind : byte
{
    /// <summary>Fill bitmap[0] from a term source, term provider, or IQueryMatch.
    /// Dispatches to QueryPrimitives.FillBitmapFromPostingSource / FillBitmapFromTreeScan / FillFromMatch
    /// depending on <see cref="PlanOp.Dispatch"/>.</summary>
    FillFromPostings,

    /// <summary>AND bitmap[0] with a term source, term provider, or IQueryMatch.
    /// Uses bitmap[1] as scratch. Emits an early-exit branch when the result is empty
    /// unless <see cref="PlanOp.SkipEarlyExit"/> is set (inside OR sub-chains).</summary>
    AndWithPostings,

    /// <summary>OR a term source / provider / match into bitmap[BitmapLocal].
    /// Fills the target bitmap slot; the caller ORs slots together with <see cref="OrBitmaps"/>.</summary>
    OrWithPostings,

    /// <summary>ANDNOT bitmap[0] with a term source / provider / match.
    /// Removes entries present in the operand from the current result set,
    /// using bitmap[1] as scratch.</summary>
    AndNotWithPostings,

    /// <summary>Lazy OR: same as <see cref="OrWithPostings"/> but defers container
    /// merging to avoid repeated decompression. Requires a subsequent
    /// <see cref="RepairAfterLazy"/> before the bitmap can be iterated.</summary>
    LazyOrWithPostings,

    /// <summary>Finalize a bitmap that was built with <see cref="LazyOrWithPostings"/>.
    /// Calls RoaringBitmap.RepairAfterBulkAdd to merge deferred containers.</summary>
    RepairAfterLazy,

    /// <summary>Heuristic check: if bitmap[0].Count is small enough relative to the
    /// IQueryMatch.Count, branch to the entry-scan path instead of continuing the
    /// bitmap pipeline. Emits a conditional goto to the entry-scan label.</summary>
    CheckAndMaybeEntryScan,

    /// <summary>Unconditional branch to the done label. Used as the final op in the
    /// bitmap pipeline before the entry-scan fallback block.</summary>
    IterateInto,

    /// <summary>Same emission as <see cref="FillFromPostings"/> — fills bitmap[0] from
    /// a source. Exists as a separate kind for plan-builder bookkeeping to distinguish
    /// the first fill of a direct-iterate plan.</summary>
    DirectIterate,

    /// <summary>Clear a specific bitmap slot. BitmapLocal = slot index.</summary>
    ClearBitmap,

    /// <summary>AND two bitmap slots. BitmapLocal = target, ParamIndex2 = source.</summary>
    AndBitmaps,

    /// <summary>ANDNOT two bitmap slots. BitmapLocal = target, ParamIndex2 = source.</summary>
    AndNotBitmaps,

    /// <summary>Check if bitmap is empty. BitmapLocal = slot. If empty, goto done.</summary>
    CheckEmpty,

    /// <summary>OR two bitmap slots. BitmapLocal = target, ParamIndex2 = source.</summary>
    OrBitmaps,

    /// <summary>Swap contents of two bitmap slots. BitmapLocal = slot A, ParamIndex2 = slot B.</summary>
    SwapBitmaps,

    /// <summary>OR a contiguous range of posting sources into bitmap[BitmapLocal].
    /// ParamIndex = start index, ParamIndex2 = count. Emits a loop in IL.
    /// Used for IN clauses to avoid one PlanOp per term.</summary>
    OrRange,

    /// <summary>AND a contiguous range of posting sources with bitmap[0].
    /// ParamIndex = start index, ParamIndex2 = count. Emits a loop in IL with
    /// empty-check after each (unless SkipEarlyExit). Used for AllIn clauses.</summary>
    AndRange,
}