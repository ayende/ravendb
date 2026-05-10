using System.Collections.Generic;
using Voron.Data.PostingLists;

namespace Corax.Querying.Planning;

public enum PlanOpKind : byte
{
    /// <summary>Fill bitmap[0] from a term source, term provider, or IQueryMatch.
    /// Dispatches to QueryPrimitives.FillBitmapFromPostingSource / FillBitmapFromTermsProvider / FillFromMatch
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
}

/// <summary>Selects the execution-time source for term ops in a <see cref="PlanOp"/>.</summary>
public enum MatchDispatch : byte
{
    /// <summary>IQueryMatch.Fill() dispatch — the general-purpose path for spatial,
    /// vector, search, boosted, and any clause that can't be expressed as a posting
    /// list or tree scan.</summary>
    QueryMatch,

    /// <summary>Native posting-list dispatch — a single resolved posting list
    /// (Single / SmallPostingList / PostingList.Iterator). The fastest path.
    /// Used for Equals and NotEquals clauses.</summary>
    PostingList,

    /// <summary>CompactTree scan — iterates the tree at execution time, decoding each
    /// matching posting list directly into the bitmap. Used for StartsWith, EndsWith,
    /// Contains, Exists, Regex, and range clauses.</summary>
    TreeScan,
}

public struct PlanOp
{
    public PlanOpKind Kind;
    public int FieldId;
    public int ParamIndex;
    public int ParamIndex2;
    public int BitmapLocal;
    public long EstimatedCardinality;

    /// <summary>Controls how <see cref="ParamIndex"/> is resolved at execution time
    /// for term ops (Fill/And/Or/AndNot WithPostings):
    /// <list type="bullet">
    /// <item><see cref="MatchDispatch.QueryMatch"/> — <c>ctx.ResolvedMatches[ParamIndex]</c>
    ///   (IQueryMatch interface dispatch; vector, spatial, search, boosted clauses).</item>
    /// <item><see cref="MatchDispatch.PostingList"/> — <c>ctx.PostingSources[ParamIndex]</c>
    ///   (native posting-list dispatch; Equals / NotEquals / In / AllIn).</item>
    /// <item><see cref="MatchDispatch.TreeScan"/> — <c>ctx.TermsProviders[ParamIndex]</c>
    ///   (multi-term bitmap fill; StartsWith / EndsWith / Contains / Exists / Regex / ranges).</item>
    /// </list></summary>
    public MatchDispatch Dispatch;

    /// <summary>When true, suppress the empty-check early exit after
    /// <see cref="PlanOpKind.AndWithPostings"/>. Used for AND sub-chains inside
    /// an OR accumulator where an empty intermediate result is not a reason to
    /// abort the whole expression.</summary>
    public bool SkipEarlyExit;
}

/// <summary>Three-way native posting-list source attached to term op.
/// Mirrors the encoding used by <see cref="ITermsProvider.FillPostingListIds"/>:
/// the low 2 bits of a CompactTree value distinguish Single / SmallPostingList /
/// PostingList. Resolved up-front by <c>ResolvePostingSources</c>; consumed by
/// <c>FillBitmapFromPostingSource</c> / <c>AndWithPostingSource</c> /
/// <c>AndNotWithPostingSource</c> at execution time.</summary>
public struct PostingSource
{
    public PostingSourceKind Kind;

    /// <summary>Decoded entry id (Kind == Single) — already passed through
    /// EntryIdEncodings.GetContainerId.</summary>
    public long SingleEntryId;

    /// <summary>Container id for the small posting list (Kind == SmallPostingList) —
    /// pass to <c>Container.Get</c> on the LowLevelTransaction, then decode the
    /// FastPFor buffer.</summary>
    public long SmallPostingListId;

    /// <summary>Iterator over a large posting list (Kind == PostingList).</summary>
    public PostingList.Iterator LargeIterator;
}

public enum PostingSourceKind : byte
{
    /// <summary>The term does not exist in the index (or the field has no compact tree).
    /// Dispatcher primitives no-op on Empty for Or-shaped ops, and clear the bitmap
    /// for And-shaped ops.</summary>
    Empty,
    Single,
    SmallPostingList,
    PostingList,
}

/// <summary>One entry-scan predicate. Numeric predicates emit an IL-inlined compare
/// against ctx.LongParams[ParamIndex] / DoubleParams[...]; slice predicates emit
/// an inline byte-sequence comparison. Between uses both ParamIndex slots.
/// OrBranches is non-null only for OR-group predicates.</summary>
public struct ScanPredicateInfo
{
    public string FieldName;
    public ScanValueType ValueType;
    public ScanCompareOp CompareOp;
    public int ParamIndex;
    public int ParamIndex2;
    public ScanPredicateInfo[] OrBranches;
}

public enum ScanValueType : byte
{
    Long,    // reader.CurrentLong vs ctx.LongParams[i]
    Double,  // reader.CurrentDouble vs ctx.DoubleParams[i]
    Slice,   // IL-emitted byte-sequence comparison
}

public enum ScanCompareOp : byte
{
    Equal,
    NotEqual,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    Between,
}

/// <summary>Spatial predicate applied after the bitmap filter phase. ANDs the spatial
/// match result with the candidate bitmap to remove non-matching entries.
/// MatchIndex is the slot in the resolved IQueryMatch[] for this post-filter.</summary>
public struct SpatialFilterOp
{
    public int MatchIndex;
    public ClauseInfo Clause;
    public ClauseExecution Exec;
}

/// <summary>Vector nearest-neighbor search applied after the bitmap filter phase.
/// The bitmap-producing match is passed as the filter source to VectorSearchMatch,
/// restricting the search to the candidate set. MatchIndex is the slot in the
/// resolved IQueryMatch[] for this vector search.</summary>
public struct VectorSearchOp
{
    public int MatchIndex;
    public ClauseInfo Clause;
    public ClauseExecution Exec;
}

public class QueryPlan
{
    public PlanOp[] Ops;
    public int OperandOrdering;

    /// <summary>Clause list from the query plan builder — structural template data.</summary>
    public List<ClauseInfo> Clauses;

    /// <summary>Per-execution state parallel to Clauses — parameter values, cardinality, etc.</summary>
    public ClauseExecution[] Executions;
    public bool IsAllEntries;
    public bool AllNegated;
    public bool IsOr;

    /// <summary>Typed parameter values for clause resolution. Populated during plan building
    /// from resolved query parameters and literal values. Each clause's PackedParam field
    /// encodes (type, index) pairs pointing into these arrays, so resolution never has to
    /// re-parse strings back to their native types.</summary>
    public long[] LongValues;
    public double[] DoubleValues;
    public string[] StringValues;

    /// <summary>Spatial operations to apply after the bitmap filter phase builds the candidate bitmap.
    /// Each spatial match is ANDed with the candidate set.</summary>
    public SpatialFilterOp[] SpatialFilters;

    /// <summary>Vector operations to apply after spatial filtering.
    /// The bitmap-producing CompiledQueryMatch is passed as the filterQuery to VectorSearchMatch.</summary>
    public VectorSearchOp[] VectorSelects;

    /// <summary>Packed parameter type signature from ScanPredicateInfos.
    /// 2 bits per predicate (0=long, 1=double, 2=string) for the FIRST 16 predicates.
    /// For ≤ 16 predicates this is the exact identity. For more, it acts as a lossy
    /// hash and <see cref="FullKinds"/> carries the disambiguator.</summary>
    public int TypeSignature;

    /// <summary>Full per-predicate kind vector. Populated only when there are more than
    /// 16 typed scan predicates. Null in the common case so PlanCache lookups stay
    /// branch-free on the hot path. When non-null, PlanCache.Add walks the slot chain
    /// (CompiledPlan.Next) and SequenceEqual-compares this vs. existing FullKinds to
    /// disambiguate plans whose <see cref="TypeSignature"/> ints collide.</summary>
    public byte[] FullKinds;

    /// <summary>Number of bitmaps this plan needs at execution time.
    /// Slot 0 = main result, slot 1 = scratch for AND-with-postings / AND-NOT and OR-group
    /// accumulation. Plans with multiple AndGroups inside an OR chain use slot 2 as a
    /// save slot during the swap-build-or pattern, so RequiredBitmaps is set to 3 for those.
    /// Default is 2 (covers all non-multi-AndGroup plans).</summary>
    public int RequiredBitmaps = 2;

    /// <summary>Metadata for entry scan predicates. Used by the IL emitter to generate
    /// direct comparison calls. Parallel to the MultiUnaryItem[] created at execution time.
    /// null if no entry scan is possible.</summary>
    public ScanPredicateInfo[] ScanPredicateInfos;
}
