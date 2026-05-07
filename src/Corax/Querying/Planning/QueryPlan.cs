using Voron.Data.PostingLists;

namespace Corax.Querying.Planning;

public enum PlanOpKind : byte
{
    FillFromPostings,
    AndWithPostings,
    OrWithPostings,
    AndNotWithPostings,
    LazyOrWithPostings,
    RepairAfterLazy,
    CheckAndMaybeEntryScan,
    IterateInto,
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

/// <summary>Selects the execution-time source array for term ops in a <see cref="PlanOp"/>.</summary>
public enum MatchDispatch : byte
{
    /// <summary>Use <c>ctx.DirectSources[ParamIndex]</c> — IQueryMatch interface dispatch.
    /// Used for vector, spatial, search, boosted, and any clause the planner cannot
    /// express as a TermSource or TermProvider.</summary>
    DirectSource,

    /// <summary>Use <c>ctx.TermSources[ParamIndex]</c> — native posting-list dispatch.
    /// Single value / SmallPostingList / PostingList.Iterator resolved at plan-compile time.
    /// Used for Equals and NotEquals clauses.</summary>
    TermSource,

    /// <summary>Use <c>ctx.TermProviders[ParamIndex]</c> — multi-term bitmap fill.
    /// The ITermProvider iterates the CompactTree at execution time, decoding each
    /// matching posting list directly into the bitmap without an intermediate flat buffer.
    /// Used for StartsWith / EndsWith / Contains / Exists / Regex / range clauses.</summary>
    TermProvider,
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
    /// <item><see cref="MatchDispatch.DirectSource"/> — <c>ctx.DirectSources[ParamIndex]</c>
    ///   (IQueryMatch interface dispatch; vector, spatial, search, boosted clauses).</item>
    /// <item><see cref="MatchDispatch.TermSource"/> — <c>ctx.TermSources[ParamIndex]</c>
    ///   (native posting-list dispatch; Equals / NotEquals / In / AllIn).</item>
    /// <item><see cref="MatchDispatch.TermProvider"/> — <c>ctx.TermProviders[ParamIndex]</c>
    ///   (multi-term bitmap fill; StartsWith / EndsWith / Contains / Exists / Regex / ranges).</item>
    /// </list></summary>
    public MatchDispatch Dispatch;

    /// <summary>When true, suppress the empty-check early exit after
    /// <see cref="PlanOpKind.AndWithPostings"/>. Used for AND sub-chains inside
    /// an OR accumulator where an empty intermediate result is not a reason to
    /// abort the whole expression.</summary>
    public bool SkipEarlyExit;
}

/// <summary>Three-way native posting-list source attached to a term op.
/// Mirrors the encoding used by <see cref="ITermProvider.FillPostingListIds"/>:
/// the low 2 bits of a CompactTree value distinguish Single / SmallPostingList /
/// PostingList. Resolved up-front by <c>ResolveTermSources</c>; consumed by
/// <c>FillBitmapFromTermSource</c> / <c>AndWithTermSource</c> /
/// <c>AndNotWithTermSource</c> at execution time.</summary>
public unsafe struct TermSource
{
    public TermSourceKind Kind;

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

public enum TermSourceKind : byte
{
    /// <summary>The term does not exist in the index (or the field has no compact tree).
    /// Dispatcher primitives no-op on Empty for Or-shaped ops, and clear the bitmap
    /// for And-shaped ops.</summary>
    Empty,
    Single,
    SmallPostingList,
    PostingList,
}

/// <summary>One entry-scan predicate. Numeric predicates emit a direct compare
/// against ctx.LongParams[ParamIndex] / DoubleParams[...]; slice predicates fall
/// back to MultiUnaryItem.CompareLiteral. Between uses both ParamIndex slots.
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
    Slice,   // MultiUnaryItem.CompareLiteral fallback
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

/// <summary>Spatial post-filter — ANDed with the candidate bitmap after the filter phase.
/// MatchIndex points into the resolved IQueryMatch[]; Clause is the originating ClauseInfo.</summary>
public struct SpatialFilterOp
{
    public int MatchIndex;
    public object Clause;
}

/// <summary>Vector select — wraps the bitmap-producing match as its filterQuery.
/// MatchIndex points into the resolved IQueryMatch[]; Clause is the originating ClauseInfo.</summary>
public struct VectorSelectOp
{
    public int MatchIndex;
    public object Clause;
}

public class QueryPlan
{
    public PlanOp[] Ops;
    public int OperandOrdering;
    public object[] Clauses;
    public bool IsAllEntries;
    public bool AllNegated;

    /// <summary>Spatial operations to apply after the bitmap filter phase builds the candidate bitmap.
    /// Each spatial match is ANDed with the candidate set.</summary>
    public SpatialFilterOp[] SpatialFilters;

    /// <summary>Vector operations to apply after spatial filtering.
    /// The bitmap-producing CompiledQueryMatch is passed as the filterQuery to VectorSearchMatch.</summary>
    public VectorSelectOp[] VectorSelects;

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
