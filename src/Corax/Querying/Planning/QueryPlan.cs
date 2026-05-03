using Corax.Querying.Matches;

namespace Corax.Querying.Planning;

public enum PlanOpKind : byte
{
    FillFromPostings,
    AndWithPostings,
    OrWithPostings,
    AndNotWithPostings,
    LazyOrWithPostings,
    RepairAfterLazy,
    FillFromRange,
    CheckAndMaybeEntryScan,
    ScanAndFilter,
    ScanAndFilterInPlace,
    SortWithFilter,
    OrderedRangeScan,
    VectorRank,
    SpatialFilter,
    SortByScore,
    SortByDistance,
    IterateInto,
    DirectIterate,
    /// <summary>Clear a specific bitmap slot. BitmapLocal = slot index.</summary>
    ClearBitmap,
    /// <summary>AND two bitmap slots. BitmapLocal = target, ParamIndex2 = source.</summary>
    AndBitmaps,
    /// <summary>Check if bitmap is empty. BitmapLocal = slot. If empty, goto done.</summary>
    CheckEmpty,
}

public struct PlanOp
{
    public PlanOpKind Kind;
    public int FieldId;
    public int ParamIndex;
    public int ParamIndex2;
    public int BitmapLocal;
    public long EstimatedCardinality;
}

/// <summary>Describes a single entry-scan predicate for IL emission.
/// For numeric predicates, the emitter generates direct comparisons
/// (reader.CurrentLong > ctx.LongParams[i]) — no delegate call, no MultiUnaryItem.
/// For string predicates, falls back to MultiUnaryItem.CompareLiteral.</summary>
public struct ScanPredicateInfo
{
    /// <summary>Field name — used by caller to resolve root page into FieldRootPages span.</summary>
    public string FieldName;
    /// <summary>What type of value to compare.</summary>
    public ScanValueType ValueType;
    /// <summary>The comparison operation.</summary>
    public ScanCompareOp CompareOp;
    /// <summary>Index into LongParams/DoubleParams/ScanPredicates span depending on ValueType.</summary>
    public int ParamIndex;
    /// <summary>For Between: second value index in the same typed span.</summary>
    public int ParamIndex2;
    /// <summary>For OR groups: sub-predicates. null for simple AND predicates.</summary>
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

/// <summary>References a spatial IQueryMatch that should be applied as a post-filter
/// after the bitmap filter phase completes. The match is ANDed with the candidate bitmap.</summary>
public struct SpatialFilterOp
{
    /// <summary>Index into the resolved IQueryMatch[] for this spatial match.</summary>
    public int MatchIndex;
    /// <summary>The clause that produced this spatial filter, for match resolution.</summary>
    public object Clause;
}

/// <summary>References a vector IQueryMatch that should wrap the bitmap filter result.
/// The compiled bitmap match is passed as the filterQuery to VectorSearchMatch.</summary>
public struct VectorSelectOp
{
    /// <summary>Index into the resolved IQueryMatch[] for this vector match.
    /// At execution time, the vector match is materialized with the bitmap match as its filter.</summary>
    public int MatchIndex;
    /// <summary>The clause that produced this vector select, for match resolution.</summary>
    public object Clause;
}

public class QueryPlan
{
    public PlanOp[] Ops;
    public MultiUnaryItem[][] EntryScanPredicates;
    public string ExplainSource;
    public int OperandOrdering;
    public int OperandCount;
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
    /// 2 bits per predicate (0=long, 1=double, 2=string). Used as part of cache key.</summary>
    public int TypeSignature;

    /// <summary>Number of bitmaps a compiled query plan currently needs.
    /// Slot 0 holds the main result, slot 1 is scratch for AND-with-postings / AND-NOT
    /// and for OR-group accumulation. Today every emitted plan stays within these two slots
    /// because OR groups are built into slot 1 sequentially (cleared between groups) and
    /// no nested AND-inside-OR shape is representable yet. When AndGroup lands, this becomes
    /// a per-plan computed value derived from the maximum BitmapLocal referenced by any op.</summary>
    public const int RequiredBitmaps = 2;

    /// <summary>Metadata for entry scan predicates. Used by the IL emitter to generate
    /// direct comparison calls. Parallel to the MultiUnaryItem[] created at execution time.
    /// null if no entry scan is possible.</summary>
    public ScanPredicateInfo[] ScanPredicateInfos;
}
