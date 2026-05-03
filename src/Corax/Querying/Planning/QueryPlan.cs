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
    public string ExplainSource;
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
