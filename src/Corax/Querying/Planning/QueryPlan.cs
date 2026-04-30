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
}

public struct PlanOp
{
    public PlanOpKind Kind;
    public int FieldId;
    public int ParamIndex;
    public int ParamIndex2;
    public int BitmapLocal;
    public int GotoLabelIndex;
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

    /// <summary>Metadata for entry scan predicates. Used by the IL emitter to generate
    /// direct comparison calls. Parallel to the MultiUnaryItem[] created at execution time.
    /// null if no entry scan is possible.</summary>
    public ScanPredicateInfo[] ScanPredicateInfos;
}
