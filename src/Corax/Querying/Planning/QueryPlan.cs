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
/// The emitter uses this to generate direct calls to CompareNumerical/CompareLiteral
/// without a type switch.</summary>
public struct ScanPredicateInfo
{
    /// <summary>Field name for GetLookupRootPage.</summary>
    public string FieldName;
    /// <summary>Index into the ScanPredicates span in QueryScanContext.</summary>
    public int PredicateIndex;
    /// <summary>Which comparison method to call.</summary>
    public ScanCompareKind CompareKind;
    /// <summary>For OR groups: sub-predicates. null for simple AND predicates.</summary>
    public ScanPredicateInfo[] OrBranches;
}

public enum ScanCompareKind : byte
{
    Numerical,  // CompareNumerical — long or double
    Literal,    // CompareLiteral — string/slice
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
