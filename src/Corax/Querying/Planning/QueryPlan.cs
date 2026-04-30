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
}
