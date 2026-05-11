namespace Corax.Querying.Planning;

/// <summary>One entry-scan predicate. Numeric predicates emit an IL-inlined compare
/// against ctx.LongParams[ParamIndex] / DoubleParams[...]; slice predicates emit
/// an inline byte-sequence comparison. Between uses both ParamIndex slots.
/// OrBranches field is non-null only for OR-group predicates.</summary>
public struct ScanPredicateInfo
{
    public string FieldName;
    public ScanValueType ValueType;
    public ScanCompareOp CompareOp;
    public int ParamIndex;
    public int ParamIndex2;
    public ScanPredicateInfo[] OrBranches;
}
