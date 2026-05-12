namespace Corax.Querying.Planning;

/// <summary>One entry-scan predicate. Numeric predicates emit an IL-inlined compare
/// against ctx.LongParams[ParamIndex] / DoubleParams[...]; slice predicates emit
/// an inline byte-sequence comparison. Between uses both ParamIndex slots.
/// SubPredicates field is non-null for OR-group and AND-group predicates.</summary>
public struct ScanPredicateInfo
{
    public string FieldName;
    public ScanValueType ValueType;
    public ScanCompareOp CompareOp;
    public int ParamIndex;
    public int ParamIndex2;
    /// <summary>Sub-predicates for OR-group or AND-group compound predicates.
    /// For OR: pass if ANY sub-predicate passes. For AND: pass if ALL pass.</summary>
    public ScanPredicateInfo[] SubPredicates;
}
