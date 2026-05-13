namespace Corax.Querying.Planning;

/// <summary>Distinguishes AND-group vs OR-group when <see cref="ScanPredicateInfo.SubPredicates"/>
/// is non-null. Ignored for leaf predicates (SubPredicates == null).</summary>
public enum GroupKind : byte
{
    And,
    Or
}

/// <summary>One entry-scan predicate. Numeric predicates emit an IL-inlined compare
/// against ctx.ResidualLongParams[ParamIndex] / ResidualDoubleParams[...];
/// slice predicates emit an inline byte-sequence comparison. Between uses both
/// ParamIndex slots.
/// SubPredicates field is non-null for OR-group and AND-group predicates;
/// <see cref="Group"/> picks which.</summary>
public struct ScanPredicateInfo
{
    public string FieldName;
    public ScanValueType ValueType;
    public ScanCompareOp CompareOp;
    public int ParamIndex;
    public int ParamIndex2;
    /// <summary>Sub-predicates for OR-group or AND-group compound predicates.
    /// For OR: pass if ANY sub-predicate passes. For AND: pass if ALL pass.
    /// <see cref="Group"/> discriminates.</summary>
    public ScanPredicateInfo[] SubPredicates;
    /// <summary>Valid only when <see cref="SubPredicates"/> is non-null.</summary>
    public GroupKind Group;
}
