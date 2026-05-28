namespace Corax.Querying.Planning;

/// <summary>Pre-built metadata for one query plan inspection node.
/// Created during IL emission, immutable, shared across cached executions.
/// Values are NOT baked in — <see cref="PackedValue"/> and <see cref="InTermCount"/>
/// store the parameter references so that values are formatted at inspection time
/// from the live <see cref="QueryExecution"/>'s typed arrays.</summary>
public sealed class InspectionOp
{
    public string Name;
    public string Dispatch;
    public string FieldName;
    public string ClauseType;
    public bool IsNegated;
    public long EstimatedCardinality;

    /// <summary>Index into the flat clause list (rebuilt per-execution in BuildInspectionGraph).
    /// -1 for ops that don't map to a clause (e.g. MaybeEntryScan, bitmap merge ops).</summary>
    public int FlatClauseIndex = -1;

    /// <summary>True when this op is part of an AND-group inside an OR chain.
    /// Used to nest these ops under an "AND-Group" node in the inspection tree.</summary>
    public bool InsideAndGroup;
}