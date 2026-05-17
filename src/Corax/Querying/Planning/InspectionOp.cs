namespace Corax.Querying.Planning;

/// <summary>Pre-built metadata for one query plan inspection node.
/// Created during IL emission, immutable, shared across cached executions.
/// At inspection time, each entry becomes a QueryInspectionNode with
/// runtime telemetry attached.</summary>
public sealed class InspectionOp
{
    public string Name;
    public string Dispatch;
    public string FieldName;
    public string Term;
    public string Term2;
    public string ClauseType;
    public string Terms;
    public bool IsNegated;
    public long EstimatedCardinality;

    /// <summary>True when this op is part of an AND-group inside an OR chain.
    /// Used to nest these ops under an "AND-Group" node in the inspection tree.</summary>
    public bool InsideAndGroup;
}