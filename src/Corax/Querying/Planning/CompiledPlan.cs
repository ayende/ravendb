namespace Corax.Querying.Planning;

public sealed class CompiledPlan
{
    /// <summary>IL-emitted delegate that executes the posting-list scan plan.</summary>
    public QueryIlEmitter.CompiledExecuteDelegate CompiledDelegate { get; init; }

    /// <summary>Packed operand ordering used as part of the cache key.</summary>
    public int Ordering { get; init; }

    /// <summary>Packed parameter type signature (2 bits per param for first 16).</summary>
    public int TypeSignature { get; init; }

    /// <summary>Full per-predicate kind vector for >16 typed scan predicates.</summary>
    public byte[] FullKinds { get; init; }

    /// <summary>Chain pointer for hash-collision disambiguation in PlanCache.</summary>
    public CompiledPlan Next;

    /// <summary>EXPLAIN pseudocode. Generated in same pass as IL emission.</summary>
    public string ExplainSource { get; init; }

    /// <summary>Template inspection nodes built during IL emission.
    /// At query time, cloned and populated with per-execution telemetry
    /// (timings, result counts, scanned entries) from CompiledQueryMatch.</summary>
    public InspectionOp[] InspectionTemplate { get; init; }
}

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
