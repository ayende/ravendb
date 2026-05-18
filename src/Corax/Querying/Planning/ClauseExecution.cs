namespace Corax.Querying.Planning;

/// <summary>Per-execution state for a clause. Parallel to <see cref="ClauseInfo"/>[] —
/// populated by PopulateClauseValues each execution. This is computed each time the query is executed, not cached.
/// Also used for operand reordering (Cardinality)</summary>
public sealed class ClauseExecution
{
    public PackedParam PackedParamValue = PackedParam.None;
    public ParamValueType TermValueType;
    public long Cardinality = -1;
    public int InTermCount;
    public bool HasNullTerm;
    public float BoostFactor;
    public SpatialParams Spatial;
    public VectorParams Vector;

    /// <summary>True when a WHEN condition evaluated to false for this execution.
    /// The clause is stripped from the plan (like empty IN).</summary>
    public bool WhenEliminated;

    public ClauseType? SentinelRewriteType;
    public bool SentinelRewriteNegated;

    /// <summary>Per-execution state for OrGroup subclauses. Parallel to <see cref="ClauseInfo.OrSubClauses"/>.</summary>
    public ClauseExecution[] OrSubExecutions;

    /// <summary>Per-execution state for AndGroup subclauses. Parallel to <see cref="ClauseInfo.AndSubClauses"/>.</summary>
    public ClauseExecution[] AndSubExecutions;
}
