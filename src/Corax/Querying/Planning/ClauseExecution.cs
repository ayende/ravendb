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

    /// <summary>For BETWEEN clauses, true when the client sent the unbounded-low sentinel ("*")
    /// as the low bound (i.e. <c>WhereBetween(field, null, high)</c>). Resolution rewrites
    /// such a clause to LessThanOrEqual(high) — null-valued docs are NOT included
    /// (matches Lucene semantics for asymmetric null-sentinel BETWEEN).</summary>
    public bool BetweenLowUnbounded;

    /// <summary>For BETWEEN clauses, true when the client sent the unbounded-high sentinel ("NULL")
    /// as the high bound (i.e. <c>WhereBetween(field, low, null)</c>). Resolution rewrites
    /// such a clause to <c>AllEntries() AndNot LessThan(low)</c>, which DOES include
    /// null-valued docs (Lucene quirk: high-null pulls in missing-field documents).</summary>
    public bool BetweenHighUnbounded;

    /// <summary>True when a WHEN condition evaluated to false for this execution.
    /// The clause is stripped from the plan (like empty IN).</summary>
    public bool WhenEliminated;

    /// <summary>Per-execution state for OrGroup subclauses. Parallel to <see cref="ClauseInfo.OrSubClauses"/>.</summary>
    public ClauseExecution[] OrSubExecutions;

    /// <summary>Per-execution state for AndGroup subclauses. Parallel to <see cref="ClauseInfo.AndSubClauses"/>.</summary>
    public ClauseExecution[] AndSubExecutions;
}
