namespace Corax.Querying.Planning;

public class QueryExecution
{
    /// <summary>Emitted plan operations. Populated only on cache miss by EmitPlan;
    /// consumed by <see cref="QueryIlEmitter.EmitDelegate"/> and
    /// <see cref="QueryPlanBuilder.BuildInspectionTemplate"/>. Transient — not
    /// needed after compilation.</summary>
    public PlanOp[] Ops;

    /// <summary>Bit 30 of <see cref="CompiledPlan.Ordering"/>. Set when any clause carries a boost factor.</summary>
    public const int HasBoostBit = 1 << 30;

    /// <summary>Bit 31 of <see cref="CompiledPlan.Ordering"/>. Set when the sort-driving clause's
    /// cardinality is &lt;= <see cref="Corax.Querying.Matches.SortedDrivingWithTieBreakMatch.MaxGroupSize"/>
    /// (16K). Queries under the cliff can use tie-break sorted scan; queries over it cannot.
    /// Different cardinality buckets get different compiled plans (and different optimization
    /// hints), so a plan cached from a small-cardinality execution isn't reused for a
    /// large-cardinality one that needs a different dispatch path.</summary>
    public const int CardinalityCliffBit = 1 << 31;

    /// <summary>Back-reference to the compiled plan this execution belongs to.
    /// Structural fields (AllNegated, OptimizationFlags, SortDrivingClauseIndex,
    /// compound indices, etc.) live on the plan — not duplicated here.</summary>
    public CompiledPlan Plan;

    /// <summary>Per-execution state — parameter values, cardinality, etc.
    /// Each element carries a back-reference to its <see cref="ClauseInfo"/> via
    /// <see cref="ClauseExecution.Clause"/>, so clause metadata is accessible as
    /// <c>Executions[i].Clause</c> without a separate parallel list.</summary>
    public ClauseExecution[] Executions;
    public bool IsAllEntries;

    /// <summary>Typed parameter values for clause resolution. Populated during plan building
    /// from resolved query parameters and literal values. Each clause's PackedParam field
    /// encodes (type, index) pairs pointing into these arrays, so resolution never has to
    /// reparse strings back to their native types.</summary>
    public long[] LongValues;
    public double[] DoubleValues;
    public string[] StringValues;

    /// <summary>Spatial operations to apply after the bitmap filter phase builds the candidate bitmap.
    /// Each spatial match is ANDed with the candidate set.</summary>
    public SpatialFilterOp[] SpatialFilters;

    /// <summary>Vector operations to apply after spatial filtering.
    /// The bitmap-producing CompiledQueryMatch is passed as the filterQuery to VectorSearchMatch.</summary>
    public VectorSearchOp[] VectorSelects;

    /// <summary>Per-execution term counts for OrRange/AndRange ops. Each range op's
    /// ParamIndex2 is an index into this array. The IL reads the count at runtime,
    /// so the same compiled delegate handles different IN parameter array sizes.</summary>
    public int[] InRangeCounts;

    /// <summary>Transient: number of bitmaps this plan needs (2 or 3). Set by EmitPlan
    /// on cache miss, read once to populate <see cref="CompiledPlan.RequiredBitmaps"/>.
    /// Not used after compilation.</summary>
    public int RequiredBitmaps = 2;
}
