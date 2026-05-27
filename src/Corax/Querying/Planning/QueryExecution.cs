using System.Collections.Generic;

namespace Corax.Querying.Planning;

public class QueryExecution
{
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
    public List<ClauseExecution> Executions;
    public bool IsAllEntries;

    /// <summary>Set during clause population/contradiction propagation when we can detect upfront that this query cannot return any results</summary>
    public bool QueryWillReturnNoResults;

    /// <summary>Cardinality of the clause that matches <see cref="PlanTemplate.SortDrivingClauseIndex"/>,
    /// captured during the per-execution cardinality estimation pass. Lets <see cref="CompiledPlan.Ordering"/>'s
    /// cliff-bit decision skip a second walk of the executions list. -1 when there is no sort-driving clause
    /// or no execution matched it.</summary>
    public long DrivingClauseCardinality = -1;

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

    /// <summary>Per-slot planner cardinality estimate consumed by the entry-scan heuristic.
    /// Plan-cached IL holds no per-query numbers, so we attach the estimate per execution and
    /// the IL indexes it by the runtime cursor. One long per match slot (same layout as the
    /// dispatch arrays — IN/AllIn occupy <c>InTermCount + 1</c> consecutive slots).</summary>
    public long[] Cardinalities;

    public bool HasSpatialOrVector => SpatialFilters is { Length: > 0 } || VectorSelects is { Length: > 0 };
}
