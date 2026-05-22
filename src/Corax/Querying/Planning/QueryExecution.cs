namespace Corax.Querying.Planning;

public class QueryExecution
{
    public PlanOp[] Ops;

    /// <summary>
    /// Cache-key disambiguator for <see cref="PlanCache"/>. Two query executions with the same
    /// queryText but different shapes must produce different OperandOrdering values to avoid
    /// reusing compiled IL across incompatible plans.
    ///
    /// Bit layout (low → high):
    /// <list type="table">
    ///   <listheader><term>Bits</term><description>Meaning</description></listheader>
    ///   <item><term>0..29</term><description>Clause-ordering encoding. Up to 10 clauses × 3 bits each;
    ///     slot i holds <c>clauses[i].OriginalIndex &amp; 0x7</c> shifted by <c>i*3</c>. Captures the
    ///     post-cardinality-sort order so a query whose clauses get reordered into a different sequence
    ///     gets a distinct cache key.</description></item>
    ///   <item><term>30</term><description>HasBoost flag. Set when any clause has boost(); forces
    ///     every op to QueryMatch dispatch (so scores are accumulated).</description></item>
    ///   <item><term>31</term><description>Cardinality cliff bucket. Set when the sort-driving clause's
    ///     cardinality ≤ MaxGroupSize (16K) — the tie-break sorted scan is viable. Clear when
    ///     cardinality exceeds the cliff. Different buckets produce different compiled plans so
    ///     the optimization hint per plan is cardinality-stable.</description></item>
    /// </list>
    /// </summary>
    public int OperandOrdering;

    /// <summary>Bit 30 of <see cref="OperandOrdering"/>. Set when any clause carries a boost factor.</summary>
    public const int HasBoostBit = 1 << 30;

    /// <summary>Bit 31 of <see cref="OperandOrdering"/>. Set when the sort-driving clause's
    /// cardinality is ≤ <see cref="Corax.Querying.Matches.SortedDrivingWithTieBreakMatch.MaxGroupSize"/>
    /// (16K). Queries under the cliff can use tie-break sorted scan; queries over it cannot.
    /// Different cardinality buckets get different compiled plans (and different optimization
    /// hints), so a plan cached from a small-cardinality execution isn't reused for a
    /// large-cardinality one that needs a different dispatch path.</summary>
    public const int CardinalityCliffBit = 1 << 31;

    /// <summary>Per-execution state — parameter values, cardinality, etc.
    /// Each element carries a back-reference to its <see cref="ClauseInfo"/> via
    /// <see cref="ClauseExecution.Clause"/>, so clause metadata is accessible as
    /// <c>Executions[i].Clause</c> without a separate parallel list.</summary>
    public ClauseExecution[] Executions;
    public bool IsAllEntries;
    public bool AllNegated;

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

    /// <summary>Survival bitmask for template WHEN clauses, evaluated against this
    /// execution's bound parameters. Bit <c>i</c> = "the <c>i</c>-th WHEN clause in
    /// template traversal order evaluated true." Cap at 32 WHEN clauses per template.
    ///
    /// Part of the plan-cache key together with OperandOrdering and TypeSignature
    /// (on <see cref="CompiledPlan"/>): plans built from the same queryText but different
    /// WHEN-survival subsets can otherwise hash to the same slot and silently reuse
    /// the wrong IL. Zero in the common case (no WHEN clauses).</summary>
    public int WhenFlags;

    /// <summary>Structural optimization flags inherited from the PlanTemplate.
    /// Checked by CoraxIndexReadOperation to skip inapplicable Try* methods.</summary>
    public PlanOptimizationFlags OptimizationFlags;

    /// <summary>Template-position index of the clause identified at plan time as the
    /// sort-driving candidate (range/eq on ORDER BY field). -1 when none. Remapped to
    /// post-sort runtime index by Build. See <see cref="PlanTemplate.SortDrivingClauseIndex"/>.</summary>
    public int SortDrivingClauseIndex = -1;

    /// <summary>Pre-identified compound-exact-match clause pair (runtime indices, remapped
    /// from template via OriginalIndex). -1/-1 when no qualifying pair exists.</summary>
    public int CompoundExactClauseA = -1;
    public int CompoundExactClauseB = -1;
    /// <summary>True when the compound field order is (A, B); false when (B, A).</summary>
    public bool CompoundExactAFirst;

    /// <summary>Pre-identified compound-field-match (WHERE Equals + ORDER BY) driving clause
    /// index (runtime, remapped from template). -1 when none.</summary>
    public int CompoundFieldDrivingClause = -1;
    /// <summary>Sort field name for compound-field match.</summary>
    public string CompoundFieldSortName;

    /// <summary>Number of bitmaps this plan needs at execution time.
    /// Slot 0 = main result, slot 1 = scratch for AND-with-postings / AND-NOT and OR-group
    /// accumulation. Plans with multiple AndGroups inside an OR chain use slot 2 as a
    /// save slot during the swap-build-or pattern, so the RequiredBitmaps field is set to 3 for those.
    /// Default is 2 (covers all non-multi-AndGroup plans).</summary>
    public int RequiredBitmaps = 2;

    /// <summary>Metadata for entry scan predicates. Used by the IL emitter to generate
    /// direct comparison calls. Null if no entry scan is possible.</summary>
    public ScanPredicateInfo[] ScanPredicateInfos;

    /// <summary>Per-execution term counts for OrRange/AndRange ops. Each range op's
    /// ParamIndex2 is an index into this array. The IL reads the count at runtime,
    /// so the same compiled delegate handles different IN parameter array sizes.</summary>
    public int[] InRangeCounts;
}
