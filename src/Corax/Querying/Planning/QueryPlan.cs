using Corax.Querying.Matches;

namespace Corax.Querying.Planning;

/// <summary>
/// The kind of operation in a query plan.
/// Each maps to a call to a Primitives method in the emitted IL.
/// </summary>
public enum PlanOpKind : byte
{
    /// <summary>Load posting list into bitmap (first operand in an AND chain).</summary>
    FillFromPostings,
    /// <summary>AND bitmap with a posting list (galloping page-scan).</summary>
    AndWithPostings,
    /// <summary>OR posting list into bitmap.</summary>
    OrWithPostings,
    /// <summary>ANDNOT bitmap with a posting list.</summary>
    AndNotWithPostings,
    /// <summary>Lazy OR for IN-clause batching (deferred cardinality).</summary>
    LazyOrWithPostings,
    /// <summary>Finalize lazy OR operations (recompute cardinalities).</summary>
    RepairAfterLazy,
    /// <summary>Fill bitmap from a CompactTree range scan.</summary>
    FillFromRange,
    /// <summary>Runtime check: should we switch to entry scan? (goto pattern)</summary>
    CheckAndMaybeEntryScan,
    /// <summary>Entry-scan: evaluate predicates per-entry from stored data.</summary>
    ScanAndFilter,
    /// <summary>Entry-scan in-place: filter bitmap by removing non-matching entries.</summary>
    ScanAndFilterInPlace,
    /// <summary>Sort + optional per-entry filter. Decides strategy internally.</summary>
    SortWithFilter,
    /// <summary>Ordered range scan (sort-skip: WHERE field = ORDER BY field).</summary>
    OrderedRangeScan,
    /// <summary>Vector similarity search (exact or HNSW based on bitmap size).</summary>
    VectorRank,
    /// <summary>Spatial filter.</summary>
    SpatialFilter,
    /// <summary>Sort by BM25 score.</summary>
    SortByScore,
    /// <summary>Sort by vector/spatial distance.</summary>
    SortByDistance,
    /// <summary>Iterate bitmap to output (final step for unsorted queries).</summary>
    IterateInto,
    /// <summary>Direct posting list iteration (single operand, no bitmap needed).</summary>
    DirectIterate,
}

/// <summary>
/// A single operation in the query plan. Flat structure — no nesting.
/// The plan is a linear sequence of PlanOps executed top-to-bottom,
/// with optional goto jumps for dynamic unary promotion.
/// </summary>
public struct PlanOp
{
    /// <summary>What operation to perform.</summary>
    public PlanOpKind Kind;

    /// <summary>Field ID (resolved at plan time from index schema). -1 if not applicable.</summary>
    public int FieldId;

    /// <summary>Index into the query parameters. -1 if not applicable.</summary>
    public int ParamIndex;

    /// <summary>Secondary parameter index (for BETWEEN: to-value; for IN: term list end).</summary>
    public int ParamIndex2;

    /// <summary>Which bitmap local variable this op reads/writes (0 = main, 1 = temp, 2 = scratch).</summary>
    public int BitmapLocal;

    /// <summary>For CheckAndMaybeEntryScan: which EntryScan label to jump to.</summary>
    public int GotoLabelIndex;

    /// <summary>Estimated cardinality at plan time (for EXPLAIN / diagnostics).</summary>
    public long EstimatedCardinality;
}

/// <summary>
/// A flat, optimized query plan. Produced by QueryPlanBuilder, consumed by QueryILEmitter.
/// </summary>
public class QueryPlan
{
    /// <summary>Flat sequence of operations.</summary>
    public PlanOp[] Ops;

    /// <summary>MultiUnaryItem arrays for each goto label in the plan.
    /// Index corresponds to GotoLabelIndex in CheckAndMaybeEntryScan ops.</summary>
    public MultiUnaryItem[][] EntryScanPredicates;

    /// <summary>Human-readable C# pseudocode for EXPLAIN.</summary>
    public string ExplainSource;

    /// <summary>Operand ordering packed as int (3 bits per operand position).
    /// Used as part of the plan cache key.</summary>
    public int OperandOrdering;

    /// <summary>Number of AND operands (for cache key sizing).</summary>
    public int OperandCount;

    /// <summary>Clause info for execution-time posting list resolution.
    /// Set by QueryPlanBuilder, consumed by the execution engine.</summary>
    public object[] Clauses;
}
