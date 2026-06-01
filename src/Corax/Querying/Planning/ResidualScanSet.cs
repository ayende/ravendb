namespace Corax.Querying.Planning;

/// <summary>One dispatch path's residual entry-scan predicate set: the structural predicates, the
/// parallel post-sort exec-position indices (entry <c>i</c> maps <c>Predicates[i]</c> to its
/// <see cref="QueryExecution.Executions"/> slot in the same order the matching delegate reads them),
/// and the IL-compiled per-entry evaluator baked from those predicates. One instance per path on the
/// <see cref="CompiledPlan"/>: the entry-scan set excludes clause 0 (the bitmap seed), the CompoundField
/// set excludes {driving, field2Range}, and the DirectScan set excludes the sort-driving clause — so the
/// three sets differ whenever the driving clause is not the smallest-cardinality clause.</summary>
public sealed class ResidualScanSet
{
    /// <summary>Structural scan predicates filtered against this path's exclusion set. Null when there
    /// are no residual predicates for the path (single clause, OR, or every clause excluded).</summary>
    public ScanPredicateInfo[] Predicates { get; init; }

    /// <summary>Post-sort exec positions parallel to <see cref="Predicates"/>: entry <c>i</c> is the
    /// clause that produced <c>Predicates[i]</c>. Used by the per-query param extractor to populate
    /// <see cref="QueryExecution.FieldRootPages"/> / analyzed slices in delegate order.</summary>
    public int[] ClauseIndices { get; init; }

    /// <summary>IL-emitted per-entry predicate evaluator baked from <see cref="Predicates"/>. Must be
    /// paired with a <c>ScanParamExtractor</c> run over the same set.</summary>
    public ResidualScanIlEmitter.ResidualScanPredicate Compiled { get; set; }

    /// <summary>True when there is at least one residual predicate to filter per entry.</summary>
    public bool HasPredicates => Predicates is { Length: > 0 };
}
