namespace Corax.Querying.Planning;

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
