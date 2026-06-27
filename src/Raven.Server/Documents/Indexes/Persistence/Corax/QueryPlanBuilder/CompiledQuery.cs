using System;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Planning;
using Corax.Utils;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

// This is a value type that is preserved across the result-enumerator's `yield` boundaries (it lives in the
// iterator state machine), so it cannot be a `ref struct`. There is exactly one dispose owner — the
// `using var ___ = compileResult;` in CoraxIndexReadOperation's query loop. Every other use (the inspection
// graph builders, sorting setup) takes a read-only by-value copy and never disposes. Do not add a second
// dispose site: copies share the same QueryMatch reference, so disposing a copy would double-free it.
internal readonly record struct CompiledQuery(
    IQueryMatch QueryMatch,
    IQueryMatch ExecutedMatch,
    IQueryMatch SortingWrapper,
    QueryExecution Execution,
    QueryBuilderParameters QueryBuilderParams,
    OrderMetadata[] OrderByFields) : IDisposable
{
    /// <summary>
    /// Vector  post-filter streams its HNSW output in score order, we skipped adding SortingMatch (which does scoring)
    /// so we need to explicitly Score() after the Fill() call
    /// </summary>
    public bool ScoresProducedDuringFill => Execution is { VectorPostFilterProvidesScoreOrder: true };

    public void Dispose()
    {
        (QueryMatch as IDisposable)?.Dispose();
    }
}
