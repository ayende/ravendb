using System;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Planning;
using Corax.Utils;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

internal readonly record struct CompiledQuery(
    IQueryMatch QueryMatch,
    IQueryMatch ExecutedMatch,
    IQueryMatch SortingWrapper,
    CompiledPlan CompiledPlan,
    QueryExecution Execution,
    QueryBuilderParameters QueryBuilderParams,
    OrderMetadata[] OrderByFields) : IDisposable
{
    /// <summary>
    /// True when no SortingMatch wrapper will surface similarity scores into the scores buffer: a single vector
    /// post-filter streams its HNSW output in score order, so the wrapper was skipped. The read loop must then
    /// call <see cref="IQueryMatch.Score"/> per Fill batch to repopulate IndexScore for the entries it returns.
    /// </summary>
    public bool ScoresProducedDuringFill => Execution is { VectorPostFilterProvidesScoreOrder: true };

    public void Dispose()
    {
        (QueryMatch as IDisposable)?.Dispose();
        (SortingWrapper as IDisposable)?.Dispose();
    }
}
