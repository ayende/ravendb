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
    public void Dispose()
    {
        (QueryMatch as IDisposable)?.Dispose();
        (SortingWrapper as IDisposable)?.Dispose();
    }
}
