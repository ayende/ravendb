using System;
using System.Collections.Generic;
using Corax.Mappings;
using Corax.Querying.Planning;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Sparrow.Json;
using Sparrow.Server;
using IndexSearcher = Corax.Querying.IndexSearcher;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

internal class PlanParameters
{
    public ByteStringContext Allocator;
    public Lazy<List<string>> DynamicFields;
    public bool HasBoost;
    public bool HasDynamics;
    public Index Index;
    public IndexFieldsMapping IndexFieldsMapping;
    public IndexSearcher IndexSearcher;
    public QueryMetadata Metadata;
    public BlittableJsonReaderObject QueryParameters;

    /// <summary>
    /// When set, the planner parses this expression instead of <c>Metadata.Query.Where</c>.
    /// Used to compile a sub-expression (e.g. the more-like-this base-document query) as a
    /// standalone filter while sharing the rest of the query context. The parent ORDER BY is
    /// ignored on this path (the override produces an unsorted filter).
    /// </summary>
    public QueryExpression WhereOverride;

    /// <summary>
    /// Human-readable label recorded on the plan bucket for diagnostics only (never the cache identity — that is
    /// the structural key from <see cref="QueryPlanBuilder.ComputeStructuralKey"/>). For a <see cref="WhereOverride"/>
    /// build this is the outer query text, which still names the query the sub-expression came from.
    /// </summary>
    public string CacheKey => Metadata.Query.QueryText;

    /// <summary>
    /// The plan bucket this query resolves to, stashed by <see cref="QueryPlanBuilder.BuildTemplate"/> so the
    /// downstream <see cref="BuildResolver"/> can probe/publish compiled plan variants without recomputing the
    /// structural key. Always set before <c>Build</c> runs.
    /// </summary>
    public PlanCache.PerQueryPlans Bucket;

    /// <summary>
    /// Per-query slot-binding vector — the value-bearing bindings collected by the canonical WHERE walk
    /// (<see cref="QueryPlanBuilder.ExtractSlotBindings"/>), indexed by <see cref="ParameterBinding.ValueOrdinal"/>.
    /// Memoized on <see cref="QueryMetadata"/> for the main path (and rebuilt fresh for an MLT
    /// <see cref="WhereOverride"/>), then stashed here by <see cref="QueryPlanBuilder.BuildTemplate"/> so the
    /// downstream resolver can supply each slot's literal value / parameter name / deferred expression without
    /// re-parsing. Always set before <c>Build</c> runs.
    /// </summary>
    public ParameterBinding[] SlotBindings;
}
