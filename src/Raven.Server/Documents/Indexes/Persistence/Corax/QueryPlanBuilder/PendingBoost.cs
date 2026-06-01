using Corax.Querying.Planning;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

internal readonly record struct PendingBoost(ClauseInfo[] InnerClauses, ParameterBinding Factor);
