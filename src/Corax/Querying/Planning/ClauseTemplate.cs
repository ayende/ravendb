namespace Corax.Querying.Planning;

/// <summary>Immutable structural template built on the first execution of a query text.
/// Cached on PerQueryPlans.Template. On cache hit, clauses are cloned and their
/// per-execution fields overwritten by PopulateParameters.</summary>
public sealed class ClauseTemplate
{
    public ClauseInfo[] Clauses;
    public bool IsAllEntries;
    public bool IsOr;              // root boolean operator

    /// <summary>Spatial clauses separated from the main filter chain (AND queries only).</summary>
    public ClauseInfo[] SpatialClauses;
    /// <summary>Vector clauses separated from the main filter chain (AND queries only).</summary>
    public ClauseInfo[] VectorClauses;

    /// <summary>WHEN condition expressions, indexed by clause position. Null entry = no WHEN.
    /// Null array = no WHEN clauses in this query. Evaluated during PopulateClauseValues
    /// to determine which clauses are active for this execution.
    /// Stored as object[] to avoid Corax depending on Raven.Server AST types
    /// (actual type: BinaryExpression from Raven.Server.Documents.Queries.AST).</summary>
    public object[] WhenConditions;
}
