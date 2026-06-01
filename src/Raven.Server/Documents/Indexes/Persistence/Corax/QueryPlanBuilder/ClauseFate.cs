namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

/// <summary>Outcome of gating a single clause during the resolution pass. <see cref="Keep"/> keeps the
/// clause in the execution; <see cref="Drop"/> removes it because it is statically match-all (WHEN(false),
/// or exists() on a field with no missing entries); <see cref="CollapseToNoResults"/> removes it AND
/// short-circuits an AND root to no-results (NOT exists() on a field with no missing entries).</summary>
internal enum ClauseFate
{
    Keep,
    Drop,
    CollapseToNoResults,
}
