namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

/// <summary>Outcome of gating a single clause during the resolution pass. <see cref="Keep"/> keeps the
/// clause in the execution; <see cref="Drop"/> turns it into a match-all sentinel because its WHEN(...)
/// guard is off for this query's parameters, so it must stop filtering.</summary>
internal enum ClauseFate
{
    Keep,
    Drop,
}
