namespace Corax.Querying.Planning;

/// <summary>
/// A compiled query plan — DynamicMethod delegate + EXPLAIN source.
/// Cached per (query text, operand ordering) in the plan cache.
/// GC-collectible when the index instance is replaced.
/// </summary>
public sealed class CompiledPlan
{
    /// <summary>The compiled DynamicMethod delegate that populates the bitmap.</summary>
    public QueryILEmitter.CompiledExecuteDelegate CompiledDelegate { get; init; }

    /// <summary>C# pseudocode for EXPLAIN. Never compiled — exists for diagnostics only.</summary>
    public string ExplainSource { get; init; }

    /// <summary>Operand ordering packed as int (3 bits per position). Part of cache key.</summary>
    public int Ordering { get; init; }
}
