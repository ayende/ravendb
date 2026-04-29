using System;

namespace Corax.Querying.Planning;

/// <summary>
/// A compiled query plan — DynamicMethod delegate + EXPLAIN source.
/// Cached per (query text, operand ordering) in the plan cache.
/// GC-collectible when the index instance is replaced.
/// </summary>
public sealed class CompiledPlan
{
    /// <summary>
    /// Delegate signature for compiled query functions.
    /// Returns count of entry IDs written to output.
    /// </summary>
    /// <param name="ctx">Runtime state (IndexSearcher, allocator, parameters)</param>
    /// <param name="output">Buffer to write matching entry IDs into</param>
    /// <param name="skip">Number of results to skip (for paging). Updated in-place.</param>
    public delegate int ExecuteDelegate(ref QueryContext ctx, Span<long> output, ref int skip);

    /// <summary>The compiled delegate — calls bitmap primitives directly.</summary>
    public ExecuteDelegate Execute { get; init; }

    /// <summary>C# pseudocode for EXPLAIN. Never compiled — exists for diagnostics only.</summary>
    public string ExplainSource { get; init; }

    /// <summary>Operand ordering packed as int (3 bits per position). Part of cache key.</summary>
    public int Ordering { get; init; }
}
