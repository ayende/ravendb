using System;
using System.Collections.Concurrent;
using System.Numerics;

namespace Corax.Querying.Planning;

/// <summary>
/// Caches compiled query plans per index instance.
/// Lives on IndexSearcher — GC'd when the index is replaced.
/// Cap: 32 plans per query text.
/// </summary>
public class PlanCache
{
    private const int MaxPlansPerQuery = 32;
    private const int MaxDistinctQueries = 1024;

    private readonly ConcurrentDictionary<string, CompiledPlan[]> _cache = new();

    public CompiledPlan Get(string queryText, int ordering, int typeSignature = 0)
    {
        if (!_cache.TryGetValue(queryText, out var plans))
            return null;

        for (int i = 0; i < plans.Length; i++)
        {
            if (plans[i].Ordering == ordering && plans[i].TypeSignature == typeSignature)
                return plans[i];
        }

        return null;
    }

    public void Add(string queryText, CompiledPlan plan)
    {
        // Cap total distinct queries to prevent unbounded growth
        if (_cache.Count > MaxDistinctQueries)
            return; // Don't evict — just stop caching new queries

        _cache.AddOrUpdate(
            queryText,
            _ => new[] { plan },
            (_, existing) =>
            {
                // Check for duplicate ordering+typeSignature before adding
                for (int i = 0; i < existing.Length; i++)
                {
                    if (existing[i].Ordering == plan.Ordering
                        && existing[i].TypeSignature == plan.TypeSignature)
                        return existing; // Already cached
                }

                if (existing.Length >= MaxPlansPerQuery)
                    return existing;

                var expanded = new CompiledPlan[existing.Length + 1];
                Array.Copy(existing, expanded, existing.Length);
                expanded[existing.Length] = plan;
                return expanded;
            });
    }

    /// <summary>
    /// Score how well two orderings match. Uses XOR + popcount:
    /// identical orderings XOR to 0 (score = 30 for 10 operands),
    /// completely different orderings have many set bits (low score).
    /// </summary>
    public static int MatchScore(int a, int b)
    {
        int diff = a ^ b;
        // 30 bits max (10 operands × 3 bits each) — higher score = better match
        return 30 - BitOperations.PopCount((uint)diff);
    }

    /// <summary>Pack operand positions into an int. 3 bits per position, up to 10 operands.</summary>
    public static int PackOrdering(ReadOnlySpan<int> operandPositions)
    {
        int result = 0;
        int count = Math.Min(operandPositions.Length, 10);
        for (int i = 0; i < count; i++)
        {
            result |= (operandPositions[i] & 0x7) << (i * 3);
        }
        return result;
    }
}
