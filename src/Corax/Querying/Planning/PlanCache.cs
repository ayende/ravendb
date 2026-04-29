using System;
using System.Collections.Concurrent;
using System.Threading;

namespace Corax.Querying.Planning;

/// <summary>
/// Caches compiled query plans per index instance.
/// Key: (query text, operand ordering). Value: array of CompiledPlan.
/// Lives on IndexSearcher — GC'd when the index is replaced.
/// Cap: 32 plans per query text (different orderings for different cardinality profiles).
/// </summary>
public class PlanCache
{
    private const int MaxPlansPerQuery = 32;

    private readonly ConcurrentDictionary<string, CompiledPlan[]> _cache = new();

    /// <summary>
    /// Look up a compiled plan for the given query text and operand ordering.
    /// Returns null on cache miss.
    /// </summary>
    public CompiledPlan Get(string queryText, int ordering)
    {
        if (!_cache.TryGetValue(queryText, out var plans))
            return null;

        for (int i = 0; i < plans.Length; i++)
        {
            if (plans[i].Ordering == ordering)
                return plans[i];
        }

        return null;
    }

    /// <summary>
    /// Add a compiled plan to the cache. Thread-safe via ConcurrentDictionary.AddOrUpdate.
    /// </summary>
    public void Add(string queryText, CompiledPlan plan)
    {
        _cache.AddOrUpdate(
            queryText,
            _ => new[] { plan },
            (_, existing) =>
            {
                // Check if this ordering already exists
                for (int i = 0; i < existing.Length; i++)
                {
                    if (existing[i].Ordering == plan.Ordering)
                    {
                        // Replace existing plan with same ordering
                        var updated = new CompiledPlan[existing.Length];
                        Array.Copy(existing, updated, existing.Length);
                        updated[i] = plan;
                        return updated;
                    }
                }

                // New ordering — add if under cap
                if (existing.Length >= MaxPlansPerQuery)
                    return existing; // At cap, don't add (closest match used at lookup time)

                var expanded = new CompiledPlan[existing.Length + 1];
                Array.Copy(existing, expanded, existing.Length);
                expanded[existing.Length] = plan;
                return expanded;
            });
    }

    /// <summary>
    /// Find the plan with the closest operand ordering to the target.
    /// Used when the exact ordering isn't cached and the cap is reached.
    /// "Closest" = most matching prefix positions.
    /// </summary>
    public CompiledPlan GetClosest(string queryText, int targetOrdering, int operandCount)
    {
        if (!_cache.TryGetValue(queryText, out var plans) || plans.Length == 0)
            return null;

        CompiledPlan best = plans[0];
        int bestScore = MatchScore(best.Ordering, targetOrdering, operandCount);

        for (int i = 1; i < plans.Length; i++)
        {
            int score = MatchScore(plans[i].Ordering, targetOrdering, operandCount);
            if (score > bestScore)
            {
                bestScore = score;
                best = plans[i];
            }
        }

        return best;
    }

    /// <summary>
    /// Count how many operand positions match between two orderings.
    /// Each position is 3 bits in the packed int.
    /// </summary>
    private static int MatchScore(int a, int b, int operandCount)
    {
        int score = 0;
        for (int i = 0; i < operandCount; i++)
        {
            int posA = (a >> (i * 3)) & 0x7;
            int posB = (b >> (i * 3)) & 0x7;
            if (posA == posB)
                score++;
        }
        return score;
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
