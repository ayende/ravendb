using System;
using System.Collections.Concurrent;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using System.Threading;

namespace Corax.Querying.Planning;

/// <summary>
/// Caches compiled query plans per index instance.
/// Lives on IndexSearcher — GC'd when the index is replaced.
///
/// Two-level structure: outer keyed by query text (ConcurrentDictionary), inner is a
/// fixed 32-slot SoA (struct-of-arrays) per query text — parallel int[] for orderings
/// and type signatures plus a CompiledPlan[] for the payloads. SIMD compares scan all
/// 32 slots in 4 Vector256 iterations (or 8 Vector128 iterations on smaller hardware).
///
/// The vast majority of queries have ≤ 8 distinct (ordering, typesig) combinations,
/// which all fit in the first SIMD lane group — so the typical lookup is one vector
/// load + one Equals + one ExtractMostSignificantBits + one TrailingZeroCount.
/// </summary>
public class PlanCache
{
    private const int MaxPlansPerQuery = 32;
    private const int MaxDistinctQueries = 2048;

    private readonly ConcurrentDictionary<string, PerQueryPlans> _cache = new();

    public CompiledPlan Get(string queryText, int ordering, int typeSignature = 0)
    {
        if (!_cache.TryGetValue(queryText, out var per))
            return null;
        return per.TryLookup(ordering, typeSignature);
    }

    public void Add(string queryText, CompiledPlan plan)
    {
        // Cap total distinct queries to prevent unbounded growth.
        // Stop caching new queries past the cap rather than evicting — eviction would
        // require a global LRU with locking; the bound is generous enough that hitting
        // it implies query churn we'd rather not silently absorb.
        if (_cache.Count > MaxDistinctQueries)
            return;

        var per = _cache.GetOrAdd(queryText, _ => new PerQueryPlans());
        per.Publish(plan);
    }

    /// <summary>
    /// 32-slot per-query plan cache. Lookup is SIMD scan over parallel int arrays;
    /// matched candidates are revalidated against the plan's own embedded keys to
    /// guard against torn-write races (key written before plan ref or vice versa).
    /// Insertion fills sequentially while there's room, then random-evicts.
    /// </summary>
    private sealed class PerQueryPlans
    {
        private readonly int[] _orderings = new int[MaxPlansPerQuery];
        private readonly int[] _typesigs = new int[MaxPlansPerQuery];
        private readonly CompiledPlan[] _plans = new CompiledPlan[MaxPlansPerQuery];
        private int _filled;

        public CompiledPlan TryLookup(int ordering, int typesig)
        {
            if (Vector256.IsHardwareAccelerated)
                return Vec256Lookup(ordering, typesig);
            if (Vector128.IsHardwareAccelerated)
                return Vec128Lookup(ordering, typesig);
            return ScalarLookup(ordering, typesig);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private CompiledPlan Vec256Lookup(int ordering, int typesig)
        {
            var ordVec = Vector256.Create(ordering);
            var typVec = Vector256.Create(typesig);
            for (int i = 0; i < MaxPlansPerQuery; i += 8)
            {
                var ords = Vector256.LoadUnsafe(ref _orderings[i]);
                var typs = Vector256.LoadUnsafe(ref _typesigs[i]);
                var match = Vector256.Equals(ords, ordVec) & Vector256.Equals(typs, typVec);
                uint mask = match.ExtractMostSignificantBits();
                while (mask != 0)
                {
                    int lane = BitOperations.TrailingZeroCount(mask);
                    mask &= mask - 1;
                    var plan = Volatile.Read(ref _plans[i + lane]);
                    // Embedded-key revalidation: a torn write may have left the slot's int
                    // keys pointing at a plan with different (Ordering, TypeSignature).
                    // Empty slots are also matched here when the lookup keys happen to be
                    // (0, 0); the null guard handles them.
                    if (plan != null && plan.Ordering == ordering && plan.TypeSignature == typesig)
                        return plan;
                }
            }
            return null;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private CompiledPlan Vec128Lookup(int ordering, int typesig)
        {
            var ordVec = Vector128.Create(ordering);
            var typVec = Vector128.Create(typesig);
            for (int i = 0; i < MaxPlansPerQuery; i += 4)
            {
                var ords = Vector128.LoadUnsafe(ref _orderings[i]);
                var typs = Vector128.LoadUnsafe(ref _typesigs[i]);
                var match = Vector128.Equals(ords, ordVec) & Vector128.Equals(typs, typVec);
                uint mask = match.ExtractMostSignificantBits();
                while (mask != 0)
                {
                    int lane = BitOperations.TrailingZeroCount(mask);
                    mask &= mask - 1;
                    var plan = Volatile.Read(ref _plans[i + lane]);
                    if (plan != null && plan.Ordering == ordering && plan.TypeSignature == typesig)
                        return plan;
                }
            }
            return null;
        }

        private CompiledPlan ScalarLookup(int ordering, int typesig)
        {
            for (int i = 0; i < MaxPlansPerQuery; i++)
            {
                if (_orderings[i] == ordering && _typesigs[i] == typesig)
                {
                    var plan = Volatile.Read(ref _plans[i]);
                    if (plan != null && plan.Ordering == ordering && plan.TypeSignature == typesig)
                        return plan;
                }
            }
            return null;
        }

        public void Publish(CompiledPlan plan)
        {
            // No dedup check: the caller already does TryLookup before compiling, so a
            // duplicate Publish only happens under a benign race between two threads that
            // both saw the lookup miss. Both versions are equivalent; the extra one gets
            // GC'd when the slot is overwritten or the master plan retires. Skipping the
            // check keeps Publish's hot path branch-free.

            int slot;
            while (true)
            {
                int filled = Volatile.Read(ref _filled);
                if (filled >= MaxPlansPerQuery)
                {
                    // Cache full — random eviction. No LRU bookkeeping; mis-evictions just
                    // trigger a recompile next time, which is correctness-preserving.
                    slot = Random.Shared.Next(0, MaxPlansPerQuery);
                    break;
                }
                // Try to claim the next sequential slot. CAS failure means another thread
                // won the race; retry with the new filled value.
                if (Interlocked.CompareExchange(ref _filled, filled + 1, filled) == filled)
                {
                    slot = filled;
                    break;
                }
            }

            // Write plan ref first (with Volatile semantics), then keys. Lookup uses
            // embedded-key revalidation so any interleaving is safe.
            Volatile.Write(ref _plans[slot], plan);
            Volatile.Write(ref _orderings[slot], plan.Ordering);
            Volatile.Write(ref _typesigs[slot], plan.TypeSignature);
        }
    }
}
