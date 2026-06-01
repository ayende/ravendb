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
/// Two-generation structure: a single atomic <see cref="CacheGeneration"/> reference
/// holds both the current and previous ConcurrentDictionaries. Rotation swaps the
/// entire generation atomically — no intermediate state where current and previous
/// point to the same dict.
///
/// Per-query: fixed-capacity SoA (struct-of-arrays, default 32 slots) — a long[] holding
/// the low 64 bits of each plan's <see cref="PlanCacheKeyHash"/> plus a CompiledPlan[] for
/// the payloads. SIMD compares scan all slots in Vector256/Vector128 iterations on the low
/// 64 bits; a lane hit is confirmed with a full 256-bit digest compare. Capacity is
/// configurable via the constructor; must be a multiple of 8 for alignment.
/// </summary>
public class PlanCache
{
    private int MaxPlansPerQuery { get; }
    private int MaxDistinctQueries { get; }

    private sealed record CacheGeneration(
        ConcurrentDictionary<string, PerQueryPlans> Current,
        ConcurrentDictionary<string, PerQueryPlans> Previous);

    private CacheGeneration _generation;

    public PlanCache(int maxPlansPerQuery = 32, int maxDistinctQueries = 2048)
    {
        // MaxPlansPerQuery must be a multiple of 8 for SIMD Vector256 alignment
        if (maxPlansPerQuery % 8 != 0)
            maxPlansPerQuery = ((maxPlansPerQuery / 8) + 1) * 8;
        MaxPlansPerQuery = maxPlansPerQuery;
        MaxDistinctQueries = maxDistinctQueries;
        _generation = new CacheGeneration([], []);
    }

    public CompiledPlan Get(string queryText, in PlanCacheKeyHash hash)
    {
        var gen = _generation;
        if (gen.Current.TryGetValue(queryText, out var per) is false)
            gen.Previous.TryGetValue(queryText, out per);

        return per?.TryLookup(hash);
    }

    /// <summary>Try to retrieve the cached plan template for a query text.
    /// Stale reads are harmless — the worst case is one redundant ParseTemplate call.
    /// Write side uses ConcurrentDictionary.GetOrAdd ensuring correctness.</summary>
    public PlanTemplate TryGetTemplate(string queryText)
    {
        var gen = _generation;
        if (gen.Current.TryGetValue(queryText, out var per) is false)
            gen.Previous.TryGetValue(queryText, out per);

        return per?.Template;
    }

    public void Add(string queryText, CompiledPlan plan, PlanTemplate template = null)
    {
        var gen = _generation;

        // When the current generation exceeds half the max, rotate.
        if (gen.Current.Count > MaxDistinctQueries / 2)
        {
            var newGen = new CacheGeneration([], gen.Current);
            // CompareExchange returns the previous value. If it equals gen, we won
            // the race and newGen is now installed. If another thread beat us, the
            // returned value is the generation they installed — use that instead.
            var prev = Interlocked.CompareExchange(ref _generation, newGen, gen);
            gen = prev == gen ? newGen : prev;
        }

        var current = gen.Current;

        var per = current.GetOrAdd(queryText,
            static (_, arg) => new PerQueryPlans(arg.MaxPlansPerQuery, arg.template),
            (MaxPlansPerQuery, template));

        per.Publish(plan);
    }

    /// <summary>
    /// Fixed-slot per-query plan cache. Two parallel arrays (_hashLo, _plans) of maxSlots
    /// entries (default 32, must be a multiple of 8).
    ///
    /// Lookup: broadcast the target hash's low 64 bits into a Vector256&lt;long&gt; (4 lanes)
    /// and compare 4 slots per iteration. ExtractMostSignificantBits yields a bitmask of hits;
    /// TrailingZeroCount walks set bits. Vec128 fallback does 2 lanes per iteration. A lane hit
    /// is the low-64-bit pre-filter only — it is confirmed by comparing the plan's full 256-bit
    /// <see cref="PlanCacheKeyHash"/>. The digest is the complete plan identity, so distinct keys
    /// occupy distinct slots and there is no collision chain.
    ///
    /// maxSlots alignment: must be a multiple of 8 so the Vec256 loop never reads past the
    /// array end (4 longs per iteration divides 8). The constructor rounds up if needed.
    /// </summary>
    private sealed class PerQueryPlans(int maxSlots, PlanTemplate template)
    {
        private readonly long[] _hashLo = new long[maxSlots];
        private readonly CompiledPlan[] _plans = new CompiledPlan[maxSlots];

        /// <summary>
        /// Monotonically increasing slot allocator. Counts from 0 up to maxSlots and
        /// then stays there — it is intentionally never decremented. Once it reaches
        /// maxSlots, all subsequent publishes use random eviction (pick any slot).
        ///
        /// This is by design: a PerQueryPlans is expected to stabilise at maxSlots
        /// distinct plan variants for the lifetime of the IndexSearcher; past that
        /// point we accept random replacement as the steady state. Decrementing on
        /// eviction would only complicate concurrency without changing the steady
        /// behaviour — the outer PlanCache.Add still drives rotation based on
        /// distinct-query count, not per-query slot occupancy.
        /// </summary>
        private int _nextSlot;

        /// <summary>Cached plan template. Set in constructor, immutable thereafter.</summary>
        public readonly PlanTemplate Template = template;

        public CompiledPlan TryLookup(in PlanCacheKeyHash hash)
        {
            if (Vector256.IsHardwareAccelerated)
                return Vec256Lookup(hash);
            if (Vector128.IsHardwareAccelerated)
                return Vec128Lookup(hash);
            return ScalarLookup(hash);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private CompiledPlan Confirm(int slot, in PlanCacheKeyHash hash)
        {
            // SIMD matched the low 64 bits; confirm the full 256-bit digest against the
            // plan's own embedded key. Volatile read guards against torn writes — the
            // _hashLo entry could be published before _plans[slot] in a concurrent Publish.
            var plan = Volatile.Read(ref _plans[slot]);
            return plan != null && plan.CacheKeyHash.Equals(hash) ? plan : null;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private CompiledPlan Vec256Lookup(in PlanCacheKeyHash hash)
        {
            var key = Vector256.Create(hash.Lo);
            for (int i = 0; i < _hashLo.Length; i += 4)
            {
                var slots = Vector256.LoadUnsafe(ref _hashLo[i]);
                uint mask = Vector256.Equals(slots, key).ExtractMostSignificantBits();
                while (mask != 0)
                {
                    int lane = BitOperations.TrailingZeroCount(mask);
                    mask &= mask - 1;
                    var resolved = Confirm(i + lane, hash);
                    if (resolved != null)
                        return resolved;
                }
            }

            return null;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private CompiledPlan Vec128Lookup(in PlanCacheKeyHash hash)
        {
            var key = Vector128.Create(hash.Lo);
            for (int i = 0; i < _hashLo.Length; i += 2)
            {
                var slots = Vector128.LoadUnsafe(ref _hashLo[i]);
                uint mask = Vector128.Equals(slots, key).ExtractMostSignificantBits();
                while (mask != 0)
                {
                    int lane = BitOperations.TrailingZeroCount(mask);
                    mask &= mask - 1;
                    var resolved = Confirm(i + lane, hash);
                    if (resolved != null)
                        return resolved;
                }
            }

            return null;
        }

        private CompiledPlan ScalarLookup(in PlanCacheKeyHash hash)
        {
            long lo = hash.Lo;
            for (int i = 0; i < _hashLo.Length; i++)
            {
                if (_hashLo[i] != lo)
                    continue;
                var resolved = Confirm(i, hash);
                if (resolved != null)
                    return resolved;
            }

            return null;
        }

        public void Publish(CompiledPlan plan)
        {
            int slot;
            while (true)
            {
                int filled = Volatile.Read(ref _nextSlot);
                if (filled >= maxSlots)
                {
                    // Cache full — random eviction. _nextSlot stays at maxSlots
                    // permanently; see field doc for why this is intentional.
                    slot = Random.Shared.Next(0, maxSlots);
                    break;
                }

                if (Interlocked.CompareExchange(ref _nextSlot, filled + 1, filled) == filled)
                {
                    slot = filled;
                    break;
                }
            }

            // Publish the payload before the pre-filter key: a reader that observes the
            // matching _hashLo entry must be able to see the corresponding plan. The Confirm
            // step re-reads _plans[slot] volatile and re-checks the full digest, so a stale
            // key with a not-yet-written (or already-replaced) plan resolves to a miss.
            Volatile.Write(ref _plans[slot], plan);
            Volatile.Write(ref _hashLo[slot], plan.CacheKeyHash.Lo);
        }
    }
}
