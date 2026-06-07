using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
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
/// Per-query: fixed-capacity SoA (struct-of-arrays, default 32 slots) — a ushort[] holding
/// a 16-bit pre-filter slice of each plan's <see cref="PlanCacheKeyHash"/> plus a CompiledPlan[]
/// for the payloads. SIMD compares scan all slots in Vector256/Vector128 iterations over 16-bit
/// lanes (16 slots per Vector256 step); a lane hit is confirmed with a full 256-bit digest
/// compare. Capacity is configurable via the constructor; must be a multiple of 16 for alignment.
/// </summary>
public class PlanCache
{
    private int MaxPlansPerQuery { get; }
    private int HalfOfMaxDistinctQueries { get; }

    private sealed record CacheGeneration(
        ConcurrentDictionary<string, PerQueryPlans> Current,
        ConcurrentDictionary<string, PerQueryPlans> Previous);

    private CacheGeneration _generation;

    public PlanCache(int maxPlansPerQuery = 32, int halfOfMaxDistinctQueries = 2048)
    {
        maxPlansPerQuery = (maxPlansPerQuery + 15) & ~15; // 16 aligned - Vector256<ushort> loop can never read past the end of the array
        MaxPlansPerQuery = maxPlansPerQuery;
        HalfOfMaxDistinctQueries = Math.Max(16, halfOfMaxDistinctQueries / 2);
        _generation = new CacheGeneration([], []);
    }

    public CompiledPlan Get(string queryText, in Vector256<long> hash)
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
        if (gen.Current.Count > HalfOfMaxDistinctQueries)
        {
            var newGen = new CacheGeneration([], gen.Current);
            // CompareExchange returns the previous value. If it equals gen, we won
            // the race and newGen is now installed. If another thread beat us, the
            // returned value is the generation they installed — use that instead.
            var prev = Interlocked.CompareExchange(ref _generation, newGen, gen);
            gen = prev == gen ? newGen : prev!;
        }

        var current = gen.Current;

        var per = current.GetOrAdd(queryText,
            static (_, arg) => new PerQueryPlans(arg.MaxPlansPerQuery, arg.template),
            (MaxPlansPerQuery, template));

        per.Publish(plan);
    }

    /// <summary>
    /// A single cached query text, its parse template, and every compiled plan variant
    /// currently held for it. Returned by <see cref="Snapshot"/>. Intended for diagnostics,
    /// introspection, and tooling — not on any hot path.
    /// </summary>
    public readonly record struct PlanCacheEntry(string QueryText, PlanTemplate Template, CompiledPlan[] Plans);

    /// <summary>
    /// Point-in-time snapshot of every cached query and its compiled plan variants across both
    /// generations. The current generation wins on duplicate query texts. Reads are lock-free and
    /// may observe concurrent publishes, so the result is best-effort — adequate for diagnostics
    /// and tooling, not for correctness-sensitive logic.
    /// </summary>
    public IReadOnlyList<PlanCacheEntry> Snapshot()
    {
        var gen = _generation;
        var result = new List<PlanCacheEntry>();
        var seen = new HashSet<string>();

        foreach (var (text, per) in gen.Current)
        {
            if (seen.Add(text))
                result.Add(new PlanCacheEntry(text, per.Template, per.SnapshotPlans()));
        }

        foreach (var (text, per) in gen.Previous)
        {
            if (seen.Add(text))
                result.Add(new PlanCacheEntry(text, per.Template, per.SnapshotPlans()));
        }

        return result;
    }

    /// <summary>
    /// Fixed-slot per-query plan cache. Two parallel arrays (_hashLo, _plans) of maxSlots
    /// entries (default 32, must be a multiple of 16).
    ///
    /// Lookup: compare 16 slots per iteration, then confirm by comparing the plan's full 256-bit
    /// <see cref="PlanCacheKeyHash"/>. The digest is the complete plan identity, so we check that too.
    /// Collision chances are 1/64K, and we have 32 slots by default. Meaning the chance is ~0.75% for
    /// a collision (acceptable, since we'll check the full digest).
    /// </summary>
    private sealed class PerQueryPlans(int maxSlots, PlanTemplate template)
    {
        private readonly ushort[] _hashLo = new ushort[maxSlots];
        private readonly CompiledPlan[] _plans = new CompiledPlan[maxSlots];

        /// <summary>
        /// Monotonically increasing slot allocator. Counts from 0 up to maxSlots and
        /// then stays there — it is intentionally never decremented. Once it reaches
        /// maxSlots, all subsequent publishes use random eviction (pick any slot).
        ///
        /// This is by design: a PerQueryPlans is expected to stabilize at maxSlots
        /// distinct plan variants for the lifetime of the IndexSearcher; past that
        /// point we accept random replacement as the steady state. Decrementing on
        /// eviction would only complicate concurrency without changing the steady
        /// behavior — the outer PlanCache.Add still drives rotation based on
        /// distinct-query count, not per-query slot occupancy.
        /// </summary>
        private int _nextSlot;

        public readonly PlanTemplate Template = template;

        public CompiledPlan TryLookup(in Vector256<long> hash)
        {
            ushort key = PreFilterKey(hash);
            if (Vector256.IsHardwareAccelerated)
                return Vec256Lookup(key, hash);
            if (Vector128.IsHardwareAccelerated)
                return Vec128Lookup(key, hash);
            return ScalarLookup(key, hash);
        }

        /// <summary>
        /// Hash bits are well-distributed, so any 16 give good coverage.
        /// Maps 0 to 1 so a populated slot's key never equals the default-zero value of an empty slot.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ushort PreFilterKey(in Vector256<long> hash)
        {
            ushort bits = (ushort)hash[0];
            return bits == 0 ? (ushort)1 : bits;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private CompiledPlan Confirm(int slot, in Vector256<long> hash)
        {
            // Already matched the 16-bit pre-filter; confirm the full 256-bit digest against the
            // plan's own embedded key. Volatile read guards against torn writes — the
            // _hashLo entry could be published before _plans[slot] in a concurrent Publish.
            var plan = Volatile.Read(ref _plans[slot]);
            return plan != null && plan.CacheKeyHash.Equals(hash) ? plan : null;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private CompiledPlan Vec256Lookup(ushort key, in Vector256<long> hash)
        {
            var keyVec = Vector256.Create(key);
            for (int i = 0; i < _hashLo.Length; i += Vector256<ushort>.Count)
            {
                var slots = Vector256.LoadUnsafe(ref _hashLo[i]);
                uint mask = Vector256.Equals(slots, keyVec).ExtractMostSignificantBits();
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
        private CompiledPlan Vec128Lookup(ushort key, in Vector256<long> hash)
        {
            var keyVec = Vector128.Create(key);
            for (int i = 0; i < _hashLo.Length; i += Vector128<ushort>.Count)
            {
                var slots = Vector128.LoadUnsafe(ref _hashLo[i]);
                uint mask = Vector128.Equals(slots, keyVec).ExtractMostSignificantBits();
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

        private CompiledPlan ScalarLookup(ushort key, in Vector256<long> hash)
        {
            for (int i = 0; i < _hashLo.Length; i++)
            {
                if (_hashLo[i] != key)
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
            Volatile.Write(ref _hashLo[slot], PreFilterKey(plan.CacheKeyHash));
        }

        /// <summary>Lock-free snapshot of all non-null plan slots. Best-effort; see <see cref="Snapshot"/>.</summary>
        public CompiledPlan[] SnapshotPlans()
        {
            var list = new List<CompiledPlan>();
            for (int i = 0; i < _plans.Length; i++)
            {
                var plan = Volatile.Read(ref _plans[i]);
                if (plan != null)
                    list.Add(plan);
            }

            return list.ToArray();
        }
    }
}
