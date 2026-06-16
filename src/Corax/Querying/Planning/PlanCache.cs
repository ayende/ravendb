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

    private static long _generationGen;

    /// <summary>
    /// Backing for <see cref="Generation"/>. Every value it ever holds — the one stamped at construction and every
    /// one assigned by <see cref="ReconcileIndexState"/> — is drawn from the single process-wide <see cref="_generationGen"/>
    /// counter, so no two assignments anywhere in the process can alias. That is what keeps a bumped generation from
    /// colliding with a later instance's birth value (a local "++" would have: instance born at 5, bumped to 6, a
    /// reset instance also born at 6 → false memo hit). Guarded together with <see cref="_multipleTermsCount"/> by
    /// <see cref="_stateLock"/> on the write side.
    /// </summary>
    private long _generation = Interlocked.Increment(ref _generationGen);

    /// <summary>
    /// Highest count of multi-valued fields any searcher has reported to <see cref="ReconcileIndexState"/>. Monotonic:
    /// a field never reverts to single-valued, so this only grows for a given index instance. It is the trigger input
    /// for a generation bump, not the generation itself.
    /// </summary>
    private long _multipleTermsCount;

    private readonly object _stateLock = new();

    /// <summary>
    /// Validity token for plan resolutions memoized outside this cache (e.g. on a QueryMetadata instance). A memo is
    /// valid only while the value it recorded still equals this; any change forces a re-resolution. Two independent
    /// events assign a fresh value:
    /// <list type="bullet">
    /// <item>construction — detects an index-instance swap (the PlanCache is replaced with the index);</item>
    /// <item><see cref="ReconcileIndexState"/> — an index-state input that can change which plan a query resolves to
    /// has changed (today: a field flipping to multi-valued).</item>
    /// </list>
    /// Because every value comes from one process-wide counter, a bumped generation can never alias a later instance's
    /// birth value. Reading it detects both kinds of change with a single equality compare and no pinning.
    /// </summary>
    public long Generation => Volatile.Read(ref _generation);

    /// <summary>
    /// Fold the index-state inputs a searcher currently observes into the generation, called on the resolution path
    /// before the memo is validated. Today the only such input is <paramref name="multipleTermsCount"/> (the number of
    /// multi-valued fields in the searcher's transaction snapshot), which is monotonic per index instance. When it
    /// advances past what the generation last reflected, a fresh generation is assigned, invalidating every memo.
    ///
    /// The dangerous direction — a multi-valued snapshot reusing a single-valued (sort-elided) plan — is impossible:
    /// any snapshot whose count is higher than the elided plan's bumps the generation here before the caller reads it,
    /// and generation values never repeat, so the elided memo can never re-match. The reverse — an older, smaller
    /// snapshot landing on a plan built for a larger count — only ever yields a correct-but-pessimistic plan (a sort
    /// applied where it could have been elided), so it is safe to leave as over-invalidation.
    ///
    /// Cheap monotonic fast path; the lock is taken only on the rare advance. As more index state begins to affect
    /// plan choice, add it as further parameters/fields here so all such inputs flow through the one generation.
    /// </summary>
    public void ReconcileIndexState(long multipleTermsCount)
    {
        if (Volatile.Read(ref _multipleTermsCount) >= multipleTermsCount)
            return;

        lock (_stateLock)
        {
            if (_multipleTermsCount >= multipleTermsCount)
                return;

            // Bump the generation, then publish the count. A reader on the fast path that observes the new count is
            // then guaranteed (release write of the count happens-after the release write of the generation) to also
            // observe the new generation, so it can never validate a memo against a generation that predates this
            // state change while believing the count is already up to date.
            Volatile.Write(ref _generation, Interlocked.Increment(ref _generationGen));
            Volatile.Write(ref _multipleTermsCount, multipleTermsCount);
        }
    }

    private sealed record CacheGeneration(
        ConcurrentDictionary<Vector256<long>, PerQueryPlans> Current,
        ConcurrentDictionary<Vector256<long>, PerQueryPlans> Previous);

    private CacheGeneration _cacheGeneration;

    public PlanCache(int maxPlansPerQuery = 32, int halfOfMaxDistinctQueries = 2048)
    {
        maxPlansPerQuery = (maxPlansPerQuery + 15) & ~15; // 16 aligned - Vector256<ushort> loop can never read past the end of the array
        MaxPlansPerQuery = maxPlansPerQuery;
        HalfOfMaxDistinctQueries = Math.Max(16, halfOfMaxDistinctQueries / 2);
        _cacheGeneration = new CacheGeneration([], []);
    }

    /// <summary>Locate the per-query bucket for a structural plan key, or null if no plan has been compiled
    /// for it yet. Stale reads are harmless — a miss just falls through to ParseTemplate + GetOrAddBucket.</summary>
    public PerQueryPlans GetBucket(in Vector256<long> structuralKey)
    {
        var gen = _cacheGeneration;
        if (gen.Current.TryGetValue(structuralKey, out var per) is false)
            gen.Previous.TryGetValue(structuralKey, out per);

        return per;
    }

    /// <summary>Get the existing bucket for a structural key or atomically create one carrying the parsed
    /// template. The caller publishes compiled plan variants into the returned bucket. Rotation is driven here
    /// (on distinct-query count), so bucket creation is the single place the two-generation swap can trigger.</summary>
    public PerQueryPlans GetOrAddBucket(in Vector256<long> structuralKey, PlanTemplate template, string queryText)
    {
        var gen = _cacheGeneration;

        // When the current generation exceeds half the max, rotate.
        if (gen.Current.Count > HalfOfMaxDistinctQueries)
        {
            var newGen = new CacheGeneration([], gen.Current);
            // CompareExchange returns the previous value. If it equals gen, we won
            // the race and newGen is now installed. If another thread beat us, the
            // returned value is the generation they installed — use that instead.
            var prev = Interlocked.CompareExchange(ref _cacheGeneration, newGen, gen);
            gen = prev == gen ? newGen : prev!;
        }

        return gen.Current.GetOrAdd(structuralKey,
            static (_, arg) => new PerQueryPlans(arg.MaxPlansPerQuery, arg.template, arg.queryText),
            (MaxPlansPerQuery, template, queryText));
    }

    /// <summary>
    /// A single cached query text, its parse template, and every compiled plan variant
    /// currently held for it. Returned by <see cref="Snapshot"/>. Intended for diagnostics,
    /// introspection, and tooling — not on any hot path.
    /// </summary>
    public readonly record struct PlanCacheEntry(string QueryText, PlanTemplate Template, CompiledPlan[] Plans);

    /// <summary>
    /// Point-in-time snapshot of every cached query and its compiled plan variants across both
    /// generations. The current generation wins on duplicate structural keys. Reads are lock-free and
    /// may observe concurrent publishes, so the result is best-effort — adequate for diagnostics
    /// and tooling, not for correctness-sensitive logic.
    /// </summary>
    public IReadOnlyList<PlanCacheEntry> Snapshot()
    {
        var gen = _cacheGeneration;
        var result = new List<PlanCacheEntry>();
        var seen = new HashSet<Vector256<long>>();

        foreach (var (key, per) in gen.Current)
        {
            if (seen.Add(key))
                result.Add(new PlanCacheEntry(per.QueryText, per.Template, per.SnapshotPlans()));
        }

        foreach (var (key, per) in gen.Previous)
        {
            if (seen.Add(key))
                result.Add(new PlanCacheEntry(per.QueryText, per.Template, per.SnapshotPlans()));
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
    public sealed class PerQueryPlans(int maxSlots, PlanTemplate template, string queryText)
    {
        private readonly ushort[] _hashLo = new ushort[maxSlots];
        private readonly CompiledPlan[] _plans = new CompiledPlan[maxSlots];

        /// <summary>The query text this bucket was first compiled for, kept only as a human-readable label.
        /// Diagnostics only — surfaced by <see cref="Snapshot"/>; never read on any hot path. The structural
        /// key, not this string, is the dictionary identity, so two texts that collapse to one plan (e.g. the
        /// same query shape with different literal values) share a bucket and only the first-seen text is
        /// recorded.</summary>
        public readonly string QueryText = queryText;

        /// <summary>
        /// Monotonically increasing slot allocator. Counts from 0 up to maxSlots and
        /// then stays there — it is intentionally never decremented. Once it reaches
        /// maxSlots, all subsequent publishes use random eviction (pick any slot).
        ///
        /// This is by design: a PerQueryPlans is expected to stabilize at maxSlots
        /// distinct plan variants for the lifetime of the IndexSearcher; past that
        /// point we accept random replacement as the steady state. Decrementing on
        /// eviction would only complicate concurrency without changing the steady
        /// behavior — the outer PlanCache.GetOrAddBucket still drives rotation based on
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
