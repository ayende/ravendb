using System;
using System.Collections.Concurrent;
using System.Diagnostics;
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
/// Per-query: fixed 32-slot SoA (struct-of-arrays) — parallel int[] for orderings
/// and type signatures plus a CompiledPlan[] for the payloads. SIMD compares scan all
/// 32 slots in 4 Vector256 iterations (or 8 Vector128 iterations on smaller hardware).
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

    public CompiledPlan Get(string queryText, int ordering, int typeSignature = 0)
        => Get(queryText, ordering, typeSignature, default);

    public CompiledPlan Get(string queryText, int ordering, int typeSignature, ReadOnlySpan<byte> kinds)
    {
        var gen = _generation;
        if (gen.Current.TryGetValue(queryText, out var per) is false)
            gen.Previous.TryGetValue(queryText, out per);

        return per?.TryLookup(ordering, typeSignature, kinds);
    }

    /// <summary>Try to retrieve the cached clause template for a query text.
    /// Stale reads are harmless — the worst case is one redundant ParseTemplate call.
    /// Write side uses ConcurrentDictionary.GetOrAdd ensuring correctness.</summary>
    public ClauseTemplate TryGetTemplate(string queryText)
    {
        var gen = _generation;
        if (gen.Current.TryGetValue(queryText, out var per) is false)
            gen.Previous.TryGetValue(queryText, out per);

        return per?.Template;
    }

    public void Add(string queryText, CompiledPlan plan, ClauseTemplate template = null)
    {
        var gen = _generation;

        // When the current generation exceeds half the max, rotate.
        if (gen.Current.Count > MaxDistinctQueries / 2)
        {
            var newGen = new CacheGeneration([], gen.Current);
            gen = Interlocked.CompareExchange(ref _generation, newGen, gen);
            Debug.Assert(gen is not null);
        }

        var current = gen.Current;

        var per = current.GetOrAdd(queryText,
            static (_, arg) => new PerQueryPlans(arg.MaxPlansPerQuery, arg.template),
            (MaxPlansPerQuery, template));

        per.Publish(plan);
    }

    /// <summary>
    /// Fixed-slot per-query plan cache. Three parallel arrays (_orderings, _typeSignatures,
    /// _plans) of maxSlots entries (default 32, must be a multiple of 8).
    ///
    /// Lookup: broadcast the target ordering/typeSignature into a Vector256&lt;int&gt; (8 lanes)
    /// and compare 8 slots per iteration. ExtractMostSignificantBits yields a bitmask of hits;
    /// TrailingZeroCount walks set bits. Vec128 fallback does 4 lanes per iteration.
    /// Matched candidates are revalidated against the plan's own embedded keys to
    /// guard against torn-write races (parallel arrays are written non-atomically).
    ///
    /// maxSlots alignment: must be a multiple of 8 so the Vec256 loop never reads past the
    /// array end. The constructor rounds up if needed.
    /// </summary>
    private sealed class PerQueryPlans(int maxSlots, ClauseTemplate template)
    {
        private readonly int[] _orderings = new int[maxSlots];
        private readonly int[] _typeSignatures = new int[maxSlots];
        private readonly CompiledPlan[] _plans = new CompiledPlan[maxSlots];
        private int _filled;

        /// <summary>Cached clause template. Set in constructor, immutable thereafter.</summary>
        public readonly ClauseTemplate Template = template;

        public CompiledPlan TryLookup(int ordering, int typeSignature, ReadOnlySpan<byte> kinds)
        {
            if (Vector256.IsHardwareAccelerated)
                return Vec256Lookup(ordering, typeSignature, kinds);
            if (Vector128.IsHardwareAccelerated)
                return Vec128Lookup(ordering, typeSignature, kinds);
            return ScalarLookup(ordering, typeSignature, kinds);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static CompiledPlan ResolveCandidate(CompiledPlan head, int ordering, int typeSignature, ReadOnlySpan<byte> kinds)
        {
            for (var p = head; p != null; p = p.Next)
            {
                if (p.Ordering != ordering || p.TypeSignature != typeSignature)
                    continue;
                if (kinds.IsEmpty)
                {
                    if (p.FullKinds is not null)
                        continue;

                    return p;
                }
                if (kinds.SequenceEqual(p.FullKinds))
                    return p;
            }

            return null;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private CompiledPlan Vec256Lookup(int ordering, int typeSignature, ReadOnlySpan<byte> kinds)
        {
            var ordVec = Vector256.Create(ordering);
            var typVec = Vector256.Create(typeSignature);
            for (int i = 0; i < _orderings.Length && i < _typeSignatures.Length; i += 8)
            {
                var ords = Vector256.LoadUnsafe(ref _orderings[i]);
                var typs = Vector256.LoadUnsafe(ref _typeSignatures[i]);
                var match = Vector256.Equals(ords, ordVec) & Vector256.Equals(typs, typVec);
                uint mask = match.ExtractMostSignificantBits();
                while (mask != 0)
                {
                    int lane = BitOperations.TrailingZeroCount(mask);
                    mask &= mask - 1;
                    var head = Volatile.Read(ref _plans[i + lane]);
                    var resolved = ResolveCandidate(head, ordering, typeSignature, kinds);
                    if (resolved != null)
                        return resolved;
                }
            }

            return null;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private CompiledPlan Vec128Lookup(int ordering, int typeSignature, ReadOnlySpan<byte> kinds)
        {
            var ordVec = Vector128.Create(ordering);
            var typVec = Vector128.Create(typeSignature);
            for (int i = 0; i < _orderings.Length && i < _typeSignatures.Length; i += 4)
            {
                var ords = Vector128.LoadUnsafe(ref _orderings[i]);
                var typs = Vector128.LoadUnsafe(ref _typeSignatures[i]);
                var match = Vector128.Equals(ords, ordVec) & Vector128.Equals(typs, typVec);
                uint mask = match.ExtractMostSignificantBits();
                while (mask != 0)
                {
                    int lane = BitOperations.TrailingZeroCount(mask);
                    mask &= mask - 1;
                    var head = Volatile.Read(ref _plans[i + lane]);
                    var resolved = ResolveCandidate(head, ordering, typeSignature, kinds);
                    if (resolved != null)
                        return resolved;
                }
            }

            return null;
        }

        private CompiledPlan ScalarLookup(int ordering, int typeSignature, ReadOnlySpan<byte> kinds)
        {
            for (int i = 0; i < _orderings.Length && i < _typeSignatures.Length; i++)
            {
                if (_orderings[i] != ordering || _typeSignatures[i] != typeSignature)
                    continue;
                var head = Volatile.Read(ref _plans[i]);
                var resolved = ResolveCandidate(head, ordering, typeSignature, kinds);
                if (resolved != null)
                    return resolved;
            }

            return null;
        }

        private const int MaxChainDepth = 8;

        public void Publish(CompiledPlan plan)
        {
            // Try chain prepend for >16-kind plans that share (ordering, type signature) hash
            if (plan.FullKinds != null && TryChainPrepend(plan))
                return;

            int slot;
            while (true)
            {
                int filled = Volatile.Read(ref _filled);
                if (filled >= maxSlots)
                {
                    // Cache full — random eviction.
                    slot = Random.Shared.Next(0, maxSlots);
                    break;
                }

                if (Interlocked.CompareExchange(ref _filled, filled + 1, filled) == filled)
                {
                    slot = filled;
                    break;
                }
            }

            Volatile.Write(ref _plans[slot], plan);
            Volatile.Write(ref _orderings[slot], plan.Ordering);
            Volatile.Write(ref _typeSignatures[slot], plan.TypeSignature);
        }

        private bool TryChainPrepend(CompiledPlan plan)
        {
            Debug.Assert(plan.FullKinds is not null);
            for (int i = 0; i < _orderings.Length && i < _typeSignatures.Length && i < _plans.Length; i++)
            {
                if (_orderings[i] != plan.Ordering || _typeSignatures[i] != plan.TypeSignature)
                    continue; // quick check
                while (true)
                {
                    var head = Volatile.Read(ref _plans[i]);
                    if (head == null)
                        return false; // Slot was emptied — fall back to slot insert

                    // Validate the head plan itself, not just the parallel arrays.
                    // The SIMD scan is a fast filter; the head's own fields are the source of truth.
                    // A concurrent eviction could have overwritten the parallel arrays.
                    if (head.Ordering != plan.Ordering || 
                        head.TypeSignature != plan.TypeSignature)
                        break; // Slot was reused for a different plan — skip

                    // Limit chain depth to prevent unbounded growth.
                    // When depth exceeds the limit, replace the entire chain.
                    if (head.ChainDepth >= MaxChainDepth)
                    {
                        plan.Next = null;
                        plan.ChainDepth = 0;
                        if (Interlocked.CompareExchange(ref _plans[i], plan, head) == head)
                            return true;
                        continue; // CAS failed, retry
                    }

                    plan.Next = head;
                    plan.ChainDepth = head.ChainDepth + 1;
                    if (Interlocked.CompareExchange(ref _plans[i], plan, head) == head)
                        return true;
                }
            }

            return false;
        }
    }
}
