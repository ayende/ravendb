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
/// Two-generation structure: two ConcurrentDictionaries hold PerQueryPlans entries.
/// New entries always go into the current generation. Lookups check current first,
/// then the previous generation. When the current generation exceeds half the max
/// capacity, it becomes the previous generation and a fresh dict takes over. The
/// old previous generation is dropped, evicting its plans.
///
/// Per-query: fixed 32-slot SoA (struct-of-arrays) — parallel int[] for orderings
/// and type signatures plus a CompiledPlan[] for the payloads. SIMD compares scan all
/// 32 slots in 4 Vector256 iterations (or 8 Vector128 iterations on smaller hardware).
/// </summary>
public class PlanCache
{
    public int MaxPlansPerQuery { get; }
    public int MaxDistinctQueries { get; }

    private ConcurrentDictionary<string, PerQueryPlans> _current;
    private ConcurrentDictionary<string, PerQueryPlans> _previous;

    public PlanCache(int maxPlansPerQuery = 32, int maxDistinctQueries = 2048)
    {
        // MaxPlansPerQuery must be a multiple of 8 for SIMD Vector256 alignment
        if (maxPlansPerQuery % 8 != 0)
            maxPlansPerQuery = ((maxPlansPerQuery / 8) + 1) * 8;
        MaxPlansPerQuery = maxPlansPerQuery;
        MaxDistinctQueries = maxDistinctQueries;
        _current = new ConcurrentDictionary<string, PerQueryPlans>();
        _previous = null;
    }

    public CompiledPlan Get(string queryText, int ordering, int typeSignature = 0)
        => Get(queryText, ordering, typeSignature, default);

    public CompiledPlan Get(string queryText, int ordering, int typeSignature, ReadOnlySpan<byte> kinds)
    {
        // Check current generation first
        var current = Volatile.Read(ref _current);
        if (current.TryGetValue(queryText, out var per))
        {
            var result = per.TryLookup(ordering, typeSignature, kinds, MaxPlansPerQuery);
            if (result != null)
                return result;
        }

        // Check previous generation
        var prev = Volatile.Read(ref _previous);
        if (prev != null && prev.TryGetValue(queryText, out per))
        {
            return per.TryLookup(ordering, typeSignature, kinds, MaxPlansPerQuery);
        }

        return null;
    }

    /// <summary>Try to retrieve the cached clause template for a query text.
    /// The template is shared across all ordering variants of the same query.</summary>
    public ClauseTemplate TryGetTemplate(string queryText)
    {
        var current = Volatile.Read(ref _current);
        if (current.TryGetValue(queryText, out var per))
        {
            var t = Volatile.Read(ref per.Template);
            if (t != null)
                return t;
        }

        var prev = Volatile.Read(ref _previous);
        if (prev != null && prev.TryGetValue(queryText, out per))
        {
            return Volatile.Read(ref per.Template);
        }

        return null;
    }

    /// <summary>Store a clause template for a query text. Called once on first execution.
    /// Subsequent calls are no-ops (first writer wins).</summary>
    public void StoreTemplate(string queryText, ClauseTemplate template)
    {
        var current = Volatile.Read(ref _current);
        var per = current.GetOrAdd(queryText, _ => new PerQueryPlans(MaxPlansPerQuery));
        // First writer wins — benign race (all templates for the same query text are identical)
        Interlocked.CompareExchange(ref per.Template, template, null);
    }

    public void Add(string queryText, CompiledPlan plan)
    {
        var current = Volatile.Read(ref _current);

        // When the current generation exceeds half the max, rotate.
        // The old previous is dropped (GC'd), current becomes previous,
        // a fresh dict takes over.
        if (current.Count > MaxDistinctQueries / 2)
        {
            // Two-generation rotation: demote current → previous, install fresh current.
            // Race between concurrent threads is benign: the CAS on _current ensures only
            // one fresh dict wins; losers get the winner's dict back from CAS and publish
            // there. An empty fresh dict can't re-trigger this block (Count == 0), so
            // cascading rotations are impossible.
            var fresh = new ConcurrentDictionary<string, PerQueryPlans>();
            Interlocked.Exchange(ref _previous, current);
            current = Interlocked.CompareExchange(ref _current, fresh, current);
        }

        var per = current.GetOrAdd(queryText, _ => new PerQueryPlans(MaxPlansPerQuery));
        per.Publish(plan, MaxPlansPerQuery);
    }

    /// <summary>
    /// Fixed-slot per-query plan cache. Lookup is SIMD scan over parallel int arrays;
    /// matched candidates are revalidated against the plan's own embedded keys to
    /// guard against torn-write races.
    /// </summary>
    internal sealed class PerQueryPlans
    {
        private readonly int[] _orderings;
        private readonly int[] _typesigs;
        private readonly CompiledPlan[] _plans;
        private int _filled;

        /// <summary>Cached clause template. Set once on first execution, immutable thereafter.
        /// Allows subsequent executions to skip AST parsing by re-resolving parameter
        /// values directly from the blittable using the template's bindings.</summary>
        public ClauseTemplate Template;

        public PerQueryPlans(int maxSlots)
        {
            _orderings = new int[maxSlots];
            _typesigs = new int[maxSlots];
            _plans = new CompiledPlan[maxSlots];
        }

        public CompiledPlan TryLookup(int ordering, int typesig, ReadOnlySpan<byte> kinds, int maxSlots)
        {
            if (Vector256.IsHardwareAccelerated)
                return Vec256Lookup(ordering, typesig, kinds, maxSlots);
            if (Vector128.IsHardwareAccelerated)
                return Vec128Lookup(ordering, typesig, kinds, maxSlots);
            return ScalarLookup(ordering, typesig, kinds, maxSlots);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static CompiledPlan ResolveCandidate(CompiledPlan head, int ordering, int typesig, ReadOnlySpan<byte> kinds)
        {
            for (var p = head; p != null; p = p.Next)
            {
                if (p.Ordering != ordering || p.TypeSignature != typesig)
                    continue;
                if (p.FullKinds == null)
                    return p;
                if (kinds.SequenceEqual(p.FullKinds))
                    return p;
            }
            return null;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private CompiledPlan Vec256Lookup(int ordering, int typesig, ReadOnlySpan<byte> kinds, int maxSlots)
        {
            var ordVec = Vector256.Create(ordering);
            var typVec = Vector256.Create(typesig);
            for (int i = 0; i < maxSlots; i += 8)
            {
                var ords = Vector256.LoadUnsafe(ref _orderings[i]);
                var typs = Vector256.LoadUnsafe(ref _typesigs[i]);
                var match = Vector256.Equals(ords, ordVec) & Vector256.Equals(typs, typVec);
                uint mask = match.ExtractMostSignificantBits();
                while (mask != 0)
                {
                    int lane = BitOperations.TrailingZeroCount(mask);
                    mask &= mask - 1;
                    var head = Volatile.Read(ref _plans[i + lane]);
                    var resolved = ResolveCandidate(head, ordering, typesig, kinds);
                    if (resolved != null)
                        return resolved;
                }
            }
            return null;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private CompiledPlan Vec128Lookup(int ordering, int typesig, ReadOnlySpan<byte> kinds, int maxSlots)
        {
            var ordVec = Vector128.Create(ordering);
            var typVec = Vector128.Create(typesig);
            for (int i = 0; i < maxSlots; i += 4)
            {
                var ords = Vector128.LoadUnsafe(ref _orderings[i]);
                var typs = Vector128.LoadUnsafe(ref _typesigs[i]);
                var match = Vector128.Equals(ords, ordVec) & Vector128.Equals(typs, typVec);
                uint mask = match.ExtractMostSignificantBits();
                while (mask != 0)
                {
                    int lane = BitOperations.TrailingZeroCount(mask);
                    mask &= mask - 1;
                    var head = Volatile.Read(ref _plans[i + lane]);
                    var resolved = ResolveCandidate(head, ordering, typesig, kinds);
                    if (resolved != null)
                        return resolved;
                }
            }
            return null;
        }

        private CompiledPlan ScalarLookup(int ordering, int typesig, ReadOnlySpan<byte> kinds, int maxSlots)
        {
            for (int i = 0; i < maxSlots; i++)
            {
                if (_orderings[i] != ordering || _typesigs[i] != typesig)
                    continue;
                var head = Volatile.Read(ref _plans[i]);
                var resolved = ResolveCandidate(head, ordering, typesig, kinds);
                if (resolved != null)
                    return resolved;
            }
            return null;
        }

        public void Publish(CompiledPlan plan, int maxSlots)
        {
            // Try chain prepend for >16-kind plans that share (ordering, typesig) hash
            if (plan.FullKinds != null && TryChainPrepend(plan, maxSlots))
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
            Volatile.Write(ref _typesigs[slot], plan.TypeSignature);
        }

        private bool TryChainPrepend(CompiledPlan plan, int maxSlots)
        {
            for (int i = 0; i < maxSlots; i++)
            {
                if (_orderings[i] != plan.Ordering || _typesigs[i] != plan.TypeSignature)
                    continue;
                while (true)
                {
                    var head = Volatile.Read(ref _plans[i]);
                    if (head == null)
                        // Slot was emptied — don't drop the plan, fall back to slot insert
                        return false;
                    plan.Next = head;
                    if (Interlocked.CompareExchange(ref _plans[i], plan, head) == head)
                        return true;
                }
            }
            // No matching slot found — fall back to normal slot insert (don't drop!)
            return false;
        }
    }
}
