using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Corax.Querying.Matches.Meta;
using Sparrow.Server;
using Voron.Data.RoaringBitmaps;

namespace Corax.Querying.Matches;

/// <summary>
/// Lightweight IQueryMatch backed by a RoaringBitmap. Used when query operations
/// (search, OR/AND of terms) produce a bitmap result that needs to be wrapped
/// as an IQueryMatch for the rest of the pipeline.
/// </summary>
public struct BitmapMatch(ByteStringContext allocator) : IBitmapQueryMatch, IDisposable
{
    private RoaringBitmap _bitmapState = new(allocator);
    private RoaringBitmapIterator _iterator;
    private bool _iteratorInitialized = false;
    private bool _isAllocated = true;

    /// <summary>
    /// Indicates whether this BitmapMatch was constructed with an allocator (true)
    /// or is a default-initialized struct (false). A default BitmapMatch holds no
    /// native memory and is safe to Dispose but must not be used for accumulation.
    /// </summary>
    public readonly bool IsAllocated => _isAllocated;

    /// <summary>Get a mutable reference to the internal bitmap state for building.
    /// The returned ref is intentionally unscoped because callers thread it through
    /// QueryPrimitives.OrWithMatch / AndWithMatch chains where the BitmapMatch lives
    /// on the caller's stack frame for the full call duration. Suppresses CS9084.</summary>
    [UnscopedRef]
    public ref RoaringBitmap BitmapState => ref _bitmapState;

    public long Count => _bitmapState.Count;
    public QueryCountConfidence Confidence => QueryCountConfidence.High;
    public bool IsBoosting => false;
    public DuplicatesOccurrence DuplicatesOccurrenceStatus => DuplicatesOccurrence.NotPossible;

    public bool Contains(long entryId) => _bitmapState.Contains(entryId);

    public long MinEntryId
    {
        get
        {
            long minKey = _bitmapState.MinContainerKey;
            return minKey < 0 ? 0 : minKey * RoaringBitmap.ContainerSize;
        }
    }

    public long MaxEntryId
    {
        get
        {
            long maxKey = _bitmapState.MaxContainerKey;
            return maxKey < 0 ? 0 : (maxKey + 1) * RoaringBitmap.ContainerSize - 1;
        }
    }

    public int Fill(Span<long> matches)
    {
        if (_iteratorInitialized is false)
        {
            _bitmapState.PrepareForReading();
            _iterator = _bitmapState.GetIterator();
            _iteratorInitialized = true;
        }
        return _iterator.Fill(ref _bitmapState, matches);
    }

    public int AndWith(Span<long> buffer, int matches)
    {
        // Cannot use AndWithSorted: callers may pass entry IDs in sort-field order
        // (e.g. alphabetical), not in entry-ID order. Cache the last (key, slot) between
        // elements — even in sort-field order, consecutive entries often share their high
        // 16 bits because doc IDs cluster by insertion time. On a hit we skip the slot
        // binary search entirely; on a miss we pay one extra compare.
        long cachedKey = -1;
        int cachedSlot = -1;
        int kept = 0;
        for (int i = 0; i < matches; i++)
        {
            long value = buffer[i];
            long key = value >> RoaringBitmap.ContainerKeyShift;
            if (key != cachedKey)
            {
                cachedSlot = _bitmapState.GetSlotForKey(key);
                cachedKey = key;
            }
            if (cachedSlot < 0)
                continue;
            if (_bitmapState.ContainsAtSlot(cachedSlot, (ushort)value))
                buffer[kept++] = value;
        }
        return kept;
    }

    public void Score(Span<long> matches, Span<float> scores, float boostFactor)
    {
        // No scoring for bitmap-only matches
    }


    public QueryInspectionNode Inspect()
    {
        return new QueryInspectionNode(nameof(BitmapMatch),
            parameters: new Dictionary<string, string>
            {
                { "Count", Count.ToString() }
            });
    }

    public void Dispose()
    {
        if (_iteratorInitialized)
            _iterator.Dispose();
        _bitmapState.Dispose();
    }
}
