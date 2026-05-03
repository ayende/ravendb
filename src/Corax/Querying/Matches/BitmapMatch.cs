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
public unsafe struct BitmapMatch : IQueryMatch, IBitmapQueryMatch, IDisposable
{
    private RoaringBitmapData _bitmapState;
    private readonly ByteStringContext _allocator;
    private RoaringBitmapIterator _iterator;
    private bool _iteratorInitialized;

    public BitmapMatch(ByteStringContext allocator)
    {
        _bitmapState = default;
        _allocator = allocator;
        _iteratorInitialized = false;
    }

    /// <summary>Get a mutable reference to the internal bitmap state for building.
    /// The returned ref is intentionally unscoped because callers thread it through
    /// QueryPrimitives.FillFromMatch / AndWithMatch chains where the BitmapMatch lives
    /// on the caller's stack frame for the full call duration. Suppresses CS9084.</summary>
    [UnscopedRef]
    public ref RoaringBitmapData BitmapState => ref _bitmapState;

    /// <summary>Returns the allocator for this bitmap match.</summary>
    public ByteStringContext Allocator => _allocator;

    public long Count => _bitmapState.Count;
    public QueryCountConfidence Confidence => QueryCountConfidence.High;
    public bool IsBoosting => false;
    public DuplicatesOccurrence DuplicatesOccurrenceStatus => DuplicatesOccurrence.NotPossible;

    public bool Contains(long entryId) => _bitmapState.Contains(entryId);

    /// <summary>
    /// Returns a reference to the underlying bitmap data for downstream consumption.
    /// </summary>
    [UnscopedRef]
    public ref RoaringBitmapData GetBitmapData() => ref _bitmapState;

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
        if (!_iteratorInitialized)
        {
            _bitmapState.PrepareForReading();
            _iterator = _bitmapState.GetIterator(_allocator);
            _iteratorInitialized = true;
        }
        return _iterator.Fill(ref _bitmapState, matches);
    }

    public int AndWith(Span<long> buffer, int matches)
    {
        int kept = 0;
        for (int i = 0; i < matches; i++)
        {
            if (_bitmapState.Contains(buffer[i]))
                buffer[kept++] = buffer[i];
        }
        return kept;
    }

    public void Score(Span<long> matches, Span<float> scores, float boostFactor)
    {
        // No scoring for bitmap-only matches
    }

    public SkipSortingResult AttemptToSkipSorting() => SkipSortingResult.SortingIsRequired;

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
        _bitmapState.Dispose(_allocator);
    }
}
