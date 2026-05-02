using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Corax.Querying.Matches.Meta;
using Voron.Data.RoaringBitmaps;

namespace Corax.Querying.Matches;

/// <summary>
/// Lightweight IQueryMatch backed by a RoaringBitmap. Used when query operations
/// (search, OR/AND of terms) produce a bitmap result that needs to be wrapped
/// as an IQueryMatch for the rest of the pipeline.
/// </summary>
public unsafe struct BitmapMatch : IQueryMatch, IBitmapQueryMatch, IDisposable
{
    private RoaringBitmap _bitmap;
    private RoaringBitmapIterator _iterator;
    private bool _iteratorInitialized;

    public BitmapMatch(RoaringBitmap bitmap)
    {
        _bitmap = bitmap;
        _iteratorInitialized = false;
    }

    public BitmapMatch(Sparrow.Server.ByteStringContext allocator)
    {
        _bitmap = new RoaringBitmap(allocator);
        _iteratorInitialized = false;
    }

    /// <summary>Get a mutable reference to the internal bitmap for building.</summary>
    public ref RoaringBitmap Bitmap => ref _bitmap;

    public long Count => _bitmap.Count;
    public QueryCountConfidence Confidence => QueryCountConfidence.High;
    public bool IsBoosting => false;
    public DuplicatesOccurrence DuplicatesOccurrenceStatus => DuplicatesOccurrence.NotPossible;

    public bool Contains(long entryId) => _bitmap.Contains(entryId);

    public long MinEntryId
    {
        get
        {
            long minKey = _bitmap.MinContainerKey;
            return minKey < 0 ? 0 : minKey * RoaringBitmap.ContainerSize;
        }
    }

    public long MaxEntryId
    {
        get
        {
            long maxKey = _bitmap.MaxContainerKey;
            return maxKey < 0 ? 0 : (maxKey + 1) * RoaringBitmap.ContainerSize - 1;
        }
    }

    public int Fill(Span<long> matches)
    {
        if (!_iteratorInitialized)
        {
            _bitmap.PrepareForReading();
            _iterator = _bitmap.GetIterator();
            _iteratorInitialized = true;
        }
        return _iterator.Fill(ref _bitmap, matches);
    }

    public int AndWith(Span<long> buffer, int matches)
    {
        int kept = 0;
        for (int i = 0; i < matches; i++)
        {
            if (_bitmap.Contains(buffer[i]))
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
        _bitmap.Dispose();
    }
}
