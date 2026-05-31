using System;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Primitives;
using Sparrow.Server;
using Voron.Data.RoaringBitmaps;
using Voron.Impl;

namespace Corax.Querying.Matches;

/// <summary>
/// Lazily fills a RoaringBitmap from the ITermsProvider on first Fill() call, then iterates the bitmap for subsequent fills.
/// Created by IndexSearcher factory methods (StartsWithQuery, EndsWithQuery, InQuery, ExistsQuery, RegexQuery, range queries, etc.) for use outside the CompiledQueryMatch pipeline.
/// </summary>
public sealed class TermsProviderMatch(ITermsProvider provider, LowLevelTransaction llt, ByteStringContext allocator) : IBitmapQueryMatch, IDisposable
{
    public ITermsProvider Provider => provider;

    public LowLevelTransaction Llt => llt;
    private RoaringBitmap _bitmap = new(allocator);
    private RoaringBitmapIterator _iterator;
    private bool _initialized;

    public bool IsBoosting => false;

    public long Count
    {
        get
        {
            Initialize();
            return _bitmap.Count;
        }
    }

    public bool Contains(long entryId)
    {
        Initialize();
        return _bitmap.Contains(entryId);
    }

    public long MinEntryId
    {
        get
        {
            Initialize();
            long minKey = _bitmap.MinContainerKey;
            return minKey < 0 ? 0 : minKey * RoaringBitmap.ContainerSize;
        }
    }

    public long MaxEntryId
    {
        get
        {
            Initialize();
            long maxKey = _bitmap.MaxContainerKey;
            return maxKey < 0 ? 0 : (maxKey + 1) * RoaringBitmap.ContainerSize - 1;
        }
    }

    public ref RoaringBitmap BitmapState
    {
        get
        {
            Initialize();
            return ref _bitmap;
        }
    }

    public QueryCountConfidence Confidence => _initialized ? QueryCountConfidence.High : QueryCountConfidence.Low;

    public DuplicatesOccurrence DuplicatesOccurrenceStatus => DuplicatesOccurrence.NotPossible;


    public int Fill(Span<long> matches)
    {
        Initialize();
        return _iterator.Fill(ref _bitmap, matches);
    }

    public int AndWith(Span<long> buffer, int matches)
    {
        Initialize();
        return _bitmap.AndWith(buffer, matches);
    }

    public void Score(Span<long> matches, Span<float> scores, float boostFactor)
    {
        if (boostFactor == 0f)
            return;
        Initialize();
        for (int i = 0; i < matches.Length; i++)
        {
            if (_bitmap.Contains(matches[i]))
                scores[i] += boostFactor;
        }
    }

    public QueryInspectionNode Inspect()
    {
        return provider.Inspect();
    }

    private void Initialize()
    {
        if (_initialized)
            return;
        _bitmap.Clear();
        QueryPrimitives.FillBitmapFromTreeScan(provider, llt, ref _bitmap);
        _bitmap.PrepareForReading();
        _iterator = _bitmap.GetIterator();
        _initialized = true;
    }

    public void Dispose()
    {
        if (_initialized)
            _iterator.Dispose();
        _bitmap.Dispose();
    }
}
