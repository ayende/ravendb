using System;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Primitives;
using Sparrow.Server;
using Voron.Data.RoaringBitmaps;
using Voron.Impl;

namespace Corax.Querying.Matches;

/// <summary>
/// Lazily fills a RoaringBitmap from the ITermsProvider on first Fill() call,
/// then iterates the bitmap for subsequent fills.
/// Created by IndexSearcher factory methods (StartsWithQuery, EndsWithQuery, InQuery,
/// ExistsQuery, RegexQuery, range queries, etc.) for use outside the CompiledQueryMatch pipeline.
/// </summary>
public sealed class TermsProviderMatch : IBitmapQueryMatch, IDisposable
{
    private readonly ITermsProvider _provider;
    private readonly LowLevelTransaction _llt;

    public ITermsProvider Provider => _provider;

    public LowLevelTransaction Llt => _llt;
    private RoaringBitmap _bitmap;
    private RoaringBitmapIterator _iterator;
    private bool _initialized;

    public TermsProviderMatch(ITermsProvider provider, LowLevelTransaction llt, ByteStringContext allocator)
    {
        _provider = provider;
        _llt = llt;
        _bitmap = new RoaringBitmap(allocator);
        _iterator = default;
        _initialized = false;
    }

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
        // Cannot use AndWithSorted here: the buffer comes from SortedIndexReader
        // which returns entry IDs in sort-field order (e.g. alphabetical), not in
        // entry-ID order. The bitmap's container structure requires ID-sorted input.
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
        // For multi-term matches (startsWith, endsWith, contains, regex, etc.) there is no
        // per-term BM25 frequency tracking, so we contribute a flat boostFactor for each
        // entry that is present in the bitmap. This ensures that entries matched by a
        // boosted multi-term clause rank above entries that are not matched at all.
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
        return _provider.Inspect();
    }

    private void Initialize()
    {
        if (_initialized)
            return;
        _bitmap.Clear();
        QueryPrimitives.FillBitmapFromTreeScan(_provider, _llt, ref _bitmap);
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
