using System;
using System.Collections.Generic;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Primitives;
using Sparrow.Server;
using Voron.Data.RoaringBitmaps;

namespace Corax.Querying.Matches;

/// <summary>
/// Lazily ORs two child matches into a RoaringBitmap on the first Fill()/Count call,
/// then iterates the bitmap for subsequent fills. Mirrors <see cref="TermsProviderMatch"/>:
/// no work is done at construction time, so the cost lands in query execution rather than
/// plan-build, and <see cref="Inspect"/> reports the real Or structure (with both children)
/// instead of an opaque already-materialized bitmap.
///
/// Used for sentinel-rewritten BETWEEN clauses such as "x BETWEEN low AND 'NULL'", which
/// rewrite to "x >= low" but must also include null-valued documents for Lucene parity.
/// </summary>
public sealed class LazyOrMatch : IBitmapQueryMatch, IDisposable
{
    private readonly IQueryMatch _left;
    private readonly IQueryMatch _right;
    private RoaringBitmap _bitmap;
    private RoaringBitmapIterator _iterator;
    private bool _initialized;

    public LazyOrMatch(ByteStringContext allocator, IQueryMatch left, IQueryMatch right)
    {
        _left = left;
        _right = right;
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

    public QueryCountConfidence Confidence => _initialized ? QueryCountConfidence.High : QueryCountConfidence.Low;

    public DuplicatesOccurrence DuplicatesOccurrenceStatus => DuplicatesOccurrence.NotPossible;

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

    public int Fill(Span<long> matches)
    {
        Initialize();
        return _iterator.Fill(ref _bitmap, matches);
    }

    public int AndWith(Span<long> buffer, int matches)
    {
        Initialize();
        // The buffer may arrive in sort-field order (e.g. from SortedIndexReader), not in
        // entry-ID order, so we cannot use a sorted intersection here — test membership
        // against the materialized bitmap one entry at a time, mirroring TermsProviderMatch.
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
        return new QueryInspectionNode($"{nameof(LazyOrMatch)} [Or]",
            children: new List<QueryInspectionNode> { _left.Inspect(), _right.Inspect() },
            parameters: new Dictionary<string, string>
            {
                { Constants.QueryInspectionNode.IsBoosting, IsBoosting.ToString() },
                { Constants.QueryInspectionNode.Count, _initialized ? Count.ToString() : "lazy" },
                { Constants.QueryInspectionNode.CountConfidence, Confidence.ToString() },
            });
    }

    private void Initialize()
    {
        if (_initialized)
            return;
        _bitmap.Clear();
        QueryPrimitives.OrWithMatch(_left, ref _bitmap);
        QueryPrimitives.OrWithMatch(_right, ref _bitmap);
        _bitmap.PrepareForReading();
        _iterator = _bitmap.GetIterator();
        _initialized = true;
    }

    public void Dispose()
    {
        if (_initialized)
            _iterator.Dispose();
        _bitmap.Dispose();
        (_left as IDisposable)?.Dispose();
        (_right as IDisposable)?.Dispose();
    }
}
