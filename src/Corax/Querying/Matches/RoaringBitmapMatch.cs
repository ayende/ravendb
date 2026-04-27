using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Corax.Querying.Matches.Meta;
using Corax.Utils.RoaringBitmaps;
using Sparrow.Server;

namespace Corax.Querying.Matches;

/// <summary>
/// IQueryMatch backed by a RoaringBitmap. Materializes child matches into bitmaps,
/// performs set operations via SIMD-accelerated bitmap ops, then streams results
/// through Fill(). This avoids the O(n+m) merge cost of MergeHelper for large sets.
///
/// Used by IndexSearcher.BitmapAnd/BitmapOr/BitmapAndNot when both sides are expected
/// to produce large result sets where bitmap operations outperform streaming merges.
/// </summary>
public unsafe struct RoaringBitmapMatch : IQueryMatch, IDisposable
{
    private RoaringBitmap _bitmap;
    private RoaringBitmapIterator _iterator;
    private readonly long _count;
    private bool _iteratorInitialized;

    public RoaringBitmapMatch(RoaringBitmap bitmap)
    {
        _bitmap = bitmap;
        _count = bitmap.Cardinality;
        _iterator = default;
        _iteratorInitialized = false;
    }

    public long Count => _count;
    public QueryCountConfidence Confidence => QueryCountConfidence.High;
    public bool IsBoosting => false;
    public DuplicatesOccurrence DuplicatesOccurrenceStatus => DuplicatesOccurrence.NotPossible;

    public SkipSortingResult AttemptToSkipSorting() => SkipSortingResult.ResultsNativelySorted;

    public int Fill(Span<long> matches)
    {
        if (!_iteratorInitialized)
        {
            _iterator = _bitmap.GetIterator();
            _iteratorInitialized = true;
        }
        return _iterator.Fill(ref _bitmap, matches);
    }

    public int AndWith(Span<long> buffer, int matches)
    {
        // Filter the incoming sorted buffer against our bitmap
        int write = 0;
        for (int i = 0; i < matches; i++)
        {
            if (_bitmap.Contains(buffer[i]))
                buffer[write++] = buffer[i];
        }
        return write;
    }

    public void Score(Span<long> matches, Span<float> scores, float boostFactor) { }

    public QueryInspectionNode Inspect()
    {
        return new QueryInspectionNode(nameof(RoaringBitmapMatch),
            parameters: new Dictionary<string, string>
            {
                { Constants.QueryInspectionNode.Count, _count.ToString() },
                { Constants.QueryInspectionNode.CountConfidence, Confidence.ToString() }
            });
    }

    /// <summary>
    /// Materialize any IQueryMatch into a RoaringBitmap by draining its Fill() output.
    /// </summary>
    public static RoaringBitmap MaterializeToRoaringBitmap<T>(ByteStringContext ctx, ref T match) where T : IQueryMatch
    {
        var bitmap = new RoaringBitmap(ctx);
        Span<long> buffer = stackalloc long[4096];

        int read;
        while ((read = match.Fill(buffer)) > 0)
        {
            for (int i = 0; i < read; i++)
                bitmap.Add(buffer[i]);
        }
        bitmap.PrepareForReading();
        return bitmap;
    }

    public void Dispose()
    {
        if (_iteratorInitialized)
            _iterator.Dispose();
        _bitmap.Dispose();
    }
}
