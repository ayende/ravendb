using System;
using System.Collections.Generic;
using Corax.Querying.Matches.Meta;
using Sparrow.Server;
using Voron.Data.RoaringBitmaps;

namespace Corax.Querying.Matches;

/// <summary>
/// Lightweight replacement for BinaryMatch. Combines two IQueryMatch instances
/// with OR or AND semantics. Production queries are emitted directly through
/// QueryILEmitter and never reach this type; it survives only for Or/And test
/// entry points on IndexSearcher. Materializes both sides into a RoaringBitmap
/// on the first Fill, applies the set operation in place, then streams the
/// resulting iterator on subsequent Fills.
/// </summary>
public struct CombinedMatch : IQueryMatch, IDisposable
{
    private IQueryMatch _left;
    private IQueryMatch _right;
    private readonly ByteStringContext _allocator;
    private readonly bool _isOr;
    private RoaringBitmap _bitmap;
    private RoaringBitmapIterator _iterator;
    private bool _materialized;
    private long _resultCount;

    private const int FillScratchSize = 4096;

    private CombinedMatch(ByteStringContext allocator, IQueryMatch left, IQueryMatch right, bool isOr)
    {
        _allocator = allocator;
        _left = left;
        _right = right;
        _isOr = isOr;
        _bitmap = new RoaringBitmap(allocator);
        _iterator = default;
        _materialized = false;
        _resultCount = 0;
    }

    public static CombinedMatch Or(ByteStringContext allocator, IQueryMatch left, IQueryMatch right) => new(allocator, left, right, isOr: true);
    public static CombinedMatch And(ByteStringContext allocator, IQueryMatch left, IQueryMatch right) => new(allocator, left, right, isOr: false);

    public long Count
    {
        get
        {
            if (_materialized)
                return _resultCount;
            return _isOr
                ? Math.Min(_left.Count + _right.Count, long.MaxValue)
                : Math.Min(_left.Count, _right.Count);
        }
    }

    public QueryCountConfidence Confidence => _materialized ? QueryCountConfidence.High : _left.Confidence.Min(_right.Confidence);
    public bool IsBoosting => _left.IsBoosting || _right.IsBoosting;
    public DuplicatesOccurrence DuplicatesOccurrenceStatus => DuplicatesOccurrence.NotPossible;

    public SkipSortingResult AttemptToSkipSorting() => SkipSortingResult.ResultsNativelySorted;

    public int Fill(Span<long> matches)
    {
        if (_materialized == false)
            Materialize();

        return _iterator.Fill(ref _bitmap, matches);
    }

    private void Materialize()
    {
        // Drain the left side directly into the result bitmap.
        FillBitmapFromMatch(_left, ref _bitmap);

        if (_isOr)
        {
            // For OR, drain the right side into a scratch bitmap and lazy-merge.
            using var rightBitmap = new RoaringBitmap(_allocator);
            var right = rightBitmap;
            FillBitmapFromMatch(_right, ref right);
            right.PrepareForReading();
            _bitmap.LazyOrWith(ref right);
            _bitmap.RepairAfterLazy();
        }
        else
        {
            // For AND, drain the right side into a scratch bitmap and intersect.
            _bitmap.PrepareForReading();
            using var rightBitmap = new RoaringBitmap(_allocator);
            var right = rightBitmap;
            FillBitmapFromMatch(_right, ref right);
            right.PrepareForReading();
            _bitmap.AndWith(ref right);
        }

        _bitmap.PrepareForReading();
        _resultCount = _bitmap.Count;
        _iterator = _bitmap.GetIterator();
        _materialized = true;
    }

    private static void FillBitmapFromMatch(IQueryMatch match, ref RoaringBitmap bitmap)
    {
        Span<long> scratch = stackalloc long[FillScratchSize];
        int read;
        while ((read = match.Fill(scratch)) > 0)
        {
            // Match Fill contracts emit ascending IDs within a batch but successive batches
            // may overlap or step backwards, so add per-element instead of AddRange.
            for (int i = 0; i < read; i++)
                bitmap.Add(scratch[i]);
        }
    }

    public int AndWith(Span<long> buffer, int matches)
    {
        throw new NotSupportedException($"{nameof(CombinedMatch)} does not support AndWith.");
    }

    public void Score(Span<long> matches, Span<float> scores, float boostFactor)
    {
        _left.Score(matches, scores, boostFactor);
        _right.Score(matches, scores, boostFactor);
    }

    public QueryInspectionNode Inspect()
    {
        return new QueryInspectionNode(_isOr ? "Or" : "And",
            children: new List<QueryInspectionNode> { _left.Inspect(), _right.Inspect() },
            parameters: new Dictionary<string, string>
            {
                { "Count", Count.ToString() },
                { "IsBoosting", IsBoosting.ToString() }
            });
    }

    public void Dispose()
    {
        _iterator.Dispose();
        _bitmap.Dispose();
    }
}
