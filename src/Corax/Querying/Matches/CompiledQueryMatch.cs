using System;
using System.Collections.Generic;
using System.Threading;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Planning;
using Corax.Utils.RoaringBitmaps;
using Sparrow.Server;
using Voron;

namespace Corax.Querying.Matches;

public unsafe struct CompiledQueryMatch : IQueryMatch, IDisposable
{
    private readonly QueryILEmitter.CompiledExecuteDelegate _compiledDelegate;
    private readonly IQueryMatch[] _resolvedMatches;
    private readonly long[] _longParams;
    private readonly double[] _doubleParams;
    private readonly Slice[] _sliceParams;
    private readonly long[] _fieldRootPages;
    private readonly string _explainSource;
    private readonly ByteStringContext _allocator;
    private readonly IndexSearcher _searcher;
    private readonly int _bitmapCount;
    private readonly long _limit;
    private readonly CancellationToken _token;

    private RoaringBitmap _bitmap;
    private RoaringBitmapIterator _iterator;
    private bool _executed;
    private long _count;

    public CompiledQueryMatch(CompiledPlan compiledPlan, int bitmapCount,
        IQueryMatch[] resolvedMatches,
        long[] longParams, double[] doubleParams, Slice[] sliceParams, long[] fieldRootPages,
        IndexSearcher searcher, ByteStringContext allocator, long limit, CancellationToken token)
    {
        _compiledDelegate = compiledPlan.CompiledDelegate;
        _bitmapCount = bitmapCount;
        _resolvedMatches = resolvedMatches;
        _longParams = longParams;
        _doubleParams = doubleParams;
        _sliceParams = sliceParams;
        _fieldRootPages = fieldRootPages;
        _explainSource = compiledPlan.ExplainSource;
        _allocator = allocator;
        _searcher = searcher;
        _limit = limit;
        _token = token;
        _bitmap = new RoaringBitmap(allocator);
        _iterator = default;
        _executed = false;
        _count = -1;
    }

    public long Count
    {
        get
        {
            if (!_executed) Execute();
            return _count;
        }
    }

    public QueryCountConfidence Confidence => _executed ? QueryCountConfidence.High : QueryCountConfidence.Normal;

    public bool IsBoosting
    {
        get
        {
            if (_resolvedMatches == null)
                return false;
            for (int i = 0; i < _resolvedMatches.Length; i++)
            {
                if (_resolvedMatches[i].IsBoosting)
                    return true;
            }
            return false;
        }
    }

    public DuplicatesOccurrence DuplicatesOccurrenceStatus => DuplicatesOccurrence.NotPossible;

    public int Fill(Span<long> matches)
    {
        if (!_executed) Execute();
        return _iterator.Fill(ref _bitmap, matches);
    }

    public int AndWith(Span<long> buffer, int matches)
    {
        if (!_executed) Execute();
        int kept = 0;
        int i = 0;
        while (i < matches)
        {
            long containerKey = buffer[i] >> RoaringBitmap.ContainerKeyShift;
            int containerSlot = _bitmap.GetSlotForKey(containerKey);

            if (containerSlot < 0)
            {
                long nextContainerStart = (containerKey + 1) << RoaringBitmap.ContainerKeyShift;
                while (i < matches && buffer[i] < nextContainerStart)
                    i++;
                continue;
            }

            long containerEnd = (containerKey + 1) << RoaringBitmap.ContainerKeyShift;
            while (i < matches && buffer[i] < containerEnd)
            {
                if (_bitmap.Contains(buffer[i]))
                    buffer[kept++] = buffer[i];
                i++;
            }
        }
        return kept;
    }

    public void Score(Span<long> matches, Span<float> scores, float boostFactor)
    {
        if (_resolvedMatches == null)
            return;

        for (int i = 0; i < _resolvedMatches.Length; i++)
        {
            _resolvedMatches[i].Score(matches, scores, boostFactor);
        }
    }

    public QueryInspectionNode Inspect()
    {
        return new QueryInspectionNode(
            nameof(CompiledQueryMatch),
            parameters: new Dictionary<string, string>
            {
                ["Explain"] = _explainSource ?? "N/A"
            });
    }

    public SkipSortingResult AttemptToSkipSorting() => SkipSortingResult.ResultsNativelySorted;

    private void Execute()
    {
        if (_executed) return;
        _executed = true;

        // Allocate bitmap pool: [0] = main, [1..N] = scratch
        var bitmaps = new RoaringBitmap[_bitmapCount];
        bitmaps[0] = _bitmap; // main bitmap (owned by this struct)
        for (int i = 1; i < bitmaps.Length; i++)
            bitmaps[i] = new RoaringBitmap(_allocator);

        try
        {
            var ctx = new QueryScanContext
            {
                Bitmaps = bitmaps.AsSpan(),
                Searcher = _searcher,
                DirectSources = _resolvedMatches.AsSpan(),
                TermProviders = Span<ITermProvider>.Empty,
                FieldRootPages = _fieldRootPages.AsSpan(),
                LongParams = _longParams.AsSpan(),
                DoubleParams = _doubleParams.AsSpan(),
                SliceParams = _sliceParams.AsSpan(),
                Token = _token
            };

            _compiledDelegate(ref ctx);

            // Take ownership of bitmaps[0] (may have been swapped during entry scan)
            _bitmap = bitmaps[0];
            _bitmap.PrepareForReading();
            _count = _bitmap.Count;
            _iterator = _bitmap.GetIterator();
        }
        finally
        {
            // Dispose scratch bitmaps only (not [0], which is _bitmap)
            for (int i = 1; i < bitmaps.Length; i++)
                bitmaps[i].Dispose();
        }
    }

    public void Dispose()
    {
        _iterator.Dispose();
        _bitmap.Dispose();
    }
}
