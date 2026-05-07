using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Planning;
using Voron.Data.RoaringBitmaps;
using Sparrow.Server;
using Voron;

namespace Corax.Querying.Matches;

public unsafe struct CompiledQueryMatch : IQueryMatch, IBitmapQueryMatch, IDisposable
{
    private readonly QueryILEmitter.CompiledExecuteDelegate _compiledDelegate;
    private readonly IQueryMatch[] _resolvedMatches;
    private readonly TermSource[] _termSources;
    private readonly ITermProvider[] _termProviders;
    private readonly long[] _longParams;
    private readonly double[] _doubleParams;
    private readonly Slice[] _sliceParams;
    private readonly long[] _fieldRootPages;
    private readonly string _explainSource;
    private readonly ByteStringContext _allocator;
    private readonly IndexSearcher _searcher;
    private readonly int _bitmapCount;
    private readonly int _opCount;
    private readonly CancellationToken _token;

    private RoaringBitmap _bitmapData;
    private RoaringBitmapIterator _iterator;
    private bool _executed;
    private long _count;

    // Telemetry — populated during Execute if timings are requested
    private long[] _timings;
    private long[] _resultCounts;
    private int _entryScanTakenAtOp;

    public CompiledQueryMatch(CompiledPlan compiledPlan, int bitmapCount, int opCount,
        IQueryMatch[] resolvedMatches, TermSource[] termSources, ITermProvider[] termProviders,
        long[] longParams, double[] doubleParams, Slice[] sliceParams, long[] fieldRootPages,
        IndexSearcher searcher, ByteStringContext allocator, long limit, CancellationToken token)
    {
        _ = limit; // not yet used; reserved for limit-aware bitmap-build early-exit (planned with #84-full)
        _compiledDelegate = compiledPlan.CompiledDelegate;
        _bitmapCount = bitmapCount;
        _opCount = opCount;
        _resolvedMatches = resolvedMatches;
        _termSources = termSources;
        _termProviders = termProviders;
        _longParams = longParams;
        _doubleParams = doubleParams;
        _sliceParams = sliceParams;
        _fieldRootPages = fieldRootPages;
        _explainSource = compiledPlan.ExplainSource;
        _allocator = allocator;
        _searcher = searcher;
        _token = token;
        _bitmapData = new RoaringBitmap(allocator);
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

    public bool Contains(long entryId)
    {
        if (!_executed) Execute();
        return _bitmapData.Contains(entryId);
    }

    public long MinEntryId
    {
        get
        {
            if (!_executed) Execute();
            long minKey = _bitmapData.MinContainerKey;
            return minKey < 0 ? 0 : minKey * RoaringBitmap.ContainerSize;
        }
    }

    public long MaxEntryId
    {
        get
        {
            if (!_executed) Execute();
            long maxKey = _bitmapData.MaxContainerKey;
            return maxKey < 0 ? 0 : (maxKey + 1) * RoaringBitmap.ContainerSize - 1;
        }
    }

    [UnscopedRef]
    public ref RoaringBitmap GetBitmapData()
    {
        if (!_executed) Execute();
        return ref _bitmapData;
    }

    public int Fill(Span<long> matches)
    {
        if (!_executed) Execute();
        return _iterator.Fill(ref _bitmapData, matches);
    }

    public int AndWith(Span<long> buffer, int matches)
    {
        if (!_executed) Execute();
        int kept = 0;
        int i = 0;
        while (i < matches)
        {
            long containerKey = buffer[i] >> RoaringBitmap.ContainerKeyShift;
            int containerSlot = _bitmapData.GetSlotForKey(containerKey);

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
                if (_bitmapData.Contains(buffer[i]))
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
        var parameters = new Dictionary<string, string>
        {
            ["Explain"] = _explainSource ?? "N/A"
        };

        if (_entryScanTakenAtOp >= 0)
            parameters["EntryScanAt"] = _entryScanTakenAtOp.ToString();

        if (_timings != null && _timings.Length > 0)
        {
            double tickFreq = System.Diagnostics.Stopwatch.Frequency / 1000.0; // ticks per ms
            for (int i = 0; i < _timings.Length; i++)
            {
                if (_timings[i] > 0)
                    parameters[$"Op{i}_ms"] = (_timings[i] / tickFreq).ToString("F3");
                if (i < _resultCounts.Length && _resultCounts[i] > 0)
                    parameters[$"Op{i}_count"] = _resultCounts[i].ToString();
            }
        }

        // Report as "MultiTermMatch" for backward compatibility with query plan inspection tests.
        // Children are the resolved inner matches (e.g., ExistsTermProvider, TermQuery, etc.).
        var children = new List<QueryInspectionNode>();
        if (_resolvedMatches != null)
        {
            for (int i = 0; i < _resolvedMatches.Length; i++)
                children.Add(_resolvedMatches[i].Inspect());
        }

        return new QueryInspectionNode("MultiTermMatch", parameters: parameters, children: children);
    }

    public SkipSortingResult AttemptToSkipSorting() => SkipSortingResult.ResultsNativelySorted;

    private void Execute()
    {
        if (_executed) return;

        // Allocate bitmap pool: [0] = main, [1..N] = scratch
        Span<RoaringBitmap> bitmaps = new RoaringBitmap[_bitmapCount];
        for (int i = 0; i < bitmaps.Length; i++) bitmaps[i] = new RoaringBitmap(_allocator);
        bitmaps[0] = _bitmapData; // main bitmap (owned by this struct)

        try
        {
            // Only allocate timing arrays when telemetry is requested (opCount > 0).
            // Caller passes opCount = 0 to skip allocation.
            _timings = _opCount > 0 ? new long[_opCount] : null;
            _resultCounts = _opCount > 0 ? new long[_opCount] : null;

            var ctx = new QueryScanContext
            {
                Bitmaps = bitmaps,
                Searcher = _searcher,
                DirectSources = _resolvedMatches.AsSpan(),
                TermSources = _termSources != null ? _termSources.AsSpan() : Span<TermSource>.Empty,
                Llt = _searcher.Transaction.LowLevelTransaction,
                TermProviders = _termProviders != null ? _termProviders.AsSpan() : Span<ITermProvider>.Empty,
                FieldRootPages = _fieldRootPages.AsSpan(),
                LongParams = _longParams.AsSpan(),
                DoubleParams = _doubleParams.AsSpan(),
                SliceParams = _sliceParams.AsSpan(),
                Token = _token,
                Timings = _timings != null ? _timings.AsSpan() : Span<long>.Empty,
                ResultCounts = _resultCounts != null ? _resultCounts.AsSpan() : Span<long>.Empty,
                EntryScanTakenAtOp = -1
            };

            _compiledDelegate(ref ctx);

            _entryScanTakenAtOp = ctx.EntryScanTakenAtOp;

            // Take ownership of bitmaps[0] (may have been swapped during entry scan)
            _bitmapData = bitmaps[0];
            _bitmapData.PrepareForReading();
            _count = _bitmapData.Count;
            _iterator = _bitmapData.GetIterator();
            _executed = true; // Mark only after successful execution
        }
        finally
        {
            // Dispose scratch bitmaps only (not [0], which is _bitmapData)
            for (int i = 1; i < bitmaps.Length; i++)
                bitmaps[i].Dispose();
        }
    }

    public void Dispose()
    {
        _iterator.Dispose();
        _bitmapData.Dispose();
    }
}
