using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Planning;
using Voron.Data.RoaringBitmaps;
using Sparrow.Server;
using Voron;
using Voron.Impl;

namespace Corax.Querying.Matches;

public unsafe struct CompiledQueryMatch : IQueryMatch, IBitmapQueryMatch, IDisposable
{
    private readonly QueryILEmitter.CompiledExecuteDelegate _compiledDelegate;
    internal readonly IQueryMatch[] _resolvedMatches;
    internal readonly TermSource[] _termSources;
    internal readonly ITermProvider[] _termProviders;
    internal readonly long[] _longParams;
    internal readonly double[] _doubleParams;
    internal readonly Slice[] _sliceParams;
    internal readonly long[] _fieldRootPages;
    private readonly string _explainSource;
    private readonly ByteStringContext _allocator;
    internal readonly IndexSearcher _searcher;
    private readonly int _bitmapCount;
    private readonly int _opCount;
    internal readonly CancellationToken _token;

    private RoaringBitmap _bitmapData;
    private RoaringBitmapIterator _iterator;
    private bool _executed;
    private long _count;

    // Bitmap pool: [0] = main result, [1..N] = scratch.
    // Allocated once in Execute(), then accessed by the compiled delegate via IL.
    internal RoaringBitmap[] _bitmaps;

    // LowLevelTransaction cached at Execute() time so the emitted IL does not re-fetch it per op.
    internal LowLevelTransaction _llt;

    // Telemetry — populated during Execute if timings are requested
    internal long[] _timings;
    internal long[] _resultCounts;
    internal int _entryScanTakenAtOp;

    public CompiledQueryMatch(CompiledPlan compiledPlan, int bitmapCount, int opCount,
        IQueryMatch[] resolvedMatches, TermSource[] termSources, ITermProvider[] termProviders,
        long[] longParams, double[] doubleParams, Slice[] sliceParams, long[] fieldRootPages,
        IndexSearcher searcher, ByteStringContext allocator, long limit, CancellationToken token)
    {
        _ = limit; // not yet used; reserved for limit-aware bitmap-build early-exit
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
        _bitmaps = null;
        _llt = null;
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
    public ref RoaringBitmap BitmapState
    {
        get
        {
            if (!_executed) Execute();
            return ref _bitmapData;
        }
    }

    public int Fill(Span<long> matches)
    {
        if (!_executed) Execute();
        return _iterator.Fill(ref _bitmapData, matches);
    }

    public int AndWith(Span<long> buffer, int matches)
    {
        if (!_executed) Execute();
        // Cannot use AndWithSorted: callers (SortUsingIndexFromBitmap) pass entry IDs
        // in sort-field order (e.g. alphabetical by Name), not in entry-ID order.
        int kept = 0;
        for (int i = 0; i < matches; i++)
        {
            if (_bitmapData.Contains(buffer[i]))
                buffer[kept++] = buffer[i];
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

    /// <summary>Get execution telemetry for external inspection graph builders.</summary>
    public void GetTelemetry(out long[] timings, out long[] resultCounts, out int entryScanTakenAtOp)
    {
        timings = _timings;
        resultCounts = _resultCounts;
        entryScanTakenAtOp = _entryScanTakenAtOp;
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
        if (_bitmaps == null || _bitmaps.Length < _bitmapCount)
            _bitmaps = new RoaringBitmap[_bitmapCount];

        for (int i = 0; i < _bitmaps.Length; i++) _bitmaps[i] = new RoaringBitmap(_allocator);
        _bitmaps[0] = _bitmapData; // main bitmap (owned by this struct)

        // Cache LLT for the delegate
        _llt = _searcher.Transaction.LowLevelTransaction;

        // Only allocate timing arrays when telemetry is requested (opCount > 0).
        // Caller passes opCount = 0 to skip allocation.
        _timings = _opCount > 0 ? new long[_opCount] : null;
        _resultCounts = _opCount > 0 ? new long[_opCount] : null;
        _entryScanTakenAtOp = -1;

        try
        {
            _compiledDelegate(ref this);

            // Take ownership of bitmaps[0] (may have been swapped during entry scan)
            _bitmapData = _bitmaps[0];
            _bitmapData.PrepareForReading();
            _count = _bitmapData.Count;
            _iterator = _bitmapData.GetIterator();
            _executed = true; // Mark only after successful execution
        }
        finally
        {
            // Dispose scratch bitmaps only (not [0], which is _bitmapData)
            for (int i = 1; i < _bitmaps.Length; i++)
                _bitmaps[i].Dispose();
        }
    }

    public void Dispose()
    {
        _iterator.Dispose();
        _bitmapData.Dispose();
    }
}
