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

public struct CompiledQueryMatch : IBitmapQueryMatch, IDisposable
{
    private readonly QueryIlEmitter.CompiledExecuteDelegate _compiledDelegate;
    public readonly IQueryMatch[] ResolvedMatches;
    public readonly TermSource[] TermSources;
    public readonly ITermsProvider[] TermsProviders;
    public readonly long[] LongParams;
    public readonly double[] DoubleParams;
    public readonly Slice[] SliceParams;
    public readonly long[] FieldRootPages;
    private readonly string _explainSource;
    private readonly ByteStringContext _allocator;
    public readonly IndexSearcher Searcher;
    private readonly int _bitmapCount;
    private readonly int _opCount;
    public readonly CancellationToken Token;

    private RoaringBitmap _bitmapData;
    private RoaringBitmapIterator _iterator;
    private bool _executed;
    private long _count;

    // Bitmap pool: [0] = main result, [1..N] = scratch.
    // Allocated once in Execute(), then accessed by the compiled delegate via IL.
    public RoaringBitmap[] Bitmaps;

    // LowLevelTransaction cached at Execute() time so the emitted IL does not re-fetch it per op.
    public LowLevelTransaction Llt;

    /// <summary>Limit for early-exit during bitmap accumulation (unsorted queries only).
    /// When set, FillFromPostings stops after limit entries and OR branches are
    /// skipped once the bitmap has enough. Set to long.MaxValue when an ORDER BY
    /// is present (sorting needs the full bitmap for Contains checks).</summary>
    public long Limit;

    // Telemetry — populated during Execute if timings are requested
    public long[] Timings;
    public long[] ResultCounts;
    public int EntryScanTakenAtOp;

    public CompiledQueryMatch(CompiledPlan compiledPlan, int bitmapCount, int opCount,
        IQueryMatch[] resolvedMatches, TermSource[] termSources, ITermsProvider[] termsProviders,
        long[] longParams, double[] doubleParams, Slice[] sliceParams, long[] fieldRootPages,
        IndexSearcher searcher, ByteStringContext allocator, CancellationToken token)
    {
        _compiledDelegate = compiledPlan.CompiledDelegate;
        _bitmapCount = bitmapCount;
        _opCount = opCount;
        ResolvedMatches = resolvedMatches;
        TermSources = termSources;
        TermsProviders = termsProviders;
        LongParams = longParams;
        DoubleParams = doubleParams;
        SliceParams = sliceParams;
        FieldRootPages = fieldRootPages;
        _explainSource = compiledPlan.ExplainSource;
        _allocator = allocator;
        Searcher = searcher;
        Token = token;
        Limit = long.MaxValue;
        _bitmapData = new RoaringBitmap(allocator);
        Bitmaps = null;
        Llt = null;
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
            if (ResolvedMatches == null)
                return false;
            for (int i = 0; i < ResolvedMatches.Length; i++)
            {
                if (ResolvedMatches[i].IsBoosting)
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
        if (ResolvedMatches == null)
            return;

        for (int i = 0; i < ResolvedMatches.Length; i++)
        {
            ResolvedMatches[i].Score(matches, scores, boostFactor);
        }
    }

    /// <summary>Get execution telemetry for external inspection graph builders.</summary>
    public void GetTelemetry(out long[] timings, out long[] resultCounts, out int entryScanTakenAtOp)
    {
        timings = Timings;
        resultCounts = ResultCounts;
        entryScanTakenAtOp = EntryScanTakenAtOp;
    }

    public QueryInspectionNode Inspect()
    {
        var parameters = new Dictionary<string, string>
        {
            ["Explain"] = _explainSource ?? "N/A"
        };

        if (EntryScanTakenAtOp >= 0)
            parameters["EntryScanAt"] = EntryScanTakenAtOp.ToString();

        if (Timings != null && Timings.Length > 0)
        {
            double tickFreq = System.Diagnostics.Stopwatch.Frequency / 1000.0; // ticks per ms
            for (int i = 0; i < Timings.Length; i++)
            {
                if (Timings[i] > 0)
                    parameters[$"Op{i}_ms"] = (Timings[i] / tickFreq).ToString("F3");
                if (i < ResultCounts.Length && ResultCounts[i] > 0)
                    parameters[$"Op{i}_count"] = ResultCounts[i].ToString();
            }
        }

        // Report as "MultiTermMatch" for backward compatibility with query plan inspection tests.
        // Children are the resolved inner matches (e.g., ExistsTermsProvider, TermQuery, etc.).
        var children = new List<QueryInspectionNode>();
        if (ResolvedMatches != null)
        {
            for (int i = 0; i < ResolvedMatches.Length; i++)
                children.Add(ResolvedMatches[i].Inspect());
        }

        return new QueryInspectionNode("MultiTermMatch", parameters: parameters, children: children);
    }

    public SkipSortingResult AttemptToSkipSorting() => SkipSortingResult.ResultsNativelySorted;

    private void Execute()
    {
        if (_executed) return;

        // Allocate bitmap pool: [0] = main, [1..N] = scratch
        if (Bitmaps == null || Bitmaps.Length < _bitmapCount)
            Bitmaps = new RoaringBitmap[_bitmapCount];

        for (int i = 0; i < Bitmaps.Length; i++) Bitmaps[i] = new RoaringBitmap(_allocator);
        Bitmaps[0] = _bitmapData; // main bitmap (owned by this struct)

        // Cache LLT for the delegate
        Llt = Searcher.Transaction.LowLevelTransaction;

        // Only allocate timing arrays when telemetry is requested (opCount > 0).
        // Caller passes opCount = 0 to skip allocation.
        Timings = _opCount > 0 ? new long[_opCount] : null;
        ResultCounts = _opCount > 0 ? new long[_opCount] : null;
        EntryScanTakenAtOp = -1;

        try
        {
            _compiledDelegate(ref this);

            // Take ownership of bitmaps[0] (may have been swapped during entry scan)
            _bitmapData = Bitmaps[0];
            _bitmapData.PrepareForReading();
            _count = _bitmapData.Count;
            _iterator = _bitmapData.GetIterator();
            _executed = true; // Mark only after successful execution
        }
        finally
        {
            // Dispose scratch bitmaps only (not [0], which is _bitmapData)
            for (int i = 1; i < Bitmaps.Length; i++)
                Bitmaps[i].Dispose();
        }
    }

    public void Dispose()
    {
        _iterator.Dispose();
        _bitmapData.Dispose();
    }
}
