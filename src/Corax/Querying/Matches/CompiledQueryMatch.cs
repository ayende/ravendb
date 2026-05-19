using System;
using System.Buffers;
using System.Collections.Generic;
using System.Threading;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Planning;
using Voron.Data.RoaringBitmaps;
using Sparrow.Server;
using Voron;
using Voron.Impl;

namespace Corax.Querying.Matches;

/// <summary>Sort seek hint — value to seek to in the sort field, plus whether the bound is inclusive.</summary>
public sealed record SortHint(string FieldName, object Value, bool Inclusive);

public class CompiledQueryMatch(
    CompiledPlan compiledPlan,
    int bitmapCount,
    int opCount,
    IQueryMatch[] resolvedMatches,
    PostingSource[] postingSources,
    ITermsProvider[] termsProviders,
    IndexSearcher searcher,
    ByteStringContext allocator,
    bool wantTimings,
    CancellationToken token)
    : IBitmapQueryMatch, IPredicateEvaluationContext, IDisposable
{
    private readonly QueryIlEmitter.CompiledExecuteDelegate _compiledDelegate =
        wantTimings ? compiledPlan.CompiledTimedDelegate : compiledPlan.CompiledDelegate;

    public readonly ResidualScanIlEmitter.ResidualScanPredicate CompiledEntryPredicate = compiledPlan.CompiledEntryPredicate;

    public SortHint SortHint;
    public readonly IQueryMatch[] ResolvedMatches = resolvedMatches;
    public readonly PostingSource[] PostingSources = postingSources;
    public readonly ITermsProvider[] TermsProviders = termsProviders;

    /// <summary>Per-execution term counts for OrRange/AndRange ops. Each range op
    /// stores its index into this array instead of a hardcoded count in the IL.
    /// Set during resolution — different executions of the same query can have
    /// different IN term counts without needing different compiled delegates.</summary>
    public int[] InRangeCounts;

    // Entry scan: predicates + parameters for CompiledQueryHelper.RunEntryScan
    public ScanPredicateInfo[] ScanPredicateInfos;
    public long[] ScanLongParams;
    public double[] ScanDoubleParams;
    public Slice[] ScanSliceParams;
    public long[] ScanFieldRootPages;

    long[] IPredicateEvaluationContext.ResidualLongParams => ScanLongParams;
    double[] IPredicateEvaluationContext.ResidualDoubleParams => ScanDoubleParams;
    Slice[] IPredicateEvaluationContext.ResidualSliceParams => ScanSliceParams;
    long[] IPredicateEvaluationContext.ResidualFieldRootPages => ScanFieldRootPages;

    // Entry scan telemetry (populated by IL/C# entry scan when it triggers)
    public long EntryScanEntriesScanned;
    public long EntryScanEntriesPassed;

    private readonly string _explainSource = compiledPlan.ExplainSource;
    private readonly string _csharpSource = compiledPlan.CSharpSource;
    public readonly IndexSearcher Searcher = searcher;
    public readonly CancellationToken Token = token;

    private RoaringBitmap _bitmapData = new(allocator);
    private RoaringBitmapIterator _iterator;
    private bool _executed;
    private long _count = -1;

    // Bitmap pool: [0] = main result, [1 ... N] = scratch.
    // Allocated once in Execute(), then accessed by the compiled delegate via IL.
    public RoaringBitmap[] Bitmaps;

    // LowLevelTransaction cached at Execute() time so the emitted IL does not re-fetch it per op.
    public LowLevelTransaction Llt;

    /// <summary>Limit for early-exit during bitmap accumulation (unsorted queries only).
    /// When set, FillFromPostings stops after limit entries and OR branches are
    /// skipped once the bitmap has enough. Set to long.MaxValue when an ORDER BY
    /// is present (sorting needs the full bitmap for Contains checks).
    /// Stored as long to avoid casts when comparing with Count (long) in the IL-emitted code.</summary>
    public long Limit = long.MaxValue;

    // Telemetry — populated during Execute if timings are requested
    public long[] Timings;
    public long[] ResultCounts;
    public int EntryScanTakenAtOp;

    public long Count
    {
        get
        {
            if (!_executed) Execute();
            return _count;
        }
    }

    public QueryCountConfidence Confidence => _executed
        ? (_count < Limit ? QueryCountConfidence.High : QueryCountConfidence.Low)
        : QueryCountConfidence.Normal;

    public bool IsBoosting
    {
        get
        {
            foreach (var it in ResolvedMatches ?? [])
            {
                if (it.IsBoosting)
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
        return _bitmapData.AndWith(buffer, matches);
    }

    public void Score(Span<long> matches, Span<float> scores, float boostFactor)
    {
        foreach (var it in ResolvedMatches ?? [])
        {
            it.Score(matches, scores, boostFactor);
        }
    }

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
            ["Explain"] = _explainSource ?? "N/A",
            ["CSharpSource"] = _csharpSource ?? "N/A"
        };

        if (EntryScanTakenAtOp >= 0)
        {
            parameters["EntryScanAt"] = EntryScanTakenAtOp.ToString();
            if (EntryScanEntriesScanned > 0)
                parameters["EntryScanScanned"] = EntryScanEntriesScanned.ToString();
            if (EntryScanEntriesPassed > 0)
                parameters["EntryScanPassed"] = EntryScanEntriesPassed.ToString();
        }

        if (Timings is { Length: > 0 })
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

        var children = new List<QueryInspectionNode>();
        if (ResolvedMatches != null)
        {
            foreach (var it in ResolvedMatches)
            {
                children.Add(it.Inspect());
            }
        }

        return new QueryInspectionNode("CompiledQuery", parameters: parameters, children: children);
    }


    private void Execute()
    {
        if (_executed) return;

        // Rent bitmap pool from ArrayPool — returned in finally block
        Bitmaps = ArrayPool<RoaringBitmap>.Shared.Rent(bitmapCount);
        Bitmaps[0] = _bitmapData; // main bitmap (owned by this instance)
        for (int i = 1; i < bitmapCount; i++) Bitmaps[i] = new RoaringBitmap(allocator);

        Llt = Searcher.Transaction.LowLevelTransaction;

        // Only allocate timing arrays when explicitly requested (include timings())
        Timings = wantTimings ? new long[opCount] : null;
        ResultCounts = wantTimings ? new long[opCount] : null;
        EntryScanTakenAtOp = -1;

        try
        {
            _compiledDelegate(this);

            _bitmapData = Bitmaps[0];
            _bitmapData.PrepareForReading();
            _count = _bitmapData.Count;
            _iterator = _bitmapData.GetIterator();
            _executed = true;
        }
        finally
        {
            for (int i = 1; i < bitmapCount; i++)
                Bitmaps[i].Dispose();
            ArrayPool<RoaringBitmap>.Shared.Return(Bitmaps, clearArray: true);
            Bitmaps = null;
        }
    }

    public void Dispose()
    {
        _iterator.Dispose();
        _bitmapData.Dispose();
    }
}
