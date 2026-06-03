using System;
using System.Buffers;
using System.Collections.Generic;
using System.Threading;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Planning;
using Voron.Data.RoaringBitmaps;
using Sparrow.Server;
using Voron.Impl;

namespace Corax.Querying.Matches;

/// <summary>Sort seek hint — value to seek to in the sort field, plus whether the bound is inclusive.</summary>
public sealed record SortHint(string FieldName, object Value);

public class CompiledQueryMatch(
    CompiledPlan compiledPlan,
    QueryExecution exec,
    int bitmapCount,
    int opCount,
    IQueryMatch[] resolvedMatches,
    LeafResolveInfo[] leaves,
    IndexSearcher searcher,
    ByteStringContext allocator,
    bool wantTimings,
    CancellationToken token)
    : IBitmapQueryMatch, IDisposable
{
    private readonly QueryIlEmitter.CompiledExecuteDelegate _compiledDelegate =
        wantTimings ? compiledPlan.CompiledTimedDelegate : compiledPlan.CompiledDelegate;

    public readonly ResidualScanIlEmitter.ResidualScanPredicate CompiledEntryPredicate = compiledPlan.EntryScanSet.Compiled;

    /// <summary>Per-execution state — entry-scan IL reads this for analyzer-encoded slices,
    /// field-root pages, and direct long/double values via baked field indices.</summary>
    public readonly QueryExecution Exec = exec;

    public SortHint SortHint;
    public readonly IQueryMatch[] ResolvedMatches = resolvedMatches;

    /// <summary>Per-leaf resolution metadata for PostingSource / TreeScan slots, parallel to
    /// <see cref="ResolvedMatches"/>. Match slots carry <see cref="LeafResolveKind.PreResolved"/>
    /// and read from <see cref="ResolvedMatches"/> instead. Filled by Raven.Server; the posting
    /// source / terms provider is materialized lazily inside <c>QueryPrimitives</c>.</summary>
    public readonly LeafResolveInfo[] Leaves = leaves;

    public int[] InRangeCounts;

    /// <summary>Per-slot planner cardinality estimate. The entry-scan heuristic reads
    /// <c>Cardinalities[cursor]</c> to decide whether bitmap[0] is small enough relative
    /// to the next clause's estimated entries to switch to per-entry scanning. Sized to
    /// match the resolver's slot layout (<see cref="ResolvedMatches"/>/<see cref="Leaves"/>),
    /// so the IL cursor can index it directly regardless of dispatch.</summary>
    public long[] Cardinalities;

    public long EntryScanEntriesScanned; 
    public long EntryScanEntriesPassed;

    public readonly IndexSearcher Searcher = searcher;
    public readonly CancellationToken Token = token;

    private RoaringBitmap _bitmapData = new(allocator);
    private RoaringBitmapIterator _iterator;
    private bool _executed;
    private long _count = -1;

    public RoaringBitmap[] Bitmaps;

    public LowLevelTransaction Llt;

    public int Limit = int.MaxValue;

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
                if (it != null && it.IsBoosting)
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
            if (it != null)
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
            ["CSharpSource"] = compiledPlan?.Source ?? "N/A",
            ["CSharpSourceFormatted"] = compiledPlan?.FormattedSource ?? "N/A"
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
                if (it != null)
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

        // The two runtime exits leave the result in different slots: the bitmap pipeline lands in slot 0,
        // the entry-scan tail writes survivors to slot 1 (RunEntryScan source 0 -> target 1) without
        // swapping back. Read the result from whichever slot the taken exit used. Stays 0 on exception so
        // the disposal below matches the original "keep slot 0, dispose the rest" behavior.
        try
        {
            _compiledDelegate(this);

            int resultSlot = EntryScanTakenAtOp >= 0 ? 1 : 0;
            _bitmapData = Bitmaps[resultSlot];
            Bitmaps[resultSlot] = default; // don't dispose this
            _bitmapData.PrepareForReading();
            _count = _bitmapData.Count;
            _iterator = _bitmapData.GetIterator();
            _executed = true;
        }
        finally
        {
            for (int i = 0; i < bitmapCount; i++)
            {
                Bitmaps[i].Dispose();
            }
            ArrayPool<RoaringBitmap>.Shared.Return(Bitmaps, clearArray: true);
            Bitmaps = null;
        }
    }

    public void Dispose()
    {
        _iterator.Dispose();
        _bitmapData.Dispose();
        Llt = null; // release transaction reference so it is not kept alive longer than needed
    }

}
