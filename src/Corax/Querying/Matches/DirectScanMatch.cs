using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Planning;
using Corax.Querying.Primitives;
using Sparrow;
using Sparrow.Server;
using Voron;
using Voron.Data.Containers;
using Voron.Data.RoaringBitmaps;
using Corax.Utils;
using Voron.Impl;

namespace Corax.Querying.Matches;

/// <summary>
/// Walks a driving tree in sort order, checking residual predicates per entry via
/// stored field reads. Replaces CompiledQueryMatch + SortingMatch when the cost model
/// determines that a bounded tree walk + entry scan is cheaper than building a bitmap.
///
/// Entry IDs from each tree batch are sorted by container location for sequential page
/// access, then predicates are checked in that order. A parallel index array tracks
/// original positions so results are emitted in field-value (sort) order.
/// </summary>
public sealed class DirectScanMatch : IQueryMatch, IPredicateEvaluationContext, IDisposable
{
    private readonly IndexSearcher _searcher;
    private readonly LowLevelTransaction _llt;
    private readonly IQueryMatch _drivingMatch;  // TermsProviderMatch or similar that walks the driving tree
    private readonly int _take;
    private long _totalMatched;

    // Residual predicate checking
    private readonly ScanPredicateInfo[] _residualPredicates;
    internal readonly long[] ScanLongParams;
    internal readonly double[] ScanDoubleParams;
    internal readonly Slice[] ScanSliceParams;
    internal readonly long[] ScanFieldRootPages;
    private readonly ResidualScanIlEmitter.ResidualScanPredicate _compiledResidualScan;

    // Dedup for multi-value fields
    private RoaringBitmap _emittedBitmap;
    private readonly ByteStringContext _allocator;

    // Telemetry (always collected — cheap)
    private long _treeEntriesScanned;
    private long _entriesPassedFilter;
    private long _entriesRejected;
    private long _treeScanTicks;
    private long _entryScanTicks;
    private string _stoppedReason;

    // Diagnostic metadata
    public string DrivingTreeName;
    public string DrivingClause;
    public string SeekBound;
    public string Direction;
    public string ResidualDescription;
    public string Reason;

    public DirectScanMatch(
        IndexSearcher searcher,
        IQueryMatch drivingMatch,
        ScanPredicateInfo[] residualPredicates,
        long[] longParams,
        double[] doubleParams,
        Slice[] sliceParams,
        long[] fieldRootPages,
        int take,
        ResidualScanIlEmitter.ResidualScanPredicate precompiledDelegate)
    {
        _searcher = searcher;
        _llt = searcher.Transaction.LowLevelTransaction;
        _drivingMatch = drivingMatch;
        _residualPredicates = residualPredicates;
        ScanLongParams = longParams;
        ScanDoubleParams = doubleParams;
        ScanSliceParams = sliceParams;
        ScanFieldRootPages = fieldRootPages;
        _take = take;
        _allocator = searcher.Allocator;
        _emittedBitmap = new RoaringBitmap(_allocator);
        _compiledResidualScan = precompiledDelegate;
    }

    public long Count => _totalMatched;
    public QueryCountConfidence Confidence => QueryCountConfidence.Low;
    public bool IsBoosting => false;
    public DuplicatesOccurrence DuplicatesOccurrenceStatus => DuplicatesOccurrence.NotPossible;

    [SkipLocalsInit]
    public unsafe int Fill(Span<long> matches)
    {
        if (_take > 0 && _totalMatched >= _take)
            return 0;

        int count = 0;
        int remaining = _take > 0 ? (int)Math.Min(matches.Length, _take - _totalMatched) : matches.Length;
        int batchSize = Math.Min(QueryPrimitives.EntryScanBatchSize, Math.Max(1, remaining));
        Span<long> batch = stackalloc long[QueryPrimitives.EntryScanBatchSize];
        Span<int> indices = stackalloc int[QueryPrimitives.EntryScanBatchSize];
        Span<bool> passed = stackalloc bool[QueryPrimitives.EntryScanBatchSize];
        Span<long> sortedIds = stackalloc long[QueryPrimitives.EntryScanBatchSize];
        Span<long> containerLocs = stackalloc long[QueryPrimitives.EntryScanBatchSize];
        Span<UnmanagedSpan> containerSpans = stackalloc UnmanagedSpan[QueryPrimitives.EntryScanBatchSize];
        Span<long> packedIds = stackalloc long[QueryPrimitives.EntryScanBatchSize];
        Span<int> packedOrigIdx = stackalloc int[QueryPrimitives.EntryScanBatchSize];

        while (count < remaining)
        {
            long t0 = Stopwatch.GetTimestamp();
            int read = _drivingMatch.Fill(batch[..batchSize]);
            _treeScanTicks += Stopwatch.GetTimestamp() - t0;

            if (read == 0)
            {
                _stoppedReason ??= "TreeExhausted";
                break;
            }
            _treeEntriesScanned += read;

            if (_residualPredicates == null || _residualPredicates.Length == 0)
            {
                for (int i = 0; i < read && count < remaining; i++)
                {
                    long id = batch[i];
                    if (_emittedBitmap.Contains(id) == false)
                    {
                        _emittedBitmap.Add(id);
                        matches[count++] = id;
                    }
                }
            }
            else
            {
                var sorted = sortedIds[..read];
                batch[..read].CopyTo(sorted);
                for (int j = 0; j < read; j++)
                    indices[j] = j;
                sorted.Sort(indices[..read]);

                passed[..read].Clear();

                long t1 = Stopwatch.GetTimestamp();

                var locs = containerLocs[..read];
                _searcher.ResolveEntryLocations(sorted, locs);

                var spans = containerSpans[..read];
                Container.GetAll(_llt, locs, spans, -1, _llt.PageLocator);

                _searcher.InitializeSpecialTermsMarkers();

                var readersArr = ArrayPool<EntryTermsReader>.Shared.Rent(read);
                var pIds = packedIds[..read];
                var pIdxs = packedOrigIdx[..read];
                int packed = 0;
                try
                {
                    for (int s = 0; s < read; s++)
                    {
                        int origIdx = indices[s];
                        long entryId = batch[origIdx];

                        if (_emittedBitmap.Contains(entryId))
                            continue; // dedup — silently drop

                        if (locs[s] == -1 || spans[s].Address == null)
                        {
                            _entriesRejected++;
                            continue;
                        }

                        readersArr[packed] = new EntryTermsReader(_llt,
                            _searcher.NullTermsMarkers, _searcher.NonExistingTermsMarkers,
                            spans[s].Address, spans[s].Length, _searcher.DictionaryId, _searcher.VectorFieldsMarkers, null);
                        pIds[packed] = entryId;
                        pIdxs[packed] = origIdx;
                        packed++;
                    }

                    int matched = _compiledResidualScan(this,
                        readersArr.AsSpan(0, packed),
                        packedIds[..packed],
                        packedOrigIdx[..packed]);

                    _entriesRejected += packed - matched;

                    for (int k = 0; k < matched; k++)
                        passed[pIdxs[k]] = true;
                }
                finally
                {
                    ArrayPool<EntryTermsReader>.Shared.Return(readersArr, clearArray: true);
                }
                _entryScanTicks += Stopwatch.GetTimestamp() - t1;

                // Emit matches in original (field-value) order
                for (int i = 0; i < read && count < remaining; i++)
                {
                    if (passed[i])
                    {
                        long id = batch[i];
                        _emittedBitmap.Add(id);
                        _entriesPassedFilter++;
                        matches[count++] = id;
                    }
                }
            }
        }

        if (_take > 0 && _totalMatched + count >= _take)
            _stoppedReason ??= $"_take({_take})";

        _totalMatched += count;
        return count;
    }

    public int AndWith(Span<long> buffer, int matches) => throw new NotSupportedException("DirectScanMatch produces final sorted results");

    public void Score(Span<long> matches, Span<float> scores, float boostFactor)
    {
        // No scoring — DirectScanMatch is only used for unboosted queries
    }

    public SkipSortingResult AttemptToSkipSorting() => SkipSortingResult.ResultsNativelySorted;

    public QueryInspectionNode Inspect()
    {
        double tickFreq = Stopwatch.Frequency / 1000.0;
        var parameters = new Dictionary<string, string>();

        if (DrivingTreeName != null) parameters["DrivingTree"] = DrivingTreeName;
        if (DrivingClause != null) parameters["DrivingClause"] = DrivingClause;
        if (SeekBound != null) parameters["SeekBound"] = SeekBound;
        if (Direction != null) parameters["TreeDirection"] = Direction;
        if (ResidualDescription != null) parameters["ResidualPredicates"] = ResidualDescription;
        if (Reason != null) parameters["Reason"] = Reason;

        if (_treeScanTicks > 0) parameters["TreeScan_ms"] = (_treeScanTicks / tickFreq).ToString("F3");
        if (_entryScanTicks > 0) parameters["EntryScans_ms"] = (_entryScanTicks / tickFreq).ToString("F3");

        parameters["TreeEntriesScanned"] = _treeEntriesScanned.ToString();
        parameters["EntriesPassedFilter"] = _entriesPassedFilter.ToString();
        parameters["EntriesRejected"] = _entriesRejected.ToString();

        if (_stoppedReason != null) parameters["StoppedAt"] = _stoppedReason;

        return new QueryInspectionNode("DirectScan", parameters: parameters);
    }

    public void Dispose()
    {
        _emittedBitmap.Dispose();
        (_drivingMatch as IDisposable)?.Dispose();
    }

    long[] IPredicateEvaluationContext.ResidualLongParams => ScanLongParams;
    double[] IPredicateEvaluationContext.ResidualDoubleParams => ScanDoubleParams;
    Slice[] IPredicateEvaluationContext.ResidualSliceParams => ScanSliceParams;
    long[] IPredicateEvaluationContext.ResidualFieldRootPages => ScanFieldRootPages;
}
