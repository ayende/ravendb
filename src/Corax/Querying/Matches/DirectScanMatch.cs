using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Planning;
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
        ResidualScanIlEmitter.ResidualScanPredicate precompiledDelegate = null)
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
        _compiledResidualScan = precompiledDelegate ??
            (residualPredicates is { Length: > 0 }
                ? ResidualScanIlEmitter.EmitDelegate(residualPredicates, multiValueStartsWith: true)
                : null);
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
        int batchSize = Math.Min(256, remaining * 4);
        Span<long> batch = stackalloc long[batchSize];
        Span<int> indices = stackalloc int[batchSize];
        Span<bool> passed = stackalloc bool[batchSize];

        while (count < remaining)
        {
            long t0 = Stopwatch.GetTimestamp();
            int read = _drivingMatch.Fill(batch);
            _treeScanTicks += Stopwatch.GetTimestamp() - t0;

            if (read == 0)
            {
                _stoppedReason ??= "TreeExhausted";
                break;
            }
            _treeEntriesScanned += read;

            if (_residualPredicates == null || _residualPredicates.Length == 0)
            {
                // No residual predicates — all tree entries are matches
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
                // Sort batch by entry ID for page locality, track original indices.
                // sortedIds gets the sorted entry IDs; indices[i] maps back to the
                // original position in batch (field-value sort order).
                Span<long> sortedIds = stackalloc long[read];
                batch.Slice(0, read).CopyTo(sortedIds);
                for (int i = 0; i < read; i++)
                    indices[i] = i;
                sortedIds.Sort(indices.Slice(0, read));

                passed.Slice(0, read).Clear();

                long t1 = Stopwatch.GetTimestamp();

                // Batch resolve to container locations for sequential page access
                Span<long> containerLocs = stackalloc long[read];
                _searcher.ResolveEntryLocations(sortedIds, containerLocs);

                Span<UnmanagedSpan> spans = stackalloc UnmanagedSpan[read];
                Container.GetAll(_llt, containerLocs, spans, -1, _llt.PageLocator);

                _searcher.InitializeSpecialTermsMarkers();

                // Pack readers/entryIds/origIndexes for entries that have valid locations
                // and are not already emitted. Invalid/dedup entries are counted but skipped.
                var readersArr = ArrayPool<EntryTermsReader>.Shared.Rent(read);
                Span<long> packedIds = stackalloc long[read];
                Span<int> packedOrigIdx = stackalloc int[read];
                int packed = 0;
                try
                {
                    for (int s = 0; s < read; s++)
                    {
                        int origIdx = indices[s];
                        long entryId = batch[origIdx];

                        if (_emittedBitmap.Contains(entryId))
                            continue; // dedup — silently drop

                        if (containerLocs[s] == -1 || spans[s].Address == null)
                        {
                            _entriesRejected++;
                            continue;
                        }

                        readersArr[packed] = new EntryTermsReader(_llt,
                            _searcher.NullTermsMarkers, _searcher.NonExistingTermsMarkers,
                            spans[s].Address, spans[s].Length, _searcher.DictionaryId, _searcher.VectorFieldsMarkers, null);
                        packedIds[packed] = entryId;
                        packedOrigIdx[packed] = origIdx;
                        packed++;
                    }

                    int matched = _compiledResidualScan(this,
                        readersArr.AsSpan(0, packed),
                        packedIds[..packed],
                        packedOrigIdx[..packed]);

                    _entriesRejected += packed - matched;

                    // Mark which original positions survived. Iterate batch in sort order to emit.
                    for (int k = 0; k < matched; k++)
                        passed[packedOrigIdx[k]] = true;
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
