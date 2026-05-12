using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Planning;
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
public sealed class DirectScanMatch : IQueryMatch, IDisposable
{
    private readonly IndexSearcher _searcher;
    private readonly LowLevelTransaction _llt;
    private readonly IQueryMatch _drivingMatch;  // TermsProviderMatch or similar that walks the driving tree
    private readonly int _take;
    private long _totalMatched;

    // Residual predicate checking
    private readonly ScanPredicateInfo[] _residualPredicates;
    private readonly long[] _longParams;
    private readonly double[] _doubleParams;
    private readonly Slice[] _sliceParams;
    private readonly long[] _fieldRootPages;

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
        int take)
    {
        _searcher = searcher;
        _llt = searcher.Transaction.LowLevelTransaction;
        _drivingMatch = drivingMatch;
        _residualPredicates = residualPredicates;
        _longParams = longParams;
        _doubleParams = doubleParams;
        _sliceParams = sliceParams;
        _fieldRootPages = fieldRootPages;
        _take = take;
        _allocator = searcher.Allocator;
        _emittedBitmap = new RoaringBitmap(_allocator);
    }

    public long Count => _totalMatched;
    public QueryCountConfidence Confidence => QueryCountConfidence.Low;
    public bool IsBoosting => false;
    public DuplicatesOccurrence DuplicatesOccurrenceStatus => DuplicatesOccurrence.NotPossible;

    [SkipLocalsInit]
    public int Fill(Span<long> matches)
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
                // Sort batch by entry ID for page locality, track original indices
                for (int i = 0; i < read; i++)
                    indices[i] = i;

                // Simple insertion sort (batch is small, ~256 entries)
                for (int i = 1; i < read; i++)
                {
                    int key = indices[i];
                    long keyVal = batch[key];
                    int j = i - 1;
                    while (j >= 0 && batch[indices[j]] > keyVal)
                    {
                        indices[j + 1] = indices[j];
                        j--;
                    }
                    indices[j + 1] = key;
                }

                // Check predicates in entry-ID order (page locality)
                // Mark which original positions passed
                passed.Slice(0, read).Clear();

                Page lastPage = default;
                long t1 = Stopwatch.GetTimestamp();
                for (int s = 0; s < read; s++)
                {
                    int origIdx = indices[s];
                    long entryId = batch[origIdx];

                    if (_emittedBitmap.Contains(entryId))
                        continue; // dedup

                    var reader = _searcher.GetEntryTermsReader(entryId, ref lastPage);
                    if (CheckAllPredicates(ref reader))
                        passed[origIdx] = true;
                    else
                        _entriesRejected++;
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

    private bool CheckAllPredicates(ref EntryTermsReader reader)
    {
        for (int p = 0; p < _residualPredicates.Length; p++)
        {
            ref readonly var pred = ref _residualPredicates[p];
            reader.Reset();

            if (reader.FindNext(_fieldRootPages[pred.ParamIndex]) == false)
            {
                // Field not found in this entry
                if (pred.CompareOp == ScanCompareOp.NotEqual)
                    continue; // NOT having the field satisfies NotEquals
                return false; // All other ops fail when field is missing
            }

            if (EvaluatePredicate(ref reader, in pred) == false)
                return false;
        }
        return true;
    }

    private bool EvaluatePredicate(ref EntryTermsReader reader, in ScanPredicateInfo pred)
    {
        switch (pred.ValueType)
        {
            case ScanValueType.Long:
            {
                long actual = reader.CurrentLong;
                long param = _longParams[pred.ParamIndex];
                if (pred.CompareOp == ScanCompareOp.Between)
                {
                    long param2 = _longParams[pred.ParamIndex2];
                    return actual >= param && actual <= param2;
                }
                return pred.CompareOp switch
                {
                    ScanCompareOp.Equal => actual == param,
                    ScanCompareOp.NotEqual => actual != param,
                    ScanCompareOp.GreaterThan => actual > param,
                    ScanCompareOp.GreaterThanOrEqual => actual >= param,
                    ScanCompareOp.LessThan => actual < param,
                    ScanCompareOp.LessThanOrEqual => actual <= param,
                    _ => false
                };
            }
            case ScanValueType.Double:
            {
                double actual = reader.CurrentDouble;
                double param = _doubleParams[pred.ParamIndex];
                if (pred.CompareOp == ScanCompareOp.Between)
                {
                    double param2 = _doubleParams[pred.ParamIndex2];
                    return actual >= param && actual <= param2;
                }
                return pred.CompareOp switch
                {
                    ScanCompareOp.Equal => actual == param,
                    ScanCompareOp.NotEqual => actual != param,
                    ScanCompareOp.GreaterThan => actual > param,
                    ScanCompareOp.GreaterThanOrEqual => actual >= param,
                    ScanCompareOp.LessThan => actual < param,
                    ScanCompareOp.LessThanOrEqual => actual <= param,
                    _ => false
                };
            }
            case ScanValueType.Slice:
            {
                var actual = reader.Current.Decoded();
                var param = _sliceParams[pred.ParamIndex].AsReadOnlySpan();
                if (pred.CompareOp == ScanCompareOp.Between)
                {
                    var param2 = _sliceParams[pred.ParamIndex2].AsReadOnlySpan();
                    return actual.SequenceCompareTo(param) >= 0 && actual.SequenceCompareTo(param2) <= 0;
                }
                return pred.CompareOp switch
                {
                    ScanCompareOp.Equal => actual.SequenceEqual(param),
                    ScanCompareOp.NotEqual => actual.SequenceEqual(param) == false,
                    ScanCompareOp.GreaterThan => actual.SequenceCompareTo(param) > 0,
                    ScanCompareOp.GreaterThanOrEqual => actual.SequenceCompareTo(param) >= 0,
                    ScanCompareOp.LessThan => actual.SequenceCompareTo(param) < 0,
                    ScanCompareOp.LessThanOrEqual => actual.SequenceCompareTo(param) <= 0,
                    _ => false
                };
            }
            default:
                return false;
        }
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
}
