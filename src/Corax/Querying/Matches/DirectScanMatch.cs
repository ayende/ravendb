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
/// Walks a driving tree in sort order, optionally checking residual predicates per entry
/// via stored field reads. Two subclasses handle the residual/no-residual cases:
/// <see cref="DirectScanSimpleMatch"/> (simple pass-through) and
/// <see cref="DirectScanFilteredMatch"/> (evaluates compiled predicate delegate).
/// </summary>
public abstract class DirectScanMatchBase : IQueryMatch, IDisposable
{
    protected readonly IndexSearcher Searcher;
    protected readonly LowLevelTransaction Llt;
    protected readonly IQueryMatch DrivingMatch;
    protected readonly int Take;
    protected long TotalMatched;

    protected RoaringBitmap EmittedBitmap;

    protected long TreeEntriesScanned;
    protected long EntriesPassedFilter;
    protected long EntriesRejected;
    protected long TreeScanTicks;
    protected long EntryScanTicks;
    protected string StoppedReason;

    public string DrivingTreeName;
    public string DrivingClause;
    public string SeekBound;
    public string Direction;
    public string ResidualDescription;
    public string Reason;

    protected DirectScanMatchBase(IndexSearcher searcher, IQueryMatch drivingMatch, int take)
    {
        Searcher = searcher;
        Llt = searcher.Transaction.LowLevelTransaction;
        DrivingMatch = drivingMatch;
        Take = take;
        ByteStringContext allocator = searcher.Allocator;
        EmittedBitmap = new RoaringBitmap(allocator);
    }

    public long Count => TotalMatched;
    public QueryCountConfidence Confidence => QueryCountConfidence.Low;
    public bool IsBoosting => false;
    public DuplicatesOccurrence DuplicatesOccurrenceStatus => DuplicatesOccurrence.NotPossible;

    public abstract int Fill(Span<long> matches);

    public int AndWith(Span<long> buffer, int matches) => throw new NotSupportedException("DirectScanMatch produces final sorted results");

    public void Score(Span<long> matches, Span<float> scores, float boostFactor) { }

    public SkipSortingResult AttemptToSkipSorting() => SkipSortingResult.ResultsNativelySorted;

    public virtual QueryInspectionNode Inspect()
    {
        double tickFreq = Stopwatch.Frequency / 1000.0;
        var parameters = new Dictionary<string, string>();

        if (DrivingTreeName != null) parameters["DrivingTree"] = DrivingTreeName;
        if (DrivingClause != null) parameters["DrivingClause"] = DrivingClause;
        if (SeekBound != null) parameters["SeekBound"] = SeekBound;
        if (Direction != null) parameters["TreeDirection"] = Direction;
        if (ResidualDescription != null) parameters["ResidualPredicates"] = ResidualDescription;
        if (Reason != null) parameters["Reason"] = Reason;

        if (TreeScanTicks > 0) parameters["TreeScan_ms"] = (TreeScanTicks / tickFreq).ToString("F3");
        if (EntryScanTicks > 0) parameters["EntryScans_ms"] = (EntryScanTicks / tickFreq).ToString("F3");

        parameters["TreeEntriesScanned"] = TreeEntriesScanned.ToString();
        parameters["EntriesPassedFilter"] = EntriesPassedFilter.ToString();
        parameters["EntriesRejected"] = EntriesRejected.ToString();

        if (StoppedReason != null) parameters["StoppedAt"] = StoppedReason;

        return new QueryInspectionNode("DirectScan", parameters: parameters);
    }

    public void Dispose()
    {
        EmittedBitmap.Dispose();
        (DrivingMatch as IDisposable)?.Dispose();
    }
}

/// <summary>DirectScan with no residual predicates — simple dedup + pass-through.</summary>
public sealed class DirectScanSimpleMatch(IndexSearcher searcher, IQueryMatch drivingMatch, int take) : DirectScanMatchBase(searcher, drivingMatch, take)
{
    [SkipLocalsInit]
    public override unsafe int Fill(Span<long> matches)
    {
        if (Take > 0 && TotalMatched >= Take)
            return 0;

        int count = 0;
        int remaining = Take > 0 ? (int)Math.Min(matches.Length, Take - TotalMatched) : matches.Length;
        int batchSize = Math.Min(QueryPrimitives.EntryScanBatchSize, Math.Max(1, remaining));
        Span<long> batch = stackalloc long[QueryPrimitives.EntryScanBatchSize];

        while (count < remaining)
        {
            long t0 = Stopwatch.GetTimestamp();
            int read = DrivingMatch.Fill(batch[..batchSize]);
            TreeScanTicks += Stopwatch.GetTimestamp() - t0;

            if (read == 0)
            {
                StoppedReason ??= "TreeExhausted";
                break;
            }
            TreeEntriesScanned += read;

            for (int i = 0; i < read && count < remaining; i++)
            {
                long id = batch[i];
                if (EmittedBitmap.Contains(id) == false)
                {
                    EmittedBitmap.Add(id);
                    matches[count++] = id;
                }
            }
        }

        if (Take > 0 && TotalMatched + count >= Take)
            StoppedReason ??= $"_take({Take})";

        TotalMatched += count;
        return count;
    }
}

/// <summary>
/// DirectScan with residual predicates: evaluates a compiled IL delegate against
/// stored-field readers for each entry batch. Entry IDs are sorted by container
/// location for sequential page access, then the delegate compacts survivors to
/// the front. A parallel index tracks original sort positions so results are
/// emitted in field-value order.
///
/// Uses the same compiled delegate as the entry-scan path (CompiledQueryMatch),
/// evaluating ALL baked-in predicates — re-evaluating the driving-clause
/// predicates is harmless since the tree scan already matched them.
/// </summary>
public sealed class DirectScanFilteredMatch(
    IndexSearcher searcher,
    IQueryMatch drivingMatch,
    long[] longParams,
    double[] doubleParams,
    Slice[] sliceParams,
    long[] fieldRootPages,
    int take,
    ResidualScanIlEmitter.ResidualScanPredicate precompiledDelegate)
    : DirectScanMatchBase(searcher, drivingMatch, take), IPredicateEvaluationContext
{
    [SkipLocalsInit]
    public override unsafe int Fill(Span<long> matches)
    {
        if (Take > 0 && TotalMatched >= Take)
            return 0;

        int count = 0;
        int remaining = Take > 0 ? (int)Math.Min(matches.Length, Take - TotalMatched) : matches.Length;
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
            int read = DrivingMatch.Fill(batch[..batchSize]);
            TreeScanTicks += Stopwatch.GetTimestamp() - t0;

            if (read == 0)
            {
                StoppedReason ??= "TreeExhausted";
                break;
            }
            TreeEntriesScanned += read;

            var sorted = sortedIds[..read];
            batch[..read].CopyTo(sorted);
            for (int j = 0; j < read; j++)
                indices[j] = j;
            sorted.Sort(indices[..read]);

            passed[..read].Clear();

            long t1 = Stopwatch.GetTimestamp();

            var locs = containerLocs[..read];
            Searcher.ResolveEntryLocations(sorted, locs);

            var spans = containerSpans[..read];
            Container.GetAll(Llt, locs, spans, -1, Llt.PageLocator);

            Searcher.InitializeSpecialTermsMarkers();

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

                    if (EmittedBitmap.Contains(entryId))
                        continue;

                    if (locs[s] == -1 || spans[s].Address == null)
                    {
                        EntriesRejected++;
                        continue;
                    }

                    readersArr[packed] = new EntryTermsReader(Llt,
                        Searcher.NullTermsMarkers, Searcher.NonExistingTermsMarkers,
                        spans[s].Address, spans[s].Length, Searcher.DictionaryId, Searcher.VectorFieldsMarkers, null);
                    pIds[packed] = entryId;
                    pIdxs[packed] = origIdx;
                    packed++;
                }

                int matched = precompiledDelegate(this,
                    readersArr.AsSpan(0, packed),
                    packedIds[..packed],
                    packedOrigIdx[..packed]);

                EntriesRejected += packed - matched;

                for (int k = 0; k < matched; k++)
                    passed[pIdxs[k]] = true;
            }
            finally
            {
                ArrayPool<EntryTermsReader>.Shared.Return(readersArr, clearArray: true);
            }
            EntryScanTicks += Stopwatch.GetTimestamp() - t1;

            for (int i = 0; i < read && count < remaining; i++)
            {
                if (passed[i])
                {
                    long id = batch[i];
                    EmittedBitmap.Add(id);
                    EntriesPassedFilter++;
                    matches[count++] = id;
                }
            }
        }

        if (Take > 0 && TotalMatched + count >= Take)
            StoppedReason ??= $"_take({Take})";

        TotalMatched += count;
        return count;
    }

    long[] IPredicateEvaluationContext.ResidualLongParams => longParams;
    double[] IPredicateEvaluationContext.ResidualDoubleParams => doubleParams;
    Slice[] IPredicateEvaluationContext.ResidualSliceParams => sliceParams;
    long[] IPredicateEvaluationContext.ResidualFieldRootPages => fieldRootPages;
}
