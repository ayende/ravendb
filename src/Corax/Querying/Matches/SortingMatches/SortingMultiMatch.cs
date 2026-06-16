using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Threading;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.SortingMatches.Meta;
using Corax.Utils;
using Corax.Utils.Spatial;
using Sparrow;
using Sparrow.Server;
using Voron.Util;

namespace Corax.Querying.Matches.SortingMatches;

[DebuggerDisplay("{DebugView,nq}")]
public sealed unsafe partial class SortingMultiMatch<TInner> : SortingMultiMatch
    where TInner : IQueryMatch
{
    private const int NextComparerOffset = 3;
    private readonly IndexSearcher _searcher;
    private TInner _inner;
    private readonly OrderMetadata[] _orderMetadata;
    private readonly bool _nullFirst;
    private readonly delegate*<SortingMultiMatch<TInner>, Span<long>, int> _fillFunc;
    private readonly IEntryComparer[] _nextComparers;
    private readonly int _take;
    private readonly CancellationToken _token;
    private const int NotStarted = -1;
        
    private ByteStringContext<ByteStringMemoryCache>.InternalScope _entriesBufferScope;

    private ContextBoundNativeList<long> _results;

    private SortingDataTransfer _sortingDataTransfer;
    private ContextBoundNativeList<SpatialResult> _distancesResults;
    private ContextBoundNativeList<float> _scoresResults;
    
    // This is data persisted for holding score from secondary comparer.
    private UnmanagedSpan<float> _secondaryScoreBuffer;
    private IDisposable _scoreBufferHandler;

    // Secondary/tertiary lookup comparers pre-resolve their per-entry sort values once over the whole batch
    // (amortized, sequential), instead of doing two B-tree lookups per pairwise Compare. Those buffers must
    // outlive Init (they back the heap's tie-break comparisons inside SortBatch), so their allocation scopes
    // are parked here and released with the match.
    private List<IDisposable> _secondaryResolveScopes;

    private int _alreadyReadIdx;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void TrackSecondaryResolveScope(IDisposable scope)
    {
        (_secondaryResolveScopes ??= new List<IDisposable>()).Add(scope);
    }



    public SortingMultiMatch(IndexSearcher searcher, in TInner inner, OrderMetadata[] orderMetadata, NullsSortMode defaultNullsSortMode, int take = -1, in CancellationToken token = default)
    {
        _searcher = searcher;
        _inner = inner;
        _orderMetadata = orderMetadata;
        _nullFirst = defaultNullsSortMode == NullsSortMode.NullsSmallest;
        _take = take;
        _token = token;
        _alreadyReadIdx = 0;
        _results = new ContextBoundNativeList<long>(searcher.Allocator);
        TotalResults = NotStarted;
        _fillFunc = SortBy(orderMetadata);
        
        _nextComparers = orderMetadata.Length > NextComparerOffset 
            ? HandleNextComparers() 
            : Array.Empty<IEntryComparer>();

        IEntryComparer[] HandleNextComparers()
        {
            var nextComparers = new IEntryComparer[orderMetadata.Length - NextComparerOffset];
            for (int metadataId = NextComparerOffset; metadataId < orderMetadata.Length; ++metadataId)
            {
                nextComparers[metadataId - NextComparerOffset] = (orderMetadata[metadataId].Ascending, orderMetadata[metadataId].FieldType) switch
                {
                    
                    (true, MatchCompareFieldType.Alphanumeric) => new EntryComparerByTermAlphaNumeric(),
                    (false, MatchCompareFieldType.Alphanumeric) => new Descending<EntryComparerByTermAlphaNumeric>(),

                    (true, MatchCompareFieldType.Floating) => new EntryComparerByDouble(),
                    (false, MatchCompareFieldType.Floating) => new Descending<EntryComparerByDouble>(),
                    
                    (true, MatchCompareFieldType.Integer) => new EntryComparerByLong(),
                    (false, MatchCompareFieldType.Integer) => new Descending<EntryComparerByLong>(),
                    
                    (true, MatchCompareFieldType.Sequence) => new EntryComparerByTerm(),
                    (false, MatchCompareFieldType.Sequence) => new Descending<EntryComparerByTerm>(),
                    
                    (true, MatchCompareFieldType.Spatial) => new EntryComparerBySpatial(),
                    (false, MatchCompareFieldType.Spatial) => new Descending<EntryComparerBySpatial>(),
                    
                    (true, MatchCompareFieldType.Score) => new EntryComparerByScore(),
                    (false, MatchCompareFieldType.Score) => new Descending<EntryComparerByScore>(),
                    
                    _ => throw new NotSupportedException($"Ascending: {orderMetadata[metadataId].Ascending} | FieldType: {orderMetadata[metadataId].FieldType}.")
                };
            }

            return nextComparers;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal bool NullIsSmallest(int comparerId)
    {
        var perField = _orderMetadata[comparerId].NullsSortMode;
        if (perField != null)
            return perField.Value == NullsSortMode.NullsSmallest;
        return _nullFirst;
    }

    public override void SetSortingDataTransfer(in SortingDataTransfer sortingDataTransfer)
    {
        _sortingDataTransfer = sortingDataTransfer;
        if (sortingDataTransfer.IncludeDistances)
            _distancesResults = new(_searcher.Allocator);
        if (sortingDataTransfer.IncludeScores)
            _scoresResults = new(_searcher.Allocator);
    }

    private static int Fill<TComparer1, TComparer2, TComparer3>(SortingMultiMatch<TInner> match, Span<long> matches)
        where TComparer1 : struct, IEntryComparer, IComparer<UnmanagedSpan> 
        where TComparer2 : struct, IEntryComparer, IComparer<int>, IComparer<UnmanagedSpan> 
        where TComparer3 : struct, IEntryComparer, IComparer<int>, IComparer<UnmanagedSpan>  
    
    {
        // This method should also be re-entrant for the case where we have already pre-sorted everything and
        // we will just need to acquire via pages the totality of the results.
        if (match.TotalResults == NotStarted)
        {
            match._token.ThrowIfCancellationRequested();

            if (match._inner is IBitmapQueryMatch bitmapMatch)
            {
                // First access to Count runs the inner compiled pipeline (the AND/OR/scan that builds the
                // candidate bitmap). Deliberately left outside the sort timer below: that execution is timed
                // onto the inner CompiledQuery node's per-op telemetry, so charging it to the sort too would
                // double-count the query. Everything after this line is sort-specific work.
                match.TotalResults = bitmapMatch.Count;
                if (match.TotalResults == 0)
                    return 0;

                long sortStart = Stopwatch.GetTimestamp();
                int total = (int)match.TotalResults;
                var scope = match._searcher.Allocator.Allocate(total * sizeof(long), out var bs);
                var allMatches = new Span<long>(bs.Ptr, total);
                int filled = bitmapMatch.Fill(allMatches);
                SortResults<TComparer1, TComparer2, TComparer3>(match, allMatches[..filled]);
                scope.Dispose();
                match.SortingTimeInTicks += Stopwatch.GetTimestamp() - sortStart;
            }
            else
            {
                // Non-bitmap path: drain via Fill to preserve match-specific state (scores, etc.).
                // Draining the inner match (Count + the Fill loop) is the inner query's execution, not sort
                // work, so it is deliberately left untimed — only the SortResults call below is charged to the sort.
                var count = match._inner.Count;
                int bufferSize = count is > 0 and < (1024 * 1024) ? (int)count : 4096;
                var scope = match._searcher.Allocator.Allocate(bufferSize * sizeof(long), out var bs);
                var allMatches = new Span<long>(bs.Ptr, bufferSize);
                int filled = 0;
                int r;
                while ((r = match._inner.Fill(allMatches[filled..])) > 0)
                {
                    filled += r;
                    if (filled >= allMatches.Length)
                    {
                        match._searcher.Allocator.GrowAllocation(ref bs, ref scope, allMatches.Length * sizeof(long));
                        allMatches = new Span<long>(bs.Ptr, bs.Length / sizeof(long));
                    }
                }
                match.TotalResults = filled;
                if (match.TotalResults == 0)
                {
                    scope.Dispose();
                    return 0;
                }
                long sortStart = Stopwatch.GetTimestamp();
                SortResults<TComparer1, TComparer2, TComparer3>(match, allMatches[..filled]);
                match.SortingTimeInTicks += Stopwatch.GetTimestamp() - sortStart;
                scope.Dispose();
            }
        }

        var read = match._results.CopyTo(matches, match._alreadyReadIdx);
        match._distancesResults.CopyTo(match._sortingDataTransfer.DistancesBuffer, match._alreadyReadIdx, read);
        match._scoresResults.CopyTo(match._sortingDataTransfer.ScoresBuffer, match._alreadyReadIdx, read);
        
        if (read != 0)
        {
            match._alreadyReadIdx += read;
            return read;
        }

        match._alreadyReadIdx = 0;

        match._results.Dispose();
        match._scoresResults.Dispose();
        match._distancesResults.Dispose();
        match._entriesBufferScope.Dispose();
        
        return 0;
    }
    
    private static void SortResults<TComparer1, TComparer2, TComparer3>(SortingMultiMatch<TInner> match, Span<long> matches) 
        where TComparer1 : struct,  IEntryComparer, IComparer<UnmanagedSpan>
        where TComparer2 : struct,  IEntryComparer, IComparer<int>, IComparer<UnmanagedSpan>
        where TComparer3 : struct,  IEntryComparer, IComparer<int>, IComparer<UnmanagedSpan>
    {
        var llt = match._searcher.Transaction.LowLevelTransaction;
        var allocator = match._searcher.Allocator;
        var take = matches.Length;
        //We supports take == -1 when it means "sort all", so then take will be size of result from TInner
        // var take = Math.Min(match._take, matches.Length);
        // take = take < 0 ? matches.Length : take;
        
        var sizeToAllocate = take * (sizeof(long) + sizeof(UnmanagedSpan));
        //OrderBySpatial relay on this order of data. If you change it please review Spatial ordering to ensure that everything works fine. [[ids], [terms], [spatial_distances]]
        if (match._sortingDataTransfer.IncludeDistances)
            sizeToAllocate += take * sizeof(SpatialResult);
        
        using var bufScope = allocator.Allocate(sizeToAllocate, out ByteString bs);
        Span<long> matchesTermIds = new(bs.Ptr, take);
        UnmanagedSpan* termsPtr = (UnmanagedSpan*)(bs.Ptr + take * sizeof(long));

        TComparer1 entryComparer = new();
        entryComparer.Init(match, default, 0);
        var pageCache = llt.PageLocator;
        fixed (long* ptrBatchResults = matches)
        {
            var resultsPtr = new UnmanagedSpan<long>(ptrBatchResults, sizeof(long)* matches.Length);
            var comp2 = new TComparer2();
            comp2.Init(match, resultsPtr, 1);
            var comp3 = new TComparer3();
            comp3.Init(match, resultsPtr, 2);
            
            for (int comparerId = 0; comparerId < match._nextComparers.Length; comparerId++)
            {
                IEntryComparer add = match._nextComparers[comparerId];
                add.Init(match, resultsPtr, NextComparerOffset + comparerId);
            }

            entryComparer.SortBatch(match, llt, pageCache, resultsPtr, matchesTermIds, termsPtr, match._orderMetadata, comp2, comp3);
        }
    }


    public override long Count => _inner.Count;

    public override QueryCountConfidence Confidence => throw new NotSupportedException();

    public override bool IsBoosting => _inner.IsBoosting || _orderMetadata[0].FieldType == MatchCompareFieldType.Score;

    public override int AndWith(Span<long> buffer, int matches)
    {
        throw new NotSupportedException($"{nameof(SortingMultiMatch<TInner>)} does not support the operation of {nameof(AndWith)}.");
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override int Fill(Span<long> matches)
    {
        return _fillFunc(this, matches);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override void Score(Span<long> matches, Span<float> scores, float boostFactor)
    {
    }

    public override QueryInspectionNode Inspect()
    {
        var parameters = new Dictionary<string, string>()
        {
            {Constants.QueryInspectionNode.IsBoosting, IsBoosting.ToString()},
            {Constants.QueryInspectionNode.Count, "0"},
            {Constants.QueryInspectionNode.CountConfidence, QueryCountConfidence.Low.ToString()},
        };

        for (int cmpId = 0; cmpId < _orderMetadata.Length; ++cmpId)
        {
            ref var order = ref _orderMetadata[cmpId];
            var prefix = Constants.QueryInspectionNode.Comparer + cmpId.ToString() + "_";

            parameters.Add(prefix+Constants.QueryInspectionNode.FieldName, order.Field.FieldName.ToString());
            parameters.Add(prefix+Constants.QueryInspectionNode.Ascending, order.Ascending.ToString());
            parameters.Add(prefix+Constants.QueryInspectionNode.FieldType, order.FieldType.ToString());
            
            switch (order.FieldType)
            {
                case MatchCompareFieldType.Spatial:
                    parameters.Add(Constants.QueryInspectionNode.Point, order.Point.ToString());
                    parameters.Add(Constants.QueryInspectionNode.Round, order.Round.ToString(CultureInfo.InvariantCulture));
                    parameters.Add(Constants.QueryInspectionNode.Units, order.Units.ToString());
                    break;
                case MatchCompareFieldType.Random:
                    parameters.Add(Constants.QueryInspectionNode.RandomSeed, order.RandomSeed.ToString());
                    break;
            }
        }
        
        // Surface the sort's own runtime cost. The inner bitmap pipeline times its ops onto the child
        // CompiledQuery node; the multi-comparer sort runs above it and is otherwise absent from include
        // timings(). SortingMultiMatch always materializes the candidate set and heap-sorts it (no
        // index-order streaming variant exists for multi-key sorts), so the strategy is constant.
        if (SortingTimeInTicks > 0)
        {
            parameters["Strategy"] = CoraxSortingStrategy.InMemorySort.ToString();
            parameters["Ms"] = (SortingTimeInTicks / (Stopwatch.Frequency / 1000.0)).ToString("F3", CultureInfo.InvariantCulture);
        }

        // The sort wrapper sits above the bitmap pipeline, so surface its own in/out (see SortingMatch.Inspect):
        // Incoming is the full candidate set ranked, Output is what the page receives — capped by _take when set.
        if (TotalResults >= 0)
        {
            parameters["Incoming"] = TotalResults.ToString("N0");
            long output = _take >= 0 ? Math.Min(_take, TotalResults) : TotalResults;
            parameters["Output"] = output.ToString("N0");
        }

        return new QueryInspectionNode($"{nameof(SortingMultiMatch)}",
            children: new List<QueryInspectionNode> { _inner.Inspect()},
            parameters: parameters);
    }

    public override void Dispose()
    {
        _results.Dispose();
        _entriesBufferScope.Dispose();
        _scoresResults.Dispose();
        _distancesResults.Dispose();
        _scoreBufferHandler?.Dispose();
        if (_secondaryResolveScopes != null)
        {
            foreach (var scope in _secondaryResolveScopes)
                scope.Dispose();
        }
        (_inner as IDisposable)?.Dispose();
    }

    string DebugView => Inspect().ToString();
}
