using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Threading;
using Corax.Indexing;
using Corax.Mappings;
using Corax.Querying.Matches.Meta;
using Voron.Data.RoaringBitmaps;
using Corax.Querying.Matches.SortingMatches.Meta;
using Corax.Utils;
using Corax.Utils.Spatial;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Compression;
using Sparrow.Server;
using Voron;
using Voron.Data.CompactTrees;
using Voron.Data.Containers;
using Voron.Data.Lookups;
using Voron.Data.PostingLists;
using Voron.Impl;
using Voron.Util;
using Voron.Util.PFor;

namespace Corax.Querying.Matches.SortingMatches;

[DebuggerDisplay("{DebugView,nq}")]
public sealed unsafe partial class SortingMatch<TInner> : SortingMatch
    where TInner : IQueryMatch
{
    private readonly IndexSearcher _searcher;
    private TInner _inner;
    private readonly OrderMetadata _orderMetadata;
    private readonly CancellationToken _cancellationToken;
    private readonly bool _nullFirst;
    private readonly delegate*<SortingMatch<TInner>, Span<long>, int> _fillFunc;
    private readonly int _take;
    private const int NotStarted = -1;

    /// <summary>UTF-8 byte buffer size for stackalloc encode/compare in sort-hint seek and SliceEqualsUtf8.
    /// Strings longer than this fall back to heap allocation.</summary>
    private const int Utf8StackAllocThreshold = 256;
    private ByteStringContext<ByteStringMemoryCache>.InternalScope _entriesBufferScope;

    private ContextBoundNativeList<long> _results;
    private ContextBoundNativeList<SpatialResult> _distancesResults;
    private ContextBoundNativeList<float> _scoresResults;
    private int _alreadyReadIdx;


    private SortingDataTransfer _sortingDataTransfer;

    /// <summary>Uniform-distribution scan estimate the cost gate computed when it chose IndexOrderStreaming,
    /// retained so the streaming run can feed (actual EntriesStreamed / this estimate) into the plan's
    /// scan-inflation EWMA. Zero when streaming wasn't gated (forced path / non-CompiledQueryMatch inner).</summary>
    private double _rawStreamScanEstimate;

    public override DuplicatesOccurrence DuplicatesOccurrenceStatus => DuplicatesOccurrence.NotPossible;

    public SortingMatch(IndexSearcher searcher, in TInner inner, OrderMetadata orderMetadata, in CancellationToken cancellationToken, NullsSortMode defaultNullsSortMode, int take = -1)
    {
        _searcher = searcher;
        _inner = inner;
        _orderMetadata = orderMetadata;
        _cancellationToken = cancellationToken;
        _nullFirst = (_orderMetadata.NullsSortMode ?? defaultNullsSortMode) == NullsSortMode.NullsSmallest;
        _take = take;
        _alreadyReadIdx = 0;
        _results = new ContextBoundNativeList<long>(searcher.Allocator);
        TotalResults = NotStarted;

        if (_orderMetadata.HasBoost)
        {
            _fillFunc = SortBy<EntryComparerByScore, NoIterationOptimization, NoIterationOptimization>(orderMetadata);
        }
        else
        {
            _fillFunc = _orderMetadata.FieldType switch
            {
                MatchCompareFieldType.Sequence => SortBy<EntryComparerByTerm, Lookup<CompactTree.CompactKeyLookup>.ForwardIterator,  Lookup<CompactTree.CompactKeyLookup>.BackwardIterator>(orderMetadata),
                MatchCompareFieldType.Alphanumeric => SortBy<EntryComparerByTermAlphaNumeric, NoIterationOptimization, NoIterationOptimization>(orderMetadata),
                MatchCompareFieldType.Integer => SortBy<EntryComparerByLong, Lookup<Int64LookupKey>.ForwardIterator, Lookup<Int64LookupKey>.BackwardIterator>(orderMetadata),
                MatchCompareFieldType.Floating => SortBy<EntryComparerByDouble,  Lookup<DoubleLookupKey>.ForwardIterator, Lookup<DoubleLookupKey>.BackwardIterator>(orderMetadata),
                MatchCompareFieldType.Spatial => SortBy<EntryComparerBySpatial, NoIterationOptimization, NoIterationOptimization>(orderMetadata),
                MatchCompareFieldType.Random => SortBy<EntryComparerByTerm,  RandomDirection, RandomDirection>(orderMetadata),
                _ => throw new ArgumentOutOfRangeException(_orderMetadata.FieldType.ToString())
            };
        }
    }
    private struct RandomDirection : ILookupIterator
    {
        
        public bool IsForward => throw new NotSupportedException($"{nameof(RandomDirection)} has no direction and should not be used in parts of code where it is required.");

        public void Init<T>(T parent) => throw new NotSupportedException();

        public void Reset() => throw new NotSupportedException();

        public int Fill(Span<long> results, long lastId, bool includeMax) => throw new NotSupportedException();
        
        public bool Skip(long count) => throw new NotSupportedException();

        public bool MoveNext(out long value) => throw new NotSupportedException();

        public bool MoveNext<TLookupKey>(out TLookupKey key, out long value, out bool hasPreviousValue) => throw new NotSupportedException();

        public void Seek<TLookupKey>(TLookupKey key) => throw new NotSupportedException();
    }
    
    private struct NoIterationOptimization : ILookupIterator
    {
        public bool IsForward => throw new NotSupportedException($"{nameof(NoIterationOptimization)} has no direction and should not be used in parts of code where it is required.");

        
        public void Init<T>(T parent) => throw new NotSupportedException();

        public void Reset() => throw new NotSupportedException();

        public int Fill(Span<long> results, long lastId = long.MaxValue, bool includeMax = true) => throw new NotSupportedException();
        
        public bool Skip(long count) => throw new NotSupportedException();

        public bool MoveNext(out long value) => throw new NotSupportedException();

        public bool MoveNext<TLookupKey>(out TLookupKey key, out long value, out bool hasPreviousValue) => throw new NotSupportedException();

        public void Seek<TLookupKey>(TLookupKey key) => throw new NotSupportedException();
    }
        
    private static delegate*<SortingMatch<TInner>, Span<long>, int> SortBy<TEntryComparer,TFwdIt,TBackIt>(OrderMetadata metadata)
        where TEntryComparer : struct, IEntryComparer, IComparer<UnmanagedSpan>
        where TFwdIt : struct,  ILookupIterator
        where TBackIt : struct, ILookupIterator
    {
        if (metadata.Ascending)
        {
            return &Fill<TEntryComparer, TFwdIt>;
        }

        return &Fill<Descending<TEntryComparer>, TBackIt>;
    }


    private static int Fill<TEntryComparer, TDirection>(SortingMatch<TInner> match, Span<long> matches)
        where TEntryComparer : struct, IEntryComparer, IComparer<UnmanagedSpan>
        where TDirection : struct, ILookupIterator
    {
        // This method should also be re-entrant for the case where we have already pre-sorted everything and
        // we will just need to acquire via pages the totality of the results.
        if (match.TotalResults == NotStarted)
        {
            if (match._inner is IBitmapQueryMatch bitmapMatch)
            {
                match.TotalResults = bitmapMatch.Count;
                if (match.TotalResults == 0)
                    return 0;

                if (typeof(TDirection) == typeof(RandomDirection))
                {
                    match.SortStrategy = CoraxSortingStrategy.RandomOrder;
                    SampleRandomOrder(match, bitmapMatch);
                }
                else if (typeof(TDirection) == typeof(NoIterationOptimization) || match._orderMetadata.MayHaveMissingEntries)
                {
                    // Score/spatial/alphanumeric: no index to walk, must materialize + heap sort.
                    // Also taken when MayHaveMissingEntries is set (e.g. dynamic CreateField sort fields):
                    // IndexOrderStreaming only walks tree terms + null/nonExisting posting lists, so
                    // docs that didn't emit the field would be silently dropped. InMemorySort drains
                    // the entire bitmap and uses the comparer's missing-value sentinel for those docs.
                    // A $rvn_corax_sort pin can't override this branch: it guards correctness, not cost.
                    match.SortStrategy = CoraxSortingStrategy.InMemorySort;
                    SortInMemory<TEntryComparer>(match, bitmapMatch);
                }
                else if (ShouldUseIndexOrderStreaming(match, bitmapMatch))
                {
                    // Cost gate chose streaming; an InMemorySort pin overrides it (forces the bounded sort).
                    if (match.ForcedStrategy == CoraxSortingStrategy.InMemorySort)
                    {
                        match.SortStrategy = CoraxSortingStrategy.InMemorySort;
                        SortInMemory<TEntryComparer>(match, bitmapMatch);
                    }
                    else
                    {
                        match.SortStrategy = CoraxSortingStrategy.IndexOrderStreaming;
                        StreamInIndexOrder<TEntryComparer, TDirection>(match, bitmapMatch);
                    }
                }
                else if (match.ForcedStrategy == CoraxSortingStrategy.IndexOrderStreaming)
                {
                    // Cost gate rejected streaming, but the query explicitly pinned it: walk the index anyway
                    // (and, being forced, with the over-scan bailout suppressed). Used to exercise the
                    // streaming path's ordering semantics regardless of the candidate distribution.
                    match.SortStrategy = CoraxSortingStrategy.IndexOrderStreaming;
                    StreamInIndexOrder<TEntryComparer, TDirection>(match, bitmapMatch);
                }
                else
                {
                    // Cost model rejected the streaming scan: the candidate set is too sparse in the
                    // sort index for early termination to pay off, so walking the index would read far
                    // more entries than the candidate set itself. Materialize the candidates and sort.
                    match.SortStrategy = CoraxSortingStrategy.InMemorySort;
                    SortInMemory<TEntryComparer>(match, bitmapMatch);
                }
            }
            else
            {
                // Non-bitmap path (VectorSearchMatch, PostFilterMatch, scoring matches, etc.)
                // Must drain via Fill to preserve match-specific state (vector distances, scores).
                SortComputedResults<TEntryComparer>(match);
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
        match._entriesBufferScope.Dispose();
        match._scoresResults.Dispose();
        match._distancesResults.Dispose();

        return 0;
    }

    /// <summary>
    /// Cost-based choice between the two indexed-sort strategies once a real iterable index exists
    /// (score/spatial/alphanumeric and MayHaveMissingEntries are already excluded by the caller).
    ///
    /// <para><b>IndexOrderStreaming</b> walks the whole sort index in order, intersecting each batch
    /// against the candidate bitmap and stopping as soon as <c>take</c> results are collected. Its
    /// cost is the number of index entries scanned before that happens — roughly
    /// <c>take · indexSize / candidates</c> under a uniform-distribution assumption (and the FULL index
    /// when there is no LIMIT, since it can never stop early). <b>InMemorySort</b> instead materializes
    /// the candidate set (cost ∝ <c>candidates</c>) and heap-sorts it.</para>
    ///
    /// <para>So streaming only wins when the estimated scan is smaller than the candidate set itself,
    /// i.e. the candidates are dense in the index and the limit is small. For a selective WHERE with a
    /// large/absent LIMIT the streaming scan degenerates into a near-full index walk to surface a handful
    /// of matches — the case this guard steers to InMemorySort. <see cref="IndexSearcher.NumberOfEntries"/>
    /// is the cheap proxy for the reader's full-scan size (terms + null/non-existing posting lists ≈ the
    /// whole index); it slightly overestimates for sparse sort fields, which only biases toward the
    /// always-bounded InMemorySort.</para>
    /// </summary>
    private static bool ShouldUseIndexOrderStreaming(SortingMatch<TInner> match, IBitmapQueryMatch bitmapMatch)
    {
        long candidates = match.TotalResults; // == bitmapMatch.Count, already set by the caller
        long indexSize = match._searcher.NumberOfEntries;

        // No LIMIT (or a limit that can't cut below the candidate count): streaming can never terminate
        // early, so it walks the entire index. InMemorySort touches only the candidates (a subset of the
        // index), so it is never worse here.
        if (match._take < 0 || match._take >= candidates)
            return false;

        // Expected entries scanned to collect `take` matches, assuming candidates are spread uniformly
        // across the index. Computed in double to avoid overflow on the multiply.
        double estimatedScan = (double)match._take * indexSize / candidates;
        match._rawStreamScanEstimate = estimatedScan; // retained for the EWMA update on completion/bailout

        if (bitmapMatch is CompiledQueryMatch { CompiledPlan.StreamScanInflation: { } scanInflation })
        {
            // Correct the uniform-distribution estimate by what this plan has actually scanned in past
            // streaming runs: clustered candidates push (actual scanned / estimate) above 1, so a plan that
            // kept over-scanning (and bailing) inflates the estimate here and stops choosing streaming. The
            // factor is 0 until the plan has streamed at least once (no history -> trust the raw estimate).
            var inflation = scanInflation.GetRate();
            if(inflation > 0)
                estimatedScan *= inflation;
        }

        return estimatedScan < candidates;
    }

    private static void SampleRandomOrder(SortingMatch<TInner> match, IBitmapQueryMatch bitmapMatch)
    {
        var random = new Random(match._orderMetadata.RandomSeed);
        int take = match._take;
        ref var bitmap = ref bitmapMatch.BitmapState;
        long totalCount = bitmapMatch.Count;

        if (totalCount == 0)
            return;

        if (take < 0)
        {
            // No LIMIT: materialize the whole bitmap in one Fill, then Fisher-Yates shuffle.
            match._results.EnsureCapacityFor((int)totalCount);
            Span<long> bulk = match._results.ToFullCapacitySpan();
            int filled = bitmapMatch.Fill(bulk);
            match._results.Count = filled;

            for (int i = filled - 1; i > 0; i--)
            {
                int j = random.Next(i + 1);
                (match._results[i], match._results[j]) = (match._results[j], match._results[i]);
            }
        }
        else
        {
            // With LIMIT k: pick k random ranks from [0, totalCount), deduplicated,
            // then resolve all ranks to entry IDs in a single bulk Select call —
            // one container walk instead of one per rank.
            var allocator = match._searcher.Allocator;
            int k = (int)Math.Min(take, totalCount);
            match._results.EnsureCapacityFor(k);

            // Generate k unique random ranks using Floyd's algorithm (O(k), no rejection).
            var selected = new HashSet<long>(k);
            for (long i = totalCount - k; i < totalCount; i++)
            {
                long r = random.NextInt64(i + 1);
                if (selected.Add(r) == false)
                    selected.Add(i);
            }

            // Materialize ranks into a contiguous buffer; results land directly in _results.
            using var ranksList = new ContextBoundNativeList<long>(allocator, k);
            foreach (long rank in selected)
                ranksList.AddUnsafe(rank);

            // Floyd's only emits ranks in [0, totalCount), so every result is valid.
            match._results.Count = k;
            bitmap.Select(allocator, ranksList.ToSpan(), match._results.ToSpan());
        }
    }
    
    internal unsafe struct SortedIndexReader<TDirection> : IDisposable
        where TDirection : struct, ILookupIterator
    {
        private PostingList.Iterator _postListIt;
        private FastPForBufferedReader _smallListReader;
        private TDirection _termsIt;
        private readonly long _min;
        private readonly long _max;
        private readonly long _nonExistingPostingListId;
        private readonly long _nullPostingListId;

        private readonly bool _nullFirst;
        private readonly bool _isForward;
        private readonly IndexSearcher _searcher;
        private readonly LowLevelTransaction _llt;

        private const int BufferSize = 1024;
        private readonly long* _itBuffer;
        private readonly UnmanagedSpan* _containerItems;
        private int _bufferIdx;
        private int _bufferCount;
        private int _smallPostingListIndex;
        private ContextBoundNativeList<long> _smallPostListIds;
        private ByteStringContext<ByteStringMemoryCache>.InternalScope _itBufferScope, _containerItemsScope;
        private readonly PageLocator _pageLocator;
        private bool _hasSmallListReader;
        private bool _nonExistingPostingListRead;
        private bool _nullPostingListRead;

        /// <summary>The iterator <paramref name="it"/> is assumed to be already positioned by the caller
        /// (caller is responsible for Reset + optional Seek).</summary>
        public SortedIndexReader(LowLevelTransaction llt, IndexSearcher searcher, TDirection it, FieldMetadata metadata, long min, long max, bool nullFirst, bool isForward)
        {
            _termsIt = it;
            _min = min;
            _max = max;
            _nullFirst = nullFirst;
            _isForward = isForward;
            _llt = llt;
            _searcher = searcher;
            _postListIt = default;
            _smallListReader = default;
            _smallPostListIds = new ContextBoundNativeList<long>(llt.Allocator,BufferSize);
            _bufferCount = _bufferIdx = 0;
            _itBufferScope = llt.Allocator.Allocate(BufferSize * sizeof(long), out ByteString bs);
            _itBuffer = (long*)bs.Ptr;
            _containerItemsScope = llt.Allocator.Allocate(BufferSize * sizeof(UnmanagedSpan), out bs);
            _containerItems = (UnmanagedSpan*)bs.Ptr;
            _pageLocator = llt.PageLocator;

            _nonExistingPostingListRead = searcher.TryGetPostingListForNonExisting(metadata, out _nonExistingPostingListId) == false;
            _nullPostingListRead = searcher.TryGetPostingListForNull(metadata, out _nullPostingListId) == false;
        }


        public int Read(Span<long> sortedIds)
        {
            fixed (long* pSortedIds = sortedIds)
            {
                int currentIdx = 0;
                // here we resume the *previous* operation
                if (_hasSmallListReader)
                {
                    ReadSmallPostingList(pSortedIds, sortedIds.Length, ref currentIdx);
                }
                else if (_postListIt.IsValid)
                {
                    ReadLargePostingList(sortedIds, ref currentIdx);
                }

                while (currentIdx < sortedIds.Length)
                {
                    if (_bufferIdx == _bufferCount)
                    {
                        RefillBuffers();
                        if (_bufferCount == 0)
                            break;
                    }

                    var postingListId = _itBuffer[_bufferIdx++];
                    var termType = (TermIdMask)postingListId & TermIdMask.EnsureIsSingleMask;
                    switch (termType)
                    {
                        case TermIdMask.Single:
                            long entryId = (long)EntryIdEncodings.GetContainerId(postingListId);
                            if(entryId >= _min && entryId <= _max)
                                sortedIds[currentIdx++] = entryId;
                            break;
                        case TermIdMask.SmallPostingList:
                            var item = _containerItems[_smallPostingListIndex++];
                            _ = VariableSizeEncoding.Read<int>(item.Address, out var offset); // discard count here
                            var start = FastPForDecoder.ReadStart(item.Address + offset);
                            if((long)EntryIdEncodings.DecodeAndDiscardFrequency(start) > _max)
                                continue;
                            if (_smallListReader.WasInitialized == false)
                            {
                                _smallListReader = new FastPForBufferedReader(_llt.Allocator);
                            }

                            _hasSmallListReader = true;
                            _smallListReader.Init(item.Address + offset, item.Length - offset);
                            ReadSmallPostingList(pSortedIds, sortedIds.Length, ref currentIdx);
                            break;
                        case TermIdMask.PostingList:
                            var postingList = _searcher.GetPostingList(postingListId);
                            _postListIt = postingList.Iterate();
                            _postListIt.Seek(_min);
                            ReadLargePostingList(sortedIds, ref currentIdx);
                            break;
                        default:
                            throw new OutOfMemoryException(termType.ToString());
                    }
                }

                return currentIdx;
            }
        }

        private void RefillBuffers()
        {
            _smallPostListIds.Clear();
            _bufferIdx = 0;
            _bufferCount = 0;
            
            bool nullsFirst = _isForward ? _nullFirst : !_nullFirst;
            var buffer = new Span<long>(_itBuffer, BufferSize);
            if (nullsFirst)
                LoadNonExistingAndNullIntoBuffer(buffer);
            
            
            _bufferCount += _termsIt.Fill(buffer.Slice(_bufferCount));
            if (_bufferCount == 0)
            {
                if (nullsFirst || (_nonExistingPostingListRead && _nullPostingListRead))
                    return;
                
                LoadNonExistingAndNullIntoBuffer(buffer);
            }
            
            for (int i = 0; i < _bufferCount; i++)
            {
                var termType = (TermIdMask)_itBuffer[i] & TermIdMask.EnsureIsSingleMask;
                if (termType == TermIdMask.SmallPostingList)
                {
                    var smallSetId = EntryIdEncodings.GetContainerId(_itBuffer[i]);
                    _smallPostListIds.Add((long)smallSetId);
                }
            }

            _smallPostingListIndex = 0;
            if (_smallPostListIds.Count == 0)
                return;

            Container.GetAll(_llt, _smallPostListIds.ToSpan(), new Span<UnmanagedSpan>(_containerItems, _smallPostListIds.Count), long.MinValue, _pageLocator);

            
        }
        
        void LoadNonExistingAndNullIntoBuffer(Span<long> buffer)
        {
            // nullFirst:  non-existing < null < normal values
            // nullLast:   normal values < null < non-existing
            bool nullsFirst = _isForward ? _nullFirst : !_nullFirst;
            if (nullsFirst)
            {
                LoadNonExistingIntoBuffer(buffer);
                LoadNullIntoBuffer(buffer);
            }
            else
            {
                LoadNullIntoBuffer(buffer);
                LoadNonExistingIntoBuffer(buffer);
            }
        }

        void LoadNonExistingIntoBuffer(Span<long> buffer)
        {
            if (_nonExistingPostingListRead == false)
            {
                buffer[_bufferCount] = _nonExistingPostingListId;
                _nonExistingPostingListRead = true;
                _bufferCount += 1;
            }
        }

        void LoadNullIntoBuffer(Span<long> buffer)
        {
            if (_nullPostingListRead == false)
            {
                buffer[_bufferCount] = _nullPostingListId;
                _nullPostingListRead = true;
                _bufferCount += 1;
            }
        }

        private void ReadLargePostingList(Span<long> sortedIds, ref int currentIdx)
        {
            if (_postListIt.Fill(sortedIds[currentIdx..], out var read) == false || (long)EntryIdEncodings.DecodeAndDiscardFrequency(sortedIds[currentIdx + read - 1]) > _max)
                _postListIt = default;

            EntryIdEncodings.DecodeAndDiscardFrequency(sortedIds.Slice(currentIdx), read);
            currentIdx += read;
        }

        private void ReadSmallPostingList(long* pSortedIds, int count, ref int currentIdx)
        {
            while (currentIdx < count)
            {
                var read = _smallListReader.Fill(pSortedIds + currentIdx, count - currentIdx);
                EntryIdEncodings.DecodeAndDiscardFrequency(new Span<long>(pSortedIds + currentIdx, read), read);
                if (read == 0)
                {
                    _hasSmallListReader = false;
                    break;
                }
                if (pSortedIds[currentIdx + read - 1] < _min)
                    continue;
                currentIdx += read;
            }
        }

        public void Dispose()
        {
            _smallListReader.Dispose();
            _smallPostListIds.Dispose();
            _containerItemsScope.Dispose();
            _itBufferScope.Dispose();
        }
    }

    /// <summary>
    /// Walk the CompactTree index in sorted order, intersecting each batch of entry IDs
    /// with the bitmap via AndWith. Stops early once _take results are collected.
    /// Avoids full materialization by intersecting directly against the bitmap.
    /// </summary>
    private static void StreamInIndexOrder<TEntryComparer, TDirection>(
        SortingMatch<TInner> match, IBitmapQueryMatch bitmapMatch)
        where TDirection : struct, ILookupIterator
        where TEntryComparer : struct, IEntryComparer, IComparer<UnmanagedSpan>
    {
        var llt = match._searcher.Transaction.LowLevelTransaction;
        var allocator = match._searcher.Allocator;
        var entryCmp = default(TEntryComparer);

        int maxResults = match._take == -1 ? int.MaxValue : match._take;

        // Runtime escape hatch. ShouldUseIndexOrderStreaming assumed candidates spread uniformly across the index);
        // if they are actually clustered far from the scan's start the walk reads far more index entries without hitting the limit.
        // Once we have scanned past this multiple of the candidate count we abandon the walk and materialize+sort the
        // candidates instead, limiting the max cost we spend
        const int maxScanCandidateMultiplier = 2;
        long scanBailoutThreshold = match.TotalResults * maxScanCandidateMultiplier;
        bool forceUsingOnlyIndex = match.ForcedStrategy == CoraxSortingStrategy.IndexOrderStreaming;

        // Per-plan learning: record (entries actually scanned / the gate's uniform estimate) so a future
        // ShouldUseIndexOrderStreaming for this plan can inflate its estimate when candidates turn out to
        // cluster. Skipped on the forced path — those runs ignore the gate and would pollute the signal.
        var scanInflation = forceUsingOnlyIndex is false && bitmapMatch is CompiledQueryMatch { CompiledPlan.StreamScanInflation: { } si } ? si : null;

        using var sortedIdsScope = allocator.Allocate(sizeof(long) * SortBatchSize, out ByteString bs);
        Span<long> sortedIdBuffer = new(bs.Ptr, SortBatchSize);

        using var emittedBitmap = new RoaringBitmap(allocator);

        // Seek optimization: when the WHERE field matches the ORDER BY field, skip walking
        // tree terms that can't match by seeking the underlying iterator to the boundary value.
        // The hint value is matched against the sort field at the per-direction branch in GetReader,
        // where the concrete key type is known.
        object hintValue = null;
        if (bitmapMatch is CompiledQueryMatch { SortHint: { } hint } &&
            SliceEqualsUtf8(entryCmp.GetSortFieldName(match), hint.FieldName))
        {
            hintValue = hint.Value;
        }

        using var reader = GetReader(bitmapMatch.MinEntryId, bitmapMatch.MaxEntryId, hintValue);

        while (match._results.Count < maxResults)
        {
            match._cancellationToken.ThrowIfCancellationRequested();

            if (forceUsingOnlyIndex == false && match.EntriesStreamed > scanBailoutThreshold)
            {
                // Degenerate walk: scanned too much for too few hits. Discard the streamed prefix
                // and re-sort the full candidate set via SortInMemory. We discard the sorted portion from the
                // scan to ensure that the sort is done consistently with the SortInMemory.
                // EntriesStreamed is kept so the wasted scan stays visible in the query plan graph.
                match.SortStrategy = CoraxSortingStrategy.IndexOrderFallbackToInMemorySort;
                scanInflation?.UpdateOnBatchCompletion(match.EntriesStreamed, (long)match._rawStreamScanEstimate);
                match._results.Clear();
                SortInMemory<TEntryComparer>(match, bitmapMatch);
                return;
            }

            var read = reader.Read(sortedIdBuffer);
            if (read == 0)
                break;

            match.EntriesStreamed += read; // sort-index IDs read before intersection

            // Intersect this batch with the WHERE bitmap, then dedup against the emitted
            // bitmap in a single pass — filters + adds new entries to emittedBitmap at once.
            read = bitmapMatch.AndWith(sortedIdBuffer, read);
            read = emittedBitmap.DedupAddNew(sortedIdBuffer, read);

            int toAdd = Math.Min(read, maxResults - match._results.Count);
            for (int i = 0; i < toAdd; i++)
                match._results.Add(sortedIdBuffer[i]);
        }

        // Streaming completed within budget: feed the observed scan back so the gate keeps trusting this plan.
        scanInflation?.UpdateOnBatchCompletion(match.EntriesStreamed, (long)match._rawStreamScanEstimate);


        [SkipLocalsInit]
        SortedIndexReader<TDirection> GetReader(long min, long max, object hint)
        {
            if (typeof(TDirection) == typeof(Lookup<CompactTree.CompactKeyLookup>.ForwardIterator) ||
                typeof(TDirection) == typeof(Lookup<CompactTree.CompactKeyLookup>.BackwardIterator))
            {
                var termsTree = match._searcher.GetTermsFor(entryCmp.GetSortFieldName(match));
                var it = termsTree.IterateValues<TDirection>();
                it.Reset();
                if (hint is string strVal)
                {
                    var compactKey = llt.AcquireCompactKey();
                    int byteCount = System.Text.Encoding.UTF8.GetByteCount(strVal);
                    if (byteCount <= Utf8StackAllocThreshold)
                    {
                        Span<byte> stackBuf = stackalloc byte[Utf8StackAllocThreshold];
                        int written = System.Text.Encoding.UTF8.GetBytes(strVal, stackBuf);
                        compactKey.Set(stackBuf[..written]);
                    }
                    else
                    {
                        int written = System.Text.Encoding.UTF8.GetBytes(strVal, GrowUtf8Buffer(byteCount));
                        compactKey.Set(((Span<byte>)Utf8ThreadBuffer)[..written]);
                    }
                    compactKey.ChangeDictionary(termsTree.DictionaryId);
                    it.Seek(new CompactTree.CompactKeyLookup(compactKey));
                }
                return new SortedIndexReader<TDirection>(llt, match._searcher, it, match._orderMetadata.Field, min, max, match._nullFirst, match._orderMetadata.Ascending);
            }

            if (typeof(TDirection) == typeof(Lookup<Int64LookupKey>.ForwardIterator) ||
                typeof(TDirection) == typeof(Lookup<Int64LookupKey>.BackwardIterator))
            {
                var termsTree = match._searcher.GetLongTermsFor(entryCmp.GetSortFieldName(match));
                var it = termsTree.Iterate<TDirection>();
                it.Reset();
                if (hint is long longVal)
                    it.Seek(new Int64LookupKey(longVal));
                return new SortedIndexReader<TDirection>(llt, match._searcher, it, match._orderMetadata.Field, min, max, match._nullFirst, match._orderMetadata.Ascending);
            }

            if (typeof(TDirection) == typeof(Lookup<DoubleLookupKey>.ForwardIterator) ||
                typeof(TDirection) == typeof(Lookup<DoubleLookupKey>.BackwardIterator))
            {
                var termsTree = match._searcher.GetDoubleTermsFor(entryCmp.GetSortFieldName(match));
                var it = termsTree.Iterate<TDirection>();
                it.Reset();
                if (hint is double doubleVal)
                    it.Seek(new DoubleLookupKey(doubleVal));
                return new SortedIndexReader<TDirection>(llt, match._searcher, it, match._orderMetadata.Field, min, max, match._nullFirst, match._orderMetadata.Ascending);
            }

            throw new NotSupportedException(typeof(TDirection).FullName);
        }
    }

    /// <summary>Ensures <see cref="SortingMatch.Utf8ThreadBuffer"/> is at least <paramref name="byteCount"/>
    /// bytes long and returns it. Grows to the next power of two on demand.
    /// Called only on the rare path where the string exceeds <see cref="Utf8StackAllocThreshold"/>.</summary>
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static byte[] GrowUtf8Buffer(int byteCount)
    {
        ref byte[] buf = ref Utf8ThreadBuffer;
        if (buf == null || buf.Length < byteCount)
            buf = new byte[Bits.PowerOf2(byteCount)];
        return buf;
    }

    /// <summary>Compare a Slice's bytes to a string's UTF-8 encoding without allocating.
    /// Used for sort-hint field-name matching where the slice comes from the index
    /// and the hint field name comes from the query AST.</summary>
    [System.Runtime.CompilerServices.SkipLocalsInit]
    private static bool SliceEqualsUtf8(Slice slice, string s)
    {
        var sliceSpan = slice.AsReadOnlySpan();
        int byteCount = System.Text.Encoding.UTF8.GetByteCount(s);
        if (byteCount != sliceSpan.Length)
            return false;
        if (byteCount <= Utf8StackAllocThreshold)
        {
            Span<byte> stackBuf = stackalloc byte[Utf8StackAllocThreshold];
            int written = System.Text.Encoding.UTF8.GetBytes(s, stackBuf);
            return sliceSpan.SequenceEqual(stackBuf[..written]);
        }
        int written2 = System.Text.Encoding.UTF8.GetBytes(s, GrowUtf8Buffer(byteCount));
        return sliceSpan.SequenceEqual(((Span<byte>)Utf8ThreadBuffer)[..written2]);
    }

    /// <summary>
    /// For sort types without an index to walk (score, spatial, alphanumeric, random),
    /// materialize all bitmap entries directly and heap sort.
    /// </summary>
    private static void SortInMemory<TEntryComparer>(SortingMatch<TInner> match, IBitmapQueryMatch bitmapMatch)
        where TEntryComparer : struct, IEntryComparer, IComparer<UnmanagedSpan>
    {
        var allocator = match._searcher.Allocator;

        if (match.TotalResults > int.MaxValue)
            throw new InvalidOperationException($"TotalResults ({match.TotalResults}) exceeds int.MaxValue — cannot materialize all bitmap entries for sorting.");

        int total = (int)match.TotalResults;

        // TotalResults == bitmapMatch.Count, so one Fill call covers everything.
        using var scope = allocator.Allocate(total * sizeof(long), out ByteString bs);
        var allMatches = new Span<long>(bs.Ptr, total);

        int filled = bitmapMatch.Fill(allMatches);

        if (filled == 0)
            return;

        SortResults<TEntryComparer>(match, allMatches[..filled]);
    }
    
    /// <summary>Drain all results from the inner match via Fill, then heap sort.
    /// Used for non-bitmap matches (VectorSearchMatch, PostFilterMatch, scoring matches)
    /// where materializing into a bitmap would lose match-specific state.</summary>
    private static void SortComputedResults<TEntryComparer>(SortingMatch<TInner> match)
        where TEntryComparer : struct, IEntryComparer, IComparer<UnmanagedSpan>
    {
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
            return;
        }

        SortResults<TEntryComparer>(match, allMatches[..filled]);
        scope.Dispose();
    }

    private static void SortResults<TEntryComparer>(SortingMatch<TInner> match, Span<long> batchResults)
        where TEntryComparer : struct,  IEntryComparer, IComparer<UnmanagedSpan>
    {
        var llt = match._searcher.Transaction.LowLevelTransaction;
        var allocator = match._searcher.Allocator;

        var sizeToAllocate = batchResults.Length * (sizeof(long) + sizeof(UnmanagedSpan));

        //OrderBySpatial relies on this order of data. If you change it, please review the spatial ordering to ensure that everything works fine: [[ids], [terms], [spatial_distances]].
        if (match._sortingDataTransfer.IncludeDistances)
            sizeToAllocate += batchResults.Length * sizeof(SpatialResult);
        
        using var bufScope = allocator.Allocate(sizeToAllocate, out ByteString bs);
        Span<long> batchTermIds = new(bs.Ptr, batchResults.Length);
        UnmanagedSpan* termsPtr = (UnmanagedSpan*)(bs.Ptr + batchResults.Length * sizeof(long));

        TEntryComparer entryComparer = new();
        entryComparer.Init(match);
        
        entryComparer.SortBatch(match, llt, llt.PageLocator, batchResults, batchTermIds, termsPtr);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override void SetSortingDataTransfer(in SortingDataTransfer sortingDataTransfer)
    {
        _sortingDataTransfer = sortingDataTransfer;
        if (_sortingDataTransfer.IncludeScores)
            _scoresResults = new(_searcher.Allocator);
        if (_sortingDataTransfer.IncludeDistances)
            _distancesResults = new(_searcher.Allocator);
    }

    public override long Count => _inner.Count;

    public override QueryCountConfidence Confidence => throw new NotSupportedException();

    public override bool IsBoosting => _inner.IsBoosting || _orderMetadata.FieldType == MatchCompareFieldType.Score;

    public override int AndWith(Span<long> buffer, int matches)
    {
        throw new NotSupportedException($"{nameof(SortingMatch<TInner>)} does not support the operation of {nameof(AndWith)}.");
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override int Fill(Span<long> matches)
    {
        // Time the whole sort. The first call does the actual sort/stream work (TotalResults == NotStarted);
        // later calls just page out already-sorted results. Two timestamps per Fill is negligible against the
        // sort itself and gives include timings() a real number for the sort node, which is otherwise untimed.
        long start = Stopwatch.GetTimestamp();
        var read = _fillFunc(this, matches);
        SortingTimeInTicks += Stopwatch.GetTimestamp() - start;
        return read;
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
            {Constants.QueryInspectionNode.FieldName, _orderMetadata.Field.FieldName.ToString()},
            {Constants.QueryInspectionNode.Ascending, _orderMetadata.Ascending.ToString()},
            {Constants.QueryInspectionNode.FieldType, _orderMetadata.FieldType.ToString()},
        };

        switch (_orderMetadata.FieldType)
        {
            case MatchCompareFieldType.Spatial:
                parameters.Add(Constants.QueryInspectionNode.Point, _orderMetadata.Point.ToString());
                parameters.Add(Constants.QueryInspectionNode.Round, _orderMetadata.Round.ToString(CultureInfo.InvariantCulture));
                parameters.Add(Constants.QueryInspectionNode.Units, _orderMetadata.Units.ToString());
                break;
            case MatchCompareFieldType.Random:
                parameters.Add(Constants.QueryInspectionNode.RandomSeed, _orderMetadata.RandomSeed.ToString());
                break;
        }

        // Surface the sort's own runtime cost. The compiled bitmap pipeline times its ops into
        // CompiledQueryMatch's telemetry array, but the sort wrapper runs above it and is otherwise
        // absent from include timings(). EntriesStreamed >> result count flags a degenerate
        // IndexOrderStreaming (scattered/tiny candidate set scanning most of the sort index).
        if (SortStrategy is { } strategy)
            parameters["Strategy"] = strategy.ToString();
        if (SortingTimeInTicks > 0)
            parameters["Ms"] = (SortingTimeInTicks / (Stopwatch.Frequency / 1000.0)).ToString("F3", CultureInfo.InvariantCulture);
        if (EntriesStreamed > 0)
        {
            parameters["EntriesStreamed"] = EntriesStreamed.ToString();
            // Pair the scan count with the candidate count so the cost ratio is legible. A healthy
            // IndexOrderStreaming keeps streamed close to candidates; a bailout shows streamed well above
            // it — the walk read far more sort-index entries than there were candidates, which is exactly
            // the degenerate case the IndexOrderFallbackToInMemorySort strategy label flags after it fell back to SortInMemory.
            parameters["Candidates"] = TotalResults.ToString();
        }

        return new QueryInspectionNode($"{nameof(SortingMatch)}",
            children: [_inner.Inspect()],
            parameters: parameters);
    }

    public override void Dispose()
    {
        _results.Dispose();
        _entriesBufferScope.Dispose();
        _scoresResults.Dispose();
        _distancesResults.Dispose();
        (_inner as IDisposable)?.Dispose();
    }

    string DebugView => Inspect().ToString();
}
