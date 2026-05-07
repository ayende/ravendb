using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Threading;
using Corax.Indexing;
using Corax.Mappings;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.SortingMatches.Meta;
using Corax.Utils;
using Corax.Utils.Spatial;
using Sparrow;
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
public unsafe sealed partial class SortingMatch<TInner> : SortingMatch
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
    private ByteStringContext<ByteStringMemoryCache>.InternalScope _entriesBufferScope;

    private ContextBoundNativeList<long> _results;
    private ContextBoundNativeList<SpatialResult> _distancesResults;
    private ContextBoundNativeList<float> _scoresResults;
    private int _alreadyReadIdx;


    private SortingDataTransfer _sortingDataTransfer;
    public override SkipSortingResult AttemptToSkipSorting() => throw new NotSupportedException();
    
    public override DuplicatesOccurrence DuplicatesOccurrenceStatus => DuplicatesOccurrence.NotPossible;
    
    public SortingMatch(IndexSearcher searcher, in TInner inner, OrderMetadata orderMetadata, in CancellationToken cancellationToken, bool nullFirst, int take = -1)
    {
        _searcher = searcher;
        _inner = inner;
        _orderMetadata = orderMetadata;
        _cancellationToken = cancellationToken;
        _nullFirst = nullFirst;
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

        public void Init<T>(T parent)
        {
            throw new NotImplementedException();
        }

        public void Reset()
        {
            throw new NotImplementedException();
        }

        public int Fill(Span<long> results, long lastId, bool includeMax)
        {
            throw new NotImplementedException();
        }
        
        public bool Skip(long count)
        {
            throw new NotImplementedException();
        }

        public bool MoveNext(out long value)
        {
            throw new NotImplementedException();
        }

        public bool MoveNext<TLookupKey>(out TLookupKey key, out long value, out bool hasPreviousValue)
        {
            throw new NotImplementedException();
        }

        public void Seek<TLookupKey>(TLookupKey key)
        {
            throw new NotImplementedException();
        }
    }
    
    private struct NoIterationOptimization : ILookupIterator
    {
        public bool IsForward => throw new NotSupportedException($"{nameof(NoIterationOptimization)} has no direction and should not be used in parts of code where it is required.");

        
        public void Init<T>(T parent)
        {
            throw new NotImplementedException();
        }

        public void Reset()
        {
            throw new NotImplementedException();
        }

        public int Fill(Span<long> results, long lastId = long.MaxValue, bool includeMax = true)
        {
            throw new NotImplementedException();
        }
        
        public bool Skip(long count)
        {
            throw new NotImplementedException();
        }

        public bool MoveNext(out long value)
        {
            throw new NotImplementedException();
        }

        public bool MoveNext<TLookupKey>(out TLookupKey key, out long value, out bool hasPreviousValue)
        {
            throw new NotImplementedException();
        }

        public void Seek<TLookupKey>(TLookupKey key)
        {
            throw new NotImplementedException();
        }
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
            // Bitmap-backed matches (CompiledQueryMatch) can avoid full materialization.
            // Walk the CompactTree index and intersect batches via AndWith, stopping early
            // when the LIMIT is reached. This avoids MemoizationMatch's O(N) copy.
            if (match._inner is IBitmapQueryMatch bitmapMatch)
            {
                match.TotalResults = bitmapMatch.Count;
                if (match.TotalResults == 0)
                    return 0;

                if (typeof(TDirection) == typeof(RandomDirection))
                {
                    // Bitmap path: reservoir-sample k entries from the bitmap iterator in one
                    // O(N) pass. No need to materialise all N entries — only the k=_take
                    // slots are ever live in memory at once.
                    ReservoirSampleFromBitmap(match, bitmapMatch);
                }
                else if (typeof(TDirection) == typeof(NoIterationOptimization))
                {
                    // Score/spatial/alphanumeric: no index to walk, must materialize + heap sort
                    SortResultsFromBitmap<TEntryComparer>(match, bitmapMatch);
                }
                else
                {
                    // Index walk: intersect CompactTree batches with bitmap via AndWith
                    SortUsingIndexFromBitmap<TEntryComparer, TDirection>(match, bitmapMatch);
                }
            }
            else
            {
                // Non-bitmap path (PostFilterMatch, VectorSearchMatch, etc.)
                // Materialize all results by calling Fill repeatedly.
                var count = match._inner.Count;
                int bufferSize = count > 0 && count < 1024 * 1024 ? (int)count : 4096;
                var scope = match._searcher.Allocator.Allocate(bufferSize * sizeof(long), out var bs);
                var allMatches = new Span<long>(bs.Ptr, bufferSize);
                int filled = 0;
                int r;
                while ((r = match._inner.Fill(allMatches[filled..])) > 0)
                {
                    filled += r;
                    if (filled >= allMatches.Length)
                    {
                        var newSize = allMatches.Length * 2;
                        var newScope = match._searcher.Allocator.Allocate(newSize * sizeof(long), out var newBs);
                        var newBuf = new Span<long>(newBs.Ptr, newSize);
                        allMatches[..filled].CopyTo(newBuf);
                        scope.Dispose();
                        scope = newScope;
                        allMatches = newBuf;
                    }
                }

                match.TotalResults = filled;
                if (match.TotalResults == 0)
                {
                    scope.Dispose();
                    return 0;
                }

                SortResults<TEntryComparer>(match, allMatches[..filled]);
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
        match._entriesBufferScope.Dispose();
        match._scoresResults.Dispose();
        match._distancesResults.Dispose();

        return 0;
    }

    private static void ReservoirSampleFromBitmap(SortingMatch<TInner> match, IBitmapQueryMatch bitmapMatch)
    {
        var random = new Random(match._orderMetadata.RandomSeed);
        int take = match._take;

        Span<long> page = stackalloc long[1024];

        if (take < 0)
        {
            // No LIMIT: materialize the whole bitmap, then Fisher-Yates shuffle in place.
            int read;
            while ((read = bitmapMatch.Fill(page)) > 0)
                for (int i = 0; i < read; i++)
                    match._results.Add(page[i]);

            // Fisher-Yates: swap from the end backward so every permutation is equiprobable.
            for (int i = match._results.Count - 1; i > 0; i--)
            {
                int j = random.Next(i + 1);
                (match._results[i], match._results[j]) = (match._results[j], match._results[i]);
            }
        }
        else
        {
            // With LIMIT k: Algorithm R — one O(N) pass, O(k) live memory.
            // Entry at position i is kept with probability k/(i+1), replacing a random slot.
            int seen = 0;
            int read;
            while ((read = bitmapMatch.Fill(page)) > 0)
            {
                for (int i = 0; i < read; i++, seen++)
                {
                    long id = page[i];
                    if (match._results.Count < take)
                    {
                        match._results.Add(id);
                    }
                    else
                    {
                        int slot = random.Next(seen + 1);
                        if (slot < take)
                            match._results[slot] = id;
                    }
                }
            }
        }
    }

    private static void SortByRandom(SortingMatch<TInner> match, Span<long> results)
    {
        var random = new Random(match._orderMetadata.RandomSeed);
        // take < 0 means "no limit" — shuffle all results.
        var take = match._take < 0 ? results.Length : Math.Min(match._take, results.Length);
        while (match._results.Count < take)
        {
            int index = random.Next(match._results.Count, results.Length);
            // Fisher-Yates partial shuffle: grow the selected prefix one entry at a time.
            var replaced = results[match._results.Count];
            var selected = results[index];
            results[match._results.Count] = selected;
            results[index] = replaced;
            match._results.Add(selected);
        }
    }

    private ref struct SortedIndexReader<TDirection>
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

        public SortedIndexReader(LowLevelTransaction llt, IndexSearcher searcher, TDirection it, FieldMetadata metadata, long min, long max, bool nullFirst, bool isForward)
        {
            _termsIt = it;
            _min = min;
            _max = max;
            _nullFirst = nullFirst;
            _isForward = isForward;
            _termsIt.Reset();
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
            if (_nonExistingPostingListRead == false)
            {
                buffer[_bufferCount] = _nonExistingPostingListId;
                _nonExistingPostingListRead = true;
                _bufferCount += 1;
            }
                
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

    private static void SortUsingIndex<TEntryComparer, TDirection>(SortingMatch<TInner> match, Span<long> allMatches)
        where TDirection : struct, ILookupIterator
        where TEntryComparer : struct,  IEntryComparer, IComparer<UnmanagedSpan>
    {
        var llt = match._searcher.Transaction.LowLevelTransaction;
        var allocator = match._searcher.Allocator;
        var entryCmp = default(TEntryComparer);

        int maxResults = match._take == -1 ? int.MaxValue : match._take;

        var indexesScope = allocator.Allocate(SortBatchSize * sizeof(long), out ByteString bs);
        Span<long> indexesBuffer = new(bs.Ptr,SortBatchSize);
        var sortedIdsScope = allocator.Allocate( sizeof(long) * SortBatchSize, out bs);
        Span<long> sortedIdBuffer = new(bs.Ptr, SortBatchSize);

        var totalRead = 0;
        var reader = GetReader(match, allMatches[0], allMatches[^1]);
        var forceUsingOnlyIndex = match._searcher._testingConfiguration is { ForceSortingUsingIndex: true };
        while (match._results.Count < maxResults)
        {
            match._cancellationToken.ThrowIfCancellationRequested();
            if (forceUsingOnlyIndex == false && totalRead > allMatches.Length * 2)
            {
                // We may have _already_ matched some items, in which case they show up as negative 
                // numbers in the matches (since we want to filter them), we need to pass the matches to the 
                // direct SortResult, but first we need to remove all the items that we already matched
                int notMatchedYet = FilterAlreadyFoundMatches(allMatches);

                if (notMatchedYet > 0)
                {
                    // if we scanned through the index more than twice the amount of records of the query, but still
                    // didn't find enough to fill the page size, we'll fall back to normal sorting, instead of using the
                    // index method. That would prevent degenerate cases.
                    SortResults<TEntryComparer>(match, allMatches[..notMatchedYet]);
                }

                return;
            }

            var read = reader.Read(sortedIdBuffer);
            if (read == 0)
            {
                // there are no more results from the index, but we may have records that don't *have* an entry here
                // in that case, we add them to the results in arbitrary order
                for (int i = 0; i < allMatches.Length; i++)
                {
                    if(allMatches[i] < 0) // meaning, it was already matched by the SortHelper
                        continue;
                    match._results.Add(allMatches[i]);
                    if (match._results.Count >= maxResults)
                        break;
                }
                break;
            }

            totalRead += read;
            var sortedIds = sortedIdBuffer[..read];
            var indexes = indexesBuffer[..read];
            // we effectively permute the indexes as well as the sortedIds to get a sorted list to compare
            // with the allMatches
            InitializeIndexesTopHalf(indexes);
            sortedIds.Sort(indexes);
            InitializeIndexesBottomHalf(indexes);
            read = SortHelper.FindMatches(indexes, sortedIds, allMatches);
            indexes = indexes[..read];
            indexes.Sort();
            // now get the *actual* matches in their sorted order
            for (int i = 0; i < indexes.Length && match._results.Count < maxResults; i++)
            {
                match._results.Add(sortedIds[(int)indexes[i]]);
            }
        }

        reader.Dispose();
        sortedIdsScope.Dispose();
        indexesScope.Dispose();
        
        
        SortedIndexReader<TDirection> GetReader(SortingMatch<TInner> match, long min, long max)
        {
            if (typeof(TDirection) == typeof(Lookup<CompactTree.CompactKeyLookup>.ForwardIterator) ||
                typeof(TDirection) == typeof(Lookup<CompactTree.CompactKeyLookup>.BackwardIterator))
            {
                var termsTree = match._searcher.GetTermsFor(entryCmp.GetSortFieldName(match));
                return new SortedIndexReader<TDirection>(llt, match._searcher, termsTree.IterateValues<TDirection>(), match._orderMetadata.Field, min, max, match._nullFirst, match._orderMetadata.Ascending);
            }

            if (typeof(TDirection) == typeof(Lookup<Int64LookupKey>.ForwardIterator) ||
                typeof(TDirection) == typeof(Lookup<Int64LookupKey>.BackwardIterator))
            {
                var termsTree = match._searcher.GetLongTermsFor(entryCmp.GetSortFieldName(match));
                return new SortedIndexReader<TDirection>(llt, match._searcher, termsTree.Iterate<TDirection>(), match._orderMetadata.Field, min, max, match._nullFirst, match._orderMetadata.Ascending);
            }

            if (typeof(TDirection) == typeof(Lookup<DoubleLookupKey>.ForwardIterator) ||
                typeof(TDirection) == typeof(Lookup<DoubleLookupKey>.BackwardIterator))
            {
                var termsTree = match._searcher.GetDoubleTermsFor(entryCmp.GetSortFieldName(match));
                return new SortedIndexReader<TDirection>(llt, match._searcher, termsTree.Iterate<TDirection>(), match._orderMetadata.Field, min, max, match._nullFirst, match._orderMetadata.Ascending);
            }

            throw new NotSupportedException(typeof(TDirection).FullName);
        }
    }
    
    private static int FilterAlreadyFoundMatches(Span<long> items)
    {
        int output = 0;
        for (int i = 0; i < items.Length; i++)
        {
            if ((items[i] & ~long.MaxValue) != 0)
                continue;
            items[output++] = items[i];
        }
        return output;
    }


    /// <summary>
    /// Walk the CompactTree index in sorted order, intersecting each batch of entry IDs
    /// with the bitmap via AndWith. Stops early once _take results are collected.
    /// Avoids the MemoizationMatch full materialization that SortUsingIndex requires.
    /// </summary>
    private static void SortUsingIndexFromBitmap<TEntryComparer, TDirection>(
        SortingMatch<TInner> match, IBitmapQueryMatch bitmapMatch)
        where TDirection : struct, ILookupIterator
        where TEntryComparer : struct, IEntryComparer, IComparer<UnmanagedSpan>
    {
        var llt = match._searcher.Transaction.LowLevelTransaction;
        var allocator = match._searcher.Allocator;
        var entryCmp = default(TEntryComparer);

        int maxResults = match._take == -1 ? int.MaxValue : match._take;

        var sortedIdsScope = allocator.Allocate(sizeof(long) * SortBatchSize, out ByteString bs);
        Span<long> sortedIdBuffer = new(bs.Ptr, SortBatchSize);

        var reader = GetReader(match, entryCmp, bitmapMatch.MinEntryId, bitmapMatch.MaxEntryId);

        while (match._results.Count < maxResults)
        {
            match._cancellationToken.ThrowIfCancellationRequested();

            var read = reader.Read(sortedIdBuffer);
            if (read == 0)
                break;

            // Intersect this batch of sorted IDs with the bitmap.
            // AndWith filters in-place, keeping only IDs present in the bitmap.
            read = bitmapMatch.AndWith(sortedIdBuffer, read);

            for (int i = 0; i < read && match._results.Count < maxResults; i++)
                match._results.Add(sortedIdBuffer[i]);
        }

        reader.Dispose();
        sortedIdsScope.Dispose();

        SortedIndexReader<TDirection> GetReader(SortingMatch<TInner> match, TEntryComparer entryCmp, long min, long max)
        {
            if (typeof(TDirection) == typeof(Lookup<CompactTree.CompactKeyLookup>.ForwardIterator) ||
                typeof(TDirection) == typeof(Lookup<CompactTree.CompactKeyLookup>.BackwardIterator))
            {
                var termsTree = match._searcher.GetTermsFor(entryCmp.GetSortFieldName(match));
                return new SortedIndexReader<TDirection>(llt, match._searcher, termsTree.IterateValues<TDirection>(), match._orderMetadata.Field, min, max, match._nullFirst, match._orderMetadata.Ascending);
            }

            if (typeof(TDirection) == typeof(Lookup<Int64LookupKey>.ForwardIterator) ||
                typeof(TDirection) == typeof(Lookup<Int64LookupKey>.BackwardIterator))
            {
                var termsTree = match._searcher.GetLongTermsFor(entryCmp.GetSortFieldName(match));
                return new SortedIndexReader<TDirection>(llt, match._searcher, termsTree.Iterate<TDirection>(), match._orderMetadata.Field, min, max, match._nullFirst, match._orderMetadata.Ascending);
            }

            if (typeof(TDirection) == typeof(Lookup<DoubleLookupKey>.ForwardIterator) ||
                typeof(TDirection) == typeof(Lookup<DoubleLookupKey>.BackwardIterator))
            {
                var termsTree = match._searcher.GetDoubleTermsFor(entryCmp.GetSortFieldName(match));
                return new SortedIndexReader<TDirection>(llt, match._searcher, termsTree.Iterate<TDirection>(), match._orderMetadata.Field, min, max, match._nullFirst, match._orderMetadata.Ascending);
            }

            throw new NotSupportedException(typeof(TDirection).FullName);
        }
    }

    /// <summary>
    /// For sort types without an index to walk (score, spatial, alphanumeric, random),
    /// materialize all bitmap entries directly (without MemoizationMatch overhead) and heap sort.
    /// </summary>
    private static void SortResultsFromBitmap<TEntryComparer>(SortingMatch<TInner> match, IBitmapQueryMatch bitmapMatch)
        where TEntryComparer : struct, IEntryComparer, IComparer<UnmanagedSpan>
    {
        var allocator = match._searcher.Allocator;
        int total = (int)match.TotalResults;

        // TotalResults == bitmapMatch.Count, so one Fill call covers everything.
        var scope = allocator.Allocate(total * sizeof(long), out ByteString bs);
        var allMatches = new Span<long>(bs.Ptr, total);

        int filled = bitmapMatch.Fill(allMatches);

        if (filled == 0)
        {
            scope.Dispose();
            return;
        }

        SortResults<TEntryComparer>(match, allMatches[..filled]);

        scope.Dispose();
    }

    private static void InitializeIndexesTopHalf(Span<long> span)
    {
        for (int i = 0; i < span.Length; i++)
            span[i] = (long)i << 32;
    }
    
    private static void InitializeIndexesBottomHalf(Span<long> span)
    {
        for (int i = 0; i < span.Length; i++)
            span[i] |= (uint)i;
    }

    private static string[] DebugTerms(LowLevelTransaction llt, Span<UnmanagedSpan> terms)
    {
        using var s = new CompactKeyCacheScope(llt);
        var l = new string[terms.Length];
        for (int i = 0; i < terms.Length; i++)
        {
            var item = terms[i];
            int remainderBits = item.Address[0] >> 4;
            int encodedKeyLengthInBits = (item.Length - 1) * 8 - remainderBits;
            long dicId = CompactTree.GetDictionaryId(llt);
            s.Key.Set(encodedKeyLengthInBits, item.ToSpan()[1..], dicId);
            l[i] = s.Key.ToString();
        }

        return l;
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
        
        var bufScope = allocator.Allocate(sizeToAllocate, out ByteString bs);
        Span<long> batchTermIds = new(bs.Ptr, batchResults.Length);
        UnmanagedSpan* termsPtr = (UnmanagedSpan*)(bs.Ptr + batchResults.Length * sizeof(long));

        // Initialize the important infrastructure for the sorting.
        TEntryComparer entryComparer = new();
        entryComparer.Init(match);
        
        entryComparer.SortBatch(match, llt, llt.PageLocator, batchResults, batchTermIds, termsPtr);

        bufScope.Dispose();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override void SetScoreAndDistanceBuffer(in SortingDataTransfer sortingDataTransfer)
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
        
        return new QueryInspectionNode($"{nameof(SortingMatch)}",
            children: new List<QueryInspectionNode> { _inner.Inspect()},
            parameters: parameters);
    }

    string DebugView => Inspect().ToString();
}
