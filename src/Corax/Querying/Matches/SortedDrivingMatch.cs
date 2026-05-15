using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Corax.Indexing;
using Corax.Mappings;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Primitives;
using Corax.Utils;
using Sparrow;
using Sparrow.Compression;
using Sparrow.Server;
using Voron;
using Voron.Data.Containers;
using Voron.Data.PostingLists;
using Voron.Data.RoaringBitmaps;
using Voron.Impl;
using Voron.Util;
using Voron.Util.PFor;

namespace Corax.Querying.Matches;

/// <summary>
/// Walks an ITermsProvider in term order, decoding each posting list and yielding
/// entry IDs directly. Unlike TermsProviderMatch (which materializes into a RoaringBitmap,
/// losing sort order), this yields entries grouped by term — preserving field-value order.
///
/// The ITermsProvider enforces range bounds (TermsRangeProvider only yields terms in range).
/// No separate bitmap phase needed.
/// </summary>
public sealed unsafe class SortedDrivingMatch : IQueryMatch, IDisposable
{
    private readonly ITermsProvider _provider;
    private readonly LowLevelTransaction _llt;
    private readonly ByteStringContext _allocator;

    // Dedup for multi-value fields
    private RoaringBitmap _emittedBitmap;

    // State for resuming across Fill calls
    private bool _providerExhausted;

    // Pending entries from a partially-consumed posting list
    private PostingList _pendingPostingList;
    private PostingList.Iterator _pendingLargeIterator;
    private bool _hasPendingLargeIterator;
    private FastPForBufferedReader _smallListReader;
    private bool _hasSmallListReader;

    // Null and non-existing entry handling (mirrors SortedIndexReader pattern)
    private readonly bool _nullFirst;
    private readonly long _nonExistingPostingListId;
    private readonly long _nullPostingListId;
    private PostingList _nonExistingPostingList;
    private PostingList _nullPostingList;
    private PostingList.Iterator _nonExistingIterator;
    private PostingList.Iterator _nullIterator;
    private bool _hasNonExistingPostingList;
    private bool _hasNullPostingList;
    private bool _nonExistingExhausted;
    private bool _nullExhausted;

    public SortedDrivingMatch(ITermsProvider provider, LowLevelTransaction llt, ByteStringContext allocator,
        Querying.IndexSearcher searcher, FieldMetadata field, bool nullFirst)
    {
        _provider = provider;
        _llt = llt;
        _allocator = allocator;
        _nullFirst = nullFirst;
        _emittedBitmap = new RoaringBitmap(allocator);

        _hasNonExistingPostingList = searcher.TryGetPostingListForNonExisting(in field, out _nonExistingPostingListId);
        _hasNullPostingList = searcher.TryGetPostingListForNull(in field, out _nullPostingListId);
        _nonExistingExhausted = !_hasNonExistingPostingList;
        _nullExhausted = !_hasNullPostingList;
        if (_hasNonExistingPostingList)
            InitPostingList(ref _nonExistingPostingList, ref _nonExistingIterator, _nonExistingPostingListId);
        if (_hasNullPostingList)
            InitPostingList(ref _nullPostingList, ref _nullIterator, _nullPostingListId);
    }

    public long Count => -1;
    public QueryCountConfidence Confidence => QueryCountConfidence.Low;
    public bool IsBoosting => false;
    public DuplicatesOccurrence DuplicatesOccurrenceStatus => DuplicatesOccurrence.Possible;

    [SkipLocalsInit]
    public int Fill(Span<long> matches)
    {
        if (_providerExhausted && _hasPendingLargeIterator == false && _hasSmallListReader == false)
        {
            // After the provider is exhausted, drain null/non-existing if they appear last
            if (_nullFirst == false && (_nonExistingExhausted == false || _nullExhausted == false))
                return DrainNullAndNonExisting(matches);
            return 0;
        }

        int count = 0;
        Span<long> entryBuffer = stackalloc long[QueryPrimitives.EntryScanBatchSize];

        // If nulls-first, drain null/non-existing iterators at the start of every Fill call.
        if (_nullFirst && (_nonExistingExhausted == false || _nullExhausted == false))
        {
            count += DrainNullAndNonExisting(matches);
            if (count >= matches.Length || (_nonExistingExhausted == false || _nullExhausted == false))
                return count;
        }

        // Resume any pending large posting list iterator
        if (_hasPendingLargeIterator)
        {
            count += DrainLargePostingList(matches.Slice(count), entryBuffer);
            if (count >= matches.Length)
                return count;
        }

        // Resume any pending small posting list reader
        if (_hasSmallListReader)
        {
            count += DrainSmallPostingList(matches.Slice(count), entryBuffer);
            if (count >= matches.Length)
                return count;
        }

        // Walk the provider's posting list IDs
        Span<long> plIds = stackalloc long[QueryPrimitives.EntryScanBatchSize];
        var pageLocator = _llt.PageLocator;

        int read;
        while (count < matches.Length && !_providerExhausted)
        {
            read = _provider.FillPostingListIds(plIds);
            if (read == 0)
            {
                _providerExhausted = true;
                break;
            }

            // Batch-resolve SmallPostingList container items
            Span<long> smallPlIds = stackalloc long[read];
            Span<UnmanagedSpan> containerItems = stackalloc UnmanagedSpan[read];
            int smallCount = 0;

            for (int i = 0; i < read; i++)
            {
                long plId = plIds[i];
                var termType = (TermIdMask)plId & TermIdMask.EnsureIsSingleMask;

                if (termType == TermIdMask.SmallPostingList)
                {
                    smallPlIds[smallCount++] = (long)EntryIdEncodings.GetContainerId(plId);
                }
            }
            if (smallCount > 0)
            {
                Container.GetAll(_llt, smallPlIds.Slice(0, smallCount), containerItems.Slice(0, smallCount), long.MinValue, pageLocator);
            }

            int smallIdx = 0;
            for (int i = 0; i < read && count < matches.Length; i++)
            {
                long plId = plIds[i];
                var termType = (TermIdMask)plId & TermIdMask.EnsureIsSingleMask;

                switch (termType)
                {
                    case TermIdMask.Single:
                    {
                        long entryId = (long)EntryIdEncodings.GetContainerId(plId);
                        if (_emittedBitmap.Contains(entryId) == false)
                        {
                            _emittedBitmap.Add(entryId);
                            matches[count++] = entryId;
                        }
                        break;
                    }
                    case TermIdMask.SmallPostingList:
                    {
                        var item = containerItems[smallIdx++];
                        _ = VariableSizeEncoding.Read<int>(item.Address, out var offset);
                        if (_smallListReader.WasInitialized == false)
                            _smallListReader = new FastPForBufferedReader(_llt.Allocator);
                        _smallListReader.Init(item.Address + offset, item.Length - offset);
                        _hasSmallListReader = true;
                        count += DrainSmallPostingList(matches.Slice(count), entryBuffer);
                        break;
                    }
                    case TermIdMask.PostingList:
                    {
                        var setStateSpan = Container.GetReadOnly(_llt, EntryIdEncodings.GetContainerId(plId));
                        ref readonly var setState = ref MemoryMarshal.AsRef<PostingListState>(setStateSpan);
                        _pendingPostingList = new PostingList(_llt, Slices.Empty, in setState);
                        _pendingLargeIterator = _pendingPostingList.Iterate();
                        _hasPendingLargeIterator = true;
                        count += DrainLargePostingList(matches.Slice(count), entryBuffer);
                        break;
                    }
                }
            }
        }

        return count;
    }

    private int DrainSmallPostingList(Span<long> matches, Span<long> entryBuffer)
    {
        int count = 0;
        fixed (long* pBuffer = entryBuffer)
        {
            int read;
            while (count < matches.Length && (read = _smallListReader.Fill(pBuffer, entryBuffer.Length)) > 0)
            {
                EntryIdEncodings.DecodeAndDiscardFrequency(entryBuffer, read);
                for (int j = 0; j < read && count < matches.Length; j++)
                {
                    long entryId = entryBuffer[j];
                    if (_emittedBitmap.Contains(entryId) == false)
                    {
                        _emittedBitmap.Add(entryId);
                        matches[count++] = entryId;
                    }
                }
            }
        }
        if (count < matches.Length)
            _hasSmallListReader = false; // exhausted
        return count;
    }

    private int DrainLargePostingList(Span<long> matches, Span<long> entryBuffer)
    {
        int count = 0;
        while (count < matches.Length && _pendingLargeIterator.Fill(entryBuffer, out int read) && read > 0)
        {
            EntryIdEncodings.DecodeAndDiscardFrequency(entryBuffer, read);
            for (int j = 0; j < read && count < matches.Length; j++)
            {
                long entryId = entryBuffer[j];
                if (_emittedBitmap.Contains(entryId) == false)
                {
                    _emittedBitmap.Add(entryId);
                    matches[count++] = entryId;
                }
            }
        }
        if (count < matches.Length)
            _hasPendingLargeIterator = false; // exhausted
        return count;
    }

    private int DrainNullAndNonExisting(Span<long> matches)
    {
        int count = 0;
        Span<long> entryBuffer = stackalloc long[QueryPrimitives.EntryScanBatchSize];

        // Drain non-existing posting list first, then null.
        if (_hasNonExistingPostingList && _nonExistingExhausted == false)
        {
            int drained = DrainIterator(matches, entryBuffer, ref _nonExistingIterator, ref _nonExistingExhausted);
            count += drained;
        }
        if (_hasNullPostingList && _nullExhausted == false && count < matches.Length)
        {
            count += DrainIterator(matches.Slice(count), entryBuffer, ref _nullIterator, ref _nullExhausted);
        }

        return count;
    }

    private int DrainIterator(Span<long> matches, Span<long> entryBuffer,
        ref PostingList.Iterator iterator, ref bool exhausted)
    {
        int count = 0;
        while (count < matches.Length && iterator.Fill(entryBuffer, out int read) && read > 0)
        {
            EntryIdEncodings.DecodeAndDiscardFrequency(entryBuffer, read);
            for (int j = 0; j < read && count < matches.Length; j++)
            {
                long entryId = entryBuffer[j];
                if (_emittedBitmap.Contains(entryId) == false)
                {
                    _emittedBitmap.Add(entryId);
                    matches[count++] = entryId;
                }
            }
        }
        if (count < matches.Length)
            exhausted = true;
        return count;
    }

    private void InitPostingList(ref PostingList postingList, ref PostingList.Iterator iterator, long postingListId)
    {
        var setStateSpan = Container.GetReadOnly(_llt, new ContainerEntryId(postingListId));
        ref readonly var setState = ref MemoryMarshal.AsRef<PostingListState>(setStateSpan);
        postingList = new PostingList(_llt, Slices.Empty, in setState);
        iterator = postingList.Iterate();
    }

    public int AndWith(Span<long> buffer, int matches) => throw new NotSupportedException();
    public void Score(Span<long> matches, Span<float> scores, float boostFactor) { }
    public SkipSortingResult AttemptToSkipSorting() => SkipSortingResult.ResultsNativelySorted;

    public QueryInspectionNode Inspect()
    {
        return new QueryInspectionNode("SortedDrivingMatch",
            parameters: new Dictionary<string, string>
            {
                ["Provider"] = _provider.Inspect().Operation
            });
    }

    public void Dispose()
    {
        if (_smallListReader.WasInitialized)
            _smallListReader.Dispose();
        _pendingPostingList?.Dispose();
        _emittedBitmap.Dispose();
    }
}
