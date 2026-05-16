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

    // Persistent batch of posting-list IDs from the provider, resumed across Fill calls.
    // The provider's iterator advances as a side effect of FillPostingListIds; if we read
    // a batch and only partially process it before the caller's matches buffer fills, the
    // unprocessed IDs would be lost. We keep the batch (and pre-resolved SmallPostingList
    // container items) on the instance and track an index so the next Fill picks up where
    // the previous one left off.
    private NativeList<long> _plIdsBuffer;
    private NativeList<UnmanagedSpan> _smallContainerItems;
    private int _plIdsRead;
    private int _plIdsIdx;
    private int _smallItemsIdx;

    // Null entry handling (non-existing entries are never included when sorting by value)
    private readonly bool _nullFirst;
    private readonly long _nullPostingListId;
    private PostingList _nullPostingList;
    private PostingList.Iterator _nullIterator;
    private bool _hasNullPostingList;
    private bool _nullExhausted;

    public SortedDrivingMatch(ITermsProvider provider, LowLevelTransaction llt, ByteStringContext allocator,
        Querying.IndexSearcher searcher, FieldMetadata field, bool nullFirst, bool drainNulls = true)
    {
        _provider = provider;
        _llt = llt;
        _allocator = allocator;
        _nullFirst = nullFirst;
        _emittedBitmap = new RoaringBitmap(allocator);

        if (drainNulls)
        {
            _hasNullPostingList = searcher.TryGetPostingListForNull(in field, out _nullPostingListId);
            _nullExhausted = !_hasNullPostingList;
            if (_hasNullPostingList)
                InitPostingList(ref _nullPostingList, ref _nullIterator, _nullPostingListId);
        }
        else
        {
            _nullExhausted = true;
        }
    }

    public long Count => -1;
    public QueryCountConfidence Confidence => QueryCountConfidence.Low;
    public bool IsBoosting => false;
    public DuplicatesOccurrence DuplicatesOccurrenceStatus => DuplicatesOccurrence.Possible;

    [SkipLocalsInit]
    public int Fill(Span<long> matches)
    {
        Span<long> entryBuffer = stackalloc long[QueryPrimitives.EntryScanBatchSize];

        bool hasPendingBatch = _plIdsIdx < _plIdsRead;
        if (_providerExhausted && hasPendingBatch == false && _hasPendingLargeIterator == false && _hasSmallListReader == false)
        {
            // After the provider is exhausted, drain nulls if they appear last
            if (_nullFirst == false && _nullExhausted == false)
                return DrainNullAndNonExisting(matches, entryBuffer);
            return 0;
        }

        int count = 0;

        // If nulls-first, drain null iterators at the start of every Fill call.
        if (_nullFirst && _nullExhausted == false)
        {
            count += DrainNullAndNonExisting(matches, entryBuffer);
            if (count >= matches.Length || _nullExhausted == false)
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

        // Lazily allocate the persistent plIds / containerItems buffers
        if (_plIdsBuffer.IsValid == false)
            _plIdsBuffer.Initialize(_allocator, QueryPrimitives.EntryScanBatchSize);
        if (_smallContainerItems.IsValid == false)
            _smallContainerItems.Initialize(_allocator, QueryPrimitives.EntryScanBatchSize);
        var pageLocator = _llt.PageLocator;

        while (count < matches.Length)
        {
            // Refill the plIds batch from the provider if the current one is exhausted
            if (_plIdsIdx >= _plIdsRead)
            {
                if (_providerExhausted)
                    break;
                _plIdsRead = _provider.FillPostingListIds(new Span<long>(_plIdsBuffer.RawItems, _plIdsBuffer.Capacity));
                if (_plIdsRead == 0)
                {
                    _providerExhausted = true;
                    break;
                }
                _plIdsIdx = 0;
                _smallItemsIdx = 0;

                // Batch-resolve SmallPostingList container items for this batch.
                // Skip the null posting list ID (handled separately by _nullIterator) so the
                // consumer-loop skip stays aligned with this prefetch.
                int smallCount = 0;
                for (int i = 0; i < _plIdsRead; i++)
                {
                    long plId = _plIdsBuffer.RawItems[i];
                    if (_hasNullPostingList && plId == _nullPostingListId)
                        continue;
                    var termType = (TermIdMask)plId & TermIdMask.EnsureIsSingleMask;
                    if (termType == TermIdMask.SmallPostingList)
                        entryBuffer[smallCount++] = (long)EntryIdEncodings.GetContainerId(plId);
                }
                if (smallCount > 0)
                {
                    Container.GetAll(_llt, entryBuffer.Slice(0, smallCount),
                        new Span<UnmanagedSpan>(_smallContainerItems.RawItems, smallCount), long.MinValue, pageLocator);
                }
            }

            // Process plIds from current position; advance _plIdsIdx as each is consumed
            while (_plIdsIdx < _plIdsRead && count < matches.Length)
            {
                long plId = _plIdsBuffer.RawItems[_plIdsIdx];

                // When we're draining nulls ourselves, skip the provider's null posting list ID — it
                // would otherwise emit the null entries inline (at the start of the iteration), but we
                // want them positioned by _nullFirst (start or end of the whole stream).
                if (_hasNullPostingList && plId == _nullPostingListId)
                {
                    _plIdsIdx++;
                    continue;
                }

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
                        _plIdsIdx++;
                        break;
                    }
                    case TermIdMask.SmallPostingList:
                    {
                        var item = _smallContainerItems.RawItems[_smallItemsIdx++];
                        _ = VariableSizeEncoding.Read<int>(item.Address, out var offset);
                        if (_smallListReader.WasInitialized == false)
                            _smallListReader = new FastPForBufferedReader(_llt.Allocator);
                        _smallListReader.Init(item.Address + offset, item.Length - offset);
                        _hasSmallListReader = true;
                        _plIdsIdx++;
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
                        _plIdsIdx++;
                        count += DrainLargePostingList(matches.Slice(count), entryBuffer);
                        break;
                    }
                    default:
                        _plIdsIdx++;
                        break;
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
            while (count < matches.Length)
            {
                // Cap request size to remaining matches slots, accounting for dedup the worst case
                // is that every entry is new — so we can't request more than slots-left without risk
                // of the iterator consuming entries we have nowhere to store.
                int slotsLeft = matches.Length - count;
                int requestSize = Math.Min(entryBuffer.Length, slotsLeft);
                int read = _smallListReader.Fill(pBuffer, requestSize);
                if (read <= 0)
                {
                    _hasSmallListReader = false; // exhausted
                    break;
                }
                EntryIdEncodings.DecodeAndDiscardFrequency(entryBuffer, read);
                for (int j = 0; j < read; j++)
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
        return count;
    }

    private int DrainLargePostingList(Span<long> matches, Span<long> entryBuffer)
    {
        int count = 0;
        while (count < matches.Length)
        {
            int slotsLeft = matches.Length - count;
            int requestSize = Math.Min(entryBuffer.Length, slotsLeft);
            var request = entryBuffer.Slice(0, requestSize);
            if (_pendingLargeIterator.Fill(request, out int read) == false || read == 0)
            {
                _hasPendingLargeIterator = false; // exhausted
                break;
            }
            EntryIdEncodings.DecodeAndDiscardFrequency(request, read);
            for (int j = 0; j < read; j++)
            {
                long entryId = request[j];
                if (_emittedBitmap.Contains(entryId) == false)
                {
                    _emittedBitmap.Add(entryId);
                    matches[count++] = entryId;
                }
            }
        }
        return count;
    }

    private int DrainNullAndNonExisting(Span<long> matches, Span<long> entryBuffer)
    {
        int count = 0;

        // Drain null posting list — entries with explicit null value.
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
        while (count < matches.Length)
        {
            int slotsLeft = matches.Length - count;
            int requestSize = Math.Min(entryBuffer.Length, slotsLeft);
            var request = entryBuffer.Slice(0, requestSize);
            if (iterator.Fill(request, out int read) == false || read == 0)
            {
                exhausted = true;
                break;
            }
            EntryIdEncodings.DecodeAndDiscardFrequency(request, read);
            for (int j = 0; j < read; j++)
            {
                long entryId = request[j];
                if (_emittedBitmap.Contains(entryId) == false)
                {
                    _emittedBitmap.Add(entryId);
                    matches[count++] = entryId;
                }
            }
        }
        return count;
    }

    private void InitPostingList(ref PostingList postingList, ref PostingList.Iterator iterator, long postingListId)
    {
        var containerEntryId = EntryIdEncodings.GetContainerId(postingListId);
        var setStateSpan = Container.GetReadOnly(_llt, containerEntryId);
        ref readonly var setState = ref MemoryMarshal.AsRef<PostingListState>(setStateSpan);
        postingList = new PostingList(_llt, Slices.Empty, in setState);
        iterator = postingList.Iterate();
    }

    public int AndWith(Span<long> buffer, int matches) => throw new NotSupportedException();
    public void Score(Span<long> matches, Span<float> scores, float boostFactor) { }

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
        _plIdsBuffer.Dispose(_allocator);
        _smallContainerItems.Dispose(_allocator);
    }
}
