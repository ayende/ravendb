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

    private NativeList<long> _plIdsBuffer;
    private NativeList<UnmanagedSpan> _smallContainerItems;
    private int _plIdsRead;
    private int _plIdsIdx;
    private int _smallItemsIdx;

    private readonly bool _nullFirst;
    private readonly long _nullPostingListId;
    private readonly PostingList _nullPostingList;
    private PostingList.Iterator _nullIterator;
    private readonly bool _hasNullPostingList;
    private bool _nullExhausted;

    // Non-existing entries (docs where the sort field was absent) are treated as null-adjacent:
    //   nullFirst=true → non-existing, then nulls, then normal values
    //   nullFirst=false → normal values, then nulls, then non-existing
    private readonly long _nonExistingPostingListId;
    private readonly PostingList _nonExistingPostingList;
    private PostingList.Iterator _nonExistingIterator;
    private readonly bool _hasNonExistingPostingList;
    private bool _nonExistingExhausted;

    public SortedDrivingMatch(ITermsProvider provider, LowLevelTransaction llt, ByteStringContext allocator,
        IndexSearcher searcher, FieldMetadata field, bool nullFirst, bool drainNulls = true)
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
                InitPostingList(out _nullPostingList, out _nullIterator, _nullPostingListId);

            _hasNonExistingPostingList = searcher.TryGetPostingListForNonExisting(in field, out _nonExistingPostingListId);
            _nonExistingExhausted = !_hasNonExistingPostingList;
            if (_hasNonExistingPostingList)
                InitPostingList(out _nonExistingPostingList, out _nonExistingIterator, _nonExistingPostingListId);
        }
        else
        {
            _nullExhausted = true;
            _nonExistingExhausted = true;
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
        if (_nullFirst && _nonExistingExhausted is false && _nullExhausted is false)
        {
            // If nulls-first, drain non-existing and null iterators at the start of every Fill call.
            HandleNullOrNonExistent();
        }
        else if (_nullFirst is false && _providerExhausted)
        {
            // After the provider is exhausted, drain nulls/non-existing if they appear last
            HandleNullOrNonExistent();
        }

        int count = 0;
        if (_hasPendingLargeIterator)
        {
            count += DrainLargePostingList(matches[count..], entryBuffer);
            if (count >= matches.Length)
                return count;
        }

        if (_hasSmallListReader)
        {
            count += DrainSmallPostingList(matches[count..], entryBuffer);
            if (count >= matches.Length)
                return count;
        }

        if (_plIdsBuffer.IsValid == false)
            _plIdsBuffer.Initialize(_allocator, QueryPrimitives.EntryScanBatchSize);
        if (_smallContainerItems.IsValid == false)
            _smallContainerItems.Initialize(_allocator, QueryPrimitives.EntryScanBatchSize);
        var pageLocator = _llt.PageLocator;

        while (count < matches.Length)
        {
            if (_plIdsIdx >= _plIdsRead)
            {
                _plIdsRead = _provider.FillPostingListIds(new Span<long>(_plIdsBuffer.RawItems, _plIdsBuffer.Capacity));
                if (_plIdsRead == 0)
                {
                    _providerExhausted = true;
                    break;
                }
                _plIdsIdx = 0;
                _smallItemsIdx = 0;

                // Batch-resolve SmallPostingList container items for this batch.
                int smallCount = 0;
                for (int i = 0; i < _plIdsRead; i++)
                {
                    long plId = _plIdsBuffer.RawItems[i];
                    var termType = (TermIdMask)plId & TermIdMask.EnsureIsSingleMask;
                    if (termType == TermIdMask.SmallPostingList)
                        entryBuffer[smallCount++] = (long)EntryIdEncodings.GetContainerId(plId);
                }
                if (smallCount > 0)
                {
                    Container.GetAll(_llt, entryBuffer[..smallCount],
                        new Span<UnmanagedSpan>(_smallContainerItems.RawItems, smallCount), long.MinValue, pageLocator);
                }
            }

            while (_plIdsIdx < _plIdsRead && count < matches.Length)
            {
                long plId = _plIdsBuffer.RawItems[_plIdsIdx];
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
                        count += DrainSmallPostingList(matches[count..], entryBuffer);
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
                        count += DrainLargePostingList(matches[count..], entryBuffer);
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

    private void HandleNullOrNonExistent()
    {
        if (_nonExistingExhausted is false && _hasNonExistingPostingList)
        {
            _pendingLargeIterator = _nonExistingIterator;
            _hasPendingLargeIterator = true;
            _nonExistingExhausted = true;
        }
        else if (_nullExhausted is false && _hasNullPostingList)
        {
            _pendingLargeIterator = _nullIterator;
            _hasPendingLargeIterator = true;
            _nullExhausted = true;
        }
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
                // of the iterator-consuming entries we have nowhere to store.
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
            var request = entryBuffer[..requestSize];
            if (_pendingLargeIterator.Fill(request, out int read) == false || read == 0)
            {
                _hasPendingLargeIterator = true;
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

    private void InitPostingList(out PostingList postingList, out PostingList.Iterator iterator, long postingListId)
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
        _nullPostingList?.Dispose();
        _nonExistingPostingList?.Dispose();
        _emittedBitmap.Dispose();
        _plIdsBuffer.Dispose(_allocator);
        _smallContainerItems.Dispose(_allocator);
    }
}
