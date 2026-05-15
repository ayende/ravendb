using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Corax.Indexing;
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

    public SortedDrivingMatch(ITermsProvider provider, LowLevelTransaction llt, ByteStringContext allocator)
    {
        _provider = provider;
        _llt = llt;
        _allocator = allocator;
        _emittedBitmap = new RoaringBitmap(allocator);
    }

    public long Count => -1;
    public QueryCountConfidence Confidence => QueryCountConfidence.Low;
    public bool IsBoosting => false;
    public DuplicatesOccurrence DuplicatesOccurrenceStatus => DuplicatesOccurrence.Possible;

    [SkipLocalsInit]
    public int Fill(Span<long> matches)
    {
        if (_providerExhausted && _hasPendingLargeIterator == false && _hasSmallListReader == false)
            return 0;

        int count = 0;
        Span<long> entryBuffer = stackalloc long[QueryPrimitives.EntryScanBatchSize];

        // Resume any pending large posting list iterator
        if (_hasPendingLargeIterator)
        {
            count += DrainLargePostingList(matches, entryBuffer);
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
