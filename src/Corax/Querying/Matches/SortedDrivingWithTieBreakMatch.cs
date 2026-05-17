using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Corax.Indexing;
using Corax.Mappings;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.SortingMatches.Meta;
using Corax.Querying.Primitives;
using Corax.Utils;
using Sparrow;
using Sparrow.Compression;
using Sparrow.Server;
using Voron;
using Voron.Data.Containers;
using Voron.Data.Lookups;
using Voron.Data.PostingLists;
using Voron.Data.RoaringBitmaps;
using Voron.Impl;
using Voron.Util;
using Voron.Util.PFor;

namespace Corax.Querying.Matches;

/// <summary>
/// Like SortedDrivingMatch but resolves ties within each primary term by a secondary
/// numeric (long/double) field. Walks the ITermsProvider in primary-term order; for each
/// term, drains the entire posting list into a per-term buffer, fetches secondary values
/// via Lookup&lt;Int64LookupKey&gt;.GetFor, sorts the buffer, then emits in sorted order.
///
/// Only supports numeric tie-break fields (MatchCompareFieldType.Integer or Floating).
/// The planner must gate by per-term group size — this class caps groups at MaxGroupSize
/// and throws if exceeded.
/// </summary>
public sealed unsafe class SortedDrivingWithTieBreakMatch : IQueryMatch, IDisposable
{
    public const int MaxGroupSize = 16384;

    private readonly ITermsProvider _provider;
    private readonly LowLevelTransaction _llt;
    private readonly ByteStringContext _allocator;
    private readonly Lookup<Int64LookupKey> _secondaryLookup;
    private readonly MatchCompareFieldType _secondaryType;
    private readonly bool _secondaryDescending;
    private readonly bool _nullIsSmallest;
    private readonly long _missingSecondaryValue;

    private RoaringBitmap _emittedBitmap;
    private bool _providerExhausted;

    // Persistent primary plId batch
    private NativeList<long> _plIdsBuffer;
    private NativeList<UnmanagedSpan> _smallContainerItems;
    private int _plIdsRead;
    private int _plIdsIdx;
    private int _smallItemsIdx;

    // Per-term group state
    private NativeList<long> _groupEntries;
    private NativeList<long> _groupSecondary;
    private NativeList<int> _groupSortedIndexes;
    private int _groupSize;
    private int _groupEmitIdx;
    private bool _groupReady;

    // Null primary term handling
    private readonly bool _nullFirst;
    private readonly long _nullPostingListId;
    private PostingList _nullPostingList;
    private PostingList.Iterator _nullIterator;
    private readonly bool _hasNullPostingList;
    private bool _nullExhausted;
    // Tracks whether the null-primary group has been loaded and secondary-sorted.
    // Null-primary docs require the same per-group secondary sort as regular terms.
    private bool _nullGroupPrepared;

    public SortedDrivingWithTieBreakMatch(
        ITermsProvider provider,
        LowLevelTransaction llt,
        ByteStringContext allocator,
        Querying.IndexSearcher searcher,
        FieldMetadata primaryField,
        FieldMetadata secondaryField,
        MatchCompareFieldType secondaryType,
        bool secondaryDescending,
        bool nullFirst,
        bool nullIsSmallest,
        bool drainNulls = true)
    {
        if (secondaryType is not (MatchCompareFieldType.Integer or MatchCompareFieldType.Floating))
            throw new NotSupportedException($"SortedDrivingWithTieBreakMatch only supports Integer or Floating tie-break fields (got {secondaryType})");

        _provider = provider;
        _llt = llt;
        _allocator = allocator;
        _nullFirst = nullFirst;
        _nullIsSmallest = nullIsSmallest;
        _secondaryType = secondaryType;
        _secondaryDescending = secondaryDescending;
        _emittedBitmap = new RoaringBitmap(allocator);

        // Resolve the secondary Lookup using the type-specific field name (long/double suffix).
        Slice secondaryLookupName;
        if (secondaryType == MatchCompareFieldType.Integer)
        {
            IndexFieldsMappingBuilder.GetFieldNameForLongs(searcher.Allocator, secondaryField.FieldName, out secondaryLookupName);
            _missingSecondaryValue = nullIsSmallest ? long.MinValue : long.MaxValue;
        }
        else
        {
            IndexFieldsMappingBuilder.GetFieldNameForDoubles(searcher.Allocator, secondaryField.FieldName, out secondaryLookupName);
            _missingSecondaryValue = BitConverter.DoubleToInt64Bits(nullIsSmallest ? double.MinValue : double.MaxValue);
        }
        _secondaryLookup = searcher.EntriesToTermsReader(secondaryLookupName);

        if (drainNulls)
        {
            _hasNullPostingList = searcher.TryGetPostingListForNull(in primaryField, out _nullPostingListId);
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
        int count = 0;

        // Nulls-first: load null-primary group and sort by secondary before regular terms.
        if (_nullFirst && _nullGroupPrepared == false)
            PrepareNullGroup(entryBuffer);

        // Emit any remaining entries from a previously-sorted group (null or regular).
        if (_groupReady)
        {
            count += EmitFromSortedGroup(matches.Slice(count));
            if (count >= matches.Length)
                return count;
        }

        // Lazily allocate persistent buffers.
        if (_plIdsBuffer.IsValid == false)
            _plIdsBuffer.Initialize(_allocator, QueryPrimitives.EntryScanBatchSize);
        if (_smallContainerItems.IsValid == false)
            _smallContainerItems.Initialize(_allocator, QueryPrimitives.EntryScanBatchSize);
        if (_groupEntries.IsValid == false)
            _groupEntries.Initialize(_allocator, 1024);
        if (_groupSecondary.IsValid == false)
            _groupSecondary.Initialize(_allocator, 1024);
        if (_groupSortedIndexes.IsValid == false)
            _groupSortedIndexes.Initialize(_allocator, 1024);

        var pageLocator = _llt.PageLocator;

        while (count < matches.Length)
        {
            // Refill primary plIds batch from the provider when exhausted.
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

            // Drain the next primary term's posting list into _groupEntries, sort, then emit.
            while (_plIdsIdx < _plIdsRead && count < matches.Length)
            {
                long plId = _plIdsBuffer.RawItems[_plIdsIdx];

                // Skip null primary posting list — drained separately at start/end.
                if (_hasNullPostingList && plId == _nullPostingListId)
                {
                    _plIdsIdx++;
                    continue;
                }

                _groupSize = 0;
                var termType = (TermIdMask)plId & TermIdMask.EnsureIsSingleMask;
                switch (termType)
                {
                    case TermIdMask.Single:
                    {
                        long entryId = (long)EntryIdEncodings.GetContainerId(plId);
                        AddToGroup(entryId);
                        _plIdsIdx++;
                        break;
                    }
                    case TermIdMask.SmallPostingList:
                    {
                        var item = _smallContainerItems.RawItems[_smallItemsIdx++];
                        _ = VariableSizeEncoding.Read<int>(item.Address, out var offset);
                        var smallReader = new FastPForBufferedReader(_llt.Allocator);
                        try
                        {
                            smallReader.Init(item.Address + offset, item.Length - offset);
                            DrainSmallIntoGroup(ref smallReader, entryBuffer);
                        }
                        finally
                        {
                            if (smallReader.WasInitialized)
                                smallReader.Dispose();
                        }
                        _plIdsIdx++;
                        break;
                    }
                    case TermIdMask.PostingList:
                    {
                        var setStateSpan = Container.GetReadOnly(_llt, EntryIdEncodings.GetContainerId(plId));
                        ref readonly var setState = ref MemoryMarshal.AsRef<PostingListState>(setStateSpan);
                        var pl = new PostingList(_llt, Slices.Empty, in setState);
                        try
                        {
                            var iter = pl.Iterate();
                            DrainLargeIntoGroup(ref iter, entryBuffer);
                        }
                        finally
                        {
                            pl.Dispose();
                        }
                        _plIdsIdx++;
                        break;
                    }
                    default:
                        _plIdsIdx++;
                        continue;
                }

                if (_groupSize == 0)
                    continue;

                SortGroupBySecondary();
                _groupReady = true;
                _groupEmitIdx = 0;
                count += EmitFromSortedGroup(matches.Slice(count));
                if (count >= matches.Length)
                    return count;
            }
        }

        // After the provider is exhausted, load and emit null-primary group (nulls-last).
        if (_providerExhausted && _nullFirst == false && _nullGroupPrepared == false)
        {
            PrepareNullGroup(entryBuffer);
            if (_groupReady)
                count += EmitFromSortedGroup(matches.Slice(count));
        }

        return count;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void AddToGroup(long entryId)
    {
        if (_emittedBitmap.Contains(entryId))
            return;
        _emittedBitmap.Add(entryId);
        EnsureGroupCapacity(_groupSize + 1);
        _groupEntries.RawItems[_groupSize++] = entryId;
    }

    private void EnsureGroupCapacity(int required)
    {
        if (required > MaxGroupSize)
            throw new InvalidOperationException($"SortedDrivingWithTieBreakMatch: per-term group exceeded cap of {MaxGroupSize}; the planner should not select this path when individual term cardinality can be that large.");

        int curCap = _groupEntries.Capacity;
        if (required <= curCap)
            return;

        int newCap = curCap;
        while (newCap < required) newCap *= 2;
        if (newCap > MaxGroupSize) newCap = MaxGroupSize;
        int addition = newCap - curCap;

        _groupEntries.Grow(_allocator, addition);
        _groupSecondary.Grow(_allocator, addition);
        _groupSortedIndexes.Grow(_allocator, addition);
    }

    private void DrainSmallIntoGroup(ref FastPForBufferedReader reader, Span<long> entryBuffer)
    {
        fixed (long* pBuffer = entryBuffer)
        {
            while (true)
            {
                int read = reader.Fill(pBuffer, entryBuffer.Length);
                if (read <= 0)
                    break;
                EntryIdEncodings.DecodeAndDiscardFrequency(entryBuffer, read);
                for (int j = 0; j < read; j++)
                    AddToGroup(entryBuffer[j]);
            }
        }
    }

    private void DrainLargeIntoGroup(ref PostingList.Iterator iter, Span<long> entryBuffer)
    {
        while (true)
        {
            if (iter.Fill(entryBuffer, out int read) == false || read == 0)
                break;
            EntryIdEncodings.DecodeAndDiscardFrequency(entryBuffer.Slice(0, read), read);
            for (int j = 0; j < read; j++)
                AddToGroup(entryBuffer[j]);
        }
    }

    private void SortGroupBySecondary()
    {
        var entriesSpan = new Span<long>(_groupEntries.RawItems, _groupSize);
        var secondarySpan = new Span<long>(_groupSecondary.RawItems, _groupSize);
        var indexesSpan = new Span<int>(_groupSortedIndexes.RawItems, _groupSize);

        if (_secondaryLookup != null)
        {
            _secondaryLookup.GetFor(entriesSpan, secondarySpan, _missingSecondaryValue);
        }
        else
        {
            // No lookup tree → field doesn't exist; treat all secondary values as missing.
            secondarySpan.Fill(_missingSecondaryValue);
        }

        for (int i = 0; i < _groupSize; i++)
            indexesSpan[i] = i;

        // Sort indexes by secondary value. For Integer fields the long IS the value.
        // For Floating fields, the long is the bit-cast of a double — sorting as longs
        // would be wrong for negative values, so we reinterpret as a Span<double>.
        if (_secondaryType == MatchCompareFieldType.Integer)
        {
            if (_secondaryDescending)
                secondarySpan.Sort(indexesSpan, Comparer<long>.Create((a, b) => b.CompareTo(a)));
            else
                secondarySpan.Sort(indexesSpan);
        }
        else
        {
            var doubleSpan = MemoryMarshal.Cast<long, double>(secondarySpan);
            if (_secondaryDescending)
                doubleSpan.Sort(indexesSpan, Comparer<double>.Create((a, b) => b.CompareTo(a)));
            else
                doubleSpan.Sort(indexesSpan);
        }
    }

    private int EmitFromSortedGroup(Span<long> matches)
    {
        int count = 0;
        var entries = _groupEntries.RawItems;
        var indexes = _groupSortedIndexes.RawItems;
        while (_groupEmitIdx < _groupSize && count < matches.Length)
        {
            matches[count++] = entries[indexes[_groupEmitIdx++]];
        }
        if (_groupEmitIdx >= _groupSize)
        {
            _groupReady = false;
            _groupSize = 0;
        }
        return count;
    }

    // Loads all null-primary docs into the group buffer and sorts them by secondary value,
    // so null-primary entries obey the same per-group secondary sort as regular terms.
    private void PrepareNullGroup(Span<long> entryBuffer)
    {
        if (_nullGroupPrepared) return;
        _nullGroupPrepared = true;

        if (_hasNullPostingList == false || _nullExhausted)
            return;

        // Ensure group buffers are allocated before we fill them.
        if (_groupEntries.IsValid == false)
            _groupEntries.Initialize(_allocator, 1024);
        if (_groupSecondary.IsValid == false)
            _groupSecondary.Initialize(_allocator, 1024);
        if (_groupSortedIndexes.IsValid == false)
            _groupSortedIndexes.Initialize(_allocator, 1024);

        _groupSize = 0;
        DrainLargeIntoGroup(ref _nullIterator, entryBuffer);
        _nullExhausted = true;

        if (_groupSize > 0)
        {
            SortGroupBySecondary();
            _groupReady = true;
            _groupEmitIdx = 0;
        }
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
        return new QueryInspectionNode("SortedDrivingWithTieBreakMatch",
            parameters: new Dictionary<string, string>
            {
                ["Provider"] = _provider.Inspect().Operation,
                ["TieBreakType"] = _secondaryType.ToString(),
                ["TieBreakDescending"] = _secondaryDescending.ToString()
            });
    }

    public void Dispose()
    {
        _nullPostingList?.Dispose();
        _emittedBitmap.Dispose();
        _plIdsBuffer.Dispose(_allocator);
        _smallContainerItems.Dispose(_allocator);
        _groupEntries.Dispose(_allocator);
        _groupSecondary.Dispose(_allocator);
        _groupSortedIndexes.Dispose(_allocator);
    }
}
