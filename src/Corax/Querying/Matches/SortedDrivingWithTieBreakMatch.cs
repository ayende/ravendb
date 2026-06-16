using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Corax.Indexing;
using Corax.Mappings;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.SortingMatches;
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
/// field. Walks the ITermsProvider in primary-term order; for each term, drains the
/// entire posting list into a per-term buffer, fetches secondary values via
/// Lookup&lt;Int64LookupKey&gt;.GetFor, sorts the buffer, then emits in sorted order.
///
/// Supports Integer, Floating, and Sequence (string/Slice) tie-break fields.
/// The planner must gate by per-term group size — this class caps groups at MaxGroupSize
/// and throws if exceeded.
///
/// Handles two-field ORDER BY queries where the first field drives the term walk
/// and the second field resolves ties within each primary term group, e.g.:
///   FROM Orders ORDER BY Status, CreatedAt DESC
///   FROM Users WHERE Age &gt; 18 ORDER BY Age, LastName
///
/// Same-field optimization applies as in SortedDrivingMatch: a WHERE on the primary
/// sort field narrows the TermsRangeProvider. Additional predicates on other fields
/// are not applied here — the wrapping <see cref="DirectScanMatch"/> handles residual
/// predicate evaluation on each yielded entry.
/// </summary>
public sealed unsafe class SortedDrivingWithTieBreakMatch : IQueryMatch, IDisposable
{
    private readonly ITermsProvider _provider;
    private readonly LowLevelTransaction _llt;
    private readonly ByteStringContext _allocator;
    private readonly Lookup<Int64LookupKey> _secondaryLookup;
    private readonly MatchCompareFieldType _secondaryType;
    private readonly bool _secondaryDescending;
    private readonly long _missingSecondaryValue;
    private readonly int _take;
    private readonly int _maxGroupSize;

    // String tie-break: container IDs for null/non-existing terms, resolved once in ctor.
    private readonly long _nullTermContainerId;
    private readonly long _nonExistingTermContainerId;

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
    // String tie-break scratch: resolved CompactKey blobs per group entry.
    private NativeList<UnmanagedSpan> _groupTerms;
    private int _groupEmitIdx;

    // Non-existing entries (docs where the primary sort field was absent) are treated as null-adjacent:
    //   nullFirst=true → non-existing, then nulls, then normal values
    //   nullFirst=false → normal values, then nulls, then non-existing
    private readonly bool _nullFirst;
    private readonly long _nullPostingListId;
    private readonly bool _hasNullPostingList;
    private bool _nullExhausted;

    private readonly long _nonExistingPostingListId;
    private readonly bool _hasNonExistingPostingList;
    private bool _nonExistingExhausted;

    // Tracks whether the null/non-existing primary group has been loaded and secondary-sorted.
    // These docs require the same per-group secondary sort as regular terms.
    private bool _nullGroupPrepared;

    public SortedDrivingWithTieBreakMatch(
        ITermsProvider provider,
        LowLevelTransaction llt,
        ByteStringContext allocator,
        IndexSearcher searcher,
        FieldMetadata primaryField,
        FieldMetadata secondaryField,
        MatchCompareFieldType secondaryType,
        bool secondaryDescending,
        bool nullFirst,
        bool nullIsSmallest,
        int take)
    {
        if (secondaryType is not (MatchCompareFieldType.Integer or MatchCompareFieldType.Floating or MatchCompareFieldType.Sequence))
            throw new NotSupportedException($"SortedDrivingWithTieBreakMatch only supports Integer, Floating, or Sequence tie-break fields (got {secondaryType})");

        _provider = provider;
        _llt = llt;
        _allocator = allocator;
        _nullFirst = nullFirst;
        _secondaryType = secondaryType;
        _secondaryDescending = secondaryDescending;
        // When take is unbounded (TakeAll = -1) or very large, disable the group truncation
        // by setting _maxGroupSize to int.MaxValue — the group grows as needed without truncation.
        if (take is Constants.IndexSearcher.TakeAll || take > int.MaxValue / 4)
        {
            _take = int.MaxValue;
            _maxGroupSize = int.MaxValue;
        }
        else
        {
            _take = Math.Max(take, 1);
            _maxGroupSize = Math.Max(
                RoaringBitmap.PadToVector256Width(_take * 4),
                QueryPrimitives.TieBreakGroupInitialCapacity);
        }
        _emittedBitmap = new RoaringBitmap(allocator);

        // Resolve the secondary Lookup using the type-specific field name (long/double suffix).
        Slice secondaryLookupName;
        switch (secondaryType)
        {
            case MatchCompareFieldType.Integer:
                IndexFieldsMappingBuilder.GetFieldNameForLongs(searcher.Allocator, secondaryField.FieldName, out secondaryLookupName);
                _missingSecondaryValue = nullIsSmallest ? long.MinValue : long.MaxValue;
                break;
            case MatchCompareFieldType.Floating:
                IndexFieldsMappingBuilder.GetFieldNameForDoubles(searcher.Allocator, secondaryField.FieldName, out secondaryLookupName);
                _missingSecondaryValue = BitConverter.DoubleToInt64Bits(nullIsSmallest ? double.MinValue : double.MaxValue);
                break;
            default:
                // Sequence: the lookup maps entry IDs to term container IDs (no type suffix).
                secondaryLookupName = secondaryField.FieldName;
                _missingSecondaryValue = SortingHelpers.MissingTermId;

                // Resolve null/non-existing term container IDs for the string path.
                if (searcher.TryGetPostingListForNull(secondaryField.FieldName, out _, out _nullTermContainerId) == false)
                    _nullTermContainerId = SortingHelpers.InvalidTermId;
                if (searcher.TryGetPostingListForNonExisting(secondaryField.FieldName, out _, out _nonExistingTermContainerId) == false)
                    _nonExistingTermContainerId = SortingHelpers.InvalidTermId;
                break;
        }
        _secondaryLookup = searcher.EntriesToTermsReader(secondaryLookupName);

        _hasNullPostingList = searcher.TryGetPostingListForNull(in primaryField, out _nullPostingListId);
        _nullExhausted = !_hasNullPostingList;

        _hasNonExistingPostingList = searcher.TryGetPostingListForNonExisting(in primaryField, out _nonExistingPostingListId);
        _nonExistingExhausted = !_hasNonExistingPostingList;

        // Allocate all persistent buffers up front — avoids 7 IsValid branches per Fill call.
        _plIdsBuffer.Initialize(allocator, QueryPrimitives.EntryScanBatchSize);
        _smallContainerItems.Initialize(allocator, QueryPrimitives.EntryScanBatchSize);
        _groupEntries.Initialize(allocator, QueryPrimitives.TieBreakGroupInitialCapacity);
        _groupSecondary.Initialize(allocator, QueryPrimitives.TieBreakGroupInitialCapacity);
        _groupSortedIndexes.Initialize(allocator, QueryPrimitives.TieBreakGroupInitialCapacity);
        if (secondaryType == MatchCompareFieldType.Sequence)
            _groupTerms.Initialize(allocator, QueryPrimitives.TieBreakGroupInitialCapacity);
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
        if (_groupEntries.Count > 0)
        {
            count += EmitFromSortedGroup(matches[count..]);
            if (count >= matches.Length)
                return count;
        }

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

            // Drain the next primary term's posting list into _groupEntries, sort, then emit.
            while (_plIdsIdx < _plIdsRead && count < matches.Length)
            {
                long plId = _plIdsBuffer.RawItems[_plIdsIdx];
                _groupEntries.Clear();
                var termType = (TermIdMask)plId & TermIdMask.EnsureIsSingleMask;
                switch (termType)
                {
                    case TermIdMask.Single:
                    {
                        long entryId = (long)EntryIdEncodings.GetContainerId(plId);
                        _plIdsIdx++;
                        if (_emittedBitmap.Contains(entryId))
                            continue;
                        _emittedBitmap.Add(entryId);
                        matches[count++] = entryId;
                        continue; // skip the group sort/emit path
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
                        using var pl = new PostingList(_llt, Slices.Empty, in setState);
                        var iter = pl.Iterate();
                        DrainLargeIntoGroup(ref iter, entryBuffer);
                        _plIdsIdx++;
                        break;
                    }
                    default:
                        throw new ArgumentException($"Unexpected TermIdMask value {termType} for plId {plId}");
                }

                if (_groupEntries.Count == 0)
                    continue;

                SortGroupBySecondary();
                count += EmitFromSortedGroup(matches[count..]);
                if (count >= matches.Length)
                    return count;
            }
        }

        // After the provider is exhausted, load and emit null-primary group (nulls-last).
        if (_providerExhausted && _nullFirst == false && _nullGroupPrepared == false)
        {
            PrepareNullGroup(entryBuffer);
            if (_groupEntries.Count> 0)
                count += EmitFromSortedGroup(matches[count..]);
        }

        return count;
    }

    private void EnsureGroupCapacity(int required)
    {
        int curCap = _groupEntries.Capacity;
        if (required <= curCap)
            return;

        int newCap = curCap;
        while (newCap < required)
            newCap = (int)Math.Min((long)newCap * 2, _maxGroupSize);
        int addition = newCap - curCap;

        _groupEntries.Grow(_allocator, addition);
        _groupSecondary.Grow(_allocator, addition);
        _groupSortedIndexes.Grow(_allocator, addition);
        if (_groupTerms.IsValid)
            _groupTerms.Grow(_allocator, addition);
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
                AddToGroup(entryBuffer, read);
            }
        }
    }

    private void DrainLargeIntoGroup(ref PostingList.Iterator iter, Span<long> entryBuffer)
    {
        while (true)
        {
            if (iter.Fill(entryBuffer, out int read) == false || read == 0)
                break;
            AddToGroup(entryBuffer, read);
        }
    }

    // Drains a null/non-existing posting list into the group. The stored id may encode a
    // single entry, a small posting list, or a large posting list — exactly like a regular
    // term id — so it must be dispatched on TermIdMask. Treating it unconditionally as a
    // large PostingList reinterprets a small-list/single container blob as a PostingListState,
    // whose bogus RootPage points at an unrelated (document) page that then decodes as garbage.
    private void DrainSpecialIntoGroup(long postingListId, Span<long> entryBuffer)
    {
        var termType = (TermIdMask)postingListId & TermIdMask.EnsureIsSingleMask;
        switch (termType)
        {
            case TermIdMask.Single:
            {
                long entryId = (long)EntryIdEncodings.GetContainerId(postingListId);
                if (_emittedBitmap.Contains(entryId))
                    return;
                _emittedBitmap.Add(entryId);
                EnsureGroupCapacity(_groupEntries.Count + 1);
                _groupEntries.AddUnsafe(entryId);
                break;
            }
            case TermIdMask.SmallPostingList:
            {
                Container.Get(_llt, EntryIdEncodings.GetContainerId(postingListId), out var item);
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
                break;
            }
            case TermIdMask.PostingList:
            {
                InitPostingList(out var pl, out var iter, postingListId);
                using (pl)
                    DrainLargeIntoGroup(ref iter, entryBuffer);
                break;
            }
            default:
                throw new ArgumentException($"Unexpected TermIdMask value {termType} for special posting list id {postingListId}");
        }
    }

    private void AddToGroup(Span<long> entryBuffer, int read)
    {
        EntryIdEncodings.DecodeAndDiscardFrequency(entryBuffer[..read], read);
        int newCount = _emittedBitmap.DedupAddNew(entryBuffer, read);
        if (newCount == 0)
            return;
        if (_groupEntries.Count + newCount >= _maxGroupSize)
            TruncateGroupToTopTake();
        EnsureGroupCapacity(_groupEntries.Count + newCount);
        entryBuffer[..newCount].CopyTo(
            new Span<long>(_groupEntries.RawItems + _groupEntries.Count, newCount));
        _groupEntries.Count += newCount;
    }

    /// <summary>Keep only the top <see cref="_take"/> entries of the current group by secondary value,
    /// discarding the rest. Uses a bounded max-heap of size <see cref="_take"/> over the resolved
    /// secondary values (O(n log take)) instead of a full O(n log n) sort — the survivors are the
    /// same set, and the final per-group <see cref="SortGroupBySecondary"/> orders them for emission.
    /// Only ever called on the bounded-take path (TakeAll never reaches <see cref="_maxGroupSize"/>).</summary>
    private void TruncateGroupToTopTake()
    {
        int n = _groupEntries.Count;
        if (n <= _take)
            return;

        ResolveGroupSecondary();

        // Max-heap (keyed by CmpKeepRank, root = worst-to-keep) of group-entry indices, capacity _take.
        // _groupSortedIndexes capacity tracks the group capacity, so it always has room for _take indices.
        var heap = _groupSortedIndexes.RawItems;
        int heapSize = 0;
        for (int i = 0; i < n; i++)
        {
            if (heapSize < _take)
            {
                heap[heapSize] = i;
                HeapSiftUp(heap, heapSize);
                heapSize++;
            }
            else if (CmpKeepRank(i, heap[0]) < 0)
            {
                // i ranks before the current worst survivor — replace it.
                heap[0] = i;
                HeapSiftDown(heap, heapSize);
            }
        }

        // Compact survivors to the front of _groupEntries. _groupSecondary is free to use as scratch
        // here: its resolved values were already consumed while building the heap above.
        var entries = _groupEntries.RawItems;
        var scratch = _groupSecondary.RawItems;
        for (int i = 0; i < heapSize; i++)
            scratch[i] = entries[heap[i]];
        new Span<long>(scratch, heapSize).CopyTo(new Span<long>(entries, heapSize));

        _groupSortedIndexes.Count = _groupSecondary.Count = _groupEntries.Count = heapSize;
    }

    /// <summary>Comparison in "keep rank" order: returns &gt;0 when entry <paramref name="a"/> should be
    /// emitted AFTER entry <paramref name="b"/> (i.e. is worse to keep). Mirrors the ascending-sort +
    /// descending-on-read emit order of <see cref="SortGroupBySecondary"/>/<see cref="EmitFromSortedGroup"/>:
    /// ascending keeps the smallest values, descending keeps the largest. Both arguments are indices into
    /// the resolved secondary buffers (<see cref="_groupSecondary"/> / <see cref="_groupTerms"/>).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int CmpKeepRank(int a, int b)
    {
        switch (_secondaryType)
        {
            case MatchCompareFieldType.Integer:
            {
                long va = _groupSecondary.RawItems[a];
                long vb = _groupSecondary.RawItems[b];
                return _secondaryDescending ? vb.CompareTo(va) : va.CompareTo(vb);
            }
            case MatchCompareFieldType.Floating:
            {
                double va = BitConverter.Int64BitsToDouble(_groupSecondary.RawItems[a]);
                double vb = BitConverter.Int64BitsToDouble(_groupSecondary.RawItems[b]);
                return _secondaryDescending ? vb.CompareTo(va) : va.CompareTo(vb);
            }
            default: // Sequence — null sorts first (ascending), matching SortBySlice's SliceComparer.
            {
                var ta = _groupTerms.RawItems[a];
                var tb = _groupTerms.RawItems[b];
                return _secondaryDescending
                    ? CompactKeyComparer.Compare(tb, ta, 1)
                    : CompactKeyComparer.Compare(ta, tb, 1);
            }
        }
    }

    private void HeapSiftUp(int* heap, int i)
    {
        while (i > 0)
        {
            int parent = (i - 1) / 2;
            if (CmpKeepRank(heap[parent], heap[i]) >= 0)
                break;
            (heap[parent], heap[i]) = (heap[i], heap[parent]);
            i = parent;
        }
    }

    private void HeapSiftDown(int* heap, int size)
    {
        int i = 0;
        while (true)
        {
            int left = i * 2 + 1;
            int right = i * 2 + 2;
            int largest = i;
            if (left < size && CmpKeepRank(heap[left], heap[largest]) > 0)
                largest = left;
            if (right < size && CmpKeepRank(heap[right], heap[largest]) > 0)
                largest = right;
            if (largest == i)
                break;
            (heap[largest], heap[i]) = (heap[i], heap[largest]);
            i = largest;
        }
    }

    /// <summary>Resolve the secondary values for the current group into <see cref="_groupSecondary"/>
    /// (and <see cref="_groupTerms"/> for Sequence), without sorting. Shared by the bounded top-K
    /// selection in <see cref="TruncateGroupToTopTake"/>.</summary>
    private void ResolveGroupSecondary()
    {
        var entriesSpan = _groupEntries.ToSpan();
        var secondarySpan = new Span<long>(_groupSecondary.RawItems, _groupSecondary.Capacity);
        switch (_secondaryType)
        {
            case MatchCompareFieldType.Integer:
            case MatchCompareFieldType.Floating:
                SortKernels.ResolveLongs(_secondaryLookup, entriesSpan, secondarySpan, _missingSecondaryValue);
                break;
            case MatchCompareFieldType.Sequence:
                var termsSpan = new Span<UnmanagedSpan>(_groupTerms.RawItems, _groupTerms.Capacity);
                SortKernels.ResolveSlices(_secondaryLookup, _llt, _llt.PageLocator,
                    entriesSpan, secondarySpan, termsSpan,
                    _nullTermContainerId, _nonExistingTermContainerId);
                break;
        }
    }

    private void SortGroupBySecondary()
    {
        var entriesSpan = _groupEntries.ToSpan();
        var secondarySpan = new Span<long>(_groupSecondary.RawItems, _groupSecondary.Capacity);
        var indexesSpan = new Span<int>(_groupSortedIndexes.RawItems, _groupSortedIndexes.Capacity);
        _groupEmitIdx = 0;
        switch (_secondaryType)
        {
            case MatchCompareFieldType.Integer:
                SortKernels.SortByLong(_secondaryLookup, entriesSpan, secondarySpan, indexesSpan, _missingSecondaryValue);
                break;
            case MatchCompareFieldType.Floating:
                SortKernels.SortByDouble(_secondaryLookup, entriesSpan, secondarySpan, indexesSpan, _missingSecondaryValue);
                break;
            case MatchCompareFieldType.Sequence:
                var termsSpan = new Span<UnmanagedSpan>(_groupTerms.RawItems, _groupTerms.Capacity);
                SortKernels.SortBySlice(_secondaryLookup, _llt, _llt.PageLocator,
                    entriesSpan, secondarySpan, termsSpan, indexesSpan,
                    _nullTermContainerId, _nonExistingTermContainerId);
                break;
            default:
                Debug.Assert(false, $"Unexpected secondary type {_secondaryType}");
                break;
        }
    }

    private int EmitFromSortedGroup(Span<long> matches)
    {
        var entries = _groupEntries.RawItems;
        var indexes = _groupSortedIndexes.RawItems;
        int remaining = _groupEntries.Count - _groupEmitIdx;
        int toEmit = Math.Min(remaining, matches.Length);
        var (pos, step) = _secondaryDescending ? ( _groupEntries.Count - 1 - _groupEmitIdx, -1) : (_groupEmitIdx, 1);
        for (int i = 0; i < toEmit; i++, pos += step)
            matches[i] = entries[indexes[pos]];
        _groupEmitIdx += toEmit;
        if (_groupEmitIdx >=  _groupEntries.Count)
            _groupEntries.Count = 0;
        return toEmit;
    }

    // Loads all null-primary docs into the group buffer and sorts them by secondary value,
    // so null-primary entries obey the same per-group secondary sort as regular terms.
    private void PrepareNullGroup(Span<long> entryBuffer)
    {
        if (_nullGroupPrepared) return;
        _nullGroupPrepared = true;

        bool hasNull = _hasNullPostingList && _nullExhausted == false;
        bool hasNonExisting = _hasNonExistingPostingList && _nonExistingExhausted == false;
        if (hasNull == false && hasNonExisting == false)
            return;

        _groupEntries.Count = 0;

        // Drain both null and non-existing entries into a single group; the secondary sort
        // determines their interleaved order within the group.
        if (hasNonExisting)
        {
            DrainSpecialIntoGroup(_nonExistingPostingListId, entryBuffer);
            _nonExistingExhausted = true;
        }
        if (hasNull)
        {
            DrainSpecialIntoGroup(_nullPostingListId, entryBuffer);
            _nullExhausted = true;
        }

        if ( _groupEntries.Count <= 0) return;
        SortGroupBySecondary();
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
        _emittedBitmap.Dispose();
        _plIdsBuffer.Dispose(_allocator);
        _smallContainerItems.Dispose(_allocator);
        _groupEntries.Dispose(_allocator);
        _groupSecondary.Dispose(_allocator);
        _groupSortedIndexes.Dispose(_allocator);
        _groupTerms.Dispose(_allocator);
    }
}
