using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Corax.Indexing;
using Corax.Mappings;
using Corax.Querying.Matches.Meta;
using Corax.Utils;
using Sparrow;
using Sparrow.Compression;
using Sparrow.Server;
using Voron;
using Voron.Data.CompactTrees;
using Voron.Data.Containers;
using Voron.Data.Lookups;
using Voron.Data.PostingLists;
using Voron.Util;
using Range = Corax.Querying.Matches.Meta.Range;

namespace Corax.Querying.Matches.TermsProviders;

[DebuggerDisplay("{DebugView,nq}")]
public struct TermsRangeProvider<TLookupIterator, TLow, THigh> : ITermsProvider, IAggregationProvider
    where TLookupIterator : struct, ILookupIterator
    where TLow : struct, Range.Marker
    where THigh : struct, Range.Marker
{
    private readonly IndexSearcher _indexSearcher;
    private readonly FieldMetadata _field;
    private readonly CompactTree _tree;
    private Slice _low, _high;

    private CompactTree.Iterator<TLookupIterator> _iterator;

    private readonly bool _isForward;
    private bool _skipRangeCheck;
    private bool _isEmpty;
    private bool _shouldIncludeLastTerm;
    private long _endContainerId;

    public TermsRangeProvider(Querying.IndexSearcher indexSearcher, CompactTree tree, in FieldMetadata field, Slice low, Slice high)
    {
        _indexSearcher = indexSearcher;
        _field = field;
        _tree = tree;
        _iterator = tree.Iterate<TLookupIterator>();
        _isForward = default(TLookupIterator).IsForward;


        _low = low;
        _high = high;

        // Optimization for unbounded ranges. We seek the proper term (depending on the iterator) and iterate through all left items.
        _skipRangeCheck = _isForward
            ? _high.Options is SliceOptions.AfterAllKeys
            : _low.Options is SliceOptions.BeforeAllKeys;
        PrepareKeys();
        Reset();
    }


    private void PrepareKeys()
    {
        CompactKey key;
        ReadOnlySpan<byte> termSlice;

        var startKey = _isForward ? _low : _high;
        var finalKey = _isForward ? _high : _low;

        if (ShouldSeek())
        {
            _iterator.Seek(startKey);
            if (_iterator.MoveNext(out key, out _, out _) == false)
            {
                _isEmpty = true;
                return; //empty set, we will go out of range immediately 
            }

            termSlice = key.Decoded();
            var shouldInclude = _isForward switch
            {
                false when typeof(THigh) == typeof(Range.Exclusive) && termSlice.SequenceCompareTo(_high.AsSpan()) >= 0 => false,
                false when typeof(THigh) == typeof(Range.Inclusive) && _high.Options != SliceOptions.AfterAllKeys &&
                           termSlice.SequenceCompareTo(_high.AsSpan()) > 0 => false,
                true when typeof(TLow) == typeof(Range.Exclusive) && termSlice.SequenceCompareTo(_low.AsSpan()) <= 0 => false,
                true when typeof(TLow) == typeof(Range.Inclusive) && _low.Options != SliceOptions.BeforeAllKeys &&
                          termSlice.SequenceCompareTo(_low.AsSpan()) < 0 => false,
                _ => true
            };

            if (shouldInclude == false)
            {
                if (_iterator.MoveNext(out key, out _, out _) == false)
                {
                    _isEmpty = true;
                    return; //empty set, we will go out of range immediately
                }

                termSlice = key.Decoded();

                //Next seek will go immediately to the right term.
                if (_isForward)
                    Slice.From(_indexSearcher.Allocator, termSlice, out _low);
                else
                    Slice.From(_indexSearcher.Allocator, termSlice, out _high);
            }
        }

        if (_skipRangeCheck)
        {
            // In this case we will accept all items left.
            _endContainerId = long.MaxValue;
            _shouldIncludeLastTerm = true;
            return;
        }


        _iterator.Seek(finalKey);
        if (_iterator.MoveNext(out key, out _endContainerId, out var hasPreviousValue) == false)
        {
            _skipRangeCheck = true; //we are out of item anyway that means we can accept all items
            _endContainerId = long.MaxValue;
            return;
        }

        termSlice = key.Decoded();
        var finalCmp = termSlice.SequenceCompareTo(finalKey.AsSpan());

        _shouldIncludeLastTerm = _isForward switch
        {
            false when typeof(TLow) == typeof(Range.Exclusive) && finalCmp <= 0 => false,
            false when typeof(TLow) == typeof(Range.Inclusive) && finalCmp < 0 => false,
            true when typeof(THigh) == typeof(Range.Exclusive) && finalCmp >= 0 => false,
            true when typeof(THigh) == typeof(Range.Inclusive) && _high.Options != SliceOptions.AfterAllKeys && finalCmp > 0 => false,
            _ => true
        };
        if (_shouldIncludeLastTerm == false && hasPreviousValue == false)
        {
            _isEmpty = true;
        }
    }

    public int FillPostingListIds(Span<long> postingListIds)
    {
        if (_isEmpty)
            return 0;

        return _iterator.Fill(postingListIds, _endContainerId, _shouldIncludeLastTerm);
    }

    public void Reset()
    {
        var shouldSeek = ShouldSeek();
        if (shouldSeek)
            _iterator.Seek(_isForward ? _low : _high);
        else
            _iterator.Reset();
    }

    private bool ShouldSeek()
    {
        return _isForward switch
        {
            true when _low.Options != SliceOptions.BeforeAllKeys => true,
            false when _high.Options != SliceOptions.AfterAllKeys => true,
            _ => false
        };
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Next(out TermMatch term)
    {
        if (_isEmpty || _iterator.MoveNext(out var termId) == false)
            goto ReturnEmpty;


        if (termId == _endContainerId)
        {
            _isEmpty = true;

            if (_shouldIncludeLastTerm == false)
                goto ReturnEmpty;
        }

        term = _indexSearcher.TermQuery(_field, termId, 1D);
        return true;

        ReturnEmpty:
        term = TermMatch.CreateEmpty(_indexSearcher, _indexSearcher.Allocator);
        return false;
    }

    public QueryInspectionNode Inspect()
    {
        var lowValue = _low.Options is SliceOptions.BeforeAllKeys
            ? null
            : _low.ToString();

        var highValue = _high.Options is SliceOptions.AfterAllKeys
            ? null
            : _high.ToString();

        return new QueryInspectionNode(nameof(TermsRangeProvider<TLookupIterator, TLow, THigh>),
            parameters: new Dictionary<string, string>()
            {
                { Constants.QueryInspectionNode.FieldName, _field.FieldName.ToString() },
                { Constants.QueryInspectionNode.LowValue, lowValue },
                { Constants.QueryInspectionNode.HighValue, highValue },
                { Constants.QueryInspectionNode.LowOption, typeof(TLow).Name },
                { Constants.QueryInspectionNode.HighOption, typeof(THigh).Name },
                { Constants.QueryInspectionNode.IteratorDirection, Constants.QueryInspectionNode.IterationDirectionName<TLookupIterator>() }
            });
    }

    public string DebugView => Inspect().ToString();

    public IDisposable AggregateByTerms(out List<string> terms, out Span<long> counts)
    {
        throw new NotImplementedException();
    }

    public long AggregateByRange()
    {
        //we do not support Long ranges since we want to perform aggregation on doubles
        if (_isEmpty)
            return 0;

        // maxTerms: 0 -> scan every in-range term, giving the exact (multi-valued-overcounting) total.
        return CountPostingsInRange(maxTerms: 0).Postings;
    }

    // One in-range term, captured for the batched header read. The type tag (Single/Small/Large) lives in the low
    // two bits of the term id, so it must travel alongside the resolved container id once we strip them off.
    private readonly struct RangeSample : IComparable<RangeSample>
    {
        public readonly long ContainerId;
        public readonly TermIdMask Type;

        public RangeSample(long containerId, TermIdMask type)
        {
            ContainerId = containerId;
            Type = type;
        }

        // Sort by container id so Container.GetAll walks the container pages in ascending order.
        public int CompareTo(RangeSample other) => ContainerId.CompareTo(other.ContainerId);
    }

    /// <summary>
    /// Header-only walk over the in-range terms (capped at <paramref name="maxTerms"/>; 0 = all). For each term we
    /// classify it branchlessly from the low two bits of the term id, resolve its posting-list container id, sort the
    /// batch by container id for page locality, then read just the header of each container — a small posting list's
    /// varint length prefix or a large posting list's <see cref="PostingListState.NumberOfEntries"/>; singles count as
    /// one. No posting ids are decoded. The returned breakdown (total postings plus the single / small / large split
    /// and their sub-totals) is the raw material the two-ended range-cardinality probe extrapolates from.
    /// </summary>
    public unsafe RangePostingStats CountPostingsInRange(int maxTerms)
    {
        var stats = new RangePostingStats();
        if (_isEmpty)
            return stats;

        const long singleMarker = -1L;
        var allocator = _indexSearcher.Allocator;
        var llt = _indexSearcher._transaction.LowLevelTransaction;
        CompactKey compactKey = llt.AcquireCompactKey();

        NativeList<RangeSample> samples = new();
        samples.Initialize(allocator);

        while (_isEmpty == false && _iterator.MoveNext(compactKey, out var termId, out _))
        {
            if (termId == _endContainerId)
            {
                _isEmpty = true;

                if (_shouldIncludeLastTerm == false)
                    break;
            }

            // Single=0b00, SmallPostingList=0b01, PostingList=0b10 are mutually exclusive in the low two bits, so the
            // mask classifies in one step (no ordered bit tests). Singles have no container -> the marker id.
            var type = (TermIdMask)(termId & (long)TermIdMask.EnsureIsSingleMask);
            long id = type == TermIdMask.Single ? singleMarker : (long)EntryIdEncodings.GetContainerId(termId);
            samples.Add(allocator, new RangeSample(id, type));

            if (maxTerms > 0 && samples.Count >= maxTerms)
                break;
        }

        stats.Terms = samples.Count;
        if (samples.Count == 0)
        {
            samples.Dispose(allocator);
            llt.ReleaseCompactKey(ref compactKey);
            return stats;
        }

        var sampleSpan = samples.ToSpan();
        sampleSpan.Sort();

        using var idsScope = allocator.Allocate(sizeof(long) * samples.Count, out ByteString idsBuffer);
        var ids = new Span<long>(idsBuffer.Ptr, samples.Count);
        for (int i = 0; i < samples.Count; i++)
            ids[i] = sampleSpan[i].ContainerId;

        using var containersScope = allocator.Allocate(sizeof(UnmanagedSpan) * samples.Count, out ByteString containers);
        var containersPtr = (UnmanagedSpan*)containers.Ptr;
        Container.GetAll(llt, ids, new Span<UnmanagedSpan>(containersPtr, samples.Count), singleMarker, llt.PageLocator);

        for (int i = 0; i < samples.Count; i++)
        {
            switch (sampleSpan[i].Type)
            {
                case TermIdMask.PostingList:
                    long large = ((PostingListState*)containersPtr[i].Address)->NumberOfEntries;
                    stats.Larges++;
                    stats.LargePostings += large;
                    stats.Postings += large;
                    break;
                case TermIdMask.SmallPostingList:
                    long small = VariableSizeEncoding.Read<long>(containersPtr[i].Address, out _);
                    stats.Smalls++;
                    stats.SmallPostings += small;
                    stats.Postings += small;
                    break;
                default: // Single
                    stats.Singles++;
                    stats.Postings += 1;
                    break;
            }
        }

        samples.Dispose(allocator);
        llt.ReleaseCompactKey(ref compactKey);
        return stats;
    }

    /// <summary>
    /// Sub-linear estimate of how many distinct terms fall in this provider's range, forwarding to
    /// <see cref="CompactTree.GetNumberOfEntriesInRangeEstimate"/>. Returns -1 when a bound is open-ended (there is no
    /// concrete high key to seek to), which tells the cardinality combiner it cannot estimate this range cheaply.
    /// </summary>
    public long EstimateTermCountInRange()
    {
        if (_isEmpty)
            return 0;

        // An open high bound (AfterAllKeys) has no concrete key to seek; let the caller fall back.
        if (_high.Options == SliceOptions.AfterAllKeys)
            return -1;

        // A "before all keys" low bound is represented by the empty span, which sorts before every stored key.
        var lowSpan = _low.Options == SliceOptions.BeforeAllKeys ? ReadOnlySpan<byte>.Empty : _low.AsSpan();
        return _tree.GetNumberOfEntriesInRangeEstimate(lowSpan, _high.AsSpan());
    }

    /// <summary>Total number of terms stored for this field (O(1)); used by the cardinality combiner's whale guard.</summary>
    public long TotalTermCount() => _tree.NumberOfEntries;
}
