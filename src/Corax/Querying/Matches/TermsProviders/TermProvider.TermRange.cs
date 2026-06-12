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

    /// <summary>
    /// Header-only walk over the in-range terms (capped at <paramref name="maxTerms"/>; 0 = all). Terms are
    /// partitioned branchlessly into per-type buckets keyed by the low two bits of the term id, then each bucket is
    /// read with one uniform pass: singles count as one apiece (no container), small/large posting lists are sorted
    /// by container id for page locality and have just their header read — a small list's varint length prefix or a
    /// large list's <see cref="PostingListState.NumberOfEntries"/>. No posting ids are decoded. The returned breakdown
    /// (total postings plus the single / small / large split and their sub-totals) is the raw material the two-ended
    /// range-cardinality probe extrapolates from.
    /// </summary>
    public unsafe RangePostingStats CountPostingsInRange(int maxTerms)
    {
        var stats = new RangePostingStats();
        if (_isEmpty)
            return stats;

        var allocator = _indexSearcher.Allocator;
        var llt = _indexSearcher._transaction.LowLevelTransaction;
        CompactKey compactKey = llt.AcquireCompactKey();

        // Branchless partition: (termId & EnsureIsSingleMask) -> 0=Single, 1=SmallPostingList, 2=PostingList. Slot 3
        // (0b11) is unused; we keep it so the index is always in range and assert it stays empty. Singles carry no
        // container, so their bucket is just a tally; the small/large buckets are read uniformly below.
        Span<NativeList<long>> buckets = stackalloc NativeList<long>[4];
        for (int b = 0; b < buckets.Length; b++)
        {
            buckets[b] = new NativeList<long>();
            buckets[b].Initialize(allocator);
        }

        try
        {
            while (_isEmpty == false && _iterator.MoveNext(compactKey, out var termId, out _))
            {
                if (termId == _endContainerId)
                {
                    _isEmpty = true;

                    if (_shouldIncludeLastTerm == false)
                        break;
                }

                int idx = (int)(termId & (long)TermIdMask.EnsureIsSingleMask);
                buckets[idx].Add(allocator, termId);
                stats.Terms++;

                if (maxTerms > 0 && stats.Terms >= maxTerms)
                    break;
            }

            if (buckets[3].Count > 0)
                throw new InvalidOperationException("Unknown TermIdMask type");

            stats.Singles = buckets[0].Count; // single = exactly one posting, no container read
            stats.SmallPostings = SumBucketPostings(buckets[1], isLarge: false, out stats.Smalls);
            stats.LargePostings = SumBucketPostings(buckets[2], isLarge: true, out stats.Larges);
            stats.Postings = stats.Singles + stats.SmallPostings + stats.LargePostings;

            return stats;
        }
        finally
        {
            for (int b = 0; b < buckets.Length; b++)
                buckets[b].Dispose(allocator);
            llt.ReleaseCompactKey(ref compactKey);
        }

        // Reads one posting-list bucket: strip the container ids, sort them so Container.GetAll walks pages in order,
        // then sum each list's header count. isLarge picks the decode once (outside the loop) so neither read path
        // carries a per-term branch.
        long SumBucketPostings(NativeList<long> bucket, bool isLarge, out int count)
        {
            count = bucket.Count;
            if (count == 0)
                return 0;

            var termIds = bucket.ToSpan();

            using var idsScope = allocator.Allocate(sizeof(long) * count, out ByteString idsBuffer);
            var ids = new Span<long>(idsBuffer.Ptr, count);
            for (int i = 0; i < count; i++)
                ids[i] = (long)EntryIdEncodings.GetContainerId(termIds[i]);
            ids.Sort();

            using var containersScope = allocator.Allocate(sizeof(UnmanagedSpan) * count, out ByteString containers);
            var containersPtr = (UnmanagedSpan*)containers.Ptr;
            Container.GetAll(llt, ids, new Span<UnmanagedSpan>(containersPtr, count), -1L, llt.PageLocator);

            long total = 0;
            if (isLarge)
            {
                for (int i = 0; i < count; i++)
                    total += ((PostingListState*)containersPtr[i].Address)->NumberOfEntries;
            }
            else
            {
                for (int i = 0; i < count; i++)
                    total += VariableSizeEncoding.Read<long>(containersPtr[i].Address, out _);
            }

            return total;
        }
    }

    /// <summary>
    /// Sub-linear estimate of how many distinct terms fall in this provider's range, forwarding to
    /// <see cref="CompactTree.GetNumberOfEntriesInRangeEstimate"/>. Open bounds are estimated directly: a
    /// "before all keys" low is the empty span (sorts before every term, descending the leftmost leaf) and an
    /// "after all keys" high descends the rightmost leaf, so an open-ended range counts to the edge of the tree.
    /// </summary>
    public long EstimateTermCountInRange()
    {
        if (_isEmpty)
            return 0;

        // A "before all keys" low bound is represented by the empty span, which sorts before every stored key.
        var lowSpan = _low.Options == SliceOptions.BeforeAllKeys ? ReadOnlySpan<byte>.Empty : _low.AsSpan();
        // An "after all keys" high has no concrete key to seek; signal the descent to walk to the rightmost leaf.
        return _tree.GetNumberOfEntriesInRangeEstimate(lowSpan, _high.AsSpan(), highToEnd: _high.Options == SliceOptions.AfterAllKeys);
    }

    /// <summary>Total number of terms stored for this field (O(1)); used by the cardinality combiner's whale guard.</summary>
    public long TotalTermCount() => _tree.NumberOfEntries;
}
