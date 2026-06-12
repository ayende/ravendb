using System;
using Corax.Querying.Matches.Meta;
using Corax.Utils;
using Sparrow;
using Sparrow.Compression;
using Sparrow.Server;
using Voron.Data.Containers;
using Voron.Data.PostingLists;
using Voron.Impl;
using Voron.Util;

namespace Corax.Querying.Matches.TermsProviders;

/// <summary>
/// Shared posting-count machinery for the textual and numeric range providers. Both partition the in-range term ids
/// branchlessly into per-type buckets keyed by the low two bits of the id (0=Single, 1=SmallPostingList,
/// 2=PostingList; slot 3 is unused and asserted empty) and then read posting-list headers uniformly: singles count as
/// one apiece (no container), small/large lists are sorted by container id for page locality and have just their
/// header read — a small list's varint length prefix or a large list's <see cref="PostingListState.NumberOfEntries"/>.
/// No posting ids are decoded. Only the iteration that fills the buckets differs between providers (compact-key term
/// walk vs. numeric lookup walk), so that loop stays in each provider while the bucket lifecycle and the header read
/// live here.
/// </summary>
internal static class RangePostingBuckets
{
    // 0=Single, 1=SmallPostingList, 2=PostingList, 3=unused (kept so (termId & EnsureIsSingleMask) is always in range).
    public const int Count = 4;

    public static void Initialize(Span<NativeList<long>> buckets, ByteStringContext allocator)
    {
        for (int b = 0; b < buckets.Length; b++)
        {
            buckets[b] = new NativeList<long>();
            buckets[b].Initialize(allocator);
        }
    }

    public static void Release(Span<NativeList<long>> buckets, ByteStringContext allocator)
    {
        for (int b = 0; b < buckets.Length; b++)
            buckets[b].Dispose(allocator);
    }

    // Folds the filled buckets into the breakdown: singles need no container read; the small/large buckets have their
    // headers summed. The total postings plus the single/small/large split is the raw material the two-ended
    // range-cardinality probe extrapolates from.
    public static unsafe void Summarize(Span<NativeList<long>> buckets, ByteStringContext allocator, LowLevelTransaction llt, ref RangePostingStats stats)
    {
        if (buckets[3].Count > 0)
            throw new InvalidOperationException("Unknown TermIdMask type");

        stats.Singles = buckets[0].Count; // single = exactly one posting, no container read
        stats.SmallPostings = SumBucketPostings(buckets[1], allocator, llt, isLarge: false, out stats.Smalls);
        stats.LargePostings = SumBucketPostings(buckets[2], allocator, llt, isLarge: true, out stats.Larges);
        stats.Postings = stats.Singles + stats.SmallPostings + stats.LargePostings;
    }

    // Reads one posting-list bucket: strip the container ids, sort them so Container.GetAll walks pages in order, then
    // sum each list's header count. isLarge picks the decode once (outside the loop) so neither read path carries a
    // per-term branch.
    private static unsafe long SumBucketPostings(NativeList<long> bucket, ByteStringContext allocator, LowLevelTransaction llt, bool isLarge, out int count)
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
