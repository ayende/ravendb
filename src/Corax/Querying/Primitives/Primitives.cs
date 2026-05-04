using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Corax.Indexing;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Utils;
using Voron.Data.RoaringBitmaps;
using Sparrow;
using Sparrow.Compression;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Voron;
using Voron.Data.Containers;
using Voron.Data.PostingLists;
using Voron.Impl;
using Voron.Util;
using Voron.Util.PFor;

namespace Corax.Querying.Primitives;

/// <summary>
/// Static methods called by compiled query functions (DynamicMethod IL).
/// Each primitive operates on a RoaringBitmapData accumulator.
/// Pre-compiled, SIMD-tuned, individually benchmarkable.
/// </summary>
public static class QueryPrimitives
{
    // Buffer size for stackalloc Fill operations.
    private const int FillBufferSize = 4096;

    // Batch size for entry scan: how many bitmap entries to read per iteration.
    private const int EntryScanBatchSize = 256;

    // Bitmap count threshold below which entry scan is considered cheaper than
    // bitmap AND with a posting list. Below this, individual entry blob reads
    // are cheaper than decoding the full posting list.
    private const long EntryScanCountThreshold = 32_000;

    // Cost multiplier: entry scan is chosen when bitmapCount * EntryScanCostMultiplier
    // is less than the posting list size. Approximates the relative cost of reading
    // entry blobs vs. decoding posting list pages.
    private const long EntryScanCostMultiplier = 64;

    /// <summary>
    /// Fill a bitmap from a posting list. Walks leaf pages, decodes PFor blocks,
    /// adds entries to the bitmap via batch AddRange.
    /// Stops once <paramref name="limit"/> entries have been added; the final batch
    /// is truncated so the bitmap never overshoots the requested limit.
    /// </summary>
    [SkipLocalsInit]
    public static void FillFromPostings(ref PostingList.Iterator iterator, ref RoaringBitmapData data, ByteStringContext allocator, long limit = long.MaxValue)
    {
        scoped RoaringBitmap bitmap = new(ref data, allocator);
        Span<long> buffer = stackalloc long[FillBufferSize];

        // Track running total locally — AddRange leaves bitmap-container cardinalities
        // dirty (LazyCardinality), so bitmap.Count would be unreliable mid-loop without
        // an explicit RepairAfterLazy call. Posting lists have no duplicates within a
        // single iterator, so running-total matches what bitmap.Count would report.
        long total = 0;
        while (iterator.Fill(buffer, out int read) && read > 0)
        {
            // Posting list Fill may encode frequencies in the high bits — decode
            EntryIdEncodings.DecodeAndDiscardFrequency(buffer, read);

            // Truncate the final batch when it would push us past `limit`. Without this
            // the bitmap could overshoot by up to FillBufferSize (4096) entries.
            long remaining = limit - total;
            if (remaining < read)
            {
                if (remaining <= 0)
                    break;
                read = (int)remaining;
                bitmap.AddRange(buffer.Slice(0, read));
                break;
            }

            bitmap.AddRange(buffer.Slice(0, read));
            total += read;
        }
    }

    /// <summary>
    /// AND bitmap with posting list using galloping page-scan.
    /// Uses bitmap's container key range to bound the posting list scan:
    /// Seek() jumps past entries below the bitmap's min, and pruneGreaterThan
    /// stops reading past the bitmap's max. Only posting list pages that overlap
    /// with the bitmap's entry ID range are read.
    ///
    /// For a 50K bitmap vs 10M posting list, this reads only the pages covering
    /// the 50K range instead of all 10M entries.
    /// </summary>
    [SkipLocalsInit]
    public static void AndWithPostings(ref PostingList.Iterator iterator, ref RoaringBitmapData data, ref RoaringBitmapData tempData, ByteStringContext allocator)
    {
        if (data.IsEmpty)
            return;

        scoped RoaringBitmap bitmap = new(ref data, allocator);
        scoped RoaringBitmap tempBitmap = new(ref tempData, allocator);
        tempBitmap.Clear();

        // Bound the posting list scan to the bitmap's container key range.
        // Each container key covers 65536 entry IDs.
        long minKey = data.MinContainerKey;
        long maxKey = data.MaxContainerKey;
        Debug.Assert(minKey is not -1 && maxKey is not -1,"shouldn't happen, we checked IsEmpty") ;

        long seekFrom = minKey * RoaringBitmap.ContainerSize;
        long pruneAfter = (maxKey + 1) * RoaringBitmap.ContainerSize - 1;

        // Seek past all posting list entries below the bitmap's range
        if (!iterator.Seek(seekFrom))
        {
            // No entries at or after seekFrom — nothing to AND
            bitmap.Clear();
            return;
        }

        // Fill only entries within the bitmap's range
        Span<long> buffer = stackalloc long[FillBufferSize];
        int read;
        bool hasMore;
        do
        {
            hasMore = iterator.Fill(buffer, out read, pruneAfter);
            if (read > 0)
            {
                EntryIdEncodings.DecodeAndDiscardFrequency(buffer, read);
                tempBitmap.AddRange(buffer.Slice(0, read));
            }
        } while (hasMore && read > 0);

        bitmap.AndWith(ref tempBitmap);
    }

    /// <summary>
    /// OR the bitmap with a posting list. Walks all leaf pages, sets bits.
    /// Idempotent — setting an already-set bit is a no-op.
    /// </summary>
    [SkipLocalsInit]
    public static void OrWithPostings(ref PostingList.Iterator iterator, ref RoaringBitmapData data, ByteStringContext allocator)
    {
        scoped RoaringBitmap bitmap = new(ref data, allocator);
        Span<long> buffer = stackalloc long[FillBufferSize];
        while (iterator.Fill(buffer, out int read) && read > 0)
        {
            EntryIdEncodings.DecodeAndDiscardFrequency(buffer, read);
            bitmap.AddRange(buffer.Slice(0, read));
        }
    }

    /// <summary>
    /// ANDNOT the bitmap with a posting list. Same galloping bounds as AndWith —
    /// only reads posting list pages that overlap with the bitmap's container range.
    /// </summary>
    [SkipLocalsInit]
    public static void AndNotWithPostings(ref PostingList.Iterator iterator, ref RoaringBitmapData data, ref RoaringBitmapData tempData, ByteStringContext allocator)
    {
        if (data.IsEmpty)
            return;

        scoped RoaringBitmap bitmap = new(ref data, allocator);
        scoped RoaringBitmap tempBitmap = new(ref tempData, allocator);
        tempBitmap.Clear();

        long minKey = data.MinContainerKey;
        long maxKey = data.MaxContainerKey;
        Debug.Assert(minKey is not -1 && maxKey is not -1,"shouldn't happen, we checked IsEmpty") ;

        long seekFrom = minKey * RoaringBitmap.ContainerSize;
        long pruneAfter = (maxKey + 1) * RoaringBitmap.ContainerSize - 1;

        if (!iterator.Seek(seekFrom))
            return; // No entries in range — nothing to subtract

        Span<long> buffer = stackalloc long[FillBufferSize];
        int read;
        bool hasMore;
        do
        {
            hasMore = iterator.Fill(buffer, out read, pruneAfter);
            if (read > 0)
            {
                EntryIdEncodings.DecodeAndDiscardFrequency(buffer, read);
                tempBitmap.AddRange(buffer.Slice(0, read));
            }
        } while (hasMore && read > 0);

        bitmap.AndNotWith(ref tempBitmap);
    }

    /// <summary>
    /// Iterate bitmap contents into an output span. Returns count of entries written.
    /// Stops when output is full or bitmap is exhausted.
    /// </summary>
    public static int IterateInto(ref RoaringBitmapData data, Span<long> output, ByteStringContext allocator, ref RoaringBitmapIterator iterator)
    {
        data.PrepareForReading(allocator);
        return iterator.Fill(ref data, output);
    }

    /// <summary>
    /// Runtime check: should we switch from bitmap AND to per-entry scan?
    /// Compares cost of reading bitmap.Count entry blobs vs galloping through
    /// the posting list. Returns true when entry scan is cheaper.
    /// Called directly from IL-emitted code.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool ShouldSwitchToEntryScan(long bitmapCount, long postingListCount)
    {
        return bitmapCount < EntryScanCountThreshold
            && bitmapCount * EntryScanCostMultiplier < postingListCount;
    }

    /// <summary>
    /// Runtime check: should we heap-sort the bitmap directly instead of
    /// walking the sort-field index?
    /// Called directly from IL-emitted code.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool ShouldHeapSortDirectly(long bitmapCount, long sortFieldTotalEntries)
    {
        return bitmapCount < EntryScanCountThreshold
            && bitmapCount * EntryScanCostMultiplier < sortFieldTotalEntries;
    }

    // --- IQueryMatch-based overloads for IL emitter ---
    // The emitted IL resolves matches as IQueryMatch[] (TermMatch, BoostingMatch, etc.).
    // These overloads let the IL call QueryPrimitives directly instead of emitting
    // inline Fill+AddRange loops. JIT inlines these with AggressiveInlining.

    /// <summary>Fill bitmap from an IQueryMatch by calling Fill repeatedly.
    /// Fast paths (consume-after-use semantics — sources are not read again):
    ///   - IBitmapQueryMatch: steal containers via LazyOrWith + one RepairAfterLazy pass.
    ///   - TermMatch backed by a large posting list: native FillFromPostings on the iterator,
    ///     skipping the per-batch IQueryMatch + function-pointer indirection.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [SkipLocalsInit]
    public static void FillFromMatch(Matches.Meta.IQueryMatch match, ref RoaringBitmapData data, ByteStringContext allocator)
    {
        scoped RoaringBitmap bitmap = new(ref data, allocator);
        if (match is Matches.Meta.IBitmapQueryMatch bm)
        {
            ref RoaringBitmapData srcData = ref bm.GetBitmapData();
            if (srcData.IsEmpty)
                return;
            bitmap.LazyOrWith(ref srcData);
            bitmap.RepairAfterLazy();
            return;
        }
        if (match is Matches.TermMatch tm && tm.TryGetLargePostingListIterator(out var iter))
        {
            FillFromPostings(ref iter, ref data, allocator);
            return;
        }
        Span<long> buffer = stackalloc long[FillBufferSize];
        int read;
        while ((read = match.Fill(buffer)) > 0)
            bitmap.AddRange(buffer.Slice(0, read));
    }

    /// <summary>Fill temp bitmap from match, then AND with target.
    /// Fast paths:
    ///   - Match exposes a RoaringBitmap (IBitmapQueryMatch): AND in place against the borrowed bitmap.
    ///   - Match is a TermMatch backed by a large posting list: use the galloping page-scan
    ///     <see cref="AndWithPostings"/>, which bounds the posting-list scan to the bitmap's
    ///     container range — only reads pages that can intersect, instead of materializing
    ///     the full posting list into a temp bitmap.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [SkipLocalsInit]
    public static void AndWithMatch(Matches.Meta.IQueryMatch match, ref RoaringBitmapData data, ref RoaringBitmapData tempData, ByteStringContext allocator)
    {
        scoped RoaringBitmap bitmap = new(ref data, allocator);
        if (match is Matches.Meta.IBitmapQueryMatch bm)
        {
            ref RoaringBitmapData srcData = ref bm.GetBitmapData();
            bitmap.AndWith(ref srcData);
            return;
        }
        if (match is Matches.TermMatch tm && tm.TryGetLargePostingListIterator(out var iter))
        {
            AndWithPostings(ref iter, ref data, ref tempData, allocator);
            return;
        }
        scoped RoaringBitmap tempBitmap = new(ref tempData, allocator);
        tempBitmap.Clear();
        FillFromMatch(match, ref tempData, allocator);
        bitmap.AndWith(ref tempBitmap);
    }

    /// <summary>Fill temp bitmap from match, then ANDNOT from target.
    /// Fast paths mirror <see cref="AndWithMatch"/> — bitmap-borrow for IBitmapQueryMatch,
    /// galloping <see cref="AndNotWithPostings"/> for TermMatch with a large posting list.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [SkipLocalsInit]
    public static void AndNotWithMatch(Matches.Meta.IQueryMatch match, ref RoaringBitmapData data, ref RoaringBitmapData tempData, ByteStringContext allocator)
    {
        scoped RoaringBitmap bitmap = new(ref data, allocator);
        if (match is Matches.Meta.IBitmapQueryMatch bm)
        {
            ref RoaringBitmapData srcData = ref bm.GetBitmapData();
            bitmap.AndNotWith(ref srcData);
            return;
        }
        if (match is Matches.TermMatch tm && tm.TryGetLargePostingListIterator(out var iter))
        {
            AndNotWithPostings(ref iter, ref data, ref tempData, allocator);
            return;
        }
        scoped RoaringBitmap tempBitmap = new(ref tempData, allocator);
        tempBitmap.Clear();
        FillFromMatch(match, ref tempData, allocator);
        bitmap.AndNotWith(ref tempBitmap);
    }

    // ---- Native posting-list dispatch on a TermSource ----
    // The IL emitter resolves each term op to a TermSource (Empty / Single /
    // SmallPostingList / PostingList). These dispatchers do the three-way switch
    // and call the right native primitive, skipping the IQueryMatch wrapper.

    /// <summary>OR a TermSource into the bitmap.
    /// Single → Add; SmallPostingList → decode FastPFor buffer + AddRange;
    /// PostingList → <see cref="FillFromPostings"/>; Empty → no-op.</summary>
    [SkipLocalsInit]
    public static unsafe void FillBitmapFromTermSource(
        ref Planning.TermSource source,
        Voron.Impl.LowLevelTransaction llt,
        ref RoaringBitmapData data,
        ByteStringContext allocator)
    {
        scoped RoaringBitmap bitmap = new(ref data, allocator);
        switch (source.Kind)
        {
            case Planning.TermSourceKind.Empty:
                return;

            case Planning.TermSourceKind.Single:
                bitmap.Add(source.SingleEntryId);
                return;

            case Planning.TermSourceKind.SmallPostingList:
                AddSmallPostingListToBitmap(llt, source.SmallPostingListId, ref data, allocator);
                return;

            case Planning.TermSourceKind.PostingList:
                FillFromPostings(ref source.LargeIterator, ref data, allocator);
                return;

            default:
                throw new InvalidOperationException($"Unknown TermSourceKind: {source.Kind}");
        }
    }

    /// <summary>AND the bitmap with a TermSource. Galloping page-scan when the
    /// source is a large PostingList; per-key membership / temp-bitmap-fill for
    /// the smaller cases. Empty source clears the bitmap (intersection with
    /// nothing = nothing).</summary>
    [SkipLocalsInit]
    public static unsafe void AndWithTermSource(
        ref Planning.TermSource source,
        Voron.Impl.LowLevelTransaction llt,
        ref RoaringBitmapData data,
        ref RoaringBitmapData tempData,
        ByteStringContext allocator)
    {
        if (data.IsEmpty)
            return;

        scoped RoaringBitmap bitmap = new(ref data, allocator);
        switch (source.Kind)
        {
            case Planning.TermSourceKind.Empty:
                bitmap.Clear();
                return;

            case Planning.TermSourceKind.Single:
            {
                long entryId = source.SingleEntryId;
                bool keep = data.Contains(entryId);
                bitmap.Clear();
                if (keep)
                    bitmap.Add(entryId);
                return;
            }

            case Planning.TermSourceKind.SmallPostingList:
            {
                scoped RoaringBitmap tempBitmap = new(ref tempData, allocator);
                tempBitmap.Clear();
                AddSmallPostingListToBitmap(llt, source.SmallPostingListId, ref tempData, allocator);
                bitmap.AndWith(ref tempBitmap);
                return;
            }

            case Planning.TermSourceKind.PostingList:
                AndWithPostings(ref source.LargeIterator, ref data, ref tempData, allocator);
                return;

            default:
                throw new InvalidOperationException($"Unknown TermSourceKind: {source.Kind}");
        }
    }

    /// <summary>ANDNOT the bitmap with a TermSource (subtract). Empty source is
    /// a no-op (subtracting nothing).</summary>
    [SkipLocalsInit]
    public static unsafe void AndNotWithTermSource(
        ref Planning.TermSource source,
        Voron.Impl.LowLevelTransaction llt,
        ref RoaringBitmapData data,
        ref RoaringBitmapData tempData,
        ByteStringContext allocator)
    {
        if (data.IsEmpty)
            return;

        scoped RoaringBitmap bitmap = new(ref data, allocator);
        scoped RoaringBitmap tempBitmap = new(ref tempData, allocator);
        switch (source.Kind)
        {
            case Planning.TermSourceKind.Empty:
                return;

            case Planning.TermSourceKind.Single:
                tempBitmap.Clear();
                tempBitmap.Add(source.SingleEntryId);
                bitmap.AndNotWith(ref tempBitmap);
                return;

            case Planning.TermSourceKind.SmallPostingList:
                tempBitmap.Clear();
                AddSmallPostingListToBitmap(llt, source.SmallPostingListId, ref tempData, allocator);
                bitmap.AndNotWith(ref tempBitmap);
                return;

            case Planning.TermSourceKind.PostingList:
                AndNotWithPostings(ref source.LargeIterator, ref data, ref tempData, allocator);
                return;

            default:
                throw new InvalidOperationException($"Unknown TermSourceKind: {source.Kind}");
        }
    }

    /// <summary>Fetch the small posting list container by id, decode the
    /// FastPFor stream into the bitmap. Allocates a stackalloc buffer +
    /// FastPForBufferedReader scoped to this call.</summary>
    [SkipLocalsInit]
    private static unsafe void AddSmallPostingListToBitmap(
        Voron.Impl.LowLevelTransaction llt,
        long smallPostingListId,
        ref RoaringBitmapData data,
        ByteStringContext allocator)
    {
        scoped RoaringBitmap bitmap = new(ref data, allocator);
        Container.Get(llt, (Voron.Data.Containers.ContainerEntryId)smallPostingListId, out var item);
        _ = VariableSizeEncoding.Read<int>(item.Address, out var offset);

        Span<long> buffer = stackalloc long[FillBufferSize];
        var reader = new FastPForBufferedReader(llt.Allocator);
        try
        {
            reader.Init(item.Address + offset, item.Length - offset);
            fixed (long* pBuffer = buffer)
            {
                int read;
                while ((read = reader.Fill(pBuffer, buffer.Length)) > 0)
                {
                    EntryIdEncodings.DecodeAndDiscardFrequency(buffer.Slice(0, read), read);
                    bitmap.AddRange(buffer.Slice(0, read));
                }
            }
        }
        finally
        {
            reader.Dispose();
        }
    }

    // Batch size for FillPostingListIds calls within FillBitmapFromTermProvider.
    private const int PostingListIdBatchSize = 256;

    /// <summary>
    /// Fill a bitmap by walking an ITermProvider's posting list IDs in batches.
    /// For each posting list ID, decodes the TermIdMask type and either:
    ///   - Single: accumulates the decoded entry ID in a buffer
    ///   - SmallPostingList: decodes inline via FastPForBufferedReader
    ///   - PostingList: iterates through the full posting list via FillFromPostings
    /// Entry IDs from Single and SmallPostingList are accumulated in a buffer,
    /// sorted, and flushed to the bitmap in batches.
    /// </summary>
    [SkipLocalsInit]
    public static unsafe void FillBitmapFromTermProvider(
        ITermProvider provider,
        LowLevelTransaction llt,
        ref RoaringBitmapData data,
        ByteStringContext allocator)
    {
        scoped RoaringBitmap bitmap = new(ref data, allocator);
        Span<long> plIds = stackalloc long[PostingListIdBatchSize];
        Span<long> entryBuffer = stackalloc long[FillBufferSize];
        int entryCount = 0;

        var smallPostListIds = new ContextBoundNativeList<long>(llt.Allocator, PostingListIdBatchSize);
        llt.Allocator.Allocate(PostingListIdBatchSize * sizeof(UnmanagedSpan), out ByteString containerItemsBs);
        var containerItems = (UnmanagedSpan*)containerItemsBs.Ptr;
        var pageLocator = llt.PageLocator;

        FastPForBufferedReader smallListReader = default;
        bool readerInitialized = false;

        try
        {
            int read;
            while ((read = provider.FillPostingListIds(plIds)) > 0)
            {
                // Collect small posting list container IDs for batch retrieval
                smallPostListIds.Clear();
                for (int i = 0; i < read; i++)
                {
                    var termType = (TermIdMask)plIds[i] & TermIdMask.EnsureIsSingleMask;
                    if (termType == TermIdMask.SmallPostingList)
                    {
                        smallPostListIds.Add((long)EntryIdEncodings.GetContainerId(plIds[i]));
                    }
                }

                // Batch-fetch all small posting list containers
                int smallIdx = 0;
                if (smallPostListIds.Count > 0)
                {
                    Container.GetAll(llt, smallPostListIds.ToSpan(),
                        new Span<UnmanagedSpan>(containerItems, smallPostListIds.Count),
                        long.MinValue, pageLocator);
                }

                for (int i = 0; i < read; i++)
                {
                    var postingListId = plIds[i];
                    var termType = (TermIdMask)postingListId & TermIdMask.EnsureIsSingleMask;

                    switch (termType)
                    {
                        case TermIdMask.Single:
                        {
                            long entryId = (long)EntryIdEncodings.GetContainerId(postingListId);
                            if (entryCount >= entryBuffer.Length)
                            {
                                FlushEntries(entryBuffer, entryCount, ref bitmap);
                                entryCount = 0;
                            }
                            entryBuffer[entryCount++] = entryId;
                            break;
                        }

                        case TermIdMask.SmallPostingList:
                        {
                            var item = containerItems[smallIdx++];
                            _ = VariableSizeEncoding.Read<int>(item.Address, out var offset);

                            if (readerInitialized == false)
                            {
                                smallListReader = new FastPForBufferedReader(llt.Allocator);
                                readerInitialized = true;
                            }

                            smallListReader.Init(item.Address + offset, item.Length - offset);

                            // Read small posting list entries into the entry buffer
                            fixed (long* pEntryBuffer = entryBuffer)
                            {
                                int smallRead;
                                while ((smallRead = smallListReader.Fill(pEntryBuffer + entryCount, entryBuffer.Length - entryCount)) > 0)
                                {
                                    EntryIdEncodings.DecodeAndDiscardFrequency(entryBuffer.Slice(entryCount, smallRead), smallRead);
                                    entryCount += smallRead;

                                    if (entryCount >= entryBuffer.Length)
                                    {
                                        FlushEntries(entryBuffer, entryCount, ref bitmap);
                                        entryCount = 0;
                                    }
                                }
                            }
                            break;
                        }

                        case TermIdMask.PostingList:
                        {
                            // Flush accumulated entries before processing the large posting list
                            if (entryCount > 0)
                            {
                                FlushEntries(entryBuffer, entryCount, ref bitmap);
                                entryCount = 0;
                            }

                            var setId = EntryIdEncodings.GetContainerId(postingListId);
                            var setStateSpan = Container.GetReadOnly(llt, setId);
                            ref readonly var setState = ref MemoryMarshal.AsRef<PostingListState>(setStateSpan);
                            var postingList = new PostingList(llt, Slices.Empty, in setState);
                            var iterator = postingList.Iterate();
                            FillFromPostings(ref iterator, ref data, allocator);
                            break;
                        }

                        default:
                            throw new InvalidOperationException($"Unknown TermIdMask type: {termType}");
                    }
                }
            }

            // Flush any remaining entries
            if (entryCount > 0)
            {
                FlushEntries(entryBuffer, entryCount, ref bitmap);
            }
        }
        finally
        {
            if (readerInitialized)
                smallListReader.Dispose();
            smallPostListIds.Dispose();
        }
    }

    /// <summary>
    /// Sort the entry buffer and add the sorted range to the bitmap.
    /// Entries from different posting lists are not guaranteed to be sorted,
    /// so we must sort before calling AddRange.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void FlushEntries(Span<long> entryBuffer, int count, ref RoaringBitmap bitmap)
    {
        var slice = entryBuffer.Slice(0, count);
        int unique = Sorting.SortAndRemoveDuplicates(slice);
        bitmap.AddRange(slice.Slice(0, unique));
    }
}
