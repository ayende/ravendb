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
/// Each primitive operates on a RoaringBitmap accumulator.
/// Pre-compiled, SIMD-tuned, individually benchmarkable.
/// </summary>
public static class QueryPrimitives
{
    // Buffer size for stackalloc Fill operations.
    private const int FillBufferSize = 4096;

    // Batch size for entry scan: how many bitmap entries to read per iteration.
    private const int EntryScanBatchSize = 256;

    // Buffer size for skip operations during paginated iteration.
    private const int SkipBufferSize = 1024;

    // Bitmap count threshold below which entry scan is considered cheaper than
    // bitmap AND with a posting list. Below this, individual entry blob reads
    // are cheaper than decoding the full posting list.
    private const long EntryScanCountThreshold = 32_000;

    // Cost multiplier: entry scan is chosen when bitmapCount * EntryScanCostMultiplier
    // is less than the posting list size. Approximates the relative cost of reading
    // entry blobs vs. decoding posting list pages.
    private const long EntryScanCostMultiplier = 64;

    // Threshold for stackalloc vs heap allocation of field root pages array.
    private const int FieldRootPagesStackAllocThreshold = 128;

    /// <summary>
    /// Fill a bitmap from a posting list. Walks leaf pages, decodes PFor blocks,
    /// adds entries to the bitmap via batch AddRange.
    /// Respects limit — stops after enough entries.
    /// </summary>
    [SkipLocalsInit]
    public static void FillFromPostings(ref PostingList.Iterator iterator, ref RoaringBitmap bitmap, long limit = long.MaxValue)
    {
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
            bitmap.AddRange(buffer.Slice(0, read));
            total += read;

            if (total >= limit)
                break;
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
    public static void AndWithPostings(ref PostingList.Iterator iterator, ref RoaringBitmap bitmap, ref RoaringBitmap tempBitmap)
    {
        if (bitmap.IsEmpty)
            return;

        tempBitmap.Clear();

        // Bound the posting list scan to the bitmap's container key range.
        // Each container key covers 65536 entry IDs.
        long minKey = bitmap.MinContainerKey;
        long maxKey = bitmap.MaxContainerKey;
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
    public static void OrWithPostings(ref PostingList.Iterator iterator, ref RoaringBitmap bitmap)
    {
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
    public static void AndNotWithPostings(ref PostingList.Iterator iterator, ref RoaringBitmap bitmap, ref RoaringBitmap tempBitmap)
    {
        if (bitmap.IsEmpty)
            return;

        tempBitmap.Clear();

        long minKey = bitmap.MinContainerKey;
        long maxKey = bitmap.MaxContainerKey;
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
    public static int IterateInto(ref RoaringBitmap bitmap, Span<long> output, ref RoaringBitmapIterator iterator)
    {
        bitmap.PrepareForReading();
        return iterator.Fill(ref bitmap, output);
    }

    /// <summary>
    /// Runtime check: should we switch from bitmap AND to per-entry scan?
    /// Compares cost of reading bitmap.Count entry blobs vs galloping through
    /// the posting list. Returns true when entry scan is cheaper.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool ShouldSwitchToEntryScan(ref RoaringBitmap bitmap, in PostingListState postingListState)
    {
        long bitmapCount = bitmap.Count;
        return bitmapCount < EntryScanCountThreshold
            && bitmapCount * EntryScanCostMultiplier < postingListState.NumberOfEntries;
    }

    /// <summary>
    /// Runtime check: should we heap-sort the bitmap directly instead of
    /// walking the sort-field index?
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool ShouldHeapSortDirectly(ref RoaringBitmap bitmap, long sortFieldTotalEntries)
    {
        long bitmapCount = bitmap.Count;
        return bitmapCount < EntryScanCountThreshold
            && bitmapCount * EntryScanCostMultiplier < sortFieldTotalEntries;
    }

    /// <summary>
    /// Entry scan: iterate bitmap, read each entry's stored field data via
    /// EntryTermsReader, evaluate predicates with early exit.
    /// Same pattern as MultiUnaryMatch.Fill() but driven from a bitmap iterator.
    /// </summary>
    [SkipLocalsInit]
    public static int ScanAndFilter(ref RoaringBitmap bitmap, IndexSearcher searcher,
        MultiUnaryItem[] predicates, Span<long> output, long limit, ref long skip)
    {
        bitmap.PrepareForReading();
        using var iter = bitmap.GetIterator();

        if (predicates is not {Length: > 0}) // No predicates — just deal with skip/limit
        {
            return ScanWithSkipAndLimitOnly(ref bitmap, output, limit, ref skip, iter);
        }

        Span<long> batch = stackalloc long[EntryScanBatchSize];
        // Batch resolution buffers — reused across iterations
        Span<long> locations = stackalloc long[EntryScanBatchSize];
        Span<Sparrow.UnmanagedSpan> spans = stackalloc Sparrow.UnmanagedSpan[EntryScanBatchSize];
        int matched = 0;

        Span<long> fieldRootPages = predicates.Length > FieldRootPagesStackAllocThreshold
            ? new long[predicates.Length]
            : stackalloc long[predicates.Length];

        for (int p = 0; p < predicates.Length; p++)
        {
            fieldRootPages[p] = searcher.FieldCache.GetLookupRootPage(predicates[p].Binding.FieldName);
        }

        int read;
        while ((read = iter.Fill(ref bitmap, batch)) > 0)
        {
            int startIdx = (int)Math.Min(skip, read);
            skip = Math.Max(0, skip - read);
            if (startIdx >= read)
                continue;

            // Batch-resolve entry data: cursor-walk B-tree + PageLocator-cached container reads
            var batchSlice = batch.Slice(startIdx, read - startIdx);
            searcher.BatchGetEntryData(batchSlice, locations.Slice(0, batchSlice.Length), spans.Slice(0, batchSlice.Length));

            for (int i = 0; i < batchSlice.Length; i++)
            {
                if (spans[i].Length == 0)
                    continue; // missing entry

                long entryId = batchSlice[i];
                var reader = searcher.CreateEntryTermsReader(spans[i]);
                bool documentMatched = true;

                for (int p = 0; p < predicates.Length; p++)
                {
                    ref var predicate = ref predicates[p];
                    long fieldRootPage = fieldRootPages[p];

                    bool isAccepted = predicate.Mode == MultiUnaryItem.UnaryMode.All;
                    reader.Reset();

                    while (reader.FindNext(fieldRootPage))
                    {
                        bool cmpResult = predicate.Type switch
                        {
                            MultiUnaryItem.DataType.Slice => predicate.CompareLiteral(reader),
                            MultiUnaryItem.DataType.Long => predicate.CompareNumerical(reader),
                            MultiUnaryItem.DataType.Double => predicate.CompareNumerical(reader),
                            _ => throw new ArgumentOutOfRangeException()
                        };

                        if (predicate.Mode == MultiUnaryItem.UnaryMode.All && !cmpResult)
                        {
                            isAccepted = false;
                            break;
                        }

                        if (predicate.Mode == MultiUnaryItem.UnaryMode.Any && cmpResult)
                        {
                            isAccepted = true;
                            break;
                        }
                    }

                    if (!isAccepted)
                    {
                        documentMatched = false;
                        break;
                    }
                }

                if (documentMatched)
                {
                    output[matched++] = entryId;
                    if (matched >= limit || matched >= output.Length)
                        return matched;
                }
            }
        }

        return matched;
    }

    private static int ScanWithSkipAndLimitOnly(ref RoaringBitmap bitmap, Span<long> output, long limit, ref long skip, RoaringBitmapIterator iter)
    {
        if (skip > 0)
        {
            Span<long> skipBuf = stackalloc long[(int)Math.Min(skip, SkipBufferSize)];
            while (skip > 0)
            {
                int toSkip = (int)Math.Min(skip, skipBuf.Length);
                int skipped = iter.Fill(ref bitmap, skipBuf.Slice(0, toSkip));
                if (skipped == 0) return 0;
                skip -= skipped;
            }
        }
        int maxOutput = (int)Math.Min(output.Length, limit);
        return iter.Fill(ref bitmap, output.Slice(0, maxOutput));
    }

    // --- IQueryMatch-based overloads for IL emitter ---
    // The emitted IL resolves matches as IQueryMatch[] (TermMatch, BoostingMatch, etc.).
    // These overloads let the IL call QueryPrimitives directly instead of emitting
    // inline Fill+AddRange loops. JIT inlines these with AggressiveInlining.

    /// <summary>Fill bitmap from an IQueryMatch by calling Fill repeatedly.
    /// Fast path: when the match already exposes a bitmap (IBitmapQueryMatch — every modern
    /// term/term-match-producing path does), borrow it and OR directly. Skips one full
    /// Fill loop and the AddRange path's per-batch sort/dedup work.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [SkipLocalsInit]
    public static void FillFromMatch(Matches.Meta.IQueryMatch match, ref RoaringBitmap bitmap)
    {
        if (match is Matches.Meta.IBitmapQueryMatch bm)
        {
            var src = bm.BorrowBitmap();
            // Empty bitmap => OR with empty is a no-op; LazyOrWith on empty doesn't repair.
            if (src.IsEmpty)
                return;
            bitmap.LazyOrWith(ref src);
            bitmap.RepairAfterLazy();
            return;
        }
        Span<long> buffer = stackalloc long[FillBufferSize];
        int read;
        while ((read = match.Fill(buffer)) > 0)
            bitmap.AddRange(buffer.Slice(0, read));
    }

    /// <summary>Fill temp bitmap from match, then AND with target.
    /// Fast path: borrow the match's bitmap directly and AND in place — no temp fill needed.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [SkipLocalsInit]
    public static void AndWithMatch(Matches.Meta.IQueryMatch match, ref RoaringBitmap bitmap, ref RoaringBitmap tempBitmap)
    {
        if (match is Matches.Meta.IBitmapQueryMatch bm)
        {
            var src = bm.BorrowBitmap();
            bitmap.AndWith(ref src);
            return;
        }
        tempBitmap.Clear();
        FillFromMatch(match, ref tempBitmap);
        bitmap.AndWith(ref tempBitmap);
    }

    /// <summary>Fill temp bitmap from match, then ANDNOT from target.
    /// Fast path: borrow the match's bitmap and ANDNOT directly.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [SkipLocalsInit]
    public static void AndNotWithMatch(Matches.Meta.IQueryMatch match, ref RoaringBitmap bitmap, ref RoaringBitmap tempBitmap)
    {
        if (match is Matches.Meta.IBitmapQueryMatch bm)
        {
            var src = bm.BorrowBitmap();
            bitmap.AndNotWith(ref src);
            return;
        }
        tempBitmap.Clear();
        FillFromMatch(match, ref tempBitmap);
        bitmap.AndNotWith(ref tempBitmap);
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
        ref RoaringBitmap bitmap)
    {
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
                            FillFromPostings(ref iterator, ref bitmap);
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
