using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Corax.Indexing;
using Corax.Querying.Matches.Meta;
using Corax.Utils;
using Voron.Data.RoaringBitmaps;
using Sparrow;
using Sparrow.Compression;
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
    // Buffer size for stackalloc Fill operations (posting-list batch reads).
    internal const int FillBufferSize = 4096;

    // ── IL entry points ──────────────────────────────────────────────
    // Called from DynamicMethod IL. Take ref CompiledQueryMatch so the
    // emitter just pushes ldarg.0 + int constants — no per-field ldfld chains.

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void CtxFillFromPostingSource(Matches.CompiledQueryMatch ctx, int paramIndex)
        => FillBitmapFromPostingSource(ref ctx.PostingSources[paramIndex], ctx.Llt, ref ctx.Bitmaps[0], ctx.Limit);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void CtxFillFromTreeScan(Matches.CompiledQueryMatch ctx, int paramIndex)
        => FillBitmapFromTreeScan(ctx.TermsProviders[paramIndex], ctx.Llt, ref ctx.Bitmaps[0], ctx.Limit);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void CtxFillFromMatch(Matches.CompiledQueryMatch ctx, int paramIndex)
        => FillFromMatch(ctx.ResolvedMatches[paramIndex], ref ctx.Bitmaps[0], ctx.Limit);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void CtxOrFillFromPostingSource(Matches.CompiledQueryMatch ctx, int paramIndex, int bitmapSlot)
    {
        long remaining = bitmapSlot == 0 ? ctx.Limit - ctx.Bitmaps[0].Count : ctx.Limit;
        if (remaining <= 0) return;
        FillBitmapFromPostingSource(ref ctx.PostingSources[paramIndex], ctx.Llt, ref ctx.Bitmaps[bitmapSlot], remaining);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void CtxOrFillFromTreeScan(Matches.CompiledQueryMatch ctx, int paramIndex, int bitmapSlot)
    {
        long remaining = bitmapSlot == 0 ? ctx.Limit - ctx.Bitmaps[0].Count : ctx.Limit;
        if (remaining <= 0) return;
        FillBitmapFromTreeScan(ctx.TermsProviders[paramIndex], ctx.Llt, ref ctx.Bitmaps[bitmapSlot], remaining);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void CtxOrFillFromMatch(Matches.CompiledQueryMatch ctx, int paramIndex, int bitmapSlot)
    {
        long remaining = bitmapSlot == 0 ? ctx.Limit - ctx.Bitmaps[0].Count : ctx.Limit;
        if (remaining <= 0) return;
        FillFromMatch(ctx.ResolvedMatches[paramIndex], ref ctx.Bitmaps[bitmapSlot], remaining);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void CtxAndFromPostingSource(Matches.CompiledQueryMatch ctx, int paramIndex)
        => AndWithPostingSource(ref ctx.PostingSources[paramIndex], ctx.Llt, ref ctx.Bitmaps[0], ref ctx.Bitmaps[1], ctx.Limit);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void CtxAndFromTreeScan(Matches.CompiledQueryMatch ctx, int paramIndex)
        => AndBitmapWithTreeScan(ctx.TermsProviders[paramIndex], ctx.Llt, ref ctx.Bitmaps[0], ref ctx.Bitmaps[1]);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void CtxAndFromMatch(Matches.CompiledQueryMatch ctx, int paramIndex)
        => AndWithMatch(ctx.ResolvedMatches[paramIndex], ref ctx.Bitmaps[0], ref ctx.Bitmaps[1]);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void CtxAndNotFromPostingSource(Matches.CompiledQueryMatch ctx, int paramIndex)
        => AndNotWithPostingSource(ref ctx.PostingSources[paramIndex], ctx.Llt, ref ctx.Bitmaps[0], ref ctx.Bitmaps[1]);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void CtxAndNotFromTreeScan(Matches.CompiledQueryMatch ctx, int paramIndex)
        => AndNotBitmapWithTreeScan(ctx.TermsProviders[paramIndex], ctx.Llt, ref ctx.Bitmaps[0], ref ctx.Bitmaps[1]);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void CtxAndNotFromMatch(Matches.CompiledQueryMatch ctx, int paramIndex)
        => AndNotWithMatch(ctx.ResolvedMatches[paramIndex], ref ctx.Bitmaps[0], ref ctx.Bitmaps[1]);

    // Batch size for entry scan: how many bitmap entries to read per iteration.
    internal const int EntryScanBatchSize = 256;

    // Bitmap count threshold below which entry scan is considered cheaper than
    // bitmap AND with a posting list. Below this, individual entry blob reads
    // are cheaper than decoding the full posting list.
    private const long EntryScanCountThreshold = 32 * 1024;

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
    private static void FillFromPostings(ref PostingList.Iterator iterator, ref RoaringBitmap bitmap, long limit = long.MaxValue)
    {
        Span<long> buffer = stackalloc long[FillBufferSize];

        long total = 0;
        while (iterator.Fill(buffer, out int read) && read > 0)
        {
            EntryIdEncodings.DecodeAndDiscardFrequency(buffer, read);

            long remaining = limit - total;
            read = (int)Math.Min(read, remaining);
            if (read <= 0)
                break;
            bitmap.AddRange(buffer[..read]);
            total += read;
        }
    }

    /// <summary>
    /// AND bitmap with a posting list using bounded range scan.
    /// Uses bitmap's container key range to bound the posting list scan:
    /// Seek() jumps past entries below the bitmap's min, and pruneGreaterThan
    /// stops reading past the bitmap's max. Only posting list pages that overlap
    /// with the bitmap's entry ID range are read.
    ///
    /// For a 50K bitmap vs 10M posting list, this reads only the pages covering
    /// the 50K range instead of all 10M entries.
    /// </summary>
    [SkipLocalsInit]
    private static void AndWithPostings(ref PostingList.Iterator iterator, ref RoaringBitmap bitmap, ref RoaringBitmap tempBitmap)
    {
        if (bitmap.IsEmpty)
            return;

        tempBitmap.Clear();

        // Bound the posting list scan to the bitmap's container key range.
        // Each container key covers 65.536 entry IDs.
        long minKey = bitmap.MinContainerKey;
        long maxKey = bitmap.MaxContainerKey;
        Debug.Assert(minKey is not -1 && maxKey is not -1, "shouldn't happen, we checked IsEmpty");

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
        while (iterator.Fill(buffer, out int read, pruneAfter) && read > 0)
        {
            EntryIdEncodings.DecodeAndDiscardFrequency(buffer, read);
            tempBitmap.AddRange(buffer[..read]);
        }

        bitmap.AndWith(ref tempBitmap);
    }

    /// <summary>
    /// Limit-aware AND: bounded range scan as <see cref="AndWithPostings"/>, but
    /// each batch from the posting list is filtered against <paramref name="bitmap"/>
    /// in place, accumulating only intersecting entries into <paramref name="tempBitmap"/>.
    /// Reading stops as soon as the intersection has ≥ limit entries — no need to
    /// materialize the rest of the posting list. The result then replaces bitmap.
    /// For unsorted queries any N valid results are sufficient.
    /// </summary>
    [SkipLocalsInit]
    private static void AndWithPostingsLimited(ref PostingList.Iterator iterator, ref RoaringBitmap bitmap, ref RoaringBitmap tempBitmap, long limit)
    {
        if (bitmap.IsEmpty)
            return;

        tempBitmap.Clear();

        long minKey = bitmap.MinContainerKey;
        long maxKey = bitmap.MaxContainerKey;
        Debug.Assert(minKey is not -1 && maxKey is not -1);

        long seekFrom = minKey * RoaringBitmap.ContainerSize;
        long pruneAfter = (maxKey + 1) * RoaringBitmap.ContainerSize - 1;

        if (!iterator.Seek(seekFrom))
        {
            bitmap.Clear();
            return;
        }

        Span<long> buffer = stackalloc long[FillBufferSize];
        long matchCount = 0;
        while (matchCount < limit && iterator.Fill(buffer, out int read, pruneAfter) && read > 0)
        {
            EntryIdEncodings.DecodeAndDiscardFrequency(buffer, read);

            // Filter the batch in-place against bitmap. The posting list is sorted by
            // entry id, so the kept prefix stays sorted — AddRange remains efficient.
            int kept = 0;
            for (int i = 0; i < read; i++)
            {
                if (bitmap.Contains(buffer[i]))
                {
                    buffer[kept++] = buffer[i];
                    if (++matchCount >= limit)
                        break;
                }
            }
            tempBitmap.AddRange(buffer[..kept]);
        }

        bitmap.SwapContents(ref tempBitmap);
    }

    /// <summary>
    /// ANDNOT the bitmap with a posting list. Same bounded range scan as AndWith —
    /// only reads posting list pages that overlap with the bitmap's container range.
    /// </summary>
    [SkipLocalsInit]
    private static void AndNotWithPostings(ref PostingList.Iterator iterator, ref RoaringBitmap bitmap, ref RoaringBitmap tempBitmap)
    {
        if (bitmap.IsEmpty)
            return;

        tempBitmap.Clear();

        long minKey = bitmap.MinContainerKey;
        long maxKey = bitmap.MaxContainerKey;
        Debug.Assert(minKey is not -1 && maxKey is not -1, "shouldn't happen, we checked IsEmpty");

        long seekFrom = minKey * RoaringBitmap.ContainerSize;
        long pruneAfter = (maxKey + 1) * RoaringBitmap.ContainerSize - 1;

        if (!iterator.Seek(seekFrom))
            return; // No entries in range — nothing to subtract

        Span<long> buffer = stackalloc long[FillBufferSize];
        while (iterator.Fill(buffer, out int read, pruneAfter) && read > 0)
        {
            EntryIdEncodings.DecodeAndDiscardFrequency(buffer, read);
            tempBitmap.AddRange(buffer[..read]);
        }

        bitmap.AndNotWith(ref tempBitmap);
    }

    /// <summary>
    /// Runtime check: should we switch from bitmap AND to per-entry scan?
    /// Compares cost of reading bitmap.Count entry blobs vs scanning through
    /// the posting list. Returns true when entry scan is cheaper.
    /// Called directly from IL-emitted code.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool ShouldSwitchToEntryScan(long bitmapCount, long postingListCount)
    {
        return bitmapCount < EntryScanCountThreshold
            && bitmapCount * EntryScanCostMultiplier < postingListCount;
    }

    /// <summary>Fill bitmap from an IQueryMatch by calling Fill repeatedly.
    /// Fast paths (consume-after-use semantics — sources are not read again):
    ///   - IBitmapQueryMatch: steal containers via LazyOrWith + one RepairAfterLazy pass.
    ///   - TermMatch backed by a large posting list: native FillFromPostings on the iterator,
    ///     skipping the per-batch IQueryMatch + function-pointer indirection.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [SkipLocalsInit]
    public static void FillFromMatch(IQueryMatch match, ref RoaringBitmap bitmap, long limit = long.MaxValue)
    {
        if (match is IBitmapQueryMatch bm)
        {
            ref RoaringBitmap srcData = ref bm.BitmapState;
            if (srcData.IsEmpty)
                return;
            bitmap.OrWith(ref srcData);
            return;
        }
        if (match is Matches.TermMatch tm && tm.TryGetPostingListIterator(out var iter))
        {
            FillFromPostings(ref iter, ref bitmap, limit);
            return;
        }
        Span<long> buffer = stackalloc long[FillBufferSize];
        int read;
        long total = 0;
        while ((read = match.Fill(buffer)) > 0)
        {
            long remaining = limit - total;
            read = (int)Math.Min(read, remaining);
            if (read <= 0) break;
            bitmap.AddRange(buffer.Slice(0, read));
            total += read;
        }
    }

    /// <summary>Fill temp bitmap from match, then AND with target.
    /// Fast paths:
    ///   - Match exposes a RoaringBitmap (IBitmapQueryMatch): AND in place against the borrowed bitmap.
    ///   - Match is a TermMatch backed by a large posting list: use the bounded range scan
    ///     <see cref="AndWithPostings"/>, which bounds the posting-list scan to the bitmap's
    ///     container range — only reads pages that can intersect, instead of materializing
    ///     the full posting list into a temp bitmap.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [SkipLocalsInit]
    public static void AndWithMatch(IQueryMatch match, ref RoaringBitmap bitmap, ref RoaringBitmap tempBitmap)
    {
        if (match is IBitmapQueryMatch bm)
        {
            ref RoaringBitmap srcData = ref bm.BitmapState;
            bitmap.AndWith(ref srcData);
            return;
        }
        if (match is Matches.TermMatch tm && tm.TryGetPostingListIterator(out var iter))
        {
            AndWithPostings(ref iter, ref bitmap, ref tempBitmap);
            return;
        }
        tempBitmap.Clear();
        FillFromMatch(match, ref tempBitmap);
        bitmap.AndWith(ref tempBitmap);
    }

    /// <summary>Fill temp bitmap from match, then ANDNOT from target.
    /// Fast paths mirror <see cref="AndWithMatch"/> — bitmap-borrow for IBitmapQueryMatch,
    /// bounded range scan <see cref="AndNotWithPostings"/> for TermMatch with a large posting list.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [SkipLocalsInit]
    public static void AndNotWithMatch(IQueryMatch match, ref RoaringBitmap bitmap, ref RoaringBitmap tempBitmap)
    {
        if (match is IBitmapQueryMatch bm)
        {
            ref RoaringBitmap srcData = ref bm.BitmapState;
            bitmap.AndNotWith(ref srcData);
            return;
        }
        if (match is Matches.TermMatch tm && tm.TryGetPostingListIterator(out var iter))
        {
            AndNotWithPostings(ref iter, ref bitmap, ref tempBitmap);
            return;
        }
        tempBitmap.Clear();
        FillFromMatch(match, ref tempBitmap);
        bitmap.AndNotWith(ref tempBitmap);
    }

    /// <summary>OR a TermSource into the bitmap.
    /// Single → Add; SmallPostingList → decode FastPFor buffer + AddRange;
    /// PostingList → <see cref="FillFromPostings"/>; Empty → no-op.</summary>
    [SkipLocalsInit]
    private static void FillBitmapFromPostingSource(
        ref Planning.PostingSource source,
        LowLevelTransaction llt,
        ref RoaringBitmap bitmap,
        long limit = long.MaxValue)
    {
        switch (source.Kind)
        {
            case Planning.PostingSourceKind.Empty:
                return;

            case Planning.PostingSourceKind.Single:
                bitmap.Add(source.SingleEntryId);
                return;

            case Planning.PostingSourceKind.SmallPostingList:
                AddSmallPostingListToBitmap(llt, source.SmallPostingListId, ref bitmap);
                return;

            case Planning.PostingSourceKind.PostingList:
                FillFromPostings(ref source.LargeIterator, ref bitmap, limit);
                return;

            default:
                throw new InvalidOperationException($"Unknown PostingSourceKind: {source.Kind}");
        }
    }

    /// <summary>AND the bitmap with a TermSource. Bounded range scan when the
    /// source is a large PostingList; per-key membership / temp-bitmap-fill for
    /// the smaller cases. Empty source clears the bitmap (intersection with
    /// nothing = nothing).</summary>
    [SkipLocalsInit]
    private static void AndWithPostingSource(
        ref Planning.PostingSource source,
        LowLevelTransaction llt,
        ref RoaringBitmap bitmap,
        ref RoaringBitmap tempBitmap,
        long limit = long.MaxValue)
    {
        if (bitmap.IsEmpty)
            return;

        switch (source.Kind)
        {
            case Planning.PostingSourceKind.Empty:
                bitmap.Clear();
                return;

            case Planning.PostingSourceKind.Single:
                {
                    long entryId = source.SingleEntryId;
                    bool keep = bitmap.Contains(entryId);
                    bitmap.Clear();
                    if (keep)
                        bitmap.Add(entryId);
                    return;
                }

            case Planning.PostingSourceKind.SmallPostingList:
                MaterializeTermSourceIntoBitmap(ref source, llt, ref tempBitmap);
                bitmap.AndWith(ref tempBitmap);
                return;

            case Planning.PostingSourceKind.PostingList:
                if (limit < long.MaxValue)
                    AndWithPostingsLimited(ref source.LargeIterator, ref bitmap, ref tempBitmap, limit);
                else
                    AndWithPostings(ref source.LargeIterator, ref bitmap, ref tempBitmap);
                return;

            default:
                throw new InvalidOperationException($"Unknown PostingSourceKind: {source.Kind}");
        }
    }

    /// <summary>ANDNOT the bitmap with a TermSource (subtract). Empty source is
    /// a no-op (subtracting nothing).</summary>
    [SkipLocalsInit]
    private static void AndNotWithPostingSource(
        ref Planning.PostingSource source,
        LowLevelTransaction llt,
        ref RoaringBitmap bitmap,
        ref RoaringBitmap tempBitmap)
    {
        if (bitmap.IsEmpty)
            return;

        switch (source.Kind)
        {
            case Planning.PostingSourceKind.Empty:
                return;

            case Planning.PostingSourceKind.Single:
            case Planning.PostingSourceKind.SmallPostingList:
                MaterializeTermSourceIntoBitmap(ref source, llt, ref tempBitmap);
                bitmap.AndNotWith(ref tempBitmap);
                return;

            case Planning.PostingSourceKind.PostingList:
                AndNotWithPostings(ref source.LargeIterator, ref bitmap, ref tempBitmap);
                return;

            default:
                throw new InvalidOperationException($"Unknown PostingSourceKind: {source.Kind}");
        }
    }

    /// <summary>Materialize a Single or SmallPostingList TermSource into the temp bitmap
    /// (clears it first). Shared by AndWithPostingSource and AndNotWithPostingSource to avoid
    /// duplicating the clear-then-populate pattern for these small-source cases.</summary>
    private static void MaterializeTermSourceIntoBitmap(
        ref Planning.PostingSource source,
        LowLevelTransaction llt,
        ref RoaringBitmap tempBitmap)
    {
        tempBitmap.Clear();
        switch (source.Kind)
        {
            case Planning.PostingSourceKind.Single:
                tempBitmap.Add(source.SingleEntryId);
                return;
            case Planning.PostingSourceKind.SmallPostingList:
                AddSmallPostingListToBitmap(llt, source.SmallPostingListId, ref tempBitmap);
                return;
            default:
                Debug.Fail($"MaterializeTermSourceIntoBitmap called with unexpected kind: {source.Kind}");
                return;
        }
    }

    /// <summary>Fetch the small posting list container by id, decode the
    /// FastPFor stream into the bitmap. Allocates a stackalloc buffer +
    /// FastPForBufferedReader scoped to this call.</summary>
    [SkipLocalsInit]
    private static unsafe void AddSmallPostingListToBitmap(
        LowLevelTransaction llt,
        long smallPostingListId,
        ref RoaringBitmap bitmap)
    {
        Container.Get(llt, (ContainerEntryId)smallPostingListId, out var item);
        _ = VariableSizeEncoding.Read<int>(item.Address, out var offset);

        var buffer = stackalloc long[FillBufferSize];
        using var reader = new FastPForBufferedReader(llt.Allocator);
        reader.Init(item.Address + offset, item.Length - offset);
        {
            int read;
            while ((read = reader.Fill(buffer, FillBufferSize)) > 0)
            {
                var results = new Span<long>(buffer, read);
                EntryIdEncodings.DecodeAndDiscardFrequency(results, read);
                bitmap.AddRange(results);
            }
        }
    }

    /// <summary>
    /// Fill a bitmap by walking an ITermsProvider's posting list IDs in batches.
    /// Each batch is partitioned into three buckets keyed by TermIdMask:
    ///   - Single: container ID strip + sort/dedup, then bitmap.AddRange.
    ///   - SmallPostingList: container ID strip + sort/dedup, batch Container.GetAll,
    ///     decode each posting list inline via FastPForBufferedReader.
    ///   - PostingList: container ID strip + sort/dedup, then iterate each via FillFromPostings.
    /// Partitioning is branchless: (id &amp; EnsureIsSingleMask) yields the bucket index.
    /// </summary>
    [SkipLocalsInit]
    public static unsafe void FillBitmapFromTreeScan(
        ITermsProvider provider,
        LowLevelTransaction llt,
        ref RoaringBitmap bitmap,
        long limit = long.MaxValue)
    {
        Span<long> plIds = stackalloc long[FillBufferSize];
        Span<long> entryBuffer = stackalloc long[FillBufferSize];

        // Branchless partition: index by (id & EnsureIsSingleMask) yields 0..3.
        // 0=Single, 1=SmallPostingList, 2=PostingList. Slot 3 is unused (mask 0b11) -
        // we keep it so indexing is safe and validate it stays empty.
        Span<NativeList<long>> buckets = stackalloc NativeList<long>[4];
        for (int b = 0; b < buckets.Length; b++)
        {
            buckets[b] = new NativeList<long>();
            buckets[b].Initialize(llt.Allocator, FillBufferSize);
        }

        var pageLocator = llt.PageLocator;

        var containerItems = new ContextBoundNativeList<UnmanagedSpan>(llt.Allocator, FillBufferSize);
        FastPForBufferedReader smallListReader = default;
        bool readerInitialized = false;
        try
        {
            int read;
            while (bitmap.Count < limit && (read = provider.FillPostingListIds(plIds)) > 0)
            {
                for (int b = 0; b < buckets.Length; b++)
                    buckets[b].Clear();

                // Branchless partition - capacity reserved up front, AddUnsafe is safe
                for (int i = 0; i < read; i++)
                {
                    var pid = plIds[i];
                    int idx = (int)(pid & (long)TermIdMask.EnsureIsSingleMask);
                    buckets[idx].AddUnsafe(pid);
                }

                if (buckets[3].Count > 0)
                    throw new InvalidOperationException("Unknown TermIdMask type");

                // Bucket 0: Single -> strip frequency first so dedup is keyed on the entry id
                var singlesSpan = buckets[0].ToSpan();
                if (singlesSpan.Length > 0)
                {
                    EntryIdEncodings.DecodeAndDiscardFrequency(singlesSpan, singlesSpan.Length);
                    var singlesLen = Sorting.SortAndRemoveDuplicates(singlesSpan);
                    bitmap.AddRange(singlesSpan[..singlesLen]);
                }

                // Bucket 1: SmallPostingList -> strip frequency, dedup, batch fetch, decode
                var smallsSpan = buckets[1].ToSpan();
                if (smallsSpan.Length > 0)
                {
                    EntryIdEncodings.DecodeAndDiscardFrequency(smallsSpan, smallsSpan.Length);
                    var smallLen = Sorting.SortAndRemoveDuplicates(smallsSpan);

                    containerItems.Clear();
                    containerItems.EnsureCapacityFor(smallLen);
                    containerItems.Count = smallLen;
                    Container.GetAll(llt, smallsSpan[..smallLen], containerItems.ToSpan(), long.MinValue, pageLocator);

                    if (readerInitialized == false)
                    {
                        smallListReader = new FastPForBufferedReader(llt.Allocator);
                        readerInitialized = true;
                    }

                    fixed (long* pEntryBuffer = entryBuffer)
                    {
                        for (int i = 0; i < smallLen; i++)
                        {
                            var item = containerItems[i];
                            _ = VariableSizeEncoding.Read<int>(item.Address, out var offset);
                            smallListReader.Init(item.Address + offset, item.Length - offset);

                            int smallRead;
                            while ((smallRead = smallListReader.Fill(pEntryBuffer, entryBuffer.Length)) > 0)
                            {
                                EntryIdEncodings.DecodeAndDiscardFrequency(entryBuffer, smallRead);
                                bitmap.AddRange(entryBuffer[..smallRead]);
                            }
                        }
                    }
                }

                // Bucket 2: PostingList -> strip frequency, dedup, then iterate each
                var largeSpan = buckets[2].ToSpan();
                if (largeSpan.Length > 0)
                {
                    EntryIdEncodings.DecodeAndDiscardFrequency(largeSpan, largeSpan.Length);
                    var largeLen = Sorting.SortAndRemoveDuplicates(largeSpan);
                    for (int i = 0; i < largeLen; i++)
                    {
                        var setStateSpan = Container.GetReadOnly(llt, new ContainerEntryId(largeSpan[i]));
                        ref readonly var setState = ref MemoryMarshal.AsRef<PostingListState>(setStateSpan);
                        using var postingList = new PostingList(llt, Slices.Empty, in setState);
                        var iterator = postingList.Iterate();
                        FillFromPostings(ref iterator, ref bitmap);
                    }
                }
            }
        }
        finally
        {
            if (readerInitialized)
                smallListReader.Dispose();
            containerItems.Dispose();
            for (int b = 0; b < buckets.Length; b++)
                buckets[b].Dispose(llt.Allocator);
        }
    }

    /// <summary>AND the bitmap with the union of all posting lists produced by the term provider.
    /// Fills a scratch bitmap from the provider, then ANDs the result bitmap with it.
    /// If the provider produces no matches, the bitmap is cleared.</summary>
    private static void AndBitmapWithTreeScan(
        ITermsProvider provider,
        LowLevelTransaction llt,
        ref RoaringBitmap bitmap,
        ref RoaringBitmap tempBitmap)
    {
        if (bitmap.IsEmpty)
            return;
        tempBitmap.Clear();
        FillBitmapFromTreeScan(provider, llt, ref tempBitmap);
        if (tempBitmap.IsEmpty)
        {
            bitmap.Clear();
            return;
        }
        bitmap.AndWith(ref tempBitmap);
    }

    /// <summary>ANDNOT the bitmap with the union of all posting lists produced by the term provider
    /// (subtract matching entries). If the provider produces no matches, the bitmap is unchanged.</summary>
    private static void AndNotBitmapWithTreeScan(
        ITermsProvider provider,
        LowLevelTransaction llt,
        ref RoaringBitmap bitmap,
        ref RoaringBitmap tempBitmap)
    {
        if (bitmap.IsEmpty)
            return;
        tempBitmap.Clear();
        FillBitmapFromTreeScan(provider, llt, ref tempBitmap);
        if (tempBitmap.IsEmpty)
            return; // subtracting nothing is a no-op
        bitmap.AndNotWith(ref tempBitmap);
    }
}
