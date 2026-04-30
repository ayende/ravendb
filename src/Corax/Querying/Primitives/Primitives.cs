using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Corax.Querying.Matches;
using Corax.Utils;
using Corax.Utils.RoaringBitmaps;
using Voron;
using Voron.Data.PostingLists;

namespace Corax.Querying.Primitives;

/// <summary>
/// Static methods called by compiled query functions (DynamicMethod IL).
/// Each primitive operates on a RoaringBitmap accumulator.
/// Pre-compiled, SIMD-tuned, individually benchmarkable.
/// </summary>
public static class QueryPrimitives
{
    // Buffer size for stackalloc Fill operations. Benchmark different values
    // (1024, 2048, 4096, 8192) with the optimized AddRange to find the sweet spot.
    // Larger buffers reduce Fill() call overhead but increase stack usage.
    private const int FillBufferSize = 4096;

    /// <summary>
    /// Fill a bitmap from a posting list. Walks leaf pages, decodes PFor blocks,
    /// adds entries to the bitmap via batch AddRange.
    /// Respects limit — stops after enough entries.
    /// </summary>
    [SkipLocalsInit]
    public static void FillFromPostings(ref PostingList.Iterator iterator, ref RoaringBitmap bitmap, int limit = int.MaxValue)
    {
        Span<long> buffer = stackalloc long[FillBufferSize];

        while (iterator.Fill(buffer, out int read) && read > 0)
        {
            // Posting list Fill may encode frequencies in the high bits — decode
            EntryIdEncodings.DecodeAndDiscardFrequency(buffer, read);
            bitmap.AddRange(buffer.Slice(0, read));

            if (bitmap.Count >= limit)
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
        if (minKey < 0)
            return; // bitmap is empty (shouldn't happen after IsEmpty check, but defensive)

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
        if (minKey < 0)
            return;

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
    /// Lazy OR — same as OrWithPostings but intended for use in multi-term IN chains.
    /// Call RepairAfterLazy() on the bitmap after all lazy OR operations.
    /// </summary>
    public static void LazyOrWithPostings(ref PostingList.Iterator iterator, ref RoaringBitmap bitmap)
    {
        // Currently delegates to OrWithPostings. When LazyOrWith on RoaringBitmap
        // is fully implemented (skip popcount per container), this will use that.
        OrWithPostings(ref iterator, ref bitmap);
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
        return bitmapCount < 32_000
            && bitmapCount * 64 < postingListState.NumberOfEntries;
    }

    /// <summary>
    /// Runtime check: should we heap-sort the bitmap directly instead of
    /// walking the sort-field index?
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool ShouldHeapSortDirectly(ref RoaringBitmap bitmap, long sortFieldTotalEntries)
    {
        long bitmapCount = bitmap.Count;
        return bitmapCount < 32_000
            && bitmapCount * 64 < sortFieldTotalEntries;
    }

    /// <summary>
    /// Entry scan: iterate bitmap, read each entry's stored field data via
    /// EntryTermsReader, evaluate predicates with early exit.
    /// Same pattern as MultiUnaryMatch.Fill() but driven from a bitmap iterator.
    /// </summary>
    [SkipLocalsInit]
    public static int ScanAndFilter(ref RoaringBitmap bitmap, IndexSearcher searcher,
        MultiUnaryItem[] predicates, Span<long> output, int limit, ref int skip)
    {
        if (predicates == null || predicates.Length == 0)
        {
            // No predicates — iterate with skip/limit
            bitmap.PrepareForReading();
            var iter = bitmap.GetIterator();
            try
            {
                if (skip > 0)
                {
                    Span<long> skipBuf = stackalloc long[Math.Min(skip, 1024)];
                    while (skip > 0)
                    {
                        int toSkip = Math.Min(skip, skipBuf.Length);
                        int skipped = iter.Fill(ref bitmap, skipBuf.Slice(0, toSkip));
                        if (skipped == 0) return 0;
                        skip -= skipped;
                    }
                }
                int maxOutput = (int)Math.Min(output.Length, limit);
                return iter.Fill(ref bitmap, output.Slice(0, maxOutput));
            }
            finally
            {
                iter.Dispose();
            }
        }

        bitmap.PrepareForReading();
        var iterator = bitmap.GetIterator();

        try
        {
            Span<long> batch = stackalloc long[256];
            int matched = 0;
            Page lastPage = default;

            Span<long> fieldRootPages = predicates.Length > 128
                ? new long[predicates.Length]
                : stackalloc long[predicates.Length];

            for (int p = 0; p < predicates.Length; p++)
            {
                fieldRootPages[p] = searcher.FieldCache.GetLookupRootPage(predicates[p].Binding.FieldName);
            }

            int read;
            while ((read = iterator.Fill(ref bitmap, batch)) > 0)
            {
                for (int i = 0; i < read; i++)
                {
                    if (skip > 0)
                    {
                        skip--;
                        continue;
                    }

                    long entryId = batch[i];
                    var reader = searcher.GetEntryTermsReader(entryId, ref lastPage);
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
        finally
        {
            iterator.Dispose();
        }
    }
}
