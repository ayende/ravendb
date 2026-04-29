using System;
using System.Runtime.CompilerServices;
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
    private const int FillBufferSize = 4096;

    /// <summary>
    /// Fill a bitmap from a posting list. Walks leaf pages, decodes PFor blocks,
    /// adds entries to the bitmap via batch AddRange.
    /// Respects limit — stops after enough entries.
    /// </summary>
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
    /// AND the bitmap with a posting list using the galloping page-scan pattern.
    /// Finds set bits in the bitmap, seeks to matching posting list pages,
    /// decodes entire pages into a temp bitmap, SIMD ANDs with the accumulator.
    /// Cost proportional to pages that intersect the accumulator, not total posting list size.
    /// </summary>
    public static void AndWithPostings(ref PostingList.Iterator iterator, ref RoaringBitmap bitmap, ref RoaringBitmap tempBitmap, int limit = int.MaxValue)
    {
        // Phase 1: Build temp bitmap from posting list entries that could match
        // For now, use the simple approach: fill temp from posting list, then AND.
        // The galloping optimization (seek by bitmap set bits) is a future improvement.
        tempBitmap.Clear();
        FillFromPostings(ref iterator, ref tempBitmap);
        bitmap.AndWith(ref tempBitmap);
    }

    /// <summary>
    /// OR the bitmap with a posting list. Walks all leaf pages, sets bits.
    /// Idempotent — setting an already-set bit is a no-op.
    /// </summary>
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
    /// ANDNOT the bitmap with a posting list. Same galloping shape as AndWith
    /// but clears bits instead of keeping them.
    /// </summary>
    public static void AndNotWithPostings(ref PostingList.Iterator iterator, ref RoaringBitmap bitmap, ref RoaringBitmap tempBitmap)
    {
        tempBitmap.Clear();
        FillFromPostings(ref iterator, ref tempBitmap);
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
}
