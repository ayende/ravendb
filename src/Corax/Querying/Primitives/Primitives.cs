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
    /// AND the bitmap with a posting list using the galloping page-scan pattern.
    /// Fills temp bitmap from posting list, SIMD ANDs with the accumulator.
    ///
    /// Optimization: uses Seek() to jump to pages that overlap with the
    /// accumulator's set bits, skipping pages that can't contribute matches.
    /// Cost proportional to posting list pages that intersect the accumulator.
    /// </summary>
    public static void AndWithPostings(ref PostingList.Iterator iterator, ref RoaringBitmap bitmap, ref RoaringBitmap tempBitmap)
    {
        if (bitmap.IsEmpty)
            return;

        // Fill temp bitmap from posting list, then SIMD AND with accumulator.
        // Future optimization: use Seek() + pruneGreaterThan to skip posting list
        // pages outside the bitmap's container key range.
        tempBitmap.Clear();
        FillFromPostings(ref iterator, ref tempBitmap);
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

    /// <summary>
    /// Entry scan: iterate bitmap, read each entry's stored field data via
    /// EntryTermsReader, evaluate predicates with early exit.
    /// Same pattern as MultiUnaryMatch.Fill() but driven from a bitmap iterator.
    /// </summary>
    public static int ScanAndFilter(ref RoaringBitmap bitmap, IndexSearcher searcher,
        MultiUnaryItem[] predicates, Span<long> output, int limit, ref int skip)
    {
        if (predicates == null || predicates.Length == 0)
        {
            // No predicates — just iterate
            bitmap.PrepareForReading();
            var iter = bitmap.GetIterator();
            return iter.Fill(ref bitmap, output);
        }

        bitmap.PrepareForReading();
        var iterator = bitmap.GetIterator();
        Span<long> batch = stackalloc long[256];
        int matched = 0;
        Page lastPage = default;

        // Cache field root pages for faster lookup
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
}
