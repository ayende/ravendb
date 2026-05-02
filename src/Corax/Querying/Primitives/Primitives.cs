using System;
using System.Diagnostics;
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
        int matched = 0;
        Page lastPage = default;

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
            int i = (int)Math.Min(skip, read);
            skip = Math.Max(0, skip - read);
            for (; i < read; i++)
            {
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

    /// <summary>Fill bitmap from an IQueryMatch by calling Fill repeatedly.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [SkipLocalsInit]
    public static void FillFromMatch(Matches.Meta.IQueryMatch match, ref RoaringBitmap bitmap)
    {
        Span<long> buffer = stackalloc long[FillBufferSize];
        int read;
        while ((read = match.Fill(buffer)) > 0)
            bitmap.AddRange(buffer.Slice(0, read));
    }

    /// <summary>Fill temp bitmap from match, then AND with target.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [SkipLocalsInit]
    public static void AndWithMatch(Matches.Meta.IQueryMatch match, ref RoaringBitmap bitmap, ref RoaringBitmap tempBitmap)
    {
        tempBitmap.Clear();
        FillFromMatch(match, ref tempBitmap);
        bitmap.AndWith(ref tempBitmap);
    }

    /// <summary>Fill temp bitmap from match, then ANDNOT from target.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    [SkipLocalsInit]
    public static void AndNotWithMatch(Matches.Meta.IQueryMatch match, ref RoaringBitmap bitmap, ref RoaringBitmap tempBitmap)
    {
        tempBitmap.Clear();
        FillFromMatch(match, ref tempBitmap);
        bitmap.AndNotWith(ref tempBitmap);
    }
}
