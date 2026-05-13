using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Corax.Querying.Matches;
using Corax.Querying.Primitives;
using Corax.Utils;
using Voron.Data.RoaringBitmaps;

namespace Corax.Querying.Planning;

/// <summary>
/// Helper methods called by emitted IL for timing, result tracking, and
/// the entry-scan iteration loop. The per-entry predicate evaluation is
/// no longer in this file — it's emitted as specialized IL by
/// <see cref="EntryScanIlEmitter"/> and reached via
/// <c>CompiledQueryMatch.CompiledEntryPredicate</c>.
/// </summary>
public static class CompiledQueryHelper
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void RecordTiming(CompiledQueryMatch ctx, int opIndex, long startTick)
    {
        var timings = ctx.Timings;
        if (timings != null && opIndex < timings.Length)
            timings[opIndex] = Stopwatch.GetTimestamp() - startTick;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void RecordResultCount(CompiledQueryMatch ctx, int opIndex)
    {
        var resultCounts = ctx.ResultCounts;
        if (resultCounts != null && opIndex < resultCounts.Length)
            resultCounts[opIndex] = ctx.Bitmaps[0].Count;
    }

    // ── Slice helpers retained for DirectScanIlEmitter / EntryScanIlEmitter callees ──

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool SliceStartsWith(ReadOnlySpan<byte> actual, ReadOnlySpan<byte> prefix)
        => actual.Length >= prefix.Length && actual.Slice(0, prefix.Length).SequenceEqual(prefix);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool SliceEndsWith(ReadOnlySpan<byte> actual, ReadOnlySpan<byte> suffix)
        => actual.Length >= suffix.Length && actual.Slice(actual.Length - suffix.Length).SequenceEqual(suffix);

    /// <summary>Check StartsWith/EndsWith against ALL terms for a field (multi-value support).
    /// Returns true if ANY term matches.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool CheckFieldTermStartsWith(ref EntryTermsReader reader, long fieldRootPage, ReadOnlySpan<byte> prefix)
    {
        reader.Reset();
        while (reader.FindNext(fieldRootPage))
        {
            if (SliceStartsWith(reader.Current.Decoded(), prefix))
                return true;
        }
        return false;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool CheckFieldTermEndsWith(ref EntryTermsReader reader, long fieldRootPage, ReadOnlySpan<byte> suffix)
    {
        reader.Reset();
        while (reader.FindNext(fieldRootPage))
        {
            if (SliceEndsWith(reader.Current.Decoded(), suffix))
                return true;
        }
        return false;
    }

    // ── Entry scan iteration loop ───────────────────────────────────────

    /// <summary>Run entry scan: iterate the source bitmap in batches, fetch each entry's
    /// stored-fields reader, dispatch into the IL-compiled per-entry predicate, and add
    /// passing entries to the target bitmap. The predicate-walk + value-type/compare-op
    /// dispatch is baked into <c>ctx.CompiledEntryPredicate</c> at plan-compile time, so
    /// this loop has no runtime switches.</summary>
    public static unsafe void RunEntryScan(
        CompiledQueryMatch ctx,
        ref RoaringBitmap sourceBitmap,
        ref RoaringBitmap targetBitmap)
    {
        Span<long> buffer = stackalloc long[QueryPrimitives.EntryScanBatchSize];
        var iterator = sourceBitmap.GetIterator();
        Voron.Page lastPage = default;
        var searcher = ctx.Searcher;
        var predicate = ctx.CompiledEntryPredicate;

        int read;
        while ((read = iterator.Fill(ref sourceBitmap, buffer)) > 0)
        {
            for (int i = 0; i < read; i++)
            {
                long entryId = buffer[i];
                ctx.EntryScanEntriesScanned++;

                var reader = searcher.GetEntryTermsReader(entryId, ref lastPage);
                if (predicate(ctx, ref reader))
                {
                    ctx.EntryScanEntriesPassed++;
                    targetBitmap.Add(entryId);
                }
            }
        }
        iterator.Dispose();
    }
}
