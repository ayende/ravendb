using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Corax.Querying.Matches;
using Corax.Querying.Primitives;
using Corax.Utils;
using Sparrow;
using Voron.Data.Containers;
using Voron.Data.RoaringBitmaps;

namespace Corax.Querying.Planning;

/// <summary>
/// Helper methods called by emitted IL for timing, result tracking, and
/// the entry-scan iteration loop. The per-entry predicate evaluation is
/// no longer in this file — it's emitted as specialized IL by
/// <see cref="ResidualScanIlEmitter"/> and reached via
/// <c>CompiledQueryMatch.CompiledEntryPredicate</c>.
/// </summary>
public static class CompiledQueryHelper
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void RecordTiming(CompiledQueryMatch ctx, int opIndex, long startTick)
    {
        var timings = ctx.Timings;
        if (opIndex < timings.Length)
            timings[opIndex] = Stopwatch.GetTimestamp() - startTick;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void RecordResultCount(CompiledQueryMatch ctx, int opIndex)
    {
        var resultCounts = ctx.ResultCounts;
        if (opIndex < resultCounts.Length)
            resultCounts[opIndex] = ctx.Bitmaps[0].Count;
    }

    /// <summary>Check StartsWith/EndsWith against ALL terms for a field (multi-value support).
    /// Returns true if ANY term matches.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool CheckFieldTermStartsWith(ref EntryTermsReader reader, long fieldRootPage, ReadOnlySpan<byte> prefix)
    {
        reader.Reset();
        while (reader.FindNext(fieldRootPage))
        {
            if (reader.Current.Decoded().StartsWith(prefix))
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
            if (reader.Current.Decoded().EndsWith(suffix))
                return true;
        }
        return false;
    }

    // ── Entry scan iteration loop ───────────────────────────────────────

    /// <summary>Run entry scan: iterate the source bitmap in batches, resolve all entries
    /// in each batch, evaluate the compiled predicate delegate once per batch (compact
    /// survivors in-place), and add passing entries to the target bitmap. Batching avoids
    /// the per-entry delegate invocation overhead — the IL-emitted predicate processes
    /// the entire reader span and compacts entry IDs in a single call.</summary>
    public static unsafe void RunEntryScan(
        CompiledQueryMatch ctx,
        ref RoaringBitmap sourceBitmap,
        ref RoaringBitmap targetBitmap)
    {
        Span<long> buffer = stackalloc long[QueryPrimitives.EntryScanBatchSize];
        Span<long> containerLocs = stackalloc long[QueryPrimitives.EntryScanBatchSize];
        Span<UnmanagedSpan> spans = stackalloc UnmanagedSpan[QueryPrimitives.EntryScanBatchSize];
        var readers = new EntryTermsReader[QueryPrimitives.EntryScanBatchSize];

        var iterator = sourceBitmap.GetIterator();
        var searcher = ctx.Searcher;
        var predicate = ctx.CompiledEntryPredicate;
        var llt = searcher.Transaction.LowLevelTransaction;

        int read;
        try
        {
            while ((read = iterator.Fill(ref sourceBitmap, buffer)) > 0)
            {
                ctx.Token.ThrowIfCancellationRequested();

                var batch = buffer[..read];
                ctx.EntryScanEntriesScanned += read;

                searcher.ResolveEntryLocations(batch, containerLocs);
                Container.GetAll(llt, containerLocs[..read], spans, -1, llt.PageLocator);
                searcher.InitializeSpecialTermsMarkers();

                int validCount = 0;
                for (int i = 0; i < read; i++)
                {
                    if (containerLocs[i] == -1 || spans[i].Address == null)
                        continue;
                    readers[validCount] = new EntryTermsReader(llt,
                        searcher.NullTermsMarkers, searcher.NonExistingTermsMarkers,
                        spans[i].Address, spans[i].Length, searcher.DictionaryId,
                        searcher.VectorFieldsMarkers, null);
                    buffer[validCount] = buffer[i]; // compact entry IDs in-place
                    validCount++;
                }

                if (validCount == 0)
                    continue;

                int passed = predicate(ctx, readers.AsSpan(0, validCount), buffer[..validCount], Span<int>.Empty);
                ctx.EntryScanEntriesPassed += passed;
                for (int i = 0; i < passed; i++)
                    targetBitmap.Add(buffer[i]);
            }
        }
        finally
        {
            iterator.Dispose();
        }
    }
}
