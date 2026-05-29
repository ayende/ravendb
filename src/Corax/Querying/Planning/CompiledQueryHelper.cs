using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Corax.Querying.Matches;
using Corax.Querying.Primitives;
using Corax.Utils;
using Sparrow;
using Voron;
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
        ctx.Timings[opIndex] = Stopwatch.GetTimestamp() - startTick;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void RecordResultCount(CompiledQueryMatch ctx, int opIndex)
    {
        ctx.ResultCounts[opIndex] = ctx.Bitmaps[0].Count;
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

    // ── IN / ALL IN against an entry's terms ────────────────────────────
    //
    // IN (OR semantics): the field has at least one term equal to one of the values.
    // ALL IN (set containment): the field's terms cover every value in the set.
    // Both iterate ALL terms for the field (multi-value support) and compare the entry's
    // stored term against the per-execution value set. Null terms are matched only when the
    // IN list itself contained a null (<paramref name="includeNull"/>), mirroring the bitmap
    // pipeline's null-term posting list. These run only on the entry-scan / direct-scan path,
    // so the small per-call scratch in ALL IN is off the common query path.

    public static bool CheckFieldTermInSlice(ref EntryTermsReader reader, long fieldRootPage, Slice[] values, bool includeNull)
    {
        reader.Reset();
        while (reader.FindNext(fieldRootPage))
        {
            if (reader.IsNull)
            {
                if (includeNull)
                    return true;
                continue;
            }

            ReadOnlySpan<byte> term = reader.Current.Decoded();
            for (int k = 0; k < values.Length; k++)
            {
                if (term.SequenceEqual(values[k].AsReadOnlySpan()))
                    return true;
            }
        }
        return false;
    }

    public static bool CheckFieldTermInLong(ref EntryTermsReader reader, long fieldRootPage, long[] values, bool includeNull)
    {
        reader.Reset();
        while (reader.FindNext(fieldRootPage))
        {
            if (reader.IsNull)
            {
                if (includeNull)
                    return true;
                continue;
            }

            long term = reader.CurrentLong;
            for (int k = 0; k < values.Length; k++)
            {
                if (term == values[k])
                    return true;
            }
        }
        return false;
    }

    public static bool CheckFieldTermInDouble(ref EntryTermsReader reader, long fieldRootPage, double[] values, bool includeNull)
    {
        reader.Reset();
        while (reader.FindNext(fieldRootPage))
        {
            if (reader.IsNull)
            {
                if (includeNull)
                    return true;
                continue;
            }

            double term = reader.CurrentDouble;
            for (int k = 0; k < values.Length; k++)
            {
                if (term == values[k])
                    return true;
            }
        }
        return false;
    }

    public static bool CheckFieldTermAllInSlice(ref EntryTermsReader reader, long fieldRootPage, Slice[] values, bool includeNull)
    {
        Span<bool> matched = values.Length <= 256 ? stackalloc bool[values.Length] : new bool[values.Length];
        matched.Clear();
        bool nullMatched = false;

        reader.Reset();
        while (reader.FindNext(fieldRootPage))
        {
            if (reader.IsNull)
            {
                nullMatched = true;
                continue;
            }

            ReadOnlySpan<byte> term = reader.Current.Decoded();
            for (int k = 0; k < values.Length; k++)
            {
                if (matched[k] == false && term.SequenceEqual(values[k].AsReadOnlySpan()))
                    matched[k] = true;
            }
        }

        for (int k = 0; k < values.Length; k++)
        {
            if (matched[k] == false)
                return false;
        }
        return includeNull == false || nullMatched;
    }

    public static bool CheckFieldTermAllInLong(ref EntryTermsReader reader, long fieldRootPage, long[] values, bool includeNull)
    {
        Span<bool> matched = values.Length <= 256 ? stackalloc bool[values.Length] : new bool[values.Length];
        matched.Clear();
        bool nullMatched = false;

        reader.Reset();
        while (reader.FindNext(fieldRootPage))
        {
            if (reader.IsNull)
            {
                nullMatched = true;
                continue;
            }

            long term = reader.CurrentLong;
            for (int k = 0; k < values.Length; k++)
            {
                if (matched[k] == false && term == values[k])
                    matched[k] = true;
            }
        }

        for (int k = 0; k < values.Length; k++)
        {
            if (matched[k] == false)
                return false;
        }
        return includeNull == false || nullMatched;
    }

    public static bool CheckFieldTermAllInDouble(ref EntryTermsReader reader, long fieldRootPage, double[] values, bool includeNull)
    {
        Span<bool> matched = values.Length <= 256 ? stackalloc bool[values.Length] : new bool[values.Length];
        matched.Clear();
        bool nullMatched = false;

        reader.Reset();
        while (reader.FindNext(fieldRootPage))
        {
            if (reader.IsNull)
            {
                nullMatched = true;
                continue;
            }

            double term = reader.CurrentDouble;
            for (int k = 0; k < values.Length; k++)
            {
                if (matched[k] == false && term == values[k])
                    matched[k] = true;
            }
        }

        for (int k = 0; k < values.Length; k++)
        {
            if (matched[k] == false)
                return false;
        }
        return includeNull == false || nullMatched;
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

        // Lazy scan-param setup: the bitmap pipeline skips analyzer/field-root work at
        // construction time and defers it until entry-scan actually triggers. Most queries
        // never reach this path, so the cost stays off the common path.
        var exec = ctx.Exec;
        if (exec.PopulateScanParams is { } populate)
        {
            populate();
            exec.PopulateScanParams = null;
        }

        try
        {
            int read;
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

                int passed = predicate(exec, readers.AsSpan(0, validCount), buffer[..validCount], Span<int>.Empty);
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
