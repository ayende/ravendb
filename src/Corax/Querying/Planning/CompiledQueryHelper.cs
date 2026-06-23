using System;
using System.Buffers;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Corax.Querying.Matches;
using Corax.Querying.Primitives;
using Corax.Utils;
using Sparrow;
using Voron;
using Voron.Data.CompactTrees;
using Voron.Data.Containers;
using Voron.Data.RoaringBitmaps;

namespace Corax.Querying.Planning;

/// <summary>
/// Helper methods called by emitted IL for timing, result tracking, and the entry-scan
/// iteration loop. Per-entry predicate evaluation is emitted as specialized IL by
/// <see cref="ResidualScanIlEmitter"/> and reached via <c>CompiledQueryMatch.CompiledEntryPredicate</c>.
/// </summary>
public static class CompiledQueryHelper
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void RecordTiming(CompiledQueryMatch ctx, int opIndex, long startTick)
    {
        ctx.Timings[opIndex] = Stopwatch.GetTimestamp() - startTick;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void RecordResultCount(CompiledQueryMatch ctx, int opIndex, int slot)
    {
        ctx.ResultCounts[opIndex] = ctx.Bitmaps[slot].ComputeCount();
    }

    /// <summary>Check StartsWith/EndsWith against ALL terms for a field (multi-value support).
    /// Returns true if ANY term matches.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool CheckFieldTermStartsWith(ref EntryTermsReader reader, long fieldRootPage, ReadOnlySpan<byte> prefix)
    {
        reader.Reset();
        while (reader.FindNext(fieldRootPage))
        {
            if (reader.IsNull || reader.IsNonExisting)
                continue; // Current holds a stale key for null/non-existing terms — don't match against it
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
            if (reader.IsNull || reader.IsNonExisting)
                continue; // Current holds a stale key for null/non-existing terms — don't match against it
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

    public static bool CheckFieldTermInSlice(ref EntryTermsReader reader, long fieldRootPage, ReadOnlySpan<Slice> values, bool includeNull)
    {
        reader.Reset();
        while (reader.FindNext(fieldRootPage))
        {
            if (reader.IsNonExisting)
                continue; // no value at all — Current is not populated; never matches an IN value
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

    public static bool CheckFieldTermInLong(ref EntryTermsReader reader, long fieldRootPage, ReadOnlySpan<long> values, bool includeNull)
    {
        reader.Reset();
        while (reader.FindNext(fieldRootPage))
        {
            if (reader.IsNonExisting)
                continue; // no value at all — CurrentLong is not populated; never matches an IN value
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

    public static bool CheckFieldTermInDouble(ref EntryTermsReader reader, long fieldRootPage, ReadOnlySpan<double> values, bool includeNull)
    {
        reader.Reset();
        while (reader.FindNext(fieldRootPage))
        {
            if (reader.IsNonExisting)
                continue; // no value at all — CurrentDouble is not populated; never matches an IN value
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

    // ALL IN scans the field's terms once per value and fails fast on the first value that no
    // term covers. A single shared match counter can't replace the per-value scan: a duplicate
    // entry term (e.g. Tags=["x","x"] against ALL IN ('x','y')) would bump the counter to the
    // value count while 'y' is still missing — a false positive. Scanning per value is immune to
    // both duplicate entry terms and duplicate IN values, costs no scratch buffer, and stays
    // O(values * terms) like the previous bitmask version.
    public static bool CheckFieldTermAllInSlice(ref EntryTermsReader reader, long fieldRootPage, ReadOnlySpan<Slice> values, bool includeNull)
    {
        for (int k = 0; k < values.Length; k++)
        {
            ReadOnlySpan<byte> needle = values[k].AsReadOnlySpan();
            bool found = false;
            reader.Reset();
            while (reader.FindNext(fieldRootPage))
            {
                if (reader.IsNonExisting == false && reader.IsNull == false && reader.Current.Decoded().SequenceEqual(needle))
                {
                    found = true;
                    break;
                }
            }

            if (found == false)
                return false;
        }

        return includeNull == false || FieldHasNull(ref reader, fieldRootPage);
    }

    public static bool CheckFieldTermAllInLong(ref EntryTermsReader reader, long fieldRootPage, ReadOnlySpan<long> values, bool includeNull)
    {
        for (int k = 0; k < values.Length; k++)
        {
            long needle = values[k];
            bool found = false;
            reader.Reset();
            while (reader.FindNext(fieldRootPage))
            {
                if (reader.IsNonExisting == false && reader.IsNull == false && reader.CurrentLong == needle)
                {
                    found = true;
                    break;
                }
            }

            if (found == false)
                return false;
        }

        return includeNull == false || FieldHasNull(ref reader, fieldRootPage);
    }

    public static bool CheckFieldTermAllInDouble(ref EntryTermsReader reader, long fieldRootPage, ReadOnlySpan<double> values, bool includeNull)
    {
        for (int k = 0; k < values.Length; k++)
        {
            double needle = values[k];
            bool found = false;
            reader.Reset();
            while (reader.FindNext(fieldRootPage))
            {
                if (reader.IsNonExisting == false && reader.IsNull == false && reader.CurrentDouble == needle)
                {
                    found = true;
                    break;
                }
            }

            if (found == false)
                return false;
        }

        return includeNull == false || FieldHasNull(ref reader, fieldRootPage);
    }

    /// <summary>True if the field has at least one null term for the current entry. Used by ALL IN
    /// when the IN list contained a null, mirroring the bitmap pipeline's null-term posting list.</summary>
    private static bool FieldHasNull(ref EntryTermsReader reader, long fieldRootPage)
    {
        reader.Reset();
        while (reader.FindNext(fieldRootPage))
        {
            if (reader.IsNull)
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
        long startTick = Stopwatch.GetTimestamp();

        Span<long> buffer = stackalloc long[QueryPrimitives.EntryScanBatchSize];
        Span<long> containerLocs = stackalloc long[QueryPrimitives.EntryScanBatchSize];
        Span<UnmanagedSpan> spans = stackalloc UnmanagedSpan[QueryPrimitives.EntryScanBatchSize];
        var readers = ArrayPool<EntryTermsReader>.Shared.Rent(QueryPrimitives.EntryScanBatchSize);
        
        var searcher = ctx.Searcher;
        var predicate = ctx.CompiledEntryPredicate;
        var llt = searcher.Transaction.LowLevelTransaction;

        // The emitted predicate evaluates readers strictly one at a time (it never decodes terms
        // from two readers simultaneously), and once it returns RunEntryScan only consumes entry
        // IDs — never the readers' decoded terms. So every reader in the batch can share a single
        // scratch CompactKey instead of one per slot. Acquire it from the pooled buffer set.
        var entryKey = llt.AcquireCompactKey();

        // The target slot may have been used as AND/AndNot scratch by an earlier op, which
        // leaves it marked consumed (and possibly holding stale containers). Reset it so the
        // survivors we Add below start from a clean, writable bitmap.
        targetBitmap.Clear();

        // The source accumulator (slot 0) is consumed raw here, before Execute() prepares the
        // result slot — prior ops may have left ArrayUnsorted containers, which the iterator
        // rejects. Sort/dedup them now so iteration sees only sorted containers.
        sourceBitmap.PrepareForReading();
        var iterator = sourceBitmap.GetIterator();

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
                Container.GetAllSortedByPage(llt, containerLocs[..read], spans, llt.PageLocator);
                searcher.InitializeSpecialTermsMarkers();

                int validCount = 0;
                for (int i = 0; i < read; i++)
                {
                    if (containerLocs[i] == -1 || spans[i].Address == null)
                        continue;
                    readers[validCount] = new EntryTermsReader(llt,
                        searcher.NullTermsMarkers, searcher.NonExistingTermsMarkers,
                        spans[i].Address, spans[i].Length, searcher.DictionaryId,
                        searcher.VectorFieldsMarkers, entryKey);
                    buffer[validCount] = buffer[i]; // compact entry IDs in-place
                    validCount++;
                }

                if (validCount == 0)
                    continue;

                int passed = predicate(exec, readers.AsSpan(0, validCount), buffer[..validCount], Span<int>.Empty);
                ctx.EntryScanEntriesPassed += passed;
                for (int i = 0; i < passed; i++)
                    targetBitmap.Add(buffer[i]);

                if (ctx.EntryScanEntriesPassed >= ctx.Limit)
                    break;
            }
        }
        finally
        {
            // Return the shared scratch key to the pool (keeps its rented buffers for reuse).
            llt.ReleaseCompactKey(ref entryKey);

            iterator.Dispose();
            // clearArray: EntryTermsReader is a struct holding references (LowLevelTransaction, marker HashSets);
            // clearing prevents the shared pool from pinning a transaction's objects until the array is reused.
            ArrayPool<EntryTermsReader>.Shared.Return(readers, clearArray: true);
            ctx.EntryScanTiming = Stopwatch.GetTimestamp() - startTick;
        }
    }
}
