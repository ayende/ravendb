using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Corax.Querying.Matches;
using Corax.Utils;
using Voron.Data.RoaringBitmaps;

namespace Corax.Querying.Planning;

/// <summary>
/// Helper methods called by emitted IL for timing, result tracking, and
/// predicate evaluation. Methods are [AggressiveInlining] so the JIT
/// can inline them into generated delegates.
/// </summary>
public static class CompiledQueryHelper
{
    /// <summary>Record timing for plan op. Called by emitted IL.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void RecordTiming(CompiledQueryMatch ctx, int opIndex, long startTick)
    {
        var timings = ctx.Timings;
        if (timings != null && opIndex < timings.Length)
            timings[opIndex] = Stopwatch.GetTimestamp() - startTick;
    }

    /// <summary>Record bitmap result count after plan op. Called by emitted IL.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static void RecordResultCount(CompiledQueryMatch ctx, int opIndex)
    {
        var resultCounts = ctx.ResultCounts;
        if (resultCounts != null && opIndex < resultCounts.Length)
            resultCounts[opIndex] = ctx.Bitmaps[0].Count;
    }

    // ── Predicate evaluation helpers ────────────────────────────────────

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool CompareLong(long actual, long param, ScanCompareOp op)
    {
        return op switch
        {
            ScanCompareOp.Equal => actual == param,
            ScanCompareOp.NotEqual => actual != param,
            ScanCompareOp.GreaterThan => actual > param,
            ScanCompareOp.GreaterThanOrEqual => actual >= param,
            ScanCompareOp.LessThan => actual < param,
            ScanCompareOp.LessThanOrEqual => actual <= param,
            _ => throw new ArgumentOutOfRangeException(nameof(op), op, "Unsupported compare op for long")
        };
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool CompareLongBetween(long actual, long low, long high)
        => actual >= low && actual <= high;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool CompareDouble(double actual, double param, ScanCompareOp op)
    {
        return op switch
        {
            ScanCompareOp.Equal => actual == param,
            ScanCompareOp.NotEqual => actual != param,
            ScanCompareOp.GreaterThan => actual > param,
            ScanCompareOp.GreaterThanOrEqual => actual >= param,
            ScanCompareOp.LessThan => actual < param,
            ScanCompareOp.LessThanOrEqual => actual <= param,
            _ => throw new ArgumentOutOfRangeException(nameof(op), op, "Unsupported compare op for double")
        };
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool CompareDoubleBetween(double actual, double low, double high)
        => actual >= low && actual <= high;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool CompareSlice(ReadOnlySpan<byte> actual, ReadOnlySpan<byte> param, ScanCompareOp op)
    {
        return op switch
        {
            ScanCompareOp.Equal => actual.SequenceEqual(param),
            ScanCompareOp.NotEqual => actual.SequenceEqual(param) == false,
            ScanCompareOp.GreaterThan => actual.SequenceCompareTo(param) > 0,
            ScanCompareOp.GreaterThanOrEqual => actual.SequenceCompareTo(param) >= 0,
            ScanCompareOp.LessThan => actual.SequenceCompareTo(param) < 0,
            ScanCompareOp.LessThanOrEqual => actual.SequenceCompareTo(param) <= 0,
            _ => false
        };
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static bool CompareSliceBetween(ReadOnlySpan<byte> actual, ReadOnlySpan<byte> low, ReadOnlySpan<byte> high)
        => actual.SequenceCompareTo(low) >= 0 && actual.SequenceCompareTo(high) <= 0;

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

    // ── Entry scan batch method ─────────────────────────────────────────

    /// <summary>Run entry scan on a batch of entry IDs. Reads each entry's stored fields,
    /// evaluates all predicates, adds passing entries to the target bitmap.
    /// Called by both the IL entry scan path and DirectScanMatch.</summary>
    public static unsafe void RunEntryScan(
        Matches.CompiledQueryMatch ctx,
        ref RoaringBitmap sourceBitmap,
        ref RoaringBitmap targetBitmap,
        ScanPredicateInfo[] predicates,
        long[] longParams, double[] doubleParams, Voron.Slice[] sliceParams, long[] fieldRootPages)
    {
        Span<long> buffer = stackalloc long[256]; // EntryScanBatchSize
        var iterator = sourceBitmap.GetIterator();
        Voron.Page lastPage = default;
        var searcher = ctx.Searcher;

        int read;
        while ((read = iterator.Fill(ref sourceBitmap, buffer)) > 0)
        {
            for (int i = 0; i < read; i++)
            {
                long entryId = buffer[i];
                ctx.EntryScanEntriesScanned++;

                var reader = searcher.GetEntryTermsReader(entryId, ref lastPage);
                bool passed = true;
                int rootIdx = 0;

                for (int p = 0; p < predicates.Length && passed; p++)
                {
                    ref readonly var pred = ref predicates[p];

                    if (pred.OrBranches != null)
                    {
                        // OR group: pass if any branch passes
                        bool anyPassed = false;
                        for (int b = 0; b < pred.OrBranches.Length; b++)
                        {
                            reader.Reset();
                            if (reader.FindNext(fieldRootPages[rootIdx + b]))
                            {
                                if (EvaluateSinglePredicate(ref reader, pred.OrBranches[b], longParams, doubleParams, sliceParams))
                                {
                                    anyPassed = true;
                                    break;
                                }
                            }
                        }
                        rootIdx += pred.OrBranches.Length;
                        passed = anyPassed;
                        continue;
                    }

                    reader.Reset();
                    bool found = reader.FindNext(fieldRootPages[rootIdx]);
                    rootIdx++;

                    if (pred.CompareOp == ScanCompareOp.Exists)
                    {
                        passed = found;
                    }
                    else if (pred.CompareOp == ScanCompareOp.NotEqual)
                    {
                        passed = found == false || EvaluateSinglePredicate(ref reader, pred, longParams, doubleParams, sliceParams);
                    }
                    else
                    {
                        passed = found && EvaluateSinglePredicate(ref reader, pred, longParams, doubleParams, sliceParams);
                    }
                }

                if (passed)
                {
                    ctx.EntryScanEntriesPassed++;
                    targetBitmap.Add(entryId);
                }
            }
        }
        iterator.Dispose();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool EvaluateSinglePredicate(ref EntryTermsReader reader, in ScanPredicateInfo pred,
        long[] longParams, double[] doubleParams, Voron.Slice[] sliceParams)
    {
        return pred.ValueType switch
        {
            ScanValueType.Long when pred.CompareOp == ScanCompareOp.Between =>
                CompareLongBetween(reader.CurrentLong, longParams[pred.ParamIndex], longParams[pred.ParamIndex2]),
            ScanValueType.Long =>
                CompareLong(reader.CurrentLong, longParams[pred.ParamIndex], pred.CompareOp),
            ScanValueType.Double when pred.CompareOp == ScanCompareOp.Between =>
                CompareDoubleBetween(reader.CurrentDouble, doubleParams[pred.ParamIndex], doubleParams[pred.ParamIndex2]),
            ScanValueType.Double =>
                CompareDouble(reader.CurrentDouble, doubleParams[pred.ParamIndex], pred.CompareOp),
            ScanValueType.Slice when pred.CompareOp == ScanCompareOp.Between =>
                CompareSliceBetween(reader.Current.Decoded(), sliceParams[pred.ParamIndex].AsReadOnlySpan(), sliceParams[pred.ParamIndex2].AsReadOnlySpan()),
            ScanValueType.Slice when pred.CompareOp == ScanCompareOp.StartsWith =>
                SliceStartsWith(reader.Current.Decoded(), sliceParams[pred.ParamIndex].AsReadOnlySpan()),
            ScanValueType.Slice when pred.CompareOp == ScanCompareOp.EndsWith =>
                SliceEndsWith(reader.Current.Decoded(), sliceParams[pred.ParamIndex].AsReadOnlySpan()),
            ScanValueType.Slice =>
                CompareSlice(reader.Current.Decoded(), sliceParams[pred.ParamIndex].AsReadOnlySpan(), pred.CompareOp),
            _ => false
        };
    }
}
