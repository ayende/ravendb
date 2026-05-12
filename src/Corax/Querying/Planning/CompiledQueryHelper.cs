using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Corax.Querying.Matches;
using Corax.Utils;

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
            _ => false
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
            _ => false
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
}
