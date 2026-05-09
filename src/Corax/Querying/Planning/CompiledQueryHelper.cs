using System.Diagnostics;
using System.Runtime.CompilerServices;
using Corax.Querying.Matches;

namespace Corax.Querying.Planning;

/// <summary>
/// Helper methods called by emitted IL for timing and result tracking.
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
}
