using System;
using System.Collections.Generic;
using System.Diagnostics;
using Corax.Utils.RoaringBitmaps;
using Sparrow.Server;
using Sparrow.Threading;

namespace Voron.Benchmark.Corax;

/// <summary>
/// Profiling benchmark that breaks down where time is spent in roaring bitmap operations.
/// Measures build, PrepareForReading (sort+dedup), and merge separately.
/// </summary>
public static class RoaringBitmapProfileBench
{
    public static void Run()
    {
        Console.WriteLine("=== RoaringBitmap Profiling ===");
        Console.WriteLine();

        (int count, int maxVal)[] scenarios =
        {
            (10_000, 1_000_000),      // 1% density, 1M range — 15 containers
            (100_000, 10_000_000),    // 1% density, 10M range — 152 containers
            (1_000_000, 100_000_000), // 1% density, 100M range — 1525 containers (REGRESSION)
            (50_000, 1_000_000_000),  // 0.005%, 1B range — sparse
            (5_000, 1_000_000_000),   // very sparse
        };

        Console.WriteLine($"{"Scenario",-22} {"Build A",10} {"Build B",10} {"Sort A",10} {"Sort B",10} {"AND merge",10} {"Containers",10} {"Avg/cont",10}");

        foreach (var (count, maxVal) in scenarios)
        {
            long[] valuesA = GenerateValues(count, maxVal, 42);
            long[] valuesB = GenerateValues(count, maxVal, 99);

            using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);

            // Build phase
            var swBuildA = Stopwatch.StartNew();
            var a = new RoaringBitmap(ctx);
            for (int i = 0; i < valuesA.Length; i++) a.Add(valuesA[i]);
            swBuildA.Stop();

            var swBuildB = Stopwatch.StartNew();
            var b = new RoaringBitmap(ctx);
            for (int i = 0; i < valuesB.Length; i++) b.Add(valuesB[i]);
            swBuildB.Stop();

            int containersA = a.ContainerCount;

            // Force sort (simulates first read) — measure separately
            var swSortA = Stopwatch.StartNew();
            a.PrepareForReading();
            swSortA.Stop();

            var swSortB = Stopwatch.StartNew();
            b.PrepareForReading();
            swSortB.Stop();

            // AND merge — arrays already sorted now
            var swMerge = Stopwatch.StartNew();
            a.AndWith(ref b);
            swMerge.Stop();

            int avgPerContainer = count / Math.Max(containersA, 1);

            Console.WriteLine($"{count,7}/{maxVal,-12} {swBuildA.Elapsed.TotalMilliseconds,8:F2}ms {swBuildB.Elapsed.TotalMilliseconds,8:F2}ms {swSortA.Elapsed.TotalMilliseconds,8:F2}ms {swSortB.Elapsed.TotalMilliseconds,8:F2}ms {swMerge.Elapsed.TotalMilliseconds,8:F2}ms {containersA,10} {avgPerContainer,10}");

            a.Dispose();
            b.Dispose();
        }

        Console.WriteLine();

        // Now test: what if we sort during build (simulating sorted input like Corax posting lists)?
        Console.WriteLine("--- Sorted input (simulating Corax posting lists) ---");
        Console.WriteLine($"{"Scenario",-22} {"Build",10} {"AND (alloc)",10} {"AND (IP)",10} {"Containers",10}");

        foreach (var (count, maxVal) in scenarios)
        {
            long[] values = GenerateSortedValues(count, maxVal, 42);
            long[] valuesB = GenerateSortedValues(count, maxVal, 99);

            using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);

            var swBuild = Stopwatch.StartNew();
            var a = new RoaringBitmap(ctx);
            for (int i = 0; i < values.Length; i++) a.Add(values[i]);
            var b = new RoaringBitmap(ctx);
            for (int i = 0; i < valuesB.Length; i++) b.Add(valuesB[i]);
            a.PrepareForReading();
            b.PrepareForReading();
            swBuild.Stop();

            int containers = a.ContainerCount;

            var swAnd = Stopwatch.StartNew();
            a.AndWith(ref b);
            swAnd.Stop();

            // In-place
            var a2 = new RoaringBitmap(ctx);
            for (int i = 0; i < values.Length; i++) a2.Add(values[i]);
            a2.PrepareForReading();
            var swAndIp = Stopwatch.StartNew();
            a2.AndWith(ref b);
            swAndIp.Stop();

            Console.WriteLine($"{count,7}/{maxVal,-12} {swBuild.Elapsed.TotalMilliseconds,8:F2}ms {swAnd.Elapsed.TotalMilliseconds,8:F2}ms {swAndIp.Elapsed.TotalMilliseconds,8:F2}ms {containers,10}");

            a.Dispose();
            a2.Dispose();
            b.Dispose();
        }
    }

    private static long[] GenerateValues(int count, int maxVal, int seed)
    {
        var rng = new Random(seed);
        var set = new HashSet<long>();
        while (set.Count < count) set.Add(rng.NextInt64(0, maxVal));
        var arr = new long[set.Count];
        set.CopyTo(arr);
        return arr;
    }

    private static long[] GenerateSortedValues(int count, int maxVal, int seed)
    {
        var values = GenerateValues(count, maxVal, seed);
        Array.Sort(values);
        return values;
    }
}
