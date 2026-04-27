using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using Corax.Utils.RoaringBitmaps;
using Sparrow.Server;
using Sparrow.Server.Collections;
using Sparrow.Threading;

namespace Voron.Benchmark.Corax;

/// <summary>
/// Quick standalone benchmark that can run without BenchmarkDotNet's auto-generated project build.
/// Invoke via: dotnet run -c Release -- quick-roaring
/// </summary>
public static class RoaringBitmapQuickBench
{
    public static void Run()
    {
        Console.WriteLine("=== RoaringBitmap Quick Benchmark ===");
        Console.WriteLine($"Runtime: {System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription}");
        Console.WriteLine();

        int[] counts = { 500, 5_000, 50_000, 500_000 };
        int[] maxValues = { 1_000_000, 1_000_000_000 };

        Console.WriteLine("--- BUILD (Add N values + Finalize) ---");
        Console.WriteLine($"{"Count",10} {"MaxValue",12} {"Roaring",12} {"GrowBitArr",12} {"BCL BitArr",12} {"Roaring MB",12} {"GrowBA MB",12} {"BCL MB",12}");

        foreach (int maxVal in maxValues)
        {
            foreach (int count in counts)
            {
                if (maxVal == 1_000_000_000 && count > 100_000)
                    continue; // skip to avoid very long GrowableBitArray at 1B

                long[] values = GenerateValues(count, maxVal);

                // Roaring
                var (roaringMs, roaringNative) = BenchBuildRoaring(values);

                // GrowableBitArray
                var (gbaMs, gbaNative) = BenchBuildGrowableBitArray(values, maxVal);

                // BCL BitArray
                var (bclMs, bclManaged) = BenchBuildBclBitArray(values, maxVal);

                Console.WriteLine($"{count,10} {maxVal,12} {roaringMs,10:F2}ms {gbaMs,10:F2}ms {bclMs,10:F2}ms {roaringNative / (1024.0 * 1024),10:F2} {gbaNative / (1024.0 * 1024),10:F2} {bclManaged / (1024.0 * 1024),10:F2}");
            }
        }

        Console.WriteLine();
        Console.WriteLine("--- CONTAINS (probe N values against built set) ---");
        Console.WriteLine($"{"Count",10} {"MaxValue",12} {"Roaring",12} {"GrowBitArr",12} {"BCL BitArr",12}");

        foreach (int maxVal in maxValues)
        {
            foreach (int count in counts)
            {
                if (maxVal == 1_000_000_000 && count > 100_000)
                    continue;

                long[] valuesA = GenerateValues(count, maxVal, seed: 42);
                long[] valuesB = GenerateValues(count, maxVal, seed: 99);

                double roaringMs = BenchContainsRoaring(valuesA, valuesB);
                double gbaMs = BenchContainsGrowableBitArray(valuesA, valuesB, maxVal);
                double bclMs = BenchContainsBclBitArray(valuesA, valuesB, maxVal);

                Console.WriteLine($"{count,10} {maxVal,12} {roaringMs,10:F2}ms {gbaMs,10:F2}ms {bclMs,10:F2}ms");
            }
        }

        Console.WriteLine();
        Console.WriteLine("--- SET OPERATIONS (AND/OR/ANDNOT on two sets) ---");
        Console.WriteLine($"{"Count",10} {"MaxValue",12} {"Density",8} {"Op",8} {"Roaring",12} {"Roaring IP",12} {"BCL BitArr",12}");

        // Test matrix: varying density from very dense to very sparse
        (int count, int maxVal)[] setOpParams =
        {
            // === 1M range ===
            (500_000, 1_000_000),   // 50%
            (100_000, 1_000_000),   // 10%
            (10_000, 1_000_000),    // 1%

            // === 10M range ===
            (5_000_000, 10_000_000),  // 50%
            (2_500_000, 10_000_000),  // 25%
            (1_000_000, 10_000_000),  // 10%
            (100_000, 10_000_000),    // 1%
            (10_000, 10_000_000),     // 0.1%

            // === 100M range ===
            (10_000_000, 100_000_000), // 10%
            (1_000_000, 100_000_000),  // 1%
            (250_000, 100_000_000),    // 0.25%
            (25_000, 100_000_000),     // 0.025%

            // === 1B range ===
            (250_000, 1_000_000_000),  // 0.025%
            (50_000, 1_000_000_000),   // 0.005%
            (5_000, 1_000_000_000),    // 0.0005%
        };

        foreach (var (count, maxVal) in setOpParams)
        {
            long[] valuesA = GenerateValues(count, maxVal, seed: 42);
            long[] valuesB = GenerateValues(count, maxVal, seed: 99);
            string density = $"{(double)count / maxVal * 100:F2}%";

            foreach (string op in new[] { "AND", "OR", "ANDNOT" })
            {
                double roaringMs = BenchSetOpRoaring(valuesA, valuesB, maxVal, op);
                double roaringIpMs = BenchSetOpRoaringInPlace(valuesA, valuesB, maxVal, op);
                double bclMs = BenchSetOpBcl(valuesA, valuesB, maxVal, op);

                Console.WriteLine($"{count,10} {maxVal,12} {density,8} {op,8} {roaringMs,10:F2}ms {roaringIpMs,10:F2}ms {bclMs,10:F2}ms");
            }
            Console.WriteLine();
        }
    }

    private static long[] GenerateValues(int count, int maxVal, int seed = 42)
    {
        var rng = new Random(seed);
        var set = new HashSet<long>();
        while (set.Count < count)
            set.Add(rng.NextInt64(0, maxVal));
        var arr = new long[set.Count];
        set.CopyTo(arr);
        return arr;
    }

    private static (double ms, long nativeBytes) BenchBuildRoaring(long[] values)
    {
        // Warmup
        using (var ctx = new ByteStringContext(SharedMultipleUseFlag.None))
        {
            var bmp = new RoaringBitmap(ctx);
            for (int i = 0; i < values.Length; i++) bmp.Add(values[i]);
            bmp.Dispose();
        }

        long nativeBefore = Sparrow.Utils.NativeMemory.CurrentThreadStats?.TotalAllocated ?? 0;
        var sw = Stopwatch.StartNew();
        using var ctx2 = new ByteStringContext(SharedMultipleUseFlag.None);
        var bmp2 = new RoaringBitmap(ctx2);
        for (int i = 0; i < values.Length; i++) bmp2.Add(values[i]);
        bmp2.PrepareForReading(); // sort all unsorted containers — part of build cost
        sw.Stop();
        long nativeAfter = Sparrow.Utils.NativeMemory.CurrentThreadStats?.TotalAllocated ?? 0;
        bmp2.Dispose();
        return (sw.Elapsed.TotalMilliseconds, nativeAfter - nativeBefore);
    }

    private static (double ms, long nativeBytes) BenchBuildGrowableBitArray(long[] values, int maxVal)
    {
        // Warmup
        using (var ctx = new ByteStringContext(SharedMultipleUseFlag.None))
        {
            var gba = new GrowableBitArray(ctx, maxVal);
            for (int i = 0; i < values.Length; i++) gba.Add(values[i]);
            gba.Dispose();
        }

        long nativeBefore = Sparrow.Utils.NativeMemory.CurrentThreadStats?.TotalAllocated ?? 0;
        var sw = Stopwatch.StartNew();
        using var ctx2 = new ByteStringContext(SharedMultipleUseFlag.None);
        var gba2 = new GrowableBitArray(ctx2, maxVal);
        for (int i = 0; i < values.Length; i++) gba2.Add(values[i]);
        sw.Stop();
        long nativeAfter = Sparrow.Utils.NativeMemory.CurrentThreadStats?.TotalAllocated ?? 0;
        gba2.Dispose();
        return (sw.Elapsed.TotalMilliseconds, nativeAfter - nativeBefore);
    }

    private static (double ms, long managedBytes) BenchBuildBclBitArray(long[] values, int maxVal)
    {
        long before = GC.GetAllocatedBytesForCurrentThread();
        var sw = Stopwatch.StartNew();
        var ba = new BitArray(maxVal + 1);
        for (int i = 0; i < values.Length; i++) ba.Set((int)values[i], true);
        sw.Stop();
        long after = GC.GetAllocatedBytesForCurrentThread();
        return (sw.Elapsed.TotalMilliseconds, after - before);
    }

    private static double BenchContainsRoaring(long[] buildValues, long[] probeValues)
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        var bmp = new RoaringBitmap(ctx);
        for (int i = 0; i < buildValues.Length; i++) bmp.Add(buildValues[i]);
        bmp.PrepareForReading();

        var sw = Stopwatch.StartNew();
        int found = 0;
        for (int i = 0; i < probeValues.Length; i++)
            if (bmp.Contains(probeValues[i])) found++;
        sw.Stop();
        bmp.Dispose();
        return sw.Elapsed.TotalMilliseconds;
    }

    private static double BenchContainsGrowableBitArray(long[] buildValues, long[] probeValues, int maxVal)
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        var gba = new GrowableBitArray(ctx, maxVal);
        for (int i = 0; i < buildValues.Length; i++) gba.Add(buildValues[i]);

        var sw = Stopwatch.StartNew();
        int found = 0;
        for (int i = 0; i < probeValues.Length; i++)
            if (gba.Contains(probeValues[i])) found++;
        sw.Stop();
        gba.Dispose();
        return sw.Elapsed.TotalMilliseconds;
    }

    private static double BenchContainsBclBitArray(long[] buildValues, long[] probeValues, int maxVal)
    {
        var ba = new BitArray(maxVal + 1);
        for (int i = 0; i < buildValues.Length; i++) ba.Set((int)buildValues[i], true);

        var sw = Stopwatch.StartNew();
        int found = 0;
        for (int i = 0; i < probeValues.Length; i++)
            if (ba.Get((int)probeValues[i])) found++;
        sw.Stop();
        return sw.Elapsed.TotalMilliseconds;
    }

    private static double BenchSetOpRoaring(long[] valuesA, long[] valuesB, int maxVal, string op)
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        var a = new RoaringBitmap(ctx);
        var b = new RoaringBitmap(ctx);
        for (int i = 0; i < valuesA.Length; i++) a.Add(valuesA[i]);
        for (int i = 0; i < valuesB.Length; i++) b.Add(valuesB[i]);
        a.PrepareForReading();
        b.PrepareForReading();

        var sw = Stopwatch.StartNew();
        switch (op)
        {
            case "AND": a.AndWith(ref b); break;
            case "OR": a.OrWith(ref b); break;
            case "ANDNOT": a.AndNotWith(ref b); break;
            default: throw new ArgumentException(op);
        }
        long c = a.Cardinality;
        sw.Stop();
        a.Dispose();
        b.Dispose();
        return sw.Elapsed.TotalMilliseconds;
    }

    private static double BenchSetOpRoaringInPlace(long[] valuesA, long[] valuesB, int maxVal, string op)
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        var a = new RoaringBitmap(ctx);
        var b = new RoaringBitmap(ctx);
        for (int i = 0; i < valuesA.Length; i++) a.Add(valuesA[i]);
        for (int i = 0; i < valuesB.Length; i++) b.Add(valuesB[i]);
        a.PrepareForReading();
        b.PrepareForReading();

        var sw = Stopwatch.StartNew();
        switch (op)
        {
            case "AND": a.AndWith(ref b); break;
            case "OR": a.OrWith(ref b); break;
            case "ANDNOT": a.AndNotWith(ref b); break;
        }
        long c = a.Cardinality;
        sw.Stop();
        a.Dispose();
        b.Dispose();
        return sw.Elapsed.TotalMilliseconds;
    }

    private static double BenchSetOpBcl(long[] valuesA, long[] valuesB, int maxVal, string op)
    {
        var a = new BitArray(maxVal + 1);
        var b = new BitArray(maxVal + 1);
        for (int i = 0; i < valuesA.Length; i++) a.Set((int)valuesA[i], true);
        for (int i = 0; i < valuesB.Length; i++) b.Set((int)valuesB[i], true);

        var sw = Stopwatch.StartNew();
        switch (op)
        {
            case "AND": a.And(b); break;
            case "OR": a.Or(b); break;
            case "ANDNOT":
                var notB = (BitArray)b.Clone();
                notB.Not();
                a.And(notB);
                break;
        }
        sw.Stop();
        return sw.Elapsed.TotalMilliseconds;
    }
}
