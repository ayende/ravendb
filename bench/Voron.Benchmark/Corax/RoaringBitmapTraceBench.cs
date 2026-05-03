using System;
using System.Collections.Generic;
using System.Threading;
using Voron.Data.RoaringBitmaps;
using Sparrow.Server;
using Sparrow.Threading;

namespace Voron.Benchmark.Corax;

/// <summary>
/// Runs hot scenarios in a loop for dotnet-trace profiling.
/// Usage: dotnet-trace collect -- dotnet run -c Release -- trace-roaring
/// </summary>
public static class RoaringBitmapTraceBench
{
    public static void Run()
    {
        Console.WriteLine("=== RoaringBitmap Trace Benchmark ===");
        Console.WriteLine("Warming up...");

        // Prepare data for the regression scenario: 1M values in 100M range
        long[] valuesA = GenerateValues(1_000_000, 100_000_000, 42);
        long[] valuesB = GenerateValues(1_000_000, 100_000_000, 99);

        // Also prepare sorted data (Corax-like)
        long[] sortedA = (long[])valuesA.Clone();
        long[] sortedB = (long[])valuesB.Clone();
        Array.Sort(sortedA);
        Array.Sort(sortedB);

        // Warmup
        RunScenario(valuesA, valuesB, "warmup-random");
        RunScenario(sortedA, sortedB, "warmup-sorted");

        Console.WriteLine("Ready for profiling. Running 20 iterations...");
        Console.WriteLine("Attach with: dotnet-trace collect -p " + Environment.ProcessId);
        Thread.Sleep(2000); // give time to attach

        // Run the hot path repeatedly for trace collection
        for (int iter = 0; iter < 20; iter++)
        {
            RunScenario(valuesA, valuesB, "random");
            RunScenario(sortedA, sortedB, "sorted");
        }

        Console.WriteLine("Done.");
    }

    private static void RunScenario(long[] valuesA, long[] valuesB, string label)
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);

        // Build
        var a = new RoaringBitmap(ctx);
        for (int i = 0; i < valuesA.Length; i++) a.Add(valuesA[i]);
        var b = new RoaringBitmap(ctx);
        for (int i = 0; i < valuesB.Length; i++) b.Add(valuesB[i]);

        // PrepareForReading
        a.PrepareForReading();
        b.PrepareForReading();

        // Contains — found count consumed to prevent dead-code elimination
        int found = 0;
        for (int i = 0; i < Math.Min(10000, valuesB.Length); i++)
            if (a.Contains(valuesB[i])) found++;
        GC.KeepAlive(found);

        // AND (in-place)
        a.AndWith(ref b);

        // OR (in-place on a copy)
        var aCopy = new RoaringBitmap(ctx);
        for (int i = 0; i < valuesA.Length; i++) aCopy.Add(valuesA[i]);
        aCopy.PrepareForReading();
        aCopy.LazyOrWith(ref b);
        aCopy.RepairAfterLazy();

        // ANDNOT (in-place)
        var aCopy2 = new RoaringBitmap(ctx);
        for (int i = 0; i < valuesA.Length; i++) aCopy2.Add(valuesA[i]);
        aCopy2.PrepareForReading();
        aCopy2.AndNotWith(ref b);

        // Iterate — total consumed to prevent dead-code elimination
        var iter = a.GetIterator();
        long[] buf = new long[4096];
        int total = 0;
        int read;
        while ((read = a.Fill(buf, ref iter)) > 0)
            total += read;
        GC.KeepAlive(total);

        a.Dispose();
        aCopy.Dispose();
        aCopy2.Dispose();
        b.Dispose();
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
}
