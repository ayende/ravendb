using System;
using System.Diagnostics;
using Corax.Utils.RoaringBitmaps;
using Sparrow.Server;
using Sparrow.Threading;

namespace Voron.Benchmark.Corax;

/// <summary>
/// Focused Contains benchmark on Array containers (sparse data).
/// Tests the SIMD quad search vs binary search path specifically.
/// Invoke via: dotnet run -c Release -- contains-bench
/// </summary>
public static class ContainsArrayBench
{
    public static void Run()
    {
        Console.WriteLine("=== Contains Benchmark: Array Container Focus ===");
        Console.WriteLine($"Runtime: {System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription}");
        Console.WriteLine();
        Console.WriteLine($"{"Values",-10} {"MaxVal",-14} {"Containers",-12} {"~Per Ctr",-10} {"Probes",-12} {"Time",-12} {"Probes/ms",-12}");
        Console.WriteLine(new string('-', 90));

        var scenarios = new (int values, long maxVal, int probes)[]
        {
            // Array containers: < 4096 values per container
            (1000,    10_000_000L, 500_000),    // ~150 containers, ~7 vals each
            (5000,    10_000_000L, 500_000),    // ~150 containers, ~33 vals each
            (10000,   10_000_000L, 500_000),    // ~150 containers, ~67 vals each
            (50000,   10_000_000L, 500_000),    // ~150 containers, ~333 vals each
            (100000,  10_000_000L, 500_000),    // ~150 containers, ~667 vals each
            (200000,  10_000_000L, 500_000),    // ~150 containers, ~1333 vals each
            (500000,  10_000_000L, 500_000),    // ~150 containers, ~3333 vals each (near bitmap threshold)
            (1000,    1_000_000_000L, 500_000), // ~1000 containers, ~1 val each
            (10000,   1_000_000_000L, 500_000), // ~10K containers, ~1 val each
            (100000,  1_000_000_000L, 500_000), // ~100K containers, ~1 val each
        };

        foreach (var (values, maxVal, probes) in scenarios)
        {
            var rng = new Random(42);
            var buildVals = new long[values];
            for (int i = 0; i < values; i++)
                buildVals[i] = rng.NextInt64(0, maxVal);

            var probeVals = new long[probes];
            var rng2 = new Random(99);
            for (int i = 0; i < probes; i++)
                probeVals[i] = rng2.NextInt64(0, maxVal);

            using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
            var bmp = new RoaringBitmap(ctx);
            for (int i = 0; i < buildVals.Length; i++)
                bmp.Add(buildVals[i]);
            bmp.PrepareForReading();

            int containers = bmp.ContainerCount;
            int perContainer = containers > 0 ? values / containers : values;

            // Warmup
            int found = 0;
            for (int i = 0; i < Math.Min(10000, probes); i++)
                if (bmp.Contains(probeVals[i])) found++;

            // Bench - 3 iterations for stable timing
            found = 0;
            int totalProbes = probes * 3;
            var sw = Stopwatch.StartNew();
            for (int iter = 0; iter < 3; iter++)
            {
                for (int i = 0; i < probes; i++)
                    if (bmp.Contains(probeVals[i])) found++;
            }
            sw.Stop();
            GC.KeepAlive(found);

            double totalMs = sw.Elapsed.TotalMilliseconds;
            double probesPerMs = totalProbes / totalMs;

            Console.WriteLine($"{values,-10} {maxVal,-14} {containers,-12} {perContainer,-10} {totalProbes,-12} {totalMs,8:F2}ms {probesPerMs,10:F0}/ms");
            bmp.Dispose();
        }
    }
}
