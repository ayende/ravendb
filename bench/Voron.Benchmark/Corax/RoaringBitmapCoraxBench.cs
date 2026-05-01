using System;
using System.Collections.Generic;
using System.Diagnostics;
using Corax.Utils.RoaringBitmaps;
using Sparrow.Server;
using Sparrow.Server.Collections;
using Sparrow.Threading;

namespace Voron.Benchmark.Corax;

/// <summary>
/// Benchmark simulating Corax query patterns with roaring bitmaps vs GrowableBitArray.
/// Posting lists produce sorted, clustered document IDs. Queries combine terms via AND/OR/ANDNOT.
/// Invoke via: dotnet run -c Release -- corax-roaring
/// </summary>
public static class RoaringBitmapCoraxBench
{
    public static void Run()
    {
        Console.WriteLine("=== Corax-Realistic RoaringBitmap Benchmark ===");
        Console.WriteLine($"Runtime: {System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription}");
        Console.WriteLine();

        // Simulate a database with N documents, where posting lists are sorted document IDs.
        // Terms have varying selectivity (how many documents contain the term).
        int totalDocs = 10_000_000; // 10M documents

        // Posting lists: sorted arrays of document IDs for each "term"
        // Selectivity varies from very common (50%) to very rare (0.01%)
        var scenarios = new (string name, double selectivityA, double selectivityB, int termCount)[]
        {
            // Two-term queries (AND/OR/ANDNOT between two posting lists)
            ("Common×Common (50%×50%)",      0.50, 0.50, 2),
            ("Common×Medium (50%×10%)",      0.50, 0.10, 2),
            ("Medium×Medium (10%×10%)",      0.10, 0.10, 2),
            ("Medium×Rare (10%×1%)",         0.10, 0.01, 2),
            ("Rare×Rare (1%×1%)",            0.01, 0.01, 2),
            ("Rare×VeryRare (1%×0.1%)",      0.01, 0.001, 2),
            ("VeryRare×VeryRare (0.1%×0.1%)", 0.001, 0.001, 2),
        };

        Console.WriteLine("--- TWO-TERM QUERIES (sorted posting list input) ---");
        Console.WriteLine($"Total documents: {totalDocs:N0}");
        Console.WriteLine();
        Console.WriteLine($"{"Scenario",-35} {"Op",-8} {"Roaring",-12} {"GrowBitArr",-12} {"Speedup",-10} {"Roaring Build",-14}");
        Console.WriteLine(new string('-', 95));

        foreach (var (name, selA, selB, _) in scenarios)
        {
            long[] postingA = GeneratePostingList(totalDocs, selA, seed: 42);
            long[] postingB = GeneratePostingList(totalDocs, selB, seed: 99);

            foreach (string op in new[] { "AND", "OR", "ANDNOT" })
            {
                var (roaringBuildMs, roaringOpMs) = BenchRoaringSetOp(postingA, postingB, op);
                double gbaMs = BenchGbaSetOp(postingA, postingB, totalDocs, op);
                double speedup = gbaMs / roaringOpMs;

                Console.WriteLine($"{name,-35} {op,-8} {roaringOpMs,9:F2}ms {gbaMs,9:F2}ms {speedup,8:F1}x {roaringBuildMs,11:F2}ms");
            }
            Console.WriteLine();
        }

        // Multi-way OR: simulate "field IN [term1, term2, ..., termN]"
        Console.WriteLine("--- MULTI-WAY OR (field IN [term1..termN]) ---");
        Console.WriteLine($"{"Terms",-10} {"Selectivity",-15} {"Roaring",-12} {"GrowBitArr",-12} {"Speedup",-10}");
        Console.WriteLine(new string('-', 65));

        foreach (int termCount in new[] { 5, 10, 25, 50 })
        {
            foreach (double sel in new[] { 0.01, 0.001 })
            {
                var postingLists = new long[termCount][];
                for (int t = 0; t < termCount; t++)
                    postingLists[t] = GeneratePostingList(totalDocs, sel, seed: 42 + t);

                double roaringMs = BenchRoaringMultiOr(postingLists);
                double gbaMs = BenchGbaMultiOr(postingLists, totalDocs);
                double speedup = gbaMs / roaringMs;

                Console.WriteLine($"{termCount,-10} {sel * 100:F1}%{"",-10} {roaringMs,9:F2}ms {gbaMs,9:F2}ms {speedup,8:F1}x");
            }
        }

        Console.WriteLine();

        // Iteration benchmark: how fast can we read results after set ops?
        Console.WriteLine("--- ITERATION (Fill after OR) ---");
        Console.WriteLine($"{"Scenario",-35} {"Roaring Fill",-14} {"Values",-12}");
        Console.WriteLine(new string('-', 65));

        foreach (var (name, selA, selB, _) in scenarios)
        {
            long[] postingA = GeneratePostingList(totalDocs, selA, seed: 42);
            long[] postingB = GeneratePostingList(totalDocs, selB, seed: 99);

            var (iterMs, count) = BenchRoaringIterate(postingA, postingB);
            Console.WriteLine($"{name,-35} {iterMs,11:F2}ms {count,10:N0}");
        }
    }

    /// <summary>
    /// Generate a sorted posting list simulating Corax's posting list output.
    /// Documents are clustered: consecutive IDs from indexing batches.
    /// </summary>
    private static long[] GeneratePostingList(int totalDocs, double selectivity, int seed)
    {
        int count = (int)(totalDocs * selectivity);
        if (count == 0) count = 1;

        var rng = new Random(seed);
        var result = new HashSet<long>();

        // Mix of clustered runs (simulating batch indexing) and scattered values
        int clustered = (int)(count * 0.7); // 70% in clusters
        int scattered = count - clustered;

        // Generate clusters: 10-500 consecutive IDs starting at random positions
        while (result.Count < clustered)
        {
            long start = rng.NextInt64(0, totalDocs);
            int clusterSize = rng.Next(10, Math.Min(500, clustered - result.Count + 10));
            for (int j = 0; j < clusterSize && result.Count < clustered; j++)
            {
                long id = start + j;
                if (id < totalDocs)
                    result.Add(id);
            }
        }

        // Generate scattered values
        while (result.Count < count)
            result.Add(rng.NextInt64(0, totalDocs));

        var arr = new long[result.Count];
        result.CopyTo(arr);
        Array.Sort(arr); // Posting lists are always sorted
        return arr;
    }

    private static (double buildMs, double opMs) BenchRoaringSetOp(long[] postingA, long[] postingB, string op)
    {
        // Warmup
        using (var ctx = new ByteStringContext(SharedMultipleUseFlag.None))
        {
            var wa = new RoaringBitmap(ctx);
            for (int i = 0; i < postingA.Length; i++) wa.Add(postingA[i]);
            var wb = new RoaringBitmap(ctx);
            for (int i = 0; i < postingB.Length; i++) wb.Add(postingB[i]);
            wa.PrepareForReading();
            wb.PrepareForReading();
            wa.AndWith(ref wb);
            wa.Dispose(); wb.Dispose();
        }

        using var ctx2 = new ByteStringContext(SharedMultipleUseFlag.None);

        // Build phase (timed separately)
        var swBuild = Stopwatch.StartNew();
        var a = new RoaringBitmap(ctx2);
        for (int i = 0; i < postingA.Length; i++) a.Add(postingA[i]);
        var b = new RoaringBitmap(ctx2);
        for (int i = 0; i < postingB.Length; i++) b.Add(postingB[i]);
        a.PrepareForReading();
        b.PrepareForReading();
        swBuild.Stop();

        // Op phase
        var swOp = Stopwatch.StartNew();
        switch (op)
        {
            case "AND": a.AndWith(ref b); break;
            case "OR": a.LazyOrWith(ref b); a.RepairAfterLazy(); break;
            case "ANDNOT": a.AndNotWith(ref b); break;
        }
        GC.KeepAlive(a.Count);
        swOp.Stop();

        a.Dispose(); b.Dispose();
        return (swBuild.Elapsed.TotalMilliseconds, swOp.Elapsed.TotalMilliseconds);
    }

    private static double BenchGbaSetOp(long[] postingA, long[] postingB, int maxVal, string op)
    {
        // GrowableBitArray doesn't have set operations, so we simulate with BCL BitArray
        // which is the comparable flat bitmap approach
        var ba = new System.Collections.BitArray(maxVal);
        var bb = new System.Collections.BitArray(maxVal);
        for (int i = 0; i < postingA.Length; i++) ba.Set((int)postingA[i], true);
        for (int i = 0; i < postingB.Length; i++) bb.Set((int)postingB[i], true);

        var sw = Stopwatch.StartNew();
        switch (op)
        {
            case "AND": ba.And(bb); break;
            case "OR": ba.Or(bb); break;
            case "ANDNOT":
                var notB = (System.Collections.BitArray)bb.Clone();
                notB.Not();
                ba.And(notB);
                break;
        }
        sw.Stop();
        return sw.Elapsed.TotalMilliseconds;
    }

    private static double BenchRoaringMultiOr(long[][] postingLists)
    {
        // Warmup
        using (var ctx = new ByteStringContext(SharedMultipleUseFlag.None))
        {
            var acc = new RoaringBitmap(ctx);
            for (int i = 0; i < postingLists[0].Length; i++) acc.Add(postingLists[0][i]);
            acc.PrepareForReading();
            acc.Dispose();
        }

        using var ctx2 = new ByteStringContext(SharedMultipleUseFlag.None);

        // Build all bitmaps
        var bitmaps = new RoaringBitmap[postingLists.Length];
        for (int t = 0; t < postingLists.Length; t++)
        {
            bitmaps[t] = new RoaringBitmap(ctx2);
            for (int i = 0; i < postingLists[t].Length; i++)
                bitmaps[t].Add(postingLists[t][i]);
            bitmaps[t].PrepareForReading();
        }

        // Time the multi-way OR
        var sw = Stopwatch.StartNew();
        var result = bitmaps[0]; // accumulate into first
        for (int t = 1; t < bitmaps.Length; t++)
            result.LazyOrWith(ref bitmaps[t]);
        result.RepairAfterLazy();
        GC.KeepAlive(result.Count);
        sw.Stop();

        // Dispose (result owns all stolen containers, others are empty shells)
        result.Dispose();
        for (int t = 1; t < bitmaps.Length; t++)
            bitmaps[t].Dispose();

        return sw.Elapsed.TotalMilliseconds;
    }

    private static double BenchGbaMultiOr(long[][] postingLists, int maxVal)
    {
        var arrays = new System.Collections.BitArray[postingLists.Length];
        for (int t = 0; t < postingLists.Length; t++)
        {
            arrays[t] = new System.Collections.BitArray(maxVal);
            for (int i = 0; i < postingLists[t].Length; i++)
                arrays[t].Set((int)postingLists[t][i], true);
        }

        var sw = Stopwatch.StartNew();
        var result = arrays[0];
        for (int t = 1; t < arrays.Length; t++)
            result.Or(arrays[t]);
        sw.Stop();
        return sw.Elapsed.TotalMilliseconds;
    }

    private static (double ms, long count) BenchRoaringIterate(long[] postingA, long[] postingB)
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        var a = new RoaringBitmap(ctx);
        for (int i = 0; i < postingA.Length; i++) a.Add(postingA[i]);
        var b = new RoaringBitmap(ctx);
        for (int i = 0; i < postingB.Length; i++) b.Add(postingB[i]);
        a.PrepareForReading();
        b.PrepareForReading();
        a.LazyOrWith(ref b);
        a.RepairAfterLazy();

        var sw = Stopwatch.StartNew();
        var iter = a.GetIterator();
        long[] buf = new long[4096];
        long count = 0;
        int read;
        while ((read = a.Fill(buf, ref iter)) > 0)
            count += read;
        sw.Stop();

        iter.Dispose();
        a.Dispose();
        b.Dispose();
        return (sw.Elapsed.TotalMilliseconds, count);
    }
}
