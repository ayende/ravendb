using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;
using Corax;
using Corax.Querying;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Mappings;
using Sparrow.Server;
using Sparrow.Threading;
using IndexSearcher = Corax.Querying.IndexSearcher;
using IndexWriter = Corax.Indexing.IndexWriter;

namespace Voron.Benchmark.Corax;

/// <summary>
/// End-to-end Corax query benchmark comparing streaming merge vs bitmap-based set ops.
/// Tests AND/OR/ANDNOT at 1M and 10M document scales.
/// Results written to /tmp/corax-query-baseline.txt
///
/// Invoke via: dotnet run -c Release -- corax-query
/// </summary>
public static class CoraxQueryBitmapBench
{
    private const string StoragePath = "/tmp/corax-bitmap-bench";
    private const string ResultsFile = "/tmp/corax-query-results.txt";

    public static void Run()
    {
        var output = new StringBuilder();
        void Log(string line)
        {
            Console.WriteLine(line);
            output.AppendLine(line);
        }

        Log("=== Corax Query Benchmark: Streaming vs RoaringBitmap ===");
        Log($"Runtime: {System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription}");
        Log($"Date: {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC");
        Log("");

        foreach (int docCount in new[] { 1_000_000, 10_000_000 })
        {
            Log($"========== {docCount:N0} DOCUMENTS ==========");
            Log("");

            DeleteStorage();
            using var env = new StorageEnvironment(StorageEnvironmentOptions.ForPathForTests(StoragePath));
            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            var fields = CreateFieldsMapping(bsc);

            Log($"Indexing {docCount:N0} documents...");
            var swIndex = Stopwatch.StartNew();
            IndexDocuments(env, fields, docCount);
            swIndex.Stop();
            Log($"Index time: {swIndex.Elapsed.TotalMilliseconds:F0}ms");
            Log("");

            var results = RunQueryBenchmarks(env, bsc, fields, docCount);
            foreach (string line in results)
                Log(line);

            Log("");
            env.Dispose();
            DeleteStorage();
        }

        File.WriteAllText(ResultsFile, output.ToString());
        Console.WriteLine($"\nResults written to {ResultsFile}");
    }

    private static void IndexDocuments(StorageEnvironment env, IndexFieldsMapping fields, int docCount)
    {
        using (var writer = new IndexWriter(env, fields, SupportedFeatures.All))
        {
            for (int i = 0; i < docCount; i++)
            {
                using var builder = writer.Index(Encoding.UTF8.GetBytes($"doc/{i}"));
                builder.Write(0, null, Encoding.UTF8.GetBytes($"doc/{i}"));
                builder.Write(1, null, Encoding.UTF8.GetBytes($"category_{i % 10}"));
                builder.Write(2, null, Encoding.UTF8.GetBytes($"tag_{i % 100}"));
                builder.Write(3, null, Encoding.UTF8.GetBytes((i % 2 == 0) ? "active" : "inactive"));
                builder.Write(4, null, Encoding.UTF8.GetBytes($"region_{i % 1000}"));
                builder.EndWriting();
            }
            writer.Commit();
        }
    }

    private static List<string> RunQueryBenchmarks(StorageEnvironment env, ByteStringContext bsc,
        IndexFieldsMapping fields, int docCount)
    {
        var lines = new List<string>();
        using var searcher = new IndexSearcher(env, fields);
        long[] ids = new long[8192];

        Slice.From(bsc, "Category", ByteStringType.Immutable, out var categorySlice);
        Slice.From(bsc, "Tag", ByteStringType.Immutable, out var tagSlice);
        Slice.From(bsc, "Status", ByteStringType.Immutable, out var statusSlice);
        Slice.From(bsc, "Region", ByteStringType.Immutable, out var regionSlice);
        Slice.From(bsc, "category_0", ByteStringType.Immutable, out var cat0Slice);
        Slice.From(bsc, "category_1", ByteStringType.Immutable, out var cat1Slice);
        Slice.From(bsc, "tag_0", ByteStringType.Immutable, out var tag0Slice);
        Slice.From(bsc, "tag_1", ByteStringType.Immutable, out var tag1Slice);
        Slice.From(bsc, "active", ByteStringType.Immutable, out var activeSlice);
        Slice.From(bsc, "region_0", ByteStringType.Immutable, out var region0Slice);
        Slice.From(bsc, "region_1", ByteStringType.Immutable, out var region1Slice);

        int warmup = 3;
        int iterations = 10;

        lines.Add($"{"Query",-50} {"Stream ms",-12} {"Bitmap ms",-12} {"Speedup",-10} {"Results",-10}");
        lines.Add(new string('-', 100));

        void Bench(string name, Func<IQueryMatch> streamFactory, Func<IQueryMatch> bitmapFactory)
        {
            var (streamMs, streamResults) = BenchQuery(warmup, iterations, ids, streamFactory);
            var (bitmapMs, bitmapResults) = BenchQuery(warmup, iterations, ids, bitmapFactory);
            double speedup = streamMs / bitmapMs;
            lines.Add($"{name,-50} {streamMs,9:F3}ms {bitmapMs,9:F3}ms {speedup,8:F2}x {streamResults,8:N0}");
        }

        // --- AND ---
        lines.Add("--- AND ---");
        Bench("Status AND Category (50%∩10%)",
            () => {
                var q1 = searcher.TermQuery(statusSlice, activeSlice);
                var q2 = searcher.TermQuery(categorySlice, cat0Slice);
                return searcher.And(q1, q2);
            },
            () => {
                var q1 = searcher.TermQuery(statusSlice, activeSlice);
                var q2 = searcher.TermQuery(categorySlice, cat0Slice);
                return searcher.BitmapAnd(ref q1, ref q2);
            });

        Bench("Category AND Tag (10%∩1%)",
            () => {
                var q1 = searcher.TermQuery(categorySlice, cat0Slice);
                var q2 = searcher.TermQuery(tagSlice, tag0Slice);
                return searcher.And(q1, q2);
            },
            () => {
                var q1 = searcher.TermQuery(categorySlice, cat0Slice);
                var q2 = searcher.TermQuery(tagSlice, tag0Slice);
                return searcher.BitmapAnd(ref q1, ref q2);
            });

        Bench("Category AND Region (10%∩0.1%)",
            () => {
                var q1 = searcher.TermQuery(categorySlice, cat0Slice);
                var q2 = searcher.TermQuery(regionSlice, region0Slice);
                return searcher.And(q1, q2);
            },
            () => {
                var q1 = searcher.TermQuery(categorySlice, cat0Slice);
                var q2 = searcher.TermQuery(regionSlice, region0Slice);
                return searcher.BitmapAnd(ref q1, ref q2);
            });

        // --- OR ---
        lines.Add("");
        lines.Add("--- OR ---");
        Bench("Category0 OR Category1 (10%∪10%)",
            () => {
                var q1 = searcher.TermQuery(categorySlice, cat0Slice);
                var q2 = searcher.TermQuery(categorySlice, cat1Slice);
                return searcher.Or(q1, q2);
            },
            () => {
                var q1 = searcher.TermQuery(categorySlice, cat0Slice);
                var q2 = searcher.TermQuery(categorySlice, cat1Slice);
                return searcher.BitmapOr(ref q1, ref q2);
            });

        Bench("Tag0 OR Tag1 (1%∪1%)",
            () => {
                var q1 = searcher.TermQuery(tagSlice, tag0Slice);
                var q2 = searcher.TermQuery(tagSlice, tag1Slice);
                return searcher.Or(q1, q2);
            },
            () => {
                var q1 = searcher.TermQuery(tagSlice, tag0Slice);
                var q2 = searcher.TermQuery(tagSlice, tag1Slice);
                return searcher.BitmapOr(ref q1, ref q2);
            });

        Bench("Region0 OR Region1 (0.1%∪0.1%)",
            () => {
                var q1 = searcher.TermQuery(regionSlice, region0Slice);
                var q2 = searcher.TermQuery(regionSlice, region1Slice);
                return searcher.Or(q1, q2);
            },
            () => {
                var q1 = searcher.TermQuery(regionSlice, region0Slice);
                var q2 = searcher.TermQuery(regionSlice, region1Slice);
                return searcher.BitmapOr(ref q1, ref q2);
            });

        // --- ANDNOT ---
        lines.Add("");
        lines.Add("--- ANDNOT ---");
        Bench("Status ANDNOT Category (50%-10%)",
            () => {
                var q1 = searcher.TermQuery(statusSlice, activeSlice);
                var q2 = searcher.TermQuery(categorySlice, cat0Slice);
                return searcher.AndNot(q1, q2);
            },
            () => {
                var q1 = searcher.TermQuery(statusSlice, activeSlice);
                var q2 = searcher.TermQuery(categorySlice, cat0Slice);
                return searcher.BitmapAndNot(ref q1, ref q2);
            });

        Bench("Category ANDNOT Tag (10%-1%)",
            () => {
                var q1 = searcher.TermQuery(categorySlice, cat0Slice);
                var q2 = searcher.TermQuery(tagSlice, tag0Slice);
                return searcher.AndNot(q1, q2);
            },
            () => {
                var q1 = searcher.TermQuery(categorySlice, cat0Slice);
                var q2 = searcher.TermQuery(tagSlice, tag0Slice);
                return searcher.BitmapAndNot(ref q1, ref q2);
            });

        // --- Mixed ---
        lines.Add("");
        lines.Add("--- Mixed ---");
        Bench("(Cat0 OR Cat1) AND Status",
            () => {
                var q1 = searcher.TermQuery(categorySlice, cat0Slice);
                var q2 = searcher.TermQuery(categorySlice, cat1Slice);
                var qOr = searcher.Or(q1, q2);
                var qStatus = searcher.TermQuery(statusSlice, activeSlice);
                return searcher.And(qOr, qStatus);
            },
            () => {
                var q1 = searcher.TermQuery(categorySlice, cat0Slice);
                var q2 = searcher.TermQuery(categorySlice, cat1Slice);
                var qOr = searcher.BitmapOr(ref q1, ref q2);
                var qStatus = searcher.TermQuery(statusSlice, activeSlice);
                return searcher.BitmapAnd(ref qOr, ref qStatus);
            });

        return lines;
    }

    private static (double avgMs, int avgResults) BenchQuery(int warmup, int iterations,
        long[] ids, Func<IQueryMatch> queryFactory)
    {
        for (int w = 0; w < warmup; w++)
        {
            var q = queryFactory();
            while (q.Fill(ids) > 0) { }
            if (q is IDisposable d) d.Dispose();
        }

        int totalResults = 0;
        var sw = Stopwatch.StartNew();
        for (int i = 0; i < iterations; i++)
        {
            var q = queryFactory();
            int read;
            while ((read = q.Fill(ids)) > 0)
                totalResults += read;
            if (q is IDisposable d) d.Dispose();
        }
        sw.Stop();

        return (sw.Elapsed.TotalMilliseconds / iterations, totalResults / iterations);
    }

    private static IndexFieldsMapping CreateFieldsMapping(ByteStringContext bsc)
    {
        Slice.From(bsc, "Id", ByteStringType.Immutable, out var idSlice);
        Slice.From(bsc, "Category", ByteStringType.Immutable, out var categorySlice);
        Slice.From(bsc, "Tag", ByteStringType.Immutable, out var tagSlice);
        Slice.From(bsc, "Status", ByteStringType.Immutable, out var statusSlice);
        Slice.From(bsc, "Region", ByteStringType.Immutable, out var regionSlice);

        using var builder = IndexFieldsMappingBuilder.CreateForWriter(false)
            .AddBinding(0, idSlice)
            .AddBinding(1, categorySlice)
            .AddBinding(2, tagSlice)
            .AddBinding(3, statusSlice)
            .AddBinding(4, regionSlice);
        return builder.Build();
    }

    private static void DeleteStorage()
    {
        if (!Directory.Exists(StoragePath))
            return;
        for (int i = 0; i < 10; i++)
        {
            try { Directory.Delete(StoragePath, true); break; }
            catch (DirectoryNotFoundException) { break; }
            catch { Thread.Sleep(20); }
        }
    }
}
