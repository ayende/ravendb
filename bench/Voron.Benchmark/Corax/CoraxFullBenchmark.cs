using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using Corax;
using Corax.Querying;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.SortingMatches.Meta;
using Corax.Mappings;
using Corax.Utils;
using Sparrow;
using Sparrow.Server;
using Sparrow.Threading;
using IndexSearcher = Corax.Querying.IndexSearcher;
using IndexWriter = Corax.Indexing.IndexWriter;

namespace Voron.Benchmark.Corax;

/// <summary>
/// Comprehensive Corax query benchmark covering all query features.
/// Tests at 10K, 100K, 1M, 10M document scales.
/// Measures execution time and memory (native + managed).
///
/// Invoke: dotnet run -c Release -- corax-full-bench [output-file]
/// </summary>
public static class CoraxFullBenchmark
{
    private const string StoragePath = "/tmp/corax-full-bench";
    private const int Warmup = 5;
    private const int Iterations = 100;

    private const int FldId = 0;
    private const int FldCategory = 1;
    private const int FldTag = 2;
    private const int FldStatus = 3;
    private const int FldRegion = 4;
    private const int FldPrice = 5;
    private const int FldRating = 6;
    private const int FldName = 7;

    public static void Run(string outputFile)
    {
        var output = new StringBuilder();
        void Log(string line)
        {
            Console.WriteLine(line);
            output.AppendLine(line);
        }

        Log("=== Corax Full Query Benchmark ===");
        Log($"Runtime: {System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription}");
        Log($"Date: {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss} UTC");
        Log($"Warmup: {Warmup}, Iterations: {Iterations}");
        Log("");

        foreach (int docCount in new[] { 10_000, 100_000, 1_000_000, 10_000_000 })
        {
            Log($"############################################################");
            Log($"# {docCount:N0} DOCUMENTS");
            Log($"############################################################");
            Log("");

            DeleteStorage();
            using var env = new StorageEnvironment(StorageEnvironmentOptions.ForPathForTests(StoragePath));
            using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
            var fields = CreateFieldsMapping(bsc);

            Log($"Indexing {docCount:N0} documents...");
            var swIndex = Stopwatch.StartNew();
            IndexDocuments(env, fields, docCount);
            swIndex.Stop();
            Log($"Index time: {swIndex.Elapsed.TotalSeconds:F1}s");

            var indexSize = new DirectoryInfo(StoragePath)
                .EnumerateFiles("*", SearchOption.AllDirectories).Sum(f => f.Length);
            Log($"Index size on disk: {indexSize / (1024.0 * 1024.0):F1} MB");
            Log("");

            RunAllBenchmarks(env, bsc, fields, docCount, Log);
            Log("");

            fields.Dispose();
            env.Dispose();
            DeleteStorage();
        }

        File.WriteAllText(outputFile, output.ToString());
        Console.WriteLine($"\nResults written to {outputFile}");
    }

    private static void RunAllBenchmarks(StorageEnvironment env, ByteStringContext bsc,
        IndexFieldsMapping fields, int docCount, Action<string> log)
    {
        using var searcher = new IndexSearcher(env, fields);
        long[] ids = new long[8192];

        log($"{"Feature",-55} {"Avg ms",-12} {"P50 ms",-12} {"P99 ms",-12} {"Results",-12} {"Managed KB",-12}");
        log(new string('-', 110));

        // ===== 1. SINGLE TERM =====
        log("--- Single Term ---");
        Bench("Term: Status='active' (50%)", ids, log,
            () => searcher.TermQuery("Status", "active"));
        Bench("Term: Category='category_0' (10%)", ids, log,
            () => searcher.TermQuery("Category", "category_0"));
        Bench("Term: Tag='tag_0' (1%)", ids, log,
            () => searcher.TermQuery("Tag", "tag_0"));
        Bench("Term: Region='region_0' (0.1%)", ids, log,
            () => searcher.TermQuery("Region", "region_0"));

        // ===== 2. AND =====
        log("");
        log("--- AND ---");
        Bench("AND: Status∩Category (50%∩10%)", ids, log,
            () =>
            {
                var q1 = searcher.TermQuery("Status", "active");
                var q2 = searcher.TermQuery("Category", "category_0");
                return searcher.And(q1, q2);
            });
        Bench("AND: Category∩Tag (10%∩1%)", ids, log,
            () =>
            {
                var q1 = searcher.TermQuery("Category", "category_0");
                var q2 = searcher.TermQuery("Tag", "tag_0");
                return searcher.And(q1, q2);
            });
        Bench("AND: Category∩Region (10%∩0.1%)", ids, log,
            () =>
            {
                var q1 = searcher.TermQuery("Category", "category_0");
                var q2 = searcher.TermQuery("Region", "region_0");
                return searcher.And(q1, q2);
            });
        Bench("AND: 3-way Tag∩Category∩Status", ids, log,
            () =>
            {
                var q1 = searcher.TermQuery("Tag", "tag_0");
                var q2 = searcher.TermQuery("Category", "category_0");
                var q12 = searcher.And(q1, q2);
                var q3 = searcher.TermQuery("Status", "active");
                return searcher.And(q12, q3);
            });

        // ===== 3. OR =====
        log("");
        log("--- OR ---");
        Bench("OR: Cat0∪Cat1 (10%∪10%)", ids, log,
            () =>
            {
                var q1 = searcher.TermQuery("Category", "category_0");
                var q2 = searcher.TermQuery("Category", "category_1");
                return searcher.Or(q1, q2);
            });
        Bench("OR: Tag0∪Tag1 (1%∪1%)", ids, log,
            () =>
            {
                var q1 = searcher.TermQuery("Tag", "tag_0");
                var q2 = searcher.TermQuery("Tag", "tag_1");
                return searcher.Or(q1, q2);
            });

        // ===== 4. ANDNOT =====
        log("");
        log("--- ANDNOT ---");
        Bench("ANDNOT: Status-Category (50%-10%)", ids, log,
            () =>
            {
                var q1 = searcher.TermQuery("Status", "active");
                var q2 = searcher.TermQuery("Category", "category_0");
                return searcher.AndNot(q1, q2);
            });
        Bench("ANDNOT: Category-Tag (10%-1%)", ids, log,
            () =>
            {
                var q1 = searcher.TermQuery("Category", "category_0");
                var q2 = searcher.TermQuery("Tag", "tag_0");
                return searcher.AndNot(q1, q2);
            });

        // ===== 5. MIXED =====
        log("");
        log("--- Mixed ---");
        Bench("(Cat0 OR Cat1) AND Status", ids, log,
            () =>
            {
                var q1 = searcher.TermQuery("Category", "category_0");
                var q2 = searcher.TermQuery("Category", "category_1");
                var qOr = searcher.Or(q1, q2);
                var qS = searcher.TermQuery("Status", "active");
                return searcher.And(qOr, qS);
            });
        Bench("(Tag0 OR Tag1) AND Cat AND Status", ids, log,
            () =>
            {
                var q1 = searcher.TermQuery("Tag", "tag_0");
                var q2 = searcher.TermQuery("Tag", "tag_1");
                var qOr = searcher.Or(q1, q2);
                var qCat = searcher.TermQuery("Category", "category_0");
                var qAnd1 = searcher.And(qOr, qCat);
                var qS = searcher.TermQuery("Status", "active");
                return searcher.And(qAnd1, qS);
            });

        // ===== 6. IN (via InQuery) =====
        log("");
        log("--- IN ---");
        Bench("IN: Category in (0,1,2)", ids, log,
            () => searcher.InQuery("Category", new List<string> { "category_0", "category_1", "category_2" }));

        // ===== 7. STARTSWITH =====
        log("");
        log("--- StartsWith ---");
        Bench("StartsWith: Name starts 'name_000001'", ids, log,
            () => searcher.StartWithQuery("Name", "name_000001"));

        // ===== 8. EXISTS =====
        log("");
        log("--- Exists ---");
        var nameMeta = searcher.FieldMetadataBuilder("Name");
        Bench("Exists: Name field", ids, log,
            () => searcher.ExistsQuery(nameMeta));

        // ===== 9. NUMERIC RANGE =====
        log("");
        log("--- Numeric Range ---");
        var priceMeta = searcher.FieldMetadataBuilder("Price");
        Bench("Range: Price > 500 (~50%)", ids, log,
            () => searcher.GreaterThanQuery(priceMeta, 500L));
        Bench("Range: Price BETWEEN 200..400 (~20%)", ids, log,
            () => searcher.BetweenQuery(priceMeta, 200L, 400L));
        Bench("Range: Price BETWEEN 990..999 (~1%)", ids, log,
            () => searcher.BetweenQuery(priceMeta, 990L, 999L));

        // ===== 10. AND + RANGE =====
        log("");
        log("--- AND + Range ---");
        Bench("Tag∩(Price>500): 1%∩50%", ids, log,
            () =>
            {
                var qTag = searcher.TermQuery("Tag", "tag_0");
                var qRange = searcher.GreaterThanQuery(priceMeta, 500L);
                return searcher.And(qTag, qRange);
            });
        Bench("Region∩(Price 200..400): 0.1%∩20%", ids, log,
            () =>
            {
                var qRegion = searcher.TermQuery("Region", "region_0");
                var qRange = searcher.BetweenQuery(priceMeta, 200L, 400L);
                return searcher.And(qRegion, qRange);
            });

        // ===== 11. SORTING =====
        log("");
        log("--- ORDER BY ---");
        var nameFieldMeta = searcher.FieldMetadataBuilder("Name");
        var nameOrder = new OrderMetadata(nameFieldMeta, true, MatchCompareFieldType.Sequence, false);

        Bench("Category='cat_0' ORDER BY Name LIMIT 25", ids, log,
            () =>
            {
                var q = searcher.TermQuery("Category", "category_0");
                return searcher.OrderBy(q, nameOrder, false, take: 25);
            });

        var priceFieldMeta = searcher.FieldMetadataBuilder("Price");
        var priceOrder = new OrderMetadata(priceFieldMeta, true, MatchCompareFieldType.Integer, false);

        Bench("Category='cat_0' ORDER BY Price LIMIT 25", ids, log,
            () =>
            {
                var q = searcher.TermQuery("Category", "category_0");
                return searcher.OrderBy(q, priceOrder, false, take: 25);
            });

        Bench("Tag='tag_0' ORDER BY Name (no limit)", ids, log,
            () =>
            {
                var q = searcher.TermQuery("Tag", "tag_0");
                return searcher.OrderBy(q, nameOrder, false);
            });

        // ===== 12. COMPLEX AND CHAINS (4-9 clauses) =====
        log("");
        log("--- Complex AND chains ---");
        Bench("AND 4-way: Tag∩Cat∩Status∩(Price>500)", ids, log,
            () =>
            {
                var q1 = searcher.TermQuery("Tag", "tag_0");
                var q2 = searcher.TermQuery("Category", "category_0");
                var q12 = searcher.And(q1, q2);
                var q3 = searcher.TermQuery("Status", "active");
                var q123 = searcher.And(q12, q3);
                var q4 = searcher.GreaterThanQuery(priceMeta, 500L);
                return searcher.And(q123, q4);
            });
        Bench("AND 5-way: Region∩Tag∩Cat∩Status∩(Price>500)", ids, log,
            () =>
            {
                var q1 = searcher.TermQuery("Region", "region_0");
                var q2 = searcher.TermQuery("Tag", "tag_0");
                var q12 = searcher.And(q1, q2);
                var q3 = searcher.TermQuery("Category", "category_0");
                var q123 = searcher.And(q12, q3);
                var q4 = searcher.TermQuery("Status", "active");
                var q1234 = searcher.And(q123, q4);
                var q5 = searcher.GreaterThanQuery(priceMeta, 500L);
                return searcher.And(q1234, q5);
            });
        Bench("AND 6-way: Region∩Tag∩Cat∩Status∩(Price200..400)∩(Rating>2)", ids, log,
            () =>
            {
                var ratingMeta = searcher.FieldMetadataBuilder("Rating");
                var q1 = searcher.TermQuery("Region", "region_0");
                var q2 = searcher.TermQuery("Tag", "tag_0");
                var q12 = searcher.And(q1, q2);
                var q3 = searcher.TermQuery("Category", "category_0");
                var q123 = searcher.And(q12, q3);
                var q4 = searcher.TermQuery("Status", "active");
                var q1234 = searcher.And(q123, q4);
                var q5 = searcher.BetweenQuery(priceMeta, 200L, 400L);
                var q12345 = searcher.And(q1234, q5);
                var q6 = searcher.GreaterThanQuery(ratingMeta, 2L);
                return searcher.And(q12345, q6);
            });

        // ===== 13. MULTI-FIELD ORDER BY =====
        log("");
        log("--- Multi-field ORDER BY ---");
        var priceDescOrder = new OrderMetadata(priceFieldMeta, false, MatchCompareFieldType.Integer, false);
        Bench("Cat='cat_0' ORDER BY Price DESC LIMIT 25", ids, log,
            () =>
            {
                var q = searcher.TermQuery("Category", "category_0");
                return searcher.OrderBy(q, priceDescOrder, false, take: 25);
            });
        Bench("Cat='cat_0' ORDER BY Price ASC, Name ASC LIMIT 25", ids, log,
            () =>
            {
                var q = searcher.TermQuery("Category", "category_0");
                return searcher.OrderBy(q,
                    new OrderMetadata[] { priceOrder, nameOrder }, false, take: 25);
            });
        Bench("Tag='tag_0' ORDER BY Price DESC, Name ASC LIMIT 50", ids, log,
            () =>
            {
                var q = searcher.TermQuery("Tag", "tag_0");
                return searcher.OrderBy(q,
                    new OrderMetadata[] { priceDescOrder, nameOrder }, false, take: 50);
            });

        // ===== 14. LIMIT VARIATIONS =====
        log("");
        log("--- LIMIT variations ---");
        Bench("Status='active' LIMIT 10 (50% → 10)", ids, log,
            () =>
            {
                var q = searcher.TermQuery("Status", "active");
                return searcher.OrderBy(q, nameOrder, false, take: 10);
            });
        Bench("Status='active' LIMIT 100 (50% → 100)", ids, log,
            () =>
            {
                var q = searcher.TermQuery("Status", "active");
                return searcher.OrderBy(q, nameOrder, false, take: 100);
            });
        Bench("Status='active' LIMIT 1000 (50% → 1000)", ids, log,
            () =>
            {
                var q = searcher.TermQuery("Status", "active");
                return searcher.OrderBy(q, nameOrder, false, take: 1000);
            });
        // No limit — stream all
        Bench("Category='cat_0' no limit (stream all)", ids, log,
            () => searcher.TermQuery("Category", "category_0"));

        // ===== 15. FULL RESULT ITERATION (stream all for large sets) =====
        log("");
        log("--- Full iteration ---");
        Bench("AND: Status∩Category stream all (no sort)", ids, log,
            () =>
            {
                var q1 = searcher.TermQuery("Status", "active");
                var q2 = searcher.TermQuery("Category", "category_0");
                return searcher.And(q1, q2);
            });
        Bench("OR 5-way: Cat0∪Cat1∪Cat2∪Cat3∪Cat4 (50%)", ids, log,
            () =>
            {
                var q1 = searcher.TermQuery("Category", "category_0");
                var q2 = searcher.TermQuery("Category", "category_1");
                IQueryMatch q = searcher.Or(q1, q2);
                for (int i = 2; i <= 4; i++)
                {
                    var qi = searcher.TermQuery("Category", $"category_{i}");
                    q = searcher.Or(q, qi);
                }
                return q;
            });

        // ===== 16. BITMAP (Corax 2.0 CompiledQueryMatch) =====
        log("");
        log("--- Bitmap (Corax 2.0) ---");
        BenchBitmap("Bitmap2 AND: Status∩Category", ids, log, bsc, searcher,
            new[] { ("Status", "active"), ("Category", "category_0") }, isAnd: true);
        BenchBitmap("Bitmap2 OR: Cat0∪Cat1", ids, log, bsc, searcher,
            new[] { ("Category", "category_0"), ("Category", "category_1") }, isAnd: false);
        BenchBitmap("Bitmap2 (Cat0∪Cat1)∩Status", ids, log, bsc, searcher,
            new[] { ("Category", "category_0"), ("Category", "category_1"), ("Status", "active") },
            isAnd: false, andWithLast: true);

        // ===== 13. ALL ENTRIES =====
        log("");
        log("--- All Entries ---");
        if (docCount <= 1_000_000)
        {
            Bench("AllEntries (full scan)", ids, log,
                () => searcher.AllEntries());
        }
        else
        {
            log($"{"AllEntries (skipped >1M)",-55} {"N/A",-12}");
        }
    }

    private static void Bench(string name, long[] ids, Action<string> log,
        Func<IQueryMatch> queryFactory)
    {
        GC.Collect(2, GCCollectionMode.Forced, true, true);
        GC.WaitForPendingFinalizers();
        GC.Collect(2, GCCollectionMode.Forced, true, true);

        long managedBefore = GC.GetTotalMemory(true);

        for (int w = 0; w < Warmup; w++)
        {
            var q = queryFactory();
            while (q.Fill(ids) > 0) { }
            if (q is IDisposable d) d.Dispose();
        }

        var times = new double[Iterations];
        int totalResults = 0;

        for (int i = 0; i < Iterations; i++)
        {
            var sw = Stopwatch.StartNew();
            var q = queryFactory();
            int read;
            int iterResults = 0;
            while ((read = q.Fill(ids)) > 0)
                iterResults += read;
            sw.Stop();
            times[i] = sw.Elapsed.TotalMilliseconds;
            totalResults += iterResults;
            if (q is IDisposable d) d.Dispose();
        }

        long managedAfter = GC.GetTotalMemory(false);
        long managedDelta = Math.Max(0, managedAfter - managedBefore);

        Array.Sort(times);
        double avg = times.Average();
        double p50 = times[Iterations / 2];
        double p99 = times[(int)(Iterations * 0.99)];
        int avgResults = totalResults / Iterations;

        log($"{name,-55} {avg,8:F3}ms {p50,8:F3}ms {p99,8:F3}ms {avgResults,10:N0} {managedDelta / 1024.0,9:F0}");
    }

    private static void IndexDocuments(StorageEnvironment env, IndexFieldsMapping fields, int docCount)
    {
        const int batchSize = 50_000;
        int indexed = 0;

        while (indexed < docCount)
        {
            using var writer = new IndexWriter(env, fields, SupportedFeatures.All);
            int batchEnd = Math.Min(indexed + batchSize, docCount);

            for (int i = indexed; i < batchEnd; i++)
            {
                using var builder = writer.Index(Encoding.UTF8.GetBytes($"doc/{i}"));

                builder.Write(FldId, null, Encoding.UTF8.GetBytes($"doc/{i}"));
                builder.Write(FldCategory, null, Encoding.UTF8.GetBytes($"category_{i % 10}"));
                builder.Write(FldTag, null, Encoding.UTF8.GetBytes($"tag_{i % 100}"));
                builder.Write(FldStatus, null, Encoding.UTF8.GetBytes((i % 2 == 0) ? "active" : "inactive"));
                builder.Write(FldRegion, null, Encoding.UTF8.GetBytes($"region_{i % 1000}"));

                long price = i % 1000;
                builder.Write(FldPrice, Encoding.UTF8.GetBytes(price.ToString()), price, (double)price);

                long rating = i % 5;
                builder.Write(FldRating, Encoding.UTF8.GetBytes(rating.ToString()), rating, (double)rating);

                builder.Write(FldName, null, Encoding.UTF8.GetBytes($"name_{i:D10}"));

                builder.EndWriting();
            }

            writer.Commit();
            indexed = batchEnd;

            if (indexed % 500_000 == 0 && indexed < docCount)
                Console.Write($"  {indexed:N0}...");
        }

        if (docCount >= 500_000)
            Console.WriteLine(" done.");
    }

    private static IndexFieldsMapping CreateFieldsMapping(ByteStringContext bsc)
    {
        Slice.From(bsc, "Id", ByteStringType.Immutable, out var idSlice);
        Slice.From(bsc, "Category", ByteStringType.Immutable, out var categorySlice);
        Slice.From(bsc, "Tag", ByteStringType.Immutable, out var tagSlice);
        Slice.From(bsc, "Status", ByteStringType.Immutable, out var statusSlice);
        Slice.From(bsc, "Region", ByteStringType.Immutable, out var regionSlice);
        Slice.From(bsc, "Price", ByteStringType.Immutable, out var priceSlice);
        Slice.From(bsc, "Rating", ByteStringType.Immutable, out var ratingSlice);
        Slice.From(bsc, "Name", ByteStringType.Immutable, out var nameSlice);

        using var builder = IndexFieldsMappingBuilder.CreateForWriter(false)
            .AddBinding(FldId, idSlice)
            .AddBinding(FldCategory, categorySlice)
            .AddBinding(FldTag, tagSlice)
            .AddBinding(FldStatus, statusSlice)
            .AddBinding(FldRegion, regionSlice)
            .AddBinding(FldPrice, priceSlice)
            .AddBinding(FldRating, ratingSlice)
            .AddBinding(FldName, nameSlice);
        return builder.Build();
    }

    private static void BenchBitmap(string name, long[] ids, Action<string> log,
        ByteStringContext bsc, IndexSearcher searcher,
        (string field, string term)[] operands, bool isAnd,
        bool andWithLast = false)
    {
        GC.Collect(2, GCCollectionMode.Forced, true, true);
        GC.WaitForPendingFinalizers();
        GC.Collect(2, GCCollectionMode.Forced, true, true);

        long managedBefore = GC.GetTotalMemory(true);

        // Warmup
        for (int w = 0; w < Warmup; w++)
        {
            RunBitmapQuery(bsc, searcher, operands, isAnd, andWithLast, ids);
        }

        var times = new double[Iterations];
        int totalResults = 0;

        for (int i = 0; i < Iterations; i++)
        {
            var sw = Stopwatch.StartNew();
            int read = RunBitmapQuery(bsc, searcher, operands, isAnd, andWithLast, ids);
            sw.Stop();
            times[i] = sw.Elapsed.TotalMilliseconds;
            totalResults += read;
        }

        long managedAfter = GC.GetTotalMemory(false);
        long managedDelta = Math.Max(0, managedAfter - managedBefore);

        Array.Sort(times);
        double avg = times.Average();
        double p50 = times[Iterations / 2];
        double p99 = times[(int)(Iterations * 0.99)];
        int avgResults = totalResults / Iterations;

        log($"{name,-55} {avg,8:F3}ms {p50,8:F3}ms {p99,8:F3}ms {avgResults,10:N0} {managedDelta / 1024.0,9:F0}");
    }

    private static int RunBitmapQuery(ByteStringContext bsc, IndexSearcher searcher,
        (string field, string term)[] operands, bool isAnd, bool andWithLast, long[] ids)
    {
        using var bitmap = new global::Corax.Utils.RoaringBitmaps.RoaringBitmap(bsc);
        Span<long> buffer = stackalloc long[4096];

        if (isAnd)
        {
            // AND chain: fill first, AND rest
            var match0 = searcher.TermQuery(operands[0].field, operands[0].term);
            int read0;
            while ((read0 = match0.Fill(buffer)) > 0)
                bitmap.AddRange(buffer.Slice(0, read0));

            using var tempBitmap = new global::Corax.Utils.RoaringBitmaps.RoaringBitmap(bsc);
            for (int i = 1; i < operands.Length; i++)
            {
                tempBitmap.Clear();
                var matchI = searcher.TermQuery(operands[i].field, operands[i].term);
                int readI;
                while ((readI = matchI.Fill(buffer)) > 0)
                    tempBitmap.AddRange(buffer.Slice(0, readI));
                bitmap.AndWith(ref Unsafe.AsRef(in tempBitmap));
            }
        }
        else if (andWithLast)
        {
            // OR first N-1, AND with last
            for (int i = 0; i < operands.Length - 1; i++)
            {
                var matchI = searcher.TermQuery(operands[i].field, operands[i].term);
                int readI;
                while ((readI = matchI.Fill(buffer)) > 0)
                    bitmap.AddRange(buffer.Slice(0, readI));
            }

            using var tempBitmap = new global::Corax.Utils.RoaringBitmaps.RoaringBitmap(bsc);
            var matchLast = searcher.TermQuery(operands[^1].field, operands[^1].term);
            int readLast;
            while ((readLast = matchLast.Fill(buffer)) > 0)
                tempBitmap.AddRange(buffer.Slice(0, readLast));
            bitmap.AndWith(ref Unsafe.AsRef(in tempBitmap));
        }
        else
        {
            // OR chain: fill all into same bitmap
            for (int i = 0; i < operands.Length; i++)
            {
                var matchI = searcher.TermQuery(operands[i].field, operands[i].term);
                int readI;
                while ((readI = matchI.Fill(buffer)) > 0)
                    bitmap.AddRange(buffer.Slice(0, readI));
            }
        }

        bitmap.PrepareForReading();
        var iterator = bitmap.GetIterator();
        int total = 0;
        int read;
        while ((read = iterator.Fill(ref Unsafe.AsRef(in bitmap), ids)) > 0)
            total += read;
        return total;
    }

    private static void DeleteStorage()
    {
        if (!Directory.Exists(StoragePath))
            return;
        for (int i = 0; i < 10; i++)
        {
            try
            {
                Directory.Delete(StoragePath, recursive: true);
                return;
            }
            catch
            {
                Thread.Sleep(100);
            }
        }
    }
}
