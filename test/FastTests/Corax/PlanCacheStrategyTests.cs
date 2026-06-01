using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Timings;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Corax;

/// <summary>
/// Guards the plan-cache cardinality hazard (RavenDB-25281 #4852). The bitmap-vs-direct-scan choice
/// must NOT be frozen into the cached plan: <c>CompiledPlan.Strategy</c> records only the parameter-
/// independent <em>structural</em> candidacy (decided once at cache-miss), while the actual strategy is
/// re-decided on every execution by the per-execution cost gate (<c>DirectScanCostEffective</c> /
/// <c>CompoundFieldCostEffective</c>) against the current bound parameters, falling back to the bitmap
/// pipeline when a scan would not pay off. The instance-time choice is surfaced as <c>OptimizationHint</c>
/// and the cached structural candidacy is always surfaced alongside it as <c>StrategyCandidate</c>, so a
/// cost-gate flip (the two diverging) is observable. Correctness is checked against brute force and across engines (Lucene never direct-scans,
/// so a match proves the rewrite is semantics-preserving).
/// </summary>
public class PlanCacheStrategyTests : RavenTestBase
{
    public PlanCacheStrategyTests(ITestOutputHelper output) : base(output)
    {
    }

    private class Item
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string Category { get; set; }
        public int Seq { get; set; }
    }

    private class Items_Index : AbstractIndexCreationTask<Item>
    {
        public Items_Index()
        {
            Map = items => from i in items
                select new { i.Name, i.Category, i.Seq };
        }
    }

    // Seq == index (so ORDER BY Seq == document order). Name is skewed 3:1 Bob:Alice, so Name='Bob' is
    // NON-selective (~75%, ~3000 of 4000 — direct-scan wins for top-N) while Name='Alice' is selective
    // (~25%, ~1000 — below the cost-gate boundary, so direct-scan loses to a cheap bitmap). Names are
    // capitalised so a residual must analyzer-lowercase to match the stored term.
    private static List<Item> BuildSeed(int count)
    {
        string[] names = { "Bob", "Bob", "Bob", "Alice" };
        string[] cats = { "red", "green", "blue" };
        var items = new List<Item>(count);
        for (int i = 0; i < count; i++)
            items.Add(new Item { Id = $"items/{i}", Name = names[i % names.Length], Category = cats[i % cats.Length], Seq = i });
        return items;
    }

    private static async Task SeedAsync(IDocumentStore store, List<Item> items)
    {
        using var bulk = store.BulkInsert();
        foreach (var it in items)
            await bulk.StoreAsync(it, it.Id);
    }

    // Top-N expectation: the lowest-Seq matches, in Seq order, capped at limit.
    private static List<string> ExpectedTopN(IEnumerable<Item> items, Func<Item, bool> predicate, int limit) =>
        items.Where(predicate).OrderBy(i => i.Seq).Take(limit).Select(i => i.Id).ToList();

    private static QueryInspectionNode Find(QueryInspectionNode node, string op)
    {
        if (node == null)
            return null;
        if (node.Operation == op)
            return node;
        if (node.Children == null)
            return null;
        foreach (var c in node.Children)
        {
            var f = Find(c, op);
            if (f != null)
                return f;
        }

        return null;
    }

    private static (string hint, string candidate) StrategyOf(QueryTimings timings)
    {
        var plan = timings.QueryPlan as QueryInspectionNode;
        Assert.NotNull(plan);
        var compiled = Find(plan, "CompiledQuery");
        Assert.True(compiled != null, "Expected a CompiledQuery node. Plan: " + Describe(plan));
        compiled.Parameters.TryGetValue("OptimizationHint", out var hint);
        compiled.Parameters.TryGetValue("StrategyCandidate", out var candidate);
        return (hint, candidate);
    }

    private static string Describe(QueryInspectionNode node, int depth = 0)
    {
        if (node == null)
            return "<null>";
        var prefix = new string(' ', depth * 2);
        var line = prefix + node.Operation;
        if (node.Children == null || node.Children.Count == 0)
            return line;
        return line + Environment.NewLine + string.Join(Environment.NewLine, node.Children.Select(c => Describe(c, depth + 1)));
    }

    // The cost gate must run PER EXECUTION: two queries with the identical direct-scan-candidate shape
    // (range on the sort field + a residual + ORDER BY sort field + a small page) resolve to different
    // ACTUAL strategies based purely on the residual's selectivity. The non-selective one streams via
    // DirectScan; the selective one is demoted to a (cheaper) bitmap sort. The demotion is observable:
    // the cached structural candidacy (DirectScan) is surfaced as StrategyCandidate while the executed
    // strategy is BitmapSort. Corax-only: Lucene has no DirectScan and never emits these hints.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task PerExecutionCostGate_FlipsStrategy_AndIsObservable(Options options)
    {
        using var store = GetDocumentStore(options);
        var index = new Items_Index();
        index.Execute(store);
        var items = BuildSeed(4000);
        await SeedAsync(store, items);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenAsyncSession();

        // Non-selective residual (Name='BOB' matches ~75%) over a wide range, top-25 -> DirectScan wins.
        await session.Advanced
            .AsyncRawQuery<Item>($"from index '{index.IndexName}' where Seq between 0 and 3999 and Name = 'BOB' order by Seq as long limit 25 include timings()")
            .Timings(out var directScanTimings)
            .ToListAsync();
        var (directHint, directCandidate) = StrategyOf(directScanTimings);
        Assert.Equal("DirectScan", directHint);
        Assert.Equal("DirectScan", directCandidate); // both planned & actual strategies are always surfaced

        // Selective residual (Name='Alice' matches ~25%, ~1000 of 4000 — below the ~1225 cost-gate
        // boundary) over the same wide range and page: a bitmap is cheaper, so the per-execution gate
        // demotes the DirectScan candidate to BitmapSort.
        await session.Advanced
            .AsyncRawQuery<Item>($"from index '{index.IndexName}' where Seq between 0 and 3999 and Name = 'Alice' order by Seq as long limit 25 include timings()")
            .Timings(out var bitmapTimings)
            .ToListAsync();
        var (bitmapHint, bitmapCandidate) = StrategyOf(bitmapTimings);
        Assert.Equal("BitmapSort", bitmapHint);
        Assert.Equal("DirectScan", bitmapCandidate); // candidacy preserved, but not chosen for these params
    }

    // The SAME query, run with no page bound (huge page) instead of a small limit, must also demote: a
    // full materialization can't benefit from streaming top-N, so the gate falls back to bitmap. Proves
    // the decision tracks the live page size / cardinality, not a frozen verdict from a prior execution.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task PageSizeDrivesStrategy_NotFrozen(Options options)
    {
        using var store = GetDocumentStore(options);
        var index = new Items_Index();
        index.Execute(store);
        var items = BuildSeed(4000);
        await SeedAsync(store, items);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenAsyncSession();

        const string body = "where Seq between 0 and 3999 and Name = 'BOB' order by Seq as long";

        // Small page -> DirectScan.
        await session.Advanced
            .AsyncRawQuery<Item>($"from index '{index.IndexName}' {body} limit 25 include timings()")
            .Timings(out var paged)
            .ToListAsync();
        Assert.Equal("DirectScan", StrategyOf(paged).hint);

        // No page bound -> demoted to BitmapSort, candidacy preserved.
        await session.Advanced
            .AsyncRawQuery<Item>($"from index '{index.IndexName}' {body} include timings()")
            .Timings(out var unpaged)
            .ToListAsync();
        var (hint, candidate) = StrategyOf(unpaged);
        Assert.Equal("BitmapSort", hint);
        Assert.Equal("DirectScan", candidate);
    }

    // Correctness across both strategies and both engines. Whatever the per-execution gate picks, the
    // top-N answer must equal the brute-force expectation, and Corax must match Lucene.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task StrategyChoiceIsResultPreserving(Options options)
    {
        using var store = GetDocumentStore(options);
        var index = new Items_Index();
        index.Execute(store);
        var items = BuildSeed(4000);
        await SeedAsync(store, items);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenAsyncSession();

        // Direct-scan-favoured (non-selective residual + small page).
        var directScanResults = await session.Advanced
            .AsyncRawQuery<Item>($"from index '{index.IndexName}' where Seq between 0 and 3999 and Name = 'BOB' order by Seq as long limit 25")
            .ToListAsync();
        Assert.Equal(
            ExpectedTopN(items, i => i.Seq is >= 0 and <= 3999 && string.Equals(i.Name, "BOB", StringComparison.OrdinalIgnoreCase), 25),
            directScanResults.Select(r => r.Id).ToList());

        // Bitmap-favoured (selective residual + small page).
        var bitmapResults = await session.Advanced
            .AsyncRawQuery<Item>($"from index '{index.IndexName}' where Seq between 0 and 3999 and Category = 'red' order by Seq as long limit 25")
            .ToListAsync();
        Assert.Equal(
            ExpectedTopN(items, i => i.Seq is >= 0 and <= 3999 && i.Category == "red", 25),
            bitmapResults.Select(r => r.Id).ToList());

        // Unpaged full result of the direct-scan-candidate query (demotes to bitmap) — must be complete.
        var fullResults = await session.Advanced
            .AsyncRawQuery<Item>($"from index '{index.IndexName}' where Seq between 0 and 3999 and Name = 'BOB' order by Seq as long")
            .ToListAsync();
        Assert.Equal(
            ExpectedTopN(items, i => i.Seq is >= 0 and <= 3999 && string.Equals(i.Name, "BOB", StringComparison.OrdinalIgnoreCase), int.MaxValue),
            fullResults.Select(r => r.Id).ToList());
    }
}
