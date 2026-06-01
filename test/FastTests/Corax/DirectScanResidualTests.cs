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
/// Regression guards for the direct-scan residual path (RavenDB-25281 #4872). A direct-scan plan is
/// driven by an ORDER BY field that also carries a range/equals WHERE clause; the remaining WHERE
/// clauses become per-entry residuals evaluated by <c>CompiledEntryPredicate</c>. Two historical bugs:
///   1. residual string-equality terms were not analyzer-encoded, so a mixed-case value (e.g. <c>Name = 'BOB'</c>)
///      never matched the stored, lower-cased term <c>bob</c>;
///   2. an <c>(A or B)</c> group residual was not handled recursively, throwing/returning wrong rows.
/// Both are fixed by the recursive-unification refactor (ScanParamExtractor.ExtractFromPredicate is the
/// single recursive core for entry-scan and direct-scan, always analyzing via GetAnalyzedSlice). These
/// tests pin the behaviour: results are checked against a brute-force expectation and across engines
/// (Corax vs Lucene — Lucene has no direct scan, so a match proves the rewrite is semantics-preserving).
/// </summary>
public class DirectScanResidualTests : RavenTestBase
{
    public DirectScanResidualTests(ITestOutputHelper output) : base(output)
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

    // Deterministic seed sized to make the direct-scan path cost-effective: a selective range on the
    // sort field (Seq) drives the scan while a ~50%-selective residual makes a bitmap plan look
    // expensive. Name is capitalised on purpose so the residual must lower-case it to match.
    private static List<Item> BuildSeed(int count)
    {
        string[] names = { "Bob", "Alice", "Carol", "Dave" };
        string[] cats = { "red", "green", "blue" };
        var items = new List<Item>(count);
        for (int i = 0; i < count; i++)
        {
            items.Add(new Item
            {
                Id = $"items/{i}",
                Name = names[i % names.Length],
                Category = cats[i % cats.Length],
                Seq = i
            });
        }

        return items;
    }

    private static async Task SeedAsync(IDocumentStore store, List<Item> items)
    {
        using var bulk = store.BulkInsert();
        foreach (var it in items)
            await bulk.StoreAsync(it, it.Id);
    }

    private static List<string> Expected(IEnumerable<Item> items, Func<Item, bool> predicate) =>
        items.Where(predicate).Select(i => i.Id).OrderBy(x => x, StringComparer.Ordinal).ToList();

    // Bug 1: a direct-scan residual string-equality with a mixed-case value. Stored term is lower-cased
    // by the default analyzer ("Bob" -> "bob"); the residual must analyzer-encode 'BOB' the same way.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task DirectScanResidual_MixedCaseStringEquality_Matches(Options options)
    {
        using var store = GetDocumentStore(options);
        var index = new Items_Index();
        index.Execute(store);
        var items = BuildSeed(4000);
        await SeedAsync(store, items);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenAsyncSession();
        var results = await session.Advanced
            .AsyncRawQuery<Item>($"from index '{index.IndexName}' where Seq between 0 and 39 and Name = 'BOB' order by Seq as long")
            .ToListAsync();

        var actual = results.Select(r => r.Id).OrderBy(x => x, StringComparer.Ordinal).ToList();
        var expected = Expected(items,
            i => i.Seq >= 0 && i.Seq <= 39 && string.Equals(i.Name, "BOB", StringComparison.OrdinalIgnoreCase));

        Assert.NotEmpty(expected);
        Assert.Equal(expected, actual);
    }

    // Bug 2: a direct-scan plan with an (A or B) group residual must recurse into the group, returning
    // correct rows without an IndexOutOfRangeException.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task DirectScanResidual_OrGroup_Matches(Options options)
    {
        using var store = GetDocumentStore(options);
        var index = new Items_Index();
        index.Execute(store);
        var items = BuildSeed(4000);
        await SeedAsync(store, items);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenAsyncSession();
        var results = await session.Advanced
            .AsyncRawQuery<Item>($"from index '{index.IndexName}' where Seq between 0 and 59 and (Category = 'red' or Category = 'blue') order by Seq as long")
            .ToListAsync();

        var actual = results.Select(r => r.Id).OrderBy(x => x, StringComparer.Ordinal).ToList();
        var expected = Expected(items,
            i => i.Seq >= 0 && i.Seq <= 59 && (i.Category == "red" || i.Category == "blue"));

        Assert.NotEmpty(expected);
        Assert.Equal(expected, actual);
    }

    // Proves the direct-scan path is actually exercised (not silently demoted to a bitmap sort) so the
    // two correctness tests above are guarding the intended code. Corax-only: Lucene has no DirectScan.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task DirectScanResidual_PlanUsesDirectScan(Options options)
    {
        using var store = GetDocumentStore(options);
        var index = new Items_Index();
        index.Execute(store);
        var items = BuildSeed(4000);
        await SeedAsync(store, items);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenAsyncSession();
        var results = await session.Advanced
            .AsyncRawQuery<Item>($"from index '{index.IndexName}' where Seq between 0 and 39 and Name = 'BOB' order by Seq as long include timings()")
            .Timings(out var timings)
            .ToListAsync();

        Assert.NotEmpty(results);
        Assert.NotNull(timings);
        var plan = timings.QueryPlan as QueryInspectionNode;
        Assert.NotNull(plan);
        var compiled = FindOperation(plan, "CompiledQuery");
        Assert.True(compiled != null, "Expected a CompiledQuery node in the plan. Plan: " + Describe(plan));
        Assert.True(compiled.Parameters.TryGetValue("OptimizationHint", out var hint) && hint == "DirectScan",
            "Expected the plan to use the DirectScan strategy, but OptimizationHint was '" + (hint ?? "<missing>") + "'. Plan: " + Describe(plan));
    }

    private static QueryInspectionNode FindOperation(QueryInspectionNode node, string operation)
    {
        if (node == null)
            return null;
        if (node.Operation == operation)
            return node;
        if (node.Children == null)
            return null;
        foreach (var child in node.Children)
        {
            var found = FindOperation(child, operation);
            if (found != null)
                return found;
        }

        return null;
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
}
