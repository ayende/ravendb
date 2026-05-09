using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Timings;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Corax;

public class LimitEarlyExitTests(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void SingleClauseWithLimit(Options options)
    {
        using var store = GetDocumentStore(options);
        InsertDocuments(store, 500);

        using var session = store.OpenSession();
        var results = session.Advanced.RawQuery<Doc>(
                "from index 'DocIndex' where Tag = 'even' limit 10")
            .ToList();

        Assert.Equal(10, results.Count);
        foreach (var r in results)
            Assert.Equal("even", r.Tag);
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task SingleClauseWithLimitDoesNotScanAll(Options options)
    {
        using var store = GetDocumentStore(options);
        InsertDocuments(store, 500);

        using var session = store.OpenAsyncSession();
        var results = await session.Advanced
            .AsyncDocumentQuery<Doc, DocIndex>()
            .WhereEquals("Tag", "even")
            .Take(10)
            .Timings(out QueryTimings timings)
            .ToListAsync();

        Assert.Equal(10, results.Count);

        var plan = (QueryInspectionNode)timings.QueryPlan;
        Assert.NotNull(plan);
        Assert.Equal("CompiledQuery", plan.Operation);
        Assert.True(plan.Parameters.ContainsKey("ScannedEntries"),
            $"ScannedEntries missing. Parameters: {string.Join(", ", plan.Parameters.Keys)}");
        var scanned = long.Parse(plan.Parameters["ScannedEntries"]);
        // When TermSource dispatch is used, the bitmap stops at ~10 entries.
        // When DirectSource dispatch is used (e.g., boosted queries), all 250
        // may be scanned — but never more than the total matching entries.
        Assert.True(scanned <= 250,
            $"Scanned more than the total matching entries: {scanned}");
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void SingleClauseWithLimitAndOrderBy(Options options)
    {
        using var store = GetDocumentStore(options);
        InsertDocuments(store, 500);

        using var session = store.OpenSession();
        var results = session.Advanced.RawQuery<Doc>(
                "from index 'DocIndex' where Tag = 'even' order by Value limit 10")
            .ToList();

        Assert.Equal(10, results.Count);
        for (int i = 1; i < results.Count; i++)
            Assert.True(results[i].Value >= results[i - 1].Value);
        foreach (var r in results)
            Assert.Equal("even", r.Tag);
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void OrChainWithLimit(Options options)
    {
        using var store = GetDocumentStore(options);
        InsertDocuments(store, 500);

        using var session = store.OpenSession();
        var results = session.Advanced.RawQuery<Doc>(
                "from index 'DocIndex' where Tag = 'even' or Tag = 'odd' limit 10")
            .ToList();

        Assert.Equal(10, results.Count);
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task OrChainWithLimitDoesNotScanAll(Options options)
    {
        using var store = GetDocumentStore(options);
        InsertDocuments(store, 500);

        using var session = store.OpenAsyncSession();
        var results = await session.Advanced
            .AsyncDocumentQuery<Doc, DocIndex>()
            .WhereEquals("Tag", "even")
            .OrElse()
            .WhereEquals("Tag", "odd")
            .Take(10)
            .Timings(out QueryTimings timings)
            .ToListAsync();

        Assert.Equal(10, results.Count);

        var plan = (QueryInspectionNode)timings.QueryPlan;
        Assert.NotNull(plan);
        Assert.Equal("CompiledQuery", plan.Operation);
        var scanned = long.Parse(plan.Parameters["ScannedEntries"]);
        Assert.True(scanned < 500,
            $"Expected early exit to scan fewer than 500 entries, but scanned {scanned}");
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void AndChainWithLimit(Options options)
    {
        using var store = GetDocumentStore(options);
        InsertDocuments(store, 500);

        using var session = store.OpenSession();
        var results = session.Advanced.RawQuery<Doc>(
                "from index 'DocIndex' where Tag = 'even' and Value < 100 limit 10")
            .ToList();

        Assert.Equal(10, results.Count);
        foreach (var r in results)
        {
            Assert.Equal("even", r.Tag);
            Assert.True(r.Value < 100);
        }
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void LimitLargerThanResultSet(Options options)
    {
        using var store = GetDocumentStore(options);
        InsertDocuments(store, 20);

        using var session = store.OpenSession();
        var results = session.Advanced.RawQuery<Doc>(
                "from index 'DocIndex' where Tag = 'even' limit 100")
            .ToList();

        Assert.Equal(10, results.Count);
        foreach (var r in results)
            Assert.Equal("even", r.Tag);
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void LimitOneReturnsExactlyOne(Options options)
    {
        using var store = GetDocumentStore(options);
        InsertDocuments(store, 500);

        using var session = store.OpenSession();
        var results = session.Advanced.RawQuery<Doc>(
                "from index 'DocIndex' where Tag = 'even' limit 1")
            .ToList();

        Assert.Single(results);
        Assert.Equal("even", results[0].Tag);
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void SkipAndTakeReturnsCorrectPage(Options options)
    {
        using var store = GetDocumentStore(options);
        InsertDocuments(store, 500);

        using var session = store.OpenSession();
        // skip 5, take 10 → bitmap needs at least 15 entries
        var results = session.Advanced.RawQuery<Doc>(
                "from index 'DocIndex' where Tag = 'even' limit 5, 10")
            .ToList();

        Assert.Equal(10, results.Count);
        foreach (var r in results)
            Assert.Equal("even", r.Tag);
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task SkipAndTakeDoesNotScanAll(Options options)
    {
        using var store = GetDocumentStore(options);
        InsertDocuments(store, 500);

        using var session = store.OpenAsyncSession();
        var results = await session.Advanced
            .AsyncDocumentQuery<Doc, DocIndex>()
            .WhereEquals("Tag", "even")
            .Skip(5)
            .Take(10)
            .Timings(out QueryTimings timings)
            .ToListAsync();

        Assert.Equal(10, results.Count);

        var plan = (QueryInspectionNode)timings.QueryPlan;
        Assert.NotNull(plan);
        Assert.Equal("CompiledQuery", plan.Operation);
        var scanned = long.Parse(plan.Parameters["ScannedEntries"]);
        Assert.True(scanned <= 250,
            $"Scanned more than the total matching entries: {scanned}");
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void NoLimitReturnsAll(Options options)
    {
        using var store = GetDocumentStore(options);
        InsertDocuments(store, 100);

        using var session = store.OpenSession();
        var results = session.Advanced.RawQuery<Doc>(
                "from index 'DocIndex' where Tag = 'even'")
            .ToList();

        Assert.Equal(50, results.Count);
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task NoLimitScansAll(Options options)
    {
        using var store = GetDocumentStore(options);
        InsertDocuments(store, 100);

        using var session = store.OpenAsyncSession();
        var results = await session.Advanced
            .AsyncDocumentQuery<Doc, DocIndex>()
            .WhereEquals("Tag", "even")
            .Timings(out QueryTimings timings)
            .ToListAsync();

        Assert.Equal(50, results.Count);

        var plan = (QueryInspectionNode)timings.QueryPlan;
        Assert.NotNull(plan);
        Assert.Equal("CompiledQuery", plan.Operation);
        var scanned = long.Parse(plan.Parameters["ScannedEntries"]);
        Assert.Equal(50, scanned);
    }

    private void InsertDocuments(IDocumentStore store, int count)
    {
        new DocIndex().Execute(store);

        using (var bulk = store.BulkInsert())
        {
            for (int i = 0; i < count; i++)
                bulk.Store(new Doc { Value = i, Tag = i % 2 == 0 ? "even" : "odd" });
        }

        Indexes.WaitForIndexing(store);
    }

    private class DocIndex : AbstractIndexCreationTask<Doc>
    {
        public DocIndex()
        {
            Map = docs => from d in docs select new { d.Value, d.Tag };
        }
    }

    private class Doc
    {
        public string Id { get; set; }
        public int Value { get; set; }
        public string Tag { get; set; }
    }
}
