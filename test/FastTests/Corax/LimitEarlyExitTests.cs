using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Corax;

/// <summary>
/// Tests that LIMIT early-exit in the bitmap pipeline produces correct results.
/// The optimization stops reading posting lists once the bitmap has enough entries
/// for unsorted queries. These tests verify correctness, not performance.
/// </summary>
public class LimitEarlyExitTests(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void SingleClauseWithLimit(Options options)
    {
        using var store = GetDocumentStore(options);
        InsertDocuments(store, 500);

        using var session = store.OpenSession();
        // No ORDER BY → limit-aware bitmap accumulation
        var results = session.Advanced.RawQuery<Doc>(
                "from index 'DocIndex' where Tag = 'even' limit 10")
            .ToList();

        Assert.Equal(10, results.Count);
        foreach (var r in results)
            Assert.Equal("even", r.Tag);
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void SingleClauseWithLimitAndOrderBy(Options options)
    {
        using var store = GetDocumentStore(options);
        InsertDocuments(store, 500);

        using var session = store.OpenSession();
        // ORDER BY → full bitmap needed, limit applies at sort level
        var results = session.Advanced.RawQuery<Doc>(
                "from index 'DocIndex' where Tag = 'even' order by Value limit 10")
            .ToList();

        Assert.Equal(10, results.Count);
        // Should be sorted ascending by Value
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
        // OR chain — limit should stop once enough from first branches
        var results = session.Advanced.RawQuery<Doc>(
                "from index 'DocIndex' where Tag = 'even' or Tag = 'odd' limit 10")
            .ToList();

        Assert.Equal(10, results.Count);
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void AndChainWithLimit(Options options)
    {
        using var store = GetDocumentStore(options);
        InsertDocuments(store, 500);

        using var session = store.OpenSession();
        // AND — both conditions must hold, limit applies after intersection
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
        // Only 10 'even' docs exist, limit is 100
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
