using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Timings;
using Sparrow.Json.Parsing;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Corax;

/// <summary>
/// Guards the static exists()/NOT exists() collapse (RavenDB-25281 #4875). When a field has NO missing
/// entries — its per-field NonExisting posting list is empty — exists(F) is match-all and NOT exists(F) is
/// match-nothing, decided at instantiate time against the live transaction and routed through the same
/// drop mechanism as the WHEN collapse (so it never walks the field's distinct terms). The decision is
/// re-made every execution: adding a document missing the field disables the collapse, removing the last
/// such document re-enables it. Dynamic CreateField fields write no NonExisting markers, so the collapse
/// must NOT apply there. The collapse is observable in the query plan: a non-collapsed exists() leaf keeps
/// its term-walk and surfaces as a node carrying <c>ClauseType=Exists</c>, whereas a collapsed one is
/// dropped entirely (no such node — the surviving plan is a match-all fill or the remaining filters only).
/// Corax-only: Lucene has no NonExisting posting list and never collapses.
/// </summary>
public class ExistsCollapseTests : RavenTestBase
{
    public ExistsCollapseTests(ITestOutputHelper output) : base(output)
    {
    }

    private class Item
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string City { get; set; }
    }

    private class Items_Index : AbstractIndexCreationTask<Item>
    {
        public Items_Index()
        {
            Map = items => from i in items
                select new { i.Name, i.City };
        }
    }

    // A static index that produces a DYNAMIC field via CreateField — such fields write no NonExisting markers.
    private class Items_DynamicIndex : AbstractIndexCreationTask<Item>
    {
        public Items_DynamicIndex()
        {
            Map = items => from i in items
                select new { _ = CreateField("Dyn", i.Name) };
        }
    }

    // True iff the query plan still contains a leaf for an exists()/NOT exists() clause — i.e. the term-walk
    // was kept and the collapse did NOT fire. A collapsed leaf is dropped, leaving no node tagged ClauseType=Exists.
    private static bool PlanHasExistsLeaf(QueryInspectionNode node)
    {
        if (node == null)
            return false;
        if (node.Parameters != null
            && node.Parameters.TryGetValue("ClauseType", out var clauseType)
            && clauseType == "Exists")
            return true;
        foreach (var c in node.Children ?? new List<QueryInspectionNode>())
            if (PlanHasExistsLeaf(c))
                return true;
        return false;
    }

    private static async Task<(bool hasExistsLeaf, List<string> ids)> RunAsync(IDocumentStore store, string indexName, string body)
    {
        using var session = store.OpenAsyncSession();
        var results = await session.Advanced
            .AsyncRawQuery<Item>($"from index '{indexName}' {body} include timings()")
            .Timings(out var timings)
            .ToListAsync();

        var plan = timings.QueryPlan as QueryInspectionNode;
        return (PlanHasExistsLeaf(plan), results.Select(r => r.Id).ToList());
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task Exists_Collapses_WhenFieldPresentOnAllDocs(Options options)
    {
        using var store = GetDocumentStore(options);
        var index = new Items_Index();
        index.Execute(store);

        using (var s = store.OpenSession())
        {
            for (int i = 0; i < 50; i++)
                s.Store(new Item { Id = $"items/{i}", Name = $"n{i}", City = i % 2 == 0 ? "red" : "blue" });
            s.SaveChanges();
        }

        Indexes.WaitForIndexing(store);

        // exists(Name): all docs have Name -> collapses to match-all (the exists leaf is dropped, no term-walk).
        var (existsHasLeaf, existsIds) = await RunAsync(store, index.IndexName, "where exists(Name)");
        Assert.False(existsHasLeaf);
        Assert.Equal(50, existsIds.Count);

        // NOT exists(Name): all docs have Name -> collapses to match-nothing.
        var (_, notExistsIds) = await RunAsync(store, index.IndexName, "where true and not exists(Name)");
        Assert.Empty(notExistsIds);

        // exists(Name) AND City = 'red' collapses the exists() leaf, leaving just the City filter.
        var (andHasLeaf, andIds) = await RunAsync(store, index.IndexName, "where exists(Name) and City = 'red'");
        Assert.False(andHasLeaf);
        Assert.Equal(25, andIds.Count);
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task Exists_DoesNotCollapse_WhenSomeMissing_AndReCollapses(Options options)
    {
        using var store = GetDocumentStore(options);
        var index = new Items_Index();
        index.Execute(store);

        using (var s = store.OpenSession())
        {
            for (int i = 0; i < 20; i++)
                s.Store(new Item { Id = $"items/{i}", Name = $"n{i}", City = "red" });
            s.SaveChanges();
        }

        Indexes.WaitForIndexing(store);

        // Baseline: collapses while every doc has Name (no exists leaf in the plan).
        var (baseHasLeaf, _) = await RunAsync(store, index.IndexName, "where exists(Name)");
        Assert.False(baseHasLeaf);

        // Add a document that genuinely LACKS the Name field (an explicit null would be written to the null
        // posting list and still count as present). Putting a raw doc with no Name -> the indexer skips the
        // absent field and writes a NonExisting marker, so the list is now non-empty and the collapse stops.
        var requestExecutor = store.GetRequestExecutor();
        using (requestExecutor.ContextPool.AllocateOperationContext(out var context))
        {
            var reader = context.ReadObject(
                new DynamicJsonValue { ["City"] = "red", ["@metadata"] = new DynamicJsonValue { ["@collection"] = "Items" } },
                "items/missing");
            requestExecutor.Execute(new PutDocumentCommand(store.Conventions, "items/missing", null, reader), context);
        }

        Indexes.WaitForIndexing(store);

        var (missHasLeaf, missIds) = await RunAsync(store, index.IndexName, "where exists(Name)");
        Assert.True(missHasLeaf);
        Assert.DoesNotContain("items/missing", missIds);
        Assert.Equal(20, missIds.Count);

        var (_, notExistsIds) = await RunAsync(store, index.IndexName, "where true and not exists(Name)");
        Assert.Equal(new List<string> { "items/missing" }, notExistsIds);

        // Remove the only missing-field document -> the list empties again and the collapse re-applies.
        using (var s = store.OpenSession())
        {
            s.Delete("items/missing");
            s.SaveChanges();
        }

        Indexes.WaitForIndexing(store);

        var (reHasLeaf, reIds) = await RunAsync(store, index.IndexName, "where exists(Name)");
        Assert.False(reHasLeaf);
        Assert.Equal(20, reIds.Count);
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task Exists_Collapses_UnderOrRoot(Options options)
    {
        using var store = GetDocumentStore(options);
        var index = new Items_Index();
        index.Execute(store);

        using (var s = store.OpenSession())
        {
            for (int i = 0; i < 50; i++)
                s.Store(new Item { Id = $"items/{i}", Name = $"n{i}", City = i % 2 == 0 ? "red" : "blue" });
            s.SaveChanges();
        }

        Indexes.WaitForIndexing(store);

        // exists(Name) under an OR root collapses to match-all (MatchAll sentinel), and x ∨ ALL = ALL,
        // so the whole disjunction matches every doc — no surviving exists() term-walk.
        var (orExistsHasLeaf, orExistsIds) = await RunAsync(store, index.IndexName, "where exists(Name) or City = 'red'");
        Assert.False(orExistsHasLeaf);
        Assert.Equal(50, orExistsIds.Count);

        // NOT exists(Name) under an OR root collapses to match-nothing (MatchNothing sentinel), and x ∨ ∅ = x,
        // so the disjunction reduces to just City = 'red' (the 25 red docs). NOT exists() cannot lead an
        // expression in RQL, so the positive term comes first (OR is commutative — same result).
        var (orNotExistsHasLeaf, orNotExistsIds) = await RunAsync(store, index.IndexName, "where City = 'red' or not exists(Name)");
        Assert.False(orNotExistsHasLeaf);
        Assert.Equal(25, orNotExistsIds.Count);
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task Exists_Collapses_NestedInGroup(Options options)
    {
        using var store = GetDocumentStore(options);
        var index = new Items_Index();
        index.Execute(store);

        using (var s = store.OpenSession())
        {
            for (int i = 0; i < 50; i++)
                s.Store(new Item { Id = $"items/{i}", Name = $"n{i}", City = i % 2 == 0 ? "red" : "blue" });
            s.SaveChanges();
        }

        Indexes.WaitForIndexing(store);

        // The exists(Name) is nested inside an OR group: City = 'red' AND (exists(Name) OR City = 'blue').
        // exists(Name) collapses to MatchAll, the inner group becomes match-all, and the query reduces to
        // City = 'red' (25 docs). The nested exists() leaf must be gone from the plan.
        var (nestedHasLeaf, nestedIds) = await RunAsync(store, index.IndexName, "where City = 'red' and (exists(Name) or City = 'blue')");
        Assert.False(nestedHasLeaf);
        Assert.Equal(25, nestedIds.Count);

        // NOT exists(Name) nested in the OR group collapses to MatchNothing, so the inner group reduces to
        // City = 'blue'; intersecting with the outer City = 'red' yields nothing (a doc has a single City).
        var (nestedNotHasLeaf, nestedNotIds) = await RunAsync(store, index.IndexName, "where City = 'red' and (City = 'blue' or not exists(Name))");
        Assert.False(nestedNotHasLeaf);
        Assert.Empty(nestedNotIds);
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task Exists_DoesNotCollapse_OnDynamicField(Options options)
    {
        using var store = GetDocumentStore(options);
        var index = new Items_DynamicIndex();
        index.Execute(store);

        using (var s = store.OpenSession())
        {
            for (int i = 0; i < 20; i++)
                s.Store(new Item { Id = $"items/{i}", Name = $"n{i}", City = "red" });
            s.SaveChanges();
        }

        Indexes.WaitForIndexing(store);

        // Even though every doc has a 'Dyn' value, a dynamic CreateField field writes no NonExisting markers,
        // so an empty list does NOT imply "nothing missing" — the collapse must not apply (term-walk kept).
        var (hasLeaf, ids) = await RunAsync(store, index.IndexName, "where exists(Dyn)");
        Assert.True(hasLeaf);
        Assert.Equal(20, ids.Count);
    }
}
