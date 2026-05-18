using System;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Corax;

public class QueryPlanTests(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void QueryPlanForMultiUnaryMatch(Options options)
    {
        using var store = GetDocumentStore(options);
        new Index().Execute(store);
        using var session = store.OpenSession();
        session.Store(new Dto("maciej", new DateTime(2024, 8, 22)));
        session.SaveChanges();
        Indexes.WaitForIndexing(store);

        var result = session.Advanced.DocumentQuery<Dto, Index>()
            .WhereEquals(d => d.Name, "maciej")
            .AndAlso()
            .WhereBetween(x => x.Date, new DateTime(2024, 8, 21), new DateTime(2024, 8, 23))
            .Timings(out var timings)
            .ToList();

        Assert.NotNull(result);
        Assert.Equal(1, result.Count);
        Assert.NotNull(timings);
        Assert.NotNull(timings.QueryPlan);
    }

    private class Index : AbstractIndexCreationTask<Dto>
    {
        public Index()
        {
            Map = docs => from d in docs
                select new { d.Name, d.Date };
        }
    }

    
    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void DecisionTrailSurfacedInTimings_NoOrderBy(Options options)
    {
        using var store = GetDocumentStore(options);
        new Index().Execute(store);
        using var session = store.OpenSession();
        session.Store(new Dto("maciej", new DateTime(2024, 8, 22)));
        session.SaveChanges();
        Indexes.WaitForIndexing(store);

        var result = session.Advanced.DocumentQuery<Dto, Index>()
            .WhereEquals(d => d.Name, "maciej")
            .Timings(out var timings)
            .ToList();

        Assert.NotNull(result);
        Assert.NotNull(timings);
        var plan = timings.QueryPlan as Raven.Client.Documents.Queries.Timings.QueryInspectionNode;
        Assert.NotNull(plan);
        Assert.NotNull(plan.Parameters);
        Assert.True(plan.Parameters.ContainsKey("OptimizationHint"));
        var trailNode = plan.Children?.FirstOrDefault(c => c.Operation == "DecisionTrail");
        Assert.NotNull(trailNode);
        Assert.True(trailNode.Children.Count > 0);
        var noOrderBy = trailNode.Children.FirstOrDefault(c => c.Operation == "NoOrderBy");
        Assert.NotNull(noOrderBy);
        Assert.Equal("True", noOrderBy.Parameters["Accepted"]);
    }

    [RavenTheory(RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void DecisionTrailSurfacedInTimings_WithOrderBy(Options options)
    {
        using var store = GetDocumentStore(options);
        new Index().Execute(store);
        using var session = store.OpenSession();
        session.Store(new Dto("maciej", new DateTime(2024, 8, 22)));
        session.SaveChanges();
        Indexes.WaitForIndexing(store);

        var result = session.Advanced.DocumentQuery<Dto, Index>()
            .WhereEquals(d => d.Name, "maciej")
            .AndAlso()
            .WhereBetween(x => x.Date, new DateTime(2024, 8, 21), new DateTime(2024, 8, 23))
            .OrderBy(x => x.Date)
            .Timings(out var timings)
            .ToList();

        Assert.NotNull(result);
        Assert.NotNull(timings);
        var plan = timings.QueryPlan as Raven.Client.Documents.Queries.Timings.QueryInspectionNode;
        Assert.NotNull(plan);
        var trailNode = plan.Children?.FirstOrDefault(c => c.Operation == "DecisionTrail");
        Assert.NotNull(trailNode);
        Assert.True(trailNode.Children.Count >= 2);
        foreach (var child in trailNode.Children)
        {
            Assert.True(child.Parameters.ContainsKey("Accepted"));
            Assert.True(child.Parameters.ContainsKey("Reason"));
        }
        var acceptedEntries = trailNode.Children.Where(c => c.Parameters["Accepted"] == "True").ToList();
        Assert.True(acceptedEntries.Count >= 1);
    }

    private record Dto(string Name, DateTime Date, string Id = null);
}
