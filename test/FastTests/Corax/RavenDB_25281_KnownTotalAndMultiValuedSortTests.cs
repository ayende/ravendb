using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Timings;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Corax;

/// <summary>
/// RavenDB-25281 regression guards for two query-planner fixes:
///   #3 exists() known-total: a single non-boosted exists() reports its exact TotalResults from O(1)
///      metadata (index entry count minus the field's non-existing posting list) instead of draining
///      the whole posting set to count it — so the read stays page-bounded (EarlyExit) even when the
///      caller asks for statistics.
///   #4 multi-valued sort guard: an equals/range clause on a MULTI-VALUED sort field must not drive a
///      DirectScan. The direct-scan residual excludes the driving clause assuming the in-order tree walk
///      enforces it, but SortedDrivingMatch walks every posting of a multi-valued field, so documents
///      matching the term under one value AND a different value elsewhere leaked through unfiltered. The
///      guard falls the plan back to the bitmap pipeline + SortingMatch, which applies the clause as a
///      real filter.
/// </summary>
public class RavenDB_25281_KnownTotalAndMultiValuedSortTests : RavenTestBase
{
    public RavenDB_25281_KnownTotalAndMultiValuedSortTests(ITestOutputHelper output) : base(output)
    {
    }

    // Base doc has no Tagline property at all -> the index records it under the field's NON-EXISTING
    // posting list, so exists(Tagline) must exclude it.
    private class Doc
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }

    private class TaggedDoc : Doc
    {
        public string Tagline { get; set; }
    }

    private class Docs_ByTagline : AbstractIndexCreationTask<TaggedDoc>
    {
        public Docs_ByTagline()
        {
            // Map Name (present on every doc) AND Tagline: the base Doc entries are indexed via Name, but
            // lack Tagline entirely -> they land in Tagline's NON-EXISTING posting list, so exists(Tagline)
            // must exclude them. (Mapping Tagline alone would index the absent value as a null term = exists.)
            Map = docs => from d in docs
                select new { d.Name, d.Tagline };
        }
    }

    // #3: exists(Tagline) reports the exact total from metadata and the read early-exits at the page even
    // though statistics are requested (which would otherwise force a full count-draining scan).
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task Exists_KnownTotal_ReportsExactTotalAndEarlyExitsWithStatistics(Options options)
    {
        const int withTag = 800;
        const int withoutTag = 400;

        options.ModifyDocumentStore = s => s.Conventions = new DocumentConventions { FindCollectionName = _ => "Docs" };
        using var store = GetDocumentStore(options);
        var index = new Docs_ByTagline();
        index.Execute(store);

        using (var bulk = store.BulkInsert())
        {
            for (int i = 0; i < withTag; i++)
                await bulk.StoreAsync(new TaggedDoc { Name = $"name/{i}", Tagline = $"line {i}" }, $"docs/tag/{i}");
            for (int i = 0; i < withoutTag; i++)
                await bulk.StoreAsync(new Doc { Name = $"name/{i}" }, $"docs/plain/{i}");
        }

        Indexes.WaitForIndexing(store);

        using var session = store.OpenAsyncSession();

        // Ground truth: the actual matching rows come from the bitmap, independent of the known-total that
        // only sources TotalResults. If the metadata count over/under-reports, it will diverge from this.
        var allMatches = await session.Advanced
            .AsyncRawQuery<TaggedDoc>($"from index '{index.IndexName}' where exists(Tagline)")
            .ToListAsync();
        int actualExists = allMatches.Count;
        Assert.True(actualExists > 25, $"Test needs more than a page of matches, but only {actualExists} exist.");

        var results = await session.Advanced
            .AsyncRawQuery<TaggedDoc>($"from index '{index.IndexName}' where exists(Tagline) limit 25 include timings()")
            .Statistics(out var stats)
            .Timings(out var timings)
            .ToListAsync();

        Assert.Equal(25, results.Count);
        // The metadata-resolved total must match the real matching-document count exactly.
        Assert.Equal(actualExists, (int)stats.TotalResults);
        // And the data setup must actually exercise the "minus non-existing" arithmetic (some docs lack Tagline).
        Assert.Equal(withTag, actualExists);

        var plan = timings.QueryPlan as QueryInspectionNode;
        Assert.NotNull(plan);
        var compiled = FindOperation(plan, "CompiledQuery");
        Assert.True(compiled != null, "Expected a CompiledQuery node. Plan: " + Describe(plan));
        // The read must NOT have drained the full posting set: the known total let the bitmap pipeline keep
        // its page limit even under statistics, so it stopped at the page (EarlyExit).
        Assert.True(compiled.Parameters.TryGetValue("EarlyExit", out var earlyExit) && earlyExit == "true",
            "Expected EarlyExit=true (known total skips the count drain), but plan was: " + Describe(plan) +
            " params: " + string.Join(", ", compiled.Parameters.Select(kv => kv.Key + "=" + kv.Value)));
    }

    private class Movie
    {
        public string Id { get; set; }
        public string[] Genres { get; set; }
        public int Seq { get; set; }
    }

    private class Movies_ByGenres : AbstractIndexCreationTask<Movie>
    {
        public Movies_ByGenres()
        {
            Map = movies => from m in movies
                select new { m.Genres, m.Seq };
        }
    }

    // Deterministic seed: every movie has Drama plus one rotating extra genre, so Genres is multi-valued
    // (some docs hold 2 terms) and "Drama" is non-selective. This is exactly the shape that historically
    // drove an (incorrect) DirectScan on the multi-valued sort field.
    private static List<Movie> BuildMovies(int count)
    {
        string[] extra = { "Action", "Comedy", "Horror", "SciFi" };
        var movies = new List<Movie>(count);
        for (int i = 0; i < count; i++)
        {
            // 1 in 4 movies is Drama-only; the rest are Drama + one extra (multi-valued).
            var genres = i % 4 == 0
                ? new[] { "Drama" }
                : new[] { "Drama", extra[i % extra.Length] };
            movies.Add(new Movie { Id = $"movies/{i}", Genres = genres, Seq = i });
        }

        return movies;
    }

    // #4: a Genres='Drama' filter that also drives ORDER BY Genres must return ONLY documents that actually
    // contain "Drama". Cross-engine: Lucene has no DirectScan, so a match proves the Corax fallback is
    // semantics-preserving. Before the guard, Corax leaked non-matching documents (inflated, wrong rows).
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task MultiValuedSortField_EqualsDriven_ReturnsOnlyMatchingDocs(Options options)
    {
        using var store = GetDocumentStore(options);
        var index = new Movies_ByGenres();
        index.Execute(store);
        var movies = BuildMovies(800);
        using (var bulk = store.BulkInsert())
        {
            foreach (var m in movies)
                await bulk.StoreAsync(m, m.Id);
        }

        Indexes.WaitForIndexing(store);

        using var session = store.OpenAsyncSession();
        var results = await session.Advanced
            .AsyncRawQuery<Movie>($"from index '{index.IndexName}' where Genres = 'Drama' order by Genres limit 25")
            .ToListAsync();

        Assert.Equal(25, results.Count);
        foreach (var r in results)
        {
            Assert.True(r.Genres != null && r.Genres.Any(g => string.Equals(g, "Drama", StringComparison.OrdinalIgnoreCase)),
                $"Document {r.Id} was returned but its Genres [{string.Join(", ", r.Genres ?? Array.Empty<string>())}] do not contain 'Drama'.");
        }
    }

    // #4 plan guard: the same query must NOT pick the FieldSortedScan (DirectScan) strategy on Corax — the
    // multi-valued sort field forces the bitmap pipeline + SortingMatch fallback.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task MultiValuedSortField_EqualsDriven_DoesNotUseDirectScan(Options options)
    {
        using var store = GetDocumentStore(options);
        var index = new Movies_ByGenres();
        index.Execute(store);
        var movies = BuildMovies(800);
        using (var bulk = store.BulkInsert())
        {
            foreach (var m in movies)
                await bulk.StoreAsync(m, m.Id);
        }

        Indexes.WaitForIndexing(store);

        using var session = store.OpenAsyncSession();
        var results = await session.Advanced
            .AsyncRawQuery<Movie>($"from index '{index.IndexName}' where Genres = 'Drama' order by Genres limit 25 include timings()")
            .Timings(out var timings)
            .ToListAsync();

        Assert.NotEmpty(results);
        var plan = timings.QueryPlan as QueryInspectionNode;
        Assert.NotNull(plan);
        plan.Parameters.TryGetValue("OptimizationHint", out var hint);
        Assert.True(hint != "FieldSortedScan",
            "A multi-valued sort field must not drive a DirectScan, but OptimizationHint was 'FieldSortedScan'. Plan: " + Describe(plan));
        var compiled = FindOperation(plan, "CompiledQuery");
        Assert.True(compiled?.Children?.FirstOrDefault(c => c.Operation == "DirectScan") == null,
            "Expected NO DirectScan node for a multi-valued sort field. Plan: " + Describe(plan));
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
