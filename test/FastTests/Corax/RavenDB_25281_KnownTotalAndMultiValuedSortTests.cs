using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Timings;
using Raven.Client.Documents.Session;
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

    // #5 (companion fix): a query shaped `where f1 = $x order by f2` over a compound(f1, f2) field has NO
    // WHERE clause on the sort field f2, so historically DirectScanCandidate was never set and the plan fell
    // back to the bitmap pipeline + SortingMatch — even though the compound tree already stores f1's entries
    // in f2 order. The fix sets DirectScanCandidate for this shape; with no residual clauses the per-execution
    // cost gate accepts the scan unconditionally, so the planner walks the compound subtree in f2 order and
    // skips the SortingMatch heap entirely.
    private class Film
    {
        public string Id { get; set; }
        public string Category { get; set; }
        public int Year { get; set; }
    }

    private class Films_ByCategoryAndYear : AbstractIndexCreationTask<Film>
    {
        public Films_ByCategoryAndYear()
        {
            Map = films => from f in films
                select new { f.Category, f.Year };
            CompoundField("Category", "Year");
        }
    }

    // Single-valued Category (equality driver) and Year (sort key), cycled so neither is pre-sorted by
    // insertion order. 1 in 3 films is "Action" -> well above a 25-row page and below the scan cost cap.
    private static List<Film> BuildFilms(int count)
    {
        string[] categories = { "Action", "Comedy", "Drama" };
        var films = new List<Film>(count);
        for (int i = 0; i < count; i++)
            films.Add(new Film { Id = $"films/{i}", Category = categories[i % categories.Length], Year = 1980 + (i * 7) % 45 });

        return films;
    }

    // #5 plan guard: equality on the compound leading key + ORDER BY the compound second key (with no filter
    // on the sort field) must drive the compound tree walk (CompoundSortedScan / DirectScan), NOT the bitmap
    // pipeline + SortingMatch. Also checks the rows are the right ones, in ascending sort order.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public async Task EqualityDrivenCompoundSort_UsesCompoundSortedScan(Options options)
    {
        using var store = GetDocumentStore(options);
        var index = new Films_ByCategoryAndYear();
        index.Execute(store);
        using (var bulk = store.BulkInsert())
        {
            foreach (var f in BuildFilms(300))
                await bulk.StoreAsync(f, f.Id);
        }

        Indexes.WaitForIndexing(store);

        using var session = store.OpenAsyncSession();

        // No filter on the sort field: equality on the compound leading key + ORDER BY the compound second key.
        // Engages DirectScanCandidate (the no-filter optimization) and must walk the compound tree in Year order.
        await AssertCompoundSortedScanInOrder(session, index.IndexName,
            $"from index '{index.IndexName}' where Category = 'Action' order by Year as long limit 25 include timings()");

        // Range on the sort field (the existing composite-range path): must ALSO come back in Year order, not
        // entry-id order. This is the shape that exposed the missing SortedDrivingMatch wrapper.
        await AssertCompoundSortedScanInOrder(session, index.IndexName,
            $"from index '{index.IndexName}' where Category = 'Action' and Year > 1990 order by Year as long limit 25 include timings()");
    }

    private async Task AssertCompoundSortedScanInOrder(IAsyncDocumentSession session, string indexName, string rql)
    {
        var results = await session.Advanced
            .AsyncRawQuery<Film>(rql)
            .Timings(out var timings)
            .ToListAsync();

        Assert.Equal(25, results.Count);
        var plan = timings.QueryPlan as QueryInspectionNode;
        Assert.NotNull(plan);
        var compiled = FindOperation(plan, "CompiledQuery");
        Assert.True(compiled != null, "Expected a CompiledQuery node. Plan: " + Describe(plan));
        compiled.Parameters.TryGetValue("OptimizationHint", out var hint);
        Assert.True(hint == "CompoundSortedScan",
            "Expected the compound tree walk to drive equality+ORDER BY on compound(Category, Year), but " +
            "OptimizationHint was '" + hint + "' for [" + rql + "]. Plan: " + Describe(plan) + " params: " +
            string.Join(", ", compiled.Parameters.Select(kv => kv.Key + "=" + kv.Value)));
        Assert.True(compiled.Children?.FirstOrDefault(c => c.Operation == "DirectScan") != null,
            "Expected a DirectScan node (compound sorted walk) for [" + rql + "]. Plan: " + Describe(plan));

        int prev = int.MinValue;
        foreach (var r in results)
        {
            Assert.Equal("Action", r.Category);
            Assert.True(r.Year >= prev, $"Results are not ascending by Year for [{rql}]: saw {prev} then {r.Year}.");
            prev = r.Year;
        }
    }

    // #5 cross-engine: the same shape must return exactly the matching rows in sort order on BOTH engines.
    // Lucene has no compound/DirectScan, so a match proves the Corax compound-scan path is semantics-preserving.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task EqualityDrivenCompoundSort_ReturnsCorrectlyOrderedMatches(Options options)
    {
        using var store = GetDocumentStore(options);
        var index = new Films_ByCategoryAndYear();
        index.Execute(store);
        var films = BuildFilms(300);
        using (var bulk = store.BulkInsert())
        {
            foreach (var f in films)
                await bulk.StoreAsync(f, f.Id);
        }

        Indexes.WaitForIndexing(store);

        int expectedActionCount = films.Count(f => f.Category == "Action");

        using var session = store.OpenAsyncSession();
        var results = await session.Advanced
            .AsyncRawQuery<Film>($"from index '{index.IndexName}' where Category = 'Action' order by Year as long")
            .ToListAsync();

        Assert.Equal(expectedActionCount, results.Count);
        int prev = int.MinValue;
        foreach (var r in results)
        {
            Assert.Equal("Action", r.Category);
            Assert.True(r.Year >= prev, $"Results are not ascending by Year: saw {prev} then {r.Year}.");
            prev = r.Year;
        }
    }

    private class FilmNullable
    {
        public string Id { get; set; }
        public string Category { get; set; }
        public int? Year { get; set; }
    }

    private class FilmsNullable_ByCategoryAndYear : AbstractIndexCreationTask<FilmNullable>
    {
        public FilmsNullable_ByCategoryAndYear()
        {
            Map = films => from f in films
                select new { f.Category, f.Year };
            CompoundField("Category", "Year");
        }
    }

    // Every null-Year film is an "Action" film (i % 9 == 0 is a subset of i % 3 == 0), so the "Action"
    // result set mixes ~1/3 null years with real years — exactly the case where the compound scan's
    // null handling matters.
    private static List<FilmNullable> BuildFilmsWithNulls(int count)
    {
        string[] categories = { "Action", "Comedy", "Drama" };
        var films = new List<FilmNullable>(count);
        for (int i = 0; i < count; i++)
        {
            int? year = i % 9 == 0 ? null : 1980 + (i * 7) % 45;
            films.Add(new FilmNullable { Id = $"films/{i}", Category = categories[i % categories.Length], Year = year });
        }

        return films;
    }

    // #5 null behavior (option A guard): `where Category = 'Action' order by Year` with some Action docs having a
    // NULL sort value. The compound walk would emit nulls at the wrong end (its null marker sorts after the real
    // values), which contradicts NullsSortMode. So when the bare shape (no field2 filter) has null/missing sort
    // values, the planner must fall back to the bitmap pipeline + SortingMatch, which honors NullsSortMode. This
    // test pins (a) Corax matches Lucene's ordered sort sequence exactly — same count, null placement, value order —
    // and (b) the Corax plan actually fell back (OptimizationHint=BitmapPipeline, not CompoundSortedScan).
    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public async Task EqualityDrivenCompoundSort_WithNullSortValues_FallsBackAndMatchesLucene()
    {
        var films = BuildFilmsWithNulls(300);

        var (luceneOrder, _) = await RunOrdered(RavenSearchEngineMode.Lucene);
        var (coraxOrder, coraxHint) = await RunOrdered(RavenSearchEngineMode.Corax);

        Assert.NotEmpty(coraxOrder);
        Assert.Contains((int?)null, coraxOrder); // the data must actually exercise null sort values

        string Shape(List<int?> o) =>
            $"count={o.Count}, nulls={o.Count(x => x == null)}, " +
            $"firstNullAt={o.FindIndex(x => x == null)}, lastNullAt={o.FindLastIndex(x => x == null)}, " +
            $"nonNullAscending={IsAscending(o.Where(x => x != null).Select(x => x.Value))}";

        Assert.True(luceneOrder.SequenceEqual(coraxOrder),
            $"Corax order diverges from Lucene.\n  Lucene: {Shape(luceneOrder)}\n  Corax:  {Shape(coraxOrder)}");

        // The guard must have demoted the bare compound shape to the bitmap pipeline (the only path that places
        // nulls per NullsSortMode). If this ever reports CompoundSortedScan, the null placement above is luck.
        Assert.Equal("BitmapPipeline", coraxHint);

        static bool IsAscending(IEnumerable<int> xs)
        {
            int prev = int.MinValue;
            foreach (var x in xs) { if (x < prev) return false; prev = x; }
            return true;
        }

        async Task<(List<int?> order, string hint)> RunOrdered(RavenSearchEngineMode mode)
        {
            using var store = GetDocumentStore(Options.ForSearchEngine(mode));
            var index = new FilmsNullable_ByCategoryAndYear();
            await index.ExecuteAsync(store);
            using (var bulk = store.BulkInsert())
            {
                foreach (var f in films)
                    await bulk.StoreAsync(f, f.Id);
            }

            Indexes.WaitForIndexing(store);

            using var session = store.OpenAsyncSession();
            var results = await session.Advanced
                .AsyncRawQuery<FilmNullable>($"from index '{index.IndexName}' where Category = 'Action' order by Year as long include timings()")
                .Timings(out var timings)
                .ToListAsync();

            string hint = null;
            if (timings.QueryPlan is QueryInspectionNode plan && FindOperation(plan, "CompiledQuery") is { } compiled)
                compiled.Parameters.TryGetValue("OptimizationHint", out hint);

            return (results.Select(r => r.Year).ToList(), hint);
        }
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
