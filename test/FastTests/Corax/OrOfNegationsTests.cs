using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Corax;

/// <summary>
/// Validates the De Morgan fold for OR chains whose members are ALL negations
/// (<c>¬A ∨ ¬B = ¬(A ∧ B)</c>): the planner intersects the positive forms once and takes a
/// single complement instead of one <c>FillAllEntries + AndNot</c> per member. The fold must be
/// result-identical to the un-folded plan, so every test compares the engine's answer against a
/// brute-force expectation computed over the seeded set. Cross-engine (Corax vs Lucene) parity is
/// also asserted via <see cref="RavenSearchEngineMode.All"/> — Lucene never folds, so a match proves
/// the rewrite is semantics-preserving, including null / missing-field handling.
/// </summary>
public class OrOfNegationsTests : RavenTestBase
{
    public OrOfNegationsTests(Xunit.ITestOutputHelper output) : base(output)
    {
    }

    private class Item
    {
        public string Id { get; set; }
        public string Color { get; set; }
        public double Score { get; set; }
        public long Code { get; set; }
    }

    // Deterministic seed: Color cycles green/blue/red/(missing), Score == Code == index.
    private static List<Item> BuildSeed(int count)
    {
        var items = new List<Item>(count);
        for (int i = 0; i < count; i++)
        {
            string color = (i % 4) switch { 0 => "green", 1 => "blue", 2 => "red", _ => null };
            items.Add(new Item { Id = $"items/{i}", Color = color, Score = i, Code = i });
        }

        return items;
    }

    private static async Task SeedAsync(IDocumentStore store, List<Item> items)
    {
        using var session = store.OpenAsyncSession();
        foreach (var it in items)
            await session.StoreAsync(it, it.Id);
        await session.SaveChangesAsync();
    }

    private static async Task<List<string>> RunIds(IDocumentStore store, string query)
    {
        using var session = store.OpenAsyncSession();
        var results = await session.Advanced.AsyncRawQuery<Item>(query).ToListAsync();
        return results.Select(r => r.Id).OrderBy(x => x, StringComparer.Ordinal).ToList();
    }

    private static List<string> Expected(IEnumerable<Item> items, Func<Item, bool> predicate) =>
        items.Where(predicate).Select(i => i.Id).OrderBy(x => x, StringComparer.Ordinal).ToList();

    // ¬(Color ∈ {green,blue}) ∨ ¬(Score == 5)  ==  ¬(Color ∈ {green,blue} ∧ Score == 5).
    // Two negated members -> folds. Docs with missing Color satisfy the complement.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task NotInOrNotEquals_Folds_MatchesComplement(Options options)
    {
        using var store = GetDocumentStore(options);
        var items = BuildSeed(40);
        await SeedAsync(store, items);
        Indexes.WaitForIndexing(store);

        var actual = await RunIds(store,
            "from Items where Score != 5 or not (Color in ('green', 'blue'))");

        var expected = Expected(items,
            i => !((i.Color is "green" or "blue") && i.Score == 5));

        Assert.Equal(expected, actual);
        // Only items/5 is excluded: i=5 -> Color "blue" (5%4==1) and Score 5, the lone member of the intersection.
        Assert.DoesNotContain("items/5", actual);
        Assert.Equal(items.Count - 1, actual.Count);
        // Missing-Color docs (i%4==3) are present in the complement.
        Assert.Contains("items/3", actual);
    }

    // Three all-negated members: ¬A ∨ ¬B ∨ ¬C == ¬(A ∧ B ∧ C). Exercises the N-way intersect-once path.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task ThreeNegations_Folds_MatchesComplement(Options options)
    {
        using var store = GetDocumentStore(options);
        var items = BuildSeed(40);
        await SeedAsync(store, items);
        Indexes.WaitForIndexing(store);

        var actual = await RunIds(store,
            "from Items where Color != 'red' or Score != 6 or Code != 7");

        var expected = Expected(items,
            i => !((i.Color == "red") && i.Score == 6 && i.Code == 7));

        // No single doc has Color=red AND Score=6 AND Code=7 (Score==Code==index, so Score=6 implies Code=6),
        // so the intersection is empty and the complement is the whole set.
        Assert.Equal(expected, actual);
        Assert.Equal(items.Count, actual.Count);
    }

    // Mixed chain (one negated, one positive) must NOT fold and must still be correct:
    // ¬(Color ∈ {green}) ∨ (Score == 6). Positive member's matches still OR in.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task MixedNegatedAndPositive_DoesNotFold_StillCorrect(Options options)
    {
        using var store = GetDocumentStore(options);
        var items = BuildSeed(40);
        await SeedAsync(store, items);
        Indexes.WaitForIndexing(store);

        var actual = await RunIds(store,
            "from Items where Score = 6 or not (Color in ('green'))");

        var expected = Expected(items,
            i => i.Color != "green" || i.Score == 6);

        Assert.Equal(expected, actual);
        // items/4 has Color=green (4%4==0) and Score 4 != 6 -> excluded.
        Assert.DoesNotContain("items/4", actual);
        // items/0 has Color=green but is rescued by nothing (Score 0 != 6) -> excluded too.
        Assert.DoesNotContain("items/0", actual);
        // items/8 Color=green Score 8 -> excluded; items/24 Color=green Score 24 -> excluded.
        // The green doc whose Score==6? none (green => i%4==0 => Score multiple of 4) so all green excluded.
        Assert.DoesNotContain("items/8", actual);
    }
}
