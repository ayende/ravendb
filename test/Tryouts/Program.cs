#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using Corax.Querying.Planning;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Timings;
using Raven.Client.Documents.Session;
using Raven.Server.Documents.Indexes;
using FastTests;
using Tests.Infrastructure;

namespace Tryouts;

public static class Program
{
    public static async Task Main(string[] args)
    {
        using var helper = new ConsoleTestOutputHelper();
        await using var gen = new CoraxCatalogGenerator(helper);
        string outPath = args.Length > 0 ? args[0] : "corax-query-catalog.md";
        await gen.Generate(outPath);
        Console.WriteLine($"Wrote {outPath}");
    }
}

public sealed class Item
{
    public string Id { get; set; }
    public string Name { get; set; }
    public string City { get; set; }
    public int Age { get; set; }
    public double Score { get; set; }
    public DateTime Created { get; set; }
    public string[] Tags { get; set; }
}

public sealed class Items_Index : AbstractIndexCreationTask<Item>
{
    public Items_Index()
    {
        Map = items => from i in items
                       select new { i.Name, i.City, i.Age, i.Score, i.Created, i.Tags };
    }
}

/// <summary>
/// Regenerates <c>corax-query-catalog.md</c> directly against the live engine on this branch.
/// For each catalog query it runs the RQL with <c>include timings()</c> over several parameter
/// sets, captures <see cref="QueryTimings.QueryPlan"/> for the structural plan, and dumps every
/// compiled plan variant the plan cache produced (strategy, decision trail, generated C#).
///
/// The compiled variants are read from the index's internal <c>SharedPlanCache</c> via reflection
/// (the field is internal to Raven.Server), then enumerated through the public
/// <see cref="PlanCache.Snapshot"/> API.
/// </summary>
public sealed class CoraxCatalogGenerator : RavenTestBase
{
    public CoraxCatalogGenerator(Xunit.ITestOutputHelper output) : base(output)
    {
    }

    private const int DocCount = 50_000;
    private const string IndexName = "Items/Index";

    private sealed record ParamSet(string Label, string Description, Action<IRawDocumentQuery<Item>> Apply);

    private sealed record CatalogQuery(string Label, string Title, string Rql, string Narrative, ParamSet[] Params);

    public async Task Generate(string outPath)
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        Seed(store);
        new Items_Index().Execute(store);
        Indexes.WaitForIndexing(store);

        var planCache = await GetPlanCache(store);

        var queries = BuildCatalog();

        var sb = new StringBuilder();
        WriteHeader(sb);

        foreach (CatalogQuery q in queries)
        {
            sb.Append("## ").Append(q.Label).Append(" — ").AppendLine(q.Title);
            sb.AppendLine();
            sb.AppendLine("```rql");
            sb.AppendLine(q.Rql);
            sb.AppendLine("```");
            sb.AppendLine();
            sb.AppendLine(q.Narrative);
            sb.AppendLine();

            // The text we actually execute carries `include timings()` so the server computes
            // the plan tree. That same text is the plan-cache key, so we match variants on it.
            string timedRql = q.Rql + " include timings()";

            foreach (ParamSet p in q.Params)
            {
                QueryInspectionNode plan = RunForPlan(store, timedRql, p);
                sb.Append("### Plan — params: ").Append(p.Label).Append(" — ").AppendLine(p.Description);
                sb.AppendLine();
                sb.AppendLine("```");
                RenderPlan(plan, 0, sb);
                sb.AppendLine("```");
                sb.AppendLine();
            }

            sb.AppendLine("### Compiled variants");
            sb.AppendLine();
            WriteVariants(sb, planCache, timedRql);
            sb.AppendLine("---");
            sb.AppendLine();
        }

        File.WriteAllText(outPath, sb.ToString());
    }

    private static void WriteHeader(StringBuilder sb)
    {
        sb.AppendLine("# Corax query catalog — RQL, query plan, and generated code");
        sb.AppendLine();
        sb.AppendLine($"Generated against `{IndexName}` over **{DocCount:N0}** documents (seed 12345).");
        sb.AppendLine();
        sb.AppendLine("For each query we show:");
        sb.AppendLine();
        sb.AppendLine("- the **RQL** (one query text, run with several parameter sets),");
        sb.AppendLine("- the **query plan** per parameter set — the structural shape surfaced by `include timings()` (`QueryTimings.QueryPlan`),");
        sb.AppendLine("- the **compiled plan variants** for that query text — strategy, decision trail, and the generated C# (`CompiledPlan.FormattedSource`) for each distinct parameter shape the plan cache produced.");
        sb.AppendLine();
        sb.AppendLine("Varying parameters for the same text is what produces multiple compiled variants: the plan cache keys on a digest of parameter *types*, operand cardinality ordering, sentinel state, and the WHEN-survival mask — not on the raw values.");
        sb.AppendLine();
        sb.AppendLine("The `key` shown next to each variant is the low 64 bits of that shape digest. It captures the *plan shape*, not the query text or field names, so structurally identical queries (e.g. a single string-param leaf with no ORDER BY) share the same low-64 value across different query texts — they do not collide in the cache because lookup is keyed by query text first, then by the full 256-bit digest.");
        sb.AppendLine();
        sb.AppendLine("> Note on selectivity: a single compiled plan often adapts to selectivity **at runtime** rather than producing distinct cached variants. The `ShouldSwitchToEntryScan` gate in the generated code chooses between a tree-scan intersection and a per-entry residual scan based on the live accumulator cardinality, so the *same* plan serves both a selective and a non-selective parameter set.");
        sb.AppendLine();
        sb.AppendLine("---");
        sb.AppendLine();
    }

    private static void RenderPlan(QueryInspectionNode node, int depth, StringBuilder sb)
    {
        sb.Append(' ', depth * 2);
        sb.Append(node.Operation);
        if (node.Parameters is { Count: > 0 })
        {
            sb.Append(" {");
            bool first = true;
            foreach (KeyValuePair<string, string> kv in node.Parameters)
            {
                if (first == false)
                    sb.Append(", ");
                first = false;
                sb.Append(kv.Key).Append('=').Append(kv.Value);
            }

            sb.Append('}');
        }

        sb.AppendLine();

        if (node.Children != null)
        {
            foreach (QueryInspectionNode child in node.Children)
                RenderPlan(child, depth + 1, sb);
        }
    }

    private static void WriteVariants(StringBuilder sb, PlanCache planCache, string queryText)
    {
        PlanCache.PlanCacheEntry entry = planCache.Snapshot().FirstOrDefault(e => e.QueryText == queryText);
        if (entry.Plans == null || entry.Plans.Length == 0)
        {
            sb.AppendLine("_(no compiled plan captured)_");
            sb.AppendLine();
            return;
        }

        for (int i = 0; i < entry.Plans.Length; i++)
        {
            CompiledPlan plan = entry.Plans[i];
            string key = plan.CacheKeyHash[0].ToString("x16");
            sb.Append("#### variant ").Append(i + 1).Append(" — strategy `").Append(plan.Strategy).Append("` (key `").Append(key).AppendLine("`)");
            sb.AppendLine();
            sb.AppendLine("Decision trail:");
            sb.AppendLine();
            if (plan.DecisionTrail is { Entries.Count: > 0 })
            {
                foreach (PlanDecisionEntry d in plan.DecisionTrail.Entries)
                {
                    string verdict = d.Accepted ? "**accepted**" : "**rejected**";
                    sb.Append("- `").Append(d.Optimization).Append("` → ").Append(verdict).Append(": ").AppendLine(d.Reason);
                }
            }
            else
            {
                sb.AppendLine("- _(none recorded)_");
            }

            sb.AppendLine();
            sb.AppendLine("Generated C#:");
            sb.AppendLine();
            sb.AppendLine("```csharp");
            // FormattedSource already ends with the residual section — either the emitted
            // ResidualScan method or the "// No residual predicates." marker (see ResidualScanIlEmitter).
            sb.Append(plan.FormattedSource.TrimEnd('\n'));
            sb.AppendLine();
            sb.AppendLine("```");
            sb.AppendLine();
        }
    }

    private static QueryInspectionNode RunForPlan(IDocumentStore store, string timedRql, ParamSet p)
    {
        using IDocumentSession session = store.OpenSession();
        IRawDocumentQuery<Item> q = session.Advanced.RawQuery<Item>(timedRql).Timings(out QueryTimings timings);
        p.Apply(q);
        _ = q.ToList();
        return (QueryInspectionNode)timings.QueryPlan;
    }

    private async Task<PlanCache> GetPlanCache(IDocumentStore store)
    {
        Raven.Server.Documents.DocumentDatabase db = await GetDocumentDatabaseInstanceFor(store);
        Raven.Server.Documents.Indexes.Index index = db.IndexStore.GetIndex(IndexName);

        FieldInfo persistenceField = typeof(Raven.Server.Documents.Indexes.Index).GetField("IndexPersistence",
            BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public);
        object persistence = persistenceField.GetValue(index);

        FieldInfo cacheField = persistence.GetType().GetField("SharedPlanCache",
            BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public);
        return (PlanCache)cacheField.GetValue(persistence);
    }

    private static CatalogQuery[] BuildCatalog()
    {
        return
        [
            new CatalogQuery("eq", "single term equality",
                "from index 'Items/Index' where Name = $p",
                "The simplest leaf. A selective term and a non-matching term still share one compiled plan: the cache keys on the parameter *type*, not the value.",
                [
                    new ParamSet("hit", "$p=\"alice\"", q => q.AddParameter("p", "alice")),
                    new ParamSet("no-match", "$p=\"zzz\"", q => q.AddParameter("p", "zzz")),
                ]),

            new CatalogQuery("or-chain", "three-way OR",
                "from index 'Items/Index' where City = $c or Name = $n or Age = $a",
                "OR queries always materialise into the bitmap pipeline (no streaming sort drive). The three leaves are lazily OR-ed into the accumulator.",
                [
                    new ParamSet("common", "$c=\"London\", $n=\"alice\", $a=30",
                        q => { q.AddParameter("c", "London"); q.AddParameter("n", "alice"); q.AddParameter("a", 30); }),
                    new ParamSet("rare", "$c=\"Rome\", $n=\"erin\", $a=99",
                        q => { q.AddParameter("c", "Rome"); q.AddParameter("n", "erin"); q.AddParameter("a", 99); }),
                ]),

            new CatalogQuery("not-exists-or", "sentinel collapse",
                "from index 'Items/Index' where City = $c or not exists(Name)",
                "`not exists(Name)` is materialised as `AllEntries ANDNOT Exists(Name)` and lazily OR-ed with `City = $c` (see the `FillAllEntries` / `ANDNOT` / `LazyOrWith` ops). Because every document has a `Name`, the negated-exists branch evaluates to empty at runtime, so the result equals `City = $c` — but the engine computes this at runtime; it does not statically fold the branch away, since the planner does not assume the field is always present.",
                [
                    new ParamSet("london", "$c=\"London\"", q => q.AddParameter("c", "London")),
                    new ParamSet("rome", "$c=\"Rome\"", q => q.AddParameter("c", "Rome")),
                ]),

            new CatalogQuery("between", "numeric range",
                "from index 'Items/Index' where Age between $lo and $hi",
                "A single range leaf. Narrow vs wide bounds change selectivity but not the compiled shape; both run the same range scan.",
                [
                    new ParamSet("narrow", "$lo=40, $hi=42", q => { q.AddParameter("lo", 40); q.AddParameter("hi", 42); }),
                    new ParamSet("wide", "$lo=18, $hi=79", q => { q.AddParameter("lo", 18); q.AddParameter("hi", 79); }),
                ]),

            new CatalogQuery("in", "set membership",
                "from index 'Items/Index' where City in ($a, $b, $c)",
                "IN over a posting-list field. Duplicate values collapse to the same posting source.",
                [
                    new ParamSet("three-distinct", "$a=\"London\", $b=\"Paris\", $c=\"Berlin\"",
                        q => { q.AddParameter("a", "London"); q.AddParameter("b", "Paris"); q.AddParameter("c", "Berlin"); }),
                    new ParamSet("all-same", "$a=\"Rome\", $b=\"Rome\", $c=\"Rome\"",
                        q => { q.AddParameter("a", "Rome"); q.AddParameter("b", "Rome"); q.AddParameter("c", "Rome"); }),
                ]),

            new CatalogQuery("nested-group", "OR of two ANDs",
                "from index 'Items/Index' where (City = $c and Age > $a) or (Name = $n and Age < $b)",
                "Two AND groups OR-ed together. Each group intersects into scratch, then the two scratch bitmaps are OR-ed. No sort drive (top-level OR).",
                [
                    new ParamSet("set1", "$c=\"London\", $a=30, $n=\"alice\", $b=70",
                        q => { q.AddParameter("c", "London"); q.AddParameter("a", 30); q.AddParameter("n", "alice"); q.AddParameter("b", 70); }),
                    new ParamSet("set2", "$c=\"Berlin\", $a=50, $n=\"bob\", $b=25",
                        q => { q.AddParameter("c", "Berlin"); q.AddParameter("a", 50); q.AddParameter("n", "bob"); q.AddParameter("b", 25); }),
                ]),

            new CatalogQuery("and-two", "selectivity flip drives operand order",
                "from index 'Items/Index' where Age > $a and City = $c",
                "One compiled plan serves both parameter sets. `City = $c` fills the accumulator and the `Age >` predicate is applied as an AND. Selectivity is handled at runtime: the `ShouldSwitchToEntryScan` gate switches between a tree-scan intersection and a per-entry residual scan based on the live cardinality.",
                [
                    new ParamSet("age-selective", "$a=78, $c=\"London\"", q => { q.AddParameter("a", 78); q.AddParameter("c", "London"); }),
                    new ParamSet("age-broad", "$a=18, $c=\"London\"", q => { q.AddParameter("a", 18); q.AddParameter("c", "London"); }),
                ]),

            new CatalogQuery("order-by", "FieldSortedScan candidacy vs fallback",
                "from index 'Items/Index' where Age > $a order by Age as long",
                "A range predicate sorted on the same field is a FieldSortedScan candidate: walk the field's term tree in order and stop at the page. When the predicate is non-selective the per-execution cost gate may fall back to the bitmap pipeline. Selective vs broad bounds show both decisions.",
                [
                    new ParamSet("selective", "$a=78", q => q.AddParameter("a", 78)),
                    new ParamSet("broad", "$a=18", q => q.AddParameter("a", 18)),
                ]),

            new CatalogQuery("and-negation", "AndNot",
                "from index 'Items/Index' where City = $c and Name != $n",
                "A positive leaf intersected with a negated leaf. `City = $c` fills the accumulator, then `Name != $n` is applied as an AndNot against the positive `Name = $n` posting list — no full negated bitmap is built. The same `ShouldSwitchToEntryScan` gate may instead run a per-entry residual scan (the `ResidualScan` method rejects rows whose `Name` equals the analyzed term).",
                [
                    new ParamSet("london-not-alice", "$c=\"London\", $n=\"alice\"", q => { q.AddParameter("c", "London"); q.AddParameter("n", "alice"); }),
                    new ParamSet("paris-not-bob", "$c=\"Paris\", $n=\"bob\"", q => { q.AddParameter("c", "Paris"); q.AddParameter("n", "bob"); }),
                ]),

            new CatalogQuery("all-negated-or", "De Morgan",
                "from index 'Items/Index' where Name != $n or City != $c",
                "An OR of two negations, folded by De Morgan: `Name != $n or City != $c` ≡ `NOT(Name = $n and City = $c)`. The generated code computes the positive intersection `Name = $n AND City = $c` into scratch (`bitmap[2]`), fills all entries into the accumulator, then `AndNotWith` subtracts the scratch — i.e. `AllEntries ANDNOT (Name = $n AND City = $c)`.",
                [
                    new ParamSet("set1", "$n=\"alice\", $c=\"London\"", q => { q.AddParameter("n", "alice"); q.AddParameter("c", "London"); }),
                    new ParamSet("set2", "$n=\"erin\", $c=\"Rome\"", q => { q.AddParameter("n", "erin"); q.AddParameter("c", "Rome"); }),
                ]),

            new CatalogQuery("search", "full-text leaf",
                "from index 'Items/Index' where search(Name, $term)",
                "A `search()` leaf tokenises the term through the analyzer pipeline and fills from a match (`CtxFillFromMatch`). A multi-token term (`alice bob`) expands internally to an OR over the analyzed tokens — handled inside the search match, so it surfaces as one `Fill` op with a higher count, not as separate plan ops.",
                [
                    new ParamSet("single", "$term=\"alice\"", q => q.AddParameter("term", "alice")),
                    new ParamSet("multi", "$term=\"alice bob\"", q => q.AddParameter("term", "alice bob")),
                ]),

            new CatalogQuery("startsWith", "prefix scan",
                "from index 'Items/Index' where startsWith(City, $p)",
                "A prefix predicate scans the term tree from the prefix boundary. A matching prefix vs a non-matching one share the compiled shape.",
                [
                    new ParamSet("lon", "$p=\"Lon\"", q => q.AddParameter("p", "Lon")),
                    new ParamSet("none", "$p=\"Zzz\"", q => q.AddParameter("p", "Zzz")),
                ]),

            new CatalogQuery("compound-sort", "exact + tie-break order",
                "from index 'Items/Index' where City = $c order by City, Age as long",
                "An equality leaf sorted by a two-field key. `City = $c` fills the accumulator (a FieldSortedScan candidate), then a `SortingMultiMatch` applies the (City, Age) comparer on top. Note the CompoundKeyLookup / CompoundSortedScan optimizations are *not* triggered here (`CompoundSortedScan` rejected) — ordering is done by the multi-field sort heap, even though the first sort field is constant within the result.",
                [
                    new ParamSet("london", "$c=\"London\"", q => q.AddParameter("c", "London")),
                    new ParamSet("rome", "$c=\"Rome\"", q => q.AddParameter("c", "Rome")),
                ]),

            new CatalogQuery("mixed-3-level", "AND of (OR, range)",
                "from index 'Items/Index' where (City = $c or Name = $n) and (Age between $lo and $hi)",
                "A two-level tree: an OR group intersected with a range. `City = $c` fills the accumulator, `Name = $n` is OR-ed into it, then the `Age between` range is AND-ed — with the runtime `ShouldSwitchToEntryScan` gate choosing tree-scan intersection vs. per-entry residual scan (the `ResidualScan` method applies the between bounds). Narrow vs wide bounds run the same plan, differing only in the runtime gate decision.",
                [
                    new ParamSet("narrow-age", "$c=\"London\", $n=\"alice\", $lo=40, $hi=42",
                        q => { q.AddParameter("c", "London"); q.AddParameter("n", "alice"); q.AddParameter("lo", 40); q.AddParameter("hi", 42); }),
                    new ParamSet("wide-age", "$c=\"Berlin\", $n=\"bob\", $lo=18, $hi=79",
                        q => { q.AddParameter("c", "Berlin"); q.AddParameter("n", "bob"); q.AddParameter("lo", 18); q.AddParameter("hi", 79); }),
                ]),
        ];
    }

    private static void Seed(IDocumentStore store)
    {
        var cities = new[] { "London", "Paris", "Berlin", "Madrid", "Rome" };
        var names = new[] { "alice", "bob", "carol", "dave", "erin" };
        var tags = new[] { "red", "green", "blue", "yellow" };
        var rng = new Random(12345);
        using Raven.Client.Documents.BulkInsert.BulkInsertOperation bulk = store.BulkInsert();
        for (int i = 0; i < DocCount; i++)
        {
            bulk.Store(new Item
            {
                Name = names[rng.Next(names.Length)],
                City = cities[rng.Next(cities.Length)],
                Age = rng.Next(18, 80),
                Score = rng.NextDouble() * 1000,
                Created = new DateTime(2000, 1, 1).AddDays(rng.Next(0, 9000)).AddSeconds(rng.Next(0, 86400)),
                Tags = new[] { tags[rng.Next(tags.Length)], tags[rng.Next(tags.Length)] }
            });
        }
    }
}
