#pragma warning disable CS1998 // Async method lacks 'await' operators and will run synchronously
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using FastTests;
using Tests.Infrastructure;

namespace Tryouts;

public static class Program
{
    public static async Task Main(string[] args)
    {
        using var helper = new ConsoleTestOutputHelper();
        await using var bench = new CoraxQueryBench(helper);
        bench.Run();
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

public sealed class CoraxQueryBench : RavenTestBase
{
    public CoraxQueryBench(Xunit.ITestOutputHelper output) : base(output)
    {
    }

    private const int DocCount = 50_000;
    private const int Warmup = 200;
    private const int Iterations = 3_000;

    public void Run()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        Seed(store);
        new Items_Index().Execute(store);
        Indexes.WaitForIndexing(store);

        var cities = new[] { "London", "Paris", "Berlin", "Madrid", "Rome" };
        var names = new[] { "alice", "bob", "carol", "dave", "erin" };

        var queries = new (string Label, string Rql)[]
        {
            ("eq",            "from index 'Items/Index' where Name = $p"),
            ("or-chain",      "from index 'Items/Index' where City = $c or Name = $n or Age = $a"),
            ("not-exists-or", "from index 'Items/Index' where City = $c or not exists(Name)"),
            ("between",       "from index 'Items/Index' where Age between $lo and $hi"),
            ("in",            "from index 'Items/Index' where City in ($a, $b, $c)"),
            ("nested-group",  "from index 'Items/Index' where (City = $c and Age > $a) or (Name = $n and Age < $b)"),
        };

        // Vary the sort by TYPE so each comparer/scan path is exercised:
        //  long (native numeric fwd), long desc (backward scan), double (floating), string, date (ticks), and a 2-field tie-break.
        var sorts = new (string Name, string Clause)[]
        {
            ("long",     "Age as long"),
            ("long-desc","Age as long desc"),
            ("double",   "Score as double"),
            ("string",   "Name as string"),
            ("date",     "Created"),
            ("compound", "City, Age as long"),
        };

        int[] pageSizes = { 10, 25 };
        const int PageCount = 3; // page 1, then the next page, then the one after

        Console.WriteLine($"=== Corax query benchmark | docs={DocCount} warmup={Warmup} iters={Iterations} | sorted, paged ===");

        foreach (var q in queries)
        {
            foreach (var sort in sorts)
            {
                string sortedRql = $"{q.Rql} order by {sort.Clause}";
                foreach (var pageSize in pageSizes)
                {
                    for (int page = 0; page < PageCount; page++)
                    {
                        int skip = page * pageSize;
                        string pagedRql = $"{sortedRql} limit {skip}, {pageSize}";

                        // warmup
                        for (int i = 0; i < Warmup; i++)
                            ExecQuery(store, q.Label, pagedRql, cities, names, i);

                        var sw = Stopwatch.StartNew();
                        var samples = new List<double>(Iterations);
                        for (int i = 0; i < Iterations; i++)
                        {
                            long t0 = Stopwatch.GetTimestamp();
                            ExecQuery(store, q.Label, pagedRql, cities, names, i);
                            long t1 = Stopwatch.GetTimestamp();
                            samples.Add((t1 - t0) * 1000.0 / Stopwatch.Frequency);
                        }
                        sw.Stop();

                        samples.Sort();
                        double median = samples[samples.Count / 2];
                        double p95 = samples[(int)(samples.Count * 0.95)];
                        double mean = samples.Average();
                        Console.WriteLine($"{q.Label,-14} {sort.Name,-9} ps{pageSize,-2} p{page + 1}  mean={mean:F4}ms  median={median:F4}ms  p95={p95:F4}ms  total={sw.ElapsedMilliseconds}ms");
                    }
                }
            }
        }
        Console.WriteLine("=== done ===");
    }

    private static int ExecQuery(IDocumentStore store, string label, string rql, string[] cities, string[] names, int i)
    {
        using var session = store.OpenSession();
        var q = session.Advanced.RawQuery<Item>(rql);
        switch (label)
        {
            case "eq":
                q.AddParameter("p", names[i % names.Length]);
                break;
            case "or-chain":
                q.AddParameter("c", cities[i % cities.Length]);
                q.AddParameter("n", names[i % names.Length]);
                q.AddParameter("a", 20 + i % 60);
                break;
            case "not-exists-or":
                q.AddParameter("c", cities[i % cities.Length]);
                break;
            case "between":
                q.AddParameter("lo", 20 + i % 30);
                q.AddParameter("hi", 50 + i % 30);
                break;
            case "in":
                q.AddParameter("a", cities[i % cities.Length]);
                q.AddParameter("b", cities[(i + 1) % cities.Length]);
                q.AddParameter("c", cities[(i + 2) % cities.Length]);
                break;
            case "nested-group":
                q.AddParameter("c", cities[i % cities.Length]);
                q.AddParameter("a", 20 + i % 40);
                q.AddParameter("n", names[i % names.Length]);
                q.AddParameter("b", 60 + i % 20);
                break;
        }
        return q.ToList().Count;
    }

    private static void Seed(IDocumentStore store)
    {
        var cities = new[] { "London", "Paris", "Berlin", "Madrid", "Rome" };
        var names = new[] { "alice", "bob", "carol", "dave", "erin" };
        var tags = new[] { "red", "green", "blue", "yellow" };
        var rng = new Random(12345);
        using var bulk = store.BulkInsert();
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
