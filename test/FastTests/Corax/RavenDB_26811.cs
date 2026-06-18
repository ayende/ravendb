using System.Linq;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Corax;

public class RavenDB_26811 : RavenTestBase
{
    public RavenDB_26811(ITestOutputHelper output) : base(output)
    {
    }

    private class Item
    {
        public string Name { get; set; }
    }

    private class Items_ByName : AbstractIndexCreationTask<Item>
    {
        public Items_ByName()
        {
            // Default (non-analyzed) field: each indexed term is the exact value, so the regex
            // term-dictionary scan matches whole values deterministically.
            Map = items => from i in items select new { i.Name };
        }
    }

    // RavenDB-26811: the regex term provider decodes each scanned term's UTF-8 bytes into a reusable
    // pooled char buffer (instead of allocating a string per term). This exercises that path over terms
    // of varying length (the buffer must grow when a longer term is encountered) and confirms matching
    // stays correct.
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void RegexScanIsCorrectAcrossVaryingLengthTerms(Options options)
    {
        using var store = GetDocumentStore(options);
        new Items_ByName().Execute(store);

        using (var s = store.OpenSession())
        {
            s.Store(new Item { Name = "alpha" });
            s.Store(new Item { Name = "alphabet" });
            s.Store(new Item { Name = "alphabetical-and-a-deliberately-long-value-to-force-the-pooled-buffer-to-grow" });
            s.Store(new Item { Name = "beta" });
            s.Store(new Item { Name = "b" });
            s.SaveChanges();
        }

        Indexes.WaitForIndexing(store);

        using (var s = store.OpenSession())
        {
            var startsWithAlpha = s.Advanced
                .RawQuery<Item>($"from index '{new Items_ByName().IndexName}' where regex(Name, $p) order by Name")
                .AddParameter("p", "^alpha")
                .ToList()
                .Select(x => x.Name)
                .ToList();

            Assert.Equal(new[]
            {
                "alpha",
                "alphabet",
                "alphabetical-and-a-deliberately-long-value-to-force-the-pooled-buffer-to-grow"
            }, startsWithAlpha);

            var containsLong = s.Advanced
                .RawQuery<Item>($"from index '{new Items_ByName().IndexName}' where regex(Name, $p)")
                .AddParameter("p", "force-the-pooled-buffer")
                .ToList();

            Assert.Single(containsLong);
            Assert.StartsWith("alphabetical-", containsLong[0].Name);
        }
    }
}
