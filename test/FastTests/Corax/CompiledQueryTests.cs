using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Corax;

public class CompiledQueryTests : RavenTestBase
{
    public CompiledQueryTests(Xunit.ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public async Task SimpleTermQuery_BitmapPipeline()
    {
        var options = Options.ForSearchEngine(RavenSearchEngineMode.Corax);
        options.ModifyDatabaseRecord += record =>
        {
            record.Settings[RavenConfiguration.GetKey(x => x.Indexing.CoraxUseBitmapPipeline)] = true.ToString();
        };
        using var store = GetDocumentStore(options);

        using (var session = store.OpenAsyncSession())
        {
            for (int i = 0; i < 100; i++)
            {
                await session.StoreAsync(new TestDoc
                {
                    Name = $"doc-{i}",
                    Category = $"cat-{i % 5}",
                    Status = i % 2 == 0 ? "active" : "inactive"
                });
            }
            await session.SaveChangesAsync();
        }

        Indexes.WaitForIndexing(store);

        // Single term query
        using (var session = store.OpenAsyncSession())
        {
            var results = await session.Query<TestDoc>()
                .Where(x => x.Status == "active")
                .ToListAsync();

            Assert.Equal(50, results.Count);
        }
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public async Task AndQuery_BitmapPipeline()
    {
        var options = Options.ForSearchEngine(RavenSearchEngineMode.Corax);
        options.ModifyDatabaseRecord += record =>
        {
            record.Settings[RavenConfiguration.GetKey(x => x.Indexing.CoraxUseBitmapPipeline)] = true.ToString();
        };
        using var store = GetDocumentStore(options);

        using (var session = store.OpenAsyncSession())
        {
            for (int i = 0; i < 100; i++)
            {
                await session.StoreAsync(new TestDoc
                {
                    Name = $"doc-{i}",
                    Category = $"cat-{i % 5}",
                    Status = i % 2 == 0 ? "active" : "inactive"
                });
            }
            await session.SaveChangesAsync();
        }

        Indexes.WaitForIndexing(store);

        // AND query: Status=active AND Category=cat-0
        using (var session = store.OpenAsyncSession())
        {
            var results = await session.Query<TestDoc>()
                .Where(x => x.Status == "active" && x.Category == "cat-0")
                .ToListAsync();

            Assert.Equal(10, results.Count);
            Assert.All(results, r =>
            {
                Assert.Equal("active", r.Status);
                Assert.Equal("cat-0", r.Category);
            });
        }
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public async Task OrQuery_BitmapPipeline()
    {
        var options = Options.ForSearchEngine(RavenSearchEngineMode.Corax);
        options.ModifyDatabaseRecord += record =>
        {
            record.Settings[RavenConfiguration.GetKey(x => x.Indexing.CoraxUseBitmapPipeline)] = true.ToString();
        };
        using var store = GetDocumentStore(options);

        using (var session = store.OpenAsyncSession())
        {
            for (int i = 0; i < 100; i++)
            {
                await session.StoreAsync(new TestDoc
                {
                    Name = $"doc-{i}",
                    Category = $"cat-{i % 5}",
                    Status = i % 2 == 0 ? "active" : "inactive"
                });
            }
            await session.SaveChangesAsync();
        }

        Indexes.WaitForIndexing(store);

        // OR query: Category=cat-0 OR Category=cat-1
        using (var session = store.OpenAsyncSession())
        {
            var results = await session.Query<TestDoc>()
                .Where(x => x.Category == "cat-0" || x.Category == "cat-1")
                .ToListAsync();

            Assert.Equal(40, results.Count);
        }
    }

    private class TestDoc
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string Category { get; set; }
        public string Status { get; set; }
    }
}
