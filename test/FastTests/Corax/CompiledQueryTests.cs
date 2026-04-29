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

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public async Task ThreeWayAnd_BitmapPipeline()
    {
        var options = Options.ForSearchEngine(RavenSearchEngineMode.Corax);
        options.ModifyDatabaseRecord += record =>
        {
            record.Settings[RavenConfiguration.GetKey(x => x.Indexing.CoraxUseBitmapPipeline)] = true.ToString();
        };
        using var store = GetDocumentStore(options);

        using (var session = store.OpenAsyncSession())
        {
            for (int i = 0; i < 200; i++)
            {
                await session.StoreAsync(new TestDoc
                {
                    Name = $"doc-{i}",
                    Category = $"cat-{i % 5}",
                    Status = i % 2 == 0 ? "active" : "inactive",
                    Tag = $"tag-{i % 10}"
                });
            }
            await session.SaveChangesAsync();
        }

        Indexes.WaitForIndexing(store);

        // 3-way AND: Status=active AND Category=cat-0 AND Tag=tag-0
        using (var session = store.OpenAsyncSession())
        {
            var results = await session.Query<TestDoc>()
                .Where(x => x.Status == "active" && x.Category == "cat-0" && x.Tag == "tag-0")
                .ToListAsync();

            // active (100/200) ∩ cat-0 (40/200) ∩ tag-0 (20/200)
            // Expected: docs where i%2==0 AND i%5==0 AND i%10==0 → i%10==0
            Assert.Equal(20, results.Count);
            Assert.All(results, r =>
            {
                Assert.Equal("active", r.Status);
                Assert.Equal("cat-0", r.Category);
                Assert.Equal("tag-0", r.Tag);
            });
        }
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying, Skip = "NotEquals standalone requires AllEntries+ANDNOT — not yet in bitmap path")]
    public async Task NotEquals_BitmapPipeline()
    {
        var options = Options.ForSearchEngine(RavenSearchEngineMode.Corax);
        options.ModifyDatabaseRecord += record =>
        {
            record.Settings[RavenConfiguration.GetKey(x => x.Indexing.CoraxUseBitmapPipeline)] = true.ToString();
        };
        using var store = GetDocumentStore(options);

        using (var session = store.OpenAsyncSession())
        {
            for (int i = 0; i < 50; i++)
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

        // Status != "active" — should get inactive docs
        using (var session = store.OpenAsyncSession())
        {
            var results = await session.Query<TestDoc>()
                .Where(x => x.Status != "active")
                .ToListAsync();

            Assert.Equal(25, results.Count);
            Assert.All(results, r => Assert.Equal("inactive", r.Status));
        }
    }

    private class TestDoc
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string Category { get; set; }
        public string Status { get; set; }
        public string Tag { get; set; }
    }
}
