using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
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

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public async Task AndWithRange_BitmapPipeline()
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
                    Status = i % 2 == 0 ? "active" : "inactive",
                    Price = i * 10
                });
            }
            await session.SaveChangesAsync();
        }

        Indexes.WaitForIndexing(store);

        // AND with range: Category=cat-0 AND Price > 200
        using (var session = store.OpenAsyncSession())
        {
            var results = await session.Query<TestDoc>()
                .Where(x => x.Category == "cat-0" && x.Price > 200)
                .ToListAsync();

            // cat-0: indices 0,5,10,15,20,...,95 (20 total)
            // Price > 200: i*10 > 200 → i > 20
            // cat-0 indices > 20: 25,30,35,40,45,50,55,60,65,70,75,80,85,90,95 → 15
            Assert.Equal(15, results.Count);
            Assert.All(results, r =>
            {
                Assert.Equal("cat-0", r.Category);
                Assert.True(r.Price > 200);
            });
        }
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public async Task InClause_BitmapPipeline()
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

        // IN clause: Category in (cat-0, cat-1, cat-2)
        using (var session = store.OpenAsyncSession())
        {
            var results = await session.Advanced.AsyncRawQuery<TestDoc>(
                "from TestDocs where Category in ('cat-0', 'cat-1', 'cat-2')")
                .ToListAsync();

            // 3 categories × 20 each = 60
            Assert.Equal(60, results.Count);
        }
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
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

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public async Task BetweenQuery_BitmapPipeline()
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
                    Status = i % 2 == 0 ? "active" : "inactive",
                    Price = i * 10
                });
            }
            await session.SaveChangesAsync();
        }

        Indexes.WaitForIndexing(store);

        // BETWEEN: Category=cat-0 AND Price between 100 and 500
        using (var session = store.OpenAsyncSession())
        {
            var results = await session.Advanced.AsyncRawQuery<TestDoc>(
                "from TestDocs where Category = 'cat-0' and Price between 100 and 500")
                .ToListAsync();

            // cat-0: 0,5,10,15,20,25,30,35,40,45,50,...
            // Price 100-500: i=10..50 → cat-0 in that range: 10,15,20,25,30,35,40,45,50 → 9
            Assert.Equal(9, results.Count);
        }
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public async Task LargeResultSet_BitmapPipeline()
    {
        var options = Options.ForSearchEngine(RavenSearchEngineMode.Corax);
        options.ModifyDatabaseRecord += record =>
        {
            record.Settings[RavenConfiguration.GetKey(x => x.Indexing.CoraxUseBitmapPipeline)] = true.ToString();
        };
        using var store = GetDocumentStore(options);

        using (var session = store.OpenAsyncSession())
        {
            for (int i = 0; i < 1000; i++)
            {
                await session.StoreAsync(new TestDoc
                {
                    Name = $"doc-{i}",
                    Category = $"cat-{i % 3}",
                    Status = i % 2 == 0 ? "active" : "inactive"
                });
            }
            await session.SaveChangesAsync();
        }

        Indexes.WaitForIndexing(store);

        // Large AND: 500 active ∩ 334 cat-0 = ~167
        using (var session = store.OpenAsyncSession())
        {
            var results = await session.Query<TestDoc>()
                .Where(x => x.Status == "active" && x.Category == "cat-0")
                .ToListAsync();

            // active: even indices, cat-0: i%3==0
            // Both: i%2==0 AND i%3==0 → i%6==0 → 0,6,12,...,996 → 167
            Assert.Equal(167, results.Count);
        }
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public async Task StartsWith_BitmapPipeline()
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
                    Name = $"alpha-{i}",
                    Category = "cat-0",
                    Status = "active"
                });
                await session.StoreAsync(new TestDoc
                {
                    Name = $"beta-{i}",
                    Category = "cat-1",
                    Status = "inactive"
                });
            }
            await session.SaveChangesAsync();
        }

        Indexes.WaitForIndexing(store);

        // startsWith
        using (var session = store.OpenAsyncSession())
        {
            var results = await session.Advanced.AsyncRawQuery<TestDoc>(
                "from TestDocs where startsWith(Name, 'alpha')")
                .ToListAsync();

            Assert.Equal(50, results.Count);
            Assert.All(results, r => Assert.StartsWith("alpha", r.Name));
        }
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public async Task ExistsQuery_BitmapPipeline()
    {
        var options = Options.ForSearchEngine(RavenSearchEngineMode.Corax);
        options.ModifyDatabaseRecord += record =>
        {
            record.Settings[RavenConfiguration.GetKey(x => x.Indexing.CoraxUseBitmapPipeline)] = true.ToString();
        };
        using var store = GetDocumentStore(options);

        // Store docs — all have Category, so exists(Category) should return all
        using (var session = store.OpenAsyncSession())
        {
            for (int i = 0; i < 30; i++)
            {
                await session.StoreAsync(new TestDoc
                {
                    Name = $"doc-{i}",
                    Category = $"cat-{i % 3}",
                    Status = "active"
                });
            }
            await session.SaveChangesAsync();
        }

        Indexes.WaitForIndexing(store);

        // exists(Category) — all 30 docs have it
        using (var session = store.OpenAsyncSession())
        {
            var results = await session.Advanced.AsyncRawQuery<TestDoc>(
                "from TestDocs where exists(Category)")
                .ToListAsync();

            Assert.Equal(30, results.Count);
        }
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public async Task AndWithStartsWith_BitmapPipeline()
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
                    Name = $"item-{i:D5}",
                    Category = $"cat-{i % 5}",
                    Status = i % 2 == 0 ? "active" : "inactive"
                });
            }
            await session.SaveChangesAsync();
        }

        Indexes.WaitForIndexing(store);

        // AND with startsWith: Status=active AND startsWith(Name, 'item-0000')
        using (var session = store.OpenAsyncSession())
        {
            var results = await session.Advanced.AsyncRawQuery<TestDoc>(
                "from TestDocs where Status = 'active' and startsWith(Name, 'item-0000')")
                .ToListAsync();

            // item-00000 to item-00009, active (even): 00000,00002,00004,00006,00008 → 5
            Assert.Equal(5, results.Count);
        }
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public async Task OrderByScore_BitmapPath()
    {
        var options = Options.ForSearchEngine(RavenSearchEngineMode.Corax);
        options.ModifyDatabaseRecord += record =>
        {
            record.Settings[RavenConfiguration.GetKey(x => x.Indexing.CoraxUseBitmapPipeline)] = true.ToString();
        };
        using var store = GetDocumentStore(options);

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new TestDoc { Name = "hello world foo", Category = "cat-0", Status = "active" });
            await session.StoreAsync(new TestDoc { Name = "hello", Category = "cat-0", Status = "active" });
            await session.StoreAsync(new TestDoc { Name = "world", Category = "cat-1", Status = "inactive" });
            await session.SaveChangesAsync();
        }

        Indexes.WaitForIndexing(store);

        // ORDER BY score() should fall back to old path and work correctly
        using (var session = store.OpenAsyncSession())
        {
            var results = await session.Advanced.AsyncRawQuery<TestDoc>(
                "from TestDocs where search(Name, 'hello') order by score()")
                .ToListAsync();

            // Should find docs with 'hello' in Name, ordered by relevance
            Assert.True(results.Count >= 1);
        }
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public async Task RegexQuery_BitmapPipeline()
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
                    Name = i % 2 == 0 ? $"alpha-{i}" : $"beta-{i}",
                    Category = "cat-0",
                    Status = "active"
                });
            }
            await session.SaveChangesAsync();
        }

        Indexes.WaitForIndexing(store);

        // regex query
        using (var session = store.OpenAsyncSession())
        {
            var results = await session.Advanced.AsyncRawQuery<TestDoc>(
                "from TestDocs where regex(Name, '^alpha')")
                .ToListAsync();

            Assert.Equal(25, results.Count);
            Assert.All(results, r => Assert.StartsWith("alpha", r.Name));
        }
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public async Task NotEqualsInAndChain_BitmapPipeline()
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

        // AND with !=: Category=cat-0 AND Status != 'active'
        // First verify old path returns correct results
        using (var session = store.OpenAsyncSession())
        {
            var oldPathResults = await session.Advanced.AsyncRawQuery<TestDoc>(
                "from TestDocs where Category = 'cat-0' and Status = 'inactive'")
                .ToListAsync();
            Assert.Equal(10, oldPathResults.Count);
            Assert.All(oldPathResults, r => Assert.Equal("inactive", r.Status));
        }

        // Now test != through bitmap path
        using (var session = store.OpenAsyncSession())
        {
            var results = await session.Advanced.AsyncRawQuery<TestDoc>(
                "from TestDocs where Category = 'cat-0' and Status != 'active'")
                .ToListAsync();

            Assert.Equal(10, results.Count);
            foreach (var r in results)
            {
                Assert.Equal("cat-0", r.Category);
                Assert.Equal("inactive", r.Status);
            }
        }
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public async Task MixedAndOr_BitmapPipeline()
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

        // Mixed: (Category=cat-0 OR Category=cat-1) AND Status=active
        using (var session = store.OpenAsyncSession())
        {
            var results = await session.Advanced.AsyncRawQuery<TestDoc>(
                "from TestDocs where (Category = 'cat-0' or Category = 'cat-1') and Status = 'active'")
                .ToListAsync();

            // (cat-0 ∪ cat-1) = 40, active = 50, intersection = 20
            Assert.Equal(20, results.Count);
            Assert.All(results, r =>
            {
                Assert.True(r.Category is "cat-0" or "cat-1");
                Assert.Equal("active", r.Status);
            });
        }
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public async Task OrderBy_BitmapPipeline()
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
                    Name = $"doc-{i:D5}",
                    Category = $"cat-{i % 5}",
                    Status = i % 2 == 0 ? "active" : "inactive",
                    Price = (99 - i) * 10 // reverse order
                });
            }
            await session.SaveChangesAsync();
        }

        Indexes.WaitForIndexing(store);

        // ORDER BY Name LIMIT 10
        using (var session = store.OpenAsyncSession())
        {
            var results = await session.Advanced.AsyncRawQuery<TestDoc>(
                "from TestDocs where Category = 'cat-0' order by Name limit 10")
                .ToListAsync();

            Assert.Equal(10, results.Count);
            // Should be sorted by Name ascending
            for (int i = 1; i < results.Count; i++)
            {
                Assert.True(string.Compare(results[i - 1].Name, results[i].Name, System.StringComparison.Ordinal) <= 0,
                    $"Results not sorted: {results[i - 1].Name} should be before {results[i].Name}");
            }
        }
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public async Task ComplexAndOrWithSort_BitmapPipeline()
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
                    Name = $"item-{i:D5}",
                    Category = $"cat-{i % 5}",
                    Status = i % 2 == 0 ? "active" : "inactive",
                    Tag = $"tag-{i % 10}",
                    Price = i * 5
                });
            }
            await session.SaveChangesAsync();
        }

        Indexes.WaitForIndexing(store);

        // Complex: (cat-0 OR cat-1) AND active, ORDER BY Name LIMIT 10
        using (var session = store.OpenAsyncSession())
        {
            var results = await session.Advanced.AsyncRawQuery<TestDoc>(
                "from TestDocs where (Category = 'cat-0' or Category = 'cat-1') and Status = 'active' order by Name limit 10")
                .ToListAsync();

            Assert.Equal(10, results.Count);
            Assert.All(results, r =>
            {
                Assert.True(r.Category is "cat-0" or "cat-1");
                Assert.Equal("active", r.Status);
            });
            // Verify sorting
            for (int i = 1; i < results.Count; i++)
            {
                Assert.True(string.Compare(results[i - 1].Name, results[i].Name, System.StringComparison.Ordinal) <= 0);
            }
        }
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public async Task EndsWith_BitmapPipeline()
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
                    Name = i % 2 == 0 ? $"item-{i}-alpha" : $"item-{i}-beta",
                    Category = $"cat-{i % 5}",
                    Status = "active"
                });
            }
            await session.SaveChangesAsync();
        }

        Indexes.WaitForIndexing(store);

        using (var session = store.OpenAsyncSession())
        {
            var results = await session.Advanced.AsyncRawQuery<TestDoc>(
                "from TestDocs where endsWith(Name, 'alpha')")
                .ToListAsync();

            Assert.Equal(25, results.Count);
            Assert.All(results, r => Assert.EndsWith("alpha", r.Name));
        }
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public async Task FiveWayAnd_BitmapPipeline()
    {
        var options = Options.ForSearchEngine(RavenSearchEngineMode.Corax);
        options.ModifyDatabaseRecord += record =>
        {
            record.Settings[RavenConfiguration.GetKey(x => x.Indexing.CoraxUseBitmapPipeline)] = true.ToString();
        };
        using var store = GetDocumentStore(options);

        using (var session = store.OpenAsyncSession())
        {
            for (int i = 0; i < 500; i++)
            {
                await session.StoreAsync(new TestDoc
                {
                    Name = $"doc-{i:D5}",
                    Category = $"cat-{i % 5}",
                    Status = i % 2 == 0 ? "active" : "inactive",
                    Tag = $"tag-{i % 10}",
                    Price = i * 3
                });
            }
            await session.SaveChangesAsync();
        }

        Indexes.WaitForIndexing(store);

        // 5-way AND: Category=cat-0 AND Status=active AND Tag=tag-0 AND Price>500 AND startsWith(Name, 'doc-0')
        using (var session = store.OpenAsyncSession())
        {
            var results = await session.Advanced.AsyncRawQuery<TestDoc>(
                "from TestDocs where Category = 'cat-0' and Status = 'active' and Tag = 'tag-0' and Price > 500 and startsWith(Name, 'doc-0')")
                .ToListAsync();

            // cat-0: i%5==0, active: i%2==0, tag-0: i%10==0 → i%10==0 AND i%2==0 → i%10==0
            // Price > 500: i*3 > 500 → i > 166
            // startsWith doc-0: i < 100000 (all match with 5-digit padding)
            // Combined: i%10==0 AND i>166 → 170,180,190,...,490 → 33 items
            Assert.True(results.Count > 0);
            Assert.All(results, r =>
            {
                Assert.Equal("cat-0", r.Category);
                Assert.Equal("active", r.Status);
                Assert.Equal("tag-0", r.Tag);
                Assert.True(r.Price > 500);
            });
        }
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public async Task NestedOrWithAnd_BitmapPipeline()
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
                    Status = i % 2 == 0 ? "active" : "inactive",
                    Tag = $"tag-{i % 10}"
                });
            }
            await session.SaveChangesAsync();
        }

        Indexes.WaitForIndexing(store);

        // (cat-0 OR cat-1) AND (tag-0 OR tag-5)
        using (var session = store.OpenAsyncSession())
        {
            var results = await session.Advanced.AsyncRawQuery<TestDoc>(
                "from TestDocs where (Category = 'cat-0' or Category = 'cat-1') and (Tag = 'tag-0' or Tag = 'tag-5')")
                .ToListAsync();

            // cat-0∪cat-1: 40 docs, tag-0∪tag-5: 20 docs
            // Intersection: cat-0 or cat-1 AND tag-0 or tag-5
            // cat-0 indices: 0,5,10,15,20,25,30,35,40,45,50,55,60,65,70,75,80,85,90,95
            // cat-1 indices: 1,6,11,16,21,26,31,36,41,46,51,56,61,66,71,76,81,86,91,96
            // tag-0: i%10==0: 0,10,20,30,40,50,60,70,80,90
            // tag-5: i%10==5: 5,15,25,35,45,55,65,75,85,95
            // (cat-0∪cat-1) ∩ (tag-0∪tag-5):
            //   cat-0 ∩ tag-0: 0,10,20,30,40,50,60,70,80,90 (all i%10==0 are cat-0 since 0%5==0) → 10
            //   cat-0 ∩ tag-5: 5,15,25,35,45,55,65,75,85,95 (all i%10==5 are cat-0 since 5%5==0) → 10
            //   cat-1 ∩ tag-0: none (cat-1 indices end in 1,6 — none mod 10 == 0)
            //   cat-1 ∩ tag-5: none
            // Wait — cat-1 is i%5==1: 1,6,11,16,21,26,...
            // tag-0 is i%10==0: 0,10,20,30,...
            // cat-1 ∩ tag-0: i%5==1 AND i%10==0 → no solution (i%10==0 means i%5==0)
            // cat-1 ∩ tag-5: i%5==1 AND i%10==5 → i%10==5 means i%5==0 (5%5=0) → no
            // Actually i%5==1 gives 1,6,11,16,... and i%10==5 gives 5,15,25,...
            // 1,6,11,16,21,26,31,36,41,46 ∩ 5,15,25,35,45 = empty
            // So total = 10 + 10 = 20
            Assert.Equal(20, results.Count);
        }
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public async Task EmptyResultSet_BitmapPipeline()
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
                    Category = "cat-0",
                    Status = "active"
                });
            }
            await session.SaveChangesAsync();
        }

        Indexes.WaitForIndexing(store);

        // No matches — nonexistent term
        using (var session = store.OpenAsyncSession())
        {
            var results = await session.Query<TestDoc>()
                .Where(x => x.Category == "cat-999")
                .ToListAsync();

            Assert.Equal(0, results.Count);
        }

        // AND that produces empty result
        using (var session = store.OpenAsyncSession())
        {
            var results = await session.Query<TestDoc>()
                .Where(x => x.Category == "cat-0" && x.Status == "inactive")
                .ToListAsync();

            Assert.Equal(0, results.Count);
        }
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public async Task Pagination_BitmapPipeline()
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
                    Name = $"doc-{i:D5}",
                    Category = "cat-0",
                    Status = "active"
                });
            }
            await session.SaveChangesAsync();
        }

        Indexes.WaitForIndexing(store);

        // Page 1: skip 0, take 10
        using (var session = store.OpenAsyncSession())
        {
            var page1 = await session.Query<TestDoc>()
                .Where(x => x.Category == "cat-0")
                .Skip(0).Take(10)
                .ToListAsync();
            Assert.Equal(10, page1.Count);
        }

        // Page 2: skip 10, take 10
        using (var session = store.OpenAsyncSession())
        {
            var page2 = await session.Query<TestDoc>()
                .Where(x => x.Category == "cat-0")
                .Skip(10).Take(10)
                .ToListAsync();
            Assert.Equal(10, page2.Count);
        }

        // Page 10: skip 90, take 10
        using (var session = store.OpenAsyncSession())
        {
            var page10 = await session.Query<TestDoc>()
                .Where(x => x.Category == "cat-0")
                .Skip(90).Take(10)
                .ToListAsync();
            Assert.Equal(10, page10.Count);
        }
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public async Task TrueOrConstantFolding_BitmapPipeline()
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
                    Category = $"cat-{i % 3}",
                    Status = "active"
                });
            }
            await session.SaveChangesAsync();
        }

        Indexes.WaitForIndexing(store);

        // (true or Category='cat-0') → should return all 50 docs
        using (var session = store.OpenAsyncSession())
        {
            var results = await session.Advanced.AsyncRawQuery<TestDoc>(
                "from TestDocs where true or Category = 'cat-0'")
                .ToListAsync();

            Assert.Equal(50, results.Count);
        }
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public async Task OrderByDesc_BitmapPipeline()
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
                    Name = $"doc-{i:D5}",
                    Category = "cat-0",
                    Status = "active",
                    Price = i * 10
                });
            }
            await session.SaveChangesAsync();
        }

        Indexes.WaitForIndexing(store);

        // ORDER BY Price DESC LIMIT 5
        using (var session = store.OpenAsyncSession())
        {
            var results = await session.Advanced.AsyncRawQuery<TestDoc>(
                "from TestDocs where Category = 'cat-0' order by Price as long desc limit 5")
                .ToListAsync();

            Assert.Equal(5, results.Count);
            // Should be highest prices first
            Assert.True(results[0].Price >= results[1].Price);
            Assert.True(results[0].Price >= results[4].Price);
        }
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public async Task SingleDocResult_BitmapPipeline()
    {
        var options = Options.ForSearchEngine(RavenSearchEngineMode.Corax);
        options.ModifyDatabaseRecord += record =>
        {
            record.Settings[RavenConfiguration.GetKey(x => x.Indexing.CoraxUseBitmapPipeline)] = true.ToString();
        };
        using var store = GetDocumentStore(options);

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new TestDoc { Name = "unique-doc", Category = "unique-cat", Status = "active" });
            for (int i = 0; i < 99; i++)
                await session.StoreAsync(new TestDoc { Name = $"doc-{i}", Category = "common-cat", Status = "active" });
            await session.SaveChangesAsync();
        }

        Indexes.WaitForIndexing(store);

        // Single result — tests cardinality-1 path
        using (var session = store.OpenAsyncSession())
        {
            var results = await session.Query<TestDoc>()
                .Where(x => x.Category == "unique-cat")
                .ToListAsync();

            Assert.Equal(1, results.Count);
            Assert.Equal("unique-doc", results[0].Name);
        }
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public async Task SearchQuery_BitmapPipeline()
    {
        var options = Options.ForSearchEngine(RavenSearchEngineMode.Corax);
        options.ModifyDatabaseRecord += record =>
        {
            record.Settings[RavenConfiguration.GetKey(x => x.Indexing.CoraxUseBitmapPipeline)] = true.ToString();
        };
        using var store = GetDocumentStore(options);

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new TestDoc { Name = "hello world", Category = "cat-0", Status = "active" });
            await session.StoreAsync(new TestDoc { Name = "hello there", Category = "cat-0", Status = "active" });
            await session.StoreAsync(new TestDoc { Name = "goodbye world", Category = "cat-1", Status = "inactive" });
            await session.StoreAsync(new TestDoc { Name = "foo bar", Category = "cat-2", Status = "active" });
            await session.SaveChangesAsync();
        }

        Indexes.WaitForIndexing(store);

        // search() query without boost — should use bitmap path
        using (var session = store.OpenAsyncSession())
        {
            var results = await session.Advanced.AsyncRawQuery<TestDoc>(
                "from TestDocs where search(Name, 'hello')")
                .ToListAsync();

            Assert.Equal(2, results.Count);
        }
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public async Task NoWhereClause_BitmapPipeline()
    {
        var options = Options.ForSearchEngine(RavenSearchEngineMode.Corax);
        options.ModifyDatabaseRecord += record =>
        {
            record.Settings[RavenConfiguration.GetKey(x => x.Indexing.CoraxUseBitmapPipeline)] = true.ToString();
        };
        using var store = GetDocumentStore(options);

        using (var session = store.OpenAsyncSession())
        {
            for (int i = 0; i < 30; i++)
            {
                await session.StoreAsync(new TestDoc
                {
                    Name = $"doc-{i}",
                    Category = $"cat-{i % 3}",
                    Status = i % 2 == 0 ? "active" : "inactive"
                });
            }
            await session.SaveChangesAsync();
        }

        Indexes.WaitForIndexing(store);

        // No WHERE clause — should return all documents
        using (var session = store.OpenAsyncSession())
        {
            var results = await session.Advanced.AsyncRawQuery<TestDoc>(
                "from TestDocs")
                .ToListAsync();

            Assert.Equal(30, results.Count);
        }
    }

    [RavenFact(RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public async Task NoWhereClauseWithOrderBy_BitmapPipeline()
    {
        var options = Options.ForSearchEngine(RavenSearchEngineMode.Corax);
        options.ModifyDatabaseRecord += record =>
        {
            record.Settings[RavenConfiguration.GetKey(x => x.Indexing.CoraxUseBitmapPipeline)] = true.ToString();
        };
        using var store = GetDocumentStore(options);

        using (var session = store.OpenAsyncSession())
        {
            for (int i = 0; i < 20; i++)
            {
                await session.StoreAsync(new TestDoc
                {
                    Name = $"doc-{i:D2}",
                    Category = $"cat-{i % 5}",
                    Status = "active"
                });
            }
            await session.SaveChangesAsync();
        }

        Indexes.WaitForIndexing(store);

        // No WHERE clause with ORDER BY — bitmap path should handle sorting
        using (var session = store.OpenAsyncSession())
        {
            var results = await session.Advanced.AsyncRawQuery<TestDoc>(
                "from TestDocs order by Name")
                .ToListAsync();

            Assert.Equal(20, results.Count);
            // Verify ordering
            for (int i = 1; i < results.Count; i++)
            {
                Assert.True(string.Compare(results[i - 1].Name, results[i].Name, StringComparison.Ordinal) <= 0,
                    $"Expected {results[i - 1].Name} <= {results[i].Name}");
            }
        }
    }

    private class TestDoc
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public string Category { get; set; }
        public string Status { get; set; }
        public string Tag { get; set; }
        public int Price { get; set; }
    }
}
