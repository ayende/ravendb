using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.ServerWide.Operations;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Corax.Vectors;

public class CentroidsVectorIndexTests(ITestOutputHelper output) : RavenTestBase(output)
{
    private const int Dimensions = 32;

    private class Item
    {
        public string Id { get; set; }
        public string Tag { get; set; }
        public float[] Embedding { get; set; }
    }

    private class ItemsByVector : AbstractIndexCreationTask<Item>
    {
        public ItemsByVector() : this(VectorEmbeddingType.Single, VectorIndexingStrategy.Centroids)
        {
        }

        public ItemsByVector(VectorEmbeddingType destination, VectorIndexingStrategy? strategy)
        {
            Map = items => from item in items
                select new { item.Tag, Embedding = CreateVector(item.Embedding) };

            VectorIndexes.Add(x => x.Embedding, new VectorOptions
            {
                SourceEmbeddingType = VectorEmbeddingType.Single,
                DestinationEmbeddingType = destination,
                Dimensions = destination == VectorEmbeddingType.Binary ? null : Dimensions,
                IndexingStrategy = strategy
            });
        }
    }

    private static List<Item> CreateItems(int count, int seed)
    {
        var random = new Random(seed);
        var centers = Enumerable.Range(0, 20).Select(_ => Enumerable.Range(0, Dimensions).Select(_ => (float)(random.NextDouble() * 2 - 1)).ToArray()).ToArray();
        return Enumerable.Range(0, count).Select(i =>
        {
            var center = centers[random.Next(centers.Length)];
            return new Item
            {
                Id = $"items/{i}",
                Tag = i % 10 == 0 ? "rare" : "common",
                Embedding = center.Select(x => x + (float)(random.NextDouble() - 0.5) * 0.5f).ToArray()
            };
        }).ToList();
    }

    private static void Store(IDocumentStore store, List<Item> items)
    {
        using var bulk = store.BulkInsert();
        foreach (var item in items)
            bulk.Store(item, item.Id);
    }

    private static List<Item> Nearest(IDocumentStore store, float[] embedding, int take = 10, bool isExact = false, int? numberOfCandidates = null)
    {
        using var session = store.OpenSession();
        return session.Query<Item, ItemsByVector>()
            .VectorSearch(f => f.WithField(x => x.Embedding), v => v.ByEmbedding(embedding), numberOfCandidates: numberOfCandidates, isExact: isExact)
            .Take(take)
            .ToList();
    }

    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    [RavenData(VectorEmbeddingType.Single, SearchEngineMode = RavenSearchEngineMode.Corax)]
    [RavenData(VectorEmbeddingType.Int8, SearchEngineMode = RavenSearchEngineMode.Corax)]
    [RavenData(VectorEmbeddingType.Binary, SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void CanIndexAndQuery(Options options, VectorEmbeddingType destination)
    {
        using var store = GetDocumentStore(options);
        var items = CreateItems(2_000, 1337);
        Store(store, items);
        new ItemsByVector(destination, VectorIndexingStrategy.Centroids).Execute(store);
        Indexes.WaitForIndexing(store);

        for (int i = 0; i < items.Count; i += 197)
        {
            var results = Nearest(store, items[i].Embedding, numberOfCandidates: 32);
            if (destination == VectorEmbeddingType.Binary)
                Assert.Equal(items[i].Embedding.Select(x => x >= 0), results[0].Embedding.Select(x => x >= 0));
            else
                Assert.Equal(items[i].Id, results[0].Id);
        }
    }

    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void ApproximateResultsMatchExactResults(Options options)
    {
        using var store = GetDocumentStore(options);
        var items = CreateItems(3_000, 42);
        Store(store, items);
        new ItemsByVector().Execute(store);
        Indexes.WaitForIndexing(store);

        double recall = 0;
        var random = new Random(7);
        const int queries = 20;
        for (int q = 0; q < queries; q++)
        {
            var query = items[random.Next(items.Count)].Embedding.Select(x => x + (float)(random.NextDouble() - 0.5) * 0.1f).ToArray();
            var exact = Nearest(store, query, isExact: true, numberOfCandidates: 10).Select(x => x.Id).ToHashSet();
            var approximate = Nearest(store, query, numberOfCandidates: 32);
            recall += approximate.Take(10).Count(x => exact.Contains(x.Id)) / 10.0;
        }

        Assert.True(recall / queries > 0.9, $"Recall {recall / queries}");
    }

    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void FilteredQueriesReturnOnlyMatchingDocuments(Options options)
    {
        using var store = GetDocumentStore(options);
        var items = CreateItems(3_000, 99);
        Store(store, items);
        new ItemsByVector().Execute(store);
        Indexes.WaitForIndexing(store);

        foreach (var item in items.Take(50))
        {
            using var session = store.OpenSession();
            var results = session.Query<Item, ItemsByVector>()
                .Where(x => x.Tag == "rare")
                .VectorSearch(f => f.WithField(x => x.Embedding), v => v.ByEmbedding(item.Embedding), numberOfCandidates: 16)
                .Take(10)
                .ToList();

            Assert.NotEmpty(results);
            Assert.All(results, r => Assert.Equal("rare", r.Tag));
            if (item.Tag == "rare")
                Assert.Equal(item.Id, results[0].Id);
        }
    }

    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void CanQueryByDocument(Options options)
    {
        using var store = GetDocumentStore(options);
        var items = CreateItems(1_000, 5);
        Store(store, items);
        new ItemsByVector().Execute(store);
        Indexes.WaitForIndexing(store);

        using var session = store.OpenSession();
        var results = session.Query<Item, ItemsByVector>()
            .VectorSearch(f => f.WithField(x => x.Embedding), v => v.ForDocument("items/17"))
            .Take(5)
            .ToList();
        Assert.Equal("items/17", results[0].Id);
    }

    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void DeletesAndUpdatesAreReflected(Options options)
    {
        using var store = GetDocumentStore(options);
        var items = CreateItems(2_000, 11);
        Store(store, items);
        new ItemsByVector().Execute(store);
        Indexes.WaitForIndexing(store);

        var replacement = CreateItems(1, 12345)[0].Embedding;
        using (var session = store.OpenSession())
        {
            for (int i = 0; i < items.Count; i += 2)
                session.Delete(items[i].Id);
            var updated = session.Load<Item>("items/1");
            updated.Embedding = replacement;
            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);

        for (int i = 0; i < items.Count; i += 101)
        {
            var results = Nearest(store, items[i].Embedding, numberOfCandidates: 32);
            Assert.DoesNotContain(results, r => int.Parse(r.Id.Split('/')[1]) % 2 == 0);
        }

        Assert.Equal("items/1", Nearest(store, replacement)[0].Id);
        Assert.DoesNotContain(Nearest(store, items[1].Embedding, take: 1), r => r.Id == "items/1");
    }

    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void DefaultStrategyComesFromConfiguration(Options options)
    {
        options.ModifyDatabaseRecord += record => record.Settings["Indexing.Corax.VectorSearch.DefaultIndexingStrategy"] = nameof(VectorIndexingStrategy.Centroids);
        using var store = GetDocumentStore(options);
        var items = CreateItems(1_000, 3);
        Store(store, items);
        new ItemsByVector(VectorEmbeddingType.Single, strategy: null).Execute(store);
        Indexes.WaitForIndexing(store);

        Assert.Equal(items[3].Id, Nearest(store, items[3].Embedding)[0].Id);

        var terms = store.Maintenance.Send(new GetTermsOperation(new ItemsByVector().IndexName, "Embedding", null, 10));
        Assert.Equal(10, terms.Length);
    }

    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void IndexingStrategyIsPartOfTheIndexDefinition(Options options)
    {
        using var store = GetDocumentStore(options);
        new ItemsByVector().Execute(store);

        var definition = store.Maintenance.Send(new GetIndexOperation(new ItemsByVector().IndexName));
        Assert.Equal(VectorIndexingStrategy.Centroids, definition.Fields["Embedding"].Vector.IndexingStrategy);

        var hnsw = new ItemsByVector(VectorEmbeddingType.Single, VectorIndexingStrategy.Hnsw).CreateIndexDefinition();
        Assert.True(definition.Compare(hnsw).HasFlag(IndexDefinitionCompareDifferences.Fields));
    }

    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Corax)]
    [RavenData(VectorIndexingStrategy.Centroids, SearchEngineMode = RavenSearchEngineMode.Corax)]
    [RavenData(VectorIndexingStrategy.Hnsw, SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void IndexCanBeReopenedAndKeepsServingQueries(Options options, VectorIndexingStrategy strategy)
    {
        options.RunInMemory = false;
        using var store = GetDocumentStore(options);
        var items = CreateItems(1_500, 21);
        Store(store, items);
        new ItemsByVector(VectorEmbeddingType.Single, strategy).Execute(store);
        Indexes.WaitForIndexing(store);

        store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, disable: true));
        store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, disable: false));

        Indexes.WaitForIndexing(store);
        Assert.Equal(items[42].Id, Nearest(store, items[42].Embedding)[0].Id);

        using (var session = store.OpenSession())
        {
            session.Store(new Item { Id = "items/new", Tag = "rare", Embedding = items[42].Embedding.Select(x => -x).ToArray() });
            session.SaveChanges();
        }

        Indexes.WaitForIndexing(store);
        Assert.Equal("items/new", Nearest(store, items[42].Embedding.Select(x => -x).ToArray())[0].Id);
    }
}
