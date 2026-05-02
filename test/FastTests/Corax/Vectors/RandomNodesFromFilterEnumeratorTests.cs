using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using Corax;
using Corax.Indexing;
using Corax.Mappings;
using Corax.Querying;
using Voron.Data.RoaringBitmaps;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron.Data.Graphs;
using Xunit;

namespace FastTests.Corax.Vectors;

public class RandomNodesFromFilterEnumeratorTests(ITestOutputHelper output) : StorageTest(output)
{
    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Vector)]
    [InlineDataWithRandomSeed]
    public void CanEnumerateRandomNodesFromFilterEmpty(int seed)
    {
        var random = new Random(seed);
        using var indexMapping = InsertData();
        // Test empty
        using (var indexSearcher = new IndexSearcher(Env, indexMapping))
        {
            var filterResult = new RoaringBitmap(indexSearcher.Allocator);
            filterResult.PrepareForReading();

            using var it = new IndexSearcher.VectorSearchUtils.RandomNodesFromFilterEnumerator(indexSearcher, indexMapping.GetByFieldId(1).Metadata, filterResult, random);

            Assert.False(it.MoveNext());
            filterResult.Dispose();
        }
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Vector)]
    [InlineDataWithRandomSeed]
    public void CanEnumerateRandomNodesFromFilter(int seed)
    {
        var random = new Random(seed);
        using var indexMapping = InsertData();
        using (var indexSearcher = new IndexSearcher(Env, indexMapping))
        {
            var filterResult = new RoaringBitmap(indexSearcher.Allocator);

            var allEntries = Enumerable.Range(1, (int)indexSearcher.LastEntryId).Select(x => (long)x).ToArray();
            random.Shuffle(allEntries.AsSpan());
            var count = random.Next(1, (int)indexSearcher.LastEntryId);

            var toInsert = allEntries.AsSpan().Slice(0, count);
            toInsert.Sort();

            filterResult.AddRange(toInsert);
            filterResult.PrepareForReading();

            List<long> results = new();

            using (var it = new IndexSearcher.VectorSearchUtils.RandomNodesFromFilterEnumerator(indexSearcher, indexMapping.GetByFieldId(1).Metadata, filterResult, random))
            {
                while (it.MoveNext())
                {
                    results.Add(it.Current);
                }
            }

            Assert.True(results.Count > 0);
            // Each entry should produce at least one node ID
            Assert.True(results.Distinct().Count() <= count * 2); // upper bound: each doc could have multiple vectors
            filterResult.Dispose();
        }
    }


    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Vector)]
    [InlineDataWithRandomSeed]
    [InlineData(479664366)]
    [InlineData(1978241581)]
    public void CanEnumerateRandomNodesFromFilterOnlyOne(int seed)
    {
        var random = new Random(seed);
        using var indexMapping = InsertData();
        //First
        using (var indexSearcher = new IndexSearcher(Env, indexMapping))
        {
            var filterResult = new RoaringBitmap(indexSearcher.Allocator);
            filterResult.Add(1);
            filterResult.PrepareForReading();

            using (var it = new IndexSearcher.VectorSearchUtils.RandomNodesFromFilterEnumerator(indexSearcher, indexMapping.GetByFieldId(1).Metadata, filterResult, random))
            {
                Assert.True(it.MoveNext());
                Assert.Equal(1, it.Current);
                Assert.False(it.MoveNext());
                Assert.Equal(-1, it.Current);
            }

            filterResult.Dispose();
        }


        //Last
        using (var indexSearcher = new IndexSearcher(Env, indexMapping))
        {
            var filterResult = new RoaringBitmap(indexSearcher.Allocator);
            filterResult.Add(indexSearcher.LastEntryId);
            filterResult.PrepareForReading();

            using (var it = new IndexSearcher.VectorSearchUtils.RandomNodesFromFilterEnumerator(indexSearcher, indexMapping.GetByFieldId(1).Metadata, filterResult, random))
            {
                Assert.True(it.MoveNext());
                Assert.Equal(indexSearcher.LastEntryId, it.Current);
                Assert.False(it.MoveNext());
                Assert.Equal(-1, it.Current);
            }

            filterResult.Dispose();
        }
        // Random
        using (var indexSearcher = new IndexSearcher(Env, indexMapping))
        {
            var filterResult = new RoaringBitmap(indexSearcher.Allocator);
            var expected = random.Next(2, (int)indexSearcher.LastEntryId);
            filterResult.Add(expected);
            filterResult.PrepareForReading();

            using (var it = new IndexSearcher.VectorSearchUtils.RandomNodesFromFilterEnumerator(indexSearcher, indexMapping.GetByFieldId(1).Metadata, filterResult, random))
            {
                Assert.True(it.MoveNext());
                Assert.Equal(expected, it.Current);
                Assert.False(it.MoveNext());
                Assert.Equal(-1, it.Current);
            }

            filterResult.Dispose();
        }
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Vector)]
    [InlineDataWithRandomSeed]
    public void CanEnumerateRandomNodesFromFilterWhenDocumentHasMultiple(int seed)
    {
        using var indexMapping = InsertData(true);
        var random = new Random(seed);
        using (var indexSearcher = new IndexSearcher(Env, indexMapping))
        {
            var filterResult = new RoaringBitmap(indexSearcher.Allocator);
            filterResult.Add(1);
            filterResult.PrepareForReading();

            using (var it = new IndexSearcher.VectorSearchUtils.RandomNodesFromFilterEnumerator(indexSearcher, indexMapping.GetByFieldId(1).Metadata, filterResult, random))
            {
                Assert.True(it.MoveNext());
                Assert.Equal(1, it.Current);
                Assert.True(it.MoveNext());
                Assert.Equal(2, it.Current);
                Assert.False(it.MoveNext());
                Assert.Equal(-1, it.Current);
            }

            filterResult.Dispose();
        }
        using (var indexSearcher = new IndexSearcher(Env, indexMapping))
        {
            var filterResult = new RoaringBitmap(indexSearcher.Allocator);
            filterResult.Add(indexSearcher.LastEntryId);
            filterResult.PrepareForReading();

            using (var it = new IndexSearcher.VectorSearchUtils.RandomNodesFromFilterEnumerator(indexSearcher, indexMapping.GetByFieldId(1).Metadata, filterResult, random))
            {
                Assert.True(it.MoveNext());
                Assert.Equal(2 * indexSearcher.LastEntryId - 1, it.Current);
                Assert.True(it.MoveNext());
                Assert.Equal(2 * indexSearcher.LastEntryId, it.Current);
                Assert.False(it.MoveNext());
                Assert.Equal(-1, it.Current);
            }

            filterResult.Dispose();
        }
        using (var indexSearcher = new IndexSearcher(Env, indexMapping))
        {
            var filterResult = new RoaringBitmap(indexSearcher.Allocator);
            var randomDoc = random.Next(1, (int)indexSearcher.LastEntryId + 1);
            filterResult.Add(randomDoc);
            filterResult.PrepareForReading();

            using (var it = new IndexSearcher.VectorSearchUtils.RandomNodesFromFilterEnumerator(indexSearcher, indexMapping.GetByFieldId(1).Metadata, filterResult, random))
            {
                Assert.True(it.MoveNext());
                Assert.Equal(2 * randomDoc - 1, it.Current);
                Assert.True(it.MoveNext());
                Assert.Equal(2 * randomDoc, it.Current);
                Assert.False(it.MoveNext());
                Assert.Equal(-1, it.Current);
            }

            filterResult.Dispose();
        }
    }

    [RavenTheory(RavenTestCategory.Corax | RavenTestCategory.Vector)]
    [InlineDataWithRandomSeed]
    public void CanEnumerateRandomNodesFromFilterMultipleNodesPerDocument(int seed)
    {
        var random = new Random(seed);
        using var indexMapping = InsertData(true);
        using (var indexSearcher = new IndexSearcher(Env, indexMapping))
        {
            var filterResult = new RoaringBitmap(indexSearcher.Allocator);

            var allEntries = Enumerable.Range(1, (int)indexSearcher.LastEntryId).Select(x => (long)x).ToArray();
            random.Shuffle(allEntries.AsSpan());
            var count = random.Next(1, (int)indexSearcher.LastEntryId);

            var toInsert = allEntries.AsSpan().Slice(0, count);
            toInsert.Sort();

            filterResult.AddRange(toInsert);
            filterResult.PrepareForReading();

            var expectedResults = toInsert.ToArray().Select(x => x * 2).Concat(toInsert.ToArray().Select(x => x * 2 - 1)).ToArray();

            List<long> results = new();

            using (var it = new IndexSearcher.VectorSearchUtils.RandomNodesFromFilterEnumerator(indexSearcher, indexMapping.GetByFieldId(1).Metadata, filterResult, random))
            {
                while (it.MoveNext())
                {
                    results.Add(it.Current);
                }
            }

            Assert.Equal(expectedResults.Length, results.Distinct().Count());
            results.Sort();
            expectedResults.AsSpan().Sort();
            Assert.Equal(expectedResults, CollectionsMarshal.AsSpan(results));
            filterResult.Dispose();
        }
    }

    private IndexFieldsMapping InsertData(bool indexAsList = false)
    {
        var indexMapping = IndexFieldsMappingBuilder.CreateForWriter(false)
            .AddBinding(0, "id()")
            .AddBinding(1, "Vector",
                vectorOptions: new VectorOptions()
                {
                    NumberOfCandidates = 16,
                    NumberOfEdges = 16,
                    VectorEmbeddingType = VectorEmbeddingType.Single
                })
            .Build();

        using (var indexWriter = new IndexWriter(Env, indexMapping, SupportedFeatures.All))
        {
            for (int i = 1; i <= 10; ++i)
            {
                using var builder = indexWriter.Index($"Dto/{i}");
                builder.Write(0, Encoding.UTF8.GetBytes($"Dto/{i}"));
                if (indexAsList == false)
                {
                    float[] vector = { (float)i * 0.1f, (float)i * 0.2f };
                    var vectorAsBytes = MemoryMarshal.Cast<float, byte>(vector);
                    builder.WriteVector(1, null, vectorAsBytes);
                }
                else
                {
                    builder.IncrementList();
                    float[] vector = { (float)2 * i * 0.1f, (float)2 * i * 0.2f };
                    var vectorAsBytes = MemoryMarshal.Cast<float, byte>(vector);
                    builder.WriteVector(1, null, vectorAsBytes);

                    vector = [(float)(2 * i + 1) * 0.1f, (float)(2 * i + 1) * 0.2f];
                    vectorAsBytes = MemoryMarshal.Cast<float, byte>(vector);
                    builder.WriteVector(1, null, vectorAsBytes);

                    builder.DecrementList();
                }

                builder.EndWriting();
            }

            indexWriter.Commit();
        }

        return indexMapping;
    }
}
