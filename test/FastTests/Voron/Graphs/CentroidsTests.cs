using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using FastTests.Voron.FixedSize;
using Sparrow.Server.Collections;
using Tests.Infrastructure;
using Voron;
using Voron.Data.Graphs;
using Xunit;

namespace FastTests.Voron.Graphs;

public class CentroidsTests(ITestOutputHelper output) : StorageTest(output)
{
    private const string TreeName = "vectors";
    private const int Dimensions = 48;

    private static float[][] ClusteredVectors(Random random, int count, int clusters, int dimensions = Dimensions)
    {
        var centers = new float[clusters][];
        for (int c = 0; c < clusters; c++)
            centers[c] = Enumerable.Range(0, dimensions).Select(_ => (float)(random.NextDouble() * 2 - 1)).ToArray();

        var vectors = new float[count][];
        for (int i = 0; i < count; i++)
        {
            var center = centers[random.Next(clusters)];
            vectors[i] = center.Select(x => x + (float)(random.NextDouble() - 0.5) * 0.6f).ToArray();
        }

        return vectors;
    }

    private void Create(VectorEmbeddingType type = VectorEmbeddingType.Single, int vectorSize = Dimensions * sizeof(float), int maxPartitionSize = 64, int bits = 4)
    {
        using var txw = Env.WriteTransaction();
        Centroids.Create(txw.LowLevelTransaction, TreeName, vectorSize, type, maxPartitionSize, bits);
        txw.Commit();
    }

    private void Insert(float[][] vectors, int offset = 0, int batchSize = 250)
    {
        for (int start = 0; start < vectors.Length; start += batchSize)
        {
            using var txw = Env.WriteTransaction();
            using (var registration = Centroids.RegistrationFor(txw.LowLevelTransaction, TreeName))
            {
                for (int i = start; i < Math.Min(vectors.Length, start + batchSize); i++)
                    registration.Register(offset + i + 1, MemoryMarshal.AsBytes(vectors[i].AsSpan()));
                registration.Commit(CancellationToken.None);
            }

            txw.Commit();
        }
    }

    private byte[] HashOf(float[] vector)
    {
        using var txw = Env.WriteTransaction();
        using var registration = Centroids.RegistrationFor(txw.LowLevelTransaction, TreeName);
        return registration.Register(1, MemoryMarshal.AsBytes(vector.AsSpan())).ToSpan().ToArray();
    }

    private void Remove(IEnumerable<(long Entry, float[] Vector)> removals)
    {
        var hashes = removals.Select(x => (x.Entry, Hash: HashOf(x.Vector))).ToList();
        using var txw = Env.WriteTransaction();
        using (var registration = Centroids.RegistrationFor(txw.LowLevelTransaction, TreeName))
        {
            foreach (var (entry, hash) in hashes)
                registration.Remove(entry, hash);
            registration.Commit(CancellationToken.None);
        }

        txw.Commit();
    }

    private List<(long Entry, float Distance)> Search(float[] query, int numberOfCandidates, Centroids.SearchMode mode = Centroids.SearchMode.Approximate,
        GrowableBitArray? filter = null)
    {
        using var txr = Env.ReadTransaction();
        using var _ = Slice.From(txr.Allocator, TreeName, out var name);
        var retriever = Centroids.Search(txr.LowLevelTransaction, name, MemoryMarshal.AsBytes(query.AsSpan()).ToArray(), numberOfCandidates, 0f, mode, filter);
        try
        {
            var results = new List<(long, float)>();
            var matches = new long[64];
            var distances = new float[64];
            while (retriever.Fill(matches, distances, filter) is var read and > 0)
            {
                for (int i = 0; i < read; i++)
                    results.Add((matches[i], distances[i]));
            }

            return results.OrderBy(x => x.Item2).ToList();
        }
        finally
        {
            retriever.Dispose();
        }
    }

    private Centroids.Options ReadOptions()
    {
        using var txr = Env.ReadTransaction();
        using var scope = Slice.From(txr.Allocator, TreeName, out var name);
        Assert.True(Centroids.TryReadOptions(txr.LowLevelTransaction, name, out var options, out _));
        return options;
    }

    private static double Recall(List<(long Entry, float Distance)> approximate, List<(long Entry, float Distance)> exact, int k)
    {
        var expected = exact.Take(k).Select(x => x.Entry).ToHashSet();
        return approximate.Take(k).Count(x => expected.Contains(x.Entry)) / (double)Math.Min(k, expected.Count);
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void EmptyIndexReturnsNothing()
    {
        Create();
        var query = Enumerable.Range(0, Dimensions).Select(i => (float)i).ToArray();
        Assert.Empty(Search(query, 16));
        Assert.Empty(Search(query, 16, Centroids.SearchMode.Exact));

        using var txr = Env.ReadTransaction();
        using var _ = Slice.From(txr.Allocator, TreeName, out var name);
        Assert.True(Centroids.IsCentroidsIndex(txr.LowLevelTransaction, name));
    }

    [RavenTheory(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    [InlineDataWithRandomSeed(4)]
    [InlineDataWithRandomSeed(8)]
    public void ApproximateSearchMatchesExactSearch(int bits, int seed)
    {
        var random = new Random(seed);
        var vectors = ClusteredVectors(random, 4_000, 40);
        Create(bits: bits);
        Insert(vectors);

        var options = ReadOptions();
        Assert.Equal(vectors.Length, options.CountOfVectors);
        Assert.True(options.NumberOfPartitions > 40, $"Expected the index to split, got {options.NumberOfPartitions} partitions");
        Assert.True(options.NumberOfSplits > 0);

        double recall = 0;
        const int queries = 50;
        for (int q = 0; q < queries; q++)
        {
            var query = vectors[random.Next(vectors.Length)].Select(x => x + (float)(random.NextDouble() - 0.5) * 0.1f).ToArray();
            var exact = Search(query, 10, Centroids.SearchMode.Exact);
            var approximate = Search(query, 32);
            recall += Recall(approximate, exact, 10);
        }

        Assert.True(recall / queries > 0.9, $"Recall {recall / queries}");
    }

    [RavenTheory(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    [InlineDataWithRandomSeed]
    public void EveryStoredVectorFindsItself(int seed)
    {
        var random = new Random(seed);
        var vectors = ClusteredVectors(random, 2_000, 20);
        Create();
        Insert(vectors);

        for (int i = 0; i < vectors.Length; i += 37)
        {
            var results = Search(vectors[i], 8);
            Assert.Equal(i + 1, results[0].Entry);
            Assert.True(results[0].Distance < 1e-4, $"Distance {results[0].Distance}");
        }
    }

    [RavenTheory(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    [InlineDataWithRandomSeed]
    public void DeletedVectorsAreNotReturnedAndPartitionsShrink(int seed)
    {
        var random = new Random(seed);
        var vectors = ClusteredVectors(random, 3_000, 30);
        Create();
        Insert(vectors);

        var partitionsBefore = ReadOptions().NumberOfPartitions;
        var deleted = new HashSet<long>();
        for (int round = 0; round < 4; round++)
        {
            var removals = new List<(long, float[])>();
            for (int i = 0; i < vectors.Length; i++)
            {
                var entry = i + 1;
                if (deleted.Contains(entry) || random.NextDouble() > 0.4)
                    continue;
                removals.Add((entry, vectors[i]));
                deleted.Add(entry);
            }

            Remove(removals);
        }

        var options = ReadOptions();
        Assert.Equal(vectors.Length - deleted.Count, options.CountOfVectors);
        Assert.True(options.NumberOfPartitions < partitionsBefore, $"{options.NumberOfPartitions} >= {partitionsBefore}");
        Assert.True(options.NumberOfMerges > 0);

        for (int i = 0; i < vectors.Length; i += 13)
        {
            var results = Search(vectors[i], 16);
            Assert.DoesNotContain(results, r => deleted.Contains(r.Entry));
            if (deleted.Contains(i + 1) == false)
                Assert.Equal(i + 1, results[0].Entry);
        }

        var all = Search(vectors[0], vectors.Length, Centroids.SearchMode.Exact);
        Assert.Equal(vectors.Length - deleted.Count, all.Count);
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void DeletingEverythingLeavesAnEmptyIndexThatCanBeReused()
    {
        var random = new Random(1234);
        var vectors = ClusteredVectors(random, 500, 5);
        Create();
        Insert(vectors);
        Remove(vectors.Select((v, i) => ((long)i + 1, v)));

        var options = ReadOptions();
        Assert.Equal(0, options.CountOfVectors);
        Assert.Equal(0, options.NumberOfPartitions);
        Assert.Empty(Search(vectors[0], 10));

        Insert(vectors, offset: 1_000);
        Assert.Equal(1_001, Search(vectors[0], 10)[0].Entry);
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void IdenticalVectorsShareTheNodeAndKeepAllEntries()
    {
        var random = new Random(42);
        var vectors = ClusteredVectors(random, 300, 3);
        Create();
        Insert(vectors);

        var duplicate = vectors[7];
        using (var txw = Env.WriteTransaction())
        {
            using (var registration = Centroids.RegistrationFor(txw.LowLevelTransaction, TreeName))
            {
                registration.Register(5_000, MemoryMarshal.AsBytes(duplicate.AsSpan()));
                registration.Register(5_001, MemoryMarshal.AsBytes(duplicate.AsSpan()));
                registration.Commit(CancellationToken.None);
            }

            txw.Commit();
        }

        Assert.Equal(300, ReadOptions().CountOfVectors);
        var top = Search(duplicate, 1).Select(x => x.Entry).ToHashSet();
        Assert.Equal(new HashSet<long> { 8, 5_000, 5_001 }, top);

        Remove([(5_000, duplicate), (8, duplicate)]);

        Assert.Equal(new HashSet<long> { 5_001 }, Search(duplicate, 1).Select(x => x.Entry).ToHashSet());
    }

    [RavenTheory(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    [InlineDataWithRandomSeed]
    public void UpdatingAVectorMovesTheEntry(int seed)
    {
        var random = new Random(seed);
        var vectors = ClusteredVectors(random, 1_500, 15);
        Create();
        Insert(vectors);

        var replacement = ClusteredVectors(random, 1, 1)[0];
        var oldHash = HashOf(vectors[10]);
        using (var txw = Env.WriteTransaction())
        {
            using (var registration = Centroids.RegistrationFor(txw.LowLevelTransaction, TreeName))
            {
                registration.Remove(11, oldHash);
                registration.Register(11, MemoryMarshal.AsBytes(replacement.AsSpan()));
                registration.Commit(CancellationToken.None);
            }

            txw.Commit();
        }

        Assert.Equal(11, Search(replacement, 4)[0].Entry);
        Assert.DoesNotContain(Search(vectors[10], 4), r => r.Entry == 11 && r.Distance < 1e-4);
    }

    [RavenTheory(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    [InlineDataWithRandomSeed]
    public void FilteredSearchReturnsOnlyMatchingEntries(int seed)
    {
        var random = new Random(seed);
        var vectors = ClusteredVectors(random, 3_000, 30);
        Create();
        Insert(vectors);

        var filter = new GrowableBitArray(Allocator, vectors.Length + 1);
        var allowed = new HashSet<long>();
        for (int i = 1; i <= vectors.Length; i++)
        {
            if (random.NextDouble() >= 0.1)
                continue;
            filter.Add(i);
            allowed.Add(i);
        }

        filter.Count = allowed.Count;
        try
        {
            double recall = 0;
            for (int q = 0; q < 20; q++)
            {
                var query = vectors[random.Next(vectors.Length)];
                var results = Search(query, 10, filter: filter);
                Assert.All(results, r => Assert.Contains(r.Entry, allowed));
                var expected = Search(query, vectors.Length, Centroids.SearchMode.Exact).Where(x => allowed.Contains(x.Entry)).Take(10).ToList();
                recall += Recall(results, expected, 10);
            }

            Assert.True(recall / 20 > 0.8, $"Recall {recall / 20}");
        }
        finally
        {
            filter.Dispose();
        }
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void QuantizedEmbeddingsAreSupported()
    {
        var random = new Random(7);
        var vectors = ClusteredVectors(random, 1_000, 10, 64);

        Create(VectorEmbeddingType.Int8, vectorSize: 64 + sizeof(float));
        var int8 = vectors.Select(v =>
        {
            var max = v.Max(Math.Abs);
            var bytes = new byte[64 + sizeof(float)];
            for (int i = 0; i < 64; i++)
                bytes[i] = (byte)(sbyte)Math.Round(v[i] / max * 127);
            BitConverter.TryWriteBytes(bytes.AsSpan(64), max);
            return bytes;
        }).ToArray();
        InsertRaw(int8);
        for (int i = 0; i < vectors.Length; i += 50)
            Assert.Equal(i + 1, SearchRaw(int8[i], 4)[0]);
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Vector)]
    public void BinaryEmbeddingsAreSupported()
    {
        var random = new Random(9);
        var vectors = ClusteredVectors(random, 1_000, 10, 128);
        Create(VectorEmbeddingType.Binary, vectorSize: 128 / 8);
        var binary = vectors.Select(v =>
        {
            var bytes = new byte[16];
            for (int i = 0; i < 128; i++)
                if (v[i] > 0)
                    bytes[i >> 3] |= (byte)(1 << (7 - (i & 7)));
            return bytes;
        }).ToArray();
        InsertRaw(binary);
        for (int i = 0; i < vectors.Length; i += 50)
        {
            var results = SearchRaw(binary[i], 4);
            Assert.Contains(i + 1, results);
        }
    }

    private void InsertRaw(byte[][] vectors)
    {
        using var txw = Env.WriteTransaction();
        using (var registration = Centroids.RegistrationFor(txw.LowLevelTransaction, TreeName))
        {
            for (int i = 0; i < vectors.Length; i++)
                registration.Register(i + 1, vectors[i]);
            registration.Commit(CancellationToken.None);
        }

        txw.Commit();
    }

    private List<long> SearchRaw(byte[] query, int numberOfCandidates)
    {
        using var txr = Env.ReadTransaction();
        using var _ = Slice.From(txr.Allocator, TreeName, out var name);
        var retriever = Centroids.Search(txr.LowLevelTransaction, name, query.ToArray(), numberOfCandidates, 0f, Centroids.SearchMode.Approximate);
        try
        {
            var results = new List<(long, float)>();
            var matches = new long[64];
            var distances = new float[64];
            while (retriever.Fill(matches, distances, null) is var read and > 0)
            {
                for (int i = 0; i < read; i++)
                    results.Add((matches[i], distances[i]));
            }

            return results.OrderBy(x => x.Item2).Select(x => x.Item1).ToList();
        }
        finally
        {
            retriever.Dispose();
        }
    }
}
