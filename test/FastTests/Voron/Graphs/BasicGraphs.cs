using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Numerics.Tensors;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Jint.Native.Json;
using Newtonsoft.Json;
using Parquet;
using Tests.Infrastructure;
using Voron.Data.Graphs;
using Xunit;
using Xunit.Abstractions;
using JsonSerializer = Newtonsoft.Json.JsonSerializer;

namespace FastTests.Voron.Graphs;

public class BasicGraphs(ITestOutputHelper output) : StorageTest(output)
{
    [RavenFact(RavenTestCategory.Voron)]
    public void CanCreateEmptyGraph()
    {
        long id;
        using (var txw = Env.WriteTransaction())
        {
            id = Hnsw.Create(txw.LowLevelTransaction, 16, 3, 12);

            txw.Commit();
        }

        using (var txr = Env.ReadTransaction())
        {
            var options = Hnsw.ReadOptions(txr.LowLevelTransaction, id);
            Assert.Equal(12, options.NumberOfCandidates);
            Assert.Equal(3, options.NumberOfEdges);
            Assert.Equal(0, options.CountOfVectors);
        }
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void BasicSearch()
    {
        float[] v1 = [0.1f, 0.2f, 0.3f, 0.4f];
        float[] v2 = [0.15f, 0.25f, 0.35f, 0.45f];

        // nearest to v2, then v1
        float[] v3 = [0.25f, 0.35f, 0.45f, 0.55f];

        long id;

        using (var txw = Env.WriteTransaction())
        {
            id = Hnsw.Create(txw.LowLevelTransaction, 16, 3, 12);

            using (var registration = Hnsw.RegistrationFor(txw.LowLevelTransaction, id))
            {
                registration.Register(4, MemoryMarshal.Cast<float, byte>(v1));
                registration.Register(8, MemoryMarshal.Cast<float, byte>(v2));
                registration.Register(12, MemoryMarshal.Cast<float, byte>(v1));
            }

            txw.Commit();
        }

        using (var txr = Env.ReadTransaction())
        {
            var options = Hnsw.ReadOptions(txr.LowLevelTransaction, id);
            Assert.Equal(12, options.NumberOfCandidates);
            Assert.Equal(3, options.NumberOfEdges);
            Assert.Equal(2, options.CountOfVectors);
        }

        using (var txr = Env.ReadTransaction())
        {
            Span<long> matches = stackalloc long[8];
            using var nearest = Hnsw.ApproximateNearest(txr.LowLevelTransaction, id,
                numberOfCandidates: 32,
                MemoryMarshal.Cast<float, byte>(v3));
            int read = nearest.Fill(matches);
            Assert.Equal(3, read);
            Assert.Equal(8, matches[0]);
            Assert.Equal(4, matches[1]);
            Assert.Equal(12, matches[2]);
        }
    }

    async IAsyncEnumerable<(long, float[])> YieldVectors(string filePath)
    {
        var file = await ParquetReader.CreateAsync(filePath);
        var schema = file.Schema;
        for (int i = 0; i < file.RowGroupCount; i++)
        {
            var reader = file.OpenRowGroupReader(i);
            var wikiId = await reader.ReadColumnAsync(schema.DataFields[4]);
            var vectors = await reader.ReadColumnAsync(schema.DataFields[8]);
            var wikiIds = (int?[])wikiId.Data;
            var vectorsArr = (float?[])vectors.Data;
            int index = 0;
            foreach (var cur in wikiIds)
            {
                var vec = new float[768];
                for(int j = 0; j < 768; j++)
                {
                    vec[j] = vectorsArr[index++].Value;
                }
                yield return (cur.Value, vec);
            }
        }
    }

    [RavenFact(RavenTestCategory.Voron)]
    public async Task MoreNodesThanNeighbors()
    {
        long id;
        int numberOfCandidates = 8;
        var expected = new Dictionary<long, float[]>();
        using (var txw = Env.WriteTransaction())
        {
            id = Hnsw.Create(txw.LowLevelTransaction, 768 * 4, 16, 64);

            using (var registration = Hnsw.RegistrationFor(txw.LowLevelTransaction, id))
            {
                registration.Random = new Random(1337);

                foreach(var file in new[]
                        {
                            @"C:\Users\ayende\Downloads\train-00000-of-00004-1a1932c9ca1c7152.parquet", 
                            @"C:\Users\ayende\Downloads\train-00001-of-00004-f4a4f5540ade14b4.parquet",
                            @"C:\Users\ayende\Downloads\train-00002-of-00004-ff770df3ab420d14.parquet",
                            @"C:\Users\ayende\Downloads\train-00003-of-00004-85b3dbbc960e92ec.parquet",
                        }
                            )
                {
                    await foreach (var (wikiId, vector) in YieldVectors(file))
                    {
                        if (expected.TryAdd(wikiId, vector))
                            continue;
                        registration.Register(wikiId * 100, MemoryMarshal.Cast<float, byte>(vector));
                    }
                }
            }

            txw.Commit();
        }

        using (var txr = Env.ReadTransaction())
        {
            // Span<byte> vector = MemoryMarshal.Cast<float, byte>(data[21500][0]);
            // Hnsw.RenderAndShow(txr.LowLevelTransaction, id, vector);
            long[] matches = new long[numberOfCandidates];
            var found = 0;
            foreach(var (wikiId, vector) in expected)
            {
                using var nearest = Hnsw.ApproximateNearest(txr.LowLevelTransaction, id, 32,
                    MemoryMarshal.Cast<float, byte>(vector));

                // we have to read 4 items, because this is *approximate*
                var read = nearest.Fill(matches);
                Assert.Equal(numberOfCandidates, read);
                if (matches.Contains(wikiId * 100))
                {
                    found++;
                }
            }
            Assert.Equal(expected.Count, found);
        }
    }
}
