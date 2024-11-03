using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Numerics.Tensors;
using System.Runtime.InteropServices;
using Jint.Native.Json;
using Newtonsoft.Json;
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
            Assert.Equal(3, options.NumberOfNeighbors);
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
            Assert.Equal(3, options.NumberOfNeighbors);
            Assert.Equal(2, options.CountOfVectors);
        }

        using (var txr = Env.ReadTransaction())
        {
            Span<long> matches = stackalloc long[8];
            using var nearest = Hnsw.Nearest(txr.LowLevelTransaction, id,
                numberOfCandidates: 32,
                MemoryMarshal.Cast<float, byte>(v3));
            int read = nearest.Fill(matches);
            Assert.Equal(3, read);
            Assert.Equal(8, matches[0]);
            Assert.Equal(4, matches[1]);
            Assert.Equal(12, matches[2]);
        }
    }


    [RavenFact(RavenTestCategory.Voron)]
    public void MoreNodesThanNeighbors()
    {
        using var reader = new GZipStream(File.OpenRead(@"F:\ravendb-7.0\test\FastTests\Data\vectors.json.gz"), CompressionMode.Decompress);
        var data = new JsonSerializer().Deserialize<Dictionary<int, List<float[]>>>(new JsonTextReader(new StreamReader(reader)));

        
        long id;
        int take = 5000;

        using (var txw = Env.WriteTransaction())
        {
            id = Hnsw.Create(txw.LowLevelTransaction, 768 * 4, 8, 16);

            using (var registration = Hnsw.RegistrationFor(txw.LowLevelTransaction, id))
            {
                registration.Random = new Random(1337);

                foreach (var (wikiId, vectors) in data.Take(take))
                {
                    for (int index = 1; index < vectors.Count; index++)
                    {
                        float[] vec = vectors[index];
                        registration.Register(wikiId * 100, MemoryMarshal.Cast<float, byte>(vec));
                    }
                }
            }

            txw.Commit();
        }

        using (var txr = Env.ReadTransaction())
        {
            Hnsw.RenderAndShow(txr.LowLevelTransaction, id);
            long[] matches = new long[8];
            {
                foreach (var (wikiId, vectors) in data.Take(take))
                {
                    using var nearest = Hnsw.Nearest(txr.LowLevelTransaction, id, 8,
                        MemoryMarshal.Cast<float, byte>(vectors[0]));

                    // we have to read 4 items, because this is *approximate*
                    var read = nearest.Fill(matches);
                    Assert.Equal(8, read);
                    Assert.Contains(wikiId * 100, matches);

                }
            }
        }
    }
}
