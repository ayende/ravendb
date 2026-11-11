using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Voron;
using Voron.Data.Graphs;

// Builds the same vectors into an HNSW and a Centroids index at the Voron level and compares build time, size on disk,
// pages written per batch, query latency and recall@10 against exact neighbors.
// usage: VectorIndexComparison <dir with X.npy, Q.npy, truth.npy> [hnsw|centroids|both] [batch size] [vector count]
var dir = args.Length > 0 ? args[0] : "/work/ravendb/vecexp";
var which = args.Length > 1 ? args[1] : "both";
var batchSize = args.Length > 2 ? int.Parse(args[2]) : 1000;
var vectors = Npy.LoadFloats(Path.Combine(dir, "X.npy"), out var count, out var dims);
if (args.Length > 3)
    count = Math.Min(count, int.Parse(args[3]));
var queries = Npy.LoadFloats(Path.Combine(dir, "Q.npy"), out var queryCount, out _);
var truth = Npy.LoadLongs(Path.Combine(dir, "truth.npy"), out _, out var truthWidth);
var candidatesToTest = new[] { 16, 32, 64, 128, 256 };

if (count < Npy.Rows(Path.Combine(dir, "X.npy")))
    truth = ExactTruth(vectors, count, dims, queries, queryCount, out truthWidth);

Console.WriteLine($"vectors={count:N0} dims={dims} queries={queryCount} batch={batchSize} cores={Environment.ProcessorCount}");

if (which is "hnsw" or "both")
    Run("hnsw", (llt, name) => Hnsw.Create(llt, name, dims * sizeof(float), 16, 100, VectorEmbeddingType.Single),
        (llt, name) => Hnsw.RegistrationFor(llt, name, new Random(454)),
        (llt, name, q, k) => Hnsw.ApproximateNearest(llt, name, k, q, 0f));

if (which is "centroids" or "both")
    Run("centroids", (llt, name) => Centroids.Create(llt, name, dims * sizeof(float), VectorEmbeddingType.Single),
        (llt, name) => Centroids.RegistrationFor(llt, name),
        (llt, name, q, k) => Centroids.Search(llt, name, q, k, 0f, Centroids.SearchMode.Approximate));

void Run<TRetriever>(string strategy, Action<Voron.Impl.LowLevelTransaction, string> create, Func<Voron.Impl.LowLevelTransaction, string, IVectorIndexer> registration,
    Func<Voron.Impl.LowLevelTransaction, Slice, Memory<byte>, int, TRetriever> search) where TRetriever : struct, IDisposable
{
    var path = Path.Combine(Path.GetTempPath(), "vector-index-comparison-" + strategy);
    if (Directory.Exists(path))
        Directory.Delete(path, true);
    using var env = new StorageEnvironment(StorageEnvironmentOptions.ForPathForTests(path));
    using (var tx = env.WriteTransaction())
    {
        create(tx.LowLevelTransaction, "f");
        tx.Commit();
    }

    long usedBefore = UsedBytes(env);
    long dirtyPages = 0;
    var sw = Stopwatch.StartNew();
    for (int start = 0; start < count; start += batchSize)
    {
        var n = Math.Min(batchSize, count - start);
        using var tx = env.WriteTransaction();
        using (var reg = registration(tx.LowLevelTransaction, "f"))
        {
            for (int i = 0; i < n; i++)
            {
                var v = new ReadOnlySpan<float>(vectors, (start + i) * dims, dims);
                reg.Register(start + i + 1, MemoryMarshal.AsBytes(v));
            }

            reg.Commit(CancellationToken.None);
        }

        dirtyPages += tx.LowLevelTransaction.NumberOfModifiedPages;
        tx.Commit();
    }

    var buildTime = sw.Elapsed;
    var used = UsedBytes(env) - usedBefore;
    Console.WriteLine($"[{strategy}] build {buildTime.TotalSeconds:F1}s ({count / buildTime.TotalSeconds:N0} vectors/s), size {used / 1024.0 / 1024:F1} MB ({used / (double)count:F0} B/vector), " +
                      $"dirty pages {dirtyPages / (double)count:F2} per vector");

    if (strategy == "centroids")
    {
        using var tx = env.ReadTransaction();
        Centroids.TryReadOptions(tx.LowLevelTransaction, Slice.From(tx.Allocator, "f", out var n) is var _ ? n : default, out var options, out _);
        Console.WriteLine($"[{strategy}] partitions {options.NumberOfPartitions:N0}, splits {options.NumberOfSplits:N0}, merges {options.NumberOfMerges:N0}, " +
                          $"reassigned {options.NumberOfReassignedVectors:N0}, vectors {options.CountOfVectors:N0}");
    }

    var matches = new long[1024];
    var distances = new float[1024];
    foreach (var k in candidatesToTest)
    {
        using var tx = env.ReadTransaction();
        Slice.From(tx.Allocator, "f", out var name);
        var latencies = new double[queryCount];
        double recall = 0;
        for (int qi = 0; qi < queryCount; qi++)
        {
            var q = new float[dims];
            Array.Copy(queries, qi * dims, q, 0, dims);
            var bytes = MemoryMarshal.AsBytes(q.AsSpan()).ToArray();
            var qsw = Stopwatch.StartNew();
            var found = new List<(long Id, float Distance)>();
            var retriever = search(tx.LowLevelTransaction, name, bytes, k);
            try
            {
                while (true)
                {
                    var read = Fill(ref retriever, matches, distances);
                    if (read == 0)
                        break;
                    for (int i = 0; i < read; i++)
                        found.Add((matches[i], distances[i]));
                }
            }
            finally
            {
                retriever.Dispose();
            }

            latencies[qi] = qsw.Elapsed.TotalMilliseconds;
            found.Sort((a, b) => a.Distance.CompareTo(b.Distance));
            var top = new HashSet<long>();
            foreach (var (id, _) in found)
            {
                if (top.Count == 10)
                    break;
                top.Add(id - 1);
            }

            int hits = 0;
            for (int t = 0; t < 10; t++)
                if (top.Contains(truth[qi * truthWidth + t]))
                    hits++;
            recall += hits / 10.0;
        }

        Array.Sort(latencies);
        Console.WriteLine($"[{strategy}] numberOfCandidates={k,4}: recall@10 {recall / queryCount:F3}  latency avg {latencies.Average():F3} ms, p50 {latencies[queryCount / 2]:F3} ms, p99 {latencies[(int)(queryCount * 0.99)]:F3} ms");
    }
}

static int Fill<TRetriever>(ref TRetriever retriever, long[] matches, float[] distances) where TRetriever : struct, IDisposable
{
    if (typeof(TRetriever) == typeof(Hnsw.VectorSearchRetriever))
        return Unsafe.As<TRetriever, Hnsw.VectorSearchRetriever>(ref retriever).Fill(matches, distances, null);
    return Unsafe.As<TRetriever, Centroids.VectorSearchRetriever>(ref retriever).Fill(matches, distances, null);
}

static long UsedBytes(StorageEnvironment env)
{
    using var tx = env.ReadTransaction();
    return env.GenerateReport(tx).DataFile.UsedSpaceInBytes;
}

static long[] ExactTruth(float[] vectors, int count, int dims, float[] queries, int queryCount, out int width)
{
    width = 10;
    var result = new long[queryCount * width];
    var norms = new float[count];
    for (int i = 0; i < count; i++)
        norms[i] = System.Numerics.Tensors.TensorPrimitives.Norm(new ReadOnlySpan<float>(vectors, i * dims, dims));
    Parallel.For(0, queryCount, qi =>
    {
        var q = new ReadOnlySpan<float>(queries, qi * dims, dims);
        var scores = new float[count];
        var ids = new long[count];
        for (int i = 0; i < count; i++)
        {
            scores[i] = -System.Numerics.Tensors.TensorPrimitives.Dot(q, new ReadOnlySpan<float>(vectors, i * dims, dims)) / norms[i];
            ids[i] = i;
        }

        Array.Sort(scores, ids);
        Array.Copy(ids, 0, result, qi * 10, 10);
    });
    return result;
}

static class Npy
{
    public static int Rows(string file)
    {
        Header(File.ReadAllBytes(file), out var rows, out _, out _);
        return rows;
    }

    public static float[] LoadFloats(string file, out int rows, out int cols)
    {
        var bytes = File.ReadAllBytes(file);
        var offset = Header(bytes, out rows, out cols, out _);
        var data = new float[rows * cols];
        Buffer.BlockCopy(bytes, offset, data, 0, data.Length * 4);
        return data;
    }

    public static long[] LoadLongs(string file, out int rows, out int cols)
    {
        var bytes = File.ReadAllBytes(file);
        var offset = Header(bytes, out rows, out cols, out _);
        var data = new long[rows * cols];
        Buffer.BlockCopy(bytes, offset, data, 0, data.Length * 8);
        return data;
    }

    private static int Header(byte[] bytes, out int rows, out int cols, out string descr)
    {
        int hlen = BitConverter.ToUInt16(bytes, 8);
        var header = System.Text.Encoding.ASCII.GetString(bytes, 10, hlen);
        descr = header;
        var shape = header[(header.IndexOf("'shape': (", StringComparison.Ordinal) + 10)..];
        var parts = shape[..shape.IndexOf(')')].Split(',', StringSplitOptions.RemoveEmptyEntries);
        rows = int.Parse(parts[0]);
        cols = parts.Length > 1 ? int.Parse(parts[1]) : 1;
        return 10 + hlen;
    }
}
