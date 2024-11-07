using System.Diagnostics;
using System.Runtime.InteropServices;
using Parquet;
using Voron;
using Voron.Data.Graphs;

var options = StorageEnvironmentOptions.ForPathForTests("vectors");
options.ManualFlushing = true;
using var env = new StorageEnvironment(options);

// long id = 16416;
// using (var txr = env.ReadTransaction())
// {
//     var options = Hnsw.ReadOptions(txr.LowLevelTransaction, id);
//     Console.WriteLine(options.CountOfVectors);
// }
var sp = Stopwatch.StartNew();
await ImportData(env, @"C:\Users\ayende\Downloads");
Console.WriteLine(sp.Elapsed);

static async Task ImportData(StorageEnvironment env, string basePath)
{
    long id;
    using (var txw = env.WriteTransaction())
    {
        id = Hnsw.Create(txw.LowLevelTransaction, 768 * 4, 16, 200);
        Console.WriteLine(id);
        txw.Commit();
    }

    foreach (var file in (string[])[
                 "train-00000-of-00004-1a1932c9ca1c7152.parquet", 
                 "train-00001-of-00004-f4a4f5540ade14b4.parquet",
                 "train-00002-of-00004-ff770df3ab420d14.parquet", 
                 "train-00003-of-00004-85b3dbbc960e92ec.parquet"
             ])
    {
        Console.WriteLine(file);
        await foreach (var (ids, vectors) in YieldVectors(Path.Combine(basePath, file)))
        {
            var batch = Stopwatch.StartNew();
            {
                using (var txw = env.WriteTransaction())
                {
                    using (var registration = Hnsw.RegistrationFor(txw.LowLevelTransaction, id))
                    {
                        for (int i = 0; i < ids.Length; i++)
                        {
                            var vector = new Memory<float>(vectors, i * 768, 768);
                            registration.Register(ids[i] * 100, MemoryMarshal.Cast<float, byte>(vector.Span));
                        }
                    }
                    txw.Commit();
                }
            }
            
            env.FlushLogToDataFile();
            
            Console.WriteLine($" * {ids.Length:N0} - {batch.Elapsed}");
        }

    }
}


static async IAsyncEnumerable<(int[], float[])> YieldVectors(string filePath)
{
    var file = await ParquetReader.CreateAsync(filePath);
    var schema = file.Schema;
    for (int i = 0; i < file.RowGroupCount; i++)
    {
        var reader = file.OpenRowGroupReader(i);
        var wikiId = await reader.ReadColumnAsync(schema.DataFields[4]);
        var vectors = await reader.ReadColumnAsync(schema.DataFields[8]);
        var wikiIds = (int[])wikiId.DefinedData;
        var vectorsArr = (float[])vectors.DefinedData;
        yield return (wikiIds, vectorsArr);
    }
}
