using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices.ComTypes;
using System.Threading.Tasks;
using FastTests.Server.Documents.Queries.Parser;
using GeoAPI.Geometries;
using Lucene.Net.Analysis;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Store;
using Newtonsoft.Json;
using Raven.Server.ServerWide.Context;
using SlowTests.Client;
using SlowTests.Issues;
using SlowTests.MailingList;
using Sparrow.Binary;
using Sparrow.Json;
using Tryouts.Corax;
using Tryouts.Corax.Queries;
using Tryouts.Tests;
using Voron;
using static Lucene.Net.Index.IndexWriter;
using IndexReader = Tryouts.Corax.IndexReader;

namespace Tryouts
{
    public static class Program
    {
        public unsafe static void Main(string[] args)
        {
            var a = Bits.LeadingZeroes((ulong)ushort.MaxValue);
            new BitmapTests().UsingBitmapsWithCompression();

            //var values = File.ReadAllText(@"C:\Users\ayende\Downloads\weather_sept_85_srt.csv39.txt").Split(',').Select(ulong.Parse).ToList();
            //values.Sort();
            //using (var ctx = JsonOperationContext.ShortTermSingleUse())
            //using(var writer = ctx.GetStream(8192))
            //{
            //    using (ctx.GetManagedBuffer(out var buffer))
            //    {
            //        var builder = new PackedBitmapBuilder(writer, buffer);
            //        foreach (var value in values)
            //        {
            //            builder.Set(value);
            //        }
            //        builder.Complete(out var buf, out var size);
            //        Console.WriteLine(writer.SizeInBytes);
            //        Console.WriteLine(values .Count);

            //        var reader = new PackedBitmapReader(buf, size);
            //        int index = 0;
            //        while (reader.MoveNext())
            //        {
            //            if (index == 65535) {
            //                global::System.Console.WriteLine("a");
            //            }

            //                if (reader.Current != values[index])
            //            {
            //                Console.WriteLine("Error on " + index+" expected " + values[index] + " but was " + reader.Current);
            //                return;
            //            }

            //            index++;
            //        }

            //        if (index != values.Count)
            //        {
            //            Console.WriteLine("missing values, expected: " + values.Count + " but was " +index );
            //        }
            //    }
            //}

            //            using (var env = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnly()))
            //            using (var pool = new TransactionContextPool(env))
            //            {
            //                var builder = new IndexBuilder(pool);
            //
            //                using (builder.BeginIndexing())
            //                {
            //                    //builder.DeleteEntry("users/1");
            //                    builder.NewEntry("users/1");
            //                    builder.Term("Name", "Oren");
            //                    builder.Term("Lang", "C#");
            //                    builder.Term("Lang", "Hebrew");
            //                    builder.Term("Lang", "Bulgerian");
            //                    builder.FinishEntry();
            //
            //                    builder.NewEntry("dogs/1");
            //                    builder.Term("Name", "Arava");
            //                    builder.Term("Lang", "Bark");
            //                    builder.FinishEntry();
            //
            //                    builder.CompleteIndexing();
            //                }
            //
            //
            //                var reader = new IndexReader(pool);
            //                using (pool.AllocateOperationContext(out TransactionOperationContext ctx))
            //                using (ctx.OpenReadTransaction())
            //                {
            //                    var query = new OrQuery(ctx, reader,
            //                        new TermQuery(ctx, reader, "Lang", "C#"),
            //                        new TermQuery(ctx, reader, "Name", "Arava")
            //                        );
            //                    foreach (var item in reader.Query(query))
            //                    {
            //                        Console.WriteLine(string.Join(", ", reader.GetTerms(ctx, item.Id, "Name")));
            //
            //                        Console.WriteLine(item);
            //                        Console.WriteLine("----");
            //                    }
            //
            //                }
            //
            //                Console.WriteLine("+============+");
            //            }
            //
            //var fsDir = FSDirectory.Open("mu");
            ////using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath("mu")))
            ////using (var pool = new TransactionContextPool(env))
            //{
            //    var writer = new IndexWriter(fsDir, new KeywordAnalyzer(), MaxFieldLength.UNLIMITED, null);
            //    var doc = new Document();
            //    var idFld = new Field("id()", "", Field.Store.YES, Field.Index.NOT_ANALYZED);
            //    var nameFld = new Field("Name", "", Field.Store.NO, Field.Index.NOT_ANALYZED);
            //    var classification = new Field("Classification", "", Field.Store.NO, Field.Index.NOT_ANALYZED);
            //    var medium = new Field("Medium", "", Field.Store.NO, Field.Index.NOT_ANALYZED);
            //    doc.Add(idFld);
            //    doc.Add(nameFld);
            //    doc.Add(classification);
            //    doc.Add(medium);
            //    //using (builder.BeginIndexing())
            //    {
            //        var serializer = new JsonSerializer();
            //        foreach (var item in System.IO.Directory.GetFiles(@"F:\collection\objects\", "*.json", SearchOption.AllDirectories))
            //        {
            //            dynamic obj = serializer.Deserialize(new JsonTextReader(new StreamReader(item)));
            //            if (obj == null)
            //                continue;
            //            string str = (string)obj.id;
            //            if (str != null)
            //                idFld.SetValue(str);
            //            str = (string)obj.object_name;
            //            if (str != null)
            //                nameFld.SetValue(str);
            //            str = ((string)obj.classification)?.Trim();
            //            if (str != null)
            //                classification.SetValue(str);
            //            str = (string)obj.medium;
            //            if (str != null)
            //                medium.SetValue(str);
            //            writer.AddDocument(doc, null);
            //            if ((items++ % 10_000) == 0)
            //            {
            //                writer.Flush(false, true, false, null);
            //            }
            //        }

            //    }
            //    writer.Close(true);
            //}
        }
    }
}
