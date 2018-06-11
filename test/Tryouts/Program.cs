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
using Lucene.Net.Search;
using Lucene.Net.Store;
using Newtonsoft.Json;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Collectors;
using Raven.Server.ServerWide.Context;
using SlowTests.Client;
using SlowTests.Issues;
using SlowTests.MailingList;
using Sparrow;
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
            for (int i = 0; i < 1000_000; i++)
            {
                var test = new BitmapTests();
                
                if(i % 100 == 0)
                    Console.WriteLine(i);
                test.PackedBitmapBuilder_Complete_should_work();
            }

            return;
            //new BitmapTests().XorUsingBitmap();

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
            {

                using (var env = new StorageEnvironment(StorageEnvironmentOptions.CreateMemoryOnly()))
                using (var pool = new TransactionContextPool(env))
                {
                    var builder = new IndexBuilder(pool);
                    var currentAllocs = GC.GetAllocatedBytesForCurrentThread();
                    var sp = Stopwatch.StartNew();
                    for (int ix = 0; ix < 10; ix++)
                    {
                        using (builder.BeginIndexing())
                        {
                            builder.NewEntry("users/" + Guid.NewGuid());
                            builder.Term("Name", "John Doe");
                            builder.Term("Lang", "Hebrew");
                            builder.Term("Lang", "Bulgerian");
                            builder.FinishEntry();

                            //builder.DeleteEntry("users/1");
                            for (int i = 0; i < 100; i++)
                            {
                                builder.NewEntry("users/" + Guid.NewGuid());
                                builder.Term("Name", "Oren");
                                builder.Term("Lang", "C#");
                                builder.Term("Lang", "Hebrew");
                                builder.Term("Lang", "Bulgerian");
                                builder.FinishEntry();

                                builder.NewEntry("dogs/" + Guid.NewGuid());
                                builder.Term("Name", "Arava");
                                builder.Term("Lang", "Bark");
                                builder.Term("Lang", "C#");
                                builder.FinishEntry();
                            }

                            builder.CompleteIndexing();
                        }
                    }

                    Console.WriteLine("Indexing time corax: " + sp.ElapsedMilliseconds + ", allocations: " +
                        new Size((GC.GetAllocatedBytesForCurrentThread() - currentAllocs), SizeUnit.Bytes));


                    using (pool.AllocateOperationContext(out TransactionOperationContext ctx))
                    {
                        var reader = new IndexReader(pool);
                        Console.WriteLine("Starting...");
                        for (int i = 0; i < 3; i++)
                        {
                            using (ctx.OpenReadTransaction())
                            using (reader.BeginReading())
                            {
                                                    
                                if (i == 0)
                                {
                                    Console.WriteLine("'John Doe' term frequency: " + reader.GetTermFreq("John Doe"));
                                    Console.WriteLine("'Bark' term frequency: " + reader.GetTermFreq( "Bark"));
                                    Console.WriteLine("'C#' term frequency: " + reader.GetTermFreq("C#"));
                                }

                                var qt = Stopwatch.StartNew();
                                currentAllocs = GC.GetAllocatedBytesForCurrentThread();
                                //var a = reader.Query(
                                //    new AndNotQuery(ctx, reader,
                                //        new Corax.Queries.PrefixQuery(ctx, reader, "Lang", "B"),
                                //        new Corax.Queries.TermQuery(ctx, reader, "Name", "Arava")
                                //       )
                                //    ).Count();
                                var a = reader.Query(
                                     new Corax.Queries.TermQuery(reader, "Name", "Arava")
                                 ).Count();
                                Console.WriteLine(qt.ElapsedMilliseconds + " " + a+ ", allocations: " +
                                    new Size((GC.GetAllocatedBytesForCurrentThread() - currentAllocs), SizeUnit.Bytes));
                                //foreach (var item in a)
                                //{
                                //    Console.WriteLine(string.Join(", ", reader.GetTerms(ctx, item.Id, "Name")));

                                //    Console.WriteLine(item);
                                //    Console.WriteLine("----");
                                //}

                            }
                        }
                    }
                    Console.WriteLine(new Size(env.Stats().AllocatedDataFileSizeInBytes, SizeUnit.Bytes));
                    Console.WriteLine("+============+");
                }
            }

            {

                var d = new Lucene.Net.Store.RAMDirectory();
                var orenIdFld = new Field("id()", "", Field.Store.YES, Field.Index.NO);
                var aravaIdFld = new Field("id()", "", Field.Store.YES, Field.Index.NO);
                var oren = CreateLuceneDocOren(orenIdFld);
                var arava = CreateLuceneDocArava(aravaIdFld);
                var currentAllocs = GC.GetAllocatedBytesForCurrentThread();
                var sp = Stopwatch.StartNew();
                for (int ix = 0; ix < 10; ix++)
                {
                    var writer = new IndexWriter(d, new KeywordAnalyzer(), MaxFieldLength.UNLIMITED, null);
                    //builder.DeleteEntry("users/1");
                    for (int i = 0; i < 15_000; i++)
                    {
                        orenIdFld.SetValue("users/" + Guid.NewGuid());
                        aravaIdFld.SetValue("dogs/" + Guid.NewGuid());
                        writer.AddDocument(oren, null);
                        writer.AddDocument(arava, null);
                    }

                    writer.Close(true);
                }
                Console.WriteLine("Indexing time Lucene: " + sp.ElapsedMilliseconds + ", allocations: " +
                    new Size((GC.GetAllocatedBytesForCurrentThread() - currentAllocs), SizeUnit.Bytes));
                var searcher = new IndexSearcher(d, null);
                for (int i = 0; i < 3; i++)
                {
                    var qt = Stopwatch.StartNew();
                    currentAllocs = GC.GetAllocatedBytesForCurrentThread();
                //    var t = searcher.Search(new BooleanQuery
                //{
                //    {new Lucene.Net.Search.PrefixQuery(new Term("Lang", "B")), Occur.MUST },
                //    {new Lucene.Net.Search.TermQuery(new Term("Name", "Arava")), Occur.MUST_NOT },
                //}, 150, null);
                    var t = searcher.Search(new Lucene.Net.Search.TermQuery(new Term("Name", "Arava")), 150, null);
                    Console.WriteLine(qt.ElapsedMilliseconds + " " + t.TotalHits + ", allocations: " +
                                   new Size((GC.GetAllocatedBytesForCurrentThread() - currentAllocs), SizeUnit.Bytes));
                }

                Console.WriteLine(new Size(d.SizeInBytes(), SizeUnit.Bytes));
            }
        }

        private static unsafe Document CreateLuceneDocOren(Field idFld)
        {
            var oren = new Document();
            var nameFld = new Field("Name", "Oren", Field.Store.NO, Field.Index.NOT_ANALYZED);
            var lng1 = new Field("Lang", "C#", Field.Store.NO, Field.Index.NOT_ANALYZED);
            var lng2 = new Field("Lang", "Hebrew", Field.Store.NO, Field.Index.NOT_ANALYZED);
            var lng3 = new Field("Lang", "Bulgerian", Field.Store.NO, Field.Index.NOT_ANALYZED);

            oren.Add(idFld);
            oren.Add(nameFld);
            oren.Add(lng1);
            oren.Add(lng2);
            oren.Add(lng3);
            return oren;
        }

        private static unsafe Document CreateLuceneDocArava(Field idFld)
        {
            var arava = new Document();
            var nameFld = new Field("Name", "Arava", Field.Store.NO, Field.Index.NOT_ANALYZED);
            var lng1 = new Field("Lang", "Bark", Field.Store.NO, Field.Index.NOT_ANALYZED);

            arava.Add(idFld);
            arava.Add(nameFld);
            arava.Add(lng1);
            return arava;
        }
    }
}
