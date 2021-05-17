using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Corax;
using FastTests.Sparrow;
using FastTests.Voron;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Compression;
using Tests.Infrastructure;
using Voron;
using Voron.Data.CompactTrees;

namespace Tryouts
{
    public static class Program
    {
        static Program()
        {
            //XunitLogging.RedirectStreams = false;
        }

        public static async Task Main(string[] args)
        {
            new CompactTreeTests(new ConsoleTestOutputHelper()).CanStoreLargeNumberOfItemsInRandomlyOrder();


            // var tests = new Encoder3GramTests();
            // tests.VerifyOrderPreservation();

            //CoraxEnron.Index(true, "Z:\\corax");
            //LuceneEnron.IndexInLucene(true);

            //CoraxReddit.Index(true, "Z:\\corax-compressed-leaf");
            //LuceneReddit.Index(true, "Z:\\corax");
            //CoraxReddit.SearchExact("Z:\\corax");
            //LuceneReddit.SearchExact("Z:\\corax");

            //using (var searcher = new IndexSearcher(env))
            //{
            //    QueryOp q = new BinaryQuery(
            //        new QueryOp[] {new TermQuery("Dogs", "Arava"), new TermQuery("Gender", "Male"),},
            //        BitmapOp.And
            //    );
            //    using var ctx = JsonOperationContext.ShortTermSingleUse();
            //    var results = searcher.Query(ctx, q, 2, "Name");

            //    foreach (object result in results)
            //    {
            //        Console.WriteLine(result);
            //    }
            //}


            //var options = StorageEnvironmentOptions.ForPath(Path.Join("Z:\\corax-deb", CoraxEnron.DirectoryEnron));
            //var env = new StorageEnvironment(options);
            //var transaction = env.ReadTransaction();

            //var tree = transaction.CreateTree("To");
            //using (var iterator = tree.Iterate(true))
            //{
            //    Slice.From(transaction.Allocator, "margaret.hall@enron.com".AsSpan(), out var prefix);
            //    if (!iterator.Seek(prefix))
            //        Console.WriteLine("Not found");

            //    using var file = File.CreateText("page.txt");
            //    for (int i = 0; i < 700; i++)
            //    {
            //        iterator.MoveNext();

            //        var key = iterator.CurrentKey.ToString();
            //        if (key.Contains(','))
            //            continue;

            //        file.WriteLine(key);
            //    }
            //}

            Console.WriteLine("Done");
            //Console.ReadLine();
        }
    }
}
