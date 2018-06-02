using System;
using System.IO;
using System.Threading.Tasks;
using FastTests.Server.Documents.Queries.Parser;
using Newtonsoft.Json;
using Raven.Server.ServerWide.Context;
using SlowTests.Client;
using SlowTests.Issues;
using SlowTests.MailingList;
using Voron;

namespace Tryouts
{
    public static class Program
    {
        public static void Main(string[] args)
        {
            //Directory.Delete("mu", true);
            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath("mu")))
            using(var pool = new TransactionContextPool(env))
            {
                var r = new IndexReader(pool);
                foreach (var item in r.Query("Classification", "Prints"))
                {
                    Console.WriteLine(item);
                }
                //var builder = new IndexBuilder(pool);

                //using (builder.BeginIndexing())
                //{
                //    var serializer = new JsonSerializer();
                //    var items = 0;
                //    foreach (var item in Directory.GetFiles(@"F:\collection\objects\", "*.json", SearchOption.AllDirectories))
                //    {
                //        dynamic obj = serializer.Deserialize(new JsonTextReader(new StreamReader(item)));
                //        if (obj == null)
                //            continue;

                //        builder.NewEntry((string)obj.id);
                //        builder.Term("Name", (string)obj.object_name);
                //        builder.Term("Classification", ((string)obj.classification)?.Trim());
                //        builder.Term("Medium", (string)obj.medium);
                //        builder.FinishEntry();
                //        if ((items++ % 10_000) == 0)
                //        {
                //            builder.FlushState();
                //            Console.WriteLine(items);
                //        }
                //    }

                //    builder.CompleteIndexing();
                //}

            }

            Console.WriteLine("+");
        }
    }
}
