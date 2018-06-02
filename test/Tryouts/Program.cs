using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using FastTests.Server.Documents.Queries.Parser;
using Lucene.Net.Analysis;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Store;
using Newtonsoft.Json;
using Raven.Server.ServerWide.Context;
using SlowTests.Client;
using SlowTests.Issues;
using SlowTests.MailingList;
using Voron;
using static Lucene.Net.Index.IndexWriter;

namespace Tryouts
{
    public static class Program
    {
        public static void Main(string[] args)
        {
            if (System.IO.Directory.Exists("mu"))
                System.IO.Directory.Delete("mu", true);
            var sp = Stopwatch.StartNew();
            var items = 0;
            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath("mu")))
            using (var pool = new TransactionContextPool(env))
            {
                var builder = new IndexBuilder(pool);

                using (builder.BeginIndexing())
                {
                    var serializer = new JsonSerializer();
                    foreach (var item in System.IO.Directory.GetFiles(@"F:\collection\objects\", "*.json", SearchOption.AllDirectories))
                    {
                        dynamic obj = serializer.Deserialize(new JsonTextReader(new StreamReader(item)));
                        if (obj == null)
                            continue;

                        builder.NewEntry((string)obj.id);
                        builder.Term("Name", (string)obj.object_name);
                        builder.Term("Classification", ((string)obj.classification)?.Trim());
                        builder.Term("Medium", (string)obj.medium);
                        builder.FinishEntry();
                        //if ((items++ % 10_000) == 0)
                        //{
                        //    builder.FlushState();
                        //}
                    }

                    builder.CompleteIndexing();
                }
                Console.WriteLine(JsonConvert.SerializeObject(env.Stats(), Formatting.Indented));
            }
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
            Console.WriteLine(sp.Elapsed);
            Console.WriteLine(items);

            Console.WriteLine("+");
        }
    }
}
