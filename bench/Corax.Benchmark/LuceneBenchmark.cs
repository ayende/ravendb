using Lucene.Net.Analysis.Standard;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Store;
using Raven.Server.Indexing;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Voron;

namespace Corax.Benchmark
{
    public class LuceneBenchmark : IBenchmark
    {
        private string _path = Configuration.Path + ".lucene";

        public int Documents { get; private set; }
        public int SizeInKb { get; private set; }

        public void Run()
        {
            Console.WriteLine("Lucene Benchmark");
            Console.WriteLine();

            Benchmark.Time(nameof(Insert), sw => Insert(sw), this, delete: true);
            Benchmark.Time(nameof(InsertOnVoron), sw => InsertOnVoron(sw), this, delete: true);
            Benchmark.Time(nameof(InsertOnVoronTransactionPerAdd), sw => InsertOnVoronTransactionPerAdd(sw), this, delete: true);

            Console.WriteLine();
        }

        public void Insert(Stopwatch sw)
        {
            Documents = 0;

            // You should download the wikipedia text only version provided at http://kopiwiki.dsd.sztaki.hu/ to play around with this.
            var loader = new WikipediaLoader(new DirectoryInfo(Configuration.WikipediaDir));

            using (var luceneIndexDirectory = MMapDirectory.Open(_path))
            {
                var analyzer = new StandardAnalyzer(Lucene.Net.Util.Version.LUCENE_29);
                using (var writer = new IndexWriter(luceneIndexDirectory, analyzer, true, IndexWriter.MaxFieldLength.UNLIMITED))
                {
                    sw.Start();

                    foreach (var doc in loader.Load())
                    {
                        var id = new Field("_document_id", doc.Item1, Field.Store.YES, Field.Index.NO, Field.TermVector.NO);
                        var text = new Field("text", doc.Item2, Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.NO);

                        var d = new Document();
                        d.Add(id);
                        d.Add(text);

                        writer.AddDocument(d);

                        Documents++;
                    }

                    writer.Commit();
                    writer.Flush(true, true, true);
                }
            }

            sw.Stop();
        }

        public void InsertOnVoron(Stopwatch sw)
        {
            Documents = 0;

            // You should download the wikipedia text only version provided at http://kopiwiki.dsd.sztaki.hu/ to play around with this.
            var loader = new WikipediaLoader(new DirectoryInfo(Configuration.WikipediaDir));

            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(_path)))
            using (var luceneIndexDirectory = new LuceneVoronDirectory(env))
            using (var tx = env.WriteTransaction())
            {
                luceneIndexDirectory.SetTransaction(tx);

                var analyzer = new StandardAnalyzer(Lucene.Net.Util.Version.LUCENE_29);
                using (var writer = new IndexWriter(luceneIndexDirectory, analyzer, true, IndexWriter.MaxFieldLength.UNLIMITED))
                {
                    sw.Start();

                    foreach (var doc in loader.Load())
                    {
                        var id = new Field("_document_id", doc.Item1, Field.Store.YES, Field.Index.NO, Field.TermVector.NO);
                        var text = new Field("text", doc.Item2, Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.NO);

                        var d = new Document();
                        d.Add(id);
                        d.Add(text);

                        writer.AddDocument(d);

                        Documents++;
                    }

                    writer.Commit();
                    writer.Flush(true, true, true);
                } 

                tx.Commit();
            }

            sw.Stop();
        }

        public void InsertOnVoronTransactionPerAdd(Stopwatch sw)
        {
            Documents = 0;

            // You should download the wikipedia text only version provided at http://kopiwiki.dsd.sztaki.hu/ to play around with this.
            var loader = new WikipediaLoader(new DirectoryInfo(Configuration.WikipediaDir));

            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(_path)))
            using (var luceneIndexDirectory = new LuceneVoronDirectory(env))           
            {
                StandardAnalyzer analyzer = new StandardAnalyzer(Lucene.Net.Util.Version.LUCENE_29);

                sw.Start();

                foreach (var doc in loader.Load())
                {
                    using (var tx = env.WriteTransaction())
                    {
                        luceneIndexDirectory.SetTransaction(tx);

                        using (var writer = new IndexWriter(luceneIndexDirectory, analyzer, true, IndexWriter.MaxFieldLength.UNLIMITED))
                        {
                            var id = new Field("_document_id", doc.Item1, Field.Store.YES, Field.Index.NO, Field.TermVector.NO);
                            var text = new Field("text", doc.Item2, Field.Store.NO, Field.Index.ANALYZED, Field.TermVector.NO);

                            var d = new Document();
                            d.Add(id);
                            d.Add(text);

                            writer.AddDocument(d);

                            Documents++;

                            writer.Commit();
                            writer.Flush(true, true, true);
                        }

                        tx.Commit();
                    }
                }
            }

            sw.Stop();
        }

        public void Clean()
        {
            Benchmark.DeleteDirectory(_path);
        }
    }
}
