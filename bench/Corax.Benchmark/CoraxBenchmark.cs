using Raven.Server.Indexing.Corax;
using Raven.Server.Indexing.Corax.Analyzers;
using Raven.Server.Indexing.Corax.Queries;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Voron;

namespace Corax.Benchmark
{
    public class CoraxBenchmark : IBenchmark
    {
        private string _path = Configuration.Path + ".corax";

        public int Documents { get; private set; }
        public int SizeInKb { get; private set; }

        public string[] searchTerms = { "prospered", "foreign", "operation", "ministry", "military", "substratum",
                                        "official", "forces", "birthday", "leaked", "distribute", "Wikipedia",
                                        "predictable", "river", "essentially" };

        public void Run()
        {
            Console.WriteLine("Corax Benchmark");
            Console.WriteLine();

            Benchmark.Time(nameof(Insert), sw => Insert(sw), this, delete: false);
            Benchmark.Time(nameof(QuerySimple), sw => QuerySimple(sw), this, delete: false);

            Console.WriteLine();
        }

        public void QuerySimple( Stopwatch sw )
        {
            try
            {
                using (var _fullTextIndex = new FullTextIndex(StorageEnvironmentOptions.ForPath(_path), new DefaultAnalyzer()))
                {
                    sw.Start();

                    var searcher = _fullTextIndex.CreateSearcher();

                    foreach (var term in searchTerms)
                    {
                        var results = searcher.Query(new QueryDefinition { Query = new TermQuery("Text", term) });
                        Documents += results.Length;
                    }

                    sw.Stop();
                }
            }
            catch (Exception e)
            {
                throw;
            }
        }

        public void Insert(Stopwatch sw)
        {
            Documents = 0;

            // You should download the wikipedia text only version provided at http://kopiwiki.dsd.sztaki.hu/ to play around with this.
            var loader = new WikipediaLoader(new DirectoryInfo(Configuration.WikipediaDir));

            using (var _fullTextIndex = new FullTextIndex(StorageEnvironmentOptions.ForPath(_path), new DefaultAnalyzer()))
            {
                sw.Start();

                var indexer = _fullTextIndex.CreateIndexer();

                foreach (var doc in loader.LoadAsDocuments())
                {
                    indexer.NewEntry(doc.Item2, doc.Item1);
                    Documents++;
                }

                indexer.Flush();

                sw.Stop();
            }
        }    

        public void Clean()
        {
            Benchmark.DeleteDirectory(_path);
        }
    }
}
