﻿using Raven.Server.Indexing.Corax;
using Raven.Server.Indexing.Corax.Analyzers;
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

        public void Run()
        {
            Benchmark.Time(nameof(InsertWikipediaFromMemory), sw => InsertWikipediaFromMemory(sw), this, delete: true);
            Benchmark.Time(nameof(InsertWikipediaFromDisk), sw => InsertWikipediaFromDisk(sw), this, delete: true);            
        }

        public void InsertWikipediaFromDisk(Stopwatch sw)
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

                sw.Stop();
            }
        }

        public void InsertWikipediaFromMemory(Stopwatch sw)
        {
            Documents = 0;

            // You should download the wikipedia text only version provided at http://kopiwiki.dsd.sztaki.hu/ to play around with this.
            var loader = new WikipediaLoader(new DirectoryInfo(Configuration.WikipediaDir));

            using (var _fullTextIndex = new FullTextIndex(StorageEnvironmentOptions.ForPath(_path), new DefaultAnalyzer()))
            {
                var documents = loader.LoadAsDocuments().Take(1000).ToList();

                sw.Start();

                var indexer = _fullTextIndex.CreateIndexer();

                foreach (var doc in documents)
                {
                    indexer.NewEntry(doc.Item2, doc.Item1);
                    Documents++;
                }

                sw.Stop();
            }
        }

        public void Clean()
        {
            Benchmark.DeleteDirectory(_path);
        }
    }
}
