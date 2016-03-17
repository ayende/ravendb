using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Corax.Benchmark
{
    public class LuceneBenchmark : IBenchmark
    {
        private string _path = Configuration.Path + ".lucene";

        public int Documents { get; private set; }
        public int SizeInKb { get; private set; }

        public void Run()
        {
            Benchmark.Time("InsertWikipedia", sw => InsertWikipedia(sw), this, delete: true);
        }

        public void InsertWikipedia(Stopwatch sw)
        {
            // You should download the wikipedia text only version provided at http://kopiwiki.dsd.sztaki.hu/ to play around with this.
            var loader = new WikipediaLoader(new DirectoryInfo(Configuration.WikipediaDir));



        }

        public void Clean()
        {
            Benchmark.DeleteDirectory(_path);
        }
    }
}
