using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks.Dataflow;
using System.Xml;
using Lucene.Net.Analysis;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Tryouts.Corax;
using Voron;
using Directory = System.IO.Directory;

namespace Tryouts
{
    public static class Program
    {
        public enum ActionType
        {
            Term,
            NewEntry,
            FinishEntry,
            RestartTx
        }

        private static BlockingCollection<(string field, string term, ActionType action)> _termActionQueue = new BlockingCollection<(string field, string term, ActionType action)>();
        private const string WikiArticlesDumpFile = @"C:\Users\Michael.HRHINOS\Downloads\wiki_articles\articles_medium.xml";
        private const int PageCountLimit = 100000;

        public static void Main(string[] args)
        {
            Console.WriteLine("Press any key to start...");
            Console.ReadKey();
            ReadXMLAndIndex("Corax", CoraxIndexTerms);
            //ReadXMLAndIndex("Lucene", LuceneIndexTerms);
            Console.ReadKey();
        }

        private static void ReadXMLAndIndex(string name, ThreadStart indexingMethod)
        {
            Console.WriteLine($"== Start indexing [{name}] ==");
            using (var fileStream = File.Open(WikiArticlesDumpFile, FileMode.Open))
            using (var xmlReader = XmlReader.Create(fileStream))
            {
                var propertyName = string.Empty;
                int total = 0;
                var indexingThread = new Thread(indexingMethod);
                var pageCount = 0;
                while (xmlReader.Read() && pageCount < PageCountLimit)
                {
                    if (xmlReader.Name == "page")
                    {
                        switch (xmlReader.NodeType)
                        {
                            case XmlNodeType.Element:
                                _termActionQueue.Add((string.Empty, string.Empty, ActionType.NewEntry));
                                break;
                            case XmlNodeType.EndElement:
                                _termActionQueue.Add((string.Empty, string.Empty, ActionType.FinishEntry));
                                if (total++ % 10000 == 0)
                                    _termActionQueue.Add((string.Empty, string.Empty, ActionType.RestartTx));
                                pageCount++;
                                break;
                        }
                    }
                    else
                        switch (xmlReader.NodeType)
                        {
                            case XmlNodeType.Element:
                                propertyName = xmlReader.Name;
                                break;
                            case XmlNodeType.Text:
                                _termActionQueue.Add((propertyName,
                                    string.IsNullOrWhiteSpace(xmlReader.Value) ?
                                        xmlReader.ReadString() : xmlReader.Value,
                                    ActionType.Term));
                                break;
                        }
                }

                Console.WriteLine($"Finished reading XML, page count is {pageCount}. Waiting for indexing to begin...");
                _termActionQueue.CompleteAdding();
                indexingThread.Start();
                indexingThread.Join();
                Console.WriteLine($"== Finished indexing [{name}] ==");
            }
            _termActionQueue.Dispose();
            _termActionQueue = new BlockingCollection<(string field, string term, ActionType action)>();
        }

        private static void LuceneIndexTerms()
        {
            var path = new FileInfo(WikiArticlesDumpFile).Directory?.FullName;
            path = Path.Combine(path, "Lucene");
            if (Directory.Exists(path))
                Directory.Delete(path, true);

            var sp = Stopwatch.StartNew();
            long terms = 0;
            long entries = 0;
            long pages = 0;
            var directory = new Lucene.Net.Store.MMapDirectory(new DirectoryInfo(path));
            var writer = new IndexWriter(directory, new KeywordAnalyzer(), IndexWriter.MaxFieldLength.UNLIMITED, null);
            var currentDocument = new Document();
            int emptyTakeTries = 0;
            while (!_termActionQueue.IsCompleted)
            {
                (string field, string term, ActionType action) termData;
                try
                {
                    termData = _termActionQueue.Take();
                }
                catch
                {
                    break;
                }
                switch (termData.action)
                {
                    case ActionType.Term:
                        var field = new Field(termData.field, termData.term, Field.Store.NO, Field.Index.NOT_ANALYZED);
                        currentDocument.Add(field);
                        terms++;
                        break;
                    case ActionType.NewEntry:
                        break;
                    case ActionType.FinishEntry:
                        writer.AddDocument(currentDocument, null);
                        currentDocument = new Document();
                        entries++;
                        pages++;
                        break;
                    case ActionType.RestartTx:
                        writer.Dispose(true);
                        writer = new IndexWriter(directory, new KeywordAnalyzer(), IndexWriter.MaxFieldLength.UNLIMITED, null);
                        break;
                }
            }

            Console.WriteLine($"Time ellapsed: {sp.Elapsed}");
            Console.WriteLine($"Total pages: {pages}");
            Console.WriteLine($"Total terms: {terms}");
            Console.WriteLine($"Total allocations: {new Size(GC.GetAllocatedBytesForCurrentThread(), SizeUnit.Bytes)}");
        }

        private static void CoraxIndexTerms()
        {
            var path = new FileInfo(WikiArticlesDumpFile).Directory?.FullName;
            path = Path.Combine(path, "Corax");
            if (Directory.Exists(path))
                Directory.Delete(path, true);

            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(path)))
            using (var pool = new TransactionContextPool(env))
            {
                var builder = new IndexBuilder(pool);
                var sp = Stopwatch.StartNew();
                var tx = builder.BeginIndexing();
                long terms = 0;
                long entries = 0;
                long pages = 0;
                try
                {
                    while (_termActionQueue.IsCompleted == false)
                    {
                        (string field, string term, ActionType action) termData = _termActionQueue.Take();

                        switch (termData.action)
                        {
                            case ActionType.Term:
                                if (termData.term.Length > 255)
                                {
                                    termData.term = termData.term.Substring(0, 255);
                                }
                                builder.Term($"fields/{termData.field}", termData.term);
                                terms++;
                                break;
                            case ActionType.NewEntry:
                                builder.NewEntry($"entries/{entries++}");
                                break;
                            case ActionType.FinishEntry:
                                builder.FinishEntry();
                                pages++;
                                break;
                            case ActionType.RestartTx:
                                builder.CompleteIndexing();
                                tx.Dispose();
                                tx = builder.BeginIndexing();
                                break;
                        }
                    }
                }
                finally
                {
                    builder.CompleteIndexing();
                    tx.Dispose();
                }

                Console.WriteLine($"Time ellapsed: {sp.Elapsed}");
                Console.WriteLine($"Total entries: {pages}");
                Console.WriteLine($"Total terms: {terms}");
                Console.WriteLine($"Total allocations: {new Size(GC.GetAllocatedBytesForCurrentThread(), SizeUnit.Bytes)}");
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
