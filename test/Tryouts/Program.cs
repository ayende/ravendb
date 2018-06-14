using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using System.Xml;
using Lucene.Net.Analysis;
using Lucene.Net.Documents;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Collectors;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Tryouts.Corax;
using Voron;
using Directory = System.IO.Directory;
using IndexReader = Lucene.Net.Index.IndexReader;
using TermQuery = Tryouts.Corax.Queries.TermQuery;

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
        private const string WikiArticlesDumpFile = @"C:\Users\Michael.HRHINOS\Downloads\wiki_articles\articles_small.xml";
        private const int PageCountLimit = 100_000;
        private const int QueryIterationsLimit = 100_000;

        //field name -> term -> list of external Ids
        private static readonly Dictionary<string, Dictionary<string, HashSet<string>>> _termQueryData = new Dictionary<string, Dictionary<string, HashSet<string>>>();

        public static void Main(string[] args)
        {
            var coraxPath = new FileInfo(WikiArticlesDumpFile).Directory?.FullName;
            coraxPath = Path.Combine(coraxPath, "Corax");
            if (Directory.Exists(coraxPath))
                Directory.Delete(coraxPath, true);

            var lucenePath = new FileInfo(WikiArticlesDumpFile).Directory?.FullName;
            lucenePath = Path.Combine(lucenePath, "Lucene");
            if (Directory.Exists(lucenePath))
                Directory.Delete(lucenePath, true);

            ReadXMLAndProcess("Preparing query data", PrepareQueryData);

            ReadXMLAndProcess("Indexing in Corax", () => CoraxIndexTerms(coraxPath));

            var queryThread = new Thread(() => QueryCoraxBenchmark(coraxPath));
            queryThread.Start();
            queryThread.Join();

            ReadXMLAndProcess("Indexing in Lucene", () => LuceneIndexTerms(lucenePath));

            queryThread = new Thread(() => QueryLuceneBenchmark(lucenePath));
            queryThread.Start();
            queryThread.Join();

            Console.ReadKey();
        }

        private static void QueryCoraxBenchmark(string path)
        {
            Console.WriteLine($"== Starting Corax Querying ==");

            const string fieldToQuery = "username";

            if (!_termQueryData.TryGetValue(fieldToQuery, out var termToEntriesMap))
            {
                throw new InvalidOperationException("Didn't find common wiki article field name. Are you sure this is wiki articles data?");
            }

            var termToQuery = termToEntriesMap.OrderByDescending(x => x.Value.Count).FirstOrDefault().Key;

            var sw = Stopwatch.StartNew();
            using (var env = new StorageEnvironment(StorageEnvironmentOptions.ForPath(path)))
            using (var pool = new TransactionContextPool(env))
            {
                var reader = new Corax.IndexReader(pool);
                using (reader.BeginReading())
                {

                    for (int i = 0; i < QueryIterationsLimit; i++)
                    {
                        using (var results = reader.Query(new TermQuery(reader, fieldToQuery, termToQuery)).GetEnumerator())
                        {
                            int count = 0;
                            while (results.MoveNext() && count++ < 1024)
                            {
                            }
                        }

                    }
                }
            }
            sw.Stop();

            Console.WriteLine($"Time ellapsed: {sw.Elapsed}");
            Console.WriteLine($"Query/sec: {QueryIterationsLimit / sw.Elapsed.TotalSeconds:#,#}");
            Console.WriteLine($"Total allocations: {new Size(GC.GetAllocatedBytesForCurrentThread(), SizeUnit.Bytes)}");
            Console.WriteLine($"== Finished Corax Querying ==");

        }

        private static void QueryLuceneBenchmark(string path)
        {
            Console.WriteLine("== Starting Lucene Querying ==");

            const string fieldToQuery = "username";

            if (!_termQueryData.TryGetValue(fieldToQuery, out var termToEntriesMap))
            {
                throw new InvalidOperationException("Didn't find common wiki article field name. Are you sure this is wiki articles data?");
            }

            var termToQuery = termToEntriesMap.OrderByDescending(x => x.Value.Count).FirstOrDefault().Key;

            var sw = Stopwatch.StartNew();
            using (var directory = new Lucene.Net.Store.MMapDirectory(new DirectoryInfo(path)))
            using (var searcher = new IndexSearcher(directory, null))
            {
                for (int i = 0; i < QueryIterationsLimit; i++)
                {
                    var query = new Lucene.Net.Search.TermQuery(new Term(fieldToQuery, termToQuery));
                    var results = searcher.Search(query, 1024, null);
                    for (int j = 0; j < results.ScoreDocs.Length; j++)
                    {
                        var doc = searcher.IndexReader.Document(results.ScoreDocs[j].Doc, null);
                        doc.Get("id()", null);
                    }
                }
            }

            sw.Stop();

            Console.WriteLine($"Time ellapsed: {sw.Elapsed}");
            Console.WriteLine($"Query/sec: {QueryIterationsLimit / sw.Elapsed.TotalSeconds:#,#}");
            Console.WriteLine($"Total allocations: {new Size(GC.GetAllocatedBytesForCurrentThread(), SizeUnit.Bytes)}");
            Console.WriteLine($"== Finished Lucene Querying ==");
        }

        private static void ReadXMLAndProcess(string name, ThreadStart indexingMethod)
        {
            Console.WriteLine($"== Start [{name}] ==");
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

        private static void PrepareQueryData()
        {
            int pages = 0;
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

                        var externalId = $"entries/{pages}";
                        var fieldValue = termData.field;
                        if (_termQueryData.TryGetValue(fieldValue, out var termToEntryList))
                        {
                            if (termToEntryList.TryGetValue(termData.term, out var entryList))
                            {
                                entryList.Add(externalId);
                            }
                            else
                            {
                                termToEntryList.Add(termData.term, new HashSet<string> { externalId });
                            }
                        }
                        else
                        {
                            var termToEntryListNew = new Dictionary<string, HashSet<string>>
                            {
                                {
                                    termData.term, new HashSet<string>
                                    {
                                        externalId
                                    }
                                }
                            };
                            _termQueryData.Add(fieldValue, termToEntryListNew);
                        }

                        break;
                    case ActionType.FinishEntry:
                        pages++;
                        break;
                }
            }
        }

        private static void LuceneIndexTerms(string path)
        {
            var sp = Stopwatch.StartNew();
            long terms = 0;
            long entries = 0;
            long pages = 0;
            var currentDocument = new Document();
            int emptyTakeTries = 0;
            IndexWriter writer = null;
            using (var directory = new Lucene.Net.Store.MMapDirectory(new DirectoryInfo(path)))
            {
                try
                {
                    writer = new IndexWriter(directory, new KeywordAnalyzer(), IndexWriter.MaxFieldLength.UNLIMITED, null);
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
                                currentDocument.Add(new Field("id()", entries.ToString(), Field.Store.YES, Field.Index.NOT_ANALYZED));
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
                }
                finally
                {
                    writer?.Dispose();
                }
            }

            Console.WriteLine($"Time ellapsed: {sp.Elapsed}");
            Console.WriteLine($"Total pages: {pages}");
            Console.WriteLine($"Total terms: {terms}");
            Console.WriteLine($"Total allocations: {new Size(GC.GetAllocatedBytesForCurrentThread(), SizeUnit.Bytes)}");
        }

        private static void CoraxIndexTerms(string path)
        {
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
                                builder.Term(termData.field, termData.term);

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
    }
}
