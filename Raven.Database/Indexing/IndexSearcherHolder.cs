using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Raven.Abstractions;
using Raven.Abstractions.Logging;
using Raven.Json.Linq;

namespace Raven.Database.Indexing
{
    using System.Diagnostics;
    using System.Threading.Tasks;

    using Raven.Abstractions.Extensions;

    public class IndexSearcherHolder
    {
        private readonly string index;
        private readonly WorkContext context;
        private static readonly ILog Log = LogManager.GetCurrentClassLogger();

        private volatile IndexSearcherHoldingState current;

        public IndexSearcherHolder(string index, WorkContext context)
        {
            this.index = index;
            this.context = context;
        }

        public ManualResetEvent SetIndexSearcher(IndexSearcher searcher, bool wait)
        {
            var old = current;
            current = new IndexSearcherHoldingState(searcher);

            if (old == null)
                return null;

             //here we try to make sure that the actual facet cache is up to do when we update the index searcher.
             //we use this to ensure that any facets that has been recently queried is warmed up and in the cache
            if (context.Configuration.PrewarmFacetsOnIndexingMaxAge != TimeSpan.Zero)
            {
                var usedFacets = old.GetUsedFacets(context.Configuration.PrewarmFacetsOnIndexingMaxAge).ToArray();

                if (usedFacets.Length > 0)
                {
                    var preFillCache = Task.Factory.StartNew(
                        () =>
                        {
                            var sp = Stopwatch.StartNew();
                            try
                            {
                                var usedFieldCacheKeys = old.GetUsedFieldCacheKeys();

                                IndexedTerms.PreFillCache(current, usedFacets, usedFieldCacheKeys);
                            }
                            catch (Exception e)
                            {
                                Log.WarnException(
                                    string.Format("Failed to properly pre-warm the facets cache ({1}) for index {0}", index, string.Join(",", usedFacets)), e);
                            }
                            finally
                            {
                                Log.Debug("Pre-warming the facet cache for {0} took {2}. Facets: {1}", index, string.Join(",", usedFacets), sp.Elapsed);
                            }
                        });
                    preFillCache.Wait(context.Configuration.PrewarmFacetsSyncronousWaitTime);
                }
            }


            Interlocked.Increment(ref old.Usage);
            using (old)
            {
                if (wait)
                    return old.MarkForDisposalWithWait();
                old.MarkForDisposal();
                return null;
            }
        }

        public IDisposable GetSearcher(out IndexSearcher searcher)
        {
            var indexSearcherHoldingState = GetCurrentStateHolder();
            try
            {
                searcher = indexSearcherHoldingState.IndexSearcher;
                return indexSearcherHoldingState;
            }
            catch (Exception e)
            {
                Log.ErrorException("Failed to get the index searcher.", e);
                indexSearcherHoldingState.Dispose();
                throw;
            }
        }

        public IDisposable GetSearcherAndTermDocs(out IndexSearcher searcher, out RavenJObject[] termDocs)
        {
            var indexSearcherHoldingState = GetCurrentStateHolder();
            try
            {
                searcher = indexSearcherHoldingState.IndexSearcher;
                termDocs = indexSearcherHoldingState.GetOrCreateTerms();
                return indexSearcherHoldingState;
            }
            catch (Exception)
            {
                indexSearcherHoldingState.Dispose();
                throw;
            }
        }

        internal IndexSearcherHoldingState GetCurrentStateHolder()
        {
            while (true)
            {
                var state = current;
                Interlocked.Increment(ref state.Usage);
                if (state.ShouldDispose)
                {
                    state.Dispose();
                    continue;
                }

                return state;
            }
        }


        public class IndexSearcherHoldingState : IDisposable
        {
            public readonly IndexSearcher IndexSearcher;

            public volatile bool ShouldDispose;
            public int Usage;
            private RavenJObject[] readEntriesFromIndex;
            private readonly Lazy<ManualResetEvent> disposed = new Lazy<ManualResetEvent>(() => new ManualResetEvent(false));


            private readonly ConcurrentDictionary<string, DateTime> lastFacetQuery = new ConcurrentDictionary<string, DateTime>();

            private readonly ReaderWriterLockSlim rwls = new ReaderWriterLockSlim();

            private readonly Dictionary<string, Dictionary<object, LinkedList<CacheVal>[]>> cache = new Dictionary<string, Dictionary<object, LinkedList<CacheVal>[]>>();

            private readonly Dictionary<object, SegmentReaderWithMetaInformation> segmentReadersCache;

            public ReaderWriterLockSlim Lock
            {
                get { return rwls; }
            }

            public class CacheVal
            {
                public Term Term;

                public override string ToString()
                {
                    return string.Format("Term: {0}", Term);
                }
            }

            public IEnumerable<string> GetUsedFacets(TimeSpan tooOld)
            {
                var now = SystemTime.UtcNow;
                return lastFacetQuery.Where(x => (now - x.Value) < tooOld).Select(x => x.Key);
            }

            public bool IsInCache(string field, DocIdWithSegmentFieldCacheKey doc)
            {
                var now = SystemTime.UtcNow;
                lastFacetQuery.AddOrUpdate(field, now, (s, time) => time > now ? time : now);

                if (!cache.ContainsKey(field))
                    return false;

                if (!cache[field].ContainsKey(doc.FieldCacheKey))
                    return false;

                var segmentReader = segmentReadersCache[doc.FieldCacheKey];
                var translatedDocId = segmentReader.TranslateDocId(doc.DocId);

                return cache[field][doc.FieldCacheKey].Length >= translatedDocId;
            }

            public IEnumerable<CacheVal> GetFromCache(string field, DocIdWithSegmentFieldCacheKey docId)
            {
                if (!cache.ContainsKey(field))
                    yield break;

                if (!cache[field].ContainsKey(docId.FieldCacheKey))
                    yield break;

                var segmentReader = segmentReadersCache[docId.FieldCacheKey];
                var translatedDocId = segmentReader.TranslateDocId(docId.DocId);

                if (cache[field][docId.FieldCacheKey][translatedDocId] == null)
                    yield break;

                foreach (var cacheVal in cache[field][docId.FieldCacheKey][translatedDocId])
                    yield return cacheVal;
            }

            public IndexSearcherHoldingState(IndexSearcher indexSearcher)
            {
                IndexSearcher = indexSearcher;

                if (indexSearcher == null)
                {
                    segmentReadersCache = new Dictionary<object, SegmentReaderWithMetaInformation>();
                    return;
                }

                var readers = GetAllSegmentReaders(indexSearcher.IndexReader as DirectoryReader).ToList();
                var starts = GetStarts(readers);

                segmentReadersCache = BuildSegmentReadersWithMetaInformation(readers, starts);
            }

            public void MarkForDisposal()
            {
                ShouldDispose = true;
            }

            public ManualResetEvent MarkForDisposalWithWait()
            {
                var x = disposed.Value;//  first create the value
                ShouldDispose = true;
                return x;
            }

            public void Dispose()
            {
                if (Interlocked.Decrement(ref Usage) > 0)
                    return;
                if (ShouldDispose == false)
                    return;
                DisposeRudely();
            }

            private void DisposeRudely()
            {
                if (IndexSearcher != null)
                {
                    using (IndexSearcher)
                    using (IndexSearcher.IndexReader) { }
                }

                if (disposed.IsValueCreated)
                    disposed.Value.Set();
            }


            [MethodImpl(MethodImplOptions.Synchronized)]
            public RavenJObject[] GetOrCreateTerms()
            {
                if (readEntriesFromIndex != null)
                    return readEntriesFromIndex;

                var indexReader = IndexSearcher.IndexReader;
                readEntriesFromIndex = IndexedTerms.ReadAllEntriesFromIndex(indexReader);
                return readEntriesFromIndex;
            }

            public void SetInCache(string field, object fieldCacheKey, LinkedList<CacheVal>[] items)
            {
                cache.GetOrAdd(field)[fieldCacheKey] = items;
            }

            public SegmentReaderWithMetaInformation GetCachedSegmentReaderByFieldCacheKey(object fieldCacheKey)
            {
                SegmentReaderWithMetaInformation reader;
                if (segmentReadersCache.TryGetValue(fieldCacheKey, out reader))
                    return reader;

                return null;
            }

            public ICollection<DocIdWithSegmentFieldCacheKey> GetSegmentReaderFieldCacheKey(IEnumerable<int> docIds)
            {
                return docIds
                    .Select(GetSegmentReaderFieldCacheKey)
                    .ToList();
            }

            private DocIdWithSegmentFieldCacheKey GetSegmentReaderFieldCacheKey(int docId)
            {
                foreach (var reader in segmentReadersCache)
                {
                    if (docId >= reader.Value.MinDoc && docId <= reader.Value.MaxDoc)
                        return new DocIdWithSegmentFieldCacheKey(docId, reader.Key);
                }

                throw new InvalidOperationException("There is no segment reader for doc: " + docId);
            }


            public IEnumerable<object> GetUsedFieldCacheKeys()
            {
                return cache.Values.SelectMany(value => value.Keys);
            }

            private static Dictionary<object, SegmentReaderWithMetaInformation> BuildSegmentReadersWithMetaInformation(IList<SegmentReader> readers, int[] starts)
            {
                var results = new Dictionary<object, SegmentReaderWithMetaInformation>();

                for (int i = 0; i < readers.Count; i++)
                {
                    var reader = readers[i];
                    var start = starts[i];

                    var minDoc = start;
                    var maxDoc = start + reader.MaxDoc - 1;

                    results.Add(reader.FieldCacheKey, new SegmentReaderWithMetaInformation(minDoc, maxDoc, reader));
                }

                return results;
            }

            private static int[] GetStarts(List<SegmentReader> readers)
            {
                var maxDoc = 0;
                var starts = new int[readers.Count];
                for (int i = 0; i < readers.Count; i++)
                {
                    starts[i] = maxDoc;
                    maxDoc += readers[i].MaxDoc;
                }

                return starts;
            }

            private static IEnumerable<SegmentReader> GetAllSegmentReaders(DirectoryReader reader)
            {
                var sequentialSubReaders = reader.GetSequentialSubReaders();
                if (sequentialSubReaders == null)
                    yield break;
                foreach (var sequentialSubReader in sequentialSubReaders)
                {
                    var segmentReader = sequentialSubReader as SegmentReader;
                    if (segmentReader != null)
                    {
                        yield return segmentReader;
                    }
                }
            }

            public class DocIdWithSegmentFieldCacheKey
            {
                public DocIdWithSegmentFieldCacheKey(int docId, object fieldCacheKey)
                {
                    DocId = docId;
                    FieldCacheKey = fieldCacheKey;
                }

                public int DocId { get; private set; }

                public object FieldCacheKey { get; private set; }
            }

            public class SegmentReaderWithMetaInformation
            {
                private readonly SegmentReader reader;

                public SegmentReaderWithMetaInformation(int minDoc, int maxDoc, SegmentReader reader)
                {
                    this.reader = reader;
                    MinDoc = minDoc;
                    MaxDoc = maxDoc;
                }

                public int MinDoc { get; private set; }

                public int MaxDoc { get; private set; }

                public object FieldCacheKey
                {
                    get
                    {
                        return reader.FieldCacheKey;
                    }
                }

                public TermDocs TermDocs()
                {
                    return reader.TermDocs();
                }

                public TermEnum Terms(Term term)
                {
                    return reader.Terms(term);
                }

                public bool IsDeleted(int docId)
                {
                    return reader.IsDeleted(docId);
                }

                public int TranslateDocId(int docId)
                {
                    return docId - MinDoc;
                }

                public int GetMaxDoc()
                {
                    return reader.MaxDoc;
                }
            }
        }
    }
}
