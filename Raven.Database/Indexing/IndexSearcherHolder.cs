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
using Raven.Abstractions.Extensions;

namespace Raven.Database.Indexing
{
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

            // here we try to make sure that the actual facet cache is up to do when we update the index searcher.
            // we use this to ensure that any facets that has been recently queried is warmed up and in the cache
            //if (context.Configuration.PrewarmFacetsOnIndexingMaxAge != TimeSpan.Zero)
            //{
            //    var usedFacets = old.GetUsedFacets(context.Configuration.PrewarmFacetsOnIndexingMaxAge).ToArray();

            //    if (usedFacets.Length > 0)
            //    {
            //        var preFillCache = Task.Factory.StartNew(() =>
            //        {
            //            var sp = Stopwatch.StartNew();
            //            try
            //            {
            //                IndexedTerms.PreFillCache(current, usedFacets, searcher.IndexReader);
            //            }
            //            catch (Exception e)
            //            {
            //                Log.WarnException(
            //                    string.Format("Failed to properly pre-warm the facets cache ({1}) for index {0}", index,
            //                        string.Join(",", usedFacets)), e);
            //            }
            //            finally
            //            {
            //                Log.Debug("Pre-warming the facet cache for {0} took {2}. Facets: {1}", index, string.Join(",", usedFacets), sp.Elapsed);
            //            }
            //        });
            //        preFillCache.Wait(context.Configuration.PrewarmFacetsSyncronousWaitTime);
            //    }
            //}


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

            private readonly Dictionary<string, Dictionary<object, LinkedList<CacheVal>[]>> cache = new Dictionary<string, Dictionary<object, LinkedList<CacheVal>[]>>();

            private readonly ConcurrentDictionary<string, DateTime> lastFacetQuery = new ConcurrentDictionary<string, DateTime>();

            private readonly ReaderWriterLockSlim rwls = new ReaderWriterLockSlim();

            private readonly Dictionary<object, SegmentReaderWithMetaInformation> segmentReadersCache;

            private readonly Dictionary<int, object> docReaderCache = new Dictionary<int, object>();

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

            public bool IsInCache(string field, int doc)
            {
                var now = SystemTime.UtcNow;
                lastFacetQuery.AddOrUpdate(field, now, (s, time) => time > now ? time : now);

                if (!cache.ContainsKey(field))
                    return false;

                var fieldCacheKey = GetCachedSegmentReaderFieldCacheKey(doc);

                if (!cache[field].ContainsKey(fieldCacheKey))
                    return false;

                var segmentReader = segmentReadersCache[fieldCacheKey];
                var translatedDocId = segmentReader.TranslateDocId(doc);

                return cache[field][fieldCacheKey].Length >= translatedDocId;
            }

            public void SetInCache(string field, object fieldCacheKey, LinkedList<CacheVal>[] items)
            {
                cache.GetOrAdd(field)[fieldCacheKey] = items;
            }

            public IEnumerable<CacheVal> GetFromCache(string field, int doc)
            {
                if (!cache.ContainsKey(field))
                    yield break;

                var fieldCacheKey = GetCachedSegmentReaderFieldCacheKey(doc);

                if (!cache[field].ContainsKey(fieldCacheKey))
                    yield break;

                var segmentReader = segmentReadersCache[fieldCacheKey];
                var translatedDocId = segmentReader.TranslateDocId(doc);

                if (cache[field][fieldCacheKey][translatedDocId] == null)
                    yield break;

                foreach (var cacheVal in cache[field][fieldCacheKey][translatedDocId])
                    yield return cacheVal;
            }

            public IndexSearcherHoldingState(IndexSearcher indexSearcher)
            {
                IndexSearcher = indexSearcher;

                if (indexSearcher == null)
                    return;

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

            public SegmentReaderWithMetaInformation GetCachedSegmentReaderByFieldCacheKey(object fieldCacheKey)
            {
                return segmentReadersCache[fieldCacheKey];
            }

            public object GetCachedSegmentReaderFieldCacheKey(int docId)
            {
                if (docReaderCache.ContainsKey(docId))
                    return docReaderCache[docId];

                var fieldCacheKey = GetSegmentReaderFieldCacheKey(docId);
                docReaderCache[docId] = fieldCacheKey;

                return fieldCacheKey;
            }

            private object GetSegmentReaderFieldCacheKey(int docId)
            {
                foreach (var reader in segmentReadersCache)
                {
                    if (docId >= reader.Value.MinDoc && docId <= reader.Value.MaxDoc)
                        return reader.Value.FieldCacheKey;
                }

                throw new InvalidOperationException("There is no segment reader for doc: " + docId);
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

                public int NumDocs()
                {
                    return reader.NumDocs();
                }
            }
        }
    }
}
