using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Corax;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.Documents.Indexes.Static.Counters;
using Raven.Server.Documents.Indexes.Static.TimeSeries;
using Raven.Server.Documents.Queries;
using Raven.Server.Indexing;
using Raven.Server.Logging;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Sparrow.Logging;
using Sparrow.Server.Logging;
using Voron;
using Voron.Data.CompactTrees;
using Voron.Data.Graphs;
using Voron.Impl;
using Constants = Raven.Client.Constants;
using IndexWriter = Corax.Indexing.IndexWriter;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

public sealed class CoraxIndexPersistence : IndexPersistenceBase
{
    private const bool DisableDictionaryTraining = false; // [DEBUG ONLY]: disable training.
    private readonly RavenLogger _logger;
    private readonly CoraxDocumentConverterBase _converter;

    /// <summary>Shared plan cache across all queries for this index instance.
    /// Compiled IL delegates and plan templates are reused across transactions,
    /// amortizing JIT costs. Thread-safe (ConcurrentDictionary + SIMD SoA lookup).
    /// GC'd when the index instance is replaced (e.g., on index reset/rebuild).</summary>
    internal readonly global::Corax.Querying.Planning.PlanCache SharedPlanCache;

    // Above this many multi-valued fields (e.g. an index with very many multi-valued dynamic fields) we stop
    // snapshotting and leave the set null, so callers fall back to a live per-call tree lookup. Bounds the
    // resident memory the snapshot can hold.
    private const int MaxFieldsWithMultipleTermsToCache = 512;

    private Dictionary<Slice, HnswIndexCache> _hnswCaches;
    // The single source of truth for both the vector caches and the multi-valued field snapshot we publish.
    // The snapshot itself lives in _currentCache.FieldsWithMultipleTerms (no separate field), so there is one
    // instance to reason about; "previous" for the monotonic rebuild is read back from it.
    private IndexTransactionCache _currentCache;
    private StorageEnvironment _environment;
    private Action<LowLevelTransaction> _newTransactionCreatedHandler;
    internal IndexWriter ActiveWriter;
    internal Dictionary<Slice, HashSet<long>> PendingDirtyVectorSets;

    public CoraxIndexPersistence(Index index, IIndexReadOperationFactory indexReadOperationFactory) : base(index, indexReadOperationFactory)
    {
        _logger = RavenLogManager.Instance.GetLoggerForIndex<CoraxIndexPersistence>(index);
        _converter = CreateConverter(index);
        SharedPlanCache = new global::Corax.Querying.Planning.PlanCache(
            index.Configuration.CoraxMaxPlansPerQuery,
            index.Configuration.CoraxMaxDistinctQueryPlans);
    }

    private int GetMaxNodesForVectorCache()
    {
        var cacheSizeBytes = _index.Configuration.CoraxVectorSearchCacheSize.GetValue(SizeUnit.Bytes);
        var bytesPerNode = HnswIndexCache.EstimateBytesPerNode(_index.Configuration.CoraxVectorDefaultNumberOfEdges);
        return (int)Math.Min(cacheSizeBytes / bytesPerNode, int.MaxValue);
    }

    private CoraxDocumentConverterBase CreateConverter(Index index)
    {
        bool storeValue = false;
        switch (index.Type)
        {
            case IndexType.AutoMapReduce:
                storeValue = true;
                break;
            case IndexType.MapReduce:
                return new AnonymousCoraxDocumentConverter(index, true);
            case IndexType.Map:
                switch (_index.SourceType)
                {
                    case IndexSourceType.Documents:
                        return new AnonymousCoraxDocumentConverter(index);
                    case IndexSourceType.TimeSeries:
                    case IndexSourceType.Counters:
                        return new CountersAndTimeSeriesAnonymousCoraxDocumentConverter(index);
                }
                break;
            case IndexType.JavaScriptMap:
                switch (_index.SourceType)
                {
                    case IndexSourceType.Documents:
                        return new CoraxJintDocumentConverter((MapIndex)index);
                    case IndexSourceType.TimeSeries:
                        return new CountersAndTimeSeriesJintCoraxDocumentConverter((MapTimeSeriesIndex)index);
                    case IndexSourceType.Counters:
                        return new CountersAndTimeSeriesJintCoraxDocumentConverter((MapCountersIndex)index);
                }
                break;
            case IndexType.JavaScriptMapReduce:
                return new CoraxJintDocumentConverter((MapReduceIndex)index, storeValue: true);
        }

        return new CoraxDocumentConverter(index, storeValue: storeValue);
    }

    public override IndexReadOperationBase OpenIndexReader(Transaction readTransaction, IndexQueryServerSide query = null)
    {
        return IndexReadOperationFactory.CreateCoraxIndexReadOperation(_index, _logger, readTransaction, _index._queryBuilderFactories,
            _converter.GetKnownFieldsForQuerying(), query);
    }

    public override bool ContainsField(string field)
    {
        if (field == Constants.Documents.Indexing.Fields.DocumentIdFieldName)
            return _index.Type.IsMap();

        return _index.Definition.IndexFields.ContainsKey(field);
    }

    public override IndexFacetReadOperationBase OpenFacetedIndexReader(Transaction readTransaction)
    {
        return new CoraxIndexFacetedReadOperation(_index, _logger, readTransaction, _index._queryBuilderFactories, _converter.GetKnownFieldsForQuerying());
    }

    public override SuggestionIndexReaderBase OpenSuggestionIndexReader(Transaction readTransaction, string field)
    {
        if (_converter.GetKnownFieldsForQuerying().TryGetByFieldName(readTransaction.Allocator, field, out var binding) == false)
            throw new InvalidOperationException($"No suggestions index found for field '{field}'.");

        return new CoraxSuggestionReader(_index, _logger, binding, readTransaction, _converter.GetKnownFieldsForQuerying());
    }

    public override void Dispose()
    {
        if (_environment != null && _newTransactionCreatedHandler != null)
        {
            _environment.NewTransactionCreated -= _newTransactionCreatedHandler;
            _newTransactionCreatedHandler = null;
            _environment = null;
        }
        _converter?.Dispose();
        if (_hnswCaches != null)
        {
            foreach (var kv in _hnswCaches)
                kv.Value.Dispose();
            _hnswCaches = null;
        }
    }

    public override bool RequireOnBeforeExecuteIndexing()
    {
        var contextPool = _index._contextPool;
        using (contextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (var tx = context.OpenReadTransaction())
        {
            if (CompactTree.HasDictionary(tx.InnerTransaction.LowLevelTransaction))
                return false; 
        }

        if (_index.IsTestRun)
            return false;
        
        if (_index.SourceType != IndexSourceType.Documents)
            return false;

        return true;
    }

    public override void OnBeforeExecuteIndexing(IndexingStatsAggregator indexingStatsAggregator, CancellationToken token)
    {
        CreatePersistentDictionary(indexingStatsAggregator, token);
    }

    private void CreatePersistentDictionary(IndexingStatsAggregator indexingStatsAggregator, CancellationToken token)
    {
        var contextPool = _index._contextPool;
        var documentStorage = _index.DocumentDatabase.DocumentsStorage;
        
        using var scope = indexingStatsAggregator.CreateScope();
        using var indexingStatsScope = scope.For(IndexingOperation.Corax.DictionaryTraining);
        using var __ = CultureHelper.EnsureInvariantCulture();
        using var ___ = contextPool.AllocateOperationContext(out TransactionOperationContext indexContext);
        using var queryContext = QueryOperationContext.Allocate(_index.DocumentDatabase, _index);
        using (CurrentIndexingScope.Current = _index.CreateIndexingScope(indexContext, queryContext))
        {
            indexContext.PersistentContext.LongLivedTransactions = true;
            queryContext.SetLongLivedTransactions(true);

            using var readTx = queryContext.OpenReadTransaction();
            using var tx = indexContext.OpenWriteTransaction();
            
            // We are creating a new converter because converters get tied through their accessors to the structure, and since on Map-Reduce indexes
            // we only care about the map and not the reduce hilarity can ensure when properties do not share the type. 
            var converter = CreateConverter(_index);
            converter.IgnoreComplexObjectsDuringIndex = true; // for training, we don't care
            
            var enumerator = new CoraxDocumentTrainEnumerator(indexContext, converter, _index, _index.Type, documentStorage, queryContext.Documents, _index.Collections, token, indexingStatsScope, _index.Configuration.DocumentsLimitForCompressionDictionaryCreation);

            var llt = tx.InnerTransaction.LowLevelTransaction;

            if (DisableDictionaryTraining || PersistentDictionary.TryCreate(llt, enumerator, out var _) == false)
                PersistentDictionary.CreateDefault(llt);

            tx.Commit();
        }
    }
    
    #region LuceneMethods

    public override bool HasWriter { get; }

    public override void CleanWritersIfNeeded()
    {
        // lucene method
    }

    public override void Clean(IndexCleanup mode)
    {
        // lucene method
    }

    public override void Initialize(StorageEnvironment environment)
    {
        HashSet<string> fieldsWithMultipleTerms = null;
        using (var roTx = environment.ReadTransaction())
        {
            WarmInitialCaches(roTx);
            fieldsWithMultipleTerms = ReadFieldsWithMultipleTerms(roTx, previous: null);
        }
        _currentCache = BuildCurrentCache(fieldsWithMultipleTerms);

        _environment = environment;
        _newTransactionCreatedHandler = tx => tx.ImmutableExternalState = Volatile.Read(ref _currentCache);
        environment.NewTransactionCreated += _newTransactionCreatedHandler;
    }

    public override void PublishIndexCacheToNewTransactions(IndexTransactionCache transactionCache)
    {
        Volatile.Write(ref _currentCache, transactionCache);
    }

    internal override IndexTransactionCache BuildStreamCacheAfterTx(Transaction tx)
    {
        // Runs at the last point a committing write tx can read its own just-written trees. Refresh the
        // multi-valued field snapshot (rebuilt only if the tree actually grew) and hand it - together with the
        // current vector caches - to the cache that PublishIndexCacheToNewTransactions promotes for new readers.
        var fields = ReadFieldsWithMultipleTerms(tx, _currentCache?.FieldsWithMultipleTerms);
        return BuildCurrentCache(fields);
    }

    // Reads the MultipleTermsInField tree into a string set. The set is monotonic (write side only ever adds),
    // so when the entry count is unchanged we reuse the previous instance instead of rebuilding. Above the cap
    // we return null, signalling consumers to fall back to a live per-call tree lookup.
    private static HashSet<string> ReadFieldsWithMultipleTerms(Transaction tx, HashSet<string> previous)
    {
        var tree = tx.ReadTree(global::Corax.Constants.IndexWriter.MultipleTermsInField);
        long count = tree?.State.Header.NumberOfEntries ?? 0;
        if (count == 0)
            return null;

        // Unchanged since last build: the previous snapshot (or previous null, when over the cap) still holds.
        if (previous != null && previous.Count == count)
            return previous;
        if (count > MaxFieldsWithMultipleTermsToCache)
            return null;

        var set = new HashSet<string>((int)count, StringComparer.Ordinal);
        using (var it = tree.Iterate(prefetch: false))
        {
            if (it.Seek(Slices.BeforeAllKeys))
            {
                do
                {
                    set.Add(it.CurrentKey.ToString());
                } while (it.MoveNext());
            }
        }

        return set;
    }

    // Single construction point for the per-tx cache so the vector caches and the multi-valued field snapshot
    // never clobber one another: every writer of _currentCache routes through here. The snapshot is passed in
    // (callers preserving it read the current value back from _currentCache.FieldsWithMultipleTerms).
    private IndexTransactionCache BuildCurrentCache(HashSet<string> fieldsWithMultipleTerms)
    {
        if (_hnswCaches is null && fieldsWithMultipleTerms is null)
            return null;

        return new IndexTransactionCache
        {
            VectorNodeCaches = _hnswCaches,
            FieldsWithMultipleTerms = fieldsWithMultipleTerms
        };
    }

    internal override void RecreateSearcher(Transaction asOfTx)
    {
        var dirty = PendingDirtyVectorSets;
        PendingDirtyVectorSets = null;
        if (dirty == null)
            return;

        var maxNodes = GetMaxNodesForVectorCache();
        if (maxNodes <= 0)
        {
            // Cache disabled at runtime (CacheSizeInMb set to 0): stop publishing the caches and drop
            // our references so new read transactions resolve from disk and a later re-enable rebuilds
            // from scratch rather than serving entries that missed the commits made while disabled.
            // In-flight readers keep their captured snapshot; the dropped instances release their native
            // memory via finalization once those transactions complete.
            if (_hnswCaches != null)
            {
                Volatile.Write(ref _hnswCaches, null);
                // Drop only the vector caches; keep publishing the multi-valued field snapshot if we have one.
                Volatile.Write(ref _currentCache, BuildCurrentCache(_currentCache?.FieldsWithMultipleTerms));
            }
            return;
        }

        var llt = asOfTx.LowLevelTransaction;
        Dictionary<Slice, HnswIndexCache> freshlyAdded = null;
        foreach (var kv in dirty)
        {
            if (_hnswCaches != null && _hnswCaches.TryGetValue(kv.Key, out var cache))
            {
                cache.ApplyCommit(llt, kv.Key, kv.Value);
                continue;
            }

            var fresh = HnswIndexCache.WarmFromScratch(llt, kv.Key, maxNodes);
            if (fresh is null)
                continue;
            (freshlyAdded ??= new Dictionary<Slice, HnswIndexCache>(SliceComparer.Instance))[kv.Key] = fresh;
        }

        if (freshlyAdded is null)
            return;

        // Copy-on-write: in-flight readers that captured the previous _hnswCaches reference
        // keep observing it unchanged; new transactions pick up the replacement via the
        // NewTransactionCreated subscription. Single-writer here (post-commit hook is serial).
        var grown = _hnswCaches is null
            ? new Dictionary<Slice, HnswIndexCache>(SliceComparer.Instance)
            : new Dictionary<Slice, HnswIndexCache>(_hnswCaches, SliceComparer.Instance);
        foreach (var kv in freshlyAdded)
            grown[kv.Key] = kv.Value;

        Volatile.Write(ref _hnswCaches, grown);
        Volatile.Write(ref _currentCache, BuildCurrentCache(_currentCache?.FieldsWithMultipleTerms));
    }

    private void WarmInitialCaches(Transaction tx)
    {
        var maxNodes = GetMaxNodesForVectorCache();
        if (maxNodes <= 0)
            return;

        // Vector fields are read from the index definition, not the fields mapping: field discovery during
        // initialization must stay independent of analyzer construction.
        var vectorFieldNames = _converter?.GetVectorFieldNames();
        if (vectorFieldNames is null)
            return;

        var llt = tx.LowLevelTransaction;
        foreach (var fieldName in vectorFieldNames)
        {
            Debug.Assert(fieldName.HasValue && fieldName.Size > 0,
                "Vector field name must be allocated and non-empty for cache keying");
            var cache = HnswIndexCache.WarmFromScratch(llt, fieldName, maxNodes);
            if (cache is null)
                continue;
            _hnswCaches ??= new Dictionary<Slice, HnswIndexCache>(SliceComparer.Instance);
            _hnswCaches[fieldName] = cache;
        }
    }

    internal override void RecreateSuggestionsSearchers(Transaction asOfTx)
    {
        //lucene method
    }

    public override void DisposeWriters()
    {
        //lucene method
    }
    #endregion
    
    public override IndexWriteOperationBase OpenIndexWriter(Transaction writeTransaction, JsonOperationContext indexContext)
    {
        if (_index.Type == IndexType.MapReduce || _index.Type == IndexType.JavaScriptMapReduce)
        {
            var mapReduceIndex = (MapReduceIndex)_index;
            if (string.IsNullOrWhiteSpace(mapReduceIndex.Definition.OutputReduceToCollection) == false)
                return new OutputReduceCoraxIndexWriteOperation(mapReduceIndex, writeTransaction, _converter, _logger, indexContext);
        }
        
        return new CoraxIndexWriteOperation(
            _index,
            writeTransaction,
            _converter,
            _logger
        );
    }

    public override void AssertCanOptimize()
    {
        throw new NotSupportedInCoraxException("Optimize is not supported in Corax.");
    }

    public override void AssertCanDump()
    {
        throw new NotSupportedInCoraxException("Dump is not supported in Corax.");
    }
}
