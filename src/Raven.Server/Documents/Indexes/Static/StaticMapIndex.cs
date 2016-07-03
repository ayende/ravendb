﻿using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client.Data.Indexes;
using Raven.Client.Indexing;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Workers;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Voron;

namespace Raven.Server.Documents.Indexes.Static
{
    public class StaticMapIndex : MapIndexBase<StaticMapIndexDefinition>
    {
        private readonly HashSet<string> _referencedCollections = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        internal readonly StaticIndexBase _compiled;

        private HandleReferences _handleReferences;

        private StaticMapIndex(int indexId, StaticMapIndexDefinition definition, StaticIndexBase compiled)
            : base(indexId, IndexType.Map, definition)
        {
            _compiled = compiled;

            if (_compiled.ReferencedCollections == null)
                return;

            foreach (var collection in _compiled.ReferencedCollections)
            {
                foreach (var referencedCollection in collection.Value)
                    _referencedCollections.Add(referencedCollection);
            }
        }

        protected override IIndexingWork[] CreateIndexWorkExecutors()
        {
            var workers = new List<IIndexingWork>();
            workers.Add(new CleanupDeletedDocuments(this, DocumentDatabase.DocumentsStorage, _indexStorage, DocumentDatabase.Configuration.Indexing, null));

            if (_referencedCollections.Count > 0)
                workers.Add(_handleReferences = new HandleReferences(this, DocumentDatabase.DocumentsStorage, _indexStorage, DocumentDatabase.Configuration.Indexing));

            workers.Add(new MapDocuments(this, DocumentDatabase.DocumentsStorage, _indexStorage, DocumentDatabase.Configuration.Indexing, null));

            return workers.ToArray();
        }

        public override void HandleDelete(DocumentTombstone tombstone, string collection, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            if (_referencedCollections.Count > 0)
                _handleReferences.HandleDelete(tombstone, collection, writer, indexContext, stats);

            base.HandleDelete(tombstone, collection, writer, indexContext, stats);
        }

        protected override bool IsStale(DocumentsOperationContext databaseContext, TransactionOperationContext indexContext, long? cutoff = null)
        {
            var isStale = base.IsStale(databaseContext, indexContext, cutoff);
            if (isStale || _referencedCollections.Count == 0)
                return isStale;

            foreach (var collection in Collections)
            {
                HashSet<string> referencedCollections;
                if (_compiled.ReferencedCollections.TryGetValue(collection, out referencedCollections) == false)
                    continue;

                foreach (var referencedCollection in referencedCollections)
                {
                    var lastDocEtag = DocumentDatabase.DocumentsStorage.GetLastDocumentEtag(databaseContext, referencedCollection);
                    var lastProcessedReferenceEtag = _indexStorage.ReadLastProcessedReferenceEtag(indexContext.Transaction, collection, referencedCollection);

                    if (cutoff == null)
                    {
                        if (lastDocEtag > lastProcessedReferenceEtag)
                            return true;

                        var lastTombstoneEtag = DocumentDatabase.DocumentsStorage.GetLastTombstoneEtag(databaseContext, referencedCollection);
                        var lastProcessedTombstoneEtag = _indexStorage.ReadLastProcessedReferenceTombstoneEtag(indexContext.Transaction, collection, referencedCollection);

                        if (lastTombstoneEtag > lastProcessedTombstoneEtag)
                            return true;
                    }
                    else
                    {
                        if (Math.Min(cutoff.Value, lastDocEtag) > lastProcessedReferenceEtag)
                            return true;

                        if (DocumentDatabase.DocumentsStorage.GetNumberOfTombstonesWithDocumentEtagLowerThan(databaseContext, referencedCollection, cutoff.Value) > 0)
                            return true;
                    }
                }
            }

            return false;
        }

        protected override void HandleDocumentChange(DocumentChangeNotification notification)
        {
            if (Collections.Contains(notification.CollectionName) == false && _referencedCollections.Contains(notification.CollectionName) == false)
                return;

            _mre.Set();
        }

        protected override unsafe long CalculateIndexEtag(bool isStale, DocumentsOperationContext documentsContext, TransactionOperationContext indexContext)
        {
            if (_referencedCollections.Count == 0)
                return base.CalculateIndexEtag(isStale, documentsContext, indexContext);

            var indexEtagBytes = new long[
                 1 + // definition hash
                 1 + // isStale
                 2 * Collections.Count + // last document etags and last mapped etags per collection
                 2 * (Collections.Count * _referencedCollections.Count) // last referenced collection etags and last processed reference collection etags
                 ];

            var index = CalculateIndexEtagInternal(indexEtagBytes, isStale, documentsContext, indexContext);

            foreach (var collection in Collections)
            {
                HashSet<string> referencedCollections;
                if (_compiled.ReferencedCollections.TryGetValue(collection, out referencedCollections) == false)
                    continue;

                foreach (var referencedCollection in referencedCollections)
                {
                    var lastDocEtag = DocumentDatabase.DocumentsStorage.GetLastDocumentEtag(documentsContext, referencedCollection);
                    var lastMappedEtag = _indexStorage.ReadLastProcessedReferenceEtag(indexContext.Transaction, collection, referencedCollection);

                    indexEtagBytes[index++] = lastDocEtag;
                    indexEtagBytes[index++] = lastMappedEtag;
                }
            }

            unchecked
            {
                fixed (long* buffer = indexEtagBytes)
                {
                    return (long)Hashing.XXHash64.Calculate((byte*)buffer, indexEtagBytes.Length * sizeof(long));
                }
            }
        }

        public override IIndexedDocumentsEnumerator GetMapEnumerator(IEnumerable<Document> documents, string collection, TransactionOperationContext indexContext)
        {
            return new StaticIndexDocsEnumerator(documents, _compiled.Maps[collection], collection);
        }

        public static Index CreateNew(int indexId, IndexDefinition definition, DocumentDatabase documentDatabase)
        {
            var staticIndex = IndexCompilationCache.GetIndexInstance(definition);
            var staticMapIndexDefinition = new StaticMapIndexDefinition(definition, staticIndex.Maps.Keys.ToArray());
            var instance = new StaticMapIndex(indexId, staticMapIndexDefinition, staticIndex);
            instance.Initialize(documentDatabase);

            return instance;
        }

        public static Index Open(int indexId, StorageEnvironment environment, DocumentDatabase documentDatabase)
        {
            var staticMapIndexDefinition = StaticMapIndexDefinition.Load(environment);
            var staticIndex = IndexCompilationCache.GetIndexInstance(staticMapIndexDefinition.IndexDefinition);

            var instance = new StaticMapIndex(indexId, staticMapIndexDefinition, staticIndex);
            instance.Initialize(environment, documentDatabase);

            return instance;
        }
    }
}