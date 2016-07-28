﻿using System;
using System.Collections;
using Raven.Client.Documents.Indexing;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Workers;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.ServerWide.Context;

using Sparrow.Json;

namespace Raven.Server.Documents.Indexes
{
    public abstract class MapIndexBase<T> : Index<T> where T : IndexDefinitionBase
    {
        protected MapIndexBase(int indexId, IndexType type, T definition) : base(indexId, type, definition)
        {
        }

        protected override IIndexingWork[] CreateIndexWorkExecutors()
        {
            return new IIndexingWork[]
            {
                new CleanupDeletedDocuments(this, DocumentDatabase.DocumentsStorage, _indexStorage, DocumentDatabase.Configuration.Indexing, null),
                new MapDocuments(this, DocumentDatabase.DocumentsStorage, _indexStorage, DocumentDatabase.Configuration.Indexing, null),
            };
        }

        public override void HandleDelete(DocumentTombstone tombstone, string collection, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            writer.Delete(tombstone.Key, stats);
        }

        public override void HandleMap(LazyStringValue key, IEnumerable mapResults, IndexWriteOperation writer, TransactionOperationContext indexContext, IndexingStatsScope stats)
        {
            writer.Delete(key, stats);

            var numberOfOutputs = 0;
            foreach (var mapResult in mapResults)
            {
                writer.IndexDocument(key, mapResult, stats);
                numberOfOutputs++;

                if (EnsureValidNumberOfOutputsForDocument(numberOfOutputs))
                    continue;

                writer.Delete(key, stats); // TODO [ppekrol] we want to delete invalid doc from index?
                throw new InvalidOperationException($"Index '{Name}' has already produced {numberOfOutputs} map results for a source document '{key}', while the allowed max number of outputs is {MaxNumberOfIndexOutputs} per one document. Please verify this index definition and consider a re-design of your entities or index.");
            }

            DocumentDatabase.Metrics.IndexedPerSecond.Mark();
        }

        public override IQueryResultRetriever GetQueryResultRetriever(DocumentsOperationContext documentsContext, TransactionOperationContext indexContext, FieldsToFetch fieldsToFetch)
        {
            return new MapQueryResultRetriever(DocumentDatabase.DocumentsStorage, documentsContext, fieldsToFetch);
        }
    }
}