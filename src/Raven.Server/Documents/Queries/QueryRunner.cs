﻿using System;
using System.Collections.Generic;
using System.Threading.Tasks;

using Microsoft.Extensions.Primitives;
using Raven.Client.Data;
using Raven.Client.Data.Indexes;
using Raven.Client.Data.Queries;
using Raven.Client.Exceptions;
using Raven.Client.Util.RateLimiting;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Queries.Dynamic;
using Raven.Server.Documents.Queries.MoreLikeThis;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

using PatchRequest = Raven.Server.Documents.Patch.PatchRequest;

namespace Raven.Server.Documents.Queries
{
    public class QueryRunner
    {
        private readonly DocumentDatabase _database;

        private readonly DocumentsOperationContext _documentsContext;

        public QueryRunner(DocumentDatabase database, DocumentsOperationContext documentsContext)
        {
            _database = database;
            _documentsContext = documentsContext;
        }

        public async Task<DocumentQueryResult> ExecuteQuery(string indexName, IndexQueryServerSide query, StringValues includes, long? existingResultEtag, OperationCancelToken token)
        {
            DocumentQueryResult result;

            if (indexName.StartsWith("dynamic", StringComparison.OrdinalIgnoreCase))
            {
                var runner = new DynamicQueryRunner(_database.IndexStore, _database.TransformerStore, _database.DocumentsStorage, _documentsContext, token);

                result = await runner.Execute(indexName, query, existingResultEtag).ConfigureAwait(false);
            }
            else
            {
                var index = GetIndex(indexName);
                if (existingResultEtag.HasValue)
                {
                    var etag = index.GetIndexEtag();
                    if (etag == existingResultEtag)
                        return DocumentQueryResult.NotModifiedResult;
                }

                return await index.Query(query, _documentsContext, token);
            }

            return result;
        }

        public Task<FacetedQueryResult> ExecuteFacetedQuery(string indexName, FacetQuery query, long? facetsEtag, long? existingResultEtag, OperationCancelToken token)
        {
            if (query.FacetSetupDoc != null)
            {
                FacetSetup facetSetup;
                using (_documentsContext.OpenReadTransaction())
                {
                    var facetSetupAsJson = _database.DocumentsStorage.Get(_documentsContext, query.FacetSetupDoc);
                    if (facetSetupAsJson == null)
                        throw new DocumentDoesNotExistException(query.FacetSetupDoc);

                    try
                    {
                        facetSetup = JsonDeserializationServer.FacetSetup(facetSetupAsJson.Data);
                    }
                    catch (Exception e)
                    {
                        throw new DocumentParseException(query.FacetSetupDoc, typeof(FacetSetup), e);
                    }

                    facetsEtag = facetSetupAsJson.Etag;
                }

                query.Facets = facetSetup.Facets;
            }

            return ExecuteFacetedQuery(indexName, query, facetsEtag.Value, existingResultEtag, token);
        }

        private async Task<FacetedQueryResult> ExecuteFacetedQuery(string indexName, FacetQuery query, long facetsEtag, long? existingResultEtag, OperationCancelToken token)
        {
            var index = GetIndex(indexName);
            if (existingResultEtag.HasValue)
            {
                var etag = index.GetIndexEtag() ^ facetsEtag;
                if (etag == existingResultEtag)
                    return FacetedQueryResult.NotModifiedResult;
            }

            return await index.FacetedQuery(query, facetsEtag, _documentsContext, token);
        }

        public TermsQueryResult ExecuteGetTermsQuery(string indexName, string field, string fromValue, long? existingResultEtag, int pageSize, DocumentsOperationContext context, OperationCancelToken token)
        {
            var index = GetIndex(indexName);

            var etag = index.GetIndexEtag();
            if (etag == existingResultEtag)
                return TermsQueryResult.NotModifiedResult;

            return index.GetTerms(field, fromValue, pageSize, context, token);
        }

        public MoreLikeThisQueryResultServerSide ExecuteMoreLikeThisQuery(string indexName, MoreLikeThisQueryServerSide query, DocumentsOperationContext context, long? existingResultEtag, OperationCancelToken token)
        {
            if (query == null)
                throw new ArgumentNullException(nameof(query));

            if (string.IsNullOrEmpty(query.DocumentId) && query.MapGroupFields.Count == 0)
                throw new InvalidOperationException("The document id or map group fields are mandatory");

            var index = GetIndex(indexName);

            var etag = index.GetIndexEtag();
            if (etag == existingResultEtag)
                return MoreLikeThisQueryResultServerSide.NotModifiedResult;

            context.OpenReadTransaction();

            return index.MoreLikeThisQuery(query, context, token);
        }

        public List<DynamicQueryToIndexMatcher.Explanation> ExplainDynamicIndexSelection(string indexName, IndexQueryServerSide indexQuery)
        {
            if (string.IsNullOrWhiteSpace(indexName) || (indexName.StartsWith("dynamic/", StringComparison.OrdinalIgnoreCase) == false && indexName.Equals("dynamic", StringComparison.OrdinalIgnoreCase) == false))
                throw new InvalidOperationException("Explain can only work on dynamic indexes");

            var runner = new DynamicQueryRunner(_database.IndexStore, _database.TransformerStore, _database.DocumentsStorage, _documentsContext, OperationCancelToken.None);

            return runner.ExplainIndexSelection(indexName, indexQuery);
        }

        public Task<IOperationResult> ExecuteDeleteQuery(string indexName, IndexQueryServerSide query, QueryOperationOptions options, DocumentsOperationContext context, Action<DeterminateProgress> onProgress, OperationCancelToken token)
        {
            return ExecuteOperation(indexName, query, options, context, onProgress, key => _database.DocumentsStorage.Delete(context, key, null), token);
        }

        public Task<IOperationResult> ExecutePatchQuery(string indexName, IndexQueryServerSide query, QueryOperationOptions options, PatchRequest patch, DocumentsOperationContext context, Action<DeterminateProgress> onProgress, OperationCancelToken token)
        {
            return ExecuteOperation(indexName, query, options, context, onProgress, key => _database.Patch.Apply(context, key, null, patch, null), token);
        }

        private async Task<IOperationResult> ExecuteOperation(string indexName, IndexQueryServerSide query, QueryOperationOptions options,
            DocumentsOperationContext context, Action<DeterminateProgress> onProgress, Action<string> action, OperationCancelToken token)
        {
            var index = GetIndex(indexName);

            if (index.Type.IsMapReduce())
                throw new InvalidOperationException("Cannot execute Delete operation on Map-Reduce indexes.");

            query = ConvertToOperationQuery(query, options);

            const int BatchSize = 1024;

            RavenTransaction tx = null;
            var operationsInCurrentBatch = 0;
            DocumentQueryResult results;
            try
            {
                results = await index.Query(query, context, token).ConfigureAwait(false);
            }
            finally //make sure to close tx if DocumentConflictException is thrown
            {
                context.CloseTransaction();
            }

            if (options.AllowStale == false && results.IsStale)
                throw new InvalidOperationException("Cannot perform delete operation. Query is stale.");

            var progress = new DeterminateProgress
            {
                Total = results.Results.Count,
                Processed = 0
            };

            onProgress(progress);

            using (var rateGate = options.MaxOpsPerSecond.HasValue ? new RateGate(options.MaxOpsPerSecond.Value, TimeSpan.FromSeconds(1)) : null)
            {
                foreach (var document in results.Results)
                {
                    if (rateGate != null && rateGate.WaitToProceed(0) == false)
                    {
                        using (tx)
                        {
                            tx?.Commit();
                        }

                        tx = null;

                        rateGate.WaitToProceed();
                    }

                    if (tx == null)
                    {
                        operationsInCurrentBatch = 0;
                        tx = context.OpenWriteTransaction();
                    }

                    action(document.Key);
                    operationsInCurrentBatch++;
                    progress.Processed++;

                    if (progress.Processed % 128 == 0)
                    {
                        onProgress(progress);
                    }

                    if (operationsInCurrentBatch < BatchSize)
                        continue;

                    using (tx)
                    {
                        tx.Commit();
                    }

                    tx = null;
                }
            }

            using (tx)
            {
                tx?.Commit();
            }

            return new BulkOperationResult
            {
                Total = progress.Total
            };
        }

        private static IndexQueryServerSide ConvertToOperationQuery(IndexQueryServerSide query, QueryOperationOptions options)
        {
            return new IndexQueryServerSide
            {
                Query = query.Query,
                Start = query.Start,
                WaitForNonStaleResultsTimeout = options.StaleTimeout,
                PageSize = int.MaxValue,
                SortedFields = query.SortedFields,
                HighlighterPreTags = query.HighlighterPreTags,
                HighlighterPostTags = query.HighlighterPostTags,
                HighlightedFields = query.HighlightedFields,
                HighlighterKeyName = query.HighlighterKeyName,
                TransformerParameters = query.TransformerParameters,
                Transformer = query.Transformer
            };
        }

        private Index GetIndex(string indexName)
        {
            var index = _database.IndexStore.GetIndex(indexName);
            if (index == null)
                throw new InvalidOperationException("There is not index with name: " + indexName);

            return index;
        }
    }
}