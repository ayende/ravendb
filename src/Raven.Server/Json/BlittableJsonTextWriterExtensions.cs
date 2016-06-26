﻿using System.Collections.Generic;

using Raven.Abstractions;
using Raven.Abstractions.Extensions;
using Raven.Client.Data;
using Raven.Client.Data.Indexes;
using Raven.Client.Data.Queries;
using Raven.Client.Indexing;
using Raven.Imports.Newtonsoft.Json;
using Raven.Server.Documents;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Dynamic;
using Raven.Server.Documents.Replication;
using Sparrow.Json;

namespace Raven.Server.Json
{
    public static class BlittableJsonTextWriterExtensions
    {
        public static void WriteChangeVector(this BlittableJsonTextWriter writer, JsonOperationContext context,
            ChangeVectorEntry[] changeVector)
        {
            writer.WriteStartArray();
            for (int i = 0; i < changeVector.Length; i++)
            {
                var entry = changeVector[i];
                writer.WriteChangeVectorEntry(context,entry);
                writer.WriteComma();
            }
            writer.WriteEndArray();
        }

        public static void WriteChangeVectorEntry(this BlittableJsonTextWriter writer, JsonOperationContext context, ChangeVectorEntry entry)
        {
            writer.WriteStartObject();

            writer.WritePropertyName(context.GetLazyString(nameof(entry.Etag)));
            writer.WriteInteger(entry.Etag);
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(entry.DbId)));
            writer.WriteString(context.GetLazyString(entry.DbId.ToString()));

            writer.WriteEndObject();
        }


        public static void WriteExplanation(this BlittableJsonTextWriter writer, JsonOperationContext context, DynamicQueryToIndexMatcher.Explanation explanation)
        {
            writer.WriteStartObject();

            writer.WritePropertyName(context.GetLazyString(nameof(explanation.Index)));
            writer.WriteString(context.GetLazyString(explanation.Index));
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(explanation.Reason)));
            writer.WriteString(context.GetLazyString(explanation.Reason));

            writer.WriteEndObject();
        }

        public static void WriteDocumentQueryResult(this BlittableJsonTextWriter writer, JsonOperationContext context, DocumentQueryResult result, bool metadataOnly)
        {
            writer.WriteStartObject();

            writer.WritePropertyName(context.GetLazyString(nameof(result.TotalResults)));
            writer.WriteInteger(result.TotalResults);
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(result.SkippedResults)));
            writer.WriteInteger(result.SkippedResults);
            writer.WriteComma();

            writer.WriteQueryResult(context, result, metadataOnly, partial: true);

            writer.WriteEndObject();
        }

        public static void WriteQueryResult(this BlittableJsonTextWriter writer, JsonOperationContext context, QueryResultBase<Document> result, bool metadataOnly, bool partial = false)
        {
            if (partial == false)
                writer.WriteStartObject();

            writer.WritePropertyName(context.GetLazyString(nameof(result.IndexName)));
            writer.WriteString(context.GetLazyString(result.IndexName));
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyStringForFieldWithCaching(nameof(result.Results)));
            writer.WriteDocuments(context, result.Results, metadataOnly);
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyStringForFieldWithCaching(nameof(result.Includes)));
            writer.WriteDocuments(context, result.Includes, metadataOnly);
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(result.IndexTimestamp)));
            writer.WriteString(context.GetLazyString(result.IndexTimestamp.ToString(Default.DateTimeFormatsToWrite)));
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(result.LastQueryTime)));
            writer.WriteString(context.GetLazyString(result.LastQueryTime.ToString(Default.DateTimeFormatsToWrite)));
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(result.IsStale)));
            writer.WriteBool(result.IsStale);
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(result.ResultEtag)));
            writer.WriteInteger(result.ResultEtag);

            if (partial == false)
                writer.WriteEndObject();
        }

        public static void WriteIndexingPerformanceStats(this BlittableJsonTextWriter writer, JsonOperationContext context, IndexingPerformanceStats stats)
        {
            writer.WriteStartObject();

            writer.WritePropertyName(context.GetLazyString(nameof(stats.Completed)));
            writer.WriteString(context.GetLazyString(stats.Completed.GetDefaultRavenFormat(isUtc: true)));
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(stats.Started)));
            writer.WriteString(context.GetLazyString(stats.Started.GetDefaultRavenFormat(isUtc: true)));
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(stats.DurationInMilliseconds)));
            writer.WriteDouble(new LazyDoubleValue(context.GetLazyString(stats.DurationInMilliseconds.ToInvariantString())));
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(stats.Details)));
            writer.WriteIndexingPerformanceOperation(context, stats.Details);

            writer.WriteEndObject();
        }

        public static void WriteIndexingPerformanceOperation(this BlittableJsonTextWriter writer, JsonOperationContext context, IndexingPerformanceOperation operation)
        {
            writer.WriteStartObject();

            writer.WritePropertyName(context.GetLazyString(nameof(operation.DurationInMilliseconds)));
            writer.WriteDouble(new LazyDoubleValue(context.GetLazyString(operation.DurationInMilliseconds.ToInvariantString())));
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(operation.Name)));
            writer.WriteString(context.GetLazyString(operation.Name));
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(operation.Operations)));
            writer.WriteStartArray();
            if (operation.Operations != null)
            {
                var isFirstInternal = true;
                foreach (var op in operation.Operations)
                {
                    if (isFirstInternal == false)
                        writer.WriteComma();

                    isFirstInternal = false;

                    writer.WriteIndexingPerformanceOperation(context, op);
                }
            }
            writer.WriteEndArray();

            writer.WriteEndObject();
        }

        public static void WriteIndexQuery(this BlittableJsonTextWriter writer, JsonOperationContext context, IndexQuery query)
        {
            writer.WriteStartObject();

            writer.WritePropertyName(context.GetLazyString(nameof(query.AllowMultipleIndexEntriesForSameDocumentToResultTransformer)));
            writer.WriteBool(query.AllowMultipleIndexEntriesForSameDocumentToResultTransformer);
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(query.CutoffEtag)));
            if (query.CutoffEtag.HasValue)
                writer.WriteInteger(query.CutoffEtag.Value);
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(query.DebugOptionGetIndexEntries)));
            writer.WriteBool(query.DebugOptionGetIndexEntries);
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(query.DefaultField)));
            if (query.DefaultField != null)
                writer.WriteString(context.GetLazyString(query.DefaultField));
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(query.DefaultOperator)));
            writer.WriteString(context.GetLazyString(query.DefaultOperator.ToString()));
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(query.DisableCaching)));
            writer.WriteBool(query.DisableCaching);
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(query.ExplainScores)));
            writer.WriteBool(query.ExplainScores);
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(query.HighlighterKeyName)));
            if (query.HighlighterKeyName != null)
                writer.WriteString(context.GetLazyString(query.HighlighterKeyName));
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(query.IsDistinct)));
            writer.WriteBool(query.IsDistinct);
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(query.PageSize)));
            writer.WriteInteger(query.PageSize);
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(query.PageSizeSet)));
            writer.WriteBool(query.PageSizeSet);
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(query.Query)));
            if (query.Query != null)
                writer.WriteString(context.GetLazyString(query.Query));
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(query.Transformer)));
            if (query.Transformer != null)
                writer.WriteString(context.GetLazyString(query.Transformer));
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(query.ShowTimings)));
            writer.WriteBool(query.ShowTimings);
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(query.SkipDuplicateChecking)));
            writer.WriteBool(query.SkipDuplicateChecking);
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(query.Start)));
            writer.WriteInteger(query.Start);
            writer.WriteComma();

            //writer.WritePropertyName(context.GetLazyString(nameof(query.TotalSize)));
            //writer.WriteInteger(query.TotalSize.Value);
            //writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(query.WaitForNonStaleResults)));
            writer.WriteBool(query.WaitForNonStaleResults);
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(query.WaitForNonStaleResultsAsOfNow)));
            writer.WriteBool(query.WaitForNonStaleResultsAsOfNow);
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(query.WaitForNonStaleResultsTimeout)));
            if (query.WaitForNonStaleResultsTimeout.HasValue)
                writer.WriteString(context.GetLazyString(query.WaitForNonStaleResultsTimeout.Value.ToString()));
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(query.DynamicMapReduceFields)));
            writer.WriteStartArray();
            var isFirstInternal = true;
            foreach (var field in query.DynamicMapReduceFields)
            {
                if (isFirstInternal == false)
                    writer.WriteComma();

                isFirstInternal = false;

                writer.WriteStartObject();

                writer.WritePropertyName(context.GetLazyString(nameof(field.Name)));
                writer.WriteString(context.GetLazyString(field.Name));
                writer.WriteComma();

                writer.WritePropertyName(context.GetLazyString(nameof(field.IsGroupBy)));
                writer.WriteBool(field.IsGroupBy);
                writer.WriteComma();

                writer.WritePropertyName(context.GetLazyString(nameof(field.OperationType)));
                writer.WriteString(context.GetLazyString(field.OperationType.ToString()));
                writer.WriteComma();

                writer.WriteEndObject();
            }
            writer.WriteEndArray();
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(query.FieldsToFetch)));
            if (query.FieldsToFetch != null)
            {
                writer.WriteStartArray();

                isFirstInternal = true;
                foreach (var field in query.FieldsToFetch)
                {
                    if (isFirstInternal == false) writer.WriteComma();

                    isFirstInternal = false;

                    writer.WriteString(context.GetLazyString(field));
                }

                writer.WriteEndArray();
            }
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(query.HighlightedFields)));
            writer.WriteStartArray();
            if (query.HighlightedFields != null)
            {
                isFirstInternal = true;
                foreach (var field in query.HighlightedFields)
                {
                    if (isFirstInternal == false)
                        writer.WriteComma();

                    isFirstInternal = false;

                    writer.WriteStartObject();

                    writer.WritePropertyName(context.GetLazyString(nameof(field.Field)));
                    writer.WriteString(context.GetLazyString(field.Field));
                    writer.WriteComma();

                    writer.WritePropertyName(context.GetLazyString(nameof(field.FragmentCount)));
                    writer.WriteInteger(field.FragmentCount);
                    writer.WriteComma();

                    writer.WritePropertyName(context.GetLazyString(nameof(field.FragmentLength)));
                    writer.WriteInteger(field.FragmentLength);
                    writer.WriteComma();

                    writer.WritePropertyName(context.GetLazyString(nameof(field.FragmentsField)));
                    writer.WriteString(context.GetLazyString(field.FragmentsField));

                    writer.WriteEndObject();
                }
            }
            writer.WriteEndArray();
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(query.HighlighterPostTags)));
            writer.WriteStartArray();
            if (query.HighlighterPostTags != null)
            {
                isFirstInternal = true;
                foreach (var tag in query.HighlighterPostTags)
                {
                    if (isFirstInternal == false)
                        writer.WriteComma();

                    isFirstInternal = false;

                    writer.WriteString(context.GetLazyString(tag));
                }
            }
            writer.WriteEndArray();
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(query.HighlighterPreTags)));
            writer.WriteStartArray();
            if (query.HighlighterPreTags != null)
            {
                isFirstInternal = true;
                foreach (var tag in query.HighlighterPreTags)
                {
                    if (isFirstInternal == false)
                        writer.WriteComma();

                    isFirstInternal = false;

                    writer.WriteString(context.GetLazyString(tag));
                }
            }
            writer.WriteEndArray();
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(query.SortedFields)));
            writer.WriteStartArray();
            if (query.SortedFields != null)
            {
                isFirstInternal = true;
                foreach (var field in query.SortedFields)
                {
                    if (isFirstInternal == false)
                        writer.WriteComma();

                    isFirstInternal = false;

                    writer.WriteStartObject();

                    writer.WritePropertyName(context.GetLazyString(nameof(field.Field)));
                    writer.WriteString(context.GetLazyString(field.Field));
                    writer.WriteComma();

                    writer.WritePropertyName(context.GetLazyString(nameof(field.Descending)));
                    writer.WriteBool(field.Descending);
                    writer.WriteComma();

                    writer.WriteEndObject();
                }
            }
            writer.WriteEndArray();
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(query.TransformerParameters)));
            writer.WriteStartObject();
            if (query.TransformerParameters != null)
            {
                isFirstInternal = true;
                foreach (var kvp in query.TransformerParameters)
                {
                    if (isFirstInternal == false)
                        writer.WriteComma();

                    isFirstInternal = false;

                    writer.WritePropertyName(context.GetLazyString(nameof(kvp.Key)));
                    writer.WriteString(context.GetLazyString(kvp.Value.ToString(Formatting.Indented)));
                }
            }
            writer.WriteEndObject();

            writer.WriteEndObject();
        }

        public static void WriteDatabaseStatistics(this BlittableJsonTextWriter writer, JsonOperationContext context, DatabaseStatistics statistics)
        {
            writer.WriteStartObject();

            writer.WritePropertyName(context.GetLazyString(nameof(statistics.CountOfIndexes)));
            writer.WriteInteger(statistics.CountOfIndexes);
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(statistics.ApproximateTaskCount)));
            writer.WriteInteger(statistics.ApproximateTaskCount);
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(statistics.CountOfDocuments)));
            writer.WriteInteger(statistics.CountOfDocuments);
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(statistics.CountOfTransformers)));
            writer.WriteInteger(statistics.CountOfTransformers);
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(statistics.CurrentNumberOfItemsToIndexInSingleBatch)));
            writer.WriteInteger(statistics.CurrentNumberOfItemsToIndexInSingleBatch);
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(statistics.CurrentNumberOfItemsToReduceInSingleBatch)));
            writer.WriteInteger(statistics.CurrentNumberOfItemsToReduceInSingleBatch);
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(statistics.CurrentNumberOfParallelTasks)));
            writer.WriteInteger(statistics.CurrentNumberOfParallelTasks);
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(statistics.DatabaseId)));
            writer.WriteString(context.GetLazyString(statistics.DatabaseId.ToString()));
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(statistics.Is64Bit)));
            writer.WriteBool(statistics.Is64Bit);
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(statistics.LastDocEtag)));
            if (statistics.LastDocEtag.HasValue)
                writer.WriteInteger(statistics.LastDocEtag.Value);
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(statistics.Indexes)));
            writer.WriteStartArray();
            var isFirstInternal = true;
            foreach (var index in statistics.Indexes)
            {
                if (isFirstInternal == false)
                    writer.WriteComma();

                isFirstInternal = false;

                writer.WriteStartObject();

                writer.WritePropertyName(context.GetLazyString(nameof(index.IsStale)));
                writer.WriteBool(index.IsStale);
                writer.WriteComma();

                writer.WritePropertyName(context.GetLazyString(nameof(index.Name)));
                writer.WriteString(context.GetLazyString(index.Name));
                writer.WriteComma();

                writer.WritePropertyName(context.GetLazyString(nameof(index.IndexId)));
                writer.WriteInteger(index.IndexId);
                writer.WriteComma();

                writer.WritePropertyName(context.GetLazyString(nameof(index.LockMode)));
                writer.WriteString(context.GetLazyString(index.LockMode.ToString()));
                writer.WriteComma();

                writer.WritePropertyName(context.GetLazyString(nameof(index.Priority)));
                writer.WriteString(context.GetLazyString(index.Priority.ToString()));

                writer.WriteEndObject();
            }
            writer.WriteEndArray();

            writer.WriteEndObject();
        }


        public static void WriteIndexDefinition(this BlittableJsonTextWriter writer, JsonOperationContext context, IndexDefinition indexDefinition)
        {
            writer.WriteStartObject();

            writer.WritePropertyName(context.GetLazyString(nameof(indexDefinition.Name)));
            writer.WriteString(context.GetLazyString(indexDefinition.Name));
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(indexDefinition.IndexId)));
            writer.WriteInteger(indexDefinition.IndexId);
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(indexDefinition.Type)));
            writer.WriteString(context.GetLazyString(indexDefinition.Type.ToString()));
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(indexDefinition.IsTestIndex)));
            writer.WriteBool(indexDefinition.IsTestIndex);
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(indexDefinition.LockMode)));
            writer.WriteString(context.GetLazyString(indexDefinition.LockMode.ToString()));
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(indexDefinition.MaxIndexOutputsPerDocument)));
            if (indexDefinition.MaxIndexOutputsPerDocument.HasValue)
                writer.WriteInteger(indexDefinition.MaxIndexOutputsPerDocument.Value);
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(indexDefinition.IndexVersion)));
            if (indexDefinition.IndexVersion.HasValue)
                writer.WriteInteger(indexDefinition.IndexVersion.Value);
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(indexDefinition.IsSideBySideIndex)));
            writer.WriteBool(indexDefinition.IsSideBySideIndex);
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(indexDefinition.IsTestIndex)));
            writer.WriteBool(indexDefinition.IsTestIndex);
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(indexDefinition.Reduce)));
            if (string.IsNullOrWhiteSpace(indexDefinition.Reduce) == false)
                writer.WriteString(context.GetLazyString(indexDefinition.Reduce));
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(indexDefinition.Maps)));
            writer.WriteStartArray();
            var isFirstInternal = true;
            foreach (var map in indexDefinition.Maps)
            {
                if (isFirstInternal == false)
                    writer.WriteComma();

                isFirstInternal = false;
                writer.WriteString(context.GetLazyString(map));
            }
            writer.WriteEndArray();
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(indexDefinition.Fields)));
            writer.WriteStartObject();
            isFirstInternal = true;
            foreach (var kvp in indexDefinition.Fields)
            {
                if (isFirstInternal == false)
                    writer.WriteComma();

                isFirstInternal = false;
                writer.WritePropertyName(context.GetLazyString(kvp.Key));
                if (kvp.Value != null)
                    writer.WriteIndexFieldOptions(context, kvp.Value);
                else
                    writer.WriteNull();
            }
            writer.WriteEndObject();

            writer.WriteEndObject();
        }

        public static void WriteIndexStats(this BlittableJsonTextWriter writer, JsonOperationContext context, IndexStats stats)
        {
            writer.WriteStartObject();

            writer.WritePropertyName(context.GetLazyString(nameof(stats.ForCollections)));
            writer.WriteStartArray();
            var isFirst = true;
            foreach (var collection in stats.ForCollections)
            {
                if (isFirst == false)
                    writer.WriteComma();

                isFirst = false;
                writer.WriteString(context.GetLazyString(collection));
            }
            writer.WriteEndArray();
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(stats.IsInMemory)));
            writer.WriteBool(stats.IsInMemory);
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(stats.LastIndexedEtags)));
            writer.WriteStartObject();
            isFirst = true;
            foreach (var kvp in stats.LastIndexedEtags)
            {
                if (isFirst == false)
                    writer.WriteComma();

                isFirst = false;

                writer.WritePropertyName(context.GetLazyString(kvp.Key));
                writer.WriteInteger(kvp.Value);
            }
            writer.WriteEndObject();
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(stats.LastIndexingTime)));
            if (stats.LastIndexingTime.HasValue)
                writer.WriteString(context.GetLazyString(stats.LastIndexingTime.Value.GetDefaultRavenFormat(isUtc: true)));
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(stats.LastQueryingTime)));
            if (stats.LastQueryingTime.HasValue)
                writer.WriteString(context.GetLazyString(stats.LastQueryingTime.Value.GetDefaultRavenFormat(isUtc: true)));
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(stats.LockMode)));
            writer.WriteString(context.GetLazyString(stats.LockMode.ToString()));
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(stats.Name)));
            writer.WriteString(context.GetLazyString(stats.Name));
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(stats.Priority)));
            writer.WriteString(context.GetLazyString(stats.Priority.ToString()));
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(stats.Type)));
            writer.WriteString(context.GetLazyString(stats.Type.ToString()));
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(stats.CreatedTimestamp)));
            writer.WriteString(context.GetLazyString(stats.CreatedTimestamp.GetDefaultRavenFormat(isUtc: true)));
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(stats.EntriesCount)));
            writer.WriteInteger(stats.EntriesCount);
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(stats.Id)));
            writer.WriteInteger(stats.Id);
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(stats.MapAttempts)));
            writer.WriteInteger(stats.MapAttempts);
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(stats.MapErrors)));
            writer.WriteInteger(stats.MapErrors);
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(stats.MapSuccesses)));
            writer.WriteInteger(stats.MapSuccesses);
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(stats.ErrorsCount)));
            writer.WriteInteger(stats.ErrorsCount);
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(stats.IsTestIndex)));
            writer.WriteBool(stats.IsTestIndex);

            writer.WriteEndObject();
        }

        private static void WriteIndexFieldOptions(this BlittableJsonTextWriter writer, JsonOperationContext context, IndexFieldOptions options)
        {
            writer.WriteStartObject();

            writer.WritePropertyName(context.GetLazyString(nameof(options.Analyzer)));
            if (string.IsNullOrWhiteSpace(options.Analyzer) == false)
                writer.WriteString(context.GetLazyString(options.Analyzer));
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(options.Indexing)));
            if (options.Indexing.HasValue)
                writer.WriteString(context.GetLazyString(options.Indexing.ToString()));
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(options.Sort)));
            if (options.Sort.HasValue)
                writer.WriteString(context.GetLazyString(options.Sort.ToString()));
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(options.Storage)));
            if (options.Storage.HasValue)
                writer.WriteString(context.GetLazyString(options.Storage.ToString()));
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(options.Suggestions)));
            if (options.Suggestions.HasValue)
                writer.WriteBool(options.Suggestions.Value);
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(options.TermVector)));
            if (options.TermVector.HasValue)
                writer.WriteString(context.GetLazyString(options.TermVector.ToString()));
            else
                writer.WriteNull();
            writer.WriteComma();

            writer.WritePropertyName(context.GetLazyString(nameof(options.Spatial)));
            if (options.Spatial != null)
            {
                writer.WriteStartObject();

                writer.WritePropertyName(context.GetLazyString(nameof(options.Spatial.Type)));
                writer.WriteString(context.GetLazyString(options.Spatial.Type.ToString()));
                writer.WriteComma();

                writer.WritePropertyName(context.GetLazyString(nameof(options.Spatial.MaxTreeLevel)));
                writer.WriteInteger(options.Spatial.MaxTreeLevel);
                writer.WriteComma();

                writer.WritePropertyName(context.GetLazyString(nameof(options.Spatial.MaxX)));
                writer.WriteDouble(new LazyDoubleValue(context.GetLazyString(options.Spatial.MaxX.ToInvariantString())));
                writer.WriteComma();

                writer.WritePropertyName(context.GetLazyString(nameof(options.Spatial.MaxY)));
                writer.WriteDouble(new LazyDoubleValue(context.GetLazyString(options.Spatial.MaxY.ToInvariantString())));
                writer.WriteComma();

                writer.WritePropertyName(context.GetLazyString(nameof(options.Spatial.MinX)));
                writer.WriteDouble(new LazyDoubleValue(context.GetLazyString(options.Spatial.MinX.ToInvariantString())));
                writer.WriteComma();

                writer.WritePropertyName(context.GetLazyString(nameof(options.Spatial.MinY)));
                writer.WriteDouble(new LazyDoubleValue(context.GetLazyString(options.Spatial.MinY.ToInvariantString())));
                writer.WriteComma();

                writer.WritePropertyName(context.GetLazyString(nameof(options.Spatial.Strategy)));
                writer.WriteString(context.GetLazyString(options.Spatial.Strategy.ToString()));
                writer.WriteComma();

                writer.WritePropertyName(context.GetLazyString(nameof(options.Spatial.Units)));
                writer.WriteString(context.GetLazyString(options.Spatial.Units.ToString()));

                writer.WriteEndObject();
            }
            else
                writer.WriteNull();

            writer.WriteEndObject();
        }

        public static long WriteDocuments(this BlittableJsonTextWriter writer, JsonOperationContext context, IEnumerable<Document> documents, bool metadataOnly)
        {
            writer.WriteStartArray();

            var first = true;
            Document lastDocument = null;
            foreach (var document in documents)
            {
                if (document == null)
                    continue;

                using (document.Data)
                {
                    if (first == false)
                        writer.WriteComma();
                    first = false;

                    writer.WriteDocument(context, document, metadataOnly);
                }
                lastDocument = document;
            }

            writer.WriteEndArray();
            return lastDocument?.Etag ?? 0;
        }

        public static void WriteDocuments(this BlittableJsonTextWriter writer, JsonOperationContext context, List<Document> documents, bool metadataOnly, int start, int count)
        {
            writer.WriteStartArray();

            bool first = true;
            for (int index = start, written = 0; written < count; index++, written++)
            {
                var document = documents[index];
                if (document == null)
                    continue;

                using (document.Data)
                {
                    if (first == false)
                        writer.WriteComma();
                    first = false;

                    writer.WriteDocument(context, document, metadataOnly);
                }
            }

            writer.WriteEndArray();
        }

        public static void WriteDocument(this BlittableJsonTextWriter writer, JsonOperationContext context, Document document, bool metadataOnly)
        {
            document.EnsureMetadata();
            if (metadataOnly)
                document.RemoveAllPropertiesExceptMetadata();

            context.Write(writer, document.Data);
        }
    }
}