﻿using System;
using System.Collections.Generic;
using System.Globalization;
using System.Net;
using System.Threading;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Logging;
using Raven.Database.Util;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Workers;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;

using Sparrow.Json;
using Sparrow.Json.Parsing;
using Voron;
using Voron.Data.BTrees;
using Voron.Data.Tables;
using Voron.Impl;

namespace Raven.Server.Documents.Indexes.MapReduce
{
    public unsafe class ReduceMapResults : IIndexingWork
    {
        protected readonly ILog Log = LogManager.GetLogger(typeof(ReduceMapResults));

        private readonly List<BlittableJsonReaderObject> _aggregationBatch = new List<BlittableJsonReaderObject>();
        private readonly AutoMapReduceIndexDefinition _indexDefinition;
        private readonly IndexStorage _indexStorage;
        private readonly MetricsCountersManager _metrics;
        private readonly MapReduceIndexingContext _mapReduceContext;

        private readonly TableSchema _reduceResultsSchema = new TableSchema()
            .DefineKey(new TableSchema.SchemaIndexDef
            {
                Name = "PageNumber",
                StartIndex = 0,
                Count = 1
            });

        public ReduceMapResults(AutoMapReduceIndexDefinition indexDefinition, IndexStorage indexStorage, MetricsCountersManager metrics, MapReduceIndexingContext mapReduceContext)
        {
            _indexDefinition = indexDefinition;
            _indexStorage = indexStorage;
            _metrics = metrics;
            _mapReduceContext = mapReduceContext;
        }

        public string Name => "Reduce";

        public bool Execute(DocumentsOperationContext databaseContext, TransactionOperationContext indexContext, Lazy<IndexWriteOperation> writeOperation,
                            IndexingStatsScope stats, CancellationToken token)
        {
            if (_mapReduceContext.StateByReduceKeyHash.Count == 0)
                return false;

            _aggregationBatch.Clear();

            _reduceResultsSchema.Create(indexContext.Transaction.InnerTransaction, "PageNumberToReduceResult");
            var table = new Table(_reduceResultsSchema, "PageNumberToReduceResult", indexContext.Transaction.InnerTransaction);

            var lowLevelTransaction = indexContext.Transaction.InnerTransaction.LowLevelTransaction;
            var parentPagesToAggregate = new Dictionary<long, Tree>();

            var writer = writeOperation.Value;

            foreach (var state in _mapReduceContext.StateByReduceKeyHash)
            {
                var reduceKeyHash = indexContext.GetLazyString(state.Key.ToString(CultureInfo.InvariantCulture));
                var modifiedState = state.Value;

                foreach (var modifiedPage in modifiedState.ModifiedPages)
                {
                    token.ThrowIfCancellationRequested();

                    if (modifiedState.FreedPages.Contains(modifiedPage))
                        continue;

                    var page = lowLevelTransaction.GetPage(modifiedPage).ToTreePage();
                    if (page.IsLeaf == false)
                        continue;

                    if (page.NumberOfEntries == 0)
                    {
                        if (page.PageNumber != modifiedState.Tree.State.RootPageNumber)
                        {
                            throw new InvalidOperationException($"Encountered empty page which isn't a root. Page #{page.PageNumber} in '{modifiedState.Tree.Name}' tree.");
                        }

                        writer.DeleteReduceResult(reduceKeyHash, stats);

                        var emptyPageNumber = page.PageNumber;
                        table.DeleteByKey(Slice.External(indexContext.Allocator, (byte*)&emptyPageNumber, sizeof(long)));

                        continue;
                    }

                    var parentPage = modifiedState.Tree.GetParentPageOf(page);

                    stats.RecordReduceAttempts(page.NumberOfEntries);

                    try
                    {
                        using (var result = AggregateLeafPage(page, lowLevelTransaction, table, indexContext))
                        {
                            if (parentPage == -1)
                            {
                                writer.DeleteReduceResult(reduceKeyHash, stats);

                                writer.IndexDocument(reduceKeyHash, new Document
                                {
                                    Key = reduceKeyHash,
                                    Data = result
                                }, stats);

                                _metrics.MapReduceReducedPerSecond.Mark(page.NumberOfEntries);

                                stats.RecordReduceSuccesses(page.NumberOfEntries);
                            }
                            else
                            {
                                parentPagesToAggregate[parentPage] = modifiedState.Tree;
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        var message = $"Failed to execute reduce function for reduce key '{modifiedState.Tree.Name}' on a leaf page #{page} of '{_indexDefinition.Name}' index.";

                        if (Log.IsWarnEnabled)
                            Log.WarnException(message, e);

                        if (parentPage == -1)
                        {
                            stats.RecordReduceErrors(page.NumberOfEntries);
                            stats.AddReduceError(message + $" Message: {message}.");
                        }
                    }
                }

                long tmp = 0;
                var pageNumberSlice = Slice.External(indexContext.Allocator, (byte*)&tmp, sizeof(long));
                foreach (var freedPage in modifiedState.FreedPages)
                {
                    tmp = freedPage;
                    table.DeleteByKey(pageNumberSlice);
                }

                while (parentPagesToAggregate.Count > 0)
                {
                    token.ThrowIfCancellationRequested();

                    var other = parentPagesToAggregate;
                    parentPagesToAggregate = new Dictionary<long, Tree>();

                    foreach (var kvp in other)
                    {
                        var pageNumber = kvp.Key;
                        var tree = kvp.Value;
                        var page = lowLevelTransaction.GetPage(pageNumber).ToTreePage();

                        if (page.IsBranch == false)
                        {
                            throw new InvalidOperationException("Parent page was found that wasn't a branch, error at " + page.PageNumber);
                        }

                        var parentPage = tree.GetParentPageOf(page);

                        int aggregatedEntries = 0;
                        
                        try
                        {
                            using (var result = AggregateBranchPage(page, table, indexContext, out aggregatedEntries))
                            {
                                if (parentPage == -1)
                                {
                                    writer.DeleteReduceResult(reduceKeyHash, stats);

                                    writer.IndexDocument(reduceKeyHash, new Document
                                    {
                                        Key = reduceKeyHash,
                                        Data = result
                                    }, stats);

                                    _metrics.MapReduceReducedPerSecond.Mark(aggregatedEntries);

                                    stats.RecordReduceSuccesses(aggregatedEntries);
                                }
                                else
                                {
                                    parentPagesToAggregate[parentPage] = tree;
                                }
                            }
                        }
                        catch (Exception e)
                        {
                            var message = $"Failed to execute reduce function for reduce key '{modifiedState.Tree.Name}' on a branch page #{page} of '{_indexDefinition.Name}' index.";

                            if (Log.IsWarnEnabled)
                                Log.WarnException(message, e);

                            stats.RecordReduceErrors(aggregatedEntries);
                            stats.AddReduceError(message + $" Message: {message}.");
                        }
                    }
                }
            }

            foreach (var lastEtag in _mapReduceContext.ProcessedDocEtags)
            {
                _indexStorage.WriteLastIndexedEtag(indexContext.Transaction, lastEtag.Key, lastEtag.Value);
            }

            foreach (var lastEtag in _mapReduceContext.ProcessedTombstoneEtags)
            {
                _indexStorage.WriteLastTombstoneEtag(indexContext.Transaction, lastEtag.Key, lastEtag.Value);
            }

            return false;
        }

        private BlittableJsonReaderObject AggregateLeafPage(TreePage page, LowLevelTransaction lowLevelTransaction, Table table, TransactionOperationContext indexContext)
        {
            for (int i = 0; i < page.NumberOfEntries; i++)
            {
                var valueReader = TreeNodeHeader.Reader(lowLevelTransaction, page.GetNode(i));
                var reduceEntry = new BlittableJsonReaderObject(valueReader.Base, valueReader.Length, indexContext);

                _aggregationBatch.Add(reduceEntry);
            }

            return AggregateBatchResults(page.PageNumber, page.NumberOfEntries, table, indexContext);
        }

        private BlittableJsonReaderObject AggregateBranchPage(TreePage page, Table table, TransactionOperationContext indexContext, out int aggregatedEntries)
        {
            aggregatedEntries = 0;

            for (int i = 0; i < page.NumberOfEntries; i++)
            {
                var childPageNumber = IPAddress.HostToNetworkOrder(page.GetNode(i)->PageNumber);
                var tvr = table.ReadByKey(Slice.External(indexContext.Allocator, (byte*)&childPageNumber, sizeof(long)));
                if (tvr == null)
                {
                    throw new InvalidOperationException("Couldn't find pre-computed results for existing page " + childPageNumber);
                }

                int size;
                _aggregationBatch.Add(new BlittableJsonReaderObject(tvr.Read(1, out size), size, indexContext));

                aggregatedEntries += *(int*)tvr.Read(2, out size);
            }

            return AggregateBatchResults(page.PageNumber, aggregatedEntries, table, indexContext);
        }

        private BlittableJsonReaderObject AggregateBatchResults(long modifiedPage, int aggregatedEntries, Table table, TransactionOperationContext indexContext)
        {
            var aggregatedResult = new Dictionary<string, PropertyResult>();

            foreach (var obj in _aggregationBatch)
            {
                foreach (var propertyName in obj.GetPropertyNames())
                {
                    string stringValue;

                    IndexField indexField;
                    if (_indexDefinition.MapFields.TryGetValue(propertyName, out indexField))
                    {
                        switch (indexField.MapReduceOperation)
                        {
                            case FieldMapReduceOperation.Count:
                            case FieldMapReduceOperation.Sum:
                                object value;

                                if (obj.TryGetMember(propertyName, out value) == false)
                                    throw new InvalidOperationException($"Could not read numeric value of '{propertyName}' property");

                                double doubleValue;
                                long longValue;

                                var numberType = BlittableNumber.Parse(value, out doubleValue, out longValue);

                                PropertyResult aggregate;
                                if (aggregatedResult.TryGetValue(propertyName, out aggregate) == false)
                                {
                                    var propertyResult = new PropertyResult();

                                    switch (numberType)
                                    {
                                        case NumberParseResult.Double:
                                            propertyResult.ResultValue = doubleValue;
                                            propertyResult.DoubleSumValue = doubleValue;
                                            break;
                                        case NumberParseResult.Long:
                                            propertyResult.ResultValue = longValue;
                                            propertyResult.LongSumValue = longValue;
                                            break;
                                    }

                                    aggregatedResult[propertyName] = propertyResult;
                                }
                                else
                                {
                                    switch (numberType)
                                    {
                                        case NumberParseResult.Double:
                                            aggregate.ResultValue = aggregate.DoubleSumValue += doubleValue;
                                            break;
                                        case NumberParseResult.Long:
                                            aggregate.ResultValue = aggregate.LongSumValue += longValue;
                                            break;
                                    };
                                }
                                break;
                            //case FieldMapReduceOperation.None:
                            default:
                                throw new ArgumentOutOfRangeException($"Unhandled field type '{indexField.MapReduceOperation}' to aggregate on");
                        }
                    }
                    else if (obj.TryGet(propertyName, out stringValue))
                    {
                        if (aggregatedResult.ContainsKey(propertyName) == false)
                        {
                            aggregatedResult[propertyName] = new PropertyResult
                            {
                                ResultValue = stringValue
                            };
                        }
                    }
                }
            }

            _aggregationBatch.Clear();

            var djv = new DynamicJsonValue();

            foreach (var aggregate in aggregatedResult)
            {
                djv[aggregate.Key] = aggregate.Value.ResultValue;
            }

            var resultObj = indexContext.ReadObject(djv, "map/reduce");

            var pageNumber = IPAddress.HostToNetworkOrder(modifiedPage);
            
            table.Set(new TableValueBuilder
                {
                    {(byte*) &pageNumber, sizeof (long)},
                    {resultObj.BasePointer, resultObj.Size},
                    {(byte*) &aggregatedEntries, sizeof (int)}
                });

            return resultObj;
        }

        private class PropertyResult
        {
            public object ResultValue;

            public long LongSumValue = 0;

            public double DoubleSumValue = 0;
        }
    }
}