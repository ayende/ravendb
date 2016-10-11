﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Threading;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Indexes.Workers;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Logging;
using Voron;
using Voron.Data.BTrees;
using Voron.Data.Tables;
using Voron.Impl;
using Raven.Client.Data.Indexes;

namespace Raven.Server.Documents.Indexes.MapReduce
{
    public abstract unsafe class ReduceMapResultsBase<T> : IIndexingWork where T : IndexDefinitionBase
    {
        public static readonly Slice PageNumberSlice;
        private Logger _logger;
        private readonly List<BlittableJsonReaderObject> _aggregationBatch = new List<BlittableJsonReaderObject>();
        private readonly Index _index;
        protected readonly T _indexDefinition;
        private readonly IndexStorage _indexStorage;
        private readonly MetricsCountersManager _metrics;
        private readonly MapReduceIndexingContext _mapReduceContext;

        private readonly TableSchema _reduceResultsSchema = new TableSchema()
            .DefineKey(new TableSchema.SchemaIndexDef
            {
                StartIndex = 0,
                Count = 1,
                Name = PageNumberSlice
            });

        protected ReduceMapResultsBase(Index index, T indexDefinition, IndexStorage indexStorage, MetricsCountersManager metrics, MapReduceIndexingContext mapReduceContext)
        {
            _index = index;
            _indexDefinition = indexDefinition;
            _indexStorage = indexStorage;
            _metrics = metrics;
            _mapReduceContext = mapReduceContext;
            _logger = LoggingSource.Instance.GetLogger<ReduceMapResultsBase<T>>(indexStorage.DocumentDatabase.Name);
        }

        static ReduceMapResultsBase()
        {
            Slice.From(StorageEnvironment.LabelsContext, "PageNumber", ByteStringType.Immutable, out PageNumberSlice);
        }

        public string Name => "Reduce";

        public bool Execute(DocumentsOperationContext databaseContext, TransactionOperationContext indexContext, Lazy<IndexWriteOperation> writeOperation,
                            IndexingStatsScope stats, CancellationToken token)
        {
            if (_mapReduceContext.StoreByReduceKeyHash.Count == 0)
            {
                WriteLastEtags(indexContext); // we need to write etags here, because if we filtered everything during map then we will loose last indexed etag information and this will cause an endless indexing loop
                return false;
            }

            _aggregationBatch.Clear();

            _reduceResultsSchema.Create(indexContext.Transaction.InnerTransaction, "PageNumberToReduceResult");
            var table = indexContext.Transaction.InnerTransaction.OpenTable(_reduceResultsSchema, "PageNumberToReduceResult");

            var lowLevelTransaction = indexContext.Transaction.InnerTransaction.LowLevelTransaction;


            var writer = writeOperation.Value;

            foreach (var store in _mapReduceContext.StoreByReduceKeyHash)
            {
                using (var reduceKeyHash = indexContext.GetLazyString(store.Key.ToString(CultureInfo.InvariantCulture)))
                using (store.Value)
                {
                    var modifiedStore = store.Value;

                    switch (modifiedStore.Type)
                    {
                        case MapResultsStorageType.Tree:
                            using (var scope = stats.For(IndexingOperation.Reduce.TreeScope))
                            {
                                HandleTreeReduction(indexContext, scope, token, modifiedStore, lowLevelTransaction, writer, reduceKeyHash, table);
                            }
                            break;
                        case MapResultsStorageType.Nested:
                            using (var scope = stats.For(IndexingOperation.Reduce.NestedValuesScope))
                            {
                                HandleNestedValuesReduction(indexContext, scope, token, modifiedStore, writer, reduceKeyHash);
                            }
                            break;

                        default:
                            throw new ArgumentOutOfRangeException(modifiedStore.Type.ToString());
                    }
                }
            }

            WriteLastEtags(indexContext);

            return false;
        }

        public bool CanContinueBatch(IndexingStatsScope stats, long currentEtag, long maxEtag)
        {
            throw new NotSupportedException();
        }

        private void WriteLastEtags(TransactionOperationContext indexContext)
        {
            foreach (var lastEtag in _mapReduceContext.ProcessedDocEtags)
            {
                _indexStorage.WriteLastIndexedEtag(indexContext.Transaction, lastEtag.Key, lastEtag.Value);
            }

            foreach (var lastEtag in _mapReduceContext.ProcessedTombstoneEtags)
            {
                _indexStorage.WriteLastTombstoneEtag(indexContext.Transaction, lastEtag.Key, lastEtag.Value);
            }
        }

        private void HandleNestedValuesReduction(TransactionOperationContext indexContext, IndexingStatsScope stats,
                    CancellationToken token, MapReduceResultsStore modifiedStore,
                    IndexWriteOperation writer, LazyStringValue reduceKeyHash)
        {
            var numberOfEntriesToReduce = 0;

            try
            {
                var section = modifiedStore.GetNestedResultsSection();

                if (section.IsModified == false)
                    return;

                using (stats.For(IndexingOperation.Reduce.NestedValuesRead))
                {
                    numberOfEntriesToReduce += section.GetResults(indexContext, _aggregationBatch);
                }

                stats.RecordReduceAttempts(numberOfEntriesToReduce);

                AggregationResult result;
                using (stats.For(IndexingOperation.Reduce.NestedValuesAggregation))
                {
                    result = AggregateOn(_aggregationBatch, indexContext, token);
                }

                if (section.IsNew == false)
                    writer.DeleteReduceResult(reduceKeyHash, stats);

                foreach (var output in result.GetOutputs())
                {
                    writer.IndexDocument(reduceKeyHash, output, stats, indexContext);
                }

                _index.ReducesPerSec.Mark(numberOfEntriesToReduce);
                _metrics.MapReduceReducedPerSecond.Mark(numberOfEntriesToReduce);

                stats.RecordReduceSuccesses(numberOfEntriesToReduce);
            }
            catch (Exception e)
            {
                foreach (var item in _aggregationBatch)
                {
                    item.Dispose();
                }

                var message = $"Failed to execute reduce function for reduce key '{reduceKeyHash}' on nested values of '{_indexDefinition.Name}' index.";

                if (_logger.IsInfoEnabled)
                    _logger.Info(message, e);

                stats.RecordReduceErrors(numberOfEntriesToReduce);
                stats.AddReduceError(message + $"  Exception: {e}");
            }
            finally
            {
                _aggregationBatch.Clear();
            }
        }

        private void HandleTreeReduction(TransactionOperationContext indexContext, IndexingStatsScope stats,
            CancellationToken token, MapReduceResultsStore modifiedStore, LowLevelTransaction lowLevelTransaction,
            IndexWriteOperation writer, LazyStringValue reduceKeyHash, Table table)
        {
            var tree = modifiedStore.Tree;

            var branchesToAggregate = new HashSet<long>();

            var parentPagesToAggregate = new HashSet<long>();

            foreach (var modifiedPage in modifiedStore.ModifiedPages)
            {
                token.ThrowIfCancellationRequested();

                var page = lowLevelTransaction.GetPage(modifiedPage).ToTreePage();

                stats.RecordReduceTreePageModified(page.IsLeaf);

                if (page.IsLeaf == false)
                {
                    Debug.Assert(page.IsBranch);
                    branchesToAggregate.Add(modifiedPage);

                    continue;
                }

                if (page.NumberOfEntries == 0)
                {
                    if (page.PageNumber != tree.State.RootPageNumber)
                    {
                        throw new InvalidOperationException(
                            $"Encountered empty page which isn't a root. Page #{page.PageNumber} in '{tree.Name}' tree.");
                    }

                    writer.DeleteReduceResult(reduceKeyHash, stats);

                    var emptyPageNumber = Bits.SwapBytes(page.PageNumber);
                    Slice pageNumSlice;
                    using (Slice.External(indexContext.Allocator, (byte*)&emptyPageNumber, sizeof(long), out pageNumSlice))
                        table.DeleteByKey(pageNumSlice);

                    continue;
                }

                var parentPage = tree.GetParentPageOf(page);

                stats.RecordReduceAttempts(page.NumberOfEntries);

                try
                {
                    using (var result = AggregateLeafPage(page, lowLevelTransaction, table, indexContext, stats, token))
                    {
                        if (parentPage == -1)
                        {
                            writer.DeleteReduceResult(reduceKeyHash, stats);

                            foreach (var output in result.GetOutputs())
                            {
                                writer.IndexDocument(reduceKeyHash, output, stats, indexContext);
                            }
                        }
                        else
                        {
                            StoreAggregationResult(page.PageNumber, page.NumberOfEntries, table, result, stats);
                            parentPagesToAggregate.Add(parentPage);
                        }

                        _metrics.MapReduceReducedPerSecond.Mark(page.NumberOfEntries);

                        stats.RecordReduceSuccesses(page.NumberOfEntries);
                    }
                }
                catch (Exception e)
                {
                    var message =
                        $"Failed to execute reduce function for reduce key '{tree.Name}' on a leaf page #{page} of '{_indexDefinition.Name}' index.";

                    if (_logger.IsInfoEnabled)
                        _logger.Info(message, e);

                    if (parentPage == -1)
                    {
                        stats.RecordReduceErrors(page.NumberOfEntries);
                        stats.AddReduceError(message + $"  Exception: {e}");
                    }
                }
            }

            long tmp = 0;
            Slice pageNumberSlice;
            using (Slice.External(indexContext.Allocator, (byte*)&tmp, sizeof(long), out pageNumberSlice))
            {
                foreach (var freedPage in modifiedStore.FreedPages)
                {
                    tmp = Bits.SwapBytes(freedPage);
                    table.DeleteByKey(pageNumberSlice);
                }
            }

            while (parentPagesToAggregate.Count > 0 || branchesToAggregate.Count > 0)
            {
                token.ThrowIfCancellationRequested();

                var branchPages = parentPagesToAggregate;
                parentPagesToAggregate = new HashSet<long>();

                foreach (var pageNumber in branchPages)
                {
                    var page = lowLevelTransaction.GetPage(pageNumber).ToTreePage();

                    try
                    {
                        if (page.IsBranch == false)
                        {
                            throw new InvalidOperationException("Parent page was found that wasn't a branch, error at " +
                                                                page.PageNumber);
                        }

                        stats.RecordReduceAttempts(page.NumberOfEntries);

                        var parentPage = tree.GetParentPageOf(page);

                        using (var result = AggregateBranchPage(page, table, indexContext, branchesToAggregate, stats, token))
                        {
                            if (parentPage == -1)
                            {
                                writer.DeleteReduceResult(reduceKeyHash, stats);

                                foreach (var output in result.GetOutputs())
                                {
                                    writer.IndexDocument(reduceKeyHash, output, stats, indexContext);
                                }
                            }
                            else
                            {
                                parentPagesToAggregate.Add(parentPage);

                                StoreAggregationResult(page.PageNumber, page.NumberOfEntries, table, result, stats);
                            }

                            _metrics.MapReduceReducedPerSecond.Mark(page.NumberOfEntries);

                            stats.RecordReduceSuccesses(page.NumberOfEntries);
                        }
                    }
                    catch (Exception e)
                    {
                        var message =
                            $"Failed to execute reduce function for reduce key '{tree.Name}' on a branch page #{page} of '{_indexDefinition.Name}' index.";

                        if (_logger.IsInfoEnabled)
                            _logger.Info(message, e);

                        stats.RecordReduceErrors(page.NumberOfEntries);
                        stats.AddReduceError(message + $" Exception: {e}");
                    }
                    finally
                    {
                        branchesToAggregate.Remove(pageNumber);
                    }
                }

                if (parentPagesToAggregate.Count == 0 && branchesToAggregate.Count > 0)
                {
                    // we still have unaggregated branches which were modified but their children were not modified (branch page splitting) so we missed them
                    parentPagesToAggregate.Add(branchesToAggregate.First());
                }
            }
        }

        private AggregationResult AggregateLeafPage(TreePage page, LowLevelTransaction lowLevelTransaction, Table table, TransactionOperationContext indexContext,
                                                    IndexingStatsScope stats, CancellationToken token)
        {
            using (stats.For(IndexingOperation.Reduce.LeafAggregation))
            {
                for (int i = 0; i < page.NumberOfEntries; i++)
                {
                    var valueReader = TreeNodeHeader.Reader(lowLevelTransaction, page.GetNode(i));
                    var reduceEntry = new BlittableJsonReaderObject(valueReader.Base, valueReader.Length, indexContext);

                    _aggregationBatch.Add(reduceEntry);
                }

                return AggregateBatchResults(_aggregationBatch, indexContext, token);
            }
        }

        private AggregationResult AggregateBranchPage(TreePage page, Table table, TransactionOperationContext indexContext, HashSet<long> remainingBranchesToAggregate,
            IndexingStatsScope stats, CancellationToken token)
        {
            using (stats.For(IndexingOperation.Reduce.BranchAggregation))
            {
                for (int i = 0; i < page.NumberOfEntries; i++)
                {
                    var pageNumber = page.GetNode(i)->PageNumber;
                    var childPageNumber = Bits.SwapBytes(pageNumber);
                    Slice childPageNumberSlice;
                    TableValueReader tvr;
                    using (Slice.External(indexContext.Allocator, (byte*)&childPageNumber, sizeof(long), out childPageNumberSlice))
                    {
                        tvr = table.ReadByKey(childPageNumberSlice);
                        if (tvr == null)
                        {
                            if (remainingBranchesToAggregate.Contains(pageNumber))
                            {
                                // we have a modified branch page but its children were not modified (branch page splitting) so we didn't aggregated it yet, let's do it now

                                try
                                {
                                    var unaggregatedPage = indexContext.Transaction.InnerTransaction.LowLevelTransaction.GetPage(pageNumber).ToTreePage();

                                    using (var result = AggregateBranchPage(unaggregatedPage, table, indexContext, remainingBranchesToAggregate, stats, token))
                                    {
                                        StoreAggregationResult(unaggregatedPage.PageNumber, page.NumberOfEntries, table, result, stats);
                                    }
                                }
                                finally
                                {
                                    remainingBranchesToAggregate.Remove(pageNumber);
                                }

                                tvr = table.ReadByKey(childPageNumberSlice);
                            }
                            else
                            {
                                throw new InvalidOperationException("Couldn't find pre-computed results for existing page " + pageNumber);
                            }
                        }

                        int size;
                        var numberOfResults = *(int*)tvr.Read(2, out size);

                        for (int j = 0; j < numberOfResults; j++)
                        {
                            _aggregationBatch.Add(new BlittableJsonReaderObject(tvr.Read(3 + j, out size), size, indexContext));
                        }
                    }
                }

                return AggregateBatchResults(_aggregationBatch, indexContext, token);
            }
        }

        private AggregationResult AggregateBatchResults(List<BlittableJsonReaderObject> aggregationBatch, TransactionOperationContext indexContext, CancellationToken token)
        {
            AggregationResult result;

            try
            {
                result = AggregateOn(aggregationBatch, indexContext, token);
            }
            finally
            {
                aggregationBatch.Clear();
            }

            return result;
        }

        private void StoreAggregationResult(long modifiedPage, int aggregatedEntries, Table table, AggregationResult result, IndexingStatsScope stats)
        {
            using (stats.For(IndexingOperation.Reduce.StoringReduceResult))
            {
                var pageNumber = Bits.SwapBytes(modifiedPage);
                var numberOfOutputs = result.Count;

                var tvb = new TableValueBuilder
                {
                    pageNumber,
                    aggregatedEntries,
                    numberOfOutputs
                };

                foreach (var output in result.GetOutputsToStore())
                {
                    tvb.Add(output.BasePointer, output.Size);
                }

                table.Set(tvb);
            }
        }

        protected abstract AggregationResult AggregateOn(List<BlittableJsonReaderObject> aggregationBatch, TransactionOperationContext indexContext, CancellationToken token);
    }
}