﻿// -----------------------------------------------------------------------
//  <copyright file="Compaction.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading;
using Sparrow.Platform;
using Sparrow.Server;
using Sparrow.Threading;
using Sparrow.Utils;
using Voron.Data;
using Voron.Data.BTrees;
using Voron.Data.Fixed;
using Voron.Data.Tables;
using Voron.Global;
using Voron.Impl.FreeSpace;
using Voron.Impl.Journal;

namespace Voron.Impl.Compaction
{
    public sealed class StorageCompactionProgress
    {
        public string TreeName;
        public long TreeProgress;
        public long TreeTotal;

        public long GlobalProgress;
        public long GlobalTotal;
        public string Message;
    }

    public static unsafe class StorageCompaction
    {
        public const string CannotCompactBecauseOfIncrementalBackup = "Cannot compact a storage that supports incremental backups. The compact operation changes internal data structures on which the incremental backup relays.";

        public static void Execute(StorageEnvironmentOptions srcOptions,
            StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions compactOptions,
            Action<StorageCompactionProgress> progressReport = null,
            Action<string, TableSchema> modifyTableSchema = null,
            CancellationToken token = default(CancellationToken))
        {
            if (srcOptions.IncrementalBackupEnabled)
                throw new InvalidOperationException(CannotCompactBecauseOfIncrementalBackup);

            long minimalCompactedDataFileSize;

            srcOptions.ManualFlushing = true; // prevent from flushing during compaction - we shouldn't touch any source files
            compactOptions.ManualFlushing = true; // let us flush manually during data copy

            using (var existingEnv = new StorageEnvironment(srcOptions))
            using (var compactedEnv = new StorageEnvironment(compactOptions))
            {
                CopyTrees(existingEnv, compactedEnv, modifyTableSchema, progressReport, token);

                compactedEnv.FlushLogToDataFile();
                bool synced;

                const int maxNumberOfRetries = 100;

                var syncRetries = 0;

                while (true)
                {
                    token.ThrowIfCancellationRequested();
                    using (var op = new WriteAheadJournal.JournalApplicator.SyncOperation(compactedEnv.Journal.Applicator))
                    {
                        try
                        {

                            synced = op.SyncDataFile();

                            if (synced || ++syncRetries >= maxNumberOfRetries)
                                break;

                            Thread.Sleep(100);
                        }
                        catch (Exception e)
                        {
                            existingEnv.Options.SetCatastrophicFailure(ExceptionDispatchInfo.Capture(e));
                            throw;
                        }
                    }
                }

                if (synced)
                    compactedEnv.Journal.Applicator.DeleteCurrentAlreadyFlushedJournal();

                compactedEnv.Cleanup();

                minimalCompactedDataFileSize = compactedEnv.NextPageNumber * Constants.Storage.PageSize;
            }

            using (var compactedDataFile = SafeFileStream.Create(compactOptions.BasePath.Combine(Constants.DatabaseFilename).FullPath, FileMode.Open, FileAccess.ReadWrite))
            {
                compactedDataFile.SetLength(minimalCompactedDataFileSize);
            }
        }

        private static void CopyTrees(StorageEnvironment existingEnv, StorageEnvironment compactedEnv, 
            Action<string, TableSchema> modifyTableSchema,
            Action<StorageCompactionProgress> progressReport, 
            CancellationToken token)
        {
            var context = new TransactionPersistentContext(true);
            using (var txr = existingEnv.ReadTransaction(context))
            using (var rootIterator = txr.LowLevelTransaction.RootObjects.Iterate(false))
            {
                if (rootIterator.Seek(Slices.BeforeAllKeys) == false)
                    return;

                var globalTableIndexesToSkipCopying = new HashSet<string>
                {
                    TableSchema.CompressionDictionariesSlice.ToString()
                };

                do
                {
                    var objectType = txr.GetRootObjectType(rootIterator.CurrentKey);
                    if (objectType != RootObjectType.Table)
                        continue;

                    var treeName = rootIterator.CurrentKey.ToString();
                    var tableTree = txr.ReadTree(treeName, RootObjectType.Table);
                    var schemaSize = tableTree.GetDataSize(TableSchema.SchemasSlice);
                    var schemaPtr = tableTree.DirectRead(TableSchema.SchemasSlice);
                    var schema = TableSchema.ReadFrom(txr.Allocator, schemaPtr, schemaSize);

                    if (schema.Key != null && schema.Key.IsGlobal)
                    {
                        globalTableIndexesToSkipCopying.Add(schema.Key.Name.ToString());
                    }
                    foreach (var index in schema.Indexes.Values)
                    {
                        if (index.IsGlobal)
                            globalTableIndexesToSkipCopying.Add(index.Name.ToString());
                    }
                    foreach (var index in schema.FixedSizeIndexes.Values)
                    {
                        if (index.IsGlobal)
                            globalTableIndexesToSkipCopying.Add(index.Name.ToString());
                    }

                } while (rootIterator.MoveNext());

                if (rootIterator.Seek(Slices.BeforeAllKeys) == false)
                    return;
                
                // substract skipped items  
                var totalTreesCount = txr.LowLevelTransaction.RootObjects.ReadHeader().NumberOfEntries - globalTableIndexesToSkipCopying.Count;
                var copiedTrees = 0L;
                do
                {
                    token.ThrowIfCancellationRequested();
                    var treeName = rootIterator.CurrentKey.ToString();
                    if (globalTableIndexesToSkipCopying.Contains(treeName))
                        continue;
                    var currentKey = rootIterator.CurrentKey.Clone(txr.Allocator);
                    var objectType = txr.GetRootObjectType(currentKey);
                    switch (objectType)
                    {
                        case RootObjectType.None:
                            break;
                        case RootObjectType.VariableSizeTree:
                            copiedTrees = CopyVariableSizeTree(compactedEnv, progressReport, txr, treeName, copiedTrees, totalTreesCount, context, token);
                            break;
                        case RootObjectType.EmbeddedFixedSizeTree:
                        case RootObjectType.FixedSizeTree:
                            if (FreeSpaceHandling.IsFreeSpaceTreeName(treeName))
                            {
                                copiedTrees++;// we don't copy the fixed size tree
                                continue;
                            }
                            if (NewPageAllocator.AllocationStorageName == treeName)
                            {
                                copiedTrees++;
                                continue; // we don't copy the allocator storage
                            }

                            copiedTrees = CopyFixedSizeTreeFromRoot(compactedEnv, progressReport, txr, rootIterator, treeName, copiedTrees, totalTreesCount, context, token);
                            break;
                        case RootObjectType.Table:
                            copiedTrees = CopyTableTree(compactedEnv, modifyTableSchema, 
                                progressReport, txr, treeName, copiedTrees, 
                                totalTreesCount, context, token);
                            break;
                        default:
                            throw new ArgumentOutOfRangeException("Unknown " + objectType);
                    }

                } while (rootIterator.MoveNext());
            }
        }

        private static long CopyFixedSizeTreeFromRoot(StorageEnvironment compactedEnv, Action<StorageCompactionProgress> progressReport, Transaction txr,
            TreeIterator rootIterator, string treeName, long copiedTrees, long totalTreesCount, TransactionPersistentContext context, CancellationToken token)
        {
            var treeNameSlice = rootIterator.CurrentKey.Clone(txr.Allocator);

            var header = (FixedSizeTreeHeader.Embedded*)txr.LowLevelTransaction.RootObjects.DirectRead(treeNameSlice);

            var fst = txr.FixedTreeFor(treeNameSlice, header->ValueSize);

            Report(copiedTrees, totalTreesCount, 0, fst.NumberOfEntries, progressReport, $"Copying fixed size tree '{treeName}'. Progress: 0/{fst.NumberOfEntries} entries.", treeName);

            CopyFixedSizeTree(fst, txw => txw.FixedTreeFor(treeNameSlice, header->ValueSize), compactedEnv, context, copiedEntries =>
            {
                Report(copiedTrees, totalTreesCount, copiedEntries, fst.NumberOfEntries, progressReport,
                    $"Copying fixed size tree '{treeName}'. Progress: {copiedEntries}/{fst.NumberOfEntries} entries.", treeName);
            }, () =>
            {
                copiedTrees++;
                Report(copiedTrees, totalTreesCount, fst.NumberOfEntries, fst.NumberOfEntries, progressReport,
                    $"Finished copying fixed size tree '{treeName}'. {fst.NumberOfEntries} entries copied.", treeName);
            }, token);

            return copiedTrees;
        }

        private static void CopyFixedSizeTree(FixedSizeTree fst, Func<Transaction, FixedSizeTree> createDestinationTree, StorageEnvironment compactedEnv, TransactionPersistentContext context, Action<long> onEntriesCopiedProgress, Action onAllEntriesCopied, CancellationToken token)
        {
            using (var it = fst.Iterate())
            {
                var copiedEntries = 0L;
                if (it.Seek(Int64.MinValue) == false)
                    return;

                do
                {
                    token.ThrowIfCancellationRequested();
                    using (var txw = compactedEnv.WriteTransaction(context))
                    {
                        var snd = createDestinationTree(txw);
                        var transactionSize = 0L;

                        do
                        {
                            token.ThrowIfCancellationRequested();

                            using (it.Value(out var val))
                                snd.Add(it.CurrentKey, val);
                            transactionSize += fst.ValueSize + sizeof(long);
                            copiedEntries++;

                            var reportRate = fst.NumberOfEntries / 33 + 1;
                            if (copiedEntries % reportRate == 0)
                            {
                                onEntriesCopiedProgress(copiedEntries);
                            }
                        } while (transactionSize < compactedEnv.Options.MaxScratchBufferSize / 2 && it.MoveNext());

                        txw.Commit();
                    }

                    compactedEnv.FlushLogToDataFile();

                    if (fst.NumberOfEntries == copiedEntries)
                    {
                        onAllEntriesCopied();
                    }

                } while (it.MoveNext());
            }
        }

        private static long CopyVariableSizeTree(StorageEnvironment compactedEnv, Action<StorageCompactionProgress> progressReport, Transaction txr, string treeName, long copiedTrees, long totalTreesCount, TransactionPersistentContext context, CancellationToken token)
        {
            var existingTree = txr.ReadTree(treeName);

            ref readonly var existingHeader = ref existingTree.ReadHeader();
            long existingHeaderNumberOfEntries = existingHeader.NumberOfEntries;
            Report(copiedTrees, totalTreesCount, 0, existingHeaderNumberOfEntries, progressReport, $"Copying variable size tree '{treeName}'. Progress: 0/{existingHeaderNumberOfEntries} entries.", treeName);

            using (var existingTreeIterator = existingTree.Iterate(true))
            {
                if (existingTreeIterator.Seek(Slices.BeforeAllKeys) == false)
                    return copiedTrees;

                token.ThrowIfCancellationRequested();
                using (var txw = compactedEnv.WriteTransaction(context))
                {
                    if (existingTree.IsLeafCompressionSupported)
                        txw.CreateTree(treeName, flags: TreeFlags.LeafsCompressed);
                    else
                        txw.CreateTree(treeName);

                    txw.Commit();
                }

                var copiedEntries = 0L;

                do
                {
                    var transactionSize = 0L;

                    token.ThrowIfCancellationRequested();

                    var txw = compactedEnv.WriteTransaction(context);

                    try
                    {
                        var newTree = txw.ReadTree(treeName);

                        do
                        {
                            token.ThrowIfCancellationRequested();
                            var key = existingTreeIterator.CurrentKey;

                            if (existingTreeIterator.Current->Flags == TreeNodeFlags.MultiValuePageRef)
                            {
                                using (var multiTreeIterator = existingTree.MultiRead(key))
                                {
                                    if (multiTreeIterator.Seek(Slices.BeforeAllKeys) == false)
                                        continue;

                                    do
                                    {
                                        token.ThrowIfCancellationRequested();
                                        var multiValue = multiTreeIterator.CurrentKey;
                                        newTree.MultiAdd(key, multiValue);
                                        transactionSize += multiValue.Size;
                                    } while (multiTreeIterator.MoveNext());
                                }
                            }
                            else if (existingTree.IsLeafCompressionSupported)
                            {
                                using (var read = existingTree.ReadDecompressed(key))
                                {
                                    var value = read.Reader.AsStream();

                                    newTree.Add(key, value);
                                    transactionSize += value.Length;
                                }
                            }
                            else if (existingHeader.Flags == (TreeFlags.FixedSizeTrees | TreeFlags.Streams))
                            {
                                var tag = existingTree.GetStreamTag(key);

                                using (var stream = existingTree.ReadStream(key))
                                {
                                    if (tag != null)
                                    {
                                        using (Slice.From(txw.Allocator, tag, out Slice tagStr))
                                            newTree.AddStream(key, stream, tagStr);
                                    }
                                    else
                                        newTree.AddStream(key, stream);

                                    transactionSize += stream.Length;
                                }
                            }
                            else if (existingHeader.Flags == TreeFlags.FixedSizeTrees)
                            {
                                var reader = existingTree.GetValueReaderFromHeader(existingTreeIterator.Current);

                                if (reader.Length >= sizeof(FixedSizeTreeHeader.Embedded))
                                {
                                    var header = (FixedSizeTreeHeader.Embedded*)reader.Base;

                                    if (header->RootObjectType == RootObjectType.FixedSizeTree || header->RootObjectType == RootObjectType.EmbeddedFixedSizeTree)
                                    {
                                        // CopyFixedSizeTree will open dedicated write transaction to copy fixed size tree

                                        txw.Commit();
                                        txw.Dispose();
                                        txw = null;

                                        var fixedSizeTreeName = key;
                                        var fst = existingTree.FixedTreeFor(fixedSizeTreeName, (byte)header->ValueSize);

                                        var currentCopiedTrees = copiedTrees;
                                        var currentCopiedEntries = copiedEntries;

                                        CopyFixedSizeTree(fst, tx =>
                                        {
                                            var treeInCompactedEnv = tx.ReadTree(treeName);
                                            return treeInCompactedEnv.FixedTreeFor(fixedSizeTreeName, (byte)header->ValueSize);
                                        }, compactedEnv, context, copiedFstEntries =>
                                        {
                                            Report(currentCopiedTrees, totalTreesCount, currentCopiedEntries, existingHeaderNumberOfEntries, progressReport,
                                                $"Copying fixed size tree '{fixedSizeTreeName}' inside '{treeName}' tree. Progress: {copiedFstEntries}/{fst.NumberOfEntries} entries.",
                                                treeName);
                                        }, () =>
                                        {
                                            Report(currentCopiedTrees, totalTreesCount, currentCopiedEntries, existingHeaderNumberOfEntries, progressReport,
                                                $"Finished copying fixed size tree '{fixedSizeTreeName}' inside '{treeName}' tree. {fst.NumberOfEntries} entries copied.",
                                                treeName);
                                        }, token);

                                        IncrementNumberOfCopiedEntries();
                                        break; // let's open new transaction after copying fixed size tree
                                    }
                                }

                                // if the entry wasn't recognized as fixed size tree then let's store it as regular value

                                using (var value = existingTree.Read(key).Reader.AsStream())
                                {
                                    newTree.Add(key, value);
                                    transactionSize += value.Length;
                                }
                            }
                            else
                            {
                                using (var value = existingTree.Read(key).Reader.AsStream())
                                {
                                    newTree.Add(key, value);
                                    transactionSize += value.Length;
                                }
                            }

                            IncrementNumberOfCopiedEntries();

                            void IncrementNumberOfCopiedEntries()
                            {
                                copiedEntries++;

                                var reportRate = existingHeaderNumberOfEntries / 33 + 1;
                                if (copiedEntries % reportRate == 0)
                                    Report(copiedTrees, totalTreesCount, copiedEntries, existingHeaderNumberOfEntries, progressReport,
                                        $"Copying variable size tree '{treeName}'. Progress: {copiedEntries:#,#;;0}/{existingHeaderNumberOfEntries:#,#;;0} entries.", treeName);
                            }

                        } while (transactionSize < compactedEnv.Options.MaxScratchBufferSize / 2 && existingTreeIterator.MoveNext());

                        txw?.Commit();
                    }
                    finally
                    {
                        txw?.Dispose();
                    }

                    if (copiedEntries == existingHeaderNumberOfEntries)
                    {
                        copiedTrees++;
                        Report(copiedTrees, totalTreesCount, copiedEntries, existingHeaderNumberOfEntries, progressReport, $"Finished copying variable size tree '{treeName}'. Progress: {copiedEntries}/{existingHeaderNumberOfEntries} entries.", treeName);
                    }

                    compactedEnv.FlushLogToDataFile();
                } while (existingTreeIterator.MoveNext());
            }
            return copiedTrees;
        }

        private static long CopyTableTree(StorageEnvironment compactedEnv,
            Action<string, TableSchema> modifyTableSchema,
            Action<StorageCompactionProgress> progressReport,
            Transaction txr, string treeName, long copiedTrees, long totalTreesCount,
            TransactionPersistentContext context, CancellationToken token)
        {
            TableSchema schema = GetTableSchema(txr, treeName, modifyTableSchema);

            (long numberOfEntries, int numberOfPagesInActiveSection) = GetNumberOfEntries(txr, treeName, schema);

            // The next three variables are used to know what our current
            // progress is
            var copiedEntries = 0;

            // It is very important that these slices be allocated in the
            // dedicated ByteStringContext, as the intermediate write transactions on
            // the compacted environment will be destroyed between each loop and
            // the read transaction can be closed after reaching certain scratch buffers size or
            // 4MB allocations when running on 32-bits
            using var lastSliceAllocator = new ByteStringContext(SharedMultipleUseFlag.None);
            var lastSlice = Slices.BeforeAllKeys;
            long lastFixedIndex = 0L;

            Report(copiedTrees, totalTreesCount, copiedEntries, numberOfEntries, progressReport, $"Copying table tree '{treeName}'. Progress: {copiedEntries:#,#;;0}/{numberOfEntries:#,#;;0} entries.", treeName);
            using (var txw = compactedEnv.WriteTransaction(context))
            {
                schema.Create(txw, treeName, Math.Max((ushort)numberOfPagesInActiveSection, (ushort)((ushort.MaxValue + 1) / Constants.Storage.PageSize)));
                txw.Commit(); // always create a table, even if it is empty
            }

            var sp = Stopwatch.StartNew();

            while (copiedEntries < numberOfEntries)
            {
                token.ThrowIfCancellationRequested();
                using(var innerTxr = txr.LowLevelTransaction.Environment.ReadTransaction())
                using (var txw = compactedEnv.WriteTransaction(context))
                {
                    long transactionSize = 0L;

                    Table inputTable = innerTxr.OpenTable(schema, treeName);
                    var outputTable = txw.OpenTable(schema, treeName);

                    if (schema.Key == null || schema.Key.IsGlobal) 
                    {
                        // There is no primary key, or there is one that is global to multiple tables
                        // we require a table to have at least a single local index that we'll use

                        var variableSizeIndex = schema.Indexes.Values.FirstOrDefault(x => x.IsGlobal == false);

                        if (variableSizeIndex != null)
                        {
                            // We have a variable size index, use it

                            // In case we continue an existing compaction, skip to the next slice
                            var skip = 0;
                            // can't use SliceComparer.Compare here
                            if (lastSlice.Options != Slices.BeforeAllKeys.Options)
                                skip = 1;

                            foreach (var tvr in inputTable.SeekForwardFrom(variableSizeIndex, lastSlice, skip))
                            {
                                // The table will take care of reconstructing indexes automatically
                                outputTable.Insert(ref tvr.Result.Reader);
                                copiedEntries++;
                                transactionSize += tvr.Result.Reader.Size;

                                ReportIfNeeded(sp, copiedTrees, totalTreesCount, copiedEntries, numberOfEntries, progressReport, $"Copying table tree '{treeName}'. Progress: {copiedEntries:#,#;;0}/{numberOfEntries:#,#;;0} entries.", treeName);

                                // The transaction has surpassed the allowed
                                // size before a flush
                                if (lastSlice.Equals(tvr.Key) == false && transactionSize >= compactedEnv.Options.MaxScratchBufferSize / 2 || ShouldCloseTxFor32Bit(transactionSize, compactedEnv))
                                {
                                    lastSlice = tvr.Key.Clone(lastSliceAllocator);
                                    break;
                                }
                                innerTxr.ForgetAbout(tvr.Result.Reader.Id);
                            }
                        }
                        else
                        {
                            // Use a fixed size index
                            var fixedSizeIndex = schema.FixedSizeIndexes.Values.FirstOrDefault(x => x.IsGlobal == false);

                            if (fixedSizeIndex == null)
                                throw new InvalidOperationException("Cannot compact table " + inputTable.Name + " because is has no local indexes, only global ones");

                            foreach (var entry in inputTable.SeekForwardFrom(fixedSizeIndex, lastFixedIndex, lastFixedIndex > 0 ? 1 : 0))
                            {
                                token.ThrowIfCancellationRequested();
                                // The table will take care of reconstructing indexes automatically
                                outputTable.Insert(ref entry.Reader);
                                copiedEntries++;
                                transactionSize += entry.Reader.Size;

                                ReportIfNeeded(sp, copiedTrees, totalTreesCount, copiedEntries, numberOfEntries, progressReport, $"Copying table tree '{treeName}'. Progress: {copiedEntries:#,#;;0}/{numberOfEntries:#,#;;0} entries.", treeName);

                                // The transaction has surpassed the allowed
                                // size before a flush
                                if (transactionSize >= compactedEnv.Options.MaxScratchBufferSize / 2 || ShouldCloseTxFor32Bit(transactionSize, compactedEnv))
                                {
                                    lastFixedIndex = fixedSizeIndex.GetValue(ref entry.Reader);
                                    break;
                                }
                                innerTxr.ForgetAbout(entry.Reader.Id);
                            }
                        }
                    }
                    else
                    {
                        // The table has a primary key, inserts in that order are expected to be faster
                        foreach (var entry in inputTable.SeekByPrimaryKey(lastSlice, 0))
                        {
                            token.ThrowIfCancellationRequested();

                            // The transaction has surpassed the allowed
                            // size before a flush
                            if (transactionSize >= compactedEnv.Options.MaxScratchBufferSize / 2 || ShouldCloseTxFor32Bit(transactionSize, compactedEnv))
                            {
                                schema.Key.GetValue(txr.Allocator, ref entry.Reader, out var slice);
                                lastSlice = slice.Clone(lastSliceAllocator);
                                break;
                            }

                            // The table will take care of reconstructing indexes automatically
                            outputTable.Insert(ref entry.Reader);
                            copiedEntries++;
                            transactionSize += entry.Reader.Size;
                            ReportIfNeeded(sp, copiedTrees, totalTreesCount, copiedEntries, numberOfEntries, progressReport, $"Copying table tree '{treeName}'. Progress: {copiedEntries:#,#;;0}/{numberOfEntries:#,#;;0} entries.", treeName);
                            innerTxr.ForgetAbout(entry.Reader.Id);
                        }
                    }

                    txw.Commit();
                }

                if (copiedEntries == numberOfEntries)
                {
                    copiedTrees++;
                    Report(copiedTrees, totalTreesCount, copiedEntries, numberOfEntries, progressReport, $"Finished copying table tree '{treeName}'. Progress: {copiedEntries:#,#;;0}/{numberOfEntries:#,#;;0} entries.", treeName);
                }

                compactedEnv.FlushLogToDataFile();
            }

            return copiedTrees;
        }

        private static (long NumberOfEntries, int ActiveDataSmallSection) GetNumberOfEntries(Transaction txr2, string treeName, TableSchema schema)
        {
            // Load table into structure 
            Table openTable = txr2.OpenTable(schema, treeName);
            return (openTable.NumberOfEntries, openTable.ActiveDataSmallSection.NumberOfPages);
        }

        private static TableSchema GetTableSchema(Transaction txr, string treeName, Action<string, TableSchema> modifyTableSchema)
        {
            // Load table
            var tableTree = txr.ReadTree(treeName, RootObjectType.Table);

            // Get the table schema
            var schemaSize = tableTree.GetDataSize(TableSchema.SchemasSlice);
            var schemaPtr = tableTree.DirectRead(TableSchema.SchemasSlice);
            var schema = TableSchema.ReadFrom(txr.Allocator, schemaPtr, schemaSize);
            modifyTableSchema?.Invoke(treeName, schema);
            return schema;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static bool ShouldCloseTxFor32Bit(long transactionSize, StorageEnvironment env)
        {
            return (env.Options.ForceUsing32BitsPager || PlatformDetails.Is32Bits) && transactionSize > 4 * Constants.Size.Megabyte;
        }

        private static void ReportIfNeeded(Stopwatch sp, long globalProgress, long globalTotal, long objectProgress, 
            long objectTotal, Action<StorageCompactionProgress> progressReport, string message = null, string treeName = null)
        {
            const int intervalInMs = 5 * 1000; // 5 seconds
            if (sp.ElapsedMilliseconds < intervalInMs)
                return;

            Report(globalProgress, globalTotal, objectProgress, objectTotal, progressReport, message, treeName);
            sp.Restart();
        }

        private static void Report(long globalProgress, long globalTotal, long objectProgress, long objectTotal, Action<StorageCompactionProgress> progressReport, string message = null, string treeName = null)
        {
            progressReport?.Invoke(new StorageCompactionProgress
            {
                TreeProgress = objectProgress,
                TreeTotal = objectTotal,
                GlobalProgress = globalProgress,
                GlobalTotal = globalTotal,
                TreeName = treeName,
                Message = message
            });
        }
    }
}
