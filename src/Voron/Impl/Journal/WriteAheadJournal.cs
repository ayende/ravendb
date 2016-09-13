﻿// -----------------------------------------------------------------------
//  <copyright file="WriteAheadJournal.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Sparrow;
using Sparrow.Binary;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using Sparrow.Compression;
using Sparrow.Utils;
using Voron.Data;
using Voron.Data.BTrees;
using Voron.Exceptions;
using Voron.Impl.FileHeaders;
using Voron.Impl.Paging;
using Voron.Util;
using Voron.Global;

namespace Voron.Impl.Journal
{
    public unsafe class WriteAheadJournal : IDisposable
    {
        private readonly StorageEnvironment _env;
        private readonly AbstractPager _dataPager;

        private long _currentJournalFileSize;
        private DateTime _lastFile;

        private long _journalIndex = -1;

        private bool _disposed;

        private readonly LZ4 _lz4 = new LZ4();
        private readonly JournalApplicator _journalApplicator;
        private readonly ModifyHeaderAction _updateLogInfo;

        private ImmutableAppendOnlyList<JournalFile> _files = ImmutableAppendOnlyList<JournalFile>.Empty;
        internal JournalFile CurrentFile;

        private readonly HeaderAccessor _headerAccessor;
        private readonly AbstractPager _compressionPager;

        private LazyTransactionBuffer _lazyTransactionBuffer;
        private readonly DiffPages _diffPage = new DiffPages();
        public bool HasDataInLazyTxBuffer() => _lazyTransactionBuffer?.HasDataInBuffer() ?? false;

        public WriteAheadJournal(StorageEnvironment env)
        {
            _env = env;
            _dataPager = _env.Options.DataPager;
            _currentJournalFileSize = env.Options.InitialLogFileSize;
            _headerAccessor = env.HeaderAccessor;
            _updateLogInfo = header =>
            {
                var journalFilesCount = _files.Count;
                var currentJournal = journalFilesCount > 0 ? _journalIndex : -1;
                header->Journal.CurrentJournal = currentJournal;
                header->Journal.JournalFilesCount = journalFilesCount;
                header->IncrementalBackup.LastCreatedJournal = _journalIndex;
            };

            _compressionPager = _env.Options.CreateScratchPager("compression.buffers");
            _journalApplicator = new JournalApplicator(this);
        }

        public ImmutableAppendOnlyList<JournalFile> Files { get { return _files; } }

        public JournalApplicator Applicator { get { return _journalApplicator; } }

        internal long CompressionBufferSize
        {
            get { return _compressionPager.NumberOfAllocatedPages * _compressionPager.PageSize; }
        }

        public bool HasLazyTransactions { get; set; }

        private JournalFile NextFile(int numberOfPages = 1)
        {
            _journalIndex++;

            var now = DateTime.UtcNow;
            if ((now - _lastFile).TotalSeconds < 90)
            {
                _currentJournalFileSize = Math.Min(_env.Options.MaxLogFileSize, _currentJournalFileSize * 2);
            }
            var actualLogSize = _currentJournalFileSize;
            var minRequiredSize = numberOfPages * _compressionPager.PageSize;
            if (_currentJournalFileSize < minRequiredSize)
            {
                actualLogSize = minRequiredSize;
            }

            _lastFile = now;

            var journalPager = _env.Options.CreateJournalWriter(_journalIndex, actualLogSize);

            var journal = new JournalFile(journalPager, _journalIndex);
            journal.AddRef(); // one reference added by a creator - write ahead log

            _files = _files.Append(journal);

            _headerAccessor.Modify(_updateLogInfo);

            return journal;
        }

        public bool RecoverDatabase(TransactionHeader* txHeader)
        {
            // note, we don't need to do any concurrency here, happens as a single threaded
            // fashion on db startup
            var requireHeaderUpdate = false;

            var logInfo = _headerAccessor.Get(ptr => ptr->Journal);

            if (logInfo.JournalFilesCount == 0)
            {
                _journalIndex = logInfo.LastSyncedJournal;
                return false;
            }

            var oldestLogFileStillInUse = logInfo.CurrentJournal - logInfo.JournalFilesCount + 1;
            if (_env.Options.IncrementalBackupEnabled == false)
            {
                // we want to check that we cleanup old log files if they aren't needed
                // this is more just to be safe than anything else, they shouldn't be there.
                var unusedfiles = oldestLogFileStillInUse;
                while (true)
                {
                    unusedfiles--;
                    if (_env.Options.TryDeleteJournal(unusedfiles) == false)
                        break;
                }

            }

            var lastSyncedTransactionId = logInfo.LastSyncedTransactionId;

            var journalFiles = new List<JournalFile>();
            long lastSyncedTxId = -1;
            long lastSyncedJournal = logInfo.LastSyncedJournal;
            for (var journalNumber = oldestLogFileStillInUse; journalNumber <= logInfo.CurrentJournal; journalNumber++)
            {
                using (var recoveryPager = _env.Options.CreateScratchPager(StorageEnvironmentOptions.JournalRecoveryName(journalNumber)))
                using (var pager = _env.Options.OpenJournalPager(journalNumber))
                {
                    RecoverCurrentJournalSize(pager);

                    var transactionHeader = txHeader->TransactionId == 0 ? null : txHeader;
                    var journalReader = new JournalReader(pager, _dataPager, recoveryPager, lastSyncedTransactionId, transactionHeader);
                    journalReader.RecoverAndValidate(_env.Options);

                    var lastReadHeaderPtr = journalReader.LastTransactionHeader;

                    if (lastReadHeaderPtr != null)
                    {
                        *txHeader = *lastReadHeaderPtr;
                        lastSyncedTxId = txHeader->TransactionId;
                        lastSyncedJournal = journalNumber;
                    }

                    if (lastSyncedTxId != -1 && (journalReader.RequireHeaderUpdate || journalNumber == logInfo.CurrentJournal))
                    {
                        var jrnlWriter = _env.Options.CreateJournalWriter(journalNumber, pager.NumberOfAllocatedPages * _compressionPager.PageSize);
                        var jrnlFile = new JournalFile(jrnlWriter, journalNumber);
                        jrnlFile.InitFrom(journalReader);
                        jrnlFile.AddRef(); // creator reference - write ahead log

                        journalFiles.Add(jrnlFile);
                    }

                    if (journalReader.RequireHeaderUpdate) //this should prevent further loading of transactions
                    {
                        requireHeaderUpdate = true;
                        break;
                    }
                }
            }

            _files = _files.AppendRange(journalFiles);

            if (lastSyncedTxId == -1 && requireHeaderUpdate)
                throw new VoronUnrecoverableErrorException(
                    "First transaction initializing the structure of Voron database is corrupted. Cannot access internal database metadata. Create a new database to recover.");

            Debug.Assert(lastSyncedTxId >= 0);
            Debug.Assert(lastSyncedJournal >= 0);

            _journalIndex = lastSyncedJournal;

            _headerAccessor.Modify(
                header =>
                {
                    header->Journal.CurrentJournal = lastSyncedJournal;
                    header->Journal.JournalFilesCount = _files.Count;
                    header->IncrementalBackup.LastCreatedJournal = _journalIndex;
                });

            CleanupInvalidJournalFiles(lastSyncedJournal);
            CleanupUnusedJournalFiles(oldestLogFileStillInUse, lastSyncedJournal);

            if (_files.Count > 0)
            {
                var lastFile = _files.Last();
                if (lastFile.AvailablePages >= 2)
                    // it must have at least one page for the next transaction header and one page for data
                    CurrentFile = lastFile;
            }

            return requireHeaderUpdate;
        }

        private void CleanupUnusedJournalFiles(long oldestLogFileStillInUse, long lastSyncedJournal)
        {
            var logFile = oldestLogFileStillInUse;
            while (logFile < lastSyncedJournal)
            {
                _env.Options.TryDeleteJournal(logFile);
                logFile++;
            }
        }

        private void CleanupInvalidJournalFiles(long lastSyncedJournal)
        {
            // we want to check that we cleanup newer log files, since everything from
            // the current file is considered corrupted
            var badJournalFiles = lastSyncedJournal;
            while (true)
            {
                badJournalFiles++;
                if (_env.Options.TryDeleteJournal(badJournalFiles) == false)
                {
                    break;
                }
            }
        }

        private void ApplyPagesToDataFileFromJournal(List<TreePage> sortedPagesToWrite)
        {
            var last = sortedPagesToWrite.Last();

            var numberOfPagesInLastPage = last.IsOverflow == false ? 1 :
                _env.Options.DataPager.GetNumberOfOverflowPages(last.OverflowSize);

            _dataPager.EnsureContinuous(last.PageNumber, numberOfPagesInLastPage);

            _dataPager.MaybePrefetchMemory(sortedPagesToWrite);

            foreach (var page in sortedPagesToWrite)
            {
                _dataPager.Write(page);
            }
        }

        private void RecoverCurrentJournalSize(AbstractPager pager)
        {
            var journalSize = Bits.NextPowerOf2(pager.NumberOfAllocatedPages * pager.PageSize);
            if (journalSize >= _env.Options.MaxLogFileSize) // can't set for more than the max log file size
                return;

            // this set the size of the _next_ journal file size
            _currentJournalFileSize = Math.Min(journalSize, _env.Options.MaxLogFileSize);
        }


        public Page ReadPage(LowLevelTransaction tx, long pageNumber, Dictionary<int, PagerState> scratchPagerStates)
        {
            // read transactions have to read from journal snapshots
            if (tx.Flags == TransactionFlags.Read)
            {
                // read log snapshots from the back to get the most recent version of a page
                for (var i = tx.JournalSnapshots.Count - 1; i >= 0; i--)
                {
                    PagePosition value;
                    if (tx.JournalSnapshots[i].PageTranslationTable.TryGetValue(tx, pageNumber, out value))
                    {
                        var page = _env.ScratchBufferPool.ReadPage(tx, value.ScratchNumber, value.ScratchPos, scratchPagerStates[value.ScratchNumber]);

                        Debug.Assert(page.PageNumber == pageNumber);

                        return page;
                    }
                }

                return null;
            }

            // write transactions can read directly from journals
            var files = _files;
            for (var i = files.Count - 1; i >= 0; i--)
            {
                PagePosition value;
                if (files[i].PageTranslationTable.TryGetValue(tx, pageNumber, out value))
                {
                    var page = _env.ScratchBufferPool.ReadPage(tx, value.ScratchNumber, value.ScratchPos);

                    Debug.Assert(page.PageNumber == pageNumber);

                    return page;
                }
            }

            return null;
        }


        public void Dispose()
        {
            if (_disposed)
                return;
            _disposed = true;

            // we cannot dispose the journal until we are done with all of the pending writes
            if (_lazyTransactionBuffer != null)
            {
                _lazyTransactionBuffer.WriteBufferToFile(CurrentFile, null);
                _lazyTransactionBuffer.Dispose();
            }
            _compressionPager.Dispose();

            _journalApplicator.Dispose();
            if (_env.Options.OwnsPagers)
            {
                foreach (var logFile in _files)
                {
                    logFile.Dispose();
                }

            }
            else
            {
                foreach (var logFile in _files)
                {
                    GC.SuppressFinalize(logFile);
                }

            }

            _files = ImmutableAppendOnlyList<JournalFile>.Empty;
        }

        public JournalInfo GetCurrentJournalInfo()
        {
            return _headerAccessor.Get(ptr => ptr->Journal);
        }

        public List<JournalSnapshot> GetSnapshots()
        {
            return _files.Select(x => x.GetSnapshot()).ToList();
        }

        public void Clear(LowLevelTransaction tx)
        {
            if (tx.Flags != TransactionFlags.ReadWrite)
                throw new InvalidOperationException("Clearing of write ahead journal should be called only from a write transaction");

            foreach (var journalFile in _files)
            {
                journalFile.Release();
            }
            _files = ImmutableAppendOnlyList<JournalFile>.Empty;
            CurrentFile = null;
        }



        public class JournalSyncEventArgs : EventArgs
        {
            public long OldestTransactionId { get; private set; }

            public JournalSyncEventArgs(long oldestTransactionId)
            {
                OldestTransactionId = oldestTransactionId;
            }
        }

        public class JournalApplicator : IDisposable
        {
            private const long DelayedDataFileSynchronizationBytesLimit = 2L * Constants.Size.Gigabyte;

            private readonly TimeSpan _delayedDataFileSynchronizationTimeLimit = TimeSpan.FromMinutes(1);
            private readonly Dictionary<long, JournalFile> _journalsToDelete = new Dictionary<long, JournalFile>();
            private readonly object _flushingLock = new object();
            private readonly WriteAheadJournal _waj;

            private long _lastSyncedTransactionId;
            private long _lastSyncedJournal;
            private long _totalWrittenButUnsyncedBytes;
            private DateTime _lastDataFileSyncTime;
            private JournalFile _lastFlushedJournal;
            private bool _ignoreLockAlreadyTaken;

            public JournalApplicator(WriteAheadJournal waj)
            {
                _waj = waj;
            }


            public void ApplyLogsToDataFile(long oldestActiveTransaction, CancellationToken token, TimeSpan timeToWait, LowLevelTransaction transaction = null)
            {
                if (token.IsCancellationRequested)
                    return;

                if (Monitor.IsEntered(_flushingLock) && _ignoreLockAlreadyTaken == false)
                    throw new InvalidJournalFlushRequestException("Applying journals to the data file has been already requested on the same thread");

                bool lockTaken = false;
                try
                {
                    _waj._env.IsFlushingScratchBuffer = true;
                    Monitor.TryEnter(_flushingLock, timeToWait, ref lockTaken);

                    if (_waj._env.Disposed)
                        return;

                    if (lockTaken == false)
                    {
                        if (timeToWait == TimeSpan.Zero)
                            // someone else is flushing, and we were explicitly told that we don't care about this
                            // so there is no point in throwing
                            return;

                        throw new TimeoutException($"Could not acquire the write lock in {timeToWait.TotalSeconds} seconds");
                    }

                    var alreadyInWriteTx = transaction != null && transaction.Flags == TransactionFlags.ReadWrite;

                    var jrnls = _waj._files.Select(x => x.GetSnapshot()).OrderBy(x => x.Number).ToList();
                    if (jrnls.Count == 0)
                        return; // nothing to do

                    Debug.Assert(jrnls.First().Number >= _lastSyncedJournal);

                    var pagesToWrite = new Dictionary<long, PagePosition>();

                    long lastProcessedJournal = -1;
                    long previousJournalMaxTransactionId = -1;

                    long lastFlushedTransactionId = -1;

                    foreach (var journalFile in jrnls)
                    {
                        if (journalFile.Number < _lastSyncedJournal)
                            continue;
                        var currentJournalMaxTransactionId = -1L;

                        var maxTransactionId = journalFile.LastTransaction;
                        if (oldestActiveTransaction != 0)
                            maxTransactionId = Math.Min(oldestActiveTransaction - 1, maxTransactionId);

                        foreach (var pagePosition in journalFile.PageTranslationTable.Iterate(_lastSyncedTransactionId, maxTransactionId))
                        {
                            if (pagePosition.Value.IsFreedPageMarker)
                            {
                                // Avoid the case where an older journal file had written a page that was freed in a different journal
                                pagesToWrite.Remove(pagePosition.Key);
                                continue;
                            }

                            if (journalFile.Number == _lastSyncedJournal && pagePosition.Value.TransactionId <= _lastSyncedTransactionId)
                                continue;

                            currentJournalMaxTransactionId = Math.Max(currentJournalMaxTransactionId, pagePosition.Value.TransactionId);

                            if (currentJournalMaxTransactionId < previousJournalMaxTransactionId)
                                throw new InvalidOperationException(
                                    "Journal applicator read beyond the oldest active transaction in the next journal file. " +
                                    "This should never happen. Current journal max tx id: " + currentJournalMaxTransactionId +
                                    ", previous journal max ix id: " + previousJournalMaxTransactionId +
                                    ", oldest active transaction: " + oldestActiveTransaction);


                            lastProcessedJournal = journalFile.Number;
                            pagesToWrite[pagePosition.Key] = pagePosition.Value;

                            lastFlushedTransactionId = currentJournalMaxTransactionId;
                        }

                        if (currentJournalMaxTransactionId == -1L)
                            continue;

                        previousJournalMaxTransactionId = currentJournalMaxTransactionId;
                    }

                    if (pagesToWrite.Count == 0)
                    {
                        return;
                    }

                    try
                    {
                        ApplyPagesToDataFileFromScratch(pagesToWrite, transaction, alreadyInWriteTx);
                    }
                    catch (DiskFullException diskFullEx)
                    {
                        _waj._env.HandleDataDiskFullException(diskFullEx);
                        return;
                    }

                    var unusedJournals = GetUnusedJournalFiles(jrnls, lastProcessedJournal, lastFlushedTransactionId);

                    foreach (var unused in unusedJournals.Where(unused => !_journalsToDelete.ContainsKey(unused.Number)))
                    {
                        _journalsToDelete.Add(unused.Number, unused);
                    }

                    var timeout = TimeSpan.FromSeconds(3);
                    bool tryEnterReadLock = false;
                    if (alreadyInWriteTx == false)
                        tryEnterReadLock = _waj._env.FlushInProgressLock.TryEnterWriteLock(timeout);
                    try
                    {
                        _waj._env.IncreaseTheChanceForGettingTheTransactionLock();
                        using (var txw = alreadyInWriteTx ? null : _waj._env.NewLowLevelTransaction(TransactionFlags.ReadWrite).JournalApplicatorTransaction())
                        {
                            _lastSyncedJournal = lastProcessedJournal;
                            _lastSyncedTransactionId = lastFlushedTransactionId;

                            _lastFlushedJournal = _waj._files.First(x => x.Number == _lastSyncedJournal);

                            if (unusedJournals.Count > 0)
                            {
                                var lastUnusedJournalNumber = unusedJournals.Last().Number;
                                _waj._files = _waj._files.RemoveWhile(x => x.Number <= lastUnusedJournalNumber);
                            }

                            if (_waj._files.Count == 0)
                                _waj.CurrentFile = null;

                            FreeScratchPages(unusedJournals, txw ?? transaction);

                            if (txw != null)
                            {
                                // we force a dummy change to a page, so when we commit, this will be written to the journal
                                // as well as force us to generate a new transaction id, which will mean that the next time
                                // that we run, we have freed lazy transactions, we have freed all the pages that were freed
                                // in this transaction
                                if (_waj.HasLazyTransactions)
                                    txw.ModifyPage(0);

                                _waj.HasLazyTransactions = false;

                                txw.Commit();
                            }
                        }
                    }
                    finally
                    {
                        _waj._env.ResetTheChanceForGettingTheTransactionLock();
                        if (tryEnterReadLock)
                            _waj._env.FlushInProgressLock.ExitWriteLock();
                    }

                    if (_totalWrittenButUnsyncedBytes > DelayedDataFileSynchronizationBytesLimit ||
                        DateTime.UtcNow - _lastDataFileSyncTime > _delayedDataFileSynchronizationTimeLimit)
                    {
                        SyncDataFile(oldestActiveTransaction);
                    }

                }
                finally
                {
                    if (lockTaken)
                        Monitor.Exit(_flushingLock);
                    _waj._env.IsFlushingScratchBuffer = false;
                }
            }

            internal void SyncDataFile(long oldestActiveTransaction)
            {
                _waj._dataPager.Sync();

                UpdateFileHeaderAfterDataFileSync(_lastFlushedJournal, oldestActiveTransaction);

                foreach (var toDelete in _journalsToDelete.Values)
                {
                    if (_waj._env.Options.IncrementalBackupEnabled == false)
                        toDelete.DeleteOnClose = true;

                    toDelete.Release();
                }

                _journalsToDelete.Clear();
                _totalWrittenButUnsyncedBytes = 0;
                _lastDataFileSyncTime = DateTime.UtcNow;
            }

            private void ApplyPagesToDataFileFromScratch(Dictionary<long, PagePosition> pagesToWrite, LowLevelTransaction transaction, bool alreadyInWriteTx)
            {
                var scratchBufferPool = _waj._env.ScratchBufferPool;
                var scratchPagerStates = new Dictionary<int, PagerState>();

                try
                {
                    var totalPages = 0L;
                    Page last = null;
                    var sortedPages = new List<Page>();
                    foreach (var pagePosition in pagesToWrite.Values)
                    {
                        var scratchNumber = pagePosition.ScratchNumber;
                        PagerState pagerState;
                        if (scratchPagerStates.TryGetValue(scratchNumber, out pagerState) == false)
                        {
                            pagerState = scratchBufferPool.GetPagerState(scratchNumber);
                            pagerState.AddRef();

                            scratchPagerStates.Add(scratchNumber, pagerState);
                        }

                        var readPage = scratchBufferPool.ReadPage(transaction, scratchNumber, pagePosition.ScratchPos, pagerState);
                        totalPages += _waj._dataPager.GetNumberOfPages(readPage);
                        sortedPages.Add(readPage);
                        if (last == null)
                            last = readPage;
                        if (last.PageNumber < readPage.PageNumber)
                            last = readPage;
                    }
                    Debug.Assert(last != null);

                    var numberOfPagesInLastPage = last.IsOverflow == false ? 1 :
                        _waj._env.Options.DataPager.GetNumberOfOverflowPages(last.OverflowSize);

                    EnsureDataPagerSpacing(transaction, last, numberOfPagesInLastPage, alreadyInWriteTx);

                    long written = 0;
                    using (_waj._dataPager.Options.IoMetrics.MeterIoRate(_waj._dataPager.FileName, IoMetrics.MeterType.WriteUsingMem,
                            totalPages * _waj._dataPager.PageSize))
                    {
                        foreach (var page in sortedPages)
                        {
                            written += _waj._dataPager.WritePage(page);
                        }
                    }

                    _totalWrittenButUnsyncedBytes += written;
                }
                finally
                {
                    foreach (var scratchPagerState in scratchPagerStates.Values)
                    {
                        scratchPagerState.Release();
                    }
                }
            }

            private void EnsureDataPagerSpacing(LowLevelTransaction transaction, Page last, int numberOfPagesInLastPage,
                    bool alreadyInWriteTx)
            {
                if (_waj._dataPager.WillRequireExtension(last.PageNumber, numberOfPagesInLastPage) == false)
                    return;

                if (alreadyInWriteTx)
                {
                    var pagerState = _waj._dataPager.EnsureContinuous(last.PageNumber, numberOfPagesInLastPage);
                    transaction.EnsurePagerStateReference(pagerState);
                }
                else
                {
                    using (var tx = _waj._env.NewLowLevelTransaction(TransactionFlags.ReadWrite).JournalApplicatorTransaction())
                    {
                        var pagerState = _waj._dataPager.EnsureContinuous(last.PageNumber, numberOfPagesInLastPage);
                        tx.EnsurePagerStateReference(pagerState);

                        tx.Commit();
                    }
                }
            }

            private void FreeScratchPages(IEnumerable<JournalFile> unusedJournalFiles, LowLevelTransaction txw)
            {
                // we have to free pages of the unused journals before the remaining ones that are still in use
                // to prevent reading from them by any read transaction (read transactions search journals from the newest
                // to read the most updated version)
                foreach (var journalFile in unusedJournalFiles.OrderBy(x => x.Number))
                {
                    journalFile.FreeScratchPagesOlderThan(txw, _lastSyncedTransactionId);
                }

                foreach (var jrnl in _waj._files.OrderBy(x => x.Number))
                {
                    jrnl.FreeScratchPagesOlderThan(txw, _lastSyncedTransactionId);
                }
            }

            private List<JournalFile> GetUnusedJournalFiles(IEnumerable<JournalSnapshot> jrnls, long lastProcessedJournal, long lastFlushedTransactionId)
            {
                var unusedJournalFiles = new List<JournalFile>();
                foreach (var j in jrnls)
                {
                    if (j.Number > lastProcessedJournal) // after the last log we synced, nothing to do here
                        continue;
                    if (j.Number == lastProcessedJournal) // we are in the last log we synced
                    {
                        if (j.AvailablePages != 0 || //　if there are more pages to be used here or 
                        j.PageTranslationTable.MaxTransactionId() != lastFlushedTransactionId) // we didn't synchronize whole journal
                            continue; // do not mark it as unused
                    }
                    unusedJournalFiles.Add(_waj._files.First(x => x.Number == j.Number));
                }
                return unusedJournalFiles;
            }

            public void UpdateFileHeaderAfterDataFileSync(JournalFile file, long oldestActiveTransaction)
            {
                var txHeaders = stackalloc TransactionHeader[2];
                var readTxHeader = &txHeaders[0];
                var lastReadTxHeader = txHeaders[1];

                var txPos = 0;
                while (true)
                {
                    if (file.ReadTransaction(txPos, readTxHeader) == false)
                        break;
                    if (readTxHeader->HeaderMarker != Constants.TransactionHeaderMarker)
                        break;
                    if (readTxHeader->TransactionId + 1 == oldestActiveTransaction)
                        break;

                    lastReadTxHeader = *readTxHeader;

                    var totalSize = readTxHeader->CompressedSize + sizeof(TransactionHeader);


                    var totalPages = (totalSize / _waj._env.Options.PageSize) +
                                     (totalSize % _waj._env.Options.PageSize == 0 ? 0 : 1);

                    // We skip to the next transaction header.
                    txPos += totalPages;
                }

                Debug.Assert(_lastSyncedJournal != -1);
                Debug.Assert(_lastSyncedTransactionId != -1);

                _waj._headerAccessor.Modify(header =>
                {
                    header->TransactionId = lastReadTxHeader.TransactionId;
                    header->LastPageNumber = lastReadTxHeader.LastPageNumber;

                    header->Journal.LastSyncedJournal = _lastSyncedJournal;
                    header->Journal.LastSyncedTransactionId = _lastSyncedTransactionId;

                    header->Root = lastReadTxHeader.Root;

                    _waj._updateLogInfo(header);
                });
            }

            public void Dispose()
            {
                foreach (var journalFile in _journalsToDelete)
                {
                    // we need to release all unused journals 
                    // however here we don't force them to DeleteOnClose
                    // because we didn't synced the data file yet
                    // and we will need them on a next database recovery
                    journalFile.Value.Release();
                }
            }

            public bool IsCurrentThreadInFlushOperation
            {
                get { return Monitor.IsEntered(_flushingLock); }
            }

            public IDisposable TryTakeFlushingLock(ref bool lockTaken, TimeSpan? timeout = null)
            {
                if (timeout == null)
                {
                    Monitor.TryEnter(_flushingLock, ref lockTaken);
                }
                else
                {
                    Monitor.TryEnter(_flushingLock, timeout.Value, ref lockTaken);
                }

                bool localLockTaken = lockTaken;

                _ignoreLockAlreadyTaken = true;

                return new DisposableAction(() =>
                {
                    _ignoreLockAlreadyTaken = false;
                    if (localLockTaken)
                        Monitor.Exit(_flushingLock);
                });
            }

            public IDisposable TakeFlushingLock()
            {
                bool lockTaken = false;
                Monitor.Enter(_flushingLock, ref lockTaken);
                _ignoreLockAlreadyTaken = true;

                return new DisposableAction(() =>
                {
                    _ignoreLockAlreadyTaken = false;
                    if (lockTaken)
                        Monitor.Exit(_flushingLock);
                });
            }

            internal void DeleteCurrentAlreadyFlushedJournal()
            {
                if (_waj._env.Options.IncrementalBackupEnabled)
                    return;

                if (_waj._files.Count == 0)
                    return;

                if (_waj._files.Count != 1)
                    throw new InvalidOperationException("Cannot delete current journal because there is more journals being in use");

                var current = _waj._files.First();

                if (current.Number != _lastSyncedJournal)
                    throw new InvalidOperationException(string.Format("Cannot delete current journal because it isn't last synced file. Current journal number: {0}, the last one which was synced {1}", _waj.CurrentFile?.Number ?? -1, _lastSyncedJournal));


                if (_waj._env.NextWriteTransactionId - 1 != _lastSyncedTransactionId)
                    throw new InvalidOperationException("Cannot delete current journal because it hasn't synced everything up to the last write transaction");

                _waj._files = _waj._files.RemoveFront(1);
                _waj.CurrentFile = null;

                current.DeleteOnClose = true;
                current.Release();

                _waj._headerAccessor.Modify(header => _waj._updateLogInfo(header));
            }
        }

        public void WriteToJournal(LowLevelTransaction tx, int pageCount)
        {
            var pages = PrepreToWriteToJournal(tx, _compressionPager, pageCount);

            if (tx.IsLazyTransaction && _lazyTransactionBuffer == null)
            {
                _lazyTransactionBuffer = new LazyTransactionBuffer(_env.Options);
            }

            if (CurrentFile == null || CurrentFile.AvailablePages < pages.NumberOfPages)
            {
                _lazyTransactionBuffer?.WriteBufferToFile(CurrentFile, tx);
                CurrentFile = NextFile(pages.NumberOfPages);
            }

            CurrentFile.Write(tx, pages, _lazyTransactionBuffer, pageCount);

            if (CurrentFile.AvailablePages == 0)
            {
                _lazyTransactionBuffer?.WriteBufferToFile(CurrentFile, tx);
                CurrentFile = null;
            }
        }

        private CompressedPagesResult PrepreToWriteToJournal(LowLevelTransaction tx, AbstractPager compressionPager, int pageCountIncludingAllOverflowPages)
        {
            //TODO: comment the memory outline that we write here

            int pageSize = tx.Environment.Options.PageSize;
            var txPages = tx.GetTransactionPages();
            var numberOfPages = txPages.Count;

            // We want to include the Transaction Header straight into the compression buffer.
            var sizeOfPagesHeader = numberOfPages * sizeof(TransactionHeaderPageInfo);
            var maxSizeRequiringCompression = pageCountIncludingAllOverflowPages * pageSize + sizeOfPagesHeader
                 + numberOfPages * sizeof(long);
            var outputBufferSize = LZ4.MaximumOutputLength(maxSizeRequiringCompression);
            var outputBufferInPages = (outputBufferSize + sizeof(TransactionHeader)) / pageSize +
                                      ((outputBufferSize + sizeof(TransactionHeader)) % pageSize == 0 ? 0 : 1);

            // The pages required includes the intermediate pages and the required output pages. 
            var pagesRequired = (pageCountIncludingAllOverflowPages + outputBufferInPages);
            var pagerState = compressionPager.EnsureContinuous(0, pagesRequired);
            tx.EnsurePagerStateReference(pagerState);

            var outputBuffer = compressionPager.AcquirePagePointer(tx, 0);

            var pagesInfo = (TransactionHeaderPageInfo*)outputBuffer;
            var write = outputBuffer + sizeOfPagesHeader;
            var pageSequencialNumber = 0;

            foreach (var txPage in txPages)
            {
                var scratchPage = tx.Environment.ScratchBufferPool.AcquirePagePointer(tx, txPage.ScratchFileNumber,
                    txPage.PositionInScratchBuffer);
                pagesInfo[pageSequencialNumber].PageNumber = ((PageHeader*) scratchPage)->PageNumber;
                *(long*) write = ((PageHeader*) scratchPage)->PageNumber;
                write += sizeof(long);
                _diffPage.Output = write;
                _diffPage.Modified = scratchPage;
                _diffPage.Size = txPage.NumberOfPages*pageSize;
                if (txPage.PreviousVersion != null)
                {
                    _diffPage.Original = txPage.PreviousVersion.Pointer;
                    _diffPage.ComputeDiff();
                }
                else
                {
                    _diffPage.Original = null;
                    _diffPage.ComputeNew();
                }
                write += _diffPage.OutputSize;
                pagesInfo[pageSequencialNumber].Size = _diffPage.OutputSize == 0 ? 0 : _diffPage.Size;
                pagesInfo[pageSequencialNumber].DiffSize = _diffPage.IsDiff ? _diffPage.OutputSize : 0;
                ++pageSequencialNumber;
            }
            var totalSizeWritten = (int)(write - outputBuffer + sizeOfPagesHeader);


            var fullTxBuffer = outputBuffer + pageCountIncludingAllOverflowPages * pageSize;

            var compressionBuffer = fullTxBuffer + sizeof(TransactionHeader);

            var compressedLen = _lz4.Encode64(
                outputBuffer,
                compressionBuffer,
                totalSizeWritten,
                outputBufferSize);

            // We need to account for the transaction header as part of the total length.
            var totalLength = compressedLen + sizeof(TransactionHeader);
            var remainder = totalLength % pageSize;
            var compressedPages = (totalLength / pageSize) + (remainder == 0 ? 0 : 1);

            if (remainder != 0)
            {
                // zero the remainder of the page
                UnmanagedMemory.Set(compressionBuffer + totalLength, 0, pageSize - remainder);
            }

            var txHeaderPage = tx.GetTransactionHeaderPage();
            var txHeaderBase = tx.Environment.ScratchBufferPool.AcquirePagePointer(tx, txHeaderPage.ScratchFileNumber,
                txHeaderPage.PositionInScratchBuffer);
            var txHeader = (TransactionHeader*)txHeaderBase;
            txHeader->CompressedSize = compressedLen;
            txHeader->UncompressedSize = totalSizeWritten;
            txHeader->PageCount = numberOfPages;
            txHeader->Hash = Hashing.XXHash64.Calculate(compressionBuffer, compressedLen);

            var prepreToWriteToJournal = new CompressedPagesResult
            {
                Base = fullTxBuffer,
                NumberOfPages = compressedPages
            };
            // Copy the transaction header to the output buffer. 
            Memory.Copy(fullTxBuffer, txHeaderBase, sizeof(TransactionHeader));
            Debug.Assert(((long)fullTxBuffer % pageSize) == 0, "Memory must be page aligned");
            return prepreToWriteToJournal;
        }

        public void TruncateJournal(int pageSize)
        {
            // switching transactions modes requires to close jounal, 
            // truncate it (in case of recovery) and create next journal file
            _lazyTransactionBuffer?.WriteBufferToFile(CurrentFile, null);
            CurrentFile?.JournalWriter.Truncate(pageSize * CurrentFile.WritePagePosition);
            CurrentFile = null;
        }
    }


    public unsafe struct CompressedPagesResult
    {
        public byte* Base;
        public int NumberOfPages;
    }

}
