using System;
using System.IO;
using System.Linq;
using System.Threading;
using Raven.Server.Config.Categories;
using Raven.Server.Logging;
using Raven.Server.Storage.Layout;
using Raven.Server.Utils;
using Sparrow.Logging;
using Sparrow;
using Sparrow.Server.Logging;
using Sparrow.Server.Utils;
using Voron;
using Voron.Impl;
using Voron.Impl.Journal;

namespace Raven.Server.Documents.Indexes;

public class SharedIndexJournals : IJournalMerger, IDisposable
{
    private readonly DocumentDatabase _documentDatabase;

    public SharedIndexJournals(DocumentDatabase documentDatabase)
    {
        _documentDatabase = documentDatabase;
        string sharedJournalsPath = documentDatabase.Configuration.Indexing.SharedJournalsPath.FullPath;
        string documentDatabaseName = $"{documentDatabase.Name}.Indexes.{IndexingConfiguration.SharedJournalsStorageName}";
        var options = documentDatabase.Configuration.Indexing.RunInMemory
            ? StorageEnvironmentOptions.CreateMemoryOnly(sharedJournalsPath, Path.Combine(sharedJournalsPath, "Temp"),
                documentDatabase.IoChanges, documentDatabase.CatastrophicFailureNotification, LoggingResource.Database(documentDatabaseName),
                LoggingComponent.Name(documentDatabaseName))
            : StorageEnvironmentOptions.ForPath(sharedJournalsPath, Path.Combine(sharedJournalsPath, "Temp"), null,
                documentDatabase.IoChanges, documentDatabase.CatastrophicFailureNotification, LoggingResource.Database(documentDatabaseName),
                LoggingComponent.Name(documentDatabaseName));
        
        options.AddToInitLog = documentDatabase.AddToInitLog;
        options.Encryption.MasterKey = documentDatabase.MasterKey?.ToArray();
        options.Encryption.RegisterForJournalCompressionHandler();
        VoronOptionsFromConfiguration.Apply(options, documentDatabase.Configuration);

        options.OnNonDurableFileSystemError += documentDatabase.HandleNonDurableFileSystemError;
        options.OnRecoveryError += (s, e) => documentDatabase.HandleOnIndexRecoveryError(IndexingConfiguration.SharedJournalsStorageName, s, e);
        options.OnIntegrityErrorOfAlreadySyncedData += (s, e) => documentDatabase.HandleOnIndexIntegrityErrorOfAlreadySyncedData(IndexingConfiguration.SharedJournalsStorageName, s, e);
        options.OnRecoverableFailure += documentDatabase.HandleRecoverableFailure;

        _env = StorageLoader.OpenEnvironment(options, StorageEnvironmentWithType.StorageEnvironmentType.SharedJournals);
        _logger = RavenLogManager.Instance.GetLoggerForDatabase<SharedIndexJournals>(documentDatabase);
        _env.Journal.BranchJournalMerger = this;
        _env.Journal.OnBranchHardLinkLimitReached = OnBranchHardLinkLimitReached;
        _scopeForSharedJournals = _env.Journal.SharedJournalsScope();

        _sharedJournalsThread = PoolOfThreads.GlobalRavenThreadPool.LongRunning(
            WriteSharedJournals, null,
            ThreadNames.ForIndexSharedJournals("Index SharedJournals for " + documentDatabase.Name, documentDatabase.Name));
    }

    
    private readonly ManualResetEventSlim _waitForJournals = new(initialState: true);
    private readonly StorageEnvironment _env;
    public StorageEnvironment Env => _env;
    private bool _disposed;
    private readonly PoolOfThreads.LongRunningWork _sharedJournalsThread;
    private readonly WriteAheadJournal.ScopeForSharedJournals _scopeForSharedJournals;
    private readonly RavenLogger _logger;

    /// <summary>
    /// The root's writer. It commits asynchronously so that FlushMergedJournalEntries sees
    /// LowLevelTransaction.IsAsyncCommit and submits the merged batch into the write pipeline instead of
    /// taking the inline path, which drains the window before every write. A branch's wait then stops
    /// including "the previous batch's write has to finish before mine is even submitted".
    /// <para>
    /// The chain is one deep: the batch being written is left in <c>inFlight</c> while the next transaction
    /// is already open and collecting, and is completed on the following round. Stage 2 runs with
    /// waitForDurability false, so ending it waits for the submission, not for the disk.
    /// </para>
    /// </summary>
    private void WriteSharedJournals(object _)
    {
        using (_scopeForSharedJournals)
        {
            var persistentContext = new TransactionPersistentContext();
            Transaction current = null;
            Transaction inFlight = null;

            try
            {
                while (_disposed is false)
                {
                    try
                    {
                        _waitForJournals.Wait();
                        _waitForJournals.Reset();
                        do
                        {
                            var curJournal = _env.Journal.CurrentFile;
                            CommitMergedBatch(persistentContext, ref current, ref inFlight, forceOwnJournalWrite: false);

                            if (curJournal == _env.Journal.CurrentFile)
                                continue;

                            // this will force us to do an actual commit
                            // to our own journal, and thus force us to 
                            // flush the journals, etc...
                            // 
                            // This is required to ensure that journals are properly
                            // flushed & handled after we switch between journals
                            CommitMergedBatch(persistentContext, ref current, ref inFlight, forceOwnJournalWrite: true);
                        } while (_env.Journal.HasBranchCommits);

                        // nothing else is queued, so nothing is going to complete the outstanding batch for
                        // us. Close the chain and let go of the open transaction too - holding the root's
                        // write transaction open while idle blocks every other writer to this environment,
                        // and the next wake-up starts a fresh one.
                        CompleteInFlightBatch(ref inFlight);

                        var idle = current;
                        current = null;
                        idle?.Dispose();
                    }
                    catch (Exception e)
                    {
                        DiscardBatchesOnError(ref current, ref inFlight);
                        Interlocked.Exchange(ref _env.Journal.SharedJournalState, new SharedJournalState()).SetException(e);
                    }
                }
            }
            finally
            {
                DiscardBatchesOnError(ref current, ref inFlight);
            }
        }
    }

    private void CommitMergedBatch(TransactionPersistentContext persistentContext, ref Transaction current, ref Transaction inFlight, bool forceOwnJournalWrite)
    {
        current ??= _env.WriteTransaction(persistentContext);

        if (forceOwnJournalWrite)
        {
            // we do a dummy change here to force the env
            // to think that it has an actual transaction and thus
            // will force it to flush / remove older journal
            current.LowLevelTransaction.ModifyPage(0);
        }

        var next = current.BeginAsyncCommitAndStartNewTransaction(persistentContext);
        var justSubmitted = current;
        current = next;

        // the batch before this one has had this whole round to be submitted
        CompleteInFlightBatch(ref inFlight);
        inFlight = justSubmitted;
    }

    private static void CompleteInFlightBatch(ref Transaction inFlight)
    {
        var batch = inFlight;
        inFlight = null;

        if (batch == null)
            return;

        using (batch)
            batch.EndAsyncCommit();
    }

    private static void DiscardBatchesOnError(ref Transaction current, ref Transaction inFlight)
    {
        // an outstanding async commit has to be ended before its transaction can be disposed, and the
        // environment will not shut down while one is still open
        try
        {
            CompleteInFlightBatch(ref inFlight);
        }
        catch
        {
            // the failure is already being reported through SharedJournalState
        }

        var open = current;
        current = null;
        open?.Dispose();
    }

    public void JournalMergeSubmitted()
    {
        _waitForJournals.Set();
    }

    private void OnBranchHardLinkLimitReached(StorageEnvironment branchEnv)
    {
        if (_logger.IsWarnEnabled)
            _logger.Warn($"Index environment at '{branchEnv.Options.BasePath}' exceeded the file system hard-link limit and switched to unshared journal mode. Subsequent journal writes for this index will go to its own Journals directory.");
    }

    public void Dispose()
    {
        _disposed = true;
        _waitForJournals.Set();
        _sharedJournalsThread.Join(Timeout.Infinite);
        _waitForJournals.Dispose();
        _env.Dispose();
    }

    public void Register(StorageEnvironmentOptions branchOptions)
    {
        branchOptions.RootJournal = _env.Journal;
    }
}
