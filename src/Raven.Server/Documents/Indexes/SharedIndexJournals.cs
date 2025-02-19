using System;
using System.IO;
using System.Threading;
using Raven.Server.Utils;
using Sparrow.Server.Logging;
using Sparrow.Server.Utils;
using Voron;
using Voron.Impl.Journal;

namespace Raven.Server.Documents.Indexes;

public class SharedIndexJournals : IJournalMerger, IDisposable
{
    private readonly DocumentDatabase _documentDatabase;

    public SharedIndexJournals(DocumentDatabase documentDatabase)
    {
        _documentDatabase = documentDatabase;
        string sharedJournalsPath = documentDatabase.Configuration.Indexing.SharedJournalsPath.FullPath;
        string documentDatabaseName = documentDatabase.Name + ".SharedJournals";
        var options = documentDatabase.Configuration.Indexing.RunInMemory
            ? StorageEnvironmentOptions.CreateMemoryOnly(sharedJournalsPath, Path.Combine(sharedJournalsPath, "Temp"),
                documentDatabase.IoChanges, documentDatabase.CatastrophicFailureNotification, LoggingResource.Database(documentDatabaseName),
                LoggingComponent.Name(documentDatabaseName))
            : StorageEnvironmentOptions.ForPath(sharedJournalsPath, Path.Combine(sharedJournalsPath, "Temp"), null,
                documentDatabase.IoChanges, documentDatabase.CatastrophicFailureNotification, LoggingResource.Database(documentDatabaseName),
                LoggingComponent.Name(documentDatabaseName));
                
        _env = new StorageEnvironment(options);
        _env.Journal.BranchJournalMerger = this;

        _sharedJournalsThread = PoolOfThreads.GlobalRavenThreadPool.LongRunning(
            WriteSharedJournals, null,
            ThreadNames.ForIndexSharedJournals("Index SharedJournals for " + documentDatabase.Name, documentDatabase.Name));
    }

    
    private readonly ManualResetEventSlim _waitForJournals = new(initialState: true);
    private readonly StorageEnvironment _env;
    private bool _disposed;
    private readonly PoolOfThreads.LongRunningWork _sharedJournalsThread;

    private void WriteSharedJournals(object _)
    {
        using (_env.Journal.SharedJournalsScope(_documentDatabase.TransactionMergerShutdown))
        {
            while (_disposed is false)
            {
                _waitForJournals.Wait();
                _waitForJournals.Reset();
                do
                {
                    using (var txw = _env.WriteTransaction())
                    {
                        txw.Commit();
                    }
                } while (_env.Journal.HasBranchCommits);
            }
        }
    }

    public void JournalMergeSubmitted()
    {
        _waitForJournals.Set();
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
