using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Util;
using Raven.Server.Documents;
using Sparrow;

namespace Raven.Server.ReplicationUtil
{
    public abstract class BaseReplicationExecuter : IDisposable
    {
        protected readonly ILog _log;

        protected readonly DocumentDatabase _database;
        protected Thread _replicationThread;
        protected bool _disposed;

        protected readonly CancellationTokenSource _cancellationTokenSource;
        public CancellationToken CancellationToken => _cancellationTokenSource.Token;
        public readonly AsyncManualResetEvent WaitForChanges;

        public abstract string ReplicationUniqueName { get; }

        protected BaseReplicationExecuter(DocumentDatabase database)
        {
            _log = LogManager.GetLogger(GetType());
            _database = database;
            _cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(_database.DatabaseShutdown);
            WaitForChanges = new AsyncManualResetEvent(_cancellationTokenSource.Token);
        }

        public void Start()
        {
            if (_replicationThread != null)
                return;

            //haven't found better way to synchronize async method
            _replicationThread = new Thread(() => AsyncHelpers.RunSync(ExecuteReplicationLoop))
            {
                Name = $"Replication thread, {ReplicationUniqueName}",
                IsBackground = true
            };

            _replicationThread.Start();
        }

        protected abstract Task ExecuteReplicationOnce();

        /// <summary>
        /// returns true if there are items left to replicate, and false 
        /// if all items were replicated and the replication needs to go to sleep
        /// </summary>
        protected abstract bool HasMoreDocumentsToSend();

        private async Task ExecuteReplicationLoop()
        {
            while (_cancellationTokenSource.IsCancellationRequested == false)
            {
                if (_log.IsDebugEnabled)
                    _log.Debug($"Starting replication for '{ReplicationUniqueName}'.");

                WaitForChanges.Reset();

                try
                {
                    _cancellationTokenSource.Token.ThrowIfCancellationRequested();

                    await ExecuteReplicationOnce();

                    if (_log.IsDebugEnabled)
                        _log.Debug($"Finished replication for '{ReplicationUniqueName}'.");
                }
                catch (OutOfMemoryException oome)
                {
                    _log.WarnException($"Out of memory occured for '{ReplicationUniqueName}'.", oome);
                    // TODO [ppekrol] GC?
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                catch (Exception e)
                {
                    _log.WarnException($"Exception occured for '{ReplicationUniqueName}'.", e);
                }

                if (HasMoreDocumentsToSend())
                    continue;

                try
                {
                    //if this returns false, this means canceled token is activated                    
                    if (await WaitForChanges.WaitAsync() == false)
                        //thus, if code reaches here, cancellation token source has "cancel" requested
                        return; 
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
        }

        public virtual void Dispose()
        {
            try
            {
                _cancellationTokenSource.Cancel();					
            }
            catch (ObjectDisposedException)
            {
                //precaution, should not happen
                _log.Warn("ObjectDisposedException thrown during replication executer disposal, should not happen. Something is wrong here.");
            }
            catch (AggregateException e)
            {
                _log.Error("Error during replication executer disposal, most likely it is a bug.",e);
            }
            finally
            {
                _disposed = true;
            }
        }
    }
}