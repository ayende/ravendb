using System;
using System.Diagnostics;
using System.Linq;
using System.Net.WebSockets;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Replication;
using Raven.Server.ReplicationUtil;
using Raven.Server.ServerWide.Context;
using Sparrow;

namespace Raven.Server.Documents.Replication
{
    public class OutgoingDocumentReplication : IDisposable
    {
        private readonly DocumentDatabase _database;
	    protected long _lastSentEtag;
        private DocumentReplicationTransport _transport;
        private readonly ILog _log = LogManager.GetLogger(typeof (OutgoingDocumentReplication));
        private readonly DocumentsOperationContext _context;
        private bool _isInitialized;
	    private DateTime _lastSentHeartbeat;
        private Thread _replicationThread;
        private readonly CancellationTokenSource _cancellationTokenSource;
	    private const int RetriesCount = 3;
		private readonly TimeSpan _minimalHeartbeatInterval = TimeSpan.FromSeconds(15);
        public readonly AsyncManualResetEvent _waitForChanges;
        private readonly string _replicationUniqueName;

	    public event Action<OutgoingDocumentReplication> ClosedConnectionRemoteFailure;

        public OutgoingDocumentReplication(DocumentDatabase database, 
            ReplicationDestination destination,
            DocumentReplicationTransport transport = null)
        {
            _database = database;
	        Destination = destination;
	        _database.Notifications.OnDocumentChange += HandleDocumentChange;
            _lastSentEtag = -1;
            _database.DocumentsStorage.ContextPool.AllocateOperationContext(out _context);
			_lastSentHeartbeat = DateTime.MinValue;
	        _transport = transport;
			_cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(_database.DatabaseShutdown);
            _waitForChanges = new AsyncManualResetEvent(_cancellationTokenSource.Token);

            _replicationUniqueName = $"Outgoing Replication Thread <{destination.Url} -> {destination.Database}>";           
        }

        //TODO : add parameter to notification that would indicate that the document
        //was received from a replication propagation and not from a user
        private void HandleDocumentChange(DocumentChangeNotification notification)
        {
            _waitForChanges.SetByAsyncCompletion();
        }

        private async Task ReplicateDocumentsAsync()
        {
            while (_cancellationTokenSource.IsCancellationRequested == false)
            {
                if (_log.IsDebugEnabled)
                    _log.Debug($"Starting replication for '{_replicationUniqueName}'.");

                _waitForChanges.Reset();

	            try
				{
                    _cancellationTokenSource.Token.ThrowIfCancellationRequested();
	                var result = await ExecuteReplicationOnce();

					if (_log.IsDebugEnabled)
						LogReplicationBatchSent(result);

					if (result == ReplicationBatchResult.RemoteFailure)
					{
						_log.Error("Received notification of an error happening in the remote node. Closing current connection, will try to reconnect later");
						OnClosedConnectionRemoteFailure();
						break;
					}
				}
                catch (OutOfMemoryException oome)
                {
                    _log.WarnException($"Out of memory occured for '{_replicationUniqueName}'.", oome);
                    // TODO [ppekrol] GC?
                }
                catch (OperationCanceledException)
                {
                    return;
                }
                catch (Exception e)
                {
                    _log.ErrorException($"Exception occured for '{_replicationUniqueName}'.", e);
	                return;
                }

                if (HasMoreDocumentsToSend)
                    continue;

                try
                {
                    //if this returns false, this means either timeout or canceled token is activated                    
	                while (await _waitForChanges.WaitAsync(_minimalHeartbeatInterval) == false)
		                await _transport.SendHeartbeatAsync();
                }
                catch (OperationCanceledException)
                {
					//Debugger.Launch();
					return;
                }
            }
        }

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		private void LogReplicationBatchSent(ReplicationBatchResult result)
		{
			switch (result)
			{
				case ReplicationBatchResult.SuccessfullySent:
					_log.Debug($"Finished replication for '{_replicationUniqueName}'. Stuff was sent successfully.");
					break;
				case ReplicationBatchResult.NothingToDo:
					_log.Debug($"Finished replication for '{_replicationUniqueName}'; was nothing to do");
					break;
				case ReplicationBatchResult.RemoteFailure:
					_log.Debug($"Finished replication for '{_replicationUniqueName}'; something went wrong on the other side.");
					break;
				default:
					throw new ArgumentOutOfRangeException();
			}
		}

		public bool HasMoreDocumentsToSend
        {
            get
            {
                using(_context.OpenReadTransaction())
                    return DocumentsStorage.ReadLastEtag(
                        _context.Transaction.InnerTransaction) < _lastSentEtag;
            }
        }

	    public ReplicationDestination Destination { get; }

	    public string ReplicationUniqueName => _replicationUniqueName;

	    private enum ReplicationBatchResult
	    {
		    SuccessfullySent,
			NothingToDo,
			RemoteFailure
	    }

        private async Task<ReplicationBatchResult> ExecuteReplicationOnce()
        {
            Debug.Assert(_isInitialized);

            //just for shorter code
            var documentStorage = _database.DocumentsStorage;
            using (_context.OpenReadTransaction())
            {
                //TODO: make replication batch size configurable
                //also, perhaps there should be timers/heuristics
                //that would dynamically resize batch size
                var replicationBatch =
                    documentStorage
                        .GetDocumentsAfter(_context, _lastSentEtag, 0, 1024)
                        .Where(x => !x.Key.ToString().StartsWith("Raven/"))
                        .ToList();
                        //the filtering here will need to be reworked -> it is not efficient
                        //TODO: do not forget to make version of GetDocumentsAfter with a prefix filter	
                        //alternatively 1 -> create efficient StartsWith() for LazyString				
                        //alternatively 2 -> create a "filter system" that would abstract the logic -> what documents 
                        //should and should not be replicated
                if (replicationBatch.Count == 0)
                    return ReplicationBatchResult.NothingToDo;
	            try
	            {
		            var currentLastSentEtag = await _transport.SendDocumentBatchAsync(replicationBatch)
			            .WithCancellation(_cancellationTokenSource.Token);
		            if (currentLastSentEtag == -1)
		            {
			            //sent batch successfully, but something went wrong on the other side
			            //do not advance last sent etag, so the batch would be resent next cycle
			            _log.Warn(
				            $"Sent the batch successfully, but something went wrong on the other side; thus, do not advance last sent etag. Current last sent etag -> {_lastSentEtag}");
			            return ReplicationBatchResult.RemoteFailure;
		            }

		            //TODO : consider retry logic here, so after multiple tries, it will fail loudly
		            _lastSentEtag = currentLastSentEtag;
	            }
	            catch (WebSocketException e)
	            {
					//Debugger.Launch();
					_log.Warn(
			            "Sending document replication batch is interrupted. This is not necessarily an issue. Reason: " + e);
		            return ReplicationBatchResult.RemoteFailure;
	            }
	            catch (OperationCanceledException e)
	            {
					//Debugger.Launch();
					//TODO: handle this properly, log the error properly
					return ReplicationBatchResult.NothingToDo;
				}
				catch (Exception e)
				{
					//Debugger.Launch();
                    _log.Error("Sending document replication batch has failed. Apparently, remote server had an exception thrown. Reason: " + e);
	                return ReplicationBatchResult.RemoteFailure;
                }
            }
			return ReplicationBatchResult.SuccessfullySent;
        }

        public async Task InitializeAsync()
        {
	        if (_transport == null)
	        {
		        _transport = new DocumentReplicationTransport(
			        Destination.Url,
			        _database.DbId,
			        _database.Name,
			        Destination.Database,
			        _database.DatabaseShutdown,
			        _context);
	        }

	        await _transport.EnsureConnectionAsync();
            _lastSentEtag = await _transport.GetLastEtag();
			_replicationThread = new Thread(() => ReplicateDocumentsAsync().Wait())
			{
				IsBackground = true,
				Name = _replicationUniqueName
			};
			_replicationThread.Start();

            _isInitialized = true;
        }

	    public void Disconnect()
	    {			
			//note: transport dispose attempts to send 'close' message as well
		    _transport.Dispose();
			_cancellationTokenSource.Cancel();
			try
			{
				_replicationThread.Join(1000);
			}
			catch (ThreadStateException)
			{
			}
		}

        public void Dispose()
        {
	        Disconnect();
			_context.Dispose();
            _database.Notifications.OnDocumentChange -= HandleDocumentChange;
        }

	    private void OnClosedConnectionRemoteFailure()
	    {
		    var closedConnectionRemoteFailure = ClosedConnectionRemoteFailure;
			closedConnectionRemoteFailure?.Invoke(this);
	    }
    }
}
