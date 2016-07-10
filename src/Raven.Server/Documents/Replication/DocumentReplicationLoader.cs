using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Replication;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Collections;

namespace Raven.Server.Documents.Replication
{
	//TODO: add code to handle DocumentReplicationStatistics from each replication executer (also aggregation code?)
	//TODO: add support to destinations changes, so they can be changed dynamically (added/removed)
	public class DocumentReplicationLoader
    {
        private readonly ILog _log;
        private readonly DocumentDatabase _database;

        private ReplicationDocument _replicationDocument;
        private readonly ConcurrentSet<OutgoingDocumentReplication> _outgoingReplications = new ConcurrentSet<OutgoingDocumentReplication>();
		private readonly ConcurrentSet<IncomingDocumentReplication> _incomingReplications = new ConcurrentSet<IncomingDocumentReplication>();

		private readonly ConcurrentDictionary<string,DateTime> _lastConnectionDisconnect = new ConcurrentDictionary<string, DateTime>();
		private readonly ConcurrentDictionary<string, int> _connectionTimeouts = new ConcurrentDictionary<string, int>();
	    private readonly TcpListener _listener;

	    private readonly Thread _clientAcceptThread;
	    public DocumentReplicationLoader(DocumentDatabase database, IPAddress listeningAddress,int listeningPort) 
        {
			//TODO : make configurable port and listening ip address
			//TODO : this _must_ be configurable, otherwise unit tests won't work
			_listener = new TcpListener(listeningAddress, listeningPort);
			_listener.Start();

            _database = database;
            _log = LogManager.GetLogger(GetType());
            _database.Notifications.OnSystemDocumentChange += HandleSystemDocumentChange;
            ReplicationUniqueName = $"{_database.Name} -> {_database.DbId}";
			LoadConfigurations();

	        _clientAcceptThread = new Thread(AcceptReplicationClients)
	        {
				IsBackground = true,

	        };

			_clientAcceptThread.Start();
        }

	    private void AcceptReplicationClients()
	    {
		    while (!_database.DatabaseShutdown.IsCancellationRequested)
		    {
			    try
			    {
				    var replicationTcpClientTask = _listener.AcceptTcpClientAsync();
				    replicationTcpClientTask.Wait();
				    var tcpClient = replicationTcpClientTask.Result;

				    StartIncomingReplication(tcpClient);
			    }
			    catch (SocketException) //this is caused by invoking _listener.Stop() and it is expected part of disposal
			    {
				    _log.Debug(
					    "Stopped tcp socket listener for incoming document replication connections. (Listening thread stopped)");
				    break;
			    }
			    catch (Exception e)
			    {
				    _log.ErrorException("Caught exception in thread that listens for incoming document replication connections. This is not supposed to happen and it is likely a bug.",e);
					//TODO: not entirely sure if this is recoverable error, but for now I will leave it like this,
					//until it can be reviewed in depth (some edge-cases might be relevant to this)
					//
					//maybe add a failure count here with back-off strategy, or maybe restart the thread with new TcpListener instance
					//also need to review what network related errors might cause TcpListener to fail while 'accepting tcp clients'
			    }
		    }
	    }

	    private void StartIncomingReplication(TcpClient tcpClient)
	    {
		    lock (_incomingReplications)
		    {
			    var newIncomingReplication = new IncomingDocumentReplication(_database, tcpClient);
			    _incomingReplications.Add(newIncomingReplication);
			    newIncomingReplication.ReceiveError += DisconnectAndDisposeIncomingReplicationHandler;
			    newIncomingReplication.Start();
		    }
	    }

		//if we received 'ReceiveError' event, this means unrecoverable error occured at the incoming replication instance,
		//so we will close the recieve thread and dispose the failted replication handler instance
	    private void DisconnectAndDisposeIncomingReplicationHandler(IncomingDocumentReplication replicationInstance)
	    {
		    replicationInstance.ReceiveError -= DisconnectAndDisposeIncomingReplicationHandler;
		    _incomingReplications.TryRemove(replicationInstance);
			replicationInstance.Dispose();
	    }

        public string ReplicationUniqueName { get; }

        protected bool ShouldReloadConfiguration(string systemDocumentKey)
        {
            return systemDocumentKey.Equals(Constants.Replication.DocumentReplicationConfiguration,
                StringComparison.OrdinalIgnoreCase);
        }

        protected void LoadConfigurations()
        {
            DocumentsOperationContext context;
            using (_database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                var configurationDocument = _database.DocumentsStorage.Get(context,
                    Constants.Replication.DocumentReplicationConfiguration);

                if (configurationDocument == null)
                    return;

                try
                {
                    _replicationDocument = JsonDeserialization.ReplicationDocument(configurationDocument.Data);
                    //the destinations here are the ones that are outbound..
                    if (_replicationDocument.Destinations == null) //precaution, should not happen
                        _log.Warn("Invalid configuration document, Destinations property must not be null. Replication will not be active");
                }
                catch (Exception e)
                {
					
					_log.Error("failed to deserialize replication configuration document. This is something that is not supposed to happen. Reason:" + e);
                }

                Debug.Assert(_replicationDocument.Destinations != null);
                OnConfigurationChanged(_replicationDocument.Destinations);
            }
        }

        //TODO: add here error handling for the following cases
        //1) what if unexpected exception happens in outgoing replication dispose?
        //2) what if sending a replication batch is happening during a call to dispose?
        protected void OnConfigurationChanged(List<ReplicationDestination> destinations)
        {
            lock (_outgoingReplications)
            {
				//TODO : do disposals in parallel
                foreach (var replication in _outgoingReplications)
                    replication.Dispose();
                _outgoingReplications.Clear();

                foreach (var dest in destinations)
                {
					var outgoingDocumentReplication = new OutgoingDocumentReplication(_database, dest);
	                try
	                {
		                outgoingDocumentReplication.InitializeAndConnect();
						outgoingDocumentReplication.ClosedConnectionRemoteFailure += OnOutgoingReplicationRemoteFault;
					}
	                catch (Exception e)
	                {
						var hasSucceeded = false;
		                Exception lastException = null;
						for (int i = 0; i < 3; i++)
						{
							try
							{
								outgoingDocumentReplication.ClosedConnectionRemoteFailure -= OnOutgoingReplicationRemoteFault;
								outgoingDocumentReplication = new OutgoingDocumentReplication(_database, dest);
								outgoingDocumentReplication.InitializeAndConnect();
								outgoingDocumentReplication.ClosedConnectionRemoteFailure += OnOutgoingReplicationRemoteFault;
								hasSucceeded = true;
							}
							catch (Exception)
							{
								// not rethrown, since we are retrying initialization
								// for cases of transient errors
								lastException = e;
							}
							if (hasSucceeded)
								break;

							Thread.Sleep(i * 1000);//increasingly wait between retries
						}

						if(!hasSucceeded)
							if(lastException != null)
								_log.ErrorException("Tried to connect to remote replication destination, but failed. Last exception was ", lastException);
							else
								_log.Error("Tried to connect to remote replication destination, but failed. lastException is null, this is very strange");
	                }

                }

            }
        }

	    private void OnOutgoingReplicationRemoteFault(OutgoingDocumentReplication instance)
	    {
			_lastConnectionDisconnect.AddOrUpdate(instance.ReplicationUniqueName, 
				key => DateTime.UtcNow, (key,existing) => DateTime.UtcNow);
		    var timeout = _connectionTimeouts.AddOrUpdate(instance.ReplicationUniqueName, 
								key => 500, (key, existing) => existing * 2 < 60000 ? existing * 2 : existing);
			instance.Disconnect();
#pragma warning disable 4014
			//will resume outgoing replication eventually
		    Task.Delay(timeout)
			    .ContinueWith(t => instance.InitializeAndConnect())				
				.WithCancellation(_database.DatabaseShutdown)
				.ContinueWith(t =>
			    {
					//reset timeout
				    _connectionTimeouts[instance.ReplicationUniqueName] = 500;
			    },TaskContinuationOptions.OnlyOnRanToCompletion)
				.ContinueWith(t => _log.ErrorException("Failed to connect to remote replication destination.",t.Exception),
								 TaskContinuationOptions.OnlyOnFaulted);
#pragma warning restore 4014
	    }

        public void Dispose()
        {
			_log.Debug("Starting disposal of DocumentReplicationLoader... stopping listening for incoming tcp replication connections.");
			_listener.Stop(); //calling this should effectively stop listening thread
			var stoppedGracefully = _clientAcceptThread.Join(2000);

	        _log.Debug(stoppedGracefully
		        ? "Listening thread stopped gracefully"
		        : "Listening thread did not end withing 5 seconds. Something might be wrong here.. if this persists, it should be investigated.");

	        lock (_outgoingReplications)
            {
                foreach (var replicationHandler in _outgoingReplications)
                    replicationHandler.Dispose();
            }

	        lock (_incomingReplications)
	        {
		        foreach(var replicationHandler in _incomingReplications)
					DisconnectAndDisposeIncomingReplicationHandler(replicationHandler);
	        }

			_database.Notifications.OnSystemDocumentChange -= HandleSystemDocumentChange;
        }

        private void HandleSystemDocumentChange(DocumentChangeNotification notification)
        {
            if (ShouldReloadConfiguration(notification.Key))
            {
                LoadConfigurations();

                if (_log.IsDebugEnabled)
                    _log.Debug($"Replication configuration was changed: {notification.Key}");
            }
        }
    }
}
