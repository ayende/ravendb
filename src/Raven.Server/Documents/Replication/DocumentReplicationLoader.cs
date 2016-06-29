using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
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
        private readonly ConcurrentSet<OutgoingDocumentReplication> _outgoingReplications;

		private readonly ConcurrentDictionary<string,DateTime> _lastConnectionDisconnect = new ConcurrentDictionary<string, DateTime>();
		private readonly ConcurrentDictionary<string, int> _connectionTimeouts = new ConcurrentDictionary<string, int>();

        public DocumentReplicationLoader(DocumentDatabase database) 
        {
            _outgoingReplications = new ConcurrentSet<OutgoingDocumentReplication>();
            _database = database;
            _log = LogManager.GetLogger(GetType());
            _database.Notifications.OnSystemDocumentChange += HandleSystemDocumentChange;
            ReplicationUniqueName = $"{_database.Name} -> {_database.DbId}";
			LoadConfigurations();
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
					//Debugger.Launch();
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

                var initializationTasks = new List<Task>();
                foreach (var dest in destinations)
                {
                    var outgoingDocumentReplication = new OutgoingDocumentReplication(_database, dest);
                    initializationTasks.Add(
						outgoingDocumentReplication.InitializeAsync()
												   .ContinueWith(async t =>
													{
														var currentReplicationInstance = outgoingDocumentReplication;
														var currentDestination = dest;
														if (t.IsFaulted)
														{
															var hasSucceeded = false;

															//retry connecting here
															for (int i = 0; i < 3; i++)
															{
																try
																{
																	currentReplicationInstance = new OutgoingDocumentReplication(_database, currentDestination);
																	await currentReplicationInstance.InitializeAsync();
																	hasSucceeded = true;
																}
																catch (Exception)
																{
																	//Debugger.Launch();
																	// ignored, since we are retrying initialization
																	// for cases of transient errors
																}
																if (hasSucceeded)
																	break;

																await Task.Delay(i * 1000);//increasingly wait between retries
															}

															if(hasSucceeded)
																_outgoingReplications.Add(currentReplicationInstance);
														}
														else
														{
															currentReplicationInstance.ClosedConnectionRemoteFailure += OnReplicationRemoteFault;
															_outgoingReplications.Add(currentReplicationInstance);
														}
													},_database.DatabaseShutdown).Unwrap());
                }

                Task.WaitAll(initializationTasks.ToArray(),_database.DatabaseShutdown);
            }
        }

	    private void OnReplicationRemoteFault(OutgoingDocumentReplication instance)
	    {
		    _lastConnectionDisconnect.AddOrUpdate(instance.ReplicationUniqueName, 
				key => DateTime.UtcNow, (key,existing) => DateTime.UtcNow);
		    var timeout = _connectionTimeouts.AddOrUpdate(instance.ReplicationUniqueName, 
								key => 500, (key, existing) => existing * 2 < 60000 ? existing * 2 : existing);
			instance.Disconnect();
#pragma warning disable 4014
			//will resume outgoing replication eventually
			Task.Delay(timeout)
				.ContinueWith(t => instance.InitializeAsync()
										   .WithCancellation(_database.DatabaseShutdown))
				.Unwrap();
#pragma warning restore 4014

	    }

        public void Dispose()
        {
            lock (_outgoingReplications)
            {
                foreach (var replication in _outgoingReplications)
                    replication.Dispose();
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
