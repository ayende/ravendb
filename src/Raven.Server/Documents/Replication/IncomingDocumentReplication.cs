using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Replication
{
    public class IncomingDocumentReplication : IDisposable
    {
		private DateTime _lastHeartbeatReceive = DateTime.MinValue;
		private Timer _heartbeatTimer;
		private readonly TimeSpan HeartbeatTimeout = Debugger.IsAttached ? TimeSpan.FromHours(1) : TimeSpan.FromSeconds(30);

		private readonly DocumentDatabase _database;
	    private readonly IncomingDocumentReplicationTransport _transport;
	    private readonly DocumentsOperationContext _context;
		private readonly ILog _log = LogManager.CurrentLogManager.GetLogger(nameof(IncomingDocumentReplication));
		private readonly CancellationTokenSource _cts = new CancellationTokenSource();
	    private readonly Thread _incomingThread;

		private readonly object _disposalSyncObj = new object();
	    private bool _isDisposed;

		public IncomingDocumentReplication(DocumentDatabase database, TcpClient tcpClient)
		{
			_isDisposed = false;
			_database = database;
	        _database.DocumentsStorage.ContextPool.AllocateOperationContext(out _context);
			_transport = new IncomingDocumentReplicationTransport(_database, tcpClient, _context);

	        string reasonForFailure;
	        if (!_transport.TryReceiveInitialHandshake(out reasonForFailure))
		        throw new InvalidOperationException(reasonForFailure);

	        _incomingThread = new Thread(ReceiveDocumentsLoop)
	        {
				IsBackground = true,
				Name = $"Incoming Document Replication {_transport.SrcDbName}/{_transport.SrcDbId}"
	        };
        }

	    public void Start() => _incomingThread.Start();

	    public event Action<IncomingDocumentReplication> ReceiveError;

	    private void ReceiveDocumentsLoop()
	    {
		    while (!_cts.Token.IsCancellationRequested)
		    {
			    try
			    {
				    BlittableJsonReaderObject message;
				    if (_transport.TryReceiveMessage(out message))
					    HandleIncomingMessage(message);
			    }
			    catch (Exception e) 
			    {
					//if we get exception at this point, this means unrecoverable error,
					//so close receiving thread and notify the upper layer
					_log.ErrorException("Received unrecoverable error on message receive thread. Closing the message receive thread..",e);
					OnReceiveError(this); //let the upper layer do disposal and stuff
				    break;
			    }

			    Thread.Sleep(100);
		    }
	    }

		private void HandleIncomingMessage(BlittableJsonReaderObject message)
		{
			if (!Monitor.TryEnter(_disposalSyncObj))
				return; //we are disposing, so abort doing this

			ThrowIfDisposed();

			try
			{
				string messageTypeAsString;

				//TODO : refactor this to more efficient method,
				//since we do not care about contents of the property at this stage,
				//but only about it's existence
				if (!message.TryGet(Constants.MessageType, out messageTypeAsString))
					throw new InvalidDataException(
						$"Got tcp socket message without a type. Expected property with name {Constants.MessageType}, but found none. ");
#if DEBUG
				DebugHelper.ThrowExceptionForDocumentReplicationReceiveIfRelevant();
#endif
				switch (messageTypeAsString)
				{
					case Constants.Replication.MessageTypes.Heartbeat:
						_lastHeartbeatReceive = DateTime.UtcNow; //not sure that this variable is actually needed
						_heartbeatTimer.Change(TimeSpan.Zero, HeartbeatTimeout);
						break;
					case Constants.Replication.MessageTypes.GetLastEtag:
						try
						{
							//read transaction to get last received etag from DB 
							using (_context.OpenReadTransaction())
								_transport.SendLastEtagResponse();
						}
						catch (Exception e)
						{
							_log.Error("Failed to write last etag response; Reason: " + e);
							throw; //re-throwing here will cause closing the existing connection and end current listening thread
						}
						break;
					case Constants.Replication.MessageTypes.ReplicationBatch:
						BlittableJsonReaderArray replicatedDocs;
						if (!message.TryGet(Constants.Replication.PropertyNames.ReplicationBatch, out replicatedDocs))
							throw new InvalidDataException(
								$"Expected the message to have a field with replicated document array, named {Constants.Replication.PropertyNames.ReplicationBatch}. The property wasn't found");
						try
						{
							using (_context.OpenWriteTransaction())
							{
								ReceiveDocuments(_context, replicatedDocs);
								_context.Transaction.Commit();
							}
						}
						catch (Exception e)
						{
							_log.Error($"Received replication batch with {replicatedDocs.Length} documents, but failed to write it locally. Closing the connection from this end.. Reason for this: {e}");
							_transport.WriteReplicationBatchAcknowledge(false);
						}
						_transport.WriteReplicationBatchAcknowledge(true);

						break;
					default:
						throw new NotSupportedException($"Received not supported message type : {messageTypeAsString}");
				}
			}
			finally
			{
				Monitor.Exit(_disposalSyncObj);
			}
		}

	    private void ThrowIfDisposed()
	    {
			if (_isDisposed)
				throw new ObjectDisposedException("Cannot use IncomingDocumentReplication after being disposed..");
		}

		//by design this method won't handle opening and commit of the transaction
		//(that should happen at the calling code)
		private void ReceiveDocuments(DocumentsOperationContext context, BlittableJsonReaderArray docs)
        {
            var dbChangeVector = _database.DocumentsStorage.GetDatabaseChangeVector(context);
            var changeVectorUpdated = false;
            var maxReceivedChangeVectorByDatabase = new Dictionary<Guid, long>();
            foreach (BlittableJsonReaderObject doc in docs)
            {
                var changeVector = doc.EnumerateChangeVector();
                foreach (var currentEntry in changeVector)
                {
                    Debug.Assert(currentEntry.DbId != Guid.Empty); //should never happen, but..

					//note: documents in a replication batch are ordered in incremental etag order
                    maxReceivedChangeVectorByDatabase[currentEntry.DbId] = currentEntry.Etag;
                }

	            const string DetachObjectDebugTag = "IncomingDocumentReplication -> Detach object from parent array";
	            var detachedDoc = context.ReadObject(doc, DetachObjectDebugTag);
				WriteReceivedDocument(context, detachedDoc);
            }

			//if any of [dbId -> etag] is larger than server pair, update it
            for (int i = 0; i < dbChangeVector.Length; i++)
            {
                long dbEtag;
                if (maxReceivedChangeVectorByDatabase.TryGetValue(dbChangeVector[i].DbId, out dbEtag) == false)
                    continue;
                maxReceivedChangeVectorByDatabase.Remove(dbChangeVector[i].DbId);
                if (dbEtag > dbChangeVector[i].Etag)
                {
                    changeVectorUpdated = true;
                    dbChangeVector[i].Etag = dbEtag;
                }
            }

            if (maxReceivedChangeVectorByDatabase.Count > 0)
            {
                changeVectorUpdated = true;
                var oldSize = dbChangeVector.Length;
                Array.Resize(ref dbChangeVector,oldSize + maxReceivedChangeVectorByDatabase.Count);

                foreach (var kvp in maxReceivedChangeVectorByDatabase)
                {
                    dbChangeVector[oldSize++] = new ChangeVectorEntry
                    {
                        DbId = kvp.Key,
                        Etag = kvp.Value,
                    };
                }
            }

            if (changeVectorUpdated)
                _database.DocumentsStorage.SetChangeVector(context, dbChangeVector);			
        }

        private void WriteReceivedDocument(DocumentsOperationContext context, BlittableJsonReaderObject doc)
        {
			
            var id = doc.GetIdFromMetadata();
            if (id == null)
                throw new InvalidDataException($"Missing {Constants.DocumentIdFieldName} field from a document; this is not something that should happen...");

            // we need to split this document to an independent blittable document
            // and this time, we'll prepare it for disk.
            doc.PrepareForStorage();
            _database.DocumentsStorage.Put(context, id, null, doc);
        }

	    public void Dispose()
	    {			
			lock (_disposalSyncObj)
			{
				_isDisposed = true;
				_cts.Cancel();
				_incomingThread.Join(1000);	//wait for reasonable time for thread to gracefully end
			    _transport.Dispose();
			    _context.Dispose();
		    }
	    }

	    protected void OnReceiveError(IncomingDocumentReplication instance) => ReceiveError?.Invoke(instance);
    }
}
