using System;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Threading;
using Raven.Abstractions.Logging;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Constants = Raven.Abstractions.Data.Constants;

namespace Raven.Server.Documents.Replication
{
    public class IncomingDocumentReplicationTransport : IDisposable
    {
		//TODO: make this configurable, and those values would be the default
		private readonly ILog _log = LogManager.CurrentLogManager.GetLogger(nameof(IncomingDocumentReplicationTransport));

		private readonly DocumentDatabase _database;
	    private readonly DocumentsOperationContext _context;
	    private readonly NetworkStream _tcpStream;
		private readonly BlittableJsonReaderObject _heartbeatMessage;

		private Guid _srcDbId;
		private string _srcDbName;
		private readonly object _disposalSyncObj = new object();

	    private bool _isDisposed;

		public IncomingDocumentReplicationTransport(
			DocumentDatabase database,
			TcpClient tcpClient,
			DocumentsOperationContext context)
		{
			_isDisposed = false;
		    _database = database;
		    _context = context;
		    _tcpStream = tcpClient.GetStream();
			_heartbeatMessage = _context.ReadObject(new DynamicJsonValue
			{
				[Constants.MessageType] = Constants.Replication.MessageTypes.Heartbeat
			}, null);
		}

	    public Guid SrcDbId => _srcDbId;
	    public string SrcDbName => _srcDbName;

	    public bool TryReceiveInitialHandshake(out string reason)
	    {
			ThrowIfDisposed();

			reason = string.Empty;
			var handshakeMessage = _context.ReadForMemory(_tcpStream, null);
		    string messageType;
		    if (!handshakeMessage.TryGet(Constants.MessageType, out messageType) ||
				 !messageType.Equals(Constants.Replication.MessageTypes.InitialHandshake,StringComparison.OrdinalIgnoreCase))
		    {
			    reason = "Handshake message does not contain message type property. Something is really wrong here...";
			    return false;
		    }

		    string srcDbId;		    
		    if (!handshakeMessage.TryGet("srcDbId", out srcDbId) &&
				!Guid.TryParse(srcDbId, out _srcDbId))
		    {
			    reason = $"Handshake message is missing srcDbId property or it's value is invalid. Could not parse Guid from it (received {srcDbId}).  This should not happen and it likely a bug.";
			    return false;
		    }

		    if (!handshakeMessage.TryGet("srcDbName", out _srcDbName))
		    {
				reason = $"Handshake message is missing srcDbName. This should not happen and it likely a bug.";
				return false;
		    }
		    return true;
	    }

		private const string ReadIncomingReplicationBatchDebugTag = "document-replication/read-incoming-batch";
		public bool TryReceiveMessage(out BlittableJsonReaderObject message)
		{
			message = null;
			if (!Monitor.TryEnter(_disposalSyncObj))
				return false; //we are disposing, so abort doing this

			ThrowIfDisposed();

			try
			{
				message = _context.ReadForMemory(_tcpStream, ReadIncomingReplicationBatchDebugTag);
				return true;
			}
			catch (IOException e)
			{
				_log.ErrorException("Failed to receive incoming replication message. There an issue with the connection. ", e);
				throw;
			}
			catch (Exception e)
			{
				_log.Error("Failed to receive incoming replication message. Reason: " + e);
			}
			finally
			{
				Monitor.Exit(_disposalSyncObj);
			}
			return false;
	    }

	    public void WriteReplicationBatchAcknowledge(bool hasSucceeded)
		{
			if (!Monitor.TryEnter(_disposalSyncObj))
				return; //we are disposing, so abort doing this

			ThrowIfDisposed();
			try
			{
				using (var responseWriter = new BlittableJsonTextWriter(_context, _tcpStream))
				{
					_context.Write(responseWriter, new DynamicJsonValue
					{
						[Constants.MessageType] = Constants.Replication.MessageTypes.ReplicationBatchAcknowledge,
						[Constants.HadSuccess] = hasSucceeded
					});
				}
			}
			catch (Exception e)
			{
				_log.Error("Failed to send back the replication batch acknowledge message. Reason: " + e);
				throw;
			}
			finally
			{
				Monitor.Exit(_disposalSyncObj);
			}
		}

		//NOTE : assumes at least read transaction open in the context
		private long GetLastReceivedEtag(Guid srcDbId, DocumentsOperationContext context)
		{
			if (!Monitor.TryEnter(_disposalSyncObj))
				return -1; //we are disposing, so abort doing this

			ThrowIfDisposed();

			try
			{
				var dbChangeVector = _database.DocumentsStorage.GetDatabaseChangeVector(context);
				var vectorEntry = dbChangeVector.FirstOrDefault(x => x.DbId == srcDbId);
				return vectorEntry.Etag;
			}
			finally
			{
				Monitor.Exit(_disposalSyncObj);
			}
		}

		public void SendHeartbeat()
		{
			if (!Monitor.TryEnter(_disposalSyncObj))
				return; //we are disposing, so abort doing this
			ThrowIfDisposed();

			try
			{
				using (var writer = new BlittableJsonTextWriter(_context, _tcpStream))
					writer.WriteObjectOrdered(_heartbeatMessage);
				_log.Debug($"Sent heartbeat to {_srcDbName}");
			}
			catch (Exception e)
			{
				_log.DebugException($"Sending heartbeat failed. ({_srcDbName})", e);
				//TODO : add throttle down/tracking of connection errors, maybe even try to reconnect
			}
			finally
			{
				Monitor.Exit(_disposalSyncObj);
			}
		}

	    public void SendLastEtagResponse()
		{
			if (!Monitor.TryEnter(_disposalSyncObj))
				return; //we are disposing, so abort doing this

			ThrowIfDisposed();
			try
			{
				using (var writer = new BlittableJsonTextWriter(_context, _tcpStream))
				{
					_context.Write(writer, new DynamicJsonValue
					{
						[Constants.Replication.PropertyNames.LastSentEtag] = GetLastReceivedEtag(_srcDbId, _context),
						[Constants.MessageType] = Constants.Replication.MessageTypes.GetLastEtag
					});
				}
				_log.Debug($"Sent last etag (response) to {_srcDbName}");
			}
			catch (Exception e)
			{
				_log.DebugException($"Sending last etag (response) failed. ({_srcDbName})", e);
				//TODO : add throttle down/tracking of connection errors, maybe even try to reconnect
			}
			finally
			{
				Monitor.Exit(_disposalSyncObj);
			}
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		private void ThrowIfDisposed()
		{
			if (_isDisposed)
				throw new ObjectDisposedException("Cannot use IncomingDocumentReplicationTransport after being disposed..");
		}

		public void Dispose()
	    {
		    lock (_disposalSyncObj)
		    {
			    _tcpStream.Dispose();
			    _isDisposed = true;
		    }
	    }
    }
}
