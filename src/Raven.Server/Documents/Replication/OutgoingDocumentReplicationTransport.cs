using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Server.Documents;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.ReplicationUtil
{
    public class OutgoingDocumentReplicationTransport : IDisposable
    {
        private readonly string _url;
        private readonly Guid _srcDbId;
        private readonly string _srcDbName;
        private readonly CancellationToken _cancellationToken;
	    private TcpClient _tcpClient;
	    private Stream _tcpStream;
        private bool _disposed;
        private readonly DocumentsOperationContext _context;
        private readonly string _targetDbName;
		private readonly BlittableJsonReaderObject _heartbeatMessage;
	    private readonly ILog _log = LogManager.CurrentLogManager.GetLogger(nameof(OutgoingDocumentReplicationTransport));
		private const int MaxRetries = 3;

		private readonly SemaphoreSlim _disposalSemaphore = new SemaphoreSlim(1);

        public OutgoingDocumentReplicationTransport(string url, 
            Guid srcDbId, 
            string srcDbName,
            string targetDbName,
            CancellationToken cancellationToken, 
            DocumentsOperationContext context)
        {
            _url = url;
            _srcDbId = srcDbId;
            _srcDbName = srcDbName;
            _targetDbName = targetDbName;
            _cancellationToken = cancellationToken;
            _context = context;
			_tcpClient = new TcpClient();
			_heartbeatMessage = _context.ReadObject(new DynamicJsonValue
			{
				[Constants.MessageType] = Constants.Replication.MessageTypes.Heartbeat
			}, null);
			_disposed = false;
        }	   

        public void EnsureConnection()
        {
            if ((_tcpClient == null ) || (_tcpClient != null && !_tcpClient.Connected))
            {	         
				_log.Debug($"Starting connecting client socket. ({_srcDbName})");
                ConnectSocket();                
				_log.Debug($"Finished connecting client socket.({_srcDbName})");
			}
		}

	    public void Disconnect()
	    {
			_log.Debug($"Starting disconnecting socket. ({_srcDbName})");
			try
			{
				_tcpStream.Dispose();
				_tcpClient.Dispose();
			}
		    catch (Exception e)
		    {
				//we are disconnecting, so except logging no action is needed
				_log.WarnException("Error happened while closing socket client",e);
			}
		    finally
			{
				_tcpClient = null;
				_tcpStream = null;
				_log.Debug($"Ended disconnecting socket. ({_srcDbName})");
			}
	    }

	    public void SendHeartbeat()
	    {
		    if (!_disposalSemaphore.Wait(TimeSpan.FromSeconds(0.5), _cancellationToken))
			    return; //we are disposing, so abort doing this
			try
			{
				var writer = new BlittableJsonTextWriter(_context, _tcpStream);
				{
					try
					{
						writer.WriteObjectOrdered(_heartbeatMessage);
					}
					finally
					{
						try
						{
							if (!_cancellationToken.IsCancellationRequested)
							{
								writer.Flush();
								_log.Debug($"Sending heartbeat. ({_srcDbName})");
							}
						}
						catch (Exception e)
						{
							_log.DebugException($"Sending heartbeat failed. ({_srcDbName})", e);
							//TODO : add throttle down/tracking of connection errors, maybe even try to reconnect
						}
					}
				}
			}
			finally
		    {
			    _disposalSemaphore.Release();
		    }
	    }

        public long GetLastEtag()
        {
			if (!_disposalSemaphore.Wait(TimeSpan.FromSeconds(0.5), _cancellationToken))
				return -1; //we are disposing, so abort doing this

			_log.Debug($"Fetching last etag. ({_srcDbName})");

			try
			{
				var sendGetLastEtagFailed = false;
				var writer = new BlittableJsonTextWriter(_context, _tcpStream);
				try
				{
					_context.Write(writer, new DynamicJsonValue
					{
						[Constants.MessageType] = Constants.Replication.MessageTypes.GetLastEtag
					});
				}
				finally
				{
					try
					{
						writer.Flush();
						_log.Debug($"Fetchied last etag successfully. ({_srcDbName})");
					}
					catch (Exception e)
					{
						sendGetLastEtagFailed = true;
						_log.Warn($"Tried to send GetLastEtag message to {_url} -> {_targetDbName}; Failed because of {e}");
					}
				}

				if (sendGetLastEtagFailed)
					return -1;

				try
				{
					var lastEtagMessage = _context.ReadForMemory(_tcpStream, null);
					_log.Debug($"Deserialized last etag. ({_srcDbName})");
					long etag;
					if (!lastEtagMessage.TryGet(Constants.Replication.PropertyNames.LastSentEtag, out etag))
						throw new InvalidDataException(
							$"Received invalid last etag message. Failed to get {Constants.Replication.PropertyNames.LastSentEtag} property from received result");
					return etag;
				}
				catch (EndOfStreamException)
				{
					_log.Warn(
						"Connection closed in the middle of data transmission. This might happen due to legimitate cause, such as remote server receiving shutdown command. Aborting GetLastEtag() operation...");
					return -1;
				}
				catch (Exception e)
				{
					_log.ErrorException($"Failed to fetch last etag (outgoing replication at {_srcDbName}). ", e);
					//TODO: probably need to count failures and maybe drop the connection if too much, so
					//if there is a non-transient error at the destination, trying to get last etag won't loop forever
					return -1;
				}
			}
			finally
	        {
		        _disposalSemaphore.Release();
	        }
        }

		private void ConnectSocket()
		{
			//var uri = new Uri($"{_url?.Replace("http://", "ws://")?.Replace(".fiddler", "")}/databases/{_targetDbName?.Replace("/", string.Empty)}/documentReplication?srcDbId={_srcDbId}&srcDbName={EscapingHelper.EscapeLongDataString(_srcDbName)}");
			//TODO: add code to resolve URL to IPAddress, for now it is loopback
			try
			{
				if(_tcpClient == null)
					_tcpClient = new TcpClient();

				_tcpClient.ConnectAsync(IPAddress.Loopback, 8080).Wait(_cancellationToken);
				_tcpStream = _tcpClient.GetStream();

				using (var writer = new BlittableJsonTextWriter(_context, _tcpStream))
				{
					_context.Write(writer, new DynamicJsonValue
					{
						[Constants.MessageType] = Constants.Replication.MessageTypes.InitialHandshake,
						["srcDbId"] = _srcDbId,
						["srcDbName"] = _srcDbName
					});
				}
			}
			catch (Exception e)
			{				
				//if we failed, then we failed...
				throw new InvalidOperationException("Failed to connect socket for remote replication node.", e);
			}
		}

        public long SendDocumentBatch(IEnumerable<Document> docs)
        {
            long lastEtag;
            EnsureConnection();
			if (!_disposalSemaphore.Wait(TimeSpan.FromSeconds(0.5), _cancellationToken))
				return -1; //we are disposing, so abort doing this

	        _log.Debug($"Starting sending replication batch ({_srcDbName})");
	        try
	        {
		        var writer = new BlittableJsonTextWriter(_context, _tcpStream);
		        try
		        {
			        writer.WriteStartObject();

			        writer.WritePropertyName(_context.GetLazyStringForFieldWithCaching(Constants.MessageType));
			        writer.WriteString(_context.GetLazyStringForFieldWithCaching(
				        Constants.Replication.MessageTypes.ReplicationBatch));

			        writer.WritePropertyName(
				        _context.GetLazyStringForFieldWithCaching(
					        Constants.Replication.PropertyNames.ReplicationBatch));
			        lastEtag = writer.WriteDocuments(_context, docs, false);
			        writer.WriteEndObject();
		        }
		        finally
		        {
			        writer.Flush();
			        _log.Debug($"Finished sending replication batch ({_srcDbName})");
		        }


		        try
		        {
			        _log.Debug($"Starting receiving replication batch ack ({_srcDbName})");
			        var acknowledgeMessage = _context.ReadForMemory(_tcpStream, null);
			        _log.Debug($"Finished receiving replication batch ack ({_srcDbName})");
			        string val = null;
			        bool hasSucceededWithBatch;
			        if (acknowledgeMessage == null ||
			            (acknowledgeMessage.TryGet(Constants.MessageType, out val) &&
			             !val.Equals(Constants.Replication.MessageTypes.ReplicationBatchAcknowledge)) ||
			            !acknowledgeMessage.TryGet(Constants.HadSuccess, out hasSucceededWithBatch))
			        {
				        var errorMsg =
					        $"Received replication batch acknowledgement message with the wrong type. Expected : {Constants.Replication.MessageTypes.ReplicationBatchAcknowledge}, Received : {val}";
				        _log.Error(errorMsg);
				        throw new InvalidOperationException(errorMsg);
			        }

			        if (!hasSucceededWithBatch)
			        {
				        _log.Debug($"Replication batch ack returned false! Something happened on the other end... ({_srcDbName})");
				        return -1;
			        }
		        }
		        catch (EndOfStreamException)
		        {
			        _log.Warn(
				        "Remote server closed the connection in the middle of sending documents batch for replication. There may be a legitimate reason for this, such as remote server receiving shutdown command. Aborting sending replication batch documents...");
			        return -1;
		        }
		        catch (Exception e)
		        {
			        var errorMsg =
				        $"Failed to get acknowledgement for replication batch. Last sent etag was not updated. Exception received : {e}";
			        _log.Error(errorMsg);
			        return -1;
		        }
	        }
	        catch (Exception e)
	        {
		        _log.Error($"Failed to sent replication batch; reason : {e}");
		        throw;
	        }
	        finally
	        {
		        _disposalSemaphore.Release();

	        }
	        return lastEtag;
        }	    

        public void Dispose()
        {	        
            _disposed = true;
			Disconnect();
		}				   
    }
}
