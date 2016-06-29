using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Client.Platform.Unix;
using Raven.Server.Documents;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow;

namespace Raven.Server.ReplicationUtil
{
    public class DocumentReplicationTransport : IDisposable
    {
        private readonly string _url;
        private readonly Guid _srcDbId;
        private readonly string _srcDbName;
        private readonly CancellationToken _cancellationToken;
        private WebSocket _webSocket;
        private bool _disposed;
        private WebsocketStream _websocketStream;
        private readonly DocumentsOperationContext _context;
        private readonly string _targetDbName;
		private readonly BlittableJsonReaderObject _heartbeatMessage;
	    private readonly ILog _log = LogManager.GetLogger(typeof(DocumentReplicationTransport));
		private const int MaxRetries = 3;

        public DocumentReplicationTransport(string url, 
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
			_heartbeatMessage = _context.ReadObject(new DynamicJsonValue
			{
				[Constants.MessageType] = Constants.Replication.MessageTypes.Heartbeat
			}, null);
			_disposed = false;
        }	   

        public async Task EnsureConnectionAsync()
        {
            if (_webSocket == null || _webSocket.State != WebSocketState.Open)
            {
                _webSocket = await GetAndConnectWebSocketAsync();
                _websocketStream = new WebsocketStream(_webSocket, _cancellationToken);
            }
        }

	    public async Task Disconnect(WebSocketCloseStatus closeStatus = WebSocketCloseStatus.Empty, string closeDescription = "")
	    {
		    try
		    {
			    await _webSocket.CloseAsync(closeStatus, closeDescription, _cancellationToken);
		    }
		    catch (Exception e)
		    {
				//Debugger.Launch();
			}
		    finally
		    {
			    _webSocket.Dispose();
			    _webSocket = null;
		    }
	    }

	    public async Task SendHeartbeatAsync()
	    {			
		    var writer = new BlittableJsonTextWriter(_context, _websocketStream);
		    try
		    {
			    writer.WriteObjectOrdered(_heartbeatMessage);
		    }
		    finally
		    {
			    try
			    {
				    await writer.FlushAsync();
			    }
				catch (Exception e)
				{
					//Debugger.Launch();
					//TODO : add throttle down/tracking of connection errors, maybe even try to reconnect
				}
			}
		}

        public async Task<long> GetLastEtag()
        {
			var sendGetLastEtagFailed = false;
			var writer = new BlittableJsonTextWriter(_context, _websocketStream);
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
			        await writer.FlushAsync();
		        }
		        catch (Exception e)
		        {
					//Debugger.Launch();
					sendGetLastEtagFailed = true;
					_log.Warn($"Tried to send GetLastEtag message to {_url} -> {_targetDbName}; Failed because of {e}");

				}
	        }

			if (sendGetLastEtagFailed)
				return -1;

	        try
	        {
		        var lastEtagMessage = await _context.ReadForMemoryAsync(_websocketStream, null);

		        long etag;
		        if (!lastEtagMessage.TryGet(Constants.Replication.PropertyNames.LastSentEtag, out etag))
			        throw new InvalidDataException(
				        $"Received invalid last etag message. Failed to get {Constants.Replication.PropertyNames.LastSentEtag} property from received result");
		        return etag;
	        }
	        catch (EndOfStreamException)
	        {
				//Debugger.Launch();
				_log.Warn(
			        "Connection closed in the middle of data transmission. This might happen due to legimitate cause, such as remote server receiving shutdown command. Aborting GetLastEtag() operation...");
		        return -1;
	        }
	        catch (Exception e)
	        {
				//Debugger.Launch();
				return -1;
	        }
        }

		private async Task<WebSocket> GetAndConnectWebSocketAsync()
		{
			var uri = new Uri($"{_url?.Replace("http://", "ws://")?.Replace(".fiddler", "")}/databases/{_targetDbName?.Replace("/", string.Empty)}/documentReplication?srcDbId={_srcDbId}&srcDbName={EscapingHelper.EscapeLongDataString(_srcDbName)}");
			try
			{
                if (Platform.RunningOnPosix)
				{
					var webSocketUnix = new RavenUnixClientWebSocket();					
					await webSocketUnix.ConnectAsync(uri, _cancellationToken);
					return webSocketUnix;
				}

				var webSocket = new ClientWebSocket();
				await webSocket.ConnectAsync(uri, _cancellationToken);
				return webSocket;
			}
			catch (Exception e)
			{
				//Debugger.Launch();
				//if we failed, then we failed...
				throw new InvalidOperationException("Failed to connect websocket for remote replication node.", e);
			}
		}

        public async Task<long> SendDocumentBatchAsync(IEnumerable<Document> docs)
        {
            long lastEtag;
            await EnsureConnectionAsync();

	        try
	        {
		        var writer = new BlittableJsonTextWriter(_context, _websocketStream);
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
			        await writer.FlushAsync();
		        }

		        try
		        {
			        var acknowledgeMessage = await _context.ReadForMemoryAsync(_websocketStream, null);
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
				        return -1;
		        }
		        catch (EndOfStreamException)
		        {
					//Debugger.Launch();
					_log.Warn("Remote server closed the connection in the middle of sending documents batch for replication. There may be a legitimate reason for this, such as remote server receiving shutdown command. Aborting sending replication batch documents...");
			        return -1;
		        }
		        catch (Exception e)
		        {
					//Debugger.Launch();
					var errorMsg = $"Failed to get acknowledgement for replication batch. Last sent etag was not updated. Exception received : {e}";
			        _log.Error(errorMsg);
			        return -1;
		        }
	        }
	        catch (Exception e)
	        {
				//Debugger.Launch();
				_log.Error($"Failed to sent replication batch; reason : {e}");
		        throw;
	        }
	        return lastEtag;
        }	    

        public void Dispose()
        {	        
            _disposed = true;
			Disconnect().Wait(_cancellationToken);
		}				   
    }
}
