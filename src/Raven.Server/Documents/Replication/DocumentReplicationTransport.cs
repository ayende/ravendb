﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;
using Raven.Client.Platform.Unix;
using Raven.Server.Documents;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

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

        public async Task<long> GetLastEtag()
        {
            using (var writer = new BlittableJsonTextWriter(_context, _websocketStream))
            {
                _context.Write(writer, new DynamicJsonValue
                {
                    ["Raven/GetLastEtag"] = true
                });

                writer.Flush();
            }

            var lastEtagMessage = await _context.ReadForMemoryAsync(_websocketStream, null);

            long etag;
            if (!lastEtagMessage.TryGet("Raven/LastSentEtag", out etag))
                throw new InvalidDataException(
                    "Received invalid last etag message. Failed to get Raven/LastSentEtag property from received result");
            return etag;
        }

        private async Task<WebSocket> GetAndConnectWebSocketAsync()
        {
            var uri = new Uri(
                $@"{_url?.Replace("http://", "ws://")
                    ?.Replace(".fiddler", "")}/databases/{_targetDbName?.Replace("/", string.Empty)}/documentReplication?srcDbId={_srcDbId}&srcDbName={EscapingHelper
                        .EscapeLongDataString(_srcDbName)}");
            try
            {
                if (Sparrow.Platform.Platform.RunningOnPosix)
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
                throw new InvalidOperationException("Failed to connect websocket for remote replication node.", e);
            }
        }

        public async Task SendDocumentBatchAsync(Document[] docs)
        {
            await EnsureConnectionAsync();
            using (var writer = new BlittableJsonTextWriter(_context, _websocketStream))
            {
                for (int i = 0; i < docs.Length; i++)
                {
                    docs[i].EnsureMetadata();
                    _context.Write(writer, docs[i].Data);
                    writer.Flush();
                }
            }
            await _websocketStream.WriteEndOfMessageAsync();
        }		

        public async Task SendHeartbeatAsync(DocumentsOperationContext context)
        {
            await EnsureConnectionAsync();
            //TODO : finish heartbeat
        }

        public void Dispose()
        {
            _disposed = true;
            _context.Dispose();
            _webSocket.Dispose();
        }				   
    }
}
