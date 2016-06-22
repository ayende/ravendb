using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.WebSockets;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Server.Documents.Replication;
using Raven.Server.Extensions;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers
{
    public class DocumentReplicationRequestHandler : DatabaseRequestHandler
    {
		//since we are handling heartbeats ourselves... handle the heartbeats
	    private DateTime _lastHeartbeatReceive = DateTime.MinValue;
		private Timer _heartbeatTimer;

	    private readonly TimeSpan HeartbeatTimeout = Debugger.IsAttached ? TimeSpan.FromHours(1) : TimeSpan.FromSeconds(30);

	    private CancellationTokenSource _cts;

        //an endpoint to establish replication websocket
        [RavenAction("/databases/*/documentReplication", "GET",
			@"@/databases/{databaseName:string}/documentReplication?
                srcDbId={databaseUniqueId:string}
                &srcDbName={databaseName:string}")]
        public async Task DocumentReplicationConnection()
        {
			_cts = CancellationTokenSource.CreateLinkedTokenSource(Database.DatabaseShutdown);
	        _heartbeatTimer = new Timer(_ => _cts.Cancel(), null, HeartbeatTimeout, HeartbeatTimeout);
			var srcDbId = Guid.Parse(GetQueryStringValueAndAssertIfSingleAndNotEmpty("srcDbId"));
            var srcDbName = GetQueryStringValueAndAssertIfSingleAndNotEmpty("srcDbName");
            var srcUrl = HttpContext.Request.GetHostnameUrl();

            var ReplicationReceiveDebugTag = $"document-replication/receive <{Database.DocumentReplicationLoader.ReplicationUniqueName}>";
            var incomingReplication = new IncomingDocumentReplication(Database);
            using (var webSocket = await HttpContext.WebSockets.AcceptWebSocketAsync())
            using (var webSocketStream = new WebsocketStream(webSocket, _cts.Token))
            {            
				DocumentsOperationContext context;
                using (ContextPool.AllocateOperationContext(out context))
                {
                    var buffer = new ArraySegment<byte>(context.GetManagedBuffer());
                    var jsonParserState = new JsonParserState();
                    using (var parser = new UnmanagedJsonParser(context, jsonParserState, ReplicationReceiveDebugTag))
                    {					
						while (!_cts.IsCancellationRequested)
                        {
                            //this loop handles one replication batch
	                        try
	                        {
		                        var result = await webSocket.ReceiveAsync(buffer, _cts.Token);
		                        if (result.CloseStatus != null)
			                        break;

		                        //open write transaction at beginning of the batch
		                        using (var writer = new BlittableJsonDocumentBuilder(context,
			                        BlittableJsonDocumentBuilder.UsageMode.None, ReplicationReceiveDebugTag,
			                        parser, jsonParserState))
		                        {
			                        writer.ReadObject();
			                        parser.SetBuffer(buffer.Array, result.Count);
			                        while (writer.Read() == false)
			                        {
				                        result = await webSocket.ReceiveAsync(buffer, _cts.Token);
				                        parser.SetBuffer(buffer.Array, result.Count);										
									}
			                        writer.FinalizeDocument();
			                        var message = writer.CreateReader();
								
									string messageTypeAsString;

			                        //TODO : refactor this to more efficient method,
			                        //since we do not care about contents of the property at this stage,
			                        //but only about it's existence
			                        if (!message.TryGet(Constants.MessageType, out messageTypeAsString))
				                        throw new InvalidDataException(
					                        $"Got websocket message without a type. Expected property with name {Constants.MessageType}, but found none.");

			                        await HandleMessage(
				                        messageTypeAsString,
				                        message,
				                        webSocketStream,
				                        context,
				                        srcDbId,
				                        incomingReplication);
		                        }
	                        }
	                        catch (Exception e)
	                        {
								await webSocket.CloseAsync(WebSocketCloseStatus.InternalServerError,
									"Exception was thrown, cannot continue replication. Check logs for details", Database.DatabaseShutdown);
		                        var msg =
			                        $@"Failed to receive replication document batch. (Origin -> Database Id = {srcDbId}, Database Name = {srcDbName}, Origin URL = {srcUrl})";
								Log.ErrorException(msg,e);
								throw new InvalidOperationException(msg,e);

							}
                        }
                    }
                }
            }
        }

        private async Task HandleMessage(string messageTypeAsString, 
            BlittableJsonReaderObject message, 
            WebsocketStream webSocketStream, 
            DocumentsOperationContext context, 
            Guid srcDbId, 
            IncomingDocumentReplication incomingReplication)
        {
            switch (messageTypeAsString)
            {
				case Constants.Replication.MessageTypes.Heartbeat:
					_lastHeartbeatReceive = DateTime.UtcNow; //not sure that this variable is actually needed
		            _heartbeatTimer.Change(TimeSpan.Zero, HeartbeatTimeout);
					break;
                case Constants.Replication.MessageTypes.GetLastEtag:
		            try
		            {
			            using (context.OpenReadTransaction())
				            await WriteLastEtagResponse(webSocketStream, context, srcDbId);
		            }
		            catch (Exception e)
		            {
			            Log.Error("Failed to write last etag response; Reason: " + e);
		            }
		            break;
                case Constants.Replication.MessageTypes.ReplicationBatch:
                    BlittableJsonReaderArray replicatedDocs;
                    if (!message.TryGet(Constants.Replication.PropertyNames.ReplicationBatch, out replicatedDocs))
                        throw new InvalidDataException(
                            $"Expected the message to have a field with replicated document array, named {Constants.Replication.PropertyNames.ReplicationBatch}. The property wasn't found");		           

		            try
					{										
						using (context.OpenWriteTransaction())
			            {
				            incomingReplication.ReceiveDocuments(context, replicatedDocs);
				            context.Transaction.Commit();
			            }
		            }
		            catch (Exception e)
		            {
						Log.Error($"Received replication batch with {replicatedDocs.Length} documents, but failed to write it locally. Reason : {e}");
						await WriteReplicationBatchAcknowledge(webSocketStream, context, false);
						return; //do not rethrow - maybe failing to write the batch is a transient error;
						//write negative ack to the other side, so the current batch will be resent
						//TODO: maybe maximum retries should be added here?
		            }
		            await WriteReplicationBatchAcknowledge(webSocketStream,context, true);
                    break;
                default:
                    throw new NotSupportedException($"Received not supported message type : {messageTypeAsString}");
            }
        }

        //NOTE : assumes at least read transaction open in the context
        private long GetLastReceivedEtag(Guid srcDbId, DocumentsOperationContext context)
        {
            var dbChangeVector = Database.DocumentsStorage.GetDatabaseChangeVector(context);
            var vectorEntry = dbChangeVector.FirstOrDefault(x => x.DbId == srcDbId);
            return vectorEntry.Etag;
        }

	    private async Task WriteReplicationBatchAcknowledge(WebsocketStream webSocketStream, 
			DocumentsOperationContext context,
			bool hasSucceeded)
	    {
		    try
		    {
			    var responseWriter = new BlittableJsonTextWriter(context, webSocketStream);
			    try
			    {
				    context.Write(responseWriter, new DynamicJsonValue
				    {
					    [Constants.MessageType] = Constants.Replication.MessageTypes.ReplicationBatchAcknowledge,
					    [Constants.HadSuccess] = hasSucceeded
				    });
			    }
			    finally
			    {
					//WebSocketStream does not support synchronous writes to the stream
				    await responseWriter.DisposeAsync();
			    }
		    }
		    catch (Exception e)
		    {
				Log.Error("Failed to send back the replication batch acknowledge message. Reason: " + e);
			    throw;
		    }
	    }

		private async Task WriteLastEtagResponse(WebsocketStream webSocketStream, DocumentsOperationContext context, Guid srcDbId)
		{
			var responseWriter = new BlittableJsonTextWriter(context, webSocketStream);
			try
			{
				context.Write(responseWriter, new DynamicJsonValue
				{
					[Constants.Replication.PropertyNames.LastSentEtag] = GetLastReceivedEtag(srcDbId, context),
					[Constants.MessageType] = Constants.Replication.MessageTypes.GetLastEtag
				});
			}
			finally
			{
				await responseWriter.DisposeAsync();
			}
        }
    }
}
