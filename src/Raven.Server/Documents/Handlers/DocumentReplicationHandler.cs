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

		[RavenAction("/replication/topology", "GET")]
		[RavenAction("/databases/*/replication/topology", "GET")]
		public Task GetReplicationTopology()
		{
			HttpContext.Response.StatusCode = 404;
			return Task.CompletedTask;
		}

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

		    var ReplicationReceiveDebugTag =
			    $"document-replication/receive <{Database.DocumentReplicationLoader.ReplicationUniqueName}>";
		    var incomingReplication = new IncomingDocumentReplication(Database);
		    try
		    {
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
								    {
									    Log.Info(
										    $"Tried to receive data from websocket, but it was closed. The CloseStatus == {result.CloseStatus.Value}; Status Message is '{result.CloseStatusDescription}'");
									    break;
								    }

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
									    using (var message = writer.CreateReader())
									    {
										    string messageTypeAsString;

										    //TODO : refactor this to more efficient method,
										    //since we do not care about contents of the property at this stage,
										    //but only about it's existence
										    if (!message.TryGet(Constants.MessageType, out messageTypeAsString))
											    throw new InvalidDataException(
												    $"Got websocket message without a type. Expected property with name {Constants.MessageType}, but found none. ");
#if DEBUG
										    DebugHelper.ThrowExceptionForDocumentReplicationReceiveIfRelevant();
#endif
										    await HandleMessage(
											    messageTypeAsString,
											    message,
											    webSocketStream,
											    context,
											    srcDbId,
											    incomingReplication);

										    if (webSocket.State != WebSocketState.Open)
										    {
											    Log.Warn(
												    "Websocket closed after handling the message. Closing current listening thread; if this is a transient issue, other end will reconnect and replication will resume.");
											    break;
										    }
									    }
								    }
							    }
							    catch (EndOfStreamException e)
							    {
								    //connection closed in the middle of transmission of data,
								    //so log the error and exit
								    Log.ErrorException("Connection closed in the middle of transmission of data", e);
								    await AttemptWebsocketClose(webSocket);
								    break;
							    }
							    catch (Exception e)
							    {
								    var msg =
									    $@"Failed to receive replication document batch. Closing the connection with 'InternalServerError' status. (Origin -> Database Id = {srcDbId}, Database Name = {srcDbName}, Origin URL = {srcUrl})";
								    Log.ErrorException(msg, e);
								    await AttemptWebsocketClose(webSocket);
									break;
							    }
						    }
					    }
				    }
			    }
		    }
		    catch (Exception e)
		    {
			    Log.Debug($"Exception thrown on disposal of incoming replication thread. This is not necessarily an issue,but still needs to be investigated. The exception: {e}");
		    }
	    }

	    private async Task AttemptWebsocketClose(WebSocket webSocket)
	    {
		    try
		    {
				//closing from both sides is not handled correctly in the current version of .net core
				//in the next RC it should be fixed,then this line should be uncomitted
			    //await webSocket.CloseAsync(WebSocketCloseStatus.InternalServerError, "Exception was thrown, cannot continue replication. Check logs for details",Database.DatabaseShutdown);
		    }
		    catch(Exception e)
		    {
			    Log.Warn($"Failed to gracefully close the websocket; This might not be an issue, since the other end may have closed the connection first. Exception thrown {e}");
			    //since we are already in error state, and just want to attempt to clean-up,
			    //it doesn't really matter if we fail sending close message.
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
						//read transaction to get last received etag from DB 
			            using (context.OpenReadTransaction())
				            await SendLastEtagResponse(webSocketStream, context, srcDbId);
		            }
		            catch (Exception e)
		            {
			            Log.Error("Failed to write last etag response; Reason: " + e);
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
						using (context.OpenWriteTransaction())
			            {
				            incomingReplication.ReceiveDocuments(context, replicatedDocs);
				            context.Transaction.Commit();
			            }
		            }
		            catch (Exception e)
		            {
						Log.Error($"Received replication batch with {replicatedDocs.Length} documents, but failed to write it locally. Closing the connection from this end.. Reason for this: {e}");
						await WriteReplicationBatchAcknowledge(webSocketStream, context, false);

						//we cannot recover from such an error, so other side needs to reconnect here;
			            await AttemptWebsocketClose(webSocketStream.UnderlyingWebsocket);
			            return;
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
				    await responseWriter.FlushAsync();
			    }
		    }
		    catch (Exception e)
		    {
				Log.Error("Failed to send back the replication batch acknowledge message. Reason: " + e);
			    throw;
		    }
	    }

		private async Task SendLastEtagResponse(WebsocketStream webSocketStream, DocumentsOperationContext context, Guid srcDbId)
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
				await responseWriter.FlushAsync();
			}
        }
    }
}
