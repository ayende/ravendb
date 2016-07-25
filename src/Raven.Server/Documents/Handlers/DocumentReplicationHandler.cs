﻿using System;
using System.Net;
using System.Threading.Tasks;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using System.Linq;

namespace Raven.Server.Documents.Handlers
{
    public class DocumentReplicationtHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/replication/topology", "GET")]
        public Task GetReplicationTopology()
        {
            HttpContext.Response.StatusCode = 404;
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/replication/active-connections", "GET")]
        public Task GetReplicationActiveConnections()
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var incoming = new DynamicJsonArray();
                foreach (var item in Database.DocumentReplicationLoader.IncomingConnections)
                {
                    incoming.Add(new DynamicJsonValue
                    {
                        ["SourceDatabaseId"] = item.ConnectionInfo.SourceDatabaseId,
                        ["SourceDatabaseName"] = item.ConnectionInfo.SourceDatabaseName,
                        ["SourceMachineName"] = item.ConnectionInfo.SourceMachineName,
                        ["SourceUrl"] = item.ConnectionInfo.SourceUrl,
                        ["WhenConnected"] = item.ConnectionInfo.WhenConnected,
                        ["LastBatchAverageDocumentSize"] = item.LastBatchAverageDocumentSize,
                        ["LastBatchMaxDocumentSize"] = item.LastBatchMaxDocumentSize
                    });
                }

                var outgoing = new DynamicJsonArray();
                foreach (var item in Database.DocumentReplicationLoader.OutgoingConnections)
                {
                    outgoing.Add(new DynamicJsonValue
                    {
                        ["Url"] = item.Destination.Url,
                        ["Database"] = item.Destination.Database,
                        ["Disabled"] = item.Destination.Disabled,
                        ["IgnoredClient"] = item.Destination.IgnoredClient,
                        ["SkipIndexReplication"] = item.Destination.SkipIndexReplication,
                        ["SpecifiedCollections"] = item.Destination.SpecifiedCollections,
                        ["WhenConnected"] = item.WhenConnected						
                    });
                }

                context.Write(writer, new DynamicJsonValue
                {
                    ["IncomingConnections"] = incoming,
                    ["OutgoingConnections"] = outgoing
                });
            }
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/replication/debug/last-received-etag", "GET",
            "/databases/{databaseName:string}/replication/debug/last-received-etag?srcDbId={SourceDBId:Guid}")]
        public Task GetLastReceivedEtag()
        {
            var srcDbIdAsString = GetQueryStringValueAndAssertIfSingleAndNotEmpty("srcDbId");

            Guid srcDbId;
            if (!Guid.TryParse(srcDbIdAsString, out srcDbId))
                throw new ArgumentException("Failed to parse 'srcDbId' parameter, it should be a Guid");

            HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                context.Write(writer, new DynamicJsonValue
                {
                    ["LastReceivedEtag"] = Database.DocumentReplicationLoader
                                                   .GetLastReceivedEtagBySrcDbId(srcDbId),
                    ["SrcDbId"] = srcDbId
                });
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/replication/debug/outgoing-failures", "GET")]
        public Task GetReplicationOugoingFailureStats()
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var data = new DynamicJsonArray();
                foreach (var item in Database.DocumentReplicationLoader.OutgoingFailureInfo)
                {
                    data.Add(new DynamicJsonValue
                    {
                        ["Key"] = new DynamicJsonValue
                        {
                            ["Url"] = item.Key.Url,
                            ["Database"] = item.Key.Database,
                            ["Disabled"] = item.Key.Disabled,
                            ["IgnoredClient"] = item.Key.IgnoredClient,
                            ["SkipIndexReplication"] = item.Key.SkipIndexReplication,
                            ["SpecifiedCollections"] = item.Key.SpecifiedCollections
                        },
                        ["Value"] = new DynamicJsonValue
                        {
                            ["ErrorCount"] = item.Value.ErrorCount,
                            ["NextTimout"] = item.Value.NextTimout
                        }
                    });
                }

                context.Write(writer, data);
            }
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/replication/debug/incoming-last-activity-time", "GET")]
        public Task GetReplicationIncomingActivityTimes()
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var data = new DynamicJsonArray();
                foreach (var item in Database.DocumentReplicationLoader.IncomingLastActivityTime)
                {
                    data.Add(new DynamicJsonValue
                    {
                        ["Key"] = new DynamicJsonValue
                        {
                            ["SourceDatabaseId"] = item.Key.SourceDatabaseId,
                            ["SourceDatabaseName"] = item.Key.SourceDatabaseName,
                            ["SourceMachineName"] = item.Key.SourceMachineName,
                            ["SourceUrl"] = item.Key.SourceUrl
                        },
                        ["Value"] = item.Value
                    });
                }

                context.Write(writer, data);
            }
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/replication/debug/incoming-rejection-info", "GET")]
        public Task GetReplicationIncomingRejectionInfo()
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var data = new DynamicJsonArray();
                foreach (var statItem in Database.DocumentReplicationLoader.IncomingRejectionStats)
                {
                    data.Add(new DynamicJsonValue
                    {
                        ["Key"] = new DynamicJsonValue
                        {
                            ["SourceDatabaseId"] = statItem.Key.SourceDatabaseId,
                            ["SourceDatabaseName"] = statItem.Key.SourceDatabaseName,
                            ["SourceMachineName"] = statItem.Key.SourceMachineName,
                            ["SourceUrl"] = statItem.Key.SourceUrl
                        },
                        ["Value"] = new DynamicJsonArray(statItem.Value.Select(x => new DynamicJsonValue
                                                        {
                                                            ["Reason"] = x.Reason,
                                                            ["When"] = x.When
                                                        }))
                    });
                }

                context.Write(writer,data);
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/replication/debug/outgoing-reconnect-queue", "GET")]
        public Task GetReplicationReconnectionQueue()
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;
            DocumentsOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var data = new DynamicJsonArray();
                foreach (var queueItem in Database.DocumentReplicationLoader.ReconnectQueue)
                {
                    data.Add(new DynamicJsonValue
                    {
                        ["Url"] = queueItem.Url,
                        ["Database"] = queueItem.Database,
                        ["Disabled"] = queueItem.Disabled,
                        ["IgnoredClient"] = queueItem.IgnoredClient,
                        ["SkipIndexReplication"] = queueItem.SkipIndexReplication,
                        ["SpecifiedCollections"] = queueItem.SpecifiedCollections						
                    });
                }

                context.Write(writer, data);
            }
            return Task.CompletedTask;
        }

        
    }
}