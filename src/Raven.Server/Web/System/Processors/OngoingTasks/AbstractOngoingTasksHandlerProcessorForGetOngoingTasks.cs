﻿using System;
using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Http;
using Raven.Server.Documents;
using Raven.Server.Documents.Commands.OngoingTasks;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.OngoingTasks;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Web.System.Processors.OngoingTasks;

internal abstract class AbstractOngoingTasksHandlerProcessorForGetOngoingTasks<TRequestHandler, TOperationContext, TSubscriptionConnectionsState> : AbstractHandlerProxyReadProcessor<OngoingTasksResult, TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
    where TSubscriptionConnectionsState : AbstractSubscriptionConnectionsState
{
    private readonly AbstractOngoingTasks<TSubscriptionConnectionsState> _ongoingTasks;

    protected AbstractOngoingTasksHandlerProcessorForGetOngoingTasks([NotNull] TRequestHandler requestHandler, [NotNull] AbstractOngoingTasks<TSubscriptionConnectionsState> ongoingTasks)
        : base(requestHandler)
    {
        _ongoingTasks = ongoingTasks ?? throw new ArgumentNullException(nameof(ongoingTasks));
    }

    protected override bool SupportsCurrentNode => true;

    protected override async ValueTask HandleCurrentNodeAsync()
    {
        using (ContextPool.AllocateOperationContext(out TOperationContext context))
        await using (var writer = new AsyncBlittableJsonTextWriterForDebug(context, ServerStore, RequestHandler.ResponseBodyStream()))
        {
            var result = GetOngoingTasksInternal();

            context.Write(writer, result.ToJson());
        }
    }

    protected override RavenCommand<OngoingTasksResult> CreateCommandForNode(string nodeTag) => new GetOngoingTasksCommand(nodeTag);

    protected abstract long SubscriptionsCount { get; }

    public OngoingTasksResult GetOngoingTasksInternal()
    {
        var server = RequestHandler.ServerStore;
        var ongoingTasksResult = new OngoingTasksResult();
        using (server.Engine.ContextPool.AllocateOperationContext(out ClusterOperationContext context))
        using (context.OpenReadTransaction())
        {
            var databaseRecord = server.Cluster.ReadDatabase(context, RequestHandler.DatabaseName);

            if (databaseRecord == null)
                return ongoingTasksResult;

            var clusterTopology = server.GetClusterTopology(context);

            foreach (var tasks in _ongoingTasks.GetAllTasks(context, clusterTopology, databaseRecord))
                ongoingTasksResult.OngoingTasks.Add(tasks);

            ongoingTasksResult.SubscriptionsCount = (int)SubscriptionsCount;

            ongoingTasksResult.PullReplications = databaseRecord.HubPullReplications.ToList();

            return ongoingTasksResult;
        }
    }
}
