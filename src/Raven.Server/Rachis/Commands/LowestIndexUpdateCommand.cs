using System;
using JetBrains.Annotations;
using Raven.Server.ServerWide;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Rachis.Commands;

public sealed class LowestIndexUpdateCommand : MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>
{
    private readonly RachisConsensus _engine;

    private long _lowestIndexInEntireCluster;


    public LowestIndexUpdateCommand([NotNull] RachisConsensus engine, long lowestIndexInEntireCluster)
    {
        _engine = engine ?? throw new ArgumentNullException(nameof(engine));
        _lowestIndexInEntireCluster = lowestIndexInEntireCluster;
    }

    protected override long ExecuteCmd(ClusterOperationContext context)
    {
        _engine.TruncateLogBefore(context, _lowestIndexInEntireCluster);
        return 1;
    }

    public override IReplayableCommandDto<ClusterOperationContext, ClusterTransaction, MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>> ToDto(ClusterOperationContext context)
    {
        return new LowestIndexUpdateCommandDto
        {
            LowestIndexInEntireCluster = _lowestIndexInEntireCluster
        };
    }
}

internal sealed class LowestIndexUpdateCommandDto : IReplayableCommandDto<ClusterOperationContext, ClusterTransaction, LowestIndexUpdateCommand>
{
    public long LowestIndexInEntireCluster { get; set; }

    public LowestIndexUpdateCommand ToCommand(ClusterOperationContext context, ServerStore serverStore)
    {
        return new LowestIndexUpdateCommand(serverStore.Engine, LowestIndexInEntireCluster);
    }
}

