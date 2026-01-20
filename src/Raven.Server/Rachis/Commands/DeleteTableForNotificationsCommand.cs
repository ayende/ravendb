using System;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide;

namespace Raven.Server.Rachis.Commands;

public class DeleteTableForNotificationsCommand : MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>
{
    private readonly string _tableName;

    public DeleteTableForNotificationsCommand(string tableName)
    {
        _tableName = tableName;
    }

    protected override long ExecuteCmd(ClusterOperationContext context)
    {
        context.Transaction.InnerTransaction.DeleteTable(_tableName);
        return 1;
    }

    public override IReplayableCommandDto<ClusterOperationContext, ClusterTransaction, MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>> ToDto(ClusterOperationContext context)
    {
        return new DeleteTableForNotificationsCommandDto
        {
            TableName = _tableName
        };
    }
}

internal sealed class DeleteTableForNotificationsCommandDto : IReplayableCommandDto<ClusterOperationContext, ClusterTransaction, DeleteTableForNotificationsCommand>
{
    public string TableName { get; set; }

    public DeleteTableForNotificationsCommand ToCommand(ClusterOperationContext context, ServerStore serverStore)
    {
        return new DeleteTableForNotificationsCommand(TableName);
    }
}
