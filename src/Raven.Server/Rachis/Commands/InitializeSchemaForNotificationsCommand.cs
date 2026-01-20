using System;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.ServerWide;

namespace Raven.Server.Rachis.Commands;

public class InitializeSchemaForNotificationsCommand : MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>
{
    private readonly string _tableName;

    public InitializeSchemaForNotificationsCommand(string tableName)
    {
        _tableName = tableName;
    }

    protected override long ExecuteCmd(ClusterOperationContext context)
    {
        Documents.Schemas.Notifications.Current.Create(context.Transaction.InnerTransaction, _tableName, 16);
        return 1;
    }

    public override IReplayableCommandDto<ClusterOperationContext, ClusterTransaction, MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>> ToDto(ClusterOperationContext context)
    {
        return new InitializeSchemaForNotificationsCommandDto
        {
            TableName = _tableName
        };
    }
}

internal sealed class InitializeSchemaForNotificationsCommandDto : IReplayableCommandDto<ClusterOperationContext, ClusterTransaction, InitializeSchemaForNotificationsCommand>
{
    public string TableName { get; set; }

    public InitializeSchemaForNotificationsCommand ToCommand(ClusterOperationContext context, ServerStore serverStore)
    {
        return new InitializeSchemaForNotificationsCommand(TableName);
    }
}
