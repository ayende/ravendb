using Raven.Server.ServerWide;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.TransactionMerger.Commands;

public interface IReplayableCommandDto<TOperationContext, TTransaction, out TCommand>
    where TCommand : MergedTransactionCommand<TOperationContext, TTransaction>
    where TOperationContext : TransactionOperationContext<TTransaction>
    where TTransaction : RavenTransaction
{
    TCommand ToCommand(TOperationContext context, DocumentDatabase database)
        => throw new System.NotSupportedException("ToCommand(context, DocumentDatabase) is not supported for this DTO.");

    TCommand ToCommand(TOperationContext context, ServerStore serverStore)
        => throw new System.NotSupportedException("ToCommand(context, ServerStore) is not supported for this DTO.");
}
