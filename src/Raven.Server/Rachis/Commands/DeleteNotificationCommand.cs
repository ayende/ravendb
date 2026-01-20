using System;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Raven.Server.NotificationCenter;
using Raven.Server.ServerWide;

namespace Raven.Server.Rachis.Commands
{
    public class DeleteNotificationCommand(string notificationId, NotificationsStorage storage) : MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>
    {
        public bool Deleted;

        protected override long ExecuteCmd(ClusterOperationContext context)
        {
            Deleted = storage.DeleteFromTable(notificationId, context.Transaction);
            return Deleted ? 1 : 0;
        }

        public override IReplayableCommandDto<ClusterOperationContext, ClusterTransaction, MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>> ToDto(ClusterOperationContext context)
        {
            return new DeleteNotificationCommandDto
            {
                NotificationId = notificationId
            };
        }
    }

    internal sealed class DeleteNotificationCommandDto : IReplayableCommandDto<ClusterOperationContext, ClusterTransaction, DeleteNotificationCommand>
    {
        public string NotificationId { get; set; }

        public DeleteNotificationCommand ToCommand(ClusterOperationContext context, ServerStore serverStore)
        {
            return new DeleteNotificationCommand(NotificationId, serverStore.NotificationCenter.Storage);
        }
    }
}
