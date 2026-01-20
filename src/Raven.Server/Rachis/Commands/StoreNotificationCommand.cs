using System;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.NotificationCenter;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Raven.Server.ServerWide;

namespace Raven.Server.Rachis.Commands
{
    public class StoreNotificationCommand(LazyStringValue id, DateTime createdAt, DateTime? postponedUntil, long notificationType, long reason, BlittableJsonReaderObject bjro, NotificationsStorage storage)
        : MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>
    {
        private readonly NotificationsStorage _storage = storage ?? throw new ArgumentNullException(nameof(storage));

        protected override long ExecuteCmd(ClusterOperationContext context)
        {
            _storage.Store(id, createdAt, postponedUntil, notificationType, reason, bjro, context.Transaction);
            return 1;
        }

        public override IReplayableCommandDto<ClusterOperationContext, ClusterTransaction, MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>> ToDto(ClusterOperationContext context)
        {
            return new StoreNotificationCommandDto
            {
                Id = id,
                CreatedAt = createdAt,
                PostponedUntil = postponedUntil,
                NotificationType = notificationType,
                Reason = reason,
                Json = bjro
            };
        }
    }

    internal sealed class StoreNotificationCommandDto : IReplayableCommandDto<ClusterOperationContext, ClusterTransaction, StoreNotificationCommand>
    {
        public LazyStringValue Id { get; set; }
        public DateTime CreatedAt { get; set; }
        public DateTime? PostponedUntil { get; set; }
        public long NotificationType { get; set; }
        public long Reason { get; set; }
        public BlittableJsonReaderObject Json { get; set; }

        public StoreNotificationCommand ToCommand(ClusterOperationContext context, ServerStore serverStore)
        {
            // Use the server's notification storage
            // Clone the id and json into the current context to ensure validity
            var idLsv = context.GetLazyString(Id);
            var json = Json?.Clone(context);
            return new StoreNotificationCommand(idLsv, CreatedAt, PostponedUntil, NotificationType, Reason, json, serverStore.NotificationCenter.Storage);
        }
    }
}
