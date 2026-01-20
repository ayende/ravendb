using System;
using Raven.Client.Documents.Operations.Backups;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Raven.Client.Json.Serialization;
using Raven.Server.Documents;
using Raven.Server.ServerWide;

namespace Raven.Server.Documents.TransactionMerger.Commands
{
    public sealed class UpdateLocalBackupStatusCommand : MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>
    {
        private readonly DynamicJsonValue _backupStatusAsJson;
        private readonly string _databaseName;
        private readonly long _taskId;

        public UpdateLocalBackupStatusCommand(PeriodicBackupStatus backupStatus, string databaseName, long taskId)
        {
            _backupStatusAsJson = (backupStatus ?? throw new ArgumentNullException(nameof(backupStatus))).ToJson();
            _databaseName = databaseName ?? throw new ArgumentNullException(nameof(databaseName));
            _taskId = taskId;
        }

        protected override long ExecuteCmd(ClusterOperationContext context)
        {
            var statusBlittable = context.ReadObject(_backupStatusAsJson, $"backup-status-update-taskId-{_taskId}", BlittableJsonDocumentBuilder.UsageMode.ToDisk);
            BackupStatusStorage.Insert(context, statusBlittable, _databaseName, _taskId);
            return 1;
        }

        public override IReplayableCommandDto<ClusterOperationContext, ClusterTransaction, MergedTransactionCommand<ClusterOperationContext, ClusterTransaction>> ToDto(ClusterOperationContext context)
        {
            return new UpdateLocalBackupStatusCommandDto
            {
                BackupStatus = _backupStatusAsJson,
                DatabaseName = _databaseName,
                TaskId = _taskId
            };
        }
    }

    internal sealed class UpdateLocalBackupStatusCommandDto : IReplayableCommandDto<ClusterOperationContext, ClusterTransaction, UpdateLocalBackupStatusCommand>
    {
        public DynamicJsonValue BackupStatus { get; set; }
        public string DatabaseName { get; set; }
        public long TaskId { get; set; }

        public UpdateLocalBackupStatusCommand ToCommand(ClusterOperationContext context, ServerStore serverStore)
        {
            // Rehydrate to PeriodicBackupStatus through blittable to keep parity with execution path
            using (var bjro = context.ReadObject(BackupStatus, $"backup-status-update-taskId-{TaskId}", BlittableJsonDocumentBuilder.UsageMode.ToDisk))
            {
                var status = JsonDeserializationClient.PeriodicBackupStatus(bjro);
                return new UpdateLocalBackupStatusCommand(status, DatabaseName, TaskId);
            }
        }
    }
}
