using System;
using Raven.Abstractions.Data;
using Raven.Database.Tasks;

namespace Raven.Database.Storage.RavenLevelDB
{
    public class LevelDBStorageActions : IStorageActionsAccessor
    {
        private readonly LevelDBAccessor accessor;
        public ITransactionStorageActions Transactions { get; private set; }
        public IDocumentStorageActions Documents { get; private set; }
        public IQueueStorageActions Queue { get; private set; }
        public IListsStorageActions Lists { get; private set; }
        public ITasksStorageActions Tasks { get; private set; }
        public IStalenessStorageActions Staleness { get; private set; }
        public IAttachmentsStorageActions Attachments { get; private set; }
        public IIndexingStorageActions Indexing { get; private set; }
        public IGeneralStorageActions General { get; private set; }
        public IMappedResultsStorageAction MapReduce { get; private set; }
        public event Action OnStorageCommit;

        public LevelDBStorageActions(LevelDBAccessor accessor)
        {
            this.accessor = accessor;
            this.Documents = new DocumentStorageActions(this.accessor);
        }

        public bool IsWriteConflict(Exception exception)
        {
            throw new NotImplementedException();
        }

        public T GetTask<T>(Func<T, bool> predicate, T newTask) where T : Task
        {
            throw new NotImplementedException();
        }

        public void AfterStorageCommitBeforeWorkNotifications(JsonDocument doc, Action<JsonDocument[]> afterCommit)
        {
            throw new NotImplementedException();
        }

        public void Dispose()
        {
            throw new NotImplementedException();
        }
    }
}
