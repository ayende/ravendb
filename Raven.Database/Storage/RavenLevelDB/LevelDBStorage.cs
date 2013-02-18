using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using LevelDB;
using Raven.Abstractions.Data;
using Raven.Abstractions.MEF;
using Raven.Database.Config;
using Raven.Database.Impl;
using Raven.Database.Plugins;

namespace Raven.Database.Storage.RavenLevelDB
{
    public class LevelDBStorage : ITransactionalStorage
    {
        private readonly ConcurrentDictionary<string, object> backing = new ConcurrentDictionary<string, object>();
        private readonly ConcurrentDictionary<string, int> keysToTransactionId = new ConcurrentDictionary<string, int>();
        private readonly ConcurrentDictionary<int, LevelDBTransaction> activeTransactions = new ConcurrentDictionary<int, LevelDBTransaction>();

        private int lastTransactionId = 0;
        private readonly DB db;

        public LevelDBStorage(string path)
        {
            var options = new Options {CreateIfMissing = true};
            this.db = new DB(options, path);
            this.db.GetProperty()
        }

        private LevelDBTransaction CreateTransaction()
        {
            Interlocked.Increment(ref this.lastTransactionId);
            var currentTransactions = this.activeTransactions.ToArray();
            var transaction = new LevelDBTransaction(this.lastTransactionId, currentTransactions.Select(x => x.Value).ToArray());
            this.activeTransactions.TryAdd(this.lastTransactionId, transaction);
            return transaction;
        }

        private void RollbackTransaction(LevelDBTransaction transaction)
        {
            transaction.Rollback();
            this.TryPruneObsoleteTransactions();
        }

        private void CommitTransaction(LevelDBTransaction transaction)
        {
            // This would be LevelDB.BatchWrite, I just use a database wide lock, mileage may vary
            lock (this.backing)
            {
                transaction.Commit(this);
            }
            this.TryPruneObsoleteTransactions();
        }

        private void TryPruneObsoleteTransactions()
        {
            var currentTransactions = this.activeTransactions.Values.OrderBy(x => x.Id).ToArray();

            var transactionsWeCanRemove = new List<int>();
            foreach (var transaction in currentTransactions)
            {
                if (transaction.RefCount == 0)
                    transactionsWeCanRemove.Add(transaction.Id);
                else
                    break;
            }

            var keysToClear = new List<string>();

            foreach (var kv in this.keysToTransactionId)
            {
                if (transactionsWeCanRemove.Contains(kv.Value))
                    keysToClear.Add(kv.Key);
            }

            keysToClear.ForEach(key =>
            {
                int ignored;
                this.keysToTransactionId.TryRemove(key, out ignored);
            });

            transactionsWeCanRemove.ForEach(key =>
            {
                LevelDBTransaction ignored;
                this.activeTransactions.TryRemove(key, out ignored);
            });
        }

        public Object Get(string id, LevelDBTransaction transaction)
        {
            // This would ordinarily be using a Snapshot for the transaction in LevelDB
            Object outValue;
            if (this.backing.TryGetValue(id, out outValue))
                return outValue;
            return null;
        }

        public void Put(string id, Object obj, LevelDBTransaction transaction)
        {
            this.keysToTransactionId.AddOrUpdate(id, (key) =>
            {
                transaction.AddOperation(storage => storage.Put(id, obj));
                return transaction.Id;
            },
                                                 (key, oldValue) =>
                                                 {
                                                     // NOTE: This doesn't handle the transaction doing multiple operations on the same key
                                                     throw new Exception("This should be a concurrency exception");
                                                 });
        }

        public void Delete(string id, LevelDBTransaction transaction)
        {
            this.keysToTransactionId.AddOrUpdate(id, (key) =>
            {
                transaction.AddOperation(storage => storage.Delete(id));
                return transaction.Id;
            },
                                                 (key, oldValue) =>
                                                 {
                                                     // NOTE: This doesn't handle the transaction doing multiple operations on the same key
                                                     throw new Exception("This should be a concurrency exception");
                                                 });
        }

        internal void Delete(string id)
        {
            object ignored;
            this.backing.TryRemove(id, out ignored);
        }

        internal void Put(string id, Object obj)
        {
            this.backing[id] = obj;
        }

        public void Batch(Action<IStorageActionsAccessor> action)
        {
            var transaction = this.CreateTransaction();
            var accessor = new LevelDBAccessor(this, transaction);
            var storageActions = new LevelDBStorageActions(accessor);
            try
            {
                action(storageActions);
            }
            catch (Exception ex)
            {
                this.RollbackTransaction(transaction);
                throw;
            }
            this.CommitTransaction(transaction);
        }

        public void Dispose()
        {
            this.db.Dispose();
        }

        #region TO BE IMPLEMENTED

        public Guid Id { get; private set; }
        public void ExecuteImmediatelyOrRegisterForSynchronization(Action action)
        {
            throw new NotImplementedException();
        }

        public bool Initialize(IUuidGenerator generator, OrderedPartCollection<AbstractDocumentCodec> documentCodecs)
        {
            throw new NotImplementedException();
        }

        public void StartBackupOperation(DocumentDatabase database, string backupDestinationDirectory, bool incrementalBackup,
                                         DatabaseDocument documentDatabase)
        {
            throw new NotImplementedException();
        }

        public void Restore(string backupLocation, string databaseLocation, Action<string> output, bool defrag)
        {
            throw new NotImplementedException();
        }

        public long GetDatabaseSizeInBytes()
        {
            throw new NotImplementedException();
        }

        public long GetDatabaseCacheSizeInBytes()
        {
            throw new NotImplementedException();
        }

        public long GetDatabaseTransactionVersionSizeInBytes()
        {
            throw new NotImplementedException();
        }

        public string FriendlyName { get; private set; }
        public bool HandleException(Exception exception)
        {
            throw new NotImplementedException();
        }

        public void Compact(InMemoryRavenConfiguration configuration)
        {
            throw new NotImplementedException();
        }

        public Guid ChangeId()
        {
            throw new NotImplementedException();
        }

        public void ClearCaches()
        {
            throw new NotImplementedException();
        }

        public void DumpAllStorageTables()
        {
            throw new NotImplementedException();
        }

        #endregion
    }
}