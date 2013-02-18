using System;
using System.Threading;
using System.Collections.Generic;

namespace Raven.Database.Storage.RavenLevelDB
{
    public class LevelDBTransaction
    {
        private readonly int id;
        private readonly List<Action<LevelDBStorage>> operations = new List<Action<LevelDBStorage>>();
        private readonly LevelDBTransaction[] trackedTransactions;
        private int transactionCountTrackingMe = 1;

        public int Id { get { return id; } }
        public int RefCount { get { return transactionCountTrackingMe; } }

        public LevelDBTransaction(int id, LevelDBTransaction[] activeTransactions)
        {
            this.id = id;
            this.trackedTransactions = activeTransactions;
            foreach (var otherTransaction in this.trackedTransactions)
            { 
                otherTransaction.IncreaseRefCount();
            }
        }

        internal void AddOperation(Action<LevelDBStorage> operation)
        {
            operations.Add(operation);
        }

        internal void Rollback()
        {
            this.Complete();
        }

        internal void Commit(LevelDBStorage storage)
        {
            try
            {
                this.operations.ForEach(action => action(storage));
            }
            finally
            {
                this.Complete();
            }
        }

        private void Complete()
        {
            this.DecreaseRefCount();
            foreach (var otherTransaction in this.trackedTransactions)
            {
                otherTransaction.DecreaseRefCount();
            }
        }

        internal void IncreaseRefCount()
        {
            Interlocked.Increment(ref this.transactionCountTrackingMe);
        }

        internal void DecreaseRefCount()
        {
            Interlocked.Decrement(ref this.transactionCountTrackingMe);
        }
    }
}