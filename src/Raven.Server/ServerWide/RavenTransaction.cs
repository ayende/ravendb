using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using Raven.Server.Logging;
using Sparrow.Logging;
using Sparrow.Server.Logging;
using Voron;
using Voron.Impl;

namespace Raven.Server.ServerWide
{
    public class RavenTransaction : IDisposable
    {
        private static readonly RavenLogger Logger = RavenLogManager.Instance.GetLoggerForServer<RavenTransaction>();

        public Transaction InnerTransaction;

        public RavenTransaction(Transaction transaction)
        {
            InnerTransaction = transaction;
        }

        public virtual void BeforeCommit()
        {

        }

        public void Commit()
        {
            BeforeCommit();
            InnerTransaction.Commit();
        }

        /// <summary>
        /// Completes this transaction in memory, hands its journal write off to run in the background,
        /// and returns a new transaction to keep working in. Must be paired with EndAsyncCommit on this
        /// instance, in submission order.
        /// </summary>
        public virtual RavenTransaction BeginAsyncCommitAndStartNewTransaction(TransactionPersistentContext persistentContext)
        {
            BeforeCommit();
            return new RavenTransaction(InnerTransaction.BeginAsyncCommitAndStartNewTransaction(persistentContext));
        }

        public void EndAsyncCommit()
        {
            InnerTransaction.EndAsyncCommit();
        }

        public bool Disposed;

        public virtual void Dispose()
        {
            if (Disposed)
                return;

            Disposed = true;

            var committed = InnerTransaction.LowLevelTransaction.Committed;

            InnerTransaction?.Dispose();
            InnerTransaction = null;

            if (committed)
                AfterCommit();
        }

        protected virtual void RaiseNotifications()
        {
        }

        protected virtual bool ShouldRaiseNotifications()
        {
            return false;
        }

        [DoesNotReturn]
        protected static void ThrowInvalidTransactionUsage()
        {
            throw new InvalidOperationException("There is a different transaction in context.");
        }

        protected virtual void AfterCommit()
        {
            if (ShouldRaiseNotifications() == false)
                return;

            ThreadPool.QueueUserWorkItem(state =>
            {
                try
                {
                    ((RavenTransaction)state).RaiseNotifications();
                }
                catch (Exception e)
                {
                    if (Logger.IsErrorEnabled)
                        Logger.Error("Failed to raise notifications", e);
                }
            }, this);
        }
    }
}
