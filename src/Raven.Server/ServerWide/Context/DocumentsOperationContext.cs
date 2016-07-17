﻿using System;
using Raven.Server.Documents;
using Sparrow;
using Sparrow.Json;
using Voron;

namespace Raven.Server.ServerWide.Context
{
    public class DocumentsOperationContext : TransactionOperationContext<DocumentsTransaction>
    {
        private readonly DocumentDatabase _documentDatabase;
       
        public DocumentsOperationContext(UnmanagedBuffersPool pool, DocumentDatabase documentDatabase)
            : base(pool)
        {
            _documentDatabase = documentDatabase;
        }

        protected override DocumentsTransaction CreateReadTransaction(ByteStringContext context)
        {
            return new DocumentsTransaction(this, _documentDatabase.DocumentsStorage.Environment.ReadTransaction(context), _documentDatabase.Notifications);
        }

        protected override DocumentsTransaction CreateWriteTransaction(ByteStringContext context)
        {
            var tx = new DocumentsTransaction(this, _documentDatabase.DocumentsStorage.Environment.WriteTransaction(context), _documentDatabase.Notifications);

            var options = _documentDatabase.DocumentsStorage.Environment.Options;

            if ((options.TransactionsMode == TransactionsMode.Lazy || options.TransactionsMode == TransactionsMode.Danger) &&
                options.NonSafeTransactionExpiration != null && options.NonSafeTransactionExpiration < DateTime.Now)
            {
                options.TransactionsMode = TransactionsMode.Safe;
            }

            tx.InnerTransaction.LowLevelTransaction.IsLazyTransaction = 
                options.TransactionsMode == TransactionsMode.Lazy;
            // IsLazyTransaction can be overriden later by a specific feature like bulk insert

            return tx;
        }

        public StorageEnvironment Environment()
        {
            return _documentDatabase.DocumentsStorage.Environment;
        }
    }
}