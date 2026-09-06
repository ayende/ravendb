using System;
using System.Collections.Generic;
using Raven.Client.Documents.Changes;
using Raven.Client.Extensions;
using Raven.Server.Documents.Changes;
using Raven.Server.Documents.Replication.Incoming;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Voron;
using Voron.Impl;

namespace Raven.Server.Documents
{
    public sealed class DocumentsTransaction : RavenTransaction
    {
        private readonly DocumentsOperationContext _context;

        private readonly DocumentsChanges _changes;

        private List<DocumentChange> _documentNotifications;

        private List<CounterChange> _counterNotifications;

        private List<TimeSeriesChange> _timeSeriesNotifications;

        private List<Slice> _attachmentHashesToMaybeDelete;

        private bool _executeDocumentsMigrationAfterCommit;

        private bool _replaced;

        private Dictionary<string, CollectionName> _collectionCache;

        // Rate meters were marked per document, and each Mark reads the (vdso) clock. The merger
        // commits many puts per transaction, so we accumulate here and mark once in BeforeCommit -
        // same time bucket, far fewer clock reads on the merger thread.
        private long _putsCount;
        private long _putsBytes;

        public void AccumulatePutMetrics(long documentSize)
        {
            _putsCount++;
            _putsBytes += documentSize;
        }

        // OpenTable caches per transaction, but the put path still paid a string->slice conversion,
        // a TableKey and a dictionary lookup for every document. Consecutive puts in a batch almost
        // always target the same collection, so a single-entry cache keyed by the (per-tx cached)
        // collection and the (static) schema references skips that work on the merger.
        private CollectionName _cachedDocsTableCollection;
        private Voron.Data.Tables.TableSchema _cachedDocsTableSchema;
        private Voron.Data.Tables.Table _cachedDocsTable;

        public Voron.Data.Tables.Table GetOrOpenDocumentsTable(CollectionName collection, Voron.Data.Tables.TableSchema schema)
        {
            if (ReferenceEquals(collection, _cachedDocsTableCollection) && ReferenceEquals(schema, _cachedDocsTableSchema))
                return _cachedDocsTable;

            var table = InnerTransaction.OpenTable(schema, collection.GetTableName(CollectionTableType.Documents));
            _cachedDocsTableCollection = collection;
            _cachedDocsTableSchema = schema;
            _cachedDocsTable = table;
            return table;
        }

        public DocumentsTransaction(DocumentsOperationContext context, Transaction transaction, DocumentsChanges changes)
            : base(transaction)
        {
            _context = context;
            _changes = changes;

            if (context.DocumentDatabase is ShardedDocumentDatabase sharded)
            {
                transaction.Owner = _context;
                transaction.OnBeforeCommit += sharded.ShardedDocumentsStorage.OnBeforeCommit;
                transaction.LowLevelTransaction.OnRollBack += sharded.ShardedDocumentsStorage.OnFailure;
            }
        }

        public override void BeforeCommit()
        {
            if (_putsCount != 0)
            {
                var docsMetrics = _context.DocumentDatabase.Metrics.Docs;
                docsMetrics.PutsPerSec.MarkSingleThreaded(_putsCount);
                docsMetrics.BytesPutsPerSec.MarkSingleThreaded(_putsBytes);
                _putsCount = 0;
                _putsBytes = 0;
            }

            if (_attachmentHashesToMaybeDelete == null)
                return;

            _context.DocumentDatabase.DocumentsStorage.AttachmentsStorage.RemoveAttachmentStreamsWithoutReferences(_context, _attachmentHashesToMaybeDelete);
        }

        protected override void AfterCommit()
        {
            if (_executeDocumentsMigrationAfterCommit)
            {
                var shardedDatabase = ShardedDocumentDatabase.CastToShardedDocumentDatabase(_context.DocumentDatabase);
                shardedDatabase.DocumentsMigrator.ExecuteMoveDocumentsAsync().IgnoreUnobservedExceptions();
            }

            base.AfterCommit();
        }

        public DocumentsTransaction BeginAsyncCommitAndStartNewTransaction(DocumentsOperationContext context)
        {
            BeforeCommit();
            _replaced = true;
            _context.ResetTablesCache();
            var tx = InnerTransaction.BeginAsyncCommitAndStartNewTransaction(context.PersistentContext);
            return new DocumentsTransaction(context, tx, _changes);
        }

        public void AddAfterCommitNotification(DocumentChange change)
        {
            change.TriggeredByReplicationThread = IncomingReplicationHandler.IsIncomingInternalReplication;

            if (_documentNotifications == null)
                _documentNotifications = new List<DocumentChange>();
            _documentNotifications.Add(change);
        }

        public void AddAfterCommitNotification(CounterChange change)
        {
            change.TriggeredByReplicationThread = IncomingReplicationHandler.IsIncomingInternalReplication;

            if (_counterNotifications == null)
                _counterNotifications = new List<CounterChange>();
            _counterNotifications.Add(change);
        }

        public void AddAfterCommitNotification(TimeSeriesChange change)
        {
            change.TriggeredByReplicationThread = IncomingReplicationHandler.IsIncomingInternalReplication;

            if (_timeSeriesNotifications == null)
                _timeSeriesNotifications = new List<TimeSeriesChange>();
            _timeSeriesNotifications.Add(change);
        }

        private bool _isDisposed;

        public override void Dispose()
        {
            if (_isDisposed)
                return;
            _isDisposed = true;

            if (_replaced == false)
            {
                if (_context.Transaction != null && _context.Transaction != this)
                    ThrowInvalidTransactionUsage();

                _context.ResetTablesCache();
                _context.Transaction = null;
            }

            base.Dispose();
        }

        protected override void RaiseNotifications()
        {
            base.RaiseNotifications();

            if (_documentNotifications?.Count > 0)
            {
                foreach (var notification in _documentNotifications)
                {
                    _changes.RaiseNotifications(notification);
                }
            }

            if (_counterNotifications?.Count > 0)
            {
                foreach (var notification in _counterNotifications)
                {
                    _changes.RaiseNotifications(notification);
                }
            }

            if (_timeSeriesNotifications?.Count > 0)
            {
                foreach (var notification in _timeSeriesNotifications)
                {
                    _changes.RaiseNotifications(notification);
                }
            }
        }

        protected override bool ShouldRaiseNotifications()
        {
            return base.ShouldRaiseNotifications()
                || _documentNotifications != null
                || _counterNotifications != null
                || _timeSeriesNotifications != null;
        }

        public bool TryGetFromCache(string collectionName, out CollectionName name)
        {
            if (_collectionCache != null)
                return _collectionCache.TryGetValue(collectionName, out name);

            name = null;
            return false;
        }

        public void AddToCache(string collectionName, CollectionName name)
        {
            if (_collectionCache == null)
                _collectionCache = new Dictionary<string, CollectionName>(StringComparer.OrdinalIgnoreCase);

            _collectionCache.Add(collectionName, name);
        }

        internal void CheckIfShouldDeleteAttachmentStream(Slice hash)
        {
            var clone = hash.Clone(InnerTransaction.Allocator);
            _attachmentHashesToMaybeDelete ??= new();
            _attachmentHashesToMaybeDelete.Add(clone);
        }

        internal void ExecuteDocumentsMigrationAfterCommit()
        {
            _executeDocumentsMigrationAfterCommit = true;
        }
    }
}
