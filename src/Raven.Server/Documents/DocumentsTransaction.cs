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

        // All documents written by this transaction share one LastModified reading - they commit at
        // the same instant, and it saves a clock read (vdso call) per document on the merger.
        private long _cachedLastModifiedTicks;

        public long GetOrCreateLastModifiedTicks()
        {
            if (_cachedLastModifiedTicks == 0)
                _cachedLastModifiedTicks = _context.DocumentDatabase.Time.GetUtcNow().Ticks;
            return _cachedLastModifiedTicks;
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

        // Same idea for the tombstones table: an id-overwriting put opens it per document to look for
        // a predecessor tombstone, and consecutive puts hit the same collection. Schema is the single
        // static TombstonesSchema, so we key only on the collection.
        private CollectionName _cachedTombstonesCollection;
        private Voron.Data.Tables.Table _cachedTombstonesTable;

        public Voron.Data.Tables.Table GetOrOpenTombstonesTable(CollectionName collection, Voron.Data.Tables.TableSchema tombstonesSchema)
        {
            if (ReferenceEquals(collection, _cachedTombstonesCollection))
                return _cachedTombstonesTable;

            var table = InnerTransaction.OpenTable(tombstonesSchema, collection.GetTableName(CollectionTableType.Tombstones));
            _cachedTombstonesCollection = collection;
            _cachedTombstonesTable = table;
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

        // Internal listeners (indexing, ETL, replication, subscriptions) only need to know which
        // collections changed; they are notified once per collection per transaction. External
        // /changes clients need per-document detail, so those are only built when a client is
        // connected. Keyed by collection name, value = whether the change came from replication.
        private Dictionary<string, bool> _changedCollections;

        public void AddAfterCommitNotification(string collectionName, string id, string changeVector, DocumentChangeTypes type)
        {
            var triggeredByReplicationThread = IncomingReplicationHandler.IsIncomingInternalReplication;

            (_changedCollections ??= new Dictionary<string, bool>(StringComparer.OrdinalIgnoreCase))[collectionName] = triggeredByReplicationThread;

            if (_changes.HasConnections == false)
                return;

            (_documentNotifications ??= new List<DocumentChange>()).Add(new DocumentChange
            {
                ChangeVector = changeVector,
                CollectionName = collectionName,
                Id = id,
                Type = type,
                TriggeredByReplicationThread = triggeredByReplicationThread
            });
        }

        public void AddAfterCommitNotification(DocumentChange change)
        {
            AddAfterCommitNotification(change.CollectionName, change.Id, change.ChangeVector, change.Type);
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

            if (_changedCollections != null)
            {
                // one wakeup per changed collection for internal listeners
                foreach (var changed in _changedCollections)
                {
                    _changes.RaiseInternalDocumentChangeNotification(new DocumentChange
                    {
                        CollectionName = changed.Key,
                        Type = DocumentChangeTypes.Put,
                        TriggeredByReplicationThread = changed.Value
                    });
                }
            }

            if (_documentNotifications?.Count > 0)
            {
                // per-document detail for connected /changes clients
                foreach (var notification in _documentNotifications)
                {
                    _changes.SendDocumentChangeToConnections(notification);
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
                || _changedCollections != null
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
