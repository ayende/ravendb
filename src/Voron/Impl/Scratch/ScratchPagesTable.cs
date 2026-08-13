using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Sparrow;
using Sparrow.Server.Utils;
using Voron.Util;

namespace Voron.Impl.Scratch
{
    /// <summary>
    /// Maps a data file page number to its current location in the scratch buffers, with multi-version
    /// visibility so that a single writer can publish new state without copying while any number of
    /// concurrent readers keep consistent snapshots.
    ///
    /// Concurrency model:
    /// - There is a single writer at any time (the write transaction, serialized by the environment's
    ///   write lock). All mutation methods assume that.
    /// - Readers are wait-free. They never observe a version newer than their snapshot because every
    ///   version is stamped with the transaction that created it, and a reader's snapshot id is always
    ///   a committed transaction id - strictly older than anything the current writer creates.
    /// - Versions become unreachable only when no active or future snapshot can observe them
    ///   (see PruneChain / PruneChainPrecise), so unlinking is safe while readers traverse.
    /// </summary>
    public sealed class ScratchPagesTable
    {
        internal struct ScratchTableSlot
        {
            // 0 marks an empty slot, so we store PageNumberInDataFile + 1 (page 0 is a valid key).
            // A slot never changes its key while readers may be probing the array.
            internal long KeyPlusOne;

            // Ordered linked list of versions, newest first. The head is always the version that the writer sees as current.
            internal PageFromScratchBuffer Head;
        }
        
        private const int InitialSize = 1024;

        // A longer chain would require a the more expensive precise prune.
        private const int ChainDepthPruneThreshold = 8;

        private readonly ActiveTransactions _activeTransactions;

        private ScratchTableSlot[] _slots = new ScratchTableSlot[InitialSize];
        private int _usedSlots;

        // Count of non tombstone pages, for snapshot enumeration.
        private int _visibleCount;

        private long _txId;
        private long _lastCommittedTxId;

        private long _pruneFloor;

        // Sorted ids of the active snapshots (plus the last committed id as a sentinel for readers
        // that are registering concurrently). Fetched lazily, at most once per write transaction,
        // only when a deep chain actually needs the precise prune.
        private readonly List<long> _activeSnapshots = new();
        private bool _activeSnapshotsFetched;

        // Pages whose chains the current transaction touched, in mutation order, possibly with duplicates - to make rollbacks easier
        private readonly List<long> _undo = [];

        private bool _hasSurvivingRemovalsInCurrentTx;
        private long _survivingRemovalsUpToTxId;

        public ScratchPagesTable(ActiveTransactions activeTransactions)
        {
            _activeTransactions = activeTransactions;
        }

        public int VisibleCount => _visibleCount;

        public void BeginWriteTransaction(long txId, long lastCommittedTxId)
        {
            _undo.Clear();
            _hasSurvivingRemovalsInCurrentTx = false;
            _txId = txId;
            _lastCommittedTxId = lastCommittedTxId;
            var oldestActive = _activeTransactions.OldestTransaction;
            _pruneFloor = oldestActive == 0 ? lastCommittedTxId : Math.Min(lastCommittedTxId, oldestActive);
            _activeSnapshotsFetched = false;

            if (_survivingRemovalsUpToTxId != 0 && _survivingRemovalsUpToTxId <= lastCommittedTxId)
                _survivingRemovalsUpToTxId = 0; // a published record's bound covers them now
        }

        /// <summary>
        /// Pruning drops versions that no active snapshot can observe, and it decides that from a racy
        /// enumeration of the active transactions plus the last committed id as a sentinel. That is only
        /// sound because a reader always snapshots at the latest published record, so it can never adopt an
        /// id below the floor we prune against. This asserts that invariant where readers take their
        /// snapshot: if a path ever hands a reader an older id, pruning silently starts dropping versions
        /// that reader still needs and it reads stale pages from the data file.
        /// </summary>
        [Conditional("DEBUG")]
        internal void AssertReaderSnapshotIsNotBelowPruneFloor(long snapshotTxId)
        {
            Debug.Assert(snapshotTxId >= _pruneFloor,
                $"A read transaction adopted snapshot {snapshotTxId}, below the prune floor {_pruneFloor}. " +
                "Versions it needs may already have been pruned.");
        }

        /// <summary>
        /// _visibleCount is maintained by hand across Set, RemoveInternal, RollbackCurrentTransaction and
        /// Rebuild, and snapshots carry it as their count. A single missed adjustment makes the count
        /// disagree with what enumeration actually yields, so recount and compare while debugging.
        /// </summary>
        [Conditional("DEBUG")]
        private void AssertVisibleCountMatches()
        {
            var actual = 0;
            foreach (var slot in _slots)
            {
                if (slot.KeyPlusOne == 0)
                    continue;

                var head = slot.Head;
                if (head != null && head.IsRemoved == false)
                    actual++;
            }

            Debug.Assert(actual == _visibleCount,
                $"Scratch pages table reports {_visibleCount} visible pages but the slots hold {actual}");
        }

        public ScratchPagesSnapshot CaptureSnapshot(long visibleAsOfTxId)
        {
            AssertVisibleCountMatches();
            return new ScratchPagesSnapshot(_slots, visibleAsOfTxId, _visibleCount);
        }

        /// <summary>
        /// Captures a snapshot for a transaction whose state record publishes without consuming a
        /// transaction id (a book-keeping commit - typically the one applying a journal flush). Such a
        /// transaction has no page modifications of its own; its only changes are free-after-flush
        /// tombstones, which are stamped with the current (never published) id and would stay invisible
        /// at the unchanged visibility bound. Those keys are dropped from a compacted copy of the slot
        /// array instead: records published earlier keep the old array and continue resolving the
        /// flushed pages to scratch, while this record's readers see them gone - atomically with the
        /// data pager state that covers their new location in the data file.
        /// </summary>
        public ScratchPagesSnapshot CaptureSnapshotWithoutCurrentTransaction(long visibleAsOfTxId)
        {
            // _survivingRemovalsUpToTxId: a rolled-back transaction left surviving removals stamped
            // with what is now our own (reused) id - they need the same compaction our own would
            if (_undo.Count > 0 || _hasSurvivingRemovalsInCurrentTx || _survivingRemovalsUpToTxId != 0)
            {
                Rebuild(dropKeysRemovedByCurrentTransaction: true);
                _survivingRemovalsUpToTxId = 0;
            }

            return new ScratchPagesSnapshot(_slots, visibleAsOfTxId, _visibleCount);
        }

        public bool TryGetValue(long pageNumber, out PageFromScratchBuffer value)
        {
            // writer-side lookup: the head is always the writer's visible version
            var slots = _slots;
            var mask = slots.Length - 1;
            var keyPlusOne = pageNumber + 1;
            var i = GetIndex(slots, pageNumber);
            while (true)
            {
                ref var slot = ref slots[i];
                var k = slot.KeyPlusOne;
                if (k == keyPlusOne)
                {
                    var head = slot.Head;
                    if (head == null || head.IsRemoved)
                        break;
                    value = head;
                    return true;
                }

                if (k == 0)
                    break;

                i = (i + 1) & mask;
            }

            value = null;
            return false;
        }

        public bool ContainsKey(long pageNumber) => TryGetValue(pageNumber, out _);

        public void Set(long pageNumber, PageFromScratchBuffer value)
        {
            Debug.Assert(value.AllocatedInTransaction == _txId,
                $"Set(page: {pageNumber}) with a version allocated in tx {value.AllocatedInTransaction}, current write tx is {_txId}");
            Debug.Assert(value.IsRemoved == false, "Set() must not be used to push tombstones");

            ref var slot = ref GetSlotForWrite(pageNumber);
            var head = slot.Head;
            _undo.Add(pageNumber);

            if (head != null && head.AllocatedInTransaction == _txId && head.SurvivesRollback == false)
            {
                // re-modification within the same transaction, the chain holds at most one version per transaction 
                value.Older = head.Older;
                if (head.IsRemoved)
                    _visibleCount++;
                Volatile.Write(ref slot.Head, value);
                return;
            }

            value.Older = head;
            if (head == null || head.IsRemoved)
                _visibleCount++;
            // the Older link must be in place before the node becomes reachable
            Volatile.Write(ref slot.Head, value);
            PruneChain(value);
        }

        public bool Remove(long pageNumber, out PageFromScratchBuffer removed)
        {
            return RemoveInternal(pageNumber, _txId, survivesRollback: false, out removed);
        }

        /// <summary>
        /// Unlike any other change it **survives a rollback**: the matching scratch pool free is applied
        /// immediately and is not undone, so restoring the mapping would resurrect entries pointing
        /// at positions that are back on the free list (RavenDB-27166).
        /// </summary>
        public void RemoveFlushed(long pageNumber)
        {
            RemoveInternal(pageNumber, _txId, survivesRollback: true, out var removed);
            Debug.Assert(removed == null || removed.AllocatedInTransaction != _txId,
                "A journal flush must never remove a version created by the transaction applying it");
        }

        private bool RemoveInternal(long pageNumber, long tombstoneTxId, bool survivesRollback, out PageFromScratchBuffer removed)
        {
            if (TryFindSlot(pageNumber, out var index) == false)
            {
                removed = null;
                return false;
            }

            ref var slot = ref _slots[index];
            var head = slot.Head;
            if (head == null || head.IsRemoved)
            {
                removed = null;
                return false;
            }

            removed = head;
            if (survivesRollback)
                _hasSurvivingRemovalsInCurrentTx = true;
            else
                _undo.Add(pageNumber);
            _visibleCount--;

            if (head.AllocatedInTransaction == _txId)
            {
                var older = head.Older;
                if (older == null || older.IsRemoved)
                {
                    // if we have no older version or it is already a tombstone, we can just drop the head
                    Volatile.Write(ref slot.Head, older);
                    return true;
                }

                var replacement = PageFromScratchBuffer.CreateTombstone(pageNumber, tombstoneTxId, survivesRollback);
                replacement.Older = older;
                Volatile.Write(ref slot.Head, replacement);
                return true;
            }

            var tombstone = PageFromScratchBuffer.CreateTombstone(pageNumber, tombstoneTxId, survivesRollback);
            tombstone.Older = head;
            Volatile.Write(ref slot.Head, tombstone);
            PruneChain(tombstone);
            return true;
        }

        public void RollbackCurrentTransaction()
        {
            if (_hasSurvivingRemovalsInCurrentTx)
            {
                // the flush removals stay, stamped with an id that was never published - remember to
                // compact them into any record published at the unchanged transaction id
                _survivingRemovalsUpToTxId = _txId;
                _hasSurvivingRemovalsInCurrentTx = false;
            }

            var pages = CollectionsMarshal.AsSpan(_undo);
            for (var i = 0; i < pages.Length; i++)
            {
                // a rebuild during the transaction may have dropped a key with a dead chain, nothing to do here
                if (TryFindSlot(pages[i], out var index) == false)
                    continue;

                ref var slot = ref _slots[index];
                var head = slot.Head;
                var restored = head;
                while (restored != null && restored.AllocatedInTransaction == _txId && restored.SurvivesRollback == false)
                    restored = restored.Older;

                if (ReferenceEquals(restored, head))
                    continue;

                var currentLive = head != null && head.IsRemoved == false;
                var restoredLive = restored != null && restored.IsRemoved == false;
                _visibleCount += restoredLive.ToInt32() - currentLive.ToInt32();
                Volatile.Write(ref slot.Head, restored);
            }

            _undo.Clear();
        }

        private void PruneChain(PageFromScratchBuffer head)
        {
            var node = head.Older;
            if (node == null)
                return;

            var prev = head;
            var depth = 1;
            while (node.AllocatedInTransaction > _pruneFloor)
            {
                prev = node;
                node = node.Older;
                depth++;
                if (node == null)
                    return;

                if (depth >= ChainDepthPruneThreshold)
                {
                    // old snapshots are pinning intermediate versions - only the precise prune can
                    // tell which of them are actually observable
                    PruneChainPrecise(head);
                    return;
                }
            }


            if (node.IsRemoved)
            {
                // a tombstone at the end of a chain is indistinguishable from the chain simply ending
                Volatile.Write(ref prev.Older, null);
                return;
            }

            // nothing can see this, we can drop it
            Volatile.Write(ref node.Older, null);
        }

        /// <summary>
        /// This is here to handle the case of a single long running transaction that keeps a snapshot open while the writer churns through many versions of a page. 
        /// For example, a backup running for an hour. We want to prune all the versions that has no active transactions reading that, while not touching the page
        /// version that the backup is reading. More expensive than normal prune, but only called if we detect a long chain of page versions. 
        /// </summary>
        private void PruneChainPrecise(PageFromScratchBuffer head)
        {
            EnsureActiveSnapshotsFetched();

            var keep = head;
            while (keep.Older != null && keep.Older.AllocatedInTransaction > _lastCommittedTxId)
                keep = keep.Older;

            var node = keep.Older;
            var activeSnapshots = CollectionsMarshal.AsSpan(_activeSnapshots);
            var snapshotIndex = activeSnapshots.Length - 1;
            while (node != null)
            {
                if (snapshotIndex < 0 || (node.AllocatedInTransaction > activeSnapshots[snapshotIndex] && node.IsRemoved == false))
                {
                    // no active snapshot can see this version, drop it from the chain
                    node = node.Older;
                    Volatile.Write(ref keep.Older, node);
                    continue;
                }

                // the current snapshot can see this version, let's check older ones
                while (snapshotIndex >= 0 && activeSnapshots[snapshotIndex] >= node.AllocatedInTransaction)
                    snapshotIndex--;

                keep = node;
                node = node.Older;
            }

            TrimTrailingTombstones(head);
        }

        private static void TrimTrailingTombstones(PageFromScratchBuffer head)
        {
            // everything below the last live node is a run of tombstones ending the chain - cutting
            // the whole run in one write keeps every bound absent exactly as the tombstones said
            var lastLive = head;
            for (var node = head.Older; node != null; node = node.Older)
            {
                if (node.IsRemoved == false)
                    lastLive = node;
            }

            Volatile.Write(ref lastLive.Older, null);
        }

        private void EnsureActiveSnapshotsFetched()
        {
            if (_activeSnapshotsFetched)
                return;
            _activeSnapshotsFetched = true;

            _activeSnapshots.Clear();
            foreach (var tx in _activeTransactions.Enumerate())
            {
                var id = tx.Id;
                if (id > _lastCommittedTxId)
                    continue; // covered by the keep-everything-newer-than-committed rule

                _activeSnapshots.Add(id);
            }

            // a reader that started registering after the scan above always snapshots at the latest
            // published id - the sentinel keeps the newest committed version of every chain reachable
            _activeSnapshots.Add(_lastCommittedTxId);

            var unique = Sorting.SortAndRemoveDuplicates(CollectionsMarshal.AsSpan(_activeSnapshots));
            CollectionsMarshal.SetCount(_activeSnapshots, unique);
        }

        private bool TryFindSlot(long pageNumber, out int index)
        {
            var slots = _slots;
            var mask = slots.Length - 1;
            var keyPlusOne = pageNumber + 1;
            var i = GetIndex(slots, pageNumber);
            while (true)
            {
                var k = slots[i].KeyPlusOne;
                if (k == keyPlusOne)
                {
                    index = i;
                    return true;
                }

                if (k == 0)
                {
                    index = -1;
                    return false;
                }

                i = (i + 1) & mask;
            }
        }

        private ref ScratchTableSlot GetSlotForWrite(long pageNumber)
        {
            var keyPlusOne = pageNumber + 1;
            while (true)
            {
                var slots = _slots;
                var mask = slots.Length - 1;
                var i = GetIndex(slots, pageNumber);
                while (true)
                {
                    ref var slot = ref slots[i];
                    var k = slot.KeyPlusOne;
                    if (k == keyPlusOne)
                        return ref slot;

                    if (k == 0)
                    {
                        if (_usedSlots + 1 > slots.Length - (slots.Length >> 2))
                            break; // over 75% full, rebuild to avoid probing too long

                        _usedSlots++;
                        // the head is still null: a reader that sees the key before the first head
                        // write treats the slot as an empty chain, which is correct
                        Volatile.Write(ref slot.KeyPlusOne, keyPlusOne);
                        return ref slot;
                    }

                    i = (i + 1) & mask;
                }

                Rebuild();
            }
        }

        private void Rebuild(bool dropKeysRemovedByCurrentTransaction = false)
        {
            EnsureActiveSnapshotsFetched();

            var oldSlots = _slots;
            var live = 0;
            for (var i = 0; i < oldSlots.Length; i++)
            {
                ref var slot = ref oldSlots[i];
                if (slot.KeyPlusOne == 0 || slot.Head == null)
                    continue;

                if (ShouldDropKey(ref slot, dropKeysRemovedByCurrentTransaction))
                    continue;

                PruneChainPrecise(slot.Head);
                if (IsDeadChain(slot.Head))
                    continue;

                live++;
            }

            // sized on what actually survives - a table that is mostly tombstones after a large flush
            // shrinks here instead of doubling
            var newSize = Math.Max(InitialSize, (int)BitOperations.RoundUpToPowerOf2((uint)(live * 2 + 1)));
            var newSlots = new ScratchTableSlot[newSize];
            var newMask = newSize - 1;
            for (var i = 0; i < oldSlots.Length; i++)
            {
                ref var slot = ref oldSlots[i];
                if (slot.KeyPlusOne == 0 || slot.Head == null || IsDeadChain(slot.Head))
                    continue;

                if (ShouldDropKey(ref slot, dropKeysRemovedByCurrentTransaction))
                    continue;

                var j = GetIndex(newSlots, slot.KeyPlusOne - 1);
                while (newSlots[j].KeyPlusOne != 0)
                    j = (j + 1) & newMask;

                newSlots[j].KeyPlusOne = slot.KeyPlusOne;
                newSlots[j].Head = slot.Head;
            }

            // published snapshots keep referencing the old array; its chains are shared with the new
            // one, and the keys it misses are only ever versions newer than those snapshots anyway
            _slots = newSlots;
            _usedSlots = live;
        }

        private bool ShouldDropKey(ref ScratchTableSlot slot, bool dropKeysRemovedByCurrentTransaction)
        {
            if (dropKeysRemovedByCurrentTransaction == false)
                return false;

            var head = slot.Head;
            if (head.AllocatedInTransaction != _txId)
                return false;

            // a book-keeping transaction cannot have page modifications - its only changes are
            // free-after-flush tombstones
            Debug.Assert(head.IsRemoved, "A transaction publishing at an unchanged id must not own live scratch versions");
            return head.IsRemoved;
        }

        private static bool IsDeadChain(PageFromScratchBuffer head)
        {
            // a lone tombstone reads as "absent" for every snapshot, same as no chain at all
            return head.IsRemoved && head.Older == null;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static int GetIndex(ScratchTableSlot[] slots, long pageNumber)
        {
            // Fibonacci hashing: the constant is 2^64 / golden-ratio (0x9E3779B97F4A7C15) as a signed
            // long. Multiplying by it spreads the mostly-sequential page numbers evenly over the value
            // space, and taking the top log2(length) bits (multiply mixes best) gives a random distribution.
            // Requires the slot array length to be a power of two.
            var shift = 64 - BitOperations.Log2((uint)slots.Length);
            return (int)(unchecked((ulong)(pageNumber * -7046029254386353131L)) >> shift);
        }
    }

    /// <summary>
    /// A consistent point-in-time view over <see cref="ScratchPagesTable"/>: the slot array as
    /// published at a commit plus the transaction id that bounds visibility. Cheap to capture (no
    /// copying) and valid forever - versions it can observe are never pruned while a transaction at
    /// this snapshot id is registered as active.
    /// </summary>
    public readonly struct ScratchPagesSnapshot : IEnumerable<KeyValuePair<long, PageFromScratchBuffer>>
    {
        private readonly ScratchPagesTable.ScratchTableSlot[] _slots;
        public readonly long VisibleAsOfTxId;
        public readonly int Count;

        internal ScratchPagesSnapshot(ScratchPagesTable.ScratchTableSlot[] slots, long visibleAsOfTxId, int count)
        {
            _slots = slots;
            VisibleAsOfTxId = visibleAsOfTxId;
            Count = count;
        }

        public static ScratchPagesSnapshot Empty => new([], 0, 0);

        public bool IsValid => _slots != null;

        public bool TryGetValue(long pageNumber, out PageFromScratchBuffer value)
        {
            if (Count == 0)
            {
                value = null;
                return false;
            }

            var slots = _slots;
            var mask = slots.Length - 1;
            var keyPlusOne = pageNumber + 1;
            var i = ScratchPagesTable.GetIndex(slots, pageNumber);
            while (true)
            {
                ref var slot = ref slots[i];
                var k = Volatile.Read(ref slot.KeyPlusOne);
                if (k == keyPlusOne)
                {
                    var node = Volatile.Read(ref slot.Head);
                    while (node != null && node.AllocatedInTransaction > VisibleAsOfTxId)
                        node = Volatile.Read(ref node.Older);

                    if (node == null || node.IsRemoved)
                        break;

                    value = node;
                    return true;
                }

                if (k == 0)
                    break;

                i = (i + 1) & mask;
            }

            value = null;
            return false;
        }

        public bool ContainsKey(long pageNumber) => TryGetValue(pageNumber, out _);

        public Enumerator GetEnumerator() => new(this);

        IEnumerator<KeyValuePair<long, PageFromScratchBuffer>> IEnumerable<KeyValuePair<long, PageFromScratchBuffer>>.GetEnumerator() => GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public struct Enumerator : IEnumerator<KeyValuePair<long, PageFromScratchBuffer>>
        {
            private readonly ScratchPagesSnapshot _snapshot;
            private int _index;
            private KeyValuePair<long, PageFromScratchBuffer> _current;

            internal Enumerator(ScratchPagesSnapshot snapshot)
            {
                _snapshot = snapshot;
                _index = -1;
                _current = default;
            }

            public bool MoveNext()
            {
                var slots = _snapshot._slots;
                if (slots == null || _snapshot.Count == 0)
                    return false;

                while (++_index < slots.Length)
                {
                    ref var slot = ref slots[_index];
                    if (Volatile.Read(ref slot.KeyPlusOne) == 0)
                        continue;

                    var node = Volatile.Read(ref slot.Head);
                    while (node != null && node.AllocatedInTransaction > _snapshot.VisibleAsOfTxId)
                        node = Volatile.Read(ref node.Older);

                    if (node == null || node.IsRemoved)
                        continue;

                    _current = new KeyValuePair<long, PageFromScratchBuffer>(slot.KeyPlusOne - 1, node);
                    return true;
                }

                return false;
            }

            public KeyValuePair<long, PageFromScratchBuffer> Current => _current;

            object IEnumerator.Current => _current;

            public void Reset()
            {
                _index = -1;
                _current = default;
            }

            public void Dispose()
            {
            }
        }
    }
}
