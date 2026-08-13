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
    /// Versioning is by the table's own publish sequence, not by transaction id: the sequence advances
    /// on every write session and is never reused, so every published state record gets a strictly
    /// higher visibility bound - including book-keeping commits that publish at an unchanged
    /// transaction id (the journal flush-state update). Their free-after-flush removals become visible
    /// exactly on the record that also carries the post-flush data pager state, older records do not see them. 
    /// Transaction ids remain on the live versions (PageFromScratchBuffer.AllocatedInTransaction) as journal 
    /// provenance for the flusher; they play no role in visibility.
    ///
    /// Concurrency model:
    /// - There is a single writer at any time (the write transaction, serialized by the environment's
    ///   write lock). All mutation methods assume that.
    /// - Readers are wait-free. They never observe a version newer than their snapshot because every
    ///   version is stamped with the sequence of the session that created it, and a reader's snapshot
    ///   bound is always a published sequence - strictly older than anything the current writer creates.
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

        // Monotone counter of write sessions - bumped on every BeginWriteTransaction and never reused,
        // whether the session publishes or rolls back. _seq stamps everything the current session
        // creates; _lastPublishedSeq is the bound of the latest published record.
        private long _seqCounter;
        private long _seq;
        private long _lastPublishedSeq;

        // Sorted sequence bounds of the active snapshots (plus the last published sequence as a
        // sentinel for readers that are registering concurrently). Fetched lazily, at most once per
        // write session, the first time a prune needs it; its minimum is the prune floor.
        private readonly List<long> _activeSnapshots = new();
        private bool _activeSnapshotsFetched;

        // Pages whose chains the current transaction touched, in mutation order, possibly with duplicates - to make rollbacks easier
        private readonly List<long> _undo = [];

        public ScratchPagesTable(ActiveTransactions activeTransactions)
        {
            _activeTransactions = activeTransactions;
        }

        public int VisibleCount => _visibleCount;

        public void BeginWriteTransaction(long lastPublishedSeq)
        {
            Debug.Assert(lastPublishedSeq <= _seqCounter, "a published bound cannot come from a session that never began");

            _undo.Clear();
            _seq = ++_seqCounter;
            _lastPublishedSeq = lastPublishedSeq;
            _activeSnapshotsFetched = false;
        }

        /// <summary>
        /// O(1): the snapshot is the live slot array plus this session's sequence as the visibility
        /// bound. Works identically for journal-written and book-keeping commits - the sequence
        /// advanced at BeginWriteTransaction either way, so this record's readers (and only they) see
        /// everything the session did, including free-after-flush removals, atomically with the rest
        /// of the record.
        /// </summary>
        public ScratchPagesSnapshot CaptureSnapshot()
        {
            return new ScratchPagesSnapshot(_slots, _seq, _visibleCount);
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

        /// <summary>
        /// Dead keys (chains that collapsed to nothing, or to a tombstone every snapshot can see) are
        /// reclaimed only by a rebuild, and rebuilds are otherwise triggered only by growth - a burst
        /// of writes that then goes quiet leaves them claimed indefinitely, costing probe length and
        /// held node memory. The environment calls this from idle cleanup, under a write transaction.
        /// May be probed without the write lock (racy but benign); IdleCleanup re-checks under it.
        /// </summary>
        public bool IdleCleanupRequired
        {
            get
            {
                var slots = _slots;
                var usedSlots = _usedSlots;

                // tombstone-headed and empty chains dominate the claimed keys
                var deadKeys = usedSlots - _visibleCount;
                if (deadKeys > usedSlots / 2 && usedSlots > InitialSize / 2)
                    return true;

                // oversized for what is visible - each rebuild steps the array down by one, so
                // repeated idle passes converge
                var targetSize = Math.Max(InitialSize, (int)BitOperations.RoundUpToPowerOf2((uint)(_visibleCount * 2 + 1)));
                return targetSize < slots.Length;
            }
        }

        public void IdleCleanup()
        {
            if (IdleCleanupRequired)
                Rebuild();
        }

        public void Set(long pageNumber, PageFromScratchBuffer value)
        {
            Debug.Assert(value.IsRemoved == false, "Set() must not be used to push tombstones");

            value.Chain.Seq = _seq;
            ref var slot = ref GetSlotForWrite(pageNumber);
            var head = slot.Head;
            _undo.Add(pageNumber);

            if (head != null && head.Chain.Seq == _seq && head.SurvivesRollback == false)
            {
                // re-modification within the same session, the chain holds at most one version per session (except free from flush).
                value.Chain.Older = head.Chain.Older;
                if (head.IsRemoved)
                    _visibleCount++;
                Volatile.Write(ref slot.Head, value);
                return;
            }

            value.Chain.Older = head;
            if (head == null || head.IsRemoved)
                _visibleCount++;
            // the Older link must be in place before the node becomes reachable
            Volatile.Write(ref slot.Head, value);
            PruneChain(value);
        }

        public bool Remove(long pageNumber, out PageFromScratchBuffer removed)
        {
            return RemoveInternal(pageNumber, survivesRollback: false, out removed);
        }

        /// <summary>
        /// Unlike any other change it **survives a rollback**: the matching scratch pool free is applied
        /// immediately and is not undone, so restoring the mapping would resurrect entries pointing
        /// at positions that are back on the free list (RavenDB-27166).
        /// </summary>
        public void RemoveFlushed(long pageNumber)
        {
            RemoveInternal(pageNumber, survivesRollback: true, out var removed);
            Debug.Assert(removed == null || removed.Chain.Seq != _seq,
                "A journal flush must never remove a version created by the session applying it");
        }

        private bool RemoveInternal(long pageNumber, bool survivesRollback, out PageFromScratchBuffer removed)
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
            if (survivesRollback == false)
                _undo.Add(pageNumber);
            _visibleCount--;

            if (head.Chain.Seq == _seq)
            {
                var older = head.Chain.Older;
                if (older == null || older.IsRemoved)
                {
                    // if we have no older version or it is already a tombstone, we can just drop the head
                    Volatile.Write(ref slot.Head, older);
                    return true;
                }

                var replacement = PageFromScratchBuffer.CreateTombstone(pageNumber, _seq, survivesRollback);
                replacement.Chain.Older = older;
                Volatile.Write(ref slot.Head, replacement);
                return true;
            }

            var tombstone = PageFromScratchBuffer.CreateTombstone(pageNumber, _seq, survivesRollback);
            tombstone.Chain.Older = head;
            Volatile.Write(ref slot.Head, tombstone);
            PruneChain(tombstone);
            return true;
        }

        public void RollbackCurrentTransaction()
        {
            var pages = CollectionsMarshal.AsSpan(_undo);
            for (var i = 0; i < pages.Length; i++)
            {
                // a rebuild during the transaction may have dropped a key with a dead chain, nothing to do here
                if (TryFindSlot(pages[i], out var index) == false)
                    continue;

                ref var slot = ref _slots[index];
                var head = slot.Head;
                var restored = head;
                while (restored != null && restored.Chain.Seq == _seq && restored.SurvivesRollback == false)
                    restored = restored.Chain.Older;

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
            var node = head.Chain.Older;
            if (node == null)
                return;

            EnsureActiveSnapshotsFetched();
            var pruneFloor = _activeSnapshots[0];

            var prev = head;
            var depth = 1;
            while (node.Chain.Seq > pruneFloor)
            {
                prev = node;
                node = node.Chain.Older;
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
                Volatile.Write(ref prev.Chain.Older, null);
                return;
            }

            // nothing can see this, we can drop it
            Volatile.Write(ref node.Chain.Older, null);
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
            while (keep.Chain.Older != null && keep.Chain.Older.Chain.Seq > _lastPublishedSeq)
                keep = keep.Chain.Older;

            var node = keep.Chain.Older;
            var activeSnapshots = CollectionsMarshal.AsSpan(_activeSnapshots);
            var snapshotIndex = activeSnapshots.Length - 1;
            while (node != null)
            {
                if (snapshotIndex < 0 || (node.Chain.Seq > activeSnapshots[snapshotIndex] && node.IsRemoved == false))
                {
                    // no active snapshot can see this version, drop it from the chain
                    node = node.Chain.Older;
                    Volatile.Write(ref keep.Chain.Older, node);
                    continue;
                }

                // the current snapshot can see this version, let's check older ones
                while (snapshotIndex >= 0 && activeSnapshots[snapshotIndex] >= node.Chain.Seq)
                    snapshotIndex--;

                keep = node;
                node = node.Chain.Older;
            }

            TrimTrailingTombstones(head);
        }

        private static void TrimTrailingTombstones(PageFromScratchBuffer head)
        {
            // everything below the last live node is a run of tombstones ending the chain - cutting
            // the whole run in one write keeps every bound absent exactly as the tombstones said
            var lastLive = head;
            for (var node = head.Chain.Older; node != null; node = node.Chain.Older)
            {
                if (node.IsRemoved == false)
                    lastLive = node;
            }

            Volatile.Write(ref lastLive.Chain.Older, null);
        }

        private void EnsureActiveSnapshotsFetched()
        {
            if (_activeSnapshotsFetched)
                return;
            _activeSnapshotsFetched = true;

            _activeSnapshots.Clear();
            foreach (var tx in _activeTransactions.Enumerate())
            {
                var seq = tx.ScratchSnapshotSeq;
                if (seq > _lastPublishedSeq)
                    continue; // covered by the keep-everything-newer-than-published rule

                _activeSnapshots.Add(seq);
            }

            // a reader that started registering after the scan above always snapshots at the latest
            // published bound - the sentinel keeps the newest published version of every chain reachable
            _activeSnapshots.Add(_lastPublishedSeq);

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

        private void Rebuild()
        {
            EnsureActiveSnapshotsFetched();

            var oldSlots = _slots;
            var live = 0;
            for (var i = 0; i < oldSlots.Length; i++)
            {
                ref var slot = ref oldSlots[i];
                if (slot.KeyPlusOne == 0 || slot.Head == null)
                    continue;

                PruneChainPrecise(slot.Head);
                if (IsDeadChain(slot.Head))
                    continue;

                live++;
            }

            // sized on what actually survives - a table that is mostly tombstones after a large flush
            // shrinks here instead of doubling, but by at most one step per rebuild to avoid oscillation
            var newSize = Math.Max(InitialSize, (int)BitOperations.RoundUpToPowerOf2((uint)(live * 2 + 1)));
            newSize = Math.Max(newSize, oldSlots.Length / 2);
            var newSlots = new ScratchTableSlot[newSize];
            var newMask = newSize - 1;
            for (var i = 0; i < oldSlots.Length; i++)
            {
                ref var slot = ref oldSlots[i];
                if (slot.KeyPlusOne == 0 || slot.Head == null || IsDeadChain(slot.Head))
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

        private static bool IsDeadChain(PageFromScratchBuffer head)
        {
            // a lone tombstone reads as "absent" for every snapshot, same as no chain at all
            return head.IsRemoved && head.Chain.Older == null;
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
    /// published at a commit plus the publish sequence that bounds visibility. Cheap to capture (no
    /// copying) and valid forever - versions it can observe are never pruned while a transaction at
    /// this snapshot bound is registered as active.
    /// </summary>
    public readonly struct ScratchPagesSnapshot : IEnumerable<KeyValuePair<long, PageFromScratchBuffer>>
    {
        private readonly ScratchPagesTable.ScratchTableSlot[] _slots;
        public readonly long VisibleAsOfSeq;
        public readonly int Count;

        internal ScratchPagesSnapshot(ScratchPagesTable.ScratchTableSlot[] slots, long visibleAsOfSeq, int count)
        {
            _slots = slots;
            VisibleAsOfSeq = visibleAsOfSeq;
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
                    while (node != null && node.Chain.Seq > VisibleAsOfSeq)
                        node = Volatile.Read(ref node.Chain.Older);

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
                    while (node != null && node.Chain.Seq > _snapshot.VisibleAsOfSeq)
                        node = Volatile.Read(ref node.Chain.Older);

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
