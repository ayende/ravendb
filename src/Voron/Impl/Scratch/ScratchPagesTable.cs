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
using Voron.Impl.Paging;
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
    /// Transaction ids remain on the live versions (ScratchEntry.AllocatedInTransaction) as journal
    /// provenance for the flusher; they play no role in visibility.
    ///
    /// Storage model:
    /// - Versions live inline in an entries array and link by index, so no version is an object and the
    ///   array holds no references for the GC to trace. The two references a version needs - the scratch
    ///   file and the pager state mapping it - sit in a small refs array, one per scratch file growth.
    /// - The three arrays form one generation, replaced together by <see cref="Rebuild"/> and append-only
    ///   in between. An index is therefore never repurposed while a generation is published, which is what
    ///   lets readers follow indices with no interlocking: there is no recycled slot for them to land on.
    /// - A snapshot captures the whole generation, so a published generation stays reachable exactly while
    ///   some reader may still resolve a version in it, and the GC frees it afterwards - including the
    ///   pager states, which unmap from their finalizers.
    ///
    /// Concurrency model:
    /// - There is a single writer at any time (the write transaction, serialized by the environment's
    ///   write lock). All mutation methods assume that.
    /// - Readers are wait-free. They never observe a version newer than their snapshot because every
    ///   version is stamped with the sequence of the session that created it, and a reader's snapshot
    ///   bound is always a published sequence - strictly older than anything the current writer creates.
    /// - Versions become unreachable only when no active or future snapshot can observe them
    ///   (see PruneChain / PruneChainPrecise), so unlinking is safe while readers traverse. Unlinking only
    ///   ever removes versions a reader was already not permitted to see, so a reader holding an older
    ///   generation that misses the unlink stays correct.
    /// </summary>
    public sealed class ScratchPagesTable
    {
        internal struct ScratchTableSlot
        {
            // 0 marks an empty slot, so we store PageNumberInDataFile + 1 (page 0 is a valid key).
            // A slot never changes its key while readers may be probing the array.
            internal long KeyPlusOne;

            // Head of the version chain, newest first, as an index into the generation's entries array.
            // -1 is an empty chain. The head is always the version that the writer sees as current.
            internal int HeadIndex;
        }

        internal const int NoEntry = -1;

        // Stamped on a freed entry so that a reader landing on one is detectable rather than silently
        // reading whatever the slot was recycled into. No real version can carry it: sequences start at 1.
        internal const long FreeSeq = long.MinValue;

        // Arrays at or above this go straight to the large object heap, which is gen2 from birth and is
        // never compacted - so they are not copied by a collection. Below it they start in gen0 and get
        // copied on every collection until they age out, and at this size that copy is the pause.
        private const int LohThresholdBytes = 85_000;

        private static readonly int MinSlots = BitOperations.RoundUpToPowerOf2(
            (uint)(LohThresholdBytes / Unsafe.SizeOf<ScratchTableSlot>()) + 1) is var s && s > 1024 ? (int)s : 1024;

        private static readonly int MinEntries = BitOperations.RoundUpToPowerOf2(
            (uint)(LohThresholdBytes / Unsafe.SizeOf<ScratchEntry>()) + 1) is var e && e > 1024 ? (int)e : 1024;

        private const int InitialRefs = 16;

        // A longer chain would require a the more expensive precise prune.
        private const int ChainDepthPruneThreshold = 8;

        private readonly ActiveTransactions _activeTransactions;

        private ScratchTableSlot[] _slots = NewSlots(MinSlots);
        private int _usedSlots;

        private ScratchEntry[] _entries = new ScratchEntry[MinEntries];
        private int _usedEntries;

        // Entries unlinked from their chains, threaded through OlderIndex. Recycling is immediate because
        // an entry is only ever unlinked once no active or future snapshot can observe it: pruning keeps
        // the newest version at or below every active bound, which is exactly where a reader's descent
        // stops, so nothing can be walking through what we unlink.
        private int _freeHead = NoEntry;
        private int _freeEntries;

        private ScratchRef[] _refs = new ScratchRef[InitialRefs];
        private int _usedRefs;

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

        // The latest floor any prune has used - floors never decrease (every active snapshot was either
        // counted in the previous minimum or opened at a later published bound), so this is also the
        // highest, making the assertion against it the strongest. Only written by the write session, read
        // by read transactions created concurrently, and only to assert the reader-never-below-floor invariant.
        private long _prunedUpToSeq;

        // Pages whose chains the current transaction touched, in mutation order, possibly with duplicates - to make rollbacks easier
        private readonly List<long> _undo = [];

        // Generations a rebuild replaced, tagged with the sequence at which they stopped being current.
        // A snapshot only ever holds the generation that was current when it was taken, so once the prune
        // floor passes that tag no reader can be holding these and the arrays can be handed out again
        // rather than reallocated - at steady state the entries array is tens of megabytes, and rebuilding
        // it from scratch each time is exactly the kind of allocation we are trying to stop making.
        private readonly List<(long RetiredAtSeq, ScratchTableSlot[] Slots, ScratchEntry[] Entries)> _retiredGenerations = [];
        private readonly List<ScratchTableSlot[]> _slotPool = [];
        private readonly List<ScratchEntry[]> _entryPool = [];

        public ScratchPagesTable(ActiveTransactions activeTransactions)
        {
            _activeTransactions = activeTransactions;
        }

        public int VisibleCount => _visibleCount;

        private static ScratchTableSlot[] NewSlots(int size)
        {
            var slots = new ScratchTableSlot[size];
            for (var i = 0; i < slots.Length; i++)
                slots[i].HeadIndex = NoEntry;
            return slots;
        }

        public void BeginWriteTransaction(long lastPublishedSeq)
        {
            Debug.Assert(lastPublishedSeq <= _seqCounter, "a published bound cannot come from a session that never began");

            _undo.Clear();
            _seq = ++_seqCounter;
            _lastPublishedSeq = lastPublishedSeq;
            _activeSnapshotsFetched = false;
        }

        /// <summary>
        /// Pruning drops versions that no active snapshot can observe, and it decides that from a racy
        /// enumeration of the active transactions plus the last published sequence as a sentinel. That is
        /// only sound because a reader always snapshots at the latest published record, so it can never
        /// adopt a sequence below the floor we prune against. This asserts that invariant where readers
        /// take their snapshot: if a path ever hands a reader an older sequence, pruning silently starts
        /// dropping versions that reader still needs and it reads stale pages from the data file.
        /// </summary>
        [Conditional("DEBUG")]
        internal void AssertReaderSnapshotIsNotBelowPruneFloor(long snapshotSeq)
        {
            // read transactions are created without the write lock, so this reads a floor that a write
            // session may be advancing concurrently - volatile keeps it a coherent value rather than a torn one
            var floor = Volatile.Read(ref _prunedUpToSeq);

            Debug.Assert(snapshotSeq >= floor,
                $"A read transaction adopted scratch snapshot sequence {snapshotSeq}, below the prune floor {floor}. " +
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

                var head = slot.HeadIndex;
                if (head != NoEntry && _entries[head].IsRemoved == false)
                    actual++;
            }

            Debug.Assert(actual == _visibleCount,
                $"Scratch pages table reports {_visibleCount} visible pages but the slots hold {actual}");
        }

        /// <summary>
        /// O(1): the snapshot is the live generation plus this session's sequence as the visibility
        /// bound. Works identically for journal-written and book-keeping commits - the sequence
        /// advanced at BeginWriteTransaction either way, so this record's readers (and only they) see
        /// everything the session did, including free-after-flush removals, atomically with the rest
        /// of the record.
        /// </summary>
        public ScratchPagesSnapshot CaptureSnapshot()
        {
            AssertVisibleCountMatches();
            return new ScratchPagesSnapshot(_slots, _entries, _refs, _seq, _visibleCount);
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
                    var head = slot.HeadIndex;
                    if (head == NoEntry || _entries[head].IsRemoved)
                        break;
                    value = Materialize(_entries, _refs, head);
                    return true;
                }

                if (k == 0)
                    break;

                i = (i + 1) & mask;
            }

            value = default;
            return false;
        }

        public bool ContainsKey(long pageNumber) => TryGetValue(pageNumber, out _);

        internal static PageFromScratchBuffer Materialize(ScratchEntry[] entries, ScratchRef[] refs, int index)
        {
            ref var entry = ref entries[index];
            var refIndex = entry.RefIndex;
            if (refIndex < 0)
                return default; // tombstone - reads as absent

            ref var scratchRef = ref refs[refIndex];
            return new PageFromScratchBuffer(
                scratchRef.File,
                scratchRef.State,
                entry.AllocatedInTransaction,
                entry.PositionInScratchBuffer,
                entry.PageNumberInDataFile,
                entry.PreviousVersion,
                entry.Size,
                entry.NumberOfPages);
        }

        /// <summary>
        /// Dead keys (chains that collapsed to nothing, or to a tombstone every snapshot can see) are
        /// reclaimed only by a rebuild, and rebuilds are otherwise triggered only by growth - a burst
        /// of writes that then goes quiet leaves them claimed indefinitely, costing probe length and
        /// held entry memory. The environment calls this from idle cleanup, under a write transaction.
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
                if (deadKeys > usedSlots / 2 && usedSlots > MinSlots / 2)
                    return true;

                // dead versions accumulate in the entries array between rebuilds even when the slot
                // count is stable - a page rewritten over and over claims a fresh entry each time
                if (_usedEntries - _freeEntries > Math.Max(MinEntries, _visibleCount * 4))
                    return true;

                // oversized for what is visible - each rebuild steps the array down by one, so
                // repeated idle passes converge
                var targetSize = Math.Max(MinSlots, (int)BitOperations.RoundUpToPowerOf2((uint)(_visibleCount * 2 + 1)));
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

            EnsureRoomForOneMoreEntry();
            ref var slotForWrite = ref GetSlotForWrite(pageNumber);
            var entryIndex = AllocateEntry();
            var head = slotForWrite.HeadIndex;
            _undo.Add(pageNumber);

            var refIndex = GetOrAddRef(value.File, value.State);

            ref var entry = ref _entries[entryIndex];
            entry.PositionInScratchBuffer = value.PositionInScratchBuffer;
            entry.PageNumberInDataFile = value.PageNumberInDataFile;
            entry.AllocatedInTransaction = value.AllocatedInTransaction;
            entry.Size = value.Size;
            entry.PreviousVersion = value.PreviousVersion;
            entry.NumberOfPages = value.NumberOfPages;
            entry.RefIndex = refIndex;
            entry.Seq = _seq;

            if (head != NoEntry && _entries[head].Seq == _seq && SurvivesRollback(head) == false)
            {
                // re-modification within the same session, the chain holds at most one version per session (except free from flush).
                entry.OlderIndex = _entries[head].OlderIndex;
                if (_entries[head].IsRemoved)
                    _visibleCount++;
                Volatile.Write(ref slotForWrite.HeadIndex, entryIndex);
                FreeEntry(head); // superseded within the session, so it was never published
                return;
            }

            entry.OlderIndex = head;
            if (head == NoEntry || _entries[head].IsRemoved)
                _visibleCount++;
            // the Older link must be in place before the version becomes reachable
            Volatile.Write(ref slotForWrite.HeadIndex, entryIndex);
            PruneChain(entryIndex);
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
            RemoveInternal(pageNumber, survivesRollback: true, out _);
        }

        private bool RemoveInternal(long pageNumber, bool survivesRollback, out PageFromScratchBuffer removed)
        {
            // may push a tombstone, so reserve before the slot is resolved
            EnsureRoomForOneMoreEntry();

            if (TryFindSlot(pageNumber, out var index) == false)
            {
                removed = default;
                return false;
            }

            ref var slot = ref _slots[index];
            var head = slot.HeadIndex;
            if (head == NoEntry || _entries[head].IsRemoved)
            {
                removed = default;
                return false;
            }

            Debug.Assert(survivesRollback == false || _entries[head].Seq != _seq,
                "A journal flush must never remove a version created by the session applying it");

            removed = Materialize(_entries, _refs, head);
            if (survivesRollback == false)
                _undo.Add(pageNumber);
            _visibleCount--;

            if (_entries[head].Seq == _seq)
            {
                var older = _entries[head].OlderIndex;
                if (older == NoEntry || _entries[older].IsRemoved)
                {
                    // if we have no older version or it is already a tombstone, we can just drop the head
                    Volatile.Write(ref slot.HeadIndex, older);
                    FreeEntry(head); // created by this session, never published
                    return true;
                }

                var replacement = CreateTombstone(pageNumber, survivesRollback, older);
                Volatile.Write(ref slot.HeadIndex, replacement);
                return true;
            }

            var tombstone = CreateTombstone(pageNumber, survivesRollback, head);
            Volatile.Write(ref slot.HeadIndex, tombstone);
            PruneChain(tombstone);
            return true;
        }

        private int CreateTombstone(long pageNumberInDataFile, bool survivesRollback, int olderIndex)
        {
            var index = AllocateEntry();
            ref var entry = ref _entries[index];
            entry = default;
            entry.PageNumberInDataFile = pageNumberInDataFile;
            entry.AllocatedInTransaction = survivesRollback
                ? PageFromScratchBuffer.SurvivingTombstoneTx
                : PageFromScratchBuffer.TombstoneTx;
            entry.PositionInScratchBuffer = -1;
            entry.RefIndex = NoEntry;
            entry.OlderIndex = olderIndex;
            entry.Seq = _seq;
            return index;
        }

        private bool SurvivesRollback(int index) =>
            _entries[index].AllocatedInTransaction == PageFromScratchBuffer.SurvivingTombstoneTx;

        public void RollbackCurrentTransaction()
        {
            var pages = CollectionsMarshal.AsSpan(_undo);
            for (var i = 0; i < pages.Length; i++)
            {
                // a rebuild during the transaction may have dropped a key with a dead chain, nothing to do here
                if (TryFindSlot(pages[i], out var index) == false)
                    continue;

                ref var slot = ref _slots[index];
                var head = slot.HeadIndex;
                var restored = head;
                while (restored != NoEntry && _entries[restored].Seq == _seq && SurvivesRollback(restored) == false)
                    restored = _entries[restored].OlderIndex;

                if (restored == head)
                    continue;

                var currentLive = head != NoEntry && _entries[head].IsRemoved == false;
                var restoredLive = restored != NoEntry && _entries[restored].IsRemoved == false;
                _visibleCount += restoredLive.ToInt32() - currentLive.ToInt32();
                Volatile.Write(ref slot.HeadIndex, restored);

                // everything between the old head and what we restored belongs to this session, so it was
                // never published and nothing can be reading it
                for (var node = head; node != restored; )
                {
                    var next = _entries[node].OlderIndex;
                    FreeEntry(node);
                    node = next;
                }
            }

            _undo.Clear();
        }

        private void PruneChain(int headIndex)
        {
            var node = _entries[headIndex].OlderIndex;
            if (node == NoEntry)
                return;

            EnsureActiveSnapshotsFetched();
            var pruneFloor = _activeSnapshots[0];

            var prev = headIndex;
            var depth = 1;
            while (_entries[node].Seq > pruneFloor)
            {
                prev = node;
                node = _entries[node].OlderIndex;
                depth++;
                if (node == NoEntry)
                    return;

                if (depth >= ChainDepthPruneThreshold)
                {
                    // old snapshots are pinning intermediate versions - only the precise prune can
                    // tell which of them are actually observable
                    PruneChainPrecise(headIndex);
                    return;
                }
            }

            if (_entries[node].IsRemoved)
            {
                // a tombstone at the end of a chain is indistinguishable from the chain simply ending
                Volatile.Write(ref _entries[prev].OlderIndex, NoEntry);
                FreeChainFrom(node);
                return;
            }

            // nothing can see this, we can drop it
            var stranded = _entries[node].OlderIndex;
            Volatile.Write(ref _entries[node].OlderIndex, NoEntry);
            FreeChainFrom(stranded);
        }

        /// <summary>
        /// This is here to handle the case of a single long running transaction that keeps a snapshot open while the writer churns through many versions of a page.
        /// For example, a backup running for an hour. We want to prune all the versions that has no active transactions reading that, while not touching the page
        /// version that the backup is reading. More expensive than normal prune, but only called if we detect a long chain of page versions.
        /// </summary>
        private void PruneChainPrecise(int headIndex)
        {
            EnsureActiveSnapshotsFetched();

            var keep = headIndex;
            while (_entries[keep].OlderIndex != NoEntry && _entries[_entries[keep].OlderIndex].Seq > _lastPublishedSeq)
                keep = _entries[keep].OlderIndex;

            var node = _entries[keep].OlderIndex;
            var activeSnapshots = CollectionsMarshal.AsSpan(_activeSnapshots);
            var snapshotIndex = activeSnapshots.Length - 1;
            while (node != NoEntry)
            {
                if (snapshotIndex < 0 || (_entries[node].Seq > activeSnapshots[snapshotIndex] && _entries[node].IsRemoved == false))
                {
                    // no active snapshot can see this version, drop it from the chain
                    var dropped = node;
                    node = _entries[node].OlderIndex;
                    Volatile.Write(ref _entries[keep].OlderIndex, node);
                    FreeEntry(dropped);
                    continue;
                }

                // the current snapshot can see this version, let's check older ones
                while (snapshotIndex >= 0 && activeSnapshots[snapshotIndex] >= _entries[node].Seq)
                    snapshotIndex--;

                keep = node;
                node = _entries[node].OlderIndex;
            }

            TrimTrailingTombstones(headIndex);
        }

        private void TrimTrailingTombstones(int headIndex)
        {
            // everything below the last live version is a run of tombstones ending the chain - cutting
            // the whole run in one write keeps every bound absent exactly as the tombstones said
            var lastLive = headIndex;
            for (var node = _entries[headIndex].OlderIndex; node != NoEntry; node = _entries[node].OlderIndex)
            {
                if (_entries[node].IsRemoved == false)
                    lastLive = node;
            }

            var stranded = _entries[lastLive].OlderIndex;
            Volatile.Write(ref _entries[lastLive].OlderIndex, NoEntry);
            FreeChainFrom(stranded);
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

            Volatile.Write(ref _prunedUpToSeq, _activeSnapshots[0]);

            ReclaimRetiredGenerations(_activeSnapshots[0]);
        }

        private void ReclaimRetiredGenerations(long floor)
        {
            for (var i = _retiredGenerations.Count - 1; i >= 0; i--)
            {
                var retired = _retiredGenerations[i];
                if (retired.RetiredAtSeq > floor)
                    continue; // a reader may still hold this generation

                _slotPool.Add(retired.Slots);
                _entryPool.Add(retired.Entries);
                _retiredGenerations.RemoveAt(i);
            }
        }

        private ScratchTableSlot[] RentSlots(int size)
        {
            for (var i = 0; i < _slotPool.Count; i++)
            {
                if (_slotPool[i].Length != size)
                    continue;

                var slots = _slotPool[i];
                _slotPool.RemoveAt(i);
                Array.Clear(slots);
                for (var j = 0; j < slots.Length; j++)
                    slots[j].HeadIndex = NoEntry;
                return slots;
            }

            return NewSlots(size);
        }

        private ScratchEntry[] RentEntries(int size)
        {
            for (var i = 0; i < _entryPool.Count; i++)
            {
                if (_entryPool[i].Length != size)
                    continue;

                var entries = _entryPool[i];
                _entryPool.RemoveAt(i);
                return entries; // every slot is written before it is read, so no clearing needed
            }

            return new ScratchEntry[size];
        }

        /// <summary>
        /// Entries are append-only within a generation: an index handed to a reader is never repurposed
        /// while that generation is published, which is what makes index-following safe without any
        /// interlocking. Reclamation happens by rebuilding into a fresh generation, so running out of
        /// entries triggers one exactly the way a full slot array does.
        /// </summary>
        /// <summary>
        /// Must be called before anything is resolved against the current generation. A rebuild renumbers
        /// every entry and publishes a new slot array, so a slot ref or a head index taken beforehand
        /// would silently refer to the previous generation.
        /// </summary>
        private void EnsureRoomForOneMoreEntry()
        {
            if (_freeHead == NoEntry && _usedEntries == _entries.Length)
                Rebuild();
        }

        private int AllocateEntry()
        {
            if (_freeHead != NoEntry)
            {
                var recycled = _freeHead;
                _freeHead = _entries[recycled].OlderIndex;
                _freeEntries--;
                return recycled;
            }

            // GetSlotForWrite may rebuild, and a rebuild always sizes the entries array above what
            // survives, so there is room here whether or not one happened
            Debug.Assert(_usedEntries < _entries.Length, "EnsureRoomForOneMoreEntry must run before the slot is resolved");
            return _usedEntries++;
        }

        /// <summary>
        /// Returns an unlinked entry to the free list. Callers must publish the unlink first: the entry is
        /// stamped free here, and a reader that could still reach it would then see the stamp instead of a
        /// version.
        /// </summary>
        private void FreeEntry(int index)
        {
            ref var entry = ref _entries[index];
            entry.RefIndex = NoEntry;
            entry.Seq = FreeSeq;
            entry.OlderIndex = _freeHead;
            _freeHead = index;
            _freeEntries++;
        }

        private void FreeChainFrom(int index)
        {
            while (index != NoEntry)
            {
                var next = _entries[index].OlderIndex;
                FreeEntry(index);
                index = next;
            }
        }

        private int GetOrAddRef(ScratchBufferFile file, Pager.State state)
        {
            // one of these per scratch file growth, so a linear scan over tens of slots
            for (var i = 0; i < _usedRefs; i++)
            {
                if (ReferenceEquals(_refs[i].File, file) && ReferenceEquals(_refs[i].State, state))
                    return i;
            }

            if (_usedRefs == _refs.Length)
            {
                // prefix preserving growth: an entry an older snapshot can see keeps resolving to the
                // same slot in the older, shorter array it captured
                var grown = new ScratchRef[_refs.Length * 2];
                Array.Copy(_refs, grown, _usedRefs);
                _refs = grown;
            }

            _refs[_usedRefs].File = file;
            _refs[_usedRefs].State = state;
            return _usedRefs++;
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
                        // the head is still empty: a reader that sees the key before the first head
                        // write treats the slot as an empty chain, which is correct
                        Volatile.Write(ref slot.KeyPlusOne, keyPlusOne);
                        return ref slot;
                    }

                    i = (i + 1) & mask;
                }

                Rebuild();
            }
        }

        /// <summary>
        /// Publishes a fresh generation holding only what survives, renumbering entry and ref indices.
        /// The three arrays are replaced together so that a snapshot never mixes generations: an index
        /// only ever means anything relative to the arrays it was captured with.
        ///
        /// Published snapshots keep referencing the old generation. The chains it holds are copies rather
        /// than shared state now, which is fine - the versions an older snapshot may observe are exactly
        /// the ones copied forward unchanged, and the ones it may not are the newer ones it never reaches.
        /// </summary>
        private void Rebuild()
        {
            EnsureActiveSnapshotsFetched();

            var oldSlots = _slots;
            var oldEntries = _entries;
            var live = 0;
            var liveEntries = 0;
            for (var i = 0; i < oldSlots.Length; i++)
            {
                ref var slot = ref oldSlots[i];
                if (slot.KeyPlusOne == 0 || slot.HeadIndex == NoEntry)
                    continue;

                PruneChainPrecise(slot.HeadIndex);
                if (IsDeadChain(slot.HeadIndex))
                    continue;

                live++;
                for (var node = slot.HeadIndex; node != NoEntry; node = oldEntries[node].OlderIndex)
                    liveEntries++;
            }

            // sized on what actually survives - a table that is mostly tombstones after a large flush
            // shrinks here instead of doubling, but by at most one step per rebuild to avoid oscillation
            var newSize = Math.Max(MinSlots, (int)BitOperations.RoundUpToPowerOf2((uint)(live * 2 + 1)));
            newSize = Math.Max(newSize, oldSlots.Length / 2);
            var newSlots = RentSlots(newSize);
            var newMask = newSize - 1;

            // room to grow before the next rebuild, and never smaller than what is live
            var newEntriesSize = Math.Max(MinEntries, (int)BitOperations.RoundUpToPowerOf2((uint)(liveEntries * 2 + 1)));
            var newEntries = RentEntries(newEntriesSize);
            var newRefs = new ScratchRef[_refs.Length];
            var refMap = new int[_usedRefs];
            for (var i = 0; i < refMap.Length; i++)
                refMap[i] = NoEntry;

            var usedEntries = 0;
            var usedRefs = 0;

            for (var i = 0; i < oldSlots.Length; i++)
            {
                ref var slot = ref oldSlots[i];
                if (slot.KeyPlusOne == 0 || slot.HeadIndex == NoEntry || IsDeadChain(slot.HeadIndex))
                    continue;

                var j = GetIndex(newSlots, slot.KeyPlusOne - 1);
                while (newSlots[j].KeyPlusOne != 0)
                    j = (j + 1) & newMask;

                newSlots[j].KeyPlusOne = slot.KeyPlusOne;

                // copy the chain, newest first, fixing up the links as we go
                var previous = NoEntry;
                for (var node = slot.HeadIndex; node != NoEntry; node = oldEntries[node].OlderIndex)
                {
                    var target = usedEntries++;
                    newEntries[target] = oldEntries[node];
                    newEntries[target].OlderIndex = NoEntry;

                    var refIndex = oldEntries[node].RefIndex;
                    if (refIndex >= 0)
                    {
                        if (refMap[refIndex] == NoEntry)
                        {
                            refMap[refIndex] = usedRefs;
                            newRefs[usedRefs] = _refs[refIndex];
                            usedRefs++;
                        }

                        newEntries[target].RefIndex = refMap[refIndex];
                    }

                    if (previous == NoEntry)
                        newSlots[j].HeadIndex = target;
                    else
                        newEntries[previous].OlderIndex = target;

                    previous = target;
                }
            }

            // the generation we are leaving is still reachable through any snapshot taken against it
            _retiredGenerations.Add((_seq, oldSlots, oldEntries));

            _slots = newSlots;
            _entries = newEntries;
            _refs = newRefs;
            _usedSlots = live;
            _usedEntries = usedEntries;
            _usedRefs = usedRefs;
            _freeHead = NoEntry;
            _freeEntries = 0;
        }

        private bool IsDeadChain(int headIndex)
        {
            // a lone tombstone reads as "absent" for every snapshot, same as no chain at all
            return _entries[headIndex].IsRemoved && _entries[headIndex].OlderIndex == NoEntry;
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
    /// A consistent point-in-time view over <see cref="ScratchPagesTable"/>: one generation of the table
    /// as published at a commit, plus the publish sequence that bounds visibility. Cheap to capture (no
    /// copying) and valid forever - versions it can observe are never pruned while a transaction at
    /// this snapshot bound is registered as active, and the generation it holds is never renumbered.
    /// </summary>
    public readonly struct ScratchPagesSnapshot : IEnumerable<KeyValuePair<long, PageFromScratchBuffer>>
    {
        private readonly ScratchPagesTable.ScratchTableSlot[] _slots;
        private readonly ScratchEntry[] _entries;
        private readonly ScratchRef[] _refs;
        public readonly long VisibleAsOfSeq;
        public readonly int Count;

        internal ScratchPagesSnapshot(ScratchPagesTable.ScratchTableSlot[] slots, ScratchEntry[] entries, ScratchRef[] refs, long visibleAsOfSeq, int count)
        {
            _slots = slots;
            _entries = entries;
            _refs = refs;
            VisibleAsOfSeq = visibleAsOfSeq;
            Count = count;
        }

        public static ScratchPagesSnapshot Empty => new([], [], [], 0, 0);

        public bool IsValid => _slots != null;

        public bool TryGetValue(long pageNumber, out PageFromScratchBuffer value)
        {
            if (Count == 0)
            {
                value = default;
                return false;
            }

            var slots = _slots;
            var entries = _entries;
            var mask = slots.Length - 1;
            var keyPlusOne = pageNumber + 1;
            var i = ScratchPagesTable.GetIndex(slots, pageNumber);
            while (true)
            {
                ref var slot = ref slots[i];
                var k = Volatile.Read(ref slot.KeyPlusOne);
                if (k == keyPlusOne)
                {
                    var node = Volatile.Read(ref slot.HeadIndex);
                    while (node != ScratchPagesTable.NoEntry && entries[node].Seq > VisibleAsOfSeq)
                    {
                        AssertNotFreed(entries, node);
                        node = Volatile.Read(ref entries[node].OlderIndex);
                    }
                    if (node != ScratchPagesTable.NoEntry)
                        AssertNotFreed(entries, node);

                    if (node == ScratchPagesTable.NoEntry || entries[node].IsRemoved)
                        break;

                    value = ScratchPagesTable.Materialize(entries, _refs, node);
                    return true;
                }

                if (k == 0)
                    break;

                i = (i + 1) & mask;
            }

            value = default;
            return false;
        }

        public bool ContainsKey(long pageNumber) => TryGetValue(pageNumber, out _);

        /// <summary>
        /// Recycling an entry a reader could still reach would hand it a different page's location, and the
        /// wrong page reads as plausible data rather than failing. This turns that into a loud failure.
        /// </summary>
        [Conditional("DEBUG")]
        private static void AssertNotFreed(ScratchEntry[] entries, int index)
        {
            Debug.Assert(entries[index].Seq != ScratchPagesTable.FreeSeq,
                $"A snapshot traversal reached entry {index}, which is on the free list. " +
                "It was recycled while still reachable.");
        }

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

                var entries = _snapshot._entries;
                while (++_index < slots.Length)
                {
                    ref var slot = ref slots[_index];
                    if (Volatile.Read(ref slot.KeyPlusOne) == 0)
                        continue;

                    var node = Volatile.Read(ref slot.HeadIndex);
                    while (node != ScratchPagesTable.NoEntry && entries[node].Seq > _snapshot.VisibleAsOfSeq)
                    {
                        AssertNotFreed(entries, node);
                        node = Volatile.Read(ref entries[node].OlderIndex);
                    }
                    if (node != ScratchPagesTable.NoEntry)
                        AssertNotFreed(entries, node);

                    if (node == ScratchPagesTable.NoEntry || entries[node].IsRemoved)
                        continue;

                    _current = new KeyValuePair<long, PageFromScratchBuffer>(
                        slot.KeyPlusOne - 1,
                        ScratchPagesTable.Materialize(entries, _snapshot._refs, node));
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
