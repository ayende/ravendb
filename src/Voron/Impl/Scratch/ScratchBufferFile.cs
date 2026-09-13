using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using Sparrow.Threading;
using Voron.Global;
using Voron.Impl.Paging;
using Sparrow.Server.Utils;
using System.Diagnostics.CodeAnalysis;
using Sparrow.Binary;
using Sparrow.Server.LowMemory;

namespace Voron.Impl.Scratch
{
    public sealed class ScratchBufferFile : IDisposable
    {
        private sealed class PendingPage
        {
            public long Page;
            public long ValidAfterTransactionId;
            public long AllocatedInTransaction;
        }

        private readonly Pager _scratchPager;
        private Pager.State _scratchPagerState;
        private readonly int _scratchNumber;

        private readonly Dictionary<long, LinkedList<PendingPage>> _freePagesBySize = new();
        private readonly Dictionary<long, PageFromScratchBuffer> _allocatedPages = new();
        private readonly DisposeOnce<SingleAttempt> _disposeOnceRunner;

        private ScratchPageOwners _owners;

        private long _allocatedPagesCount;
        private long _lastUsedPage;
        private long _txIdAfterWhichLatestFreePagesBecomeAvailable = -1;
        private StrongReference<Func<long>> _strongRefToAllocateInBytesFunc;

        public long LastUsedPage => _lastUsedPage;

        public ScratchBufferFile(Pager scratchPager,  Pager.State scratchPagerState, int scratchNumber)
        {
            _scratchPager = scratchPager;
            _scratchPagerState = scratchPagerState;
            _scratchNumber = scratchNumber;
            _allocatedPagesCount = 0;

            _strongRefToAllocateInBytesFunc = new StrongReference<Func<long>>
            {
                Value = () => AllocatedPagesCount * Constants.Storage.PageSize
            };
            MemoryInformation.DirtyMemoryObjects.TryAdd(_strongRefToAllocateInBytesFunc);

            DebugInfo = new ScratchFileDebugInfo(this);

            _disposeOnceRunner = new DisposeOnce<SingleAttempt>(DisposeImpl);
        }

        private void DisposeImpl()
        {
            _strongRefToAllocateInBytesFunc.Value = null; // remove ref (so if there's a left over refs in DirtyMemoryObjects but also function as _disposed = true for racy func invoke)
            MemoryInformation.DirtyMemoryObjects.TryRemove(_strongRefToAllocateInBytesFunc);
            _strongRefToAllocateInBytesFunc = null;

            _scratchPager.Dispose();
            ClearDictionaries();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void ClearDictionaries()
        {
            _allocatedPages.Clear();
            _freePagesBySize.Clear();
            _owners.UntrackAll();
        }

        public void Reset()
        {
            _scratchPager.DiscardWholeFile(_scratchPagerState);

            ClearDictionaries();
            _txIdAfterWhichLatestFreePagesBecomeAvailable = -1;
            _lastUsedPage = 0;
            _allocatedPagesCount = 0;

            DebugInfo.NumberOfResets++;
            DebugInfo.LastResetTime = DateTime.UtcNow;
        }

        internal (Pager, Pager.State) GetPagerAndState() => (_scratchPager, _scratchPagerState);
        
        public Pager Pager => _scratchPager;

        public int Number => _scratchNumber;

        public int NumberOfAllocations => _allocatedPages.Count;

        public long Size => _scratchPagerState.NumberOfAllocatedPages * Constants.Storage.PageSize;

        public long NumberOfAllocatedPages => _scratchPagerState.NumberOfAllocatedPages;

        public long AllocatedPagesCount => _allocatedPagesCount;

        public long TxIdAfterWhichLatestFreePagesBecomeAvailable => _txIdAfterWhichLatestFreePagesBecomeAvailable;

        public ScratchFileDebugInfo DebugInfo { get; }

        public PageFromScratchBuffer Allocate(LowLevelTransaction tx, int numberOfPages, int sizeToAllocate, long pageNumber, Page previousVersion)
        {
            _scratchPager.EnsureContinuous(ref _scratchPagerState, _lastUsedPage, sizeToAllocate);
            
            var result = new PageFromScratchBuffer(this,_scratchPagerState, tx.Id, _lastUsedPage, pageNumber, previousVersion, sizeToAllocate, numberOfPages);

            _allocatedPagesCount += numberOfPages;
            AddAllocatedPage(_lastUsedPage, result);
            _lastUsedPage += sizeToAllocate;

            return result;
        }

        public bool TryGettingFromAllocatedBuffer(LowLevelTransaction tx, int numberOfPages, int size, long pageNumber, Page previousVersion, out PageFromScratchBuffer result)
        {
            result = default;

            if (_freePagesBySize.TryGetValue(size, out LinkedList<PendingPage> list) == false || list.Count <= 0)
                return false;

            var val = list.Last!.Value;

            if (val.ValidAfterTransactionId >= tx.Environment.PossibleOldestReadTransaction(tx))
                return false;

            list.RemoveLast();

#if VALIDATE
            byte* freePageBySizePointer = _scratchPager.AcquirePagePointer(tx, val.Page, PagerState);
            ulong freePageBySizeSize = (ulong)size * Constants.Storage.PageSize;
            // This has to be forced, as the list of available pages should be protected by default, but this
            // is a policy we implement inside the ScratchBufferFile only.
            _scratchPager.UnprotectPageRange(freePageBySizePointer, freePageBySizeSize, true);
#endif

            result = new PageFromScratchBuffer(this, _scratchPagerState, tx.Id,val.Page, pageNumber, previousVersion, size, numberOfPages);

            _allocatedPagesCount += numberOfPages;
            AddAllocatedPage(val.Page, result);
            return true;
        }

        private void AddAllocatedPage(long positionInScratchBuffer, in PageFromScratchBuffer value)
        {
            _allocatedPages.Add(positionInScratchBuffer, value);
            _owners.Track(positionInScratchBuffer, value.PageNumberInDataFile, value.NumberOfPages);
        }

        private bool RemoveAllocatedPage(long positionInScratchBuffer)
        {
            var removed = _allocatedPages.Remove(positionInScratchBuffer);
            _owners.Untrack(positionInScratchBuffer);
            return removed;
        }

        public bool HasActivelyUsedBytes(long oldestActiveTransaction)
        {
            return _allocatedPagesCount > 0 || oldestActiveTransaction <= _txIdAfterWhichLatestFreePagesBecomeAvailable;
        }

        public bool Free(LowLevelTransaction tx, long page)
        {
            return Free(tx, tx.Id, page);
        }
        
        public bool Free(LowLevelTransaction tx, long asOfTxId, long page)
        {
#if VALIDATE
            // If we have encryption enabled, then VALIDATE calls are handled by the EncryptionBufferPool
            if (Pager.Options.Encryption.IsEnabled == false)
            {
                using (var tempTx = new TempPagerTransaction())
                {
                    var pagePointer = _scratchPager.AcquirePagePointer(tempTx, pageNumber, PagerState);
                    if (_allocatedPages.TryGetValue(pageNumber, out _))
                    {
                        var page = new Page(pagePointer);
                        var pageSize = (ulong)(page.IsOverflow ? VirtualPagerLegacyExtensions.GetNumberOfOverflowPages(page.OverflowSize) : 1) *
                                       Constants.Storage.PageSize;
                        _scratchPager.ProtectPageRange(pagePointer, pageSize, true);
                    }
                }
            }
#endif
            
            if (_allocatedPages.TryGetValue(page, out PageFromScratchBuffer value) == false)
            {
                ThrowInvalidFreeOfUnusedPage(page);
                return default; // never called
            }

            tx.ForgetAboutScratchPage(value);
            DebugInfo.LastFreeTime = DateTime.UtcNow;
            // use current write tx id to prevent from overriding a scratch page by write tx 
            // while there might be old read tx looking at it, so we'll only allocate from it
            // _after_ all transactions are past the _current_ write transaction
            DebugInfo.LastAsOfTxIdWhenFree = asOfTxId;

            _allocatedPagesCount -= value.NumberOfPages;
            RemoveAllocatedPage(page);

            Debug.Assert(value.NumberOfPages > 0);

            if (_freePagesBySize.TryGetValue(value.Size, out LinkedList<PendingPage> list) == false)
            {
                list = new LinkedList<PendingPage>();
                _freePagesBySize[value.Size] = list;
            }

            var pending = new PendingPage
            {
                Page = value.PositionInScratchBuffer,
                ValidAfterTransactionId = asOfTxId,
                AllocatedInTransaction = value.AllocatedInTransaction,
            };

            if (asOfTxId >= 0)
            {
                _txIdAfterWhichLatestFreePagesBecomeAvailable = asOfTxId;
                list.AddFirst(pending);
            }
            else
            { 
                // -1 indicates that this is visible to all transactions, so make it the first available in the queue 
                list.AddLast(pending);
            }


            return NumberOfAllocations == 0;
        }

        public ref Pager.State GetStateRef() => ref _scratchPagerState;

        [DoesNotReturn]
        private static void ThrowInvalidFreeOfUnusedPage(long page)
        {
            throw new InvalidOperationException("Attempt to free page that wasn't currently allocated: " + page);
        }

        public void Dispose()
        {
            _disposeOnceRunner.Dispose();
        }

        public bool IsDisposed => _disposeOnceRunner.Disposed;

        public PageFromScratchBuffer ShrinkOverflowPage(in PageFromScratchBuffer value, int newNumberOfPages)
        {
            if (RemoveAllocatedPage(value.PositionInScratchBuffer) == false)
                InvalidAttemptToShrinkPageThatWasntAllocated(value);

            Debug.Assert(value.NumberOfPages > 1);
            Debug.Assert(value.NumberOfPages > newNumberOfPages);

            var shrinked = value with
            {
                NumberOfPages = newNumberOfPages, 
                PreviousVersion = value.PreviousVersion
            }; 

            AddAllocatedPage(shrinked.PositionInScratchBuffer, shrinked);

            _allocatedPagesCount -= value.NumberOfPages - newNumberOfPages;

            return shrinked;
        }

        private static void InvalidAttemptToShrinkPageThatWasntAllocated(in PageFromScratchBuffer value)
        {
            throw new InvalidOperationException($"Attempt to shrink a page that wasn't currently allocated: {value.PositionInScratchBuffer}");
        }

        public sealed class ScratchFileDebugInfo
        {
            private readonly ScratchBufferFile _parent;

            public ScratchFileDebugInfo(ScratchBufferFile parent)
            {
                _parent = parent;
            }

            public DateTime? LastResetTime { get; set; }

            public int NumberOfResets { get; set; }

            public DateTime? LastFreeTime { get; set; }

            public long LastAsOfTxIdWhenFree { get; set; }

            internal Dictionary<long, (long ValidAfterTransactionId, long AllocatedInTransaction)> GetMostAvailableFreePagesBySize()
            {
                return _parent._freePagesBySize.Keys.ToDictionary(size => size, size =>
                {
                    if (_parent._freePagesBySize.TryGetValue(size, out var pendingPages) == false)
                        return (-1, -1);

                    var value = pendingPages.Last?.Value;
                    if (value == null)
                        return (-1, -1);

                    return (value.ValidAfterTransactionId, value.AllocatedInTransaction);
                });
            }

            internal List<PageFromScratchBuffer> GetFirst10AllocatedPages()
            {
                var pages = new List<PageFromScratchBuffer>();

                foreach (var key in _parent._allocatedPages.Keys)
                {
                    if (_parent._allocatedPages.TryGetValue(key, out var pageFromScratchBuffer) == false)
                        continue;

                    if (pageFromScratchBuffer.IsValid is false)
                        continue;

                    pages.Add(pageFromScratchBuffer);

                    if (pages.Count == 10)
                        break;
                }

                return pages;
            }
        }

        /// <summary>
        /// Debug only pages ownership tracking. 
        /// 
        /// Lock-free view of _allocatedPages so VerifyMatch can read it concurrently.
        /// </summary>
        private struct ScratchPageOwners
        {
            private const long NoOwner = -1;
            private const int NumberOfPagesBits = 24; // fits 127GB, big enough
            private const long NumberOfPagesMask = (1L << NumberOfPagesBits) - 1;

            private long[] _owners;

            [Conditional("DEBUG")]
            public void Track(long positionInScratchBuffer, long pageNumberInDataFile, int numberOfPages)
            {
                Debug.Assert(pageNumberInDataFile >= 0, "pageNumberInDataFile >= 0");
                Debug.Assert(numberOfPages >= 0 && numberOfPages <= NumberOfPagesMask,
                    $"{numberOfPages} does not fit in {NumberOfPagesBits} bits");

                var owners = _owners ??= [];
                if (positionInScratchBuffer >= owners.Length)
                {
                    var grown = new long[Math.Max(Math.Max(owners.Length * 2, 64), positionInScratchBuffer + 1)];
                    Array.Fill(grown, NoOwner);
                    Array.Copy(owners, grown, owners.Length);

                    // a reader may still hold the previous array. The scratch file only grows and positions
                    // are append-only, so every position that reader can resolve is in there with this value
                    Volatile.Write(ref _owners, grown);
                    owners = grown;
                }

                Volatile.Write(ref owners[positionInScratchBuffer],
                    (pageNumberInDataFile << NumberOfPagesBits) | (uint)numberOfPages);
            }

            [Conditional("DEBUG")]
            public void Untrack(long positionInScratchBuffer)
            {
                var owners = _owners;
                if (owners != null && positionInScratchBuffer < owners.Length)
                    Volatile.Write(ref owners[positionInScratchBuffer], NoOwner);
            }

            [Conditional("DEBUG")]
            public void UntrackAll()
            {
                var owners = _owners;
                if (owners != null)
                    Array.Fill(owners, NoOwner);
            }

            /// False when nothing owns the position - it was never tracked, or it has been freed.
            public bool TryGetOwner(long positionInScratchBuffer, out long pageNumberInDataFile, out long numberOfPages)
            {
                pageNumberInDataFile = numberOfPages = 0;

                var owners = Volatile.Read(ref _owners);
                if (owners == null || positionInScratchBuffer >= owners.Length)
                    return false;

                var owner = Volatile.Read(ref owners[positionInScratchBuffer]);
                if (owner == NoOwner)
                    return false;

                pageNumberInDataFile = owner >> NumberOfPagesBits;
                numberOfPages = owner & NumberOfPagesMask;
                return true;
            }
        }

        [Conditional("DEBUG")]
        public void VerifyMatch(long pageNumberInDataFile, long positionInScratchBuffer, int numberOfPages)
        {
            if (_owners.TryGetOwner(positionInScratchBuffer, out var ownerPageNumber, out var ownerNumberOfPages) == false)
                return;

            if (ownerPageNumber != pageNumberInDataFile || ownerNumberOfPages != numberOfPages)
                throw new InvalidOperationException(
                    $"Failed to verify page {pageNumberInDataFile} when reading scratch page {positionInScratchBuffer}, values different!" +
                    $"Page: {pageNumberInDataFile} vs. {ownerPageNumber} ({numberOfPages} vs {ownerNumberOfPages})!");
        }

        [Conditional("DEBUG")]
        public void AssertNoPagesAllocatedInTransactionOlderThan(long txId)
        {
            foreach (PageFromScratchBuffer p in _allocatedPages.Values)
            {
                if (p.AllocatedInTransaction < txId)
                {
                    var message =
                        $"Found page #{p.PageNumberInDataFile} allocated in tx {p.AllocatedInTransaction} (scratch {p.File.Number}, pos in scratch: {p.PositionInScratchBuffer}) while we freed up to tx {txId}";

                    throw new InvalidOperationException(message);
                }
            }
        }
    }
}
