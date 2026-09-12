using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using Sparrow.Platform;
using Voron.Global;
using Voron.Impl.Paging;

namespace Voron.Impl.Journal;

/// <summary>
/// Hands off temporary memory that is deliberately file backed rather than anonymous.
/// Under memory pressure will prevent the OOM killer from killing us.
/// </summary>
public sealed unsafe class CompressionBufferRing : IDisposable
{
    public sealed class Lease : IDisposable
    {
        internal CompressionBufferRing Owner;
        public Pager.State State;
        public long BaseOffsetInPages;
        public int NumberOfPages;

        internal volatile bool Returned;

        public void Dispose() => Owner.Return(this);
    }

    private readonly StorageEnvironmentOptions _options;
    private readonly WriteAheadJournal _journal;
    private readonly Queue<Lease> _live = new();
    private readonly ManualResetEventSlim _returned = new(false);
    private Pager _current;
    private Pager.State _state;
    private long _recreateWhenEmptyTo;
    private long _head;
    private Lease _openReservation;
    private long _highWaterInPages;
    private long _compressionPagerCounter;
    private DateTime _lastReduceCheck = DateTime.UtcNow;
    private long _maxPagesRequiredSinceReduceCheck;
    private bool _disposed;

    public CompressionBufferRing(StorageEnvironmentOptions options, WriteAheadJournal journal)
    {
        _options = options;
        _journal = journal ?? throw new ArgumentNullException(nameof(journal));
        CreatePager(options.InitialFileSize ?? options.InitialLogFileSize);
    }

    public Pager Pager => _current;
    public Pager.State State => _state;

    private long CapacityInPages => _state.NumberOfAllocatedPages;

    private void CreatePager(long initialSize)
    {
        var (pager, state) = _options.CreateTemporaryBufferPager(
            $"compression.{_compressionPagerCounter++:D10}{StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions.BuffersFileExtension}",
            initialSize, encrypted: false);
        _current = pager;
        _state = state;
    }

    public Lease Reserve(int pages)
    {
        _journal.AssertWriteLockHeld();

        while (true)
        {
            Debug.Assert(_openReservation == null, "a reservation is already open");

            ThrowIfDisposed();            
            DrainReturned();

            _returned.Reset();
            _maxPagesRequiredSinceReduceCheck = Math.Max(_maxPagesRequiredSinceReduceCheck, pages);

            if (_live.Count == 0)
            {
                // nothing is in flight, so the whole file is ours and there is no reason to be anywhere else
                _head = 0;
                if (pages > CapacityInPages)
                    Grow(pages);

                return Open(pages);
            }

            var tail = _live.Peek().BaseOffsetInPages;

            if (_head < tail) // wrapped: the free run is [head..tail)
            {
                if (_head + pages <= tail)
                    return Open(pages);
            }
            else // not wrapped: [head..capacity) and, after a wrap, [0..tail)
            {
                if (_head + pages <= CapacityInPages)
                    return Open(pages);

                if (pages <= tail)
                {
                    _head = 0;
                    return Open(pages);
                }

                // neither run fits. only grow the file when waiting isn't likely to help
                if (pages > CapacityInPages / 2)
                {
                    Grow(_head + pages);
                    return Open(pages);
                }
            }

            // Nothing fits - park until a lease comes back. 
            // We are waiting while holding the journal write lock, *intentional back-pressure*
            _returned.Wait();
        }
    }

    public Lease Trim(Lease lease, int usedPages)
    {
        _journal.AssertWriteLockHeld();
        Debug.Assert(ReferenceEquals(lease, _openReservation), "sealing a reservation that is not open");
        Debug.Assert(usedPages > 0 && usedPages <= lease.NumberOfPages, "usedPages must be within the bounds of the lease");
        
        lease.NumberOfPages = usedPages;
        _head = lease.BaseOffsetInPages + usedPages;

        _openReservation = null;
        return lease;
    }
    
    public void Cancel(Lease lease)
    {
        _journal.AssertWriteLockHeld();
        if (lease == null || ReferenceEquals(lease, _openReservation) == false)
            return;

        lease.NumberOfPages = 0;
        lease.Returned = true;
        _head = lease.BaseOffsetInPages;
        _openReservation = null;
    }

    private Lease Open(int pages)
    {
        var lease = new Lease
        {
            Owner = this,
            State = _state,
            BaseOffsetInPages = _head,
            NumberOfPages = pages
        };
        _live.Enqueue(lease);
        _openReservation = lease;
        _head += pages;
        _highWaterInPages = Math.Max(_highWaterInPages, _head);
        return lease;
    }

    // This may run on a *different* thread, concurrent to everything else!
    private void Return(Lease lease)
    {
        // safe to call this twice
        lease.Returned = true;
        _returned.Set();
    }

    private void DrainReturned()
    {
        _journal.AssertWriteLockHeld();

        while (_live.TryPeek(out var oldest) && oldest.Returned)
        {
            _live.Dequeue();
        }

        if (_live.Count == 0)
        {
            _head = 0;
            RecreateIfRequested();
        }
    }

    private void Grow(long minPages) => _current.EnsureContinuous(ref _state, 0, checked((int)minPages));

    public void ZeroWhenEmpty(ref Pager.PagerTransactionState txState)
    {
        _journal.AssertWriteLockHeld();
        
        DrainReturned();

        if (_disposed || _live.Count > 0)
            return;

        var pages = Math.Min(_highWaterInPages, _state.NumberOfAllocatedPages);
        if (pages == 0)
            return;

        _current.EnsureMapped(_state, ref txState, 0, checked((int)pages));
        var p = _current.MakeWritable(_state, _current.AcquirePagePointer(_state, ref txState, 0));
        Sodium.sodium_memzero(p, (UIntPtr)(pages * Constants.Storage.PageSize));
        _highWaterInPages = 0;
    }

    public void ReduceSizeIfNeeded(bool forceReduce)
    {
        _journal.AssertWriteLockHeld();
    
        DrainReturned();

        if (_disposed || _live.Count > 0)
            return;

        var maxSize = _options.MaxScratchBufferSize;
        var currentSize = _state.NumberOfAllocatedPages * Constants.Storage.PageSize;
        if (currentSize <= maxSize)
        {
            if (forceReduce && _options.DiscardVirtualMemory && _highWaterInPages > 0)
            {
                _current.DiscardPages(_state, 0, Math.Min(_highWaterInPages, _state.NumberOfAllocatedPages));
                _highWaterInPages = 0;
            }

            return;
        }

        if (forceReduce == false)
        {
            if ((DateTime.UtcNow - _lastReduceCheck).TotalMinutes < 5)
                return;

            var stillNeeded = _maxPagesRequiredSinceReduceCheck > _state.NumberOfAllocatedPages / 2;
            _maxPagesRequiredSinceReduceCheck = 0;
            _lastReduceCheck = DateTime.UtcNow;
            if (stillNeeded)
                return;
        }

        _lastReduceCheck = DateTime.UtcNow;
        ReplacePager(maxSize);
    }

    // recovery path when we have insufficient memory (usually to lock when using encryption)
    public void Recreate()
    {
        _journal.AssertWriteLockHeld();

        if (_disposed)
            return;

        DrainReturned();
        _recreateWhenEmptyTo = _options.InitialFileSize ?? _options.InitialLogFileSize;
        RecreateIfRequested();
        _lastReduceCheck = DateTime.UtcNow;
    }

    private void RecreateIfRequested()
    {
        if (_recreateWhenEmptyTo == 0 || _live.Count > 0)
            return;

        var size = _recreateWhenEmptyTo;
        _recreateWhenEmptyTo = 0;
        ReplacePager(size);
    }

    private void ReplacePager(long newSize)
    {
        Debug.Assert(_live.Count == 0, "replacing a pager that still has leases pointing into its mappings");

        _current.Dispose();

        _forTestingPurposes?.AfterReplacePager?.Invoke();

        CreatePager(newSize);
        _head = 0;
        _highWaterInPages = 0;
    }

    private void WaitForLeasesToReturn()
    {
        var timeout = TimeSpan.FromSeconds(30);
        var sw = Stopwatch.StartNew();

        while (true)
        {
            _returned.Reset();
            DrainReturned();
            if (_live.Count == 0)
                return;

            var remaining = timeout - sw.Elapsed;
            if (remaining <= TimeSpan.Zero)
                return; // the write is stuck or failed without releasing - disposing is still better than hanging

            if (_returned.Wait(remaining) == false)
                return;
        }
    }

    private TestingStuff _forTestingPurposes;

    internal TestingStuff ForTestingPurposesOnly() => _forTestingPurposes ??= new TestingStuff(this);

    internal sealed class TestingStuff(CompressionBufferRing ring)
    {
        internal Action AfterReplacePager;

        /// <summary>
        /// Ranges a journal write has not released yet. Drains first, so it reports what is still held rather
        /// than what has not been tidied up. Caller holds the journal write lock, as for any other read of the
        /// ring's state.
        /// </summary>
        internal int OutstandingLeases
        {
            get
            {
                ring._journal.AssertWriteLockHeld();
                ring.DrainReturned();
                return ring._live.Count;
            }
        }
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
            throw new ObjectDisposedException(nameof(CompressionBufferRing));
    }

    public void Dispose()
    {
        _journal.AssertWriteLockHeld();
    
        if (_disposed)
            return;
        _disposed = true;

        WaitForLeasesToReturn();

        _current.Dispose();
        _returned.Dispose();
    }
}
