﻿#nullable enable

using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Collections;
using Sparrow.LowMemory;
using Sparrow.Platform;
using Sparrow.Server.Exceptions;
using Sparrow.Server.Meters;
using Sparrow.Server.Platform;
using Sparrow.Server.Utils;
using Voron.Exceptions;
using Voron.Global;

namespace Voron.Impl.Paging;

public unsafe partial class Pager : IDisposable
{
    public readonly string FileName;
    public readonly StorageEnvironmentOptions Options;
    private readonly Pal.OpenFileFlags _flags;
    private readonly StateFor32Bits? _32BitsState;

    private class StateFor32Bits
    {
        public readonly ReaderWriterLockSlim AllocationLock = new ReaderWriterLockSlim();
        public readonly ConcurrentDictionary<long, ConcurrentSet<MappedAddresses>> MemoryMapping = new();
    }

    private readonly Functions _functions;
    private readonly ConcurrentSet<WeakReference<State>> _states = [];
    private readonly EncryptionBuffersPool _encryptionBuffersPool;
    private readonly byte[] _masterKey;
    private PrefetchTableHint _prefetchState;
    private DateTime _lastIncrease;
    private long _increaseSize;

    private const int MinIncreaseSize = 16 * Constants.Size.Kilobyte;
    private const int MaxIncreaseSize = Constants.Size.Gigabyte;

    public static (Pager Pager, State State) Create(StorageEnvironmentOptions options, string filename, long initialFileSize, Pal.OpenFileFlags flags)
    {
        var pager = new Pager(options, filename, flags, GetFunctions(options, flags));
        var result = Pal.rvn_init_pager(filename, initialFileSize, flags, 
            out var handle, out var readOnlyMemory, out var writeMemory, out var memorySize, out var error);
        if (result != PalFlags.FailCodes.Success)
            RaiseError(filename, error, result, initialFileSize);
        var state = new State(pager, readOnlyMemory, writeMemory, memorySize, handle);
        (state.TotalFileSize, state.TotalDiskSpace) = pager.GetFileSize(state);
        pager.InstallState(state);
        pager.Initialize(memorySize);
        return (pager, state);
    }

    private static Functions GetFunctions(StorageEnvironmentOptions options, Pal.OpenFileFlags flags)
    {
        var funcs = options.RunningOn32Bits ? Bits32.CreateFunctions() : Bits64.CreateFunctions();
        if (flags.HasFlag(Pal.OpenFileFlags.Encrypted))
        {
            funcs.AcquirePagePointer = &Crypto.AcquirePagePointer;
            funcs.AcquirePagePointerForNewPage = &Crypto.AcquirePagePointerForNewPage;
            funcs.ConvertToWritePointer = &Crypto.ConvertToWritePointer;
        }
        return funcs;
    }

    private static void RaiseError(string filename, int errorCode, PalFlags.FailCodes rc, long initialFileSize, [CallerMemberName] string? caller = null)
    {
        if (rc == PalFlags.FailCodes.FailLockMemory)
            throw new InsufficientMemoryException(
                $"Failed to increase the min working set size so we can lock memory for {filename}. With encrypted " +
                "databases we lock some memory in order to avoid leaking secrets to disk. Treating this as a catastrophic error " +
                "and aborting the current operation.");

        try
        {
            PalHelper.ThrowLastError(rc, errorCode, $"{caller} failed on {rc} for '{filename}'");
        }
        catch (DiskFullException dfEx)
        {
            var diskSpaceResult = DiskUtils.GetDiskSpaceInfo(filename);
            throw new DiskFullException(filename, initialFileSize, diskSpaceResult?.TotalFreeSpace.GetValue(SizeUnit.Bytes), dfEx.Message);
        }
    }

    private Pager(StorageEnvironmentOptions options,
        string filename,
        Pal.OpenFileFlags flags,
        Functions functions)
    {
        Options = options;
        _flags = flags;
        FileName = filename;
        _canPrefetch = PlatformDetails.CanPrefetch == false || options.EnablePrefetching == false;
        _encryptionBuffersPool = options.Encryption.EncryptionBuffersPool;
        _masterKey = options.Encryption.MasterKey;
        _functions = functions;
        _increaseSize = MinIncreaseSize;
        _prefetchState = PrefetchTableHint.Empty;
        if (options.RunningOn32Bits)
        {
            _32BitsState = new StateFor32Bits();
        }
    }

    private void Initialize(long initialSize)
    {
        _prefetchState = new PrefetchTableHint(Options.PrefetchSegmentSize, Options.PrefetchResetThreshold, initialSize);
    }

    public void DiscardWholeFile(State state)
    {
        DiscardPages(state, 0, state.NumberOfAllocatedPages);
    }

    public void DiscardPages(State state, long pageNumber, long numberOfPages)
    {
        if (Options.DiscardVirtualMemory == false)
            return;

        byte* baseAddress = state.ReadAddress;
        long offset = pageNumber * Constants.Storage.PageSize;

        _ = Pal.rvn_discard_virtual_memory(baseAddress + offset, numberOfPages * Constants.Storage.PageSize, out _);
    }

    public void TryPrefetchingWholeFile(State state)
    {
        MaybePrefetchMemory(state, 0, state.NumberOfAllocatedPages);
    }

    public bool EnsureMapped(State state, ref PagerTransactionState txState, long page, int numberOfPages)
    {
        if (_functions.EnsureMapped == null)
            return false;
        
        return _functions.EnsureMapped(this, state, ref txState, page, numberOfPages);
    }

    public struct PageIterator : IEnumerator<long>
    {
        private readonly long _startPage;
        private readonly long _endPage;
        private long _currentPage;

        public PageIterator(long pageNumber, long pagesToPrefetch)
        {
            this._startPage = pageNumber;
            this._endPage = pageNumber + pagesToPrefetch;
            this._currentPage = pageNumber;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public bool MoveNext()
        {
            this._currentPage++;
            return _currentPage < _endPage;
        }

        public void Reset()
        {
            this._currentPage = this._startPage;
        }

        object IEnumerator.Current => Current;

        public long Current
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get => _currentPage;
        }

        public void Dispose() { }
    }


    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void MaybePrefetchMemory(State state, long pageNumber, long pagesToPrefetch)
    {
        MaybePrefetchMemory(state, new PageIterator(pageNumber, pagesToPrefetch));
    }

    public void MaybePrefetchMemory<T>(State state, T pagesToPrefetch) where T : struct, IEnumerator<long>
    {
        if (!_canPrefetch)
            return;

        if (pagesToPrefetch.MoveNext() == false)
            return;

        var prefetcher = GlobalPrefetchingBehavior.GlobalPrefetcher.Value;

        do
        {
            long pageNumber = pagesToPrefetch.Current;
            if (!_prefetchState.ShouldPrefetchSegment(pageNumber, out var offset, out long bytes))
                continue;

            prefetcher.CommandQueue.TryAdd(new PalDefinitions.PrefetchRanges { VirtualAddress = state.ReadAddress + offset, NumberOfBytes = (nint)bytes }, 0);
        } while (pagesToPrefetch.MoveNext());

        _prefetchState.CheckResetPrefetchTable();
    }


    public void Sync(State state, long totalUnsynced)
    {
        if (state.Disposed || _flags.HasFlag(Pal.OpenFileFlags.Temporary))
            return; // nothing to do here

        using var metric = Options.IoMetrics.MeterIoRate(FileName, IoMetrics.MeterType.DataSync, 0);
        metric.IncrementFileSize(state.TotalAllocatedSize);
        var rc = Pal.rvn_sync_pager(state.Handle, out var errorCode);
        if (rc != PalFlags.FailCodes.Success)
        {
            PalHelper.ThrowLastError(rc, errorCode, $"Failed to sync file: {FileName}");
        }
        metric.IncrementSize(totalUnsynced);
    }


    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public byte* AcquirePagePointerWithOverflowHandling(State state, ref PagerTransactionState txState, long pageNumber)
    {
        var pageHeader = (PageHeader*)AcquirePagePointer(state, ref txState, pageNumber);
        if (_functions.EnsureMapped == null) 
            return (byte*)pageHeader;

        return EnsureOverflowPageIsLoaded(state, ref txState, pageNumber, pageHeader);
    }

    private byte* EnsureOverflowPageIsLoaded(State state, ref PagerTransactionState txState, long pageNumber, PageHeader* pageHeader)
    {
        // Case 1: Page is not overflow ==> no problem, returning a pointer to existing mapping
        if ((pageHeader->Flags & PageFlags.Overflow) != PageFlags.Overflow)
            return (byte*)pageHeader;

        // Case 2: Page is overflow and already mapped large enough ==> no problem, returning a pointer to existing mapping
        if (EnsureMapped(state, ref txState, pageNumber, Paging.GetNumberOfOverflowPages(pageHeader->OverflowSize)) == false)
            return (byte*)pageHeader;

        // Case 3: Page is overflow and was ensuredMapped above, view was re-mapped so we need to acquire a pointer to the new mapping.
        return _functions.AcquirePagePointer(this, state, ref txState, pageNumber);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public byte* AcquireRawPagePointerWithOverflowHandling(State state, ref PagerTransactionState txState, long pageNumber)
    {
        var pageHeader = (PageHeader*)AcquireRawPagePointer(state, ref txState, pageNumber);
        if (_functions.EnsureMapped == null)
            return (byte*)pageHeader;

        return EnsureOverflowRawPageIsLoaded(state, ref txState, pageNumber, pageHeader);
    }

    private byte* EnsureOverflowRawPageIsLoaded(State state, ref PagerTransactionState txState, long pageNumber, PageHeader* pageHeader)
    {
        // Case 1: Page is not overflow ==> no problem, returning a pointer to existing mapping
        if ((pageHeader->Flags & PageFlags.Overflow) != PageFlags.Overflow)
            return (byte*)pageHeader;
        // Case 2: Page is overflow and already mapped large enough ==> no problem, returning a pointer to existing mapping
        if (EnsureMapped(state, ref txState, pageNumber, Paging.GetNumberOfOverflowPages(pageHeader->OverflowSize)) == false)
            return (byte*)pageHeader;

        // Case 3: Page is overflow and was ensuredMapped above, view was re-mapped so we need to acquire a pointer to the new mapping.
        return _functions.AcquireRawPagePointer(this, state, ref txState, pageNumber);
    }

    public byte* AcquirePagePointer(State state, ref PagerTransactionState txState, long pageNumber)
    {
        if (pageNumber <= state.NumberOfAllocatedPages && pageNumber >= 0)
            return _functions.AcquirePagePointer(this, state, ref txState, pageNumber);
        return ThrowInvalidPage(state, pageNumber);
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private byte* ThrowInvalidPage(State state, long pageNumber)
    {
        VoronUnrecoverableErrorException.Raise(Options,
            "The page " + pageNumber + " was not allocated, allocated pages: " + state.NumberOfAllocatedPages + " in " + FileName);
        return null; // never hit
    }

    public byte* AcquireRawPagePointer(State state, ref PagerTransactionState txState, long pageNumber)
    {
        if (pageNumber <= state.NumberOfAllocatedPages && pageNumber >= 0)
            return _functions.AcquireRawPagePointer(this, state, ref txState, pageNumber);

        return ThrowInvalidPage(state, pageNumber);
    }

    public byte* AcquirePagePointerForNewPage(State state, ref PagerTransactionState txState, long pageNumber, int numberOfPages)
    {
        return _functions.AcquirePagePointerForNewPage(this, pageNumber, numberOfPages, state, ref txState);
    }

    public void EnsureContinuous(ref State state, long requestedPageNumber, int numberOfPages)
    {
        if (state.Disposed)
            throw new ObjectDisposedException("PagerState was already disposed");

        if (requestedPageNumber + numberOfPages <= state.NumberOfAllocatedPages)
            return;

        // this ensures that if we want to get a range that is more than the current expansion
        // we will increase as much as needed in one shot
        var minRequested = (requestedPageNumber + numberOfPages) * Constants.Storage.PageSize;
        var allocationSize = Math.Max(state.NumberOfAllocatedPages * Constants.Storage.PageSize, Constants.Storage.PageSize);
        while (minRequested > allocationSize)
        {
            allocationSize = GetNewLength(allocationSize, minRequested);
        }
        Debug.Assert(allocationSize > state.TotalAllocatedSize, "allocationSize > state.TotalAllocatedSize");
        
        if (Options.CopyOnWriteMode && state.Pager.FileName.EndsWith(Constants.DatabaseFilename))
            throw new IncreasingDataFileInCopyOnWriteModeException(state.Pager.FileName, allocationSize);

        var rc = Pal.rvn_increase_pager_size(state.Handle, 
            allocationSize, out var handle, out var readAddress, out var writeAddress, 
            out var totalAllocatedSize, out var errorCode);
        if (rc != PalFlags.FailCodes.Success)
        {
            PalHelper.ThrowLastError(rc, errorCode, $"Failed to increase file '{state.Pager.FileName}' to {new Size(allocationSize, SizeUnit.Bytes)}");
        }
        Debug.Assert(totalAllocatedSize >= state.TotalAllocatedSize, "totalAllocatedSize >= state.TotalAllocatedSize");
        state = new State(this, readAddress, writeAddress, totalAllocatedSize, handle);
        InstallState(state);
    }


    private long GetNewLength(long current, long minRequested)
    {
        DateTime now = DateTime.UtcNow;
        if (_lastIncrease == DateTime.MinValue)
        {
            _lastIncrease = now;
            return MinIncreaseSize;
        }

        if (LowMemoryNotification.Instance.InLowMemory)
        {
            _lastIncrease = now;
            // cannot return less than the minRequested
            return GetNearestFileSize(minRequested);
        }

        TimeSpan timeSinceLastIncrease = (now - _lastIncrease);
        _lastIncrease = now;
        if (timeSinceLastIncrease.TotalMinutes < 3)
        {
            _increaseSize = Math.Min(_increaseSize * 2, MaxIncreaseSize);
        }
        else if (timeSinceLastIncrease.TotalMinutes > 15)
        {
            _increaseSize = Math.Max(MinIncreaseSize, _increaseSize / 2);
        }

        // At any rate, we won't do an increase by over 50% of current size, to prevent huge empty spaces
        // 
        // The reasoning behind this is that we want to make sure that we increase in size very slowly at first
        // because users tend to be sensitive to a lot of "wasted" space. 
        // We also consider the fact that small increases in small files would probably result in cheaper costs, and as
        // the file size increases, we will reserve more & more from the OS.
        // This also avoids "I added 300 records and the file size is 64MB" problems that occur when we are too
        // eager to reserve space
        var actualIncrease = Math.Min(_increaseSize, current / 2);

        // we then want to get the next power of two number, to get pretty file size
        var totalSize = current + actualIncrease;
        return GetNearestFileSize(totalSize);
    }

    private static readonly long IncreaseByPowerOf2Threshold = new Size(512, SizeUnit.Megabytes).GetValue(SizeUnit.Bytes);
    private readonly bool _canPrefetch;

    private static long GetNearestFileSize(long neededSize)
    {
        if (neededSize < IncreaseByPowerOf2Threshold)
            return Bits.PowerOf2(neededSize);

        // if it is over 0.5 GB, then we grow at 1 GB intervals
        var remainder = neededSize % Constants.Size.Gigabyte;
        if (remainder == 0)
            return neededSize;

        // above 0.5GB we need to round to the next GB number
        return neededSize + Constants.Size.Gigabyte - remainder;
    }

    private void InstallState(State state)
    {
        _states.Add(state.WeakSelf);
    }

    private class PrefetchTableHint
    {
        public static PrefetchTableHint Empty = new(1024, 1024, 0);
        
        private const byte EvenPrefetchCountMask = 0x70;
        private const byte EvenPrefetchMaskShift = 4;
        private const byte OddPrefetchCountMask = 0x07;
        private const byte AlreadyPrefetch = 7;

        private readonly int _prefetchSegmentSize;
        private readonly int _prefetchResetThreshold;
        private readonly int _segmentShift;

        // this state is accessed by multiple threads
        // concurrently in an unsafe manner, we do so
        // explicitly with the intention of dealing with
        // dirty reads and writes. The only impact that this
        // can have is a spurious call to the OS's 
        // madvice() / PrefetchVirtualMemory
        // Thread safety is based on the OS's own thread safety
        // for concurrent calls to these methods. 
        private int _refreshCounter;
        private readonly byte[] _prefetchTable;

        public PrefetchTableHint(long prefetchSegmentSize, long prefetchResetThreshold, long initialFileSize)
        {
            _segmentShift = Bits.MostSignificantBit(prefetchSegmentSize);

            _prefetchSegmentSize = 1 << _segmentShift;
            _prefetchResetThreshold = (int)((float)prefetchResetThreshold / _prefetchSegmentSize);

            Debug.Assert((_prefetchSegmentSize - 1) >> _segmentShift == 0);
            Debug.Assert(_prefetchSegmentSize >> _segmentShift == 1);

            long numberOfAllocatedSegments = (initialFileSize / _prefetchSegmentSize) + 1;
            _prefetchTable = new byte[(numberOfAllocatedSegments / 2) + 1];
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private int GetSegmentState(long segment)
        {
            if (segment < 0 || segment > _prefetchTable.Length)
                return AlreadyPrefetch;

            byte value = _prefetchTable[segment / 2];
            if (segment % 2 == 0)
            {
                // The actual value is in the high byte.
                value = (byte)(value >> EvenPrefetchMaskShift);
            }
            else
            {
                value = (byte)(value & OddPrefetchCountMask);
            }

            return value;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void SetSegmentState(long segment, int state)
        {
            byte value = this._prefetchTable[segment / 2];
            if (segment % 2 == 0)
            {
                // The actual value is in the high byte.
                value = (byte)((value & OddPrefetchCountMask) | (state << EvenPrefetchMaskShift));
            }
            else
            {
                value = (byte)((value & EvenPrefetchCountMask) | state);
            }

            this._prefetchTable[segment / 2] = value;
        }

        public bool ShouldPrefetchSegment(long pageNumber, out long offsetFromFileBase, out long sizeInBytes)
        {
            long segmentNumber = (pageNumber * Constants.Storage.PageSize) >> this._segmentShift;

            int segmentState = GetSegmentState(segmentNumber);
            if (segmentState < AlreadyPrefetch)
            {
                // We update the current segment counter
                segmentState++;

                // if the previous or next segments were loaded, eagerly
                // load this one, probably a sequential scan of one type or
                // another
                int previousSegmentState = GetSegmentState(segmentNumber - 1);
                if (previousSegmentState == AlreadyPrefetch)
                {
                    segmentState = AlreadyPrefetch;
                }
                else
                {
                    int nextSegmentState = GetSegmentState(segmentNumber + 1);
                    if (nextSegmentState == AlreadyPrefetch)
                    {
                        segmentState = AlreadyPrefetch;
                    }
                }

                SetSegmentState(segmentNumber, segmentState);

                if (segmentState == AlreadyPrefetch)
                {
                    _refreshCounter++;

                    // Prepare the segment information. 
                    sizeInBytes = _prefetchSegmentSize;
                    offsetFromFileBase = segmentNumber * _prefetchSegmentSize;
                    return true;
                }
            }

            sizeInBytes = 0;
            offsetFromFileBase = 0;
            return false;
        }

        public void CheckResetPrefetchTable()
        {
            if (_refreshCounter > _prefetchResetThreshold)
            {
                _refreshCounter = 0;

                // We will zero out the whole table to reset the prefetching behavior. 
                Array.Clear(_prefetchTable, 0, this._prefetchTable.Length);
            }
        }
    }

    public void Dispose()
    {
        foreach (WeakReference<State> state in _states)
        {
            if (state.TryGetTarget(out var v))
            {
                v.Dispose();
            }
        }

        if (PlatformDetails.RunningOnPosix &&
            Options.OwnsPagers &&
            _flags.HasFlag(Pal.OpenFileFlags.Temporary))
        {
            // Posix doesn't support DeleteOnClose
            try
            {
                File.Delete(FileName);
            }
            catch
            {
                // if we can't delete it, there isn't much that 
                // we can do about it
            }
        }
    }

    public void TryReleasePage(ref PagerTransactionState txState, long pageNumber)
    {
        if (txState.ForCrypto?.TryGetValue(this, out var cyprtoState) is not true)
            return;
        if (cyprtoState.TryGetValue(pageNumber, out var buffer) is not true)
            return;

        if (buffer.Modified)
            return;

        buffer.ReleaseRef();

        if (!buffer.CanRelease) 
            return;
        
        cyprtoState.RemoveBuffer(this,pageNumber);
    }

    public byte* MakeWritable(State state, byte* ptr)
    {
        return _functions.ConvertToWritePointer(this, state, ptr);
    }

    public void SetSparseRange(State state, long offset, long size)
    {
        var rc = Pal.rvn_pager_set_sparse_region(state.Handle, offset, size, out int errorCode);

        if (rc is PalFlags.FailCodes.Success or
            PalFlags.FailCodes.FailSparseNotSupported)  // explicitly ignoring this
            return;
        
        PalHelper.ThrowLastError(rc, errorCode, "Failed to set sparse range on " + state.Pager.FileName);
    }

    public (long AllocatedSize, long PhysicalSize) GetFileSize(State state)
    {
        var rc = Pal.rvn_pager_get_file_size(state.Handle, out var totalSize, out var physicalSize, out var errorCode);
        if (rc is not PalFlags.FailCodes.Success)
            PalHelper.ThrowLastError(rc, errorCode, "Failed to get file size for " + state.Pager.FileName);

        return (totalSize, physicalSize);
    }
}
