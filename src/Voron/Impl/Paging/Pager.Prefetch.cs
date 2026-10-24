using System;
using Sparrow.Platform;
using Sparrow.Server.Platform;
using Voron.Global;

namespace Voron.Impl.Paging;

public unsafe partial class Pager
{
    // Data files are read with random access advice, so a page fault reads only the 4 KB it touches. Where we know
    // that a whole page (or several) will be needed, we ask the kernel for the exact range instead:
    //  - asynchronously, through the process wide prefetch ring (fire and forget, races the consumer), or
    //  - synchronously, just before we touch the range ourselves (one exact read instead of two faults).
    // Experiment switches: RAVEN_PREFETCH_RING=1 (ring), RAVEN_PREFETCH_VALIDATE=1 (first touch of a page).
    public static readonly bool PrefetchRingEnabled = PlatformDetails.RunningOnLinux && Environment.GetEnvironmentVariable("RAVEN_PREFETCH_RING") == "1";
    // RAVEN_PREFETCH_VALIDATE=1: every transaction; =reads: read transactions only (keeps the syscall off the merger)
    public static readonly bool PrefetchOnFirstTouch = PlatformDetails.RunningOnLinux && Environment.GetEnvironmentVariable("RAVEN_PREFETCH_VALIDATE") is "1" or "reads";
    public static readonly bool PrefetchOnFirstTouchReadsOnly = Environment.GetEnvironmentVariable("RAVEN_PREFETCH_VALIDATE") == "reads";

    private int _prefetchSlot = -1;
    private uint _prefetchGeneration;
    private int _prefetchFd = -1;
    private Microsoft.Win32.SafeHandles.SafeFileHandle _prefetchFileHandle;

    private void RegisterForPrefetch(State state)
    {
        if (PrefetchRingEnabled == false && PrefetchOnFirstTouch == false)
            return;
        if (FileName.EndsWith(Constants.DatabaseFilename, StringComparison.OrdinalIgnoreCase) == false)
            return;

        // PAL hands back a dup of the pager's fd; we keep it for the synchronous hint and the ring registers its own reference
        if (Pal.rvn_pager_get_file_handle(state.Handle, out var fileHandle, out _) != PalFlags.FailCodes.Success)
            return;

        _prefetchFileHandle = fileHandle;
        _prefetchFd = fileHandle.DangerousGetHandle().ToInt32();

        if (PrefetchRingEnabled &&
            Pal.rvn_prefetch_register_file(_prefetchFd, out var slot, out var generation, out _) == PalFlags.FailCodes.Success)
        {
            _prefetchGeneration = generation;
            _prefetchSlot = slot;
        }
    }

    private void UnregisterForPrefetch()
    {
        var slot = _prefetchSlot;
        _prefetchSlot = -1;
        if (slot >= 0)
            Pal.rvn_prefetch_unregister_file(slot, out _);

        _prefetchFd = -1;
        _prefetchFileHandle?.Dispose();
        _prefetchFileHandle = null;
    }

    /// <summary>Queues an asynchronous read of the pages into the page cache. Returns false if not queued.</summary>
    public bool PrefetchPagesAsync(long pageNumber, int numberOfPages = 1)
    {
        var slot = _prefetchSlot;
        if (slot < 0)
            return false;
        return Pal.rvn_prefetch_file_range(slot, _prefetchGeneration, pageNumber * Constants.Storage.PageSize, (long)numberOfPages * Constants.Storage.PageSize) == 1;
    }

    /// <summary>Starts the read of the pages before the caller touches them, so the fault waits on one exact read.</summary>
    public void WillNeedPages(long pageNumber, int numberOfPages = 1)
    {
        var fd = _prefetchFd;
        if (fd < 0)
            return;
        Pal.rvn_fadvise_willneed(fd, pageNumber * Constants.Storage.PageSize, (long)numberOfPages * Constants.Storage.PageSize);
    }
}
