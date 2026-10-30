using System;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Sparrow.Binary;
using Sparrow.Platform;

namespace Sparrow.Server.Platform;

public readonly record struct PrefetchRingStats(long Submitted, long DroppedFull, long DroppedStale, long Failed, long ReadGroups, long Reads, bool UsesIoUring);

/// <summary>
/// The PAL's queue of read-ahead requests for data files: drained by io_uring, or by a posix_fadvise thread where
/// io_uring is not allowed. The process uses <see cref="Default"/>; tests can create their own. On Windows and macOS
/// there is no ring (<see cref="IsAvailable"/> is false). A ring can only be disposed after every data file opened
/// with it was closed.
/// </summary>
public sealed unsafe class PrefetchRing : IDisposable
{
    public static readonly PrefetchRing Default = new(PalConfiguration.PrefetchRingEntries, PalConfiguration.PrefetchMaxFiles, PalConfiguration.PrefetchQueueSize);

    private void* _handle;

    /// <summary>The PAL handle, passed to rvn_init_pager; null when there is no ring.</summary>
    public void* Handle => _handle;

    /// <summary>The number of data files that can be open with the ring at the same time.</summary>
    public int MaxFiles { get; }

    public bool IsAvailable => _handle != null;

    public PrefetchRing(int ringEntries, int maxFiles, int queueSize)
    {
        MaxFiles = maxFiles;
        if (PlatformDetails.RunningOnLinux == false)
            return;

        queueSize = Bits.PowerOf2(queueSize); // the PAL indexes the queue by mask
        var rc = Pal.rvn_prefetch_ring_create(ringEntries, maxFiles, queueSize, &OnReadsDone, out var handle, out var errorCode);
        if (rc != PalFlags.FailCodes.Success)
            PalHelper.ThrowLastError(rc, errorCode, $"Failed to create a prefetch ring ({ringEntries} entries, {maxFiles} files, queue of {queueSize})");
        _handle = handle;
    }

    public PrefetchRingStats? GetStats()
    {
        if (_handle == null || Pal.rvn_prefetch_ring_stats(_handle, out var submitted, out var droppedFull, out var droppedStale, out var failed,
                out var readGroups, out var reads, out var usesIoUring) != PalFlags.FailCodes.Success)
            return null;
        return new PrefetchRingStats(submitted, droppedFull, droppedStale, failed, readGroups, reads, usesIoUring != 0);
    }

    /// <summary>
    /// Returns false when the read group was not queued. Otherwise <paramref name="done"/> completes once the pages are in the
    /// page cache, or soon after if they cannot be read.
    /// </summary>
    public static bool TryReadPages(void* pagerHandle, ReadOnlySpan<long> offsets, out Task done)
    {
        // the submitter reads the offsets after this returns, so they stay in native memory until the callback
        var waiter = new ReadGroupWaiter((long*)NativeMemory.Alloc((nuint)offsets.Length, sizeof(long)));
        offsets.CopyTo(new Span<long>(waiter.Offsets, offsets.Length));
        var handle = GCHandle.Alloc(waiter);
        if (Pal.rvn_pager_read_pages(pagerHandle, waiter.Offsets, offsets.Length, (ulong)GCHandle.ToIntPtr(handle)) == 1)
        {
            done = waiter.Task;
            return true;
        }
        handle.Free();
        NativeMemory.Free(waiter.Offsets);
        done = Task.CompletedTask;
        return false;
    }

    private sealed class ReadGroupWaiter(long* offsets) : TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)
    {
        public readonly long* Offsets = offsets;
    }

    // runs on the ring's submitter thread; each token is a GCHandle to a ReadGroupWaiter
    [UnmanagedCallersOnly]
    private static void OnReadsDone(ulong* tokens, int count)
    {
        for (var i = 0; i < count; i++)
        {
            try
            {
                var handle = GCHandle.FromIntPtr((nint)tokens[i]);
                var waiter = (ReadGroupWaiter)handle.Target;
                handle.Free();
                NativeMemory.Free(waiter!.Offsets);
                waiter.TrySetResult();
            }
            catch
            {
                // an exception must not reach the native thread
            }
        }
    }

    public void Dispose()
    {
        var handle = _handle;
        if (handle == null)
            return;
        var rc = Pal.rvn_prefetch_ring_destroy(handle, out var errorCode);
        if (rc != PalFlags.FailCodes.Success)
            PalHelper.ThrowLastError(rc, errorCode, "Failed to destroy the prefetch ring: data files are still open with it");
        _handle = null;
    }
}
