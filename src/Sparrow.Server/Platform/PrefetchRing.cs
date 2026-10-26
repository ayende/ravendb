using System;
using Sparrow.Binary;
using Sparrow.Platform;

namespace Sparrow.Server.Platform;

/// <summary>
/// An io_uring that the PAL uses to read data file ranges into the page cache. The process uses <see cref="Default"/>;
/// tests can create their own. Where io_uring is not available (an old kernel, a seccomp policy, Windows, macOS)
/// <see cref="Handle"/> is null and the PAL starts the reads with posix_fadvise instead.
/// A ring can only be disposed after every data file registered with it was closed.
/// </summary>
public sealed unsafe class PrefetchRing : IDisposable
{
    public static readonly PrefetchRing Default = new(PalConfiguration.PrefetchRingEntries, PalConfiguration.PrefetchMaxFiles, PalConfiguration.PrefetchQueueSize);

    public void* Handle { get; private set; }

    /// <summary>The number of data files that can be registered with the ring at the same time.</summary>
    public int MaxFiles { get; }

    public PrefetchRing(int ringEntries, int maxFiles, int queueSize)
    {
        MaxFiles = maxFiles;
        if (PlatformDetails.RunningOnLinux == false)
            return;

        queueSize = Bits.PowerOf2(queueSize); // the PAL indexes the queue by mask
        var rc = Pal.rvn_prefetch_ring_create(ringEntries, maxFiles, queueSize, out var handle, out var errorCode);
        if (rc == PalFlags.FailCodes.Success)
            Handle = handle;
        else if (rc != PalFlags.FailCodes.FailCreateIoRing) // io_uring is not allowed here: no ring, posix_fadvise instead
            PalHelper.ThrowLastError(rc, errorCode, $"Failed to create a prefetch ring ({ringEntries} entries, {maxFiles} files, queue of {queueSize})");
    }

    public bool TryGetStats(out long submitted, out long droppedFull, out long droppedStale, out long failed)
    {
        submitted = droppedFull = droppedStale = failed = 0;
        return Handle != null && Pal.rvn_prefetch_ring_stats(Handle, out submitted, out droppedFull, out droppedStale, out failed) == PalFlags.FailCodes.Success;
    }

    public void Dispose()
    {
        var handle = Handle;
        if (handle == null)
            return;
        var rc = Pal.rvn_prefetch_ring_destroy(handle, out var errorCode);
        if (rc != PalFlags.FailCodes.Success)
            PalHelper.ThrowLastError(rc, errorCode, "Failed to destroy the prefetch ring: data files are still registered with it");
        Handle = null;
    }
}
