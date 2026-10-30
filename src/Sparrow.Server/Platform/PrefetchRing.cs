using System;
using System.Numerics;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
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

    private const int WaitSlots = 1024;

    private void* _handle;
    private GCHandle _self;
    private readonly bool _usesIoUring;
    private readonly WaitSlot[] _slots;
    private readonly ulong[] _busySlots;

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
        _self = GCHandle.Alloc(this);
        var rc = Pal.rvn_prefetch_ring_create(ringEntries, maxFiles, queueSize, &OnReadsDone, (void*)GCHandle.ToIntPtr(_self), out var handle, out var errorCode);
        if (rc != PalFlags.FailCodes.Success)
        {
            _self.Free();
            PalHelper.ThrowLastError(rc, errorCode, $"Failed to create a prefetch ring ({ringEntries} entries, {maxFiles} files, queue of {queueSize})");
        }
        _handle = handle;

        _usesIoUring = GetStats()?.UsesIoUring == true;
        if (_usesIoUring == false)
            return; // the posix_fadvise thread cannot tell when a read completes

        _slots = new WaitSlot[WaitSlots];
        for (var i = 0; i < _slots.Length; i++)
            _slots[i] = new WaitSlot(this, i);
        _busySlots = new ulong[WaitSlots / 64];
    }

    public PrefetchRingStats? GetStats()
    {
        if (_handle == null || Pal.rvn_prefetch_ring_stats(_handle, out var submitted, out var droppedFull, out var droppedStale, out var failed,
                out var readGroups, out var reads, out var usesIoUring) != PalFlags.FailCodes.Success)
            return null;
        return new PrefetchRingStats(submitted, droppedFull, droppedStale, failed, readGroups, reads, usesIoUring != 0);
    }

    /// <summary>
    /// After <see cref="Pal.rvn_pager_start_reads"/>: completes once the pages at <paramref name="offsets"/> are in the page
    /// cache. Completes at once when the ring cannot wait for them; their reads are already started.
    /// </summary>
    public ValueTask WaitForPagesAsync(void* pagerHandle, ReadOnlySpan<long> offsets)
    {
        if (_usesIoUring == false || TryRentSlot(out var slot) == false)
            return ValueTask.CompletedTask;

        var buffer = slot.GetOffsets(offsets.Length);
        offsets.CopyTo(new Span<long>(buffer, offsets.Length));
        var version = slot.Version;
        if (Pal.rvn_pager_read_pages(pagerHandle, buffer, offsets.Length, (ulong)slot.Index) != 1)
        {
            ReleaseSlot(slot.Index);
            return ValueTask.CompletedTask;
        }
        return new ValueTask(slot, version);
    }

    private bool TryRentSlot(out WaitSlot slot)
    {
        var words = _busySlots;
        var start = (uint)Environment.CurrentManagedThreadId; // spreads concurrent callers over the words
        for (var n = 0u; n < words.Length; n++)
        {
            var w = (int)((start + n) % (uint)words.Length);
            var bits = Volatile.Read(ref words[w]);
            while (bits != ulong.MaxValue)
            {
                var bit = BitOperations.TrailingZeroCount(~bits);
                var seen = Interlocked.CompareExchange(ref words[w], bits | (1UL << bit), bits);
                if (seen == bits)
                {
                    slot = _slots[w * 64 + bit];
                    return true;
                }
                bits = seen;
            }
        }
        slot = null;
        return false;
    }

    private void ReleaseSlot(int index) => Interlocked.And(ref _busySlots[index / 64], ~(1UL << (index % 64)));

    // the ring's slot table holds the offsets until the callback: the submitter reads them after rvn_pager_read_pages returns
    private sealed class WaitSlot(PrefetchRing ring, int index) : IValueTaskSource
    {
        public readonly int Index = index;
        private ManualResetValueTaskSourceCore<bool> _core = new() { RunContinuationsAsynchronously = true };
        private long* _offsets;
        private int _capacity;

        public short Version => _core.Version;

        public long* GetOffsets(int count)
        {
            if (count > _capacity)
            {
                _capacity = Math.Max(count, Math.Max(16, _capacity * 2));
                _offsets = (long*)NativeMemory.Realloc(_offsets, (nuint)_capacity * sizeof(long));
            }
            return _offsets;
        }

        public void Complete() => _core.SetResult(true);

        public void GetResult(short token)
        {
            _core.GetResult(token);
            _core.Reset();
            ring.ReleaseSlot(Index);
        }

        public ValueTaskSourceStatus GetStatus(short token) => _core.GetStatus(token);

        public void OnCompleted(Action<object> continuation, object state, short token, ValueTaskSourceOnCompletedFlags flags) =>
            _core.OnCompleted(continuation, state, token, flags);

        public void FreeOffsets()
        {
            NativeMemory.Free(_offsets);
            _offsets = null;
            _capacity = 0;
        }
    }

    // runs on the ring's submitter thread; the context is the ring's GCHandle and each token is a slot index
    [UnmanagedCallersOnly]
    private static void OnReadsDone(void* context, ulong* tokens, int count)
    {
        var ring = (PrefetchRing)GCHandle.FromIntPtr((nint)context).Target;
        for (var i = 0; i < count; i++)
        {
            try
            {
                ring!._slots[(int)tokens[i]].Complete();
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
        _self.Free();
        if (_slots != null)
        {
            foreach (var slot in _slots)
                slot.FreeOffsets();
        }
    }
}
