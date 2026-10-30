using System;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Platform;
using Sparrow.Server.Platform;
using Voron.Global;

namespace Voron.Impl.Paging;

public unsafe partial class Pager
{
    // Data files on Linux are opened with `MADV_RANDOM`, faulting at 4 KB at a time (vs. Voron's 8kb pages)
    // This help us avoid separate faults
    private static readonly bool WillNeedSupported = PlatformDetails.RunningOnLinux;

    // Avoid repeated prefetching (syscall overhead only), slot is page num+1 (0 means empty)
    // duplicates will override, so it automatically refresh. Optimization only, not correctness, so we can afford to miss
    private const int RecentHintsBits = 12;
    private int[] _recentHints;

    // above this, one wait on the ring costs less than a non-blocking read of each page on the caller's thread
    private const int CheckedReadAheadLimit = 16;

    /// <summary>Completes once the pages are in the page cache. Recently requested pages are skipped.</summary>
    [SkipLocalsInit]
    public static ValueTask ReadPagesAhead(State state, ReadOnlySpan<long> pageNumbers)
    {
        if (WillNeedSupported == false || state.Pager.Options.RunningOn32Bits)
            return ValueTask.CompletedTask;

        var recent = state.Pager._recentHints ??= new int[1 << RecentHintsBits];
        long[] rented = null;
        Span<long> offsets = pageNumbers.Length <= 64 ? stackalloc long[pageNumbers.Length] : (rented = ArrayPool<long>.Shared.Rent(pageNumbers.Length));
        try
        {
            var count = 0;
            foreach (var pageNumber in pageNumbers)
            {
                ref var slot = ref RecentSlot(recent, pageNumber);
                var key = (int)(pageNumber + 1);
                if (Volatile.Read(ref slot) == key)
                    continue; // asked for a moment ago, or earlier in this group
                Volatile.Write(ref slot, key);
                offsets[count++] = pageNumber * Constants.Storage.PageSize;
            }
            if (count == 0)
                return ValueTask.CompletedTask;

            var ring = state.Pager._prefetchRing;
            if (count > CheckedReadAheadLimit)
            {
                if (ring is { CanWaitForPages: true })
                    return WaitForPages(state, ring, offsets[..count]);
                foreach (var offset in offsets[..count]) // nothing started these reads yet
                    Pal.rvn_pager_will_need(state.Handle, offset, Constants.Storage.PageSize);
                return ValueTask.CompletedTask;
            }

            int missing;
            fixed (long* p = offsets)
                missing = Pal.rvn_pager_start_reads(state.Handle, p, count);
            if (missing == 0 || ring is not { CanWaitForPages: true })
                return ValueTask.CompletedTask;
            return WaitForPages(state, ring, offsets[..missing]);
        }
        finally
        {
            if (rented != null)
                ArrayPool<long>.Shared.Return(rented);
        }
    }

    private static ValueTask WaitForPages(State state, PrefetchRing ring, ReadOnlySpan<long> offsets)
    {
        if (ring.TryRentSlot(out var slot))
            return ring.WaitForPages(slot, state.Handle, offsets);
        var copy = ArrayPool<long>.Shared.Rent(offsets.Length);
        offsets.CopyTo(copy);
        return ReadAheadSlotWait.WaitAsync(state, ring, copy, offsets.Length);
    }

    // the caller's transaction may have ended while it waited for the slot: the state's lock keeps the handle open
    internal static ValueTask WaitForPagesOnSlot(State state, PrefetchRing ring, int slot, ReadOnlySpan<long> offsets)
    {
        lock (state.WeakSelf)
        {
            if (state.Disposed == false)
                return ring.WaitForPages(slot, state.Handle, offsets);
        }
        ring.ReturnSlot(slot);
        return ValueTask.CompletedTask;
    }

    private static ref int RecentSlot(int[] recent, long pageNumber) =>
        ref recent[(int)(((ulong)pageNumber * 0x9E3779B97F4A7C15UL) >> (64 - RecentHintsBits))]; // Fibonacci hashing

    public static void WillNeedPages(State state, long pageNumber, int numberOfPages = 1)
    {
        if (WillNeedSupported == false || state.Pager.Options.RunningOn32Bits)
            return;

        var recent = state.Pager._recentHints ??= new int[1 << RecentHintsBits];
        ref var slot = ref RecentSlot(recent, pageNumber);
        var key = (int)(pageNumber + 1);
        if (Volatile.Read(ref slot) == key)
            return;
        Volatile.Write(ref slot, key);

        Pal.rvn_pager_will_need(state.Handle, pageNumber * Constants.Storage.PageSize, (long)numberOfPages * Constants.Storage.PageSize);
    }
}

internal static class ReadAheadSlotWait
{
    // all wait slots are taken: wait for one without holding a thread
    public static async ValueTask WaitAsync(Pager.State state, PrefetchRing ring, long[] offsets, int count)
    {
        try
        {
            var slot = await ring.RentSlotAsync().ConfigureAwait(false);
            await Pager.WaitForPagesOnSlot(state, ring, slot, offsets.AsSpan(0, count)).ConfigureAwait(false);
        }
        finally
        {
            ArrayPool<long>.Shared.Return(offsets);
        }
    }
}
