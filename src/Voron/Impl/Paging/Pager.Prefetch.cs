using System;
using System.Buffers;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Platform;
using Sparrow.Server.Platform;
using Voron.Global;

namespace Voron.Impl.Paging;

public unsafe partial class Pager
{
    // Data files are opened with random access advice, so a page fault reads only the 4 KB it touches. Where we know that
    // whole pages will be needed:
    //  - WillNeedPages: right before we touch them ourselves, a madvise starts the exact read (a request queued on the
    //    prefetch ring would arrive after the fault, and each 4 KB half would be read on its own);
    //  - ReadPagesAhead: before another thread (the transaction merger) touches them, the ring reads them and the caller
    //    waits for that without holding a thread, so the merger does not stall on the fault.
    private static readonly bool WillNeedSupported = PlatformDetails.RunningOnLinux;

    // Hot pages (index leaves, section headers) are asked for on almost every request, so we remember the recent ones:
    // a slot holds the page number + 1 (0 is empty), and a page leaves the table when another page takes its slot, so a
    // hot page is asked for again after a while.
    private const int RecentHintsBits = 12;
    private int[] _recentHints;

    /// <summary>
    /// Reads the pages into the page cache and returns a task that completes once they are all in, without holding a thread:
    /// the reads go through the prefetch ring as one read group, in parallel. Pages asked for a moment ago are skipped, and so
    /// is the wait when all of them are, so hot pages cost neither a system call nor a wait. Where the ring cannot take the
    /// group (no ring, queue full), the reads start with madvise and the task is already complete.
    /// </summary>
    public static Task ReadPagesAhead(State state, ReadOnlySpan<long> pageNumbers)
    {
        if (WillNeedSupported == false || state.Pager.Options.RunningOn32Bits)
            return Task.CompletedTask;

        var recent = state.Pager._recentHints ??= new int[1 << RecentHintsBits];
        long[] rented = null;
        Span<long> offsets = pageNumbers.Length <= 64 ? stackalloc long[pageNumbers.Length] : (rented = ArrayPool<long>.Shared.Rent(pageNumbers.Length));
        try
        {
            var count = 0;
            foreach (var pageNumber in pageNumbers)
            {
                if (pageNumber < 0)
                    continue;
                ref var slot = ref RecentSlot(recent, pageNumber);
                var key = (int)(pageNumber + 1); // a file of more than 16 TB may skip a page on a collision of the low bits
                if (Volatile.Read(ref slot) == key)
                    continue; // asked for a moment ago, or earlier in this group
                Volatile.Write(ref slot, key);
                offsets[count++] = pageNumber * Constants.Storage.PageSize;
            }

            if (count == 0)
                return Task.CompletedTask;
            if (PrefetchRing.TryReadPages(state.Handle, offsets[..count], out var done))
                return done;

            foreach (var offset in offsets[..count])
                StartRead(state, offset / Constants.Storage.PageSize, 1);
            return Task.CompletedTask;
        }
        finally
        {
            if (rented != null)
                ArrayPool<long>.Shared.Return(rented);
        }
    }

    private static ref int RecentSlot(int[] recent, long pageNumber) =>
        ref recent[(int)(((ulong)pageNumber * 0x9E3779B97F4A7C15UL) >> (64 - RecentHintsBits))]; // Fibonacci hashing

    /// <summary>
    /// Starts reading the pages into the page cache now (a madvise call) and returns. A range that starts at a page that
    /// was asked for a moment ago is skipped.
    /// </summary>
    public static void WillNeedPages(State state, long pageNumber, int numberOfPages = 1)
    {
        if (WillNeedSupported == false || state.Pager.Options.RunningOn32Bits)
            return;

        var recent = state.Pager._recentHints ??= new int[1 << RecentHintsBits];
        ref var slot = ref RecentSlot(recent, pageNumber);
        var key = (int)(pageNumber + 1); // a file of more than 16 TB may skip a request on a collision of the low bits
        if (Volatile.Read(ref slot) == key)
            return;

        if (StartRead(state, pageNumber, numberOfPages))
            Volatile.Write(ref slot, key);
    }

    private static bool StartRead(State state, long pageNumber, int numberOfPages)
    {
        var range = new PalDefinitions.PrefetchRanges(state.ReadAddress + pageNumber * Constants.Storage.PageSize,
            (long)numberOfPages * Constants.Storage.PageSize, state.Pager._prefetchRequestSize);
        return Pal.rvn_prefetch_ranges(&range, 1, out _) == PalFlags.FailCodes.Success;
    }
}
