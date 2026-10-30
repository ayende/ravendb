using System;
using System.Collections.Generic;
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

    private const int MaxReadAheadPages = 8; // the PAL's read group size

    // Hot pages (index leaves, section headers) are asked for on almost every request, so we remember the recent ones:
    // a slot holds the page number + 1 (0 is empty), and a page leaves the table when another page takes its slot, so a
    // hot page is asked for again after a while.
    private const int RecentHintsBits = 12;
    private int[] _recentHints;

    /// <summary>
    /// Reads the pages into the page cache and returns a task that completes once they are all in, without holding a thread:
    /// the reads go through the prefetch ring, in parallel, in groups of up to 8 pages. Pages asked for a moment ago are
    /// skipped, and so is the wait when all of them are, so hot pages cost neither a system call nor a wait. Where the ring
    /// cannot take a group (no ring, queue full), its reads start with madvise and it does not delay the task.
    /// </summary>
    public static Task ReadPagesAhead(State state, ReadOnlySpan<long> pageNumbers)
    {
        if (WillNeedSupported == false || state.Pager.Options.RunningOn32Bits)
            return Task.CompletedTask;

        var recent = state.Pager._recentHints ??= new int[1 << RecentHintsBits];
        Span<long> group = stackalloc long[MaxReadAheadPages];
        var count = 0;
        Task pending = null;
        List<Task> morePending = null;
        foreach (var pageNumber in pageNumbers)
        {
            if (pageNumber < 0)
                continue;
            if (Volatile.Read(ref RecentSlot(recent, pageNumber)) == (int)(pageNumber + 1))
                continue; // asked for a moment ago (a page repeated across groups is caught here too)
            var offset = pageNumber * Constants.Storage.PageSize;
            if (group[..count].Contains(offset))
                continue;
            group[count++] = offset;
            if (count == MaxReadAheadPages)
            {
                Track(ReadGroup(state, recent, group), ref pending, ref morePending);
                count = 0;
            }
        }
        if (count > 0)
            Track(ReadGroup(state, recent, group[..count]), ref pending, ref morePending);

        if (morePending != null)
            return Task.WhenAll(morePending);
        return pending ?? Task.CompletedTask;

        static void Track(Task read, ref Task pending, ref List<Task> morePending)
        {
            if (read.IsCompleted)
                return;
            if (pending == null)
            {
                pending = read;
                return;
            }
            morePending ??= [pending];
            morePending.Add(read);
        }
    }

    private static Task ReadGroup(State state, int[] recent, ReadOnlySpan<long> offsets)
    {
        if (PrefetchRing.TryReadPages(state.Handle, offsets, out var done) == false)
        {
            foreach (var offset in offsets)
                WillNeedPages(state, offset / Constants.Storage.PageSize);
            return Task.CompletedTask;
        }

        foreach (var offset in offsets)
        {
            var pageNumber = offset / Constants.Storage.PageSize;
            Volatile.Write(ref RecentSlot(recent, pageNumber), (int)(pageNumber + 1));
        }
        return done;
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

        var range = new PalDefinitions.PrefetchRanges(state.ReadAddress + pageNumber * Constants.Storage.PageSize,
            (long)numberOfPages * Constants.Storage.PageSize, state.Pager._prefetchRequestSize);
        if (Pal.rvn_prefetch_ranges(&range, 1, out _) == PalFlags.FailCodes.Success)
            Volatile.Write(ref slot, key);
    }
}
