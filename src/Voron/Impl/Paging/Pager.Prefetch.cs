using System.Threading;
using Sparrow.Platform;
using Sparrow.Server.Platform;
using Voron.Global;

namespace Voron.Impl.Paging;

public unsafe partial class Pager
{
    // Data files are opened with random access advice, so a page fault reads only the 4 KB it touches. Where we know
    // that a whole page (or several) will be needed soon, we ask the kernel for the exact range right away: the read is
    // in flight before the code that needs the pages runs, and that code waits at most for the rest of it. (A request
    // queued on the prefetch ring arrived after the fault: the merger then read each half of a page on its own.)
    private static readonly bool WillNeedSupported = PlatformDetails.RunningOnLinux;

    // Hot pages (index leaves, section headers) are asked for on almost every request, so we remember the recent ones:
    // a slot holds the page number + 1 (0 is empty), and a page leaves the table when another page takes its slot, so a
    // hot page is asked for again after a while.
    private const int RecentHintsBits = 12;
    private int[] _recentHints;

    /// <summary>
    /// Starts reading the pages into the page cache now (a madvise call) and returns. A range that starts at a page that
    /// was asked for a moment ago is skipped.
    /// </summary>
    public static void WillNeedPages(State state, long pageNumber, int numberOfPages = 1)
    {
        if (WillNeedSupported == false || state.Pager.Options.RunningOn32Bits)
            return;

        var recent = state.Pager._recentHints ??= new int[1 << RecentHintsBits];
        ref var slot = ref recent[(int)(((ulong)pageNumber * 0x9E3779B97F4A7C15UL) >> (64 - RecentHintsBits))]; // Fibonacci hashing
        var key = (int)(pageNumber + 1); // a file of more than 16 TB may skip a request on a collision of the low bits
        if (Volatile.Read(ref slot) == key)
            return;

        var range = new PalDefinitions.PrefetchRanges(state.ReadAddress + pageNumber * Constants.Storage.PageSize,
            (long)numberOfPages * Constants.Storage.PageSize, state.Pager._prefetchRequestSize);
        if (Pal.rvn_prefetch_ranges(&range, 1, out _) == PalFlags.FailCodes.Success)
            Volatile.Write(ref slot, key);
    }
}
