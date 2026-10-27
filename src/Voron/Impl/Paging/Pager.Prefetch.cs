using System.Threading;
using Sparrow.Platform;
using Sparrow.Server.Platform;
using Voron.Global;

namespace Voron.Impl.Paging;

public unsafe partial class Pager
{
    // Data files are opened with random access advice, so a page fault reads only the 4 KB it touches. Where we know
    // that a whole page (or several) will be needed, we queue a read of the exact range on the PAL's prefetch ring: fire and
    // forget, it races the code that touches the pages.
    private static readonly bool PrefetchRingSupported = PlatformDetails.RunningOnLinux;

    // Hot pages (index leaves, section headers) are hinted on almost every request, and each queued hint costs an io_uring
    // worker pass even when the page is in memory. So we remember the recently queued pages: a slot holds the page
    // number + 1 (0 is empty), and a page leaves the table when another page takes its slot, so a hot page is hinted
    // again after a while.
    private const int RecentHintsBits = 12;
    private int[] _recentHints;

    /// <summary>
    /// Starts reading the pages into the page cache. Returns false if the request was dropped. A range that starts at a
    /// page that was queued a moment ago is not queued again.
    /// </summary>
    public static bool PrefetchPages(State state, long pageNumber, int numberOfPages = 1)
    {
        if (PrefetchRingSupported == false)
            return false;

        var recent = state.Pager._recentHints ??= new int[1 << RecentHintsBits];
        ref var slot = ref recent[(int)(((ulong)pageNumber * 0x9E3779B97F4A7C15UL) >> (64 - RecentHintsBits))]; // Fibonacci hashing
        var key = (int)(pageNumber + 1); // a file of more than 16 TB may skip a hint on a collision of the low bits
        if (Volatile.Read(ref slot) == key)
            return true;

        if (Pal.rvn_pager_prefetch(state.Handle, pageNumber * Constants.Storage.PageSize, (long)numberOfPages * Constants.Storage.PageSize) != 1)
            return false; // dropped: a later hint may queue it

        Volatile.Write(ref slot, key);
        return true;
    }
}
