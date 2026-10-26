using Sparrow.Platform;
using Sparrow.Server.Platform;
using Voron.Global;

namespace Voron.Impl.Paging;

public unsafe partial class Pager
{
    // Data files are opened with random access advice, so a page fault reads only the 4 KB it touches. Where we know
    // that a whole page (or several) will be needed, we ask the PAL to start reading the exact range: on its prefetch ring
    // (fire and forget, it races the code that touches the pages), or with posix_fadvise where io_uring is not allowed.
    private static readonly bool PrefetchRingSupported = PlatformDetails.RunningOnLinux;

    /// <summary>Starts reading the pages into the page cache. Returns false if the request was dropped.</summary>
    public static bool PrefetchPages(State state, long pageNumber, int numberOfPages = 1)
    {
        if (PrefetchRingSupported == false)
            return false;
        return Pal.rvn_pager_prefetch(state.Handle, pageNumber * Constants.Storage.PageSize, (long)numberOfPages * Constants.Storage.PageSize) == 1;
    }
}
