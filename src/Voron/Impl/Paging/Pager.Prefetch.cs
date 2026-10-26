using Sparrow.Platform;
using Sparrow.Server.Platform;
using Voron.Global;

namespace Voron.Impl.Paging;

public unsafe partial class Pager
{
    // Data files are opened with random access advice, so a page fault reads only the 4 KB it touches. Where we know
    // that a whole page (or several) will be needed, we queue a read of the exact range on the PAL's prefetch ring:
    // it is fire and forget and races the code that touches the pages.
    private static readonly bool PrefetchRingSupported = PlatformDetails.RunningOnLinux;

    /// <summary>Queues an asynchronous read of the pages into the page cache. Returns false if the request was not queued.</summary>
    public static bool PrefetchPages(State state, long pageNumber, int numberOfPages = 1)
    {
        if (PrefetchRingSupported == false)
            return false;
        return Pal.rvn_pager_prefetch(state.Handle, pageNumber * Constants.Storage.PageSize, (long)numberOfPages * Constants.Storage.PageSize) == 1;
    }
}
