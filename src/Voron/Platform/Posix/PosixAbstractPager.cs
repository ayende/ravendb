using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Voron.Data.BTrees;
using Voron.Impl.Paging;
using Voron.Platform.Posix;

namespace Voron.Platform.Posix
{
    public abstract class PosixAbstractPager : AbstractPager
    {
        protected PosixAbstractPager(int pageSize) : base(pageSize)
        {
        }

        public override unsafe void TryPrefetchingWholeFile()
        {
            if (Sparrow.Platform.Platform.CanPrefetch == false)
                return; // not supported

            var pagerState = PagerState;
            for (var i = 0; i < pagerState.AllocationInfos.Length; i++)
            {
                var ptr = pagerState.AllocationInfos[i].BaseAddress;
                if (
                    Syscall.madvise(new IntPtr(ptr), (int)pagerState.AllocationInfos[i].Size, MAdvFlags.MADV_WILLNEED) ==
                    -1)
                {
                    // TODO :: ignore error ?
                    var err = Marshal.GetLastWin32Error();
                    PosixHelper.ThrowLastError(err);
                }
            }
        }

        public override unsafe void MaybePrefetchMemory(List<TreePage> sortedPages)
        {
            if (Sparrow.Platform.Platform.CanPrefetch == false)
                return; // not supported

            if (sortedPages.Count == 0)
                return;

            long lastPage = -1;
            const int numberOfPagesInBatch = 8;
            var sizeInPages = numberOfPagesInBatch; // OS uses 32K when you touch a page, let us reuse this
            foreach (var page in sortedPages)
            {
                if (lastPage == -1)
                {
                    lastPage = page.PageNumber;
                }

                var numberOfPagesInLastPage = page.IsOverflow == false
                    ? 1
                    : this.GetNumberOfOverflowPages(page.OverflowSize);

                var endPage = page.PageNumber + numberOfPagesInLastPage - 1;

                if (endPage <= lastPage + sizeInPages)
                    continue; // already within the allocation granularity we have

                if (page.PageNumber <= lastPage + sizeInPages + numberOfPagesInBatch)
                {
                    while (endPage > lastPage + sizeInPages)
                    {
                        sizeInPages += numberOfPagesInBatch;
                    }

                    continue;
                }

                var ptr = (IntPtr)AcquirePagePointer(null, lastPage);
                if (Syscall.madvise(ptr, sizeInPages*PageSize, MAdvFlags.MADV_WILLNEED) == -1)
                {
                    // TODO :: ignore error ?
                    var err = Marshal.GetLastWin32Error();
                    PosixHelper.ThrowLastError(err);
                }

                lastPage = page.PageNumber;
                sizeInPages = numberOfPagesInBatch;
                while (endPage > lastPage + sizeInPages)
                {
                    sizeInPages += numberOfPagesInBatch;
                }
            }

            var ptrLastPage = (IntPtr)AcquirePagePointer(null, lastPage);
            if (Syscall.madvise(ptrLastPage, sizeInPages*PageSize, MAdvFlags.MADV_WILLNEED) == -1)
            {
                // TODO :: ignore error ?
                var err = Marshal.GetLastWin32Error();
                PosixHelper.ThrowLastError(err);
            }
        }

        public override unsafe void MaybePrefetchMemory(List<long> pagesToPrefetch)
        {
            if (Sparrow.Platform.Platform.CanPrefetch == false)
                return; // not supported

            if (pagesToPrefetch.Count == 0)
                return;

            for (int i = 0; i < pagesToPrefetch.Count; i++)
            {
                var ptr = (IntPtr)AcquirePagePointer(null, pagesToPrefetch[i]);
                if (Syscall.madvise(ptr, 4 * PageSize, MAdvFlags.MADV_WILLNEED) == -1)
                {
                    // TODO :: ignore error ?
                    var err = Marshal.GetLastWin32Error();
                    PosixHelper.ThrowLastError(err);
                }
            }
        }
    }
}