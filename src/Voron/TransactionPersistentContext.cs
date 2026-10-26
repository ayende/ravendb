using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Voron.Data.BTrees;
using Voron.Impl;

namespace Voron
{
    public sealed class TransactionPersistentContext(bool longLivedTransactions = false)
    {
        public bool LongLivedTransactions { get; set; } = longLivedTransactions;
        

        private readonly Stack<PageLocator> _pageLocators = new();
        

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public PageLocator AllocatePageLocator()
        {
            PageLocator locator;
            if (_pageLocators.Count != 0)
            {
                locator = _pageLocators.Pop();
                locator.Renew();
            }
            else
            {
                locator = new PageLocator();
            }
            return locator;
        }

        internal void FreePageLocator(PageLocator locator)
        {
            Debug.Assert(locator != null);
            if (_pageLocators.Count < 1024)
                _pageLocators.Push(locator);
        }

        // PERF: like the page locators, the trees of a transaction reuse the caches of the previous transactions of
        // this context, with no shared pool to synchronize with and no per tree registration to dispose them
        private readonly Stack<RecentlyFoundTreePages> _recentlyFoundPages = new();

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal RecentlyFoundTreePages AllocateRecentlyFoundPages()
        {
            // cleared when it was returned, see FreeRecentlyFoundPages
            return _recentlyFoundPages.TryPop(out var pages) ? pages : new RecentlyFoundTreePages();
        }

        internal void FreeRecentlyFoundPages(RecentlyFoundTreePages pages)
        {
            Debug.Assert(pages != null);
            pages.Clear();
            if (_recentlyFoundPages.Count < 1024)
                _recentlyFoundPages.Push(pages);
        }
    }
}
