using System;
using Voron.Global;

namespace Voron.Data.Sets
{
    public readonly unsafe struct SetBranchPage
    {
        private readonly Page _page;

        public SetBranchPage(Page page)
        {
            _page = page;
        }

        public void Init()
        {
            var header = (SetBranchPageHeader*)_page.Pointer;
            header->Flags = PageFlags.Single | PageFlags.SetPage;
            header->SetFlags = SetPageFlags.Branch;
            header->Lower = PageHeader.SizeOf;
            header->Upper = Constants.Storage.PageSize;
        }

        public bool TryAdd(long key, long page)
        {
            var keyEncoder = new ZigZag();
            keyEncoder.Encode(key);
            var pageEncoder = new ZigZag();
            pageEncoder.
            
        }
    }
}
