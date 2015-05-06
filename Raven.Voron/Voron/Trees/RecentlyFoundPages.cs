﻿// -----------------------------------------------------------------------
//  <copyright file="BinaryTree.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Diagnostics;

namespace Voron.Trees
{
    public class RecentlyFoundPages
    {
        public class FoundPage
        {
            public long Number;
			public Page Page;
			public MemorySlice FirstKey;
			public MemorySlice LastKey;
            public readonly long[] CursorPath;

            public FoundPage(int size)
            {
                CursorPath = new long[size];
            }
        }

        private readonly FoundPage[] _cache;

        private readonly int _cacheSize;

        private int current = 0;

        public RecentlyFoundPages(int cacheSize)
        {
            _cache = new FoundPage[cacheSize];
            _cacheSize = cacheSize;
        }

        public void Add(FoundPage page)
        {
#if DEBUG
            if ((page.FirstKey.Options == SliceOptions.BeforeAllKeys) && (page.LastKey.Options == SliceOptions.AfterAllKeys))
            {
                Debug.Assert(page.CursorPath.Length == 1);
            }
#endif

            int itemsLeft = _cacheSize;
            int position = current + _cacheSize;
            while (itemsLeft > 0)
            {
	            var itemIndex = position % _cacheSize;
	            var item = _cache[itemIndex];
                if (item == null || item.Number == page.Number)
                {
                    _cache[itemIndex] = page;
                    return;
                }

                itemsLeft--;
                position--;
            }

            current = (++current) % _cacheSize;
            _cache[current] = page;
        }

        public FoundPage Find(MemorySlice key)
        {
            int position = current;

            int itemsLeft = _cacheSize;
            while ( itemsLeft > 0 )
            {
                var page = _cache[position % _cacheSize];
                if (page == null)
                {
                    itemsLeft--;
                    position++;

                    continue;
                }

                var first = page.FirstKey;
                var last = page.LastKey;

                switch (key.Options)
                {
                    case SliceOptions.Key:
                        if ((first.Options != SliceOptions.BeforeAllKeys && key.Compare(first) < 0))
                            break;
                        if (last.Options != SliceOptions.AfterAllKeys && key.Compare(last) > 0)
                            break;
                        return page;
                    case SliceOptions.BeforeAllKeys:
                        if (first.Options == SliceOptions.BeforeAllKeys)
                            return page;
                        break;
                    case SliceOptions.AfterAllKeys:
                        if (last.Options == SliceOptions.AfterAllKeys)
                            return page;
                        break;
                    default:
                        throw new ArgumentException(key.Options.ToString());
                }

                itemsLeft--;
                position++;
            }

            return null;
        }

        public void Clear()
        {
            Array.Clear(_cache, 0, _cacheSize);
        }

	    public void Reset(long num)
	    {
		    for (int i = 0; i < _cache.Length; i++)
		    {
			    var page = _cache[i];
			    if (page != null && page.Number == num)
			    {
					page.Page = null;
				    return;
			    }
		    }
	    }
    }
}
