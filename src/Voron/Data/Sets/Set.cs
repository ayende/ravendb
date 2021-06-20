using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Sparrow;
using Sparrow.Server;
using Sparrow.Server.Platform.Posix.macOS;
using Voron.Debugging;
using Voron.Global;
using Voron.Impl;

namespace Voron.Data.Sets
{
    public unsafe class Set
    {
        private LowLevelTransaction _llt;
        private SetState _state;
        private SetCursorState[] _stk = new SetCursorState[8];
        private int _pos = -1, _len;
        
        internal SetState State => _state;
        internal LowLevelTransaction Llt => _llt;

        private Set()
        {
        }

        public static Set Create(LowLevelTransaction llt, string name)
        {
            using var _ = Slice.From(llt.Allocator, name, out var slice);
            return Create(llt, slice);
        }
        public static Set Create(LowLevelTransaction llt, Slice name)
        {
            SetState* header;
            var existing = llt.RootObjects.Read(name);
            if (existing == null)
            {
                var newPage = llt.AllocatePage(1);
                new SetLeafPage(newPage.Pointer).Init(0);
                using var _ = llt.RootObjects.DirectAdd(name, sizeof(SetState), out var p);
                header = (SetState*)p;
                *header = new SetState
                {
                    RootObjectType = RootObjectType.Set,
                    Depth = 1,
                    BranchPages = 0,
                    LeafPages = 1,
                    RootPage = newPage.PageNumber,
                    NumberOfEntries = 0,
                };
            }
            else
            {
                header = (SetState*)existing.Reader.Base;
            }

            if (header->RootObjectType != RootObjectType.Set)
                throw new InvalidOperationException($"Tried to open {name} as a set, but it is actually a " +
                                                    header->RootObjectType);

            return new Set
            {
                _llt = llt,
                _state = *header
            };
        }

        public void Remove(long val)
        {
            FindPageFor(val);
            ref var state = ref _stk[_pos];
            state.Page = _llt.ModifyPage(state.Page.PageNumber);
            var leaf = new SetLeafPage(state.Page.Pointer);
            if (leaf.IsValidValue(val))
                leaf.Remove(_llt, val);

            if (_pos == 0)
                return;  // this is the root page

            if (leaf.SpaceUsed > Constants.Storage.PageSize / 4)
                return; // don't merge too eagerly

            MaybeMergeLeafPage(in leaf);
        }
        
        private void MaybeMergeLeafPage(in SetLeafPage leaf)
        {
            PopPage();
            
            ref var parent = ref _stk[_pos];
            var siblingIdx = parent.LastSearchPosition == 0 ? 1 : parent.LastSearchPosition - 1;
            var branch = new SetBranchPage(parent.Page.Pointer);
            Debug.Assert(branch.Header->NumberOfEntries >= 2);
            var (_, siblingPageNum) = branch.GetByIndex(siblingIdx);
            var siblingPage = _llt.GetPage(siblingPageNum);
            var siblingHeader = (SetLeafPageHeader*)siblingPage.Pointer;
            if (siblingHeader->SetFlags != SetPageFlags.Leaf)
                return;
            var sibling = new SetLeafPage(siblingPage.Pointer);
            // if the two pages together will be bigger than 75%, can skip merging
            if (sibling.SpaceUsed + leaf.SpaceUsed > Constants.Storage.PageSize / 2 + Constants.Storage.PageSize / 4)
                return;
            Span<int> scratch = stackalloc int[PForEncoder.BufferLen];
            var it = sibling.GetIterator(scratch);
            while (it.MoveNext(out var v))
            {
                var success = leaf.Add(_llt, v);
                Debug.Assert(success);
            }

            _state.LeafPages--;
            
            MergeSiblingsAtParent();
        }

        private void MergeSiblingsAtParent()
        {
            ref var state = ref _stk[_pos];
            var current = new SetBranchPage(state.Page.Pointer);
            Debug.Assert(current.Header->SetFlags == SetPageFlags.Branch);
            var (leafKey, leafPageNum) = current.GetByIndex(state.LastSearchPosition);
            var (siblingKey, siblingPageNum) = current.GetByIndex(GetSiblingIndex(in state));

            state.Page = _llt.ModifyPage(state.Page.PageNumber);
            var siblingPageHeader = (SetLeafPageHeader*)_llt.GetPage(siblingPageNum).Pointer;
            if (siblingPageHeader->SetFlags == SetPageFlags.Branch)
                _state.BranchPages--;
            else
                _state.LeafPages--;

            _llt.FreePage(siblingPageNum);
            current.Remove(siblingKey);
            current.Remove(leafKey);

            // if it is empty, can just replace with the child
            if (current.Header->NumberOfEntries == 0)
            {
                var leafPage = _llt.GetPage(leafPageNum);
                long cpy = state.Page.PageNumber;
                leafPage.AsSpan().CopyTo(state.Page.AsSpan());
                state.Page.PageNumber = cpy;
                _state.BranchPages--;
                _llt.FreePage(leafPageNum);
                return;
            }

            var newKey = Math.Min(siblingKey, leafKey);
            var success = current.TryAdd(_llt, newKey, leafPageNum);
            Debug.Assert(success);

            if (_pos == 0)
                return; // root has no siblings

            if (current.Header->NumberOfEntries > SetBranchPage.MinNumberOfValuesBeforeMerge)
                return;

            PopPage();
            ref var parent = ref _stk[_pos];
            var siblingIdx = GetSiblingIndex(parent);
            var gp = new SetBranchPage(parent.Page.Pointer);
            (_, siblingPageNum) = gp.GetByIndex(siblingIdx);
            var siblingPage = _llt.GetPage(siblingPageNum);
            var siblingHeader = (SetLeafPageHeader*)siblingPage.Pointer;
            if (siblingHeader->SetFlags != SetPageFlags.Branch)
                return;// cannot merge leaf & branch
            var sibling = new SetBranchPage(siblingPage.Pointer);
            if (sibling.Header->NumberOfEntries + current.Header->NumberOfEntries > SetBranchPage.MinNumberOfValuesBeforeMerge * 2)
                return; // not enough space to _ensure_ that we can merge

            for (int i = 0; i < sibling.Header->NumberOfEntries; i++)
            {
                (long key, long page) = sibling.GetByIndex(i);
                success = current.TryAdd(_llt, key, page);
                Debug.Assert(success);
            }

            MergeSiblingsAtParent();
        }

        private static int GetSiblingIndex(in SetCursorState parent)
        {
            return parent.LastSearchPosition == 0 ? 1 : parent.LastSearchPosition - 1;
        }

        public void Add(long value)
        {
            if (value < 0)
                throw new ArgumentOutOfRangeException(nameof(value), "Only positive values are allowed");
            
            FindPageFor(value);
            AddToPage(value);
        }

        private void AddToPage(long value)
        {
            ref var state = ref _stk[_pos];
            
            state.Page = _llt.ModifyPage(state.Page.PageNumber);
            
            var leafPage = new SetLeafPage(state.Page.Pointer);
            if (leafPage.IsValidValue(value) && // may have enough space, but too far out to fit 
                leafPage.Add(_llt, value))
                return; // successfully added

            var (separator, newPage) = SplitLeafPage(value);
            AddToParentPage(separator, newPage);
        }

        private void AddToParentPage(long separator, long newPage)
        {
            if (_pos == 0) // need to create a root page
            {
                CreateRootPage();
            }

            PopPage();
            ref var state = ref _stk[_pos];
            var parent = new SetBranchPage(state.Page.Pointer);
            if (parent.TryAdd(_llt, separator, newPage))
                return;

            SplitBranchPage(separator, newPage);
        }

        private void SplitBranchPage(long key,  long value)
        {
            ref var state = ref _stk[_pos];

            var pageToSplit = new SetBranchPage(state.Page.Pointer);
            var page = _llt.AllocatePage(1);
            var branch = new SetBranchPage(page.Pointer);
            branch.Init();
            _state.BranchPages++;
            bool success;

            // grow rightward
            if (key > pageToSplit.Last)
            {
                 success = branch.TryAdd(_llt, key, value);
                Debug.Assert(success);
                AddToParentPage(key, value);
                return;
            }

            // grow leftward
            if (key < pageToSplit.First)
            {
                long oldFirst = pageToSplit.First;
                var cpy = page.PageNumber;
                state.Page.AsSpan().CopyTo(page.AsSpan());
                page.PageNumber = cpy;

                cpy = state.Page.PageNumber;
                state.Page.AsSpan().Clear();
                state.Page.PageNumber = cpy;
                
                var curPage = new SetBranchPage(state.Page.Pointer);
                curPage.Init();
                 success = curPage.TryAdd(_llt, key, value);
                Debug.Assert(success);
                AddToParentPage(oldFirst, page.PageNumber);
                return;
            }
            
            // split in half
            for (int i = pageToSplit.Header->NumberOfEntries/2; i < pageToSplit.Header->NumberOfEntries; i++)
            {
                var (k, v) = pageToSplit.GetByIndex(i);
                 success = branch.TryAdd(_llt, k, v);
                Debug.Assert(success);
            }

            pageToSplit.Header->NumberOfEntries /= 2;// truncate entries
            success = pageToSplit.Last > key ?
                branch.TryAdd(_llt, key, value) :
                pageToSplit.TryAdd(_llt, key, value);
            Debug.Assert(success);
            
            AddToParentPage(branch.First, page.PageNumber);
        }

        private (long Separator, long NewPage) SplitLeafPage(long value)
        {
            ref var state = ref _stk[_pos];
            var curPage = new SetLeafPage(state.Page.Pointer);
            var (first, last) = curPage.GetRange();
            _state.LeafPages++;

            if (value  >=  first && value <= last)
            {
                return SplitLeafPageInHalf(value, curPage, state);
            }

            bool success;
            Page page;
            if (value > last)
            {
                // optimize sequential writes, can create a new page directly
                page = _llt.AllocatePage(1);
                var newPage = new SetLeafPage(page.Pointer);
                newPage.Init(value);
                success = newPage.Add(_llt, value);
                Debug.Assert(success);
                return (value, page.PageNumber);
            }
            Debug.Assert((first < value));
            // smaller than current, we'll move the higher values to the new location
            // instead of update the entry position
            page = _llt.AllocatePage(1);
            var cpy = page.PageNumber;
            curPage.Span.CopyTo(page.AsSpan());
            page.PageNumber = cpy;

            cpy = state.Page.PageNumber;
            curPage.Span.Clear();
            state.Page.PageNumber = cpy;

            curPage.Init(value);
            success = curPage.Add(_llt, value);
            Debug.Assert(success);
            return (first, page.PageNumber);
        }

        private (long Separator, long NewPage) SplitLeafPageInHalf(long value, SetLeafPage curPage, SetCursorState state)
        {
            // we have to split this in the middle page
            var page = _llt.AllocatePage(1);
            var newPage = new SetLeafPage(page.Pointer);
            long baseline = curPage.Header->Baseline;
            newPage.Init(baseline);

            using var _ = _llt.Environment.GetTemporaryPage(_llt, out var tmpPage);
            state.Page.AsSpan().CopyTo(tmpPage.AsSpan());
            var cpy = state.Page.PageNumber;
            state.Page.AsSpan().Clear();
            state.Page.PageNumber = cpy;
            curPage.Init(baseline); // empty current page

            var originalPageCopy = new SetLeafPage(tmpPage.TempPagePointer);
            Span<int> scratch = stackalloc int[PForEncoder.BufferLen];
            var it = originalPageCopy.GetIterator(scratch);


            bool success;
            var midway = long.MinValue;
            var left = true;
            var dest = curPage;
            while (it.MoveNext(out var v))
            {
                success = dest.Add(_llt, v);
                Debug.Assert(success);
                if (!left) continue;
                // we want to fill about half the page
                if (Constants.Storage.PageSize / 2 > dest.Header->CompressedValuesCeiling)
                    continue;

                // now write the entries to the other side
                left = false;
                dest = newPage;
                midway = baseline | v;
            }
            if (value > midway)
            {
                success =  newPage.Add(_llt, value);
            }
            else
            {
                success= curPage.Add(_llt, value);
            }
            Debug.Assert(success);

            return (midway, page.PageNumber);
        }



        [Conditional("DEBUG")]
        public void Render()
        {
            DebugStuff.RenderAndShow(this);
        }

        private void CreateRootPage()
        {
            _state.Depth++;
            _state.BranchPages++;
            // we'll copy the current page and reuse it, to avoid changing the root page number
            var page = _llt.AllocatePage(1);
            long cpy = page.PageNumber;
            ref var state = ref _stk[_pos];
            Memory.Copy(page.Pointer, state.Page.Pointer, Constants.Storage.PageSize);
            page.PageNumber = cpy;
            Memory.Set(state.Page.DataPointer, 0, Constants.Storage.PageSize - PageHeader.SizeOf);
            var rootPage = new SetBranchPage(state.Page.Pointer);
            rootPage.Init();
            rootPage.TryAdd(_llt, long.MinValue, cpy);
            _state.BranchPages++;
            InsertToStack(new SetCursorState
            {
                Page = page,
                LastMatch = state.LastMatch,
                LastSearchPosition = state.LastSearchPosition
            });
            state.LastMatch = -1;
            state.LastSearchPosition = 0;
        }

        private void InsertToStack(SetCursorState newPageState)
        {
            // insert entry and shift other elements
            if (_len + 1 >= _stk.Length)// should never happen
                Array.Resize(ref _stk, _stk.Length * 2); // but let's handle it
            Array.Copy(_stk, _pos + 1, _stk, _pos + 2, _len - (_pos + 1));
            _len++;
            _stk[_pos + 1] = newPageState;
            _pos++;
        }

        private void FindPageFor(long value)
        {
            _pos = -1;
            _len = 0;
            PushPage(_state.RootPage);
            ref var state = ref _stk[_pos];

            while (state.IsLeaf == false)
            {
                SearchPageAndPushNext(value);
                state = ref _stk[_pos];
            }
        }

        private void SearchPageAndPushNext(long value)
        {
            ref var state = ref _stk[_pos];

            var branch = new SetBranchPage(state.Page.Pointer);
            long nextPage;
            (nextPage, state.LastSearchPosition, state.LastMatch) = branch.SearchPage(value);

            PushPage(nextPage);
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void PopPage()
        {
            _stk[_pos--] = default;
            _len--;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void PushPage(long nextPage)
        {
            if (_pos + 1 >= _stk.Length) //  should never actually happen
                Array.Resize(ref _stk, _stk.Length * 2); // but let's be safe
            Page page = _llt.GetPage(nextPage);
            _stk[++_pos] = new SetCursorState { Page = page, };
            _len++;
        }

        public Iterator Iterate()
        {
            return new Iterator(this);
        }

        public ref  struct Iterator 
        {
            private readonly Set _parent;
            private readonly Span<int> _scratch;
            private ByteStringContext<ByteStringMemoryCache>.InternalScope _scope;
            private SetLeafPage.Iterator _it;
            
            public long Current;

            public Iterator(Set parent)
            {
                _parent = parent;
                _scope = _parent._llt.Allocator.Allocate(PForEncoder.BufferLen, out _scratch);
                Current = default;
                _it = default;
            }

            public bool Seek(long from)
            {
                _parent.FindPageFor(from);
                ref var state = ref _parent._stk[_parent._pos];
                var leafPage = new SetLeafPage(state.Page.Pointer);
                _it = leafPage.GetIterator(_scratch);
                _it.SkipTo(from);
                while (_it.MoveNext(out var v))
                {
                    if (v < from) 
                        continue;
                    Current = v;
                    return true;
                }
                return false;
            }

            public bool MoveNext()
            {
                if (_it.MoveNext(out Current))
                    return true;

                while (true)
                {
                    if (_parent._pos == 0)
                        return false;
                    
                    _parent.PopPage();
                    ref var state = ref _parent._stk[_parent._pos];
                    state.LastSearchPosition++;
                    Debug.Assert(state.IsLeaf == false);
                    if (state.LastSearchPosition >= state.BranchHeader->NumberOfEntries) 
                        continue;
                    
                    var branch = new SetBranchPage(state.Page.Pointer);
                    (_, long pageNum) = branch.GetByIndex(state.LastSearchPosition);
                    var page = _parent._llt.GetPage(pageNum);
                    var header = (SetLeafPageHeader*)page.Pointer;
                    _parent.PushPage(pageNum);
                    if (header->SetFlags == SetPageFlags.Branch)
                    {
                        // we'll increment on the next
                        _parent._stk[_parent._pos].LastSearchPosition = -1;
                        continue;
                    }
                    _parent.PushPage(pageNum);
                    _it = new SetLeafPage(page.Pointer).GetIterator(_scratch);
                    if (_it.MoveNext(out Current))
                        return true;
                }                
            }

            public void Reset()
            {
                throw new NotSupportedException();
            }

            public void Dispose()
            {
                _scope.Dispose();
            }
        }
    }
}
