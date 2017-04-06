using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Runtime.InteropServices;
using Sparrow;
using Sparrow.Collections;
using Sparrow.Collections.LockFree;
using Sparrow.Platform;
using Sparrow.Platform.Posix;
using Sparrow.Platform.Win32;
using Voron.Global;
using Voron.Platform.Posix;

namespace Voron.Impl.Paging
{
    public unsafe class PureMemoryPager : AbstractPager
    {
        private long _totalAllocationSize;
        private bool _memoryBelongsToSomeoneElse;

        public PureMemoryPager(StorageEnvironmentOptions options, string name) : base(options)
        {
            
            FileName = name;
            SetPagerState(CreatePagerState(4 * Constants.Storage.PageSize));
        }

        public PureMemoryPager(StorageEnvironmentOptions options, string name, byte*ptr, long size) : base(options)
        {
            _memoryBelongsToSomeoneElse = true;
            FileName = name;
            SetPagerState(new PagerState(this)
            {
                MapBase = ptr,
                AllocationInfos = new[] { new PagerState.AllocationInfo
                {
                    BaseAddress = ptr,
                    Size = size
                } }
            });
        }

        private PagerState CreatePagerState(long sizeInBytes)
        {
            var ptr = Alloc(sizeInBytes);
            _totalAllocationSize = sizeInBytes;
            NumberOfAllocatedPages = _totalAllocationSize / Constants.Storage.PageSize;
            if (ptr == null)
                throw new OutOfMemoryException("Could not allocate any longer");

            var newPager = new PagerState(this)
            {
                MapBase = ptr,
                AllocationInfos = new[] 
                {
                    new PagerState.AllocationInfo
                    {
                        BaseAddress = ptr,
                        Size = sizeInBytes
                    }
                }
            };
            return newPager;
        }

        private byte* Alloc(long size)
        {
            if (PlatformDetails.RunningOnPosix)
            {
                var result = Syscall.mmap64(IntPtr.Zero, (UIntPtr)size, MmapProts.PROT_READ | MmapProts.PROT_WRITE, MmapFlags.MAP_ANONYMOUS, 0, 0);
                if(result == IntPtr.Zero)
                    throw new OutOfMemoryException("Could not allocate memory", new Win32Exception(Marshal.GetLastWin32Error()));
                return (byte*)result;
            }
            var ptr = Win32MemoryProtectMethods.VirtualAlloc(null, (UIntPtr)size, Win32MemoryProtectMethods.AllocationType.COMMIT, Win32MemoryProtectMethods.MemoryProtection.READWRITE);
            if(ptr == null)
                throw new OutOfMemoryException("Could not allocate memory", new Win32Exception(Marshal.GetLastWin32Error()));
            return ptr;
        }

        public override long TotalAllocationSize => _totalAllocationSize;

        protected override string GetSourceName()
        {
            return ":mem: - " + FileName;
        }

        public override void Sync(long totalUnsynced)
        {
        }

        protected override PagerState AllocateMorePages(long newLength)
        {
            lock (this)
            {
                if (newLength <= _totalAllocationSize)
                    return null;
                var pagerState = CreatePagerState(newLength);

                Memory.Copy(pagerState.MapBase, PagerState.MapBase, PagerState.AllocationInfos[0].Size);

                //in this pager we will always have only one pager state
                //not sure about this, but from what I see,
                //one of the issues is that when ScratchBufferFile::Allocate() is run, 
                //there is old pagerState in the pager, and not the newly allocated one,
                //and thus AcquirePage gets invalid pointer
                SetPagerState(pagerState);

                return pagerState;
            }
        }

        public override string ToString()
        {
            return GetSourceName();
        }

        public override void ReleaseAllocationInfo(byte* baseAddress, long size)
        {
            if (_memoryBelongsToSomeoneElse)
                return;
            if (PlatformDetails.RunningOnPosix)
            {
                if(Syscall.munmap((IntPtr)baseAddress, (UIntPtr)size)!= 0)
                    throw new InvalidOperationException("Could not free memory", new Win32Exception(Marshal.GetLastWin32Error()));
                return;
            }

            if (Win32MemoryProtectMethods.VirtualFree(baseAddress, UIntPtr.Zero, Win32MemoryProtectMethods.FreeType.MEM_RELEASE) == false)
                throw new InvalidOperationException("Could not free memory", new Win32Exception(Marshal.GetLastWin32Error()));
        }

        public override void TryPrefetchingWholeFile()
        {
        }

        public override void MaybePrefetchMemory(List<long> pagesToPrefetch)
        {
        }

        public override int CopyPage(I4KbBatchWrites destwI4KbBatchWrites, long p, PagerState pagerState)
        {
            return CopyPageImpl(destwI4KbBatchWrites, p, pagerState);
        }
    }
}