using System;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Microsoft.Win32.SafeHandles;
using Voron.Impl;
using Voron.Impl.Paging;

namespace Voron.Platform.Win32
{
    public unsafe class Win32PageFileMemoryMappedPager : AbstractPager
    {
        private readonly string filename;
        private long totalAllocationSize;
        private static int counter;
        private readonly int instanceId;
        private readonly SafeFileHandle fileHandle;
        private readonly uint allocationGranularity;

        public Win32PageFileMemoryMappedPager(int pageSize, string name, long? initialFileSize = null)
            : base(pageSize)
        {
            instanceId = Interlocked.Increment(ref counter);

            Win32NativeMethods.SYSTEM_INFO systemInfo;
            Win32NativeMethods.GetSystemInfo(out systemInfo);
            allocationGranularity = systemInfo.allocationGranularity;
            totalAllocationSize = initialFileSize.HasValue ? NearestSizeToAllocationGranularity(initialFileSize.Value) : systemInfo.allocationGranularity;

            PagerState.Release();

            Debug.Assert(systemInfo.allocationGranularity % PageSize == 0);
            NumberOfAllocatedPages = totalAllocationSize / PageSize;

            totalAllocationSize = NearestSizeToAllocationGranularity(totalAllocationSize);

            filename = $"{Path.GetTempPath()}ravendb-{Process.GetCurrentProcess().Id}-{instanceId}-{name}";

            fileHandle = Win32NativeFileMethods.CreateFile(filename,
                       Win32NativeFileAccess.GenericRead | Win32NativeFileAccess.GenericWrite | Win32NativeFileAccess.Delete,
                       Win32NativeFileShare.Read | Win32NativeFileShare.Write | Win32NativeFileShare.Delete,
                       IntPtr.Zero,
                       Win32NativeFileCreationDisposition.CreateAlways,
                       Win32NativeFileAttributes.RandomAccess | Win32NativeFileAttributes.DeleteOnClose,
                       IntPtr.Zero);

            if (fileHandle.IsInvalid)
            {
                if (Marshal.GetLastWin32Error() == (int)Win32NativeFileErrors.ERROR_FILE_NOT_FOUND)
                    throw new FileNotFoundException(filename);
                throw new Win32Exception(Marshal.GetLastWin32Error(), $"Could not open file {filename}");
            }

            var newPager = AllocateAndRemap();

            newPager.AddRef();
            PagerState = newPager;
        }

        public override void AllocateMorePages(Transaction tx, long newLength)
        {
            ThrowObjectDisposedIfNeeded();
            var newLengthAfterAdjustment = NearestSizeToAllocationGranularity(newLength);

            if (newLengthAfterAdjustment <= totalAllocationSize)
                return;

            totalAllocationSize = newLengthAfterAdjustment;

            var newPagerState = AllocateAndRemap();

            newPagerState.AddRef(); // one for the pager

            newPagerState.DebugVerify(newLengthAfterAdjustment);

            if (tx != null)
            {
                newPagerState.AddRef();
                tx.AddPagerState(newPagerState);
            }

            // we always share the same memory mapped files references between all pages, since to close them 
            // would be to lose all the memory associated with them
            PagerState.DisposeFilesOnDispose = true;
            var tmp = PagerState;
            PagerState = newPagerState;
            tmp.Release(); //replacing the pager state --> so one less reference for it

            NumberOfAllocatedPages = totalAllocationSize / PageSize;
        }


        private PagerState AllocateAndRemap()
        {
            Win32NativeFileMethods.SetFileLength(fileHandle, totalAllocationSize);

            var mmf = Win32MemoryMapNativeMethods
                        .CreateFileMapping(fileHandle.DangerousGetHandle(),
                                           IntPtr.Zero,
                                           Win32MemoryMapNativeMethods.FileMapProtection.PageReadWrite,
                                            0, 0, null);

            if (mmf == IntPtr.Zero)
                throw new Win32Exception(Marshal.GetLastWin32Error(), $"Could not create file mapping for {filename}");

            var startingBaseAddressPtr =
                Win32MemoryMapNativeMethods.MapViewOfFileEx(mmf,
                                                            Win32MemoryMapNativeMethods.NativeFileMapAccessType.Read |
                                                            Win32MemoryMapNativeMethods.NativeFileMapAccessType.Write,
                                                            0, 0, UIntPtr.Zero, null);

            if (startingBaseAddressPtr == (byte*)0)
                throw new Win32Exception(Marshal.GetLastWin32Error(),
                    $"Unable to map view for file {filename} of size = {(totalAllocationSize):##,###;;0} bytes");

            var allocationInfo = new PagerState.AllocationInfo
            {
                BaseAddress = startingBaseAddressPtr,
                Size = totalAllocationSize,
                MappedFile = null // no remapes with several mem map files
            };

            var newPager = new PagerState(this)
            {
                Files = null, // no remapes with several mem map files
                MapBase = startingBaseAddressPtr,
                AllocationInfos = new[] { allocationInfo }
            };
            return newPager;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private long NearestSizeToAllocationGranularity(long size)
        {
            var modulos = size % allocationGranularity;
            if (modulos == 0)
                return Math.Max(size, allocationGranularity);

            return ((size / allocationGranularity) + 1) * allocationGranularity;
        }

        public override byte* AcquirePagePointer(long pageNumber, PagerState pagerState = null)
        {
            return (pagerState ?? PagerState).MapBase + (pageNumber * PageSize);
        }

        public override void Sync()
        {
            // nothing to do here, we are already synced to memory, and we 
            // don't go anywhere
        }

        protected override string GetSourceName()
        {
            return $"MemMapInSystemPage:{filename} {instanceId} Size : {totalAllocationSize}";
        }

        public override string ToString()
        {
            return $"{GetSourceName()}, Length: {totalAllocationSize / 1024d / 1024d:#,#.##;;0} MB";
        }

        public override void ReleaseAllocationInfo(byte* baseAddress, long size)
        {
            if (Win32MemoryMapNativeMethods.UnmapViewOfFile(baseAddress) == false)
                throw new Win32Exception();
        }

        public override void Dispose()
        {
            base.Dispose();
            if (fileHandle != null)
            {
                fileHandle.Close();
                fileHandle.Dispose();
            }
        }
    }
}
