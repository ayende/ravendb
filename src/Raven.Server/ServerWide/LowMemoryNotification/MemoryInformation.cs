﻿using System;
using System.Runtime.InteropServices;
using Raven.Server.Config.Settings;
using Voron;
using Voron.Platform.Posix;

namespace Raven.Server.ServerWide.LowMemoryNotification
{
    public static class MemoryInformation
    {
        private static readonly ILog Log = LogManager.GetLogger(typeof(MemoryInformation));

        private static int memoryLimit;
        private static bool failedToGetAvailablePhysicalMemory;
        private static readonly MemoryInfoResult failedResult = new MemoryInfoResult
        {
            AvailableMemory = new Size(256, SizeUnit.Megabytes),
            TotalPhysicalMemory = new Size(256, SizeUnit.Megabytes),
        };

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern IntPtr CreateMemoryResourceNotification(int notificationType);

        [StructLayout(LayoutKind.Sequential)]
        public struct MemoryStatusEx
        {
            public uint dwLength;
            public uint dwMemoryLoad;
            public ulong ullTotalPhys;
            public ulong ullAvailPhys;
            public ulong ullTotalPageFile;
            public ulong ullAvailPageFile;
            public ulong ullTotalVirtual;
            public ulong ullAvailVirtual;
            public ulong ullAvailExtendedVirtual;
        }

        [return: MarshalAs(UnmanagedType.Bool)]
        [DllImport("kernel32.dll",SetLastError = true)]
        public static unsafe extern bool GlobalMemoryStatusEx(MemoryStatusEx* lpBuffer);

        /// <summary>
        /// This value is in MB
        /// </summary>
        public static int MemoryLimit
        {
            get { return memoryLimit; }
            set
            {
                memoryLimit = value;
            }
        }

        public static unsafe MemoryInfoResult GetMemoryInfo()
        {
            if (failedToGetAvailablePhysicalMemory)
            {
                Log.Info("Because of a previous error in getting available memory, we are now lying and saying we have 256MB free");
                return failedResult;
            }

            try
            {
                if (StorageEnvironmentOptions.RunningOnPosix)
                {
                    sysinfo_t info = new sysinfo_t();
                    if (Syscall.sysinfo(ref info) != 0)
                    {
                        Log.Warn("Failure when trying to read memory info from posix, error code was: " + Marshal.GetLastWin32Error());
                        return failedResult;
                    }

                    return new MemoryInfoResult
                    {
                        AvailableMemory = new Size((long)info.AvailableRam, SizeUnit.Bytes),
                        TotalPhysicalMemory = new Size((long)info.TotalRam, SizeUnit.Bytes),
                    };
                }

              

                var memoryStatus = new MemoryStatusEx
                {
                    dwLength = (uint)sizeof(MemoryStatusEx)
                };
                var result = GlobalMemoryStatusEx(&memoryStatus);
                if (result == false)
                {
                    Log.Warn("Failure when trying to read memory info from Windows, error code is: " + Marshal.GetLastWin32Error());
                    return failedResult;
                }

                return new MemoryInfoResult
                {
                    AvailableMemory = new Size((long)memoryStatus.ullAvailPhys, SizeUnit.Bytes),
                    TotalPhysicalMemory = new Size((long)memoryStatus.ullTotalPhys, SizeUnit.Bytes),
                };
            }
            catch (Exception e)
            {
                Log.ErrorException("Error while trying to get available memory, will stop trying and report that there is 256MB free only from now on", e);
                failedToGetAvailablePhysicalMemory = true;
                return failedResult;
            }
        }
    }

    public struct MemoryInfoResult
    {
        public Size TotalPhysicalMemory;
        public Size AvailableMemory;
    }
}