﻿using System;
using System.Runtime.InteropServices;

namespace Sparrow
{
    public static class Win32ThreadsMethods
    {
        [DllImport("kernel32.dll", CallingConvention = CallingConvention.Cdecl, SetLastError = true)]
        public static extern int SetThreadPriority(IntPtr hThread, int nPriority);

        [DllImport("kernel32.dll", CallingConvention = CallingConvention.Cdecl, SetLastError = true)]
        public static extern ThreadPriority GetThreadPriority(IntPtr hThread);

        [DllImport("kernel32.dll", CallingConvention = CallingConvention.Cdecl, SetLastError = true)]
        public static extern uint GetCurrentThreadId();

        [DllImport("kernel32.dll", CallingConvention = CallingConvention.Cdecl, SetLastError = true)]
        public static extern IntPtr OpenThread(ThreadAccess desiredAccess, bool inheritHandle, uint threadId);

        [DllImport("kernel32.dll", CallingConvention = CallingConvention.Cdecl, SetLastError = true)]
        public static extern bool CloseHandle(IntPtr handle);

        [DllImport("kernel32.dll")]
        public static extern IntPtr GetCurrentThread();
    }

    public enum ThreadPriority
    {
        Lowest = -2,
        BelowNormal = -1,
        Normal = 0,
        AboveNormal = 1,
        Highest = 2
    }

    [Flags]
    public enum ThreadAccess
    {
        Terminate = (0x0001),
        SuspendResume = (0x0002),
        GetContext = (0x0008),
        SetContext = (0x0010),
        SetInformation = (0x0020),
        QueryInformation = (0x0040),
        SetThreadToken = (0x0080),
        Impersonate = (0x0100),
        DirectImpersonation = (0x0200),
    }
}
