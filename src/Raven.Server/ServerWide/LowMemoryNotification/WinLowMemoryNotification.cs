﻿using System;
using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Threading;
using Raven.Abstractions.Logging;
using Sparrow.Logging;

namespace Raven.Server.ServerWide.LowMemoryNotification
{
    public class WinLowMemoryNotification : AbstractLowMemoryNotification
    {
        private static readonly Logger _logger = _loggerSetup.GetLogger<WinLowMemoryNotification>("WinLowMemoryNotification");

        private const int LowMemoryResourceNotification = 0;

        const uint WAIT_FAILED = 0xFFFFFFFF;
        const uint WAIT_TIMEOUT = 0x00000102;

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern IntPtr CreateMemoryResourceNotification(int notificationType);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool QueryMemoryResourceNotification(IntPtr hResNotification, out bool isResourceStateMet);

        [DllImport("kernel32.dll")]
        private static extern uint WaitForMultipleObjects(uint nCount, IntPtr[] lpHandles, bool bWaitAll, uint dwMilliseconds);

        [DllImport("Kernel32.dll", SetLastError = true, CallingConvention = CallingConvention.Winapi, CharSet = CharSet.Unicode)]
        private static extern IntPtr CreateEvent(IntPtr lpEventAttributes, bool bManualReset, bool bInitialState, string lpName);

        [DllImport("Kernel32.dll", SetLastError = true)]
        public static extern bool SetEvent(IntPtr hEvent);

        [DllImport("Kernel32.dll")]
        public static extern IntPtr GetCurrentProcess();

        private readonly IntPtr lowMemorySimulationEvent;
        private readonly IntPtr lowMemoryNotificationHandle;
        private readonly IntPtr softMemoryReleaseEvent;

        public WinLowMemoryNotification(CancellationToken shutdownNotification)
        {
            lowMemorySimulationEvent = CreateEvent(IntPtr.Zero, false, false, null);
            lowMemoryNotificationHandle = CreateMemoryResourceNotification(LowMemoryResourceNotification); // the handle will be closed by the system if the process terminates

            var appDomainUnloadEvent = CreateEvent(IntPtr.Zero, false, false, null);
            shutdownNotification.Register(() => SetEvent(appDomainUnloadEvent));

            softMemoryReleaseEvent = CreateEvent(IntPtr.Zero, false, false, null);

            if (lowMemoryNotificationHandle == null)
            {
                if (_logger.IsOperationsEnabled)
                    _logger.Operations("lowMemoryNotificationHandle is null. might be because of permissions issue.");
                throw new Win32Exception();
            }

            new Thread(() =>
            {
                while (true)
                {
                    var waitForResult = WaitForMultipleObjects(4,
                        new[] {lowMemoryNotificationHandle, appDomainUnloadEvent, lowMemorySimulationEvent, softMemoryReleaseEvent}, false,
                        5*60*1000);

                    switch (waitForResult)
                    {
                        case 0: // lowMemoryNotificationHandle
                            if (_logger.IsInfoEnabled)
                                _logger.Info("Low memory detected, will try to reduce memory usage...");
                            RunLowMemoryHandlers();
                            break;
                        case 1:
                            // app domain unload
                            return;
                        case 2: // LowMemorySimulationEvent
                            if (_logger.IsInfoEnabled)
                                _logger.Info("Low memory simulation, will try to reduce memory usage...");
                            RunLowMemoryHandlers();
                            break;
                        case 3: // SoftMemoryReleaseEvent
                            if (_logger.IsInfoEnabled)
                                _logger.Info("Releasing memory before Garbage Collection operation");
                            RunLowMemoryHandlers();
                            break;
                        case WAIT_TIMEOUT:
                            ClearInactiveHandlers();
                            break;
                        case WAIT_FAILED:
                            if (_logger.IsInfoEnabled)
                                _logger.Info("Failure when trying to wait for low memory notification. No low memory notifications will be raised.");
                            break;
                    }

                    Thread.Sleep(TimeSpan.FromSeconds(60)); // prevent triggering the event oto frequent when the low memory notification object is in the signaled state
                }
            })
            {
                IsBackground = true,
                Name = "Low memory notification thread"
            }.Start();
        }

        public override void SimulateLowMemoryNotification()
        {
            SetEvent(lowMemorySimulationEvent);
        }

        public void InitiateSoftMemoryRelease()
        {
            SetEvent(softMemoryReleaseEvent);
        }
    }
}