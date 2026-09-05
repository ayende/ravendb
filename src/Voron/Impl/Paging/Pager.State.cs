#nullable enable

using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Runtime.CompilerServices;
using Sparrow;
using Sparrow.Logging;
using Sparrow.Server.Platform;
using Sparrow.Utils;
using Voron.Global;
using Voron.Logging;

namespace Voron.Impl.Paging;

public unsafe partial class Pager
{
    public class State : IDisposable
    {
        public readonly Pager Pager;

        public readonly WeakReference<State> WeakSelf;

        public State(Pager pager, byte* readAddress, byte* writeMemory, long totalAllocatedSize, void* handle, int pageSize)
        {
            ReadAddress = readAddress;
            WriteAddress = writeMemory;
            TotalAllocatedSize = totalAllocatedSize;
            Handle = handle;
            _pageSize = pageSize;

            Pager = pager;
            WeakSelf = new WeakReference<State>(this);
            NativeMemory.RegisterFileMapping(pager.FileName, new IntPtr(ReadAddress), TotalAllocatedSize, null);
        }


        public readonly byte* ReadAddress;
        public readonly byte* WriteAddress;
        public long NumberOfAllocatedPages => TotalAllocatedSize / _pageSize;
        public long TotalAllocatedSize;
        public long TotalPhysicalSpace;

        public bool Disposed;

        public void* Handle;
        private readonly int _pageSize;

        public void Dispose()
        {
            if (Disposed)
                return;
            // we may call this via a weak reference, so we need to ensure that 
            // we aren't racing through the finalizer and explicit dispose
            lock (WeakSelf)
            {
                if (Disposed)
                    return;

                Disposed = true;

                Pager._states.TryRemove(WeakSelf);

                var rc = Pal.rvn_close_pager(Handle, out var errorCode);
                NativeMemory.UnregisterFileMapping(Pager.FileName, (nint)ReadAddress, TotalAllocatedSize);

                if (rc != PalFlags.FailCodes.Success)
                {
                    PalHelper.ThrowLastError(rc, errorCode, $"Failed to close data pager for: {Pager.FileName}");
                }
            }

            GC.SuppressFinalize(this);
        }

        // Closing a pager is munmap + close. Under write load pager growth retires a
        // steady stream of State instances, and paying those syscalls on the finalizer
        // thread kept it at ~75% of a core (munmap also broadcasts TLB shootdowns),
        // starving every other finalizable object. The finalizer just hands the state
        // to a dedicated background thread instead.
        private static readonly ConcurrentQueue<State> PendingDisposal = new();
        private static readonly AutoResetEvent HasPendingDisposal = new(false);

        static State()
        {
            new Thread(BackgroundDisposalWork)
            {
                IsBackground = true,
                Name = "Voron Pager Disposal",
                Priority = ThreadPriority.BelowNormal
            }.Start();
        }

        private static void BackgroundDisposalWork()
        {
            while (true)
            {
                HasPendingDisposal.WaitOne();
                while (PendingDisposal.TryDequeue(out var state))
                {
                    try
                    {
                        state.Dispose();
                    }
                    catch (Exception e)
                    {
                        try
                        {
                            // cannot let the disposal thread die, just log it
                            var logger = RavenLogManager.Instance.GetLoggerForGlobalVoron<State>();

                            if (logger.IsErrorEnabled)
                            {
                                logger.Error("Failed to dispose a pager state from the background disposer", e);
                            }
                        }
                        catch
                        {
                            // nothing we can do here
                        }
                    }
                }
            }
        }

        ~State()
        {
            try
            {
                // resurrecting the instance is fine: the background thread holds the only
                // reference until Dispose completes, and Dispose is idempotent under its lock
                PendingDisposal.Enqueue(this);
                HasPendingDisposal.Set();
            }
            catch
            {
                // queueing failed (shutdown, OOM) - the native handle leaks rather than
                // risking a blocking syscall storm on the finalizer thread
            }
        }
    }
}
