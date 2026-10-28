using System;
using System.Diagnostics;
using System.Threading;
using Sparrow.Binary;

namespace Sparrow.Server.Threading;

/// <summary>
/// This class allows many "visitors" to enter/exit it, with minimal concurrency / contention between them
/// It also allows a *single* thread to move guard to closed state, with the following rules:
/// * Once the guard is marked closed, no future entries our allowed
/// * Closing the guard may block until there are no more visitors
/// </summary>
public class AsyncGuard : IDisposable
{
    // PERF: each marker gets its own cache line (128 bytes, to cover adjacent line prefetch), otherwise 8 markers
    // share a line and every enter / exit on one core invalidates the line for the others
    private const int Stride = 16;

    private readonly ulong[] _coreMarkers;
    private readonly int _mask;
    private readonly ManualResetEvent _event;
    private bool _locked;
    private const ulong WriterLockMask = 0x8000_0000_0000_0000;

    public AsyncGuard()
    {
        int len = Bits.PowerOf2(Environment.ProcessorCount * 2);
        _mask = len - 1;
        _coreMarkers = new ulong[len * Stride];
        _event = new ManualResetEvent(false);
    }

    public bool TryEnter(out int idx)
    {
        // the processor rather than the thread, so threads running on different cores touch different markers
        idx = (Thread.GetCurrentProcessorId() & _mask) * Stride;
        ref var cur = ref _coreMarkers[idx];
        ulong copy = cur;
        while (true)
        {
            // If the write lock mask is set, this means that we have a writer, so we
            // cannot take a lock, that is too late now (and forever)
            if ((copy & WriterLockMask) != 0)
                return false; // already taken

            var result = Interlocked.CompareExchange(ref cur, copy + 1, copy);
            if (result == copy)
                return true; // got it successfully

            copy = result;
        }
    }

    public void Exit(int idx)
    {
        // we use the idx here to ensure that we can call this from a different thread
        ref var cur = ref _coreMarkers[idx];
        ulong copy = cur;
        ulong masked;
        while (true)
        {
            masked = copy & WriterLockMask;
            ulong update = (copy & ~WriterLockMask) - 1;
            if (update > copy)
                throw new OverflowException("BUG! We released a guard too much?");

            update |= masked;

            var result = Interlocked.CompareExchange(ref cur, update, copy);
            if (result == copy)
            {
                break;
            }
            copy = result;
        }

        if (masked != 0) // we exit and there is a lock in place, need to notify the locker
        {
            _event.Set();
        }
    }

    public void CloseAndLock()
    {
        if (_locked)
            throw new InvalidOperationException("Guard was already locked!");
        _locked = true;
        
        // first, mark all the cores as busy 
        while (true)
        {
            bool allMarked = true;
            for (int i = 0; i < _coreMarkers.Length; i += Stride)
            {
                ref var cur = ref _coreMarkers[i];
                var copy = cur;
                Debug.Assert((copy & WriterLockMask) == 0, "(copy & WriterLockMask) == 0 - meaning we already called it?");
                var result = Interlocked.CompareExchange(ref cur, copy | WriterLockMask, copy);
                allMarked &= result == copy;
            }

            if (allMarked)
                break;
        }

        while (true)
        {
            _event.Reset();

            bool allCleared = true;
            for (int i = 0; i < _coreMarkers.Length; i += Stride)
            {
                var cur = Interlocked.Read(ref _coreMarkers[i]);
                if (cur != WriterLockMask)
                {
                    allCleared = false;
                    break;
                }
            }

            if (allCleared)
                break;

            _event.WaitOne();
        }
    }

    public void Dispose()
    {
        _event?.Dispose();
    }
}
