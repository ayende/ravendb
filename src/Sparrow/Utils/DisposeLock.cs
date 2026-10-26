using System;
using System.Threading;

namespace Sparrow.Utils
{
    /// <summary>
    /// This class allow us to perform disposal operations without running
    /// into concurrency issues with calling code: disposal waits for the operations
    /// in flight, and operations that start after it fail. Only one caller gets to dispose.
    /// </summary>
    internal sealed class DisposeLock
    {
        // PERF: a counter instead of a reader / writer lock, entering and leaving an operation is a single interlocked
        // instruction and allocates nothing, which matters since we guard every write of compression streams
        private const int DisposingFlag = 1 << 30;

        private readonly string _name;
        private int _state; // operations in flight, plus DisposingFlag once disposal started
        private ManualResetEventSlim _drained; // only created when disposal has to wait for operations in flight

        public DisposeLock(string name)
        {
            _name = name;
        }

        public OperationScope EnsureNotDisposed()
        {
            if ((Interlocked.Increment(ref _state) & DisposingFlag) != 0)
            {
                Exit();
                ThrowDisposed();
            }

            return new OperationScope(this);
        }

        /// <summary>
        /// Waits for the operations in flight to complete, and fails all the operations that start afterward.
        /// </summary>
        /// <returns>false when disposal was already started by someone else</returns>
        public bool TryStartDisposing()
        {
            while (true)
            {
                var state = Volatile.Read(ref _state);
                if ((state & DisposingFlag) != 0)
                    return false;

                if (Interlocked.CompareExchange(ref _state, state | DisposingFlag, state) == state)
                    break;
            }

            if (Volatile.Read(ref _state) == DisposingFlag)
                return true;

            var drained = new ManualResetEventSlim();
            Interlocked.Exchange(ref _drained, drained);

            // the last operation might have left before it could see the event
            if (Volatile.Read(ref _state) != DisposingFlag)
                drained.Wait();

            // not disposed: operations that fail after disposal still signal it on their way out
            // (it doesn't hold a kernel handle, since we never use its WaitHandle)
            return true;
        }

        private void Exit()
        {
            if (Interlocked.Decrement(ref _state) == DisposingFlag)
                Volatile.Read(ref _drained)?.Set();
        }

        private void ThrowDisposed()
        {
            throw new LockAlreadyDisposedException(_name);
        }

        public readonly struct OperationScope : IDisposable
        {
            private readonly DisposeLock _parent;

            internal OperationScope(DisposeLock parent)
            {
                _parent = parent;
            }

            public void Dispose()
            {
                _parent?.Exit();
            }
        }
    }

    public sealed class LockAlreadyDisposedException : ObjectDisposedException
    {
        public LockAlreadyDisposedException(string message) : base(message)
        {
        }

        public LockAlreadyDisposedException(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}
