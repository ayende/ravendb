using System;
using System.Diagnostics;
using Sparrow.Threading;

namespace Sparrow.Utils
{
    public abstract class PooledItem : IDisposable
    {
        public MultipleUseFlag InUse = new MultipleUseFlag();

        /// <summary>
        /// <see cref="CurrentTickCount"/> when the item was returned to its pool.
        /// </summary>
        // PERF: JSON contexts are returned on every request, and reading the tick count costs less than half of reading the wall clock
        public long InPoolSinceTickCount;

        /// <summary>
        /// A monotonic clock in milliseconds, to measure how long items stay in their pools.
        /// </summary>
        public static long CurrentTickCount =>
#if NETSTANDARD2_0 || NETSTANDARD2_1
            Stopwatch.GetTimestamp() / (Stopwatch.Frequency / 1000);
#else
            Environment.TickCount64;
#endif

        public abstract void Dispose();
    }
}
