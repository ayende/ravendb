using System;
using System.Threading;

namespace Sparrow.Utils
{
    /// <summary>
    /// Timeouts for operations that rarely time out, where precision of <see cref="Granularity"/> is good enough.
    /// A single timer moves a hand over a ring of slots and cancels the shared token of each slot it reaches,
    /// so getting a timeout token is two volatile reads: no allocation, no timer, no lock.
    /// </summary>
    internal static class CoarseTimeout
    {
        public const int GranularityInMs = 15_000;

        /// <summary>
        /// Shorter timeouts are too imprecise here, the token fires up to two slots late.
        /// </summary>
        public const int MinimumTimeoutInMs = 4 * GranularityInMs;

        private const int SlotCount = 64;

        private static readonly CancellationTokenSource[] Slots = CreateSlots();
        private static long _hand;
        private static readonly Timer Ticker = new(OnTick, null, GranularityInMs, GranularityInMs);

        /// <summary>
        /// A token that is canceled no sooner than <paramref name="timeoutInMs"/> and no later than two slots after it.
        /// </summary>
        public static CancellationToken After(int timeoutInMs)
        {
            // one extra slot, so we never take the slot that the hand is about to reach
            long slotsAhead = (timeoutInMs + GranularityInMs - 1) / GranularityInMs + 1;
            if (slotsAhead >= SlotCount)
                return new CancellationTokenSource(timeoutInMs).Token;

            var hand = Volatile.Read(ref _hand);
            return Volatile.Read(ref Slots[(int)((hand + slotsAhead) % SlotCount)]).Token;
        }

        private static CancellationTokenSource[] CreateSlots()
        {
            var slots = new CancellationTokenSource[SlotCount];
            for (int i = 0; i < slots.Length; i++)
                slots[i] = new CancellationTokenSource();
            return slots;
        }

        private static void OnTick(object state)
        {
            // a slow tick (many callbacks to run) must not overlap with the next one
            if (Monitor.TryEnter(Slots) == false)
                return;

            try
            {
                var hand = Interlocked.Increment(ref _hand);
                var expired = Interlocked.Exchange(ref Slots[(int)(hand % SlotCount)], new CancellationTokenSource());

                // not disposed, tokens handed out from it may still be registered to
                expired.Cancel();
            }
            catch
            {
                // a failing callback belongs to its operation, the ticker has to keep going
            }
            finally
            {
                Monitor.Exit(Slots);
            }
        }
    }
}
