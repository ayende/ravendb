using System;
using System.Diagnostics;

namespace Voron.Util
{
    // Probe-only event trace (RAVEN_EVTRACE=1): single-timeline events for correlating
    // client latency spikes with flush/sync/journal-state work. Not for production.
    public static class EvTrace
    {
        public static readonly bool Enabled = Environment.GetEnvironmentVariable("RAVEN_EVTRACE") == "1";

        private static readonly double TicksPerMs = Stopwatch.Frequency / 1000.0;

        public static long Now => Stopwatch.GetTimestamp();

        public static double ToMs(long fromTicks, long toTicks) => (toTicks - fromTicks) / TicksPerMs;

        public static void Emit(string line)
        {
            Console.WriteLine($"EVT|{(long)(Stopwatch.GetTimestamp() / TicksPerMs)}|{Environment.CurrentManagedThreadId}|{line}");
        }
    }
}
