using System;
using System.Diagnostics;
using System.IO;

namespace Raven.Server.Documents.TransactionMerger
{
    // Temporary probe for RavenDB-27377: decomposes each transaction merger batch cycle so we can
    // see where the cycle time goes at the throughput ceiling. Enabled by RAVEN_MERGERTRACE=<dir>.
    internal static class MergerProbe
    {
        private static readonly string Dir = Environment.GetEnvironmentVariable("RAVEN_MERGERTRACE");
        public static readonly bool Enabled = string.IsNullOrEmpty(Dir) == false;
        private static readonly object Lock = new object();
        private static StreamWriter _writer;
        private static int _lines;
        private static readonly double TicksToMs = 1000.0 / Stopwatch.Frequency;

        public static double TicksMs(long ticks) => ticks * TicksToMs;

        public static double Ms(long fromTimestamp, long toTimestamp) => (toTimestamp - fromTimestamp) * TicksToMs;

        public static void Log(string resource, int ops, string closeReason, long modifiedBytes, int queueDepth,
            double beginMs, double execMs, double waitMs, double pumpMs, double drainMs)
        {
            WriteLine(
                $"{DateTime.UtcNow:HH:mm:ss.fff}|{resource}|MB|ops={ops}|close={closeReason}|modKB={modifiedBytes / 1024}|q={queueDepth}|" +
                $"beginMs={beginMs:F2}|execMs={execMs:F2}|waitMs={waitMs:F2}|pumpMs={pumpMs:F2}|drainMs={drainMs:F2}");
        }

        // chain boundary: one line per MergeTransactionsOnce call, splitting the un-stamped stretch
        // between two async chains into idle wait / context+write-lock open / first exec / sync commit
        public static void LogChain(string resource, string kind, int ops, int queueDepth,
            double idleMs, double openMs, double execMs, double commitMs)
        {
            WriteLine(
                $"{DateTime.UtcNow:HH:mm:ss.fff}|{resource}|CB|kind={kind}|ops={ops}|q={queueDepth}|" +
                $"idleMs={idleMs:F2}|openMs={openMs:F2}|execMs={execMs:F2}|commitMs={commitMs:F2}");
        }

        // async chain exit: the DrainAll + final synchronous commit that closes a chain
        public static void LogChainExit(string resource, double drainMs, double commitMs)
        {
            WriteLine($"{DateTime.UtcNow:HH:mm:ss.fff}|{resource}|CX|drainMs={drainMs:F2}|commitMs={commitMs:F2}");
        }

        private static void WriteLine(string line)
        {
            if (Enabled == false)
                return;

            try
            {
                lock (Lock)
                {
                    if (_writer == null)
                    {
                        Directory.CreateDirectory(Dir);
                        _writer = new StreamWriter(new FileStream(Path.Combine(Dir, $"mergertrace-{Environment.ProcessId}.log"),
                            FileMode.Append, FileAccess.Write, FileShare.Read)) { AutoFlush = false };
                    }

                    _writer.WriteLine(line);

                    if (++_lines % 256 == 0)
                        _writer.Flush();
                }
            }
            catch
            {
                // never let the probe take down the merger
            }
        }
    }
}
