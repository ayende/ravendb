using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Linq;
using System.Threading;

namespace Raven.Server.Documents.TransactionMerger;

// TEMP latency-diag: samples every 64th merged command's stage timings and dumps percentile summaries
// to stdout every 15 seconds. Stages: queue (enqueue -> merger picks it), exec (command execution),
// durable (exec end -> batch handed to notification, i.e. journal write + commit machinery),
// notify (notification submit -> TrySetResult on a worker), resume (TrySetResult -> awaiter resumed).
internal static class MergerStageTimings
{
    private struct Sample
    {
        public float Queue, Exec, Durable, Notify, Resume, Total;
    }

    private static readonly ConcurrentQueue<Sample> Samples = new();
    private static long _n;
    private static long _lastDump = Stopwatch.GetTimestamp();

    public static void Record(long t0, long t1, long t2, long t3, long t4, long resumeTs)
    {
        if ((Interlocked.Increment(ref _n) & 63) != 0)
            return;

        if (t0 <= 0 || t1 < t0 || t2 < t1 || t3 < t2 || t4 < t3 || resumeTs < t4)
            return; // command skipped a stage (rejection / independent-retry path)

        const double toMs = 1000.0;
        double f = toMs / Stopwatch.Frequency;
        Samples.Enqueue(new Sample
        {
            Queue = (float)((t1 - t0) * f),
            Exec = (float)((t2 - t1) * f),
            Durable = (float)((t3 - t2) * f),
            Notify = (float)((t4 - t3) * f),
            Resume = (float)((resumeTs - t4) * f),
            Total = (float)((resumeTs - t0) * f),
        });
        while (Samples.Count > 20_000)
            Samples.TryDequeue(out _);

        var last = Volatile.Read(ref _lastDump);
        var now = Stopwatch.GetTimestamp();
        if (now - last > 15 * Stopwatch.Frequency &&
            Interlocked.CompareExchange(ref _lastDump, now, last) == last)
        {
            Dump();
        }
    }

    private static void Dump()
    {
        var arr = Samples.ToArray();
        if (arr.Length < 200)
            return;

        string P(Func<Sample, float> sel)
        {
            var v = arr.Select(sel).OrderBy(x => x).ToArray();
            return $"{v[v.Length / 2]:F3}/{v[(int)(v.Length * 0.99)]:F3}";
        }

        Console.WriteLine($"[MERGER-DIAG] n={arr.Length} (p50/p99 ms) " +
                          $"queue={P(s => s.Queue)} exec={P(s => s.Exec)} durable={P(s => s.Durable)} " +
                          $"notify={P(s => s.Notify)} resume={P(s => s.Resume)} total={P(s => s.Total)}");
    }
}
