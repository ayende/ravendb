using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;
using Sparrow.Logging;
using Sparrow.Server.Logging;
using Sparrow.Server.Utils;
using Voron.Logging;

namespace Voron.Impl.Journal
{
    /// <summary>
    /// Per-physical-device selector for the data-file writeback mode (RavenDB-27375).
    /// Two modes exist. Trickle: each flush starts writeback of its dirty ranges (initiate only)
    /// and the sync barrier is a plain fdatasync - correct while the device has headroom.
    /// Drain: flushes do not start writeback; the sync pushes the dirty ranges in bounded,
    /// waited blocks before the barrier - correct while the device is congested.
    ///
    /// The trigger is the time-weighted device queue depth (the iostat "aqu-sz" number), sampled
    /// at most once each second through one handle that stays open. Calibration on gp3
    /// (RavenDB-27375): the trickle region shows a queue of ~2.3-3.8; the drain region shows
    /// ~6.4-11; the default threshold of 5 sits in the gap. The signal is passive - no test
    /// traffic, no injected latency. The barrier cost is a second trigger: it covers devices
    /// with no queue data (macOS, containers) and devices whose speed changed (burst credits).
    /// The gate returns to trickle mode after the queue stays below 60% of the threshold, and
    /// the barrier stays cheap, for 30 seconds.
    /// </summary>
    public sealed class WritebackPacingGate
    {
        private static readonly ConcurrentDictionary<ulong, WritebackPacingGate> DevicesById = new();
        private static readonly RavenLogger Log = RavenLogManager.Instance.GetLoggerForGlobalVoron<WritebackPacingGate>();

        public static WritebackPacingGate GetForDevice(ulong deviceId, string pathOnDevice)
        {
            return DevicesById.GetOrAdd(deviceId, static (id, path) => new WritebackPacingGate(id, path), pathOnDevice);
        }

        private const int TrickleMode = 0;
        private const int DrainMode = 1;
        private static readonly long SampleIntervalTimestamps = Stopwatch.Frequency; // one second
        private static readonly long ExitQuietTimestamps = Stopwatch.Frequency * 30; // thirty seconds

        private readonly DeviceQueueDepthReader _queueReader; // null = no signal on this platform
        private long _barrierCostTicksEwma;
        private double _queueDepthEwma;
        private long _lastSampleTimestamp;
        private long _lastBusyTimestamp;
        private int _mode = TrickleMode;

        private WritebackPacingGate(ulong deviceId, string pathOnDevice)
        {
            _queueReader = DeviceQueueDepthReader.TryCreate(pathOnDevice, deviceId);
            _lastSampleTimestamp = Stopwatch.GetTimestamp();
        }

        internal WritebackPacingGate(DeviceQueueDepthReader readerForTests)
        {
            _queueReader = readerForTests;
            _lastSampleTimestamp = Stopwatch.GetTimestamp();
        }

        public long BarrierCostTicks => Volatile.Read(ref _barrierCostTicksEwma);
        public double QueueDepth => Volatile.Read(ref _queueDepthEwma);
        public bool HasQueueSignal => _queueReader != null;

        public void RecordBarrierCost(long ticks)
        {
            // approximate EWMA (alpha = 1/4); a racy update just loses a sample
            var current = Volatile.Read(ref _barrierCostTicksEwma);
            Volatile.Write(ref _barrierCostTicksEwma, current == 0 ? ticks : current + (ticks - current) / 4);
        }

        public bool ShouldDrain(long barrierThresholdTicks, int queueDepthThreshold)
        {
            MaybeSampleQueue();

            var now = Stopwatch.GetTimestamp();
            var busy = Volatile.Read(ref _queueDepthEwma) > (Volatile.Read(ref _mode) == DrainMode
                           ? queueDepthThreshold * 0.6 // hysteresis: leave only well below the entry point
                           : queueDepthThreshold)
                       || Volatile.Read(ref _barrierCostTicksEwma) > barrierThresholdTicks;

            if (busy)
            {
                Volatile.Write(ref _lastBusyTimestamp, now);
                if (Interlocked.Exchange(ref _mode, DrainMode) == TrickleMode && Log.IsDebugEnabled)
                    Log.Debug($"Writeback gate -> drain (queue {QueueDepth:0.0}, barrier {BarrierCostTicks / TimeSpan.TicksPerMillisecond}ms)");
                return true;
            }

            if (Volatile.Read(ref _mode) == DrainMode)
            {
                if (now - Volatile.Read(ref _lastBusyTimestamp) < ExitQuietTimestamps)
                    return true;

                if (Interlocked.Exchange(ref _mode, TrickleMode) == DrainMode && Log.IsDebugEnabled)
                    Log.Debug($"Writeback gate -> trickle (queue {QueueDepth:0.0})");
            }

            return false;
        }

        private void MaybeSampleQueue()
        {
            if (_queueReader == null)
                return;

            var now = Stopwatch.GetTimestamp();
            var last = Volatile.Read(ref _lastSampleTimestamp);
            if (now - last < SampleIntervalTimestamps)
                return;
            if (Interlocked.CompareExchange(ref _lastSampleTimestamp, now, last) != last)
                return; // another thread samples

            double value;
            try
            {
                value = _queueReader.Read();
            }
            catch
            {
                return; // a torn read is a lost sample, nothing more
            }

            var current = Volatile.Read(ref _queueDepthEwma);
            Volatile.Write(ref _queueDepthEwma, current == 0 ? value : current + (value - current) / 4);
        }
    }
}
