using System;
using System.Diagnostics;
using System.Collections.Concurrent;
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
    /// at most once each second through one handle that stays open. Calibration on gp3: the
    /// trickle region shows a queue of ~2.3-3.8; the drain region shows ~6.4-11; the default
    /// threshold of 5 sits in the gap. The signal is passive - no test traffic, no injected
    /// latency. The barrier cost is a second trigger: it covers devices with no queue data
    /// (macOS, containers) and devices whose speed changed (burst credits).
    ///
    /// One gate exists for each physical device, shared by every environment on it; the first
    /// environment to open the device sets the thresholds. Reads and writes of the fields are
    /// racy by design - every field is word-sized, and a lost sample or a one-call-late mode
    /// change has no effect that matters. The one Interlocked call protects the reader: two
    /// concurrent reads of the sysfs handle would corrupt its delta baseline.
    /// </summary>
    public sealed class WritebackPacingGate
    {
        private static readonly ConcurrentDictionary<ulong, WritebackPacingGate> DevicesById = new();
        private static readonly RavenLogger Log = RavenLogManager.Instance.GetLoggerForGlobalVoron<WritebackPacingGate>();

        public static WritebackPacingGate GetForDevice(ulong deviceId, string pathOnDevice, long barrierCostThresholdTicks, int queueDepthThreshold)
        {
            return DevicesById.GetOrAdd(deviceId,
                static (id, a) => new WritebackPacingGate(DeviceQueueDepthReader.TryCreate(a.Path, id), a.Path, a.BarrierTicks, a.QueueDepth),
                (Path: pathOnDevice, BarrierTicks: barrierCostThresholdTicks, QueueDepth: queueDepthThreshold));
        }

        private const long SampleIntervalMs = 1000;
        private const long ExitQuietMs = 30_000;

        private readonly DeviceQueueDepthReader _queueReader; // null = no queue signal on this platform
        private readonly string _pathOnDevice;
        private readonly long _barrierThresholdTicks;
        private readonly double _enterQueueDepth;
        private readonly double _exitQueueDepth;
        private readonly Stopwatch _clock = Stopwatch.StartNew();

        private double _activeQueueThreshold; // the enter value while trickling, the exit value while draining
        private long _barrierCostTicksEwma;
        private double _queueDepthEwma;
        private long _lastSampleMs;
        private long _lastBusyMs;
        private bool _draining;

        internal WritebackPacingGate(DeviceQueueDepthReader queueReader, string pathOnDevice, long barrierCostThresholdTicks, int queueDepthThreshold)
        {
            _queueReader = queueReader;
            _pathOnDevice = pathOnDevice;
            _barrierThresholdTicks = barrierCostThresholdTicks;
            _enterQueueDepth = queueDepthThreshold;
            _exitQueueDepth = queueDepthThreshold * 0.6; // hysteresis: leave only well below the entry point
            _activeQueueThreshold = _enterQueueDepth;
        }

        public long BarrierCostTicks => _barrierCostTicksEwma;
        public double QueueDepth => _queueDepthEwma;
        public bool HasQueueSignal => _queueReader != null;

        public void RecordBarrierCost(long ticks)
        {
            // approximate EWMA (alpha = 1/4); a racy update just loses a sample
            var current = _barrierCostTicksEwma;
            _barrierCostTicksEwma = current == 0 ? ticks : current + (ticks - current) / 4;
        }

        public bool ShouldDrain()
        {
            var (nowMs, queueDepth) = SampleQueue();

            if (queueDepth > _activeQueueThreshold || _barrierCostTicksEwma > _barrierThresholdTicks)
            {
                _lastBusyMs = nowMs;
                if (_draining == false)
                {
                    _draining = true;
                    _activeQueueThreshold = _exitQueueDepth;
                    if (Log.IsDebugEnabled)
                    {
                        Log.Debug($"The device that holds '{_pathOnDevice}' is congested (queue {queueDepth:0.0}, " +
                                  $"barrier {_barrierCostTicksEwma / TimeSpan.TicksPerMillisecond}ms). Every environment on this device " +
                                  "moves to drain mode: flushes stop the writeback trickle, and each sync pushes its dirty ranges " +
                                  "in paced blocks before the fdatasync barrier.");
                    }
                }
                return true;
            }

            if (_draining)
            {
                if (nowMs - _lastBusyMs < ExitQuietMs)
                    return true;

                _draining = false;
                _activeQueueThreshold = _enterQueueDepth;
                if (Log.IsDebugEnabled)
                {
                    Log.Debug($"The device that holds '{_pathOnDevice}' is quiet again (queue {queueDepth:0.0}). Every environment " +
                              "on this device returns to trickle mode: flushes start writeback of their dirty ranges, and syncs " +
                              "use a plain fdatasync barrier.");
                }
            }

            return false;
        }

        private (long NowMs, double QueueDepth) SampleQueue()
        {
            var nowMs = _clock.ElapsedMilliseconds;
            var queueDepth = _queueDepthEwma;
            if (_queueReader == null)
                return (nowMs, queueDepth);

            var last = _lastSampleMs;
            if (nowMs - last < SampleIntervalMs ||
                Interlocked.CompareExchange(ref _lastSampleMs, nowMs, last) != last)
                return (nowMs, queueDepth); // not due yet, or another thread samples

            try
            {
                var value = _queueReader.Read();
                queueDepth = queueDepth == 0 ? value : queueDepth + (value - queueDepth) / 4;
                _queueDepthEwma = queueDepth;
            }
            catch
            {
                // a torn read is a lost sample, nothing more
            }

            return (nowMs, queueDepth);
        }
    }
}
