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
    /// The device-scoped half of the write-path policy: one shared instance per physical device,
    /// arbitrating the budget that every environment on that disk spends together. The
    /// per-environment half (batch shaping, pipelining, per-stream codec use) is
    /// <see cref="WriteFlowPolicy"/>, which delegates the device-scoped questions here. The
    /// sharing is load-bearing - these signals are properties of the DISK, and per-environment
    /// copies would disagree (a quiet database never gathers enough evidence to classify the
    /// device its busy neighbor already classified) or flap (each env reading a queue its
    /// neighbors fill).
    ///
    /// Decisions owned here:
    ///
    /// * Writeback mode - Trickle: each flush starts writeback of its dirty ranges (initiate
    ///   only) and the sync is a plain fdatasync; best while the device has headroom. Drain:
    ///   flushes do not start writeback; the sync pushes the dirty ranges in bounded blocks
    ///   before the fdatasync(), limiting the I/O we generate under load and benefiting from
    ///   dead-write merging. Selected by the time-weighted device queue depth (the iostat
    ///   "aqu-sz" number, sampled once a second) and the measured sync cost; all environments
    ///   on the device switch together.
    ///
    /// * Device classification - Fast (nvme-like: limits so high we never hit them) vs
    ///   Budgeted (gp3-like: metered bandwidth/IOPS we do hit), from the journal write latency
    ///   and size observed across every environment on the device. Feeds the codec choice and
    ///   the compression threshold in each environment's WriteFlowPolicy.
    ///
    /// * Journal zeroing / pool prewarming - prepaying the filesystem extent-conversion cost
    ///   only pays on a fast local device; on a budgeted volume the fill competes with every
    ///   journal on the disk (measured 8-17% of throughput on gp3 under load). The fill also
    ///   stands down while any journal write is active or imminent.
    /// </summary>
    public sealed class DeviceWriteBudget
    {
        private static readonly ConcurrentDictionary<ulong, DeviceWriteBudget> DevicesById = new();
        private static readonly RavenLogger Log = RavenLogManager.Instance.GetLoggerForGlobalVoron<DeviceWriteBudget>();

        public static DeviceWriteBudget GetForDevice(ulong deviceId, string pathOnDevice, long syncCostThresholdTicks, int queueDepthThreshold)
        {
            return DevicesById.GetOrAdd(deviceId,
                static (id, a) => new DeviceWriteBudget(DeviceQueueDepthReader.TryCreate(a.Path, id), a.Path, a.SyncTicks, a.QueueDepth),
                (Path: pathOnDevice, SyncTicks: syncCostThresholdTicks, QueueDepth: queueDepthThreshold));
        }

        // for environments whose device cannot be identified (in-memory, PAL failure): a private,
        // unshared budget with no queue signal - the rules degrade to their safe defaults
        public static DeviceWriteBudget CreateUnshared(long syncCostThresholdTicks, int queueDepthThreshold) =>
            new(queueReader: null, pathOnDevice: "(unshared)", syncCostThresholdTicks, queueDepthThreshold);

        private const long SampleIntervalMs = 1000;
        private const long ExitQuietMs = 30_000;

        private readonly DeviceQueueDepthReader _queueReader; // null = no queue signal on this platform
        private readonly string _pathOnDevice;
        private readonly long _syncThresholdTicks;
        private readonly double _enterQueueDepth;
        private readonly double _exitQueueDepth;
        private readonly Stopwatch _clock = Stopwatch.StartNew();

        private double _activeQueueThreshold; // the enter value while trickling, the exit value while draining
        private long _syncCostTicksEwma;
        private double _queueDepthEwma;
        private long _lastSampleMs;
        private long _lastBusyMs;
        private bool _draining;

        internal DeviceWriteBudget(DeviceQueueDepthReader queueReader, string pathOnDevice, long syncCostThresholdTicks, int queueDepthThreshold)
        {
            _queueReader = queueReader;
            _pathOnDevice = pathOnDevice;
            _syncThresholdTicks = syncCostThresholdTicks;
            _enterQueueDepth = queueDepthThreshold;
            _exitQueueDepth = queueDepthThreshold * 0.6; // leave only well below the entry point
            _activeQueueThreshold = _enterQueueDepth;
        }

        public long SyncCostTicks => _syncCostTicksEwma;
        public double QueueDepth => _queueDepthEwma;
        public bool HasQueueSignal => _queueReader != null;

        public enum DeviceClass
        {
            Unknown, // no evidence yet, go for safe defaults
            Fast,    // example: nvme - very high limits, or we never hit them
            Budgeted // example: gp3 - both bandwidth & IOPS limits that we hit
        }

        // journal write telemetry across EVERY environment on this device
        private Sparrow.Utils.SimpleEwma _journalWriteLatencyTicks = new(smoothing: 8);
        private Sparrow.Utils.SimpleEwma _journalWriteSizeBytes = new(smoothing: 8);
        private long _lastJournalWriteActivityTimestamp;
        private long _classifyAboveLatencyTicks; // set by the first environment's options

        public void RecordJournalWrite(long latencyTicks, long sizeInBytes, long classifyAboveLatencyTicks)
        {
            Volatile.Write(ref _lastJournalWriteActivityTimestamp, Stopwatch.GetTimestamp());
            _journalWriteLatencyTicks.Update(latencyTicks);
            _journalWriteSizeBytes.Update(sizeInBytes);
            _classifyAboveLatencyTicks = classifyAboveLatencyTicks;
        }

        public void RecordJournalWriteActivity()
        {
            Volatile.Write(ref _lastJournalWriteActivityTimestamp, Stopwatch.GetTimestamp());
        }

        internal const int RecentWriteActivityWindowMs = 3;

        // a journal write on this device was running recently or is likely imminent - background
        // work (pool zeroing, etc.) uses this to stand down
        public bool JournalWriteRecentlyActive =>
            Stopwatch.GetElapsedTime(Volatile.Read(ref _lastJournalWriteActivityTimestamp)).TotalMilliseconds < RecentWriteActivityWindowMs;

        public DeviceClass MeasuredDeviceClass
        {
            get
            {
                // small writes can be fast on a slow device, so we can't estimate from small writes only
                // gp3 writes small batches in 1.3-1.9ms, gp2 in 3-4ms, we need more than that...
                if (_journalWriteSizeBytes.Current < 256 * Voron.Global.Constants.Size.Kilobyte)
                    return DeviceClass.Unknown;

                var ewma = _journalWriteLatencyTicks.Current;
                var threshold = _classifyAboveLatencyTicks;
                if (ewma == 0 || threshold == 0)
                    return DeviceClass.Unknown;

                return ewma < threshold / 2 ? DeviceClass.Fast : DeviceClass.Budgeted;
            }
        }

        public bool IsMeasuredFastDevice => MeasuredDeviceClass == DeviceClass.Fast;

        // pre-zeroed pool files only pay where the filesystem's extent-conversion cost is what the
        // journal writes wait on (a fast local device). On a bandwidth-budgeted volume the fill
        // competes with every journal on the disk for the byte budget - measured 8-17% of
        // throughput on gp3 at high write load - so we skip the pool there entirely.
        public bool ShouldPrepareZeroedJournalsInBackground => IsMeasuredFastDevice;

        private const int MaxJournalZeroingStallMs = 500;

        /// <summary>
        /// Paces the background zero-fill against the journals on this device: journal writes are
        /// user facing, so the fill stands down while a write is running or imminent. Returns the
        /// milliseconds to wait before the next chunk, 0 to write it now, or -1 to abort the fill
        /// (the partially zeroed file is still banked). A fill that never finds a quiet gap aborts
        /// after a bounded cumulative stall instead of shadowing the journal forever.
        /// </summary>
        public int NextJournalZeroingStepMs(bool journalWriteActive, int stalledSoFarMs)
        {
            if (IsMeasuredFastDevice == false)
                return -1;

            if (journalWriteActive == false && JournalWriteRecentlyActive == false)
                return 0; // write the next chunk immediately

            if (stalledSoFarMs >= MaxJournalZeroingStallMs)
                return -1; // no sign of going quiet - abort

            return RecentWriteActivityWindowMs;
        }

        public void RecordSyncCost(long ticks)
        {
            // approximate EWMA (alpha = 1/4); a racy update just loses a sample
            var current = _syncCostTicksEwma;
            _syncCostTicksEwma = current == 0 ? ticks : current + (ticks - current) / 4;
        }

        public bool ShouldDrain()
        {
            var (nowMs, queueDepth) = SampleQueue();

            if (queueDepth > _activeQueueThreshold || _syncCostTicksEwma > _syncThresholdTicks)
            {
                _lastBusyMs = nowMs;
                if (_draining == false)
                {
                    _draining = true;
                    _activeQueueThreshold = _exitQueueDepth;
                    if (Log.IsDebugEnabled)
                    {
                        Log.Debug($"The device that holds '{_pathOnDevice}' is congested (queue {queueDepth:0.0}, " +
                                  $"sync {_syncCostTicksEwma / TimeSpan.TicksPerMillisecond}ms). Every environment on this device " +
                                  "moves to drain mode: flushes stop the writeback trickle, and each sync pushes its dirty ranges " +
                                  "in paced blocks before the fdatasync.");
                    }
                }
                return true;
            }
            
            if (_draining is false) 
                return false;

            if (nowMs - _lastBusyMs < ExitQuietMs)
                return true;

            _draining = false;
            _activeQueueThreshold = _enterQueueDepth;
            if (Log.IsDebugEnabled)
            {
                Log.Debug($"The device that holds '{_pathOnDevice}' is quiet again (queue {queueDepth:0.0}). Every environment " +
                            "on this device returns to trickle mode: flushes start writeback of their dirty ranges, and syncs " +
                            "use a plain fdatasync.");
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
