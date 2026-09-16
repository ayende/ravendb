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
    /// Per physical device write policy / budget goes here. All environments on the same disk
    /// share the same limits and budgets.
    ///
    /// Decisions owned here:
    ///
    /// * Writeback mode:
    ///     - Trickle: each flush starts writeback of its dirty ranges (async) and the sync is a plain fdatasync; best while the device has headroom. 
    ///     - Drain:   flushes do not start writeback; the sync pushes the dirty ranges in bounded blocks before the fdatasync(), limiting the I/O we 
    ///                 generate benefitting from dead-write merging. 
    ///        
    ///     Using device queue depth (the iostat "aqu-sz" number, sampled once a second).
    ///
    /// * Device classification:
    ///     - Fast (nvme-like: limits so high we never hit them)
    ///     - Budgeted (gp3-like: metered bandwidth/IOPS we do hit)
    /// 
    ///   Using: the journal write latency and size observed across every environment on the device. 
    ///   Feeds the codec choice and the compression threshold in each environment's WriteFlowPolicy.
    ///
    /// * Journal zeroing / pool prewarming - prepaying the filesystem extent-conversion cost
    ///   only pays on a fast local device; on a budgeted volume the fill competes with every
    ///   journal on the disk (measured 8-17% of throughput on gp3 under load).
    /// </summary>
    public sealed class DeviceWriteBudget
    {
        private static readonly ConcurrentDictionary<ulong, DeviceWriteBudget> DevicesById = new();
        private static readonly RavenLogger Log = RavenLogManager.Instance.GetLoggerForGlobalVoron<DeviceWriteBudget>();

        public static DeviceWriteBudget GetForDevice(ulong deviceId, string pathOnDevice, long syncCostThresholdTicks, int queueDepthThreshold)
        {
            if (DevicesById.TryGetValue(deviceId, out var existing))
                return existing;

            return Unlikely();

            DeviceWriteBudget Unlikely()
            {
                var reader = DeviceQueueDepthReader.TryCreate(pathOnDevice, deviceId);
                var candidate = new DeviceWriteBudget(reader, pathOnDevice, syncCostThresholdTicks, queueDepthThreshold);
                var winner = DevicesById.GetOrAdd(deviceId, candidate);
                if (ReferenceEquals(winner, candidate) == false)
                    reader?.Dispose(); // lost the race - don't leak the device handle
                return winner;
            }
        }

        public static DeviceWriteBudget CreateUnshared(StorageEnvironmentOptions opts) =>
            new(queueReader: null, pathOnDevice: "(unshared)", opts.SyncWritebackBarrierCostThresholdTicks, opts.SyncWritebackDrainQueueDepthThreshold);

        private const long SampleIntervalMs = 1_000;
        private const long ExitQuietMs = 30_000;
        internal const int RecentWriteActivityWindowMs = 3;
        // in Stopwatch.GetTimestamp units (Stopwatch.Frequency per second - NOT TimeSpan ticks)
        private static readonly long RecentWriteActivityWindowTimestampTicks = RecentWriteActivityWindowMs * Stopwatch.Frequency / 1000;
        private readonly DeviceQueueDepthReader _queueReader; // null = no queue signal on this platform
        private readonly string _pathOnDevice;
        private readonly long _syncThresholdTicks;
        private readonly double _enterQueueDepth;
        private readonly double _exitQueueDepth;
        private readonly Stopwatch _clock = Stopwatch.StartNew();
        private double _activeQueueThreshold; // the enter value while trickling, the exit value while draining
        
        private SimpleEwma<long> _syncCostTicksEwma = new(smoothing: 4, 
            validityMs: 60_000); // sync happens rarely, so we give it plenty of time to expire any measurements
        
        private SimpleEwma<double> _queueDepth = new(smoothing: 4);
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

        public enum DeviceClass
        {
            Unknown, // no evidence yet, go for safe defaults
            Fast,    // example: nvme - very high limits, or we never hit them
            Budgeted // example: gp3 - both bandwidth & IOPS limits that we hit
        }

        // There is a fixed latency per write, we account for the size of the write when computing speed.
        //
        // Measured MB/s at the median write latency, 10 map + 2 map-reduce Corax indexes:
        // NVMe (m6idn)   gp3 (throttled EBS)
        //    171 MB/s      4 -  8 MB/s
        //  1,121 MB/s     19 - 26 MB/s
        //  1,881 MB/s           9 MB/s
        //  2,825 MB/s             -
        private static readonly (long UnderSizeInBytes, long FastBytesPerSecond)[] WriteSizeClasses =
        [
            (256 * Global.Constants.Size.Kilobyte,  64L * 1024 * 1024), // < 256 KB, 64 MB/s
            (1 * Global.Constants.Size.Megabyte,   512L * 1024 * 1024), // < 1 MB, 512 MB/s
            (8 * Global.Constants.Size.Megabyte,   512L * 1024 * 1024), // < 8 MB, 512 MB/s
            (long.MaxValue,                        512L * 1024 * 1024), // >= 8 MB, 512 MB/s
        ];

        private static readonly int BucketCount = WriteSizeClasses.Length;

        private static int BucketFor(long sizeInBytes)
        {
            for (var bucket = 0; bucket < BucketCount - 1; bucket++)
            {
                if (sizeInBytes < WriteSizeClasses[bucket].UnderSizeInBytes)
                    return bucket;
            }

            return BucketCount - 1; // the last class is open-ended, everything else lands here
        }

        // enough samples that one stalled write cannot flip the verdict
        private const int MinSamplesToClassify = 16;

        // journal write telemetry across EVERY environment on this device, intentionally long-lived because it shows disk perf
        // if the disk perf change (burstable, load, etc), we'll update the status with ~8 measurements anyway
        private readonly SimpleEwma<long>[] _bucketLatencyTicks = CreateEwmas();
        private readonly SimpleEwma<long>[] _bucketSizeBytes = CreateEwmas();
        private readonly long[] _bucketSamples = new long[BucketCount];

        private int _decidingBucket = NoBucketDecided; // The largest bucket holding enough samples decides, and it only ever moves up
        private const int NoBucketDecided = -1;
        private long _lastJournalWriteActivityTimestamp;

        private static SimpleEwma<long>[] CreateEwmas()
        {
            var ewmas = new SimpleEwma<long>[BucketCount];
            for (int i = 0; i < ewmas.Length; i++)
                ewmas[i] = new SimpleEwma<long>(smoothing: 8, validityMs: SimpleEwma.NeverExpires);
            return ewmas;
        }

        public void RecordJournalWrite(long latencyTicks, long sizeInBytes, long time)
        {
            Volatile.Write(ref _lastJournalWriteActivityTimestamp, time);

            var bucket = BucketFor(sizeInBytes);
            _bucketLatencyTicks[bucket].Update(latencyTicks);
            _bucketSizeBytes[bucket].Update(sizeInBytes);

            var samples = Interlocked.Increment(ref _bucketSamples[bucket]);

            // the deciding class only ever moves up
            if (samples >= MinSamplesToClassify && bucket > Volatile.Read(ref _decidingBucket))
                Volatile.Write(ref _decidingBucket, bucket);
        }

        public void RecordJournalWriteActivity(long time)
        {
            Volatile.Write(ref _lastJournalWriteActivityTimestamp, time);
        }

        public bool JournalWriteRecentlyActive =>
            Stopwatch.GetTimestamp() - Volatile.Read(ref _lastJournalWriteActivityTimestamp) < RecentWriteActivityWindowTimestampTicks;

        public DeviceClass MeasuredDeviceClass
        {
            get
            {
                var bucket = Volatile.Read(ref _decidingBucket);
                if (bucket == NoBucketDecided)
                    return DeviceClass.Unknown; // no bucket has enough evidence yet, go for safe defaults

                var latencyTicks = _bucketLatencyTicks[bucket].Current;
                var sizeBytes = _bucketSizeBytes[bucket].Current;
                if (latencyTicks <= 0 || sizeBytes <= 0)
                    return DeviceClass.Unknown;

                var bytesPerSecond = (long)(sizeBytes * (double)TimeSpan.TicksPerSecond / latencyTicks);

                return bytesPerSecond >= WriteSizeClasses[bucket].FastBytesPerSecond ? DeviceClass.Fast : DeviceClass.Budgeted;
            }
        }

        public bool IsMeasuredFastDevice => MeasuredDeviceClass == DeviceClass.Fast;

        public readonly record struct WriteSizeClassStats(
            long UnderSizeInBytes,
            long NumberOfWrites,
            double LatencyMs,
            long AverageSizeInBytes,
            long BytesPerSecond,
            long FastBytesPerSecond);

        public WriteSizeClassStats[] GetWriteSizeClassStats()
        {
            var deciding = Volatile.Read(ref _decidingBucket);
            var stats = new WriteSizeClassStats[BucketCount];

            for (var bucket = 0; bucket < stats.Length; bucket++)
            {
                var latencyTicks = _bucketLatencyTicks[bucket].Current;
                var sizeBytes = _bucketSizeBytes[bucket].Current;

                stats[bucket] = new WriteSizeClassStats(
                    WriteSizeClasses[bucket].UnderSizeInBytes,
                    Volatile.Read(ref _bucketSamples[bucket]),
                    latencyTicks / (double)TimeSpan.TicksPerMillisecond,
                    sizeBytes,
                    latencyTicks > 0 ? (long)(sizeBytes * (double)TimeSpan.TicksPerSecond / latencyTicks) : 0,
                    WriteSizeClasses[bucket].FastBytesPerSecond);
            }

            return stats;
        }
        private const int MaxJournalZeroingStallMs = 500;

        // callback from zero fill PAL, let it know when it should pace itself to avoid contentions with journal
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

        public void RecordSyncCost(long ticks) => _syncCostTicksEwma.Update(ticks);

        public bool ShouldDrain()
        {
            var (nowMs, queueDepth) = SampleQueue();
            // either high device queue depth, or high sync cost (too much I/O for the device to keep up)
            if (queueDepth > _activeQueueThreshold || _syncCostTicksEwma.Current > _syncThresholdTicks)
            {
                _lastBusyMs = nowMs;
                if (_draining == false)
                {
                    _draining = true;
                    _activeQueueThreshold = _exitQueueDepth;
                    if (Log.IsDebugEnabled)
                    {
                        Log.Debug($"The device that holds '{_pathOnDevice}' is congested (queue {queueDepth:0.0}, " +
                                  $"sync {_syncCostTicksEwma.Current / TimeSpan.TicksPerMillisecond}ms). Every environment on this device " +
                                  "moves to drain mode: flushes stop the writeback trickle, and each sync pushes its dirty ranges " +
                                  "in paced blocks before the fdatasync.");
                    }
                }
                return true;
            }
            
            if (_draining is false) 
                return false;

            // we require a quiet period before we exit drain mode, to avoid thrashing back and forth
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
            if (_queueReader == null)
                return (nowMs, _queueDepth.Current);

            var last = _lastSampleMs;
            if (nowMs - last < SampleIntervalMs ||
                Interlocked.CompareExchange(ref _lastSampleMs, nowMs, last) != last)
                return (nowMs, _queueDepth.Current); // not due yet, or another thread samples

            try
            {
                _queueDepth.Update(_queueReader.Read());
            }
            catch
            {
                // a failed read is a lost sample, nothing more
            }

            return (nowMs, _queueDepth.Current);
        }
    }
}
