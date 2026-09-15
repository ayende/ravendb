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
        
        private Sparrow.Server.Utils.SimpleEwma<long> _syncCostTicksEwma = new(smoothing: 4, 
            validityMs: 60_000); // sync happens rarely, so we give it plenty of time to expire any measurements
        
        private Sparrow.Server.Utils.SimpleEwma<double> _queueDepth = new(smoothing: 4);
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

        // Write size buckets. Latency on its own cannot tell a slow device from a big write - a shared
        // index journal writes ~4.5MB at a time, which costs ~2.5ms on NVMe, and a latency rule reads
        // that as a slow disk. Throughput tells them apart, but only within a size class, because a
        // small write is mostly fixed overhead however fast the device is.
        //
        // Measured MB/s at the median write latency, 10 map + 2 map-reduce Corax indexes:
        //
        //                    NVMe (m6idn)     gp3 (throttled EBS)
        //   < 256KB               171               4 -  8
        //   256KB - 1MB          1121              19 - 26
        //   1MB - 8MB            1881               9
        //   > 8MB                2825               -
        //
        // The two devices separate by more than 20x in every bucket, so the thresholds below sit in
        // a wide gap rather than on a knife edge.
        internal enum WriteSizeBucket
        {
            Tiny,   // < 256KB
            Small,  // < 1MB
            Medium, // < 8MB
            Large   // >= 8MB
        }

        private const int BucketCount = 4;

        private static WriteSizeBucket BucketFor(long sizeInBytes) => sizeInBytes switch
        {
            < 256 * Global.Constants.Size.Kilobyte => WriteSizeBucket.Tiny,
            < 1 * Global.Constants.Size.Megabyte => WriteSizeBucket.Small,
            < 8 * Global.Constants.Size.Megabyte => WriteSizeBucket.Medium,
            _ => WriteSizeBucket.Large
        };

        // A fast device sustains hundreds of MB/sec once a write is big enough to amortise the
        // per-write cost. Below 256KB even NVMe only reached ~170MB/sec, so that bucket gets a far
        // lower bar - a flat threshold there would call a genuine NVMe slow whenever it happened to
        // be writing small.
        private const long FastDeviceBytesPerSecond = 512L * 1024 * 1024;
        private const long FastDeviceBytesPerSecondForTinyWrites = 64L * 1024 * 1024;

        // enough samples that one stalled write cannot flip the verdict
        private const int MinSamplesToClassify = 16;

        // journal write telemetry across EVERY environment on this device, intentionally long-lived because it shows disk perf
        // if the disk perf change (burstable, load, etc), we'll update the status with ~8 measurements anyway
        private readonly Sparrow.Server.Utils.SimpleEwma<long>[] _bucketLatencyTicks = CreateEwmas();
        private readonly Sparrow.Server.Utils.SimpleEwma<long>[] _bucketSizeBytes = CreateEwmas();
        private readonly long[] _bucketSamples = new long[BucketCount];
        private long _lastJournalWriteActivityTimestamp;

        private static Sparrow.Server.Utils.SimpleEwma<long>[] CreateEwmas()
        {
            var ewmas = new Sparrow.Server.Utils.SimpleEwma<long>[BucketCount];
            for (int i = 0; i < ewmas.Length; i++)
                ewmas[i] = new Sparrow.Server.Utils.SimpleEwma<long>(smoothing: 8, validityMs: Sparrow.Server.Utils.SimpleEwma.NeverExpires);
            return ewmas;
        }

        public void RecordJournalWrite(long latencyTicks, long sizeInBytes, long time)
        {
            Volatile.Write(ref _lastJournalWriteActivityTimestamp, time);

            var bucket = (int)BucketFor(sizeInBytes);
            _bucketLatencyTicks[bucket].Update(latencyTicks);
            _bucketSizeBytes[bucket].Update(sizeInBytes);
            Interlocked.Increment(ref _bucketSamples[bucket]);
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
                // Largest bucket first. A big write is the honest measurement: it is dominated by
                // bandwidth, and no write cache can absorb it indefinitely. Small writes are the
                // easiest for a slow device to look good on, so they only get a say when nothing
                // larger has been observed.
                for (var bucket = BucketCount - 1; bucket >= 0; bucket--)
                {
                    if (Volatile.Read(ref _bucketSamples[bucket]) < MinSamplesToClassify)
                        continue;

                    var latencyTicks = _bucketLatencyTicks[bucket].Current;
                    var sizeBytes = _bucketSizeBytes[bucket].Current;
                    if (latencyTicks <= 0 || sizeBytes <= 0)
                        continue;

                    var bytesPerSecond = (long)(sizeBytes * (double)TimeSpan.TicksPerSecond / latencyTicks);
                    var required = bucket == (int)WriteSizeBucket.Tiny
                        ? FastDeviceBytesPerSecondForTinyWrites
                        : FastDeviceBytesPerSecond;

                    return bytesPerSecond >= required ? DeviceClass.Fast : DeviceClass.Budgeted;
                }

                return DeviceClass.Unknown; // no bucket has enough evidence yet, go for safe defaults
            }
        }

        public bool IsMeasuredFastDevice => MeasuredDeviceClass == DeviceClass.Fast;

        // fallocated file still pay for extent allocation, visible on NVMe devices (60% of write cost), pre-zero fill fixes that.
        // slow devices (gp3) have a bandwidth budget, zero-fill competes with journal writes, so we need to skip that there.
        public bool ShouldPrepareZeroedJournalsInBackground => IsMeasuredFastDevice;

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
