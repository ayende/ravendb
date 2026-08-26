using System;
using System.Diagnostics;
using System.Threading;
using Sparrow.Utils;
using Voron.Global;

namespace Voron.Impl.Journal;

/// <summary>
/// The single place where every adaptive decision on the commit write path is made.
///
/// Three mechanisms share this path, and each one helps in a region where the others cannot:
///
///  1. Async transaction chaining (the merger) - executes batch N+1 while batch N's journal
///     write is in flight. The durability of batch N is the batching window: everything that
///     arrives during the write joins the next batch. This is the only mechanism that GROWS
///     batches. Measured: removing it costs 30-46% under load, but costs ~19% at idle
///     (nothing to overlap, the chain is pure bookkeeping).
///
///  2. Journal write pipelining - submits write N+1 to the device while write N is still in
///     flight, instead of serializing them behind the write lock. Helps ONLY when the batch
///     cannot grow anyway: at low closed-loop concurrency the population splits into two
///     groups (batch N's clients are notified only after batch N+1 closes), and overlapping
///     the two groups' writes is the only available lever (+77% at gp3 writes c8, p50 halved).
///     Everywhere else it RELEASES THE MERGER EARLY AND SHRINKS THE BATCH: -18% at gp3 patch
///     c96, -25% at NVMe patch c96 when forced.
///
///  3. Batch consolidation - holds the current batch open past the natural window so that one
///     large write replaces several small ones. Pays only when BOTH: the queue stays non-empty
///     during the hold (otherwise the wait is pure added latency: -34..-47% at NVMe patch c96),
///     AND the resulting write is still small (past ~80-150KB the fixed cost is amortized and
///     a bigger batch only adds execute time: +17% at NVMe patch c1024 at ~80KB writes,
///     -27% at NVMe writes c1024 when pushed from 152KB to 757KB).
///
/// The mechanisms interact through the batch size and through this policy's own telemetry
/// (pipelining lowers the measured write latency, consolidation raises it), so decisions made
/// in separate places fight each other and produce bistable throughput. Keeping every rule
/// here, reading shared telemetry, is what keeps them coherent.
///
/// Telemetry inputs (owned here, fed by the components):
///  - journal write latency and size (from the write pipeline, per completed device write)
///  - whether the merger is currently consolidating (set by the merger through this policy)
///  - why each merged batch closed (recorded for future rules, not yet consumed)
/// </summary>
public sealed class WriteFlowPolicy
{
    public enum DeviceClass
    {
        Unknown, // no evidence yet, go for safe defaults
        Fast,    // example: nvme - very high limits, or we never hit them
        Budgeted // example: gp3 - both bandwidth & IOPS limits that we hit
    }

    /// <summary>
    /// Why a merged batch stopped accumulating. Recorded per batch so future rules can key on
    /// it (a starved close means the batch could not grow; a window/size close means it could),
    /// not yet consumed by any decision below.
    /// </summary>
    public enum BatchCloseReason
    {
        QueueStarved,   // ran out of operations and the dry-up exit fired
        WindowElapsed,  // the batching window expired with work still queued
        SizeReached,    // hit the transaction / consolidation size cap
    }

    private readonly long _pipelineAboveLatencyTicks;
    private readonly int _maxConcurrentJournalWrites;

    private SimpleEwma _writeLatencyTicks = new(smoothing: 8);
    private SimpleEwma _writeSizeBytes = new(smoothing: 8);
    private long _lastWriteActivityTimestamp;

    private volatile bool _consolidatingBatches;

    private long _batchesClosedStarved;
    private long _batchesClosedOnWindow;
    private long _batchesClosedOnSize;

    // per-batch telemetry: how large batches get, and what fraction close starved. A starved
    // close means the batch could not have grown - the discriminator between the regime where
    // pipelining pays (batches are arrival-capped) and the one where it shrinks batches that
    // could have grown. Recorded here for future rules; no current decision reads these yet.
    private SimpleEwma _batchOperations = new(smoothing: 16);
    private SimpleEwma _batchModifiedBytes = new(smoothing: 16);
    private SimpleEwma _starvedClosesPerMille = new(smoothing: 16);

    public WriteFlowPolicy(StorageEnvironmentOptions options)
    {
        _pipelineAboveLatencyTicks = options.PipelineJournalWritesAboveLatencyInTicks;
        _maxConcurrentJournalWrites = Math.Clamp(options.MaxConcurrentJournalWrites, 1, StorageEnvironmentOptions.MaxSupportedConcurrentJournalWrites);
    }

    // ---------------------------------------------------------------------------------------
    // Telemetry
    // ---------------------------------------------------------------------------------------

    public void RecordJournalWrite(long latencyTicks, long sizeInBytes)
    {
        Volatile.Write(ref _lastWriteActivityTimestamp, Stopwatch.GetTimestamp());
        _writeLatencyTicks.Update(latencyTicks);
        _writeSizeBytes.Update(sizeInBytes);
    }

    public void RecordJournalWriteSubmitted()
    {
        Volatile.Write(ref _lastWriteActivityTimestamp, Stopwatch.GetTimestamp());
    }

    public void RecordBatchClosed(BatchCloseReason reason, int operations, long modifiedBytes)
    {
        switch (reason)
        {
            case BatchCloseReason.QueueStarved: _batchesClosedStarved++; break;
            case BatchCloseReason.WindowElapsed: _batchesClosedOnWindow++; break;
            case BatchCloseReason.SizeReached: _batchesClosedOnSize++; break;
        }

        _batchOperations.Update(operations);
        _batchModifiedBytes.Update(modifiedBytes);
        _starvedClosesPerMille.Update(reason == BatchCloseReason.QueueStarved ? 1000 : 0);
    }

    public long BatchOperationsEwma => _batchOperations.Current;

    public long BatchModifiedBytesEwma => _batchModifiedBytes.Current;

    // 0..1000: what share of recent batches closed because the arrivals dried up
    public long StarvedClosesPerMille => _starvedClosesPerMille.Current;

    public long WriteLatencyEwmaTicks => _writeLatencyTicks.Current;

    public long WriteSizeEwmaBytes => _writeSizeBytes.Current;

    // ---------------------------------------------------------------------------------------
    // Regime classification
    // ---------------------------------------------------------------------------------------

    // if we are making large writes, we'll be limited by device bandwidth, not latency.
    // latency is meaningful if we have many small commits, not large ones
    public bool IsCommitLatencyBound => _writeSizeBytes.Current < 256 * Constants.Size.Kilobyte;

    public DeviceClass MeasuredDeviceClass
    {
        get
        {
            // small writes can be fast on a slow device, so we can't estimate from small writes only
            // gp3 writes small batches in 1.3-1.9ms, gp2 in 3-4ms, we need more than that...
            if (_writeSizeBytes.Current < 256 * Constants.Size.Kilobyte)
                return DeviceClass.Unknown;

            var ewma = _writeLatencyTicks.Current;
            if (ewma == 0)
                return DeviceClass.Unknown;

            return ewma < _pipelineAboveLatencyTicks / 2 ? DeviceClass.Fast : DeviceClass.Budgeted;
        }
    }

    public bool IsMeasuredFastDevice => MeasuredDeviceClass == DeviceClass.Fast;

    internal const int RecentWriteActivityWindowMs = 3;

    // journal writes are user facing - background work (pool zeroing, etc.) uses this to stand down
    // while a write was recently running or another is likely imminent. Writes currently in flight
    // are tracked by the pipeline itself and OR-ed in by the caller.
    public bool JournalWriteRecentlyActive =>
        Stopwatch.GetElapsedTime(Volatile.Read(ref _lastWriteActivityTimestamp)).TotalMilliseconds < RecentWriteActivityWindowMs;

    // ---------------------------------------------------------------------------------------
    // Pipelining (overlapping journal writes on the device)
    // ---------------------------------------------------------------------------------------

    public bool PipeliningEnabled => _maxConcurrentJournalWrites > 1;

    public bool ShouldPipelineNow =>
        PipeliningEnabled &&
        // the merger is waiting to get bigger batches, overlapping writes will do the reverse
        _consolidatingBatches == false &&
        // the device is slow enough that overlapping writes pays for the smaller batches
        _writeLatencyTicks.Current >= _pipelineAboveLatencyTicks &&
        // if we are bounded by device bandwidth, pipelining won't help
        IsCommitLatencyBound;

    public bool CanPipeline(long totalNumberOf4Kbs) =>
        PipeliningEnabled &&
        _consolidatingBatches == false &&
        // < 1MB, otherwise we'll be copying to our own buffer, then we have large write, etc. Doesn't pay off.
        totalNumberOf4Kbs <= JournalWritePipeline.MaxPipelinedBatch4Kbs &&
        // the device is slow enough that overlapping writes pays for the smaller batches
        _writeLatencyTicks.Current >= _pipelineAboveLatencyTicks;

    // how many async-committed transactions the merger leaves in flight instead of completing.
    // Overlapping writes doesn't make sense when we have large journal writes - we are bandwidth
    // bound then anyway
    public int KeepInFlightJournalWrites => ShouldPipelineNow ? _maxConcurrentJournalWrites - 1 : 0;

    // ---------------------------------------------------------------------------------------
    // Batch consolidation (the merger holding a batch open to get one large write)
    // ---------------------------------------------------------------------------------------

    // On a slow device the group-commit equilibrium can collapse into tiny batches: short writes
    // release few clients, next requests dribble in and we get small batches.
    // result: volume IOPS bound at a fraction of its bandwidth. We intentionally increase
    // transaction time here, to batch more
    private const long EnterBatchConsolidationAtLatencyTicks = 8 * TimeSpan.TicksPerMillisecond;   // the device is queuing under the write traffic
    private const long ExitBatchConsolidationAtLatencyTicks = 4 * TimeSpan.TicksPerMillisecond;    // consolidation grows the writes, and must not turn itself off too quickly
    private const long BatchConsolidationWindowLatencyFactor = 2;                                  // hold for up to this many write durations...
    private const long MaxBatchConsolidationWindowInMs = 50;                                       // ...but never longer than this

    public const long MaxBatchConsolidationSizeInBytes = 128 * Constants.Size.Megabyte;

    // two empty one-millisecond waits in a row mean the arrivals dried up - every writer is already
    // aboard this batch, and holding it any longer is pure added latency for them
    public const int MaxConsecutiveEmptyConsolidationWaits = 2;

    public bool ConsolidatingBatches => _consolidatingBatches;

    /// <summary>
    /// Called by the merger on every batch to decide how long the batch may stay open.
    /// Enter/exit use different thresholds (sticky hysteresis): consolidation grows the writes,
    /// which raises the very latency it reads, so a single threshold would flap.
    /// While consolidating, the window scales with the measured write duration - a flat hold
    /// measurably hurts healthy workloads.
    /// </summary>
    public double GetBatchingWindowDurationInMs(double configuredMinimumMs)
    {
        var writeLatencyTicks = _writeLatencyTicks.Current;

        _consolidatingBatches = _consolidatingBatches
            ? writeLatencyTicks >= ExitBatchConsolidationAtLatencyTicks
            : writeLatencyTicks >= EnterBatchConsolidationAtLatencyTicks && IsCommitLatencyBound;

        if (_consolidatingBatches == false)
            return configuredMinimumMs;

        var windowMs = BatchConsolidationWindowLatencyFactor * writeLatencyTicks / TimeSpan.TicksPerMillisecond;
        return Math.Max(configuredMinimumMs, Math.Min(MaxBatchConsolidationWindowInMs, windowMs));
    }

    // ---------------------------------------------------------------------------------------
    // Journal compression
    // ---------------------------------------------------------------------------------------

    // On NVMe devices, writing the full data to disk is _faster_ than compressing it first,
    // so a measured-fast device only compresses transactions that are large enough for the
    // bandwidth saving to matter.
    private const int FastDeviceCompressTxAboveSizeInBytes = 512 * Constants.Size.Kilobyte;

    public long GetCompressTxAboveSizeInBytes(long configured) =>
        IsMeasuredFastDevice ? Math.Max(configured, FastDeviceCompressTxAboveSizeInBytes) : configured;

    // Zstd is 400MB/sec vs. LZ4 1.5GB/sec. It pays to pay for Zstd if the device is constrained:
    // there, bytes are the metered resource and the journal spends the same budget as writeback
    // and sync. Measured at the one point the classifier fires (gp3 writes c1531): throughput
    // neutral vs Lz4, -27% journal bytes.
    public JournalCompressionAlgorithm ResolveJournalCompressionAlgorithm(JournalCompressionAlgorithm configured)
    {
        if (configured != JournalCompressionAlgorithm.Auto)
            return configured; // pinned by the user, in either direction

        return MeasuredDeviceClass == DeviceClass.Budgeted
            ? JournalCompressionAlgorithm.Zstd
            : JournalCompressionAlgorithm.Lz4;
    }

    // ---------------------------------------------------------------------------------------
    // Flusher interaction
    // ---------------------------------------------------------------------------------------

    // journal writes are user facing, high latency there effects the user, so we want to
    // prioritize them over flushing - but we can't starve the flusher indefinitely either,
    // so the caller bounds this by the flush backlog.
    public bool ShouldFlusherYieldToJournal =>
        _writeLatencyTicks.Current >= _pipelineAboveLatencyTicks * 2;
}
