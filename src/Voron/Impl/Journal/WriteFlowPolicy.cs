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
///  - why each merged batch closed (the close-reason policy keys its decisions on this)
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
    /// Why a merged batch stopped accumulating: a starved close means the batch could not
    /// grow; a window/size close means it could. Under the close-reason policy this is the
    /// discriminator that selects between pipelining and consolidation.
    /// </summary>
    public enum BatchCloseReason
    {
        QueueStarved,   // ran out of operations and the dry-up exit fired
        WindowElapsed,  // the batching window expired with work still queued
        SizeReached,    // hit the transaction / consolidation size cap
    }

    // -------------------------------------------------------------------------------------
    // EXPERIMENT (probe): the close-reason policy. The shipping rules key pipelining and
    // consolidation on the write-latency EWMA - a signal both mechanisms perturb, which makes
    // the regime choice bistable (a 32% run-to-run band was measured on identical config).
    // The alternate mode keys them on batch state instead:
    //   - consolidate while the typical journal write is below a target size, and stop the
    //     moment the queue empties (a starved batch cannot grow, waiting is pure latency)
    //   - pipeline when recent batches closed starved (they could not have grown, so
    //     overlapping their writes costs nothing) and the write is worth hiding
    // The two exclude each other through the close reason itself: while consolidation absorbs,
    // closes are window/size and the starved share collapses; when arrivals are the cap,
    // consolidation's wait exits immediately and the starved share rises. No veto needed.
    // Defaults preserve shipping behavior; flip after the A/B validates.
    // -------------------------------------------------------------------------------------
    private static readonly bool UseCloseReasonPolicy =
        Environment.GetEnvironmentVariable("RAVEN_WRITEFLOW") == "closereason";

    // where consolidation aims the journal write. Measured optima: ~80KB (NVMe patch c1024),
    // 43-54KB (gp3 patch c1531), and no gain past 152KB (NVMe writes c1024) - the default sits
    // inside that band until the online estimator replaces it
    private static readonly long TargetWriteSizeBytes =
        long.TryParse(Environment.GetEnvironmentVariable("RAVEN_WRITEFLOW_TARGET_KB"), out var kb)
            ? kb * Constants.Size.Kilobyte
            : 96 * Constants.Size.Kilobyte;

    // most recent batches closed starved => the batches are arrival-capped
    private const long MostlyStarvedPerMille = 900;

    private readonly long _pipelineAboveLatencyTicks;
    private readonly int _maxConcurrentJournalWrites;

    private SimpleEwma _writeLatencyTicks = new(smoothing: 8);
    private SimpleEwma _writeSizeBytes = new(smoothing: 8);
    private long _lastWriteActivityTimestamp;

    private volatile bool _consolidatingBatches;

    private long _batchesClosedStarved;
    private long _batchesClosedOnWindow;
    private long _batchesClosedOnSize;

    // per-batch telemetry: how large batches get, and what fraction close starved (i.e. the
    // batch was arrival-capped and could not have grown)
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

        MaybeTrace();
    }

    public long BatchOperationsEwma => _batchOperations.Current;

    public long BatchModifiedBytesEwma => _batchModifiedBytes.Current;

    // 0..1000: what share of recent batches closed because the arrivals dried up
    public long StarvedClosesPerMille => _starvedClosesPerMille.Current;

    // PROBE: one line per second describing the policy state, for the benchmark harness
    private static readonly bool Trace = Environment.GetEnvironmentVariable("RAVEN_WRITEFLOW_TRACE") == "1";
    private long _lastTraceTimestamp;
    private readonly string _traceId = Guid.NewGuid().ToString("N")[..8];

    private void MaybeTrace()
    {
        if (Trace == false)
            return;
        var last = Volatile.Read(ref _lastTraceTimestamp);
        var now = Stopwatch.GetTimestamp();
        if (last != 0 && Stopwatch.GetElapsedTime(last, now).TotalSeconds < 1)
            return;
        Volatile.Write(ref _lastTraceTimestamp, now);
        Console.WriteLine(
            $"WRITEFLOW id={_traceId} mode={(UseCloseReasonPolicy ? "closereason" : "latency")} " +
            $"starvedpm={_starvedClosesPerMille.Current} batchOps={_batchOperations.Current} batchKb={_batchModifiedBytes.Current / 1024} " +
            $"closes(s/w/z)={_batchesClosedStarved}/{_batchesClosedOnWindow}/{_batchesClosedOnSize} " +
            $"writeKb={_writeSizeBytes.Current / 1024} latMs={Math.Round((double)_writeLatencyTicks.Current / TimeSpan.TicksPerMillisecond, 2)} " +
            $"consol={_consolidatingBatches} pipe={ShouldPipelineNow} keep={KeepInFlightJournalWrites} consolLimitKb={ConsolidationSizeLimitInBytes / 1024}");
    }

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
        (UseCloseReasonPolicy
            // recent batches closed starved: they could not have grown, so overlapping their
            // writes shrinks nothing - and the latency floor keeps this off where the write is
            // too cheap to be worth hiding. The starved share is honest here: consolidation
            // absorbing means window/size closes, which turns this off by itself.
            ? _starvedClosesPerMille.Current >= MostlyStarvedPerMille &&
              _writeLatencyTicks.Current >= _pipelineAboveLatencyTicks &&
              IsCommitLatencyBound
            // shipping rule: the merger is waiting to get bigger batches, overlapping writes
            // would do the reverse; the device is slow enough that overlapping pays; and if we
            // are bounded by device bandwidth, pipelining won't help
            : _consolidatingBatches == false &&
              _writeLatencyTicks.Current >= _pipelineAboveLatencyTicks &&
              IsCommitLatencyBound);

    public bool CanPipeline(long totalNumberOf4Kbs) =>
        PipeliningEnabled &&
        // < 1MB, otherwise we'll be copying to our own buffer, then we have large write, etc. Doesn't pay off.
        totalNumberOf4Kbs <= JournalWritePipeline.MaxPipelinedBatch4Kbs &&
        (UseCloseReasonPolicy
            ? _starvedClosesPerMille.Current >= MostlyStarvedPerMille &&
              _writeLatencyTicks.Current >= _pipelineAboveLatencyTicks
            : _consolidatingBatches == false &&
              // the device is slow enough that overlapping writes pays for the smaller batches
              _writeLatencyTicks.Current >= _pipelineAboveLatencyTicks);

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

        if (UseCloseReasonPolicy)
        {
            // consolidate whenever the typical journal write is still below the target size -
            // the batch is not yet amortizing its fixed cost. The time cap only bounds the
            // pathological case; size is the real close condition, and a starved queue exits
            // immediately (see EmptyConsolidationWaitLimit)
            var writeSize = _writeSizeBytes.Current;
            _consolidatingBatches = writeSize > 0 && writeSize < TargetWriteSizeBytes;

            return _consolidatingBatches
                ? Math.Max(configuredMinimumMs, MaxBatchConsolidationWindowInMs)
                : configuredMinimumMs;
        }

        _consolidatingBatches = _consolidatingBatches
            ? writeLatencyTicks >= ExitBatchConsolidationAtLatencyTicks
            : writeLatencyTicks >= EnterBatchConsolidationAtLatencyTicks && IsCommitLatencyBound;

        if (_consolidatingBatches == false)
            return configuredMinimumMs;

        var windowMs = BatchConsolidationWindowLatencyFactor * writeLatencyTicks / TimeSpan.TicksPerMillisecond;
        return Math.Max(configuredMinimumMs, Math.Min(MaxBatchConsolidationWindowInMs, windowMs));
    }

    /// <summary>
    /// The merger stops absorbing once the batch's modified bytes reach this. Under the
    /// close-reason policy this is the write-size target translated to modified-bytes terms
    /// through the measured end-to-end ratio (diffing plus compression shrink the modified
    /// pages by 10-80x on real workloads, so the target must be converted, not compared raw).
    /// </summary>
    public long ConsolidationSizeLimitInBytes
    {
        get
        {
            if (UseCloseReasonPolicy == false)
                return MaxBatchConsolidationSizeInBytes;

            var writeSize = _writeSizeBytes.Current;
            var modified = _batchModifiedBytes.Current;
            if (writeSize <= 0 || modified <= 0)
                return MaxBatchConsolidationSizeInBytes; // no evidence yet - the time cap still bounds us

            var modifiedBytesPerWrittenByte = (double)modified / writeSize;
            var limit = (long)(TargetWriteSizeBytes * Math.Max(1, modifiedBytesPerWrittenByte));
            return Math.Clamp(limit, TargetWriteSizeBytes, MaxBatchConsolidationSizeInBytes);
        }
    }

    // under the close-reason policy a starved queue ends the hold at once: every writer is
    // already aboard, and (measured) even a 2ms speculative wait costs 34% where the batch is
    // arrival-capped. The shipping rule waits out two empty 1ms polls before giving up.
    public int EmptyConsolidationWaitLimit => UseCloseReasonPolicy ? 0 : MaxConsecutiveEmptyConsolidationWaits;

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
