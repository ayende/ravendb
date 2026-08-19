using System.Collections.Concurrent;
using System.Threading;

namespace Voron.Impl.Journal
{
    /// <summary>
    /// Per-physical-device signal for the data-file writeback mode (RavenDB-27375): an EWMA of
    /// the measured cost of the sync barrier path, in TimeSpan ticks. While the barrier is cheap
    /// the kernel's background writeback (helped by the per-flush trickle) is keeping up, and any
    /// waiting writeback we add only taxes journal writes sharing the device - so flushes trickle
    /// (initiate-only) and syncs are a plain fdatasync. Once the barrier turns expensive (the
    /// avalanche regime), the trickle stops and syncs drain the dirty ranges in bounded, waited
    /// blocks first.
    ///
    /// To keep the mode from oscillating, the recorded cost is the WHOLE barrier path
    /// (drain + fdatasync): when draining, that total still reflects what a monolithic sync
    /// would have cost for the same bytes, so the drain mode persists until the backlog itself
    /// subsides. The threshold comes from the caller (Storage.SyncWritebackBarrierCostThresholdInMs)
    /// because environments sharing a device may be configured differently.
    /// </summary>
    public sealed class WritebackPacingGate
    {
        private static readonly ConcurrentDictionary<ulong, WritebackPacingGate> DevicesById = new();

        public static WritebackPacingGate GetForDevice(ulong deviceId)
        {
            return DevicesById.GetOrAdd(deviceId, static _ => new WritebackPacingGate());
        }

        // A trickling device can never look expensive through the barrier alone - the trickle
        // is what keeps the barrier cheap, and when it floods the device the damage lands on
        // journal write latency instead (the RavenDB-27168 failure mode). So the mode flip
        // watches both: journal degradation (vs the device's own best-ever baseline) ENTERS
        // drain mode, and the barrier cost (drain + fdatasync, which is large exactly while a
        // backlog exists) HOLDS it until the backlog subsides.
        private const long JournalDegradedFactor = 2;

        private long _barrierCostTicksEwma;
        private long _journalCostTicksEwma;
        private long _journalBaselineTicks = long.MaxValue;

        public long BarrierCostTicks => Volatile.Read(ref _barrierCostTicksEwma);
        public long JournalCostTicks => Volatile.Read(ref _journalCostTicksEwma);
        public long JournalBaselineTicks => Volatile.Read(ref _journalBaselineTicks);

        public bool ShouldDrain(long barrierThresholdTicks)
        {
            if (Volatile.Read(ref _barrierCostTicksEwma) > barrierThresholdTicks)
                return true;

            var baseline = Volatile.Read(ref _journalBaselineTicks);
            return baseline != long.MaxValue &&
                   Volatile.Read(ref _journalCostTicksEwma) > baseline * JournalDegradedFactor;
        }

        public void RecordBarrierCost(long ticks)
        {
            // approximate EWMA (alpha = 1/4); a racy update just loses a sample
            var current = Volatile.Read(ref _barrierCostTicksEwma);
            Volatile.Write(ref _barrierCostTicksEwma, current == 0 ? ticks : current + (ticks - current) / 4);
        }

        public void RecordJournalWrite(long ticks)
        {
            var current = Volatile.Read(ref _journalCostTicksEwma);
            var next = current == 0 ? ticks : current + (ticks - current) / 8;
            Volatile.Write(ref _journalCostTicksEwma, next);

            // the smallest sustained journal cost this device ever showed is its healthy
            // baseline - no quiet-window bookkeeping needed
            if (next < Volatile.Read(ref _journalBaselineTicks))
                Volatile.Write(ref _journalBaselineTicks, next);
        }
    }
}
