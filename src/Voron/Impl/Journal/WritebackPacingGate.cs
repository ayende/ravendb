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

        private long _barrierCostTicksEwma;

        public long BarrierCostTicks => Volatile.Read(ref _barrierCostTicksEwma);

        public bool IsBarrierExpensive(long thresholdTicks)
        {
            return Volatile.Read(ref _barrierCostTicksEwma) > thresholdTicks;
        }

        public void RecordBarrierCost(long ticks)
        {
            // approximate EWMA (alpha = 1/4); a racy update just loses a sample
            var current = Volatile.Read(ref _barrierCostTicksEwma);
            Volatile.Write(ref _barrierCostTicksEwma, current == 0 ? ticks : current + (ticks - current) / 4);
        }
    }
}
