using System.Collections.Concurrent;
using System.Threading;

namespace Voron.Impl.Journal
{
    /// <summary>
    /// Per-physical-device gate for paced data-file writeback (RavenDB-27375), keyed on the
    /// directly measured cost of the exact operation the pacing exists to avoid: the sync
    /// barrier. While the kernel's background writeback keeps up, fdatasync is cheap and
    /// pre-draining only taxes journal writes - so we don't. Once the barrier turns expensive
    /// (the avalanche regime), we drain the dirty ranges in bounded blocks first.
    ///
    /// To keep the gate from oscillating, the recorded cost is the WHOLE barrier path
    /// (drain + fdatasync): when draining, that total still reflects what a monolithic sync
    /// would have cost for the same bytes, so the gate stays open until the backlog itself
    /// subsides.
    /// </summary>
    public sealed class WritebackPacingGate
    {
        private static readonly ConcurrentDictionary<ulong, WritebackPacingGate> DevicesById = new();

        public static WritebackPacingGate GetForDevice(ulong deviceId)
        {
            return DevicesById.GetOrAdd(deviceId, static _ => new WritebackPacingGate());
        }

        // above this the barrier is hurting whoever shares the device with it; the regimes we
        // measured sit an order of magnitude apart (healthy: ~5ms, avalanche: hundreds of ms)
        private const long DrainWhenBarrierCostExceedsMicros = 100_000;

        private long _barrierCostMicrosEwma;

        public bool ShouldDrainBeforeSync => Volatile.Read(ref _barrierCostMicrosEwma) > DrainWhenBarrierCostExceedsMicros;

        public long BarrierCostMicrosEwma => Volatile.Read(ref _barrierCostMicrosEwma);

        public void RecordBarrierCost(long micros)
        {
            // approximate EWMA (alpha = 1/4); a racy update just loses a sample
            var current = Volatile.Read(ref _barrierCostMicrosEwma);
            Volatile.Write(ref _barrierCostMicrosEwma, current == 0 ? micros : current + (micros - current) / 4);
        }
    }
}
