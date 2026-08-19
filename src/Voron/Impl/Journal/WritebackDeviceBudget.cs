using System;
using System.Collections.Concurrent;
using System.Threading;

namespace Voron.Impl.Journal
{
    /// <summary>
    /// Per-physical-device budget for paced data-file writeback (RavenDB-27375).
    /// The controlled variable is the total number of writeback blocks in flight against the
    /// device; concurrent drains split it between them. Journal-write latency on the same device
    /// is the guard signal: paced writeback must never do to journal commit latency what the
    /// writeback flood did, so we decrease multiplicatively on a breach of the quiet baseline
    /// and increase additively only after a long quiet stretch.
    /// </summary>
    public sealed class WritebackDeviceBudget
    {
        private static readonly ConcurrentDictionary<ulong, WritebackDeviceBudget> DevicesById = new();

        public static WritebackDeviceBudget GetForDevice(ulong deviceId)
        {
            return DevicesById.GetOrAdd(deviceId, static _ => new WritebackDeviceBudget());
        }

        public const int MinDepth = 1;
        public const int MaxDepth = 8;

        // breach when a journal write during writeback exceeds 1.5x the quiet baseline
        private const long GuardBreachNumerator = 3;
        private const long GuardBreachDenominator = 2;
        private const long DecreaseCooldownMs = 5_000;
        private const long IncreaseAfterQuietMs = 30_000;

        private int _depth = 2;
        private int _activeDrains;
        private long _quietJournalLatencyMicros; // EWMA of journal writes seen while no drain is active
        private long _lastDecreaseTicks;
        private long _lastIncreaseTicks;

        public int CurrentDepth => Volatile.Read(ref _depth);
        public int ActiveDrains => Volatile.Read(ref _activeDrains);
        public long QuietJournalLatencyMicros => Volatile.Read(ref _quietJournalLatencyMicros);

        public void RecordJournalWrite(long micros)
        {
            if (Volatile.Read(ref _activeDrains) == 0)
            {
                // approximate EWMA (alpha = 1/8); a racy update just loses a sample
                var quiet = Volatile.Read(ref _quietJournalLatencyMicros);
                Volatile.Write(ref _quietJournalLatencyMicros, quiet == 0 ? micros : quiet + (micros - quiet) / 8);
                return;
            }

            var baseline = Volatile.Read(ref _quietJournalLatencyMicros);
            if (baseline == 0 || micros * GuardBreachDenominator <= baseline * GuardBreachNumerator)
                return;

            var now = Environment.TickCount64;
            var last = Volatile.Read(ref _lastDecreaseTicks);
            if (now - last < DecreaseCooldownMs ||
                Interlocked.CompareExchange(ref _lastDecreaseTicks, now, last) != last)
                return;

            var depth = Volatile.Read(ref _depth);
            if (depth > MinDepth)
                Volatile.Write(ref _depth, Math.Max(MinDepth, depth / 2));
        }

        public DrainScope EnterDrain(out int depthForThisDrain)
        {
            var active = Interlocked.Increment(ref _activeDrains);
            MaybeIncreaseDepth();
            depthForThisDrain = Math.Max(MinDepth, Volatile.Read(ref _depth) / active);
            return new DrainScope(this);
        }

        private void MaybeIncreaseDepth()
        {
            var now = Environment.TickCount64;
            if (now - Volatile.Read(ref _lastDecreaseTicks) < IncreaseAfterQuietMs)
                return;
            var lastIncrease = Volatile.Read(ref _lastIncreaseTicks);
            if (now - lastIncrease < IncreaseAfterQuietMs ||
                Interlocked.CompareExchange(ref _lastIncreaseTicks, now, lastIncrease) != lastIncrease)
                return;

            var depth = Volatile.Read(ref _depth);
            if (depth < MaxDepth)
                Volatile.Write(ref _depth, depth + 1);
        }

        public readonly struct DrainScope : IDisposable
        {
            private readonly WritebackDeviceBudget _parent;

            public DrainScope(WritebackDeviceBudget parent)
            {
                _parent = parent;
            }

            public void Dispose()
            {
                if (_parent != null)
                    Interlocked.Decrement(ref _parent._activeDrains);
            }
        }
    }
}
