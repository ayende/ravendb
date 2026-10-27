using System;
using System.Threading;
using Sparrow.Binary;

namespace Sparrow.Server.Threading;

/// <summary>
/// The maximum of a value (like the time of the last access) that many threads raise and few threads read. Each core
/// raises its own cache line, so raising it doesn't contend, and <see cref="Value"/> is the maximum of all the slots.
/// </summary>
/// <remarks>
/// Raising is intentionally racy, a thread that was preempted between reading the clock and raising the value can
/// store an older value than another thread of the same core did. For a time that is used with a coarse granularity
/// (seconds or minutes), being a few milliseconds behind is harmless.
/// </remarks>
public sealed class PerCoreMaximum
{
    // 128 bytes per slot, to also cover adjacent line prefetch
    private const int Stride = 16;

    // bounds the memory on large machines, a few cores sharing a slot still contend far less than all of them
    private const int DefaultMaxSlots = 32;

    private readonly long[] _slots;
    private readonly int _mask;

    public PerCoreMaximum(long initialValue, int maxSlots = DefaultMaxSlots)
    {
        int count = Math.Min(Bits.PowerOf2(Environment.ProcessorCount), Bits.PowerOf2(maxSlots));
        _mask = count - 1;
        _slots = new long[count * Stride];
        Reset(initialValue);
    }

    public void Raise(long value)
    {
        ref long slot = ref _slots[(Thread.GetCurrentProcessorId() & _mask) * Stride];

        // PERF: the value usually didn't change since the last time (a coarse clock), so we skip writing to the cache line
        if (value > slot)
            slot = value;
    }

    public long Value
    {
        get
        {
            long max = long.MinValue;
            for (int i = 0; i < _slots.Length; i += Stride)
                max = Math.Max(max, Volatile.Read(ref _slots[i]));
            return max;
        }
    }

    /// <summary>
    /// Sets every slot, so it can also lower the value. A concurrent <see cref="Raise"/> may still win over it.
    /// </summary>
    public void Reset(long value)
    {
        for (int i = 0; i < _slots.Length; i += Stride)
            Volatile.Write(ref _slots[i], value);
    }
}
