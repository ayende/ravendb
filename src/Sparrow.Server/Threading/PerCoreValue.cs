using System;
using System.Runtime.InteropServices;
using System.Threading;
using Sparrow.Binary;

namespace Sparrow.Server.Threading;

/// <summary>
/// How a <see cref="PerCoreValue{TOp}"/> applies a value to the slot of a core, and combines the slots into its value.
/// </summary>
public interface IPerCoreOp
{
    void Apply(ref long slot, long value);

    long Combine(long a, long b);
}

public struct PerCoreSum : IPerCoreOp
{
    public void Apply(ref long slot, long value) => Interlocked.Add(ref slot, value);

    public long Combine(long a, long b) => a + b;
}

public struct PerCoreMax : IPerCoreOp
{
    public void Apply(ref long slot, long value)
    {
        // Intentionally racy: a thread that was preempted between reading the time and raising the slot can store an older
        // value than another thread of the same core did. For a time that is used in seconds or minutes, being a few
        // milliseconds behind is harmless.
        // PERF: the value usually didn't change since the last time (a time in milliseconds), so we skip writing to the cache line
        if (value > slot)
            slot = value;
    }

    public long Combine(long a, long b) => Math.Max(a, b);
}

/// <summary>
/// A value that many threads update and few threads read, each core updates its own cache line and <see cref="Value"/>
/// combines them.
/// </summary>
public sealed class PerCoreValue<TOp> where TOp : struct, IPerCoreOp
{
    [StructLayout(LayoutKind.Explicit, Size = 64)]
    private struct PaddedValue
    {
        // at the end of the 64 bytes: every indexing reads the array's length (bounds check), which is 8 bytes before
        // the first element, so a value at the start of the first slot would share that line and each write to it
        // would invalidate the line on all the cores
        [FieldOffset(56)]
        public long Value;
    }

    private readonly PaddedValue[] _slots;
    private readonly int _mask;

    public PerCoreValue(long initialValue = 0, int maxSlots = int.MaxValue)
    {
        int count = Bits.PowerOf2(Math.Min(Environment.ProcessorCount, maxSlots));
        _mask = count - 1;
        _slots = new PaddedValue[count];
        Reset(initialValue);
    }

    /// <summary>
    /// Applies the value to the slot of the current core. The returned <see cref="Slot"/> applies to the same slot later,
    /// even from another core (e.g. -1 to undo +1).
    /// </summary>
    public Slot Apply(long value)
    {
        int slot = Thread.GetCurrentProcessorId() & _mask;
        default(TOp).Apply(ref _slots[slot].Value, value);
        return new Slot(this, slot);
    }

    public long Value
    {
        get
        {
            long value = Volatile.Read(ref _slots[0].Value);
            for (int i = 1; i < _slots.Length; i++)
                value = default(TOp).Combine(value, Volatile.Read(ref _slots[i].Value));
            return value;
        }
    }

    /// <summary>
    /// Sets every slot. A concurrent <see cref="Apply"/> may still win over it.
    /// </summary>
    public void Reset(long value)
    {
        for (int i = 0; i < _slots.Length; i++)
            Volatile.Write(ref _slots[i].Value, value);
    }

    public readonly struct Slot(PerCoreValue<TOp> owner, int slot)
    {
        public void Apply(long value) => default(TOp).Apply(ref owner._slots[slot].Value, value);
    }
}
