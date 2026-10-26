using System;
using System.Threading;
using Sparrow.Binary;

namespace Sparrow.Server.Threading;

/// <summary>
/// A counter that many threads update and few threads read. Each core updates its own cache line, so updates
/// don't contend with each other. <see cref="Increment"/> returns the slot it used and the matching
/// <see cref="Decrement"/> must get it back, so every slot stays exact and never goes negative, even when
/// the decrement runs on another core.
/// </summary>
public sealed class PerCoreCounter
{
    // 128 bytes per slot, to also cover adjacent line prefetch
    private const int Stride = 16;

    // bounds the memory of a counter on large machines, a few cores sharing a slot still contend far less than all of them
    private const int DefaultMaxSlots = 32;

    private readonly long[] _slots;
    private readonly int _mask;

    /// <param name="maxSlots">Upper bound on the slots, <see cref="Value"/> reads all of them.</param>
    public PerCoreCounter(int maxSlots = DefaultMaxSlots)
    {
        int count = Math.Min(Bits.PowerOf2(Environment.ProcessorCount), Bits.PowerOf2(maxSlots));
        _mask = count - 1;
        _slots = new long[count * Stride];
    }

    public int Increment()
    {
        int slot = (Thread.GetCurrentProcessorId() & _mask) * Stride;
        Interlocked.Increment(ref _slots[slot]);
        return slot;
    }

    public void Decrement(int slot)
    {
        Interlocked.Decrement(ref _slots[slot]);
    }

    /// <summary>
    /// The sum of all the slots. Exact when nothing updates the counter concurrently, otherwise a snapshot that
    /// can be off by the updates in flight. A thread that set a flag before reading it, and sees zero, knows that every
    /// increment it missed comes from a thread that will see the flag (each increment is followed by checking the flag).
    /// </summary>
    public long Value
    {
        get
        {
            long sum = 0;
            for (int i = 0; i < _slots.Length; i += Stride)
                sum += Volatile.Read(ref _slots[i]);
            return sum;
        }
    }
}
