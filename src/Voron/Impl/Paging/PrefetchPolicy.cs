using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Sparrow;
using Sparrow.Binary;
using Sparrow.LowMemory;
using Sparrow.Server.LowMemory;

namespace Voron.Impl.Paging;

/// <summary>
/// Decides, for every data file in the process, when a segment (4 MB by default) is worth reading ahead.
///
/// A segment is prefetched when it collects <see cref="Threshold"/> hits within one epoch. An epoch lasts until the
/// process has made <see cref="HitsPerEpoch"/> x (memory for the page cache, in segments) page accesses, or
/// <see cref="MaxEpochSeconds"/>. A segment of any working set that fits in memory then gets about
/// <see cref="HitsPerEpoch"/> hits per epoch, while uniform access over data larger than memory gets
/// HitsPerEpoch x memory / data and almost never reaches the threshold. The accesses of all data files count
/// (all databases share the page cache), so more active databases need hotter segments. Every device has a
/// <see cref="DevicePrefetchGate"/> that stops prefetches while the device is congested.
/// </summary>
public sealed class PrefetchPolicy : IDisposable
{
    public const int HitsPerEpoch = 15;
    public const int MaxEpochSeconds = 30;
    public const int RearmSeconds = 60;
    public const int SequentialSeconds = 2;
    public const long SegmentSize = 4 * 1024 * 1024;
    private const int TickMs = 250;

    public static readonly PrefetchPolicy Global = new(startTimer: true);

    // read on every page access
    private volatile int _epoch = 1;
    private volatile int _nowSeconds = 1;

    [StructLayout(LayoutKind.Explicit, Size = 64)]
    private struct PaddedCounter
    {
        [FieldOffset(0)]
        public long Value;
    }

    // the page accesses of every data file, a cache line per processor (a power of two, so a mask picks it)
    private readonly PaddedCounter[] _accesses = new PaddedCounter[Bits.PowerOf2(Environment.ProcessorCount)];

    private readonly object _locker = new();
    private readonly Dictionary<ulong, DevicePrefetchGate> _gates = new();
    private DevicePrefetchGate[] _gatesSnapshot = []; // replaced when a gate is added, read by every tick
    private readonly Stopwatch _clock = Stopwatch.StartNew();
    private readonly Timer _timer;
    private long _epochAccesses;
    private long _epochStartMs;
    private long _lastTotalAccesses;

    public PrefetchPolicy(bool startTimer)
    {
        if (startTimer)
            _timer = new Timer(_ => TickFromSystem(), null, TickMs, TickMs);
    }

    public int Epoch => _epoch;
    public int Threshold => HitsPerEpoch;
    public int NowSeconds => _nowSeconds;

    /// <summary>Counts a page access. Not atomic: a lost increment only delays the end of the epoch a little.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal void CountAccess() => _accesses[Environment.CurrentManagedThreadId & (_accesses.Length - 1)].Value++;

    /// <summary>The gate of a device, shared by every data file on it.</summary>
    internal DevicePrefetchGate GetGate(ulong deviceId)
    {
        lock (_locker)
        {
            if (_gates.TryGetValue(deviceId, out var gate) == false)
            {
                _gates[deviceId] = gate = DevicePrefetchGate.Create(deviceId);
                _gatesSnapshot = [.. _gates.Values];
            }
            return gate;
        }
    }

    private void TickFromSystem()
    {
        try
        {
            // the low-memory monitor samples memory every few seconds; before it runs (tools, tests) we read it ourselves
            var memory = LowMemoryNotification.Instance.LastMemoryInfo;
            if (memory.TotalPhysicalMemory.GetValue(SizeUnit.Bytes) == 0)
                memory = MemoryInformation.GetMemoryInfo();
            Tick(_clock.ElapsedMilliseconds, memory.AvailableMemory.GetValue(SizeUnit.Bytes));
        }
        catch
        {
            // a lost tick only delays the next epoch
        }
    }

    /// <summary>Advances the epoch, the clock and the device gates. Called every 250 ms (tests call it directly).</summary>
    internal void Tick(long nowMs, long memoryForCacheBytes)
    {
        long total = 0;
        for (var i = 0; i < _accesses.Length; i++)
            total += Volatile.Read(ref _accesses[i].Value);
        _epochAccesses += total - _lastTotalAccesses;
        _lastTotalAccesses = total;

        var memorySegments = Math.Max(1, memoryForCacheBytes / SegmentSize);
        if (_epochAccesses >= HitsPerEpoch * memorySegments || nowMs - _epochStartMs >= MaxEpochSeconds * 1000L)
        {
            _epoch = _epoch == ushort.MaxValue ? 1 : _epoch + 1;
            _epochAccesses = 0;
            _epochStartMs = nowMs;
        }

        _nowSeconds = (int)(nowMs / 1000 % ushort.MaxValue) + 1; // never 0: 0 marks a segment that was never prefetched

        foreach (var gate in _gatesSnapshot)
            gate.Sample(nowMs);
    }

    public void Dispose()
    {
        _timer?.Dispose();
        lock (_locker)
        {
            foreach (var gate in _gates.Values)
                gate.Dispose();
            _gates.Clear();
        }
    }
}
