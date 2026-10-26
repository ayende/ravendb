using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using Sparrow.Server.LowMemory;
using Sparrow.Platform;

namespace Voron.Impl.Paging;

/// <summary>
/// Decides, for every data file in the process, when a segment (4 MB by default) is worth reading ahead.
///
/// A segment is prefetched when it collects <see cref="Threshold"/> hits within one epoch. An epoch lasts until the
/// process has made <see cref="HitsPerEpoch"/> x (memory for the page cache, in segments) page accesses, or
/// <see cref="MaxEpochSeconds"/>. A segment of any working set that fits in memory then gets about
/// <see cref="HitsPerEpoch"/> hits per epoch, while uniform access over data larger than memory gets
/// HitsPerEpoch x memory / data and almost never reaches the threshold. The accesses of all data files count
/// (all databases share the page cache), so more active databases need hotter segments. While the page cache still
/// has free memory the threshold is lower, so a cold cache warms up quickly when the device has bandwidth to spare.
/// Every device has a <see cref="DevicePrefetchGate"/> that stops prefetches while the device is congested.
/// </summary>
public sealed class PrefetchPolicy : IDisposable
{
    public const int HitsPerEpoch = 15;
    public const int WarmupHitsPerEpoch = 4;
    public const int MaxEpochSeconds = 30;
    public const int RearmSeconds = 60;
    public const int SequentialSeconds = 2;
    public const long SegmentSize = 4 * 1024 * 1024;
    private const int TickMs = 250;

    public static readonly PrefetchPolicy Global = new(startTimer: true);

    // read on every page access
    private volatile int _epoch = 1;
    private volatile int _threshold = HitsPerEpoch;
    private volatile int _nowSeconds = 1;

    private readonly object _locker = new();
    private readonly List<WeakReference<SegmentPrefetchTable>> _tables = new();
    private readonly Dictionary<ulong, DevicePrefetchGate> _gates = new();
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
    public int Threshold => _threshold;
    public int NowSeconds => _nowSeconds;

    internal void Register(SegmentPrefetchTable table)
    {
        lock (_locker)
            _tables.Add(new WeakReference<SegmentPrefetchTable>(table));
    }

    internal void Unregister(SegmentPrefetchTable table)
    {
        lock (_locker)
            _tables.RemoveAll(x => x.TryGetTarget(out var t) == false || ReferenceEquals(t, table));
    }

    /// <summary>The gate of a device, shared by every data file on it.</summary>
    internal DevicePrefetchGate GetGate(ulong deviceId)
    {
        lock (_locker)
        {
            if (_gates.TryGetValue(deviceId, out var gate) == false)
                _gates[deviceId] = gate = DevicePrefetchGate.Create(deviceId);
            return gate;
        }
    }

    private void TickFromSystem()
    {
        try
        {
            var (memoryForCache, cacheHasRoom) = ReadMemory();
            Tick(_clock.ElapsedMilliseconds, memoryForCache, cacheHasRoom);
        }
        catch
        {
            // a lost tick only delays the next epoch
        }
    }

    /// <summary>Advances the epoch, the threshold, the clock and the device gates. Called every 250 ms (tests call it directly).</summary>
    internal void Tick(long nowMs, long memoryForCacheBytes, bool cacheHasRoom)
    {
        long total = 0;
        DevicePrefetchGate[] gates;
        lock (_locker)
        {
            _tables.RemoveAll(x => x.TryGetTarget(out _) == false);
            foreach (var weak in _tables)
            {
                if (weak.TryGetTarget(out var table))
                    total += table.Accesses;
            }
            gates = new DevicePrefetchGate[_gates.Count];
            _gates.Values.CopyTo(gates, 0);
        }

        var delta = Math.Max(0, total - _lastTotalAccesses); // a closed file takes its count with it
        _lastTotalAccesses = total;
        _epochAccesses += delta;

        var memorySegments = Math.Max(1, memoryForCacheBytes / SegmentSize);
        if (_epochAccesses >= HitsPerEpoch * memorySegments || nowMs - _epochStartMs >= MaxEpochSeconds * 1000L)
        {
            _epoch = _epoch == ushort.MaxValue ? 1 : _epoch + 1;
            _epochAccesses = 0;
            _epochStartMs = nowMs;
        }

        _threshold = cacheHasRoom ? WarmupHitsPerEpoch : HitsPerEpoch;
        _nowSeconds = (int)(nowMs / 1000 % ushort.MaxValue) + 1; // never 0: 0 marks a segment that was never prefetched

        foreach (var gate in gates)
            gate.Sample(nowMs);
    }

    // The page cache can grow into MemAvailable (free plus reclaimable memory); it still has room while a good part of
    // memory is free.
    private static (long MemoryForCache, bool CacheHasRoom) ReadMemory()
    {
        if (PlatformDetails.RunningOnLinux)
        {
            long total = 0, free = 0, available = 0;
            foreach (var line in File.ReadLines("/proc/meminfo"))
            {
                if (line.StartsWith("MemTotal:", StringComparison.Ordinal))
                    total = ParseKb(line);
                else if (line.StartsWith("MemFree:", StringComparison.Ordinal))
                    free = ParseKb(line);
                else if (line.StartsWith("MemAvailable:", StringComparison.Ordinal))
                    available = ParseKb(line);
            }
            if (total > 0)
                return (available > 0 ? available : free, free > total / 4);
        }

        var info = MemoryInformation.GetMemoryInfo();
        return (info.AvailableMemory.GetValue(Sparrow.SizeUnit.Bytes), false);

        static long ParseKb(string line)
        {
            var span = line.AsSpan(line.IndexOf(':') + 1).Trim();
            var space = span.IndexOf(' ');
            return long.TryParse(space < 0 ? span : span[..space], out var kb) ? kb * 1024 : 0;
        }
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
