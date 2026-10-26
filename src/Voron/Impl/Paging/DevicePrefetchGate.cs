using System;
using System.Buffers.Text;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using Microsoft.Win32.SafeHandles;
using Sparrow.Platform;

namespace Voron.Impl.Paging;

/// <summary>
/// Admits prefetch reads to one device, for every data file on it (and so for every database on it). It reads the
/// device's own counters (/sys/dev/block/major:minor/stat), so it also sees the I/O of other processes. Prefetches
/// stop while the device is congested: read latency above twice its baseline, or a deep queue. Otherwise they may use
/// a byte budget that grows while the device keeps up and halves when it does not (additive increase, multiplicative
/// decrease). Without device counters (not Linux, or a device without stats) every prefetch is admitted.
/// </summary>
public sealed class DevicePrefetchGate : IDisposable
{
    public const long InitialBytesPerSecond = 64L * 1024 * 1024;
    public const long MinBytesPerSecond = 4L * 1024 * 1024;
    public const long MaxBytesPerSecond = 4L * 1024 * 1024 * 1024;
    public const long BytesPerSecondStep = 16L * 1024 * 1024;
    public const double CongestedLatencyFactor = 2.0;
    public const double MinCongestedLatencyMs = 0.5;
    public const double CongestedQueueDepth = 32;
    public const long MinReadsForLatency = 20;

    private readonly SafeFileHandle _stat;
    private readonly bool _hasSignal;
    private long _tokens;
    private double _bytesPerSecond = InitialBytesPerSecond;
    private double _baselineLatencyMs = double.NaN;
    private long _lastMs = -1, _lastReads, _lastReadMs, _lastWeightedMs;

    internal DevicePrefetchGate(bool hasSignal, SafeFileHandle stat = null)
    {
        _hasSignal = hasSignal;
        _stat = stat;
    }

    public bool Congested { get; private set; }
    public double BytesPerSecond => _bytesPerSecond;
    public double BaselineLatencyMs => _baselineLatencyMs;

    internal static DevicePrefetchGate Create(ulong deviceId)
    {
        if (PlatformDetails.RunningOnLinux == false)
            return new DevicePrefetchGate(hasSignal: false);

        // st_dev encodes major:minor, see sysmacros.h
        var major = ((deviceId & 0x00000000000fff00UL) >> 8) | ((deviceId & 0xfffff00000000000UL) >> 32);
        var minor = (deviceId & 0x00000000000000ffUL) | ((deviceId & 0x00000ffffff00000UL) >> 12);
        var path = $"/sys/dev/block/{major}:{minor}/stat";
        if (File.Exists(path) == false)
            return new DevicePrefetchGate(hasSignal: false);
        return new DevicePrefetchGate(hasSignal: true, File.OpenHandle(path));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool TryAdmit(long bytes)
    {
        if (_hasSignal == false)
            return true;
        if (Volatile.Read(ref _tokens) < bytes)
            return false;
        Interlocked.Add(ref _tokens, -bytes);
        return true;
    }

    internal void Sample(long nowMs)
    {
        if (_stat == null)
            return;
        Span<long> fields = stackalloc long[11];
        if (ReadStat(_stat, fields) == false)
            return;
        // fields: 1 reads completed, 4 ms reading, 11 weighted ms in queue
        Sample(nowMs, fields[0], fields[3], fields[10]);
    }

    internal void Sample(long nowMs, long reads, long readMs, long weightedMs)
    {
        if (_lastMs < 0)
        {
            (_lastMs, _lastReads, _lastReadMs, _lastWeightedMs) = (nowMs, reads, readMs, weightedMs);
            Volatile.Write(ref _tokens, (long)_bytesPerSecond);
            return;
        }

        var elapsedMs = nowMs - _lastMs;
        if (elapsedMs <= 0)
            return;
        var deltaReads = reads - _lastReads;
        var deltaReadMs = readMs - _lastReadMs;
        var queueDepth = (weightedMs - _lastWeightedMs) / (double)elapsedMs;
        (_lastMs, _lastReads, _lastReadMs, _lastWeightedMs) = (nowMs, reads, readMs, weightedMs);

        var congested = queueDepth > CongestedQueueDepth;
        if (deltaReads >= MinReadsForLatency)
        {
            var latencyMs = deltaReadMs / (double)deltaReads;
            // the lowest latency seen recently, rising slowly (about 3 minutes to double) so a lasting change is learned
            _baselineLatencyMs = double.IsNaN(_baselineLatencyMs) ? latencyMs : Math.Min(latencyMs, _baselineLatencyMs * 1.001 + 0.0001);
            congested |= latencyMs > CongestedLatencyFactor * _baselineLatencyMs && latencyMs > MinCongestedLatencyMs;
        }

        Congested = congested;
        _bytesPerSecond = congested
            ? Math.Max(MinBytesPerSecond, _bytesPerSecond / 2)
            : Math.Min(MaxBytesPerSecond, _bytesPerSecond + BytesPerSecondStep);

        var tokens = congested ? 0 : Volatile.Read(ref _tokens) + (long)(_bytesPerSecond * elapsedMs / 1000);
        Volatile.Write(ref _tokens, Math.Min(tokens, (long)_bytesPerSecond)); // at most one second of budget
    }

    [SkipLocalsInit]
    private static bool ReadStat(SafeFileHandle handle, Span<long> fields)
    {
        Span<byte> buffer = stackalloc byte[512];
        ReadOnlySpan<byte> span = buffer[..RandomAccess.Read(handle, buffer, 0)];
        for (var i = 0; i < fields.Length; i++)
        {
            span = span.TrimStart((byte)' ');
            if (Utf8Parser.TryParse(span, out long value, out var consumed) == false)
                return false;
            fields[i] = value;
            span = span[consumed..];
        }
        return true;
    }

    public void Dispose()
    {
        _stat?.Dispose();
    }
}
