using System;
using System.Threading;
using Sparrow;
using Sparrow.LowMemory;

namespace Voron.Impl.Paging;

/// <summary>
/// Reads ahead each touched segment once per warm-up. A warm-up ends below <see cref="StopPercent"/>% free memory
/// (MemFree on Linux) and a new one starts above <see cref="StartPercent"/>%.
/// </summary>
public sealed class PrefetchPolicy : IDisposable
{
    public const int StopPercent = 5;
    public const int StartPercent = 10;
    private const int TickMs = 1000;

    public static readonly PrefetchPolicy Global = new(startTimer: true);

    private volatile byte _generation;
    private byte _lastGeneration;
    private readonly Timer _timer;

    public PrefetchPolicy(bool startTimer)
    {
        if (startTimer == false)
            return;

        _timer = new Timer(_ =>
        {
            // all zero until the server's low-memory monitor runs: no warm-up without it
            var memory = LowMemoryNotification.Instance.LastMemoryInfo;
            Tick(memory.AvailableMemory.GetValue(SizeUnit.Bytes), memory.TotalPhysicalMemory.GetValue(SizeUnit.Bytes));
        }, null, 0, TickMs);
    }

    /// <summary>1 to 255 during a warm-up, 0 when memory is full.</summary>
    public byte Generation => _generation;

    internal void Tick(long freeBytes, long totalBytes)
    {
        if (_generation != 0)
        {
            if (freeBytes * 100 < totalBytes * StopPercent)
                _generation = 0;
            return;
        }

        if (freeBytes * 100 > totalBytes * StartPercent)
        {
            _lastGeneration = (byte)(_lastGeneration % byte.MaxValue + 1); // skips 0: a segment never read ahead holds 0
            _generation = _lastGeneration;
        }
    }

    public void Dispose() => _timer?.Dispose();
}
