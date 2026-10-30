using System;
using System.Threading;
using Sparrow;
using Sparrow.LowMemory;

namespace Voron.Impl.Paging;

/// <summary>
/// Decides when the data files read ahead whole segments (4 MB by default): only while the page cache still has free
/// memory to fill, as after a restart, a failover or on a new node. One large read fills the cache much faster than
/// page faults do. When memory is full, a read-ahead only evicts pages that are in use.
/// A segment is read ahead at most once per warm-up. A warm-up ends when free memory drops below
/// <see cref="StopPercent"/>% and a new one starts when it is above <see cref="StartPercent"/>% again (for example,
/// after large files were deleted). On Linux the low-memory monitor's available memory is MemFree, the memory that the
/// page cache did not take yet; on Windows and macOS it includes the cache, so there a segment is read ahead once per
/// process lifetime.
/// </summary>
public sealed class PrefetchPolicy : IDisposable
{
    public const int StopPercent = 5;
    public const int StartPercent = 10;
    private const int TickMs = 1000;

    public static readonly PrefetchPolicy Global = new(startTimer: true);

    private volatile byte _generation; // set by Tick on the timer, read on every page access
    private byte _lastGeneration;
    private readonly Timer _timer;

    public PrefetchPolicy(bool startTimer)
    {
        if (startTimer)
            _timer = new Timer(_ => TickFromSystem(), null, 0, TickMs);
    }

    /// <summary>The current warm-up (1 to 255), or 0 while memory is full and nothing is read ahead.</summary>
    public byte Generation => _generation;

    private void TickFromSystem()
    {
        var memory = LowMemoryNotification.Instance.LastMemoryInfo; // all zero without the server's monitor: no warm-up
        Tick(memory.AvailableMemory.GetValue(SizeUnit.Bytes), memory.TotalPhysicalMemory.GetValue(SizeUnit.Bytes));
    }

    /// <summary>Starts or ends the warm-up from the free memory. Called every second (tests call it directly).</summary>
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
            _lastGeneration = (byte)(_lastGeneration % byte.MaxValue + 1); // 255 wraps to 1: 0 marks a segment never read ahead
            _generation = _lastGeneration;
        }
    }

    public void Dispose() => _timer?.Dispose();
}
