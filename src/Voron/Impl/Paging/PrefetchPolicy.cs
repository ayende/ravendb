using System;
using System.Threading;
using Sparrow;
using Sparrow.LowMemory;
using Sparrow.Server.LowMemory;

namespace Voron.Impl.Paging;

/// <summary>
/// Decides when the data files read ahead whole segments (4 MB by default): only while the page cache still has free
/// memory to fill. After a restart, a failover or on a new node, one large read brings in the pages that the next
/// accesses need; on a volume with an IOPS limit this fills the cache many times faster than page faults do (gp3: about
/// 15 minutes instead of 80 for 60 GB). When memory is full, a read-ahead only evicts pages that are in use: with uniform
/// access over data larger than memory, a prefetcher that kept reading segments used the whole device bandwidth for it
/// (18.5k instead of 203k ops/s on NVMe).
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

    private volatile int _generation; // read on every page access
    private int _lastGeneration;
    private readonly Timer _timer;

    public PrefetchPolicy(bool startTimer)
    {
        if (startTimer)
            _timer = new Timer(_ => TickFromSystem(), null, 0, TickMs);
    }

    /// <summary>The current warm-up (1 to 255), or 0 while memory is full and nothing is read ahead.</summary>
    public int Generation => _generation;

    private void TickFromSystem()
    {
        try
        {
            // the low-memory monitor samples memory every few seconds; before it runs (tools, tests) we read it ourselves
            var memory = LowMemoryNotification.Instance.LastMemoryInfo;
            if (memory.TotalPhysicalMemory.GetValue(SizeUnit.Bytes) == 0)
                memory = MemoryInformation.GetMemoryInfo();
            Tick(memory.AvailableMemory.GetValue(SizeUnit.Bytes), memory.TotalPhysicalMemory.GetValue(SizeUnit.Bytes));
        }
        catch
        {
            // a lost tick only delays the next decision
        }
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
            _lastGeneration = _lastGeneration == byte.MaxValue ? 1 : _lastGeneration + 1;
            _generation = _lastGeneration;
        }
    }

    public void Dispose() => _timer?.Dispose();
}
