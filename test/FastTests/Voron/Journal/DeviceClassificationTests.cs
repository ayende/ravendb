using System;
using FastTests;
using Tests.Infrastructure;
using Voron;
using Voron.Impl.Journal;
using Xunit;

namespace FastTests.Voron.Journal;

public class DeviceClassificationTests : NoDisposalNeeded
{
    public DeviceClassificationTests(ITestOutputHelper output) : base(output)
    {
    }

    private static DeviceWriteBudget CreateBudget()
    {
        using var options = StorageEnvironmentOptions.CreateMemoryOnlyForTests();
        return DeviceWriteBudget.CreateUnshared(options);
    }

    private static void Record(DeviceWriteBudget budget, long sizeInBytes, double latencyMs, int samples = 64)
    {
        var ticks = (long)(latencyMs * TimeSpan.TicksPerMillisecond);
        for (int i = 0; i < samples; i++)
            budget.RecordJournalWrite(ticks, sizeInBytes, time: i);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void WithNoWritesTheDeviceIsUnknown()
    {
        Assert.Equal(DeviceWriteBudget.DeviceClass.Unknown, CreateBudget().MeasuredDeviceClass);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void TooFewSamplesLeaveTheDeviceUnknown()
    {
        var budget = CreateBudget();

        // one lucky write must not classify a device, in either direction
        Record(budget, 4 * 1024 * 1024, latencyMs: 0.1, samples: 4);

        Assert.Equal(DeviceWriteBudget.DeviceClass.Unknown, budget.MeasuredDeviceClass);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void ALargeWriteIsJudgedOnThroughputNotLatency()
    {
        var budget = CreateBudget();

        // the measured NVMe shape: a shared index journal writes ~4.5MB in ~2.5ms. That is 2.5ms per
        // write, which the old latency rule called slow - it is ~1.8GB/sec, which is not.
        Record(budget, sizeInBytes: 4_500_000, latencyMs: 2.5);

        Assert.Equal(DeviceWriteBudget.DeviceClass.Fast, budget.MeasuredDeviceClass);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void AThrottledDeviceIsBudgetedAtTheSameWriteSize()
    {
        var budget = CreateBudget();

        // the measured gp3 shape at a comparable size: ~1.4MB taking ~163ms, roughly 9MB/sec
        Record(budget, sizeInBytes: 1_390_000, latencyMs: 163);

        Assert.Equal(DeviceWriteBudget.DeviceClass.Budgeted, budget.MeasuredDeviceClass);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void SmallWritesAreJudgedAgainstTheirOwnBar()
    {
        // a small write is mostly fixed overhead, so even NVMe only reached ~170MB/sec there. Judged
        // against the large-write bar it would look slow; judged against its own, it does not.
        var fast = CreateBudget();
        Record(fast, sizeInBytes: 90_000, latencyMs: 0.5);
        Assert.Equal(DeviceWriteBudget.DeviceClass.Fast, fast.MeasuredDeviceClass);

        // gp3 at the same size took ~3.8ms for ~20KB, about 5MB/sec
        var slow = CreateBudget();
        Record(slow, sizeInBytes: 20_000, latencyMs: 3.8);
        Assert.Equal(DeviceWriteBudget.DeviceClass.Budgeted, slow.MeasuredDeviceClass);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void LargeWritesOutvoteSmallOnes()
    {
        var budget = CreateBudget();

        // a device that absorbs small writes into a cache but cannot sustain bandwidth must not pass
        // as fast. The large bucket is the honest measurement, so it decides even though the small
        // bucket looks good and carries far more samples.
        Record(budget, sizeInBytes: 60_000, latencyMs: 0.2, samples: 512);
        Record(budget, sizeInBytes: 4 * 1024 * 1024, latencyMs: 100, samples: 32);

        Assert.Equal(DeviceWriteBudget.DeviceClass.Budgeted, budget.MeasuredDeviceClass);
    }
}
