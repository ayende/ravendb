using System;
using System.Collections.Generic;
using System.Linq;
using Tests.Infrastructure;
using Voron.Global;
using Voron.Impl.Paging;
using Xunit;

namespace FastTests.Voron;

public class SegmentPrefetchPolicyTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    private const long SegmentSize = PrefetchPolicy.SegmentSize;
    private const int PagesPerSegment = (int)(SegmentSize / Constants.Storage.PageSize);

    // a private policy driven by hand: no timer, no real memory or device
    private sealed class Harness : IDisposable
    {
        private readonly long _memoryBytes;
        private readonly bool _cacheHasRoom;
        private long _nowMs;

        public readonly PrefetchPolicy Policy = new(startTimer: false);
        public readonly SegmentPrefetchTable Table;
        public readonly List<long> Prefetches = new(); // segment numbers, in the order they were read ahead

        public Harness(long fileSegments, long memorySegments, bool cacheHasRoom = false)
        {
            _memoryBytes = memorySegments * SegmentSize;
            _cacheHasRoom = cacheHasRoom;
            Table = new SegmentPrefetchTable(Policy, SegmentSize, fileSegments * SegmentSize);
            Tick();
        }

        public void Access(IEnumerable<long> pages, int tickEvery = 100)
        {
            var n = 0;
            foreach (var page in pages)
            {
                if (Table.ShouldPrefetchSegment(page, out var offset, out var bytes))
                {
                    for (var o = offset; o < offset + bytes; o += SegmentSize)
                        Prefetches.Add(o / SegmentSize);
                }
                if (++n % tickEvery == 0)
                    Tick();
            }
        }

        public void Tick(long elapsedMs = 250)
        {
            _nowMs += elapsedMs;
            Policy.Tick(_nowMs, _memoryBytes, _cacheHasRoom);
        }

        public void Dispose() => Policy.Dispose();
    }

    private static IEnumerable<long> RandomPages(Random random, long firstSegment, long segments, int count)
    {
        for (var i = 0; i < count; i++)
            yield return firstSegment * PagesPerSegment + random.NextInt64(segments * PagesPerSegment);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void UniformAccessOverDataLargerThanMemoryIsNotPrefetched()
    {
        // 10x more data than memory: a segment gets ~1.5 hits per epoch, far from the threshold of 15
        using var h = new Harness(fileSegments: 640, memorySegments: 64);
        h.Access(RandomPages(new Random(1), 0, 640, 50_000));
        Assert.Empty(h.Prefetches);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void AWorkingSetThatFitsInMemoryIsPrefetched()
    {
        using var h = new Harness(fileSegments: 640, memorySegments: 64);
        h.Access(RandomPages(new Random(2), 100, 32, 5_000));
        AssertPrefetched(h, first: 100, count: 32);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void AMovingWorkingSetIsFollowed()
    {
        using var h = new Harness(fileSegments: 640, memorySegments: 64);
        var random = new Random(3);
        h.Access(RandomPages(random, 0, 32, 5_000));
        h.Prefetches.Clear();
        h.Access(RandomPages(random, 300, 32, 5_000));
        AssertPrefetched(h, first: 300, count: 32);
    }

    // the whole set; neighbours in a hot set pass the threshold moments apart, so the sequential rule may also read the
    // one segment above it
    private static void AssertPrefetched(Harness h, long first, int count)
    {
        var prefetched = h.Prefetches.ToHashSet();
        Assert.Subset(prefetched, Enumerable.Range((int)first, count).Select(x => (long)x).ToHashSet());
        Assert.Subset(Enumerable.Range((int)first, count + 1).Select(x => (long)x).ToHashSet(), prefetched);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void ASegmentIsNotPrefetchedAgainUntilItsRearmTime()
    {
        using var h = new Harness(fileSegments: 640, memorySegments: 64);
        var random = new Random(4);
        h.Access(RandomPages(random, 0, 4, 2_000));
        AssertPrefetched(h, first: 0, count: 4);

        h.Prefetches.Clear();
        h.Access(RandomPages(random, 0, 4, 2_000)); // well within the rearm time
        Assert.Empty(h.Prefetches);

        for (var i = 0; i < PrefetchPolicy.RearmSeconds * 4 + 4; i++)
            h.Tick();
        h.Access(RandomPages(random, 0, 4, 2_000));
        AssertPrefetched(h, first: 0, count: 4);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void ASequentialScanReadsAhead()
    {
        using var h = new Harness(fileSegments: 640, memorySegments: 64);
        h.Access(Enumerable.Range(0, 10 * PagesPerSegment).Select(x => (long)x), tickEvery: 1_000);

        // every scanned segment, and the one after the last, before the scan reaches it
        Assert.Equal(Enumerable.Range(0, 11).Select(x => (long)x), h.Prefetches.Distinct().Order());
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void AColdCacheWarmsUpWhenItHasRoom()
    {
        using var cold = new Harness(fileSegments: 640, memorySegments: 64, cacheHasRoom: true);
        cold.Access(RandomPages(new Random(5), 0, 640, 50_000));
        Assert.NotEmpty(cold.Prefetches);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void ACongestedDeviceStopsPrefetches()
    {
        var gate = new DevicePrefetchGate(hasSignal: true);
        long reads = 0, readMs = 0, weighted = 0, now = 0;
        void Sample(long newReads, double latencyMs, double queueDepth)
        {
            now += 250;
            reads += newReads;
            readMs += (long)(newReads * latencyMs);
            weighted += (long)(queueDepth * 250);
            gate.Sample(now, reads, readMs, weighted);
        }

        gate.Sample(now, reads, readMs, weighted);
        Sample(1_000, 0.1, 1);
        Assert.False(gate.Congested);
        Assert.True(gate.TryAdmit(SegmentSize));

        Sample(1_000, 1.0, 1); // ten times the baseline latency
        Assert.True(gate.Congested);
        Assert.False(gate.TryAdmit(SegmentSize));

        Sample(1_000, 0.1, 1);
        Assert.False(gate.Congested);
        Assert.True(gate.TryAdmit(SegmentSize));

        Sample(1_000, 0.1, 64); // a deep queue
        Assert.True(gate.Congested);
        Assert.False(gate.TryAdmit(SegmentSize));

        // the shared gate holds back a hot set too
        using var h = new Harness(fileSegments: 640, memorySegments: 64);
        h.Table.Gate = gate;
        h.Access(RandomPages(new Random(6), 0, 32, 5_000), tickEvery: 10_000);
        Assert.Empty(h.Prefetches);
    }
}
