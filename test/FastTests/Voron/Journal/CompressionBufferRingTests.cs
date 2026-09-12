using System;
using System.Threading.Tasks;
using Tests.Infrastructure;
using Voron;
using Voron.Impl.Journal;
using Xunit;

namespace FastTests.Voron.Journal;

public class CompressionBufferRingTests(ITestOutputHelper output) : RavenTestBase(output)
{
    // The ring asserts that the journal write lock is held, and a journal is not optional - so these tests
    // drive a real environment's ring rather than standing one up on its own.
    private StorageEnvironment CreateEnvironment(Action<StorageEnvironmentOptions> configure = null)
    {
        var options = StorageEnvironmentOptions.ForPathForTests(NewDataPath());
        options.ManualFlushing = true;
        options.ManualSyncing = true;
        configure?.Invoke(options);
        return new StorageEnvironment(options);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void ReservationsWrapToTheStartOnceTheTailIsReturned()
    {
        using var env = CreateEnvironment();
        var testing = env.Journal.ForTestingPurposesOnly();
        var ring = testing.CompressionBuffer;
        using var _ = testing.EnterWriteLock();

        var quarter = checked((int)(ring.State.NumberOfAllocatedPages / 4));
        Assert.True(quarter > 0, "the ring must start with room for at least four reservations");

        var leases = new CompressionBufferRing.Lease[4];
        for (int i = 0; i < leases.Length; i++)
        {
            var reservation = ring.Reserve(quarter);
            Assert.Equal(i * (long)quarter, reservation.BaseOffsetInPages);
            leases[i] = ring.Trim(reservation, reservation.NumberOfPages);
        }

        leases[0].Dispose();

        var wrapped = ring.Reserve(quarter);
        Assert.Equal(0, wrapped.BaseOffsetInPages);

        ring.Trim(wrapped, wrapped.NumberOfPages).Dispose();
        for (int i = 1; i < leases.Length; i++)
            leases[i].Dispose();
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void AReservationWaitsForTheTailInsteadOfGrowingTheRing()
    {
        using var env = CreateEnvironment();
        var testing = env.Journal.ForTestingPurposesOnly();
        var ring = testing.CompressionBuffer;

        long capacity;
        var quarter = 0;
        var leases = new CompressionBufferRing.Lease[4];
        using (testing.EnterWriteLock())
        {
            capacity = ring.State.NumberOfAllocatedPages;
            quarter = checked((int)(capacity / 4));

            for (int i = 0; i < leases.Length; i++)
            {
                var reservation = ring.Reserve(quarter);
                leases[i] = ring.Trim(reservation, reservation.NumberOfPages);
            }
        }

        // the ring is now full - the next reservation has nowhere to go until the tail comes back. It parks
        // while holding the write lock, which is exactly what back-pressure does to a committer
        var blocked = Task.Run(() =>
        {
            using (testing.EnterWriteLock())
            {
                var reservation = ring.Reserve(quarter);
                return ring.Trim(reservation, reservation.NumberOfPages);
            }
        });

        Assert.False(blocked.Wait(TimeSpan.FromMilliseconds(500)), "the reservation should have waited for the tail");
        Assert.Equal(capacity, ring.State.NumberOfAllocatedPages);

        leases[0].Dispose();

        Assert.True(blocked.Wait(TimeSpan.FromSeconds(30)), "returning the tail should have released the reservation");
        Assert.Equal(0, blocked.Result.BaseOffsetInPages);

        blocked.Result.Dispose();
        for (int i = 1; i < leases.Length; i++)
            leases[i].Dispose();
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void GrowsForAReservationLargerThanTheWholeRing()
    {
        using var env = CreateEnvironment();
        var testing = env.Journal.ForTestingPurposesOnly();
        var ring = testing.CompressionBuffer;
        using var _ = testing.EnterWriteLock();

        var capacity = ring.State.NumberOfAllocatedPages;

        var reservation = ring.Reserve(checked((int)capacity + 1));
        Assert.True(ring.State.NumberOfAllocatedPages > capacity, "a request that does not fit must grow the ring");

        ring.Trim(reservation, reservation.NumberOfPages).Dispose();
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void ALeaseKeepsTheMappingItWasPreparedUnderAlive()
    {
        using var env = CreateEnvironment();
        var testing = env.Journal.ForTestingPurposesOnly();
        var ring = testing.CompressionBuffer;
        using var _ = testing.EnterWriteLock();

        var small = ring.Reserve(8);
        var lease = ring.Trim(small, small.NumberOfPages);
        var stateBeforeGrowth = lease.State;

        // growth installs a new state at a new address, the old mapping has to stay until the lease returns
        var large = ring.Reserve(checked((int)ring.State.NumberOfAllocatedPages));
        Assert.NotSame(stateBeforeGrowth, ring.State);
        Assert.False(stateBeforeGrowth.Disposed, "the entry in the lease still points into this mapping");

        ring.Cancel(large);
        lease.Dispose();
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void RecreateWaitsForTheRingToDrainInsteadOfUnmappingUnderALease()
    {
        using var env = CreateEnvironment();
        var testing = env.Journal.ForTestingPurposesOnly();
        var ring = testing.CompressionBuffer;
        using var _ = testing.EnterWriteLock();

        var reservation = ring.Reserve(8);
        var lease = ring.Trim(reservation, 8);
        var pagerBeforeRecreate = ring.Pager;
        var stateBeforeRecreate = lease.State;

        ring.Recreate();

        // disposing the pager would unmap the range the journal write is still reading from
        Assert.Same(pagerBeforeRecreate, ring.Pager);
        Assert.False(stateBeforeRecreate.Disposed, "the pager was replaced while a lease still pointed into it");

        lease.Dispose();

        // returning a lease does no bookkeeping - it publishes a flag and a token, and the thread that next
        // needs the ring drains it. So the deferred recreate lands on the next reservation, not on the return
        ring.Trim(ring.Reserve(8), 8).Dispose();

        Assert.NotSame(pagerBeforeRecreate, ring.Pager);
        Assert.True(stateBeforeRecreate.Disposed, "the deferred recreate did not happen once the ring drained");
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void SealingReturnsTheStagingAreaBehindTheEntry()
    {
        using var env = CreateEnvironment(o => o.InitialFileSize = global::Voron.Global.Constants.Size.Megabyte);
        var testing = env.Journal.ForTestingPurposesOnly();
        var ring = testing.CompressionBuffer;
        using var _ = testing.EnterWriteLock();

        var capacity = ring.State.NumberOfAllocatedPages;
        var half = checked((int)(capacity / 2));
        var entryPages = half / 8;

        // the reservation covered the entry and the staging it was built from; only the entry is leased
        var first = ring.Reserve(half);
        var entry = ring.Trim(first, entryPages);
        Assert.Equal(0, entry.BaseOffsetInPages);
        Assert.Equal(entryPages, entry.NumberOfPages);

        // everything behind the entry came back at seal time, so the rest of the ring is free again - without
        // the trim this would have to grow the file instead
        var rest = ring.Reserve(checked((int)capacity - entryPages));
        Assert.Equal(entryPages, rest.BaseOffsetInPages);
        Assert.Equal(capacity, ring.State.NumberOfAllocatedPages);

        ring.Trim(rest, rest.NumberOfPages).Dispose();
        entry.Dispose();
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void AnOversizedRingIsOnlyShrunkOnceNothingIsLeased()
    {
        using var env = CreateEnvironment(o => o.MaxScratchBufferSize = 4 * global::Voron.Global.Constants.Size.Megabyte);
        var testing = env.Journal.ForTestingPurposesOnly();
        var ring = testing.CompressionBuffer;
        using var _ = testing.EnterWriteLock();

        var reservation = ring.Reserve(16 * global::Voron.Global.Constants.Size.Megabyte / global::Voron.Global.Constants.Storage.PageSize);
        var lease = ring.Trim(reservation, reservation.NumberOfPages);

        var grown = ring.State.NumberOfAllocatedPages;
        Assert.True(grown * global::Voron.Global.Constants.Storage.PageSize > env.Options.MaxScratchBufferSize);

        ring.ReduceSizeIfNeeded(forceReduce: true);
        Assert.Equal(grown, ring.State.NumberOfAllocatedPages);

        lease.Dispose();

        ring.ReduceSizeIfNeeded(forceReduce: true);
        Assert.True(ring.State.NumberOfAllocatedPages < grown, "the spike should not pin the space once the ring is empty");
    }
}
