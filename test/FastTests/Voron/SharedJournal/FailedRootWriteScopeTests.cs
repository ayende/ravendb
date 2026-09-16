using System;
using System.Threading.Tasks;
using Tests.Infrastructure;
using Voron;
using Voron.Exceptions;
using Voron.Impl.Journal;
using Xunit;
using ITestOutputHelper = Xunit.ITestOutputHelper;

namespace FastTests.Voron.SharedJournal;

public class FailedRootWriteScopeTests : NoDisposalNeeded
{
    public FailedRootWriteScopeTests(ITestOutputHelper output) : base(output)
    {
    }

    private static StorageEnvironment CreateEnvironment(string name, out Func<bool> poisoned)
    {
        var wasPoisoned = false;
        var notification = new CatastrophicFailureNotification((_, _, _, _) => wasPoisoned = true);
        poisoned = () => wasPoisoned;

        var options = StorageEnvironmentOptions.CreateMemoryOnly(name, tempPath: null,
            ioChangesNotifications: null, catastrophicFailureNotification: notification,
            loggingResource: null, loggingComponent: null);

        return new StorageEnvironment(options);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void AFailedRootWriteDoesNotPoisonBranchesItNeverCarried()
    {
        // A root write owns only the records it merged. Once the root's merged commit became
        // asynchronous, the state can still hold records from LATER batches when a failure
        // surfaces - and failing those catastrophically unloads databases that did nothing wrong.
        using var carried = CreateEnvironment("carried", out var carriedPoisoned);
        using var queued = CreateEnvironment("queued", out var queuedPoisoned);

        using var carriedTx = carried.WriteTransaction();
        using var queuedTx = queued.WriteTransaction();

        var carriedRecord = new WriteAheadJournal.JournalStateRecord(
            carriedTx.LowLevelTransaction, new TaskCompletionSource(), default);
        var queuedRecord = new WriteAheadJournal.JournalStateRecord(
            queuedTx.LowLevelTransaction, new TaskCompletionSource(), default);

        var state = new SharedJournalState();
        state.PrepareForCommit(carriedRecord); // merged into the write that is about to fail
        state.Enqueue(queuedRecord);           // arrived after it, never part of it

        state.SetException(new InvalidOperationException("simulated root write failure"));

        Assert.True(carriedPoisoned(), "the environment whose entry was in the failed write must be failed catastrophically");
        Assert.False(queuedPoisoned(), "an environment that was merely queued was not part of the write and must survive it");

        // the bystander's commit still has to fail, otherwise it would wait forever
        Assert.True(queuedRecord.Tcs.Task.IsFaulted);
        Assert.IsType<InvalidOperationException>(queuedRecord.Tcs.Task.Exception?.InnerException);

        // and it must still be usable afterwards
        queued.Options.AssertNoCatastrophicFailure();
    }
}
