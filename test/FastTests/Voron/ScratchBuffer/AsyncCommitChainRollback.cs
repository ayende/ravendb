using Tests.Infrastructure;
using Voron.Impl.Scratch;
using Voron.Util;
using Xunit;

namespace FastTests.Voron.ScratchBuffer
{
    public class AsyncCommitChainRollback : StorageTest
    {
        public AsyncCommitChainRollback(ITestOutputHelper output) : base(output)
        {
        }

        // RavenDB-27407: an async commit opens the next write session on the shared scratch pages table while
        // the previous one can still fail - a branch environment learns that its shared journal write was
        // rejected only after its successor has started. Rolling the failed session back has to restore the
        // versions it replaced. Before the fix, starting the chained session cleared the single undo log, so
        // the rollback undid nothing while LowLevelTransaction.Rollback freed the session's scratch pages:
        // every later read resolved those pages to freed scratch space, and the environment's root objects
        // tree stopped resolving any tree at all.
        [RavenFact(RavenTestCategory.Voron)]
        public void A_failed_session_that_an_async_commit_already_followed_restores_the_published_version()
        {
            const long pageNumber = 17;
            const long committedPosition = 100;

            var file = Env.ScratchBufferPool._current.File;
            var table = new ScratchPagesTable(new ActiveTransactions());

            var publishedSeq = table.BeginWriteTransaction(lastPublishedSeq: 0, chainedToPreviousSession: false);
            table.Set(pageNumber, PageAt(file, publishedSeq, committedPosition));

            // the session whose commit will be rejected. Readers still see publishedSeq
            var failingSeq = table.BeginWriteTransaction(publishedSeq, chainedToPreviousSession: false);
            table.Set(pageNumber, PageAt(file, failingSeq, position: 200));

            // the async commit starts the next session before the one before it is known to have succeeded
            var chainedSeq = table.BeginWriteTransaction(publishedSeq, chainedToPreviousSession: true);
            table.Set(pageNumber, PageAt(file, chainedSeq, position: 300));

            // the rejected session unwinds first, which is the order the failure arrives in
            table.RollbackTransactionsAfter(failingSeq);

            Assert.True(table.TryGetValue(pageNumber, out var current),
                $"page {pageNumber} lost its published version when the failed session rolled back");
            Assert.Equal(committedPosition, current.PositionInScratchBuffer);

            // the chained session unwinds afterwards, and has nothing of its own left to undo
            table.RollbackTransactionsAfter(chainedSeq);

            Assert.True(table.TryGetValue(pageNumber, out current),
                $"page {pageNumber} lost its published version when the chained session rolled back");
            Assert.Equal(committedPosition, current.PositionInScratchBuffer);
        }

        private static PageFromScratchBuffer PageAt(ScratchBufferFile file, long allocatedInTransaction, long position) =>
            new(file, State: null, allocatedInTransaction, position, PageNumberInDataFile: 17, PreviousVersion: default, Size: 1, NumberOfPages: 1);
    }
}
