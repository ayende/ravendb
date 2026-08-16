using Tests.Infrastructure;
using Voron;
using Xunit;

namespace FastTests.Voron.Journal
{
    /// <summary>
    /// TotalCommittedSinceLastFlushPages is what tells the global flusher how much has piled up since
    /// the last flush. GlobalFlushingBehavior flushes immediately once it reaches
    /// MaxNumberOfPagesInJournalBeforeFlush, and otherwise defers on a timer. If committed pages are
    /// not counted, the size trigger never fires, flushing happens only on the timer, and pages stay
    /// in the scratch buffers far longer than they should.
    ///
    /// Flushing is disabled for these tests so that the only thing changing the counter is the commits
    /// the test itself performs.
    /// </summary>
    public class TotalCommittedSinceLastFlushPagesTests(ITestOutputHelper output) : StorageTest(output)
    {
        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ManualFlushing = true;
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void CommittedPagesAreCountedForRegularTransactions()
        {
            const int numberOfTransactions = 20;
            var value = new byte[512];

            long previous = Env.Journal.Applicator.TotalCommittedSinceLastFlushPages;

            for (int i = 0; i < numberOfTransactions; i++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    tx.CreateTree("tree").Add("key/" + i, value);
                    tx.Commit();
                }

                long current = Env.Journal.Applicator.TotalCommittedSinceLastFlushPages;

                Assert.True(current > previous,
                    $"After commit #{i + 1} the flusher should have been told about the pages that transaction wrote, " +
                    $"but TotalCommittedSinceLastFlushPages stayed at {current} (was {previous} before the commit).");

                previous = current;
            }
        }

        /// <summary>
        /// The transaction merger commits asynchronously: it starts writing one transaction to the journal
        /// and immediately opens the next one on top of it. That is the path virtually every write takes
        /// under load, so pages committed this way have to reach the counter too.
        /// </summary>
        [RavenFact(RavenTestCategory.Voron)]
        public void CommittedPagesAreCountedForAsyncCommittedTransactions()
        {
            const int numberOfTransactions = 20;
            var value = new byte[512];

            long previous = Env.Journal.Applicator.TotalCommittedSinceLastFlushPages;

            var tx = Env.WriteTransaction();
            try
            {
                for (int i = 0; i < numberOfTransactions; i++)
                {
                    tx.CreateTree("tree").Add("key/" + i, value);

                    // hand off to the journal and keep going on the next transaction, exactly as the merger does
                    var next = tx.BeginAsyncCommitAndStartNewTransaction(tx.LowLevelTransaction.PersistentContext);

                    using (tx)
                    {
                        tx.EndAsyncCommit();
                    }

                    tx = next;

                    long current = Env.Journal.Applicator.TotalCommittedSinceLastFlushPages;

                    Assert.True(current > previous,
                        $"After async commit #{i + 1} the flusher should have been told about the pages that transaction wrote, " +
                        $"but TotalCommittedSinceLastFlushPages stayed at {current} (was {previous} before the commit). " +
                        "When async-committed pages are not counted, the size-based flush trigger never fires and " +
                        "flushing falls back to the timer, which lets the scratch buffers grow without bound.");

                    previous = current;
                }

                tx.Commit();
            }
            finally
            {
                tx?.Dispose();
            }
        }

        /// <summary>
        /// It is not enough that the counter moves: it has to move by the number of pages the transaction
        /// actually dirtied. If it under-counts, the flush threshold is reached far later than it should be,
        /// and since the opportunistic path is gated behind TimeToSyncAfterFlushInSec (30s by default) the
        /// size trigger is the only one that realistically fires, so under-counting directly delays flushing.
        /// </summary>
        [RavenFact(RavenTestCategory.Voron)]
        public void CounterGrowsByTheNumberOfPagesTheTransactionActuallyDirtied()
        {
            var value = new byte[8192]; // more than a page, so each transaction dirties several

            for (int i = 0; i < 20; i++)
            {
                long before = Env.Journal.Applicator.TotalCommittedSinceLastFlushPages;
                long modified;

                using (var tx = Env.WriteTransaction())
                {
                    tx.CreateTree("tree").Add("key/" + i, value);
                    modified = tx.LowLevelTransaction.NumberOfModifiedPages;
                    tx.Commit();
                }

                long delta = Env.Journal.Applicator.TotalCommittedSinceLastFlushPages - before;

                Assert.True(delta >= modified,
                    $"Transaction #{i + 1} dirtied {modified} pages but only {delta} were added to " +
                    $"TotalCommittedSinceLastFlushPages. Under-counting here delays the size-based flush trigger, " +
                    "which is the only trigger that fires in practice.");
            }
        }

        /// <summary>
        /// The counter has to reach the flush threshold in a realistic number of transactions, otherwise the
        /// size-based trigger is dead in practice and only the timer ever flushes.
        /// </summary>
        [RavenFact(RavenTestCategory.Voron)]
        public void CounterReachesTheFlushThresholdUnderSustainedWrites()
        {
            long threshold = Env.Options.MaxNumberOfPagesInJournalBeforeFlush;
            var value = new byte[8192]; // more than a page, so every transaction dirties several

            const int maxTransactions = 5000;
            int committed = 0;

            while (committed < maxTransactions && Env.Journal.Applicator.TotalCommittedSinceLastFlushPages < threshold)
            {
                using (var tx = Env.WriteTransaction())
                {
                    tx.CreateTree("tree").Add("key/" + committed, value);
                    tx.Commit();
                }

                committed++;
            }

            long reached = Env.Journal.Applicator.TotalCommittedSinceLastFlushPages;

            Assert.True(reached >= threshold,
                $"After {committed} committed transactions TotalCommittedSinceLastFlushPages only reached {reached}, " +
                $"below the {threshold} pages that trigger a flush. The size-based flush trigger cannot fire, " +
                "so flushing is left entirely to the timer.");
        }
    }
}
