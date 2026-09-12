using System;
using System.Collections.Generic;
using System.Threading;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Voron;
using Voron.Data.BTrees;
using Xunit;

namespace FastTests.Voron.Journal;

public class JournalEntryLayoutTests(ITestOutputHelper output) : RavenTestBase(output)
{
    // The journal entry's reservation is laid out from an upper bound on the encoding, before a byte is
    // written, because the entry has to end up at the reservation's base either way. When that bound is above
    // the compression threshold but the diffs bring the real size below it, compressing would burn CPU on the
    // write lock for nothing, so the transaction is moved to the base instead. That move is the only path that
    // relocates an entry, and getting its offset or length wrong corrupts the journal.
    [RavenFact(RavenTestCategory.Voron)]
    public void ATransactionThatEstimatesAboveTheCompressionThresholdButDiffsBelowItRoundTrips()
    {
        var path = NewDataPath();
        IOExtensions.DeleteDirectory(path);

        var expected = new Dictionary<string, string>();
        var sawRelocatedEntry = 0;

        {
            using var options = StorageEnvironmentOptions.ForPathForTests(path);
            options.ManualFlushing = true;
            options.ManualSyncing = true;
            options.CompressTxAboveSizeInBytes = 1024 * 1024;

            using var env = new StorageEnvironment(options);

            // seed enough pages that touching them all estimates well above the threshold
            using (var tx = env.WriteTransaction())
            {
                var tree = tx.CreateTree("tree");
                for (int i = 0; i < 20_000; i++)
                {
                    var key = $"key/{i:D6}";
                    var value = new string((char)('a' + i % 26), 200);
                    tree.Add(key, value);
                    expected[key] = value;
                }

                tx.Commit();
            }

            env.Journal.ForTestingPurposesOnly().OnEntryPrepared = layout =>
            {
                if (layout == global::Voron.Impl.Journal.WriteAheadJournal.JournalEntryLayout.Relocated)
                    Interlocked.Exchange(ref sawRelocatedEntry, 1);
            };

            // one character per key, spread across the whole tree, and the same length as before so the diff
            // of each touched page stays tiny: many modified pages, very little actual data
            using (var tx = env.WriteTransaction())
            {
                var tree = tx.CreateTree("tree");
                for (int i = 0; i < 20_000; i += 20)
                {
                    var key = $"key/{i:D6}";
                    var value = "Z" + expected[key][1..];
                    tree.Add(key, value);
                    expected[key] = value;
                }

                tx.Commit();
            }

            Assert.Equal(1, sawRelocatedEntry);
        }

        // the relocated entry has to survive recovery, which is what proves the move was correct
        {
            using var options = StorageEnvironmentOptions.ForPathForTests(path);
            options.ManualFlushing = true;
            options.ManualSyncing = true;

            using var env = new StorageEnvironment(options);
            using var rtx = env.ReadTransaction();

            var tree = rtx.ReadTree("tree");
            Assert.NotNull(tree);
            foreach (var (key, value) in expected)
            {
                var read = tree.Read(key);
                Assert.True(read != null, $"'{key}' is gone after recovery");
                Assert.Equal(value, read.Reader.ToStringValue());
            }
        }
    }
}
