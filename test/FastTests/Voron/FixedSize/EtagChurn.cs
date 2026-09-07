using System;
using System.Collections.Generic;
using Tests.Infrastructure;
using Voron;
using Xunit;

namespace FastTests.Voron.FixedSize
{
    // Regression for the etag-index churn pattern an update-heavy workload produces:
    // every document update deletes its old etag from the fixed size tree and appends a new,
    // strictly larger one. The delete must always find the previously added entry.
    public class EtagChurn(ITestOutputHelper output) : StorageTest(output)
    {
        [RavenFact(RavenTestCategory.Voron)]
        public void DeleteOldAppendNew_ManyTransactions()
        {
            Slice.From(Allocator, "etags", out Slice treeId);
            var rnd = new Random(1337);
            var live = new List<long>();
            long next = 1;

            using (var tx = Env.WriteTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: 8);
                for (int i = 0; i < 200_000; i++)
                {
                    fst.Add(next, new byte[8]);
                    live.Add(next);
                    next++;
                }
                tx.Commit();
            }

            for (int txn = 0; txn < 200; txn++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    var fst = tx.FixedTreeFor(treeId, valSize: 8);
                    for (int i = 0; i < 2_000; i++)
                    {
                        var victimIdx = rnd.Next(live.Count);
                        var victim = live[victimIdx];

                        var result = fst.Delete(victim);
                        Assert.True(result.NumberOfEntriesDeleted == 1,
                            $"tx {txn}, op {i}: delete of existing key {victim} removed {result.NumberOfEntriesDeleted} entries");

                        fst.Add(next, new byte[8]);
                        live[victimIdx] = next;
                        next++;
                    }
                    fst.ValidateTree_Forced();
                    tx.Commit();
                }
            }

            using (var tx = Env.ReadTransaction())
            {
                var fst = tx.FixedTreeFor(treeId, valSize: 8);
                Assert.Equal(200_000, fst.NumberOfEntries);
                live.Sort();
                using (var it = fst.Iterate())
                {
                    Assert.True(it.Seek(long.MinValue));
                    foreach (var expected in live)
                    {
                        Assert.Equal(expected, it.CurrentKey);
                        it.MoveNext();
                    }
                }
            }
        }
    }
}
