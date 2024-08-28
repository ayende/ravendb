﻿using System;
using System.Linq;
using Raven.Client.Documents.Changes;
using Tests.Infrastructure;
using Voron;
using Voron.Impl;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Voron.Bugs
{
    public class RavenDB_16464 : StorageTest
    {
        public RavenDB_16464(ITestOutputHelper output) : base(output)
        {
        }

        private const int _64KB = 64 * 1024;

        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.MaxScratchBufferSize = _64KB * 4;
            options.ManualFlushing = true;
            options.ManualSyncing = true;
            options.MaxLogFileSize = _64KB;
        }

        [Fact]
        public void ShouldFreeAllocationsInScratchBuffersSoTheyCanBeCleaned()
        {
            RequireFileBasedPager();

            var r = new Random(3);

            for (int j = 0; j < 2; j++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    var tree = tx.CreateTree("tree");

                    for (int i = 0; i < 8; i++)
                    {
                        var overflowSize = r.Next(5, 10);

                        var bytes = new byte[overflowSize * 8192];

                        r.NextBytes(bytes);

                        tree.Add("items/" + i, bytes);
                    }

                    tx.Commit();
                }
            }

            Assert.Equal(3, Env.Journal.Files.Count); 
            Assert.Null(Env.Journal.CurrentFile); // this is very important condition to run into the issue - see details in RavenDB-16464

            Env.FlushLogToDataFile(); // this will flush all journals, the issue was that it also marked all of them as unused so they were removed from _files list, but didn't free the allocations in scratch buffers to ensure we don't free pages that can be still read
            var old = Env.Journal.Files;
            using (var wtx = Env.WriteTransaction())
            {
                wtx.LowLevelTransaction.ModifyPage(0);
                wtx.Commit();
            }

            Env.FlushLogToDataFile();
            Assert.Equal(1, Env.Journal.Files.Count);

            Env.Cleanup(tryCleanupRecycledJournals: true);

            var scratchBufferPoolInfo = Env.ScratchBufferPool.InfoForDebug(Env.PossibleOldestReadTransaction(null));
            Assert.False(old.Any(j => Env.Journal.Files.Contains(j)));

            Assert.Equal(1, scratchBufferPoolInfo.ScratchFilesUsage.Count);
        }

        [RavenMultiplatformFact(RavenTestCategory.Voron, RavenArchitecture.X64| RavenArchitecture.Arm64)]
        public unsafe void CanKeepReadScratchPagesWhenFlushingWhenThereIsTransactionThatMightReadFromIt()
        {
            RequireFileBasedPager();

            var r = new Random(3);

            byte[] bytes = [];
            for (int j = 0; j < 2; j++)
            {
                using (var tx = Env.WriteTransaction())
                {
                    var tree = tx.CreateTree("tree");

                    for (int i = 0; i < 8; i++)
                    {
                        var overflowSize = r.Next(5, 10);

                        bytes = new byte[overflowSize * 8192];

                        r.NextBytes(bytes);

                        tree.Add("items/" + i, bytes);
                    }

                    tx.Commit();
                }
            }

            Assert.Equal(3, Env.Journal.Files.Count);
            Assert.Null(Env.Journal.CurrentFile);

            byte* basePtr;
            using (var rtx = Env.ReadTransaction())
            {
                basePtr = rtx.ReadTree("tree").Read("items/7").Reader.Base;
                Assert.Equal(3, rtx.LowLevelTransaction.Id);
            }
            
            Transaction readTx = null;

            Env.Journal.Applicator.ForTestingPurposesOnly().OnUpdateJournalStateUnderWriteTransactionLock += () =>
            {
                readTx = Env.ReadTransaction();
            };
            var old = Env.Journal.Files;
            using (var wtx = Env.WriteTransaction())
            {
                wtx.LowLevelTransaction.ModifyPage(0);
                wtx.Commit();
            }
            Env.FlushLogToDataFile();

            {
                using var _ = readTx;
                Assert.False(old.Any(j => Env.Journal.Files.Contains(j)));
                Assert.Equal(1, Env.Journal.Files.Count);

                ValueReader reader = readTx.ReadTree("tree").Read("items/7").Reader;
                Assert.Equal(bytes, reader.AsSpan());
                Assert.Equal((nint)basePtr, (nint)reader.Base);
                Assert.Equal(4, readTx.LowLevelTransaction.Id);
            }

            
            using (var wtx = Env.ReadTransaction())
            {
                var reader = wtx.ReadTree("tree").Read("items/7").Reader;
                Assert.Equal(bytes, reader.AsSpan());
                Assert.NotEqual((nint)basePtr, (nint)reader.Base);
            }
        }
    }
}
