﻿using Voron;
using Voron.Data.BTrees;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Voron.NextGenPagers;

public class Bugs : StorageTest
{
    public Bugs(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void SplitRootPageWhileReading()
    {
        using (var txw = Env.WriteTransaction())
        {
            int i=0;
            Tree rootObjects = txw.LowLevelTransaction.RootObjects;
            while (rootObjects.ReadHeader().RootPageNumber == 0)
            {
                rootObjects.Add(i.ToString(), i.ToString());
                i++;
            }

            using (var txr = Env.ReadTransaction())
            {
                txr.LowLevelTransaction.RootObjects.Read("1");
            }
        }
    }

    [Fact]
    public void CanHandleRollbackOfPageInScratches()
    {
        Options.ManualFlushing = true;

        long pageNum;
        using (var txw = Env.WriteTransaction())
        {
            // here we force the database to grow
            pageNum = txw.LowLevelTransaction.DataPagerState.NumberOfAllocatedPages + 10;
            while (true)
            {
                var p = txw.LowLevelTransaction.AllocatePage(1);
                if (p.PageNumber == pageNum)
                    break;
            }
            txw.Commit();
        }

        using (var txwOne = Env.WriteTransaction())
        {
            txwOne.LowLevelTransaction.ModifyPage(pageNum-1);

            using (var txwTwo = txwOne.BeginAsyncCommitAndStartNewTransaction(new TransactionPersistentContext()))
            {
                txwTwo.LowLevelTransaction.ModifyPage(pageNum);
                
                txwOne.EndAsyncCommit();
                
                // implicit rollback
            }
        }

        using (var txw = Env.WriteTransaction())
        {
            txw.LowLevelTransaction.GetPage(pageNum-1);
            txw.LowLevelTransaction.GetPage(pageNum);
        }
    }
}
