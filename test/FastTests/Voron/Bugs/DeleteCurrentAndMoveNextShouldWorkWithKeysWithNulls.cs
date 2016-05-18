﻿using System.Linq;
using Xunit;
using Voron;

namespace FastTests.Voron.Bugs
{
    public class DeleteCurrentAndMoveNextTests : StorageTest
    {
        public DeleteCurrentAndMoveNextTests()
        {
            StartDatabase();
        }

        [Fact]
        public void DeleteCurrentAndMoveNextShouldWork()
        {
            var treeName = CreateTrees(Env, 1, "test").First();

            using (var tx = Env.WriteTransaction())
            {
                var testTree = tx.ReadTree(treeName);
                testTree.Add("a", "test_value_1");
                testTree.Add("b", "test_value_2");
                testTree.Add("c", "test_value_3");

                tx.Commit();
            }
            
            using (var tx = Env.WriteTransaction())
            {
                var testTree = tx.ReadTree(treeName);
                using (var iter = testTree.Iterate(false))
                {
                    iter.Seek(Slice.BeforeAllKeys);
                    while (iter.DeleteCurrentAndMoveNext())
                    {						
                    }
                    tx.Commit();
                }
            }

            using (var tx = Env.WriteTransaction())
            {
                var testTree = tx.ReadTree(treeName);
                using (var iter = testTree.Iterate(false))
                    Assert.False(iter.Seek(Slice.BeforeAllKeys));				
            }
        }

        [Fact]
        public void DeleteCurrentAndMoveNextShouldWorkWhenTreeModified()
        {
            var treeName = CreateTrees(Env, 1, "test").First();

            using (var tx = Env.WriteTransaction())
            {
                var testTree = tx.ReadTree(treeName);
                testTree.Add("a", "test_value_1");
                testTree.Add("b", "test_value_2");
                testTree.Add("c", "test_value_3");

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var testTree = tx.ReadTree(treeName);
                using (var iter = testTree.Iterate(false))
                {
                    iter.Seek("b");
                    while (iter.DeleteCurrentAndMoveNext())
                    {
                    }
                    tx.Commit();
                }
            }

            using (var tx = Env.WriteTransaction())
            {
                var testTree = tx.ReadTree(treeName);
                Assert.Equal(1, testTree.State.NumberOfEntries);
            }
        }
    
    }
}
