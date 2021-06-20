using System;
using System.Collections.Generic;
using System.Linq;
using Voron.Data.Sets;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Voron.Sets
{
    public class CompactTreeTests : StorageTest
    {
        private readonly List<long> _data;
        private readonly List<long> _random;

        public CompactTreeTests(ITestOutputHelper output) : base(output)
        {
            const int Size = 400_000;
            var diff = new[] { 17, 250, 4828, 28, 12, 3 };
            var random = new Random(231);
            _data = new List<long>();
            long s = 0;
            for (int i = 0; i < Size; i++)
            {
                s += diff[random.Next(diff.Length)];
                _data.Add(s);
            }

            _random = _data.OrderBy(x => random.Next()).ToList();
        }

        private List<long> AllValues(Set set)
        {
            using var it = set.Iterate();
            var l = new List<long>();
            if (it.Seek(0) == false)
                return l;
            do
            {
                l.Add(it.Current);
            } while (it.MoveNext());

            return l;
        }

        [Fact]
        public void CanCreateCompactTree()
        {
            using (var wtx = Env.WriteTransaction())
            {
                var tree = Set.Create(wtx.LowLevelTransaction, "test");
                tree.Add(5);
                wtx.Commit();
            }
            using (var rtx = Env.ReadTransaction())
            {
                var tree = Set.Create(rtx.LowLevelTransaction, "test");
                var values = AllValues(tree);
                Assert.Equal(new[] { 5L }, values);
            }
        }


        [Fact]
        public void CanDeleteItem()
        {
            using (var wtx = Env.WriteTransaction())
            {
                var tree = Set.Create(wtx.LowLevelTransaction, "test");
                tree.Add(5);
                wtx.Commit();
            }

            using (var wtx = Env.WriteTransaction())
            {
                var tree = Set.Create(wtx.LowLevelTransaction, "test");
                tree.Remove(5);
                wtx.Commit();
            }
            using (var rtx = Env.ReadTransaction())
            {
                var tree = Set.Create(rtx.LowLevelTransaction, "test");
                Assert.Empty(AllValues(tree));
            }
        }


        [Fact]
        public void CanStoreLargeNumberOfItemsInRandomlyOrder()
        {
            using (var wtx = Env.WriteTransaction())
            {
                var tree = Set.Create(wtx.LowLevelTransaction, "test");

                foreach (long i in _random)
                {
                    if (i == 256194701)
                    {
                        tree.Render();
                    }
                    tree.Add(i);

                }
                wtx.Commit();
            }
            using (var rtx = Env.ReadTransaction())
            {
                var tree = Set.Create(rtx.LowLevelTransaction, "test");
                Assert.Equal(_data, AllValues(tree));
            }
        }


        [Fact]
        public void CanDeleteLargeNumberOfItems()
        {

            using (var wtx = Env.WriteTransaction())
            {
                var tree = Set.Create(wtx.LowLevelTransaction, "test");
                foreach (long i in _random)
                {
                    tree.Add(i);
                }

                wtx.Commit();
            }

            using (var wtx = Env.WriteTransaction())
            {
                var tree = Set.Create(wtx.LowLevelTransaction, "test");
                foreach (long i in _random)
                {
                    tree.Remove(i);
                }
                wtx.Commit();
            }
            using (var rtx = Env.ReadTransaction())
            {
                var tree = Set.Create(rtx.LowLevelTransaction, "test");
                Assert.Equal(0, tree.State.NumberOfEntries);
                Assert.Equal(0, tree.State.BranchPages);
                Assert.Equal(1, tree.State.LeafPages);
                Assert.Equal(1, tree.State.Depth);
            }
        }


        [Fact]
        public void CanDeleteLargeNumberOfItemsFromStart()
        {
            using (var wtx = Env.WriteTransaction())
            {
                var tree = Set.Create(wtx.LowLevelTransaction, "test");
                foreach (long i in _data)
                {
                    tree.Add(i);
                }
                wtx.Commit();
            }

            using (var wtx = Env.WriteTransaction())
            {
                var tree = Set.Create(wtx.LowLevelTransaction, "test");
                foreach (long i in _data)
                {
                    tree.Remove(i);
                }
                wtx.Commit();
            }
            using (var rtx = Env.ReadTransaction())
            {
                var tree = Set.Create(rtx.LowLevelTransaction, "test");
                Assert.Equal(0, tree.State.NumberOfEntries);
                Assert.Equal(0, tree.State.BranchPages);
                Assert.Equal(1, tree.State.LeafPages);
                Assert.Equal(1, tree.State.Depth);
            }
        }


        [Fact]
        public void CanDeleteLargeNumberOfItemsFromEnd()
        {
            using (var wtx = Env.WriteTransaction())
            {
                var tree = Set.Create(wtx.LowLevelTransaction, "test");
                foreach (long i in _data)
                {
                    tree.Add(i);
                }
                wtx.Commit();
            }

            using (var wtx = Env.WriteTransaction())
            {
                var tree = Set.Create(wtx.LowLevelTransaction, "test");
                for (int i = _data.Count - 1; i >= 0; i--)
                {
                    tree.Remove(_data[i]);
                }
                wtx.Commit();
            }
            using (var rtx = Env.ReadTransaction())
            {
                var tree = Set.Create(rtx.LowLevelTransaction, "test");
                Assert.Equal(0, tree.State.NumberOfEntries);
                Assert.Equal(0, tree.State.BranchPages);
                Assert.Equal(1, tree.State.LeafPages);
                Assert.Equal(1, tree.State.Depth);
            }
        }


        [Fact]
        public void CanStoreLargeNumberOfItemsInSequentialOrder()
        {
            using (var wtx = Env.WriteTransaction())
            {
                var tree = Set.Create(wtx.LowLevelTransaction, "test");
                foreach (long i in _data)
                {
                    tree.Add(i);
                }
                wtx.Commit();
            }
            using (var rtx = Env.ReadTransaction())
            {
                var tree = Set.Create(rtx.LowLevelTransaction, "test");
                Assert.Equal(_data, AllValues(tree));
            }
        }
    }
}
