using System;
using System.Collections.Generic;
using Tests.Infrastructure;
using Voron.Data.BTrees;
using Xunit;

namespace FastTests.Voron.Trees
{
    public class RecentlyFoundPages(ITestOutputHelper output) : StorageTest(output)
    {
        private const int KeySpace = 20_000;

        [RavenTheory(RavenTestCategory.Voron)]
        [InlineData(1)]
        [InlineData(7)]
        [InlineData(42)]
        public void ReadsInTheWriteTransactionSeeItsOwnChanges(int seed)
        {
            var random = new Random(seed);
            var expected = new Dictionary<string, string>(StringComparer.Ordinal);

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("test");
                for (int i = 0; i < KeySpace; i += 2)
                    Put(tree, expected, Key(i), random);
                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.ReadTree("test");
                for (int op = 0; op < 50_000; op++)
                {
                    var key = Key(random.Next(KeySpace));
                    if (random.Next(3) == 0)
                    {
                        tree.Delete(key);
                        expected.Remove(key);
                    }
                    else
                    {
                        Put(tree, expected, key, random);
                    }

                    AssertRead(tree, expected, key);
                    AssertRead(tree, expected, Key(random.Next(KeySpace)));
                }

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                var tree = tx.ReadTree("test");
                for (int i = 0; i < KeySpace; i++)
                    AssertRead(tree, expected, Key(i));
            }
        }

        private static string Key(int i) => i.ToString("D8");

        private static void Put(Tree tree, Dictionary<string, string> expected, string key, Random random)
        {
            var value = new string((char)('a' + random.Next(26)), random.Next(8, 300));
            tree.Add(key, value);
            expected[key] = value;
        }

        private static void AssertRead(Tree tree, Dictionary<string, string> expected, string key)
        {
            var read = tree.Read(key);
            if (expected.TryGetValue(key, out var value) == false)
            {
                Assert.Null(read);
                return;
            }

            Assert.NotNull(read);
            var reader = read.Reader;
            Assert.Equal(value, reader.ReadString(reader.Length));
        }
    }
}
