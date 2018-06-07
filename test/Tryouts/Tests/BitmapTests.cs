using System;
using System.Collections.Generic;
using System.Linq;
using Sparrow.Json;
using Tryouts.Corax;
using Xunit;
using Assert = Xunit.Assert;

namespace Tryouts.Tests
{
    public class BitmapTests
    {
        [Fact]
        public void Empty()
        {
            Validate(Array.Empty<ulong>());
        }


        [Fact]
        public void SimpleXor()
        {
            var size = ValidateAndNot(new ulong[] { 1, 2, 3 }, new ulong[] { 2, 4, 8 });
            Assert.Equal(14, size);
        }


        [Fact]
        public void XorUsingArray()
        {
            var size = ValidateAndNot(
                Enumerable.Range(0, 2048).Select(x => (ulong)x * 2),
                Enumerable.Range(0, 1048).Select(x => (ulong)x * 2 + 1024)
                );
            Assert.Equal(4003, size);
        }
        [Fact]
        public void XorUsingBitmap()
        {
            var size = ValidateAndNot(
                Enumerable.Range(0, 8048).Select(x => (ulong)x * 2),
                Enumerable.Range(0, 1048).Select(x => (ulong)x * 2 + 1024)
                );
            Assert.Equal(8193, size);
        }

        [Fact]
        public void SimpleAnd()
        {
            var size = ValidateAnd(new ulong[] { 1, 2, 3 }, new ulong[] { 2, 4, 8 });
            Assert.Equal(6, size);
        }


        [Fact]
        public void AndUsingArray()
        {
            var size = ValidateAnd(
                Enumerable.Range(0, 2048).Select(x => (ulong)x * 2),
                Enumerable.Range(0, 1048).Select(x => (ulong)x * 2 + 1024)
                );
            Assert.Equal(4195, size);
        }
        [Fact]
        public void AndUsingBitmap()
        {
            var size = ValidateAnd(
                Enumerable.Range(0, 8048).Select(x => (ulong)x * 2),
                Enumerable.Range(0, 1048).Select(x => (ulong)x * 2  + 1024)
                );
            Assert.Equal(8193, size);
        }

        [Fact]
        public void SimpleOr()
        {
            var size = ValidateOr(new ulong[] { 1, 2, 3}, new ulong[] { 2, 4, 8 });
            Assert.Equal(10, size);
        }

        [Fact]
        public void OrUsingArray()
        {
            var size = ValidateOr(
                Enumerable.Range(0, 2048).Select(x=>  (ulong)x*2),
                Enumerable.Range(0, 1048).Select(x => (ulong)x * 2 + 16*1024)
                );
            Assert.Equal(6195, size);
        }

        [Fact]
        public void OrUsingBitmap()
        {
            var size = ValidateOr(
                Enumerable.Range(0, 8048).Select(x => (ulong)x * 2),
                Enumerable.Range(0, 1048).Select(x => (ulong)x * 2 + 16 * 1024)
                );
            Assert.Equal(8193, size);
        }

        [Fact]
        public void SmallConsecutiveValues()
        {
            Validate(new ulong[] { 1, 2, 3, 4, 5 });
        }

        [Fact]
        public void Manual()
        {
            Validate(new ulong[] { 1, 2, 7, 70_000 + 48, 70_000 + 49, 1_000_000, (ulong)int.MaxValue + 3, (ulong)int.MaxValue*18});
        }

        [Fact]
        public void LargeConsecutiveValues()
        {
            Validate(Enumerable.Range(0, 1_000_000).Select(i=>(ulong)i));
        }

        [Fact]
        public void SkippingSequetial()
        {
            Validate(
                Enumerable.Range(0, 1_000).Select(i => (ulong)i)
                .Concat(Enumerable.Range(0, 1_000).Select(i => (ulong)i + 5_000))
                .Concat(Enumerable.Range(0, 1_000).Select(i => (ulong)i + 50_000))
                .Concat(Enumerable.Range(0, 1_000).Select(i => (ulong)i + 500_000))
                );
        }

        [Fact]
        public void SkippingHops()
        {
            Validate(
                Enumerable.Range(0, 1_000).Select(i => (ulong)i)
                .Concat(Enumerable.Range(0, 1_000).Select(i => (ulong)i*2 + 5_000))
                .Concat(Enumerable.Range(0, 1_000).Select(i => (ulong)i*3 + 50_000))
                .Concat(Enumerable.Range(0, 1_000).Select(i => (ulong)i*17 + 500_000))
                );
        }

        [Fact]
        public void UsingArrays()
        {
            var bytes = Validate(
                Enumerable.Range(0, 1_000).Select(i => (ulong)i * 200)
                .Concat(Enumerable.Range(0, 1_000).Select(i => (ulong)i * 200 + 100_000))
                );
            Assert.Equal(4025, bytes);
        }

        [Fact]
        public void UsingBitmap()
        {
            var bytes = Validate(
                Enumerable.Range(0, 8_000).Select(i => (ulong)i * 2)
                .Concat(Enumerable.Range(0, 8_000).Select(i => (ulong)i * 2 + 100_000))
                );
            Assert.Equal(16386, bytes);
        }

        [Fact]
        public void UsingBitmapsWithCompression()
        {
            var bytes = Validate(
                Enumerable.Range(0, 8_000).Select(i => (ulong)i)
                .Concat(Enumerable.Range(0, 8_000).Select(i => (ulong)i  + 10_000))
                );
            Assert.Equal(10, bytes);
        }

        [Fact]
        public void UsingArraysCanCompressSome()
        {
            var bytes = Validate(
                Enumerable.Range(0, 1_000).Select(i => (ulong)i)
                .Concat(Enumerable.Range(0, 1_000).Select(i => (ulong)i + 100_000))
                );
            Assert.Equal(12, bytes);
        }

        private unsafe int Validate(IEnumerable<ulong> vals)
        {
            var items = vals.ToArray();
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                PackedBitmapReader reader;
                using (var builder = new PackedBitmapBuilder(ctx))
                {
                    foreach (var item in items)
                    {
                        builder.Set(item);
                    }
                    builder.Complete(out reader);
                }
                
                int index = 0;
                while (reader.MoveNext())
                {
                    Assert.Equal(items[index++], reader.Current);
                }
                Assert.Equal(items.Length, index);
                return reader.SizeInBytes;
            }
        }

        private unsafe int ValidateOr(IEnumerable<ulong> a, IEnumerable<ulong> b)
        {
            var itemsA = a.ToArray();
            var itemsb = b.ToArray();
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                var readerA = Build(ctx, itemsA);
                var readerB = Build(ctx, itemsb);
                var final = itemsA.Union(itemsb).ToList();
                final.Sort();

                var reader = PackedBitmapReader.Or(ctx, ref readerA, ref readerB);
                int index = 0;
                while (reader.MoveNext())
                {
                    Assert.Equal(final[index++], reader.Current);
                }
                Assert.Equal(final.Count, index);
                return reader.SizeInBytes;
            }
        }

        private unsafe int ValidateAnd(IEnumerable<ulong> a, IEnumerable<ulong> b)
        {
            var itemsA = a.ToArray();
            var itemsb = b.ToArray();
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                var readerA = Build(ctx, itemsA);
                var readerB = Build(ctx, itemsb);
                var final = itemsA.Intersect(itemsb).ToList();
                final.Sort();

                var reader = PackedBitmapReader.And(ctx, ref readerA, ref readerB);
                int index = 0;
                while (reader.MoveNext())
                {
                    Assert.Equal(final[index++], reader.Current);
                }
                Assert.Equal(final.Count, index);
                return reader.SizeInBytes;
            }
        }

        private unsafe int ValidateAndNot(IEnumerable<ulong> a, IEnumerable<ulong> b)
        {
            var itemsA = a.ToArray();
            var itemsb = b.ToArray();
            using (var ctx = JsonOperationContext.ShortTermSingleUse())
            {
                var readerA = Build(ctx, itemsA);
                var readerB = Build(ctx, itemsb);
                var final = itemsA.Union(itemsb).ToList();
                var toRemove = itemsA.Intersect(itemsb).ToList();
                foreach (var item in toRemove)
                {
                    final.Remove(item);
                }
                final.Sort();

                var reader = PackedBitmapReader.AndNot(ctx, ref readerA, ref readerB);
                int index = 0;
                while (reader.MoveNext())
                {
                    Assert.Equal(final[index++], reader.Current);
                }
                Assert.Equal(final.Count, index);
                return reader.SizeInBytes;
            }
        }


        private static unsafe PackedBitmapReader Build(JsonOperationContext ctx, ulong[] items)
        {
            using(var builder = new PackedBitmapBuilder(ctx))
            {
                foreach (var item in items)
                {
                    builder.Set(item);
                }
                builder.Complete(out var reader);

                return reader;
            }
        }
    }
}
