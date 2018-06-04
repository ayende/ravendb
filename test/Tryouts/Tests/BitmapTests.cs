using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices.ComTypes;
using NetTopologySuite.Utilities;
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
        public void SmallConsecutiveValues()
        {
            Validate(new ulong[] { 1, 2, 3, 4, 5 });
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
            using (var writer = ctx.GetStream(8192))
            using (ctx.GetManagedBuffer(out var buffer))
            {
                var builder = new PackedBitmapBuilder(writer, buffer);
                foreach (var item in items)
                {
                    builder.Set(item);
                }
                builder.Complete(out var ptr, out var size);
                
                var reader = new PackedBitmapReader(ptr,size);
                int index = 0;
                while (reader.MoveNext())
                {
                    Assert.Equal(items[index++], reader.Current);
                }
                Assert.Equal(items.Length, index);
                return writer.SizeInBytes;
            }
        }
    }
}
