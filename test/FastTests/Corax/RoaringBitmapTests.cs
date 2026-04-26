using System;
using System.Collections.Generic;
using System.Linq;
using Corax.Utils.RoaringBitmaps;
using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Corax;

public class RoaringBitmapTests : NoDisposalNeeded
{
    public RoaringBitmapTests(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void CanAddAndContainsSingleValue()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        var bitmap = new RoaringBitmap(ctx);
        try
        {
            bitmap.Add(42);
            Assert.True(bitmap.Contains(42));
            Assert.False(bitmap.Contains(43));
            Assert.Equal(1, bitmap.Cardinality);
        }
        finally
        {
            bitmap.Dispose();
        }
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void CanAddManyValuesInSameContainer()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        var bitmap = new RoaringBitmap(ctx);
        try
        {
            for (int i = 0; i < 1000; i++)
                bitmap.Add(i);

            for (int i = 0; i < 1000; i++)
                Assert.True(bitmap.Contains(i));

            Assert.False(bitmap.Contains(1000));
            Assert.Equal(1000, bitmap.Cardinality);
        }
        finally
        {
            bitmap.Dispose();
        }
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void CanAddValuesAcrossMultipleContainers()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        var bitmap = new RoaringBitmap(ctx);
        try
        {
            // Values in different 64K ranges
            bitmap.Add(0);
            bitmap.Add(65536);     // container key 1
            bitmap.Add(131072);    // container key 2
            bitmap.Add(1_000_000); // container key 15

            Assert.True(bitmap.Contains(0));
            Assert.True(bitmap.Contains(65536));
            Assert.True(bitmap.Contains(131072));
            Assert.True(bitmap.Contains(1_000_000));
            Assert.False(bitmap.Contains(1));
            Assert.Equal(4, bitmap.Cardinality);
            Assert.Equal(4, bitmap.ContainerCount);
        }
        finally
        {
            bitmap.Dispose();
        }
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void ArrayToBitmapConversionOnThreshold()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        var bitmap = new RoaringBitmap(ctx);
        try
        {
            // Add more than 4096 values to trigger array->bitmap conversion
            for (int i = 0; i < 5000; i++)
                bitmap.Add(i);

            Assert.Equal(5000, bitmap.Cardinality);

            for (int i = 0; i < 5000; i++)
                Assert.True(bitmap.Contains(i));
        }
        finally
        {
            bitmap.Dispose();
        }
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void CanRemoveValues()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        var bitmap = new RoaringBitmap(ctx);
        try
        {
            for (int i = 0; i < 100; i++)
                bitmap.Add(i);

            bitmap.Remove(50);
            Assert.False(bitmap.Contains(50));
            Assert.True(bitmap.Contains(49));
            Assert.True(bitmap.Contains(51));
            Assert.Equal(99, bitmap.Cardinality);
        }
        finally
        {
            bitmap.Dispose();
        }
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void RemoveFromBitmapContainerConvertsToArray()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        var bitmap = new RoaringBitmap(ctx);
        try
        {
            // Add values to create a bitmap container (>4096)
            for (int i = 0; i < 4097; i++)
                bitmap.Add(i * 2); // even numbers

            Assert.Equal(4097, bitmap.Cardinality);

            // Remove enough to go below threshold
            for (int i = 4096; i >= 4096; i--)
                bitmap.Remove(i * 2);

            Assert.Equal(4096, bitmap.Cardinality);
        }
        finally
        {
            bitmap.Dispose();
        }
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void FullContainerPromotion()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        var bitmap = new RoaringBitmap(ctx);
        try
        {
            // Fill an entire container
            for (int i = 0; i < 65536; i++)
                bitmap.Add(i);

            Assert.Equal(65536, bitmap.Cardinality);
            Assert.True(bitmap.Contains(0));
            Assert.True(bitmap.Contains(65535));

            // Remove one to go back to bitmap
            bitmap.Remove(100);
            Assert.False(bitmap.Contains(100));
            Assert.Equal(65535, bitmap.Cardinality);
        }
        finally
        {
            bitmap.Dispose();
        }
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void DuplicateAddIsIdempotent()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        var bitmap = new RoaringBitmap(ctx);
        try
        {
            bitmap.Add(42);
            bitmap.Add(42);
            bitmap.Add(42);
            Assert.Equal(1, bitmap.Cardinality);
        }
        finally
        {
            bitmap.Dispose();
        }
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void RemoveNonexistentIsNoop()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        var bitmap = new RoaringBitmap(ctx);
        try
        {
            bitmap.Add(1);
            bitmap.Remove(2);
            Assert.Equal(1, bitmap.Cardinality);
            Assert.True(bitmap.Contains(1));
        }
        finally
        {
            bitmap.Dispose();
        }
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void CanHandle64BitValues()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        var bitmap = new RoaringBitmap(ctx);
        try
        {
            long largeValue = (long)int.MaxValue + 100;
            bitmap.Add(largeValue);
            Assert.True(bitmap.Contains(largeValue));
            Assert.False(bitmap.Contains(largeValue + 1));
            Assert.Equal(1, bitmap.Cardinality);
        }
        finally
        {
            bitmap.Dispose();
        }
    }

    #region Set Operations

    [RavenFact(RavenTestCategory.Corax)]
    public void AndIntersection()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        var a = new RoaringBitmap(ctx);
        var b = new RoaringBitmap(ctx);
        try
        {
            for (int i = 0; i < 1000; i++)
                a.Add(i);
            for (int i = 500; i < 1500; i++)
                b.Add(i);

            var result = RoaringBitmapSetOps.And(ctx, ref a, ref b);
            try
            {
                Assert.Equal(500, result.Cardinality);
                for (int i = 500; i < 1000; i++)
                    Assert.True(result.Contains(i));
                Assert.False(result.Contains(499));
                Assert.False(result.Contains(1000));
            }
            finally
            {
                result.Dispose();
            }
        }
        finally
        {
            a.Dispose();
            b.Dispose();
        }
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void OrUnion()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        var a = new RoaringBitmap(ctx);
        var b = new RoaringBitmap(ctx);
        try
        {
            for (int i = 0; i < 1000; i++)
                a.Add(i);
            for (int i = 500; i < 1500; i++)
                b.Add(i);

            var result = RoaringBitmapSetOps.Or(ctx, ref a, ref b);
            try
            {
                Assert.Equal(1500, result.Cardinality);
                for (int i = 0; i < 1500; i++)
                    Assert.True(result.Contains(i));
                Assert.False(result.Contains(1500));
            }
            finally
            {
                result.Dispose();
            }
        }
        finally
        {
            a.Dispose();
            b.Dispose();
        }
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void XorSymmetricDifference()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        var a = new RoaringBitmap(ctx);
        var b = new RoaringBitmap(ctx);
        try
        {
            for (int i = 0; i < 1000; i++)
                a.Add(i);
            for (int i = 500; i < 1500; i++)
                b.Add(i);

            var result = RoaringBitmapSetOps.Xor(ctx, ref a, ref b);
            try
            {
                // 0..499 and 1000..1499 = 1000 elements
                Assert.Equal(1000, result.Cardinality);
                for (int i = 0; i < 500; i++)
                    Assert.True(result.Contains(i));
                for (int i = 500; i < 1000; i++)
                    Assert.False(result.Contains(i));
                for (int i = 1000; i < 1500; i++)
                    Assert.True(result.Contains(i));
            }
            finally
            {
                result.Dispose();
            }
        }
        finally
        {
            a.Dispose();
            b.Dispose();
        }
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void AndNotDifference()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        var a = new RoaringBitmap(ctx);
        var b = new RoaringBitmap(ctx);
        try
        {
            for (int i = 0; i < 1000; i++)
                a.Add(i);
            for (int i = 500; i < 1500; i++)
                b.Add(i);

            var result = RoaringBitmapSetOps.AndNot(ctx, ref a, ref b);
            try
            {
                Assert.Equal(500, result.Cardinality);
                for (int i = 0; i < 500; i++)
                    Assert.True(result.Contains(i));
                for (int i = 500; i < 1000; i++)
                    Assert.False(result.Contains(i));
            }
            finally
            {
                result.Dispose();
            }
        }
        finally
        {
            a.Dispose();
            b.Dispose();
        }
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void AndWithDisjointBitmaps()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        var a = new RoaringBitmap(ctx);
        var b = new RoaringBitmap(ctx);
        try
        {
            for (int i = 0; i < 100; i++)
                a.Add(i);
            for (int i = 200; i < 300; i++)
                b.Add(i);

            var result = RoaringBitmapSetOps.And(ctx, ref a, ref b);
            try
            {
                Assert.Equal(0, result.Cardinality);
                Assert.True(result.IsEmpty);
            }
            finally
            {
                result.Dispose();
            }
        }
        finally
        {
            a.Dispose();
            b.Dispose();
        }
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void SetOpsWithDenseBitmapContainers()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        var a = new RoaringBitmap(ctx);
        var b = new RoaringBitmap(ctx);
        try
        {
            // Dense enough to be bitmap containers (>4096 each)
            for (int i = 0; i < 10000; i++)
                a.Add(i * 2); // evens 0..19998
            for (int i = 0; i < 10000; i++)
                b.Add(i * 2 + 1); // odds 1..19999

            // AND should be empty (no overlap)
            var andResult = RoaringBitmapSetOps.And(ctx, ref a, ref b);
            Assert.Equal(0, andResult.Cardinality);
            andResult.Dispose();

            // OR should have all 20000
            var orResult = RoaringBitmapSetOps.Or(ctx, ref a, ref b);
            Assert.Equal(20000, orResult.Cardinality);
            orResult.Dispose();
        }
        finally
        {
            a.Dispose();
            b.Dispose();
        }
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void SetOpsWithMixedContainerTypes()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        var sparse = new RoaringBitmap(ctx);
        var dense = new RoaringBitmap(ctx);
        try
        {
            // Sparse (array container): 100 values
            for (int i = 0; i < 100; i++)
                sparse.Add(i * 10);

            // Dense (bitmap container): 5000 values
            for (int i = 0; i < 5000; i++)
                dense.Add(i);

            // AND: sparse values that exist in dense
            var andResult = RoaringBitmapSetOps.And(ctx, ref sparse, ref dense);
            try
            {
                // Values 0, 10, 20, ..., 490 (up to 4990 but dense only goes to 4999)
                int expected = 0;
                for (int i = 0; i < 100; i++)
                {
                    if (i * 10 < 5000)
                        expected++;
                }
                Assert.Equal(expected, andResult.Cardinality);
            }
            finally
            {
                andResult.Dispose();
            }
        }
        finally
        {
            sparse.Dispose();
            dense.Dispose();
        }
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void SetOpsAcrossMultipleContainers()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        var a = new RoaringBitmap(ctx);
        var b = new RoaringBitmap(ctx);
        try
        {
            // a: containers 0 and 1
            for (int i = 0; i < 200; i++)
                a.Add(i);
            for (int i = 65536; i < 65736; i++)
                a.Add(i);

            // b: containers 1 and 2
            for (int i = 65536; i < 65636; i++)
                b.Add(i);
            for (int i = 131072; i < 131172; i++)
                b.Add(i);

            var orResult = RoaringBitmapSetOps.Or(ctx, ref a, ref b);
            try
            {
                Assert.Equal(200 + 200 + 100, orResult.Cardinality); // container0(200) + container1(200 union) + container2(100)
            }
            finally
            {
                orResult.Dispose();
            }

            var andResult = RoaringBitmapSetOps.And(ctx, ref a, ref b);
            try
            {
                Assert.Equal(100, andResult.Cardinality); // only container1 intersection
            }
            finally
            {
                andResult.Dispose();
            }
        }
        finally
        {
            a.Dispose();
            b.Dispose();
        }
    }

    #endregion

    #region Iterator Tests

    [RavenFact(RavenTestCategory.Corax)]
    public void IteratorReturnsAllValuesInOrder()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        var bitmap = new RoaringBitmap(ctx);
        try
        {
            long[] expected = { 5, 10, 100, 1000, 65536, 65537, 131072 };
            foreach (long val in expected)
                bitmap.Add(val);

            var iterator = bitmap.GetIterator();
            Span<long> buffer = stackalloc long[100];
            int count = bitmap.Fill(buffer, ref iterator);

            Assert.Equal(expected.Length, count);
            for (int i = 0; i < expected.Length; i++)
                Assert.Equal(expected[i], buffer[i]);
        }
        finally
        {
            bitmap.Dispose();
        }
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void IteratorWorksWithSmallBuffer()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        var bitmap = new RoaringBitmap(ctx);
        try
        {
            for (int i = 0; i < 100; i++)
                bitmap.Add(i);

            var iterator = bitmap.GetIterator();
            List<long> allValues = new();
            Span<long> buffer = stackalloc long[10];

            int total = 0;
            int count;
            while ((count = bitmap.Fill(buffer, ref iterator)) > 0)
            {
                for (int i = 0; i < count; i++)
                    allValues.Add(buffer[i]);
                total += count;
            }

            Assert.Equal(100, total);
            for (int i = 0; i < 100; i++)
                Assert.Equal(i, allValues[i]);
        }
        finally
        {
            bitmap.Dispose();
        }
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void IteratorHandlesBitmapContainers()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        var bitmap = new RoaringBitmap(ctx);
        try
        {
            // Dense enough for bitmap container
            for (int i = 0; i < 5000; i++)
                bitmap.Add(i);

            var iterator = bitmap.GetIterator();
            List<long> allValues = new();
            long[] buffer = new long[256];

            int count;
            while ((count = bitmap.Fill(buffer, ref iterator)) > 0)
            {
                for (int i = 0; i < count; i++)
                    allValues.Add(buffer[i]);
            }

            Assert.Equal(5000, allValues.Count);
            for (int i = 0; i < 5000; i++)
                Assert.Equal(i, allValues[i]);
        }
        finally
        {
            bitmap.Dispose();
        }
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void IteratorHandlesFullContainer()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        var bitmap = new RoaringBitmap(ctx);
        try
        {
            for (int i = 0; i < 65536; i++)
                bitmap.Add(i);

            var iterator = bitmap.GetIterator();
            long[] buffer = new long[1024];
            int total = 0;

            int count;
            while ((count = bitmap.Fill(buffer, ref iterator)) > 0)
                total += count;

            Assert.Equal(65536, total);
        }
        finally
        {
            bitmap.Dispose();
        }
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void IteratorHandlesMultipleContainers()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        var bitmap = new RoaringBitmap(ctx);
        try
        {
            bitmap.Add(10);
            bitmap.Add(65546); // container 1, value 10
            bitmap.Add(131082); // container 2, value 10

            var iterator = bitmap.GetIterator();
            Span<long> buffer = stackalloc long[10];
            int count = bitmap.Fill(buffer, ref iterator);

            Assert.Equal(3, count);
            Assert.Equal(10, buffer[0]);
            Assert.Equal(65546, buffer[1]);
            Assert.Equal(131082, buffer[2]);
        }
        finally
        {
            bitmap.Dispose();
        }
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void EmptyBitmapIteratorReturnsZero()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        var bitmap = new RoaringBitmap(ctx);
        try
        {
            var iterator = bitmap.GetIterator();
            Span<long> buffer = stackalloc long[10];
            int count = bitmap.Fill(buffer, ref iterator);
            Assert.Equal(0, count);
        }
        finally
        {
            bitmap.Dispose();
        }
    }

    #endregion

    #region Range Container Tests

    [RavenFact(RavenTestCategory.Corax)]
    public void RangeContainerCreatedForSequentialAdds()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        var bitmap = new RoaringBitmap(ctx);
        try
        {
            // Sequential adds from 0 should create a Range container
            for (int i = 0; i < 1000; i++)
                bitmap.Add(i);

            Assert.Equal(1000, bitmap.Cardinality);
            for (int i = 0; i < 1000; i++)
                Assert.True(bitmap.Contains(i));
            Assert.False(bitmap.Contains(1000));
        }
        finally
        {
            bitmap.Dispose();
        }
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void RangeContainerFullContainer()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        var bitmap = new RoaringBitmap(ctx);
        try
        {
            // Fill entire container sequentially — should be Range with count=65536
            for (int i = 0; i < 65536; i++)
                bitmap.Add(i);

            Assert.Equal(65536, bitmap.Cardinality);
            Assert.True(bitmap.Contains(0));
            Assert.True(bitmap.Contains(65535));

            // Remove from middle converts to bitmap
            bitmap.Remove(100);
            Assert.False(bitmap.Contains(100));
            Assert.Equal(65535, bitmap.Cardinality);

            // Remove from end is cheap
            bitmap.Add(100); // re-add via bitmap
            bitmap.Remove(65535);
            Assert.Equal(65535, bitmap.Cardinality);
        }
        finally
        {
            bitmap.Dispose();
        }
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void RangeContainerRemoveFromEnd()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        var bitmap = new RoaringBitmap(ctx);
        try
        {
            for (int i = 0; i < 1000; i++)
                bitmap.Add(i);

            // Remove from end is O(1) decrement
            bitmap.Remove(999);
            Assert.Equal(999, bitmap.Cardinality);
            Assert.False(bitmap.Contains(999));
            Assert.True(bitmap.Contains(998));
        }
        finally
        {
            bitmap.Dispose();
        }
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void RangeContainerIteratesCorrectly()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        var bitmap = new RoaringBitmap(ctx);
        try
        {
            for (int i = 0; i < 10000; i++)
                bitmap.Add(i);

            var iterator = bitmap.GetIterator();
            long[] buffer = new long[1024];
            int total = 0;

            int count;
            while ((count = bitmap.Fill(buffer, ref iterator)) > 0)
                total += count;

            Assert.Equal(10000, total);
        }
        finally
        {
            bitmap.Dispose();
        }
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void SetOpsWithRangeContainers()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        var a = new RoaringBitmap(ctx);
        var b = new RoaringBitmap(ctx);
        HashSet<long> setA = new();
        HashSet<long> setB = new();

        try
        {
            // a: range container (sequential from 0)
            for (int i = 0; i < 50000; i++)
            {
                a.Add(i);
                setA.Add(i);
            }

            // b: overlapping bitmap container
            for (int i = 30000; i < 60000; i++)
            {
                b.Add(i);
                setB.Add(i);
            }

            // AND
            var andResult = RoaringBitmapSetOps.And(ctx, ref a, ref b);
            HashSet<long> expectedAnd = new(setA);
            expectedAnd.IntersectWith(setB);
            Assert.Equal(expectedAnd.Count, andResult.Cardinality);
            andResult.Dispose();

            // OR
            var orResult = RoaringBitmapSetOps.Or(ctx, ref a, ref b);
            HashSet<long> expectedOr = new(setA);
            expectedOr.UnionWith(setB);
            Assert.Equal(expectedOr.Count, orResult.Cardinality);
            orResult.Dispose();

            // ANDNOT
            var andNotResult = RoaringBitmapSetOps.AndNot(ctx, ref a, ref b);
            HashSet<long> expectedAndNot = new(setA);
            expectedAndNot.ExceptWith(setB);
            Assert.Equal(expectedAndNot.Count, andNotResult.Cardinality);
            andNotResult.Dispose();
        }
        finally
        {
            a.Dispose();
            b.Dispose();
        }
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void AddRangeOptimized()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        var bitmap = new RoaringBitmap(ctx);
        try
        {
            // AddRange should create Range and Bitmap containers efficiently
            bitmap.AddRange(0, 200_000);

            Assert.Equal(200_000, bitmap.Cardinality);
            Assert.True(bitmap.Contains(0));
            Assert.True(bitmap.Contains(65535));
            Assert.True(bitmap.Contains(65536));
            Assert.True(bitmap.Contains(199_999));
            Assert.False(bitmap.Contains(200_000));
        }
        finally
        {
            bitmap.Dispose();
        }
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void RangeContainerGapConvertsToArrayOrBitmap()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        var bitmap = new RoaringBitmap(ctx);
        try
        {
            // Build a range 0..99
            for (int i = 0; i < 100; i++)
                bitmap.Add(i);

            Assert.Equal(100, bitmap.Cardinality);

            // Add a value with a gap — should convert from Range to Array
            bitmap.Add(200);
            Assert.Equal(101, bitmap.Cardinality);
            Assert.True(bitmap.Contains(0));
            Assert.True(bitmap.Contains(99));
            Assert.True(bitmap.Contains(200));
            Assert.False(bitmap.Contains(100));
        }
        finally
        {
            bitmap.Dispose();
        }
    }

    #endregion

    #region SIMD Correctness Verification

    [RavenFact(RavenTestCategory.Corax)]
    public unsafe void SimdAndMatchesScalar()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        int count = RoaringBitmap.BitmapContainerSizeInUlongs;

        ctx.Allocate(RoaringBitmap.BitmapContainerSizeInBytes, out var aStorage);
        ctx.Allocate(RoaringBitmap.BitmapContainerSizeInBytes, out var bStorage);
        ctx.Allocate(RoaringBitmap.BitmapContainerSizeInBytes, out var simdResult);
        ctx.Allocate(RoaringBitmap.BitmapContainerSizeInBytes, out var scalarResult);

        try
        {
            ulong* a = (ulong*)aStorage.Ptr;
            ulong* b = (ulong*)bStorage.Ptr;
            ulong* sr = (ulong*)simdResult.Ptr;
            ulong* sc = (ulong*)scalarResult.Ptr;

            Random rng = new(42);
            for (int i = 0; i < count; i++)
            {
                a[i] = (ulong)rng.NextInt64();
                b[i] = (ulong)rng.NextInt64();
            }

            int simdCard = RoaringBitmapSetOps.BitmapAndSimd(a, b, sr, count);
            int scalarCard = RoaringBitmapSetOps.BitmapAndScalar(a, b, sc, count);

            Assert.Equal(scalarCard, simdCard);
            for (int i = 0; i < count; i++)
                Assert.Equal(sc[i], sr[i]);
        }
        finally
        {
            ctx.Release(ref aStorage);
            ctx.Release(ref bStorage);
            ctx.Release(ref simdResult);
            ctx.Release(ref scalarResult);
        }
    }

    [RavenFact(RavenTestCategory.Corax)]
    public unsafe void SimdOrMatchesScalar()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        int count = RoaringBitmap.BitmapContainerSizeInUlongs;

        ctx.Allocate(RoaringBitmap.BitmapContainerSizeInBytes, out var aStorage);
        ctx.Allocate(RoaringBitmap.BitmapContainerSizeInBytes, out var bStorage);
        ctx.Allocate(RoaringBitmap.BitmapContainerSizeInBytes, out var simdResult);
        ctx.Allocate(RoaringBitmap.BitmapContainerSizeInBytes, out var scalarResult);

        try
        {
            ulong* a = (ulong*)aStorage.Ptr;
            ulong* b = (ulong*)bStorage.Ptr;
            ulong* sr = (ulong*)simdResult.Ptr;
            ulong* sc = (ulong*)scalarResult.Ptr;

            Random rng = new(42);
            for (int i = 0; i < count; i++)
            {
                a[i] = (ulong)rng.NextInt64();
                b[i] = (ulong)rng.NextInt64();
            }

            int simdCard = RoaringBitmapSetOps.BitmapOrSimd(a, b, sr, count);
            int scalarCard = RoaringBitmapSetOps.BitmapOrScalar(a, b, sc, count);

            Assert.Equal(scalarCard, simdCard);
            for (int i = 0; i < count; i++)
                Assert.Equal(sc[i], sr[i]);
        }
        finally
        {
            ctx.Release(ref aStorage);
            ctx.Release(ref bStorage);
            ctx.Release(ref simdResult);
            ctx.Release(ref scalarResult);
        }
    }

    [RavenFact(RavenTestCategory.Corax)]
    public unsafe void SimdXorMatchesScalar()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        int count = RoaringBitmap.BitmapContainerSizeInUlongs;

        ctx.Allocate(RoaringBitmap.BitmapContainerSizeInBytes, out var aStorage);
        ctx.Allocate(RoaringBitmap.BitmapContainerSizeInBytes, out var bStorage);
        ctx.Allocate(RoaringBitmap.BitmapContainerSizeInBytes, out var simdResult);
        ctx.Allocate(RoaringBitmap.BitmapContainerSizeInBytes, out var scalarResult);

        try
        {
            ulong* a = (ulong*)aStorage.Ptr;
            ulong* b = (ulong*)bStorage.Ptr;
            ulong* sr = (ulong*)simdResult.Ptr;
            ulong* sc = (ulong*)scalarResult.Ptr;

            Random rng = new(42);
            for (int i = 0; i < count; i++)
            {
                a[i] = (ulong)rng.NextInt64();
                b[i] = (ulong)rng.NextInt64();
            }

            int simdCard = RoaringBitmapSetOps.BitmapXorSimd(a, b, sr, count);
            int scalarCard = RoaringBitmapSetOps.BitmapXorScalar(a, b, sc, count);

            Assert.Equal(scalarCard, simdCard);
            for (int i = 0; i < count; i++)
                Assert.Equal(sc[i], sr[i]);
        }
        finally
        {
            ctx.Release(ref aStorage);
            ctx.Release(ref bStorage);
            ctx.Release(ref simdResult);
            ctx.Release(ref scalarResult);
        }
    }

    [RavenFact(RavenTestCategory.Corax)]
    public unsafe void SimdAndNotMatchesScalar()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        int count = RoaringBitmap.BitmapContainerSizeInUlongs;

        ctx.Allocate(RoaringBitmap.BitmapContainerSizeInBytes, out var aStorage);
        ctx.Allocate(RoaringBitmap.BitmapContainerSizeInBytes, out var bStorage);
        ctx.Allocate(RoaringBitmap.BitmapContainerSizeInBytes, out var simdResult);
        ctx.Allocate(RoaringBitmap.BitmapContainerSizeInBytes, out var scalarResult);

        try
        {
            ulong* a = (ulong*)aStorage.Ptr;
            ulong* b = (ulong*)bStorage.Ptr;
            ulong* sr = (ulong*)simdResult.Ptr;
            ulong* sc = (ulong*)scalarResult.Ptr;

            Random rng = new(42);
            for (int i = 0; i < count; i++)
            {
                a[i] = (ulong)rng.NextInt64();
                b[i] = (ulong)rng.NextInt64();
            }

            int simdCard = RoaringBitmapSetOps.BitmapAndNotSimd(a, b, sr, count);
            int scalarCard = RoaringBitmapSetOps.BitmapAndNotScalar(a, b, sc, count);

            Assert.Equal(scalarCard, simdCard);
            for (int i = 0; i < count; i++)
                Assert.Equal(sc[i], sr[i]);
        }
        finally
        {
            ctx.Release(ref aStorage);
            ctx.Release(ref bStorage);
            ctx.Release(ref simdResult);
            ctx.Release(ref scalarResult);
        }
    }

    #endregion

    #region Stress / Large Scale Tests

    [RavenFact(RavenTestCategory.Corax)]
    public void LargeScaleAddAndIterate()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        var bitmap = new RoaringBitmap(ctx);
        try
        {
            int totalValues = 100_000;
            HashSet<long> expected = new();
            Random rng = new(123);

            for (int i = 0; i < totalValues; i++)
            {
                long value = rng.NextInt64(0, 1_000_000);
                bitmap.Add(value);
                expected.Add(value);
            }

            Assert.Equal(expected.Count, bitmap.Cardinality);

            // Verify all values via iteration
            var iterator = bitmap.GetIterator();
            long[] buffer = new long[4096];
            HashSet<long> iterated = new();

            int count;
            while ((count = bitmap.Fill(buffer, ref iterator)) > 0)
            {
                for (int i = 0; i < count; i++)
                    iterated.Add(buffer[i]);
            }

            Assert.Equal(expected.Count, iterated.Count);
            Assert.True(expected.SetEquals(iterated));
        }
        finally
        {
            bitmap.Dispose();
        }
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void SetOpsCorrectnessAgainstHashSet()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        var a = new RoaringBitmap(ctx);
        var b = new RoaringBitmap(ctx);
        HashSet<long> setA = new();
        HashSet<long> setB = new();

        try
        {
            Random rng = new(456);
            for (int i = 0; i < 10_000; i++)
            {
                long va = rng.NextInt64(0, 200_000);
                long vb = rng.NextInt64(0, 200_000);
                a.Add(va);
                b.Add(vb);
                setA.Add(va);
                setB.Add(vb);
            }

            // AND
            var andResult = RoaringBitmapSetOps.And(ctx, ref a, ref b);
            HashSet<long> expectedAnd = new(setA);
            expectedAnd.IntersectWith(setB);
            Assert.Equal(expectedAnd.Count, andResult.Cardinality);
            foreach (long v in expectedAnd)
                Assert.True(andResult.Contains(v));
            andResult.Dispose();

            // OR
            var orResult = RoaringBitmapSetOps.Or(ctx, ref a, ref b);
            HashSet<long> expectedOr = new(setA);
            expectedOr.UnionWith(setB);
            Assert.Equal(expectedOr.Count, orResult.Cardinality);
            orResult.Dispose();

            // XOR
            var xorResult = RoaringBitmapSetOps.Xor(ctx, ref a, ref b);
            HashSet<long> expectedXor = new(setA);
            expectedXor.SymmetricExceptWith(setB);
            Assert.Equal(expectedXor.Count, xorResult.Cardinality);
            xorResult.Dispose();

            // ANDNOT
            var andNotResult = RoaringBitmapSetOps.AndNot(ctx, ref a, ref b);
            HashSet<long> expectedAndNot = new(setA);
            expectedAndNot.ExceptWith(setB);
            Assert.Equal(expectedAndNot.Count, andNotResult.Cardinality);
            andNotResult.Dispose();
        }
        finally
        {
            a.Dispose();
            b.Dispose();
        }
    }

    #endregion
}
