using System;
using System.Collections.Generic;
using System.Linq;
using Voron.Data.RoaringBitmaps;
using Sparrow.Server;
using Sparrow.Threading;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Corax;

public unsafe class RoaringBitmapTests : NoDisposalNeeded
{
    public RoaringBitmapTests(ITestOutputHelper output) : base(output)
    {
    }

    // Set op helpers for testing: deep-clone left bitmap, then mutate in-place.
    // Preserves the original bitmap's container structure for subsequent assertions.
    // Returns RoaringBitmap; caller must dispose it.
    private static RoaringBitmap And(ByteStringContext ctx, RoaringBitmap a, RoaringBitmap b)
    {
        RoaringBitmap result = a.Clone();
        result.AndWith(ref b);
        result.PrepareForReading();
        return result;
    }

    private static RoaringBitmap Or(ByteStringContext ctx, RoaringBitmap a, RoaringBitmap b)
    {
        RoaringBitmap result = a.Clone();
        RoaringBitmap bClone = b.Clone();
        try
        {
            result.OrWith(ref bClone);
            result.PrepareForReading();
        }
        finally
        {
            bClone.Dispose();
        }
        return result;
    }

    private static RoaringBitmap AndNot(ByteStringContext ctx, RoaringBitmap a, RoaringBitmap b)
    {
        RoaringBitmap result = a.Clone();
        result.AndNotWith(ref b);
        result.PrepareForReading();
        return result;
    }


    [RavenFact(RavenTestCategory.Corax)]
    public void CanAddAndContainsSingleValue()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        RoaringBitmap bitmap = new(ctx);
        bitmap.Add(42);
        bitmap.PrepareForReading();
        Assert.True(bitmap.Contains(42));
        Assert.False(bitmap.Contains(43));
        Assert.Equal(1, bitmap.Count);
        bitmap.Dispose();
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void CanAddManyValuesInSameContainer()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        RoaringBitmap bitmap = new(ctx);
        for (int i = 0; i < 1000; i++)
            bitmap.Add(i);

        for (int i = 0; i < 1000; i++)
            Assert.True(bitmap.Contains(i));

        bitmap.PrepareForReading();
        Assert.False(bitmap.Contains(1000));
        Assert.Equal(1000, bitmap.Count);
        bitmap.Dispose();
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void CanAddValuesAcrossMultipleContainers()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        RoaringBitmap bitmap = new(ctx);
        // Values in different 64K ranges
        bitmap.Add(0);
        bitmap.Add(65536);     // container key 1
        bitmap.Add(131072);    // container key 2
        bitmap.Add(1_000_000); // container key 15

        bitmap.PrepareForReading();
        Assert.True(bitmap.Contains(0));
        Assert.True(bitmap.Contains(65536));
        Assert.True(bitmap.Contains(131072));
        Assert.True(bitmap.Contains(1_000_000));
        Assert.False(bitmap.Contains(1));
        Assert.Equal(4, bitmap.Count);
        Assert.Equal(4, bitmap.ContainerCount);
        bitmap.Dispose();
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void CanAddAndContainsValuesLargerThan32Bits()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        RoaringBitmap bitmap = new(ctx);

        // Corax entry IDs are 54-bit values, so a large or long-lived dataset legitimately
        // crosses the 32-bit boundary. The bitmap must accept such values. These stay within a
        // realistic entry-ID range (the container key value >> 16 remains modest) rather than
        // testing pathological sparsity.
        var values = new long[]
        {
            5,                       // small, low container
            (1L << 32) + 7,          // just over 32 bits
            5_000_000_000L,          // ~4.66e9
            (1L << 33) + 11,         // ~8.6e9
            17_000_000_000L,         // ~1.7e10
        };

        foreach (long v in values)
            bitmap.Add(v);

        bitmap.PrepareForReading();

        foreach (long v in values)
            Assert.True(bitmap.Contains(v), $"expected bitmap to contain {v}");

        Assert.False(bitmap.Contains(6));
        Assert.False(bitmap.Contains((1L << 32) + 8));
        Assert.Equal(values.Length, bitmap.Count);
        bitmap.Dispose();
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void ArrayToBitmapConversionOnThreshold()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        RoaringBitmap bitmap = new(ctx);
        // Add more than 4096 values within a single container using a non-sequential
        // pattern (evens) to force ArrayUnsorted→Bitmap conversion (sequential 0..N
        // creates a Range container, which never hits the ArrayUnsorted→Bitmap path).
        for (int i = 0; i < 10000; i++)
            bitmap.Add(i * 2);

        bitmap.PrepareForReading();
        Assert.Equal(10000, bitmap.Count);

        for (int i = 0; i < 10000; i++)
            Assert.True(bitmap.Contains(i * 2));
        Assert.False(bitmap.Contains(1));
        bitmap.Dispose();
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void FullContainerAsRange()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        RoaringBitmap bitmap = new(ctx);
        // Fill an entire container — should become Range with count=65536
        for (int i = 0; i < 65536; i++)
            bitmap.Add(i);

        bitmap.PrepareForReading();
        Assert.Equal(65536, bitmap.Count);
        Assert.True(bitmap.Contains(0));
        Assert.True(bitmap.Contains(65535));
        Assert.False(bitmap.Contains(65536));
        bitmap.Dispose();
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void DuplicateAddIsIdempotent()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        RoaringBitmap bitmap = new(ctx);
        bitmap.Add(42);
        bitmap.Add(42);
        bitmap.Add(42);
        bitmap.PrepareForReading();
        Assert.Equal(1, bitmap.Count);
        bitmap.Dispose();
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void CanHandle64BitValues()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        RoaringBitmap bitmap = new(ctx);
        long largeValue = (long)int.MaxValue + 100;
        bitmap.Add(largeValue);
        bitmap.PrepareForReading();
        Assert.True(bitmap.Contains(largeValue));
        Assert.False(bitmap.Contains(largeValue + 1));
        Assert.Equal(1, bitmap.Count);
        bitmap.Dispose();
    }

    #region Set Operations

    [RavenFact(RavenTestCategory.Corax)]
    public void AndIntersection()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        RoaringBitmap a = new(ctx);
        RoaringBitmap b = new(ctx);
        for (int i = 0; i < 1000; i++)
            a.Add(i);
        for (int i = 500; i < 1500; i++)
            b.Add(i);

        a.PrepareForReading();
        b.PrepareForReading();
        RoaringBitmap result = And(ctx, a, b);
        try
        {
            Assert.Equal(500, result.Count);
            for (int i = 500; i < 1000; i++)
                Assert.True(result.Contains(i));
            Assert.False(result.Contains(499));
            Assert.False(result.Contains(1000));
        }
        finally
        {
            result.Dispose();
            a.Dispose();
            b.Dispose();
        }
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void OrUnion()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        RoaringBitmap a = new(ctx);
        RoaringBitmap b = new(ctx);

        for (int i = 0; i < 1000; i++)
            a.Add(i);
        for (int i = 500; i < 1500; i++)
            b.Add(i);

        a.PrepareForReading();
        b.PrepareForReading();
        RoaringBitmap result = Or(ctx, a, b);

        Assert.Equal(1500, result.Count);
        for (int i = 0; i < 1500; i++)
            Assert.True(result.Contains(i));
        Assert.False(result.Contains(1500));
        result.Dispose();
        a.Dispose();
        b.Dispose();
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void AndNotDifference()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        RoaringBitmap a = new(ctx);
        RoaringBitmap b = new(ctx);
        for (int i = 0; i < 1000; i++)
            a.Add(i);
        for (int i = 500; i < 1500; i++)
            b.Add(i);

        a.PrepareForReading();
        b.PrepareForReading();
        RoaringBitmap result = AndNot(ctx, a, b);
        try
        {
            Assert.Equal(500, result.Count);
            for (int i = 0; i < 500; i++)
                Assert.True(result.Contains(i));
            for (int i = 500; i < 1000; i++)
                Assert.False(result.Contains(i));
        }
        finally
        {
            result.Dispose();
            a.Dispose();
            b.Dispose();
        }
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void AndWithDisjointBitmaps()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        RoaringBitmap a = new(ctx);
        RoaringBitmap b = new(ctx);
        for (int i = 0; i < 100; i++)
            a.Add(i);
        for (int i = 200; i < 300; i++)
            b.Add(i);

        a.PrepareForReading();
        b.PrepareForReading();
        RoaringBitmap result = And(ctx, a, b);
        try
        {
            Assert.Equal(0, result.Count);
            Assert.True(result.IsEmpty);
        }
        finally
        {
            result.Dispose();
            a.Dispose();
            b.Dispose();
        }
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void SetOpsWithDenseBitmapContainers()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        RoaringBitmap a = new(ctx);
        RoaringBitmap b = new(ctx);
        // Dense enough to be bitmap containers (>4096 each)
        for (int i = 0; i < 10000; i++)
            a.Add(i * 2); // evens 0..19998
        for (int i = 0; i < 10000; i++)
            b.Add(i * 2 + 1); // odds 1..19999

        a.PrepareForReading();
        b.PrepareForReading();

        // AND should be empty (no overlap)
        RoaringBitmap andResult = And(ctx, a, b);
        Assert.Equal(0, andResult.Count);
        andResult.Dispose();

        // OR should have all 20000
        RoaringBitmap orResult = Or(ctx, a, b);
        Assert.Equal(20000, orResult.Count);
        orResult.Dispose();
        a.Dispose();
        b.Dispose();
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void SetOpsWithMixedContainerTypes()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        RoaringBitmap sparse = new(ctx);
        RoaringBitmap dense = new(ctx);
        // Sparse (array container): 100 values
        for (int i = 0; i < 100; i++)
            sparse.Add(i * 10);

        // Dense (bitmap container): 5000 values
        for (int i = 0; i < 5000; i++)
            dense.Add(i);

        // AND: sparse values that exist in dense
        sparse.PrepareForReading();
        dense.PrepareForReading();
        RoaringBitmap andResult = And(ctx, sparse, dense);
        try
        {
            // Values 0, 10, 20, ..., 490 (up to 4990 but dense only goes to 4999)
            int expected = 0;
            for (int i = 0; i < 100; i++)
            {
                if (i * 10 < 5000)
                    expected++;
            }
            Assert.Equal(expected, andResult.Count);
        }
        finally
        {
            andResult.Dispose();
            sparse.Dispose();
            dense.Dispose();
        }
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void SetOpsAcrossMultipleContainers()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        RoaringBitmap a = new(ctx);
        RoaringBitmap b = new(ctx);
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

        a.PrepareForReading();
        b.PrepareForReading();
        RoaringBitmap orResult = Or(ctx, a, b);
        try
        {
            Assert.Equal(200 + 200 + 100, orResult.Count); // container0(200) + container1(200 union) + container2(100)
        }
        finally
        {
            orResult.Dispose();
        }

        RoaringBitmap andResult = And(ctx, a, b);
        try
        {
            Assert.Equal(100, andResult.Count); // only container1 intersection
        }
        finally
        {
            andResult.Dispose();
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
        RoaringBitmap bitmap = new(ctx);
        long[] expected = { 5, 10, 100, 1000, 65536, 65537, 131072 };
        foreach (long val in expected)
            bitmap.Add(val);

        var iterator = bitmap.GetIterator();
        Span<long> buffer = stackalloc long[100];
        bitmap.PrepareForReading();
        int count = bitmap.Fill(buffer, ref iterator);

        Assert.Equal(expected.Length, count);
        for (int i = 0; i < expected.Length; i++)
            Assert.Equal(expected[i], buffer[i]);
        iterator.Dispose();
        bitmap.Dispose();
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void IteratorWorksWithSmallBuffer()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        RoaringBitmap bitmap = new(ctx);
        for (int i = 0; i < 100; i++)
            bitmap.Add(i);

        var iterator = bitmap.GetIterator();
        List<long> allValues = new();
        Span<long> buffer = stackalloc long[10];

        int total = 0;
        int count;
        bitmap.PrepareForReading();
        while ((count = bitmap.Fill(buffer, ref iterator)) > 0)
        {
            for (int i = 0; i < count; i++)
                allValues.Add(buffer[i]);
            total += count;
        }

        Assert.Equal(100, total);
        for (int i = 0; i < 100; i++)
            Assert.Equal(i, allValues[i]);
        iterator.Dispose();
        bitmap.Dispose();
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void IteratorHandlesBitmapContainers()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        RoaringBitmap bitmap = new(ctx);
        // Dense enough for bitmap container — use a non-sequential pattern (evens)
        // within a single 64K range to produce Bitmap containers rather than Range.
        for (int i = 0; i < 10000; i++)
            bitmap.Add(i * 2);

        var iterator = bitmap.GetIterator();
        List<long> allValues = new();
        long[] buffer = new long[256];

        int count;
        bitmap.PrepareForReading();
        while ((count = bitmap.Fill(buffer, ref iterator)) > 0)
        {
            for (int i = 0; i < count; i++)
                allValues.Add(buffer[i]);
        }

        Assert.Equal(10000, allValues.Count);
        for (int i = 0; i < 10000; i++)
            Assert.Equal(i * 2, allValues[i]);
        iterator.Dispose();
        bitmap.Dispose();
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void IteratorHandlesFullContainer()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        RoaringBitmap bitmap = new(ctx);
        for (int i = 0; i < 65536; i++)
            bitmap.Add(i);

        var iterator = bitmap.GetIterator();
        long[] buffer = new long[1024];
        int total = 0;

        int count;
        bitmap.PrepareForReading();
        while ((count = bitmap.Fill(buffer, ref iterator)) > 0)
            total += count;

        Assert.Equal(65536, total);
        iterator.Dispose();
        bitmap.Dispose();
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void IteratorHandlesMultipleContainers()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        RoaringBitmap bitmap = new(ctx);
        bitmap.Add(10);
        bitmap.Add(65546); // container 1, value 10
        bitmap.Add(131082); // container 2, value 10

        var iterator = bitmap.GetIterator();
        Span<long> buffer = stackalloc long[10];
        bitmap.PrepareForReading();
        int count = bitmap.Fill(buffer, ref iterator);

        Assert.Equal(3, count);
        Assert.Equal(10, buffer[0]);
        Assert.Equal(65546, buffer[1]);
        Assert.Equal(131082, buffer[2]);
        iterator.Dispose();
        bitmap.Dispose();
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void EmptyBitmapIteratorReturnsZero()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        RoaringBitmap bitmap = new(ctx);
        var iterator = bitmap.GetIterator();
        Span<long> buffer = stackalloc long[10];
        bitmap.PrepareForReading();
        int count = bitmap.Fill(buffer, ref iterator);
        Assert.Equal(0, count);
        iterator.Dispose();
        bitmap.Dispose();
    }

    #endregion

    #region Range Container Tests

    [RavenFact(RavenTestCategory.Corax)]
    public void RangeContainerCreatedForSequentialAdds()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        RoaringBitmap bitmap = new(ctx);
        // Sequential adds from 0 should create a Range container
        for (int i = 0; i < 1000; i++)
            bitmap.Add(i);

        bitmap.PrepareForReading();
        Assert.Equal(1000, bitmap.Count);
        for (int i = 0; i < 1000; i++)
            Assert.True(bitmap.Contains(i));
        Assert.False(bitmap.Contains(1000));
        bitmap.Dispose();
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void RangeContainerFullContainer()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        RoaringBitmap bitmap = new(ctx);
        // Fill entire container sequentially — should be Range with count=65536
        for (int i = 0; i < 65536; i++)
            bitmap.Add(i);

        bitmap.PrepareForReading();
        Assert.Equal(65536, bitmap.Count);
        Assert.True(bitmap.Contains(0));
        Assert.True(bitmap.Contains(65535));
        Assert.False(bitmap.Contains(65536));
        bitmap.Dispose();
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void RangeContainerIteratesCorrectly()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        RoaringBitmap bitmap = new(ctx);
        for (int i = 0; i < 10000; i++)
            bitmap.Add(i);

        var iterator = bitmap.GetIterator();
        long[] buffer = new long[1024];
        int total = 0;

        int count;
        bitmap.PrepareForReading();
        while ((count = bitmap.Fill(buffer, ref iterator)) > 0)
            total += count;

        Assert.Equal(10000, total);
        iterator.Dispose();
        bitmap.Dispose();
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void SetOpsWithRangeContainers()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        RoaringBitmap a = new(ctx);
        RoaringBitmap b = new(ctx);
        HashSet<long> setA = new();
        HashSet<long> setB = new();

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
        a.PrepareForReading();
        b.PrepareForReading();
        RoaringBitmap andResult = And(ctx, a, b);
        HashSet<long> expectedAnd = new(setA);
        expectedAnd.IntersectWith(setB);
        Assert.Equal(expectedAnd.Count, andResult.Count);
        andResult.Dispose();

        // OR
        RoaringBitmap orResult = Or(ctx, a, b);
        HashSet<long> expectedOr = new(setA);
        expectedOr.UnionWith(setB);
        Assert.Equal(expectedOr.Count, orResult.Count);
        orResult.Dispose();

        // ANDNOT
        RoaringBitmap andNotResult = AndNot(ctx, a, b);
        HashSet<long> expectedAndNot = new(setA);
        expectedAndNot.ExceptWith(setB);
        Assert.Equal(expectedAndNot.Count, andNotResult.Count);
        andNotResult.Dispose();
        a.Dispose();
        b.Dispose();
    }


    [RavenFact(RavenTestCategory.Corax)]
    public void RangeContainerGapConvertsToArrayOrBitmap()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        RoaringBitmap bitmap = new(ctx);
        // Build a range 0..99
        for (int i = 0; i < 100; i++)
            bitmap.Add(i);

        bitmap.PrepareForReading();
        Assert.Equal(100, bitmap.Count);

        // Add a value with a gap — should convert from Range to ArrayUnsorted
        bitmap.Add(200);
        bitmap.PrepareForReading();
        Assert.Equal(101, bitmap.Count);
        Assert.True(bitmap.Contains(0));
        Assert.True(bitmap.Contains(99));
        Assert.True(bitmap.Contains(200));
        Assert.False(bitmap.Contains(100));
        bitmap.Dispose();
    }

    #endregion

    #region Stress / Large Scale Tests

    [RavenFact(RavenTestCategory.Corax)]
    public void LargeScaleAddAndIterate()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        RoaringBitmap bitmap = new(ctx);
        int totalValues = 100_000;
        HashSet<long> expected = new();
        Random rng = new(123);

        for (int i = 0; i < totalValues; i++)
        {
            long value = rng.NextInt64(0, 1_000_000);
            bitmap.Add(value);
            expected.Add(value);
        }

        bitmap.PrepareForReading();
        Assert.Equal(expected.Count, bitmap.Count);

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
        iterator.Dispose();
        bitmap.Dispose();
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void SetOpsCorrectnessAgainstHashSet()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        RoaringBitmap a = new(ctx);
        RoaringBitmap b = new(ctx);
        HashSet<long> setA = new();
        HashSet<long> setB = new();

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
        a.PrepareForReading();
        b.PrepareForReading();
        RoaringBitmap andResult = And(ctx, a, b);
        HashSet<long> expectedAnd = new(setA);
        expectedAnd.IntersectWith(setB);
        Assert.Equal(expectedAnd.Count, andResult.Count);
        foreach (long v in expectedAnd)
            Assert.True(andResult.Contains(v));
        andResult.Dispose();

        // OR
        RoaringBitmap orResult = Or(ctx, a, b);
        HashSet<long> expectedOr = new(setA);
        expectedOr.UnionWith(setB);
        Assert.Equal(expectedOr.Count, orResult.Count);
        orResult.Dispose();

        // ANDNOT
        RoaringBitmap andNotResult = AndNot(ctx, a, b);
        HashSet<long> expectedAndNot = new(setA);
        expectedAndNot.ExceptWith(setB);
        Assert.Equal(expectedAndNot.Count, andNotResult.Count);
        andNotResult.Dispose();
        a.Dispose();
        b.Dispose();
    }

    #endregion

    #region WP1: New methods (Clear, Count, AddRange, OrWith, RepairAfterLazy)

    [RavenFact(RavenTestCategory.Corax)]
    public void Count_ReturnsCorrectValue()
    {
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        RoaringBitmap bitmap = new(bsc);
        Assert.Equal(0, bitmap.Count);

        for (int i = 0; i < 1000; i++)
            bitmap.Add(i);
        bitmap.PrepareForReading();
        Assert.Equal(1000, bitmap.Count);

        // Verify after clear
        bitmap.Clear();
        Assert.Equal(0, bitmap.Count);
        bitmap.Dispose();
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void Clear_ResetsToEmpty()
    {
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        RoaringBitmap bitmap = new(bsc);
        for (int i = 0; i < 100_000; i++)
            bitmap.Add(i);
        bitmap.PrepareForReading();
        Assert.True(bitmap.Count > 0);

        bitmap.Clear();
        Assert.Equal(0, bitmap.Count);
        Assert.True(bitmap.IsEmpty);
        Assert.Equal(0, bitmap.ContainerCount);

        // Can reuse after clear
        bitmap.Add(42);
        bitmap.PrepareForReading();
        Assert.Equal(1, bitmap.Count);
        Assert.True(bitmap.Contains(42));
        bitmap.Dispose();
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void Clear_RepeatedClearAndReuse()
    {
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        RoaringBitmap bitmap = new(bsc);

        for (int round = 0; round < 10; round++)
        {
            for (int i = round * 1000; i < (round + 1) * 1000; i++)
                bitmap.Add(i);
            bitmap.PrepareForReading();
            Assert.Equal(1000, bitmap.Count);
            bitmap.Clear();
            Assert.Equal(0, bitmap.Count);
        }
        bitmap.Dispose();
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void AddRange_SortedValues()
    {
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        RoaringBitmap bitmap = new(bsc);
        var values = new long[10_000];
        for (int i = 0; i < values.Length; i++)
            values[i] = i * 3; // sparse but sorted
        bitmap.AddRange(values);
        bitmap.PrepareForReading();

        Assert.Equal(10_000, bitmap.Count);
        Assert.True(bitmap.Contains(0));
        Assert.True(bitmap.Contains(3));
        Assert.True(bitmap.Contains(29997));
        Assert.False(bitmap.Contains(1));
        Assert.False(bitmap.Contains(2));
        bitmap.Dispose();
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void AddRange_MatchesIndividualAdds()
    {
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        var values = new long[50_000];
        var rng = new Random(42);
        var set = new HashSet<long>();
        for (int i = 0; i < values.Length; i++)
        {
            long v = rng.NextInt64(0, 1_000_000);
            values[i] = v;
            set.Add(v);
        }
        Array.Sort(values);
        // Remove duplicates for AddRange (expects sorted, unique not required but let's test)
        var unique = new List<long>();
        long prev = -1;
        foreach (var v in values)
        {
            if (v != prev) unique.Add(v);
            prev = v;
        }

        RoaringBitmap bitmapRange = new(bsc);
        bitmapRange.AddRange(unique.ToArray());
        bitmapRange.PrepareForReading();

        RoaringBitmap bitmapSingle = new(bsc);
        foreach (var v in set)
            bitmapSingle.Add(v);
        bitmapSingle.PrepareForReading();

        Assert.Equal(bitmapSingle.Count, bitmapRange.Count);

        // Spot-check containment
        int checked_ = 0;
        foreach (var v in set)
        {
            if (++checked_ > 100) break;
            Assert.True(bitmapRange.Contains(v));
        }

        bitmapRange.Dispose();
        bitmapSingle.Dispose();
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void AddRange_EmptySpan()
    {
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        RoaringBitmap bitmap = new(bsc);
        bitmap.AddRange(ReadOnlySpan<long>.Empty);
        Assert.True(bitmap.IsEmpty);
        bitmap.Dispose();
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void OrWith_ComputesCorrectCardinalityAfterRepair()
    {
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        RoaringBitmap a = new(bsc);
        RoaringBitmap b = new(bsc);
        for (int i = 0; i < 10_000; i++) a.Add(i * 2);     // evens
        for (int i = 0; i < 10_000; i++) b.Add(i * 2 + 1); // odds
        a.PrepareForReading();
        b.PrepareForReading();

        RoaringBitmap result = a.Clone();
        RoaringBitmap bClone = b.Clone();
        result.OrWith(ref bClone);

        Assert.Equal(20_000, result.Count);

        result.Dispose();
        a.Dispose();
        b.Dispose();
        bClone.Dispose();
    }

    #endregion

    #region AndWith Edge Cases (replacement for RavenDB-21052 SortHelper.FindMatches)

    [RavenFact(RavenTestCategory.Corax)]
    public void AndWithGapBetweenContainers()
    {
        // Two bitmaps with no overlapping containers — gap between key ranges.
        // Equivalent to the old FindMatches gap detection test.
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        RoaringBitmap a = new(ctx);
        RoaringBitmap b = new(ctx);

        // a: container 0 (values 8, 9)
        a.Add(8);
        a.Add(9);
        // b: container 1 (values 65546, 65547)
        b.Add(65546);
        b.Add(65547);

        a.PrepareForReading();
        b.PrepareForReading();
        RoaringBitmap result = And(ctx, a, b);
        Assert.Equal(0, result.Count);
        Assert.True(result.IsEmpty);
        result.Dispose();

        // Reverse order: a entirely after b
        RoaringBitmap result2 = And(ctx, b, a);
        Assert.Equal(0, result2.Count);
        Assert.True(result2.IsEmpty);
        result2.Dispose();

        a.Dispose();
        b.Dispose();
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void AndWithMatchAtFirstElement()
    {
        // Match is the first element of the right array.
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        RoaringBitmap a = new(ctx);
        RoaringBitmap b = new(ctx);

        a.Add(10);
        a.Add(20);
        // b starts at 10
        b.Add(10);
        b.Add(11);
        b.Add(13);
        b.Add(15);

        a.PrepareForReading();
        b.PrepareForReading();
        RoaringBitmap result = And(ctx, a, b);
        Assert.Equal(1, result.Count);
        Assert.True(result.Contains(10));
        Assert.False(result.Contains(20));
        result.Dispose();
        a.Dispose();
        b.Dispose();
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void AndWithMatchAtLastElement()
    {
        // Match is the last element of the right array.
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        RoaringBitmap a = new(ctx);
        RoaringBitmap b = new(ctx);

        a.Add(15);
        a.Add(20);
        // b ends at 15
        b.Add(10);
        b.Add(11);
        b.Add(13);
        b.Add(15);

        a.PrepareForReading();
        b.PrepareForReading();
        RoaringBitmap result = And(ctx, a, b);
        Assert.Equal(1, result.Count);
        Assert.True(result.Contains(15));
        Assert.False(result.Contains(20));
        result.Dispose();
        a.Dispose();
        b.Dispose();
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void AndWithSubset()
    {
        // One bitmap is a subset of the other — AND should return the subset.
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        RoaringBitmap small = new(ctx);
        RoaringBitmap large = new(ctx);

        small.Add(5);
        small.Add(10);
        small.Add(15);
        for (int i = 0; i < 20; i++)
            large.Add(i);

        small.PrepareForReading();
        large.PrepareForReading();
        RoaringBitmap result = And(ctx, large, small);
        Assert.Equal(3, result.Count);
        Assert.True(result.Contains(5));
        Assert.True(result.Contains(10));
        Assert.True(result.Contains(15));
        result.Dispose();
        small.Dispose();
        large.Dispose();
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void AndWithEmptyBitmap()
    {
        // AND with empty bitmap should produce empty result.
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        RoaringBitmap a = new(ctx);
        RoaringBitmap empty = new(ctx);

        for (int i = 0; i < 100; i++)
            a.Add(i);

        a.PrepareForReading();
        RoaringBitmap result = And(ctx, a, empty);
        Assert.Equal(0, result.Count);
        Assert.True(result.IsEmpty);
        result.Dispose();
        a.Dispose();
        empty.Dispose();
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void AndWithContainerBoundaryValues()
    {
        // Test values at container boundaries: first (0) and last (65535) within a container.
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        RoaringBitmap a = new(ctx);
        RoaringBitmap b = new(ctx);

        a.Add(0);       // first value in container 0
        a.Add(65535);   // last value in container 0
        a.Add(65536);   // first value in container 1

        b.Add(0);
        b.Add(65535);
        b.Add(131072);  // first value in container 2 (no match with a)

        a.PrepareForReading();
        b.PrepareForReading();
        RoaringBitmap result = And(ctx, a, b);
        Assert.Equal(2, result.Count);
        Assert.True(result.Contains(0));
        Assert.True(result.Contains(65535));
        Assert.False(result.Contains(65536));
        Assert.False(result.Contains(131072));
        result.Dispose();
        a.Dispose();
        b.Dispose();
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void AndWithCrossContainerTypes()
    {
        // Force different container types: sparse (Array) vs dense (Bitmap) vs Range.
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);

        // Sparse array container: 3 values
        RoaringBitmap sparse = new(ctx);
        sparse.Add(100);
        sparse.Add(200);
        sparse.Add(300);

        // Dense bitmap container: 5000 values (triggers Bitmap type)
        RoaringBitmap dense = new(ctx);
        for (int i = 0; i < 5000; i++)
            dense.Add(i);

        // Range container: sequential 0..999
        RoaringBitmap range = new(ctx);
        for (int i = 0; i < 1000; i++)
            range.Add(i);

        sparse.PrepareForReading();
        dense.PrepareForReading();
        range.PrepareForReading();

        // Array AND Bitmap
        RoaringBitmap r1 = And(ctx, sparse, dense);
        Assert.Equal(3, r1.Count);
        Assert.True(r1.Contains(100));
        Assert.True(r1.Contains(200));
        Assert.True(r1.Contains(300));
        r1.Dispose();

        // Array AND Range
        RoaringBitmap r2 = And(ctx, sparse, range);
        Assert.Equal(3, r2.Count);
        r2.Dispose();

        // Bitmap AND Range
        RoaringBitmap r3 = And(ctx, dense, range);
        Assert.Equal(1000, r3.Count);
        r3.Dispose();

        sparse.Dispose();
        dense.Dispose();
        range.Dispose();
    }

    #endregion

    #region SIMD Boundary Tests (replacement for RavenDB-21471 vectorized AND out-of-bounds)

    [RavenFact(RavenTestCategory.Corax)]
    public void SimdAndWithSmallArraysDoesNotProduceFalsePositives()
    {
        // Small arrays (1-2 elements) where SIMD Vector256 reads past live data.
        // The padding in the over-allocated buffer may contain stale values.
        // Verify no false positives from phantom matches in padding.
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        RoaringBitmap a = new(ctx);
        RoaringBitmap b = new(ctx);

        // a: 2 values high in the container range
        a.Add(5708);
        a.Add(5709);

        // b: 6 values low in the container range — no overlap with a
        b.Add(763);
        b.Add(764);
        b.Add(941);
        b.Add(942);
        b.Add(946);
        b.Add(966);

        a.PrepareForReading();
        b.PrepareForReading();
        RoaringBitmap result = And(ctx, a, b);
        Assert.Equal(0, result.Count);
        Assert.True(result.IsEmpty);
        result.Dispose();
        a.Dispose();
        b.Dispose();
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void SimdAndWithSingleElementArrays()
    {
        // Minimal arrays: 1 element each. SIMD reads a full vector (16 ushorts)
        // from a buffer that has only 1 live value.
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);

        // No match
        RoaringBitmap a = new(ctx);
        RoaringBitmap b = new(ctx);
        a.Add(42);
        b.Add(99);
        a.PrepareForReading();
        b.PrepareForReading();
        RoaringBitmap r1 = And(ctx, a, b);
        Assert.Equal(0, r1.Count);
        r1.Dispose();

        // Match
        RoaringBitmap c = new(ctx);
        c.Add(42);
        c.PrepareForReading();
        RoaringBitmap r2 = And(ctx, a, c);
        Assert.Equal(1, r2.Count);
        Assert.True(r2.Contains(42));
        r2.Dispose();

        a.Dispose();
        b.Dispose();
        c.Dispose();
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void SimdAndNotWithSmallArraysDoesNotProduceFalseExclusions()
    {
        // ANDNOT with small arrays — verify no false exclusions from padding.
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        RoaringBitmap a = new(ctx);
        RoaringBitmap b = new(ctx);

        // a has values; b has non-overlapping values
        a.Add(100);
        a.Add(200);
        a.Add(300);
        b.Add(50);
        b.Add(150);

        a.PrepareForReading();
        b.PrepareForReading();
        RoaringBitmap result = AndNot(ctx, a, b);
        // All of a's values should survive — none match b
        Assert.Equal(3, result.Count);
        Assert.True(result.Contains(100));
        Assert.True(result.Contains(200));
        Assert.True(result.Contains(300));
        result.Dispose();
        a.Dispose();
        b.Dispose();
    }

    #endregion

    #region Exhaustive Primitive Compatibility (replacement for deleted Primitives.cs 256^3 test)

    /// <summary>Verify AND/OR/ANDNOT produce correct results for every combination of
    /// container types (empty, sparse array, dense bitmap, range, cross-container).
    /// Uses a reference HashSet to compare against RoaringBitmap set operations.</summary>
    [RavenFact(RavenTestCategory.Corax)]
    public void ExhaustiveSetOpCompatibility()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        var rng = new Random(42);

        // Build bitmaps with different container configurations
        var configs = new (string Name, long[] Values)[]
        {
            ("Empty", []),
            ("SingleValue", [42]),
            ("SparseArray_Small", GenerateValues(rng, 10, 0, 65535)),
            ("SparseArray_Medium", GenerateValues(rng, 500, 0, 65535)),
            ("DenseBitmap", GenerateValues(rng, 5000, 0, 65535)),
            ("FullRange", Enumerable.Range(0, 1000).Select(x => (long)x).ToArray()),
            ("TwoContainers_Sparse", GenerateValues(rng, 100, 0, 131071)),
            ("TwoContainers_Dense", GenerateValues(rng, 8000, 0, 131071)),
            ("HighValues", GenerateValues(rng, 200, 100000, 200000)),
            ("Scattered", [0L, 100, 65535, 65536, 131071, 200000]),
        };

        int failures = 0;
        foreach (var left in configs)
        {
            foreach (var right in configs)
            {
                // AND
                if (VerifySetOp("AND", left, right, ctx,
                    (a, b) => { var r = And(ctx, a, b); return r; },
                    (a, b) => new HashSet<long>(a.Intersect(b))) == false)
                    failures++;

                // OR
                if (VerifySetOp("OR", left, right, ctx,
                    (a, b) => Or(ctx, a, b),
                    (a, b) => new HashSet<long>(a.Union(b))) == false)
                    failures++;

                // ANDNOT
                if (VerifySetOp("ANDNOT", left, right, ctx,
                    (a, b) => AndNot(ctx, a, b),
                    (a, b) => new HashSet<long>(a.Except(b))) == false)
                    failures++;
            }
        }

        Assert.Equal(0, failures);
    }

    private static long[] GenerateValues(Random rng, int count, long min, long max)
    {
        var set = new HashSet<long>();
        while (set.Count < count)
            set.Add(min + (long)(rng.NextDouble() * (max - min)));
        var arr = set.ToArray();
        Array.Sort(arr);
        return arr;
    }

    private static bool VerifySetOp(string opName, (string Name, long[] Values) left, (string Name, long[] Values) right,
        ByteStringContext ctx,
        Func<RoaringBitmap, RoaringBitmap, RoaringBitmap> bitmapOp,
        Func<long[], long[], HashSet<long>> referenceOp)
    {
        RoaringBitmap a = new(ctx);
        RoaringBitmap b = new(ctx);
        foreach (long v in left.Values) a.Add(v);
        foreach (long v in right.Values) b.Add(v);
        a.PrepareForReading();
        b.PrepareForReading();

        RoaringBitmap result;
        try
        {
            result = bitmapOp(a, b);
        }
        catch (Exception ex)
        {
            Assert.Fail($"{opName}({left.Name}, {right.Name}) threw: {ex.Message}");
            return false;
        }

        var expected = referenceOp(left.Values, right.Values);
        bool ok = true;

        if (result.Count != expected.Count)
        {
            Assert.Fail($"{opName}({left.Name}, {right.Name}): count {result.Count} != expected {expected.Count}");
            ok = false;
        }

        foreach (long v in expected)
        {
            if (result.Contains(v) == false)
            {
                Assert.Fail($"{opName}({left.Name}, {right.Name}): missing value {v}");
                ok = false;
                break;
            }
        }

        result.Dispose();
        a.Dispose();
        b.Dispose();
        return ok;
    }

    [RavenTheory(RavenTestCategory.Corax)]
    [InlineDataWithRandomSeed]
    public void ExhaustiveSetOpCompatibility_RandomSeed(int seed)
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        var rng = new Random(seed);

        var configs = new (string Name, long[] Values)[]
        {
            ("Empty", []),
            ("SingleValue", [rng.NextInt64(0, 100000)]),
            ("SparseArray", GenerateValues(rng, 10, 0, 65535)),
            ("MediumArray", GenerateValues(rng, 500, 0, 65535)),
            ("DenseBitmap", GenerateValues(rng, 5000, 0, 65535)),
            ("FullRange", Enumerable.Range(rng.Next(0, 1000), 1000).Select(x => (long)x).ToArray()),
            ("TwoContainers", GenerateValues(rng, 200, 0, 131071)),
            ("HighValues", GenerateValues(rng, 200, 100000, 200000)),
        };

        int failures = 0;
        foreach (var left in configs)
        foreach (var right in configs)
        {
            if (VerifySetOp("AND", left, right, ctx, (a, b) => And(ctx, a, b), (a, b) => new HashSet<long>(a.Intersect(b))) == false) failures++;
            if (VerifySetOp("OR", left, right, ctx, (a, b) => Or(ctx, a, b), (a, b) => new HashSet<long>(a.Union(b))) == false) failures++;
            if (VerifySetOp("ANDNOT", left, right, ctx, (a, b) => AndNot(ctx, a, b), (a, b) => new HashSet<long>(a.Except(b))) == false) failures++;
        }

        Assert.Equal(0, failures);
    }

    /// <summary>Targeted regression test for the (ArrayUnsorted, Range) dangling stack pointer bug.
    /// LazyOrContainerInPlace materialized Range into a stack buffer, then (Array, Bitmap) stole
    /// the pointer — dangling after stack unwind. Fixed by converting Array to heap Bitmap first.</summary>
    [RavenFact(RavenTestCategory.Corax)]
    public void OrWithArrayAndRange_NoDanglingPointer()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);

        // Sparse array with values OUTSIDE the range (different container positions)
        RoaringBitmap sparse = new(ctx);
        sparse.Add(5000);
        sparse.Add(10000);
        sparse.Add(17209); // the value that was lost in the original bug
        sparse.Add(33614);
        sparse.Add(50000);

        // Range container: sequential 0..999
        RoaringBitmap range = new(ctx);
        for (int i = 0; i < 1000; i++) range.Add(i);

        sparse.PrepareForReading();
        range.PrepareForReading();

        // OR: must include all values from both
        RoaringBitmap result = Or(ctx, sparse, range);
        Assert.Equal(1005, result.Count); // 1000 from range + 5 from sparse
        Assert.True(result.Contains(5000));
        Assert.True(result.Contains(17209));
        Assert.True(result.Contains(50000));
        Assert.True(result.Contains(0));
        Assert.True(result.Contains(999));
        result.Dispose();

        // Reverse order: range OR sparse
        RoaringBitmap result2 = Or(ctx, range, sparse);
        Assert.Equal(1005, result2.Count);
        Assert.True(result2.Contains(17209));
        result2.Dispose();

        sparse.Dispose();
        range.Dispose();
    }

    #endregion
}
