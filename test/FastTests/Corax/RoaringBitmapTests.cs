using System;
using System.Collections.Generic;
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
    private static RoaringBitmap And(ByteStringContext ctx, RoaringBitmap a, RoaringBitmap b)
    {
        var result = a.Clone();
        result.AndWith(ref b);
        result.PrepareForReading();
        return result;
    }

    private static RoaringBitmap Or(ByteStringContext ctx, RoaringBitmap a, RoaringBitmap b)
    {
        var result = a.Clone();
        var bClone = b.Clone();
        result.LazyOrWith(ref bClone);
        result.PrepareForReading();
        bClone.Dispose();
        return result;
    }

    private static RoaringBitmap AndNot(ByteStringContext ctx, RoaringBitmap a, RoaringBitmap b)
    {
        var result = a.Clone();
        result.AndNotWith(ref b);
        result.PrepareForReading();
        return result;
    }


    [RavenFact(RavenTestCategory.Corax)]
    public void CanAddAndContainsSingleValue()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        using var bitmap = new RoaringBitmap(ctx);
        bitmap.Add(42);
        bitmap.PrepareForReading();
        Assert.True(bitmap.Contains(42));
        Assert.False(bitmap.Contains(43));
        Assert.Equal(1, bitmap.Count);
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void CanAddManyValuesInSameContainer()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        using var bitmap = new RoaringBitmap(ctx);
        for (int i = 0; i < 1000; i++)
            bitmap.Add(i);

        for (int i = 0; i < 1000; i++)
            Assert.True(bitmap.Contains(i));

        bitmap.PrepareForReading();
        Assert.False(bitmap.Contains(1000));
        Assert.Equal(1000, bitmap.Count);
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void CanAddValuesAcrossMultipleContainers()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        using var bitmap = new RoaringBitmap(ctx);
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
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void ArrayToBitmapConversionOnThreshold()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        using var bitmap = new RoaringBitmap(ctx);
        // Add more than 4096 values to trigger array->bitmap conversion
        for (int i = 0; i < 5000; i++)
            bitmap.Add(i);

        bitmap.PrepareForReading();
        Assert.Equal(5000, bitmap.Count);

        for (int i = 0; i < 5000; i++)
            Assert.True(bitmap.Contains(i));
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void FullContainerAsRange()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        using var bitmap = new RoaringBitmap(ctx);
        // Fill an entire container — should become Range with count=65536
        for (int i = 0; i < 65536; i++)
            bitmap.Add(i);

        bitmap.PrepareForReading();
        Assert.Equal(65536, bitmap.Count);
        Assert.True(bitmap.Contains(0));
        Assert.True(bitmap.Contains(65535));
        Assert.False(bitmap.Contains(65536));
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void DuplicateAddIsIdempotent()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        using var bitmap = new RoaringBitmap(ctx);
        bitmap.Add(42);
        bitmap.Add(42);
        bitmap.Add(42);
        bitmap.PrepareForReading();
        Assert.Equal(1, bitmap.Count);
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void CanHandle64BitValues()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        using var bitmap = new RoaringBitmap(ctx);
        long largeValue = (long)int.MaxValue + 100;
        bitmap.Add(largeValue);
        bitmap.PrepareForReading();
        Assert.True(bitmap.Contains(largeValue));
        Assert.False(bitmap.Contains(largeValue + 1));
        Assert.Equal(1, bitmap.Count);
    }

    #region Set Operations

    [RavenFact(RavenTestCategory.Corax)]
    public void AndIntersection()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        using var a = new RoaringBitmap(ctx);
        using var b = new RoaringBitmap(ctx);
        for (int i = 0; i < 1000; i++)
            a.Add(i);
        for (int i = 500; i < 1500; i++)
            b.Add(i);

        a.PrepareForReading();
        b.PrepareForReading();
        var result = And(ctx, a, b);
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
        }
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void OrUnion()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        using var a = new RoaringBitmap(ctx);
        using var b = new RoaringBitmap(ctx);

        for (int i = 0; i < 1000; i++)
            a.Add(i);
        for (int i = 500; i < 1500; i++)
            b.Add(i);

        a.PrepareForReading();
        b.PrepareForReading();
        var result = Or(ctx, a, b);

        Assert.Equal(1500, result.Count);
        for (int i = 0; i < 1500; i++)
            Assert.True(result.Contains(i));
        Assert.False(result.Contains(1500));
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void AndNotDifference()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        using var a = new RoaringBitmap(ctx);
        using var b = new RoaringBitmap(ctx);
        for (int i = 0; i < 1000; i++)
            a.Add(i);
        for (int i = 500; i < 1500; i++)
            b.Add(i);

        a.PrepareForReading();
        b.PrepareForReading();
        var result = AndNot(ctx, a, b);
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
        }
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void AndWithDisjointBitmaps()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        using var a = new RoaringBitmap(ctx);
        using var b = new RoaringBitmap(ctx);
        for (int i = 0; i < 100; i++)
            a.Add(i);
        for (int i = 200; i < 300; i++)
            b.Add(i);

        a.PrepareForReading();
        b.PrepareForReading();
        var result = And(ctx, a, b);
        try
        {
            Assert.Equal(0, result.Count);
            Assert.True(result.IsEmpty);
        }
        finally
        {
            result.Dispose();
        }
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void SetOpsWithDenseBitmapContainers()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        using var a = new RoaringBitmap(ctx);
        using var b = new RoaringBitmap(ctx);
        // Dense enough to be bitmap containers (>4096 each)
        for (int i = 0; i < 10000; i++)
            a.Add(i * 2); // evens 0..19998
        for (int i = 0; i < 10000; i++)
            b.Add(i * 2 + 1); // odds 1..19999

        // AND should be empty (no overlap)
        var andResult = And(ctx, a, b);
        Assert.Equal(0, andResult.Count);
        andResult.Dispose();

        // OR should have all 20000
        a.PrepareForReading();
        b.PrepareForReading();
        var orResult = Or(ctx, a, b);
        Assert.Equal(20000, orResult.Count);
        orResult.Dispose();
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void SetOpsWithMixedContainerTypes()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        using var sparse = new RoaringBitmap(ctx);
        using var dense = new RoaringBitmap(ctx);
        // Sparse (array container): 100 values
        for (int i = 0; i < 100; i++)
            sparse.Add(i * 10);

        // Dense (bitmap container): 5000 values
        for (int i = 0; i < 5000; i++)
            dense.Add(i);

        // AND: sparse values that exist in dense
        sparse.PrepareForReading();
        dense.PrepareForReading();
        var andResult = And(ctx, sparse, dense);
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
        }
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void SetOpsAcrossMultipleContainers()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        using var a = new RoaringBitmap(ctx);
        using var b = new RoaringBitmap(ctx);
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
        var orResult = Or(ctx, a, b);
        try
        {
            Assert.Equal(200 + 200 + 100, orResult.Count); // container0(200) + container1(200 union) + container2(100)
        }
        finally
        {
            orResult.Dispose();
        }

        var andResult = And(ctx, a, b);
        try
        {
            Assert.Equal(100, andResult.Count); // only container1 intersection
        }
        finally
        {
            andResult.Dispose();
        }
    }

    #endregion

    #region Iterator Tests

    [RavenFact(RavenTestCategory.Corax)]
    public void IteratorReturnsAllValuesInOrder()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        using var bitmap = new RoaringBitmap(ctx);
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
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void IteratorWorksWithSmallBuffer()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        using var bitmap = new RoaringBitmap(ctx);
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
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void IteratorHandlesBitmapContainers()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        using var bitmap = new RoaringBitmap(ctx);
        // Dense enough for bitmap container
        for (int i = 0; i < 5000; i++)
            bitmap.Add(i);

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

        Assert.Equal(5000, allValues.Count);
        for (int i = 0; i < 5000; i++)
            Assert.Equal(i, allValues[i]);
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void IteratorHandlesFullContainer()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        using var bitmap = new RoaringBitmap(ctx);
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
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void IteratorHandlesMultipleContainers()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        using var bitmap = new RoaringBitmap(ctx);
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
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void EmptyBitmapIteratorReturnsZero()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        using var bitmap = new RoaringBitmap(ctx);
        var iterator = bitmap.GetIterator();
        Span<long> buffer = stackalloc long[10];
        bitmap.PrepareForReading();
        int count = bitmap.Fill(buffer, ref iterator);
        Assert.Equal(0, count);
    }

    #endregion

    #region Range Container Tests

    [RavenFact(RavenTestCategory.Corax)]
    public void RangeContainerCreatedForSequentialAdds()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        using var bitmap = new RoaringBitmap(ctx);
        // Sequential adds from 0 should create a Range container
        for (int i = 0; i < 1000; i++)
            bitmap.Add(i);

        bitmap.PrepareForReading();
        Assert.Equal(1000, bitmap.Count);
        for (int i = 0; i < 1000; i++)
            Assert.True(bitmap.Contains(i));
        Assert.False(bitmap.Contains(1000));
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void RangeContainerFullContainer()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        using var bitmap = new RoaringBitmap(ctx);
        // Fill entire container sequentially — should be Range with count=65536
        for (int i = 0; i < 65536; i++)
            bitmap.Add(i);

        bitmap.PrepareForReading();
        Assert.Equal(65536, bitmap.Count);
        Assert.True(bitmap.Contains(0));
        Assert.True(bitmap.Contains(65535));
        Assert.False(bitmap.Contains(65536));
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void RangeContainerIteratesCorrectly()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        using var bitmap = new RoaringBitmap(ctx);
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
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void SetOpsWithRangeContainers()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        using var a = new RoaringBitmap(ctx);
        using var b = new RoaringBitmap(ctx);
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
        var andResult = And(ctx, a, b);
        HashSet<long> expectedAnd = new(setA);
        expectedAnd.IntersectWith(setB);
        Assert.Equal(expectedAnd.Count, andResult.Count);
        andResult.Dispose();

        // OR
        var orResult = Or(ctx, a, b);
        HashSet<long> expectedOr = new(setA);
        expectedOr.UnionWith(setB);
        Assert.Equal(expectedOr.Count, orResult.Count);
        orResult.Dispose();

        // ANDNOT
        var andNotResult = AndNot(ctx, a, b);
        HashSet<long> expectedAndNot = new(setA);
        expectedAndNot.ExceptWith(setB);
        Assert.Equal(expectedAndNot.Count, andNotResult.Count);
        andNotResult.Dispose();
    }


    [RavenFact(RavenTestCategory.Corax)]
    public void RangeContainerGapConvertsToArrayOrBitmap()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        using var bitmap = new RoaringBitmap(ctx);
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
    }

    #endregion

    #region Stress / Large Scale Tests

    [RavenFact(RavenTestCategory.Corax)]
    public void LargeScaleAddAndIterate()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        using var bitmap = new RoaringBitmap(ctx);
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
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void SetOpsCorrectnessAgainstHashSet()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        using var a = new RoaringBitmap(ctx);
        using var b = new RoaringBitmap(ctx);
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
        var andResult = And(ctx, a, b);
        HashSet<long> expectedAnd = new(setA);
        expectedAnd.IntersectWith(setB);
        Assert.Equal(expectedAnd.Count, andResult.Count);
        foreach (long v in expectedAnd)
            Assert.True(andResult.Contains(v));
        andResult.Dispose();

        // OR
        var orResult = Or(ctx, a, b);
        HashSet<long> expectedOr = new(setA);
        expectedOr.UnionWith(setB);
        Assert.Equal(expectedOr.Count, orResult.Count);
        orResult.Dispose();

        // ANDNOT
        var andNotResult = AndNot(ctx, a, b);
        HashSet<long> expectedAndNot = new(setA);
        expectedAndNot.ExceptWith(setB);
        Assert.Equal(expectedAndNot.Count, andNotResult.Count);
        andNotResult.Dispose();
    }

    #endregion

    #region WP1: New methods (Clear, Count, AddRange, LazyOrWith, RepairAfterLazy)

    [RavenFact(RavenTestCategory.Corax)]
    public void Count_ReturnsCorrectValue()
    {
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        using var bitmap = new RoaringBitmap(bsc);
        Assert.Equal(0, bitmap.Count);

        for (int i = 0; i < 1000; i++)
            bitmap.Add(i);
        bitmap.PrepareForReading();
        Assert.Equal(1000, bitmap.Count);

        // Verify after clear
        bitmap.Clear();
        Assert.Equal(0, bitmap.Count);
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void Clear_ResetsToEmpty()
    {
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        var bitmap = new RoaringBitmap(bsc);
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
        var bitmap = new RoaringBitmap(bsc);

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
        var bitmap = new RoaringBitmap(bsc);
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

        var bitmapRange = new RoaringBitmap(bsc);
        bitmapRange.AddRange(unique.ToArray());
        bitmapRange.PrepareForReading();

        var bitmapSingle = new RoaringBitmap(bsc);
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
        var bitmap = new RoaringBitmap(bsc);
        bitmap.AddRange(ReadOnlySpan<long>.Empty);
        Assert.True(bitmap.IsEmpty);
        bitmap.Dispose();
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void LazyOrWith_ComputesCorrectCardinalityAfterRepair()
    {
        using var bsc = new ByteStringContext(SharedMultipleUseFlag.None);
        var a = new RoaringBitmap(bsc);
        var b = new RoaringBitmap(bsc);
        for (int i = 0; i < 10_000; i++) a.Add(i * 2);     // evens
        for (int i = 0; i < 10_000; i++) b.Add(i * 2 + 1); // odds
        a.PrepareForReading();
        b.PrepareForReading();

        var result = a.Clone();
        var bClone = b.Clone();
        result.LazyOrWith(ref bClone);
        result.RepairAfterLazy();

        Assert.Equal(20_000, result.Count);

        result.Dispose();
        a.Dispose();
        b.Dispose();
        bClone.Dispose();
    }

    #endregion
}
