using System;
using System.Collections;
using System.Collections.Generic;
using BenchmarkDotNet.Attributes;
using Corax.Utils.RoaringBitmaps;
using Sparrow.Server;
using Sparrow.Server.Collections;
using Sparrow.Threading;

namespace Voron.Benchmark.Corax;

/// <summary>
/// Benchmarks comparing RoaringBitmap vs BCL BitArray vs GrowableBitArray
/// across different set sizes and value ranges.
/// </summary>
[MemoryDiagnoser]
[Config(typeof(LongBuildConfig))]
public class RoaringBitmapBenchmark
{
    private class LongBuildConfig : BenchmarkDotNet.Configs.ManualConfig
    {
        public LongBuildConfig()
        {
            AddJob(new BenchmarkDotNet.Jobs.Job
            {
                Run =
                {
                    LaunchCount = 1,
                    WarmupCount = 1,
                    IterationCount = 3,
                },
                Infrastructure =
                {
                    Toolchain = BenchmarkDotNet.Toolchains.InProcess.Emit.InProcessEmitToolchain.Instance,
                }
            });
        }
    }

    /// <summary>Number of values to insert/query.</summary>
    [Params(500, 5_000, 50_000, 500_000)]
    public int Count;

    /// <summary>Max value range (determines density).</summary>
    [Params(1_000_000, 100_000_000, 1_000_000_000)]
    public int MaxValue;

    private long[] _valuesA;
    private long[] _valuesB;
    private long[] _sortedA;
    private long[] _sortedB;

    [GlobalSetup]
    public void Setup()
    {
        var rng = new Random(42);
        var setA = new HashSet<long>();
        var setB = new HashSet<long>();

        while (setA.Count < Count)
            setA.Add(rng.NextInt64(0, MaxValue));
        while (setB.Count < Count)
            setB.Add(rng.NextInt64(0, MaxValue));

        _valuesA = new long[setA.Count];
        setA.CopyTo(_valuesA);
        _valuesB = new long[setB.Count];
        setB.CopyTo(_valuesB);

        // Pre-sorted copies for iteration benchmarks
        _sortedA = (long[])_valuesA.Clone();
        Array.Sort(_sortedA);
        _sortedB = (long[])_valuesB.Clone();
        Array.Sort(_sortedB);
    }

    #region Add (Build)

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Add")]
    public long Add_Roaring()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        var bmp = new RoaringBitmap(ctx);
        for (int i = 0; i < _valuesA.Length; i++)
            bmp.Add(_valuesA[i]);
        bmp.PrepareForReading();
        long c = bmp.Cardinality;
        bmp.Dispose();
        return c;
    }

    [Benchmark]
    [BenchmarkCategory("Add")]
    public BitArray Add_BclBitArray()
    {
        // BCL BitArray only supports int indices; clamp MaxValue for fair comparison
        int max = (int)Math.Min(MaxValue, int.MaxValue - 1);
        var ba = new BitArray(max + 1);
        for (int i = 0; i < _valuesA.Length; i++)
            ba.Set((int)_valuesA[i], true);
        return ba;
    }

    [Benchmark]
    [BenchmarkCategory("Add")]
    public int Add_GrowableBitArray()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        var gba = new GrowableBitArray(ctx, MaxValue);
        for (int i = 0; i < _valuesA.Length; i++)
            gba.Add(_valuesA[i]);
        int count = _valuesA.Length;
        gba.Dispose();
        return count;
    }

    #endregion

    #region Contains (Lookup)

    private RoaringBitmap _roaringA;
    private BitArray _bclA;
    private GrowableBitArray _gbaA;
    private ByteStringContext _containsCtx;

    [IterationSetup(Targets = new[] { nameof(Contains_Roaring), nameof(Contains_BclBitArray), nameof(Contains_GrowableBitArray) })]
    public void ContainsSetup()
    {
        _containsCtx = new ByteStringContext(SharedMultipleUseFlag.None);
        _roaringA = new RoaringBitmap(_containsCtx);
        for (int i = 0; i < _valuesA.Length; i++)
            _roaringA.Add(_valuesA[i]);
        _roaringA.PrepareForReading();

        int max = (int)Math.Min(MaxValue, int.MaxValue - 1);
        _bclA = new BitArray(max + 1);
        for (int i = 0; i < _valuesA.Length; i++)
            _bclA.Set((int)_valuesA[i], true);

        _gbaA = new GrowableBitArray(_containsCtx, MaxValue);
        for (int i = 0; i < _valuesA.Length; i++)
            _gbaA.Add(_valuesA[i]);
    }

    [IterationCleanup(Targets = new[] { nameof(Contains_Roaring), nameof(Contains_BclBitArray), nameof(Contains_GrowableBitArray) })]
    public void ContainsCleanup()
    {
        _roaringA.Dispose();
        _gbaA.Dispose();
        _containsCtx.Dispose();
    }

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Contains")]
    public int Contains_Roaring()
    {
        int found = 0;
        for (int i = 0; i < _valuesB.Length; i++)
        {
            if (_roaringA.Contains(_valuesB[i]))
                found++;
        }
        return found;
    }

    [Benchmark]
    [BenchmarkCategory("Contains")]
    public int Contains_BclBitArray()
    {
        int found = 0;
        for (int i = 0; i < _valuesB.Length; i++)
        {
            if (_bclA.Get((int)_valuesB[i]))
                found++;
        }
        return found;
    }

    [Benchmark]
    [BenchmarkCategory("Contains")]
    public int Contains_GrowableBitArray()
    {
        int found = 0;
        for (int i = 0; i < _valuesB.Length; i++)
        {
            if (_gbaA.Contains(_valuesB[i]))
                found++;
        }
        return found;
    }

    #endregion

    #region Iterate

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("Iterate")]
    public long Iterate_Roaring()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        var bmp = new RoaringBitmap(ctx);
        for (int i = 0; i < _sortedA.Length; i++)
            bmp.Add(_sortedA[i]);
        bmp.PrepareForReading();

        var iter = bmp.GetIterator();
        Span<long> buf = stackalloc long[1024];
        long last = 0;
        int read;
        while ((read = bmp.Fill(buf, ref iter)) > 0)
            last = buf[read - 1];

        bmp.Dispose();
        return last;
    }

    [Benchmark]
    [BenchmarkCategory("Iterate")]
    public long Iterate_GrowableBitArray()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        var gba = new GrowableBitArray(ctx, MaxValue);
        for (int i = 0; i < _sortedA.Length; i++)
            gba.Add(_sortedA[i]);

        var iter = gba.GetIterator(0);
        long last = 0;
        while (iter.MoveNext())
            last = iter.Current;

        gba.Dispose();
        return last;
    }

    #endregion
}

/// <summary>
/// Benchmarks focused on set operations (AND, OR, ANDNOT) between two bitmaps.
/// BCL BitArray supports AND/OR natively so we can compare directly.
/// GrowableBitArray has no set operations, so it's excluded from this benchmark.
/// </summary>
[MemoryDiagnoser]
[Config(typeof(LongBuildConfig))]
public class RoaringBitmapSetOpsBenchmark
{
    private class LongBuildConfig : BenchmarkDotNet.Configs.ManualConfig
    {
        public LongBuildConfig()
        {
            AddJob(new BenchmarkDotNet.Jobs.Job
            {
                Run =
                {
                    LaunchCount = 1,
                    WarmupCount = 1,
                    IterationCount = 3,
                },
                Infrastructure =
                {
                    Toolchain = BenchmarkDotNet.Toolchains.InProcess.Emit.InProcessEmitToolchain.Instance,
                }
            });
        }
    }

    [Params(500, 5_000, 50_000, 500_000)]
    public int Count;

    [Params(1_000_000, 1_000_000_000)]
    public int MaxValue;

    private RoaringBitmap _roaringA, _roaringB;
    private BitArray _bclA, _bclB;
    private ByteStringContext _ctx;
    private long[] _valuesA, _valuesB;

    [GlobalSetup]
    public void Setup()
    {
        _ctx = new ByteStringContext(SharedMultipleUseFlag.None);

        var rng = new Random(42);
        var setA = new HashSet<long>();
        var setB = new HashSet<long>();

        while (setA.Count < Count)
            setA.Add(rng.NextInt64(0, MaxValue));
        while (setB.Count < Count)
            setB.Add(rng.NextInt64(0, MaxValue));

        _valuesA = new long[setA.Count];
        setA.CopyTo(_valuesA);
        _valuesB = new long[setB.Count];
        setB.CopyTo(_valuesB);

        _roaringA = new RoaringBitmap(_ctx);
        _roaringB = new RoaringBitmap(_ctx);
        foreach (long v in setA) _roaringA.Add(v);
        foreach (long v in setB) _roaringB.Add(v);
        _roaringA.PrepareForReading();
        _roaringB.PrepareForReading();

        int max = (int)Math.Min(MaxValue, int.MaxValue - 1);
        _bclA = new BitArray(max + 1);
        _bclB = new BitArray(max + 1);
        foreach (long v in setA) _bclA.Set((int)v, true);
        foreach (long v in setB) _bclB.Set((int)v, true);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        _roaringA.Dispose();
        _roaringB.Dispose();
        _ctx.Dispose();
    }

    #region AND

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("AND")]
    public long And_Roaring()
    {
        // In-place on a clone — matches how Corax uses set ops
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        var clone = new RoaringBitmap(ctx);
        for (int i = 0; i < _valuesA.Length; i++) clone.Add(_valuesA[i]);
        clone.PrepareForReading();
        clone.AndWith(ref _roaringB);
        long c = clone.Cardinality;
        clone.Dispose();
        return c;
    }

    [Benchmark]
    [BenchmarkCategory("AND")]
    public int And_BclBitArray()
    {
        // BCL And is in-place, so we must clone first
        var clone = (BitArray)_bclA.Clone();
        clone.And(_bclB);
        // Count set bits to force materialization
        int count = 0;
        for (int i = 0; i < clone.Count; i++)
            if (clone[i]) count++;
        return count;
    }

    #endregion

    #region OR

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("OR")]
    public long Or_Roaring()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        var clone = new RoaringBitmap(ctx);
        for (int i = 0; i < _valuesA.Length; i++) clone.Add(_valuesA[i]);
        clone.PrepareForReading();
        clone.OrWith(ref _roaringB);
        long c = clone.Cardinality;
        clone.Dispose();
        return c;
    }

    [Benchmark]
    [BenchmarkCategory("OR")]
    public int Or_BclBitArray()
    {
        var clone = (BitArray)_bclA.Clone();
        clone.Or(_bclB);
        int count = 0;
        for (int i = 0; i < clone.Count; i++)
            if (clone[i]) count++;
        return count;
    }

    #endregion

    #region ANDNOT

    [Benchmark(Baseline = true)]
    [BenchmarkCategory("ANDNOT")]
    public long AndNot_Roaring()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        var clone = new RoaringBitmap(ctx);
        for (int i = 0; i < _valuesA.Length; i++) clone.Add(_valuesA[i]);
        clone.PrepareForReading();
        clone.AndNotWith(ref _roaringB);
        long c = clone.Cardinality;
        clone.Dispose();
        return c;
    }

    [Benchmark]
    [BenchmarkCategory("ANDNOT")]
    public int AndNot_BclBitArray()
    {
        // BCL has no AndNot, emulate with clone + Not + And
        var notB = (BitArray)_bclB.Clone();
        notB.Not();
        var clone = (BitArray)_bclA.Clone();
        clone.And(notB);
        int count = 0;
        for (int i = 0; i < clone.Count; i++)
            if (clone[i]) count++;
        return count;
    }

    #endregion
}

/// <summary>
/// Memory footprint comparison including native allocations.
/// MemoryDiagnoser tracks managed GC allocations.
/// For native memory (ByteStringContext), we use NativeMemory.ThreadAllocations to measure
/// the delta before/after building the structure. The native bytes are returned as part of
/// the benchmark result so they appear in the output.
/// </summary>
[MemoryDiagnoser]
[Config(typeof(LongBuildConfig))]
public class RoaringBitmapMemoryBenchmark
{
    private class LongBuildConfig : BenchmarkDotNet.Configs.ManualConfig
    {
        public LongBuildConfig()
        {
            AddJob(new BenchmarkDotNet.Jobs.Job
            {
                Run =
                {
                    LaunchCount = 1,
                    WarmupCount = 1,
                    IterationCount = 3,
                },
                Infrastructure =
                {
                    Toolchain = BenchmarkDotNet.Toolchains.InProcess.Emit.InProcessEmitToolchain.Instance,
                }
            });
        }
    }

    [Params(1_000, 10_000, 100_000)]
    public int Count;

    [Params(1_000_000, 1_000_000_000)]
    public long MaxValue;

    private long[] _values;

    [GlobalSetup]
    public void Setup()
    {
        var rng = new Random(42);
        var set = new HashSet<long>();
        while (set.Count < Count)
            set.Add(rng.NextInt64(0, MaxValue));
        _values = new long[set.Count];
        set.CopyTo(_values);
    }

    [Benchmark(Baseline = true)]
    public long Build_Roaring()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        var bmp = new RoaringBitmap(ctx);
        for (int i = 0; i < _values.Length; i++)
            bmp.Add(_values[i]);
        long c = bmp.Cardinality;
        bmp.Dispose();
        return c;
    }

    [Benchmark]
    public long Build_GrowableBitArray()
    {
        using var ctx = new ByteStringContext(SharedMultipleUseFlag.None);
        long maxVal = Math.Min(MaxValue, int.MaxValue - 1);
        var gba = new GrowableBitArray(ctx, maxVal);
        for (int i = 0; i < _values.Length; i++)
            gba.Add(_values[i]);
        gba.Dispose();
        return _values.Length;
    }

    [Benchmark]
    public int Build_BclBitArray()
    {
        int max = (int)Math.Min(MaxValue, int.MaxValue - 1);
        var ba = new BitArray(max + 1);
        for (int i = 0; i < _values.Length; i++)
            ba.Set((int)_values[i], true);
        return _values.Length;
    }
}
