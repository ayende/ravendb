using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using Sparrow;
using Sparrow.Server;

namespace Corax.Utils.RoaringBitmaps;

/// <summary>
/// SIMD-accelerated set operations (AND, OR, XOR, ANDNOT) for roaring bitmaps.
/// Each operation handles the full container-type combination matrix.
/// Bitmap-bitmap operations use Vector256/Vector512 for throughput.
/// </summary>
public static unsafe class RoaringBitmapSetOps
{
    #region AND (Intersection)

    /// <summary>
    /// Compute the intersection of two roaring bitmaps, producing a new bitmap.
    /// Walks the shorter bitmap's index and checks each key in the longer one (O(1) per key).
    /// </summary>
    public static RoaringBitmap And(ByteStringContext ctx, ref RoaringBitmap a, ref RoaringBitmap b)
    {
        var result = new RoaringBitmap(ctx);

        // Walk the shorter index for efficiency
        ref RoaringBitmap shorter = ref (a.IndexLength <= b.IndexLength ? ref a : ref b);
        ref RoaringBitmap longer = ref (a.IndexLength <= b.IndexLength ? ref b : ref a);

        int shortLen = shorter.IndexLength;
        for (int key = 0; key < shortLen; key++)
        {
            int sSlot = shorter.GetSlotForKey(key);
            if (sSlot < 0) continue;

            int lSlot = longer.GetSlotForKey(key);
            if (lSlot < 0) continue;

            ref ContainerEntry sc = ref shorter.GetEntryBySlot(sSlot);
            ref ContainerEntry lc = ref longer.GetEntryBySlot(lSlot);

            ContainerEntry rc = AndContainers(ctx, ref sc, ref lc);
            if (rc.Cardinality > 0)
                result.AddContainer(rc);
            else if (rc.Storage.HasValue)
                ctx.Release(ref rc.Storage);
        }

        return result;
    }

    private static ContainerEntry AndContainers(ByteStringContext ctx, ref ContainerEntry a, ref ContainerEntry b)
    {
        RoaringBitmap.EnsureSorted(ref a);
        RoaringBitmap.EnsureSorted(ref b);

        if (a.Type == ContainerType.Range)
            return MaterializeRangeAndRedispatch(ctx, ref a, ref b, AndContainers);
        if (b.Type == ContainerType.Range)
            return MaterializeRangeAndRedispatch(ctx, ref b, ref a, (c, ref x, ref y) => AndContainers(c, ref y, ref x));

        return (a.Type, b.Type) switch
        {
            (ContainerType.Bitmap, ContainerType.Bitmap) => AndBitmapBitmap(ctx, ref a, ref b),
            (ContainerType.Array, ContainerType.Array) => AndArrayArray(ctx, ref a, ref b),
            (ContainerType.Array, ContainerType.Bitmap) => AndArrayBitmap(ctx, ref a, ref b),
            (ContainerType.Bitmap, ContainerType.Array) => AndArrayBitmap(ctx, ref b, ref a),
            _ => default
        };
    }

    private static ContainerEntry AndBitmapBitmap(ByteStringContext ctx, ref ContainerEntry a, ref ContainerEntry b)
    {
        ctx.Allocate(RoaringBitmap.BitmapContainerSizeInBytes, out ByteString storage);
        ulong* dst = (ulong*)storage.Ptr;
        ulong* ap = (ulong*)a.Data;
        ulong* bp = (ulong*)b.Data;

        int cardinality = BitmapAndSimd(ap, bp, dst, RoaringBitmap.BitmapContainerSizeInUlongs);

        var result = new ContainerEntry
        {
            Key = a.Key,
            Data = storage.Ptr,
            Cardinality = cardinality,
            Storage = storage,
            Type = ContainerType.Bitmap
        };

        OptimizeBitmapResult(ctx, ref result);
        return result;
    }

    private static ContainerEntry AndArrayArray(ByteStringContext ctx, ref ContainerEntry a, ref ContainerEntry b)
    {
        int maxResult = Math.Min(a.Cardinality, b.Cardinality);
        ctx.Allocate(Math.Max(64, maxResult * sizeof(ushort)), out ByteString storage);

        int count = RoaringBitmap.ArrayContainerAnd(
            (ushort*)a.Data, a.Cardinality,
            (ushort*)b.Data, b.Cardinality,
            (ushort*)storage.Ptr);

        return new ContainerEntry
        {
            Key = a.Key,
            Data = storage.Ptr,
            Cardinality = count,
            Storage = storage,
            Type = ContainerType.Array
        };
    }

    private static ContainerEntry AndArrayBitmap(ByteStringContext ctx, ref ContainerEntry array, ref ContainerEntry bitmap)
    {
        // Check which bits from the array exist in the bitmap
        ctx.Allocate(Math.Max(64, array.Cardinality * sizeof(ushort)), out ByteString storage);
        ushort* arr = (ushort*)array.Data;
        ulong* bmp = (ulong*)bitmap.Data;
        ushort* dst = (ushort*)storage.Ptr;

        int count = 0;
        for (int i = 0; i < array.Cardinality; i++)
        {
            ushort val = arr[i];
            if ((bmp[val >> 6] & (1UL << (val & 63))) != 0)
                dst[count++] = val;
        }

        return new ContainerEntry
        {
            Key = array.Key,
            Data = storage.Ptr,
            Cardinality = count,
            Storage = storage,
            Type = ContainerType.Array
        };
    }

    #endregion

    #region OR (Union)

    /// <summary>
    /// Compute the union of two roaring bitmaps, producing a new bitmap.
    /// </summary>
    public static RoaringBitmap Or(ByteStringContext ctx, ref RoaringBitmap a, ref RoaringBitmap b)
    {
        var result = new RoaringBitmap(ctx);
        int maxLen = Math.Max(a.IndexLength, b.IndexLength);

        for (int key = 0; key < maxLen; key++)
        {
            int aSlot = a.GetSlotForKey(key);
            int bSlot = b.GetSlotForKey(key);

            if (aSlot >= 0 && bSlot >= 0)
            {
                result.AddContainer(OrContainers(ctx, ref a.GetEntryBySlot(aSlot), ref b.GetEntryBySlot(bSlot)));
            }
            else if (aSlot >= 0)
            {
                result.AddContainer(CloneContainer(ctx, ref a.GetEntryBySlot(aSlot)));
            }
            else if (bSlot >= 0)
            {
                result.AddContainer(CloneContainer(ctx, ref b.GetEntryBySlot(bSlot)));
            }
        }

        return result;
    }

    private static ContainerEntry OrContainers(ByteStringContext ctx, ref ContainerEntry a, ref ContainerEntry b)
    {
        RoaringBitmap.EnsureSorted(ref a);
        RoaringBitmap.EnsureSorted(ref b);

        if (a.Type == ContainerType.Range)
            return MaterializeRangeAndRedispatch(ctx, ref a, ref b, OrContainers);
        if (b.Type == ContainerType.Range)
            return MaterializeRangeAndRedispatch(ctx, ref b, ref a, (c, ref x, ref y) => OrContainers(c, ref y, ref x));

        return (a.Type, b.Type) switch
        {
            (ContainerType.Bitmap, ContainerType.Bitmap) => OrBitmapBitmap(ctx, ref a, ref b),
            (ContainerType.Array, ContainerType.Array) => OrArrayArray(ctx, ref a, ref b),
            (ContainerType.Array, ContainerType.Bitmap) => OrArrayBitmap(ctx, ref a, ref b),
            (ContainerType.Bitmap, ContainerType.Array) => OrArrayBitmap(ctx, ref b, ref a),
            _ => default
        };
    }

    private static ContainerEntry OrBitmapBitmap(ByteStringContext ctx, ref ContainerEntry a, ref ContainerEntry b)
    {
        ctx.Allocate(RoaringBitmap.BitmapContainerSizeInBytes, out ByteString storage);
        ulong* dst = (ulong*)storage.Ptr;
        ulong* ap = (ulong*)a.Data;
        ulong* bp = (ulong*)b.Data;

        int cardinality = BitmapOrSimd(ap, bp, dst, RoaringBitmap.BitmapContainerSizeInUlongs);

        var result = new ContainerEntry
        {
            Key = a.Key,
            Data = storage.Ptr,
            Cardinality = cardinality,
            Storage = storage,
            Type = ContainerType.Bitmap
        };

        OptimizeBitmapResult(ctx, ref result);
        return result;
    }

    private static ContainerEntry OrArrayArray(ByteStringContext ctx, ref ContainerEntry a, ref ContainerEntry b)
    {
        int maxResult = a.Cardinality + b.Cardinality;

        if (maxResult > RoaringBitmap.ArrayContainerMaxCardinality)
        {
            // Result might be a bitmap container
            ctx.Allocate(RoaringBitmap.BitmapContainerSizeInBytes, out ByteString bmpStorage);
            ulong* bitmap = (ulong*)bmpStorage.Ptr;
            new Span<byte>(bitmap, RoaringBitmap.BitmapContainerSizeInBytes).Clear();

            // Set bits from both arrays
            ushort* aArr = (ushort*)a.Data;
            ushort* bArr = (ushort*)b.Data;

            for (int i = 0; i < a.Cardinality; i++)
            {
                ushort val = aArr[i];
                bitmap[val >> 6] |= 1UL << (val & 63);
            }
            for (int i = 0; i < b.Cardinality; i++)
            {
                ushort val = bArr[i];
                bitmap[val >> 6] |= 1UL << (val & 63);
            }

            int cardinality = RoaringBitmap.BitmapContainerCardinality((byte*)bitmap);

            var result = new ContainerEntry
            {
                Key = a.Key,
                Data = bmpStorage.Ptr,
                Cardinality = cardinality,
                Storage = bmpStorage,
                Type = ContainerType.Bitmap
            };

            OptimizeBitmapResult(ctx, ref result);
            return result;
        }

        // Both are small enough that the result fits in an array
        ctx.Allocate(Math.Max(64, maxResult * sizeof(ushort)), out ByteString storage);
        int count = RoaringBitmap.ArrayContainerOr(
            (ushort*)a.Data, a.Cardinality,
            (ushort*)b.Data, b.Cardinality,
            (ushort*)storage.Ptr);

        return new ContainerEntry
        {
            Key = a.Key,
            Data = storage.Ptr,
            Cardinality = count,
            Storage = storage,
            Type = ContainerType.Array
        };
    }

    private static ContainerEntry OrArrayBitmap(ByteStringContext ctx, ref ContainerEntry array, ref ContainerEntry bitmap)
    {
        // Clone bitmap, set bits from array
        ctx.Allocate(RoaringBitmap.BitmapContainerSizeInBytes, out ByteString storage);
        Unsafe.CopyBlockUnaligned(storage.Ptr, bitmap.Data, RoaringBitmap.BitmapContainerSizeInBytes);

        ulong* dst = (ulong*)storage.Ptr;
        ushort* arr = (ushort*)array.Data;

        for (int i = 0; i < array.Cardinality; i++)
        {
            ushort val = arr[i];
            dst[val >> 6] |= 1UL << (val & 63);
        }

        int cardinality = RoaringBitmap.BitmapContainerCardinality(storage.Ptr);

        var result = new ContainerEntry
        {
            Key = array.Key,
            Data = storage.Ptr,
            Cardinality = cardinality,
            Storage = storage,
            Type = ContainerType.Bitmap
        };

        OptimizeBitmapResult(ctx, ref result);
        return result;
    }

    #endregion

    #region XOR

    /// <summary>
    /// Compute the symmetric difference of two roaring bitmaps.
    /// </summary>
    public static RoaringBitmap Xor(ByteStringContext ctx, ref RoaringBitmap a, ref RoaringBitmap b)
    {
        var result = new RoaringBitmap(ctx);
        int maxLen = Math.Max(a.IndexLength, b.IndexLength);

        for (int key = 0; key < maxLen; key++)
        {
            int aSlot = a.GetSlotForKey(key);
            int bSlot = b.GetSlotForKey(key);

            if (aSlot >= 0 && bSlot >= 0)
            {
                ContainerEntry rc = XorContainers(ctx, ref a.GetEntryBySlot(aSlot), ref b.GetEntryBySlot(bSlot));
                if (rc.Cardinality > 0)
                    result.AddContainer(rc);
                else if (rc.Storage.HasValue)
                    ctx.Release(ref rc.Storage);
            }
            else if (aSlot >= 0)
            {
                result.AddContainer(CloneContainer(ctx, ref a.GetEntryBySlot(aSlot)));
            }
            else if (bSlot >= 0)
            {
                result.AddContainer(CloneContainer(ctx, ref b.GetEntryBySlot(bSlot)));
            }
        }

        return result;
    }

    private static ContainerEntry XorContainers(ByteStringContext ctx, ref ContainerEntry a, ref ContainerEntry b)
    {
        RoaringBitmap.EnsureSorted(ref a);
        RoaringBitmap.EnsureSorted(ref b);

        if (a.Type == ContainerType.Range)
            return MaterializeRangeAndRedispatch(ctx, ref a, ref b, XorContainers);
        if (b.Type == ContainerType.Range)
            return MaterializeRangeAndRedispatch(ctx, ref b, ref a, (c, ref x, ref y) => XorContainers(c, ref y, ref x));

        return (a.Type, b.Type) switch
        {
            (ContainerType.Bitmap, ContainerType.Bitmap) => XorBitmapBitmap(ctx, ref a, ref b),
            (ContainerType.Array, ContainerType.Array) => XorArrayArray(ctx, ref a, ref b),
            (ContainerType.Array, ContainerType.Bitmap) => XorArrayBitmap(ctx, ref a, ref b),
            (ContainerType.Bitmap, ContainerType.Array) => XorArrayBitmap(ctx, ref b, ref a),
            _ => default
        };
    }

    private static ContainerEntry XorBitmapBitmap(ByteStringContext ctx, ref ContainerEntry a, ref ContainerEntry b)
    {
        ctx.Allocate(RoaringBitmap.BitmapContainerSizeInBytes, out ByteString storage);
        ulong* dst = (ulong*)storage.Ptr;
        ulong* ap = (ulong*)a.Data;
        ulong* bp = (ulong*)b.Data;

        int cardinality = BitmapXorSimd(ap, bp, dst, RoaringBitmap.BitmapContainerSizeInUlongs);

        var result = new ContainerEntry
        {
            Key = a.Key,
            Data = storage.Ptr,
            Cardinality = cardinality,
            Storage = storage,
            Type = ContainerType.Bitmap
        };

        OptimizeBitmapResult(ctx, ref result);
        return result;
    }

    private static ContainerEntry XorArrayArray(ByteStringContext ctx, ref ContainerEntry a, ref ContainerEntry b)
    {
        int maxResult = a.Cardinality + b.Cardinality;

        if (maxResult > RoaringBitmap.ArrayContainerMaxCardinality)
        {
            // Result might be a bitmap
            ctx.Allocate(RoaringBitmap.BitmapContainerSizeInBytes, out ByteString bmpStorage);
            ulong* bitmap = (ulong*)bmpStorage.Ptr;
            new Span<byte>(bitmap, RoaringBitmap.BitmapContainerSizeInBytes).Clear();

            ushort* aArr = (ushort*)a.Data;
            ushort* bArr = (ushort*)b.Data;

            for (int i = 0; i < a.Cardinality; i++)
            {
                ushort val = aArr[i];
                bitmap[val >> 6] ^= 1UL << (val & 63);
            }
            for (int i = 0; i < b.Cardinality; i++)
            {
                ushort val = bArr[i];
                bitmap[val >> 6] ^= 1UL << (val & 63);
            }

            int cardinality = RoaringBitmap.BitmapContainerCardinality((byte*)bitmap);

            var result = new ContainerEntry
            {
                Key = a.Key,
                Data = bmpStorage.Ptr,
                Cardinality = cardinality,
                Storage = bmpStorage,
                Type = ContainerType.Bitmap
            };

            OptimizeBitmapResult(ctx, ref result);
            return result;
        }

        // Small enough for array output
        ctx.Allocate(Math.Max(64, maxResult * sizeof(ushort)), out ByteString storage);
        int count = RoaringBitmap.ArrayContainerXor(
            (ushort*)a.Data, a.Cardinality,
            (ushort*)b.Data, b.Cardinality,
            (ushort*)storage.Ptr);

        return new ContainerEntry
        {
            Key = a.Key,
            Data = storage.Ptr,
            Cardinality = count,
            Storage = storage,
            Type = ContainerType.Array
        };
    }

    private static ContainerEntry XorArrayBitmap(ByteStringContext ctx, ref ContainerEntry array, ref ContainerEntry bitmap)
    {
        // Clone bitmap, XOR bits from array
        ctx.Allocate(RoaringBitmap.BitmapContainerSizeInBytes, out ByteString storage);
        Unsafe.CopyBlockUnaligned(storage.Ptr, bitmap.Data, RoaringBitmap.BitmapContainerSizeInBytes);

        ulong* dst = (ulong*)storage.Ptr;
        ushort* arr = (ushort*)array.Data;

        for (int i = 0; i < array.Cardinality; i++)
        {
            ushort val = arr[i];
            dst[val >> 6] ^= 1UL << (val & 63);
        }

        int cardinality = RoaringBitmap.BitmapContainerCardinality(storage.Ptr);

        var result = new ContainerEntry
        {
            Key = array.Key,
            Data = storage.Ptr,
            Cardinality = cardinality,
            Storage = storage,
            Type = ContainerType.Bitmap
        };

        OptimizeBitmapResult(ctx, ref result);
        return result;
    }

    #endregion

    #region ANDNOT (Difference)

    /// <summary>
    /// Compute A AND NOT B (elements in A but not in B).
    /// </summary>
    public static RoaringBitmap AndNot(ByteStringContext ctx, ref RoaringBitmap a, ref RoaringBitmap b)
    {
        var result = new RoaringBitmap(ctx);
        int aLen = a.IndexLength;

        for (int key = 0; key < aLen; key++)
        {
            int aSlot = a.GetSlotForKey(key);
            if (aSlot < 0) continue;

            int bSlot = b.GetSlotForKey(key);
            if (bSlot < 0)
            {
                result.AddContainer(CloneContainer(ctx, ref a.GetEntryBySlot(aSlot)));
            }
            else
            {
                ContainerEntry rc = AndNotContainers(ctx, ref a.GetEntryBySlot(aSlot), ref b.GetEntryBySlot(bSlot));
                if (rc.Cardinality > 0)
                    result.AddContainer(rc);
                else if (rc.Storage.HasValue)
                    ctx.Release(ref rc.Storage);
            }
        }

        return result;
    }

    private static ContainerEntry AndNotContainers(ByteStringContext ctx, ref ContainerEntry a, ref ContainerEntry b)
    {
        RoaringBitmap.EnsureSorted(ref a);
        RoaringBitmap.EnsureSorted(ref b);

        if (a.Type == ContainerType.Range)
            return MaterializeRangeAndRedispatch(ctx, ref a, ref b, AndNotContainers);
        if (b.Type == ContainerType.Range)
            return MaterializeRangeAndRedispatch(ctx, ref b, ref a, (c, ref matB, ref origA) => AndNotContainers(c, ref origA, ref matB));

        return (a.Type, b.Type) switch
        {
            (ContainerType.Bitmap, ContainerType.Bitmap) => AndNotBitmapBitmap(ctx, ref a, ref b),
            (ContainerType.Array, ContainerType.Array) => AndNotArrayArray(ctx, ref a, ref b),
            (ContainerType.Array, ContainerType.Bitmap) => AndNotArrayBitmap(ctx, ref a, ref b),
            (ContainerType.Bitmap, ContainerType.Array) => AndNotBitmapArray(ctx, ref a, ref b),
            _ => default
        };
    }

    private static ContainerEntry AndNotBitmapBitmap(ByteStringContext ctx, ref ContainerEntry a, ref ContainerEntry b)
    {
        ctx.Allocate(RoaringBitmap.BitmapContainerSizeInBytes, out ByteString storage);
        ulong* dst = (ulong*)storage.Ptr;
        ulong* ap = (ulong*)a.Data;
        ulong* bp = (ulong*)b.Data;

        int cardinality = BitmapAndNotSimd(ap, bp, dst, RoaringBitmap.BitmapContainerSizeInUlongs);

        var result = new ContainerEntry
        {
            Key = a.Key,
            Data = storage.Ptr,
            Cardinality = cardinality,
            Storage = storage,
            Type = ContainerType.Bitmap
        };

        OptimizeBitmapResult(ctx, ref result);
        return result;
    }

    private static ContainerEntry AndNotArrayArray(ByteStringContext ctx, ref ContainerEntry a, ref ContainerEntry b)
    {
        ctx.Allocate(Math.Max(64, a.Cardinality * sizeof(ushort)), out ByteString storage);

        int count = RoaringBitmap.ArrayContainerAndNot(
            (ushort*)a.Data, a.Cardinality,
            (ushort*)b.Data, b.Cardinality,
            (ushort*)storage.Ptr);

        return new ContainerEntry
        {
            Key = a.Key,
            Data = storage.Ptr,
            Cardinality = count,
            Storage = storage,
            Type = ContainerType.Array
        };
    }

    private static ContainerEntry AndNotArrayBitmap(ByteStringContext ctx, ref ContainerEntry array, ref ContainerEntry bitmap)
    {
        ctx.Allocate(Math.Max(64, array.Cardinality * sizeof(ushort)), out ByteString storage);
        ushort* arr = (ushort*)array.Data;
        ulong* bmp = (ulong*)bitmap.Data;
        ushort* dst = (ushort*)storage.Ptr;

        int count = 0;
        for (int i = 0; i < array.Cardinality; i++)
        {
            ushort val = arr[i];
            if ((bmp[val >> 6] & (1UL << (val & 63))) == 0)
                dst[count++] = val;
        }

        return new ContainerEntry
        {
            Key = array.Key,
            Data = storage.Ptr,
            Cardinality = count,
            Storage = storage,
            Type = ContainerType.Array
        };
    }

    private static ContainerEntry AndNotBitmapArray(ByteStringContext ctx, ref ContainerEntry bitmap, ref ContainerEntry array)
    {
        // Clone bitmap, clear bits from array
        ctx.Allocate(RoaringBitmap.BitmapContainerSizeInBytes, out ByteString storage);
        Unsafe.CopyBlockUnaligned(storage.Ptr, bitmap.Data, RoaringBitmap.BitmapContainerSizeInBytes);

        ulong* dst = (ulong*)storage.Ptr;
        ushort* arr = (ushort*)array.Data;

        for (int i = 0; i < array.Cardinality; i++)
        {
            ushort val = arr[i];
            dst[val >> 6] &= ~(1UL << (val & 63));
        }

        int cardinality = RoaringBitmap.BitmapContainerCardinality(storage.Ptr);

        var result = new ContainerEntry
        {
            Key = bitmap.Key,
            Data = storage.Ptr,
            Cardinality = cardinality,
            Storage = storage,
            Type = ContainerType.Bitmap
        };

        OptimizeBitmapResult(ctx, ref result);
        return result;
    }

    #endregion

    #region SIMD Bitmap Operations

    // ---- Generic SIMD bitmap operations ----
    // Each operation (AND, OR, XOR, ANDNOT) is a zero-cost struct implementing IBitmapOp.
    // The JIT specializes each generic instantiation, eliminating all virtual dispatch.
    // One method per SIMD width handles all operations — 4 methods instead of 16.

    private interface IBitmapOp
    {
        static abstract ulong Apply(ulong a, ulong b);
        static abstract Vector128<ulong> Apply(Vector128<ulong> a, Vector128<ulong> b);
        static abstract Vector256<ulong> Apply(Vector256<ulong> a, Vector256<ulong> b);
        static abstract Vector512<ulong> Apply(Vector512<ulong> a, Vector512<ulong> b);
    }

    private struct AndOp : IBitmapOp
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)] public static ulong Apply(ulong a, ulong b) => a & b;
        [MethodImpl(MethodImplOptions.AggressiveInlining)] public static Vector128<ulong> Apply(Vector128<ulong> a, Vector128<ulong> b) => a & b;
        [MethodImpl(MethodImplOptions.AggressiveInlining)] public static Vector256<ulong> Apply(Vector256<ulong> a, Vector256<ulong> b) => a & b;
        [MethodImpl(MethodImplOptions.AggressiveInlining)] public static Vector512<ulong> Apply(Vector512<ulong> a, Vector512<ulong> b) => a & b;
    }

    private struct OrOp : IBitmapOp
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)] public static ulong Apply(ulong a, ulong b) => a | b;
        [MethodImpl(MethodImplOptions.AggressiveInlining)] public static Vector128<ulong> Apply(Vector128<ulong> a, Vector128<ulong> b) => a | b;
        [MethodImpl(MethodImplOptions.AggressiveInlining)] public static Vector256<ulong> Apply(Vector256<ulong> a, Vector256<ulong> b) => a | b;
        [MethodImpl(MethodImplOptions.AggressiveInlining)] public static Vector512<ulong> Apply(Vector512<ulong> a, Vector512<ulong> b) => a | b;
    }

    private struct XorOp : IBitmapOp
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)] public static ulong Apply(ulong a, ulong b) => a ^ b;
        [MethodImpl(MethodImplOptions.AggressiveInlining)] public static Vector128<ulong> Apply(Vector128<ulong> a, Vector128<ulong> b) => a ^ b;
        [MethodImpl(MethodImplOptions.AggressiveInlining)] public static Vector256<ulong> Apply(Vector256<ulong> a, Vector256<ulong> b) => a ^ b;
        [MethodImpl(MethodImplOptions.AggressiveInlining)] public static Vector512<ulong> Apply(Vector512<ulong> a, Vector512<ulong> b) => a ^ b;
    }

    private struct AndNotOp : IBitmapOp
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)] public static ulong Apply(ulong a, ulong b) => a & ~b;
        [MethodImpl(MethodImplOptions.AggressiveInlining)] public static Vector128<ulong> Apply(Vector128<ulong> a, Vector128<ulong> b) => Vector128.AndNot(a, b);
        [MethodImpl(MethodImplOptions.AggressiveInlining)] public static Vector256<ulong> Apply(Vector256<ulong> a, Vector256<ulong> b) => Vector256.AndNot(a, b);
        [MethodImpl(MethodImplOptions.AggressiveInlining)] public static Vector512<ulong> Apply(Vector512<ulong> a, Vector512<ulong> b) => Vector512.AndNot(a, b);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static int BitmapAndSimd(ulong* a, ulong* b, ulong* dst, int count) => BitmapOpDispatch<AndOp>(a, b, dst, count);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static int BitmapOrSimd(ulong* a, ulong* b, ulong* dst, int count) => BitmapOpDispatch<OrOp>(a, b, dst, count);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static int BitmapXorSimd(ulong* a, ulong* b, ulong* dst, int count) => BitmapOpDispatch<XorOp>(a, b, dst, count);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static int BitmapAndNotSimd(ulong* a, ulong* b, ulong* dst, int count) => BitmapOpDispatch<AndNotOp>(a, b, dst, count);

    internal static int BitmapNotSimd(ulong* src, ulong* dst, int count)
    {
        int cardinality = 0;
        for (int i = 0; i < count; i++)
        {
            dst[i] = ~src[i];
            cardinality += BitOperations.PopCount(dst[i]);
        }
        return cardinality;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int BitmapOpDispatch<TOp>(ulong* a, ulong* b, ulong* dst, int count) where TOp : struct, IBitmapOp
    {
        if (AdvInstructionSet.IsAcceleratedVector512)
            return BitmapOpVector512<TOp>(a, b, dst, count);
        if (AdvInstructionSet.IsAcceleratedVector256)
            return BitmapOpVector256<TOp>(a, b, dst, count);
        if (AdvInstructionSet.IsAcceleratedVector128)
            return BitmapOpVector128<TOp>(a, b, dst, count);
        return BitmapOpScalar<TOp>(a, b, dst, count);
    }

    private static int BitmapOpVector512<TOp>(ulong* a, ulong* b, ulong* dst, int count) where TOp : struct, IBitmapOp
    {
        int N = Vector512<ulong>.Count;
        int cardinality = 0;
        int i = 0;

        for (; i + N <= count; i += N)
        {
            TOp.Apply(Vector512.Load(a + i), Vector512.Load(b + i)).Store(dst + i);
            for (int j = 0; j < N; j++)
                cardinality += BitOperations.PopCount(dst[i + j]);
        }

        for (; i < count; i++)
        {
            dst[i] = TOp.Apply(a[i], b[i]);
            cardinality += BitOperations.PopCount(dst[i]);
        }

        return cardinality;
    }

    private static int BitmapOpVector256<TOp>(ulong* a, ulong* b, ulong* dst, int count) where TOp : struct, IBitmapOp
    {
        int N = Vector256<ulong>.Count;
        int cardinality = 0;
        int i = 0;

        for (; i + N <= count; i += N)
        {
            TOp.Apply(Vector256.Load(a + i), Vector256.Load(b + i)).Store(dst + i);
            for (int j = 0; j < N; j++)
                cardinality += BitOperations.PopCount(dst[i + j]);
        }

        for (; i < count; i++)
        {
            dst[i] = TOp.Apply(a[i], b[i]);
            cardinality += BitOperations.PopCount(dst[i]);
        }

        return cardinality;
    }

    private static int BitmapOpVector128<TOp>(ulong* a, ulong* b, ulong* dst, int count) where TOp : struct, IBitmapOp
    {
        int N = Vector128<ulong>.Count;
        int cardinality = 0;
        int i = 0;

        for (; i + N <= count; i += N)
        {
            TOp.Apply(Vector128.Load(a + i), Vector128.Load(b + i)).Store(dst + i);
            for (int j = 0; j < N; j++)
                cardinality += BitOperations.PopCount(dst[i + j]);
        }

        for (; i < count; i++)
        {
            dst[i] = TOp.Apply(a[i], b[i]);
            cardinality += BitOperations.PopCount(dst[i]);
        }

        return cardinality;
    }

    private static int BitmapOpScalar<TOp>(ulong* a, ulong* b, ulong* dst, int count) where TOp : struct, IBitmapOp
    {
        int cardinality = 0;
        for (int i = 0; i < count; i++)
        {
            dst[i] = TOp.Apply(a[i], b[i]);
            cardinality += BitOperations.PopCount(dst[i]);
        }
        return cardinality;
    }

    // Keep named scalar methods for test backward compatibility
    internal static int BitmapAndScalar(ulong* a, ulong* b, ulong* dst, int count) => BitmapOpScalar<AndOp>(a, b, dst, count);
    internal static int BitmapOrScalar(ulong* a, ulong* b, ulong* dst, int count) => BitmapOpScalar<OrOp>(a, b, dst, count);
    internal static int BitmapXorScalar(ulong* a, ulong* b, ulong* dst, int count) => BitmapOpScalar<XorOp>(a, b, dst, count);
    internal static int BitmapAndNotScalar(ulong* a, ulong* b, ulong* dst, int count) => BitmapOpScalar<AndNotOp>(a, b, dst, count);

    #endregion

    #region Helpers

    private delegate ContainerEntry ContainerOp(ByteStringContext ctx, ref ContainerEntry a, ref ContainerEntry b);

    /// <summary>
    /// Materialize a Range container to a temporary Bitmap, run the operation, release the temp.
    /// </summary>
    private static ContainerEntry MaterializeRangeAndRedispatch(
        ByteStringContext ctx, ref ContainerEntry rangeEntry, ref ContainerEntry other, ContainerOp op)
    {
        ctx.Allocate(RoaringBitmap.BitmapContainerSizeInBytes, out ByteString tempStorage);
        ulong* bitmap = (ulong*)tempStorage.Ptr;
        new Span<byte>(bitmap, RoaringBitmap.BitmapContainerSizeInBytes).Clear();

        int rangeCount = rangeEntry.Cardinality;
        int fullWords = rangeCount / 64;
        for (int i = 0; i < fullWords; i++)
            bitmap[i] = ulong.MaxValue;
        int remainder = rangeCount & 63;
        if (remainder > 0)
            bitmap[fullWords] = (1UL << remainder) - 1;

        var tempEntry = new ContainerEntry
        {
            Key = rangeEntry.Key,
            Data = tempStorage.Ptr,
            Cardinality = rangeEntry.Cardinality,
            Storage = tempStorage,
            Type = ContainerType.Bitmap
        };

        var result = op(ctx, ref tempEntry, ref other);
        ctx.Release(ref tempStorage);
        return result;
    }

    internal static ContainerEntry CloneContainer(ByteStringContext ctx, ref ContainerEntry entry)
    {
        if (entry.Type == ContainerType.Range)
        {
            return new ContainerEntry
            {
                Key = entry.Key,
                Type = ContainerType.Range,
                Cardinality = entry.Cardinality,
                Data = null,
                Storage = default
            };
        }

        int dataSize = entry.Type switch
        {
            ContainerType.Array or ContainerType.ArrayUnsorted => Math.Max(64, entry.Cardinality * sizeof(ushort)),
            ContainerType.Bitmap => RoaringBitmap.BitmapContainerSizeInBytes,
            _ => 0
        };

        if (dataSize == 0)
            return default;

        ctx.Allocate(dataSize, out ByteString storage);
        Unsafe.CopyBlockUnaligned(storage.Ptr, entry.Data, (uint)dataSize);

        return new ContainerEntry
        {
            Key = entry.Key,
            Type = entry.Type,
            Cardinality = entry.Cardinality,
            Data = storage.Ptr,
            Storage = storage
        };
    }

    private static void ConvertResultBitmapToArray(ByteStringContext ctx, ref ContainerEntry entry)
    {
        ulong* bitmap = (ulong*)entry.Data;
        ctx.Allocate(Math.Max(64, entry.Cardinality * sizeof(ushort)), out ByteString newStorage);

        int count = RoaringBitmap.BitmapToArray(bitmap, (ushort*)newStorage.Ptr);

        ctx.Release(ref entry.Storage);
        entry.Storage = newStorage;
        entry.Data = newStorage.Ptr;
        entry.Type = ContainerType.Array;
    }

    /// <summary>
    /// Convert a bitmap result to the optimal container type based on cardinality.
    /// </summary>
    private static void OptimizeBitmapResult(ByteStringContext ctx, ref ContainerEntry entry)
    {
        if (entry.Cardinality <= RoaringBitmap.ArrayContainerMaxCardinality && entry.Cardinality > 0)
            ConvertResultBitmapToArray(ctx, ref entry);
    }


    #endregion
}
