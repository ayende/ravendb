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
    /// </summary>
    public static RoaringBitmap And(ByteStringContext ctx, ref RoaringBitmap a, ref RoaringBitmap b)
    {
        var result = new RoaringBitmap(ctx);

        int ai = 0, bi = 0;

        while (ai < a.ContainerCount && bi < b.ContainerCount)
        {
            ref ContainerEntry ac = ref a.GetContainer(ai);
            ref ContainerEntry bc = ref b.GetContainer(bi);

            if (ac.Key < bc.Key)
            {
                ai++;
            }
            else if (ac.Key > bc.Key)
            {
                bi++;
            }
            else
            {
                // Same key - intersect containers
                ContainerEntry rc = AndContainers(ctx, ref ac, ref bc);
                if (rc.Cardinality > 0)
                    result.AddContainer(rc);
                else if (rc.Storage.HasValue)
                    ctx.Release(ref rc.Storage);

                ai++;
                bi++;
            }
        }

        return result;
    }

    private static ContainerEntry AndContainers(ByteStringContext ctx, ref ContainerEntry a, ref ContainerEntry b)
    {
        // Full AND X = X (clone)
        if (a.Type == ContainerType.Full)
            return CloneContainer(ctx, ref b);
        if (b.Type == ContainerType.Full)
            return CloneContainer(ctx, ref a);

        // Materialize Run/Negated to Bitmap, then re-dispatch
        if (a.Type == ContainerType.Run)
            return MaterializeAndRedispatch(ctx, ref a, ref b, AndContainers);
        if (b.Type == ContainerType.Run)
            return MaterializeAndRedispatch(ctx, ref b, ref a, (c, ref x, ref y) => AndContainers(c, ref y, ref x));
        if (a.Type == ContainerType.Negated)
            return MaterializeNegatedAndRedispatch(ctx, ref a, ref b, AndContainers);
        if (b.Type == ContainerType.Negated)
            return MaterializeNegatedAndRedispatch(ctx, ref b, ref a, (c, ref x, ref y) => AndContainers(c, ref y, ref x));

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

        int ai = 0, bi = 0;

        while (ai < a.ContainerCount && bi < b.ContainerCount)
        {
            ref ContainerEntry ac = ref a.GetContainer(ai);
            ref ContainerEntry bc = ref b.GetContainer(bi);

            if (ac.Key < bc.Key)
            {
                result.AddContainer(CloneContainer(ctx, ref ac));
                ai++;
            }
            else if (ac.Key > bc.Key)
            {
                result.AddContainer(CloneContainer(ctx, ref bc));
                bi++;
            }
            else
            {
                result.AddContainer(OrContainers(ctx, ref ac, ref bc));
                ai++;
                bi++;
            }
        }

        // Remaining containers from a
        while (ai < a.ContainerCount)
        {
            result.AddContainer(CloneContainer(ctx, ref a.GetContainer(ai)));
            ai++;
        }

        // Remaining containers from b
        while (bi < b.ContainerCount)
        {
            result.AddContainer(CloneContainer(ctx, ref b.GetContainer(bi)));
            bi++;
        }

        return result;
    }

    private static ContainerEntry OrContainers(ByteStringContext ctx, ref ContainerEntry a, ref ContainerEntry b)
    {
        // Full OR X = Full
        if (a.Type == ContainerType.Full || b.Type == ContainerType.Full)
        {
            return new ContainerEntry
            {
                Key = a.Key,
                Type = ContainerType.Full,
                Cardinality = RoaringBitmap.BitsPerContainer,
                Data = null,
                Storage = default
            };
        }

        // Materialize Run/Negated to Bitmap, then re-dispatch
        if (a.Type == ContainerType.Run)
            return MaterializeAndRedispatch(ctx, ref a, ref b, OrContainers);
        if (b.Type == ContainerType.Run)
            return MaterializeAndRedispatch(ctx, ref b, ref a, (c, ref x, ref y) => OrContainers(c, ref y, ref x));
        if (a.Type == ContainerType.Negated)
            return MaterializeNegatedAndRedispatch(ctx, ref a, ref b, OrContainers);
        if (b.Type == ContainerType.Negated)
            return MaterializeNegatedAndRedispatch(ctx, ref b, ref a, (c, ref x, ref y) => OrContainers(c, ref y, ref x));

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

        int ai = 0, bi = 0;

        while (ai < a.ContainerCount && bi < b.ContainerCount)
        {
            ref ContainerEntry ac = ref a.GetContainer(ai);
            ref ContainerEntry bc = ref b.GetContainer(bi);

            if (ac.Key < bc.Key)
            {
                result.AddContainer(CloneContainer(ctx, ref ac));
                ai++;
            }
            else if (ac.Key > bc.Key)
            {
                result.AddContainer(CloneContainer(ctx, ref bc));
                bi++;
            }
            else
            {
                ContainerEntry rc = XorContainers(ctx, ref ac, ref bc);
                if (rc.Cardinality > 0)
                    result.AddContainer(rc);
                else if (rc.Storage.HasValue)
                    ctx.Release(ref rc.Storage);

                ai++;
                bi++;
            }
        }

        while (ai < a.ContainerCount)
        {
            result.AddContainer(CloneContainer(ctx, ref a.GetContainer(ai)));
            ai++;
        }

        while (bi < b.ContainerCount)
        {
            result.AddContainer(CloneContainer(ctx, ref b.GetContainer(bi)));
            bi++;
        }

        return result;
    }

    private static ContainerEntry XorContainers(ByteStringContext ctx, ref ContainerEntry a, ref ContainerEntry b)
    {
        // Full XOR Full = Empty
        if (a.Type == ContainerType.Full && b.Type == ContainerType.Full)
            return default;

        // Full XOR X = NOT X (complement within container)
        if (a.Type == ContainerType.Full)
            return NotContainer(ctx, ref b);
        if (b.Type == ContainerType.Full)
            return NotContainer(ctx, ref a);

        // Materialize Run/Negated to Bitmap, then re-dispatch
        if (a.Type == ContainerType.Run)
            return MaterializeAndRedispatch(ctx, ref a, ref b, XorContainers);
        if (b.Type == ContainerType.Run)
            return MaterializeAndRedispatch(ctx, ref b, ref a, (c, ref x, ref y) => XorContainers(c, ref y, ref x));
        if (a.Type == ContainerType.Negated)
            return MaterializeNegatedAndRedispatch(ctx, ref a, ref b, XorContainers);
        if (b.Type == ContainerType.Negated)
            return MaterializeNegatedAndRedispatch(ctx, ref b, ref a, (c, ref x, ref y) => XorContainers(c, ref y, ref x));

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

        int ai = 0, bi = 0;

        while (ai < a.ContainerCount && bi < b.ContainerCount)
        {
            ref ContainerEntry ac = ref a.GetContainer(ai);
            ref ContainerEntry bc = ref b.GetContainer(bi);

            if (ac.Key < bc.Key)
            {
                result.AddContainer(CloneContainer(ctx, ref ac));
                ai++;
            }
            else if (ac.Key > bc.Key)
            {
                ai++;
            }
            else
            {
                ContainerEntry rc = AndNotContainers(ctx, ref ac, ref bc);
                if (rc.Cardinality > 0)
                    result.AddContainer(rc);
                else if (rc.Storage.HasValue)
                    ctx.Release(ref rc.Storage);

                ai++;
                bi++;
            }
        }

        // Remaining containers from a (nothing to subtract)
        while (ai < a.ContainerCount)
        {
            result.AddContainer(CloneContainer(ctx, ref a.GetContainer(ai)));
            ai++;
        }

        return result;
    }

    private static ContainerEntry AndNotContainers(ByteStringContext ctx, ref ContainerEntry a, ref ContainerEntry b)
    {
        // X ANDNOT Full = Empty
        if (b.Type == ContainerType.Full)
            return default;

        // Full ANDNOT X = NOT X
        if (a.Type == ContainerType.Full)
            return NotContainer(ctx, ref b);

        // Materialize Run/Negated to Bitmap, then re-dispatch
        // For ANDNOT, the order matters: we materialize whichever side needs it
        if (a.Type == ContainerType.Run)
            return MaterializeAndRedispatch(ctx, ref a, ref b, AndNotContainers);
        if (a.Type == ContainerType.Negated)
            return MaterializeNegatedAndRedispatch(ctx, ref a, ref b, AndNotContainers);
        if (b.Type == ContainerType.Run)
        {
            // Materialize b, keeping a in first position
            return MaterializeAndRedispatch(ctx, ref b, ref a, (c, ref matB, ref origA) => AndNotContainers(c, ref origA, ref matB));
        }
        if (b.Type == ContainerType.Negated)
        {
            return MaterializeNegatedAndRedispatch(ctx, ref b, ref a, (c, ref matB, ref origA) => AndNotContainers(c, ref origA, ref matB));
        }

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

    #region NOT (Complement within container)

    private static ContainerEntry NotContainer(ByteStringContext ctx, ref ContainerEntry entry)
    {
        switch (entry.Type)
        {
            case ContainerType.Full:
                return default; // NOT Full = Empty

            case ContainerType.Array:
            {
                // NOT(Array) = Negated with the same data (the set values become the absent values)
                int newCardinality = RoaringBitmap.BitsPerContainer - entry.Cardinality;
                int dataSize = Math.Max(64, entry.Cardinality * sizeof(ushort));
                ctx.Allocate(dataSize, out ByteString storage);
                Unsafe.CopyBlockUnaligned(storage.Ptr, entry.Data, (uint)(entry.Cardinality * sizeof(ushort)));

                return new ContainerEntry
                {
                    Key = entry.Key,
                    Data = storage.Ptr,
                    Cardinality = newCardinality,
                    Storage = storage,
                    Type = ContainerType.Negated
                };
            }

            case ContainerType.Negated:
            {
                // NOT(Negated) = Array with the same data (the absent values become the set values)
                int absentCount = RoaringBitmap.BitsPerContainer - entry.Cardinality;
                int dataSize = Math.Max(64, absentCount * sizeof(ushort));
                ctx.Allocate(dataSize, out ByteString storage);
                Unsafe.CopyBlockUnaligned(storage.Ptr, entry.Data, (uint)(absentCount * sizeof(ushort)));

                return new ContainerEntry
                {
                    Key = entry.Key,
                    Data = storage.Ptr,
                    Cardinality = absentCount,
                    Storage = storage,
                    Type = ContainerType.Array
                };
            }

            case ContainerType.Bitmap:
            {
                ctx.Allocate(RoaringBitmap.BitmapContainerSizeInBytes, out ByteString storage);
                ulong* src = (ulong*)entry.Data;
                ulong* dst = (ulong*)storage.Ptr;

                int cardinality = BitmapNotSimd(src, dst, RoaringBitmap.BitmapContainerSizeInUlongs);

                var result = new ContainerEntry
                {
                    Key = entry.Key,
                    Data = storage.Ptr,
                    Cardinality = cardinality,
                    Storage = storage,
                    Type = ContainerType.Bitmap
                };

                OptimizeBitmapResult(ctx, ref result);
                return result;
            }

            case ContainerType.Run:
            {
                ctx.Allocate(RoaringBitmap.BitmapContainerSizeInBytes, out ByteString storage);
                ulong* bitmap = (ulong*)storage.Ptr;
                new Span<byte>(bitmap, RoaringBitmap.BitmapContainerSizeInBytes).Fill(0xFF);

                ctx.Allocate(RoaringBitmap.BitmapContainerSizeInBytes, out ByteString tempStorage);
                ulong* tempBitmap = (ulong*)tempStorage.Ptr;
                RoaringBitmap.RunToBitmap(entry.Data, tempBitmap);

                int cardinality = BitmapAndNotSimd(bitmap, tempBitmap, bitmap, RoaringBitmap.BitmapContainerSizeInUlongs);
                ctx.Release(ref tempStorage);

                var result = new ContainerEntry
                {
                    Key = entry.Key,
                    Data = storage.Ptr,
                    Cardinality = cardinality,
                    Storage = storage,
                    Type = ContainerType.Bitmap
                };

                OptimizeBitmapResult(ctx, ref result);
                return result;
            }

            default:
                return default;
        }
    }

    #endregion

    #region SIMD Bitmap Operations

    /// <summary>
    /// AND two bitmap containers using SIMD. Returns popcount of result.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static int BitmapAndSimd(ulong* a, ulong* b, ulong* dst, int count)
    {
        if (AdvInstructionSet.IsAcceleratedVector512)
            return BitmapAndVector512(a, b, dst, count);
        if (AdvInstructionSet.IsAcceleratedVector256)
            return BitmapAndVector256(a, b, dst, count);
        if (AdvInstructionSet.IsAcceleratedVector128)
            return BitmapAndVector128(a, b, dst, count);
        return BitmapAndScalar(a, b, dst, count);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static int BitmapOrSimd(ulong* a, ulong* b, ulong* dst, int count)
    {
        if (AdvInstructionSet.IsAcceleratedVector512)
            return BitmapOrVector512(a, b, dst, count);
        if (AdvInstructionSet.IsAcceleratedVector256)
            return BitmapOrVector256(a, b, dst, count);
        if (AdvInstructionSet.IsAcceleratedVector128)
            return BitmapOrVector128(a, b, dst, count);
        return BitmapOrScalar(a, b, dst, count);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static int BitmapXorSimd(ulong* a, ulong* b, ulong* dst, int count)
    {
        if (AdvInstructionSet.IsAcceleratedVector512)
            return BitmapXorVector512(a, b, dst, count);
        if (AdvInstructionSet.IsAcceleratedVector256)
            return BitmapXorVector256(a, b, dst, count);
        if (AdvInstructionSet.IsAcceleratedVector128)
            return BitmapXorVector128(a, b, dst, count);
        return BitmapXorScalar(a, b, dst, count);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static int BitmapAndNotSimd(ulong* a, ulong* b, ulong* dst, int count)
    {
        if (AdvInstructionSet.IsAcceleratedVector512)
            return BitmapAndNotVector512(a, b, dst, count);
        if (AdvInstructionSet.IsAcceleratedVector256)
            return BitmapAndNotVector256(a, b, dst, count);
        if (AdvInstructionSet.IsAcceleratedVector128)
            return BitmapAndNotVector128(a, b, dst, count);
        return BitmapAndNotScalar(a, b, dst, count);
    }

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

    #region Vector512 Paths

    private static int BitmapAndVector512(ulong* a, ulong* b, ulong* dst, int count)
    {
        int N = Vector512<ulong>.Count; // 8 ulongs = 512 bits
        int cardinality = 0;
        int i = 0;

        for (; i + N <= count; i += N)
        {
            Vector512<ulong> va = Vector512.Load(a + i);
            Vector512<ulong> vb = Vector512.Load(b + i);
            Vector512<ulong> result = va & vb;
            result.Store(dst + i);

            // Popcount the result
            for (int j = 0; j < N; j++)
                cardinality += BitOperations.PopCount(dst[i + j]);
        }

        for (; i < count; i++)
        {
            dst[i] = a[i] & b[i];
            cardinality += BitOperations.PopCount(dst[i]);
        }

        return cardinality;
    }

    private static int BitmapOrVector512(ulong* a, ulong* b, ulong* dst, int count)
    {
        int N = Vector512<ulong>.Count;
        int cardinality = 0;
        int i = 0;

        for (; i + N <= count; i += N)
        {
            Vector512<ulong> va = Vector512.Load(a + i);
            Vector512<ulong> vb = Vector512.Load(b + i);
            Vector512<ulong> result = va | vb;
            result.Store(dst + i);

            for (int j = 0; j < N; j++)
                cardinality += BitOperations.PopCount(dst[i + j]);
        }

        for (; i < count; i++)
        {
            dst[i] = a[i] | b[i];
            cardinality += BitOperations.PopCount(dst[i]);
        }

        return cardinality;
    }

    private static int BitmapXorVector512(ulong* a, ulong* b, ulong* dst, int count)
    {
        int N = Vector512<ulong>.Count;
        int cardinality = 0;
        int i = 0;

        for (; i + N <= count; i += N)
        {
            Vector512<ulong> va = Vector512.Load(a + i);
            Vector512<ulong> vb = Vector512.Load(b + i);
            Vector512<ulong> result = va ^ vb;
            result.Store(dst + i);

            for (int j = 0; j < N; j++)
                cardinality += BitOperations.PopCount(dst[i + j]);
        }

        for (; i < count; i++)
        {
            dst[i] = a[i] ^ b[i];
            cardinality += BitOperations.PopCount(dst[i]);
        }

        return cardinality;
    }

    private static int BitmapAndNotVector512(ulong* a, ulong* b, ulong* dst, int count)
    {
        int N = Vector512<ulong>.Count;
        int cardinality = 0;
        int i = 0;

        for (; i + N <= count; i += N)
        {
            Vector512<ulong> va = Vector512.Load(a + i);
            Vector512<ulong> vb = Vector512.Load(b + i);
            Vector512<ulong> result = Vector512.AndNot(va, vb); // AndNot(a, b) = a & ~b
            result.Store(dst + i);

            for (int j = 0; j < N; j++)
                cardinality += BitOperations.PopCount(dst[i + j]);
        }

        for (; i < count; i++)
        {
            dst[i] = a[i] & ~b[i];
            cardinality += BitOperations.PopCount(dst[i]);
        }

        return cardinality;
    }

    #endregion

    #region Vector256 Paths

    private static int BitmapAndVector256(ulong* a, ulong* b, ulong* dst, int count)
    {
        int N = Vector256<ulong>.Count; // 4 ulongs = 256 bits
        int cardinality = 0;
        int i = 0;

        for (; i + N <= count; i += N)
        {
            Vector256<ulong> va = Vector256.Load(a + i);
            Vector256<ulong> vb = Vector256.Load(b + i);
            Vector256<ulong> result = va & vb;
            result.Store(dst + i);

            for (int j = 0; j < N; j++)
                cardinality += BitOperations.PopCount(dst[i + j]);
        }

        for (; i < count; i++)
        {
            dst[i] = a[i] & b[i];
            cardinality += BitOperations.PopCount(dst[i]);
        }

        return cardinality;
    }

    private static int BitmapOrVector256(ulong* a, ulong* b, ulong* dst, int count)
    {
        int N = Vector256<ulong>.Count;
        int cardinality = 0;
        int i = 0;

        for (; i + N <= count; i += N)
        {
            Vector256<ulong> va = Vector256.Load(a + i);
            Vector256<ulong> vb = Vector256.Load(b + i);
            Vector256<ulong> result = va | vb;
            result.Store(dst + i);

            for (int j = 0; j < N; j++)
                cardinality += BitOperations.PopCount(dst[i + j]);
        }

        for (; i < count; i++)
        {
            dst[i] = a[i] | b[i];
            cardinality += BitOperations.PopCount(dst[i]);
        }

        return cardinality;
    }

    private static int BitmapXorVector256(ulong* a, ulong* b, ulong* dst, int count)
    {
        int N = Vector256<ulong>.Count;
        int cardinality = 0;
        int i = 0;

        for (; i + N <= count; i += N)
        {
            Vector256<ulong> va = Vector256.Load(a + i);
            Vector256<ulong> vb = Vector256.Load(b + i);
            Vector256<ulong> result = va ^ vb;
            result.Store(dst + i);

            for (int j = 0; j < N; j++)
                cardinality += BitOperations.PopCount(dst[i + j]);
        }

        for (; i < count; i++)
        {
            dst[i] = a[i] ^ b[i];
            cardinality += BitOperations.PopCount(dst[i]);
        }

        return cardinality;
    }

    private static int BitmapAndNotVector256(ulong* a, ulong* b, ulong* dst, int count)
    {
        int N = Vector256<ulong>.Count;
        int cardinality = 0;
        int i = 0;

        for (; i + N <= count; i += N)
        {
            Vector256<ulong> va = Vector256.Load(a + i);
            Vector256<ulong> vb = Vector256.Load(b + i);
            Vector256<ulong> result = Vector256.AndNot(va, vb);
            result.Store(dst + i);

            for (int j = 0; j < N; j++)
                cardinality += BitOperations.PopCount(dst[i + j]);
        }

        for (; i < count; i++)
        {
            dst[i] = a[i] & ~b[i];
            cardinality += BitOperations.PopCount(dst[i]);
        }

        return cardinality;
    }

    #endregion

    #region Vector128 Paths

    private static int BitmapAndVector128(ulong* a, ulong* b, ulong* dst, int count)
    {
        int N = Vector128<ulong>.Count; // 2 ulongs = 128 bits
        int cardinality = 0;
        int i = 0;

        for (; i + N <= count; i += N)
        {
            Vector128<ulong> va = Vector128.Load(a + i);
            Vector128<ulong> vb = Vector128.Load(b + i);
            Vector128<ulong> result = va & vb;
            result.Store(dst + i);

            for (int j = 0; j < N; j++)
                cardinality += BitOperations.PopCount(dst[i + j]);
        }

        for (; i < count; i++)
        {
            dst[i] = a[i] & b[i];
            cardinality += BitOperations.PopCount(dst[i]);
        }

        return cardinality;
    }

    private static int BitmapOrVector128(ulong* a, ulong* b, ulong* dst, int count)
    {
        int N = Vector128<ulong>.Count;
        int cardinality = 0;
        int i = 0;

        for (; i + N <= count; i += N)
        {
            Vector128<ulong> va = Vector128.Load(a + i);
            Vector128<ulong> vb = Vector128.Load(b + i);
            Vector128<ulong> result = va | vb;
            result.Store(dst + i);

            for (int j = 0; j < N; j++)
                cardinality += BitOperations.PopCount(dst[i + j]);
        }

        for (; i < count; i++)
        {
            dst[i] = a[i] | b[i];
            cardinality += BitOperations.PopCount(dst[i]);
        }

        return cardinality;
    }

    private static int BitmapXorVector128(ulong* a, ulong* b, ulong* dst, int count)
    {
        int N = Vector128<ulong>.Count;
        int cardinality = 0;
        int i = 0;

        for (; i + N <= count; i += N)
        {
            Vector128<ulong> va = Vector128.Load(a + i);
            Vector128<ulong> vb = Vector128.Load(b + i);
            Vector128<ulong> result = va ^ vb;
            result.Store(dst + i);

            for (int j = 0; j < N; j++)
                cardinality += BitOperations.PopCount(dst[i + j]);
        }

        for (; i < count; i++)
        {
            dst[i] = a[i] ^ b[i];
            cardinality += BitOperations.PopCount(dst[i]);
        }

        return cardinality;
    }

    private static int BitmapAndNotVector128(ulong* a, ulong* b, ulong* dst, int count)
    {
        int N = Vector128<ulong>.Count;
        int cardinality = 0;
        int i = 0;

        for (; i + N <= count; i += N)
        {
            Vector128<ulong> va = Vector128.Load(a + i);
            Vector128<ulong> vb = Vector128.Load(b + i);
            Vector128<ulong> result = Vector128.AndNot(va, vb);
            result.Store(dst + i);

            for (int j = 0; j < N; j++)
                cardinality += BitOperations.PopCount(dst[i + j]);
        }

        for (; i < count; i++)
        {
            dst[i] = a[i] & ~b[i];
            cardinality += BitOperations.PopCount(dst[i]);
        }

        return cardinality;
    }

    #endregion

    #region Scalar Paths

    internal static int BitmapAndScalar(ulong* a, ulong* b, ulong* dst, int count)
    {
        int cardinality = 0;
        for (int i = 0; i < count; i++)
        {
            dst[i] = a[i] & b[i];
            cardinality += BitOperations.PopCount(dst[i]);
        }
        return cardinality;
    }

    internal static int BitmapOrScalar(ulong* a, ulong* b, ulong* dst, int count)
    {
        int cardinality = 0;
        for (int i = 0; i < count; i++)
        {
            dst[i] = a[i] | b[i];
            cardinality += BitOperations.PopCount(dst[i]);
        }
        return cardinality;
    }

    internal static int BitmapXorScalar(ulong* a, ulong* b, ulong* dst, int count)
    {
        int cardinality = 0;
        for (int i = 0; i < count; i++)
        {
            dst[i] = a[i] ^ b[i];
            cardinality += BitOperations.PopCount(dst[i]);
        }
        return cardinality;
    }

    internal static int BitmapAndNotScalar(ulong* a, ulong* b, ulong* dst, int count)
    {
        int cardinality = 0;
        for (int i = 0; i < count; i++)
        {
            dst[i] = a[i] & ~b[i];
            cardinality += BitOperations.PopCount(dst[i]);
        }
        return cardinality;
    }

    #endregion

    #endregion

    #region Helpers

    private delegate ContainerEntry ContainerOp(ByteStringContext ctx, ref ContainerEntry a, ref ContainerEntry b);

    /// <summary>
    /// Materialize a Run container to a temporary Bitmap, run the operation, release the temp.
    /// The materialized entry is passed as the first argument to the operation.
    /// </summary>
    private static ContainerEntry MaterializeAndRedispatch(
        ByteStringContext ctx, ref ContainerEntry runEntry, ref ContainerEntry other, ContainerOp op)
    {
        ctx.Allocate(RoaringBitmap.BitmapContainerSizeInBytes, out ByteString tempStorage);
        RoaringBitmap.RunToBitmap(runEntry.Data, (ulong*)tempStorage.Ptr);

        var tempEntry = new ContainerEntry
        {
            Key = runEntry.Key,
            Data = tempStorage.Ptr,
            Cardinality = runEntry.Cardinality,
            Storage = tempStorage,
            Type = ContainerType.Bitmap
        };

        var result = op(ctx, ref tempEntry, ref other);
        ctx.Release(ref tempStorage);
        return result;
    }

    /// <summary>
    /// Materialize a Negated container to a temporary Bitmap, run the operation, release the temp.
    /// </summary>
    private static ContainerEntry MaterializeNegatedAndRedispatch(
        ByteStringContext ctx, ref ContainerEntry negatedEntry, ref ContainerEntry other, ContainerOp op)
    {
        var tempEntry = MaterializeNegatedAsBitmap(ctx, ref negatedEntry);
        var tempStorage = tempEntry.Storage;

        var result = op(ctx, ref tempEntry, ref other);
        ctx.Release(ref tempStorage);
        return result;
    }

    internal static ContainerEntry CloneContainer(ByteStringContext ctx, ref ContainerEntry entry)
    {
        if (entry.Type == ContainerType.Full)
        {
            return new ContainerEntry
            {
                Key = entry.Key,
                Type = ContainerType.Full,
                Cardinality = RoaringBitmap.BitsPerContainer,
                Data = null,
                Storage = default
            };
        }

        int dataSize = entry.Type switch
        {
            ContainerType.Array => Math.Max(64, entry.Cardinality * sizeof(ushort)),
            ContainerType.Bitmap => RoaringBitmap.BitmapContainerSizeInBytes,
            ContainerType.Run => GetRunContainerDataSize(entry.Data),
            ContainerType.Negated => Math.Max(64, RoaringBitmap.NegatedContainerAbsentCount(entry.Cardinality) * sizeof(ushort)),
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

    private static int GetRunContainerDataSize(byte* data)
    {
        ushort numRuns = *(ushort*)data;
        return 2 + numRuns * 4; // header + pairs
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

    private static void ConvertResultBitmapToNegated(ByteStringContext ctx, ref ContainerEntry entry)
    {
        ulong* bitmap = (ulong*)entry.Data;
        int absentCount = RoaringBitmap.BitsPerContainer - entry.Cardinality;

        ctx.Allocate(Math.Max(64, absentCount * sizeof(ushort)), out ByteString newStorage);
        ushort* arr = (ushort*)newStorage.Ptr;

        int count = 0;
        for (int wordIdx = 0; wordIdx < RoaringBitmap.BitmapContainerSizeInUlongs; wordIdx++)
        {
            ulong word = ~bitmap[wordIdx];
            while (word != 0)
            {
                int bit = BitOperations.TrailingZeroCount(word);
                arr[count++] = (ushort)(wordIdx * 64 + bit);
                word &= word - 1;
            }
        }

        ctx.Release(ref entry.Storage);
        entry.Storage = newStorage;
        entry.Data = newStorage.Ptr;
        entry.Type = ContainerType.Negated;
    }

    /// <summary>
    /// Convert a bitmap result to the optimal container type based on cardinality.
    /// </summary>
    private static void OptimizeBitmapResult(ByteStringContext ctx, ref ContainerEntry entry)
    {
        if (entry.Cardinality == RoaringBitmap.BitsPerContainer)
        {
            ctx.Release(ref entry.Storage);
            entry.Storage = default;
            entry.Data = null;
            entry.Type = ContainerType.Full;
        }
        else if (entry.Cardinality > RoaringBitmap.NegatedArrayMinCardinality)
        {
            ConvertResultBitmapToNegated(ctx, ref entry);
        }
        else if (entry.Cardinality <= RoaringBitmap.ArrayContainerMaxCardinality && entry.Cardinality > 0)
        {
            ConvertResultBitmapToArray(ctx, ref entry);
        }
    }

    /// <summary>
    /// Materialize a Negated container into a temporary bitmap for set operations.
    /// The caller must release the returned storage when done.
    /// </summary>
    private static ContainerEntry MaterializeNegatedAsBitmap(ByteStringContext ctx, ref ContainerEntry negated)
    {
        int absentCount = RoaringBitmap.BitsPerContainer - negated.Cardinality;
        ushort* absentArr = (ushort*)negated.Data;

        ctx.Allocate(RoaringBitmap.BitmapContainerSizeInBytes, out ByteString storage);
        ulong* bitmap = (ulong*)storage.Ptr;

        // Start with all bits set, then clear the absent ones
        new Span<byte>(bitmap, RoaringBitmap.BitmapContainerSizeInBytes).Fill(0xFF);

        for (int i = 0; i < absentCount; i++)
        {
            ushort val = absentArr[i];
            bitmap[val >> 6] &= ~(1UL << (val & 63));
        }

        return new ContainerEntry
        {
            Key = negated.Key,
            Data = storage.Ptr,
            Cardinality = negated.Cardinality,
            Storage = storage,
            Type = ContainerType.Bitmap
        };
    }

    #endregion
}
