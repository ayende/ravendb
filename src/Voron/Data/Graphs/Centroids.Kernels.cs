using System;
using System.Numerics.Tensors;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using System.Runtime.Intrinsics.X86;

namespace Voron.Data.Graphs;

public unsafe partial class Centroids
{
    internal static class Kernels
    {
        public const int RouterQueryRange = 127;
        public const int Code8QueryRange = 64;
        public const int Code4QueryRange = 127;

        /// <summary>
        /// Dot product of two int8 vectors whose values are in [-127, 127].
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int DotI8(sbyte* a, sbyte* b, int n)
        {
            int i = 0;
            int sum = 0;
            if (Avx2.IsSupported && n >= 32)
            {
                var ones = Vector256.Create((short)1);
                var acc = Vector256<int>.Zero;
                for (; i + 32 <= n; i += 32)
                {
                    var va = Avx.LoadVector256(a + i);
                    var vb = Avx.LoadVector256(b + i);
                    var products = Avx2.MultiplyAddAdjacent(Avx2.Abs(va), Avx2.Sign(vb, va));
                    acc = Avx2.Add(acc, Avx2.MultiplyAddAdjacent(products, ones));
                }

                sum = Vector256.Sum(acc);
            }
            else if (Vector128.IsHardwareAccelerated && n >= 16)
            {
                var acc = Vector128<int>.Zero;
                for (; i + 16 <= n; i += 16)
                {
                    var (aLo, aHi) = Vector128.Widen(Vector128.Load(a + i));
                    var (bLo, bHi) = Vector128.Widen(Vector128.Load(b + i));
                    var (p0, p1) = Vector128.Widen(aLo * bLo);
                    var (p2, p3) = Vector128.Widen(aHi * bHi);
                    acc += (p0 + p1) + (p2 + p3);
                }

                sum = Vector128.Sum(acc);
            }

            for (; i < n; i++)
                sum += a[i] * b[i];
            return sum;
        }

        /// <summary>
        /// Dot product of 8 bits codes with an int8 vector whose values are in [-64, 64], so the pairwise
        /// sums of <c>vpmaddubsw</c> cannot saturate.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int DotU8I8(byte* codes, sbyte* q, int n)
        {
            int i = 0;
            int sum = 0;
            if (Avx2.IsSupported && n >= 32)
            {
                var ones = Vector256.Create((short)1);
                var acc = Vector256<int>.Zero;
                for (; i + 32 <= n; i += 32)
                {
                    var products = Avx2.MultiplyAddAdjacent(Avx.LoadVector256(codes + i), Avx.LoadVector256(q + i));
                    acc = Avx2.Add(acc, Avx2.MultiplyAddAdjacent(products, ones));
                }

                sum = Vector256.Sum(acc);
            }
            else if (Vector128.IsHardwareAccelerated && n >= 16)
            {
                var acc = Vector128<int>.Zero;
                for (; i + 16 <= n; i += 16)
                {
                    var (cLo, cHi) = Vector128.Widen(Vector128.Load(codes + i));
                    var (qLo, qHi) = Vector128.Widen(Vector128.Load(q + i));
                    var (p0, p1) = Vector128.Widen(cLo.AsInt16() * qLo);
                    var (p2, p3) = Vector128.Widen(cHi.AsInt16() * qHi);
                    acc += (p0 + p1) + (p2 + p3);
                }

                sum = Vector128.Sum(acc);
            }

            for (; i < n; i++)
                sum += codes[i] * q[i];
            return sum;
        }

        /// <summary>
        /// Dot product of 4 bits codes with an int8 vector. Byte <c>j</c> holds dimension <c>j</c> in its low nibble and
        /// dimension <c>j + n</c> in its high nibble, <paramref name="qLow"/> and <paramref name="qHigh"/> are the matching
        /// halves of the query.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static int DotU4I8(byte* packed, sbyte* qLow, sbyte* qHigh, int n)
        {
            int i = 0;
            int sum = 0;
            if (Avx2.IsSupported && n >= 32)
            {
                var ones = Vector256.Create((short)1);
                var mask = Vector256.Create((byte)0x0F);
                var acc = Vector256<int>.Zero;
                for (; i + 32 <= n; i += 32)
                {
                    var bytes = Avx.LoadVector256(packed + i);
                    var low = Avx2.And(bytes, mask);
                    var high = Avx2.And(Avx2.ShiftRightLogical(bytes.AsUInt16(), 4).AsByte(), mask);
                    var products = Avx2.Add(Avx2.MultiplyAddAdjacent(low, Avx.LoadVector256(qLow + i)), Avx2.MultiplyAddAdjacent(high, Avx.LoadVector256(qHigh + i)));
                    acc = Avx2.Add(acc, Avx2.MultiplyAddAdjacent(products, ones));
                }

                sum = Vector256.Sum(acc);
            }
            else if (Vector128.IsHardwareAccelerated && n >= 16)
            {
                var mask = Vector128.Create((byte)0x0F);
                var acc = Vector128<int>.Zero;
                for (; i + 16 <= n; i += 16)
                {
                    var bytes = Vector128.Load(packed + i);
                    var (lLo, lHi) = Vector128.Widen(bytes & mask);
                    var (hLo, hHi) = Vector128.Widen(Vector128.ShiftRightLogical(bytes, 4));
                    var (qlLo, qlHi) = Vector128.Widen(Vector128.Load(qLow + i));
                    var (qhLo, qhHi) = Vector128.Widen(Vector128.Load(qHigh + i));
                    var (p0, p1) = Vector128.Widen(lLo.AsInt16() * qlLo + hLo.AsInt16() * qhLo);
                    var (p2, p3) = Vector128.Widen(lHi.AsInt16() * qlHi + hHi.AsInt16() * qhHi);
                    acc += (p0 + p1) + (p2 + p3);
                }

                sum = Vector128.Sum(acc);
            }

            for (; i < n; i++)
            {
                var b = packed[i];
                sum += (b & 0x0F) * qLow[i] + (b >> 4) * qHigh[i];
            }

            return sum;
        }

        /// <summary>
        /// Quantizes <paramref name="src"/> into <paramref name="dst"/> with values in [-range, range].
        /// Returns the scale, so that src[i] ~= scale * dst[i].
        /// </summary>
        public static float QuantizeI8(ReadOnlySpan<float> src, sbyte* dst, int range)
        {
            var maxAbs = TensorPrimitives.MaxMagnitude(src);
            maxAbs = MathF.Abs(maxAbs);
            if (maxAbs == 0)
            {
                new Span<sbyte>(dst, src.Length).Clear();
                return 0;
            }

            var scaled = System.Buffers.ArrayPool<float>.Shared.Rent(src.Length);
            var span = scaled.AsSpan(0, src.Length);
            TensorPrimitives.Multiply(src, range / maxAbs, span);
            TensorPrimitives.Round(span, span);
            TensorPrimitives.ConvertTruncating<float, sbyte>(span, new Span<sbyte>(dst, src.Length));
            System.Buffers.ArrayPool<float>.Shared.Return(scaled);
            return maxAbs / range;
        }

        /// <summary>
        /// Encodes the residual <c>x - c</c> as <paramref name="bits"/> bits per dimension, such that
        /// <c>x[i] ~= c[i] + lo + step * code[i]</c>.
        /// </summary>
        public static void Encode(ReadOnlySpan<float> x, ReadOnlySpan<float> c, Span<float> residual, int bits, byte* code, out float lo, out float step)
        {
            TensorPrimitives.Subtract(x, c, residual);
            lo = TensorPrimitives.Min(residual);
            var hi = TensorPrimitives.Max(residual);
            int levels = (1 << bits) - 1;
            step = (hi - lo) / levels;
            var inv = step > 0 ? 1f / step : 0f;

            TensorPrimitives.Subtract(residual, lo, residual);
            TensorPrimitives.Multiply(residual, inv, residual);
            TensorPrimitives.Round(residual, residual);

            int dims = residual.Length;
            if (bits == 8)
            {
                TensorPrimitives.ConvertTruncating<float, byte>(residual, new Span<byte>(code, dims));
                return;
            }

            int half = (dims + 1) / 2;
            for (int i = 0; i < half; i++)
            {
                var j = i + half;
                var high = j < dims ? (int)residual[j] : 0;
                code[i] = (byte)((int)residual[i] | (high << 4));
            }
        }
    }
}
