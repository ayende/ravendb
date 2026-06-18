using System;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using Sparrow.Server;

namespace Voron.Data.RoaringBitmaps;

public unsafe partial struct RoaringBitmap
{
    /// <summary>
    /// Segregated storage free list: one intrusive singly-linked list head per size class.
    ///
    /// Storage blocks are bucketed into <see cref="NumSizeClasses"/> size classes on a 32-byte
    /// aligned x1.091 geometric ladder spanning <see cref="InitialArrayContainerSizeInBytes"/> (64)
    /// .. <see cref="BitmapContainerSizeInBytes"/> (8192). Allocations round their requested byte
    /// count up to the enclosing class size, so every block in a class is exactly
    /// <see cref="ClassSize"/>[c] bytes: a recycled block always fits any request mapping to its class,
    /// the class is recovered on free from its Length, and every Length stays a multiple of
    /// <see cref="SimdAlignment"/> (32) as the array-container SIMD set ops require. Worst-case internal
    /// fragmentation is the x1.091 step (~9%); measured waste is ~4%.
    ///
    /// Each free block's first <c>sizeof(ByteString)</c> bytes hold the next <see cref="ByteString"/> in
    /// its class chain (a default head means the class is empty). <see cref="_classMask"/> bit <c>c</c> is
    /// set iff class <c>c</c> is non-empty, so <see cref="Allocate"/> finds the smallest sufficient class
    /// in O(1) via <see cref="BitOperations.TrailingZeroCount(ulong)"/> rather than a best-fit walk. The
    /// struct lives inline in <see cref="RoaringBitmap"/> and is swapped wholesale by <see cref="SwapContents"/>.
    /// </summary>
    internal unsafe struct FreeListHeads
    {
        private const int NumSizeClasses = 42;

        /// <summary>Byte size of each storage class (32-aligned, strictly increasing).</summary>
        private static readonly int[] ClassSize =
        [
            64, 96, 128, 160, 192, 224, 256, 288, 320, 352, 416, 480, 544, 608, 672, 736, 832, 928,
            1024, 1120, 1248, 1376, 1504, 1664, 1824, 2016, 2208, 2432, 2656, 2912, 3200, 3520, 3872,
            4256, 4672, 5120, 5600, 6112, 6688, 7328, 8000, 8192
        ];

        /// <summary>Maps a 32-byte quantum (<c>bytes / 32</c>) to the smallest class whose size covers it.
        /// Indexed by <c>(neededBytes + 31) &gt;&gt; 5</c>; covers quanta 0..256 (0..8192 bytes).</summary>
        private static ReadOnlySpan<byte> ClassOfQuantum =>
        [
            0, 0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 10, 11, 11, 12, 12, 13, 13, 14, 14, 15, 15, 16,
            16, 16, 17, 17, 17, 18, 18, 18, 19, 19, 19, 20, 20, 20, 20, 21, 21, 21, 21, 22, 22, 22,
            22, 23, 23, 23, 23, 23, 24, 24, 24, 24, 24, 25, 25, 25, 25, 25, 25, 26, 26, 26, 26, 26,
            26, 27, 27, 27, 27, 27, 27, 27, 28, 28, 28, 28, 28, 28, 28, 29, 29, 29, 29, 29, 29, 29,
            29, 30, 30, 30, 30, 30, 30, 30, 30, 30, 31, 31, 31, 31, 31, 31, 31, 31, 31, 31, 32, 32,
            32, 32, 32, 32, 32, 32, 32, 32, 32, 33, 33, 33, 33, 33, 33, 33, 33, 33, 33, 33, 33, 34,
            34, 34, 34, 34, 34, 34, 34, 34, 34, 34, 34, 34, 35, 35, 35, 35, 35, 35, 35, 35, 35, 35,
            35, 35, 35, 35, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 36, 37, 37, 37,
            37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 37, 38, 38, 38, 38, 38, 38, 38, 38, 38,
            38, 38, 38, 38, 38, 38, 38, 38, 38, 39, 39, 39, 39, 39, 39, 39, 39, 39, 39, 39, 39, 39,
            39, 39, 39, 39, 39, 39, 39, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40, 40,
            40, 40, 40, 40, 40, 40, 41, 41, 41, 41, 41, 41
        ];

        /// <summary>One free-list head per size class, stored inline (no extra allocation).</summary>
        [InlineArray(NumSizeClasses)]
        private struct Heads
        {
            private ByteString _head0;
        }

        private Heads _heads;

        /// <summary>Bit <c>c</c> set iff <see cref="_heads"/>[c] is non-empty. 42 classes fit one ulong.</summary>
        private ulong _classMask;

        /// <summary>Smallest size class whose <see cref="ClassSize"/> is &gt;= <paramref name="bytes"/>.</summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static int SizeClassFor(int bytes)
        {
            Debug.Assert(bytes is >= 0 and <= BitmapContainerSizeInBytes, "storage request out of size-class range");
            return ClassOfQuantum[(bytes + SimdAlignment - 1) >> 5];
        }

        /// <summary>
        /// Push <paramref name="bs"/> onto the head of its size class's free list. The first
        /// <c>sizeof(ByteString)</c> bytes of <paramref name="bs"/>'s data are overwritten with the
        /// current head, forming the intrusive next-pointer. The class is recovered from
        /// <c>bs.Length</c>, which is always exactly a <see cref="ClassSize"/> value.
        /// </summary>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Return(ByteString bs)
        {
            int c = SizeClassFor(bs.Length);
            *(ByteString*)bs.Ptr = _heads[c]; // chain: new node's next = old class head
            _heads[c] = bs;                   // new class head = bs
            _classMask |= 1UL << c;
        }

        /// <summary>
        /// Acquire storage of at least <paramref name="neededBytes"/>. Pops the head of the smallest
        /// non-empty size class that can satisfy the request (O(1) via the class mask), unlinking it
        /// in-place. Falls back to a fresh <paramref name="ctx"/> allocation rounded up to the class
        /// size when every sufficient class is empty.
        /// </summary>
        public void Allocate(ByteStringContext ctx, int neededBytes, out ByteString storage)
        {
            int c = SizeClassFor(neededBytes);

            // Classes >= c hold blocks of ClassSize[>=c] >= neededBytes; pick the smallest non-empty one.
            ulong avail = _classMask & (~0UL << c);
            if (avail != 0)
            {
                int fc = BitOperations.TrailingZeroCount(avail);
                storage = _heads[fc];                  // pop head
                ByteString next = *(ByteString*)storage.Ptr;
                _heads[fc] = next;
                if (next.HasValue == false)
                    _classMask &= ~(1UL << fc);        // class now empty
                return;
            }

            ctx.Allocate(ClassSize[c], out storage);
        }

        /// <summary>
        /// Release every storage parked on the segregated free lists back to <paramref name="ctx"/> —
        /// these are storages that were recycled via FreeContainer/Clear but never re-used before
        /// Dispose. Walks only the non-empty classes by consuming set bits of the class mask.
        /// </summary>
        public void ReleaseAll(ByteStringContext ctx)
        {
            ulong mask = _classMask;
            while (mask != 0)
            {
                int c = BitOperations.TrailingZeroCount(mask);
                mask &= mask - 1;

                ByteString freeNode = _heads[c];
                while (freeNode.HasValue)
                {
                    ByteString nextNode = *(ByteString*)freeNode.Ptr; // read next before releasing
                    ctx.Release(ref freeNode);
                    freeNode = nextNode;
                }
                _heads[c] = default;
            }
            _classMask = 0;
        }
    }
}
