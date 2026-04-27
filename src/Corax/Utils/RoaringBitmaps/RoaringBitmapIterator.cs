using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using Sparrow;

namespace Corax.Utils.RoaringBitmaps;

/// <summary>
/// 16 bytes. Forward iterator for RoaringBitmap supporting Fill(Span&lt;long&gt;) streaming.
/// _positionInContainer is shared: Array uses it as array index, Range as current value,
/// Bitmap as the current ulong word index (0..1023). _bitmapCurrentWord stores remaining
/// bits in the current word for bitmap iteration only.
/// </summary>
[StructLayout(LayoutKind.Explicit)]
public unsafe struct RoaringBitmapIterator
{
    [FieldOffset(0)]  private int _containerIndex;
    /// <summary>Array: index into sorted array. Range: current value. Bitmap: current ulong index (0..1023).</summary>
    [FieldOffset(4)]  private int _positionInContainer;
    [FieldOffset(8)]  private ulong _bitmapCurrentWord; // Bitmap only: remaining bits in current word

    public RoaringBitmapIterator()
    {
        _containerIndex = 0;
        _positionInContainer = 0;
        _bitmapCurrentWord = 0;
    }

    /// <summary>
    /// Reset the iterator to the beginning.
    /// </summary>
    public void Reset()
    {
        _containerIndex = 0;
        _positionInContainer = 0;
        _bitmapCurrentWord = 0;
    }

    /// <summary>
    /// Fill the buffer with the next batch of values from the bitmap.
    /// Returns the number of values written.
    /// </summary>
    public int Fill(ref RoaringBitmap bitmap, Span<long> buffer)
    {
        int written = 0;
        int indexLength = bitmap.IndexLength;

        while (written < buffer.Length && _containerIndex < indexLength)
        {
            // Skip absent keys in the index
            int slot = bitmap.GetSlotForKey(_containerIndex);
            if (slot < 0)
            {
                _containerIndex++;
                continue;
            }

            ref ContainerEntry entry = ref bitmap.GetEntryBySlot(slot);
            RoaringBitmap.AssertPrepared(ref entry);
            long baseValue = (long)_containerIndex << RoaringBitmap.ContainerKeyShift;

            switch (entry.Type)
            {
                case ContainerType.Array:
                    written = FillFromArray(ref entry, baseValue, buffer, written);
                    break;

                case ContainerType.Bitmap:
                    written = FillFromBitmap(ref entry, baseValue, buffer, written);
                    break;

                case ContainerType.Range:
                    written = FillFromRange(ref entry, baseValue, buffer, written);
                    break;

                case ContainerType.ArrayUnsorted:
                    throw new InvalidOperationException("Call PrepareForReading() before iterating.");
            }

            bool containerCompleted = entry.Type == ContainerType.Bitmap
                ? _positionInContainer >= RoaringBitmap.BitmapContainerSizeInUlongs && _bitmapCurrentWord == 0
                : _positionInContainer >= entry.Cardinality;

            if (containerCompleted)
            {
                _containerIndex++;
                _positionInContainer = 0;
                _bitmapCurrentWord = 0;
            }
        }

        return written;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int FillFromArray(ref ContainerEntry entry, long baseValue, Span<long> buffer, int written)
    {
        ushort* arr = entry.ArrayData;
        int count = entry.Cardinality;
        int remaining = count - _positionInContainer;
        int space = buffer.Length - written;
        int toCopy = Math.Min(remaining, space);

        if (toCopy <= 0)
            return written;

        fixed (long* dst = &buffer[written])
        {
            ushort* src = arr + _positionInContainer;
            int i = 0;

            // SIMD: widen 4 ushorts → 4 longs, OR with baseValue, store 4 longs.
            if (AdvInstructionSet.IsAcceleratedVector256 && toCopy >= 4)
            {
                Vector256<long> vBase = Vector256.Create(baseValue);
                for (; i + 4 <= toCopy; i += 4)
                {
                    Vector256<long> vals = Vector256.Create(
                        (long)src[i], (long)src[i + 1], (long)src[i + 2], (long)src[i + 3]);
                    (vals | vBase).Store(dst + i);
                }
            }

            // Scalar remainder (or all, if no SIMD)
            for (; i < toCopy; i++)
                dst[i] = baseValue | src[i];
        }

        _positionInContainer += toCopy;
        return written + toCopy;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int FillFromBitmap(ref ContainerEntry entry, long baseValue, Span<long> buffer, int written)
    {
        ulong* bitmap = entry.BitmapData;

        while (written < buffer.Length && _positionInContainer < RoaringBitmap.BitmapContainerSizeInUlongs)
        {
            if (_bitmapCurrentWord == 0)
            {
                _bitmapCurrentWord = bitmap[_positionInContainer];
                if (_bitmapCurrentWord == 0)
                {
                    _positionInContainer++;
                    continue;
                }
            }

            while (_bitmapCurrentWord != 0 && written < buffer.Length)
            {
                int bit = BitOperations.TrailingZeroCount(_bitmapCurrentWord);
                buffer[written++] = baseValue | (uint)(_positionInContainer * 64 + bit);
                _bitmapCurrentWord &= _bitmapCurrentWord - 1;
            }

            if (_bitmapCurrentWord == 0)
                _positionInContainer++;
        }

        return written;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int FillFromRange(ref ContainerEntry entry, long baseValue, Span<long> buffer, int written)
    {
        int rangeEnd = entry.Cardinality;
        int remaining = rangeEnd - _positionInContainer;
        int space = buffer.Length - written;
        int toCopy = Math.Min(remaining, space);

        if (toCopy <= 0)
            return written;

        fixed (long* dst = &buffer[written])
        {
            int i = 0;

            // SIMD: generate 4 sequential values at a time
            if (AdvInstructionSet.IsAcceleratedVector256 && toCopy >= 4)
            {
                Vector256<long> vCurrent = Vector256.Create(
                    baseValue + _positionInContainer,
                    baseValue + _positionInContainer + 1,
                    baseValue + _positionInContainer + 2,
                    baseValue + _positionInContainer + 3);
                Vector256<long> vStep = Vector256.Create(4L);

                for (; i + 4 <= toCopy; i += 4)
                {
                    vCurrent.Store(dst + i);
                    vCurrent += vStep;
                }
                _positionInContainer += i;
            }

            // Scalar remainder (or all, if no SIMD)
            for (; i < toCopy; i++)
            {
                dst[i] = baseValue | (uint)_positionInContainer;
                _positionInContainer++;
            }
        }

        return written + toCopy;
    }
}
