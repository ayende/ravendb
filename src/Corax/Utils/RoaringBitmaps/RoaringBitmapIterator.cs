using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using Sparrow;

namespace Corax.Utils.RoaringBitmaps;

/// <summary>
/// Forward iterator for RoaringBitmap that supports Corax's Fill(Span&lt;long&gt;) streaming pattern.
/// Maintains iteration state across calls, allowing the bitmap to be consumed in chunks.
/// </summary>
/// <summary>
/// 24 bytes total. _containerIndex and _positionInContainer are shared across all types.
/// _bitmapWordIndex and _bitmapCurrentWord are only used for Bitmap containers.
/// Array and Range only use _positionInContainer.
/// </summary>
public unsafe struct RoaringBitmapIterator
{
    private int _containerIndex;       // current key in the index array
    private int _positionInContainer;  // Array: index into sorted array. Range: current value. Bitmap: emitted count.
    private int _bitmapWordIndex;      // Bitmap only: current ulong index (0..1023)
    private ulong _bitmapCurrentWord;  // Bitmap only: remaining bits in current word

    public RoaringBitmapIterator()
    {
        _containerIndex = 0;
        _positionInContainer = 0;
        _bitmapWordIndex = 0;
        _bitmapCurrentWord = 0;
    }

    /// <summary>
    /// Reset the iterator to the beginning.
    /// </summary>
    public void Reset()
    {
        _containerIndex = 0;
        _positionInContainer = 0;
        _bitmapWordIndex = 0;
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
            RoaringBitmap.EnsureSorted(ref entry);
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
            }

            bool containerCompleted = entry.Type == ContainerType.Bitmap
                ? _bitmapWordIndex >= RoaringBitmap.BitmapContainerSizeInUlongs && _bitmapCurrentWord == 0
                : _positionInContainer >= entry.Cardinality;

            if (containerCompleted)
            {
                _containerIndex++;
                _positionInContainer = 0;
                _bitmapWordIndex = 0;
                _bitmapCurrentWord = 0;
            }
        }

        return written;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int FillFromArray(ref ContainerEntry entry, long baseValue, Span<long> buffer, int written)
    {
        ushort* arr = (ushort*)entry.Data;
        int count = entry.Cardinality;
        int remaining = count - _positionInContainer;
        int space = buffer.Length - written;
        int toCopy = Math.Min(remaining, space);

        if (toCopy <= 0)
            return written;

        fixed (long* dst = &buffer[written])
        {
            // SIMD: widen 4 ushorts → 4 longs at a time, adding baseValue
            if (AdvInstructionSet.IsAcceleratedVector256 && toCopy >= 4)
            {
                Vector256<long> vBase = Vector256.Create(baseValue);
                int i = 0;
                for (; i + 4 <= toCopy; i += 4)
                {
                    // Load 4 ushorts, zero-extend to 4 longs
                    long v0 = arr[_positionInContainer + i];
                    long v1 = arr[_positionInContainer + i + 1];
                    long v2 = arr[_positionInContainer + i + 2];
                    long v3 = arr[_positionInContainer + i + 3];
                    Vector256<long> vals = Vector256.Create(v0, v1, v2, v3);
                    (vals | vBase).Store(dst + i);
                }

                // Scalar remainder
                for (; i < toCopy; i++)
                    dst[i] = baseValue | arr[_positionInContainer + i];
            }
            else
            {
                for (int i = 0; i < toCopy; i++)
                    dst[i] = baseValue | arr[_positionInContainer + i];
            }
        }

        _positionInContainer += toCopy;
        return written + toCopy;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int FillFromBitmap(ref ContainerEntry entry, long baseValue, Span<long> buffer, int written)
    {
        ulong* bitmap = (ulong*)entry.Data;

        while (written < buffer.Length && _bitmapWordIndex < RoaringBitmap.BitmapContainerSizeInUlongs)
        {
            if (_bitmapCurrentWord == 0)
            {
                _bitmapCurrentWord = bitmap[_bitmapWordIndex];
                if (_bitmapCurrentWord == 0)
                {
                    _bitmapWordIndex++;
                    continue;
                }
            }

            while (_bitmapCurrentWord != 0 && written < buffer.Length)
            {
                int bit = BitOperations.TrailingZeroCount(_bitmapCurrentWord);
                buffer[written++] = baseValue | (uint)(_bitmapWordIndex * 64 + bit);
                _bitmapCurrentWord &= _bitmapCurrentWord - 1;
            }

            if (_bitmapCurrentWord == 0)
                _bitmapWordIndex++;
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
            // SIMD: generate 4 sequential values at a time
            if (AdvInstructionSet.IsAcceleratedVector256 && toCopy >= 4)
            {
                Vector256<long> vCurrent = Vector256.Create(
                    baseValue + _positionInContainer,
                    baseValue + _positionInContainer + 1,
                    baseValue + _positionInContainer + 2,
                    baseValue + _positionInContainer + 3);
                Vector256<long> vStep = Vector256.Create(4L);

                int i = 0;
                for (; i + 4 <= toCopy; i += 4)
                {
                    vCurrent.Store(dst + i);
                    vCurrent += vStep;
                }

                _positionInContainer += i;

                // Scalar remainder
                for (; i < toCopy; i++)
                {
                    dst[i] = baseValue | (uint)_positionInContainer;
                    _positionInContainer++;
                }
            }
            else
            {
                for (int i = 0; i < toCopy; i++)
                {
                    dst[i] = baseValue | (uint)_positionInContainer;
                    _positionInContainer++;
                }
            }
        }

        return written + toCopy;
    }
}
