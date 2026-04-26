using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using Sparrow;

namespace Corax.Utils.RoaringBitmaps;

/// <summary>
/// Forward iterator for RoaringBitmap that supports Corax's Fill(Span&lt;long&gt;) streaming pattern.
/// Maintains iteration state across calls, allowing the bitmap to be consumed in chunks.
/// </summary>
[StructLayout(LayoutKind.Explicit)]
public unsafe struct RoaringBitmapIterator
{
    [FieldOffset(0)] private int _containerIndex;
    [FieldOffset(4)] private int _positionInContainer;

    // Container-type-specific state — unioned since only one type is active at a time.
    // Bitmap: _wordIndex is the current ulong index (0..1023), _currentWord has remaining bits.
    // Run/Negated: _wordIndex is the current run/absent index, _runPosition is offset within run.
    [FieldOffset(8)] private int _wordIndex;
    [FieldOffset(12)] private int _runPosition;
    [FieldOffset(16)] private ulong _currentWord; // bitmap only — overlaps nothing

    public RoaringBitmapIterator()
    {
        _containerIndex = 0;
        _positionInContainer = 0;
        _wordIndex = 0;
        _runPosition = 0;
        _currentWord = 0;
    }

    /// <summary>
    /// Reset the iterator to the beginning.
    /// </summary>
    public void Reset()
    {
        _containerIndex = 0;
        _positionInContainer = 0;
        _wordIndex = 0;
        _runPosition = 0;
        _currentWord = 0;
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

            bool containerCompleted = _positionInContainer >= entry.Cardinality;

            if (containerCompleted)
            {
                _containerIndex++;
                _positionInContainer = 0;
                _wordIndex = 0;
                _runPosition = 0;
                _currentWord = 0;
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
        int bitmapPopulated = 0;

        while (written < buffer.Length && _wordIndex < RoaringBitmap.BitmapContainerSizeInUlongs)
        {
            // Load the next word if the current one is exhausted
            if (_currentWord == 0)
            {
                _currentWord = bitmap[_wordIndex];
                if (_currentWord == 0)
                {
                    _wordIndex++;
                    continue;
                }
            }

            // Process set bits in the current word
            while (_currentWord != 0 && written < buffer.Length)
            {
                int bit = BitOperations.TrailingZeroCount(_currentWord);
                buffer[written++] = baseValue | (uint)(_wordIndex * 64 + bit);
                _currentWord &= _currentWord - 1; // clear lowest set bit
                bitmapPopulated++;
            }

            // Advance to the next word if this one is fully consumed
            if (_currentWord == 0)
                _wordIndex++;
        }

        _positionInContainer += bitmapPopulated;
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
