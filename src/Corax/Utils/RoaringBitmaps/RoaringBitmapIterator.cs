using System;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

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

        while (written < buffer.Length && _positionInContainer < count)
        {
            buffer[written++] = baseValue | arr[_positionInContainer];
            _positionInContainer++;
        }

        return written;
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
        while (written < buffer.Length && _positionInContainer < rangeEnd)
        {
            buffer[written++] = baseValue | (uint)_positionInContainer;
            _positionInContainer++;
        }

        return written;
    }
}
