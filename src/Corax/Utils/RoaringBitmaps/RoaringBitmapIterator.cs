using System;
using System.Numerics;
using System.Runtime.CompilerServices;

namespace Corax.Utils.RoaringBitmaps;

/// <summary>
/// Forward iterator for RoaringBitmap that supports Corax's Fill(Span&lt;long&gt;) streaming pattern.
/// Maintains iteration state across calls, allowing the bitmap to be consumed in chunks.
/// </summary>
public unsafe struct RoaringBitmapIterator
{
    private int _containerIndex;
    private int _positionInContainer;

    // For bitmap container iteration: current word index and remaining bits
    private int _bitmapWordIndex;
    private ulong _bitmapCurrentWord;

    // For run container iteration: current run index and position within run
    private int _runIndex;
    private int _runPosition;

    public RoaringBitmapIterator()
    {
        _containerIndex = 0;
        _positionInContainer = 0;
        _bitmapWordIndex = 0;
        _bitmapCurrentWord = 0;
        _runIndex = 0;
        _runPosition = 0;
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
        _runIndex = 0;
        _runPosition = 0;
    }

    /// <summary>
    /// Fill the buffer with the next batch of values from the bitmap.
    /// Returns the number of values written.
    /// </summary>
    public int Fill(ref RoaringBitmap bitmap, Span<long> buffer)
    {
        int written = 0;

        while (written < buffer.Length && _containerIndex < bitmap.ContainerCount)
        {
            ref ContainerEntry entry = ref bitmap.GetContainer(_containerIndex);
            long baseValue = entry.Key << RoaringBitmap.ContainerKeyShift;

            switch (entry.Type)
            {
                case ContainerType.Array:
                    written = FillFromArray(ref entry, baseValue, buffer, written);
                    break;

                case ContainerType.Bitmap:
                    written = FillFromBitmap(ref entry, baseValue, buffer, written);
                    break;

                case ContainerType.Negated:
                    written = FillFromNegated(ref entry, baseValue, buffer, written);
                    break;

                case ContainerType.Run:
                    written = FillFromRun(ref entry, baseValue, buffer, written);
                    break;

                case ContainerType.Full:
                    written = FillFromFull(baseValue, buffer, written);
                    break;
            }

            if (_positionInContainer >= entry.Cardinality ||
                (entry.Type == ContainerType.Full && _positionInContainer >= RoaringBitmap.BitsPerContainer))
            {
                // Move to next container
                _containerIndex++;
                _positionInContainer = 0;
                _bitmapWordIndex = 0;
                _bitmapCurrentWord = 0;
                _runIndex = 0;
                _runPosition = 0;
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

        while (written < buffer.Length && _bitmapWordIndex < RoaringBitmap.BitmapContainerSizeInUlongs)
        {
            // Load the next word if the current one is exhausted
            if (_bitmapCurrentWord == 0)
            {
                _bitmapCurrentWord = bitmap[_bitmapWordIndex];
                if (_bitmapCurrentWord == 0)
                {
                    _bitmapWordIndex++;
                    continue;
                }
            }

            // Process set bits in the current word
            while (_bitmapCurrentWord != 0 && written < buffer.Length)
            {
                int bit = BitOperations.TrailingZeroCount(_bitmapCurrentWord);
                buffer[written++] = baseValue | (uint)(_bitmapWordIndex * 64 + bit);
                _bitmapCurrentWord &= _bitmapCurrentWord - 1; // clear lowest set bit
                bitmapPopulated++;
            }

            // Advance to the next word if this one is fully consumed
            if (_bitmapCurrentWord == 0)
                _bitmapWordIndex++;
        }

        _positionInContainer += bitmapPopulated;
        return written;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int FillFromRun(ref ContainerEntry entry, long baseValue, Span<long> buffer, int written)
    {
        ushort* runs = (ushort*)entry.Data;
        int numRuns = runs[0];
        int emitted = 0;

        while (written < buffer.Length && _runIndex < numRuns)
        {
            ushort start = runs[1 + _runIndex * 2];
            ushort length = runs[1 + _runIndex * 2 + 1];

            while (written < buffer.Length && _runPosition <= length)
            {
                buffer[written++] = baseValue | (uint)(start + _runPosition);
                _runPosition++;
                emitted++;
            }

            if (_runPosition > length)
            {
                _runIndex++;
                _runPosition = 0;
            }
        }

        _positionInContainer += emitted;
        return written;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int FillFromNegated(ref ContainerEntry entry, long baseValue, Span<long> buffer, int written)
    {
        // Iterate 0..65535 skipping values that appear in the absent list.
        // _positionInContainer tracks the current value (0..65535).
        // _runIndex tracks the current position in the absent array.
        ushort* absent = (ushort*)entry.Data;
        int absentCount = RoaringBitmap.BitsPerContainer - entry.Cardinality;

        while (written < buffer.Length && _positionInContainer < RoaringBitmap.BitsPerContainer)
        {
            // Skip absent values
            while (_runIndex < absentCount && absent[_runIndex] == (ushort)_positionInContainer)
            {
                _positionInContainer++;
                _runIndex++;
                if (_positionInContainer >= RoaringBitmap.BitsPerContainer)
                    return written;
            }

            if (_positionInContainer < RoaringBitmap.BitsPerContainer)
            {
                buffer[written++] = baseValue | (uint)_positionInContainer;
                _positionInContainer++;
            }
        }

        return written;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int FillFromFull(long baseValue, Span<long> buffer, int written)
    {
        while (written < buffer.Length && _positionInContainer < RoaringBitmap.BitsPerContainer)
        {
            buffer[written++] = baseValue | (uint)_positionInContainer;
            _positionInContainer++;
        }

        return written;
    }
}
