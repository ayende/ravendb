using System;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using Sparrow;
using Sparrow.Server;
using Sparrow.Server.Utils.VxSort;

namespace Voron.Data.RoaringBitmaps;

/// <summary>
/// Forward iterator for RoaringBitmap supporting Fill(Span&lt;long&gt;) streaming.
/// On construction, builds a sorted array of packed (key, slot) ulongs for deterministic traversal.
/// Each ulong packs: key in upper 32 bits, slot in lower 32 bits.
/// _positionInContainer is shared: Array uses it as array index, Range as offset from RangeStart,
/// Bitmap as the current ulong word index (0..1023). _bitmapCurrentWord stores remaining
/// bits in the current word for bitmap iteration only.
/// </summary>
public unsafe struct RoaringBitmapIterator : IDisposable
{
    private ByteStringContext _ctx;
    private ByteString _packedEntries; // array of packed (key << 32) | slot
    private int _entryCount;
    private int _containerIndex; // index into _packedEntries
    /// <summary>Array: index into sorted array. Range: offset from RangeStart. Bitmap: current ulong index (0..1023).</summary>
    private int _positionInContainer;
    private ulong _bitmapCurrentWord; // Bitmap only: remaining bits in current word

    public RoaringBitmapIterator(ref RoaringBitmapData data, ByteStringContext ctx)
    {
        _ctx = ctx;
        _containerIndex = 0;
        _positionInContainer = 0;
        _bitmapCurrentWord = 0;

        // Build sorted array of packed (key, slot) ulongs from active containers
        int containerCount = data.ContainerCount;
        if (containerCount is 0)
        {
            _packedEntries = default;
            _entryCount = 0;
            return;
        }

        int sizeNeeded = containerCount * sizeof(ulong);
        ctx.Allocate(sizeNeeded, out _packedEntries);

        var packedPtr = (ulong*)_packedEntries.Ptr;
        _entryCount = 0;

        // Pack all active entries: (key << 32) | slot
        var entries = new ReadOnlySpan<ContainerEntry>(data._entries.RawItems, data._entries.Count);
        var types = data._types.RawItems;
        for (int i = 0; i < entries.Length; i++)
        {
            if (types[i] != ContainerType.Free)
            {
                // Pack: upper 32 bits = key, lower 32 bits = slot
                packedPtr[_entryCount] = ((ulong)entries[i].Key << 32) | (uint)i;
                _entryCount++;
            }
        }

        // Sort by packed value (key is in upper bits, so sorts by key)
        Sort.Run(packedPtr, _entryCount);
    }

    /// <summary>
    /// Fill the buffer with the next batch of values from the bitmap.
    /// Returns the number of values written.
    /// </summary>
    public int Fill(ref RoaringBitmapData data, Span<long> buffer)
    {
        int written = 0;
        if (_entryCount == 0)
            return 0;

        var packedPtr = (ulong*)_packedEntries.Ptr;

        while (written < buffer.Length && _containerIndex < _entryCount)
        {
            ulong packed = packedPtr[_containerIndex];
            int slot = (int)(packed & 0xFFFFFFFF);          // Lower 32 bits = slot
            uint key = (uint)(packed >> 32);                 // Upper 32 bits = key

            ref ContainerEntry entry = ref data._entries[slot];
            ContainerType type = data._types.RawItems[slot];
            long baseValue = (long)key << RoaringBitmap.ContainerKeyShift;

            // Iteration contract: callers MUST run RoaringBitmap.PrepareForReading() before
            // constructing the iterator, which converts every ArrayUnsorted container into
            // a sorted Array. If we still see ArrayUnsorted here, the caller skipped that
            // step and Fill() will emit unsorted entry IDs within that container, breaking
            // the ascending-output contract that streaming and paging consumers depend on.
            Debug.Assert(type != ContainerType.ArrayUnsorted,
                "RoaringBitmapIterator: ArrayUnsorted container at iteration time. " +
                "PrepareForReading() must be called before GetIterator() / Fill().");

            switch (type)
            {
                case ContainerType.Array:
                case ContainerType.ArrayUnsorted: // accepted defensively in Release; PrepareForReading should have removed these
                    written = FillFromArray(ref entry, baseValue, buffer, written);
                    break;

                case ContainerType.Bitmap:
                    written = FillFromBitmap(ref entry, baseValue, buffer, written);
                    break;

                case ContainerType.Range:
                    written = FillFromRange(ref entry, baseValue, buffer, written);
                    break;
            }

            bool containerCompleted = type == ContainerType.Bitmap
                ? _positionInContainer >= RoaringBitmap.BitmapContainerSizeInUInt64 && _bitmapCurrentWord == 0
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
        ushort* arr = (ushort*)entry.ArrayData;
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
        var bitmap = entry.BitmapPtr;

        while (written < buffer.Length && _positionInContainer < RoaringBitmap.BitmapContainerSizeInUInt64)
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
        int rangeStart = entry.RangeStart;
        int remaining = entry.Cardinality - _positionInContainer;
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
                    baseValue + rangeStart + _positionInContainer,
                    baseValue + rangeStart + _positionInContainer + 1,
                    baseValue + rangeStart + _positionInContainer + 2,
                    baseValue + rangeStart + _positionInContainer + 3);
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
                dst[i] = baseValue | (uint)(rangeStart + _positionInContainer);
                _positionInContainer++;
            }
        }

        return written + toCopy;
    }

    public void Dispose()
    {
        if (_packedEntries.HasValue)
        {
            _ctx.Release(ref _packedEntries);
            _packedEntries = default; // make Dispose idempotent — repeated callers are common in finally chains
        }
    }
}
