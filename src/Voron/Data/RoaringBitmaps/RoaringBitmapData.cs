using System.Numerics;
using System.Runtime.InteropServices;
using Sparrow.Server;
using Voron.Util;

namespace Voron.Data.RoaringBitmaps;

/// <summary>
/// Unmanaged state for a RoaringBitmap. Contains all native-pointer fields.
/// Can be stored in class or struct fields without restriction.
/// Use <see cref="RoaringBitmap"/> (ref struct) for operations that need allocation.
/// </summary>
[StructLayout(LayoutKind.Auto)]
public unsafe struct RoaringBitmapData
{
    internal NativeList<ContainerEntry> _entries;
    internal NativeList<ContainerType> _types;
    internal NativeList<int> _index;
    internal int _containerCount;
    /// <summary>
    /// Head of the entry free list, 1-based: 0 = empty, n = real slot index (n-1).
    /// Zero-init (default struct) is therefore a valid empty state.
    /// <see cref="ContainerEntry.NextFreeSlot"/> uses the same encoding for chaining.
    /// </summary>
    internal int _freeListHead;
    internal NativeList<ByteString> _freeStorages;

    public readonly int ContainerCount => _containerCount;

    public long Count
    {
        get
        {
            long total = 0;
            ContainerEntry* entries = _entries.RawItems;
            ContainerType* types = _types.RawItems;
            int count = _entries.Count;
            for (int i = 0; i < count; i++)
            {
                if (types[i] == ContainerType.Free)
                    continue;

                int card = entries[i].Cardinality;
                if (card < 0)
                {
                    // LazyCardinality: bitmap container was updated without recomputing the popcount.
                    // Count bits directly so callers (e.g. ShouldSwitchToEntryScan) see a correct value
                    // rather than the sentinel -1, which would incorrectly trigger early entry scan.
                    ulong* bitmapPtr = (ulong*)entries[i].Data;
                    for (int w = 0; w < RoaringBitmap.BitmapContainerSizeInUInt64; w++)
                        total += BitOperations.PopCount(bitmapPtr[w]);
                }
                else
                    total += card;
            }
            return total;
        }
    }

    public readonly bool IsEmpty => _containerCount == 0;

    public readonly bool Contains(long value)
    {
        long key = value >> RoaringBitmap.ContainerKeyShift;
        ushort low = (ushort)(value & RoaringBitmap.ContainerValueMaskPublic);

        int slot = GetSlotForKey(key);
        if (slot < 0)
            return false;

        ref ContainerEntry entry = ref _entries[slot];
        ContainerType type = _types.RawItems[slot];
        return type switch
        {
            ContainerType.Array => RoaringBitmap.ArrayContainerContains(entry.ArrayData, entry.Cardinality, low),
            ContainerType.ArrayUnsorted => RoaringBitmap.SimdLinearContains(entry.ArrayData, entry.Cardinality, low),
            ContainerType.Bitmap => RoaringBitmap.BitmapContainsPublic((ulong*)entry.Data, low),
            ContainerType.Range => low >= entry.RangeStart && low < entry.RangeStart + entry.Cardinality,
            _ => false
        };
    }

    public readonly long MinContainerKey
    {
        get
        {
            if (_containerCount == 0)
                return -1;
            for (int i = 0; i < _index.Count; i++)
            {
                if (_index[i] != RoaringBitmap.IndexAbsentPublic)
                    return i;
            }
            return -1;
        }
    }

    public readonly long MaxContainerKey
    {
        get
        {
            if (_containerCount == 0)
                return -1;
            for (int i = _index.Count - 1; i >= 0; i--)
            {
                if (_index[i] != RoaringBitmap.IndexAbsentPublic)
                    return i;
            }
            return -1;
        }
    }

    public readonly int GetSlotForKey(long key)
    {
        if (key < 0 || key >= _index.Count)
            return RoaringBitmap.IndexAbsentPublic;
        return _index.RawItems[key];
    }

    public void PrepareForReading(ByteStringContext ctx = null)
    {
        RoaringBitmap view = new(ref this, ctx);
        view.PrepareForReading();
    }

    public void Dispose(ByteStringContext ctx)
    {
        RoaringBitmap view = new(ref this, ctx);
        view.Dispose();
    }

    public void Clear(ByteStringContext ctx)
    {
        RoaringBitmap view = new(ref this, ctx);
        view.Clear();
    }

    public RoaringBitmapIterator GetIterator(ByteStringContext ctx)
    {
        PrepareForReading();
        return new RoaringBitmapIterator(ref this, ctx);
    }

    // --- IL-emitter callable methods ---
    // These are instance methods called directly by the emitted IL on ref RoaringBitmapData.
    // They create a local RoaringBitmap view to reuse the existing logic.

    public void AndWith(ref RoaringBitmapData other, ByteStringContext ctx)
    {
        RoaringBitmap view = new(ref this, ctx);
        view.AndWith(ref other);
    }

    public void AndNotWith(ref RoaringBitmapData other, ByteStringContext ctx)
    {
        RoaringBitmap view = new(ref this, ctx);
        view.AndNotWith(ref other);
    }

    public void OrWith(ref RoaringBitmapData other, ByteStringContext ctx)
    {
        RoaringBitmap view = new(ref this, ctx);
        view.LazyOrWith(ref other);
        view.RepairAfterLazy();
    }

    public void RepairAfterLazy(ByteStringContext ctx)
    {
        RoaringBitmap view = new(ref this, ctx);
        view.RepairAfterLazy();
    }

    public void Add(long value, ByteStringContext ctx)
    {
        RoaringBitmap view = new(ref this, ctx);
        view.Add(value);
    }

    public void SwapContents(ref RoaringBitmapData other)
    {
        (this, other) = (other, this);
    }
}
