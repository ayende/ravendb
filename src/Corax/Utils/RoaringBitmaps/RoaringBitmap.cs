using System;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using Sparrow;
using Sparrow.Server;
using Voron.Util;

namespace Corax.Utils.RoaringBitmaps;

/// <summary>
/// A roaring bitmap implementation optimized for Corax's native memory model.
/// All memory is allocated through ByteStringContext, ensuring zero managed heap allocations
/// for the bitmap data. Non-negative values are split into container key (value &gt;&gt; 16)
/// and low 16 bits. Container lookup is O(1) via a flat index array sized to the max key.
///
/// Container types:
/// - Range: contiguous values 0..count-1 (no data allocation). count=65536 means full.
///   Sequential Add from 0 is O(1). Created automatically for sequential inserts.
/// - ArrayUnsorted: append-only ushort[]. Add is O(1). Sorted lazily on first read.
/// - Array: sorted ushort[] for sparse data (cardinality &lt;= 4096, up to 8KB)
/// - Bitmap: 8KB fixed bitmap (1024 ulongs) for dense data (&gt; 4096 values)
/// </summary>
public unsafe struct RoaringBitmap : IDisposable
{
    public const int BitmapContainerSizeInBytes = 8192; // 8KB
    public const int BitmapContainerSizeInUlongs = BitmapContainerSizeInBytes / sizeof(ulong);
    public const int BitsPerContainer = BitmapContainerSizeInBytes * 8; // each byte holds 8 bits
    public const int ArrayContainerMaxCardinality = BitmapContainerSizeInBytes / sizeof(ushort); // crossover: array at max costs same as bitmap
    public const int ContainerKeyShift = 16;
    public const int ContainerValueMask = 0xFFFF;

    private const int FreeSlotTerminator = -2; // end of free list
    private const int IndexAbsent = -1;        // key not present in index

    private ByteStringContext _ctx;
    /// <summary>Packed container entries. May have tombstones (gaps reused via free list).</summary>
    private NativeList<ContainerEntry> _entries;
    /// <summary>Container types, parallel to _entries. Split to reduce padding in ContainerEntry.</summary>
    internal NativeList<ContainerType> _types;
    /// <summary>Key → entry slot index. -1 = absent. Length = maxKey + 1.</summary>
    private NativeList<int> _index;
    private int _containerCount;
    /// <summary>Head of free list in _entries. Dead entry's Cardinality field stores next free index; -2 = end.</summary>
    private int _freeListHead;

    public RoaringBitmap(ByteStringContext ctx)
    {
        _ctx = ctx;
        _entries = new NativeList<ContainerEntry>();
        _types = new NativeList<ContainerType>();
        _index = new NativeList<int>();
        _containerCount = 0;
        _freeListHead = FreeSlotTerminator;
    }

    public readonly int ContainerCount => _containerCount;

    /// <summary>Length of the key index — the maximum container key that can be looked up in O(1).</summary>
    public readonly int IndexLength => _index.Count;

    public long Cardinality
    {
        get
        {
            // Walk entries directly — more cache-friendly than index indirection.
            // Skip free entries (Type == 0xFF).
            long total = 0;
            ContainerEntry* entries = _entries.RawItems;
            ContainerType* types = _types.RawItems;
            int count = _entries.Count;
            for (int i = 0; i < count; i++)
            {
                if (types[i] != ContainerType.Free)
                {
                    AssertPrepared(types[i]);
                    total += entries[i].Cardinality;
                }
            }
            return total;
        }
    }

    public readonly bool IsEmpty => _containerCount == 0;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Add(long value)
    {
        Debug.Assert(value >= 0, "RoaringBitmap only supports non-negative values.");
        long key = value >> ContainerKeyShift;
        ushort low = (ushort)(value & ContainerValueMask);

        int slot = GetSlotForKey(key);
        if (slot >= 0)
        {
            ref ContainerEntry entry = ref _entries[slot];
            AddToContainer(ref entry, slot, low);
        }
        else
        {
            // First value in this container: Range if starting from 0, Array otherwise
            if (low == 0)
            {
                AddNewContainer(key, ContainerType.Range, new ContainerEntry
                {
                    Cardinality = 1,
                    Data = null,
                    Storage = default
                });
            }
            else
            {
                // First value not at 0: start as unsorted array for O(1) appends
                ContainerEntry newEntry = CreateArrayContainer(key);
                (newEntry.ArrayData)[0] = low;
                newEntry.Cardinality = 1;
                AddNewContainer(key, ContainerType.ArrayUnsorted, newEntry);
            }
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool Contains(long value)
    {
        long key = value >> ContainerKeyShift;
        ushort low = (ushort)(value & ContainerValueMask);

        int slot = GetSlotForKey(key);
        if (slot < 0)
            return false;

        ref ContainerEntry entry = ref _entries[slot];
        return ContainerContains(ref entry, _types.RawItems[slot], low);
    }

    /// <summary>
    /// Fill the buffer with values from the bitmap, starting from the current iteration state.
    /// Returns the number of values written. Compatible with Corax's streaming evaluation.
    /// </summary>
    public int Fill(Span<long> buffer, ref RoaringBitmapIterator iterator)
    {
        return iterator.Fill(ref this, buffer);
    }

    public RoaringBitmapIterator GetIterator()
    {
        return new RoaringBitmapIterator(ref this, _ctx);
    }

    /// <summary>
    /// Create a deep copy of this bitmap. All container data is cloned into the same ByteStringContext.
    /// The source bitmap is not modified. The clone preserves container types (Range, Array, Bitmap).
    /// </summary>
    public RoaringBitmap Clone()
    {
        var copy = new RoaringBitmap(_ctx);

        // Walk entries directly using the Key field — no index indirection needed
        ContainerEntry* entries = _entries.RawItems;
        ContainerType* types = _types.RawItems;
        int entryCount = _entries.Count;
        for (int i = 0; i < entryCount; i++)
        {
            if (types[i] != ContainerType.Free)
                copy.AddContainer(entries[i].Key, types[i], CloneContainer(_ctx, ref entries[i], types[i]));
        }
        return copy;
    }

    /// <summary>
    /// Prepare for reading: sort and deduplicate all unsorted array containers.
    /// Call this after all Add calls and before any read operations (Contains,
    /// Fill, set ops). This separates the sort cost from the first query, making
    /// performance more predictable.
    ///
    /// For sorted input (e.g., Corax posting lists), this is nearly free since
    /// the arrays are already in order and dedup finds no duplicates.
    /// </summary>
    [SkipLocalsInit]
    public void PrepareForReading()
    {
        // 8KB scratch bitmap for radix sort: explode unsorted values into bits,
        // extract back as sorted array. O(n) bit-sets + O(1024) word scan vs O(n log n).
        // Dedup is free. Scratch reused across all containers (one clear per chunk touched).
        ulong* scratch = stackalloc ulong[BitmapContainerSizeInUlongs];

        ContainerEntry* entries = _entries.RawItems;
        ContainerType* types = _types.RawItems;
        int entryCount = _entries.Count;
        for (int i = 0; i < entryCount; i++)
        {
            ref ContainerEntry entry = ref entries[i];
            if (types[i] == ContainerType.ArrayUnsorted)
            {
                if (entry.Cardinality >= BitmapSortThreshold)
                    SortViaBitmapScratch(ref entry, ref types[i], scratch);
                else
                    SortSmallArray(ref entry, ref types[i]);
            }
        }
    }

    /// <summary>
    /// Radix sort using 8KB bitmap scratch space.
    /// O(n) bit-sets + O(n) word scan. Dedup is free (duplicate bit-set is noop).
    /// dirtyMap tracks which 4-ulong chunks have been written so extraction only
    /// visits touched chunks, skipping clean regions entirely.
    /// </summary>
    private static void SortViaBitmapScratch(ref ContainerEntry entry, ref ContainerType type, ulong* scratch)
    {
        Debug.Assert(type == ContainerType.ArrayUnsorted);

        var arr = entry.ArrayData;
        int count = entry.Cardinality;

        Vector256<ulong> dirtyMapVec = Vector256<ulong>.Zero; // 256 chunks in 8KB = 32 bytes in chunk
        ulong* dirtyMap = (ulong*)Unsafe.AsPointer(ref dirtyMapVec);

        // Explode: set bits for each value, mark the chunk dirty on first touch and clear it.
        for (int i = 0; i < count; i++)
        {
            ushort val = arr[i];
            int wordIdx = val >> 6;
            int chunkIdx = wordIdx >> 2;
            if ((dirtyMap[chunkIdx >> 6] & (1UL << (chunkIdx & 63))) == 0)
            {
                dirtyMap[chunkIdx >> 6] |= 1UL << (chunkIdx & 63);
                new Span<byte>(scratch + chunkIdx * 4, 4 * sizeof(ulong)).Clear();
            }
            scratch[wordIdx] |= 1UL << (val & 63);
        }

        // Extract: only visit chunks marked dirty
        int sorted = 0;
        for (int i = 0; i < 4; i++)
        {
            ulong currentWork = dirtyMap[i];
            while (currentWork != 0)
            {
                int chunkBit = BitOperations.TrailingZeroCount(currentWork);
                int wordBaseIdx = ((i << 6) + chunkBit) << 2;
                for (int wordOffset = 0; wordOffset < 4; wordOffset++)
                {
                    int wordIdx = wordBaseIdx + wordOffset;
                    ulong currentWord = scratch[wordIdx];
                    while (currentWord != 0)
                    {
                        int bit = BitOperations.TrailingZeroCount(currentWord);
                        arr[sorted++] = (ushort)((wordIdx << 6) + bit);
                        currentWord &= currentWord - 1;
                    }
                    scratch[wordIdx] = 0;
                }
                currentWork &= currentWork - 1;
            }
        }
        entry.Cardinality = sorted;
        type = ContainerType.Array;
    }

    #region In-place Set Operations

    // [1] Bitmap→Array conversion after set ops: we intentionally skip this.
    // Standard roaring bitmaps convert sparse bitmap results back to array containers
    // to save memory and speed up subsequent operations. But in Corax, these bitmaps
    // are temporary — built during query evaluation and discarded immediately after.
    // The 8KB bitmap is already allocated; converting to Array allocates another buffer
    // and scans 1024 words, costing more than it saves for short-lived data.

    /// <summary>
    /// In-place AND: retain only values that also exist in other.
    /// Walks both index arrays; containers in this bitmap with no match in other are freed.
    /// </summary>
    public void AndWith(ref RoaringBitmap other)
    {
        int* myIdx = _index.RawItems;
        int myLen = _index.Count;

        for (int key = 0; key < myLen; key++)
        {
            int mySlot = myIdx[key];
            if (mySlot < 0)
                continue;

            int otherSlot = other.GetSlotForKey(key);
            if (otherSlot < 0)
            {
                // Not in other — remove
                FreeContainer(key, mySlot);
            }
            else
            {
                ref ContainerEntry myEntry = ref _entries[mySlot];
                ref ContainerEntry otherEntry = ref other.GetEntryBySlot(otherSlot);
                AndContainerInPlace(ref myEntry, ref _types.RawItems[mySlot], ref otherEntry, other._types.RawItems[otherSlot]);
                if (myEntry.Cardinality == 0)
                    FreeContainer(key, mySlot);
            }
        }
    }

    /// <summary>
    /// In-place OR: add all values from other into this bitmap.
    /// Matching containers are unioned in-place. Unmatched containers from other are cloned in.
    /// </summary>
    public void OrWith(ref RoaringBitmap other)
    {
        if (other.ContainerCount == 0)
            return;

        int otherLen = other.IndexLength;
        for (int key = 0; key < otherLen; key++)
        {
            int otherSlot = other.GetSlotForKey(key);
            if (otherSlot < 0)
                continue;

            ref ContainerEntry otherEntry = ref other.GetEntryBySlot(otherSlot);
            int mySlot = GetSlotForKey(key);

            if (mySlot >= 0)
            {
                OrContainerInPlace(ref _entries[mySlot], ref _types.RawItems[mySlot], ref otherEntry, other._types.RawItems[otherSlot]);
            }
            else
            {
                AddNewContainer(key, other._types.RawItems[otherSlot], CloneContainer(_ctx, ref otherEntry, other._types.RawItems[otherSlot]));
            }
        }
    }

    /// <summary>
    /// In-place ANDNOT: remove all values that exist in other from this bitmap.
    /// </summary>
    public void AndNotWith(ref RoaringBitmap other)
    {
        int myLen = _index.Count;
        int* myIdx = _index.RawItems;

        for (int key = 0; key < myLen; key++)
        {
            int mySlot = myIdx[key];
            if (mySlot < 0)
                continue;

            int otherSlot = other.GetSlotForKey(key);
            if (otherSlot < 0)
                continue; // nothing to subtract

            ref ContainerEntry otherEntry = ref other.GetEntryBySlot(otherSlot);
            AndNotContainerInPlace(ref _entries[mySlot], ref _types.RawItems[mySlot], ref otherEntry, other._types.RawItems[otherSlot]);
            if (_entries[mySlot].Cardinality == 0)
                FreeContainer(key, mySlot);
        }
    }

    [SkipLocalsInit]
    private void AndContainerInPlace(ref ContainerEntry left, ref ContainerType leftType, ref ContainerEntry right, ContainerType rightType)
    {
        AssertPrepared(leftType);
        AssertPrepared(rightType);

        // Range×Range fast paths — no allocation needed
        if (leftType == ContainerType.Range && rightType == ContainerType.Range)
        {
            left.Cardinality = Math.Min(left.Cardinality, right.Cardinality); // AND = min
            return;
        }
        if (leftType == ContainerType.Range)
            ConvertRangeToBitmap(ref left, ref leftType);
        if (rightType == ContainerType.Range)
        {
            ulong* stackBmp = stackalloc ulong[BitmapContainerSizeInUlongs];
            ContainerEntry temp = MaterializeRangeToStack(ref right, stackBmp);
            AndContainerInPlace(ref left, ref leftType, ref temp, ContainerType.Bitmap);
            return;
        }

        switch (leftType, rightType)
        {
            case (ContainerType.Bitmap, ContainerType.Bitmap):
                {
                    var count = BitmapAndSimd(
                    left.BitmapPtr, right.BitmapPtr, left.BitmapPtr, BitmapContainerSizeInUlongs);
                    left.Cardinality = count;
                    break;
                }
            case (ContainerType.Bitmap, ContainerType.Array):
                {
                    // Result is at most right.Cardinality entries — always an array
                    var arr = right.ArrayData;
                    var bmp = left.BitmapData;
                    _ctx.Allocate(Math.Max(InitialArrayContainerSizeInBytes, right.Cardinality * sizeof(ushort)), out ByteString newStorage);
                    ushort* dst = (ushort*)newStorage.Ptr;
                    int count = 0;
                    for (int i = 0; i < right.Cardinality; i++)
                    {
                        ushort val = arr[i];
                        if ((bmp[val >> 6] & (1UL << (val & 63))) != 0)
                            dst[count++] = val;
                    }
                    _ctx.Release(ref left.Storage);
                    left.Storage = newStorage;
                    left.Data = newStorage.Ptr;
                    leftType = ContainerType.Array;
                    left.Cardinality = count;
                    break;
                }

            case (ContainerType.Array, ContainerType.Bitmap):
                {
                    // Filter left array against right bitmap, in-place
                    var arr = left.ArrayData;
                    var bmp = right.BitmapData;
                    int count = 0;
                    for (int i = 0; i < left.Cardinality; i++)
                    {
                        ushort val = arr[i];
                        if ((bmp[val >> 6] & (1UL << (val & 63))) != 0)
                            arr[count++] = val;
                    }
                    left.Cardinality = count;
                    break;
                }

            case (ContainerType.Array, ContainerType.Array):
                {
                    // In-place intersection of two sorted arrays
                    ushort* a = left.ArrayPtr;
                    ushort* b = right.ArrayPtr;
                    int count = ArrayContainerAnd(a, left.Cardinality, b, right.Cardinality, a);
                    left.Cardinality = count;
                    break;
                }
        }
    }

    [SkipLocalsInit]
    private void OrContainerInPlace(ref ContainerEntry left, ref ContainerType leftType, ref ContainerEntry right, ContainerType rightType)
    {
        AssertPrepared(leftType);
        AssertPrepared(rightType);

        if (leftType == ContainerType.Range && rightType == ContainerType.Range)
        {
            left.Cardinality = Math.Max(left.Cardinality, right.Cardinality); // OR = max
            return;
        }
        if (leftType == ContainerType.Range)
            ConvertRangeToBitmap(ref left, ref leftType);
        if (rightType == ContainerType.Range)
        {
            ulong* stackBmp = stackalloc ulong[BitmapContainerSizeInUlongs];
            ContainerEntry temp = MaterializeRangeToStack(ref right, stackBmp);
            OrContainerInPlace(ref left, ref leftType, ref temp, ContainerType.Bitmap);
            return;
        }

        switch (leftType, rightType)
        {
            case (ContainerType.Bitmap, ContainerType.Bitmap):
                {
                    var count = BitmapOrSimd(
                    left.BitmapPtr, right.BitmapPtr, left.BitmapPtr, BitmapContainerSizeInUlongs);
                    left.Cardinality = count;
                    break;
                }
            case (ContainerType.Bitmap, ContainerType.Array):
                {
                    var bmp = left.BitmapData;
                    var arr = right.ArrayData;
                    for (int i = 0; i < right.Cardinality; i++)
                    {
                        ushort val = arr[i];
                        ulong mask = 1UL << (val & 63);
                        if ((bmp[val >> 6] & mask) == 0)
                        {
                            bmp[val >> 6] |= mask;
                            left.Cardinality++;
                        }
                    }
                    // No Bitmap→Array conversion: these are temporary, discarded after query. See note [1].
                    break;
                }

            case (ContainerType.Array, ContainerType.Array):
                {
                    int maxResult = left.Cardinality + right.Cardinality;
                    if (maxResult > ArrayContainerMaxCardinality)
                    {
                        // Result will be a bitmap — convert left, then OR
                        ConvertArrayToBitmap(ref left, ref leftType);
                        OrContainerInPlace(ref left, ref leftType, ref right, rightType);
                    }
                    else
                    {
                        // Merge two sorted arrays into stackalloc (max 8KB), then copy back
                        ushort* tmp = stackalloc ushort[ArrayContainerMaxCardinality];
                        int count = ArrayContainerOr(left.ArrayPtr, left.Cardinality, right.ArrayPtr, right.Cardinality, tmp);
                        EnsureArrayCapacity(ref left, count);
                        Unsafe.CopyBlockUnaligned(left.Data, (byte*)tmp, (uint)(count * sizeof(ushort)));
                        left.Cardinality = count;
                    }
                    break;
                }

            case (ContainerType.Array, ContainerType.Bitmap):
                {
                    ConvertArrayToBitmap(ref left, ref leftType);
                    OrContainerInPlace(ref left, ref leftType, ref right, rightType);
                    break;
                }
        }
    }


    [SkipLocalsInit]
    private void AndNotContainerInPlace(ref ContainerEntry left, ref ContainerType leftType, ref ContainerEntry right, ContainerType rightType)
    {
        AssertPrepared(leftType);
        AssertPrepared(rightType);

        // Range×Range: values in left not in right. If right covers all of left, empty.
        if (leftType == ContainerType.Range && rightType == ContainerType.Range)
        {
            if (right.Cardinality >= left.Cardinality)
            {
                left.Cardinality = 0; // right covers everything
                return;
            }
            // Result is values right.Cardinality..left.Cardinality-1 — not contiguous from 0, materialize
        }
        if (leftType == ContainerType.Range)
            ConvertRangeToBitmap(ref left, ref leftType);
        if (rightType == ContainerType.Range)
        {
            ulong* stackBmp = stackalloc ulong[BitmapContainerSizeInUlongs];
            ContainerEntry temp = MaterializeRangeToStack(ref right, stackBmp);
            AndNotContainerInPlace(ref left, ref leftType, ref temp, ContainerType.Bitmap);
            return;
        }

        switch (leftType, rightType)
        {
            case (ContainerType.Bitmap, ContainerType.Bitmap):
                {
                    var count = BitmapAndNotSimd(
                    left.BitmapPtr, right.BitmapPtr, left.BitmapPtr, BitmapContainerSizeInUlongs);
                    left.Cardinality = count;
                    break;
                }
            case (ContainerType.Bitmap, ContainerType.Array):
                {
                    var bmp = left.BitmapData;
                    var arr = right.ArrayData;
                    for (int i = 0; i < right.Cardinality; i++)
                    {
                        ushort val = arr[i];
                        ulong mask = 1UL << (val & 63);
                        if ((bmp[val >> 6] & mask) != 0)
                        {
                            bmp[val >> 6] &= ~mask;
                            left.Cardinality--;
                        }
                    }
                    break;
                }

            case (ContainerType.Array, ContainerType.Bitmap):
                {
                    var arr = left.ArrayData;
                    var bmp = right.BitmapData;
                    int count = 0;
                    for (int i = 0; i < left.Cardinality; i++)
                    {
                        ushort val = arr[i];
                        if ((bmp[val >> 6] & (1UL << (val & 63))) == 0)
                            arr[count++] = val;
                    }
                    left.Cardinality = count;
                    break;
                }

            case (ContainerType.Array, ContainerType.Array):
                {
                    ushort* a = left.ArrayPtr;
                    ushort* b = right.ArrayPtr;
                    int count = ArrayContainerAndNot(a, left.Cardinality, b, right.Cardinality, a);
                    left.Cardinality = count;
                    break;
                }
        }
    }

    /// <summary>
    /// Create a temporary bitmap on the stack from a Range container.
    /// Returns a ContainerEntry pointing to the stackalloc'd buffer (no ByteStringContext allocation).
    /// The returned entry has Storage=default — caller must NOT release it.
    /// </summary>
    [SkipLocalsInit]
    private static ContainerEntry MaterializeRangeToStack(ref ContainerEntry entry, ulong* stackBitmap)
    {
        ClearBitmap(stackBitmap);
        FillBitmapFromRange(stackBitmap, entry.Cardinality);

        return new ContainerEntry
        {
            Data = (byte*)stackBitmap,
            Cardinality = entry.Cardinality,
            Storage = default // no allocation to release
        };
    }


    #endregion

    #region Index Operations

    /// <summary>
    /// O(1) lookup: returns the entry slot for this key, or -1 if not present.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal readonly int GetSlotForKey(long key)
    {
        if (key < 0 || key >= _index.Count)
            return IndexAbsent;
        return _index.RawItems[key];
    }

    /// <summary>
    /// Get a direct reference to an entry by its slot index in the entries array.
    /// </summary>
    internal readonly ref ContainerEntry GetEntryBySlot(int slot) => ref _entries[slot];

    /// <summary>
    /// Get entries array pointer and count for iterator construction.
    /// </summary>
    internal readonly ReadOnlySpan<ContainerEntry> GetEntriesForIterator()
    {
        return new ReadOnlySpan<ContainerEntry>(_entries.RawItems, _entries.Count);
    }

    /// <summary>
    /// Ensure the index array covers the given key, filling new slots with -1.
    /// </summary>
    private void EnsureIndexCoversKey(long key)
    {
        if (key < _index.Count)
            return;

        if (key < 0 || key >= int.MaxValue - 16)
            throw new ArgumentOutOfRangeException(nameof(key), $"Container key {key} is out of the valid range (0..{int.MaxValue - 17}).");

        int needed = checked((int)(key + 1));
        int oldCount = _index.Count;
        _index.EnsureCapacityFor(_ctx, needed - oldCount);
        _index.Count = needed;

        // Fill new slots with -1 (absent)
        int* ptr = _index.RawItems;
        new Span<int>(ptr + oldCount, needed - oldCount).Fill(IndexAbsent);
    }

    /// <summary>
    /// Add a new container entry and register it in the index. Returns the entry slot.
    /// </summary>
    private int AddNewContainer(long key, ContainerType type, in ContainerEntry entry)
    {
        EnsureIndexCoversKey(key);

        int slot;
        if (_freeListHead > 0)
        {
            slot = _freeListHead;
            Debug.Assert(_types.RawItems[slot] == ContainerType.Free, "Expected free entry");
            _freeListHead = (int)_entries[slot].NextFreeSlot;
            _entries[slot] = entry;
            _types.RawItems[slot] = type;
        }
        else
        {
            slot = _entries.Count;
            _entries.Add(_ctx, entry);
            _types.Add(_ctx, type);
        }
        // we checked size of key in EnsureIndexCoversKey, so this cast is safe
        _entries[slot].Key = (uint)key;

        _index.RawItems[key] = slot;
        _containerCount++;
        return slot;
    }

    /// <summary>
    /// Remove a container: release its storage, mark the index as absent, add slot to free list.
    /// </summary>
    private void FreeContainer(long key, int slot)
    {
        ref ContainerEntry entry = ref _entries[slot];
        if (entry.Storage.HasValue)
            _ctx.Release(ref entry.Storage);

        // Mark as free and chain into free list
        entry = default;
        _types.RawItems[slot] = ContainerType.Free;
        entry.NextFreeSlot = (uint)_freeListHead;
        _freeListHead = slot;

        _index.RawItems[key] = IndexAbsent;
        _containerCount--;
    }

    /// <summary>
    /// Add a container entry during result-building (set operations). Used when building
    /// a new bitmap from scratch where keys are added in order.
    /// </summary>
    internal void AddContainer(long key, ContainerType type, in ContainerEntry entry)
    {
        if (entry.Cardinality == 0)
            return;

        AddNewContainer(key, type, entry);
    }

    #endregion

    #region Container Management

    private const int InitialArrayContainerSizeInBytes = 64; // 32 entries worth of ushort

    private ContainerEntry CreateArrayContainer(long key)
    {
        _ctx.Allocate(InitialArrayContainerSizeInBytes, out ByteString storage);
        storage.ToSpan<byte>().Clear();

        return new ContainerEntry
        {
            Cardinality = 0,
            Data = storage.Ptr,
            Storage = storage
        };
    }

    /// <summary>
    /// Ensure the array container has room for the given number of entries.
    /// Doubles the buffer size up to BitmapContainerSizeInBytes (8KB).
    /// </summary>
    private void EnsureArrayCapacity(ref ContainerEntry entry, int requiredEntries)
    {
        int requiredBytes = requiredEntries * sizeof(ushort);
        if (requiredBytes <= entry.Storage.Length)
            return;

        int newSize = Math.Max(entry.Storage.Length * 2, requiredBytes);
        newSize = Math.Min(newSize, BitmapContainerSizeInBytes);

        _ctx.Allocate(newSize, out ByteString newStorage);
        int copyBytes = entry.Cardinality * sizeof(ushort);
        if (copyBytes > 0)
            Unsafe.CopyBlockUnaligned(newStorage.Ptr, entry.Data, (uint)copyBytes);

        if (entry.Storage.HasValue)
            _ctx.Release(ref entry.Storage);
        entry.Storage = newStorage;
        entry.Data = newStorage.Ptr;
    }

    internal ContainerEntry CreateBitmapContainer(long key)
    {
        _ctx.Allocate(BitmapContainerSizeInBytes, out ByteString storage);
        storage.ToSpan<byte>().Clear();

        return new ContainerEntry
        {
            Cardinality = 0,
            Data = storage.Ptr,
            Storage = storage
        };
    }

    #endregion

    #region Container Operations

    [SkipLocalsInit]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void AddToContainer(ref ContainerEntry entry, int slot, ushort value)
    {
        ref ContainerType type = ref _types.RawItems[slot];
        switch (type)
        {
            case ContainerType.Array:
                // If value is >= the last element, append (or noop for duplicate) and stay sorted
                if (entry.Cardinality > 0 && value >= (entry.ArrayData)[entry.Cardinality - 1])
                {
                    if (value == (entry.ArrayData)[entry.Cardinality - 1])
                        break; // duplicate of last element — noop
                    if (entry.Cardinality >= ArrayContainerMaxCardinality)
                    {
                        ConvertArrayToBitmap(ref entry, ref type);
                        BitmapContainerAdd(ref entry, value);
                        break;
                    }
                    EnsureArrayCapacity(ref entry, entry.Cardinality + 1);
                    (entry.ArrayData)[entry.Cardinality] = value;
                    entry.Cardinality++;
                    break;
                }
                // Would break sort order — switch to unsorted for O(1) appends
                type = ContainerType.ArrayUnsorted;
                goto case ContainerType.ArrayUnsorted;

            case ContainerType.ArrayUnsorted:
                if (entry.Cardinality >= ArrayContainerMaxCardinality)
                {
                    // At capacity: explode into bitmap scratch, count unique values
                    ulong* scratch = stackalloc ulong[BitmapContainerSizeInUlongs];
                    new Span<byte>(scratch, BitmapContainerSizeInBytes).Clear();
                    ushort* unsortedArr = (ushort*)entry.Data;
                    for (int i = 0; i < entry.Cardinality; i++)
                    {
                        ushort val = unsortedArr[i];
                        scratch[val >> 6] |= 1UL << (val & 63);
                    }
                    int uniqueCount = BitmapContainerCardinality((byte*)scratch);
                    if (uniqueCount >= ArrayContainerMaxCardinality)
                    {
                        _ctx.Allocate(BitmapContainerSizeInBytes, out ByteString bmpStorage);
                        Unsafe.CopyBlockUnaligned(bmpStorage.Ptr, (byte*)scratch, BitmapContainerSizeInBytes);
                        if (entry.Storage.HasValue)
                            _ctx.Release(ref entry.Storage);
                            entry.Storage = bmpStorage;
                            entry.Data = bmpStorage.Ptr;
                            type = ContainerType.Bitmap;
                            entry.Cardinality = uniqueCount;
                        BitmapContainerAdd(ref entry, value);
                        break;
                    }
                    // Has room after dedup — extract sorted array from scratch
                    int sorted = 0;
                    for (int wordIdx = 0; wordIdx < BitmapContainerSizeInUlongs; wordIdx++)
                    {
                        ulong word = scratch[wordIdx];
                        while (word != 0)
                        {
                            int bit = BitOperations.TrailingZeroCount(word);
                            unsortedArr[sorted++] = (ushort)(wordIdx * 64 + bit);
                            word &= word - 1;
                        }
                    }
                    entry.Cardinality = sorted;
                    type = ContainerType.Array;
                }
                EnsureArrayCapacity(ref entry, entry.Cardinality + 1);
                (entry.ArrayData)[entry.Cardinality] = value;
                entry.Cardinality++;
                break;

            case ContainerType.Bitmap:
                BitmapContainerAdd(ref entry, value);
                break;

            case ContainerType.Range:
                if (value == entry.Cardinality)
                {
                    entry.Cardinality++;
                }
                else if (value < entry.Cardinality)
                {
                    // Already in range — noop
                }
                else
                {
                    ConvertRangeForAdd(ref entry, ref type, value);
                }
                break;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool ContainerContains(ref ContainerEntry entry, ContainerType type, ushort value)
    {
        AssertPrepared(type);

        return type switch
        {
            ContainerType.Array => ArrayContainerContains(entry.Data, entry.Cardinality, value),
            ContainerType.Bitmap => BitmapContainerContains(entry.Data, value),
            ContainerType.Range => value < entry.Cardinality,
            _ => false
        };
    }

    #endregion

    #region Array Sorting

    // Threshold: below this count, comparison sort on a small array
    // is cheaper than the bitmap radix sort path.
    private const int BitmapSortThreshold = 128;

    /// <summary>
    /// Assert that a container has been prepared for reading (not ArrayUnsorted).
    /// PrepareForReading() must be called before any read operation.
    /// </summary>
    [Conditional("DEBUG")]
    internal static void AssertPrepared(ContainerType type)
    {
        Debug.Assert(type != ContainerType.ArrayUnsorted,
            "Container is still unsorted. Call PrepareForReading() after all Add calls and before any read operation.");
    }

    /// <summary>
    /// Comparison sort + dedup for small arrays below the bitmap radix sort threshold.
    /// </summary>
    private static void SortSmallArray(ref ContainerEntry entry, ref ContainerType type)
    {
        var arr = entry.ArrayData;
        int count = entry.Cardinality;

#if DEBUG
        arr.Slice(0, count).Sort();
#else
        new Span<ushort>(arr, count).Sort();
#endif

        if (count > 1)
        {
            int write = 1;
            for (int read = 1; read < count; read++)
            {
                if (arr[read] != arr[write - 1])
                    arr[write++] = arr[read];
            }
            count = write;
        }

        entry.Cardinality = count;
        type = ContainerType.Array;
    }

    #endregion

    #region Array Container

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static bool ArrayContainerContains(byte* data, int cardinality, ushort value)
    {
        return ArrayContainerFind((ushort*)data, cardinality, value) >= 0;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static int ArrayContainerFind(ushort* arr, int count, ushort value)
    {
        int lo = 0;
        int hi = count - 1;

        while (lo <= hi)
        {
            int mid = lo + ((hi - lo) >> 1);
            ushort midVal = arr[mid];

            if (midVal == value)
                return mid;
            if (midVal < value)
                lo = mid + 1;
            else
                hi = mid - 1;
        }

        return ~lo;
    }

    /// <summary>
    /// Strategy interface for generic array match operations (AND / ANDNOT).
    /// Both share the same SIMD galloping structure; only the keep/discard logic differs.
    /// </summary>
    private interface IArrayMatchStrategy
    {
        /// <summary>true for AND (keep matches), false for ANDNOT (discard matches).</summary>
        static abstract bool KeepOnMatch { get; }
        /// <summary>false for AND (skip A values not in B), true for ANDNOT (keep A values not in B).</summary>
        static abstract bool KeepOnMissInSmaller { get; }
    }

    private struct AndStrategy : IArrayMatchStrategy
    {
        public static bool KeepOnMatch => true;
        public static bool KeepOnMissInSmaller => false;
    }

    private struct AndNotStrategy : IArrayMatchStrategy
    {
        public static bool KeepOnMatch => false;
        public static bool KeepOnMissInSmaller => true;
    }

    /// <summary>
    /// Compute the intersection of two array containers, writing the result to dst.
    /// Uses SIMD galloping when Vector256 is available.
    /// </summary>
    internal static int ArrayContainerAnd(ushort* a, int aLen, ushort* b, int bLen, ushort* dst)
    {
        if (AdvInstructionSet.IsAcceleratedVector256)
            return ArrayContainerMatchVectorized<AndStrategy>(a, aLen, b, bLen, dst);
        return ArrayContainerMatchScalar<AndStrategy>(a, aLen, b, bLen, dst);
    }

    /// <summary>
    /// Compute A AND NOT B for two array containers.
    /// Uses SIMD galloping to skip blocks in B that don't affect A.
    /// </summary>
    internal static int ArrayContainerAndNot(ushort* a, int aLen, ushort* b, int bLen, ushort* dst)
    {
        if (AdvInstructionSet.IsAcceleratedVector256)
            return ArrayContainerMatchVectorized<AndNotStrategy>(a, aLen, b, bLen, dst);
        return ArrayContainerMatchScalar<AndNotStrategy>(a, aLen, b, bLen, dst);
    }

    private static int ArrayContainerMatchVectorized<TStrategy>(ushort* a, int aLen, ushort* b, int bLen, ushort* dst)
        where TStrategy : struct, IArrayMatchStrategy
    {
        uint N = (uint)Vector256<ushort>.Count; // 16
        int ai = 0, bi = 0, di = 0;

        while (ai < aLen && bi + (int)N <= bLen)
        {
            ushort val = a[ai];

            // If val is past the current block of B, advance B
            if (val > b[bi + N - 1])
            {
                bi += (int)N;
                continue;
            }

            // If val is before the current block of B, it's not in B
            if (val < b[bi])
            {
                if (TStrategy.KeepOnMissInSmaller)
                    dst[di++] = val;
                ai++;
                continue;
            }

            // Check if val exists in this block of B
            Vector256<ushort> vVal = Vector256.Create(val);
            Vector256<ushort> vBlock = Vector256.Load(b + bi);
            bool found = Vector256.EqualsAny(vVal, vBlock);
            if (found == TStrategy.KeepOnMatch)
                dst[di++] = val;

            ai++;
        }

        // Scalar tail
        while (ai < aLen && bi < bLen)
        {
            if (a[ai] < b[bi])
            {
                if (TStrategy.KeepOnMissInSmaller)
                    dst[di++] = a[ai];
                ai++;
            }
            else if (a[ai] > b[bi])
                bi++;
            else
            {
                if (TStrategy.KeepOnMatch)
                    dst[di++] = a[ai];
                ai++;
                bi++;
            }
        }

        if (TStrategy.KeepOnMissInSmaller)
        {
            while (ai < aLen)
                dst[di++] = a[ai++];
        }

        return di;
    }

    private static int ArrayContainerMatchScalar<TStrategy>(ushort* a, int aLen, ushort* b, int bLen, ushort* dst)
        where TStrategy : struct, IArrayMatchStrategy
    {
        int ai = 0, bi = 0, di = 0;

        while (ai < aLen && bi < bLen)
        {
            if (a[ai] < b[bi])
            {
                if (TStrategy.KeepOnMissInSmaller)
                    dst[di++] = a[ai];
                ai++;
            }
            else if (a[ai] > b[bi])
                bi++;
            else
            {
                if (TStrategy.KeepOnMatch)
                    dst[di++] = a[ai];
                ai++;
                bi++;
            }
        }

        if (TStrategy.KeepOnMissInSmaller)
        {
            while (ai < aLen)
                dst[di++] = a[ai++];
        }

        return di;
    }

    /// <summary>
    /// Compute the union of two array containers. dst must have space for aLen + bLen entries.
    /// Returns the number of elements in the result.
    /// </summary>
    internal static int ArrayContainerOr(ushort* a, int aLen, ushort* b, int bLen, ushort* dst)
    {
        int ai = 0, bi = 0, di = 0;

        while (ai < aLen && bi < bLen)
        {
            if (a[ai] < b[bi])
                dst[di++] = a[ai++];
            else if (a[ai] > b[bi])
                dst[di++] = b[bi++];
            else
            {
                dst[di++] = a[ai];
                ai++;
                bi++;
            }
        }

        while (ai < aLen)
            dst[di++] = a[ai++];
        while (bi < bLen)
            dst[di++] = b[bi++];

        return di;
    }

    #endregion

    #region Range Container

    /// <summary>
    /// Convert a Range container to Bitmap or Array to handle an Add outside the contiguous range.
    /// </summary>
    private void ConvertRangeForAdd(ref ContainerEntry entry, ref ContainerType type, ushort value)
    {
        Debug.Assert(type == ContainerType.Range);
        int rangeCount = entry.Cardinality;

        if (rangeCount >= ArrayContainerMaxCardinality)
        {
            // Range alone fills an array container — use bitmap
            ConvertRangeToBitmap(ref entry, ref type);
            BitmapContainerAdd(ref entry, value);
        }
        else
        {
            // Create unsorted array: copy range values 0..rangeCount-1, then append the new value
            int newCount = rangeCount + 1;
            _ctx.Allocate(Math.Max(InitialArrayContainerSizeInBytes, newCount * sizeof(ushort)), out ByteString storage);
            ushort* arr = (ushort*)storage.Ptr;
            for (int i = 0; i < rangeCount; i++)
                arr[i] = (ushort)i;
            arr[rangeCount] = value;

            if (entry.Storage.HasValue)
                _ctx.Release(ref entry.Storage);
            entry.Storage = storage;
            entry.Data = storage.Ptr;
            entry.Cardinality = newCount;
            type = ContainerType.ArrayUnsorted;
        }
    }

    /// <summary>
    /// Fill a cleared bitmap buffer with bits 0..rangeCount-1 set.
    /// </summary>
    internal static void FillBitmapFromRange(ulong* bitmap, int rangeCount)
    {
        int fullWords = rangeCount / 64;
        new Span<ulong>(bitmap, fullWords).Fill(ulong.MaxValue);
        int remainder = rangeCount & 63;
        if (remainder > 0)
            bitmap[fullWords] = (1UL << remainder) - 1;
    }

    private void ConvertRangeToBitmap(ref ContainerEntry entry, ref ContainerType type)
    {
        Debug.Assert(type == ContainerType.Range);

        _ctx.Allocate(BitmapContainerSizeInBytes, out ByteString storage);
        ulong* bitmap = (ulong*)storage.Ptr;
        ClearBitmap(bitmap);
        FillBitmapFromRange(bitmap, entry.Cardinality);

        if (entry.Storage.HasValue)
            _ctx.Release(ref entry.Storage);

        entry.Storage = storage;
        entry.Data = storage.Ptr;
        type = ContainerType.Bitmap;
    }

    #endregion

    #region Bitmap Container

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void BitmapContainerAdd(ref ContainerEntry e, ushort value)
    {
        var bitmap = e.BitmapData;
        int wordIdx = value >> 6;
        ulong mask = 1UL << (value & 63);

        if ((bitmap[wordIdx] & mask) == 0)
        {
            bitmap[wordIdx] |= mask;
            e.Cardinality++;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static bool BitmapContainerContains(byte* data, ushort value)
    {
        ulong* bitmap = (ulong*)data;
        return (bitmap[value >> 6] & (1UL << (value & 63))) != 0;
    }

    internal static int BitmapContainerCardinality(byte* data)
    {
        ulong* bitmap = (ulong*)data;
        int count = 0;

        // Vector256 load fetches 4 ulongs (32 bytes) per iteration, prefetching the next cache line.
        // PopCount each element after the load.
        if (AdvInstructionSet.IsAcceleratedVector256)
        {
            int i = 0;
            for (; i + Vector256<ulong>.Count <= BitmapContainerSizeInUlongs; i += Vector256<ulong>.Count)
            {
                Vector256<ulong> vec = Vector256.Load(bitmap + i);
                count += BitOperations.PopCount(vec.GetElement(0));
                count += BitOperations.PopCount(vec.GetElement(1));
                count += BitOperations.PopCount(vec.GetElement(2));
                count += BitOperations.PopCount(vec.GetElement(3));
            }
            for (; i < BitmapContainerSizeInUlongs; i++)
                count += BitOperations.PopCount(bitmap[i]);
        }
        else
        {
            for (int i = 0; i < BitmapContainerSizeInUlongs; i++)
                count += BitOperations.PopCount(bitmap[i]);
        }

        return count;
    }

    /// <summary>Helper to clear an 8KB bitmap buffer.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void ClearBitmap(ulong* bitmap)
    {
        new Span<byte>(bitmap, BitmapContainerSizeInBytes).Clear();
    }

    /// <summary>
    /// Convert an array container to a bitmap from a sorted ushort array.
    /// </summary>
    internal static void ArrayToBitmap(ushort* arr, int arrLen, ulong* bitmap)
    {
        ClearBitmap(bitmap);
        for (int i = 0; i < arrLen; i++)
        {
            ushort val = arr[i];
            bitmap[val >> 6] |= 1UL << (val & 63);
        }
    }

    /// <summary>
    /// Convert a bitmap container to a sorted ushort array.
    /// Returns the number of elements written.
    /// </summary>
    internal static int BitmapToArray(ulong* bitmap, ushort* arr)
    {
        int count = 0;
        for (int wordIdx = 0; wordIdx < BitmapContainerSizeInUlongs; wordIdx++)
        {
            ulong word = bitmap[wordIdx];
            while (word != 0)
            {
                int bit = BitOperations.TrailingZeroCount(word);
                arr[count++] = (ushort)(wordIdx * 64 + bit);
                word &= word - 1; // clear lowest set bit
            }
        }
        return count;
    }

    #endregion


    #region Container Conversions

    /// <summary>
    /// Convert an array container (sorted or unsorted) to bitmap.
    /// For unsorted arrays with possible duplicates, cardinality is recounted via popcount.
    /// </summary>
    private void ConvertArrayToBitmap(ref ContainerEntry entry, ref ContainerType type)
    {
        Debug.Assert(type is ContainerType.Array or ContainerType.ArrayUnsorted);

        bool unsorted = type == ContainerType.ArrayUnsorted;
        ushort* arr = (ushort*)entry.Data;
        int count = entry.Cardinality;

        _ctx.Allocate(BitmapContainerSizeInBytes, out ByteString newStorage);
        ArrayToBitmap(arr, count, (ulong*)newStorage.Ptr);

        if (unsorted)
        {
            var updatedCount = BitmapContainerCardinality(newStorage.Ptr);
            entry.Cardinality = updatedCount;
        }
        if (entry.Storage.HasValue)
            _ctx.Release(ref entry.Storage);

        entry.Storage = newStorage;
        entry.Data = newStorage.Ptr;
        type = ContainerType.Bitmap;
    }




    #endregion

    internal ByteStringContext Context => _ctx;

    #region SIMD Bitmap Operations

    private interface IBitmapOp
    {
        static abstract ulong Apply(ulong a, ulong b);
        static abstract Vector128<ulong> Apply(Vector128<ulong> a, Vector128<ulong> b);
        static abstract Vector256<ulong> Apply(Vector256<ulong> a, Vector256<ulong> b);
        static abstract Vector512<ulong> Apply(Vector512<ulong> a, Vector512<ulong> b);
    }

    private struct AndOp : IBitmapOp
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)] public static ulong Apply(ulong a, ulong b) => a & b;
        [MethodImpl(MethodImplOptions.AggressiveInlining)] public static Vector128<ulong> Apply(Vector128<ulong> a, Vector128<ulong> b) => a & b;
        [MethodImpl(MethodImplOptions.AggressiveInlining)] public static Vector256<ulong> Apply(Vector256<ulong> a, Vector256<ulong> b) => a & b;
        [MethodImpl(MethodImplOptions.AggressiveInlining)] public static Vector512<ulong> Apply(Vector512<ulong> a, Vector512<ulong> b) => a & b;
    }

    private struct OrOp : IBitmapOp
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)] public static ulong Apply(ulong a, ulong b) => a | b;
        [MethodImpl(MethodImplOptions.AggressiveInlining)] public static Vector128<ulong> Apply(Vector128<ulong> a, Vector128<ulong> b) => a | b;
        [MethodImpl(MethodImplOptions.AggressiveInlining)] public static Vector256<ulong> Apply(Vector256<ulong> a, Vector256<ulong> b) => a | b;
        [MethodImpl(MethodImplOptions.AggressiveInlining)] public static Vector512<ulong> Apply(Vector512<ulong> a, Vector512<ulong> b) => a | b;
    }

    private struct AndNotOp : IBitmapOp
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)] public static ulong Apply(ulong a, ulong b) => a & ~b;
        [MethodImpl(MethodImplOptions.AggressiveInlining)] public static Vector128<ulong> Apply(Vector128<ulong> a, Vector128<ulong> b) => Vector128.AndNot(a, b);
        [MethodImpl(MethodImplOptions.AggressiveInlining)] public static Vector256<ulong> Apply(Vector256<ulong> a, Vector256<ulong> b) => Vector256.AndNot(a, b);
        [MethodImpl(MethodImplOptions.AggressiveInlining)] public static Vector512<ulong> Apply(Vector512<ulong> a, Vector512<ulong> b) => Vector512.AndNot(a, b);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static int BitmapAndSimd(ulong* a, ulong* b, ulong* dst, int count) => BitmapOpDispatch<AndOp>(a, b, dst, count);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static int BitmapOrSimd(ulong* a, ulong* b, ulong* dst, int count) => BitmapOpDispatch<OrOp>(a, b, dst, count);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static int BitmapAndNotSimd(ulong* a, ulong* b, ulong* dst, int count) => BitmapOpDispatch<AndNotOp>(a, b, dst, count);

    // Scalar methods for test verification
    internal static int BitmapAndScalar(ulong* a, ulong* b, ulong* dst, int count) => BitmapOpScalar<AndOp>(a, b, dst, count);
    internal static int BitmapOrScalar(ulong* a, ulong* b, ulong* dst, int count) => BitmapOpScalar<OrOp>(a, b, dst, count);
    internal static int BitmapAndNotScalar(ulong* a, ulong* b, ulong* dst, int count) => BitmapOpScalar<AndNotOp>(a, b, dst, count);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int BitmapOpDispatch<TOp>(ulong* a, ulong* b, ulong* dst, int count) where TOp : struct, IBitmapOp
    {
        if (AdvInstructionSet.IsAcceleratedVector512)
            return BitmapOpVector512<TOp>(a, b, dst, count);
        if (AdvInstructionSet.IsAcceleratedVector256)
            return BitmapOpVector256<TOp>(a, b, dst, count);
        if (AdvInstructionSet.IsAcceleratedVector128)
            return BitmapOpVector128<TOp>(a, b, dst, count);
        return BitmapOpScalar<TOp>(a, b, dst, count);
    }

    private static int BitmapOpVector512<TOp>(ulong* a, ulong* b, ulong* dst, int count) where TOp : struct, IBitmapOp
    {
        int N = Vector512<ulong>.Count;
        int cardinality = 0, i = 0;
        for (; i + N <= count; i += N)
        {
            TOp.Apply(Vector512.Load(a + i), Vector512.Load(b + i)).Store(dst + i);
            for (int j = 0; j < N; j++)
                cardinality += BitOperations.PopCount(dst[i + j]);
        }
        for (; i < count; i++)
        { dst[i] = TOp.Apply(a[i], b[i]); cardinality += BitOperations.PopCount(dst[i]); }
        return cardinality;
    }

    private static int BitmapOpVector256<TOp>(ulong* a, ulong* b, ulong* dst, int count) where TOp : struct, IBitmapOp
    {
        int N = Vector256<ulong>.Count;
        int cardinality = 0, i = 0;
        for (; i + N <= count; i += N)
        {
            TOp.Apply(Vector256.Load(a + i), Vector256.Load(b + i)).Store(dst + i);
            for (int j = 0; j < N; j++)
                cardinality += BitOperations.PopCount(dst[i + j]);
        }
        for (; i < count; i++)
        { dst[i] = TOp.Apply(a[i], b[i]); cardinality += BitOperations.PopCount(dst[i]); }
        return cardinality;
    }

    private static int BitmapOpVector128<TOp>(ulong* a, ulong* b, ulong* dst, int count) where TOp : struct, IBitmapOp
    {
        int N = Vector128<ulong>.Count;
        int cardinality = 0, i = 0;
        for (; i + N <= count; i += N)
        {
            TOp.Apply(Vector128.Load(a + i), Vector128.Load(b + i)).Store(dst + i);
            for (int j = 0; j < N; j++)
                cardinality += BitOperations.PopCount(dst[i + j]);
        }
        for (; i < count; i++)
        { dst[i] = TOp.Apply(a[i], b[i]); cardinality += BitOperations.PopCount(dst[i]); }
        return cardinality;
    }

    private static int BitmapOpScalar<TOp>(ulong* a, ulong* b, ulong* dst, int count) where TOp : struct, IBitmapOp
    {
        int cardinality = 0;
        for (int i = 0; i < count; i++)
        { dst[i] = TOp.Apply(a[i], b[i]); cardinality += BitOperations.PopCount(dst[i]); }
        return cardinality;
    }

    internal static ContainerEntry CloneContainer(ByteStringContext ctx, ref ContainerEntry entry, ContainerType type)
    {
        if (type == ContainerType.Range)
            return new ContainerEntry { Cardinality = entry.Cardinality, Data = null, Storage = default };

        if (!entry.Storage.HasValue)
            return default;

        int dataSize = entry.Storage.Length;
        ctx.Allocate(dataSize, out ByteString storage);
        Unsafe.CopyBlockUnaligned(storage.Ptr, entry.Data, (uint)dataSize);
        return new ContainerEntry { Cardinality = entry.Cardinality, Data = storage.Ptr, Storage = storage };
    }

    #endregion

    public void Dispose()
    {
        // Walk entries directly — more cache-friendly than index indirection.
        // Skip free entries (Type == Free).
        if (_entries.IsValid)
        {
            ContainerEntry* entries = _entries.RawItems;
            ContainerType* types = _types.RawItems;
            int count = _entries.Count;
            for (int i = 0; i < count; i++)
            {
                if (types[i] != ContainerType.Free && entries[i].Storage.HasValue)
                    _ctx.Release(ref entries[i].Storage);
            }

            _entries.Dispose(_ctx);
        }

        if (_types.IsValid)
            _types.Dispose(_ctx);

        if (_index.IsValid)
            _index.Dispose(_ctx);
    }
}

public enum ContainerType : byte
{
    /// <summary>Sorted ushort array. Binary search for Contains. Merge-based set ops.</summary>
    Array = 0,
    /// <summary>8KB bitmap (1024 ulongs). Direct bit access.</summary>
    Bitmap = 1,
    /// <summary>
    /// Contiguous values 0..Cardinality-1 are set. No data allocation needed.
    /// Cardinality == BitsPerContainer means all 65536 bits set (full container).
    /// Sequential Add at the end is O(1) increment.
    /// </summary>
    Range = 2,
    /// <summary>
    /// Unsorted ushort array. Add is O(1) append. On first read (Contains, set ops, iteration),
    /// sorts and deduplicates, converting to Array. Avoids O(log n + shift) per Add.
    /// </summary>
    ArrayUnsorted = 3,
    /// <summary>Tombstone marker for free-list entries in the entries array.</summary>
    Free = 0xFF
}

public unsafe struct ContainerEntry
{
    /// <summary>
    /// Direct pointer to container data for Array, ArrayUnsorted, and Bitmap containers.
    /// Null for Range containers (which require no data allocation).
    /// We pay 8 bytes per entry to cache this instead of going through Storage.Ptr,
    /// which is a double-dereference (ByteString._pointer->Ptr). Every Contains, Add,
    /// and iterator step accesses this pointer. The 8 bytes per container is negligible
    /// compared to container data (64B–8KB each), and it also avoids a null check on
    /// Storage for Range containers which have Storage=default.
    /// </summary>
    public byte* Data;

    /// <summary>Memory handle for disposal. Default for Range containers.</summary>
    internal ByteString Storage;

    /// <summary>Number of set bits (0..65536)..</summary>
    public int Cardinality;

    /// <summary>
    /// Container key (value >> 16). Allows walking entries without index indirection.
    /// The key allows us to have ~140 T containers in the bitmap (2^47), bit engouh
    /// </summary>
    public uint Key;

    /// <summary>
    /// This is to reuse the Key field in a clearer manner
    /// </summary>
    internal uint NextFreeSlot
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => Key;
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        set => Key = value;
    }


#if DEBUG
    /// <summary>
    /// Access container data as ushort array. Debug: Span with bounds checking. Release: raw pointer.
    /// </summary>
    public Span<ushort> ArrayData
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get
        {
            Debug.Assert(Data != null, "ArrayData is null");
            return new Span<ushort>(Data, Storage.Length / sizeof(ushort));
        }
    }

    /// <summary>
    /// Access container data as bitmap (ulong array). Debug: Span with bounds checking. Release: raw pointer.
    /// </summary>
    public Span<ulong> BitmapData
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get
        {
            Debug.Assert(Data != null, "BitmapData is null");
            return new Span<ulong>(Data, RoaringBitmap.BitmapContainerSizeInUlongs);
        }
    }
#else
    /// <summary>
    /// Access container data as ushort array. Debug: Span with bounds checking. Release: raw pointer.
    /// </summary>
    public ushort* ArrayData
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => (ushort*)Data;
    }

    /// <summary>
    /// Access container data as bitmap (ulong array). Debug: Span with bounds checking. Release: raw pointer.
    /// </summary>
    public ulong* BitmapData
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => (ulong*)Data;
    }
#endif

    /// <summary>Raw ushort pointer for SIMD operations and methods requiring pointers.</summary>
    public ushort* ArrayPtr
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => (ushort*)Data;
    }

    /// <summary>Raw ulong pointer for SIMD operations and methods requiring pointers.</summary>
    public ulong* BitmapPtr
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => (ulong*)Data;
    }
}
