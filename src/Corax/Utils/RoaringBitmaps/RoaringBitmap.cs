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
/// for the bitmap data. Supports 64-bit values by using the high 48 bits as container keys
/// and the low 16 bits as positions within 8KB containers.
///
/// Container types:
/// - Array: sorted ushort[] for sparse data (cardinality &lt;= 4096, up to 8KB)
/// - Bitmap: 8KB fixed bitmap (1024 ulongs) for dense data (4097..65535)
/// - Range: contiguous values 0..count-1 (no data allocation). count=65536 means full.
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
    /// <summary>Key → entry slot index. -1 = absent. Length = maxKey + 1.</summary>
    private NativeList<int> _index;
    private int _containerCount;
    /// <summary>Head of free list in _entries. Dead entry's Cardinality field stores next free index; -2 = end.</summary>
    private int _freeListHead;

    public RoaringBitmap(ByteStringContext ctx)
    {
        _ctx = ctx;
        _entries = new NativeList<ContainerEntry>();
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
            long total = 0;
            int* idx = _index.RawItems;
            for (int k = 0; k < _index.Count; k++)
            {
                int slot = idx[k];
                if (slot >= 0)
                {
                    ref ContainerEntry entry = ref _entries[slot];
                    if (entry.Type == ContainerType.ArrayUnsorted)
                        SortAndDeduplicateArray(ref entry);
                    total += entry.Cardinality;
                }
            }
            return total;
        }
    }

    public readonly bool IsEmpty => _containerCount == 0;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Add(long value)
    {
        long key = value >> ContainerKeyShift;
        ushort low = (ushort)(value & ContainerValueMask);

        int slot = GetSlotForKey(key);
        if (slot >= 0)
        {
            ref ContainerEntry entry = ref _entries[slot];
            AddToContainer(ref entry, low);
        }
        else
        {
            // First value in this container: Range if starting from 0, Array otherwise
            if (low == 0)
            {
                AddNewContainer(key, new ContainerEntry
                {
                    Key = key,
                    Type = ContainerType.Range,
                    Cardinality = 1,
                    Data = null,
                    Storage = default
                });
            }
            else
            {
                // First value not at 0: start as unsorted array for O(1) appends
                ContainerEntry newEntry = CreateArrayContainer(key);
                ((ushort*)newEntry.Data)[0] = low;
                newEntry.Cardinality = 1;
                newEntry.Type = ContainerType.ArrayUnsorted;
                AddNewContainer(key, newEntry);
            }
        }
    }

    public void AddRange(long start, long exclusiveEnd)
    {
        if (start >= exclusiveEnd)
            return;

        long startKey = start >> ContainerKeyShift;
        long endKey = (exclusiveEnd - 1) >> ContainerKeyShift;

        for (long key = startKey; key <= endKey; key++)
        {
            ushort lo = (key == startKey) ? (ushort)(start & ContainerValueMask) : (ushort)0;
            ushort hi = (key == endKey) ? (ushort)((exclusiveEnd - 1) & ContainerValueMask) : (ushort)(BitsPerContainer - 1);

            if (lo == 0 && hi == BitsPerContainer - 1)
            {
                // Entire container becomes a full Range
                int slot = GetSlotForKey(key);
                if (slot >= 0)
                {
                    ref ContainerEntry entry = ref _entries[slot];
                    if (entry.Storage.HasValue)
                        _ctx.Release(ref entry.Storage);
                    entry.Data = null;
                    entry.Storage = default;
                    entry.Type = ContainerType.Range;
                    entry.Cardinality = BitsPerContainer;
                }
                else
                {
                    AddNewContainer(key, new ContainerEntry
                    {
                        Key = key,
                        Type = ContainerType.Range,
                        Cardinality = BitsPerContainer,
                        Data = null,
                        Storage = default
                    });
                }
            }
            else if (lo == 0)
            {
                // Range from start: use Range container
                int slot = GetSlotForKey(key);
                int rangeLen = hi + 1;
                if (slot >= 0)
                {
                    ref ContainerEntry e = ref _entries[slot];
                    // Convert to bitmap, set bits, optimize back
                    if (e.Type == ContainerType.Range)
                    {
                        if (rangeLen > e.Cardinality)
                            e.Cardinality = rangeLen;
                        continue;
                    }
                    if (e.Type == ContainerType.Array)
                        ConvertArrayToBitmap(ref e);
                    ulong* bitmap = (ulong*)e.Data;
                    for (int v = lo; v <= hi; v++)
                        bitmap[v >> 6] |= 1UL << (v & 63);
                    e.Cardinality = BitmapContainerCardinality(e.Data);
                }
                else
                {
                    AddNewContainer(key, new ContainerEntry
                    {
                        Key = key,
                        Type = ContainerType.Range,
                        Cardinality = rangeLen,
                        Data = null,
                        Storage = default
                    });
                }
            }
            else
            {
                // Partial range not from start — use bitmap
                int slot = GetSlotForKey(key);
                if (slot < 0)
                {
                    ContainerEntry newEntry = CreateBitmapContainer(key);
                    slot = AddNewContainer(key, newEntry);
                }

                ref ContainerEntry e = ref _entries[slot];

                if (e.Type == ContainerType.Array)
                    ConvertArrayToBitmap(ref e);
                else if (e.Type == ContainerType.Range)
                    ConvertRangeToBitmap(ref e);

                if (e.Type == ContainerType.Bitmap)
                {
                    ulong* bitmap = (ulong*)e.Data;
                    for (int v = lo; v <= hi; v++)
                        bitmap[v >> 6] |= 1UL << (v & 63);
                    e.Cardinality = BitmapContainerCardinality(e.Data);
                }
            }
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly bool Contains(long value)
    {
        long key = value >> ContainerKeyShift;
        ushort low = (ushort)(value & ContainerValueMask);

        int slot = GetSlotForKey(key);
        if (slot < 0)
            return false;

        ref ContainerEntry entry = ref _entries[slot];
        return ContainerContains(ref entry, low);
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
        return new RoaringBitmapIterator();
    }

    /// <summary>
    /// Optimize containers: convert Bitmap containers to Range if all set bits are
    /// contiguous from 0, or to Array if sparse enough.
    /// </summary>
    public void Optimize()
    {
        int* idx = _index.RawItems;
        for (int key = 0; key < _index.Count; key++)
        {
            int slot = idx[key];
            if (slot < 0) continue;

            ref ContainerEntry entry = ref _entries[slot];
            if (entry.Type == ContainerType.Bitmap)
            {
                if (TryConvertBitmapToRange(ref entry))
                    continue;
                if (entry.Cardinality <= ArrayContainerMaxCardinality)
                    ConvertBitmapToArray(ref entry);
            }
        }
    }

    private bool TryConvertBitmapToRange(ref ContainerEntry entry)
    {
        ulong* bitmap = (ulong*)entry.Data;
        int cardinality = entry.Cardinality;

        // Check if bits 0..cardinality-1 are all set and everything after is clear
        int fullWords = cardinality / 64;
        for (int i = 0; i < fullWords; i++)
        {
            if (bitmap[i] != ulong.MaxValue)
                return false;
        }

        int remainder = cardinality & 63;
        if (remainder > 0 && bitmap[fullWords] != (1UL << remainder) - 1)
            return false;

        // Verify remaining words are zero
        for (int i = fullWords + (remainder > 0 ? 1 : 0); i < BitmapContainerSizeInUlongs; i++)
        {
            if (bitmap[i] != 0)
                return false;
        }

        // Convert to Range
        if (entry.Storage.HasValue)
            _ctx.Release(ref entry.Storage);

        entry.Data = null;
        entry.Storage = default;
        entry.Type = ContainerType.Range;
        return true;
    }

    #region In-place Set Operations

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
                AndContainerInPlace(ref myEntry, ref otherEntry);
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
                OrContainerInPlace(ref _entries[mySlot], ref otherEntry);
            }
            else
            {
                AddNewContainer(key, RoaringBitmapSetOps.CloneContainer(_ctx, ref otherEntry));
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
            AndNotContainerInPlace(ref _entries[mySlot], ref otherEntry);
            if (_entries[mySlot].Cardinality == 0)
                FreeContainer(key, mySlot);
        }
    }

    private void AndContainerInPlace(ref ContainerEntry left, ref ContainerEntry right)
    {
        EnsureSorted(ref left);
        // Note: right belongs to another bitmap, but EnsureSorted is safe (just sorts in-place)

        if (left.Type == ContainerType.Range)
            ConvertRangeToBitmap(ref left);
        if (right.Type == ContainerType.Range)
        {
            ContainerEntry temp = MaterializeRangeToBitmapTemp(ref right);
            AndContainerInPlace(ref left, ref temp);
            _ctx.Release(ref temp.Storage);
            return;
        }

        switch (left.Type, right.Type)
        {
            case (ContainerType.Bitmap, ContainerType.Bitmap):
                left.Cardinality = RoaringBitmapSetOps.BitmapAndSimd(
                    (ulong*)left.Data, (ulong*)right.Data, (ulong*)left.Data, BitmapContainerSizeInUlongs);
                OptimizeContainerType(ref left);
                break;

            case (ContainerType.Bitmap, ContainerType.Array):
            {
                // Result is at most right.Cardinality entries — always an array
                ushort* arr = (ushort*)right.Data;
                ulong* bmp = (ulong*)left.Data;
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
                left.Type = ContainerType.Array;
                left.Cardinality = count;
                break;
            }

            case (ContainerType.Array, ContainerType.Bitmap):
            {
                // Filter left array against right bitmap, in-place
                ushort* arr = (ushort*)left.Data;
                ulong* bmp = (ulong*)right.Data;
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
                ushort* a = (ushort*)left.Data;
                ushort* b = (ushort*)right.Data;
                int count = ArrayContainerAnd(a, left.Cardinality, b, right.Cardinality, a);
                left.Cardinality = count;
                break;
            }
        }
    }

    private void OrContainerInPlace(ref ContainerEntry left, ref ContainerEntry right)
    {
        EnsureSorted(ref left);

        if (left.Type == ContainerType.Range)
            ConvertRangeToBitmap(ref left);
        if (right.Type == ContainerType.Range)
        {
            ContainerEntry temp = MaterializeRangeToBitmapTemp(ref right);
            OrContainerInPlace(ref left, ref temp);
            _ctx.Release(ref temp.Storage);
            return;
        }

        switch (left.Type, right.Type)
        {
            case (ContainerType.Bitmap, ContainerType.Bitmap):
                left.Cardinality = RoaringBitmapSetOps.BitmapOrSimd(
                    (ulong*)left.Data, (ulong*)right.Data, (ulong*)left.Data, BitmapContainerSizeInUlongs);
                OptimizeContainerType(ref left);
                break;

            case (ContainerType.Bitmap, ContainerType.Array):
            {
                ulong* bmp = (ulong*)left.Data;
                ushort* arr = (ushort*)right.Data;
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
                OptimizeContainerType(ref left);
                break;
            }

            case (ContainerType.Array, ContainerType.Array):
            {
                int maxResult = left.Cardinality + right.Cardinality;
                if (maxResult > ArrayContainerMaxCardinality)
                {
                    // Result will be a bitmap — convert left, then OR
                    ConvertArrayToBitmap(ref left);
                    OrContainerInPlace(ref left, ref right);
                }
                else
                {
                    // Merge two sorted arrays into a new buffer
                    _ctx.Allocate(Math.Max(InitialArrayContainerSizeInBytes, maxResult * sizeof(ushort)), out ByteString newStorage);
                    int count = ArrayContainerOr((ushort*)left.Data, left.Cardinality, (ushort*)right.Data, right.Cardinality, (ushort*)newStorage.Ptr);
                    _ctx.Release(ref left.Storage);
                    left.Storage = newStorage;
                    left.Data = newStorage.Ptr;
                    left.Cardinality = count;
                }
                break;
            }

            case (ContainerType.Array, ContainerType.Bitmap):
            {
                ConvertArrayToBitmap(ref left);
                OrContainerInPlace(ref left, ref right);
                break;
            }
        }
    }


    private void AndNotContainerInPlace(ref ContainerEntry left, ref ContainerEntry right)
    {
        EnsureSorted(ref left);

        if (left.Type == ContainerType.Range)
            ConvertRangeToBitmap(ref left);
        if (right.Type == ContainerType.Range)
        {
            ContainerEntry temp = MaterializeRangeToBitmapTemp(ref right);
            AndNotContainerInPlace(ref left, ref temp);
            _ctx.Release(ref temp.Storage);
            return;
        }

        switch (left.Type, right.Type)
        {
            case (ContainerType.Bitmap, ContainerType.Bitmap):
                left.Cardinality = RoaringBitmapSetOps.BitmapAndNotSimd(
                    (ulong*)left.Data, (ulong*)right.Data, (ulong*)left.Data, BitmapContainerSizeInUlongs);
                OptimizeContainerType(ref left);
                break;

            case (ContainerType.Bitmap, ContainerType.Array):
            {
                ulong* bmp = (ulong*)left.Data;
                ushort* arr = (ushort*)right.Data;
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
                OptimizeContainerType(ref left);
                break;
            }

            case (ContainerType.Array, ContainerType.Bitmap):
            {
                ushort* arr = (ushort*)left.Data;
                ulong* bmp = (ulong*)right.Data;
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
                ushort* a = (ushort*)left.Data;
                ushort* b = (ushort*)right.Data;
                int count = ArrayContainerAndNot(a, left.Cardinality, b, right.Cardinality, a);
                left.Cardinality = count;
                break;
            }
        }
    }

    /// <summary>
    /// Create a temporary bitmap from a Range container (for read-only right-hand side).
    /// Caller must release the returned storage.
    /// </summary>
    private ContainerEntry MaterializeRangeToBitmapTemp(ref ContainerEntry entry)
    {
        Debug.Assert(entry.Type == ContainerType.Range);

        _ctx.Allocate(BitmapContainerSizeInBytes, out ByteString storage);
        ulong* bitmap = (ulong*)storage.Ptr;
        new Span<byte>(bitmap, BitmapContainerSizeInBytes).Clear();

        int rangeCount = entry.Cardinality;
        int fullWords = rangeCount / 64;
        for (int i = 0; i < fullWords; i++)
            bitmap[i] = ulong.MaxValue;
        int remainder = rangeCount & 63;
        if (remainder > 0)
            bitmap[fullWords] = (1UL << remainder) - 1;

        return new ContainerEntry
        {
            Key = entry.Key,
            Data = storage.Ptr,
            Cardinality = entry.Cardinality,
            Storage = storage,
            Type = ContainerType.Bitmap
        };
    }

    /// <summary>
    /// After in-place modification of a bitmap container, convert to Array if sparse enough.
    /// </summary>
    private void OptimizeContainerType(ref ContainerEntry entry)
    {
        if (entry.Type != ContainerType.Bitmap)
            return;

        if (entry.Cardinality <= ArrayContainerMaxCardinality)
            ConvertBitmapToArray(ref entry);
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
    /// Ensure the index array covers the given key, filling new slots with -1.
    /// Keeps length aligned to 16 ints (512 bits) for SIMD scanning.
    /// </summary>
    private void EnsureIndexCoversKey(long key)
    {
        if (key < _index.Count)
            return;

        int needed = (int)(key + 1);
        // Align to 16 ints (512 bits / 32 bits per int) for SIMD-friendly scanning
        needed = (needed + 15) & ~15;

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
    private int AddNewContainer(long key, ContainerEntry entry)
    {
        EnsureIndexCoversKey(key);

        int slot;
        if (_freeListHead != FreeSlotTerminator)
        {
            // Reuse a tombstone slot
            slot = _freeListHead;
            _freeListHead = _entries[slot].Cardinality; // next pointer stored in Cardinality
            _entries[slot] = entry;
        }
        else
        {
            _entries.EnsureCapacityFor(_ctx, 1);
            slot = _entries.Count;
            _entries.AddUnsafe(entry);
        }

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

        // Add to free list: store next pointer in Cardinality field of the dead entry
        entry = default;
        entry.Cardinality = _freeListHead;
        _freeListHead = slot;

        _index.RawItems[key] = IndexAbsent;
        _containerCount--;
    }

    /// <summary>
    /// Add a container entry during result-building (set operations). Used when building
    /// a new bitmap from scratch where keys are added in order.
    /// </summary>
    internal void AddContainer(ContainerEntry entry)
    {
        if (entry.Cardinality == 0)
            return;

        AddNewContainer(entry.Key, entry);
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
            Key = key,
            Type = ContainerType.Array,
            Cardinality = 0,
            Data = storage.Ptr,
            Storage = storage
        };
    }

    /// <summary>
    /// Ensure the array/negated container has room for one more entry.
    /// Doubles the buffer size up to BitmapContainerSizeInBytes.
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
            Key = key,
            Type = ContainerType.Bitmap,
            Cardinality = 0,
            Data = storage.Ptr,
            Storage = storage
        };
    }

    #endregion

    #region Container Operations

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void AddToContainer(ref ContainerEntry entry, ushort value)
    {
        switch (entry.Type)
        {
            case ContainerType.ArrayUnsorted:
                EnsureArrayCapacity(ref entry, entry.Cardinality + 1);
                ((ushort*)entry.Data)[entry.Cardinality] = value;
                entry.Cardinality++;
                if (entry.Cardinality > ArrayContainerMaxCardinality)
                {
                    // Sort+dedup to see if we're still over threshold
                    SortAndDeduplicateArray(ref entry);
                    if (entry.Cardinality > ArrayContainerMaxCardinality)
                        ConvertArrayToBitmap(ref entry);
                }
                break;

            case ContainerType.Array:
                EnsureArrayCapacity(ref entry, entry.Cardinality + 1);
                ArrayContainerAdd(entry.Data, ref entry.Cardinality, value);
                if (entry.Cardinality > ArrayContainerMaxCardinality)
                    ConvertArrayToBitmap(ref entry);
                break;

            case ContainerType.Bitmap:
                BitmapContainerAdd(entry.Data, ref entry.Cardinality, value);
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
                    ConvertRangeForAdd(ref entry, value);
                }
                break;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool ContainerContains(ref ContainerEntry entry, ushort value)
    {
        if (entry.Type == ContainerType.ArrayUnsorted)
            SortAndDeduplicateArray(ref entry);

        return entry.Type switch
        {
            ContainerType.Array => ArrayContainerContains(entry.Data, entry.Cardinality, value),
            ContainerType.Bitmap => BitmapContainerContains(entry.Data, value),
            ContainerType.Range => value < entry.Cardinality,
            _ => false
        };
    }

    #endregion

    #region Array Sorting

    /// <summary>
    /// Sort an ArrayUnsorted container in-place and deduplicate. Converts to Array type.
    /// </summary>
    private static void SortAndDeduplicateArray(ref ContainerEntry entry)
    {
        Debug.Assert(entry.Type == ContainerType.ArrayUnsorted);

        ushort* arr = (ushort*)entry.Data;
        int count = entry.Cardinality;

        // Sort
        new Span<ushort>(arr, count).Sort();

        // Deduplicate in-place
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
        entry.Type = ContainerType.Array;
    }

    /// <summary>
    /// Ensure any ArrayUnsorted container is sorted before use in operations that require sorted data.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void EnsureSorted(ref ContainerEntry entry)
    {
        if (entry.Type == ContainerType.ArrayUnsorted)
            SortAndDeduplicateArray(ref entry);
    }

    #endregion

    #region Array Container

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void ArrayContainerAdd(byte* data, ref int cardinality, ushort value)
    {
        ushort* arr = (ushort*)data;
        int count = cardinality;

        // Binary search for insertion point
        int idx = ArrayContainerFind(arr, count, value);
        if (idx >= 0)
            return; // Already exists

        int insertAt = ~idx;

        // Shift right
        for (int i = count; i > insertAt; i--)
            arr[i] = arr[i - 1];

        arr[insertAt] = value;
        cardinality = count + 1;
    }

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
    /// Compute the intersection of two array containers, writing the result to dst.
    /// Uses SIMD galloping when Vector256 is available: broadcasts the smaller side's
    /// current value and checks 16 elements of the larger side in one comparison.
    /// </summary>
    internal static int ArrayContainerAnd(ushort* a, int aLen, ushort* b, int bLen, ushort* dst)
    {
        if (AdvInstructionSet.IsAcceleratedVector256)
            return ArrayContainerAndVectorized(a, aLen, b, bLen, dst);
        return ArrayContainerAndScalar(a, aLen, b, bLen, dst);
    }

    private static int ArrayContainerAndVectorized(ushort* a, int aLen, ushort* b, int bLen, ushort* dst)
    {
        // Ensure smaller is the one we iterate element-by-element
        ushort* smaller, larger;
        int smallerLen, largerLen;
        if (aLen <= bLen)
        {
            smaller = a; smallerLen = aLen;
            larger = b; largerLen = bLen;
        }
        else
        {
            smaller = b; smallerLen = bLen;
            larger = a; largerLen = aLen;
        }

        uint N = (uint)Vector256<ushort>.Count; // 16
        int si = 0, li = 0, di = 0;

        while (si < smallerLen && li + (int)N <= largerLen)
        {
            ushort val = smaller[si];

            // Skip blocks in larger that are entirely below val
            if (val > larger[li + N - 1])
            {
                li += (int)N;
                continue;
            }

            // Skip smaller values below the current block
            if (val < larger[li])
            {
                si++;
                continue;
            }

            // Check if val exists in this block of 16
            Vector256<ushort> vVal = Vector256.Create(val);
            Vector256<ushort> vBlock = Vector256.Load(larger + li);
            if (Vector256.EqualsAny(vVal, vBlock))
            {
                dst[di++] = val;
            }

            si++;
        }

        // Scalar tail
        while (si < smallerLen && li < largerLen)
        {
            if (smaller[si] < larger[li])
                si++;
            else if (smaller[si] > larger[li])
                li++;
            else
            {
                dst[di++] = smaller[si];
                si++;
                li++;
            }
        }

        return di;
    }

    private static int ArrayContainerAndScalar(ushort* a, int aLen, ushort* b, int bLen, ushort* dst)
    {
        int ai = 0, bi = 0, di = 0;

        while (ai < aLen && bi < bLen)
        {
            if (a[ai] < b[bi])
                ai++;
            else if (a[ai] > b[bi])
                bi++;
            else
            {
                dst[di++] = a[ai];
                ai++;
                bi++;
            }
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

    /// <summary>
    /// Compute A AND NOT B for two array containers.
    /// Uses SIMD galloping to skip blocks in B that don't affect A.
    /// </summary>
    internal static int ArrayContainerAndNot(ushort* a, int aLen, ushort* b, int bLen, ushort* dst)
    {
        if (AdvInstructionSet.IsAcceleratedVector256)
            return ArrayContainerAndNotVectorized(a, aLen, b, bLen, dst);
        return ArrayContainerAndNotScalar(a, aLen, b, bLen, dst);
    }

    private static int ArrayContainerAndNotVectorized(ushort* a, int aLen, ushort* b, int bLen, ushort* dst)
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

            // If val is before the current block of B, it's not in B — keep it
            if (val < b[bi])
            {
                dst[di++] = val;
                ai++;
                continue;
            }

            // Check if val exists in this block of B
            Vector256<ushort> vVal = Vector256.Create(val);
            Vector256<ushort> vBlock = Vector256.Load(b + bi);
            if (!Vector256.EqualsAny(vVal, vBlock))
            {
                dst[di++] = val; // Not found in B — keep it
            }

            ai++;
        }

        // Scalar tail
        while (ai < aLen && bi < bLen)
        {
            if (a[ai] < b[bi])
                dst[di++] = a[ai++];
            else if (a[ai] > b[bi])
                bi++;
            else
            {
                ai++;
                bi++;
            }
        }

        while (ai < aLen)
            dst[di++] = a[ai++];

        return di;
    }

    private static int ArrayContainerAndNotScalar(ushort* a, int aLen, ushort* b, int bLen, ushort* dst)
    {
        int ai = 0, bi = 0, di = 0;

        while (ai < aLen && bi < bLen)
        {
            if (a[ai] < b[bi])
                dst[di++] = a[ai++];
            else if (a[ai] > b[bi])
                bi++;
            else
            {
                ai++;
                bi++;
            }
        }

        while (ai < aLen)
            dst[di++] = a[ai++];

        return di;
    }

    #endregion

    #region Range Container

    /// <summary>
    /// Convert a Range container to Bitmap or Array to handle an Add outside the contiguous range.
    /// </summary>
    private void ConvertRangeForAdd(ref ContainerEntry entry, ushort value)
    {
        Debug.Assert(entry.Type == ContainerType.Range);
        int rangeCount = entry.Cardinality;

        if (rangeCount + 1 > ArrayContainerMaxCardinality || value >= ArrayContainerMaxCardinality)
        {
            // Result will be dense — use bitmap
            ConvertRangeToBitmap(ref entry);
            BitmapContainerAdd(entry.Data, ref entry.Cardinality, value);
        }
        else
        {
            // Small enough for array
            _ctx.Allocate(Math.Max(InitialArrayContainerSizeInBytes, (rangeCount + 1) * sizeof(ushort)), out ByteString storage);
            ushort* arr = (ushort*)storage.Ptr;
            for (int i = 0; i < rangeCount; i++)
                arr[i] = (ushort)i;

            entry.Storage = storage;
            entry.Data = storage.Ptr;
            entry.Type = ContainerType.Array;
            // Cardinality stays the same (rangeCount)
            EnsureArrayCapacity(ref entry, entry.Cardinality + 1);
            ArrayContainerAdd(entry.Data, ref entry.Cardinality, value);
            if (entry.Cardinality > ArrayContainerMaxCardinality)
                ConvertArrayToBitmap(ref entry);
        }
    }

    /// <summary>
    /// Convert a Range container to a Bitmap container.
    /// Sets bits 0..cardinality-1.
    /// </summary>
    private void ConvertRangeToBitmap(ref ContainerEntry entry)
    {
        Debug.Assert(entry.Type == ContainerType.Range);
        int rangeCount = entry.Cardinality;

        _ctx.Allocate(BitmapContainerSizeInBytes, out ByteString storage);
        ulong* bitmap = (ulong*)storage.Ptr;
        new Span<byte>(bitmap, BitmapContainerSizeInBytes).Clear();

        // Set full words
        int fullWords = rangeCount / 64;
        for (int i = 0; i < fullWords; i++)
            bitmap[i] = ulong.MaxValue;

        // Set remaining bits in the last partial word
        int remainder = rangeCount & 63;
        if (remainder > 0)
            bitmap[fullWords] = (1UL << remainder) - 1;

        if (entry.Storage.HasValue)
            _ctx.Release(ref entry.Storage);

        entry.Storage = storage;
        entry.Data = storage.Ptr;
        entry.Type = ContainerType.Bitmap;
        // Cardinality stays the same
    }

    #endregion

    #region Bitmap Container

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void BitmapContainerAdd(byte* data, ref int cardinality, ushort value)
    {
        ulong* bitmap = (ulong*)data;
        int wordIdx = value >> 6;
        ulong mask = 1UL << (value & 63);

        if ((bitmap[wordIdx] & mask) == 0)
        {
            bitmap[wordIdx] |= mask;
            cardinality++;
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
        for (int i = 0; i < BitmapContainerSizeInUlongs; i++)
            count += BitOperations.PopCount(bitmap[i]);
        return count;
    }

    /// <summary>
    /// Convert an array container to a bitmap from a sorted ushort array.
    /// </summary>
    internal static void ArrayToBitmap(ushort* arr, int arrLen, ulong* bitmap)
    {
        new Span<byte>(bitmap, BitmapContainerSizeInBytes).Clear();
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

    private void ConvertArrayToBitmap(ref ContainerEntry entry)
    {
        Debug.Assert(entry.Type == ContainerType.Array);

        ushort* arr = (ushort*)entry.Data;
        int count = entry.Cardinality;

        _ctx.Allocate(BitmapContainerSizeInBytes, out ByteString newStorage);
        ulong* bitmap = (ulong*)newStorage.Ptr;
        new Span<byte>(bitmap, BitmapContainerSizeInBytes).Clear();

        for (int i = 0; i < count; i++)
        {
            ushort val = arr[i];
            bitmap[val >> 6] |= 1UL << (val & 63);
        }

        if (entry.Storage.HasValue)
            _ctx.Release(ref entry.Storage);

        entry.Storage = newStorage;
        entry.Data = newStorage.Ptr;
        entry.Type = ContainerType.Bitmap;
    }

    private void ConvertBitmapToArray(ref ContainerEntry entry)
    {
        Debug.Assert(entry.Type == ContainerType.Bitmap);
        Debug.Assert(entry.Cardinality <= ArrayContainerMaxCardinality);

        ulong* bitmap = (ulong*)entry.Data;

        _ctx.Allocate(BitmapContainerSizeInBytes, out ByteString newStorage);
        ushort* arr = (ushort*)newStorage.Ptr;

        int count = BitmapToArray(bitmap, arr);
        Debug.Assert(count == entry.Cardinality);

        if (entry.Storage.HasValue)
            _ctx.Release(ref entry.Storage);

        entry.Storage = newStorage;
        entry.Data = newStorage.Ptr;
        entry.Type = ContainerType.Array;
    }


    #endregion

    #region Allocation Helpers

    internal ContainerEntry AllocateArrayContainer(long key, int maxCardinality)
    {
        int bytes = Math.Max(InitialArrayContainerSizeInBytes, maxCardinality * sizeof(ushort));
        bytes = Math.Min(bytes, BitmapContainerSizeInBytes);
        _ctx.Allocate(bytes, out ByteString storage);
        storage.ToSpan<byte>().Clear();

        return new ContainerEntry
        {
            Key = key,
            Type = ContainerType.Array,
            Cardinality = 0,
            Data = storage.Ptr,
            Storage = storage
        };
    }

    internal ContainerEntry AllocateBitmapContainer(long key)
    {
        return CreateBitmapContainer(key);
    }

    internal ByteStringContext Context => _ctx;

    #endregion

    public void Dispose()
    {
        // Walk the index to find live entries and release their storage
        if (_index.IsValid)
        {
            int* idx = _index.RawItems;
            for (int key = 0; key < _index.Count; key++)
            {
                int slot = idx[key];
                if (slot >= 0)
                {
                    ref ContainerEntry entry = ref _entries[slot];
                    if (entry.Storage.HasValue)
                        _ctx.Release(ref entry.Storage);
                }
            }

            _index.Dispose(_ctx);
        }

        if (_entries.IsValid)
            _entries.Dispose(_ctx);
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
    ArrayUnsorted = 3
}

[StructLayout(LayoutKind.Sequential)]
public unsafe struct ContainerEntry
{
    public long Key;            // high 48 bits of value (value >> 16)
    /// <summary>
    /// Direct pointer to container data for hot-path access without indirection through ByteString.
    /// Null for Full containers (which have no data). Kept separate from Storage because Full containers
    /// have Storage=default (no allocation to release), so we can't rely on Storage.Ptr universally.
    /// </summary>
    public byte* Data;
    public int Cardinality;     // number of set bits in this container
    public ContainerType Type;
    internal ByteString Storage; // memory handle for disposal
}
