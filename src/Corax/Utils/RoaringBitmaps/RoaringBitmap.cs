using System;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
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
/// - Bitmap: 8KB fixed bitmap (1024 ulongs) for dense data (4097..61440)
/// - Negated: sorted ushort[] of ABSENT values for nearly-full data (cardinality &gt; 61440)
/// - Full: all 65536 bits set (no data allocation needed)
/// </summary>
public unsafe struct RoaringBitmap : IDisposable
{
    public const int BitmapContainerSizeInBytes = 8192; // 8KB
    public const int BitmapContainerSizeInUlongs = BitmapContainerSizeInBytes / sizeof(ulong);
    public const int BitsPerContainer = BitmapContainerSizeInBytes * 8; // each byte holds 8 bits
    public const int ArrayContainerMaxCardinality = BitmapContainerSizeInBytes / sizeof(ushort); // crossover: array at max costs same as bitmap
    public const int NegatedArrayMinCardinality = BitsPerContainer - ArrayContainerMaxCardinality; // 61440
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
            // Walk the index to find only live entries
            int* idx = _index.RawItems;
            for (int k = 0; k < _index.Count; k++)
            {
                int slot = idx[k];
                if (slot >= 0)
                    total += _entries[slot].Cardinality;
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
            ContainerEntry newEntry = CreateArrayContainer(key);
            ArrayContainerAdd(newEntry.Data, ref newEntry.Cardinality, low);
            AddNewContainer(key, newEntry);
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
                // Entire container is full
                int slot = GetSlotForKey(key);
                if (slot >= 0)
                {
                    ref ContainerEntry entry = ref _entries[slot];
                    if (entry.Storage.HasValue)
                        _ctx.Release(ref entry.Storage);
                    entry.Data = null;
                    entry.Storage = default;
                    entry.Type = ContainerType.Full;
                    entry.Cardinality = BitsPerContainer;
                }
                else
                {
                    AddNewContainer(key, new ContainerEntry
                    {
                        Key = key,
                        Type = ContainerType.Full,
                        Cardinality = BitsPerContainer,
                        Data = null,
                        Storage = default
                    });
                }
            }
            else
            {
                // Partial range — ensure a bitmap container and set bits directly
                int slot = GetSlotForKey(key);
                if (slot < 0)
                {
                    ContainerEntry newEntry = CreateBitmapContainer(key);
                    slot = AddNewContainer(key, newEntry);
                }

                ref ContainerEntry e = ref _entries[slot];

                // Convert to bitmap if not already (needed for direct bit setting)
                if (e.Type == ContainerType.Array)
                    ConvertArrayToBitmap(ref e);
                else if (e.Type == ContainerType.Run)
                    ConvertRunToBitmap(ref e);
                else if (e.Type == ContainerType.Negated)
                    ConvertNegatedToBitmap(ref e);
                else if (e.Type == ContainerType.Full)
                    continue; // already all set

                ulong* bitmap = (ulong*)e.Data;
                for (int v = lo; v <= hi; v++)
                    bitmap[v >> 6] |= 1UL << (v & 63);

                e.Cardinality = BitmapContainerCardinality(e.Data);
                if (e.Cardinality == BitsPerContainer)
                    ConvertToFull(ref e);
                else if (e.Cardinality > NegatedArrayMinCardinality)
                    ConvertBitmapToNegated(ref e);
            }
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Remove(long value)
    {
        long key = value >> ContainerKeyShift;
        ushort low = (ushort)(value & ContainerValueMask);

        int slot = GetSlotForKey(key);
        if (slot < 0)
            return;

        ref ContainerEntry entry = ref _entries[slot];
        RemoveFromContainer(ref entry, low);

        if (entry.Cardinality == 0)
            FreeContainer(key, slot);
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

    public void OptimizeToRun()
    {
        int* idx = _index.RawItems;
        for (int key = 0; key < _index.Count; key++)
        {
            int slot = idx[key];
            if (slot < 0) continue;

            ref ContainerEntry entry = ref _entries[slot];
            if (entry.Type == ContainerType.Bitmap)
                TryConvertBitmapToRun(ref entry);
            else if (entry.Type == ContainerType.Array)
                TryConvertArrayToRun(ref entry);
        }
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
    /// In-place XOR: toggle all values from other in this bitmap.
    /// </summary>
    public void XorWith(ref RoaringBitmap other)
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
                XorContainerInPlace(ref _entries[mySlot], ref otherEntry);
                if (_entries[mySlot].Cardinality == 0)
                    FreeContainer(key, mySlot);
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
        // Full AND X = X: convert left to a copy of right
        if (left.Type == ContainerType.Full)
        {
            ContainerEntry cloned = RoaringBitmapSetOps.CloneContainer(_ctx, ref right);
            if (left.Storage.HasValue)
                _ctx.Release(ref left.Storage);
            left = cloned;
            return;
        }
        // X AND Full = X: no change
        if (right.Type == ContainerType.Full)
            return;

        // Materialize Run/Negated to bitmap first
        if (left.Type is ContainerType.Run or ContainerType.Negated)
            MaterializeContainerToBitmap(ref left);
        if (right.Type is ContainerType.Run or ContainerType.Negated)
        {
            // We can't modify right (it belongs to the other bitmap), so materialize to temp
            ContainerEntry temp = MaterializeToBitmapTemp(ref right);
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
        if (left.Type == ContainerType.Full)
            return; // Full | X = Full
        if (right.Type == ContainerType.Full)
        {
            if (left.Storage.HasValue)
                _ctx.Release(ref left.Storage);
            left.Data = null;
            left.Storage = default;
            left.Type = ContainerType.Full;
            left.Cardinality = BitsPerContainer;
            return;
        }

        if (left.Type is ContainerType.Run or ContainerType.Negated)
            MaterializeContainerToBitmap(ref left);
        if (right.Type is ContainerType.Run or ContainerType.Negated)
        {
            ContainerEntry temp = MaterializeToBitmapTemp(ref right);
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

    private void XorContainerInPlace(ref ContainerEntry left, ref ContainerEntry right)
    {
        if (left.Type == ContainerType.Full && right.Type == ContainerType.Full)
        {
            // Full ^ Full = empty
            if (left.Storage.HasValue)
                _ctx.Release(ref left.Storage);
            left = default;
            return;
        }

        if (left.Type is ContainerType.Run or ContainerType.Negated)
            MaterializeContainerToBitmap(ref left);
        if (right.Type is ContainerType.Run or ContainerType.Negated or ContainerType.Full)
        {
            ContainerEntry temp = MaterializeToBitmapTemp(ref right);
            XorContainerInPlace(ref left, ref temp);
            _ctx.Release(ref temp.Storage);
            return;
        }
        if (left.Type == ContainerType.Full)
        {
            // Full ^ bitmap/array: convert Full to bitmap first
            ConvertFullToBitmap(ref left);
        }

        switch (left.Type, right.Type)
        {
            case (ContainerType.Bitmap, ContainerType.Bitmap):
                left.Cardinality = RoaringBitmapSetOps.BitmapXorSimd(
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
                        left.Cardinality--;
                    else
                        left.Cardinality++;
                    bmp[val >> 6] ^= mask;
                }
                OptimizeContainerType(ref left);
                break;
            }

            case (ContainerType.Array, ContainerType.Array):
            {
                int maxResult = left.Cardinality + right.Cardinality;
                if (maxResult > ArrayContainerMaxCardinality)
                {
                    ConvertArrayToBitmap(ref left);
                    XorContainerInPlace(ref left, ref right);
                }
                else
                {
                    _ctx.Allocate(Math.Max(InitialArrayContainerSizeInBytes, maxResult * sizeof(ushort)), out ByteString newStorage);
                    int count = ArrayContainerXor((ushort*)left.Data, left.Cardinality, (ushort*)right.Data, right.Cardinality, (ushort*)newStorage.Ptr);
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
                XorContainerInPlace(ref left, ref right);
                break;
            }
        }
    }

    private void AndNotContainerInPlace(ref ContainerEntry left, ref ContainerEntry right)
    {
        if (right.Type == ContainerType.Full)
        {
            // X & ~Full = empty
            if (left.Storage.HasValue)
                _ctx.Release(ref left.Storage);
            left = default;
            return;
        }
        if (left.Type == ContainerType.Full)
            ConvertFullToBitmap(ref left);

        if (left.Type is ContainerType.Run or ContainerType.Negated)
            MaterializeContainerToBitmap(ref left);
        if (right.Type is ContainerType.Run or ContainerType.Negated)
        {
            ContainerEntry temp = MaterializeToBitmapTemp(ref right);
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
    /// Convert a container to bitmap in-place (for Run/Negated containers).
    /// </summary>
    private void MaterializeContainerToBitmap(ref ContainerEntry entry)
    {
        if (entry.Type == ContainerType.Run)
            ConvertRunToBitmap(ref entry);
        else if (entry.Type == ContainerType.Negated)
            ConvertNegatedToBitmap(ref entry);
    }

    /// <summary>
    /// Create a temporary bitmap from a Run/Negated/Full container (for read-only right-hand side).
    /// Caller must release the returned storage.
    /// </summary>
    private ContainerEntry MaterializeToBitmapTemp(ref ContainerEntry entry)
    {
        _ctx.Allocate(BitmapContainerSizeInBytes, out ByteString storage);
        ulong* bitmap = (ulong*)storage.Ptr;

        switch (entry.Type)
        {
            case ContainerType.Run:
                RunToBitmap(entry.Data, bitmap);
                break;
            case ContainerType.Negated:
            {
                new Span<byte>(bitmap, BitmapContainerSizeInBytes).Fill(0xFF);
                int absentCount = BitsPerContainer - entry.Cardinality;
                ushort* absent = (ushort*)entry.Data;
                for (int i = 0; i < absentCount; i++)
                {
                    ushort val = absent[i];
                    bitmap[val >> 6] &= ~(1UL << (val & 63));
                }
                break;
            }
            case ContainerType.Full:
                new Span<byte>(bitmap, BitmapContainerSizeInBytes).Fill(0xFF);
                break;
            default:
                // Bitmap or Array — shouldn't reach here, but handle gracefully
                Unsafe.CopyBlockUnaligned(storage.Ptr, entry.Data, BitmapContainerSizeInBytes);
                break;
        }

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
    /// After in-place modification of a bitmap container, convert to the optimal type.
    /// </summary>
    private void OptimizeContainerType(ref ContainerEntry entry)
    {
        if (entry.Type != ContainerType.Bitmap)
            return;

        if (entry.Cardinality == BitsPerContainer)
            ConvertToFull(ref entry);
        else if (entry.Cardinality > NegatedArrayMinCardinality)
            ConvertBitmapToNegated(ref entry);
        else if (entry.Cardinality <= ArrayContainerMaxCardinality)
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

    private static ContainerEntry CreateFullContainer(long key)
    {
        return new ContainerEntry
        {
            Key = key,
            Type = ContainerType.Full,
            Cardinality = BitsPerContainer,
            Data = null,
            Storage = default
        };
    }


    #endregion

    #region Container Operations

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void AddToContainer(ref ContainerEntry entry, ushort value)
    {
        switch (entry.Type)
        {
            case ContainerType.Array:
                EnsureArrayCapacity(ref entry, entry.Cardinality + 1);
                ArrayContainerAdd(entry.Data, ref entry.Cardinality, value);
                if (entry.Cardinality > ArrayContainerMaxCardinality)
                    ConvertArrayToBitmap(ref entry);
                break;

            case ContainerType.Bitmap:
                BitmapContainerAdd(entry.Data, ref entry.Cardinality, value);
                if (entry.Cardinality > NegatedArrayMinCardinality)
                    ConvertBitmapToNegated(ref entry);
                break;

            case ContainerType.Negated:
                NegatedContainerAdd(entry.Data, ref entry.Cardinality, value);
                if (entry.Cardinality == BitsPerContainer)
                    ConvertToFull(ref entry);
                break;

            case ContainerType.Run:
                RunContainerAdd(ref entry, value);
                break;

            case ContainerType.Full:
                // Already contains everything
                break;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void RemoveFromContainer(ref ContainerEntry entry, ushort value)
    {
        switch (entry.Type)
        {
            case ContainerType.Array:
                ArrayContainerRemove(entry.Data, ref entry.Cardinality, value);
                break;

            case ContainerType.Bitmap:
                BitmapContainerRemove(entry.Data, ref entry.Cardinality, value);
                if (entry.Cardinality <= ArrayContainerMaxCardinality)
                    ConvertBitmapToArray(ref entry);
                break;

            case ContainerType.Negated:
                EnsureArrayCapacity(ref entry, NegatedContainerAbsentCount(entry.Cardinality) + 1);
                NegatedContainerRemove(entry.Data, ref entry.Cardinality, value);
                if (entry.Cardinality <= NegatedArrayMinCardinality)
                    ConvertNegatedToBitmap(ref entry);
                break;

            case ContainerType.Run:
                RunContainerRemove(ref entry, value);
                break;

            case ContainerType.Full:
                ConvertFullToNegated(ref entry);
                EnsureArrayCapacity(ref entry, NegatedContainerAbsentCount(entry.Cardinality) + 1);
                NegatedContainerRemove(entry.Data, ref entry.Cardinality, value);
                // Full had 65536 set bits, removing one gives 65535 — always above NegatedArrayMinCardinality (61440)
                Debug.Assert(entry.Cardinality > NegatedArrayMinCardinality);
                break;
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool ContainerContains(ref ContainerEntry entry, ushort value)
    {
        return entry.Type switch
        {
            ContainerType.Array => ArrayContainerContains(entry.Data, entry.Cardinality, value),
            ContainerType.Bitmap => BitmapContainerContains(entry.Data, value),
            ContainerType.Negated => NegatedContainerContains(entry.Data, entry.Cardinality, value),
            ContainerType.Run => RunContainerContains(entry.Data, entry.Cardinality, value),
            ContainerType.Full => true,
            _ => false
        };
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
    internal static void ArrayContainerRemove(byte* data, ref int cardinality, ushort value)
    {
        ushort* arr = (ushort*)data;
        int count = cardinality;

        int idx = ArrayContainerFind(arr, count, value);
        if (idx < 0)
            return;

        // Shift left
        for (int i = idx; i < count - 1; i++)
            arr[i] = arr[i + 1];

        cardinality = count - 1;
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
    /// Returns the number of elements in the result.
    /// </summary>
    internal static int ArrayContainerAnd(ushort* a, int aLen, ushort* b, int bLen, ushort* dst)
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
    /// Compute XOR of two array containers. dst must have space for aLen + bLen entries.
    /// Returns the number of elements in the result.
    /// </summary>
    internal static int ArrayContainerXor(ushort* a, int aLen, ushort* b, int bLen, ushort* dst)
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
    /// Returns the number of elements in the result.
    /// </summary>
    internal static int ArrayContainerAndNot(ushort* a, int aLen, ushort* b, int bLen, ushort* dst)
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

    #region Negated Container

    // Negated container: stores sorted ushort[] of ABSENT values.
    // The array length is (BitsPerContainer - Cardinality), i.e. the number of zeros.
    // Cardinality tracks the number of SET bits (not the array length).

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static bool NegatedContainerContains(byte* data, int cardinality, ushort value)
    {
        // Value is present if it is NOT in the absent list
        int absentCount = BitsPerContainer - cardinality;
        return ArrayContainerFind((ushort*)data, absentCount, value) < 0;
    }

    /// <summary>
    /// Add a value to a negated container (remove it from the absent list).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void NegatedContainerAdd(byte* data, ref int cardinality, ushort value)
    {
        int absentCount = BitsPerContainer - cardinality;
        ushort* arr = (ushort*)data;

        int idx = ArrayContainerFind(arr, absentCount, value);
        if (idx < 0)
            return; // already present (not in absent list)

        // Remove from absent list (shift left)
        for (int i = idx; i < absentCount - 1; i++)
            arr[i] = arr[i + 1];

        cardinality++;
    }

    /// <summary>
    /// Remove a value from a negated container (add it to the absent list).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void NegatedContainerRemove(byte* data, ref int cardinality, ushort value)
    {
        int absentCount = BitsPerContainer - cardinality;
        ushort* arr = (ushort*)data;

        int idx = ArrayContainerFind(arr, absentCount, value);
        if (idx >= 0)
            return; // already absent

        int insertAt = ~idx;

        // Insert into absent list (shift right)
        for (int i = absentCount; i > insertAt; i--)
            arr[i] = arr[i - 1];

        arr[insertAt] = value;
        cardinality--;
    }

    /// <summary>
    /// Get the number of absent values in a negated container.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static int NegatedContainerAbsentCount(int cardinality)
    {
        return BitsPerContainer - cardinality;
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
    internal static void BitmapContainerRemove(byte* data, ref int cardinality, ushort value)
    {
        ulong* bitmap = (ulong*)data;
        int wordIdx = value >> 6;
        ulong mask = 1UL << (value & 63);

        if ((bitmap[wordIdx] & mask) != 0)
        {
            bitmap[wordIdx] &= ~mask;
            cardinality--;
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

    #region Run Container

    // Run container format:
    // First 2 bytes: ushort numberOfRuns
    // Then pairs of (ushort start, ushort length) where length means (length+1) values
    // e.g., (5, 3) means values 5, 6, 7, 8

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static bool RunContainerContains(byte* data, int cardinality, ushort value)
    {
        ushort* runs = (ushort*)data;
        int numRuns = runs[0];

        for (int i = 0; i < numRuns; i++)
        {
            ushort start = runs[1 + i * 2];
            ushort length = runs[1 + i * 2 + 1];

            if (value < start)
                return false; // runs are sorted
            if (value <= start + length)
                return true;
        }

        return false;
    }

    private void RunContainerAdd(ref ContainerEntry entry, ushort value)
    {
        // Convert to bitmap, add, then back to optimal format
        // This is simpler than modifying run-length encoding in place
        ConvertRunToBitmap(ref entry);
        BitmapContainerAdd(entry.Data, ref entry.Cardinality, value);

        if (entry.Cardinality == BitsPerContainer)
            ConvertToFull(ref entry);
    }

    private void RunContainerRemove(ref ContainerEntry entry, ushort value)
    {
        ConvertRunToBitmap(ref entry);
        BitmapContainerRemove(entry.Data, ref entry.Cardinality, value);

        if (entry.Cardinality <= ArrayContainerMaxCardinality)
            ConvertBitmapToArray(ref entry);
    }

    /// <summary>
    /// Convert a run container to a bitmap container.
    /// </summary>
    internal static void RunToBitmap(byte* runData, ulong* bitmap)
    {
        ushort* runs = (ushort*)runData;
        int numRuns = runs[0];

        new Span<byte>(bitmap, BitmapContainerSizeInBytes).Clear();

        for (int i = 0; i < numRuns; i++)
        {
            ushort start = runs[1 + i * 2];
            ushort length = runs[1 + i * 2 + 1];

            for (int j = 0; j <= length; j++)
            {
                int val = start + j;
                bitmap[val >> 6] |= 1UL << (val & 63);
            }
        }
    }

    /// <summary>
    /// Try to convert a bitmap container to a run container if it saves space.
    /// </summary>
    private void TryConvertBitmapToRun(ref ContainerEntry entry)
    {
        Debug.Assert(entry.Type == ContainerType.Bitmap);

        int numRuns = CountRunsBitmap(entry.Data);
        int runBytes = 2 + numRuns * 4; // header + pairs

        if (runBytes >= BitmapContainerSizeInBytes)
            return; // not worth it

        // Also not worth it if the array representation would be smaller
        int arrayBytes = entry.Cardinality * 2;
        if (arrayBytes < runBytes && entry.Cardinality <= ArrayContainerMaxCardinality)
        {
            ConvertBitmapToArray(ref entry);
            return;
        }

        BitmapToRun(ref entry, numRuns);
    }

    private void TryConvertArrayToRun(ref ContainerEntry entry)
    {
        Debug.Assert(entry.Type == ContainerType.Array);

        ushort* arr = (ushort*)entry.Data;
        int count = entry.Cardinality;

        int numRuns = CountRunsArray(arr, count);
        int runBytes = 2 + numRuns * 4;
        int arrayBytes = count * 2;

        if (runBytes >= arrayBytes)
            return;

        // Convert array to run
        ArrayToRun(ref entry, numRuns);
    }

    private static int CountRunsBitmap(byte* data)
    {
        ulong* bitmap = (ulong*)data;
        int runs = 0;

        for (int i = 0; i < BitmapContainerSizeInUlongs; i++)
        {
            ulong word = bitmap[i];
            if (word == 0)
                continue;

            // Count transitions from 0->1
            ulong prevBit = (i > 0) ? (bitmap[i - 1] >> 63) : 0;
            ulong shifted = (word << 1) | prevBit;
            runs += BitOperations.PopCount(word & ~shifted);
        }

        return runs;
    }

    private static int CountRunsArray(ushort* arr, int count)
    {
        if (count == 0)
            return 0;

        int runs = 1;
        for (int i = 1; i < count; i++)
        {
            if (arr[i] != arr[i - 1] + 1)
                runs++;
        }

        return runs;
    }

    private void BitmapToRun(ref ContainerEntry entry, int numRuns)
    {
        ulong* bitmap = (ulong*)entry.Data;

        // We need a temporary buffer for the run data since we're reusing the same storage
        int runDataSize = 2 + numRuns * 4;

        _ctx.Allocate(Math.Max(runDataSize, 64), out ByteString newStorage);
        ushort* runs = (ushort*)newStorage.Ptr;
        runs[0] = (ushort)numRuns;

        int runIdx = 0;
        bool inRun = false;
        ushort runStart = 0;

        for (int wordIdx = 0; wordIdx < BitmapContainerSizeInUlongs; wordIdx++)
        {
            ulong word = bitmap[wordIdx];
            int baseVal = wordIdx * 64;

            for (int bit = 0; bit < 64; bit++)
            {
                bool isSet = (word & (1UL << bit)) != 0;
                ushort val = (ushort)(baseVal + bit);

                if (isSet && !inRun)
                {
                    runStart = val;
                    inRun = true;
                }
                else if (!isSet && inRun)
                {
                    runs[1 + runIdx * 2] = runStart;
                    runs[1 + runIdx * 2 + 1] = (ushort)(val - runStart - 1);
                    runIdx++;
                    inRun = false;
                }
            }
        }

        if (inRun)
        {
            runs[1 + runIdx * 2] = runStart;
            runs[1 + runIdx * 2 + 1] = (ushort)(BitsPerContainer - 1 - runStart);
            runIdx++;
        }

        if (entry.Storage.HasValue)
            _ctx.Release(ref entry.Storage);

        entry.Storage = newStorage;
        entry.Data = newStorage.Ptr;
        entry.Type = ContainerType.Run;
    }

    private void ArrayToRun(ref ContainerEntry entry, int numRuns)
    {
        ushort* arr = (ushort*)entry.Data;
        int count = entry.Cardinality;

        int runDataSize = 2 + numRuns * 4;
        _ctx.Allocate(Math.Max(runDataSize, 64), out ByteString newStorage);
        ushort* runs = (ushort*)newStorage.Ptr;
        runs[0] = (ushort)numRuns;

        int runIdx = 0;
        ushort runStart = arr[0];
        ushort runEnd = arr[0];

        for (int i = 1; i < count; i++)
        {
            if (arr[i] == runEnd + 1)
            {
                runEnd = arr[i];
            }
            else
            {
                runs[1 + runIdx * 2] = runStart;
                runs[1 + runIdx * 2 + 1] = (ushort)(runEnd - runStart);
                runIdx++;
                runStart = arr[i];
                runEnd = arr[i];
            }
        }

        runs[1 + runIdx * 2] = runStart;
        runs[1 + runIdx * 2 + 1] = (ushort)(runEnd - runStart);

        if (entry.Storage.HasValue)
            _ctx.Release(ref entry.Storage);

        entry.Storage = newStorage;
        entry.Data = newStorage.Ptr;
        entry.Type = ContainerType.Run;
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

    private void ConvertBitmapToNegated(ref ContainerEntry entry)
    {
        Debug.Assert(entry.Type == ContainerType.Bitmap);
        Debug.Assert(entry.Cardinality > NegatedArrayMinCardinality);

        ulong* bitmap = (ulong*)entry.Data;
        int absentCount = BitsPerContainer - entry.Cardinality;

        _ctx.Allocate(BitmapContainerSizeInBytes, out ByteString newStorage);
        ushort* arr = (ushort*)newStorage.Ptr;

        // Extract the ABSENT (zero) bits from the bitmap
        int count = 0;
        for (int wordIdx = 0; wordIdx < BitmapContainerSizeInUlongs; wordIdx++)
        {
            ulong word = ~bitmap[wordIdx]; // invert: zeros become ones
            while (word != 0)
            {
                int bit = BitOperations.TrailingZeroCount(word);
                arr[count++] = (ushort)(wordIdx * 64 + bit);
                word &= word - 1;
            }
        }

        Debug.Assert(count == absentCount);

        if (entry.Storage.HasValue)
            _ctx.Release(ref entry.Storage);

        entry.Storage = newStorage;
        entry.Data = newStorage.Ptr;
        entry.Type = ContainerType.Negated;
        // Cardinality stays the same (tracks SET bits)
    }

    private void ConvertNegatedToBitmap(ref ContainerEntry entry)
    {
        Debug.Assert(entry.Type == ContainerType.Negated);

        int absentCount = BitsPerContainer - entry.Cardinality;
        ushort* absentArr = (ushort*)entry.Data;

        _ctx.Allocate(BitmapContainerSizeInBytes, out ByteString newStorage);
        ulong* bitmap = (ulong*)newStorage.Ptr;

        // Start with all bits set, then clear the absent ones
        new Span<byte>(bitmap, BitmapContainerSizeInBytes).Fill(0xFF);

        for (int i = 0; i < absentCount; i++)
        {
            ushort val = absentArr[i];
            bitmap[val >> 6] &= ~(1UL << (val & 63));
        }

        if (entry.Storage.HasValue)
            _ctx.Release(ref entry.Storage);

        entry.Storage = newStorage;
        entry.Data = newStorage.Ptr;
        entry.Type = ContainerType.Bitmap;
        // Cardinality stays the same
    }

    private void ConvertToFull(ref ContainerEntry entry)
    {
        if (entry.Storage.HasValue)
            _ctx.Release(ref entry.Storage);

        entry.Data = null;
        entry.Storage = default;
        entry.Type = ContainerType.Full;
        entry.Cardinality = BitsPerContainer;
    }

    private void ConvertFullToNegated(ref ContainerEntry entry)
    {
        Debug.Assert(entry.Type == ContainerType.Full);

        // Full container with one removal → negated with empty absent list
        _ctx.Allocate(InitialArrayContainerSizeInBytes, out ByteString newStorage);
        newStorage.ToSpan<byte>().Clear();

        entry.Storage = newStorage;
        entry.Data = newStorage.Ptr;
        entry.Type = ContainerType.Negated;
        entry.Cardinality = BitsPerContainer;
    }

    private void ConvertFullToBitmap(ref ContainerEntry entry)
    {
        Debug.Assert(entry.Type == ContainerType.Full);

        _ctx.Allocate(BitmapContainerSizeInBytes, out ByteString newStorage);
        ulong* bitmap = (ulong*)newStorage.Ptr;

        // Set all bits
        new Span<byte>(bitmap, BitmapContainerSizeInBytes).Fill(0xFF);

        entry.Storage = newStorage;
        entry.Data = newStorage.Ptr;
        entry.Type = ContainerType.Bitmap;
        entry.Cardinality = BitsPerContainer;
    }

    private void ConvertRunToBitmap(ref ContainerEntry entry)
    {
        Debug.Assert(entry.Type == ContainerType.Run);

        _ctx.Allocate(BitmapContainerSizeInBytes, out ByteString newStorage);
        ulong* bitmap = (ulong*)newStorage.Ptr;

        RunToBitmap(entry.Data, bitmap);

        if (entry.Storage.HasValue)
            _ctx.Release(ref entry.Storage);

        entry.Storage = newStorage;
        entry.Data = newStorage.Ptr;
        entry.Type = ContainerType.Bitmap;
        // Cardinality stays the same
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
    Array = 0,
    Bitmap = 1,
    Run = 2,
    Full = 3,
    /// <summary>
    /// Negated array: stores the sorted list of values that are NOT set.
    /// Used when cardinality > NegatedArrayMinCardinality (61440), meaning fewer than 4096 zeros.
    /// Data format is identical to Array (sorted ushort[]), but the Cardinality field
    /// tracks the number of SET bits (65536 - absentCount), and the array length is (65536 - Cardinality).
    /// </summary>
    Negated = 4
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
