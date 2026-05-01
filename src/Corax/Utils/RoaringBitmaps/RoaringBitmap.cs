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
/// All memory is allocated through ByteStringContext, ensuring zero-managed heap allocations
/// for the bitmap data. Non-negative values are split into container key (value &gt;&gt; 16)
/// and low 16 bits. Container lookup is O(1) via a flat index array sized to the max key.
///
/// Container types:
/// - Range: contiguous values start..start+count-1 (no data allocation). count=65536 means full.
///   Sequential Add at either range edge is O(1). Created automatically for contiguous inserts.
/// - ArrayUnsorted: append-only ushort[]. Add is O(1). Sorted lazily on first read.
/// - Array: sorted ushort[] for sparse data (cardinality &lt;= 4096, up to 8KB)
/// - Bitmap: 8KB fixed bitmap (1024 longs) for dense data (&gt; 4096 values)
/// </summary>
public unsafe struct RoaringBitmap(ByteStringContext ctx) : IDisposable // CPF: should this be a ref struct?
{
    public const int BitmapContainerSizeInBytes = 8192; // 8KB
    public const int BitmapContainerSizeInUInt64 = BitmapContainerSizeInBytes / sizeof(ulong);
    private const int ArrayContainerMaxCardinality = BitmapContainerSizeInBytes / sizeof(ushort); // crossover: array at max costs same as bitmap
    public const int ContainerKeyShift = 16;
    public const int ContainerSize = 1 << ContainerKeyShift; // 65,536 entry IDs per container
    private const int ContainerValueMask = 0xFFFF;
    private const int LazyCardinality = -1;
    

    private const int FreeSlotTerminator = -2; // end of free list
    private const int IndexAbsent = -1;        // key is not present in index

    /// <summary>Packed container entries. May have tombstones (gaps reused via free list).</summary>
    private NativeList<ContainerEntry> _entries = new();
    /// <summary>Container types, parallel to _entries. Split to reduce padding in ContainerEntry.</summary>
    internal NativeList<ContainerType> _types = new();
    /// <summary>Key → entry slot index. -1 = absent. Length = maxKey + 1.</summary>
    private NativeList<int> _index = new();
    private int _containerCount = 0;
    /// <summary>Head of the free list in _entries. Dead entry's Cardinality field stores the next free index; -2 = end.</summary>
    private int _freeListHead = FreeSlotTerminator;

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
                
                AssertPrepared(types[i]);
                total += entries[i].Cardinality;
            }
            return total;
        }
    }

    public readonly bool IsEmpty => _containerCount == 0;

    /// <summary>
    /// Reset all containers without deallocating the backing storage for _entries, _types, _indexes, etc.
    /// *Does* deallocate the actual bitmaps (but the context is expected to reuse those allocations for future containers, amortizing the cost).
    /// </summary>
    public void Clear()
    {
        // Release container data storage but keep the NativeList allocations
        ContainerEntry* entries = _entries.RawItems;
        ContainerType* types = _types.RawItems;
        int count = _entries.Count;
        for (int i = 0; i < count; i++)
        {
            if (types[i] != ContainerType.Free)
            {
                ref ContainerEntry entry = ref entries[i];
                if (entry.Storage.HasValue)
                    ctx.Release(ref entry.Storage);
                entry.Data = null;
            }
        }
        _entries.Clear();
        _types.Clear();
        _index.Clear();
        _containerCount = 0;
        _freeListHead = FreeSlotTerminator;
    }

    /// <summary>
    /// Swap the internal state of two-bitmaps. O(1) — just swaps pointers and counts.
    /// Used after entry scan to swap the filtered results into the main bitmap.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SwapContents(ref RoaringBitmap other)
    {
        (this, other) = (other, this);
    }

    /// <summary>Batch-add strictly sorted (no duplicates) values. Groups values by container
    /// key and adds them in bulk per container. For large runs within a single container (>4096),
    /// switches directly to bitmap. For contiguous runs, extends/creates Range containers.
    ///
    /// Input contract: values must be strictly increased The <c>Array</c>-type append fast-path
    /// and the bitmap creation path both rely on input length equaling the unique-value count.
    ///
    /// Bitmap-container updates are *lazy*: cardinality is left as LazyCardinality and
    /// must be repaired before reading <see cref="Count"/>. <see cref="PrepareForReading"/>
    /// calls <see cref="RepairAfterLazy"/> as part of its normal pre-read fixup.</summary>
    public void AddRange(ReadOnlySpan<long> sortedValues)
    {
        if (sortedValues.IsEmpty)
            return;

        AssertSorted(sortedValues);

        int index = 0;
        while (index < sortedValues.Length)
        {
            long value = sortedValues[index];
            Debug.Assert(value >= 0, "RoaringBitmap only supports non-negative values.");
            long key = value >> ContainerKeyShift;

            int start = index;
            // here we search for the index on the value in sortedValues that is the last that match
            // the current container, so we can bulk insert it
            index = SearchForContainerRangeEnd(sortedValues, (key + 1) << ContainerKeyShift, index + 1);
            ReadOnlySpan<long> containerValues = sortedValues.Slice(start, index - start);

            int slot = GetSlotForKey(key);
            if (slot >= 0)
            {
                // Existing container — batch add via AddRangeToContainer
                AddRangeToContainer(slot, containerValues);
                continue;
            }

            // New container — keep contiguous runs as a range (any start offset)
            ushort firstLow = (ushort)(containerValues[0] & ContainerValueMask);
            ushort lastLow = (ushort)(containerValues[^1] & ContainerValueMask);
            bool isRange = lastLow - firstLow == containerValues.Length - 1;

            if (isRange)
            {
                AddNewContainer(key, ContainerType.Range, new ContainerEntry
                {
                    Cardinality = containerValues.Length,
                    RangeStart = firstLow,
                    Storage = default
                });
                continue;
            }

            if (containerValues.Length > ArrayContainerMaxCardinality)
            {
                CreateBitmapContainerFromSorted(key, containerValues);
            }
            else
            {
                CreateArrayContainerFromSorted(key, containerValues);
            }
        }
    }

    private static int SearchForContainerRangeEnd(ReadOnlySpan<long> sortedValues, long nextKeyStart, int start)
    {
        int jump = 1;
        // Start with exponential jumps, then binary search within the last interval.
        while (start + jump < sortedValues.Length && sortedValues[start + jump] < nextKeyStart)
            jump <<= 1;
        int lo = start + (jump >> 1);
        int hi = Math.Min(start + jump, sortedValues.Length);
        while (lo < hi)
        {
            int mid = lo + ((hi - lo) >> 1);
            if (sortedValues[mid] < nextKeyStart)
                lo = mid + 1;
            else
                hi = mid;
        }
        return lo;
    }

    /// <summary>
    /// Create a new bitmap container from sorted values.
    /// </summary>
    private void CreateBitmapContainerFromSorted(long key, ReadOnlySpan<long> sortedValues)
    {
        ctx.Allocate(BitmapContainerSizeInBytes, out ByteString storage);
        new Span<byte>(storage.Ptr, BitmapContainerSizeInBytes).Clear();

        OrSortedIntoBitmap((ulong*)storage.Ptr, sortedValues);

        AddNewContainer(key, ContainerType.Bitmap, new ContainerEntry
        {
            Cardinality = sortedValues.Length,
            Data = storage.Ptr,
            Storage = storage
        });
    }

    /// <summary>
    /// OR sorted values into a bitmap, accumulating per word and writing once per word boundary.
    /// Sorted input means consecutive values often share the same word, collapsing N bit-sets
    /// into ~N/64 word writes. Idempotent on already-zero words, so safe for fresh and existing bitmaps.
    /// Cardinality tracking is the caller's responsibility.
    /// </summary>
    private static void OrSortedIntoBitmap(ulong* bitmapPtr, ReadOnlySpan<long> sortedValues)
    {
        ushort firstLow = (ushort)(sortedValues[0] & ContainerValueMask);
        int currentWordIndex = firstLow >> 6;
        ulong currentMask = 1UL << (firstLow & 63);

        for (int j = 1; j < sortedValues.Length; j++)
        {
            ushort low = (ushort)(sortedValues[j] & ContainerValueMask);
            int wordIndex = low >> 6;
            
            if (wordIndex == currentWordIndex)
            {
                currentMask |= 1UL << (low & 63);
            }
            else
            {
                bitmapPtr[currentWordIndex] |= currentMask;
                currentWordIndex = wordIndex;
                currentMask = 1UL << (low & 63);
            }
        }
        bitmapPtr[currentWordIndex] |= currentMask;
    }

    /// <summary>Create a new array container from sorted values with SIMD-friendly allocation.</summary>
    private void CreateArrayContainerFromSorted(long key, ReadOnlySpan<long> sortedValues)
    {
        int neededBytes = AlignForSimd(sortedValues.Length * sizeof(ushort));
        ctx.Allocate(neededBytes, out ByteString storage);
        new Span<byte>(storage.Ptr, storage.Length).Clear();

        NarrowLongToUshort(sortedValues, (ushort*)storage.Ptr);

        AddNewContainer(key, ContainerType.ArrayUnsorted, new ContainerEntry
        {
            Cardinality = sortedValues.Length,
            Data = storage.Ptr,
            Storage = storage
        });
    }

    /// <summary>Batch-add sorted values into an existing container.</summary>
    private void AddRangeToContainer(int slot, ReadOnlySpan<long> sortedValues)
    {
        ref ContainerEntry entry = ref _entries[slot];
        ContainerType type = _types.RawItems[slot];

        switch (type)
        {
            case ContainerType.Bitmap:
            {
                // Lazy: bits are OR'd in; cardinality is marked dirty, so RepairAfterLazy
                // (called by PrepareForReading) recomputes it via popcount.
                OrSortedIntoBitmap((ulong*)entry.Data, sortedValues);
                entry.Cardinality = LazyCardinality;
                break;
            }

            case ContainerType.Range:
            {
                int rangeStart = entry.RangeStart;
                int rangeEnd = rangeStart + entry.Cardinality;

                // Check if all new values are contiguous and can extend one edge.
                ushort firstLow = (ushort)(sortedValues[0] & ContainerValueMask);
                ushort lastLow = (ushort)(sortedValues[^1] & ContainerValueMask);

                // Fully contained in existing range - noop.
                if (firstLow >= rangeStart && lastLow < rangeEnd)
                {
                    break;
                }

                bool contiguousBatch = lastLow - firstLow == sortedValues.Length - 1;
                if (contiguousBatch && TryMergeRangeInPlace(ref entry, firstLow, lastLow + 1))
                    break;

                // Non-contiguous batch (or disjoint contiguous batch) cannot stay as Range.
                int totalCount = entry.Cardinality + sortedValues.Length;
                if (totalCount <= ArrayContainerMaxCardinality)
                {
                    ConvertRangeToArray(ref entry, ref _types.RawItems[slot], rangeStart, entry.Cardinality, sortedValues);
                    break;
                }

                ConvertRangeToBitmap(ref entry, ref _types.RawItems[slot]);
                goto case ContainerType.Bitmap;
            }

            case ContainerType.Array:
            case ContainerType.ArrayUnsorted:
            {
                int newTotal = entry.Cardinality + sortedValues.Length;
                if (newTotal > ArrayContainerMaxCardinality)
                {
                    ConvertArrayToBitmap(ref entry, ref _types.RawItems[slot]);
                    goto case ContainerType.Bitmap; // Convert to bitmap and add
                }

                // If existing data was sorted and the new batch starts strictly after the last
                // existing value, the appended result stays sorted — no need to degrade to
                // ArrayUnsorted (which would force a sort pass in PrepareForReading).
                ushort firstNew = (ushort)(sortedValues[0] & ContainerValueMask);
                bool stillSorted = type == ContainerType.Array
                    && (entry.Cardinality == 0 || firstNew > ((ushort*)entry.Data)[entry.Cardinality - 1]);

                // Ensure enough space
                int neededBytes = AlignForSimd(newTotal * sizeof(ushort));
                if (entry.Storage.Length < neededBytes)
                {
                    ctx.Allocate(neededBytes, out ByteString newStorage);
                    new Span<byte>(entry.Data, entry.Cardinality * sizeof(ushort))
                        .CopyTo(new Span<byte>(newStorage.Ptr, newStorage.Length));
                    ctx.Release(ref entry.Storage);
                    entry.Data = newStorage.Ptr;
                    entry.Storage = newStorage;
                }

                // Append new values
                NarrowLongToUshort(sortedValues, (ushort*)entry.Data + entry.Cardinality);
                entry.Cardinality = newTotal;
                _types.RawItems[slot] = stillSorted ? ContainerType.Array : ContainerType.ArrayUnsorted;
                break;
            }
        }
    }
    
    /// <summary>Lazy OR — same as OrWith but skips per-container cardinality tracking.
    /// Bitmap containers get Cardinality = -1 (dirty). Call RepairAfterLazy() once
    /// after all lazy OR operations to recompute cardinality in a single popcount pass.</summary>
    [SkipLocalsInit]
    public void LazyOrWith(ref RoaringBitmap other)
    {
        if (other.IsEmpty)
            return;

        int otherLen = other._index.Count;
        if (otherLen > 0)
            EnsureIndexCoversKey(otherLen - 1);
        
        _entries.EnsureCapacityFor(ctx, other.ContainerCount);
        _types.EnsureCapacityFor(ctx, other.ContainerCount);

        for (int key = 0; key < otherLen; key++)
        {
            int otherSlot = other.GetSlotForKey(key);
            if (otherSlot < 0)
                continue;

            ref ContainerEntry otherEntry = ref other.GetEntryBySlot(otherSlot);
            int mySlot = GetSlotForKey(key);

            if (mySlot >= 0)
            {
                LazyOrContainerInPlace(ref _entries[mySlot], ref _types.RawItems[mySlot],
                    ref otherEntry, other._types.RawItems[otherSlot]);
            }
            else
            {
                // Steal container from other (zero-copy).
                ContainerType otherType = other._types.RawItems[otherSlot];
                ContainerEntry stolen = otherEntry;
                otherEntry.Storage = default; // we stole this storage, so detach it from the source entry to avoid double-free
                other.FreeContainer(key, otherSlot);
                AddNewContainer(key, otherType, stolen);
            }
        }
    }

    /// <summary>Lazy OR for a single container pair. Skips popcount — marks bitmap
    /// containers with Cardinality = -1.</summary>
    [SkipLocalsInit]
    private void LazyOrContainerInPlace(ref ContainerEntry left, ref ContainerType leftType,
        ref ContainerEntry right, ContainerType rightType)
    {
        if (leftType == ContainerType.Range && rightType == ContainerType.Range)
        {
            if (TryMergeRangeInPlace(ref left, right.RangeStart, RangeEndExclusive(ref right)))
                return;

            // Disjoint ranges: materialize once and keep lazy OR semantics.
            ConvertRangeToBitmap(ref left, ref leftType);
            ulong* stackBmp = stackalloc ulong[BitmapContainerSizeInUInt64];
            ContainerEntry temp = MaterializeRangeIntoBuffer(ref right, stackBmp);
            LazyOrContainerInPlace(ref left, ref leftType, ref temp, ContainerType.Bitmap);
            return;
        }
        if (leftType == ContainerType.Range)
            ConvertRangeToBitmap(ref left, ref leftType);
        if (rightType == ContainerType.Range)
        {
            ulong* stackBmp = stackalloc ulong[BitmapContainerSizeInUInt64];
            ContainerEntry temp = MaterializeRangeIntoBuffer(ref right, stackBmp);
            LazyOrContainerInPlace(ref left, ref leftType, ref temp, ContainerType.Bitmap);
            return;
        }

        switch (leftType, rightType)
        {
            case (ContainerType.Bitmap, ContainerType.Bitmap):
                // OR bitmaps without popcount — just bitwise OR
                BitmapOrNoPop(left.BitmapPtr, right.BitmapPtr, left.BitmapPtr);
                left.Cardinality = LazyCardinality; // mark dirty
                break;

            case (ContainerType.Bitmap, ContainerType.Array):
            case (ContainerType.Bitmap, ContainerType.ArrayUnsorted):
            {
                // Set bits unconditionally — no per-bit cardinality check
                var bmp = left.BitmapPtr;
                ushort* arr = (ushort*)right.Data;
                for (int i = 0; i < right.Cardinality; i++)
                {
                    ushort val = arr[i];
                    bmp[val >> 6] |= 1UL << (val & 63);
                }
                left.Cardinality = LazyCardinality;
                break;
            }

            default:
            {
                // Array×Array: same as eager path (small containers, popcount is trivial)
                ulong* scratch = stackalloc ulong[BitmapContainerSizeInUInt64];
                ulong* dirtyMap = stackalloc ulong[4];
                OrContainerInPlace(ref left, ref leftType, ref right, rightType, scratch, dirtyMap);
                break;
            }
        }
    }


    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void BitmapOrNoPop(ulong* a, ulong* b, ulong* dst) =>
        BitmapOpDispatch<OrOp>(a, b, dst);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void BitmapAndNoPop(ulong* a, ulong* b, ulong* dst) =>
        BitmapOpDispatch<AndOp>(a, b, dst);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void BitmapAndNotNoPop(ulong* a, ulong* b, ulong* dst) =>
        BitmapOpDispatch<AndNotOp>(a, b, dst);

    /// <summary>Recompute cardinality for all containers marked dirty (Cardinality == -1)
    /// after a sequence of lazy bitmap ops (AddRange, LazyOrWith, AndWith, OrWith on
    /// bitmap containers, etc.). Single popcount passes. Containers that pop to zero
    /// (e.g., AND/ANDNOT removed all bits) are freed here.</summary>
    public void RepairAfterLazy()
    {
        for (int i = 0; i < _entries.Count; i++)
        {
            if (_entries[i].Cardinality != LazyCardinality)
                continue;

            ref ContainerEntry entry = ref _entries[i];
            // Only Bitmap containers can be marked Lazy in current paths
            if (_types.RawItems[i] != ContainerType.Bitmap)
                continue;

            int card = 0;
var bmp = entry.BitmapPtr;
            for (int w = 0; w < BitmapContainerSizeInUInt64; w++)
                card += BitOperations.PopCount(bmp[w]);

            if (card == 0)
                FreeContainer(entry.Key, i);
            else
                entry.Cardinality = card;
        }
    }

    /// <summary>Returns the minimum container key in the bitmap, or -1 if empty.</summary>
    public long MinContainerKey
    {
        get
        {
            if (_containerCount == 0) return -1;
            for (int i = 0; i < _index.Count; i++)
            {
                if (_index[i] != IndexAbsent) return i;
            }
            return -1;
        }
    }

    /// <summary>Returns the maximum container key in the bitmap, or -1 if empty.</summary>
    public long MaxContainerKey
    {
        get
        {
            if (_containerCount == 0) return -1;
            for (int i = _index.Count - 1; i >= 0; i--)
            {
                if (_index[i] != IndexAbsent) return i;
            }
            return -1;
        }
    }

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
            AddNewContainer(key, ContainerType.Range, new ContainerEntry
            {
                Cardinality = 1,
                RangeStart = low,
                Storage = default
            });
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
    /// Returns the number of values written. Compatible with Cora's streaming evaluation.
    /// </summary>
    public int Fill(Span<long> buffer, ref RoaringBitmapIterator iterator)
    {
        return iterator.Fill(ref this, buffer);
    }

    public RoaringBitmapIterator GetIterator()
    {
        return new RoaringBitmapIterator(ref this, ctx);
    }

    /// <summary>
    /// Create a deep copy of this bitmap. All container data is cloned into the same ByteStringContext.
    /// The source bitmap is not modified. The clone preserves container types (Range, Array, Bitmap).
    /// </summary>
    public RoaringBitmap Clone()
    {
        var copy = new RoaringBitmap(ctx);

        // Walk entries directly using the Key field - no index indirection needed
        ContainerEntry* entries = _entries.RawItems;
        ContainerType* types = _types.RawItems;
        int entryCount = _entries.Count;
        for (int i = 0; i < entryCount; i++)
        {
            if (types[i] != ContainerType.Free)
                copy.AddContainer(entries[i].Key, types[i], CloneContainer(ctx, ref entries[i], types[i]));
        }
        return copy;
    }

    /// <summary>
    /// Prepare for reading: sort and deduplicate all unsorted array containers,
    /// and repair lazy bitmap cardinalities left dirty by <see cref="AddRange"/>
    /// or <see cref="LazyOrWith"/>. Call this after all writes and before any
    /// read operations (Contains, Fill, set ops, Count). Separates the sort
    /// + popcount cost from the first query, making performance more predictable.
    ///
    /// For sorted input (e.g., Corax posting lists), the array sort step is
    /// nearly free since the arrays are already in order.
    /// </summary>
    [SkipLocalsInit]
    public void PrepareForReading()
    {
        // 8KB scratch bitmap for radix sort: explode unsorted values into bits,
        // extract back as sorted array. O(n) bit-sets + O(1024) word scan vs O(n log n).
        // Dedup is free. Scratch reused across all containers (one clear per chunk touched).
        ulong* scratch = stackalloc ulong[BitmapContainerSizeInUInt64];

        ContainerEntry* entries = _entries.RawItems;
        ContainerType* types = _types.RawItems;
        int entryCount = _entries.Count;
        for (int i = 0; i < entryCount; i++)
        {
            ref ContainerEntry entry = ref entries[i];
            if (types[i] == ContainerType.ArrayUnsorted)
            {
                // All unsorted containers must be sorted for correct iteration order.
                // Contains() can use SIMD linear scan on unsorted data, but
                // Fill() (iteration) must emit entry IDs in ascending order.
                if (entry.Cardinality >= BitmapSortThreshold)
                    SortViaBitmapScratch(ref entry, ref types[i], scratch);
                else
                    SortSmallArray(ref entry, ref types[i]);
            }
        }

        RepairAfterLazy();
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
    // are temporarily - built during query evaluation and discarded immediately after.
    // The 8KB bitmap is already allocated; converting to Array allocates another buffer
    // and scans 1024 words, costing more than it saves for short-lived data.

    /// <summary>
    /// In-place AND: retain only values that also exist in others.
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
                // Not in other - remove
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

        // Range×Range fast paths - no allocation needed
        if (leftType == ContainerType.Range && rightType == ContainerType.Range)
        {
            int intersectStart = Math.Max(left.RangeStart, right.RangeStart);
            int intersectEnd = Math.Min(RangeEndExclusive(ref left), RangeEndExclusive(ref right));
            if (intersectStart >= intersectEnd)
            {
                left.Cardinality = 0;
            }
            else
            {
                left.RangeStart = (ushort)intersectStart;
                left.Cardinality = intersectEnd - intersectStart;
            }
            return;
        }
        if (leftType == ContainerType.Range)
            ConvertRangeToBitmap(ref left, ref leftType);
        if (rightType == ContainerType.Range)
        {
            ulong* stackBmp = stackalloc ulong[BitmapContainerSizeInUInt64];
            ContainerEntry temp = MaterializeRangeIntoBuffer(ref right, stackBmp);
            AndContainerInPlace(ref left, ref leftType, ref temp, ContainerType.Bitmap);
            return;
        }

        switch (leftType, rightType)
        {
            case (ContainerType.Bitmap, ContainerType.Bitmap):
                // Lazy: bitwise AND only; RepairAfterLazy will popcount + free if empty.
                BitmapAndNoPop(left.BitmapPtr, right.BitmapPtr, left.BitmapPtr);
                left.Cardinality = LazyCardinality;
                break;

            case (ContainerType.Bitmap, ContainerType.Array):
            case (ContainerType.Bitmap, ContainerType.ArrayUnsorted):
            {
                // AND bitmap with array: build the intersection in a stack scratch by
                // OR'ing only values that are set in left; then copy back. Lazy cardinality.
                ushort* arr = (ushort*)right.Data;
                var bmp = left.BitmapPtr;
                ulong* scratch = stackalloc ulong[BitmapContainerSizeInUInt64];
                new Span<byte>(scratch, BitmapContainerSizeInBytes).Clear();

                for (int i = 0; i < right.Cardinality; i++)
                {
                    ushort val = arr[i];
                    if ((bmp[val >> 6] & (1UL << (val & 63))) != 0)
                        scratch[val >> 6] |= 1UL << (val & 63);
                }

                new Span<byte>(scratch, BitmapContainerSizeInBytes)
                    .CopyTo(new Span<byte>(left.Data, BitmapContainerSizeInBytes));
                left.Cardinality = LazyCardinality;
                break;
            }

            case (ContainerType.Array, ContainerType.Bitmap):
            case (ContainerType.ArrayUnsorted, ContainerType.Bitmap):
            {
                // Filter left's values against right's bitmap, in-place. Order doesn't matter.
                ushort* arr = (ushort*)left.Data;
                var bmp = right.BitmapPtr;
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
            case (ContainerType.ArrayUnsorted, ContainerType.ArrayUnsorted):
            case (ContainerType.Array, ContainerType.ArrayUnsorted):
            case (ContainerType.ArrayUnsorted, ContainerType.Array):
            {
                if (AdvInstructionSet.IsAcceleratedVector256
                    && left.Cardinality <= SimdLinearScanThreshold
                    && right.Cardinality <= SimdLinearScanThreshold)
                {
                    left.Cardinality = SimdCrossAnd((ushort*)left.Data, left.Cardinality, (ushort*)right.Data, right.Cardinality, (ushort*)left.Data);
                }
                else
                {
                    // Need sorted arrays for galloping merge
                    if (leftType == ContainerType.ArrayUnsorted) SortSmallArray(ref left, ref leftType);
                    if (rightType == ContainerType.ArrayUnsorted) SortSmallArray(ref right, ref rightType);
                    ushort* a = left.ArrayPtr;
                    ushort* b = right.ArrayPtr;
                    left.Cardinality = ArrayContainerAnd(a, left.Cardinality, b, right.Cardinality, a);
                }
                break;
            }
        }
    }

    [SkipLocalsInit]
    private void OrContainerInPlace(ref ContainerEntry left, ref ContainerType leftType,
        ref ContainerEntry right, ContainerType rightType, ulong* scratch, ulong* dirtyMap)
    {
        AssertPrepared(leftType);
        AssertPrepared(rightType);

        if (leftType == ContainerType.Range && rightType == ContainerType.Range)
        {
            if (TryMergeRangeInPlace(ref left, right.RangeStart, RangeEndExclusive(ref right)))
                return;

            // A single range cannot represent disjoint ranges.
            ConvertRangeToBitmap(ref left, ref leftType);
            ulong* stackBmp = stackalloc ulong[BitmapContainerSizeInUInt64];
            ContainerEntry temp = MaterializeRangeIntoBuffer(ref right, stackBmp);
            OrContainerInPlace(ref left, ref leftType, ref temp, ContainerType.Bitmap, scratch, dirtyMap);
            return;
        }
        if (leftType == ContainerType.Range)
            ConvertRangeToBitmap(ref left, ref leftType);
        if (rightType == ContainerType.Range)
        {
            ulong* stackBmp = stackalloc ulong[BitmapContainerSizeInUInt64];
            ContainerEntry temp = MaterializeRangeIntoBuffer(ref right, stackBmp);
            OrContainerInPlace(ref left, ref leftType, ref temp, ContainerType.Bitmap, scratch, dirtyMap);
            return;
        }

        switch (leftType, rightType)
        {
            case (ContainerType.Bitmap, ContainerType.Bitmap):
                // Lazy: bitwise OR only.
                BitmapOrNoPop(left.BitmapPtr, right.BitmapPtr, left.BitmapPtr);
                left.Cardinality = LazyCardinality;
                break;

            case (ContainerType.Bitmap, ContainerType.Array):
            case (ContainerType.Bitmap, ContainerType.ArrayUnsorted):
            {
                // Lazy: set bits unconditionally — no per-bit cardinality check.
                var bmp = left.BitmapPtr;
                ushort* arr = (ushort*)right.Data;
                for (int i = 0; i < right.Cardinality; i++)
                {
                    ushort val = arr[i];
                    bmp[val >> 6] |= 1UL << (val & 63);
                }
                left.Cardinality = LazyCardinality;
                break;
            }

            case (ContainerType.Array, ContainerType.Array):
            case (ContainerType.ArrayUnsorted, ContainerType.Array):
            case (ContainerType.Array, ContainerType.ArrayUnsorted):
            case (ContainerType.ArrayUnsorted, ContainerType.ArrayUnsorted):
            {
                int maxResult = left.Cardinality + right.Cardinality;
                if (maxResult > ArrayContainerMaxCardinality)
                {
                    ConvertArrayToBitmap(ref left, ref leftType);
                    OrContainerInPlace(ref left, ref leftType, ref right, rightType, scratch, dirtyMap);
                }
                else if (leftType == ContainerType.ArrayUnsorted || rightType == ContainerType.ArrayUnsorted)
                {
                    // At least one side unsorted — just append right after left (duplicates harmless,
                    // deduped if/when we sort later or when the bitmap is iterated)
                    EnsureArrayCapacity(ref left, maxResult);
                    ushort* dst = (ushort*)left.Data;
                    ushort* src = (ushort*)right.Data;
                    Unsafe.CopyBlockUnaligned(dst + left.Cardinality, src, (uint)(right.Cardinality * sizeof(ushort)));
                    left.Cardinality += right.Cardinality;
                    leftType = ContainerType.ArrayUnsorted;
                }
                else
                {
                    // Both sorted — merge
                    ushort* tmp = stackalloc ushort[ArrayContainerMaxCardinality];
                    int count = ArrayContainerOr(left.ArrayPtr, left.Cardinality, right.ArrayPtr, right.Cardinality, tmp);
                    EnsureArrayCapacity(ref left, count);
                    Unsafe.CopyBlockUnaligned(left.Data, (byte*)tmp, (uint)(count * sizeof(ushort)));
                    left.Cardinality = count;
                }
                break;
            }

            case (ContainerType.Array, ContainerType.Bitmap):
            case (ContainerType.ArrayUnsorted, ContainerType.Bitmap):
            {
                // Steal right's 8KB buffer if available, otherwise convert left
                if (right.Storage.HasValue && right.Storage.Length >= BitmapContainerSizeInBytes)
                {
                    // Right has 8KB — OR left's array into right's bitmap, then swap ownership
                    ushort* arr = (ushort*)left.Data;
                    ulong* bmp = right.BitmapPtr;
                    for (int i = 0; i < left.Cardinality; i++)
                    {
                        ushort val = arr[i];
                        bmp[val >> 6] |= 1UL << (val & 63);
                    }
                    // Swap: left takes right's bitmap buffer
                    if (left.Storage.HasValue)
                        ctx.Release(ref left.Storage);
                    left.Storage = right.Storage;
                    left.Data = right.Data;
                    left.Cardinality = LazyCardinality;
                    leftType = ContainerType.Bitmap;
                    // Clear right's ownership
                    right.Storage = default;
                    right.Data = null;
                }
                else
                {
                    ConvertArrayToBitmap(ref left, ref leftType);
                    OrContainerInPlace(ref left, ref leftType, ref right, rightType, scratch, dirtyMap);
                }
                break;
            }
        }
    }

    [SkipLocalsInit]
    private void AndNotContainerInPlace(ref ContainerEntry left, ref ContainerType leftType, ref ContainerEntry right, ContainerType rightType)
    {
        AssertPrepared(leftType);
        AssertPrepared(rightType);

        // Range×Range fast path.
        if (leftType == ContainerType.Range && rightType == ContainerType.Range)
        {
            int leftStart = left.RangeStart;
            int leftEnd = RangeEndExclusive(ref left);
            int rightStart = right.RangeStart;
            int rightEnd = RangeEndExclusive(ref right);

            // No overlap.
            if (rightEnd <= leftStart || rightStart >= leftEnd)
                return;

            // Rightfully covers left.
            if (rightStart <= leftStart && rightEnd >= leftEnd)
            {
                left.Cardinality = 0;
                return;
            }

            // Trim low edge.
            if (rightStart <= leftStart)
            {
                left.RangeStart = (ushort)rightEnd;
                left.Cardinality = leftEnd - rightEnd;
                return;
            }

            // Trim high edge.
            if (rightEnd >= leftEnd)
            {
                left.Cardinality = rightStart - leftStart;
                return;
            }

            // Middle cut would split into two ranges - materialize.
            ConvertRangeToBitmap(ref left, ref leftType);
            ulong* stackBmp = stackalloc ulong[BitmapContainerSizeInUInt64];
            ContainerEntry temp = MaterializeRangeIntoBuffer(ref right, stackBmp);
            AndNotContainerInPlace(ref left, ref leftType, ref temp, ContainerType.Bitmap);
            return;
        }
        if (leftType == ContainerType.Range)
            ConvertRangeToBitmap(ref left, ref leftType);
        if (rightType == ContainerType.Range)
        {
            ulong* stackBmp = stackalloc ulong[BitmapContainerSizeInUInt64];
            ContainerEntry temp = MaterializeRangeIntoBuffer(ref right, stackBmp);
            AndNotContainerInPlace(ref left, ref leftType, ref temp, ContainerType.Bitmap);
            return;
        }

        switch (leftType, rightType)
        {
            case (ContainerType.Bitmap, ContainerType.Bitmap):
                // Lazy: bitwise ANDNOT only.
                BitmapAndNotNoPop(left.BitmapPtr, right.BitmapPtr, left.BitmapPtr);
                left.Cardinality = LazyCardinality;
                break;

            case (ContainerType.Bitmap, ContainerType.Array):
            case (ContainerType.Bitmap, ContainerType.ArrayUnsorted):
            {
                // Lazy: clear bits unconditionally — no per-bit cardinality check.
                var bmp = left.BitmapPtr;
                ushort* arr = (ushort*)right.Data;
                for (int i = 0; i < right.Cardinality; i++)
                {
                    ushort val = arr[i];
                    bmp[val >> 6] &= ~(1UL << (val & 63));
                }
                left.Cardinality = LazyCardinality;
                break;
            }

            case (ContainerType.Array, ContainerType.Bitmap):
            case (ContainerType.ArrayUnsorted, ContainerType.Bitmap):
            {
                // Keep left values NOT in the right's bitmap. Order doesn't matter.
                ushort* arr = (ushort*)left.Data;
                var bmp = right.BitmapPtr;
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
            case (ContainerType.ArrayUnsorted, ContainerType.ArrayUnsorted):
            case (ContainerType.Array, ContainerType.ArrayUnsorted):
            case (ContainerType.ArrayUnsorted, ContainerType.Array):
            {
                if (AdvInstructionSet.IsAcceleratedVector256
                    && left.Cardinality <= SimdLinearScanThreshold
                    && right.Cardinality <= SimdLinearScanThreshold)
                {
                    left.Cardinality = SimdCrossAndNot((ushort*)left.Data, left.Cardinality, (ushort*)right.Data, right.Cardinality, (ushort*)left.Data);
                }
                else
                {
                    if (leftType == ContainerType.ArrayUnsorted) SortSmallArray(ref left, ref leftType);
                    if (rightType == ContainerType.ArrayUnsorted) SortSmallArray(ref right, ref rightType);
                    ushort* a = left.ArrayPtr;
                    ushort* b = right.ArrayPtr;
                    left.Cardinality = ArrayContainerAndNot(a, left.Cardinality, b, right.Cardinality, a);
                }
                break;
            }
        }
    }

    /// <summary>
    /// Create a temporary bitmap on the stack from a Range container.
    /// Returns a ContainerEntry pointing to the stackalloc'd buffer (no ByteStringContext allocation).
    /// The returned entry has Storage=default - the caller must NOT release it.
    /// </summary>
    [SkipLocalsInit]
    private static ContainerEntry MaterializeRangeIntoBuffer(ref ContainerEntry entry, ulong* stackBitmap)
    {
        ClearBitmap(stackBitmap);
        FillBitmapFromRange(stackBitmap, entry.RangeStart, entry.Cardinality);

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
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void EnsureIndexCoversKey(long key)
    {
        if (key < _index.Count)
            return;

        IncreaseIndexCapacity(key);
    }


    private void IncreaseIndexCapacity(long key)
    {
        if (key < 0 || key >= int.MaxValue - 16)
            throw new ArgumentOutOfRangeException(nameof(key), $"Container key {key} is out of the valid range (0..{int.MaxValue - 17}).");

        int needed = checked((int)(key + 1));
        int oldCount = _index.Count;
        _index.EnsureCapacityFor(ctx, needed - oldCount);
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
            _entries.Add(ctx, entry);
            _types.Add(ctx, type);
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
            ctx.Release(ref entry.Storage);

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

    private const int InitialArrayContainerSizeInBytes = 128; // 64 shorts — minimum for SIMD linear scan without scalar tail
    private const int SimdAlignment = 32; // Vector256 width in bytes
    private const int SimdLinearScanThreshold = 64; // below this, SIMD linear scan beats binary/quad search

    /// <summary>
    /// Round up to SIMD-aligned size. Ensures all array allocations
    /// are multiples of 32 bytes so Vector256 operations don't need bounds checking.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int AlignForSimd(int bytes)
    {
        return Math.Max(InitialArrayContainerSizeInBytes, (bytes + SimdAlignment - 1) & ~(SimdAlignment - 1));
    }

    private ContainerEntry CreateArrayContainer(long key)
    {
        ctx.Allocate(InitialArrayContainerSizeInBytes, out ByteString storage);
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
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void EnsureArrayCapacity(ref ContainerEntry entry, int requiredEntries)
    {
        int requiredBytes = requiredEntries * sizeof(ushort);
        if (requiredBytes <= entry.Storage.Length)
            return;

        IncreaseArrayCapacity(ref entry, requiredBytes);
    }

    private void IncreaseArrayCapacity(ref ContainerEntry entry, int requiredBytes)
    {
        int newSize = Math.Max(entry.Storage.Length * 2, requiredBytes);
        newSize = Math.Min(newSize, BitmapContainerSizeInBytes);

        ctx.Allocate(newSize, out ByteString newStorage);
        int copyBytes = entry.Cardinality * sizeof(ushort);
        if (copyBytes > 0)
            Unsafe.CopyBlockUnaligned(newStorage.Ptr, entry.Data, (uint)copyBytes);

        if (entry.Storage.HasValue)
            ctx.Release(ref entry.Storage);
        entry.Storage = newStorage;
        entry.Data = newStorage.Ptr;
    }

    internal ContainerEntry CreateBitmapContainer(long key)
    {
        ctx.Allocate(BitmapContainerSizeInBytes, out ByteString storage);
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
                        break; // duplicate of last element - noop
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
                // Would break sort order - switch to unsorted for O(1) appends
                type = ContainerType.ArrayUnsorted;
                goto case ContainerType.ArrayUnsorted;

            case ContainerType.ArrayUnsorted:
                if (entry.Cardinality >= ArrayContainerMaxCardinality)
                {
                    // At capacity: explode into bitmap scratch, count unique values
                    ulong* scratch = stackalloc ulong[BitmapContainerSizeInUInt64];
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
                        ctx.Allocate(BitmapContainerSizeInBytes, out ByteString bmpStorage);
                        Unsafe.CopyBlockUnaligned(bmpStorage.Ptr, (byte*)scratch, BitmapContainerSizeInBytes);
                        if (entry.Storage.HasValue)
                            ctx.Release(ref entry.Storage);
                            entry.Storage = bmpStorage;
                            entry.Data = bmpStorage.Ptr;
                            type = ContainerType.Bitmap;
                            entry.Cardinality = uniqueCount;
                        BitmapContainerAdd(ref entry, value);
                        break;
                    }
                    // Has room after dedup - extract sorted array from scratch
                    int sorted = 0;
                    for (int wordIdx = 0; wordIdx < BitmapContainerSizeInUInt64; wordIdx++)
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
                if (!TryMergeRangeInPlace(ref entry, value, value + 1))
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
            ContainerType.ArrayUnsorted => SimdLinearContains((ushort*)entry.Data, entry.Cardinality, value),
            ContainerType.Bitmap => BitmapContainerContains(entry.Data, value),
            ContainerType.Range => value >= entry.RangeStart && value < entry.RangeStart + entry.Cardinality,
            _ => false
        };
    }

    #endregion

    #region Array Sorting

    /// <summary>
    /// Remove duplicates from a small unsorted array without sorting.
    /// For each element, SIMD-scans the preceding elements to check if it's a duplicate.
    /// O(n²/16) with SIMD, fast for n ≤ 64.
    /// </summary>
    private static void DeduplicateSmallUnsorted(ref ContainerEntry entry)
    {
        ushort* arr = (ushort*)entry.Data;
        int count = entry.Cardinality;
        if (count <= 1)
            return;

        int write = 1;
        for (int i = 1; i < count; i++)
        {
            ushort val = arr[i];
            bool isDup = false;

            // Scalar scan for duplicates in the already-written portion.
            // write is at most SimdLinearScanThreshold (64), so this is fast.
            for (int j = 0; j < write; j++)
            {
                if (arr[j] == val) { isDup = true; break; }
            }

            if (!isDup)
                arr[write++] = val;
        }
        entry.Cardinality = write;
    }

    // Threshold: below this count, comparison sort on a small array
    // is cheaper than the bitmap radix sort path.
    private const int BitmapSortThreshold = 128;

    /// <summary>
    /// Assert that a container has been prepared for reading.
    /// After PrepareForReading(), ArrayUnsorted is valid only for small containers (≤ SimdLinearScanThreshold)
    /// which are handled via SIMD linear scan without sorting.
    /// </summary>
    [Conditional("DEBUG")]
    internal static void AssertPrepared(ContainerType type)
    {
        // ArrayUnsorted is allowed for small containers after PrepareForReading
    }

    [Conditional("DEBUG")]
    private static void AssertSorted(ReadOnlySpan<long> values)
    {
        for (int i = 1; i < values.Length; i++)
        {
            Debug.Assert(values[i] > values[i - 1],
                $"AddRange requires strictly sorted (unique) input: values[{i - 1}]={values[i - 1]} >= values[{i}]={values[i]}");
        }
    }

    /// <summary>
    /// Comparison sort + dedup for small arrays below the bitmap radix sort threshold.
    /// </summary>
    private static void SortSmallArray(ref ContainerEntry entry, ref ContainerType type)
    {
        var arr = entry.ArrayData;
        int count = entry.Cardinality;

        new Span<ushort>(arr, count).Sort();

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
        if (AdvInstructionSet.IsAcceleratedVector128)
        {
            if (cardinality <= SimdLinearScanThreshold)
                return SimdLinearContains((ushort*)data, cardinality, value);
            return SimdQuadContains((ushort*)data, cardinality, value);
        }
        return ArrayContainerFind((ushort*)data, cardinality, value) >= 0;
    }

    /// <summary>
    /// SIMD linear scan for small arrays (&lt;= 64 values). Works on both sorted and unsorted data.
    /// Uses Vector256 (16 shorts) when available, Vector128 (8 shorts) otherwise.
    /// For cardinality smaller than the vector width, falls back to scalar linear scan.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static bool SimdLinearContains(ushort* arr, int cardinality, ushort value)
    {
        // All array containers are allocated with AlignForSimd (≥128 bytes = 64 shorts),
        // so Vector256/128 loads from arr+0 through arr+capacity are always safe.
        // For small cardinalities, the extra bytes beyond cardinality are zeroed and
        // can't match a non-zero value (ushort values are 0-65535, zero is a valid value
        // but only matches if actually present in the set).
        // Guard: for cardinality == 0, skip entirely.
        if (cardinality == 0)
            return false;

        int i = 0;

        if (AdvInstructionSet.IsAcceleratedVector256)
        {
            Vector256<ushort> needle256 = Vector256.Create(value);
            for (; i + Vector256<ushort>.Count <= cardinality; i += Vector256<ushort>.Count)
            {
                if (Vector256.EqualsAny(Vector256.Load(arr + i), needle256))
                    return true;
            }
            if (i < cardinality)
            {
                // Tail overlap load — safe because allocation is SIMD-aligned.
                // May read beyond cardinality into zeroed padding. If value is 0,
                // we might false-positive on padding, so check the found index.
                var tail = Vector256.Load(arr + cardinality - Vector256<ushort>.Count);
                if (Vector256.EqualsAny(tail, needle256))
                {
                    // Verify the match is within cardinality (not in padding)
                    for (int j = i; j < cardinality; j++)
                        if (arr[j] == value) return true;
                }
            }
            return false;
        }

        Vector128<ushort> needle128 = Vector128.Create(value);
        for (; i + Vector128<ushort>.Count <= cardinality; i += Vector128<ushort>.Count)
        {
            if (Vector128.EqualsAny(Vector128.Load(arr + i), needle128))
                return true;
        }
        if (i < cardinality)
        {
            var tail = Vector128.Load(arr + cardinality - Vector128<ushort>.Count);
            if (Vector128.EqualsAny(tail, needle128))
            {
                for (int j = i; j < cardinality; j++)
                    if (arr[j] == value) return true;
            }
        }
        return false;
    }

    /// <summary>
    /// SIMD Quad search for larger sorted arrays (&gt; 64 values): quaternary interpolation + SIMD block check.
    /// Divides sorted ushort[] into 8-element blocks (Vector128), narrows via quaternary
    /// search on block boundaries, then checks the final block with a single SIMD compare.
    /// Based on Daniel Lemire's algorithm (https://lemire.me/blog/2026/04/27/you-can-beat-the-binary-search/)
    /// </summary>
    internal static bool SimdQuadContains(ushort* arr, int cardinality, ushort value)
    {
        int gap = Vector128<ushort>.Count; // 8
        int numBlocks = cardinality / gap;
        int @base = 0;
        int n = numBlocks;

        // Quaternary search on block boundaries
        while (n > 3)
        {
            int quarter = n >> 2;
            int k1 = arr[(@base + quarter + 1) * gap - 1];
            int k2 = arr[(@base + 2 * quarter + 1) * gap - 1];
            int k3 = arr[(@base + 3 * quarter + 1) * gap - 1];
            @base += ((k1 < value ? 1 : 0) + (k2 < value ? 1 : 0) + (k3 < value ? 1 : 0)) * quarter;
            n -= 3 * quarter;
        }

        while (n > 1)
        {
            int half = n >> 1;
            @base = arr[(@base + half + 1) * gap - 1] < value ? @base + half : @base;
            n -= half;
        }

        int lo = arr[(@base + 1) * gap - 1] < value ? @base + 1 : @base;

        if (lo < numBlocks)
        {
            if (Vector128.EqualsAny(Vector128.Load(arr + lo * gap), Vector128.Create(value)))
                return true;
        }

        // Remainder past last full block
        for (int j = numBlocks * gap; j < cardinality; j++)
        {
            ushort v = arr[j];
            if (v >= value) return v == value;
        }
        return false;
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
    /// SIMD cross-compare AND: for each element in A, check if it exists anywhere in B.
    /// Broadcasts each A[i] across Vector256, compares against all chunks of B simultaneously.
    /// Works on unsorted data. Both arrays must be ≤ SimdLinearScanThreshold and buffers ≥ 128 bytes.
    /// </summary>
    internal static int SimdCrossAnd(ushort* a, int aLen, ushort* b, int bLen, ushort* dst)
    {
        int di = 0;
        int bVecCount = (bLen + Vector256<ushort>.Count - 1) / Vector256<ushort>.Count;

        for (int i = 0; i < aLen; i++)
        {
            Vector256<ushort> needle = Vector256.Create(a[i]);
            bool found = false;

            for (int bv = 0; bv < bVecCount; bv++)
            {
                // Safe to over-read: buffer guaranteed ≥ 128 bytes, and bLen ≤ 64
                Vector256<ushort> bChunk = Vector256.Load(b + bv * Vector256<ushort>.Count);
                if (Vector256.EqualsAny(needle, bChunk))
                {
                    found = true;
                    break;
                }
            }

            if (found)
                dst[di++] = a[i];
        }

        return di;
    }

    /// <summary>
    /// SIMD cross-compare ANDNOT: keep elements in A that do NOT exist in B.
    /// Same cross-compare pattern, inverted match logic.
    /// </summary>
    internal static int SimdCrossAndNot(ushort* a, int aLen, ushort* b, int bLen, ushort* dst)
    {
        int di = 0;
        int bVecCount = (bLen + Vector256<ushort>.Count - 1) / Vector256<ushort>.Count;

        for (int i = 0; i < aLen; i++)
        {
            Vector256<ushort> needle = Vector256.Create(a[i]);
            bool found = false;

            for (int bv = 0; bv < bVecCount; bv++)
            {
                Vector256<ushort> bChunk = Vector256.Load(b + bv * Vector256<ushort>.Count);
                if (Vector256.EqualsAny(needle, bChunk))
                {
                    found = true;
                    break;
                }
            }

            if (!found)
                dst[di++] = a[i];
        }

        return di;
    }

    /// <summary>
    /// Strategy interface for generic array match operations (AND / ANDNOT).
    /// Both share the same SIMD galloping structure; only the keep/discard logic differs.
    /// </summary>
    private interface IArrayMatchStrategy
    {
        /// <summary>true for AND (keep matches), false for AND NOT (discard matches).</summary>
        static abstract bool KeepOnMatch { get; }
        /// <summary>false for AND (skip A values not in B), true for AND NOT (keep A values not in B).</summary>
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

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int RangeEndExclusive(ref ContainerEntry entry) => entry.RangeStart + entry.Cardinality;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static bool TryMergeRangeInPlace(ref ContainerEntry entry, int otherStart, int otherEndExclusive)
    {
        int rangeStart = entry.RangeStart;
        int rangeEnd = rangeStart + entry.Cardinality;

        // Half-open interval merge: [rangeStart, rangeEnd) with [otherStart, otherEndExclusive).
        // Merge on overlap or exact touch; fail only when there is an actual gap.
        if (otherStart > rangeEnd || otherEndExclusive < rangeStart)
            return false;

        int mergedStart = Math.Min(rangeStart, otherStart);
        int mergedEnd = Math.Max(rangeEnd, otherEndExclusive);
        entry.RangeStart = (ushort)mergedStart;
        entry.Cardinality = mergedEnd - mergedStart;
        return true;
    }

    /// <summary>
    /// Convert a Range container to Bitmap or Array to handle an Add outside the contiguous range.
    /// </summary>
    private void ConvertRangeForAdd(ref ContainerEntry entry, ref ContainerType type, ushort value)
    {
        Debug.Assert(type == ContainerType.Range);
        int rangeStart = entry.RangeStart;
        int rangeEnd = rangeStart + entry.Cardinality;
        int rangeCount = entry.Cardinality;

        if (rangeCount >= ArrayContainerMaxCardinality)
        {
            // Range alone fills an array container - use bitmap
            ConvertRangeToBitmap(ref entry, ref type);
            BitmapContainerAdd(ref entry, value);
        }
        else
        {
            ConvertRangeToArray(ref entry, ref type, rangeStart, rangeCount, [value]);
        }
    }

    /// <summary>
    /// Fill a cleared bitmap buffer with bits rangeStart..rangeStart+rangeCount-1 set.
    /// </summary>
    internal static void FillBitmapFromRange(ulong* bitmap, int rangeStart, int rangeCount)
    {
        if (rangeCount <= 0)
            return;

        int firstWord = rangeStart >> 6;
        int firstBit = rangeStart & 63;
        int lastValue = rangeStart + rangeCount - 1;
        int lastWord = lastValue >> 6;
        int lastBit = lastValue & 63;

        if (firstWord == lastWord)
        {
            ulong startMask = ulong.MaxValue << firstBit;
            ulong endMask = lastBit == 63 ? ulong.MaxValue : (1UL << (lastBit + 1)) - 1;
            bitmap[firstWord] = startMask & endMask;
            return;
        }

        bitmap[firstWord] = ulong.MaxValue << firstBit;
        if (lastWord > firstWord + 1)
            new Span<ulong>(bitmap + firstWord + 1, lastWord - firstWord - 1).Fill(ulong.MaxValue);
        bitmap[lastWord] = lastBit == 63 ? ulong.MaxValue : (1UL << (lastBit + 1)) - 1;
    }

    private void ConvertRangeToBitmap(ref ContainerEntry entry, ref ContainerType type)
    {
        Debug.Assert(type == ContainerType.Range);

        ctx.Allocate(BitmapContainerSizeInBytes, out ByteString storage);
        ulong* bitmap = (ulong*)storage.Ptr;
        ClearBitmap(bitmap);
        FillBitmapFromRange(bitmap, entry.RangeStart, entry.Cardinality);

        if (entry.Storage.HasValue)
            ctx.Release(ref entry.Storage);

        entry.Storage = storage;
        entry.Data = storage.Ptr;
        type = ContainerType.Bitmap;
    }

    private void ConvertRangeToArray(ref ContainerEntry entry, ref ContainerType type, int rangeStart, int rangeCount, ReadOnlySpan<long> sortedValues)
    {
        int totalCount = rangeCount + sortedValues.Length;
        int neededBytes = AlignForSimd(totalCount * sizeof(ushort));
        ctx.Allocate(neededBytes, out ByteString storage);
        ushort* arr = (ushort*)storage.Ptr;

        FillSequentialUshorts(arr, rangeStart, rangeCount);
        NarrowLongToUshort(sortedValues, arr + rangeCount);

        Debug.Assert(entry.Storage.HasValue is false, "Range containers should not have Storage");

        entry.Storage = storage;
        entry.Data = storage.Ptr;
        entry.Cardinality = totalCount;
        type = ContainerType.ArrayUnsorted;
    }


    #endregion

    #region Bitmap Container

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void BitmapContainerAdd(ref ContainerEntry e, ushort value)
    {
        var bitmap = e.BitmapPtr;
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

        // Vector256 load fetches 4 longs (32 bytes) per iteration, prefetching the next cache line.
        // PopCount each element after the load.
        if (AdvInstructionSet.IsAcceleratedVector256)
        {
            int i = 0;
            for (; i + Vector256<ulong>.Count <= BitmapContainerSizeInUInt64; i += Vector256<ulong>.Count)
            {
                Vector256<ulong> vec = Vector256.Load(bitmap + i);
                count += BitOperations.PopCount(vec.GetElement(0));
                count += BitOperations.PopCount(vec.GetElement(1));
                count += BitOperations.PopCount(vec.GetElement(2));
                count += BitOperations.PopCount(vec.GetElement(3));
            }
            for (; i < BitmapContainerSizeInUInt64; i++)
                count += BitOperations.PopCount(bitmap[i]);
        }
        else
        {
            for (int i = 0; i < BitmapContainerSizeInUInt64; i++)
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
        for (int wordIdx = 0; wordIdx < BitmapContainerSizeInUInt64; wordIdx++)
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

        ctx.Allocate(BitmapContainerSizeInBytes, out ByteString newStorage);
        ArrayToBitmap(arr, count, (ulong*)newStorage.Ptr);

        if (unsorted)
        {
            var updatedCount = BitmapContainerCardinality(newStorage.Ptr);
            entry.Cardinality = updatedCount;
        }
        if (entry.Storage.HasValue)
            ctx.Release(ref entry.Storage);

        entry.Storage = newStorage;
        entry.Data = newStorage.Ptr;
        type = ContainerType.Bitmap;
    }




    #endregion

    internal ByteStringContext Context => ctx;

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

    /// <summary>
    /// Narrow long values to ushort (extract low 16 bits) using SIMD when available.
    /// Uses Vector256.IsHardwareAccelerated for cross-platform support (x86 + ARM).
    /// </summary>
    internal static void NarrowLongToUshort(ReadOnlySpan<long> source, ushort* destination)
    {
        int j = 0;
        int count = source.Length;
        ref long src = ref MemoryMarshal.GetReference(source);

        // Vector load + chained Narrow (ulong→uint→ushort) — truncation is equivalent to masking with 0xFFFF
        // for non-negative values, so no explicit AND with ContainerValueMask is needed.
        if (Vector256.IsHardwareAccelerated && count >= 16)
        {
            for (; j <= count - 16; j += 16)
            {
                var v0 = Vector256.LoadUnsafe(ref src, (nuint)j).AsUInt64();
                var v1 = Vector256.LoadUnsafe(ref src, (nuint)(j + 4)).AsUInt64();
                var v2 = Vector256.LoadUnsafe(ref src, (nuint)(j + 8)).AsUInt64();
                var v3 = Vector256.LoadUnsafe(ref src, (nuint)(j + 12)).AsUInt64();

                var u0 = Vector256.Narrow(v0, v1); // 8 uints
                var u1 = Vector256.Narrow(v2, v3); // 8 uints
                Vector256.Narrow(u0, u1).StoreUnsafe(ref *destination, (nuint)j); // 16 shorts
            }
        }
        else if (Vector128.IsHardwareAccelerated && count >= 8)
        {
            for (; j <= count - 8; j += 8)
            {
                var v0 = Vector128.LoadUnsafe(ref src, (nuint)j).AsUInt64();
                var v1 = Vector128.LoadUnsafe(ref src, (nuint)(j + 2)).AsUInt64();
                var v2 = Vector128.LoadUnsafe(ref src, (nuint)(j + 4)).AsUInt64();
                var v3 = Vector128.LoadUnsafe(ref src, (nuint)(j + 6)).AsUInt64();

                var u0 = Vector128.Narrow(v0, v1); // 4 uints
                var u1 = Vector128.Narrow(v2, v3); // 4 uints
                Vector128.Narrow(u0, u1).StoreUnsafe(ref *destination, (nuint)j); // 8 shorts
            }
        }

        for (; j < count; j++)
            destination[j] = (ushort)(source[j] & ContainerValueMask);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void FillSequentialUshorts(ushort* destination, int startValue, int count)
    {
        int i = 0;
        if (AdvInstructionSet.IsAcceleratedVector256 && count >= Vector256<ushort>.Count)
        {
            Vector256<ushort> offsets = Vector256.Create(
                (ushort)0, (ushort)1, (ushort)2, (ushort)3, (ushort)4, (ushort)5, (ushort)6, (ushort)7,
                (ushort)8, (ushort)9, (ushort)10, (ushort)11, (ushort)12, (ushort)13, (ushort)14, (ushort)15);
            Vector256<ushort> valueVec = Vector256.Create((ushort)startValue);
            Vector256<ushort> stride = Vector256.Create((ushort)Vector256<ushort>.Count);

            for (; i + Vector256<ushort>.Count <= count; i += Vector256<ushort>.Count)
            {
                (valueVec + offsets).Store(destination + i);
                valueVec = valueVec + stride;
            }
        }

        if (AdvInstructionSet.IsAcceleratedVector128 && count - i >= Vector128<ushort>.Count)
        {
            Vector128<ushort> offsets = Vector128.Create(
                (ushort)0, (ushort)1, (ushort)2, (ushort)3, (ushort)4, (ushort)5, (ushort)6, (ushort)7);
            Vector128<ushort> valueVec = Vector128.Create((ushort)(startValue + i));
            Vector128<ushort> stride = Vector128.Create((ushort)Vector128<ushort>.Count);

            for (; i + Vector128<ushort>.Count <= count; i += Vector128<ushort>.Count)
            {
                (valueVec + offsets).Store(destination + i);
                valueVec = valueVec + stride;
            }
        }

        for (; i < count; i++)
            destination[i] = (ushort)(startValue + i);
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

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void BitmapOpDispatch<TOp>(ulong* a, ulong* b, ulong* dst) where TOp : struct, IBitmapOp
    {
        if (AdvInstructionSet.IsAcceleratedVector512)
            BitmapOpVector512NoPop<TOp>(a, b, dst);
        else if (AdvInstructionSet.IsAcceleratedVector256)
            BitmapOpVector256NoPop<TOp>(a, b, dst);
        else if (AdvInstructionSet.IsAcceleratedVector128)
            BitmapOpVector128NoPop<TOp>(a, b, dst);
        else
            BitmapOpScalarNoPop<TOp>(a, b, dst);
    }

    private static void BitmapOpVector512NoPop<TOp>(ulong* a, ulong* b, ulong* dst) where TOp : struct, IBitmapOp
    {
        int N = Vector512<ulong>.Count;
        for (int i = 0; i < BitmapContainerSizeInUInt64; i += N)
            TOp.Apply(Vector512.Load(a + i), Vector512.Load(b + i)).Store(dst + i);
    }

    private static void BitmapOpVector256NoPop<TOp>(ulong* a, ulong* b, ulong* dst) where TOp : struct, IBitmapOp
    {
        int N = Vector256<ulong>.Count;
        for (int i = 0; i < BitmapContainerSizeInUInt64; i += N)
            TOp.Apply(Vector256.Load(a + i), Vector256.Load(b + i)).Store(dst + i);
    }

    private static void BitmapOpVector128NoPop<TOp>(ulong* a, ulong* b, ulong* dst) where TOp : struct, IBitmapOp
    {
        int N = Vector128<ulong>.Count;
        for (int i = 0; i < BitmapContainerSizeInUInt64; i += N)
            TOp.Apply(Vector128.Load(a + i), Vector128.Load(b + i)).Store(dst + i);
    }

    private static void BitmapOpScalarNoPop<TOp>(ulong* a, ulong* b, ulong* dst) where TOp : struct, IBitmapOp
    {
        for (int i = 0; i < BitmapContainerSizeInUInt64; i++)
            dst[i] = TOp.Apply(a[i], b[i]);
    }

    internal static ContainerEntry CloneContainer(ByteStringContext ctx, ref ContainerEntry entry, ContainerType type)
    {
        if (type == ContainerType.Range)
            return new ContainerEntry { Cardinality = entry.Cardinality, RangeStart = entry.RangeStart, Storage = default };

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
        // Walk entries directly - more cache-friendly than index indirection.
        // Skip free entries (Type == Free).
        if (_entries.IsValid)
        {
            ContainerEntry* entries = _entries.RawItems;
            ContainerType* types = _types.RawItems;
            int count = _entries.Count;
            for (int i = 0; i < count; i++)
            {
                if (types[i] != ContainerType.Free && entries[i].Storage.HasValue)
                    ctx.Release(ref entries[i].Storage);
            }

            _entries.Dispose(ctx);
        }

        if (_types.IsValid)
            _types.Dispose(ctx);

        if (_index.IsValid)
            _index.Dispose(ctx);
    }
}

public enum ContainerType : byte
{
    /// <summary>Sorted ushort array. Binary search for Contains. Merge-based set ops.</summary>
    Array = 0,
    /// <summary>8KB bitmap (1024 longs). Direct bit access.</summary>
    Bitmap = 1,
    /// <summary>
    /// Contiguous values RangeStart..RangeStart+Cardinality-1 are set. No data allocation needed.
    /// Cardinality == BitsPerContainer means all 65,536 bits set (full container).
    /// Sequential Add at either edge is an O(1) increment.
    /// </summary>
    Range = 2,
    /// <summary>
    /// Unsorted ushort array. Add is O(1) append. On the first read (Contains, set ops, iteration),
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
    /// For Range containers, this stores RangeStart encoded as (RangeStart + 1), avoiding
    /// per-entry size growth while keeping Range as allocation-free.
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
    /// Container key (value >> 16). Allows walking entries without index direction.
    /// The key allows us to have ~140 T containers in the bitmap (2^47), bit enough
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


    /// <summary>
    /// Access container data as ushort array.
    /// </summary>
    public ushort* ArrayData
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => (ushort*)Data;
    }

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

    /// <summary>
    /// Start offset (0..65535) for Range containers. Encoded in Data as (start + 1),
    /// so start=0 remains representable.
    /// </summary>
    internal ushort RangeStart
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => (ushort)((nuint)Data - 1);
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        set => Data = (byte*)(nuint)(value + 1);
    }
}
