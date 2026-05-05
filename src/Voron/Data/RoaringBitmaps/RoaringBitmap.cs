using System;
using System.Diagnostics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using Sparrow;
using Sparrow.Server;
using Voron.Util;

namespace Voron.Data.RoaringBitmaps;

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
///
/// Threading and consumption model:
/// - Single-threaded by design. Concurrent access from multiple threads is not supported;
///   no internal locking, no atomic state. Callers are responsible for not sharing a bitmap
///   across threads simultaneously.
/// - Set operations (<see cref="AndWith"/>, <see cref="AndNotWith"/>, <see cref="LazyOrWith"/>)
///   are intentionally destructive on their right-hand argument. The right side may have its
///   containers stolen, sorted in place, or otherwise mutated. This is a deliberate trade —
///   it lets the implementation skip a copy in hot paths. After being passed as the right side
///   of a set op, a bitmap is considered consumed and must not be used for further reads or
///   set ops on its own. Pair this with <see cref="Clear"/>'s storage recycling: a consumed
///   bitmap can be Clear()'d and reused as a scratch buffer with no allocator round-trip.
/// </summary>
[StructLayout(LayoutKind.Auto)]
public unsafe ref partial struct RoaringBitmap : IDisposable
{
    private ref RoaringBitmapData _data;
    private readonly ByteStringContext _ctx;

    public RoaringBitmap(ref RoaringBitmapData data, ByteStringContext ctx)
    {
        _data = ref data;
        _ctx = ctx;
        // _freeListHead == 0 is the terminator; default(RoaringBitmapData) already has it.
    }

    private const int BitmapContainerSizeInBytes = 8192; // 8KB
    public const int BitmapContainerSizeInUInt64 = BitmapContainerSizeInBytes / sizeof(ulong);
    private const int ArrayContainerMaxCardinality = BitmapContainerSizeInBytes / sizeof(ushort); // crossover: array at max costs same as bitmap
    public const int ContainerKeyShift = 16;
    public const int ContainerSize = 1 << ContainerKeyShift; // 65,536 entry IDs per container
    private const int ContainerValueMask = 0xFFFF;
    internal const int ContainerValueMaskPublic = ContainerValueMask;
    private const int LazyCardinality = -1;
    
    private const int InitialArrayContainerSizeInBytes = 64; // 32 shorts — minimum for SIMD linear scan without scalar tail
    private const int SimdAlignment = 32; // Vector256 width in bytes
    private const int SimdLinearScanThreshold = 64; // below this, SIMD linear scan beats binary/quad search


    // _freeListHead / NextFreeSlot use a 1-based encoding: 0 = empty (zero-init), slot+1 = real index.
    // This lets default(RoaringBitmapData) be a valid empty bitmap without any explicit init.
    internal const int IndexAbsent = -1;        // key is not present in index

    public readonly int ContainerCount => _data.ContainerCount;

    public long Count => _data.Count;

    public readonly bool IsEmpty => _data.IsEmpty;

    /// <summary>
    /// Reset all containers without releasing memory. Container storages are pushed onto
    /// the dedicated free list and made available for reuse on the next allocation,
    /// avoiding the round-trip through the ByteStringContext free pool.
    /// </summary>
    public void Clear()
    {
        int count = _data._entries.Count;
        if (count == 0 && _data._index.Count == 0)
            return;

        ContainerEntry* entries = _data._entries.RawItems;
        ContainerType* types = _data._types.RawItems;

        // Recover every storage and park it on the storage free list.
        for (int i = 0; i < count; i++)
        {
            if (types[i] != ContainerType.Free && entries[i].Storage.HasValue)
                ReturnStorageToFreeList(entries[i].Storage);
        }

        _data._entries.Clear();
        _data._types.Clear();
        _data._containerCount = 0;
        _data._freeListHead = 0; // 0 = empty (entries are gone, free list is empty too)

        int* indexRaw = _data._index.RawItems;
        int indexLen = _data._index.Count;
        new Span<int>(indexRaw, indexLen).Fill(IndexAbsent);
    }

    /// <summary>
    /// Push <paramref name="bs"/> onto the head of the storage free list.
    /// The first <c>sizeof(ByteString)</c> bytes of <paramref name="bs"/>'s data are
    /// overwritten with the current head, forming the intrusive next-pointer.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void ReturnStorageToFreeList(ByteString bs)
    {
        *(ByteString*)bs.Ptr = _data._nextFreeStorage; // chain: new node's next = old head
        _data._nextFreeStorage = bs;                   // new head = bs
    }

    /// <summary>
    /// Acquire storage of at least <paramref name="neededBytes"/>. Walks the intrusive
    /// storage free list for the best (smallest sufficient) fit, unlinks it in-place,
    /// and returns it. Falls back to a fresh ctx allocation when nothing fits.
    /// </summary>
    private void AllocateOrRecycle(int neededBytes, out ByteString storage)
    {
        // `scan` is a managed ref to the ByteString field that holds the current node.
        // Starting at the head field, it advances to each node's embedded next-pointer.
        // `bestRef` tracks the field pointing to the best-fit node found so far, so we
        // can unlink the winner with a single assignment after the walk.
        ref ByteString scan = ref _data._nextFreeStorage;
        ref ByteString bestRef = ref Unsafe.NullRef<ByteString>();
        int bestLen = int.MaxValue;

        while (scan.HasValue)
        {
            if (scan.Length >= neededBytes && scan.Length < bestLen)
            {
                bestRef = ref scan;
                bestLen = scan.Length;
            }
            // Advance: the next ByteString is stored in the first bytes of scan's data.
            scan = ref *(ByteString*)scan.Ptr;
        }

        if (Unsafe.IsNullRef(ref bestRef) == false)
        {
            storage = bestRef;                       // read the winner
            bestRef = *(ByteString*)storage.Ptr;     // unlink: point prev's next to winner's next
            return;
        }

        _ctx.Allocate(neededBytes, out storage);
    }

    /// <summary>
    /// Swap the internal state of two-bitmaps. O(1) — just swaps the RoaringBitmapData contents.
    /// Used after entry scan to swap the filtered results into the main bitmap.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void SwapContents(ref RoaringBitmap other)
    {
        (_data, other._data) = (other._data, _data);
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

            int slot = _data.GetSlotForKey(key);
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
        AllocateOrRecycle(BitmapContainerSizeInBytes, out ByteString storage);
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
        AllocateOrRecycle(neededBytes, out ByteString storage);

        CopyDenseBottom16BitsToUshortArray(sortedValues, (ushort*)storage.Ptr);

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
        ref ContainerEntry entry = ref _data._entries[slot];
        ref ContainerType type = ref _data._types.RawItems[slot];

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
                if (MaybeConvertRangeToArray(ref entry, ref type, rangeStart, entry.Cardinality, sortedValues))
                    break;

                ConvertRangeToBitmap(ref entry, ref type);
                goto case ContainerType.Bitmap; // Convert to bitmap and add
            }

            case ContainerType.Array or ContainerType.ArrayUnsorted:
            {
                int newTotal = entry.Cardinality + sortedValues.Length;
                if (newTotal > ArrayContainerMaxCardinality)
                {
                    ConvertArrayToBitmap(ref entry, ref _data._types.RawItems[slot]);
                    goto case ContainerType.Bitmap; // Convert to bitmap and add
                }

                // If existing data was sorted and the new batch starts strictly after the last
                // existing value, the appended result stays sorted — no need to degrade to
                // ArrayUnsorted (which would force a sort pass in PrepareForReading).
                ushort firstNew = (ushort)(sortedValues[0] & ContainerValueMask);
                bool stillSorted = type == ContainerType.Array
                    && (entry.Cardinality == 0 || firstNew > entry.ArrayData[entry.Cardinality - 1]);

                // Ensure enough space
                int neededBytes = AlignForSimd(newTotal * sizeof(ushort));
                if (entry.Storage.Length < neededBytes)
                {
                    AllocateOrRecycle(neededBytes, out ByteString newStorage);
                    new Span<byte>(entry.Data, entry.Cardinality * sizeof(ushort))
                        .CopyTo(new Span<byte>(newStorage.Ptr, newStorage.Length));
                    ReturnStorageToFreeList(entry.Storage); // recycle old; new is larger
                    entry.Data = newStorage.Ptr;
                    entry.Storage = newStorage;
                }

                // Append new values
                CopyDenseBottom16BitsToUshortArray(sortedValues, entry.ArrayData + entry.Cardinality);
                entry.Cardinality = newTotal;
                _data._types.RawItems[slot] = stillSorted ? ContainerType.Array : ContainerType.ArrayUnsorted;
                break;
            }
        }
    }

    /// <summary>
    /// Lazy OR skips per-container cardinality tracking.
    /// Bitmap containers get Cardinality = -1 (dirty). Call RepairAfterLazy() once
    /// after all lazy OR operations to recompute cardinality in a single popcount pass.
    ///
    /// [1] Bitmap→Array conversion after set ops: we intentionally skip this.
    /// Standard roaring bitmaps convert sparse bitmap results back to array containers
    /// to save memory and speed up later operations. But in Corax, these bitmaps
    /// are temporarily - built during query evaluation and discarded immediately after.
    /// The 8KB bitmap is already allocated; converting to Array allocates another buffer
    /// and scans 1024 words, costing more than it saves for short-lived data.
    /// </summary>
    private void LazyOrWith(scoped ref RoaringBitmap other)
    {
        if (other.IsEmpty)
            return;

        int otherLen = other._data._index.Count;
        if (otherLen > 0)
            EnsureIndexCoversKey(otherLen - 1);

        _data._entries.EnsureCapacityFor(_ctx, other.ContainerCount);
        _data._types.EnsureCapacityFor(_ctx, other.ContainerCount);

        for (int key = 0; key < otherLen; key++)
        {
            int otherSlot = other._data.GetSlotForKey(key);
            if (otherSlot < 0)
                continue;

            ref ContainerEntry otherEntry = ref other._data._entries[otherSlot];
            int mySlot = _data.GetSlotForKey(key);

            if (mySlot >= 0)
            {
                LazyOrContainerInPlace(ref _data._entries[mySlot], ref _data._types.RawItems[mySlot],
                    ref otherEntry, other._data._types.RawItems[otherSlot]);
                continue;
            }

            // Steal container from other (zero-copy).
            ContainerType otherType = other._data._types.RawItems[otherSlot];
            ContainerEntry stolen = otherEntry;
            otherEntry = default; // Clear the entry
            AddNewContainer(key, otherType, stolen);
        }
    }

    public void LazyOrWith(ref RoaringBitmapData otherData)
    {
        scoped RoaringBitmap other = new(ref otherData, _ctx);
        LazyOrWith(ref other);
    }

    /// <summary>Lazy OR for a single container pair. Skips popcount — marks bitmap
    /// containers with Cardinality = -1.</summary>
    [SkipLocalsInit]
    private void LazyOrContainerInPlace(ref ContainerEntry left, ref ContainerType leftType,
        ref ContainerEntry right, ContainerType rightType)
    {
        switch (leftType, rightType)
        {
            case (ContainerType.Range, ContainerType.Range):
            {
                if (TryMergeRangeInPlace(ref left, right.RangeStart, RangeEndExclusive(ref right)))
                    return;

                if (TryConvertRangeRangeToBestContainer(ref left, ref leftType, ref right))
                    return;

                // Disjoint ranges: materialize the other range and keep lazy OR semantics.
                ulong* stackBmp = stackalloc ulong[BitmapContainerSizeInUInt64];
                ContainerEntry temp = MaterializeRangeIntoBuffer(ref right, stackBmp);
                LazyOrContainerInPlace(ref left, ref leftType, ref temp, ContainerType.Bitmap);
                break;
            }
            case (ContainerType.Range, _):
            {
                ConvertRangeToBitmap(ref left, ref leftType);
                LazyOrContainerInPlace(ref left, ref leftType, ref right, rightType);
                break;
            }
            case (_, ContainerType.Range):
            {
                // Materialize the other range and keep lazy OR semantics.
                ulong* stackBitmap = stackalloc ulong[BitmapContainerSizeInUInt64];
                ContainerEntry temp2 = MaterializeRangeIntoBuffer(ref right, stackBitmap);
                LazyOrContainerInPlace(ref left, ref leftType, ref temp2, ContainerType.Bitmap);
                break;
            }
            // From here, we no longer have ranges to worry about
            case (ContainerType.Bitmap, ContainerType.Bitmap):
            {
                // OR bitmaps without popcount — just bitwise OR
                BitmapOrNoPop(left.BitmapPtr, right.BitmapPtr, left.BitmapPtr);
                left.Cardinality = LazyCardinality; // mark dirty
                break;
            }
            case (ContainerType.Bitmap, ContainerType.Array or ContainerType.ArrayUnsorted):
            {
                // Set bits unconditionally — no per-bit cardinality check
                SetArrayInBitmap(right.ArrayData, right.Cardinality, left.BitmapPtr);
                left.Cardinality = LazyCardinality;
                break;
            }
            case (ContainerType.Array or ContainerType.ArrayUnsorted, ContainerType.Array or ContainerType.ArrayUnsorted):
            {
                int maxResult = left.Cardinality + right.Cardinality;
                if (maxResult > ArrayContainerMaxCardinality)
                {
                    ConvertArrayToBitmap(ref left, ref leftType);
                    LazyOrContainerInPlace(ref left, ref leftType, ref right, rightType);
                    return;
                }
                // Append values left & right — duplicates are harmless, deduped on PrepareForReading.
                AppendArrayContainers(ref left, ref right, maxResult);
                leftType = ContainerType.ArrayUnsorted;
                break;
            }
            case (ContainerType.Array or ContainerType.ArrayUnsorted, ContainerType.Bitmap):
            {
                // Steal right's 8KB buffer
                Debug.Assert(right.Storage.Length is BitmapContainerSizeInBytes, "Right container bitmap buffer must be exactly 8KB");
                // OR left's array values into right's bitmap, then take ownership of the buffer.
                SetArrayInBitmap(left.ArrayData, left.Cardinality, right.BitmapPtr);
                if (left.Storage.HasValue)
                    _ctx.Release(ref left.Storage);
                left.Storage = right.Storage;
                left.Data = right.Data;
                left.Cardinality = LazyCardinality;
                leftType = ContainerType.Bitmap;
                right = default;
                break;
            }
            default: // should never reach here
                throw new InvalidOperationException($"Unexpected container type pair: {leftType}, {rightType}");
        }
    }

    /// <summary>Recompute cardinality for all containers marked dirty (Cardinality == -1)
    /// after a sequence of lazy bitmap ops (AddRange, LazyOrWith, AndWith, OrWith on
    /// bitmap containers, etc.). Single popcount passes. Containers that pop to zero
    /// (e.g., AND/ANDNOT removed all bits) are freed here.</summary>
    public void RepairAfterLazy()
    {
        for (int i = 0; i < _data._entries.Count; i++)
        {
            if (// _types is a dense array, so cheaper to scan through it first
                _data._types.RawItems[i] != ContainerType.Bitmap ||
                _data._entries[i].Cardinality != LazyCardinality)
                continue;

            ref ContainerEntry entry = ref _data._entries[i];

             entry.Cardinality  = BitmapContainerCardinality(entry.Data);

            if (entry.Cardinality is 0)
                FreeContainer(entry.Key, i);
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Add(long value)
    {
        Debug.Assert(value >= 0, "RoaringBitmap only supports non-negative values.");
        long key = value >> ContainerKeyShift;
        ushort low = (ushort)(value & ContainerValueMask);

        int slot = _data.GetSlotForKey(key);
        if (slot >= 0)
        {
            ref ContainerEntry entry = ref _data._entries[slot];
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
    public bool Contains(long value) => _data.Contains(value);

    /// <summary>
    /// Fill the buffer with values from the bitmap, starting from the current iteration state.
    /// Returns the number of values written. Compatible with Cora's streaming evaluation.
    /// </summary>
    public int Fill(Span<long> buffer, ref RoaringBitmapIterator iterator)
    {
        return iterator.Fill(ref _data, buffer);
    }

    public RoaringBitmapIterator GetIterator()
    {
        return new RoaringBitmapIterator(ref _data, _ctx);
    }

    /// <summary>
    /// Create a deep copy of the bitmap's data. All container data is cloned into the same ByteStringContext.
    /// The source bitmap is not modified. The clone preserves container types (Range, Array, Bitmap).
    /// </summary>
    public RoaringBitmapData CloneData()
    {
        RoaringBitmapData target = default;
        RoaringBitmap copy = new(ref target, _ctx);

        // Walk entries directly using the Key field - no index indirection needed
        ContainerEntry* entries = _data._entries.RawItems;
        ContainerType* types = _data._types.RawItems;
        int entryCount = _data._entries.Count;
        for (int i = 0; i < entryCount; i++)
        {
            if (types[i] != ContainerType.Free)
                copy.AddContainer(entries[i].Key, types[i], CloneContainer(_ctx, ref entries[i], types[i]));
        }
        return target;
    }

    // Threshold: below this count, comparison sort on a small array
    // is cheaper than the bitmap radix sort path.
    private const int BitmapSortThreshold = 128;

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

        ContainerEntry* entries = _data._entries.RawItems;
        ContainerType* types = _data._types.RawItems;
        int entryCount = _data._entries.Count;
        for (int i = 0; i < entryCount; i++)
        {
            switch (types[i])
            {
                case ContainerType.ArrayUnsorted:
                {
                    // All unsorted containers must be sorted for correct iteration order.
                    // Contains() can use SIMD linear scan on unsorted data, but
                    // Fill() (iteration) must emit entry IDs in ascending order.
                    ref ContainerEntry entry = ref entries[i];
                    if (entry.Cardinality >= BitmapSortThreshold)
                        SortViaBitmapScratch(ref entry, ref types[i], scratch);
                    else
                        SortAndDedupSmallArray(ref entry, ref types[i]);
                    break;
                }

                case ContainerType.Bitmap when entries[i].Cardinality == LazyCardinality:
                {
                    ref ContainerEntry entry = ref entries[i];
                    entry.Cardinality = BitmapContainerCardinality(entry.Data);
                    if (entry.Cardinality == 0)
                        FreeContainer(entry.Key, i);
                    break;
                }
            }
        }
    }

    /// <summary>
    /// Radix sort using 8KB bitmap scratch space.
    /// O(n) bit-sets and O(n) word scan. Dedup is free (a duplicate bit-set is noop).
    /// dirtyMap tracks which 4-ulong chunks have been written so extraction only
    /// visits touched chunks, skipping clean regions entirely.
    /// </summary>
    private static void SortViaBitmapScratch(ref ContainerEntry entry, ref ContainerType type, ulong* scratch)
    {
        Debug.Assert(type == ContainerType.ArrayUnsorted);

        var arr = entry.ArrayData;
        int count = entry.Cardinality;

        ulong* dirtyMap = stackalloc ulong[4];
        Debug.Assert(dirtyMap[0] is 0 ,"This ensure that the stackalloc was cleared, *this* method should not have [SkipLocalsInit]");

        // Explode: set bits for each value, mark the chunk dirty on first touch and clear it.
        for (int i = 0; i < count; i++)
        {
            ushort val = arr[i];
            int wordIdx = val >> 6;
            int chunkIdx = wordIdx >> 2;
            if (BitmapContains(dirtyMap, (ushort)chunkIdx) == false)
            {
                BitmapSet(dirtyMap, (ushort)chunkIdx);
                new Span<byte>(scratch + chunkIdx * 4, 4 * sizeof(ulong)).Clear();
            }

            BitmapSet(scratch, val);
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

    /// <summary>
    /// In-place AND: retain only values that also exist in others.
    /// Walks both index arrays; containers in this bitmap with no match in other are freed.
    /// </summary>
    public void AndWith(scoped ref RoaringBitmap other)
    {
        int* myIdx = _data._index.RawItems;
        int myLen = _data._index.Count;

        for (int key = 0; key < myLen; key++)
        {
            int mySlot = myIdx[key];
            if (mySlot < 0)
                continue;

            int otherSlot = other._data.GetSlotForKey(key);
            if (otherSlot < 0)
            {
                // Not in other - remove; donate storage to other's free list so any
                // remaining container conversions on that side can reuse it without
                // round-tripping through ctx.Allocate.
                DonateStorageThenFreeContainer(key, mySlot, ref other);
            }
            else
            {
                ref ContainerEntry myEntry = ref _data._entries[mySlot];
                ref ContainerEntry otherEntry = ref other._data._entries[otherSlot];
                AndContainerInPlace(ref myEntry, ref _data._types.RawItems[mySlot], ref otherEntry, other._data._types.RawItems[otherSlot]);
                if (myEntry.Cardinality == 0)
                    DonateStorageThenFreeContainer(key, mySlot, ref other);
            }
        }
    }

    public void AndWith(ref RoaringBitmapData otherData)
    {
        scoped RoaringBitmap other = new(ref otherData, _ctx);
        AndWith(ref other);
    }

    /// <summary>Donate this slot's storage to <paramref name="recipient"/>'s free pool
    /// before freeing the entry. Used during AndWith / AndNotWith to arm the consumed
    /// right-side bitmap with buffers that this side would otherwise have stranded on
    /// its own pool.</summary>
    private void DonateStorageThenFreeContainer(long key, int slot, scoped ref RoaringBitmap recipient)
    {
        ref ContainerEntry entry = ref _data._entries[slot];
        if (entry.Storage.HasValue)
        {
            recipient.ReturnStorageToFreeList(entry.Storage);
            entry.Storage = default;
        }
        FreeContainer(key, slot);
    }

    /// <summary>
    /// In-place ANDNOT: remove all values that exist in other from this bitmap.
    /// </summary>
    public void AndNotWith(scoped ref RoaringBitmap other)
    {
        int myLen = _data._index.Count;
        int* myIdx = _data._index.RawItems;

        for (int key = 0; key < myLen; key++)
        {
            int mySlot = myIdx[key];
            if (mySlot < 0)
                continue;

            int otherSlot = other._data.GetSlotForKey(key);
            if (otherSlot < 0)
                continue; // nothing to subtract

            ref ContainerEntry otherEntry = ref other._data._entries[otherSlot];
            AndNotContainerInPlace(ref _data._entries[mySlot], ref _data._types.RawItems[mySlot], ref otherEntry, other._data._types.RawItems[otherSlot]);
            if (_data._entries[mySlot].Cardinality == 0)
                DonateStorageThenFreeContainer(key, mySlot, ref other);
        }
    }

    public void AndNotWith(ref RoaringBitmapData otherData)
    {
        scoped RoaringBitmap other = new(ref otherData, _ctx);
        AndNotWith(ref other);
    }

    [SkipLocalsInit]
    private void AndContainerInPlace(ref ContainerEntry left, ref ContainerType leftType, ref ContainerEntry right, ContainerType rightType)
    {
        ulong* scratch = stackalloc ulong[BitmapContainerSizeInUInt64];

        while (true)
        {
            switch (leftType, rightType)
            {
                // Range×Range fast paths - no allocation needed
                case (ContainerType.Range, ContainerType.Range):
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
                case (ContainerType.Range, _):
                {
                    ConvertRangeToBitmap(ref left, ref leftType);
                    continue;
                }
                case (_, ContainerType.Range):
                {
                    ContainerEntry temp = MaterializeRangeIntoBuffer(ref right, scratch);
                    AndContainerInPlace(ref left, ref leftType, ref temp, ContainerType.Bitmap);
                    return;
                }
                case (ContainerType.Bitmap, ContainerType.Bitmap):
                {
                    // Lazy: bitwise AND only; PreparForReading will popcount + free if empty.
                    BitmapAndNoPop(left.BitmapPtr, right.BitmapPtr, left.BitmapPtr);
                    left.Cardinality = LazyCardinality;
                    break;
                }
                case (ContainerType.Bitmap, ContainerType.Array or ContainerType.ArrayUnsorted):
                {
                    // AND bitmap with array: build the intersection in a stack scratch by
                    // OR'ing only values that are set in left; then copy back. Lazy cardinality.
                    ushort* arr = right.ArrayData;
                    var bmp = left.BitmapPtr;
                    new Span<byte>(scratch, BitmapContainerSizeInBytes).Clear();

                    for (int i = 0; i < right.Cardinality; i++)
                    {
                        ushort val = arr[i];
                        if (BitmapContains(bmp, val)) BitmapSet(scratch, val);
                    }

                    new Span<byte>(scratch, BitmapContainerSizeInBytes).CopyTo(new Span<byte>(left.Data, BitmapContainerSizeInBytes));
                    left.Cardinality = LazyCardinality;
                    break;
                }
                case (ContainerType.Array or ContainerType.ArrayUnsorted, ContainerType.Bitmap):
                {
                    // Filter left's values against right's bitmap, in-place. Order doesn't matter.
                    ushort* arr = left.ArrayData;
                    var bmp = right.BitmapPtr;
                    int count = 0;
                    for (int i = 0; i < left.Cardinality; i++)
                    {
                        ushort val = arr[i];
                        if (BitmapContains(bmp, val)) arr[count++] = val;
                    }

                    left.Cardinality = count;
                    break;
                }
                case (ContainerType.Array or ContainerType.ArrayUnsorted, ContainerType.Array or ContainerType.ArrayUnsorted):
                {
                    if (AdvInstructionSet.IsAcceleratedVector256 && left.Cardinality <= SimdLinearScanThreshold && right.Cardinality <= SimdLinearScanThreshold)
                    {
                        Debug.Assert(left.Storage.Length % Vector256<ushort>.Count == 0 && right.Storage.Length % Vector256<ushort>.Count == 0, "array containers must be SIMD-aligned for SIMD AND, see SimdContains for details");

                        left.Cardinality = SimdCrossAnd(left.ArrayData, left.Cardinality, right.ArrayData, right.Cardinality, left.ArrayData);
                    }
                    else
                    {
                        // Need sorted arrays for galloping merge
                        if (leftType == ContainerType.ArrayUnsorted) SortAndDedupSmallArray(ref left, ref leftType);
                        if (rightType == ContainerType.ArrayUnsorted) SortAndDedupSmallArray(ref right, ref rightType);
                        ushort* a = left.ArrayData;
                        ushort* b = right.ArrayData;
                        left.Cardinality = ArrayContainerAnd(a, left.Cardinality, b, right.Cardinality, a);
                    }

                    break;
                }
                default:
                    throw new InvalidOperationException($"Invalid container combination: {leftType} and {rightType}");
            }

            break;
        }
    }

    [SkipLocalsInit]
    private void AndNotContainerInPlace(ref ContainerEntry left, ref ContainerType leftType, ref ContainerEntry right, ContainerType rightType)
    {
        switch (leftType, rightType)
        {
            case (ContainerType.Range, ContainerType.Range):
            {
                AndNotTwoRangesInPlace(ref left, ref leftType, ref right);
                break;
            }
            case (ContainerType.Range, _):
            {
                ConvertRangeToBitmap(ref left, ref leftType);
                AndNotContainerInPlace(ref left, ref leftType, ref right, rightType);
                break;
            }
            case (_, ContainerType.Range):
            {
                ulong* stackBmp = stackalloc ulong[BitmapContainerSizeInUInt64];
                ContainerEntry temp = MaterializeRangeIntoBuffer(ref right, stackBmp);
                AndNotContainerInPlace(ref left, ref leftType, ref temp, ContainerType.Bitmap);
                break;
            }
            case (ContainerType.Bitmap, ContainerType.Bitmap):
            {
                // Lazy: bitwise ANDNOT only.
                BitmapAndNotNoPop(left.BitmapPtr, right.BitmapPtr, left.BitmapPtr);
                left.Cardinality = LazyCardinality;
                break;
            }
            case (ContainerType.Bitmap, ContainerType.Array or ContainerType.ArrayUnsorted):
            {
                // Lazy: clear bits unconditionally — no per-bit cardinality check.
                ClearArrayInBitmap(right.ArrayData, right.Cardinality, left.BitmapPtr);
                left.Cardinality = LazyCardinality;
                break;
            }
            case (ContainerType.Array or ContainerType.ArrayUnsorted, ContainerType.Bitmap):
            {
                // Keep left values NOT in the right's bitmap. Order doesn't matter.
                ushort* arr = left.ArrayData;
                var bmp = right.BitmapPtr;
                int count = 0;
                for (int i = 0; i < left.Cardinality; i++)
                {
                    ushort val = arr[i];
                    if (BitmapContains(bmp, val) == false)
                        arr[count++] = val;
                }

                left.Cardinality = count;
                break;
            }
            case (ContainerType.Array or ContainerType.ArrayUnsorted, ContainerType.Array or ContainerType.ArrayUnsorted):
            {
                // merge small arrays using SIMD
                if (AdvInstructionSet.IsAcceleratedVector256
                    && left.Cardinality <= SimdLinearScanThreshold
                    && right.Cardinality <= SimdLinearScanThreshold)
                {
                    Debug.Assert(left.Storage.Length % Vector256<ushort>.Count == 0 && right.Storage.Length % Vector256<ushort>.Count == 0,
                        "array containers must be SIMD-aligned for SIMD ANDNOT, see SimdCrossAndNot for details");
                    left.Cardinality = SimdCrossAndNot(left.ArrayData, left.Cardinality, right.ArrayData, right.Cardinality, left.ArrayData);
                    break;
                }

                if (leftType == ContainerType.ArrayUnsorted)
                    SortAndDedupSmallArray(ref left, ref leftType);
                if (rightType == ContainerType.ArrayUnsorted)
                    SortAndDedupSmallArray(ref right, ref rightType);
                ushort* a = left.ArrayData;
                ushort* b = right.ArrayData;
                left.Cardinality = ArrayContainerAndNot(a, left.Cardinality, b, right.Cardinality, a);
                break;
            }
            default:
                throw new InvalidOperationException($"Invalid container combination: {leftType} and {rightType}");
        }
    }

    private void AndNotTwoRangesInPlace(ref ContainerEntry left, ref ContainerType leftType, ref ContainerEntry right)
    {
        int leftStart = left.RangeStart;
        int leftEnd = RangeEndExclusive(ref left);
        int rightStart = right.RangeStart;
        int rightEnd = RangeEndExclusive(ref right);

        // No overlap.
        if (rightEnd <= leftStart || rightStart >= leftEnd)
            return;

        // Right is covering all of left.
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
    }

    /// <summary>
    /// Create a temporary bitmap on the stack from a Range container.
    /// Returns a ContainerEntry pointing to the stackalloc buffer (no ByteStringContext allocation).
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
    /// Ensure the index array covers the given key, filling new slots with -1.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void EnsureIndexCoversKey(long key)
    {
        if (key < _data._index.Count)
            return;

        IncreaseIndexCapacity(key);
    }


    private void IncreaseIndexCapacity(long key)
    {
        if (key < 0 || key >= int.MaxValue - 16)
            throw new ArgumentOutOfRangeException(nameof(key), $"Container key {key} is out of the valid range (0..{int.MaxValue - 17}).");

        int needed = checked((int)(key + 1));
        int oldCount = _data._index.Count;
        _data._index.EnsureCapacityFor(_ctx, needed - oldCount);
        _data._index.Count = needed;

        // Fill new slots with -1 (absent)
        int* ptr = _data._index.RawItems;
        new Span<int>(ptr + oldCount, needed - oldCount).Fill(IndexAbsent);
    }

    /// <summary>
    /// Add a new container entry and register it in the index. Returns the entry slot.
    /// </summary>
    private int AddNewContainer(long key, ContainerType type, in ContainerEntry entry)
    {
        EnsureIndexCoversKey(key);

        int slot;
        if (_data._freeListHead != 0) // 0 = empty; non-zero = slot+1 (1-based)
        {
            slot = _data._freeListHead - 1; // decode: real index = stored - 1
            Debug.Assert(_data._types.RawItems[slot] == ContainerType.Free, "Expected free entry");
            _data._freeListHead = (int)_data._entries[slot].NextFreeSlot; // next encoded value (0 or slot+1)
            _data._entries[slot] = entry;
            _data._types.RawItems[slot] = type;
        }
        else
        {
            slot = _data._entries.Count;
            _data._entries.Add(_ctx, entry);
            _data._types.Add(_ctx, type);
        }

        // we checked size of key in EnsureIndexCoversKey, so this cast is safe
        _data._entries[slot].Key = (uint)key;

        _data._index.RawItems[key] = slot;
        _data._containerCount++;
        return slot;
    }

    /// <summary>
    /// Remove a container: detach its storage onto <see cref="_data._freeStorages"/> for reuse,
    /// mark the index as absent, and chain the slot onto the entry free list.
    /// </summary>
    private void FreeContainer(long key, int slot)
    {
        ref ContainerEntry entry = ref _data._entries[slot];
        if (entry.Storage.HasValue)
            ReturnStorageToFreeList(entry.Storage);

        entry = default;
        _data._types.RawItems[slot] = ContainerType.Free;
        entry.NextFreeSlot = (uint)_data._freeListHead; // current head (0 or prev_slot+1)
        _data._freeListHead = slot + 1;                 // encode: store as real_index + 1

        _data._index.RawItems[key] = IndexAbsent;
        _data._containerCount--;
    }

    /// <summary>
    /// Add a container entry during result-building (set operations). Used when building
    /// a new bitmap from scratch where keys are added in order.
    /// </summary>
    private void AddContainer(long key, ContainerType type, in ContainerEntry entry)
    {
        if (entry.Cardinality == 0)
            return;

        AddNewContainer(key, type, entry);
    }

    #endregion

    #region Container Management
    
    /// <summary>
    /// Round up to SIMD-aligned size. Ensures all array allocations
    /// are multiples of 32 bytes so Vector256 operations don't need bounds checking.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int AlignForSimd(int bytes)
    {
        return Math.Max(InitialArrayContainerSizeInBytes, (bytes + SimdAlignment - 1) & ~(SimdAlignment - 1));
    }

    /// <summary>
    /// Append all values from <paramref name="right"/> into <paramref name="left"/>, which together
    /// hold <paramref name="totalCount"/> entries. Steals an existing buffer when possible to avoid allocation:
    /// uses left's buffer if it already has room, right's buffer (swapping ownership) if it fits, otherwise
    /// allocates a new one. Result is always <see cref="ContainerType.ArrayUnsorted"/>.
    /// </summary>
    private void AppendArrayContainers(ref ContainerEntry left, ref ContainerEntry right, int totalCount)
    {
        int neededBytes = totalCount * sizeof(ushort);
        Debug.Assert(neededBytes <= BitmapContainerSizeInBytes, "Total count exceeds maximum for array container");
        int leftCardinality = left.Cardinality;
        left.Cardinality = totalCount;
        if (neededBytes > right.Storage.Length)
        {
            EnsureArrayCapacity(ref left, totalCount);
            Unsafe.CopyBlockUnaligned(left.ArrayData + leftCardinality, right.ArrayData, (uint)(right.Cardinality * sizeof(ushort)));
            return;
        }
        // Right buffer fits both — prepend left's values into right, then take ownership.
        Unsafe.CopyBlockUnaligned(right.ArrayData + right.Cardinality, left.ArrayData, (uint)(leftCardinality * sizeof(ushort)));
        if (left.Storage.HasValue)
            _ctx.Release(ref left.Storage);
        left.Storage = right.Storage;
        left.Data = right.Data;
        right = default;
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

        AllocateOrRecycle(newSize, out ByteString newStorage);
        int copyBytes = entry.Cardinality * sizeof(ushort);
        if (copyBytes > 0)
            Unsafe.CopyBlockUnaligned(newStorage.Ptr, entry.Data, (uint)copyBytes);

        if (entry.Storage.HasValue)
            ReturnStorageToFreeList(entry.Storage);
        entry.Storage = newStorage;
        entry.Data = newStorage.Ptr;
    }

    #endregion

    #region Container Operations

    [SkipLocalsInit]
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void AddToContainer(ref ContainerEntry entry, int slot, ushort value)
    {
        ref ContainerType type = ref _data._types.RawItems[slot];
        switch (type)
        {
            case ContainerType.Array:
                // If value is >= the last element, append (or noop for duplicate) and stay sorted
                if (entry.Cardinality > 0 && value >= entry.ArrayData[entry.Cardinality - 1])
                {
                    if (value == entry.ArrayData[entry.Cardinality - 1])
                        break; // duplicate of last element - noop
                    if (entry.Cardinality >= ArrayContainerMaxCardinality)
                    {
                        ConvertArrayToBitmap(ref entry, ref type);
                        BitmapSet(entry.BitmapPtr, value);
                        entry.Cardinality = LazyCardinality;
                        break;
                    }
                    EnsureArrayCapacity(ref entry, entry.Cardinality + 1);
                    entry.ArrayData[entry.Cardinality++] = value;
                    break;
                }
                // Would break sort order - switch to unsorted for O(1) appends
                type = ContainerType.ArrayUnsorted;
                goto case ContainerType.ArrayUnsorted;

            case ContainerType.ArrayUnsorted:
                if (entry.Cardinality >= ArrayContainerMaxCardinality)
                {
                    // At capacity: sort+dedup via bitmap scratch, then promote if still full
                    ulong* scratch = stackalloc ulong[BitmapContainerSizeInUInt64];
                    SortViaBitmapScratch(ref entry, ref type, scratch); // type → Array, deduped
                    if (entry.Cardinality >= ArrayContainerMaxCardinality)
                    {
                        ConvertArrayToBitmap(ref entry, ref type);
                        goto case ContainerType.Bitmap;
                    }
                    goto case ContainerType.Array;// we are now sorted (and not full), we'll let the array handler deal with it.
                }
                EnsureArrayCapacity(ref entry, entry.Cardinality + 1);
                entry.ArrayData[entry.Cardinality++] = value;
                break;

            case ContainerType.Bitmap:
                BitmapSet(entry.BitmapPtr, value);
                entry.Cardinality = LazyCardinality;
                break;

            case ContainerType.Range:
                if (TryMergeRangeInPlace(ref entry, value, value + 1) == false)
                {
                    ConvertRangeForAdd(ref entry, ref type, value);
                }
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(type), $"Unexpected value to add: {type}");
        }
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
        ushort* arr = entry.ArrayData;
        int count = entry.Cardinality;

        AllocateOrRecycle(BitmapContainerSizeInBytes, out ByteString newStorage);
        ClearBitmap((ulong*)newStorage.Ptr);
        SetArrayInBitmap(arr, count, (ulong*)newStorage.Ptr);

        if (unsorted)
        {
            var updatedCount = BitmapContainerCardinality(newStorage.Ptr);
            entry.Cardinality = updatedCount;
        }
        if (entry.Storage.HasValue)
            ReturnStorageToFreeList(entry.Storage);

        entry.Storage = newStorage;
        entry.Data = newStorage.Ptr;
        type = ContainerType.Bitmap;
    }

    #endregion

    public void Dispose()
    {
        if (_data._entries.IsValid)
        {
            ContainerEntry* entries = _data._entries.RawItems;
            int count = _data._entries.Count;
            for (int i = 0; i < count; i++)
            {
                if (entries[i].Storage.HasValue)
                    _ctx.Release(ref entries[i].Storage);
            }

            _data._entries.Dispose(_ctx);
        }

        // Release every storage parked on the storage free list — these are storages that
        // were recycled via FreeContainer/Clear but never re-used before Dispose.
        ByteString freeNode = _data._nextFreeStorage;
        while (freeNode.HasValue)
        {
            ByteString nextNode = *(ByteString*)freeNode.Ptr; // read next before releasing
            _ctx.Release(ref freeNode);
            freeNode = nextNode;
        }

        if (_data._types.IsValid)
            _data._types.Dispose(_ctx);

        if (_data._index.IsValid)
            _data._index.Dispose(_ctx);
    }
}
