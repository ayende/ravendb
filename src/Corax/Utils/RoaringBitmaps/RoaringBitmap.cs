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
    public const int BitmapContainerSizeInUlongs = BitmapContainerSizeInBytes / sizeof(ulong); // 1024
    public const int BitsPerContainer = 65536; // 2^16
    public const int ArrayContainerMaxCardinality = 4096; // crossover point: 4096 * 2 = 8KB
    public const int NegatedArrayMinCardinality = BitsPerContainer - ArrayContainerMaxCardinality; // 61440
    public const int ContainerKeyShift = 16;
    public const int ContainerValueMask = 0xFFFF;

    private ByteStringContext _ctx;
    private NativeList<ContainerEntry> _containers;

    public RoaringBitmap(ByteStringContext ctx)
    {
        _ctx = ctx;
        _containers = new NativeList<ContainerEntry>();
    }

    public readonly int ContainerCount => _containers.Count;

    public long Cardinality
    {
        get
        {
            long total = 0;
            for (int i = 0; i < _containers.Count; i++)
                total += _containers[i].Cardinality;
            return total;
        }
    }

    public readonly bool IsEmpty => _containers.Count == 0;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Add(long value)
    {
        long key = value >> ContainerKeyShift;
        ushort low = (ushort)(value & ContainerValueMask);

        int idx = FindContainer(key);
        if (idx >= 0)
        {
            ref ContainerEntry entry = ref _containers[idx];
            AddToContainer(ref entry, low);
        }
        else
        {
            // Insert new container at the correct sorted position
            int insertAt = ~idx;
            ContainerEntry newEntry = CreateArrayContainer(key);
            ArrayContainerAdd(newEntry.Data, ref newEntry.Cardinality, low);
            InsertContainerAt(insertAt, newEntry);
        }
    }

    public void AddRange(long start, long end)
    {
        for (long v = start; v < end; v++)
            Add(v);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Remove(long value)
    {
        long key = value >> ContainerKeyShift;
        ushort low = (ushort)(value & ContainerValueMask);

        int idx = FindContainer(key);
        if (idx < 0)
            return;

        ref ContainerEntry entry = ref _containers[idx];
        RemoveFromContainer(ref entry, low);

        if (entry.Cardinality == 0)
            RemoveContainerAt(idx);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly bool Contains(long value)
    {
        long key = value >> ContainerKeyShift;
        ushort low = (ushort)(value & ContainerValueMask);

        int idx = FindContainer(key);
        if (idx < 0)
            return false;

        ref ContainerEntry entry = ref _containers[idx];
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
        for (int i = 0; i < _containers.Count; i++)
        {
            ref ContainerEntry entry = ref _containers[i];
            if (entry.Type == ContainerType.Bitmap)
            {
                TryConvertBitmapToRun(ref entry);
            }
            else if (entry.Type == ContainerType.Array)
            {
                TryConvertArrayToRun(ref entry);
            }
        }
    }

    #region Container Search

    /// <summary>
    /// Binary search for a container by key. Returns index if found, or ~insertionPoint if not found.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal readonly int FindContainer(long key)
    {
        int lo = 0;
        int hi = _containers.Count - 1;

        while (lo <= hi)
        {
            int mid = lo + ((hi - lo) >> 1);
            long midKey = _containers[mid].Key;

            if (midKey == key)
                return mid;
            if (midKey < key)
                lo = mid + 1;
            else
                hi = mid - 1;
        }

        return ~lo;
    }

    internal readonly ref ContainerEntry GetContainer(int index) => ref _containers[index];

    #endregion

    #region Container Management

    private ContainerEntry CreateArrayContainer(long key)
    {
        // Allocate max array container size upfront (8KB) to avoid reallocation
        _ctx.Allocate(BitmapContainerSizeInBytes, out ByteString storage);
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

    private void InsertContainerAt(int index, ContainerEntry entry)
    {
        _containers.EnsureCapacityFor(_ctx, 1);

        // Shift elements right
        int count = _containers.Count;
        _containers.Count = count + 1;

        ContainerEntry* items = _containers.RawItems;
        for (int i = count; i > index; i--)
            items[i] = items[i - 1];

        items[index] = entry;
    }

    private void RemoveContainerAt(int index)
    {
        ref ContainerEntry entry = ref _containers[index];
        if (entry.Storage.HasValue)
            _ctx.Release(ref entry.Storage);

        int count = _containers.Count;
        ContainerEntry* items = _containers.RawItems;
        for (int i = index; i < count - 1; i++)
            items[i] = items[i + 1];

        _containers.Count = count - 1;
    }

    /// <summary>
    /// Add a container entry. Used by set operations to build result bitmaps.
    /// </summary>
    internal void AddContainer(ContainerEntry entry)
    {
        if (entry.Cardinality == 0)
            return;

        _containers.EnsureCapacityFor(_ctx, 1);
        _containers.AddUnsafe(entry);
    }

    #endregion

    #region Container Operations

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void AddToContainer(ref ContainerEntry entry, ushort value)
    {
        switch (entry.Type)
        {
            case ContainerType.Array:
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
                NegatedContainerRemove(entry.Data, ref entry.Cardinality, value);
                if (entry.Cardinality <= NegatedArrayMinCardinality)
                    ConvertNegatedToBitmap(ref entry);
                break;

            case ContainerType.Run:
                RunContainerRemove(ref entry, value);
                break;

            case ContainerType.Full:
                ConvertFullToNegated(ref entry);
                NegatedContainerRemove(entry.Data, ref entry.Cardinality, value);
                if (entry.Cardinality <= NegatedArrayMinCardinality)
                    ConvertNegatedToBitmap(ref entry);
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

    internal static int RunContainerCardinality(byte* data)
    {
        ushort* runs = (ushort*)data;
        int numRuns = runs[0];
        int cardinality = 0;

        for (int i = 0; i < numRuns; i++)
            cardinality += runs[1 + i * 2 + 1] + 1;

        return cardinality;
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
        _ctx.Allocate(BitmapContainerSizeInBytes, out ByteString newStorage);
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
        int bytes = Math.Max(64, maxCardinality * sizeof(ushort));
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
        for (int i = 0; i < _containers.Count; i++)
        {
            ref ContainerEntry entry = ref _containers[i];
            if (entry.Storage.HasValue)
                _ctx.Release(ref entry.Storage);
        }

        _containers.Dispose(_ctx);
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
    public byte* Data;          // pointer to container data
    public int Cardinality;     // number of set bits in this container
    public ContainerType Type;  // container type
    internal ByteString Storage; // memory handle for disposal
}
