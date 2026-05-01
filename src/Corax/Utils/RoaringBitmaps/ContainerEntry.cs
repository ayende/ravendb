using System.Runtime.CompilerServices;
using Sparrow.Server;

namespace Corax.Utils.RoaringBitmaps;

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
    /// Access container data as an ushort array.
    /// </summary>
    public ushort* ArrayData
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
