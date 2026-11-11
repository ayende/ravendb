using System;
using System.Numerics.Tensors;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow;
using Voron.Data.Containers;
using Voron.Data.Lookups;
using Voron.Global;
using Voron.Impl;

namespace Voron.Data.Graphs;

public unsafe partial class Centroids
{
    // Block: [Count:int][Reserved:int][DeadMask:ulong] VectorIds[S] EntryHints[S] Lo[S] Step[S] Codes[S * CodeSizeBytes]
    internal const int BlockHeaderSize = 16;
    internal const int SlotOverheadSize = sizeof(long) + sizeof(long) + sizeof(float) + sizeof(float);

    // Router chunk: [Count:int][Reserved:int] PartitionIds[R] Scales[R] NormsSquared[R] Rows[R * Dimensions]
    internal const int RouterChunkHeaderSize = 8;

    internal const int PartitionFlagUnsplittable = 1;

    internal static int RouterRowSize(int dimensions) => sizeof(long) + sizeof(float) + sizeof(float) + dimensions;

    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    internal struct BlockHeader
    {
        public int Count;
        public int Reserved;
        public ulong DeadMask;
    }

    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    internal struct PartitionHeader
    {
        public long PartitionId;
        public long CentroidId;
        public int Count;
        public int Slots;
        public int RouterRow;
        public int BlockCount;
        public int BlockCapacity;
        public int Flags;

        public static int SizeFor(int blockCapacity) => sizeof(PartitionHeader) + blockCapacity * sizeof(long);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static long* PartitionBlocks(PartitionHeader* header) => (long*)(header + 1);

    internal readonly struct BlockLayout
    {
        public readonly int SlotsPerBlock;
        public readonly int CodeSizeBytes;

        public BlockLayout(in Options options)
        {
            SlotsPerBlock = options.SlotsPerBlock;
            CodeSizeBytes = options.CodeSizeBytes;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long* VectorIds(byte* block) => (long*)(block + BlockHeaderSize);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long* EntryHints(byte* block) => (long*)(block + BlockHeaderSize + sizeof(long) * SlotsPerBlock);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public float* Lo(byte* block) => (float*)(block + BlockHeaderSize + 2 * sizeof(long) * SlotsPerBlock);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public float* Step(byte* block) => (float*)(block + BlockHeaderSize + (2 * sizeof(long) + sizeof(float)) * SlotsPerBlock);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public byte* Code(byte* block, int slot) => block + BlockHeaderSize + SlotOverheadSize * SlotsPerBlock + slot * CodeSizeBytes;
    }

    internal readonly struct RouterLayout
    {
        public readonly int RowsPerChunk;
        public readonly int Dimensions;

        public RouterLayout(in Options options)
        {
            RowsPerChunk = options.RouterRowsPerChunk;
            Dimensions = options.Dimensions;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public long* PartitionIds(byte* chunk) => (long*)(chunk + RouterChunkHeaderSize);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public float* Scales(byte* chunk) => (float*)(chunk + RouterChunkHeaderSize + sizeof(long) * RowsPerChunk);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public float* NormsSquared(byte* chunk) => (float*)(chunk + RouterChunkHeaderSize + (sizeof(long) + sizeof(float)) * RowsPerChunk);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public sbyte* Row(byte* chunk, int row) => (sbyte*)(chunk + RouterChunkHeaderSize + (sizeof(long) + 2 * sizeof(float)) * RowsPerChunk + row * Dimensions);
    }

    /// <summary>
    /// Scores every routing centroid against a query, the higher the score, the nearer the partition.
    /// The score is <c>q·c - |c|^2 / 2</c>, which orders partitions by the L2 distance between the query and the centroid.
    /// </summary>
    internal static void ScoreRouter(LowLevelTransaction llt, Lookup<Int64LookupKey> routerChunks, in Options options, sbyte* query, float queryScale,
        Span<float> scores, Span<long> partitionIds)
    {
        var layout = new RouterLayout(options);
        int rows = options.NumberOfPartitions;
        int dims = options.Dimensions;
        int row = 0;
        for (long chunkNumber = 0; row < rows; chunkNumber++)
        {
            routerChunks.TryGetValue(chunkNumber, out var chunkId);
            Container.Get(llt, new ContainerEntryId(chunkId), out var item);
            var chunk = item.Address;
            var count = Math.Min(layout.RowsPerChunk, rows - row);
            var ids = layout.PartitionIds(chunk);
            var scales = layout.Scales(chunk);
            var norms = layout.NormsSquared(chunk);
            for (int i = 0; i < count; i++, row++)
            {
                var dot = Kernels.DotI8(query, layout.Row(chunk, i), dims);
                scores[row] = dot * queryScale * scales[i] - 0.5f * norms[i];
                partitionIds[row] = ids[i];
            }
        }
    }

    internal static VectorEmbeddingType GetEmbeddingType(in Options options) => (VectorEmbeddingType)options.EmbeddingType;

    /// <summary>
    /// Converts a vector in its stored form to the unit length float representation used for routing and quantization.
    /// </summary>
    internal static void ToWorkingVector(in Options options, ReadOnlySpan<byte> stored, Span<float> destination)
    {
        int dims = options.Dimensions;
        switch ((VectorEmbeddingType)options.EmbeddingType)
        {
            case VectorEmbeddingType.Single:
                MemoryMarshal.Cast<byte, float>(stored)[..dims].CopyTo(destination);
                return;
            case VectorEmbeddingType.Int8:
                var values = MemoryMarshal.Cast<byte, sbyte>(stored);
                for (int i = 0; i < dims; i++)
                    destination[i] = values[i];
                break;
            default:
                for (int i = 0; i < dims; i++)
                    destination[i] = ((stored[i >> 3] >> (7 - (i & 7))) & 1) == 1 ? 1f : -1f;
                break;
        }

        var norm = TensorPrimitives.Norm(destination[..dims]);
        if (norm > 0)
            TensorPrimitives.Divide(destination[..dims], norm, destination[..dims]);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static long EntryHintFor(long postingList)
    {
        return (postingList & Constants.Graphs.VectorId.EnsureIsSingleMask) == Constants.Graphs.VectorId.Single
            ? Hnsw.Registration.InternalEntryIdToEntryId(postingList & Constants.Graphs.VectorId.ContainerType)
            : 0;
    }
}
