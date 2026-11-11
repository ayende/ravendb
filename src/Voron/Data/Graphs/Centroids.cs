using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow.Server;
using Voron.Data.BTrees;
using Voron.Data.Containers;
using Voron.Data.Lookups;
using Voron.Global;
using Voron.Impl;

namespace Voron.Data.Graphs;

/// <summary>
/// Clustering based vector index. The vectors of a field are split into partitions, each with a routing centroid.
/// A partition stores, per vector, a scalar quantized residual against its centroid. A query ranks the centroids,
/// scans the codes of the nearest partitions and reranks the best candidates using the full precision vectors.
/// Partitions are kept balanced incrementally: an oversized partition is split in two and the vectors near the
/// split are reassigned to their nearest partition, an undersized one is dissolved into its neighbors.
/// </summary>
public unsafe partial class Centroids
{
    public const int DefaultMaxPartitionSize = 768;
    public const int DefaultQuantizationBits = 4;

    internal const ushort CurrentVersion = 1;
    internal const int MaxSlotsPerBlock = 64;
    internal const int RouterChunkTargetSizeInBytes = 48 * 1024;
    internal const int NeighborsToReassign = 16;

    internal static readonly Slice OptionsSlice;
    internal static readonly Slice NodePostingsSlice;
    internal static readonly Slice NodePartitionsSlice;
    internal static readonly Slice PartitionsSlice;
    internal static readonly Slice RouterChunksSlice;

    static Centroids()
    {
        using (StorageEnvironment.GetStaticContext(out var ctx))
        {
            Slice.From(ctx, "CentroidsOptions", ByteStringType.Immutable, out OptionsSlice);
            Slice.From(ctx, "NodePostings", ByteStringType.Immutable, out NodePostingsSlice);
            Slice.From(ctx, "NodePartitions", ByteStringType.Immutable, out NodePartitionsSlice);
            Slice.From(ctx, "Partitions", ByteStringType.Immutable, out PartitionsSlice);
            Slice.From(ctx, "RouterChunks", ByteStringType.Immutable, out RouterChunksSlice);
        }
    }

    [StructLayout(LayoutKind.Explicit, Pack = 1, Size = 128)]
    public struct Options
    {
        [FieldOffset(0)]
        public int VectorSizeBytes;

        [FieldOffset(4)]
        public int Dimensions;

        [FieldOffset(8)]
        public ushort Version;

        [FieldOffset(10)]
        public Hnsw.SimilarityMethod SimilarityMethod;

        [FieldOffset(11)]
        public byte QuantizationBits;

        [FieldOffset(12)]
        public byte VectorBatchIndex;

        [FieldOffset(13)]
        public byte EmbeddingType;

        [FieldOffset(14)]
        public ushort SlotsPerBlock;

        [FieldOffset(16)]
        public long CountOfVectors;

        [FieldOffset(24)]
        public ContainerId Container;

        [FieldOffset(32)]
        public ContainerId BlocksContainer;

        [FieldOffset(40)]
        public ContainerId LastUsedContainerId;

        [FieldOffset(48)]
        public long NextPartitionId;

        [FieldOffset(56)]
        public int NumberOfPartitions;

        [FieldOffset(60)]
        public int MaxPartitionSize;

        [FieldOffset(64)]
        public int MinPartitionSize;

        [FieldOffset(68)]
        public int CodeSizeBytes;

        [FieldOffset(72)]
        public int BlockSizeBytes;

        [FieldOffset(76)]
        public int RouterRowsPerChunk;

        [FieldOffset(80)]
        public int RouterChunkSizeBytes;

        [FieldOffset(84)]
        public int CentroidSizeBytes;

        [FieldOffset(88)]
        public long NumberOfSplits;

        [FieldOffset(96)]
        public long NumberOfMerges;

        [FieldOffset(104)]
        public long NumberOfReassignedVectors;

        public VectorEmbeddingType VectorEmbeddingType => (VectorEmbeddingType)EmbeddingType;

        internal Hnsw.Options AsHnswOptions() => new()
        {
            VectorSizeBytes = VectorSizeBytes,
            SimilarityMethod = SimilarityMethod,
            Version = Constants.Graphs.HnswVersion.CurrentVersion
        };
    }

    public static void Create(LowLevelTransaction llt, string name, int vectorSizeBytes, VectorEmbeddingType embeddingType,
        int maxPartitionSize = DefaultMaxPartitionSize, int quantizationBits = DefaultQuantizationBits)
    {
        using var _ = Slice.From(llt.Allocator, name, out var slice);
        Create(llt, slice, vectorSizeBytes, embeddingType, maxPartitionSize, quantizationBits);
    }

    public static void Create(LowLevelTransaction llt, Slice name, int vectorSizeBytes, VectorEmbeddingType embeddingType,
        int maxPartitionSize = DefaultMaxPartitionSize, int quantizationBits = DefaultQuantizationBits)
    {
        var tree = llt.Transaction.CreateTree(name);
        ref readonly var stateHeader = ref tree.ReadHeader();
        if (stateHeader.NumberOfEntries is not 0)
            return;

        if (quantizationBits is not (4 or 8))
            throw new ArgumentOutOfRangeException(nameof(quantizationBits), quantizationBits, "Only 4 or 8 bits quantization is supported.");
        if (maxPartitionSize < 16)
            throw new ArgumentOutOfRangeException(nameof(maxPartitionSize), maxPartitionSize, "Partition size must be at least 16.");

        Hnsw.CreateHnswGlobalState(llt);

        var similarityMethod = embeddingType switch
        {
            VectorEmbeddingType.Single => Hnsw.SimilarityMethod.CosineSimilaritySingles,
            VectorEmbeddingType.Int8 => Hnsw.SimilarityMethod.CosineSimilarityI8,
            VectorEmbeddingType.Binary => Hnsw.SimilarityMethod.HammingDistance,
            _ => throw new InvalidOperationException($"Unexpected value of {nameof(VectorEmbeddingType)}: {embeddingType}")
        };

        int dimensions;
        switch (embeddingType)
        {
            case VectorEmbeddingType.Single:
                dimensions = vectorSizeBytes / sizeof(float);
                vectorSizeBytes += Hnsw.MagnitudeSizeInBytes;
                break;
            case VectorEmbeddingType.Int8:
                dimensions = vectorSizeBytes - sizeof(float);
                break;
            default:
                dimensions = vectorSizeBytes * 8;
                break;
        }

        var codeSize = CodeSizeFor(dimensions, ref quantizationBits);
        var slotsPerBlock = Math.Min(MaxSlotsPerBlock, (Container.MaxSizeInsideContainerPage - BlockHeaderSize) / (SlotOverheadSize + codeSize));
        if (slotsPerBlock < 1)
            throw new NotSupportedException($"Vectors with {dimensions} dimensions are too big for the centroids index.");

        var rowSize = RouterRowSize(dimensions);
        var rowsPerChunk = Math.Max(1, RouterChunkTargetSizeInBytes / rowSize);

        tree.LookupFor<Int64LookupKey>(NodePostingsSlice);
        tree.LookupFor<Int64LookupKey>(NodePartitionsSlice);
        tree.LookupFor<Int64LookupKey>(PartitionsSlice);
        tree.LookupFor<Int64LookupKey>(RouterChunksSlice);

        var options = new Options
        {
            Version = CurrentVersion,
            VectorSizeBytes = vectorSizeBytes,
            Dimensions = dimensions,
            SimilarityMethod = similarityMethod,
            EmbeddingType = (byte)embeddingType,
            QuantizationBits = (byte)quantizationBits,
            Container = Container.Create(llt),
            BlocksContainer = Container.Create(llt),
            NextPartitionId = 1,
            MaxPartitionSize = maxPartitionSize,
            MinPartitionSize = Math.Max(1, maxPartitionSize / 8),
            CodeSizeBytes = codeSize,
            SlotsPerBlock = (ushort)slotsPerBlock,
            BlockSizeBytes = BlockHeaderSize + slotsPerBlock * (SlotOverheadSize + codeSize),
            RouterRowsPerChunk = rowsPerChunk,
            RouterChunkSizeBytes = RouterChunkHeaderSize + rowsPerChunk * rowSize,
            CentroidSizeBytes = (dimensions + 1) * sizeof(float),
        };

        using (tree.DirectAdd(OptionsSlice, sizeof(Options), out var output))
        {
            Unsafe.Write(output, options);
        }
    }

    private static int CodeSizeFor(int dimensions, ref int quantizationBits)
    {
        if (quantizationBits == 8 && SlotOverheadSize + dimensions > Container.MaxSizeInsideContainerPage - BlockHeaderSize)
            quantizationBits = 4;
        return quantizationBits == 8 ? dimensions : (dimensions + 1) / 2;
    }

    public static bool IsCentroidsIndex(LowLevelTransaction llt, Slice name)
    {
        var tree = llt.Transaction.ReadTree(name);
        return tree is not null && tree.DirectRead(OptionsSlice) is not null;
    }

    public static bool TryReadOptions(LowLevelTransaction llt, Slice name, out Options options, out Tree tree)
    {
        tree = llt.Transaction.ReadTree(name);
        var ptr = tree?.DirectRead(OptionsSlice);
        if (ptr is null)
        {
            options = default;
            return false;
        }

        options = Unsafe.Read<Options>(ptr);
        return true;
    }

    public static Registration RegistrationFor(LowLevelTransaction llt, string name)
    {
        Slice.From(llt.Allocator, name, out var slice);
        return RegistrationFor(llt, slice);
    }

    public static Registration RegistrationFor(LowLevelTransaction llt, Slice name) => new(llt, name);
}
