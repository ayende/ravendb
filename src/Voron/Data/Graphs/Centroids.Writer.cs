using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Numerics.Tensors;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Sparrow;
using Voron.Data.BTrees;
using Voron.Data.Containers;
using Voron.Data.Lookups;
using Voron.Impl;

namespace Voron.Data.Graphs;

public unsafe partial class Centroids
{
    internal sealed class Writer : IDisposable
    {
        private const int RoutingCandidates = 4;
        private const int ParallelRoutingThreshold = 1 << 20;
        private const int SplitIterations = 8;

        public readonly LowLevelTransaction Llt;
        private readonly Tree _tree;
        public Options Options;
        private readonly Lookup<Int64LookupKey> _nodePostings;
        private readonly Lookup<Int64LookupKey> _nodePartitions;
        private readonly Lookup<Int64LookupKey> _partitions;
        private readonly Lookup<Int64LookupKey> _routerChunks;
        private readonly BlockLayout _block;
        private readonly RouterLayout _router;
        private readonly int _dims;
        private readonly Dictionary<long, float[]> _centroids = new();
        private readonly HashSet<long> _touched = new();
        private readonly HashSet<long> _shrunk = new();
        private readonly float[] _residual;
        private readonly byte[] _code;
        private readonly sbyte[] _quantized;
        public int MaxDegreeOfParallelism = Environment.ProcessorCount;

        public Writer(LowLevelTransaction llt, Tree tree)
        {
            Llt = llt;
            _tree = tree;
            Options = Unsafe.Read<Options>(tree.DirectRead(OptionsSlice));
            _nodePostings = tree.LookupFor<Int64LookupKey>(NodePostingsSlice);
            _nodePartitions = tree.LookupFor<Int64LookupKey>(NodePartitionsSlice);
            _partitions = tree.LookupFor<Int64LookupKey>(PartitionsSlice);
            _routerChunks = tree.LookupFor<Int64LookupKey>(RouterChunksSlice);
            _block = new BlockLayout(Options);
            _router = new RouterLayout(Options);
            _dims = Options.Dimensions;
            _residual = new float[_dims];
            _code = new byte[Options.CodeSizeBytes];
            _quantized = new sbyte[_dims];
        }

        public Lookup<Int64LookupKey> NodePostings => _nodePostings;

        public void Flush()
        {
            using (_tree.DirectAdd(OptionsSlice, sizeof(Options), out var dst))
            {
                Unsafe.Write(dst, Options);
            }
        }

        public void Dispose()
        {
        }

        public void Insert(ReadOnlySpan<long> vectorIds, ReadOnlySpan<long> postingLists)
        {
            int count = vectorIds.Length;
            if (count == 0)
                return;

            var vectors = ArrayPool<float>.Shared.Rent(count * _dims);
            var targets = ArrayPool<long>.Shared.Rent(count);
            var order = ArrayPool<int>.Shared.Rent(count);
            try
            {
                LoadVectors(vectorIds, vectors);

                if (Options.NumberOfPartitions == 0)
                {
                    var mean = new float[_dims];
                    for (int i = 0; i < count; i++)
                        TensorPrimitives.Add(mean, vectors.AsSpan(i * _dims, _dims), mean);
                    TensorPrimitives.Divide(mean, count, mean);
                    CreatePartition(mean);
                }

                Route(vectors, count, targets, excludedPartition: -1);

                for (int i = 0; i < count; i++)
                    order[i] = i;
                Array.Sort(targets, order, 0, count);

                int start = 0;
                while (start < count)
                {
                    var partitionId = targets[start];
                    int end = start + 1;
                    while (end < count && targets[end] == partitionId)
                        end++;

                    AppendVectors(partitionId, order.AsSpan(start, end - start), vectors, vectorIds, postingLists, hints: default);
                    for (int i = start; i < end; i++)
                    {
                        var idx = order[i];
                        _nodePostings.Add(vectorIds[idx], postingLists[idx]);
                        _nodePartitions.Add(vectorIds[idx], partitionId);
                    }

                    _touched.Add(partitionId);
                    start = end;
                }

                Options.CountOfVectors += count;
            }
            finally
            {
                ArrayPool<float>.Shared.Return(vectors);
                ArrayPool<long>.Shared.Return(targets);
                ArrayPool<int>.Shared.Return(order);
            }
        }

        public void Delete(ReadOnlySpan<long> vectorIds)
        {
            if (vectorIds.Length == 0)
                return;

            var byPartition = new Dictionary<long, HashSet<long>>();
            foreach (var vectorId in vectorIds)
            {
                if (_nodePartitions.TryRemove(vectorId, out var partitionId) == false)
                    continue;
                _nodePostings.TryRemove(vectorId);
                if (byPartition.TryGetValue(partitionId, out var set) == false)
                    byPartition[partitionId] = set = new HashSet<long>();
                set.Add(vectorId);
                Options.CountOfVectors--;
            }

            foreach (var (partitionId, set) in byPartition)
            {
                MarkDead(partitionId, set);
                _touched.Add(partitionId);
                _shrunk.Add(partitionId);
            }
        }

        public void UpdatePostingList(long vectorId, long postingList)
        {
            _nodePostings.Add(vectorId, postingList);
            if (_nodePartitions.TryGetValue(vectorId, out var partitionId) == false)
                return;

            var header = GetHeader(partitionId, out _);
            var blocks = PartitionBlocks(header);
            var hint = EntryHintFor(postingList);
            for (int b = 0; b < header->BlockCount; b++)
            {
                Container.Get(Llt, new ContainerEntryId(blocks[b]), out var item);
                var ids = _block.VectorIds(item.Address);
                var count = ((BlockHeader*)item.Address)->Count;
                for (int s = 0; s < count; s++)
                {
                    if (ids[s] != vectorId)
                        continue;
                    var block = Container.GetMutable(Llt, new ContainerEntryId(blocks[b]));
                    fixed (byte* p = block)
                    {
                        if ((((BlockHeader*)p)->DeadMask & (1UL << s)) != 0)
                            continue;
                        _block.EntryHints(p)[s] = hint;
                    }

                    return;
                }
            }
        }

        public void RunMaintenance()
        {
            while (_touched.Count > 0)
            {
                long partitionId = 0;
                foreach (var id in _touched)
                {
                    partitionId = id;
                    break;
                }

                _touched.Remove(partitionId);

                if (_partitions.TryGetValue(partitionId, out var headerId) == false)
                    continue;

                Container.Get(Llt, new ContainerEntryId(headerId), out var item);
                var header = (PartitionHeader*)item.Address;
                if (header->Count > Options.MaxPartitionSize && (header->Flags & PartitionFlagUnsplittable) == 0)
                {
                    Split(partitionId);
                    continue;
                }

                if (Options.NumberOfPartitions > 1 && (header->Count == 0 || header->Count < Options.MinPartitionSize && _shrunk.Contains(partitionId)))
                {
                    Merge(partitionId);
                    continue;
                }

                if (header->Count == 0)
                {
                    DeletePartition(partitionId);
                    continue;
                }

                var dead = header->Slots - header->Count;
                if (dead > Math.Max(header->Slots / 4, Options.SlotsPerBlock))
                    Compact(partitionId);
            }

            _shrunk.Clear();
        }

        private void LoadVectors(ReadOnlySpan<long> vectorIds, Span<float> destination)
        {
            int count = vectorIds.Length;
            var ids = ArrayPool<long>.Shared.Rent(count);
            var order = ArrayPool<int>.Shared.Rent(count);
            var spans = ArrayPool<UnmanagedSpan>.Shared.Rent(count);
            try
            {
                for (int i = 0; i < count; i++)
                {
                    ids[i] = Hnsw.GetVectorContainerId(vectorIds[i]);
                    order[i] = i;
                }

                Array.Sort(ids, order, 0, count);
                Container.GetAll(Llt, ids.AsSpan(0, count), spans.AsSpan(0, count), -1, Llt.PageLocator);
                for (int k = 0; k < count; k++)
                {
                    var i = order[k];
                    var stored = Hnsw.SliceVector(spans[k], vectorIds[i], Options.VectorSizeBytes);
                    ToWorkingVector(Options, stored.ToReadOnlySpan(), destination.Slice(i * _dims, _dims));
                }
            }
            finally
            {
                ArrayPool<long>.Shared.Return(ids);
                ArrayPool<int>.Shared.Return(order);
                ArrayPool<UnmanagedSpan>.Shared.Return(spans);
            }
        }

        private float[] GetCentroid(long partitionId)
        {
            if (_centroids.TryGetValue(partitionId, out var centroid))
                return centroid;

            var header = GetHeader(partitionId, out _);
            Container.Get(Llt, new ContainerEntryId(header->CentroidId), out var item);
            centroid = new float[_dims + 1];
            new ReadOnlySpan<float>(item.Address, _dims + 1).CopyTo(centroid);
            _centroids[partitionId] = centroid;
            return centroid;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static float Score(ReadOnlySpan<float> vector, float[] centroid, int dims)
        {
            return TensorPrimitives.Dot(vector, centroid.AsSpan(0, dims)) - 0.5f * centroid[dims];
        }

        private PartitionHeader* GetHeader(long partitionId, out long headerId)
        {
            if (_partitions.TryGetValue(partitionId, out headerId) == false)
                throw new InvalidOperationException($"Partition {partitionId} does not exist");
            Container.Get(Llt, new ContainerEntryId(headerId), out var item);
            return (PartitionHeader*)item.Address;
        }

        private PartitionHeader* GetMutableHeader(long partitionId, out long headerId)
        {
            if (_partitions.TryGetValue(partitionId, out headerId) == false)
                throw new InvalidOperationException($"Partition {partitionId} does not exist");
            fixed (byte* p = Container.GetMutable(Llt, new ContainerEntryId(headerId)))
                return (PartitionHeader*)p;
        }

        private PartitionHeader* EnsureBlockCapacity(long partitionId, PartitionHeader* header, int additionalSlots)
        {
            var tailFree = 0;
            if (header->BlockCount > 0)
            {
                Container.Get(Llt, new ContainerEntryId(PartitionBlocks(header)[header->BlockCount - 1]), out var tail);
                tailFree = Options.SlotsPerBlock - ((BlockHeader*)tail.Address)->Count;
            }

            var requiredBlocks = header->BlockCount + (Math.Max(0, additionalSlots - tailFree) + Options.SlotsPerBlock - 1) / Options.SlotsPerBlock;
            if (requiredBlocks <= header->BlockCapacity)
                return header;

            var capacity = Math.Max(requiredBlocks, header->BlockCapacity * 2);
            var copySize = PartitionHeader.SizeFor(header->BlockCount);
            var copy = ArrayPool<byte>.Shared.Rent(copySize);
            try
            {
                fixed (byte* c = copy)
                    Unsafe.CopyBlock(c, header, (uint)copySize);

                _partitions.TryGetValue(partitionId, out var oldHeaderId);
                Container.Delete(Llt, Options.Container, new ContainerEntryId(oldHeaderId));
                var newHeaderId = Container.Allocate(Llt, Options.Container, PartitionHeader.SizeFor(capacity), out var space);
                fixed (byte* p = space)
                fixed (byte* c = copy)
                {
                    var newHeader = (PartitionHeader*)p;
                    Unsafe.CopyBlock(newHeader, c, (uint)copySize);
                    newHeader->BlockCapacity = capacity;
                    _partitions.Add(partitionId, (long)newHeaderId);
                    return newHeader;
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(copy);
            }
        }

        private long CreatePartition(ReadOnlySpan<float> centroid)
        {
            var partitionId = Options.NextPartitionId++;
            var centroidId = WriteCentroid(centroid, out var stored);
            var capacity = (Options.MaxPartitionSize * 2 + Options.SlotsPerBlock - 1) / Options.SlotsPerBlock + 2;
            var headerId = Container.Allocate(Llt, Options.Container, PartitionHeader.SizeFor(capacity), out var space);
            fixed (byte* p = space)
            {
                var header = (PartitionHeader*)p;
                *header = new PartitionHeader
                {
                    PartitionId = partitionId,
                    CentroidId = centroidId,
                    BlockCapacity = capacity,
                    RouterRow = Options.NumberOfPartitions
                };
            }

            _partitions.Add(partitionId, (long)headerId);
            _centroids[partitionId] = stored;
            AppendRouterRow(partitionId, stored);
            return partitionId;
        }

        private long WriteCentroid(ReadOnlySpan<float> centroid, out float[] stored)
        {
            stored = new float[_dims + 1];
            centroid[.._dims].CopyTo(stored);
            stored[_dims] = TensorPrimitives.SumOfSquares(centroid[.._dims]);
            var id = Container.Allocate(Llt, Options.Container, Options.CentroidSizeBytes, out var space);
            stored.AsSpan().CopyTo(System.Runtime.InteropServices.MemoryMarshal.Cast<byte, float>(space));
            return (long)id;
        }

        private void SetCentroid(long partitionId, ReadOnlySpan<float> centroid)
        {
            var header = GetMutableHeader(partitionId, out _);
            Container.Delete(Llt, Options.Container, new ContainerEntryId(header->CentroidId));
            var centroidId = WriteCentroid(centroid, out var stored);
            header = GetMutableHeader(partitionId, out _);
            header->CentroidId = centroidId;
            _centroids[partitionId] = stored;
            WriteRouterRow(header->RouterRow, partitionId, stored);
        }

        private void DeletePartition(long partitionId)
        {
            var header = GetHeader(partitionId, out var headerId);
            var blocks = PartitionBlocks(header);
            for (int b = 0; b < header->BlockCount; b++)
                Container.Delete(Llt, Options.BlocksContainer, new ContainerEntryId(blocks[b]));
            var row = header->RouterRow;
            Container.Delete(Llt, Options.Container, new ContainerEntryId(header->CentroidId));
            Container.Delete(Llt, Options.Container, new ContainerEntryId(headerId));
            _partitions.TryRemove(partitionId);
            _centroids.Remove(partitionId);
            RemoveRouterRow(row);
        }

        private byte* GetMutableRouterChunk(int row)
        {
            var chunkNumber = row / _router.RowsPerChunk;
            if (_routerChunks.TryGetValue(chunkNumber, out var chunkId) == false)
            {
                var id = Container.Allocate(Llt, Options.Container, Options.RouterChunkSizeBytes, out var space);
                space.Clear();
                _routerChunks.Add(chunkNumber, (long)id);
                fixed (byte* p = space)
                    return p;
            }

            fixed (byte* p = Container.GetMutable(Llt, new ContainerEntryId(chunkId)))
                return p;
        }

        private void AppendRouterRow(long partitionId, float[] centroid)
        {
            var row = Options.NumberOfPartitions++;
            WriteRouterRow(row, partitionId, centroid);
        }

        private void WriteRouterRow(int row, long partitionId, float[] centroid)
        {
            var chunk = GetMutableRouterChunk(row);
            var index = row % _router.RowsPerChunk;
            _router.PartitionIds(chunk)[index] = partitionId;
            _router.Scales(chunk)[index] = Kernels.QuantizeI8(centroid.AsSpan(0, _dims), _router.Row(chunk, index), Kernels.RouterQueryRange);
            _router.NormsSquared(chunk)[index] = centroid[_dims];
        }

        private void RemoveRouterRow(int row)
        {
            var last = --Options.NumberOfPartitions;
            if (row != last)
            {
                var lastChunkNumber = last / _router.RowsPerChunk;
                _routerChunks.TryGetValue(lastChunkNumber, out var lastChunkId);
                Container.Get(Llt, new ContainerEntryId(lastChunkId), out var lastItem);
                var src = lastItem.Address;
                var srcIndex = last % _router.RowsPerChunk;
                var movedPartitionId = _router.PartitionIds(src)[srcIndex];
                var scale = _router.Scales(src)[srcIndex];
                var norm = _router.NormsSquared(src)[srcIndex];
                var rowCopy = stackalloc sbyte[_dims];
                Unsafe.CopyBlock(rowCopy, _router.Row(src, srcIndex), (uint)_dims);

                var dst = GetMutableRouterChunk(row);
                var dstIndex = row % _router.RowsPerChunk;
                _router.PartitionIds(dst)[dstIndex] = movedPartitionId;
                _router.Scales(dst)[dstIndex] = scale;
                _router.NormsSquared(dst)[dstIndex] = norm;
                Unsafe.CopyBlock(_router.Row(dst, dstIndex), rowCopy, (uint)_dims);

                GetMutableHeader(movedPartitionId, out _)->RouterRow = row;
            }

            if (last % _router.RowsPerChunk == 0 && _routerChunks.TryRemove(last / _router.RowsPerChunk, out var chunkId))
                Container.Delete(Llt, Options.Container, new ContainerEntryId(chunkId));
        }

        /// <summary>
        /// Finds the nearest partition of each vector: the router ranks all partitions with int8 centroids, then the best few
        /// candidates are compared with the full precision centroids.
        /// </summary>
        private void Route(float[] vectors, int count, long[] targets, long excludedPartition)
        {
            int partitions = Options.NumberOfPartitions;
            if (partitions <= RoutingCandidates * 2)
            {
                RouteExact(vectors, count, targets, excludedPartition);
                return;
            }

            var candidates = ArrayPool<long>.Shared.Rent(count * RoutingCandidates);
            try
            {
                var chunks = new long[(partitions + _router.RowsPerChunk - 1) / _router.RowsPerChunk];
                for (int c = 0; c < chunks.Length; c++)
                {
                    _routerChunks.TryGetValue(c, out var chunkId);
                    Container.Get(Llt, new ContainerEntryId(chunkId), out var item);
                    chunks[c] = (long)item.Address;
                }

                var router = _router;
                var dims = _dims;
                void RouteOne(int i)
                {
                    var query = stackalloc sbyte[dims];
                    var queryScale = Kernels.QuantizeI8(new ReadOnlySpan<float>(vectors, i * dims, dims), query, Kernels.RouterQueryRange);
                    var bestScores = stackalloc float[RoutingCandidates];
                    var bestIds = new Span<long>(candidates, i * RoutingCandidates, RoutingCandidates);
                    for (int k = 0; k < RoutingCandidates; k++)
                    {
                        bestScores[k] = float.NegativeInfinity;
                        bestIds[k] = -1;
                    }

                    int row = 0;
                    for (int c = 0; c < chunks.Length; c++)
                    {
                        var chunk = (byte*)chunks[c];
                        var rows = Math.Min(router.RowsPerChunk, partitions - row);
                        var ids = router.PartitionIds(chunk);
                        var scales = router.Scales(chunk);
                        var norms = router.NormsSquared(chunk);
                        for (int r = 0; r < rows; r++, row++)
                        {
                            if (ids[r] == excludedPartition)
                                continue;
                            var score = Kernels.DotI8(query, router.Row(chunk, r), dims) * queryScale * scales[r] - 0.5f * norms[r];
                            if (score <= bestScores[RoutingCandidates - 1])
                                continue;
                            int pos = RoutingCandidates - 1;
                            while (pos > 0 && bestScores[pos - 1] < score)
                            {
                                bestScores[pos] = bestScores[pos - 1];
                                bestIds[pos] = bestIds[pos - 1];
                                pos--;
                            }

                            bestScores[pos] = score;
                            bestIds[pos] = ids[r];
                        }
                    }
                }

                if ((long)count * partitions >= ParallelRoutingThreshold && MaxDegreeOfParallelism > 1)
                    Parallel.For(0, count, new ParallelOptions { MaxDegreeOfParallelism = MaxDegreeOfParallelism }, RouteOne);
                else
                    for (int i = 0; i < count; i++)
                        RouteOne(i);

                for (int i = 0; i < count; i++)
                {
                    var vector = new ReadOnlySpan<float>(vectors, i * _dims, _dims);
                    var best = float.NegativeInfinity;
                    long target = -1;
                    for (int k = 0; k < RoutingCandidates; k++)
                    {
                        var partitionId = candidates[i * RoutingCandidates + k];
                        if (partitionId < 0)
                            continue;
                        var score = Score(vector, GetCentroid(partitionId), _dims);
                        if (score <= best)
                            continue;
                        best = score;
                        target = partitionId;
                    }

                    targets[i] = target;
                }
            }
            finally
            {
                ArrayPool<long>.Shared.Return(candidates);
            }
        }

        private void RouteExact(float[] vectors, int count, long[] targets, long excludedPartition)
        {
            var partitionIds = new long[Options.NumberOfPartitions];
            var scores = new float[Options.NumberOfPartitions];
            fixed (sbyte* query = _quantized)
            {
                ScoreRouter(Llt, _routerChunks, Options, query, 0, scores, partitionIds);
            }

            for (int i = 0; i < count; i++)
            {
                var vector = new ReadOnlySpan<float>(vectors, i * _dims, _dims);
                var best = float.NegativeInfinity;
                long target = -1;
                foreach (var partitionId in partitionIds)
                {
                    if (partitionId == excludedPartition)
                        continue;
                    var score = Score(vector, GetCentroid(partitionId), _dims);
                    if (score <= best)
                        continue;
                    best = score;
                    target = partitionId;
                }

                targets[i] = target;
            }
        }

        private void NearestPartitions(ReadOnlySpan<float> vector, int count, long excludedPartition, List<long> result)
        {
            result.Clear();
            int partitions = Options.NumberOfPartitions;
            var scores = ArrayPool<float>.Shared.Rent(partitions);
            var ids = ArrayPool<long>.Shared.Rent(partitions);
            try
            {
                fixed (sbyte* query = _quantized)
                {
                    var scale = Kernels.QuantizeI8(vector, query, Kernels.RouterQueryRange);
                    ScoreRouter(Llt, _routerChunks, Options, query, scale, scores, ids);
                }

                for (int i = 0; i < partitions; i++)
                    scores[i] = -scores[i];
                Array.Sort(scores, ids, 0, partitions);
                for (int i = 0; i < partitions && result.Count < count; i++)
                {
                    if (ids[i] != excludedPartition)
                        result.Add(ids[i]);
                }
            }
            finally
            {
                ArrayPool<float>.Shared.Return(scores);
                ArrayPool<long>.Shared.Return(ids);
            }
        }

        private void AppendVectors(long partitionId, ReadOnlySpan<int> indexes, float[] vectors, ReadOnlySpan<long> vectorIds, ReadOnlySpan<long> postingLists, ReadOnlySpan<long> hints)
        {
            var centroid = GetCentroid(partitionId).AsSpan(0, _dims);
            var header = GetMutableHeader(partitionId, out _);
            header = EnsureBlockCapacity(partitionId, header, indexes.Length);
            var blocks = PartitionBlocks(header);
            byte* tail = null;
            if (header->BlockCount > 0)
            {
                fixed (byte* p = Container.GetMutable(Llt, new ContainerEntryId(blocks[header->BlockCount - 1])))
                    tail = p;
                if (((BlockHeader*)tail)->Count == Options.SlotsPerBlock)
                    tail = null;
            }

            int bits = Options.QuantizationBits;
            foreach (var idx in indexes)
            {
                if (tail == null)
                {
                    var blockId = Container.Allocate(Llt, Options.BlocksContainer, Options.BlockSizeBytes, out var space);
                    space[..BlockHeaderSize].Clear();
                    blocks[header->BlockCount++] = (long)blockId;
                    fixed (byte* p = space)
                        tail = p;
                }

                var slot = ((BlockHeader*)tail)->Count++;
                _block.VectorIds(tail)[slot] = vectorIds[idx];
                _block.EntryHints(tail)[slot] = hints.IsEmpty ? EntryHintFor(postingLists[idx]) : hints[idx];
                Kernels.Encode(new ReadOnlySpan<float>(vectors, idx * _dims, _dims), centroid, _residual, bits, _block.Code(tail, slot), out var lo, out var step);
                _block.Lo(tail)[slot] = lo;
                _block.Step(tail)[slot] = step;
                header->Count++;
                header->Slots++;

                if (slot + 1 == Options.SlotsPerBlock)
                    tail = null;
            }
        }

        private void MarkDead(long partitionId, HashSet<long> vectorIds)
        {
            var header = GetMutableHeader(partitionId, out _);
            var blocks = PartitionBlocks(header);
            int remaining = vectorIds.Count;
            for (int b = 0; b < header->BlockCount && remaining > 0; b++)
            {
                Container.Get(Llt, new ContainerEntryId(blocks[b]), out var item);
                var ids = _block.VectorIds(item.Address);
                var blockHeader = (BlockHeader*)item.Address;
                byte* mutable = null;
                for (int s = 0; s < blockHeader->Count; s++)
                {
                    if ((blockHeader->DeadMask & (1UL << s)) != 0 || vectorIds.Contains(ids[s]) == false)
                        continue;
                    if (mutable == null)
                    {
                        fixed (byte* p = Container.GetMutable(Llt, new ContainerEntryId(blocks[b])))
                            mutable = p;
                        blockHeader = (BlockHeader*)mutable;
                        ids = _block.VectorIds(mutable);
                    }

                    blockHeader->DeadMask |= 1UL << s;
                    header->Count--;
                    remaining--;
                }
            }
        }

        /// <summary>
        /// Evaluates, directly on a code, how much nearer the encoded vector is to <c>target</c> than to the centroid <c>own</c>
        /// the code is encoded against: <c>x·(target - own) - (|target|^2 - |own|^2) / 2</c>, with
        /// <c>x = own + lo + step * code</c> and <c>target - own</c> quantized to int8.
        /// </summary>
        private readonly struct CodeProjection
        {
            public readonly sbyte[] Query;
            private readonly float _scale;
            private readonly float _sum;
            private readonly float _offset;

            public CodeProjection(float[] target, float[] own, int dims, int bits)
            {
                var half = (dims + 1) / 2;
                var difference = new float[bits == 8 ? dims : half * 2];
                TensorPrimitives.Subtract(target.AsSpan(0, dims), own.AsSpan(0, dims), difference.AsSpan(0, dims));
                Query = new sbyte[difference.Length];
                fixed (sbyte* q = Query)
                    _scale = Kernels.QuantizeI8(difference, q, bits == 8 ? Kernels.Code8QueryRange : Kernels.Code4QueryRange);
                _sum = TensorPrimitives.Sum(difference.AsSpan(0, dims));
                _offset = TensorPrimitives.Dot(own.AsSpan(0, dims), difference.AsSpan(0, dims)) - 0.5f * (target[dims] - own[dims]);
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public float Gain(float lo, float step, int dot) => _offset + lo * _sum + step * _scale * dot;
        }

        private struct Member
        {
            public long VectorId;
            public long Hint;
        }

        private void ReadMembers(PartitionHeader* header, List<Member> members)
        {
            members.Clear();
            var blocks = PartitionBlocks(header);
            for (int b = 0; b < header->BlockCount; b++)
            {
                Container.Get(Llt, new ContainerEntryId(blocks[b]), out var item);
                var block = item.Address;
                var blockHeader = (BlockHeader*)block;
                var ids = _block.VectorIds(block);
                var hints = _block.EntryHints(block);
                for (int s = 0; s < blockHeader->Count; s++)
                {
                    if ((blockHeader->DeadMask & (1UL << s)) != 0)
                        continue;
                    members.Add(new Member { VectorId = ids[s], Hint = hints[s] });
                }
            }
        }

        private void ClearPartition(long partitionId)
        {
            var header = GetMutableHeader(partitionId, out _);
            var blocks = PartitionBlocks(header);
            for (int b = 0; b < header->BlockCount; b++)
                Container.Delete(Llt, Options.BlocksContainer, new ContainerEntryId(blocks[b]));
            header->BlockCount = 0;
            header->Count = 0;
            header->Slots = 0;
        }

        private void Compact(long partitionId)
        {
            var header = GetHeader(partitionId, out _);
            var live = new List<(long VectorId, long Hint, float Lo, float Step, byte[] Code)>(header->Count);
            var blocks = PartitionBlocks(header);
            for (int b = 0; b < header->BlockCount; b++)
            {
                Container.Get(Llt, new ContainerEntryId(blocks[b]), out var item);
                var block = item.Address;
                var blockHeader = (BlockHeader*)block;
                for (int s = 0; s < blockHeader->Count; s++)
                {
                    if ((blockHeader->DeadMask & (1UL << s)) != 0)
                        continue;
                    var code = new byte[Options.CodeSizeBytes];
                    new ReadOnlySpan<byte>(_block.Code(block, s), Options.CodeSizeBytes).CopyTo(code);
                    live.Add((_block.VectorIds(block)[s], _block.EntryHints(block)[s], _block.Lo(block)[s], _block.Step(block)[s], code));
                }
            }

            ClearPartition(partitionId);
            header = GetMutableHeader(partitionId, out _);
            header = EnsureBlockCapacity(partitionId, header, live.Count);
            var headerBlocks = PartitionBlocks(header);
            byte* tail = null;
            foreach (var slotData in live)
            {
                if (tail == null)
                {
                    var blockId = Container.Allocate(Llt, Options.BlocksContainer, Options.BlockSizeBytes, out var space);
                    space[..BlockHeaderSize].Clear();
                    headerBlocks[header->BlockCount++] = (long)blockId;
                    fixed (byte* p = space)
                        tail = p;
                }

                var slot = ((BlockHeader*)tail)->Count++;
                _block.VectorIds(tail)[slot] = slotData.VectorId;
                _block.EntryHints(tail)[slot] = slotData.Hint;
                _block.Lo(tail)[slot] = slotData.Lo;
                _block.Step(tail)[slot] = slotData.Step;
                slotData.Code.AsSpan().CopyTo(new Span<byte>(_block.Code(tail, slot), Options.CodeSizeBytes));
                header->Count++;
                header->Slots++;
                if (slot + 1 == Options.SlotsPerBlock)
                    tail = null;
            }
        }

        private void Merge(long partitionId)
        {
            var members = new List<Member>();
            ReadMembers(GetHeader(partitionId, out _), members);
            if (members.Count > 0)
            {
                var vectorIds = new long[members.Count];
                var hints = new long[members.Count];
                for (int i = 0; i < members.Count; i++)
                {
                    vectorIds[i] = members[i].VectorId;
                    hints[i] = members[i].Hint;
                }

                var vectors = new float[members.Count * _dims];
                LoadVectors(vectorIds, vectors);
                var targets = new long[members.Count];
                Route(vectors, members.Count, targets, excludedPartition: partitionId);
                MoveTo(targets, vectors, vectorIds, hints);
            }

            DeletePartition(partitionId);
            Options.NumberOfMerges++;
        }

        private void MoveTo(long[] targets, float[] vectors, long[] vectorIds, long[] hints)
        {
            int count = targets.Length;
            var order = new int[count];
            for (int i = 0; i < count; i++)
                order[i] = i;
            var sortedTargets = (long[])targets.Clone();
            Array.Sort(sortedTargets, order);
            int start = 0;
            while (start < count)
            {
                var target = sortedTargets[start];
                int end = start + 1;
                while (end < count && sortedTargets[end] == target)
                    end++;
                AppendVectors(target, order.AsSpan(start, end - start), vectors, vectorIds, default, hints);
                for (int i = start; i < end; i++)
                    _nodePartitions.Add(vectorIds[order[i]], target);
                _touched.Add(target);
                start = end;
            }
        }

        /// <summary>
        /// Splits an oversized partition with 2-means, then reassigns the vectors near the split: vectors of the split partition
        /// for which the old centroid was nearer than both new ones may now belong to a neighbor, and vectors of a neighbor may
        /// now be nearer to one of the new centroids than to their own.
        /// </summary>
        private void Split(long partitionId)
        {
            var members = new List<Member>();
            ReadMembers(GetHeader(partitionId, out _), members);
            int count = members.Count;
            var vectorIds = new long[count];
            var hints = new long[count];
            for (int i = 0; i < count; i++)
            {
                vectorIds[i] = members[i].VectorId;
                hints[i] = members[i].Hint;
            }

            var vectors = new float[count * _dims];
            LoadVectors(vectorIds, vectors);

            var oldCentroid = (float[])GetCentroid(partitionId).Clone();
            var sides = new byte[count];
            if (TwoMeans(vectors, count, sides, out var centroidA, out var centroidB) == false)
            {
                GetMutableHeader(partitionId, out _)->Flags |= PartitionFlagUnsplittable;
                return;
            }

            var neighbors = new List<long>();
            NearestPartitions(oldCentroid.AsSpan(0, _dims), NeighborsToReassign, partitionId, neighbors);
            var neighborCentroids = new float[neighbors.Count][];
            for (int n = 0; n < neighbors.Count; n++)
                neighborCentroids[n] = GetCentroid(neighbors[n]);

            var sideA = new List<int>();
            var sideB = new List<int>();
            var moveOutTargets = new List<long>();
            var moveOutIndexes = new List<int>();
            for (int i = 0; i < count; i++)
            {
                var vector = new ReadOnlySpan<float>(vectors, i * _dims, _dims);
                var own = sides[i] == 0 ? centroidA : centroidB;
                var ownScore = Score(vector, own, _dims);
                if (Score(vector, oldCentroid, _dims) >= ownScore)
                {
                    var bestNeighbor = -1;
                    var bestScore = ownScore;
                    for (int n = 0; n < neighbors.Count; n++)
                    {
                        var score = Score(vector, neighborCentroids[n], _dims);
                        if (score <= bestScore)
                            continue;
                        bestScore = score;
                        bestNeighbor = n;
                    }

                    if (bestNeighbor >= 0)
                    {
                        moveOutIndexes.Add(i);
                        moveOutTargets.Add(neighbors[bestNeighbor]);
                        continue;
                    }
                }

                (sides[i] == 0 ? sideA : sideB).Add(i);
            }

            var moveIn = new List<(long VectorId, long Hint, long From)>();
            var half = (_dims + 1) / 2;
            var bits = Options.QuantizationBits;
            for (int n = 0; n < neighbors.Count; n++)
            {
                var neighborCentroid = neighborCentroids[n];
                var towardsA = new CodeProjection(centroidA, neighborCentroid, _dims, bits);
                var towardsB = new CodeProjection(centroidB, neighborCentroid, _dims, bits);
                var header = GetHeader(neighbors[n], out _);
                var blocks = PartitionBlocks(header);
                fixed (sbyte* qa = towardsA.Query, qb = towardsB.Query)
                {
                    for (int b = 0; b < header->BlockCount; b++)
                    {
                        Container.Get(Llt, new ContainerEntryId(blocks[b]), out var item);
                        var block = item.Address;
                        var blockHeader = (BlockHeader*)block;
                        var lo = _block.Lo(block);
                        var step = _block.Step(block);
                        for (int s = 0; s < blockHeader->Count; s++)
                        {
                            if ((blockHeader->DeadMask & (1UL << s)) != 0)
                                continue;
                            var code = _block.Code(block, s);
                            var dotA = bits == 8 ? Kernels.DotU8I8(code, qa, _dims) : Kernels.DotU4I8(code, qa, qa + half, half);
                            var dotB = bits == 8 ? Kernels.DotU8I8(code, qb, _dims) : Kernels.DotU4I8(code, qb, qb + half, half);
                            if (towardsA.Gain(lo[s], step[s], dotA) > 0 || towardsB.Gain(lo[s], step[s], dotB) > 0)
                                moveIn.Add((_block.VectorIds(block)[s], _block.EntryHints(block)[s], neighbors[n]));
                        }
                    }
                }
            }

            var moveInVectors = new float[moveIn.Count * _dims];
            var moveInSides = new byte[moveIn.Count];
            var confirmed = new bool[moveIn.Count];
            if (moveIn.Count > 0)
            {
                var ids = new long[moveIn.Count];
                for (int i = 0; i < moveIn.Count; i++)
                    ids[i] = moveIn[i].VectorId;
                LoadVectors(ids, moveInVectors);
                var removals = new Dictionary<long, HashSet<long>>();
                for (int i = 0; i < moveIn.Count; i++)
                {
                    var vector = new ReadOnlySpan<float>(moveInVectors, i * _dims, _dims);
                    var own = Score(vector, GetCentroid(moveIn[i].From), _dims);
                    var a = Score(vector, centroidA, _dims);
                    var b = Score(vector, centroidB, _dims);
                    if (Math.Max(a, b) <= own)
                        continue;
                    confirmed[i] = true;
                    moveInSides[i] = a >= b ? (byte)0 : (byte)1;
                    if (removals.TryGetValue(moveIn[i].From, out var set) == false)
                        removals[moveIn[i].From] = set = new HashSet<long>();
                    set.Add(moveIn[i].VectorId);
                }

                foreach (var (from, set) in removals)
                {
                    MarkDead(from, set);
                    _touched.Add(from);
                    _shrunk.Add(from);
                }
            }

            ClearPartition(partitionId);
            SetCentroid(partitionId, centroidA.AsSpan(0, _dims));
            var newPartitionId = CreatePartition(centroidB.AsSpan(0, _dims));

            WriteSide(partitionId, sideA, vectors, vectorIds, hints, moveIn, moveInVectors, moveInSides, confirmed, 0, updateOwnMembers: false);
            WriteSide(newPartitionId, sideB, vectors, vectorIds, hints, moveIn, moveInVectors, moveInSides, confirmed, 1, updateOwnMembers: true);

            if (moveOutIndexes.Count > 0)
            {
                var outVectors = new float[moveOutIndexes.Count * _dims];
                var outIds = new long[moveOutIndexes.Count];
                var outHints = new long[moveOutIndexes.Count];
                for (int i = 0; i < moveOutIndexes.Count; i++)
                {
                    var idx = moveOutIndexes[i];
                    Array.Copy(vectors, idx * _dims, outVectors, i * _dims, _dims);
                    outIds[i] = vectorIds[idx];
                    outHints[i] = hints[idx];
                }

                MoveTo(moveOutTargets.ToArray(), outVectors, outIds, outHints);
            }

            Options.NumberOfSplits++;
            Options.NumberOfReassignedVectors += moveOutIndexes.Count + Array.FindAll(confirmed, x => x).Length;
            _touched.Add(partitionId);
            _touched.Add(newPartitionId);
        }

        private void WriteSide(long partitionId, List<int> own, float[] vectors, long[] vectorIds, long[] hints,
            List<(long VectorId, long Hint, long From)> moveIn, float[] moveInVectors, byte[] moveInSides, bool[] confirmed, byte side, bool updateOwnMembers)
        {
            int count = own.Count;
            for (int i = 0; i < moveIn.Count; i++)
            {
                if (confirmed[i] && moveInSides[i] == side)
                    count++;
            }

            var sideVectors = new float[count * _dims];
            var sideIds = new long[count];
            var sideHints = new long[count];
            int k = 0;
            foreach (var idx in own)
            {
                Array.Copy(vectors, idx * _dims, sideVectors, k * _dims, _dims);
                sideIds[k] = vectorIds[idx];
                sideHints[k] = hints[idx];
                k++;
            }

            for (int i = 0; i < moveIn.Count; i++)
            {
                if (confirmed[i] == false || moveInSides[i] != side)
                    continue;
                Array.Copy(moveInVectors, i * _dims, sideVectors, k * _dims, _dims);
                sideIds[k] = moveIn[i].VectorId;
                sideHints[k] = moveIn[i].Hint;
                k++;
            }

            var indexes = new int[count];
            for (int i = 0; i < count; i++)
                indexes[i] = i;
            AppendVectors(partitionId, indexes, sideVectors, sideIds, default, sideHints);
            for (int i = updateOwnMembers ? 0 : own.Count; i < count; i++)
                _nodePartitions.Add(sideIds[i], partitionId);
        }

        private bool TwoMeans(float[] vectors, int count, byte[] sides, out float[] centroidA, out float[] centroidB)
        {
            centroidA = new float[_dims + 1];
            centroidB = new float[_dims + 1];
            if (count < 2)
                return false;

            var first = new ReadOnlySpan<float>(vectors, 0, _dims);
            int farthest = 0;
            float lowest = float.MaxValue;
            for (int i = 1; i < count; i++)
            {
                var dot = TensorPrimitives.Dot(first, new ReadOnlySpan<float>(vectors, i * _dims, _dims));
                if (dot >= lowest)
                    continue;
                lowest = dot;
                farthest = i;
            }

            int seedB = farthest;
            int seedA = 0;
            lowest = float.MaxValue;
            var seed = new ReadOnlySpan<float>(vectors, seedB * _dims, _dims);
            for (int i = 0; i < count; i++)
            {
                var dot = TensorPrimitives.Dot(seed, new ReadOnlySpan<float>(vectors, i * _dims, _dims));
                if (dot >= lowest)
                    continue;
                lowest = dot;
                seedA = i;
            }

            if (seedA == seedB)
                return false;

            new ReadOnlySpan<float>(vectors, seedA * _dims, _dims).CopyTo(centroidA);
            new ReadOnlySpan<float>(vectors, seedB * _dims, _dims).CopyTo(centroidB);
            centroidA[_dims] = TensorPrimitives.SumOfSquares(centroidA.AsSpan(0, _dims));
            centroidB[_dims] = TensorPrimitives.SumOfSquares(centroidB.AsSpan(0, _dims));

            var sumA = new float[_dims];
            var sumB = new float[_dims];
            for (int iteration = 0; iteration < SplitIterations; iteration++)
            {
                Array.Clear(sumA);
                Array.Clear(sumB);
                int countA = 0, countB = 0;
                bool changed = false;
                for (int i = 0; i < count; i++)
                {
                    var vector = new ReadOnlySpan<float>(vectors, i * _dims, _dims);
                    var side = Score(vector, centroidA, _dims) >= Score(vector, centroidB, _dims) ? (byte)0 : (byte)1;
                    changed |= side != sides[i] || iteration == 0;
                    sides[i] = side;
                    if (side == 0)
                    {
                        TensorPrimitives.Add(sumA, vector, sumA);
                        countA++;
                    }
                    else
                    {
                        TensorPrimitives.Add(sumB, vector, sumB);
                        countB++;
                    }
                }

                if (countA == 0 || countB == 0)
                    return false;

                TensorPrimitives.Divide(sumA, countA, centroidA.AsSpan(0, _dims));
                TensorPrimitives.Divide(sumB, countB, centroidB.AsSpan(0, _dims));
                centroidA[_dims] = TensorPrimitives.SumOfSquares(centroidA.AsSpan(0, _dims));
                centroidB[_dims] = TensorPrimitives.SumOfSquares(centroidB.AsSpan(0, _dims));
                if (changed == false)
                    break;
            }

            return true;
        }
    }
}
