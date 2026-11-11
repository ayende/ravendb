using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Sparrow;
using Sparrow.Platform;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Voron.Data.BTrees;
using Voron.Data.CompactTrees;
using Voron.Data.Containers;
using Voron.Data.Lookups;
using Voron.Global;
using Voron.Impl;
using Voron.Util;

namespace Voron.Data.Graphs;

public unsafe partial class Centroids
{
    public class Registration : IVectorIndexer
    {
        private static readonly HashSet<long> NoDirtyNodes = new();

        private readonly LowLevelTransaction _llt;
        private readonly Tree _tree;
        private Options _options;
        private readonly Lookup<Int64LookupKey> _nodePostings;
        private readonly CompactTree _vectorsByHash;
        private readonly ContainerId _globalVectorsContainerId;
        private readonly int _vectorBatchSizeInPages;
        private readonly Dictionary<ByteString, (ByteString Hash, long VectorId, NativeList<long> Modifications)> _vectorHashCache = new(ByteStringContentComparer.Instance);
        private ByteStringContext<ByteStringMemoryCache>.InternalScope _normalizationScope;
        private ByteString _normalizationBuffer;

        public bool IsCommited { get; private set; }

        public int AmountOfModifiedVectorsInTransaction => _vectorHashCache.Count;

        public HashSet<long> DirtyNodeIds => NoDirtyNodes;

        public int MaxConcurrentBatches { get; set; } = Environment.ProcessorCount;

        public Options Options => _options;

        public Registration(LowLevelTransaction llt, Slice name)
        {
            _llt = llt;
            _tree = llt.Transaction.ReadTree(name);
            _options = Unsafe.Read<Options>(_tree.DirectRead(OptionsSlice));
            _nodePostings = _tree.LookupFor<Int64LookupKey>(NodePostingsSlice);
            _vectorsByHash = llt.Transaction.CompactTreeFor(Hnsw.VectorsIdByHashSlice);
            _globalVectorsContainerId = Hnsw.ReadGlobalVectorsContainerId(llt);
            _vectorBatchSizeInPages = Hnsw.GetVectorBatchInPages(_options.VectorSizeBytes);
        }

        public ByteString Register(long entryId, ReadOnlySpan<byte> vector)
        {
            entryId = Hnsw.Registration.EntryIdToInternalEntryId(entryId);
            if (vector.Length != _options.VectorSizeBytes)
                vector = NormalizeIntoScratch(vector);

            _llt.Allocator.AllocateDirect(Sodium.GenericHashSize, out var hashBuffer);
            Sodium.GenericHash(vector, hashBuffer.ToSpan());

            ref var entry = ref CollectionsMarshal.GetValueRefOrAddDefault(_vectorHashCache, hashBuffer, out var exists);
            if (exists)
            {
                entry.Modifications.Add(_llt.Allocator, entryId);
                _llt.Allocator.Release(ref hashBuffer);
                return entry.Hash;
            }

            if (_vectorsByHash.TryGetValue(hashBuffer.ToReadOnlySpan(), out _, out var vectorId) is false)
            {
                vectorId = (long)Hnsw.RegisterVector(_llt, _globalVectorsContainerId, _vectorBatchSizeInPages, _options.VectorSizeBytes,
                    ref _options.LastUsedContainerId, ref _options.VectorBatchIndex, vector);
                _vectorsByHash.Add(hashBuffer.ToReadOnlySpan(), vectorId);
            }

            entry = (hashBuffer, vectorId, NativeList<long>.Create(_llt.Allocator, entryId));
            return hashBuffer;
        }

        public void Remove(long entryId, ReadOnlySpan<byte> vectorHash)
        {
            entryId = Hnsw.Registration.EntryIdToInternalEntryId(entryId);
            const long removalMask = 1;

            _llt.Allocator.AllocateDirect(Sodium.GenericHashSize, out var hashBuffer);
            vectorHash.CopyTo(hashBuffer.ToSpan());

            ref var entry = ref CollectionsMarshal.GetValueRefOrAddDefault(_vectorHashCache, hashBuffer, out var exists);
            if (exists)
            {
                entry.Modifications.Add(_llt.Allocator, entryId | removalMask);
                _llt.Allocator.Release(ref hashBuffer);
                return;
            }

            if (_vectorsByHash.TryGetValue(vectorHash, out var vectorId) is false)
                PortableExceptions.Throw<InvalidOperationException>($"Unable to find the vector corresponding to the provided vector hash: base64({Convert.ToBase64String(vectorHash)}).");

            entry = (hashBuffer, vectorId, NativeList<long>.Create(_llt.Allocator, entryId | removalMask));
        }

        private ReadOnlySpan<byte> NormalizeIntoScratch(ReadOnlySpan<byte> vector)
        {
            var floats = MemoryMarshal.Cast<byte, float>(vector);
            PortableExceptions.ThrowIf<ArgumentOutOfRangeException>(
                _options.SimilarityMethod != Hnsw.SimilarityMethod.CosineSimilaritySingles || Hnsw.TensorSizeBytes<float>(floats.Length) != _options.VectorSizeBytes,
                $"Vector size {vector.Length} does not match expected size: {_options.VectorSizeBytes}");

            if (_normalizationBuffer.HasValue == false)
                _normalizationScope = _llt.Allocator.Allocate(_options.VectorSizeBytes, out _normalizationBuffer);

            var dst = _normalizationBuffer.ToSpan();
            Hnsw.WriteNormalizedTensor(floats, dst);
            return dst;
        }

        public void Commit(CancellationToken token)
        {
            PortableExceptions.ThrowIfOnDebug<InvalidOperationException>(_llt.Committed);

            using var writer = new Writer(_llt, _tree);
            writer.Options.LastUsedContainerId = _options.LastUsedContainerId;
            writer.Options.VectorBatchIndex = _options.VectorBatchIndex;
            writer.MaxDegreeOfParallelism = Math.Max(1, Math.Min(MaxConcurrentBatches, Environment.ProcessorCount));

            var postingListWriter = new VectorPostingListWriter(_llt, writer.Options.Container);
            var inserts = new List<long>();
            var insertPostingLists = new List<long>();
            var deletes = new List<long>();
            try
            {
                foreach (var (_, (_, vectorId, modifications)) in _vectorHashCache)
                {
                    token.ThrowIfCancellationRequested();

                    var exists = writer.NodePostings.TryGetValue(vectorId, out var current);
                    var updated = postingListWriter.Merge(exists ? current : 0, modifications);
                    if (exists == false)
                    {
                        if (updated == 0)
                            continue;
                        inserts.Add(vectorId);
                        insertPostingLists.Add(updated);
                    }
                    else if (updated == 0)
                    {
                        deletes.Add(vectorId);
                    }
                    else if (updated != current)
                    {
                        writer.UpdatePostingList(vectorId, updated);
                    }
                }

                writer.Delete(CollectionsMarshal.AsSpan(deletes));
                writer.Insert(CollectionsMarshal.AsSpan(inserts), CollectionsMarshal.AsSpan(insertPostingLists));
                writer.RunMaintenance();
                writer.Flush();
                _options = writer.Options;
            }
            finally
            {
                postingListWriter.Dispose();
            }

            IsCommited = true;
        }

        public void Dispose()
        {
            if (_normalizationBuffer.HasValue)
                _normalizationScope.Dispose();
        }
    }
}
