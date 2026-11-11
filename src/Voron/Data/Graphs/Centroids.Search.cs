using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Numerics.Tensors;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Sparrow;
using Sparrow.Server;
using Sparrow.Server.Collections;
using Voron.Data.Containers;
using Voron.Data.Lookups;
using Voron.Data.PostingLists;
using Voron.Global;
using Voron.Impl;
using Voron.Util;
using Voron.Util.PFor;

namespace Voron.Data.Graphs;

public unsafe partial class Centroids
{
    public const int DefaultScanFactor = 512;

    public enum SearchMode
    {
        Approximate,
        Exact,
        Scan
    }

    public static VectorSearchRetriever Search(LowLevelTransaction llt, Slice name, Memory<byte> vector, int numberOfCandidates, float minimumSimilarity,
        SearchMode mode, GrowableBitArray? filter = null, ContextBoundNativeList<long>? vectorsToScan = null, int scanFactor = DefaultScanFactor, int rerankFactor = 0)
    {
        return new VectorSearchRetriever(llt, name, vector, numberOfCandidates, minimumSimilarity, mode, filter, vectorsToScan, scanFactor, rerankFactor);
    }

    public struct VectorSearchRetriever : IDisposable
    {
        private readonly LowLevelTransaction _llt;
        private readonly Options _options;
        private readonly Lookup<Int64LookupKey> _nodePostings;
        private readonly Lookup<Int64LookupKey> _partitions;
        private readonly Lookup<Int64LookupKey> _routerChunks;
        private readonly int _numberOfCandidates;
        private readonly int _rerankCount;
        private readonly float _maximumDistance;
        private readonly GrowableBitArray? _filter;
        private readonly SearchMode _mode;
        private readonly delegate*<ReadOnlySpan<byte>, ReadOnlySpan<byte>, float> _distance;
        private readonly bool _isEmpty;
        private Memory<byte> _query;
        private ByteStringContext<ByteStringMemoryCache>.InternalScope _queryScope;
        private bool _hasQueryScope;
        private float[] _queryVector;
        private sbyte[] _queryCodes;
        private float _queryCodesScale;
        private float _querySum;
        private int _budget;

        private long[] _partitionOrder;
        private long[] _orderedPartitions;
        private int _partitionCount;
        private int _nextPartition;
        private bool _partitionsSorted;
        private float[] _partitionScores;

        private long[] _results;
        private float[] _resultDistances;
        private int _resultCount;
        private int _currentResult;
        private HashSet<long> _seen;
        private bool _batchComputed;
        private int _returnedCandidates;
        private ContextBoundNativeList<long> _vectorsToScan;
        private bool _hasVectorsToScan;

        private ContextBoundNativeList<long> _postingListResults;
        private FastPForDecoder _pforDecoder;
        private PostingList.Iterator _postingListIterator;
        private PostingList _postingList;
        private int _currentMatchesIndex;
        private bool _foundCandidateInCurrentPostingList;
        private float _currentDistance;

        public bool IsSortedByDistance;

        public long CandidatesProcessed;

        public Hnsw.SimilarityMethod? SimilarityMethod => _isEmpty ? null : _options.SimilarityMethod;

        public bool IsEmpty => _isEmpty;

        public int NumberOfCandidates => _numberOfCandidates;

        public VectorSearchRetriever(LowLevelTransaction llt, Slice name, Memory<byte> vector, int numberOfCandidates, float minimumSimilarity, SearchMode mode,
            GrowableBitArray? filter, ContextBoundNativeList<long>? vectorsToScan, int scanFactor, int rerankFactor)
        {
            _llt = llt;
            _numberOfCandidates = Math.Max(1, numberOfCandidates);
            _filter = filter;
            _mode = mode;
            IsSortedByDistance = true;
            _postingListResults = new ContextBoundNativeList<long>(llt.Allocator);
            _pforDecoder = new FastPForDecoder(llt.Allocator);
            if (vectorsToScan is { } toScan)
            {
                _vectorsToScan = toScan;
                _hasVectorsToScan = true;
            }

            if (TryReadOptions(llt, name, out _options, out var tree) == false || _options.CountOfVectors == 0)
            {
                _isEmpty = true;
                return;
            }

            _nodePostings = tree.LookupFor<Int64LookupKey>(NodePostingsSlice);
            _partitions = tree.LookupFor<Int64LookupKey>(PartitionsSlice);
            _routerChunks = tree.LookupFor<Int64LookupKey>(RouterChunksSlice);
            var hnswOptions = _options.AsHnswOptions();
            _distance = Hnsw.GetDistanceKernel(hnswOptions);
            _maximumDistance = Hnsw.MinimumSimilarityToDistance(_options.SimilarityMethod, _options.VectorSizeBytes, minimumSimilarity);
            _hasQueryScope = Hnsw.TryNormalizeQueryVector(hnswOptions, llt.Allocator, ref vector, out _queryScope);
            _query = vector;
            if (_query.Length != _options.VectorSizeBytes)
                throw new InvalidDataException($"Query vector has {_query.Length} bytes, but the vectors of this index have {_options.VectorSizeBytes} bytes.");

            if (rerankFactor <= 0)
                rerankFactor = _options.QuantizationBits == 8 ? 2 : 3;
            _rerankCount = Math.Max(_numberOfCandidates * rerankFactor, 32);
            _budget = Math.Max(_numberOfCandidates * Math.Max(1, scanFactor), 2 * _options.MaxPartitionSize);
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public float DistanceToScore(float distance)
        {
            return _options.SimilarityMethod switch
            {
                Hnsw.SimilarityMethod.CosineSimilaritySingles or Hnsw.SimilarityMethod.CosineSimilarityI8 => 1 - distance,
                Hnsw.SimilarityMethod.HammingDistance => (_options.VectorSizeBytes * 8 - distance) / (8f * _options.VectorSizeBytes),
                _ => throw new InvalidDataException($"Unknown similarity method {_options.SimilarityMethod}")
            };
        }

        public void DistancesToScores(Span<float> distances)
        {
            for (int i = 0; i < distances.Length; i++)
                distances[i] = DistanceToScore(distances[i]);
        }

        public int Fill(Span<long> matches, Span<float> distances, GrowableBitArray? filter)
        {
            if (_isEmpty)
                return 0;

            if (_batchComputed == false)
            {
                _batchComputed = true;
                if (ComputeBatch() == false)
                    return 0;
            }

            return filter is { } f ? Fill(matches, distances, f) : Fill(matches, distances);
        }

        private int Fill(Span<long> matches, Span<float> distances)
        {
            int index = 0;
            var limit = Math.Min(_resultCount, _numberOfCandidates);
            while (index < matches.Length)
            {
                if (_postingList != null)
                {
                    if (_postingListIterator.Fill(matches[index..], out var total) is false && total is 0)
                    {
                        _postingListIterator = default;
                        _postingList = null;
                        _currentResult++;
                        continue;
                    }

                    Hnsw.Registration.InternalEntryIdToEntryId(matches.Slice(index, total));
                    distances.Slice(index, total).Fill(_currentDistance);
                    index += total;
                    continue;
                }

                if (_currentMatchesIndex < _postingListResults.Count)
                {
                    var copy = Math.Min(_postingListResults.Count - _currentMatchesIndex, matches.Length - index);
                    _postingListResults.CopyTo(matches[index..], _currentMatchesIndex, copy);
                    Hnsw.Registration.InternalEntryIdToEntryId(matches.Slice(index, copy));
                    distances.Slice(index, copy).Fill(_currentDistance);
                    index += copy;
                    _currentMatchesIndex += copy;
                    if (_currentMatchesIndex == _postingListResults.Count)
                    {
                        _currentMatchesIndex = 0;
                        _postingListResults.Clear();
                        _currentResult++;
                    }

                    continue;
                }

                if (_currentResult >= limit)
                    break;

                if (LoadPostingList(out var single))
                {
                    distances[index] = _currentDistance;
                    matches[index++] = single;
                    _currentResult++;
                }
            }

            return index;
        }

        private int Fill(Span<long> matches, Span<float> distances, GrowableBitArray filter)
        {
            int index = 0;
            while (index < matches.Length && _returnedCandidates < _numberOfCandidates)
            {
                if (_postingList != null)
                {
                    if (_postingListIterator.Fill(matches[index..], out var total) is false && total is 0)
                    {
                        _postingListIterator = default;
                        _postingList = null;
                        _returnedCandidates += _foundCandidateInCurrentPostingList.ToInt32();
                        _foundCandidateInCurrentPostingList = false;
                        _currentResult++;
                        continue;
                    }

                    Hnsw.Registration.InternalEntryIdToEntryId(matches.Slice(index, total));
                    var end = index + total;
                    for (int i = index; i < end; i++)
                    {
                        if (filter.Contains(matches[i]) == false)
                            continue;
                        _foundCandidateInCurrentPostingList = true;
                        matches[index] = matches[i];
                        distances[index] = _currentDistance;
                        index++;
                    }

                    continue;
                }

                if (_currentMatchesIndex < _postingListResults.Count)
                {
                    var fillLimit = _currentMatchesIndex + Math.Min(_postingListResults.Count - _currentMatchesIndex, matches.Length - index);
                    for (; _currentMatchesIndex < fillLimit; _currentMatchesIndex++)
                    {
                        var entryId = Hnsw.Registration.InternalEntryIdToEntryId(_postingListResults[_currentMatchesIndex]);
                        if (filter.Contains(entryId) == false)
                            continue;
                        matches[index] = entryId;
                        distances[index] = _currentDistance;
                        _foundCandidateInCurrentPostingList = true;
                        index++;
                    }

                    if (_currentMatchesIndex == _postingListResults.Count)
                    {
                        _returnedCandidates += _foundCandidateInCurrentPostingList.ToInt32();
                        _foundCandidateInCurrentPostingList = false;
                        _currentMatchesIndex = 0;
                        _postingListResults.Clear();
                        _currentResult++;
                    }

                    continue;
                }

                if (_currentResult >= _resultCount)
                {
                    if (_mode != SearchMode.Approximate || ComputeBatch() == false)
                        break;
                    IsSortedByDistance = false;
                    continue;
                }

                if (LoadPostingList(out var single))
                {
                    _currentResult++;
                    if (filter.Contains(single) == false)
                        continue;
                    distances[index] = _currentDistance;
                    matches[index++] = single;
                    _returnedCandidates++;
                }
            }

            return index;
        }

        /// <summary>
        /// Prepares the posting list of the current result. Returns true when it holds a single entry, which is returned directly.
        /// </summary>
        private bool LoadPostingList(out long single)
        {
            single = 0;
            var vectorId = _results[_currentResult];
            _currentDistance = _resultDistances[_currentResult];
            if (_nodePostings.TryGetValue(vectorId, out var postingList) == false)
            {
                _currentResult++;
                return false;
            }

            var raw = postingList & Constants.Graphs.VectorId.ContainerType;
            switch (postingList & Constants.Graphs.VectorId.EnsureIsSingleMask)
            {
                case Constants.Graphs.VectorId.Single:
                    single = Hnsw.Registration.InternalEntryIdToEntryId(raw);
                    return true;
                case Constants.Graphs.VectorId.SmallPostingList:
                    Hnsw.SearchState.ReadPostingList(_llt, new ContainerEntryId(raw), ref _postingListResults, ref _pforDecoder, out _);
                    return false;
                case Constants.Graphs.VectorId.PostingList:
                    var setStateSpan = Container.GetReadOnly(_llt, new ContainerEntryId(raw));
                    ref readonly var setState = ref MemoryMarshal.AsRef<PostingListState>(setStateSpan);
                    _postingList = new PostingList(_llt, Slices.Empty, setState);
                    _postingListIterator = _postingList.Iterate();
                    return false;
                default:
                    _currentResult++;
                    return false;
            }
        }

        private bool ComputeBatch()
        {
            _currentResult = 0;
            _resultCount = 0;
            switch (_mode)
            {
                case SearchMode.Exact:
                    if (_resultDistances != null)
                        return false;
                    ComputeExact();
                    break;
                case SearchMode.Scan:
                    if (_resultDistances != null)
                        return false;
                    ComputeScan();
                    break;
                default:
                    if (ComputeApproximate() == false)
                        return false;
                    break;
            }

            return _resultCount > 0;
        }

        private void PrepareQuery()
        {
            var dims = _options.Dimensions;
            _queryVector = new float[dims];
            ToWorkingVector(_options, _query.Span, _queryVector);
            _querySum = TensorPrimitives.Sum(_queryVector);
            if (_options.QuantizationBits == 8)
            {
                _queryCodes = new sbyte[dims];
                fixed (sbyte* q = _queryCodes)
                    _queryCodesScale = Kernels.QuantizeI8(_queryVector, q, Kernels.Code8QueryRange);
                return;
            }

            var half = (dims + 1) / 2;
            _queryCodes = new sbyte[half * 2];
            var padded = new float[half * 2];
            _queryVector.AsSpan().CopyTo(padded);
            fixed (sbyte* q = _queryCodes)
                _queryCodesScale = Kernels.QuantizeI8(padded, q, Kernels.Code4QueryRange);
        }

        private void RoutePartitions()
        {
            var partitions = _options.NumberOfPartitions;
            _partitionOrder = ArrayPool<long>.Shared.Rent(partitions);
            _partitionScores = ArrayPool<float>.Shared.Rent(partitions);
            var query = stackalloc sbyte[_options.Dimensions];
            var scale = Kernels.QuantizeI8(_queryVector, query, Kernels.RouterQueryRange);
            ScoreRouter(_llt, _routerChunks, _options, query, scale, _partitionScores.AsSpan(0, partitions), _partitionOrder.AsSpan(0, partitions));
            for (int i = 0; i < partitions; i++)
                _partitionScores[i] = -_partitionScores[i];

            var averageSize = Math.Max(1, (int)(_options.CountOfVectors / Math.Max(1, partitions)));
            var needed = Math.Min(partitions, 2 * _budget / averageSize + 8);
            if (needed * 4 >= partitions)
            {
                SortAllPartitions();
                return;
            }

            var heap = new CandidateHeap(needed);
            for (int i = 0; i < partitions; i++)
                heap.Add(_partitionScores[i], i);
            _orderedPartitions = new long[heap.Count];
            var keys = new float[heap.Count];
            for (int i = 0; i < heap.Count; i++)
            {
                keys[i] = heap.Keys[i];
                _orderedPartitions[i] = _partitionOrder[heap.Values[i]];
            }

            Array.Sort(keys, _orderedPartitions);
            _partitionCount = _orderedPartitions.Length;
        }

        private void SortAllPartitions()
        {
            var partitions = _options.NumberOfPartitions;
            Array.Sort(_partitionScores, _partitionOrder, 0, partitions);
            _orderedPartitions = _partitionOrder;
            _partitionCount = partitions;
            _partitionsSorted = true;
        }

        private bool ComputeApproximate()
        {
            if (_queryVector == null)
            {
                PrepareQuery();
                RoutePartitions();
            }
            else
            {
                _budget *= 2;
            }

            if (_nextPartition >= _partitionCount)
            {
                if (_partitionsSorted)
                    return false;
                SortAllPartitions();
                if (_nextPartition >= _partitionCount)
                    return false;
            }

            var block = new BlockLayout(_options);
            var dims = _options.Dimensions;
            var half = (dims + 1) / 2;
            var bits = _options.QuantizationBits;
            var heap = new CandidateHeap(_rerankCount);
            var centroid = new ReadOnlySpan<float>();
            long scanned = 0;
            var filter = _filter;

            fixed (sbyte* q = _queryCodes)
            {
                while (scanned < _budget && _nextPartition < _partitionCount)
                {
                    var partitionId = _orderedPartitions[_nextPartition++];
                    if (_partitions.TryGetValue(partitionId, out var headerId) == false)
                        continue;

                    Container.Get(_llt, new ContainerEntryId(headerId), out var headerItem);
                    var header = (PartitionHeader*)headerItem.Address;
                    Container.Get(_llt, new ContainerEntryId(header->CentroidId), out var centroidItem);
                    centroid = new ReadOnlySpan<float>(centroidItem.Address, dims);
                    var centroidDot = TensorPrimitives.Dot(centroid, _queryVector);
                    var codeScale = _queryCodesScale;
                    var blocks = PartitionBlocks(header);
                    for (int b = 0; b < header->BlockCount; b++)
                    {
                        Container.Get(_llt, new ContainerEntryId(blocks[b]), out var blockItem);
                        var data = blockItem.Address;
                        var blockHeader = (BlockHeader*)data;
                        var ids = block.VectorIds(data);
                        var hints = block.EntryHints(data);
                        var lo = block.Lo(data);
                        var step = block.Step(data);
                        for (int s = 0; s < blockHeader->Count; s++)
                        {
                            if ((blockHeader->DeadMask & (1UL << s)) != 0)
                                continue;
                            if (filter is { } f && hints[s] != 0 && f.Contains(hints[s]) == false)
                                continue;
                            if (_seen != null && _seen.Contains(ids[s]))
                                continue;

                            var dot = bits == 8
                                ? Kernels.DotU8I8(block.Code(data, s), q, dims)
                                : Kernels.DotU4I8(block.Code(data, s), q, q + half, half);
                            var estimate = centroidDot + lo[s] * _querySum + step[s] * codeScale * dot;
                            heap.Add(1 - estimate, ids[s]);
                        }
                    }

                    scanned += header->Count;
                }
            }

            CandidatesProcessed += scanned;
            Rerank(heap.Values, heap.Count, keepAll: _filter != null);
            if (_filter != null)
            {
                _seen ??= new HashSet<long>();
                for (int i = 0; i < heap.Count; i++)
                    _seen.Add(heap.Values[i]);
            }

            return true;
        }

        private void ComputeExact()
        {
            var ids = new List<long>((int)Math.Min(int.MaxValue, _options.CountOfVectors));
            var it = _nodePostings.Iterate();
            it.Seek<Int64LookupKey>(long.MinValue);
            while (it.MoveNext(out Int64LookupKey key, out _, out _))
                ids.Add(key.Value);
            CandidatesProcessed = ids.Count;
            var array = ids.ToArray();
            Rerank(array, array.Length, keepAll: _filter != null);
        }

        private void ComputeScan()
        {
            if (_hasVectorsToScan == false)
                return;
            var array = _vectorsToScan.ToSpan().ToArray();
            CandidatesProcessed = array.Length;
            Rerank(array, array.Length, keepAll: true);
        }

        /// <summary>
        /// Computes the exact distances of the candidates and keeps them sorted by distance.
        /// </summary>
        private void Rerank(long[] candidates, int count, bool keepAll)
        {
            const int batchSize = 4096;
            var exact = new float[count];
            var ids = ArrayPool<long>.Shared.Rent(Math.Min(count, batchSize));
            var order = ArrayPool<int>.Shared.Rent(Math.Min(count, batchSize));
            var spans = ArrayPool<UnmanagedSpan>.Shared.Rent(Math.Min(count, batchSize));
            try
            {
                var query = _query.Span;
                for (int start = 0; start < count; start += batchSize)
                {
                    var n = Math.Min(batchSize, count - start);
                    for (int i = 0; i < n; i++)
                    {
                        ids[i] = Hnsw.GetVectorContainerId(candidates[start + i]);
                        order[i] = start + i;
                    }

                    Array.Sort(ids, order, 0, n);
                    Container.GetAll(_llt, ids.AsSpan(0, n), spans.AsSpan(0, n), -1, _llt.PageLocator);
                    for (int k = 0; k < n; k++)
                    {
                        var idx = order[k];
                        var stored = Hnsw.SliceVector(spans[k], candidates[idx], _options.VectorSizeBytes);
                        exact[idx] = _distance(query, stored.ToReadOnlySpan());
                    }
                }
            }
            finally
            {
                ArrayPool<long>.Shared.Return(ids);
                ArrayPool<int>.Shared.Return(order);
                ArrayPool<UnmanagedSpan>.Shared.Return(spans);
            }

            var sortedIds = candidates.AsSpan(0, count).ToArray();
            Array.Sort(exact, sortedIds);
            var limit = keepAll ? count : Math.Min(count, _numberOfCandidates);
            int kept = 0;
            while (kept < limit && exact[kept] <= _maximumDistance)
                kept++;

            _results = sortedIds;
            _resultDistances = exact;
            _resultCount = kept;
            _currentResult = 0;
        }

        public void Dispose()
        {
            if (_partitionOrder != null)
            {
                ArrayPool<long>.Shared.Return(_partitionOrder);
                ArrayPool<float>.Shared.Return(_partitionScores);
                _partitionOrder = null;
                _partitionScores = null;
                _orderedPartitions = null;
            }

            _postingListResults.Dispose();
            _pforDecoder.Dispose();
            if (_hasQueryScope)
            {
                _queryScope.Dispose();
                _hasQueryScope = false;
            }
        }
    }

    /// <summary>
    /// Keeps the <c>capacity</c> candidates with the smallest estimated distance, as a binary max heap.
    /// </summary>
    private struct CandidateHeap
    {
        public readonly float[] Keys;
        public readonly long[] Values;
        public int Count;

        public CandidateHeap(int capacity)
        {
            Keys = new float[capacity];
            Values = new long[capacity];
            Count = 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Add(float key, long value)
        {
            if (Count < Keys.Length)
            {
                int i = Count++;
                while (i > 0)
                {
                    int parent = (i - 1) >> 1;
                    if (Keys[parent] >= key)
                        break;
                    Keys[i] = Keys[parent];
                    Values[i] = Values[parent];
                    i = parent;
                }

                Keys[i] = key;
                Values[i] = value;
                return;
            }

            if (key >= Keys[0])
                return;

            int pos = 0;
            while (true)
            {
                int left = 2 * pos + 1;
                if (left >= Count)
                    break;
                int right = left + 1;
                int largest = right < Count && Keys[right] > Keys[left] ? right : left;
                if (Keys[largest] <= key)
                    break;
                Keys[pos] = Keys[largest];
                Values[pos] = Values[largest];
                pos = largest;
            }

            Keys[pos] = key;
            Values[pos] = value;
        }
    }
}
