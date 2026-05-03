using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using Corax.Indexing;
using Corax.Mappings;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Utils;
using Voron.Data.RoaringBitmaps;
using Sparrow;
using Sparrow.Server.Utils;
using Voron;
using Voron.Data.CompactTrees;
using Voron.Data.Graphs;
using Voron.Data.Lookups;
using Voron.Util;

namespace Corax.Querying;

public partial class IndexSearcher
{
    internal static class VectorSearchUtils
    {
        public static bool ShouldScan(IndexSearcher indexSearcher, long filterMatchesCount, bool isExact, IQueryMatch filterQuery, int scanningThreshold, int numberOfCandidates)
        {
            var shouldScan = filterQuery != null && (filterMatchesCount < scanningThreshold || isExact || filterMatchesCount * 0.5 < numberOfCandidates);
            if (indexSearcher._testingConfiguration is {DisableVectorSearchScanning: true})
                return false;
             
            return shouldScan;
        }

        public static RoaringBitmap LoadFilterMatches(IndexSearcher indexSearcher, ref IQueryMatch query, out bool owned)
        {
            // Fast path: the upstream pipeline (CompiledQueryMatch / BitmapMatch / PostFilterMatch
            // wrapping a bitmap) already has the bitmap built. Borrow it directly — we only read,
            // and the source query keeps the storage alive for our entire scope. Skipping
            // re-materialization avoids one full Fill loop and one bitmap construction per query.
            if (query is IBitmapQueryMatch bitmapMatch)
            {
                owned = false;
                var borrowed = bitmapMatch.BorrowBitmap();
                borrowed.PrepareForReading();
                return borrowed;
            }

            owned = true;
            var filter = new RoaringBitmap(indexSearcher.Allocator);

            using var _ = indexSearcher.Allocator.Allocate(4096, out Span<long> workingBuffer);
            int read;
            while ((read = query.Fill(workingBuffer)) > 0)
            {
                for (int i = 0; i < read; i++)
                    filter.Add(workingBuffer[i]);
            }

            filter.PrepareForReading();
            return filter;
        }

        /// <summary>
        /// Materializes all entry IDs from a RoaringBitmap filter, shuffles them for random access,
        /// and converts each entry to its corresponding HNSW node ID(s) on demand.
        /// Designed for probing random starting nodes during approximate filtered nearest neighbor search.
        /// </summary>
        public struct RandomNodesFromFilterEnumerator : IEnumerator<long>
        {
            private List<long> _results;
            private long _current;
            private readonly IndexSearcher _indexSearcher;
            private long[] _entryIds;
            private int _entryIndex;
            private readonly Random _random;
            private bool _isDone;
            private Page p = default;
            private CompactKey _key;
            private readonly CompactTree _vectorsByHash;
            private readonly Lookup<Int64LookupKey> _nodesByVectorId;
            private readonly long _vectorRootPage;

            public RandomNodesFromFilterEnumerator(IndexSearcher indexSearcher, FieldMetadata metadata, RoaringBitmap filterResults, Random random = null)
            {
                _indexSearcher = indexSearcher;
                _random = random ?? Random.Shared;
                _key = new();
                _key.Initialize(indexSearcher._transaction.LowLevelTransaction);
                var searchState = new Hnsw.SearchState(indexSearcher.Transaction.LowLevelTransaction, metadata.FieldName);
                _vectorsByHash = indexSearcher._transaction.CompactTreeFor(Hnsw.VectorsIdByHashSlice);
                _nodesByVectorId = searchState.NodeIdsByVectorId;
                _current = -1L;

                var filterCount = filterResults.Count;
                _isDone =
                    indexSearcher.TryGetRootPageByFieldName(metadata.FieldName, out _vectorRootPage) == false
                    || filterCount == 0;

                if (_isDone)
                {
                    _entryIds = Array.Empty<long>();
                    return;
                }

                // Materialize all entry IDs from the bitmap, then shuffle for random access
                _entryIds = new long[filterCount];
                var iterator = filterResults.GetIterator();
                Span<long> batch = stackalloc long[256];
                int totalRead = 0;
                int read;
                while ((read = filterResults.Fill(batch, ref iterator)) > 0)
                {
                    for (int i = 0; i < read && totalRead < _entryIds.Length; i++)
                        _entryIds[totalRead++] = batch[i];
                }
                iterator.Dispose();

                if (totalRead < _entryIds.Length)
                    Array.Resize(ref _entryIds, totalRead);

                _random.Shuffle(_entryIds.AsSpan());
                _entryIndex = 0;
            }

            public RandomNodesFromFilterEnumerator()
            {
                throw new NotSupportedException($"Default constructor is not supported for {nameof(RandomNodesFromFilterEnumerator)}");
            }

            public void Dispose()
            {
                _current = -1;
                _isDone = true;
                _key.Dispose();
            }

            public bool MoveNext()
            {
                if (_isDone)
                    return false;

                if (_results is not null && _results.Count > 0)
                {
                    _current = _results[^1];
                    _results.RemoveAt(_results.Count - 1);
                    return true;
                }

                _current = -1L;
                while (_entryIndex < _entryIds.Length)
                {
                    var entryId = _entryIds[_entryIndex++];

                    var entryTermsReader = _indexSearcher.GetEntryTermsReader(entryId, ref p, _key);
                    bool found = false;
                    while (entryTermsReader.FindNextStored(_vectorRootPage))
                    {
                        var vectorHash = entryTermsReader.StoredField.Value;
                        var vectorExists = _vectorsByHash.TryGetValue(vectorHash, out var vectorId);
                        Debug.Assert(vectorExists, "Vector hash not found in vectors by hash tree");
                        var nodeIdExists = _nodesByVectorId.TryGetValue(vectorId, out var nodeId);
                        Debug.Assert(nodeIdExists, "Node ID not found in nodes by vector ID tree");
                        found = true;
                        if (_current == -1L)
                        {
                            _current = nodeId;
                        }
                        else
                        {
                            _results ??= new();
                            _results.Add(nodeId);
                        }
                    }

                    if (found)
                        return true;
                }

                _isDone = true;
                return false;
            }

            public void Reset()
            {
                throw new NotSupportedException($"Reset is not supported for {nameof(RandomNodesFromFilterEnumerator)}");
            }

            public long Current => _current;

            object IEnumerator.Current
            {
                get => Current;
            }
        }

        public static bool TryConvertDocumentsIdsToNodesIds(IndexSearcher indexSearcher, in FieldMetadata metadata, ref RoaringBitmap filterResults, out ContextBoundNativeList<long> nodesIdsToScan)
        {
            var searchState = new Hnsw.SearchState(indexSearcher.Transaction.LowLevelTransaction, metadata.FieldName);
            var vectorsByHash = indexSearcher._transaction.CompactTreeFor(Hnsw.VectorsIdByHashSlice);
            var nodesByVectorId = searchState.NodeIdsByVectorId;
            if (indexSearcher.TryGetRootPageByFieldName(metadata.FieldName, out var vectorRootPage) is false)
            {
                nodesIdsToScan = default;
                return false;
            }

            nodesIdsToScan = new ContextBoundNativeList<long>(indexSearcher.Allocator);

            // Scan all entries from the filter to retrieve all node IDs. This is important for returning the correct number of results.
            // To satisfy the NumberOfCandidates requirement, we need to return up to `NumberOfCandidates` posting lists with the filter applied to the query.
            // Instead of building a mapping of nodes to matching posting lists (and distances),
            // we reuse ExactSearch mechanisms to find the nearest nodes to the vector (so we know that each node has at least one matching document)
            // and then filter the documents stored in the posting list of each node individually.
            // Ideally, each node represents only a single document.
            Page p = default;
            var iterator = filterResults.GetIterator();
            Span<long> batch = stackalloc long[256];
            int read;
            while ((read = filterResults.Fill(batch, ref iterator)) > 0)
            {
                for (int i = 0; i < read; i++)
                {
                    var entryTermsReader = indexSearcher.GetEntryTermsReader(batch[i], ref p);
                    while (entryTermsReader.FindNextStored(vectorRootPage))
                    {
                        var vectorHash = entryTermsReader.StoredField.Value;
                        if (vectorsByHash.TryGetValue(vectorHash, out var vectorId))
                        {
                            if (nodesByVectorId.TryGetValue(vectorId, out var nodeId))
                                nodesIdsToScan.Add(nodeId);
                        }
                    }
                }
            }
            iterator.Dispose();

            var uniqueCount = Sorting.SortAndRemoveDuplicates(nodesIdsToScan.ToSpan());
            nodesIdsToScan.Count = uniqueCount;

            if (nodesIdsToScan.Count == 0)
            {
                nodesIdsToScan.Dispose();
                nodesIdsToScan = default;
                return false;
            }

            return nodesIdsToScan.Count > 0;
        }
    }


    public VectorSearchMatch VectorSearch(in FieldMetadata metadata, in VectorValue vectorValue, float minimumMatch, in int numberOfCandidates, bool isExact, bool isSingleVectorSearch, IQueryMatch filterQuery = null, int scanningThreshold = 1024, Random random = null)
    {
        return new VectorSearchMatch(this, metadata, vectorValue, minimumMatch, numberOfCandidates, isExact, isSingleVectorSearch, filterQuery, scanningThreshold, random);
    }

    public IQueryMatch VectorSearch(in FieldMetadata metadata, in string documentId, float minimumMatch, in int numberOfCandidates, bool isExact, bool isSingleVectorSearch, IQueryMatch filterQuery = null, int scanningThreshold = 1024)
    {
        var idField = _fieldsTree.CompactTreeFor(_fieldMapping.GetByFieldId(Constants.IndexWriter.PrimaryKeyFieldId).FieldName);
        string loweredDocumentId = documentId.ToLowerInvariant();
        if (idField.TryGetValue(loweredDocumentId, out var rawId) is false ||
            TryGetRootPageByFieldName(metadata.FieldName, out var vectorRootPage) is false)
            return EmptyMatch();
        var vectorsByHash = _transaction.CompactTreeFor(Hnsw.VectorsIdByHashSlice);
        PortableExceptions.ThrowIf<InvalidOperationException>((rawId & (long)TermIdMask.EnsureIsSingleMask) != (long)TermIdMask.Single,
            "The provided id must be a document id mapped to a single value, but got: " + documentId + ", which maps to: " + rawId);

        Page page = default;
        var singleEntryId = EntryIdEncodings.GetContainerId(rawId);
        var reader = GetEntryTermsReader((long)singleEntryId, ref page);

        var searchState = new Hnsw.SearchState(_transaction.LowLevelTransaction, metadata.FieldName);

        if (reader.FindNextStored(vectorRootPage) is false)
            return EmptyMatch();

        PortableExceptions.ThrowIf<InvalidOperationException>(reader.IsVectorHash is false, "Expected vector field, but got " + metadata.FieldName + ", which isn't a vector");

        Span<byte> hash = reader.StoredField.Value.ToSpan();
        if (vectorsByHash.TryGetValue(hash, out var vectorId) is false)
            return EmptyMatch();

        var vectorSpan = Hnsw.NodeReader.ReadVector(vectorId, searchState);

        var vectorValue = new VectorValue(null, vectorSpan.AsMemory());
        if (reader.FindNextStored(vectorRootPage) is false) // just a single vector
            return new VectorSearchMatch(this, metadata, vectorValue, minimumMatch, numberOfCandidates, isExact, isSingleVectorSearch, filterQuery, scanningThreshold);

        List<VectorValue> vectors = [vectorValue];
        do
        {
            vectorSpan = Hnsw.NodeReader.ReadVector(vectorId, searchState);
            vectorValue = new VectorValue(null, vectorSpan.AsMemory());
            vectors.Add(vectorValue);
        } while (reader.FindNextStored(vectorRootPage));

        return new MultiVectorSearchMatch(this, metadata, vectors.ToArray(), minimumMatch, numberOfCandidates, isExact, isSingleVectorSearch, filterQuery, scanningThreshold);
    }

    public MultiVectorSearchMatch MultiVectorSearch(in FieldMetadata metadata, in VectorValue[] vectorValues, float minimumMatch, in int numberOfCandidates, bool isExact, bool isSingleVectorSearch, IQueryMatch filterQuery = null, int scanningThreshold = 1024, Random random = null)
        => new(this, metadata, vectorValues, minimumMatch, numberOfCandidates, isExact, isSingleVectorSearch, filterQuery, scanningThreshold, random);
}
