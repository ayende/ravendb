using System;
using System.Runtime.CompilerServices;
using Sparrow.Server.Collections;
using Voron.Data.Graphs;

namespace Corax.Querying.Matches;

/// <summary>
/// The result stream of a vector search, served either by the HNSW graph or by the centroids index of the field.
/// </summary>
internal struct VectorRetriever : IDisposable
{
    private Hnsw.VectorSearchRetriever _hnsw;
    private Centroids.VectorSearchRetriever _centroids;
    private readonly bool _isCentroids;

    public VectorRetriever(Hnsw.VectorSearchRetriever hnsw)
    {
        _hnsw = hnsw;
        _isCentroids = false;
    }

    public VectorRetriever(Centroids.VectorSearchRetriever centroids)
    {
        _centroids = centroids;
        _isCentroids = true;
    }

    public bool IsEmpty => _isCentroids ? _centroids.IsEmpty : _hnsw.IsEmpty;

    public bool IsSortedByDistance => _isCentroids ? _centroids.IsSortedByDistance : _hnsw.IsSortedByDistance;

    public Hnsw.SimilarityMethod? SimilarityMethod => _isCentroids ? _centroids.SimilarityMethod : _hnsw.SimilarityMethod;

    public long CandidatesProcessed => _isCentroids ? _centroids.CandidatesProcessed : _hnsw.CandidatesProcessed;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public int Fill(Span<long> matches, Span<float> distances, GrowableBitArray? filter) =>
        _isCentroids ? _centroids.Fill(matches, distances, filter) : _hnsw.Fill(matches, distances, filter);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public float DistanceToScore(float distance) => _isCentroids ? _centroids.DistanceToScore(distance) : _hnsw.DistanceToScore(distance);

    public void DistancesToScores(Span<float> distances)
    {
        if (_isCentroids)
            _centroids.DistancesToScores(distances);
        else
            _hnsw.DistancesToScores(distances);
    }

    public void Dispose()
    {
        if (_isCentroids)
            _centroids.Dispose();
        else
            _hnsw.Dispose();
    }
}
