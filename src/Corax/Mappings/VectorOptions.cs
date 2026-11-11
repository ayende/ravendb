using Voron.Data.Graphs;

namespace Corax.Mappings;

public class VectorOptions
{
    public int NumberOfCandidates { get; init; }
    public int NumberOfEdges { get; init; }
    public VectorEmbeddingType VectorEmbeddingType { get; init; }
    public VectorIndexingStrategy IndexingStrategy { get; init; }
    public int CentroidsMaxPartitionSize { get; init; } = Centroids.DefaultMaxPartitionSize;
    public int CentroidsQuantizationBits { get; init; } = Centroids.DefaultQuantizationBits;
}
