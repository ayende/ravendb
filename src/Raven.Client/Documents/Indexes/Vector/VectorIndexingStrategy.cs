namespace Raven.Client.Documents.Indexes.Vector;

/// <summary>
/// Defines the structure used to index the vectors of a field.
/// </summary>
public enum VectorIndexingStrategy
{
    /// <summary>
    /// Hierarchical navigable small world graph.
    /// </summary>
    Hnsw,

    /// <summary>
    /// Vectors are clustered into partitions around centroids. A query scans the nearest partitions.
    /// </summary>
    Centroids
}
