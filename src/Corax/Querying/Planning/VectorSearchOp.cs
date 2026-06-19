namespace Corax.Querying.Planning;

/// <summary>Vector nearest-neighbor search applied after the bitmap filter phase.
/// The bitmap-producing match is passed as the filter source to VectorSearchMatch,
/// restricting the search to the candidate set.</summary>
public struct VectorSearchOp
{
    public ClauseInfo Clause;
    public ClauseExecution Exec;
}
