namespace Corax.Querying.Matches.Meta;

/// <summary>Implemented by a per-entry post-filter (spatial) that can restrict its own evaluation to a candidate
/// set. When <see cref="FilterQuery"/> is set the match drives off those candidates and tests only them, instead
/// of enumerating its full result — mirroring the vector search's filter query. Used by
/// <c>NegatedPostFilterMatch</c> to scope a negated clause to the candidate universe it subtracts from.</summary>
public interface ISpatialFilterQuery
{
    IQueryMatch FilterQuery { get; set; }
}
