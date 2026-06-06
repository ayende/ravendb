using System;

namespace Corax.Querying.Matches.Meta;

public static class QueryMatch
{
    public const long Invalid = -1;
    public const long Start = 0;
}

public enum QueryCountConfidence : int
{
    Low = 0,
    Normal = 1,
    High = 2,
}

public interface IQueryMatch
{
    long Count { get; }
    
        
    // The confidence of the query count.
    //  - High: We know exactly how many items there are.
    //  - Normal: We know roughly that it is in the order of magnitude.
    //  - Low: We know very little about it.
    QueryCountConfidence Confidence { get; }

    bool IsBoosting { get; }

    // Guarantees: The output of Fill will be sorted and deduplicated for the call.
    //             Different calls to Fill may return identical values are not guaranteed to be sorted between calls.
    //             0 return means no more matches. 
    int Fill(Span<long> matches);

    // Guarantees: AndWith accepts sorted and returns sorted.
    //             May optimize for continued sorted.
    //             0 return means no more matches from the provided span, and may need to go to the next batch
    // Requirements: Cannot be called with .Fill() from same instance.
    int AndWith(Span<long> buffer, int matches);

    // Guarantees: The output of this for unscored sequences should be a no-op.
    // Requirements: The upmost call 
    void Score(Span<long> matches, Span<float> scores, float boostFactor);

    QueryInspectionNode Inspect();

    string DebugView => Inspect().ToString();
    
    DuplicatesOccurrence DuplicatesOccurrenceStatus { get; }
}

/// <summary>
/// Implemented by query matches backed by a RoaringBitmap, enabling SortingMatch
/// to walk the CompactTree index and intersect batches via AndWith, stopping early
/// when the LIMIT is reached — no full materialization needed.
/// </summary>
public interface IBitmapQueryMatch : IQueryMatch
{
    bool Contains(long entryId);
    long MinEntryId { get; }
    long MaxEntryId { get; }

    /// <summary>
    /// Returns a reference to the underlying bitmap data. The caller MUST NOT dispose it.
    /// Used by downstream consumers (vector search filter, faceted lookups) to skip re-materialization.
    /// </summary>
    [System.Diagnostics.CodeAnalysis.UnscopedRef]
    ref Voron.Data.RoaringBitmaps.RoaringBitmap BitmapState { get; }
}

public enum DuplicatesOccurrence
{
    Possible,
    NotPossible
}

/// <summary>
/// Implemented by the per-entry post-filter match families (spatial / vector). The flag is NOT intrinsic to the
/// match type: the same match is a top-level post-filter when the planner lifted it out of an AND, but an ordinary
/// pipeline leaf when it sits inside an OR branch (the planner only lifts in an AND context — see
/// <c>PlanWalker.GroupCollapse</c>). The locus that actually applies post-filters (<c>QueryPlanBuilder.ApplyPostFilters</c>)
/// sets the flag on the matches it wraps, so inspection reads the recorded role instead of re-deriving it from the type.
/// </summary>
public interface IPostFilterMatch
{
    bool IsPostFilter { get; set; }
}
