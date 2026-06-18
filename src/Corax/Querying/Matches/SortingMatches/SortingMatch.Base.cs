using System;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.SortingMatches.Meta;

namespace Corax.Querying.Matches.SortingMatches;

/// <summary>The concrete strategy a <see cref="SortingMatch"/> used to produce the sorted result set.
/// Surfaced as "Strategy" in the query plan graph and the value a query may pin via the reserved
/// <c>$rvn_corax_sort</c> parameter. Pinning is honored only where a runtime choice actually exists
/// (the <see cref="InMemorySort"/> vs <see cref="IndexOrderStreaming"/> decision on an iterable sort
/// index); a pin that cannot apply to the query shape is ignored, mirroring <c>$rvn_corax_strategy</c>.</summary>
public enum CoraxSortingStrategy : byte
{
    /// <summary>Random ORDER BY: reservoir-sample entry ids straight from the candidate bitmap.</summary>
    RandomOrder,

    /// <summary>Materialize the whole candidate set and heap-sort it. The always-bounded choice: used for
    /// score/spatial/alphanumeric sorts and missing-entry sorts (no order-preserving index to walk), and
    /// whenever the cost gate decides walking the index would read more than it saves.</summary>
    InMemorySort,

    /// <summary>Walk the sort index in order, intersecting each batch against the candidate bitmap and
    /// stopping once the page limit is filled. Wins when the candidates are dense in the index and the
    /// limit is small.</summary>
    IndexOrderStreaming,

    /// <summary>Started <see cref="IndexOrderStreaming"/> but the walk over-scanned the index for too few
    /// hits, so it abandoned the walk and re-sorted the candidates via <see cref="InMemorySort"/>. The
    /// wasted scan stays visible as EntriesStreamed so the degenerate case is legible in the plan graph.</summary>
    IndexOrderFallbackToInMemorySort,

    /// <summary>Non-bitmap inner match (vector/spatial/boosting/scoring): drain it via Fill, then heap-sort.</summary>
    ComputedResultsSort,
}

/// <summary>
/// Non-generic abstract base for sorting matches (single-field and multi-field).
/// Lets callers in <c>CoraxIndexReadOperation</c> pattern-match on <c>SortingMatch</c>
/// without referencing the <c>TInner</c> type parameter and read
/// <see cref="TotalResults"/> after sorting completes.
/// </summary>
public abstract class SortingMatch : IQueryMatch, IDisposable, IRequireSortingDataTransfer
{
    public const int SortBatchSize = 8192;

    /// <summary>
    /// Per-thread byte buffer for UTF-8 encode in SliceEqualsUtf8.
    /// Lives here (non-generic base) so all closed SortingMatch&lt;TInner&gt; instantiations
    /// share one buffer per thread instead of one per generic instantiation per thread.
    /// Grown to the next power-of-two size on demand.
    /// </summary>
    [ThreadStatic]
    internal static byte[] Utf8ThreadBuffer;

    /// <summary>Total number of matching entries (set after the first Fill call).</summary>
    public long TotalResults;

    /// <summary>Wall-clock ticks spent on sort-specific work — heap-sorting the candidates or walking the
    /// sort index in order. Excludes the inner match's execution (the compiled bitmap pipeline), which is
    /// timed onto the child CompiledQuery node's per-op telemetry; counting it here too would double-count
    /// the query as sort time. <see cref="Inspect"/> emits this as the sort node's "Ms" so include timings().</summary>
    public long SortingTimeInTicks;

    /// <summary>The sort strategy that actually ran this query. Set lazily on the first Fill;
    /// null until then. Surfaced as "Strategy" in the query plan graph.</summary>
    public CoraxSortingStrategy? SortStrategy;

    /// <summary>True when the candidate batch handed to the score comparer is sorted ascending by entry id —
    /// i.e. it came from the bitmap-backed <c>SortInMemory</c> path (the bitmap iterator yields in order). The
    /// non-bitmap <c>SortComputedResults</c> drain (vector / post-filter, score-ordered) leaves it false. The score
    /// comparer uses this to call the sorted-aware <see cref="IQueryMatch.ScoreSorted"/> fast path only when valid.</summary>
    internal bool CandidatesAreSorted;

    /// <summary>Sort strategy pinned by the reserved <c>$rvn_corax_sort</c> query parameter, or null to
    /// let the runtime cost gate choose. Honored only for the InMemorySort vs IndexOrderStreaming choice
    /// on an iterable sort index; forcing IndexOrderStreaming also suppresses the over-scan bailout.</summary>
    public CoraxSortingStrategy? ForcedStrategy;

    /// <summary>For the streaming strategy only: how many entry IDs were read from the sort index and
    /// intersected against the candidate set. A value far larger than the result count is the signature
    /// of a degenerate stream-and-intersect — a tiny/scattered candidate set forces a near-full scan of
    /// the sort index, which is exactly the cost the streaming strategy is supposed to avoid.</summary>
    public long EntriesStreamed;

    public abstract bool IsBoosting { get; }
    public abstract DuplicatesOccurrence DuplicatesOccurrenceStatus { get; }
    public abstract long Count { get; }
    public abstract QueryCountConfidence Confidence { get; }
    public abstract int Fill(Span<long> buffer);
    public abstract int AndWith(Span<long> buffer, int matches);
    public abstract void Score(Span<long> matches, Span<float> scores, float boostFactor);
    // A SortingMatch is never nested inside another match's score chain (it is the top-level sort), so its own
    // Score is a no-op; ScoreSorted just mirrors it.
    public void ScoreSorted(Span<long> matches, Span<float> scores, float boostFactor) => Score(matches, scores, boostFactor);
    public abstract QueryInspectionNode Inspect();
    public abstract void SetSortingDataTransfer(in SortingDataTransfer sortingDataTransfer);
    public abstract void Dispose();
}

public interface IRequireSortingDataTransfer
{
    void SetSortingDataTransfer(in SortingDataTransfer sortingDataTransfer);
}
