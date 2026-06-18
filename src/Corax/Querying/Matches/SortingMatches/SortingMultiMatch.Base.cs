using System;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.SortingMatches.Meta;

namespace Corax.Querying.Matches.SortingMatches;

/// <summary>
/// Non-generic abstract base for multi-field (ORDER BY ..., ...) sorting matches.
/// Mirrors <see cref="SortingMatch"/> for the multi-comparator case so callers can
/// pattern-match without referencing the <c>TInner</c> type parameter.
/// </summary>
public abstract class SortingMultiMatch : IQueryMatch, IDisposable, IRequireSortingDataTransfer
{
    /// <summary>Total number of matching entries (set after the first Fill call).</summary>
    public long TotalResults;

    /// <summary>True when the candidate batch is sorted ascending by entry id — i.e. it came from the
    /// bitmap-backed materialization (the bitmap iterator yields in order) rather than the non-bitmap drain.
    /// The score comparer uses this to take the sorted-aware <see cref="IQueryMatch.ScoreSorted"/> fast path.</summary>
    internal bool CandidatesAreSorted;

    /// <summary>Wall-clock ticks spent on sort-specific work (the multi-comparer heap sort). Excludes the inner
    /// match's execution, which is timed onto the child CompiledQuery node's per-op telemetry; counting it here
    /// too would double-count the query. <see cref="SortingMultiMatch{TInner}.Inspect"/> emits this as the sort
    /// node's "Ms" so include timings() can attribute the sort cost that sits above the bitmap pipeline.</summary>
    public long SortingTimeInTicks;

    public abstract bool IsBoosting { get; }
    public abstract DuplicatesOccurrence DuplicatesOccurrenceStatus { get; }
    public abstract long Count { get; }
    public abstract QueryCountConfidence Confidence { get; }
    public abstract int Fill(Span<long> buffer);
    public abstract int AndWith(Span<long> buffer, int matches);
    public abstract void Score(Span<long> matches, Span<float> scores, float boostFactor);
    // Top-level sort: its own Score is a no-op, so ScoreSorted just mirrors it.
    public void ScoreSorted(Span<long> matches, Span<float> scores, float boostFactor) => Score(matches, scores, boostFactor);
    public abstract QueryInspectionNode Inspect();
    public abstract void SetSortingDataTransfer(in SortingDataTransfer sortingDataTransfer);
    public abstract void Dispose();
}
