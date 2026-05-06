using System;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.SortingMatches.Meta;

namespace Corax.Querying.Matches.SortingMatches;

/// <summary>
/// Abstract base for multi-field (ORDER BY ... , ...) sorting matches. Mirrors
/// <see cref="SortingMatch"/> for the multi-comparator case.
///
/// Architecture note: see <see cref="SortingMatch"/> for the rationale. The same
/// erasure-struct → abstract-class trade-off applies here: <c>Fill</c> dispatches
/// through the <c>IQueryMatch</c> vtable at the call site in <c>CoraxIndexReadOperation</c>,
/// while the <c>sealed</c> concrete subclass (<see cref="SortingMultiMatch{TInner}"/>)
/// allows the JIT to devirtualize where the concrete type is statically visible.
/// </summary>
public abstract class SortingMultiMatch : IQueryMatch
{
    /// <summary>Total number of matching entries (set after the first Fill call).</summary>
    public long TotalResults;

    public abstract bool IsBoosting { get; }
    public abstract DuplicatesOccurrence DuplicatesOccurrenceStatus { get; }
    public abstract long Count { get; }
    public abstract QueryCountConfidence Confidence { get; }
    public abstract SkipSortingResult AttemptToSkipSorting();
    public abstract int Fill(Span<long> buffer);
    public abstract int AndWith(Span<long> buffer, int matches);
    public abstract void Score(Span<long> matches, Span<float> scores, float boostFactor);
    public abstract QueryInspectionNode Inspect();
    public abstract void SetSortingDataTransfer(in SortingDataTransfer sortingDataTransfer);
}
