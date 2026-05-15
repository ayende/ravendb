using System;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.SortingMatches.Meta;

namespace Corax.Querying.Matches.SortingMatches;

/// <summary>
/// Abstract base for single-field sorting matches. Allows callers in
/// <c>CoraxIndexReadOperation</c> to pattern-match on <c>SortingMatch</c> without knowing
/// the <c>TInner</c> type parameter, and to read <see cref="TotalResults"/> after sorting.
///
/// Architecture note: the original design used a non-generic erasure struct that stored a
/// <c>delegate*</c> function pointer per operation, routing every <c>Fill</c> call through a
/// direct (non-virtual) pointer. That avoided vtable dispatch on the hot path at the cost of
/// considerable boilerplate. The current design uses a plain abstract class: <c>Fill</c> and
/// the other <c>IQueryMatch</c> members dispatch through the interface vtable when called via
/// the <c>IQueryMatch queryMatch</c> variable in the read operation. Because the concrete
/// subclass (<see cref="SortingMatch{TInner}"/>) is <c>sealed</c>, the JIT can devirtualize
/// those calls at sites where the concrete type is statically known, but not at the
/// <c>IQueryMatch</c> call site in <c>CoraxIndexReadOperation</c>. The trade-off is simpler
/// code with one interface dispatch per <c>Fill</c> batch instead of one function-pointer
/// indirection.
///
/// Sharing note: <see cref="SortingMatch{TInner}"/> and <see cref="SortingMultiMatch{TInner}"/>
/// have similar Fill/Sort patterns, but their <c>IEntryComparer</c> interfaces differ (single-field
/// vs multi-field with 3 generic comparer parameters) and the methods take their own concrete match
/// type as a parameter for JIT specialization. Extracting shared code here would require either
/// virtualizing field access (vtable cost on the hot path) or a large comparer refactor with no
/// runtime benefit. Keep the implementations side-by-side until a comparer unification is done.
/// </summary>
public abstract class SortingMatch : IQueryMatch
{
    public const int SortBatchSize = 8192;

    /// <summary>Total number of matching entries (set after the first Fill call).</summary>
    public long TotalResults;

    public abstract bool IsBoosting { get; }
    public abstract DuplicatesOccurrence DuplicatesOccurrenceStatus { get; }
    public abstract long Count { get; }
    public abstract QueryCountConfidence Confidence { get; }
    public abstract int Fill(Span<long> buffer);
    public abstract int AndWith(Span<long> buffer, int matches);
    public abstract void Score(Span<long> matches, Span<float> scores, float boostFactor);
    public abstract QueryInspectionNode Inspect();
    public abstract void SetScoreAndDistanceBuffer(in SortingDataTransfer sortingDataTransfer);
}
