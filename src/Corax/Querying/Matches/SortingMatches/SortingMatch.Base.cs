using System;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.SortingMatches.Meta;

namespace Corax.Querying.Matches.SortingMatches;

/// <summary>
/// Non-generic abstract base for sorting matches (single-field and multi-field).
/// Lets callers in <c>CoraxIndexReadOperation</c> pattern-match on <c>SortingMatch</c>
/// without referencing the <c>TInner</c> type parameter and read
/// <see cref="TotalResults"/> after sorting completes.
/// </summary>
public abstract class SortingMatch : IQueryMatch, IDisposable
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

    public abstract bool IsBoosting { get; }
    public abstract DuplicatesOccurrence DuplicatesOccurrenceStatus { get; }
    public abstract long Count { get; }
    public abstract QueryCountConfidence Confidence { get; }
    public abstract int Fill(Span<long> buffer);
    public abstract int AndWith(Span<long> buffer, int matches);
    public abstract void Score(Span<long> matches, Span<float> scores, float boostFactor);
    public abstract QueryInspectionNode Inspect();
    public abstract void SetSortingDataTransfer(in SortingDataTransfer sortingDataTransfer);
    public abstract void Dispose();
}
