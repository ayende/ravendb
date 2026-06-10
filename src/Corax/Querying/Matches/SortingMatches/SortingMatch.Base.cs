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

    /// <summary>Wall-clock ticks spent producing the sorted result set, accumulated across Fill calls.
    /// <see cref="Inspect"/> emit it as the sort node's "Ms" so include timings().</summary>
    public long SortingTimeInTicks;

    /// <summary>Name of the sort strategy that actually ran this query (stream-and-intersect,
    /// extract-and-sort, reservoir-sample, drain-and-sort). Set lazily on the first Fill.</summary>
    public string SortStrategy;

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
    public abstract QueryInspectionNode Inspect();
    public abstract void SetSortingDataTransfer(in SortingDataTransfer sortingDataTransfer);
    public abstract void Dispose();
}

public interface IRequireSortingDataTransfer
{
    void SetSortingDataTransfer(in SortingDataTransfer sortingDataTransfer);
}
