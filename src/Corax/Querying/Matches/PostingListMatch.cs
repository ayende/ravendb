using System;
using Corax.Querying.Matches.Meta;
using Corax.Utils;
using Voron.Data.PostingLists;

namespace Corax.Querying.Matches;

/// <summary>
/// A thin IQueryMatch wrapper around a PostingList that bypasses TermMatch.
/// Uses PostingList.Iterator.Fill() directly, decoding entry ID frequencies
/// in bulk. Faster than TermMatch for bitmap materialization since it avoids
/// the TermMatch function-pointer dispatch and per-entry encoding logic.
/// </summary>
public struct PostingListMatch : IQueryMatch
{
    private readonly PostingList _postingList;
    private PostingList.Iterator _iterator;
    private readonly long _count;
    private bool _initialized;

    public PostingListMatch(PostingList postingList, long count)
    {
        _postingList = postingList;
        _iterator = postingList.Iterate();
        _count = count;
        _initialized = true;
    }

    public long Count => _count;
    public QueryCountConfidence Confidence => QueryCountConfidence.High;
    public bool IsBoosting => false;
    public DuplicatesOccurrence DuplicatesOccurrenceStatus => DuplicatesOccurrence.NotPossible;

    public int Fill(Span<long> matches)
    {
        if (!_initialized)
            return 0;

        if (!_iterator.Fill(matches, out int read))
        {
            if (read > 0)
            {
                EntryIdEncodings.DecodeAndDiscardFrequency(matches, read);
                return read;
            }
            return 0;
        }

        if (read > 0)
            EntryIdEncodings.DecodeAndDiscardFrequency(matches, read);

        return read;
    }

    public int AndWith(Span<long> buffer, int matches)
    {
        // Not optimized — fall back to standard pattern
        return matches;
    }

    public void Score(Span<long> matches, Span<float> scores, float boostFactor)
    {
        // No scoring in direct posting list path
    }

    public QueryInspectionNode Inspect()
    {
        return new QueryInspectionNode(nameof(PostingListMatch));
    }

    public SkipSortingResult AttemptToSkipSorting() => SkipSortingResult.ResultsNativelySorted;
}
