using System;
using Corax.Querying.Matches.Meta;

namespace Corax.Querying.Matches;

/// <summary>
/// Chains an inner IQueryMatch through one or more additional filter matches.
/// On Fill(), retrieves entries from the inner match, then applies each
/// filter's AndWith() to narrow the results. Currently only used to apply
/// spatial predicates after the bitmap filter phase builds the candidate set,
/// but the construct itself is just an AndWith-chain — nothing here is
/// spatial-specific.
/// </summary>
public struct PostFilterMatch : IQueryMatch
{
    private readonly IQueryMatch _inner;
    private readonly IQueryMatch[] _postFilters;

    public PostFilterMatch(IQueryMatch inner, IQueryMatch[] postFilters)
    {
        _inner = inner;
        _postFilters = postFilters;
    }

    public long Count => _inner.Count;
    public QueryCountConfidence Confidence => _inner.Confidence;
    public bool IsBoosting => _inner.IsBoosting;
    public DuplicatesOccurrence DuplicatesOccurrenceStatus => _inner.DuplicatesOccurrenceStatus;


    public int Fill(Span<long> matches)
    {
        int read = _inner.Fill(matches);
        if (read == 0)
            return 0;

        for (int i = 0; i < _postFilters.Length; i++)
        {
            read = _postFilters[i].AndWith(matches, read);
            if (read == 0)
                return 0;
        }

        return read;
    }

    public int AndWith(Span<long> buffer, int matches)
    {
        // First apply inner's AndWith, then each post-filter in order.
        int count = _inner.AndWith(buffer, matches);
        if (count == 0)
            return 0;

        for (int i = 0; i < _postFilters.Length; i++)
        {
            count = _postFilters[i].AndWith(buffer, count);
            if (count == 0)
                return 0;
        }

        return count;
    }

    public void Score(Span<long> matches, Span<float> scores, float boostFactor)
    {
        _inner.Score(matches, scores, boostFactor);
        for (int i = 0; i < _postFilters.Length; i++)
            _postFilters[i].Score(matches, scores, boostFactor);
    }

    public QueryInspectionNode Inspect()
    {
        var children = new System.Collections.Generic.List<QueryInspectionNode>
        {
            _inner.Inspect()
        };
        for (int i = 0; i < _postFilters.Length; i++)
            children.Add(_postFilters[i].Inspect());

        return new QueryInspectionNode(nameof(PostFilterMatch), children: children);
    }

}
