using System;
using Corax.Querying.Matches.Meta;

namespace Corax.Querying.Matches;

/// <summary>
/// Chains an inner IQueryMatch through one or more spatial filter matches.
/// On Fill(), retrieves entries from the inner match, then applies each
/// spatial match's AndWith() to narrow the results. This implements the
/// post-filter phase where spatial predicates are applied after the bitmap
/// filter phase builds the candidate set.
/// </summary>
public struct PostFilterMatch : IQueryMatch
{
    private readonly IQueryMatch _inner;
    private readonly IQueryMatch[] _spatialFilters;

    public PostFilterMatch(IQueryMatch inner, IQueryMatch[] spatialFilters)
    {
        _inner = inner;
        _spatialFilters = spatialFilters;
    }

    public long Count => _inner.Count;
    public QueryCountConfidence Confidence => _inner.Confidence;
    public bool IsBoosting => _inner.IsBoosting;
    public DuplicatesOccurrence DuplicatesOccurrenceStatus => DuplicatesOccurrence.NotPossible;

    public int Fill(Span<long> matches)
    {
        int read = _inner.Fill(matches);
        if (read == 0)
            return 0;

        for (int i = 0; i < _spatialFilters.Length; i++)
        {
            read = _spatialFilters[i].AndWith(matches, read);
            if (read == 0)
                return 0;
        }

        return read;
    }

    public int AndWith(Span<long> buffer, int matches)
    {
        // First apply inner's AndWith, then each spatial filter
        int count = _inner.AndWith(buffer, matches);
        if (count == 0)
            return 0;

        for (int i = 0; i < _spatialFilters.Length; i++)
        {
            count = _spatialFilters[i].AndWith(buffer, count);
            if (count == 0)
                return 0;
        }

        return count;
    }

    public void Score(Span<long> matches, Span<float> scores, float boostFactor)
    {
        _inner.Score(matches, scores, boostFactor);
        for (int i = 0; i < _spatialFilters.Length; i++)
            _spatialFilters[i].Score(matches, scores, boostFactor);
    }

    public QueryInspectionNode Inspect()
    {
        var children = new System.Collections.Generic.List<QueryInspectionNode>
        {
            _inner.Inspect()
        };
        for (int i = 0; i < _spatialFilters.Length; i++)
            children.Add(_spatialFilters[i].Inspect());

        return new QueryInspectionNode(nameof(PostFilterMatch), children: children);
    }

    public SkipSortingResult AttemptToSkipSorting() => _inner.AttemptToSkipSorting();
}
