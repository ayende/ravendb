using System;
using System.Collections.Generic;
using System.Diagnostics;
using Corax.Querying.Matches.Meta;

namespace Corax.Querying.Matches;

/// <summary>
/// Chains an inner IQueryMatch through one or more additional filter matches.
/// On Fill(), retrieves entries from the inner match, then applies each
/// filter's AndWith() to narrow the results. Currently only used to apply
/// spatial predicates after the bitmap filter phase builds the candidate set,
/// but the construct itself is just an AndWith-chain — nothing here is
/// spatial-specific.
///
/// Records per-call wall time for the inner Fill and for each post-filter
/// AndWith step, plus per-step survivor counts. Surfaced via <see cref="Inspect"/>
/// so spatial query introspection JSON shows where time and rejection happen.
/// Stored on the heap (class, not struct) because the counters must survive
/// across the boxing performed when this match is assigned to <c>IQueryMatch</c>.
/// </summary>
public sealed class PostFilterMatch : IQueryMatch
{
    private readonly IQueryMatch _inner;
    private readonly IQueryMatch[] _postFilters;

    // Wall-clock ticks (Stopwatch.GetTimestamp()) spent inside the inner Fill / AndWith.
    private long _innerTicks;
    // Cumulative number of entries the inner returned across all calls.
    private long _innerEmitted;

    // Per-post-filter cumulative ticks spent inside that filter's AndWith.
    private readonly long[] _filterTicks;
    // Per-post-filter cumulative survivor count (after that filter ran).
    private readonly long[] _filterSurvivors;
    // Per-post-filter cumulative rejection count: input - survivors aggregated across calls.
    private readonly long[] _filterRejected;

    public PostFilterMatch(IQueryMatch inner, IQueryMatch[] postFilters)
    {
        _inner = inner;
        _postFilters = postFilters;
        _filterTicks = new long[postFilters.Length];
        _filterSurvivors = new long[postFilters.Length];
        _filterRejected = new long[postFilters.Length];
    }

    public long Count => _inner.Count;
    public QueryCountConfidence Confidence => _inner.Confidence;
    public bool IsBoosting => _inner.IsBoosting;
    public DuplicatesOccurrence DuplicatesOccurrenceStatus => _inner.DuplicatesOccurrenceStatus;


    public int Fill(Span<long> matches)
    {
        long t0 = Stopwatch.GetTimestamp();
        int read = _inner.Fill(matches);
        _innerTicks += Stopwatch.GetTimestamp() - t0;
        _innerEmitted += read;

        if (read == 0)
            return 0;

        return ApplyPostFilters(matches, read);
    }

    public int AndWith(Span<long> buffer, int matches)
    {
        // First apply inner's AndWith, then each post-filter in order.
        long t0 = Stopwatch.GetTimestamp();
        int count = _inner.AndWith(buffer, matches);
        _innerTicks += Stopwatch.GetTimestamp() - t0;
        _innerEmitted += count;

        if (count == 0)
            return 0;

        return ApplyPostFilters(buffer, count);
    }

    private int ApplyPostFilters(Span<long> buffer, int count)
    {
        for (int i = 0; i < _postFilters.Length; i++)
        {
            int input = count;
            long ti = Stopwatch.GetTimestamp();
            count = _postFilters[i].AndWith(buffer, count);
            _filterTicks[i] += Stopwatch.GetTimestamp() - ti;
            _filterSurvivors[i] += count;
            _filterRejected[i] += input - count;
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
        double tickFreq = Stopwatch.Frequency / 1000.0;
        var parameters = new Dictionary<string, string>();

        if (_innerTicks > 0)
            parameters["Inner_ms"] = (_innerTicks / tickFreq).ToString("F3");
        parameters["InnerEmitted"] = _innerEmitted.ToString();

        for (int i = 0; i < _postFilters.Length; i++)
        {
            string prefix = $"Filter[{i}]_";
            if (_filterTicks[i] > 0)
                parameters[prefix + "ms"] = (_filterTicks[i] / tickFreq).ToString("F3");
            parameters[prefix + "kept"] = _filterSurvivors[i].ToString();
            parameters[prefix + "rejected"] = _filterRejected[i].ToString();
        }

        var children = new List<QueryInspectionNode>(_postFilters.Length + 1)
        {
            _inner.Inspect()
        };
        for (int i = 0; i < _postFilters.Length; i++)
            children.Add(_postFilters[i].Inspect());

        return new QueryInspectionNode(nameof(PostFilterMatch), parameters: parameters, children: children);
    }
}
