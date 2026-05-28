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
/// When timing capture is enabled (introspection JSON requested by the caller),
/// records per-call wall time for the inner Fill and for each post-filter
/// AndWith step, plus per-step survivor counts. When disabled, the per-call
/// rdtsc + counter writes are skipped entirely. Stored on the heap (class,
/// not struct) because the counters must survive across the boxing performed
/// when this match is assigned to <c>IQueryMatch</c>.
/// </summary>
public sealed class PostFilterMatch : IQueryMatch
{
    private readonly IQueryMatch _inner;
    private readonly IQueryMatch[] _postFilters;

    // Wall-clock ticks (Stopwatch.GetTimestamp()) spent inside the inner Fill / AndWith.
    // Null when timing capture is disabled.
    private readonly long[] _innerTicks;
    // Cumulative number of entries the inner returned across all calls.
    // Null when timing capture is disabled.
    private readonly long[] _innerEmitted;

    // Per-post-filter cumulative ticks spent inside that filter's AndWith.
    // Null when timing capture is disabled.
    private readonly long[] _filterTicks;
    // Per-post-filter cumulative survivor count (after that filter ran).
    // Null when timing capture is disabled.
    private readonly long[] _filterSurvivors;
    // Per-post-filter cumulative rejection count: input - survivors aggregated across calls.
    // Null when timing capture is disabled.
    private readonly long[] _filterRejected;

    public PostFilterMatch(IQueryMatch inner, IQueryMatch[] postFilters, bool wantTimings)
    {
        _inner = inner;
        _postFilters = postFilters;
        if (wantTimings)
        {
            _innerTicks = new long[1];
            _innerEmitted = new long[1];
            _filterTicks = new long[postFilters.Length];
            _filterSurvivors = new long[postFilters.Length];
            _filterRejected = new long[postFilters.Length];
        }
    }

    public long Count => _inner.Count;
    public QueryCountConfidence Confidence => _inner.Confidence;
    public bool IsBoosting => _inner.IsBoosting;
    public DuplicatesOccurrence DuplicatesOccurrenceStatus => _inner.DuplicatesOccurrenceStatus;


    public int Fill(Span<long> matches)
    {
        if (_innerTicks is null)
        {
            int r = _inner.Fill(matches);
            return r == 0 ? 0 : ApplyPostFilters(matches, r);
        }

        long t0 = Stopwatch.GetTimestamp();
        int read = _inner.Fill(matches);
        _innerTicks[0] += Stopwatch.GetTimestamp() - t0;
        _innerEmitted[0] += read;

        if (read == 0)
            return 0;

        return ApplyPostFilters(matches, read);
    }

    public int AndWith(Span<long> buffer, int matches)
    {
        if (_innerTicks is null)
        {
            int c = _inner.AndWith(buffer, matches);
            return c == 0 ? 0 : ApplyPostFilters(buffer, c);
        }

        long t0 = Stopwatch.GetTimestamp();
        int count = _inner.AndWith(buffer, matches);
        _innerTicks[0] += Stopwatch.GetTimestamp() - t0;
        _innerEmitted[0] += count;

        if (count == 0)
            return 0;

        return ApplyPostFilters(buffer, count);
    }

    private int ApplyPostFilters(Span<long> buffer, int count)
    {
        if (_filterTicks is null)
        {
            for (int i = 0; i < _postFilters.Length; i++)
            {
                count = _postFilters[i].AndWith(buffer, count);
                if (count == 0)
                    return 0;
            }
            return count;
        }

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
        var parameters = new Dictionary<string, string>();

        if (_innerTicks is not null)
        {
            double tickFreq = Stopwatch.Frequency / 1000.0;
            if (_innerTicks[0] > 0)
                parameters["Inner_ms"] = (_innerTicks[0] / tickFreq).ToString("F3");
            parameters["InnerEmitted"] = _innerEmitted[0].ToString();

            for (int i = 0; i < _postFilters.Length; i++)
            {
                string prefix = $"Filter[{i}]_";
                if (_filterTicks[i] > 0)
                    parameters[prefix + "ms"] = (_filterTicks[i] / tickFreq).ToString("F3");
                parameters[prefix + "kept"] = _filterSurvivors[i].ToString();
                parameters[prefix + "rejected"] = _filterRejected[i].ToString();
            }
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
