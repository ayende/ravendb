using System;
using System.Collections.Generic;
using System.Diagnostics;
using Corax.Querying.Matches.Meta;

namespace Corax.Querying.Matches;

/// <summary>
/// Wraps the bitmap produced for a <c>moreLikeThis(...)</c> sub-expression so
/// the build-time work is visible to query introspection. The wrapped inner is
/// the assembled bitmap (or single sub-clause match) and is fully materialized
/// before this object is constructed — <see cref="Fill"/>/<see cref="AndWith"/>
/// just delegate. The interesting payload is in <see cref="Inspect"/>: total
/// build ticks, per-sub-clause cardinality + build ticks, per-AND-step bitmap
/// size, and the inspection snapshots captured from each sub-clause match
/// *before* those matches were consumed into the bitmap (otherwise they would
/// be discarded with no introspection trace).
/// </summary>
public sealed class MoreLikeThisBaseMatch : IQueryMatch
{
    private readonly IQueryMatch _inner;
    private readonly long _buildTicks;
    private readonly long[] _clauseBuildTicks;
    private readonly long[] _clauseCardinality;
    private readonly long[] _bitmapAfterAnd;
    private readonly QueryInspectionNode[] _capturedChildInspections;

    /// <param name="inner">The fully-materialized bitmap (or single-clause match).</param>
    /// <param name="buildTicks">Total <see cref="Stopwatch.GetTimestamp"/> ticks spent in the build (parse + resolve + AND chain).</param>
    /// <param name="clauseBuildTicks">Per-sub-clause ticks measured around its <c>ResolveClause</c> call.</param>
    /// <param name="clauseCardinality">Per-sub-clause <c>Count</c> read from the resolved match before it was consumed.</param>
    /// <param name="bitmapAfterAnd">Per-step bitmap count after that AND step; index 0 is the initial OR, index i (i≥1) is after the i-th AND. Pass <c>null</c> if the bitmap chain didn't run (single-clause case).</param>
    /// <param name="capturedChildInspections">Inspection snapshots of each sub-clause match, captured before consumption.</param>
    public MoreLikeThisBaseMatch(
        IQueryMatch inner,
        long buildTicks,
        long[] clauseBuildTicks,
        long[] clauseCardinality,
        long[] bitmapAfterAnd,
        QueryInspectionNode[] capturedChildInspections)
    {
        _inner = inner;
        _buildTicks = buildTicks;
        _clauseBuildTicks = clauseBuildTicks;
        _clauseCardinality = clauseCardinality;
        _bitmapAfterAnd = bitmapAfterAnd;
        _capturedChildInspections = capturedChildInspections;
    }

    public long Count => _inner.Count;
    public QueryCountConfidence Confidence => _inner.Confidence;
    public bool IsBoosting => _inner.IsBoosting;
    public DuplicatesOccurrence DuplicatesOccurrenceStatus => _inner.DuplicatesOccurrenceStatus;

    public int Fill(Span<long> matches) => _inner.Fill(matches);
    public int AndWith(Span<long> buffer, int matches) => _inner.AndWith(buffer, matches);
    public void Score(Span<long> matches, Span<float> scores, float boostFactor) => _inner.Score(matches, scores, boostFactor);

    public QueryInspectionNode Inspect()
    {
        double tickFreq = Stopwatch.Frequency / 1000.0;
        var parameters = new Dictionary<string, string>
        {
            ["TotalBuild_ms"] = (_buildTicks / tickFreq).ToString("F3"),
            ["SubClauseCount"] = _clauseBuildTicks.Length.ToString()
        };

        for (int i = 0; i < _clauseBuildTicks.Length; i++)
        {
            string prefix = $"Clause[{i}]_";
            parameters[prefix + "ms"] = (_clauseBuildTicks[i] / tickFreq).ToString("F3");
            parameters[prefix + "cardinality"] = _clauseCardinality[i].ToString();
            if (_bitmapAfterAnd != null && i < _bitmapAfterAnd.Length)
                parameters[prefix + "bitmap_after_step"] = _bitmapAfterAnd[i].ToString();
        }

        var children = new List<QueryInspectionNode>(_capturedChildInspections.Length + 1)
        {
            _inner.Inspect()
        };
        children.AddRange(_capturedChildInspections);

        return new QueryInspectionNode(nameof(MoreLikeThisBaseMatch), parameters: parameters, children: children);
    }
}
