using System;
using System.Collections.Generic;
using System.Threading;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Primitives;
using Voron.Data.RoaringBitmaps;

namespace Corax.Querying.Matches;

/// <summary>
/// Applies one or more negated post-filters (`not spatial.within(...)` / `not vector.search(...)`) as a single
/// global set-difference. Ordering is irrelevant for a negated clause, so the result is just a bitmap: materialize
/// the candidate universe R once, then for each clause subtract the entries it matches — scoped to R via the
/// clause's own filter query — using the pipeline's AndNotWithMatch (the same primitive an ordinary `not` uses).
/// Centralizes negation that would otherwise be duplicated inside each post-filter match.
/// </summary>
public sealed class NegatedPostFilterMatch : IQueryMatch, IDisposable
{
    private readonly Querying.IndexSearcher _searcher;
    private readonly IQueryMatch _universe;
    private readonly Func<IQueryMatch, IQueryMatch>[] _negatedFactories;
    private readonly CancellationToken _token;

    private BitmapMatch _result; // holds R, mutated in place to R \ M1 \ M2 ...
    private RoaringBitmap _temp;
    private bool _initialized;

    // The clauses built by _negatedFactories, retained solely so Inspect() can report them; populated in EnsureInitialized.
    private readonly List<IQueryMatch> _builtClauses = new();

    public NegatedPostFilterMatch(Querying.IndexSearcher searcher, IQueryMatch universe, Func<IQueryMatch, IQueryMatch>[] negatedFactories, CancellationToken token = default)
    {
        _searcher = searcher;
        _universe = universe;
        _negatedFactories = negatedFactories;
        _token = token;
    }

    // Intentional upper-bound estimate: the true surviving count is |universe| minus whatever the negated
    // clauses subtract, and can be far smaller once the universe is AllEntries(). Corax treats Count as an
    // estimate used for sizing, not an exact value, so this is left unrefined.
    public long Count => _universe.Count;
    public bool IsBoosting => false;

    private void EnsureInitialized()
    {
        if (_initialized)
            return;
        _initialized = true;

        var allocator = _searcher.Allocator;
        _result = new BitmapMatch(allocator);
        _temp = new RoaringBitmap(allocator);

        // Materialize the candidate universe R (drain the driver once) into the result bitmap.
        using (allocator.Allocate(4096, out Span<long> buffer))
        {
            int read;
            while ((read = _universe.Fill(buffer)) > 0)
            {
                for (int i = 0; i < read; i++)
                    _result.BitmapState.Add(buffer[i]);
            }
        }

        // R := R \ M_c for each negated clause, each scoped to the current R via its filter query.
        foreach (var factory in _negatedFactories)
        {
            var clause = factory(_result); // filter query = R (borrowed via LoadFilterMatches, no copy)
            _builtClauses.Add(clause);
            QueryPrimitives.AndNotWithMatch(clause, ref _result.BitmapState, ref _temp, _token);
        }

        _result.BitmapState.PrepareForReading();
    }

    public int Fill(Span<long> matches)
    {
        EnsureInitialized();
        return _result.Fill(matches);
    }

    // A negated post-filter carries no similarity score.
    public void Score(Span<long> matches, Span<float> scores, float boostFactor)
    {
    }

    public void ScoreSorted(Span<long> matches, Span<float> scores, float boostFactor)
    {
    }

    // Side-effect free: must not call EnsureInitialized, since Inspect() can run without the match ever executing.
    public QueryInspectionNode Inspect()
    {
        var parameters = new Dictionary<string, string> { ["IsNegated"] = "true" };

        if (_initialized == false)
        {
            return new QueryInspectionNode(nameof(NegatedPostFilterMatch), parameters: parameters,
                children: new List<QueryInspectionNode> { _universe.Inspect() });
        }

        parameters[Constants.QueryInspectionNode.MatchedResults] = _result.Count.ToString("N0");

        var children = new List<QueryInspectionNode>(_builtClauses.Count + 1) { _universe.Inspect() };
        foreach (var clause in _builtClauses)
            children.Add(clause.Inspect());

        return new QueryInspectionNode(nameof(NegatedPostFilterMatch), parameters: parameters, children: children);
    }

    public void Dispose()
    {
        if (_initialized == false)
            return;
        _result.Dispose();
        _temp.Dispose();
    }
}
