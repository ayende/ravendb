using System;
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

    public NegatedPostFilterMatch(Querying.IndexSearcher searcher, IQueryMatch universe, Func<IQueryMatch, IQueryMatch>[] negatedFactories, CancellationToken token = default)
    {
        _searcher = searcher;
        _universe = universe;
        _negatedFactories = negatedFactories;
        _token = token;
    }

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

    public QueryInspectionNode Inspect() => new(nameof(NegatedPostFilterMatch));

    public void Dispose()
    {
        if (_initialized == false)
            return;
        _result.Dispose();
        _temp.Dispose();
    }
}
