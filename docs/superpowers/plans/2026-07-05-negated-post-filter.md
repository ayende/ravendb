# Negated Post-Filter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the per-match-type `_negated` modes with a single `NegatedPostFilterMatch` wrapper that computes `R \ M` as one global bitmap set-difference, and give `SpatialMatch` a candidate `filterQuery` so spatial self-scopes like vector.

**Architecture:** Negated spatial/vector clauses are lifted out of the positive post-filter paths and routed to one wrapper. The wrapper materializes the candidate universe `R` as a `RoaringBitmap` and subtracts each positive clause (scoped to `R` via its filter query) using the pipeline's existing `QueryPrimitives.AndNotWithMatch`. Ordering is irrelevant for negation, so the result is a plain bitmap — no score/order special-cases.

**Tech Stack:** C# (.NET 10), Corax search engine, Voron `RoaringBitmap`, xUnit via `RavenFact`.

## Global Constraints

- No tabs in `.cs` files (CI fails on tabs). Use 4 spaces.
- Private/internal fields: `_camelCase`. Explicit types preferred over `var` where it aids clarity; match surrounding style.
- Newline before every brace.
- Warnings are treated as errors — no unused fields/usings left behind.
- Commit messages: `RavenDB-25281 <description>`, ending with the `Claude-Session:` trailer.
- Work on branch `RavenDB-25281-corax2-v8-review-4619060794` in worktree `/work/ravendb/RavenDB-25281-corax2-v8-review`.
- Build: `dotnet build RavenDB.sln -c Release` (from the worktree root). Tests: `dotnet test test/FastTests --configuration Release --filter <...>`.

## File Structure

- Create: `src/Corax/Querying/Matches/NegatedPostFilterMatch.cs` — the single negation wrapper.
- Create: `src/Corax/Querying/Matches/Meta/ISpatialFilterQuery.cs` — tiny interface to set a candidate filter on a spatial match.
- Modify: `src/Corax/Querying/Matches/SpatialMatch/SpatialMatch.cs` — add `FilterQuery` + filter-driven `Fill`.
- Modify: `src/Raven.Server/Documents/Indexes/Persistence/Corax/QueryPlanBuilder/QueryPlanBuilder.Resolution.cs` — partition clauses, route negated to the wrapper.
- Modify (cleanup): `src/Corax/Querying/Matches/PostFilterMatch.cs`, `VectorSearchMatch.cs`, `MultiVectorSearchMatch.cs`, `src/Corax/Querying/IndexSearcher.VectorSearch.cs`, `src/Corax/Querying/Matches/Meta/MergeHelper.cs`, `.../QueryOptimizer/CoraxVectorItem.cs`.
- Test: `test/FastTests/Corax/RavenDB_25281_NegatedSpatialPostFilter.cs`, `test/FastTests/Corax/Vectors/RavenDB_25281_NegatedVectorPostFilter.cs` (existing, must keep passing), and a new mixed test.

---

## Task 1: Switch negation to the single wrapper (behavioral cutover)

Adds the wrapper + spatial `filterQuery` and rewires routing so the two existing regression tests pass **via the new path**. The old `_negated` code is left in place but becomes dead (nothing constructs it); it is removed in Task 2. Keeping the cutover in one task means the tree compiles and the regression tests are the gate.

**Files:**
- Create: `src/Corax/Querying/Matches/Meta/ISpatialFilterQuery.cs`
- Create: `src/Corax/Querying/Matches/NegatedPostFilterMatch.cs`
- Modify: `src/Corax/Querying/Matches/SpatialMatch/SpatialMatch.cs`
- Modify: `src/Raven.Server/Documents/Indexes/Persistence/Corax/QueryPlanBuilder/QueryPlanBuilder.Resolution.cs:353-419`
- Test (existing, gate): `test/FastTests/Corax/RavenDB_25281_NegatedSpatialPostFilter.cs`, `test/FastTests/Corax/Vectors/RavenDB_25281_NegatedVectorPostFilter.cs`

**Interfaces:**
- Consumes: `QueryPrimitives.AndNotWithMatch(IQueryMatch, ref RoaringBitmap, ref RoaringBitmap, CancellationToken, bool)`; `BitmapMatch(ByteStringContext)` with `[UnscopedRef] ref RoaringBitmap BitmapState` and `int Fill(Span<long>)`; `IndexSearcher.Allocator`, `IndexSearcher.AllEntries()`; `CoraxVectorItem.Materialize(IQueryMatch inner, bool isPostFilter, bool streamScoreOrder = false)`.
- Produces:
  - `public interface ISpatialFilterQuery { IQueryMatch FilterQuery { get; set; } }` (namespace `Corax.Querying.Matches.Meta`).
  - `public sealed class NegatedPostFilterMatch : IQueryMatch, IDisposable` with ctor `NegatedPostFilterMatch(IndexSearcher searcher, IQueryMatch universe, Func<IQueryMatch, IQueryMatch>[] negatedFactories)`.

- [ ] **Step 1: Add the `ISpatialFilterQuery` interface**

Create `src/Corax/Querying/Matches/Meta/ISpatialFilterQuery.cs`:

```csharp
namespace Corax.Querying.Matches.Meta;

/// <summary>Implemented by a per-entry post-filter (spatial) that can restrict its own evaluation to a candidate
/// set. When <see cref="FilterQuery"/> is set the match drives off those candidates and tests only them, instead
/// of enumerating its full result — mirroring the vector search's filter query. Used by
/// <c>NegatedPostFilterMatch</c> to scope a negated clause to the candidate universe it subtracts from.</summary>
public interface ISpatialFilterQuery
{
    IQueryMatch FilterQuery { get; set; }
}
```

- [ ] **Step 2: Give `SpatialMatch` a filter-driven Fill**

In `src/Corax/Querying/Matches/SpatialMatch/SpatialMatch.cs`, change the class declaration to also implement the new interface:

```csharp
public sealed class SpatialMatch<TBoosting> : IPostFilterMatch, ISpatialFilterQuery
    where TBoosting : IBoostingMarker
```

Add a field next to the other private fields (near line 43):

```csharp
    private IQueryMatch _filterQuery;
```

Add the interface member (place it after the `IsPostFilter` property, ~line 24):

```csharp
    /// <summary>When set, this spatial match drives off the candidate set instead of enumerating the shape:
    /// every entry the filter yields is geo-tested and non-candidates are never touched. Set by
    /// NegatedPostFilterMatch to scope a `not spatial.within(...)` clause to the candidate universe.</summary>
    public IQueryMatch FilterQuery
    {
        get => _filterQuery;
        set => _filterQuery = value;
    }
```

Replace the existing `Fill` method (lines 95-129) so it delegates to a candidate-driven fill when a filter is present:

```csharp
    public int Fill(Span<long> matches)
    {
        if (_filterQuery != null)
            return FillFromFilter(matches);

        int currentIdx = 0;
        do
        {
            int read;
            if ((read = _currentMatch.Fill(matches.Slice(currentIdx))) == 0)
            {
                if (GoNextMatch() == false)
                {
                    break;
                }

                continue;
            }

            if (_isTermMatch)
            {
                currentIdx += read;
            }
            else if (read > 0)
            {
                var slicedMatches = matches.Slice(currentIdx);
                for (int i = 0; i < read; ++i)
                {
                    if (CheckEntryManually(slicedMatches[i]))
                    {
                        matches[currentIdx++] = slicedMatches[i];
                    }
                }
            }
        } while (currentIdx != matches.Length);

        return currentIdx;
    }

    // Candidate-driven fill: pull a page of candidate ids from the filter and keep only those inside the shape.
    // Non-candidate ids are never geo-tested (the discard-before-test optimization). Returns 0 only when the
    // filter is exhausted; a page with no survivors pulls the next page rather than signalling completion.
    private int FillFromFilter(Span<long> matches)
    {
        while (true)
        {
            int read = _filterQuery.Fill(matches);
            if (read == 0)
                return 0;

            int w = 0;
            for (int i = 0; i < read; ++i)
            {
                if ((i & 1023) == 0)
                    _token.ThrowIfCancellationRequested();

                if (CheckEntryManually(matches[i]))
                    matches[w++] = matches[i];
            }

            if (w > 0)
                return w;
        }
    }
```

- [ ] **Step 3: Add the `NegatedPostFilterMatch` wrapper**

Create `src/Corax/Querying/Matches/NegatedPostFilterMatch.cs`:

```csharp
using System;
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

    private BitmapMatch _result; // holds R, mutated in place to R \ M1 \ M2 ...
    private RoaringBitmap _temp;
    private bool _initialized;

    public NegatedPostFilterMatch(Querying.IndexSearcher searcher, IQueryMatch universe, Func<IQueryMatch, IQueryMatch>[] negatedFactories)
    {
        _searcher = searcher;
        _universe = universe;
        _negatedFactories = negatedFactories;
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
            QueryPrimitives.AndNotWithMatch(clause, ref _result.BitmapState, ref _temp);
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
```

- [ ] **Step 4: Rewire `ApplyPostFilters` to partition and route negated clauses**

In `src/Raven.Server/Documents/Indexes/Persistence/Corax/QueryPlanBuilder/QueryPlanBuilder.Resolution.cs`, replace the whole `ApplyPostFilters` method (lines 353-411) with:

```csharp
    private static IQueryMatch ApplyPostFilters(
        IQueryMatch source, IQueryMatch[] spatialMatches,
        QueryExecution exec, QueryBuilderParameters builderParameters, bool wantTimings)
    {
        IQueryMatch result = source;

        // Negated post-filters (spatial and vector) are collected as factories: given the materialized candidate
        // universe R, each returns its positive clause scoped to R. NegatedPostFilterMatch subtracts them globally.
        List<Func<IQueryMatch, IQueryMatch>> negatedFactories = null;

        if (spatialMatches is { Length: > 0 })
        {
            List<IQueryMatch> positiveSpatial = null;
            for (int sf = 0; sf < spatialMatches.Length; sf++)
            {
                var sm = spatialMatches[sf];
                if (sm is IPostFilterMatch postFilter)
                    postFilter.IsPostFilter = true;

                if (exec.SpatialFilters[sf].Clause.IsNegated)
                {
                    negatedFactories ??= new List<Func<IQueryMatch, IQueryMatch>>();
                    var spatial = sm; // capture per-iteration
                    negatedFactories.Add(filter =>
                    {
                        ((ISpatialFilterQuery)spatial).FilterQuery = filter;
                        return spatial;
                    });
                }
                else
                {
                    positiveSpatial ??= new List<IQueryMatch>();
                    positiveSpatial.Add(sm);
                }
            }

            if (positiveSpatial is { Count: > 0 })
            {
                var arr = positiveSpatial.ToArray();
                result = result is null
                    ? new PostFilterMatch(arr[0], arr.Length == 1 ? [] : arr[1..], wantTimings)
                    : new PostFilterMatch(result, arr, wantTimings);
            }
        }

        if (exec.VectorSelects is { Length: > 0 })
        {
            foreach (var item in ResolveVectorItems(exec, builderParameters))
            {
                if (item.IsNegated)
                {
                    negatedFactories ??= new List<Func<IQueryMatch, IQueryMatch>>();
                    var vec = item; // capture per-iteration
                    negatedFactories.Add(filter => vec.Materialize(filter, isPostFilter: true));
                }
                else
                {
                    result = item.Materialize(result, isPostFilter: true, streamScoreOrder: exec.VectorPostFilterProvidesScoreOrder);
                }
            }
        }

        if (negatedFactories is { Count: > 0 })
        {
            // A pure-negated query has no positive universe — subtract from every entry.
            result ??= builderParameters.IndexSearcher.AllEntries();
            result = new NegatedPostFilterMatch(builderParameters.IndexSearcher, result, negatedFactories.ToArray());
        }

        return result;
    }
```

Add the required usings at the top of the file if not present: `using System;`, `using System.Collections.Generic;`, `using Corax.Querying.Matches;`, `using Corax.Querying.Matches.Meta;`. (Check the existing using block first; only add what's missing.)

- [ ] **Step 5: Build**

Run: `dotnet build RavenDB.sln -c Release`
Expected: build succeeds. (The old `_negated` code and `RemoveAt<T>` are now unused but still compile; they are removed in Task 2. If the compiler flags `RemoveAt<T>` as unused with warnings-as-errors, delete that helper now — lines 413-419 of the original file.)

- [ ] **Step 6: Run the two existing regression tests**

Run: `dotnet test test/FastTests --configuration Release --filter "FullyQualifiedName~RavenDB_25281_NegatedSpatialPostFilter|FullyQualifiedName~RavenDB_25281_NegatedVectorPostFilter"`
Expected: all tests PASS — negation now flows through `NegatedPostFilterMatch`.

- [ ] **Step 7: Verify the tests have teeth**

Temporarily comment out the `result = new NegatedPostFilterMatch(...)` line in `ApplyPostFilters` (so negated clauses fall back to nothing / positive behavior), rebuild, and re-run Step 6.
Expected: the negated-case assertions FAIL (e.g. spatial returns the docs inside the shape). Then restore the line, rebuild, and confirm Step 6 passes again.

- [ ] **Step 8: Commit**

```bash
git add src/Corax/Querying/Matches/Meta/ISpatialFilterQuery.cs \
        src/Corax/Querying/Matches/NegatedPostFilterMatch.cs \
        src/Corax/Querying/Matches/SpatialMatch/SpatialMatch.cs \
        src/Raven.Server/Documents/Indexes/Persistence/Corax/QueryPlanBuilder/QueryPlanBuilder.Resolution.cs
git commit -m "$(cat <<'EOF'
RavenDB-25281 Route negated post-filters through a single NegatedPostFilterMatch

Negated spatial/vector clauses are lifted out of the positive post-filter paths and
subtracted globally: the wrapper materializes the candidate universe R as a RoaringBitmap
and removes each clause's matches (scoped to R via its filter query) with the pipeline's
existing AndNotWithMatch. SpatialMatch gains a filter query so it self-scopes like vector.

Claude-Session: https://claude.ai/code/session_01C9ygMjjXeBNoqrgu9TiSRh
EOF
)"
```

---

## Task 2: Remove the now-dead per-match negation code

The cutover in Task 1 made the old negation code unreachable. Remove it so there is one negation mechanism.

**Files:**
- Modify: `src/Corax/Querying/Matches/PostFilterMatch.cs`
- Modify: `src/Corax/Querying/Matches/VectorSearchMatch.cs`
- Modify: `src/Corax/Querying/Matches/MultiVectorSearchMatch.cs`
- Modify: `src/Corax/Querying/IndexSearcher.VectorSearch.cs`
- Modify: `src/Corax/Querying/Matches/Meta/MergeHelper.cs`
- Modify: `src/Raven.Server/Documents/Indexes/Persistence/Corax/QueryOptimizer/CoraxVectorItem.cs`

**Interfaces:**
- Consumes: nothing new.
- Produces: `PostFilterMatch(IQueryMatch inner, IQueryMatch[] postFilters, bool wantTimings)` as the only ctor; `VectorSearch(...)` / `MultiVectorSearch(...)` without the `isNegated` parameter; `MergeHelper` without `AndNot`.

- [ ] **Step 1: Strip negation from `PostFilterMatch`**

In `src/Corax/Querying/Matches/PostFilterMatch.cs`:
- Delete the `_negated` field (the `private readonly bool[] _negated;` and its comment).
- Delete the extra ctor `PostFilterMatch(IQueryMatch inner, IQueryMatch[] postFilters, bool[] negated, bool wantTimings)` and collapse back to a single ctor:

```csharp
    public PostFilterMatch(IQueryMatch inner, IQueryMatch[] postFilters, bool wantTimings)
    {
        _inner = inner;
        _postFilters = postFilters;
        _wantTimings = wantTimings;
        if (wantTimings)
        {
            _filterTicks = new long[postFilters.Length];
            _filterSurvivors = new long[postFilters.Length];
            _filterRejected = new long[postFilters.Length];
        }
    }
```

- In `ApplyPostFilters(Span<long> buffer, int count)` restore the positive-only call:

```csharp
            count = FilterSpan(_postFilters[i], buffer, count);
```

- Delete the `FilterSpanNegated` and `SubtractSorted` private methods entirely.
- Remove `using System.Buffers;` if it is now unused.
- Restore the class doc comment to describe an AndWith-only chain (drop the negated-filter paragraph).

- [ ] **Step 2: Strip negation from `VectorSearchMatch`**

In `src/Corax/Querying/Matches/VectorSearchMatch.cs`:
- Delete the `_negated`, `_negatedCandidates`, `_negatedComplementComputed` fields and their comment block.
- Restore `CanStreamResults`:

```csharp
    private bool CanStreamResults => IsBoosting == false && _singleVectorSearchDoNotSort;
```

- Remove the `in bool isNegated = false` ctor parameter and the `_negated = isNegated;` assignment.
- In `InitializeVectorSearch`, delete the `if (_negated) SnapshotNegatedCandidates();` block.
- In `Fill`, delete the `if (_negated) return FillNegated(matches);` block.
- In `AndWith`, delete the `if (_negated) { EnsureNegatedComplement(); ... }` block.
- In `Score`, delete the `if (_negated) return;` guard.
- Delete the `FillNegated`, `SnapshotNegatedCandidates`, and `EnsureNegatedComplement` methods.

- [ ] **Step 3: Strip negation from `MultiVectorSearchMatch`**

Apply the same removals as Step 2 in `src/Corax/Querying/Matches/MultiVectorSearchMatch.cs`: the `_negated` / `_negatedCandidates` / `_negatedComplementComputed` fields, the ctor `in bool isNegated = false` parameter and assignment, the `SnapshotNegatedCandidates` call in `InitializeVectorSearch`, the `if (_negated) return FillNegated(matches);` in `Fill`, the `Score` guard, and the `FillNegated` / `SnapshotNegatedCandidates` / `EnsureNegatedComplement` methods.

- [ ] **Step 4: Remove `isNegated` from the `IndexSearcher` vector entry points**

In `src/Corax/Querying/IndexSearcher.VectorSearch.cs`, restore the four signatures to their pre-negation form (remove the `bool isNegated = false` parameter and the `isNegated:` / `, isNegated` arguments):

```csharp
    public VectorSearchMatch VectorSearch(in FieldMetadata metadata, in VectorValue vectorValue, float minimumMatch, in int numberOfCandidates, bool isExact, bool isSingleVectorSearch, IQueryMatch filterQuery = null, int scanningThreshold = 1024, Random random = null)
    {
        return new VectorSearchMatch(this, metadata, vectorValue, minimumMatch, numberOfCandidates, isExact, isSingleVectorSearch, filterQuery, scanningThreshold, random);
    }

    public IQueryMatch VectorSearch(in FieldMetadata metadata, in string documentId, float minimumMatch, in int numberOfCandidates, bool isExact, bool isSingleVectorSearch, IQueryMatch filterQuery = null, int scanningThreshold = 1024)
```

For the two `return new VectorSearchMatch(...)` / `return new MultiVectorSearchMatch(...)` statements inside the `documentId` overload, drop the `isNegated: isNegated` argument. Restore `MultiVectorSearch`:

```csharp
    public MultiVectorSearchMatch MultiVectorSearch(in FieldMetadata metadata, in VectorValue[] vectorValues, float minimumMatch, in int numberOfCandidates, bool isExact, bool isSingleVectorSearch, IQueryMatch filterQuery = null, int scanningThreshold = 1024, Random random = null)
        => new(this, metadata, vectorValues, minimumMatch, numberOfCandidates, isExact, isSingleVectorSearch, filterQuery, scanningThreshold, random);
```

- [ ] **Step 5: Remove `MergeHelper.AndNot`**

In `src/Corax/Querying/Matches/Meta/MergeHelper.cs`, delete the `AndNot(Span<long> buffer, int count, ReadOnlySpan<long> removed)` method and its doc comment (added by the vector commit).

- [ ] **Step 6: Simplify `CoraxVectorItem.Materialize`**

In `src/Raven.Server/Documents/Indexes/Persistence/Corax/QueryOptimizer/CoraxVectorItem.cs`:
- Restore the `singleVectorSearch` line (negated vectors no longer take this path):

```csharp
        bool singleVectorSearch = _isVectorSingleClause || streamScoreOrder;
```

- Drop the `isNegated: IsNegated` argument from all three `VectorSearch` / `MultiVectorSearch` calls.
- Keep the `IsNegated` property — it is the routing signal read in `ApplyPostFilters` and `VectorPostFilterProvidesResultOrder`.

- [ ] **Step 7: Build**

Run: `dotnet build RavenDB.sln -c Release`
Expected: build succeeds with no warnings (warnings-as-errors). If `using System.Buffers;` or any field is now unused, remove it.

- [ ] **Step 8: Re-run the regression tests**

Run: `dotnet test test/FastTests --configuration Release --filter "FullyQualifiedName~RavenDB_25281_NegatedSpatialPostFilter|FullyQualifiedName~RavenDB_25281_NegatedVectorPostFilter"`
Expected: all PASS (behavior unchanged; only dead code removed).

- [ ] **Step 9: Commit**

```bash
git add -A
git commit -m "$(cat <<'EOF'
RavenDB-25281 Remove the per-match negation code superseded by the wrapper

Drops the _negated modes from PostFilterMatch, VectorSearchMatch and
MultiVectorSearchMatch, the isNegated plumbing through IndexSearcher.VectorSearch,
and MergeHelper.AndNot. Negation now lives solely in NegatedPostFilterMatch.

Claude-Session: https://claude.ai/code/session_01C9ygMjjXeBNoqrgu9TiSRh
EOF
)"
```

---

## Task 3: Cover multi-clause negation + full validation

Adds a test the current commits do not cover — a query mixing a negated spatial and a negated vector clause through the single wrapper — and runs the broader suite.

**Files:**
- Create: `test/FastTests/Corax/Vectors/RavenDB_25281_MixedNegatedPostFilter.cs`

**Interfaces:**
- Consumes: `RavenTestBase`, `Options.ForSearchEngine(RavenSearchEngineMode.Corax)`, `AbstractIndexCreationTask<T>`, `CreateSpatialField`, `CreateVector`.

- [ ] **Step 1: Write the mixed negated-spatial + negated-vector test**

Create `test/FastTests/Corax/Vectors/RavenDB_25281_MixedNegatedPostFilter.cs`:

```csharp
using System.Linq;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Corax.Vectors;

/// <summary>
/// A query with BOTH a negated spatial and a negated vector clause exercises multi-clause subtraction through the
/// single NegatedPostFilterMatch: R \ (spatial matches) \ (vector matches). The term clause (City = 'NYC') is the
/// candidate universe; each negated clause is scoped to it and subtracted in turn.
/// </summary>
public class RavenDB_25281_MixedNegatedPostFilter(ITestOutputHelper output) : RavenTestBase(output)
{
    private sealed class GeoVecDoc
    {
        public string Id { get; set; }
        public string City { get; set; }
        public double Lat { get; set; }
        public double Lon { get; set; }
        public float[] Embedding { get; set; }
    }

    private sealed class GeoVecIndex : AbstractIndexCreationTask<GeoVecDoc>
    {
        public GeoVecIndex()
        {
            Map = docs => from d in docs
                          select new
                          {
                              d.City,
                              Location = CreateSpatialField(d.Lat, d.Lon),
                              Embedding = CreateVector(d.Embedding),
                          };
        }
    }

    private static readonly float[] Query = [1f, 0f];

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Corax | RavenTestCategory.Querying)]
    public void Negated_spatial_and_negated_vector_subtract_through_one_wrapper()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Corax));
        using (var session = store.OpenSession())
        {
            // All are City = NYC (the candidate universe R = {1,2,3,4}).
            session.Store(new GeoVecDoc { Id = "docs/1", City = "NYC", Lat = 0, Lon = 0, Embedding = [1.0f, 0.0f] });  // in circle, near [1,0]
            session.Store(new GeoVecDoc { Id = "docs/2", City = "NYC", Lat = 0, Lon = 0, Embedding = [0.0f, 1.0f] });  // in circle, far from [1,0]
            session.Store(new GeoVecDoc { Id = "docs/3", City = "NYC", Lat = 30, Lon = 30, Embedding = [1.0f, 0.0f] }); // out of circle, near [1,0]
            session.Store(new GeoVecDoc { Id = "docs/4", City = "NYC", Lat = 30, Lon = 30, Embedding = [0.0f, 1.0f] }); // out of circle, far from [1,0]
            session.SaveChanges();
        }

        new GeoVecIndex().Execute(store);
        Indexes.WaitForIndexing(store);

        using var session2 = store.OpenSession();
        var ids = session2.Advanced
            .RawQuery<GeoVecDoc>(
                "from index 'GeoVecIndex' where City = $c " +
                "and not spatial.within(Location, spatial.circle(60, 0, 0, 'miles')) " +
                "and not vector.search(Embedding, $vec, 0.0, 2)")
            .AddParameter("c", "NYC")
            .AddParameter("vec", Query)
            .WaitForNonStaleResults()
            .ToList()
            .Select(d => d.Id)
            .OrderBy(id => id)
            .ToList();

        // R = {1,2,3,4}.
        // not within(origin circle) removes the origin docs {1,2} -> {3,4}.
        // not vector.search([1,0], top 2) removes the two nearest {1,3} -> from {3,4} remove 3 -> {4}.
        Assert.Equal(new[] { "docs/4" }, ids);
    }
}
```

- [ ] **Step 2: Run the mixed test**

Run: `dotnet test test/FastTests --configuration Release --filter "FullyQualifiedName~RavenDB_25281_MixedNegatedPostFilter"`
Expected: PASS.

- [ ] **Step 3: Run the full negated + Corax vector/spatial slice**

Run: `dotnet test test/FastTests --configuration Release --filter "FullyQualifiedName~RavenDB_25281|Category=Vector&Category=Corax"`
Expected: PASS. Investigate any failure before proceeding.

- [ ] **Step 4: Broader regression — Corax querying**

Run: `dotnet test test/FastTests --configuration Release --filter "Category=Corax&Category=Querying"`
Expected: PASS (no regression from the routing change).

- [ ] **Step 5: Commit**

```bash
git add test/FastTests/Corax/Vectors/RavenDB_25281_MixedNegatedPostFilter.cs
git commit -m "$(cat <<'EOF'
RavenDB-25281 Add mixed negated-spatial + negated-vector regression test

Covers multi-clause subtraction through the single NegatedPostFilterMatch wrapper.

Claude-Session: https://claude.ai/code/session_01C9ygMjjXeBNoqrgu9TiSRh
EOF
)"
```

---

## Self-Review

**Spec coverage:**
- Routing / partition on `IsNegated` → Task 1 Step 4. ✓
- `NegatedPostFilterMatch` (materialize R, `AndNotWithMatch`, no score/order) → Task 1 Step 3. ✓
- `AllEntries()` fallback for pure-negated → Task 1 Step 4. ✓
- `filterQuery` on `SpatialMatch` (discard-before-test, drive off candidates) → Task 1 Step 2. ✓
- Removals: `PostFilterMatch._negated`, vector `_negated`, `MergeHelper.AndNot`, `isNegated` plumbing, `RemoveAt<T>` → Task 2 + Task 1 Step 5. ✓
- Keep `CoraxVectorItem.IsNegated` + `VectorPostFilterProvidesResultOrder` guard → Task 2 Step 6. ✓
- Existing regression tests pass unchanged → Task 1 Step 6, Task 2 Step 8. ✓
- Mixed negated test → Task 3. ✓

**Placeholder scan:** No TBD/TODO; every code step has full code. ✓

**Type consistency:** `NegatedPostFilterMatch(IndexSearcher, IQueryMatch, Func<IQueryMatch, IQueryMatch>[])` used identically in Task 1 Steps 3 and 4; `ISpatialFilterQuery.FilterQuery` defined (Step 1), implemented (Step 2), consumed via cast (Step 4). `BitmapMatch.BitmapState` / `Add` / `Fill` and `QueryPrimitives.AndNotWithMatch` signatures match the verified source. ✓

## Risks / watch-points during execution

- **`RoaringBitmap` read/write state machine.** `AndNotWithMatch` borrows `R` (calls `PrepareForReading` on it) to materialize a clause, then mutates `R` via `AndNotWith`. If a test throws inside init or returns wrong counts, the fix is the placement of `PrepareForReading` calls around the `Add` / `AndNotWith` / `Fill` sequence in `NegatedPostFilterMatch.EnsureInitialized` and `BitmapMatch.Fill` — adjust guided by the failure, do not add speculative calls.
- **Filter-query borrow lifetime.** The clause reads `R` (borrowed, `owned == false`) fully during `AndNotWithMatch` before `R` is mutated, so a single clause is safe. For multiple negated clauses each subsequent clause sees the already-reduced `R` (correct — still an AND). Confirm the second clause in the mixed test subtracts against the reduced set.
- **`CreateVector` / `CreateSpatialField` in one index.** If the combined index fails to build, split the assertion into two indexes is NOT acceptable (the point is one query with both clauses); instead check the field-name casing against the existing single-purpose tests.
- **Unused-code warnings.** Task 1 leaves dead code that must still compile under warnings-as-errors; if the build breaks on unused `RemoveAt<T>`, remove it in Task 1 Step 5 rather than waiting for Task 2.
