# Negated post-filter via a single wrapper + bitmap `AndNot`

**Issue:** RavenDB-25281
**Date:** 2026-07-05
**Status:** Design approved, ready for implementation plan

## Problem

Most `WHERE` clauses run in Corax's bitmap pipeline, where negation is native and
cheap: the plan emitter turns `not X` into `AndNotFromMatch` / `AndNotBitmaps` — a
set-difference against the accumulator (`PlanEmitter`, `QueryPrimitives`).

Spatial (`spatial.within(...)`) and vector (`vector.search(...)`) clauses cannot live
in that pipeline. `GroupCollapse` lifts them out and re-attaches them as **post-filters**
that run after the bitmap phase has built a candidate set `R`. A post-filter historically
only knew how to intersect (`AndWith`) `R` with its matches `M`, so a negated post-filter
silently dropped its `IsNegated` flag and returned `M` (docs inside the shape / nearest the
vector) instead of `R \ M`.

Two commits on this branch fixed the behavior by pushing a `_negated` mode down into each
post-filter match type:

- `1aebd7f` — spatial: a `bool[] negated` array parallel to `_postFilters` in
  `PostFilterMatch`, plus `FilterSpanNegated` / `SubtractSorted`, plus a `driverIdx` /
  `RemoveAt` fallback in `ApplyPostFilters` for the no-driver case.
- `c4f2338` — vector: a `_negated` mode in `VectorSearchMatch` *and* `MultiVectorSearchMatch`
  (`SnapshotNegatedCandidates`, `EnsureNegatedComplement`, `FillNegated`), a shared
  `MergeHelper.AndNot`, and `Score` / `CanStreamResults` / `VectorPostFilterProvidesResultOrder`
  guards.

Both are correct, but the negation logic is now duplicated across three match types (the two
vector ones are near-identical copies) plus a set of downstream special-cases. This design
replaces that with a single mechanism.

## Key insight

For a negated clause **ordering is irrelevant** — the result is always a plain, unordered
bitmap `R \ M`. Every post-filter already knows how to compute `M` positively when scoped to
a candidate set. So negation is not a *mode of the match*; it is a *composition*:

> materialize the candidate universe `R`, run the **positive** clause scoped to `R` to get
> `M`, then subtract: `R \ M`.

This is exactly the set-difference the bitmap pipeline already performs for an ordinary `not`.
There is no need for span-based negation code inside each match, and no need for a bespoke
`MergeHelper.AndNot`.

## Design

### 1. Routing (`QueryPlanBuilder.Resolution.ApplyPostFilters`)

Partition each clause list on the **already-tracked** negation flag:

- spatial: `exec.SpatialFilters[sf].Clause.IsNegated`
- vector: `CoraxVectorItem.IsNegated` (from `exec.VectorSelects[i].IsNegated`)

Then:

```
result = source                                   // bitmap-pipeline driver, may be null
result = positive spatial post-filters over result   // existing streaming PostFilterMatch
result = positive vector over result                 // existing CoraxVectorItem.Materialize
if (any negated clause)
    result = new NegatedPostFilterMatch(result ?? AllEntries(), negatedClauses)
```

- Positive paths are **unchanged** (minimal + centralized).
- All negated clauses — spatial and vector together — go into **one** `NegatedPostFilterMatch`.
- `AllEntries()` is used only for a pure-negated query with no positive universe. This
  subsumes and removes the current spatial-only `driverIdx` / `RemoveAt` fallback.

The `negated[]` array that `c4f2338`/`1aebd7f` added to `PostFilterMatch` disappears — positive
and negated no longer share a `PostFilterMatch`, and every clause inside the wrapper is negated
by construction, so no per-clause flag is needed.

### 2. `NegatedPostFilterMatch` (the one place negation lives)

A new `IQueryMatch` (also exposing `AndWith` so it can be intersected) that wraps the
positively-narrowed universe and the list of positive clause matches to subtract.

Lazy, on first `Fill`/`AndWith`:

1. Materialize the wrapped universe into a **`RoaringBitmap` `R`** (the pipeline's native
   candidate representation).
2. For each negated clause `c` (a positive `IPostFilterMatch`, `filterQuery = R`):
   `QueryPrimitives.AndNotWithMatch(c, ref R, ref temp)` → `R := R \ M_c`.
   This is the same primitive an ordinary `not` clause uses (`CtxAndNotFromMatch`).
3. The final `R` is the result.

- `Fill` streams `R`; `AndWith(buffer)` intersects `buffer` with `R`.
- `Score` is a no-op and `IsBoosting` is `false` — a negated clause carries no score/order.
  Because the result is a plain materialized bitmap this falls out structurally; no
  `Score`/`CanStreamResults` special-cases are needed anywhere.

Each clause self-scopes to `R` via its `filterQuery`, so the wrapper needs **no**
spatial-vs-vector branching — it drives every clause through `AndNotWithMatch`.

### 3. `filterQuery` on `SpatialMatch`

Give `SpatialMatch` an optional candidate set, mirroring `VectorSearchMatch.filterQuery`, so
spatial self-scopes like vector. When a filter is present:

1. **Discard before test** — in `CheckEntryManually` and the exact-term-cell path, drop any
   id not in the candidate set before reading terms or running the point-in-shape relate.
   Membership is O(1) (`RoaringBitmap.Contains`); the geo test is expensive.
2. **Drive off the candidates, not the shape** — when a filter is present, `Fill` iterates the
   candidate set and tests each entry (its existing `AndWith` logic) instead of enumerating the
   shape's geohash cells. For `not spatial.within(bigShape)` with a selective `R` this is
   `|R|` tests rather than a walk of every cell in the shape.

When no filter is present (pure-spatial driver, `source == null`), behavior is unchanged:
`Fill` enumerates the shape.

### 4. Removals (undo the two commits' scattering)

- `PostFilterMatch`: drop `_negated[]`, `FilterSpanNegated`, `SubtractSorted`, and the extra
  ctor — back to a positive-only `AndWith` chain.
- `VectorSearchMatch` / `MultiVectorSearchMatch`: drop `_negated`, `SnapshotNegatedCandidates`,
  `EnsureNegatedComplement`, `FillNegated`, and the `Score` / `CanStreamResults` negation guards
  — back to positive-only. Remove the `isNegated` parameter threaded through
  `IndexSearcher.VectorSearch(...)` / `MultiVectorSearch(...)`.
- `MergeHelper.AndNot`: **deleted** — the wrapper uses the existing `RoaringBitmap.AndNotWith`
  via `QueryPrimitives.AndNotWithMatch`.
- `CoraxVectorItem.IsNegated` and the `VectorPostFilterProvidesResultOrder` negated guard
  **stay** as the routing signal (which clauses go to the wrapper). `CoraxVectorItem.Materialize`
  no longer forces `singleVectorSearch = false` for the negated case (negated vectors no longer
  go through the positive materialize path at all).

## Testing

- The two existing regression tests are behavior specs and must pass **unchanged**:
  - `test/FastTests/Corax/RavenDB_25281_NegatedSpatialPostFilter.cs`
  - `test/FastTests/Corax/Vectors/RavenDB_25281_NegatedVectorPostFilter.cs`
  This is the correctness gate — reverting the fix must make them fail.
- Add one test the current commits do not cover: a query mixing a **negated spatial and a
  negated vector** clause, exercising multi-clause subtraction through the single wrapper.
- Full FastTests Corax + Querying pass in Release.

## Non-goals

- No change to positive spatial/vector execution paths beyond the `SpatialMatch.filterQuery`
  addition.
- No unification of positive post-filters into the new materializing mechanism (they keep
  streaming).
- No index-wide complement is ever materialized; the candidate-set optimization is preserved
  for both spatial and vector.

## Risks / open points for the plan

- **Cost profile of spatial `filterQuery` Fill** — driving off `R` is the intended win; confirm
  the exact-term-cell path also intersects with `R` rather than returning cell entries wholesale.
- **`AndNotWithMatch` temp-bitmap lifetime** — the wrapper owns `R` and a scratch `temp`
  `RoaringBitmap`; pin allocation/disposal against the allocator.
- **Vector `filterQuery` = `R` wiring** — the negated vector clause's filter must be the same
  narrowed universe the wrapper materializes, so the driver is scanned once (not double-consumed).
