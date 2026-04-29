# Corax 2.0 Feature Audit

Every query feature in Corax 1.0 and how it maps to Corax 2.0.
Status: **Covered** = addressed in design, **Gap** = not yet addressed.

---

## Query Operations

| Feature | Corax 1.0 | Corax 2.0 | Status |
|---------|-----------|-----------|--------|
| Single term match | `TermMatch` | `FillFromPostings` | Covered |
| AND | `BinaryMatch` + streaming merge | `AndWithPostings` on bitmap | Covered |
| OR | `BinaryMatch` + streaming merge with balancing | `OrWithPostings` on bitmap | Covered |
| ANDNOT | `AndNotMatch` (lazy buffer outer) | `AndNotWithPostings` galloping | Covered |
| IN clause | `MultiTermMatch` via `InQuery` | `LazyOrWithPostings` loop + `RepairAfterLazy` | Covered |
| ALL IN | `MultiTermMatch` with All mode | Bitmap AND chain (each term AND'd) | Covered |
| BETWEEN / range | `BetweenQuery` → `MultiTermMatch` | `FillFromRange` on CompactTree | Covered |
| Greater/Less than | `GreaterThanQuery` etc → `MultiTermMatch` | `FillFromRange` with Min/Max sentinels | Covered |
| NOT equals | `AndNot(AllEntries, term)` or `MultiUnaryMatch` | Bitmap ANDNOT or entry-scan | Covered |
| StartsWith | `StartWithQuery` → `MultiTermMatch` via `TermProvider.StartWith` | `FillFromRange` (prefix range) or entry-scan | Covered |
| EndsWith | `EndsWithQuery` → `MultiTermMatch` via `TermProvider.EndsWith` | Linear term scan into bitmap | Covered |
| Contains | `ContainsQuery` → `MultiTermMatch` via `TermProvider.Contains` | Linear term scan into bitmap | Covered |
| Regex | `RegexQuery` → `MultiTermMatch` via `TermProvider.Regex` | Linear term scan into bitmap | Covered |
| Exists | `ExistsQuery` → `MultiTermMatch` via `TermProvider.Exists` | `FillFromPostings` (all terms in field) | Covered |
| All entries | `AllEntriesMatch` via Lookup iterator | Iterate all entry IDs into bitmap, or `OrderedRangeScan` for sorted | Covered |
| Negated startsWith | `TryUseNegatedQuery` → `NotStartsWith` provider | Linear term scan (terms NOT matching prefix) into bitmap | Covered |
| Negated endsWith | `NotEndsWith` provider | Linear term scan into bitmap | Covered |
| Negated contains | `NotContains` provider | Linear term scan into bitmap | Covered |

## Sorting

| Feature | Corax 1.0 | Corax 2.0 | Status |
|---------|-----------|-----------|--------|
| ORDER BY single field | `SortingMatch` with `SortUsingIndex` | `SortWithFilter` (walks sort index or heap-sorts) | Covered |
| ORDER BY multiple fields | `SortingMultiMatch` with cascading comparers | `SortWithFilter` with `OrderMetadata` carrying multiple fields | **Gap** |
| ORDER BY score() | `EntryComparerByScore` | `SortByScore` with `PriorityQueue` | Covered |
| ORDER BY distance() | `EntryComparerBySpatial` | `SortByDistance` from `DistanceLookup` | Covered |
| ORDER BY alphanumeric | `EntryComparerByTermAlphaNumeric` | Needs alphanumeric comparer in `SortWithFilter` | **Gap** |
| ORDER BY random | `RandomDirection` comparer | Not addressed | **Gap** |
| Sort-skip (WHERE field = ORDER BY field) | `StreamingOptimization` | `OrderedRangeScan` primitive | Covered |
| Compound field sort-skip | `StreamingOptimization.OptimizeCompoundField` via `StartWithQuery` on compound field | Not addressed | **Gap** |
| Null first / null last | `IncludeNullMatch` / `IncludeNonExistingMatch` with streaming merge | Not addressed | **Gap** |
| Sort with LIMIT (top-K) | `NumericalMaxHeapSorter` / `TextualMaxHeapSorter` | `SortWithFilter` with `PriorityQueue` | Covered |
| `SortUsingIndex` with candidate set | Intersects sorted index walk with `allMatches` span | `SortWithFilter` checks `bitmap.Contains()` or heap-sorts | Covered |

### Multi-field sort detail

`SortingMultiMatch` uses an array of comparers with priority ordering.
The first comparer sorts the batch, ties are broken by the second
comparer, etc. Up to 3 comparers are inlined; the rest go into a
cached array.

**Corax 2.0 gap:** The design's `SortWithFilter` takes a single
`OrderMetadata`. For multi-field sort, `OrderMetadata` needs to carry
an array of sort specifications, and the heap-sort / index-walk path
needs to handle tie-breaking across multiple fields. The existing
comparers (`EntryComparerByTerm`, `EntryComparerByLong`, etc.) can be
reused inside `SortWithFilter`.

### Compound field sort-skip detail

Compound fields combine two indexed fields into one composite field
(e.g., `Status + CreatedAt`). When the WHERE clause filters on
`Status` and ORDER BY is on `CreatedAt`, the compound field allows
a `StartWithQuery` on the composite field that produces results in
`CreatedAt` order for a given `Status` value — no separate sort step.

**Corax 2.0 gap:** The planner needs a pass that detects compound
field opportunities. The existing `Index.HasCompoundField(queryField,
sortField, out bindingId)` API can be reused. The emitted code would
call `OrderedRangeScan` on the compound field's CompactTree with the
WHERE value as prefix.

Constraint from existing code: compound field optimisation only works
with a single WHERE condition (cannot combine with other AND predicates
via the compound path).

### Null / non-existing field sort detail

`IncludeNullMatch` and `IncludeNonExistingMatch` merge entries from
dedicated null/non-existing posting lists into the result stream,
with a `nullFirst` flag controlling whether nulls sort before or after
real values.

**Corax 2.0 gap:** When ORDER BY includes null positioning, the bitmap
needs to include entries from the null posting list, and the sort must
respect the null-first/null-last ordering. The planner should emit a
`OrWithPostings` for the null posting list into the bitmap, and the
sort comparers should handle null values (compare as min or max
depending on the flag).

## Scoring

| Feature | Corax 1.0 | Corax 2.0 | Status |
|---------|-----------|-----------|--------|
| BM25 scoring | `Bm25Relevance` with frequency buffer | Galloping frequency collection + `SortByScore` with `PriorityQueue` | Covered |
| boost() | `BoostingMatch` multiplies boostFactor | `BoostingTermInfo` with boost field, passed to `SortByScore` | Covered |
| Score accumulation (OR of boosted terms) | `BinaryMatch.Score()` calls both sides, scores += | `SortByScore` iterates each boosted term's frequency array, sums BM25 | Covered |
| Document boost | `InitialScoreValue` (1/1M) multiplied by doc boost | Same pattern — default score for entries not in any boosted posting list | Covered |
| Frequency > 102K threshold | `PostingListCalculateScoreDynamically` re-reads posting list | Same pattern — galloping walk re-reads frequencies from posting list | Covered |

## Vector Search

| Feature | Corax 1.0 | Corax 2.0 | Status |
|---------|-----------|-----------|--------|
| Single vector search | `VectorSearchMatch` with HNSW retriever | `VectorRank` (exact for small bitmap, HNSW for large) | Covered |
| Multi-vector search | `MultiVectorSearchMatch` merges results | `VectorRank` handles internally (runtime type check) | Covered |
| Vector with pre-filter | Filter loaded into `GrowableBitArray`, HNSW filtered | Bitmap from WHERE clause passed to `VectorRank` | Covered |
| Distance to score conversion | `DistanceToScore()` on retriever | `DistanceLookup` stores distances, projection reads them | Covered |
| Streaming vs memoized | `CanStreamResults` check | `VectorRank` always produces bitmap (no streaming) | Covered |

### Multi-vector search detail

`MultiVectorSearchMatch` runs multiple HNSW searches (one per input
vector), merges results with `SortAndMinOnDuplicates` (keeps best
distance per entry). Used for multi-modal search (image + text).

`VectorRank` handles this internally: it inspects the parameter type at
runtime — single vector vs array of vectors. If array, it runs multiple
HNSW searches and merges results (min distance per entry) inside the
same primitive call. One `VectorRank` call in the generated IL. The
planner doesn't need to know the parameter type upfront.

## Spatial

| Feature | Corax 1.0 | Corax 2.0 | Status |
|---------|-----------|-----------|--------|
| Spatial within/contains/disjoint/intersects | `SpatialMatch` with geohash term generation | `SpatialFilter` primitive | Covered |
| Spatial distance scoring | `SpatialScore` computes distance to center | `SortByDistance` from `DistanceLookup` | Covered |
| Spatial + boosting | `SpatialMatch<HasBoosting>` | `DistanceLookup` carries distances for `@distance` metadata | Covered |

## Query Optimisation

| Feature | Corax 1.0 | Corax 2.0 | Status |
|---------|-----------|-----------|--------|
| Range fusion (`x > a AND x < b` → BETWEEN) | `CoraxQueryBuilder` lines 306-331 | Pass 3 — Range fusion | Covered |
| Operand reordering by cardinality | `CoraxAndQueries.PrioritizeSort` | Pass 4 — Operand reordering | Covered |
| MultiUnary scan (< 32K threshold) | `CoraxAndQueries.ShouldPerformScan` | Dynamic goto promotion with `ShouldSwitchToEntryScan` | Covered |
| IN-clause batching (same field → InQuery) | `CoraxOrQueries._termMatchesList` | Pass 6 — IN-clause batching → `LazyOrWithPostings` | Covered |
| Sort-skip (WHERE = ORDER BY field) | `StreamingOptimization.TrySetAsStreamingField` | Pass 5 — Sort-skip detection → `OrderedRangeScan` | Covered |
| NOT optimisation (`And(X, AndNot(All, Y))` → `AndNot(X, Y)`) | Implicit in `CoraxAndQueries` | ANDNOT handled directly by `AndNotWithPostings` — no AllEntries needed | Covered |
| `true OR X → true` (constant folding) | Not implemented in 1.0 | Pass 1 — Constant folding | New in 2.0 |
| `when(false, X)` → identity | `CoraxWhenQuery` (neutral element) | Constant folding | Covered |
| Memoization of outer match | `MemoizationMatchProvider` | Not needed — bitmap is the memoized form | Covered |
| Deduplication | `DeduplicationMatch` | Bitmap is inherently deduplicated | Covered |

## Server Integration

| Feature | Corax 1.0 | Corax 2.0 | Status |
|---------|-----------|-----------|--------|
| Fill(Span<long>) loop | `IQueryMatch.Fill()` in QueryInternal | Bitmap `IterateInto` or `SortWithFilter` output | Covered |
| Entry ID → document loading | `GetEntryTermsReader` + `IQueryResultRetriever` | Same — unchanged | Covered |
| LIMIT / pagination | `pageSize` + `query.Start`, Fill loop exit | `ctx.Limit` passed to primitives + early exit | Covered |
| FILTER (JavaScript post-filter) | `QueryFilter` applied per-document in QueryInternal | Unchanged — applied after entry IDs, not part of Corax | Covered |
| DISTINCT | `IdentityTracker<TDistinct>` in QueryInternal | Unchanged — applied after entry IDs | Covered |
| Highlighting | `CoraxHighlightingTermIndex` metadata, per-term tracking | Not part of query execution — metadata carried through | Covered |
| EXPLAIN | `QueryInspectionNode` via `Inspect()` | EXPLAIN C# source from `QueryPlan.ExplainSource` | Covered |
| Faceted queries | `CoraxIndexFacetedReadOperation` uses `CoraxQueryBuilder` | Not addressed | **Gap** |
| Suggestions | `CoraxSuggestionIndexReader` uses `IndexSearcher.Suggest()` | Not affected — suggestion path doesn't use IQueryMatch | Covered |

### Faceted queries detail

`CoraxIndexFacetedReadOperation` has two paths:
1. **Indexed path** (no aggregations): materializes matching doc IDs to
   HashSet, intersects with per-facet-term posting lists. No IQueryMatch
   needed beyond the initial WHERE clause.
2. **Scanning path** (with aggregations): requires per-document field
   access via `EntryTermsReader`.

**Corax 2.0 gap:** The faceted operation calls `CoraxQueryBuilder.BuildQuery`
to get the WHERE clause match. This needs to use `QueryPlanBuilder`
instead. The faceted-specific logic (posting list intersection, term
aggregation) stays — only the WHERE clause execution changes.

## Phrase Queries

| Feature | Corax 1.0 | Corax 2.0 | Status |
|---------|-----------|-----------|--------|
| Phrase match | `PharseMatch` scans term vectors for subsequence | Not addressed | **Gap** |

### Phrase query detail

`PharseMatch` takes a term match and post-filters by checking if the
matched terms appear as a phrase (in order) in the document's stored
term vector. It reads `EntryTermsReader`, extracts term positions,
sorts by position, and searches for the phrase subsequence.

**Corax 2.0 gap:** Phrase matching is a per-entry post-filter — it
reads the entry blob and checks term positions. This maps naturally to
the entry-scan pattern: after bitmap ops narrow the candidate set, run
the phrase check per entry. The existing `PharseMatch` logic (term
position extraction and subsequence search) should be extracted into a
primitive or reused as a `MultiUnaryItem`-like predicate that
`ScanAndFilter` can evaluate.

However, phrase matching is different from `MultiUnaryItem` — it doesn't
compare a single field value against a constant. It checks the
*ordering* of multiple terms within the same field. This needs a
dedicated predicate type (e.g., `PhraseCheckItem`) alongside
`MultiUnaryItem`, or a separate `ScanAndFilterPhrase` primitive.

## Type Erasure / Match Composition

| Feature | Corax 1.0 | Corax 2.0 | Status |
|---------|-----------|-----------|--------|
| Seven *.Erasure.cs files | Box generic structs for collections | Deleted — DynamicMethod calls concrete types | Covered |
| delegate* function pointers | Runtime dispatch in BinaryMatch | Deleted — IL calls static primitives directly | Covered |
| IQueryMatch interface | Streaming contract | Deleted — bitmap + primitives | Covered |
| MemoizationMatch | Buffer entire match for reuse | Bitmap IS the memoized form | Covered |
| MergeHelper | Merge two sorted spans | Bitmap AND/OR replaces | Covered |
| DeduplicationMatch | Remove cross-batch duplicates | Bitmap is inherently unique | Covered |
| RoaringBitmapMatch | Bridge bitmap to IQueryMatch | Deleted — bitmap used directly | Covered |

---

## Summary of Gaps

| # | Gap | Severity | Resolution |
|---|-----|----------|------------|
| 1 | Multi-field ORDER BY | High | `SortWithFilter` needs `OrderMetadata[]` with cascading comparers for tie-breaking. Reuse existing comparers. |
| 2 | Compound field sort-skip | Medium | Add planner pass using `Index.HasCompoundField()`. Emit `OrderedRangeScan` on compound field CompactTree. |
| 3 | Null first / null last in sort | Medium | OR the null posting list into bitmap, sort comparers treat null as min/max per flag. |
| 4 | Alphanumeric sort | Low | Port `EntryComparerByTermAlphaNumeric` into `SortWithFilter` comparers. |
| 5 | ORDER BY random | Low | Port `RandomDirection` into `SortWithFilter`. |
| ~~6~~ | ~~Multi-vector search~~ | ~~Medium~~ | Resolved: `VectorRank` handles single vs multi internally via runtime parameter type check. |
| 7 | Phrase queries | High | Need `PhraseCheckItem` or `ScanAndFilterPhrase` primitive for term-position-based filtering. |
| 8 | Faceted queries | Medium | `CoraxIndexFacetedReadOperation` needs to call `QueryPlanBuilder` instead of `CoraxQueryBuilder` for WHERE clause. |

All gaps are addressable within the existing design framework — they
need additions to `SortWithFilter`, planner passes, or new predicate
types for `ScanAndFilter`. No architectural changes needed.
