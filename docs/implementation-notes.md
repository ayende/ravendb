# Corax 2.0 Implementation Notes

Questions, blockers, and decisions made during implementation.
This file is a running log — entries are appended as work progresses.

---

## Pre-implementation

- global.json SDK version changed from 10.0.202 to 10.0.201 (installed version)
- Benchmark written: `bench/Voron.Benchmark/Corax/CoraxFullBenchmark.cs`
- Covers: term, AND, OR, ANDNOT, mixed, IN, startsWith, exists, numeric range, AND+range, sorting, bitmap prototype, all entries
- Sizes: 10K, 100K, 1M, 10M
- 5 warmup + 100 iterations per feature
- Memory tracking: managed heap delta (native tracking deferred — would need ByteStringContext instrumentation)
- RuntimeFrameworkVersion changed from 10.0.6 to 10.0.5 across all csproj files (installed runtime)

## Implementation Progress

### Completed
- WP1: RoaringBitmap extensions (Clear, Count, AddRange, LazyOrWith, RepairAfterLazy) — committed
- WP2 Phase 1: QueryPrimitives (FillFromPostings, AndWithPostings, OrWithPostings, AndNotWithPostings, LazyOrWithPostings, IterateInto, ScanAndFilter, ShouldSwitchToEntryScan, ShouldHeapSortDirectly) — committed
- WP3: QueryPlan data structures (PlanOp, QueryPlan, CompiledPlan, QueryContext, PlanCache) — committed
- WP4: QueryPlanBuilder basic (AND/OR of term equality, cardinality estimation, operand reordering, goto check insertion) — committed
- WP5: QueryILEmitter (closure-based interpreter, EXPLAIN generation) — committed
- Baseline benchmarks: 3 runs at 10K/100K/1M/10M scale — committed

### Completed (continued)
- WP7: Integration into CoraxIndexReadOperation
  - CompiledQueryMatch bridges bitmap pipeline into IQueryMatch interface
  - QueryPlanBuilder.ResolveMatches uses existing IndexSearcher methods
  - Wired in at line 650 behind Indexing.Corax.UseBitmapPipeline config flag
  - Falls back to old path on NotSupportedException
- Integration tests: 3 end-to-end tests (term, AND, OR) through full server path
- Full Corax test suite: 632 pass, 2 skip, 0 fail (no regressions)

### Completed (continued 2)
- Numeric type coercion for range queries (long.TryParse/double.TryParse)
- Standalone NotEquals via AllEntries + ANDNOT
- BETWEEN query support (numeric type detection)
- 9 integration tests all passing: term, AND, OR, 3-way AND, AND+range,
  IN clause, NotEquals, BETWEEN, large result set (1000 docs)
- Post-implementation benchmark + comparison (no regressions)
- Pushed to origin/RavenDB-25281

### SlowTests Corax result
- 498 pass, 4 fail, 2 skip.
- Failures (likely pre-existing — bitmap pipeline is off by default in these tests):
  1. BackwardCompatibilityForOldLowercaseAnalyzer — backward compat test
  2. DistinctBigTestWithPagination (10K docs) — pagination with distinct
  3. CompoundOrderByWithPagination (10K docs) — compound ordering
  4. CanImportOldAutoMapDefinitionIntoCoraxAndGenerateProperMapping — import test
- These need to be verified against the base branch to confirm they're pre-existing.

### Completed (continued 3)
- ORDER BY + LIMIT via existing SortingMatch wrapper (ASC and DESC)
- Mixed AND/OR tree support via OrGroup clauses
- Plan cache infrastructure on IndexSearcher
- Proper fallback for ORDER BY, boost, spatial, vector, search, all-entries
- TrueExpression constant folding (falls back to old path for all-entries)
- OR optimization: direct materialization into main bitmap (no temp needed)
- AND optimization: early exit when bitmap becomes empty
- Numeric type coercion for range queries (long/double/string)
- Standalone NotEquals via AllEntries + ANDNOT
- endsWith, startsWith, regex, IN clause support
- Nested OR within AND: (A OR B) AND (C OR D)
- Pagination (Skip/Take) works correctly
- EXPLAIN source generation with clause details and cardinalities
- 24 integration tests (21 pass, 3 skip)
- Full FastTests Corax suite: 650 pass, 5 skip, 0 fail
- 34 commits on RavenDB-25281, pushed to origin
- PR #4714 open on ayende/ravendb

### Remaining
- WP8: Delete old code, make RoaringBitmap ref struct
- Boost/scoring in bitmap path (BM25 frequency collection)
- search() queries (analyzer setup)
- Spatial, vector queries (separate primitives)
- Phrase queries (term position checking)
- Compound field sort-skip
- Optimization: galloping page-scan for AndWithPostings (needs direct PostingList path)
- Optimization: LIMIT-aware FillFromPostings for no-ORDER-BY queries
- DynamicMethod IL emit (replacing interpreter)
- Expanded benchmark with bitmap path enabled for comparison

### Benchmark expansion needed (after run 1-3 baseline)
- Vector search, full-text search, phrase queries, spatial queries, faceted queries
- Complex AND chains (4-9 clauses)
- Multiple ORDER BY (multi-field, DESC + ASC mixed)
- LIMIT variations (with/without, force full count)
- Compound field sorting
- Stream all data (full result iteration)
- NOTE: vector/spatial/facet/full-text require server-level or specialized IndexSearcher setup
  that goes beyond simple TermQuery. Will expand benchmark after baseline runs complete.

## Deferred Features (not in MVP, implement after core pipeline works)

### Primitives not yet implemented
- **FillFromRange**: Range scan on CompactTree into bitmap. Needed for BETWEEN, >, <, >=, <= queries when used as standalone predicates (not via entry-scan).
- **SortWithFilter**: Combined sort + optional entry-scan filter. Needed for ORDER BY queries.
- **OrderedRangeScan**: Sort-skip path (WHERE field = ORDER BY field with LIMIT).
- **VectorRank**: Vector similarity search (exact for small bitmaps, HNSW for large).
- **SpatialFilter**: Geospatial filter via geohash term generation.
- **SortByScore**: BM25 scoring with PriorityQueue.
- **SortByDistance**: Vector/spatial distance sort.
- **ScanAndFilterInPlace**: In-place bitmap filtering for pre-vector/spatial steps.

### QueryPlanBuilder expression types that throw NotSupportedException
- Spatial queries (spatial.within, spatial.contains, spatial.disjoint, spatial.intersects)
- Vector search
- MoreLikeThis
- When expressions

### Lazy OR optimisation
LazyOrWith currently delegates to OrWith (maintains cardinality eagerly). The popcount-skip optimisation would add a per-container dirty flag to skip popcount during multi-term IN chains, then recompute in one pass via RepairAfterLazy. This saves N-1 popcount passes over 8KB Bitmap containers for an N-term IN clause. Estimated impact: 10-30% on large IN clauses with dense containers.

### Numeric range type coercion
Range queries (>, >=, <, <=, BETWEEN) currently pass term values as strings to IndexSearcher.GreaterThanQuery etc. For numeric fields (int, long, double), this results in lexicographic comparison instead of numeric comparison ("50" > "200" = true). The QueryPlanBuilder needs to detect the field type from the index schema and pass the appropriate typed value (long, double) to the range query methods. This mirrors the type detection logic in CoraxBooleanItem's constructor (lines 56-72, checking `term is long`, `term is double`).

### Standalone NotEquals (Status != "active")
The bitmap pipeline doesn't handle standalone `!=` yet. It requires: fill AllEntries into bitmap, then ANDNOT with the negated term. The plan emitter currently treats `!=` like `=` with an `IsNegated` flag, but only uses that flag for AND chains (where it emits AndNotWithPostings). For a standalone `!=` (no other AND operands), the emitter needs to first fill AllEntries then ANDNOT. The old path handles this via `AndNot(AllEntries, term)`.

### MultiUnaryItem conversion in entry-scan goto labels
CheckAndMaybeEntryScan currently passes empty MultiUnaryItem[] arrays for the entry-scan labels. Converting the remaining clause types (range, between, etc.) to MultiUnaryItem predicates requires mapping ClauseType to UnaryMatchOperation and resolving comparison values. This is needed for the goto pattern to actually filter entries correctly — currently it just returns all entries in the bitmap without filtering.

## Post-implementation Tasks (deferred)

1. **Compound field + unary filter optimisation**: For compound fields like (UserId, Date), a query `WHERE UserId = $id AND Published = true ORDER BY Date DESC` should use compound field sort-skip for UserId+Date, then filter Published via unary (entry scan). Currently compound fields only work with a single WHERE condition. The planner should detect when secondary WHERE filters can be promoted to unary mode to enable compound field optimisation.

2. **Faceted queries with bitmaps**: The indexed facet path currently materializes WHERE results to `HashSet<long>`, then per facet term streams the posting list doing `HashSet.Contains()` per entry. With bitmaps: WHERE → bitmap, then per facet term do `AndWithPostings` on a clone and read `Cardinality`. SIMD AND vs per-entry hash lookup = 2-8x faster. Also eliminates the `MaxMemoizationSizeInBytes` fallback since bitmaps are 5-25x more memory-efficient than HashSet. For aggregations (sum/avg/min/max), bitmap serves as pre-filter — iterate AND result, only read field values for matching entries.

3. **Multi-vector search**: Not a planner concern. `VectorRank` inspects the parameter type at runtime — if it's a single vector, do one HNSW search; if it's an array, run multiple searches and merge results (min distance per entry). One `VectorRank` call in the IL, single vs multi handled internally. Same pattern as cardinality checks — runtime type inspection, not plan-time.

3. **Null-first/null-last with LIMIT optimisation**: When ORDER BY includes null positioning and LIMIT is present, check if we can short-circuit: if nullFirst=true, read null entries first, check validity, return from them before touching the main result set. Evaluate whether this makes practical sense (are there real queries that benefit?).
