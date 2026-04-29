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

### Benchmark expansion needed (after run 1-3 baseline)
- Vector search, full-text search, phrase queries, spatial queries, faceted queries
- Complex AND chains (4-9 clauses)
- Multiple ORDER BY (multi-field, DESC + ASC mixed)
- LIMIT variations (with/without, force full count)
- Compound field sorting
- Stream all data (full result iteration)
- NOTE: vector/spatial/facet/full-text require server-level or specialized IndexSearcher setup
  that goes beyond simple TermQuery. Will expand benchmark after baseline runs complete.

## Post-implementation Tasks (deferred)

1. **Compound field + unary filter optimisation**: For compound fields like (UserId, Date), a query `WHERE UserId = $id AND Published = true ORDER BY Date DESC` should use compound field sort-skip for UserId+Date, then filter Published via unary (entry scan). Currently compound fields only work with a single WHERE condition. The planner should detect when secondary WHERE filters can be promoted to unary mode to enable compound field optimisation.

2. **Faceted queries with bitmaps**: The indexed facet path currently materializes WHERE results to `HashSet<long>`, then per facet term streams the posting list doing `HashSet.Contains()` per entry. With bitmaps: WHERE → bitmap, then per facet term do `AndWithPostings` on a clone and read `Cardinality`. SIMD AND vs per-entry hash lookup = 2-8x faster. Also eliminates the `MaxMemoizationSizeInBytes` fallback since bitmaps are 5-25x more memory-efficient than HashSet. For aggregations (sum/avg/min/max), bitmap serves as pre-filter — iterate AND result, only read field values for matching entries.

3. **Multi-vector search**: Not a planner concern. `VectorRank` inspects the parameter type at runtime — if it's a single vector, do one HNSW search; if it's an array, run multiple searches and merge results (min distance per entry). One `VectorRank` call in the IL, single vs multi handled internally. Same pattern as cardinality checks — runtime type inspection, not plan-time.

3. **Null-first/null-last with LIMIT optimisation**: When ORDER BY includes null positioning and LIMIT is present, check if we can short-circuit: if nullFirst=true, read null entries first, check validity, return from them before touching the main result set. Evaluate whether this makes practical sense (are there real queries that benefit?).
