# Corax Query Pipeline: v7.2 vs RavenDB-25281 Branch

## Architectural Overview

### v7.2: Optimizer Tree → Lazy Materialization

```
RQL AST
  │
  ▼
CoraxQueryBuilder.BuildQuery()
  │  ToCoraxQuery() walks AST recursively
  │  Produces optimizer nodes: CoraxBooleanItem, CoraxAndQueries, CoraxOrQueries, CoraxVectorItem
  │
  ▼
streamingOptimization (stack struct, ref-threaded through all methods)
  │  Detects: WHERE field == ORDER BY field → enables forward/backward iterators
  │  If streaming: SkipOrderByClause = true → SortingMatch omitted
  │
  ▼
MaterializeWhenNeeded()
  │  CoraxAndQueries.Materialize()  → MultiUnaryMatch scan OR chained And()
  │  CoraxOrQueries.Materialize()   → InQuery consolidation
  │  CoraxBooleanItem.Materialize() → TermQuery / BetweenQuery / RangeQuery
  │  CoraxVectorItem.Materialize()  → VectorSearch + AND with prior matches
  │
  ▼
DeduplicationMatch() ← if duplicates possible
OrderBy()            ← SortingMatch / SortingMultiMatch (if not skipped)
```

Key characteristics:
- **Optimizer-first**: Clauses exist as optimizer nodes BEFORE becoming IQueryMatch objects
- **Deferred materialization**: Clauses can be reordered, merged, or converted to scans before execution
- **Streaming is baked in**: MultiTermMatch has `_doNotSortResultsDueToStreaming` flag; BetweenQuery has `streamingEnabled` parameter
- **Single query context**: `CoraxQueryBuilder.Parameters` struct holds everything (searcher, allocator, fields, timings, etc.)

### Branch: Plan → IL Compile → Bitmap Pipeline

```
RQL AST
  │
  ▼
QueryPlanBuilder.BuildAndCompile()
  │  ParseTemplate() → ClauseTemplate (immutable, cached)
  │  PopulateClauseValues() → PackedParam (typed packed 32-bit)
  │  EstimateCardinality() → sort clauses by selectivity
  │  EmitPlan() → PlanOp[] (linear sequence)
  │
  ▼
QueryILEmitter.EmitDelegate()
  │  Compiles PlanOp[] to DynamicMethod IL
  │  IL calls QueryPrimitives.Ctx* static methods
  │
  ▼
CompiledQueryMatch (cached per query text + operand order)
  │  Bitmaps[] pool (slot 0=main, 1=scratch, 2=save)
  │  ResolvedMatches[], PostingSources[], TermsProviders[]
  │
  ▼
Post-build optimizations (in CoraxIndexReadOperation):
  ├─ DirectScan: bypasses bitmap for range-ORDER BY patterns
  ├─ Compound field: single tree lookup for (f1,f2) patterns
  └─ Sort seek hint: skip irrelevant tree terms in SortingMatch
  │
  ▼
SortingMatch / SortingMultiMatch ← if ORDER BY present
```

Key characteristics:
- **Plan-first**: AST → flat PlanOp[] sequence → IL-compiled delegate
- **Bitmap-centric**: All AND/OR operations are bitmap set ops (RoaringBitmap)
- **JIT-compiled execution**: QueryLogic is compiled to IL once, reused across invocations
- **Plan cache**: Shared per index instance, two-generation eviction, SIMD key lookup

---

## Optimizations Comparison

### 1. Sort Avoidance (Streaming / Index Walk)

| Aspect | v7.2 | Branch |
|--------|------|--------|
| **No WHERE, only ORDER BY** | `BetweenQuery(min, max, streamingEnabled=true)` → MultiTermMatch with `_doNotSortResultsDueToStreaming` → walks CompactTree directly, no SortingMatch | `SortedDrivingMatch` (via no-WHERE DirectScan, new in this branch) → walks ExistsQuery ITermsProvider → DirectScanSimpleMatch, no SortingMatch |
| **WHERE range on ORDER BY field** | `BetweenQuery` with `streamingEnabled=true`, forward/backward direction set → no SortingMatch | `SortedDrivingMatch` via `TryCreateSimpleFieldDirectScan` → DirectScan, no SortingMatch |
| **WHERE equals on ORDER BY field** | `CoraxBooleanItem.TrySetAsStreamingField` → BetweenQuery with exact bounds → no SortingMatch | **Not DirectScan-eligible** — goes through standard bitmap + SortingMatch path. Since all entries share the same sort key, this is a no-op sort. The bitmap path is fine here. |
| **Null/non-existing handling** | `IncludeNullMatch` + `IncludeNonExistingMatch` decorator structs interleave null/non-existing posting lists at correct stream position | `SortedDrivingMatch` has built-in iterator-based null/non-existing draining (nulls-first/last based on NullsSortMode) |
| **Multi-field ORDER BY** | Streaming on first field, SortingMultiMatch for tie-breakers | SortingMultiMatch with per-field `NullIsSmallest(comparerId)` |

### 2. AND/OR Boolean Optimization

| Aspect | v7.2 | Branch |
|--------|------|--------|
| **AND reordering** | `CoraxAndQueries.Materialize()` sorts clauses by `PrioritizeSort()`: Equals first, Between second, then by count descending | `EmitPlan()` sorts clauses by `(IsNegated, Cardinality)` — smallest (most selective) first |
| **OR consolidation** | `CoraxOrQueries` groups same-field Equals terms into `InQuery` (single multi-term lookup) | OR chain: `FillFromPostings` seeds bitmap[0], `OrWithPostings` ORs subsequent terms into bitmap[0]; limit check after each OR |
| **AND-group within OR** | Nested `CoraxAndQueries` within OR chain, materialized separately then OR-chained | Three-bitmap swap pattern: `SwapBitmaps[0,2]` (save), build AND in slot 0, `OrBitmaps[0,2]` (lazy OR), `ClearBitmap[2]` |
| **NOT handling** | `AndNot(AllEntries, TermQuery)` for standalone NotEquals | `AndNotWithPostings` bounded scan — ANDNOT the bitmap with the negated posting list |

### 3. Scan Optimization

| Aspect | v7.2 | Branch |
|--------|------|--------|
| **Entry scan** | `MultiUnaryMatch`: one cheap Equals anchor + N constraints → single-pass scan (anchor posting list < 32K entries) | Entry scan IL: `CheckAndMaybeEntryScan` → `CompiledQueryHelper.RunEntryScan()` — walks bitmap[0] entries, reads stored fields, evaluates residual predicates via second IL delegate. Activated when `bitmapCount < 32K && 64× cheaper than posting-list decode` |
| **Direct tree scan** | N/A (closest: BetweenQuery with streaming) | `TryCreateSimpleFieldDirectScan` + `TryCreateCompoundFieldMatch` — bypasses bitmap pipeline entirely for simple patterns |
| **Bounded range scan** | Not available (full posting list read) | `AndWithPostings`: uses bitmap's MinContainerKey/MaxContainerKey to Seek past below-bounds entries and prune above-bounds entries. 50K bitmap vs 10M posting list → only reads 50K pages |
| **Limit-aware AND** | Not available | `AndWithPostingsLimited`: filters each batch in-place against bitmap, stops at `limit` entries |

### 4. Sort Infrastructure

| Aspect | v7.2 | Branch |
|--------|------|--------|
| **Single-field sort** | `SortingMatch<TInner>` (full materialization + heap sort) | Same class, but with 3 strategies: (1) `SortUsingIndexFromBitmap` (index walk + bitmap intersect — no full materialization for bitmap-backed matches), (2) `SortResultsFromBitmap` (materialize + heap sort for non-index types), (3) Non-bitmap path (materialize + heap sort for non-bitmap inner matches) |
| **Multi-field sort** | `SortingMultiMatch<TInner>` (full materialization + heap sort with next-comparers chain) | Same pattern — full materialization + heap sort with compound comparers |
| **Random sort** | `SortHelper` + random seed | `ReservoirSampleFromBitmap` (Floyd's algorithm for k unique ranks, O(N) single pass) |
| **Null ordering** | `nullFirst` boolean parameter on SortingMatch | `NullsSortMode` enum (NullsSmallest/NullsLargest) — supports per-query `nulls first/last` via `OrderMetadata.NullsSortMode?`; SortingMultiMatch has per-field `NullIsSmallest(comparerId)` |

### 5. Caching

| Aspect | v7.2 | Branch |
|--------|------|--------|
| **Query plan** | N/A (no plan cache) | `PlanCache` per index instance — caches `ClauseTemplate` (AST parse) + `CompiledPlan` (IL delegate). Two-generation eviction. SIMD key lookup |
| **Result memoization** | `MemoizationMatch`: caches AllEntries / sub-query results in growable buffer for replay | Removed — bitmap pipeline avoids shared sub-queries through the combo of bitmap AND/OR operations |

### 6. Execution Model

| Aspect | v7.2 | Branch |
|--------|------|--------|
| **Dispatch** | `IQueryMatch` tree — recursive `Fill()` / `AndWith()` calls | `CompiledQueryMatch` wraps IL delegate → IL calls `QueryPrimitives.Ctx*` static methods → RoaringBitmap ops |
| **Bitmap** | `GrowableBitArray` (hash-based or bit-array per entry ID) | `RoaringBitmap` (ref struct, container-based with Array/Bitmap/Range types) — SIMD-accelerated set ops, container-level AND/OR/ANDNOT, storage recycling |
| **Per-op telemetry** | Scoped `QueryTimingsScope` | Per-op `Timings[]` and `ResultCounts[]` arrays in CompiledQueryMatch (keeps IL overhead minimal) |
| **EXPLAIN** | String appended per match by `Inspect()` recursion | Pseudocode generated during `EmitPlan`/`EmitDelegate`, stored as `ExplainSource` string on `CompiledPlan` |

### 7. Vector / Spatial

| Aspect | v7.2 | Branch |
|--------|------|--------|
| **Vector search** | `CoraxVectorItem` in optimizer tree → `VectorSearch()` with filter bitmap | `QueryPlanBuilder` separates vector clauses from AND chain, attaches as post-filter phase. `VectorSearchRetriever` accepts `RoaringBitmap` directly (not `Func<long,bool>`) |
| **Spatial** | `SpatialMatch` / `SpatialQuery()` in optimizer tree | Post-filter phase attached via `AttachPostFilterPhases()` |

---

## Key Differences Summary

### Architecture

| | v7.2 | Branch |
|--|------|--------|
| Execution | Interpreted `IQueryMatch` tree | JIT-compiled IL delegate |
| Data structure | `GrowableBitArray` | `RoaringBitmap` (container-based ref struct) |
| Clause representation | `CoraxBooleanItem` structs (deferred materialization) | `PlanOp[]` (flat, pre-compiled) + `PackedParam` (typed packed values) |
| Sort avoidance | Streaming baked into MultiTermMatch/BetweenQuery (internal `streamingEnabled` flag) | DirectScan (bypasses bitmap entirely) or SortingMatch.SortUsingIndexFromBitmap (index walk + bitmap intersect) |

### What the Branch Gains

1. **IL-compiled execution** — plan ops compile to `DynamicMethod` IL calling `QueryPrimitives` directly, avoiding per-clause `IQueryMatch.Fill()` / `AndWith()` virtual dispatch overhead
2. **Plan caching** — shared per index instance; identical query text gets cached plan without re-parsing the AST
3. **Bounded range scans** — `AndWithPostings` only reads pages overlapping the bitmap's container range
4. **Limit-aware AND** — stops accumulation early when enough results exist
5. **Container-level set ops** — RoaringBitmap's SIMD-accelerated `AndWith`/`OrWith`/`AndNotWith` with storage recycling
6. **Per-query nulls ordering** — `order by ... nulls first/last` works through `OrderMetadata.NullsSortMode`

### What the Branch Loses

1. **Memoization** — removed. The bitmap pipeline avoids shared sub-queries through bitmap AND/OR operations.
2. **MultiUnaryMatch scan** — replaced by entry-scan IL which is more general (any bitmap can be the anchor, not just one Equals clause).
3. **OR → InQuery consolidation** — removed. OR clauses are accumulated via `OrWithPostings` in the bitmap pipeline.
4. **`IncludeNullMatch` / `IncludeNonExistingMatch` decorators** — null handling is now built into `SortedDrivingMatch` and `SortedIndexReader` (iterator-based draining).

### Streaming Optimization — What Changed

v7.2's `StreamingOptimization` detected when the WHERE field equals the ORDER BY field and disabled SortingMatch. The branch achieves the same outcome through:

| v7.2 Streaming Pattern | Branch Equivalent |
|------------------------|-------------------|
| `order by X` (no WHERE) | **DirectScan** via `ExistsQuery` + `SortedDrivingMatch` → `DirectScanSimpleMatch` (new, active on this branch) |
| `where X > 5 order by X` | **DirectScan** via `BetweenQuery` + `SortedDrivingMatch` → `DirectScanSimpleMatch` or `DirectScanFilteredMatch` |
| `where X = 'val' order by X` | Standard bitmap + `SortingMatch` (sort is a no-op since all entries have the same field value) |

In v7.2, streaming avoided SortingMatch by having `MultiTermMatch` walk the tree in sort order directly. In the branch, DirectScan avoids both the bitmap pipeline AND SortingMatch by having `SortedDrivingMatch` walk the tree in sort order. For patterns that DirectScan doesn't cover, `SortingMatch.SortUsingIndexFromBitmap` walks the tree and intersects with the bitmap — functionally equivalent to v7.2's tree walk but wrapped in a SortingMatch object.
