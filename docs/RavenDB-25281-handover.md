# RavenDB-25281 Handover Document

**Branch:** `RavenDB-25281`  
**Base:** `v7.2`  

---

## Section 1 — Architectural Summary (for AI agents)

### 1.1 Why this branch exists — the problem with the old pipeline

The pre-existing Corax query pipeline was built around an `IQueryMatch` interface with a deep hierarchy of generic structs (`BinaryMatch<TLeft,TRight,TMode>`, `MemoizationMatch`, `MultiUnaryMatch`, etc.). Each operator was a separate generic type, which required runtime erasure types (`*.Erasure.cs` wrapper classes) to box them behind a common interface for composition. The resulting object graph was:

- **Allocation-heavy** — every combinator allocated a new struct/class and its associated buffers.
- **Hard to extend** — adding a new operator required a new generic struct, an Erasure wrapper, and wiring through `IndexSearcher`.
- **No plan reuse** — every query compiled fresh with no caching of the query structure.
- **Full materialisation before sorting** — `SortingMatch` always wrapped the inner match in `MemoizationMatch`, draining all results into a flat array before sorting. For `ORDER BY Name LIMIT 10` on 1 M entries this meant reading and copying 1 M records.

### 1.2 The new architecture — Corax 2.0

The replacement is a **plan–compile–execute** pipeline operating on `RoaringBitmap` as the single intermediate representation.

```
Query text
    ↓
QueryPlanBuilder.BuildPlan()   ← parse AST → ClauseInfo list → PlanOp[]
    ↓
PlanCache.GetOrCompile()       ← cache key: query text + OperandOrdering int
    ↓
QueryILEmitter.Compile()       ← DynamicMethod over PlanOp[]
    ↓
CompiledQueryMatch             ← IQueryMatch wrapper that holds compiled delegate + bitmap
    ↓ Execute()
  compiled delegate(ref QueryScanContext)
    ├─ FillFromPostings / FillBitmapFromTermSource
    ├─ AndWithPostings / AndWithTermSource
    ├─ OrWithPostings
    ├─ AndNotWithPostings
    └─ CheckAndMaybeEntryScan → (keep bitmap ops) OR (switch to EntryTermsReader scan)
    ↓
  bitmaps[0] holds final entry-ID set
    ↓
SortingMatch                   ← bitmap-aware path: skips MemoizationMatch entirely
    ↓
CoraxIndexReadOperation        ← iterates entry IDs → document load → yield
```

#### Key design decisions

| Decision | Rationale |
|---|---|
| `RoaringBitmap` as sole IR | One data structure for all set ops; O(1) `Contains()` enables bitmap-aware sorting without materialisation |
| IL emission (DynamicMethod) | Zero virtual-dispatch overhead per-op; the plan shape is baked into bytecode, per-call values are resolved at execution time |
| Two-level plan cache | Outer key = query text (amortises parse + clause-shape work); inner key = `OperandOrdering` int (cardinality-reordering differs per call) |
| `TermSource` union | Single/SmallPostingList/PostingList.Iterator resolved once per query, avoiding repeated CompactTree lookups inside the emitted loop |
| `CheckAndMaybeEntryScan` op | Runtime escape hatch: after a tight AND step reduces the bitmap below ~32 K entries, the emitted IL switches from bitmap set-ops to per-entry `EntryTermsReader` scans for remaining predicates — matching the old `CoraxAndQueries.ShouldPerformScan` heuristic |
| `UseTermSource = false` for boosted queries | Boosted queries need `Bm25Relevance._matchBuffer` populated, which only happens when `TermMatch.Fill()` is called through the `IQueryMatch` path (not the native posting-list dispatcher) |
| Score ordering: always `ascending=true` | `EntryComparerByScore.SortBatch` uses `BuildSingleNumericalSorter(descending == false, ...)`, so `ascending=true` → K-largest heap → highest scores first. Both `score() ASC` and `score() DESC` in RQL mean "highest relevance first" by convention — the parser cannot distinguish them |

### 1.3 New file layout

```
src/
├── Voron/Data/RoaringBitmaps/
│   ├── RoaringBitmap.cs            ← ref struct (C# 13); all set ops, PrepareForReading, Dispose
│   ├── RoaringBitmapData.cs        ← unmanaged-safe companion struct (class/struct fields)
│   ├── RoaringBitmapIterator.cs    ← streaming iterator over prepared bitmap
│   ├── RoaringBitmap.Primitives.cs ← SIMD helpers (SimdLinearContains, etc.)
│   ├── ContainerEntry.cs           ← Array / Bitmap / Range container union
│   └── ContainerType.cs            ← enum: Array, ArrayUnsorted, Bitmap, Range, Free
│
├── Corax/Querying/
│   ├── Planning/
│   │   ├── QueryPlan.cs            ← PlanOp[], TermSource, PlanOpKind enum, OperandOrdering
│   │   ├── QueryContext.cs         ← ref struct QueryScanContext (delegate argument)
│   │   ├── QueryILEmitter.cs       ← DynamicMethod emitter; one method per PlanOp kind
│   │   ├── CompiledPlan.cs         ← wrapper: delegate + ExplainSource string + ordering key
│   │   └── PlanCache.cs            ← two-level concurrent cache; 512 query texts × 32 orderings
│   │
│   ├── Primitives/
│   │   └── Primitives.cs           ← QueryPrimitives static class: FillFromPostings, AndWithPostings,
│   │                                  OrWithPostings, AndNotWithPostings, FillBitmapFromTermSource,
│   │                                  AndWithTermSource, AndNotWithTermSource, FillFromMatch,
│   │                                  AndWithMatch, ShouldSwitchToEntryScan, ShouldHeapSortDirectly
│   │
│   └── Matches/
│       ├── CompiledQueryMatch.cs   ← IQueryMatch + IBitmapQueryMatch; owns the post-execute bitmap
│       ├── BitmapMatch.cs          ← lightweight IQueryMatch wrapping a bitmap (search/OR results)
│       └── Meta/IQueryMatch.cs     ← adds IBitmapQueryMatch interface (Contains, MinEntryId, MaxEntryId)
│
└── Raven.Server/Documents/Indexes/Persistence/Corax/
    ├── QueryPlanBuilder.cs          ← 3 500 lines; the main query compiler
    │   ├── BuildPlan()              ← AST → ClauseInfo[] → PlanOp[] with optimisation passes
    │   ├── BuildAndCompile()        ← public entry point; handles cache, boost flag, fallback
    │   ├── GetSortMetadata()        ← ORDER BY → OrderMetadata[] (score always ascending=true)
    │   ├── ApplyScoreOrdering()     ← test/direct-path score sort helper
    │   ├── GetInTermValueTokenType()← type inference for IN-clause parameters (incl. DateTime)
    │   └── ConvertInValue()         ← DateTime/LazyStringValue → ticks string
    └── QueryBuilderParameters.cs    ← parameter bag shared with sort/spatial/vector helpers
```

### 1.4 Deleted code

| Deleted | Replaced by |
|---|---|
| `CoraxQueryBuilder.cs` + `.Parameters.cs` + `.VectorSearch.cs` | `QueryPlanBuilder.cs` absorbs all query-building logic |
| `BinaryMatch<TLeft,TRight,TMode>` | No longer needed — set ops done on `RoaringBitmapData` directly |
| `MemoizationMatch` | `CompiledQueryMatch` owns its own bitmap; `SortingMatch` has bitmap-aware path |
| `MultiUnaryMatch` + `.Erasure.cs` | Entry-scan predicates emitted directly in IL via `EmitEntryScan` |
| `AndNotMatch` + `.Erasure.cs` | `AndNotWithPostings` / `AndNotWithMatch` in Primitives |
| `CombinedMatch` | `BitmapMatch` + `OrWithPostings` in Primitives |
| `MergeHelper` | `RoaringBitmap.LazyOrWith` + `RepairAfterLazy` |
| `IndexSearcher.BinaryMatches.cs` | N/A |
| `IndexSearcher.MultiUnaryMatches.cs` | N/A |
| `IndexSearcher.MultiTermMatches.In.cs` | IN-clause handled in `QueryPlanBuilder` |
| `IndexSearcher.QueryBuilderHelper.cs` | Moved to `QueryBuilderHelper.cs` in Raven.Server |
| `QueryOptimizer/CoraxAndQueries.cs` | Folded into `QueryPlanBuilder.EmitOps()` |
| All `docs/*.md` design docs | Superseded by this document and code comments |
| All `RoaringBitmap*Bench.cs` benchmarks | Removed; replaced by full-pipeline benchmarks |

### 1.5 Dead ends and refactoring paths not taken

1. **Bitmap fast path in FillFromMatch** (reverted, commit `844ade9`): An early attempt embedded a bitmap fast path directly inside `FillFromMatch`. It was reverted because it caused incorrect results when `FillFromMatch` was called on a match that had already partially consumed — the bitmap was built from only the remaining entries.

2. **Memoize-then-sort approach for bitmap inner matches**: The original `SortingMatch` always used `MemoizationMatch` (drain all IDs → sort). The bitmap-aware path in `SortingMatch.Fill` (`SortUsingIndexFromBitmap`) now walks the CompactTree sort index using `IBitmapQueryMatch.Contains()` for intersection, stopping early when `_take` results are collected. This was the primary motivation for the `IBitmapQueryMatch` interface on `CompiledQueryMatch`.

3. **Score ordering direction swap** (abandoned): An early fix for BM25 ordering tried swapping `ascending/descending` in `GetSortMetadata`. This broke `SpatialBoostingInCorax` because `OrderByScore()` uses `ascending=true` by default, which after the swap became lowest-score-first. The correct fix is: score always uses `ascending=true` regardless of the RQL direction keyword, since `ascending=true` → `EntryComparerByScore` → K-largest heap → highest relevance first.

4. **`ref struct RoaringBitmap` holding allocator** (WP8): Made `RoaringBitmap` a `ref struct` so it cannot escape to the heap, and `_ctx` is always valid for the struct's lifetime. However `RoaringBitmapData` (the companion non-ref struct) still exists so the bitmap state can be stored in class fields (`CompiledQueryMatch._bitmapData`).

### 1.6 Known non-obvious invariants


- **`LazyCardinality = -1`** sentinel: bitmap containers updated via `LazyOrWith` leave `entry.Cardinality = -1`. `RoaringBitmapData.Count` explicitly handles this by calling popcount directly. `PrepareForReading` converts lazy containers to sorted arrays / proper bitmaps and frees zero-cardinality ones.

- **`UseTermSource = false` for `HasBoost`**: When any clause has a boost factor, the plan builder sets all `PlanOp.UseTermSource = false` and records the boost via `OperandOrdering |= (1 << 30)`. This forces the emitted IL to go through `FillFromMatch` / `AndWithMatch`, which call `TermMatch.Fill()`, which populates `Bm25Relevance._matchBuffer` before `Score()` is invoked.

- **Cache key = query text + `OperandOrdering` int**: Cardinality reordering of AND operands is done at plan time and recorded as a packed int (3 bits per operand position). Two calls with the same text but different data may reorder operands differently; the cache stores up to 32 orderings per query text.

- **`GetInTermValueTokenType` must mirror `ConvertInValue`**: These two functions are always called as a pair (terms.Add / termTypes.Add). If they disagree on the type (e.g., `ConvertInValue` produces ticks string but `GetInTermValueTokenType` returns `String`), `ResolveInTermSource` uses the wrong posting-list lookup and finds nothing.

---

## Section 2 — Review Guide (for humans and AI reviewers)

### 2.1 Key areas of focus

#### `QueryPlanBuilder.cs` — the core compiler (3 500 lines)

The most critical file. Review priority order:

| Method | Lines | What to check |
|---|---|---|
| `BuildPlan(PlanParameters)` | ~206–430 | Optimisation passes: constant folding, range fusion, operand reordering, IN-clause batching, CheckAndMaybeEntryScan insertion |
| `RefreshClauseValues()` | ~635–725 | Per-call value hydration from cached shape; must mirror `ParseClauseInfo` exactly for every parameter-bearing field |
| `GetInTermValueTokenType()` | ~1131–1152 | Type inference for IN parameters — must agree with `ConvertInValue`. See §2.3 |
| `EmitOps()` / `EmitAndChain()` | ~1480–1700 | PlanOp[] construction: correct bitmap-local assignment, correct CheckAndMaybeEntryScan placement |
| `ResolveInTermSource()` | ~2380–2411 | Long vs string vs double posting-list dispatch for each IN term |
| `GetSortMetadata()` | ~2795–2860 | Score always `ascending=true`; spatial/vector special cases |
| `ApplyScoreOrdering()` | ~2984–3010 | Test/direct-path helper; same invariant as `GetSortMetadata` |

#### `QueryILEmitter.cs` — the IL generator (1 066 lines)

| Method | What to check |
|---|---|
| `Compile(QueryPlan)` | Op dispatch loop; correct label layout for `CheckAndMaybeEntryScan`; `hasEntryScan` flag |
| `EmitEntryScan()` | Entry scan emits `EntryTermsReader` loop — must handle all `ScanPredicateInfo` kinds |
| `EmitLoadAllocator()` | Uses `ctx.Searcher.Allocator`; verify this is non-null in all call sites |
| `EmitExplainSource()` | Lazy string builder; deferred until first read |

#### `Primitives.cs` — the low-level bitmap ops (641 lines)

| Method | What to check |
|---|---|
| `FillFromPostings` | Page-loop with `limit` cap; does not overshoot beyond one page after limit reached |
| `AndWithPostings` | Galloping pattern — seek posting list to first set bit in bitmap; SIMD AND per page |
| `FillBitmapFromTermSource` | Three-way dispatch: Single (add 1 entry), SmallPostingList (decode container), PostingList.Iterator |
| `ShouldSwitchToEntryScan` | Threshold: `bitmap.Count < 32_000 && bitmap.Count * 64 < postingListCount` |
| `ShouldHeapSortDirectly` | Threshold for choosing heap-sort vs index-walk in `SortingMatch` |

#### `RoaringBitmap.cs` — the bitmap engine (1 396 lines)

| Area | What to check |
|---|---|
| Container transitions | Array → Bitmap promotion threshold (4096); Bitmap → Array demotion on `AndWith` |
| `LazyOrWith` | Sets `Cardinality = LazyCardinality`; caller must call `RepairAfterLazy()` or `PrepareForReading()` |
| `FreeContainer` | Requires non-null `_ctx` when `entry.Storage.HasValue` |
| `PrepareForReading` | Sorts `ArrayUnsorted`, recomputes lazy cardinalities, frees empty bitmap containers |
| `AndWith` / `AndNotWith` | Donate freed buffers to the other bitmap's free list when contexts match |

#### `SortingMatch.cs` — bitmap-aware sort path

Review `Fill<TEntryComparer, TDirection>` (lines ~179–269):
- Checks `match._inner is IBitmapQueryMatch` before the old `MemoizationMatch` path.
- `NoIterationOptimization` direction → `SortResultsFromBitmap` (materialise then heap-sort).
- All other directions → `SortUsingIndexFromBitmap` (walk index, filter via `IBitmapQueryMatch.Contains`, stop at `_take`).

#### `CompiledQueryMatch.cs` — the IQueryMatch wrapper (256 lines)

Review `Execute()`:
- Lines 244–248: scratch bitmaps `[1..N]` disposed; `[0]` transferred to `_bitmapData` (owned by this struct).
- `IsBoosting` property walks `_resolvedMatches` — only populated when `UseTermSource = false`.

### 2.2 Breaking changes and mitigations

| Change | Impact | Mitigation |
|---|---|---|
| `IndexSearcher.And()`, `.Or()`, `CreateMultiUnaryMatch()`, `TermQuery()` (public) removed | Any external caller will get compile errors | `IndexSearcher.Deleted.cs` contains stub tombstones with `[Obsolete(error:true)]` providing clear error messages |
| `CoraxQueryBuilder` removed | Internal callers in Raven.Server | All callers migrated to `QueryPlanBuilder.BuildAndCompile()` |
| `MemoizationMatch` removed | `SortingMatch` and any code that called `.Memoize()` | `SortingMatch` now has bitmap-aware path; `CompiledQueryMatch` owns the bitmap |
| `MultiUnaryMatch` removed | Entry-scan predicates | Re-emitted as IL in `QueryILEmitter.EmitEntryScan()` |
| `MergeHelper` removed | OR / union helpers | `RoaringBitmap.LazyOrWith` + `RepairAfterLazy` |
| `RoaringBitmap` is now `ref struct` | Cannot be stored in fields or heap | `RoaringBitmapData` (non-ref companion struct) used for storage; `RoaringBitmap` created on-stack as a view |
| `BinaryMatch<,,>` generic tree removed | Caller-composed match trees | Plan builder emits equivalent op sequence; `BitmapMatch` wraps results where `IQueryMatch` is needed downstream |

**Not broken (verified):** Spatial queries, vector search, MoreLikeThis, faceted queries, boosting, BM25 scoring, highlighting, streaming queries, sharding.

### 2.3 Edge cases and error handling

#### DateTime IN-clause parameters (`GetInTermValueTokenType`, line ~1131)

**Problem:** When a client sends `WHERE Date IN ($p0)` with DateTime parameter values, the server deserialises the array elements as `LazyStringValue` (ISO format, e.g. `"2024-01-15T10:30:00.0000000"`). `ConvertInValue` correctly converts these to ticks strings. `GetInTermValueTokenType` must return `ValueTokenType.Long` for the same values so `ResolveInTermSource` uses `GetTermPostingListId(fieldMeta, longValue)` rather than the string posting-list lookup.

**Fix applied:** `GetInTermValueTokenType` now handles:
1. `DateTime`, `DateTimeOffset` → `Long`
2. String/LazyStringValue matching `ConvertInValue`'s date-string heuristic (length 18–35, contains `'T'`, parses as DateTime) → `Long`

**Review:** Ensure no new date/time type (e.g., `DateOnly`, `TimeOnly`) bypasses this without a corresponding `ConvertInValue` path.

#### Score ordering (`GetSortMetadata` / `ApplyScoreOrdering`)

**Convention:** RavenDB always returns highest-relevance documents first, regardless of the direction keyword in `ORDER BY score()`. The parser cannot distinguish `score() ASC` from plain `score()` — both produce `field.Ascending = true`.

**Implementation:** Both `GetSortMetadata` and `ApplyScoreOrdering` always emit `new OrderMetadata(ascending: true, MatchCompareFieldType.Score)`. `EntryComparerByScore.SortBatch` calls `BuildSingleNumericalSorter(descending == false, ...)` — when `ascending=true`, `descending=false`, so `BuildSingleNumericalSorter(true)` → K-largest heap → highest scores first.

**Review:** `Descending<EntryComparerByScore>` (used when `ascending=false`) passes `descending=true` to inner, making `BuildSingleNumericalSorter(false)` → K-smallest heap → lowest scores first. This path is now unreachable for score fields but the code is still present in `SortingMatch.Comparers.cs`.

#### Null allocator in `RoaringBitmap.FreeContainer`

**Trigger:** `PrepareForReading()` called without an allocator, on a bitmap that has non-empty container storage (e.g., a bitmap container from a lazy-OR path that ended up with Cardinality = 0 after RepairAfterLazy). The sharded path triggers this more often because cross-shard OR operations produce more lazy containers.

**Fix:** `_bitmapData.PrepareForReading(_allocator)` in `CompiledQueryMatch.Execute()` line 238. All other `PrepareForReading()` call sites in `RoaringBitmapData` already accept and forward a nullable context — passing null is only safe if the bitmap is known to have no allocated container storage.

#### `CheckAndMaybeEntryScan` placement

The emitted IL checks `ShouldSwitchToEntryScan(bitmap.Count, directSource.Count)` after each AND step. The plan builder (in `EmitOps`) must only insert `CheckAndMaybeEntryScan` before an operand that can supply a `.Count` estimate (i.e., a `DirectSources` entry, not a `TermSource`). Verify that `UseTermSource = true` ops are never immediately preceded by a `CheckAndMaybeEntryScan` that references them via `ParamIndex`.

#### IN-clause with null values

When an IN-clause includes `null` (e.g., `WHERE Date IN ($p0)` where `$p0` includes a null element), `ConvertInValue(null, ...)` returns `null` and `GetInTermValueTokenType(null, ...)` returns `ValueTokenType.String` (falls through all checks). The null term then goes through `ResolveInTermSource` as a string lookup for null — which is the correct path for null documents.

### 2.4 Testing verification

#### Running the full test suite

```bash
# Full FastTests — primary gate (3 515 tests, ~4 min)
dotnet test test/FastTests --configuration Release

# Corax + Querying category subset (~2.5 min)
dotnet test test/FastTests --configuration Release --filter "Category=Corax|Category=Querying"

# SlowTests Corax subset
dotnet test test/SlowTests --configuration Release --filter "Category=Corax"
```

#### Targeted tests for the new pipeline

```bash
# Score ordering (BM25 correctness)
dotnet test test/FastTests --configuration Release \
  --filter "FullyQualifiedName~RankingFunctionTests|FullyQualifiedName~BoostingQuery|FullyQualifiedName~RavenDB_23245"

# IN-clause DateTime fix
dotnet test test/FastTests --configuration Release \
  --filter "FullyQualifiedName~InQuery|FullyQualifiedName~QueryDateTime"

# Sharded + Corax (allocator fix)
dotnet test test/FastTests --configuration Release \
  --filter "DatabaseMode=Sharded.*Corax|FullyQualifiedName~QueryDateTime"

# Sorting / ORDER BY
dotnet test test/FastTests --configuration Release \
  --filter "FullyQualifiedName~OrderBySorting|FullyQualifiedName~CompoundSorting|FullyQualifiedName~StreamingOptimization"

# Boosting / spatial
dotnet test test/FastTests --configuration Release \
  --filter "FullyQualifiedName~SpatialBoost|FullyQualifiedName~Boost"

# Plan cache and multi-query
dotnet test test/FastTests --configuration Release \
  --filter "FullyQualifiedName~RavenDB_22603|FullyQualifiedName~CoraxQueries"
```

#### Scenarios to verify manually / via integration tests

1. **`ORDER BY score()` and `ORDER BY score() DESC`** — both must return the same (highest-relevance-first) order.
2. **`WHERE Date IN ($p0)`** with `DateTime?[]` parameter — must return the correct count (not 0).
3. **Sharded database, Corax engine, date range query** — must not throw `NullReferenceException` from `NativeList.Grow`.
4. **Boosted queries** (`boost()`) with `ORDER BY score()` — verify `_matchBuffer` is populated (no zero-score results appearing before high-score ones).
5. **Large result set with `LIMIT`** — `ORDER BY Name LIMIT 10` on 1 M entries should walk the sort index and stop early (verify via timing or `EXPLAIN`).
6. **`ORDER BY` with `NULL FIRST` / `NULL LAST`** — null posting-list OR'd into bitmap during plan; sort comparers treat them as min/max.
7. **Vector search with score exposed** — `CoraxVectorSearchOrderByScoreAutomatically` config path; single-clause vector query gets score injected.
8. **MoreLikeThis** — uses `BitmapMatch` as the source; verify result set is non-empty.
9. **Faceted queries** — `CoraxIndexFacetedReadOperation` calls `QueryPlanBuilder.BuildAndCompile`; verify facet counts are accurate.
10. **IN-clause with mixed types** (string, long, double, null) in the same parameter array — verify `GetInTermValueTokenType` returns the correct type for each element.

#### What the `EXPLAIN` output looks like

Set `query.ExplainScores = true` or call `QueryPlanBuilder.ApplyScoreOrdering` in tests. The `CompiledQueryMatch.Inspect()` node includes an `Explain` key with the C#-pseudocode representation of the emitted IL, and per-op timing/count fields when telemetry is enabled.

---

## Section 3 — Benchmark Strategy

### 3.1 What the architecture claims to improve (the hypotheses)

Before writing or running any benchmark, pin down exactly what you are trying to validate. The new pipeline makes five distinct performance claims:

| # | Claim | Mechanism |
|---|---|---|
| H1 | **ORDER BY + LIMIT is sublinear in total results** | `SortUsingIndexFromBitmap` walks the sort index and stops at `_take`; old path drained all results via `MemoizationMatch` first |
| H2 | **Repeated identical queries are faster after the first call** | Two-level plan cache: query text hit skips parse + IL emission; ordering hit reuses compiled delegate entirely |
| H3 | **AND-chain queries have lower steady-state overhead** | IL-emitted loop with no virtual dispatch; `FillBitmapFromTermSource` bypasses `IQueryMatch` wrapper for non-boosted terms |
| H4 | **Large OR / IN queries are faster at high result counts** | `LazyOrWithPostings` defers `popcount` until `RepairAfterLazy`; old path computed cardinality after every OR step |
| H5 | **Entry-scan switch reduces work for tight AND chains** | `CheckAndMaybeEntryScan` switches to `EntryTermsReader` per-entry scan when bitmap is small; avoids posting-list traversal for remaining predicates |

Each benchmark should target exactly one hypothesis. A benchmark that mixes H1 and H3 cannot isolate the cause of a regression.

### 3.2 State of the existing benchmarks

All benchmarks live in `bench/Voron.Benchmark/` and use **BenchmarkDotNet**.

| File | Status | Issue |
|---|---|---|
| `AndOrBenchmark.cs` | **Partially broken** | `ParserQuery` / `ParserQueryOnlyIteration` call `CoraxQueryEvaluator.Search()` which now throws `NotSupportedException` for AND/OR — those methods delegate to the deleted direct API |
| `InBenchmark.cs` | **Partially broken** | `InFirstRuntimeQuery` / `InSecondRuntimeQuery` call `indexSearcher.InQuery()` which still exists; `OrFirstParserQuery` etc. call `CoraxQueryEvaluator` with AND expressions → same throw |
| `OrderByBenchmark.cs` | **Works** | Uses `indexSearcher.OrderBy()` directly; does not go through `QueryPlanBuilder`; measures only the sort path, not the full pipeline |
| `StartWithBenchmark.cs` | **Works** | Uses `indexSearcher.StartWithQuery()` directly; single-clause, no combinator |
| `CoraxQueryEvaluator.cs` | **Needs rewrite** | `AND` and `OR` cases throw; it needs to call `QueryPlanBuilder.BuildAndCompile()` instead |
| `LuceneOrderByBenchmark.cs` | **Works** | Lucene path; unaffected |
| `VectorSearchBenchmark.cs` | **Works** | Vector path; unaffected |

**First action before running any benchmark:** fix `CoraxQueryEvaluator` to call `QueryPlanBuilder.BuildAndCompile()` for multi-term queries, or replace it with a helper that builds a `PlanParameters` struct and calls `BuildAndCompile` directly. The existing `ParserQuery` benchmarks in `AndOrBenchmark` and `InBenchmark` are the natural home for the new-pipeline numbers.

### 3.3 How to run the benchmarks

```bash
# Build in Release first (mandatory — Debug disables JIT optimisations)
dotnet build bench/Voron.Benchmark -c Release

# Run all Corax benchmarks
cd bench/Voron.Benchmark
dotnet run -c Release -- --filter "*.Corax.*"

# Run a specific benchmark class
dotnet run -c Release -- --filter "*OrderByBenchmark*"

# Run with disassembly output (Windows only — requires WinDbg)
dotnet run -c Release -- --filter "*AndOrBenchmark*" --disassembly

# Run in dry mode to validate setup (no timing, fast)
dotnet run -c Release -- --filter "*AndOrBenchmark*" --dry
```

For results you can share, always use:
```bash
dotnet run -c Release -- --filter "..." \
  --exporters json \          # machine-readable output
  --statisticalTest 5%        # flag regressions vs baseline at 5% threshold
```

The BenchmarkDotNet config in each benchmark class sets `IterationCount = 1` and `InvocationCount = 1` — **these are placeholders marked with `TODO: fine tune parameters`**. Before doing any comparative run, change these to at least:
```csharp
Run = { LaunchCount = 1, WarmupCount = 3, IterationCount = 10 }
```

### 3.4 Recommended benchmark matrix

The table below lists the benchmarks that should be written (or fixed) to validate the five hypotheses, the dataset size, and the expected direction of the result.

#### H1 — ORDER BY + LIMIT is sublinear in total results

**File to modify:** `bench/Voron.Benchmark/Corax/OrderByBenchmark.cs`

Add a new benchmark that pairs a filtered query with ORDER BY and varies `TakeSize` while holding total matches fixed:

```csharp
[Params(10, 100, 1_000, 10_000)]  // take N from 100_000 results
public int TakeSize { get; set; }

[Benchmark]
public void OrderByWithLimitNewPipeline()
{
    // 100_000 docs indexed, all match "Dog"; ORDER BY Age LIMIT TakeSize
    var planParams = new QueryPlanBuilder.PlanParameters
    {
        IndexSearcher = _indexSearcher,
        Metadata = new QueryMetadata(
            $"FROM Dogs WHERE Type = 'Dog' ORDER BY Age LIMIT {TakeSize}", null, 0),
        Allocator = _bsc, Token = default
    };
    var match = QueryPlanBuilder.BuildAndCompile(planParams, null, TakeSize, out _, null, default);
    Span<long> ids = _ids;
    while (match.Fill(ids) != 0) ;
}
```

**Expected result:** elapsed time grows slowly with `TakeSize` (logarithmic in index size), not linearly. The old `MemoizationMatch` path would be flat regardless of `TakeSize` at ~100 K entries cost.

#### H2 — Plan cache hit performance

**New file:** `bench/Voron.Benchmark/Corax/PlanCacheBenchmark.cs`

```csharp
// Cold: first call per query text — parses AST, emits IL, compiles delegate
[Benchmark]
public void FirstCall_ColdCache()
{
    var cache = new PlanCache();  // fresh cache each invocation
    var planParams = BuildParams(_indexSearcher);
    QueryPlanBuilder.BuildAndCompile(planParams, cache, long.MaxValue, out _, null, default);
}

// Warm: subsequent calls — cache hit, only re-resolve parameter values
[Benchmark]
public void SubsequentCall_WarmCache()
{
    // _warmCache is populated in [GlobalSetup]
    var planParams = BuildParams(_indexSearcher);
    QueryPlanBuilder.BuildAndCompile(planParams, _warmCache, long.MaxValue, out _, null, default);
}
```

**Expected result:** warm-cache call is 5–20× faster than cold. If the gap is smaller, the parameter re-resolution pass in `RefreshClauseValues` is the bottleneck.

#### H3 — AND-chain steady-state overhead

**File to modify:** `bench/Voron.Benchmark/Corax/AndOrBenchmark.cs`

Fix `ParserQuery` / `ParserQueryOnlyIteration` to call `QueryPlanBuilder.BuildAndCompile()`. Add a `[Baseline]` attribute to the new-pipeline version and keep the old `RuntimeQuery` (direct API) as the comparison point:

```csharp
[Benchmark(Baseline = true)]
public void RuntimeQuery_DirectApi()
{
    // existing code using indexSearcher.TermQuery + AndWith — still valid
}

[Benchmark]
public void PlanBuilderQuery()
{
    var planParams = new QueryPlanBuilder.PlanParameters { ... };
    var match = QueryPlanBuilder.BuildAndCompile(planParams, _warmCache, long.MaxValue, out _, null, default);
    Span<long> ids = _ids;
    while (match.Fill(ids) != 0) ;
}
```

**Dataset:** 10 000 docs (existing `GenerateData`). Query: `WHERE Type = 'Dog' AND Age = '15'`.
**Expected result:** `PlanBuilderQuery` within 10–20% of `RuntimeQuery_DirectApi` on warm cache. If it's slower, profile the `FillBitmapFromTermSource` dispatch path.

#### H4 — Large OR / IN at high result counts

**File to modify:** `bench/Voron.Benchmark/Corax/InBenchmark.cs`

Replace the broken `OrFirstParserQuery` etc. with `QueryPlanBuilder`-based versions. Add a large-scale variant:

```csharp
// Dataset: 100_000 docs; IN clause with 50 terms each matching ~2000 docs
[Benchmark]
public void LargeInQuery()
{
    var terms = string.Join(", ", Enumerable.Range(0, 50).Select(i => $"'families/{i}'"));
    var planParams = new QueryPlanBuilder.PlanParameters
    {
        Metadata = new QueryMetadata($"FROM Dogs WHERE Family IN ({terms})", null, 0),
        IndexSearcher = _indexSearcher, Allocator = _bsc, Token = default
    };
    var match = QueryPlanBuilder.BuildAndCompile(planParams, _warmCache, long.MaxValue, out _, null, default);
    Span<long> ids = stackalloc long[4096];
    while (match.Fill(ids) != 0) ;
}
```

**Expected result:** `LazyOrWithPostings` path avoids per-term `popcount`; result should be faster than running 50 individual OR operations assembled via the old pipeline.

#### H5 — Entry-scan threshold effectiveness

**New file:** `bench/Voron.Benchmark/Corax/EntryScanBenchmark.cs`

This benchmark should index documents across a range of cardinalities and measure when `CheckAndMaybeEntryScan` fires:

```csharp
// Query with two AND conditions; vary the selectivity of the first term
// so that after the first AND step, the bitmap has:
//   [Params(100, 1_000, 10_000, 100_000)] entries
// Hypothesis: at <32_000 entries, entry scan is faster; above it, bitmap AND is faster.
[Params(100, 1_000, 10_000, 100_000)]
public int FirstTermSelectivity { get; set; }

[Benchmark]
public void AndWithEntryScan()
{
    // Build query such that first term matches `FirstTermSelectivity` docs
    // Second term is a string-range predicate (requires entry-scan predicate)
}
```

Inspect `CompiledQueryMatch.Inspect()["EntryScanAt"]` to confirm the scan fired at the expected op index.

### 3.5 Methodology for before/after comparison

Since the old pipeline code is deleted on this branch, comparisons must be done against the `v7.2` tag:

```bash
# Baseline: checkout v7.2 tag into a separate worktree
git worktree add ../ravendb-v72-bench v7.2
cd ../ravendb-v72-bench/bench/Voron.Benchmark
dotnet run -c Release -- --filter "*AndOrBenchmark*" --exporters json
# → saves BenchmarkDotNet.Artifacts/results/*.json

# New pipeline: same benchmark on this branch
cd /work/ravendb/RavenDB-25281/bench/Voron.Benchmark
dotnet run -c Release -- --filter "*AndOrBenchmark*" --exporters json

# Compare
# BenchmarkDotNet --baseline flag or manual ratio from the JSON outputs
```

For the ORDER BY + LIMIT hypothesis (H1), the comparison is clearest as a **scaling plot**: run both branches with `TakeSize ∈ {10, 100, 1000, 10000, 100000}` and plot elapsed time vs TakeSize. The old pipeline should be flat (dominated by full materialisation); the new pipeline should grow sub-linearly.

### 3.6 What to look for in profiler output

If a benchmark is slower than expected, attach a profiler before investigating code:

```bash
# Linux: perf + flamegraph
dotnet-trace collect --process-id <pid> --providers Microsoft-DotNETCore-SampleProfiler
dotnet-trace convert --format speedscope trace.nettrace

# Windows: ETW via BenchmarkDotNet EtwProfiler
dotnet run -c Release -- --filter "*PlanBuilderQuery*" --profiler ETW
```

**Hot paths to watch in profiler output:**

| Symptom | Probable cause | Where to look |
|---|---|---|
| Warm cache call slow | `RefreshClauseValues` doing excess allocation | `QueryPlanBuilder.cs` ~line 635; check for `new List<>` inside the loop |
| `FillBitmapFromTermSource` slow | PostingList.Iterator decode overhead | `Primitives.cs:FillBitmapFromTermSource`; verify SIMD path taken for large containers |
| `SortUsingIndexFromBitmap` not stopping early | `_take` not threaded into the bitmap path | `SortingMatch.cs:SortUsingIndexFromBitmap`; verify the loop breaks on `results.Count >= maxResults` |
| `PrepareForReading` hot | Many zero-cardinality bitmap containers being freed | Too many lazy OR steps on sparse data; consider threshold in `LazyOrWithPostings` |
| `PlanCache.GetOrCompile` contention | `ConcurrentDictionary` lock under high parallelism | `PlanCache.cs`; the per-query `PerQueryPlans` lock is the inner contention point |

### 3.7 Benchmark checklist before submitting results

- [ ] All benchmarks run in **Release** mode (`-c Release`)
- [ ] `WarmupCount ≥ 3`, `IterationCount ≥ 10` in the `Config` inner class
- [ ] Machine is **idle** — no other heavy processes; disable turbo-boost if comparing across machines
- [ ] Results exported as JSON (`--exporters json`) and committed alongside the code
- [ ] `[Baseline]` attribute set on the old-API or v7.2 equivalent so BenchmarkDotNet prints ratios
- [ ] `CompiledQueryMatch.Inspect()` verified manually on at least one run to confirm `EntryScanAt` field appears when expected
- [ ] EXPLAIN pseudocode (`Explain` key in `Inspect()`) reviewed to confirm the emitted plan matches expectations for the query being benchmarked

---

*Document generated 2026-05-04. Branch head: `904db86cbe7`.*
