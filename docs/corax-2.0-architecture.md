# Corax 2.0: Bitmap Query Pipeline Architecture Guide

## Why We Did This

The original Corax query pipeline built queries as deeply nested generic struct trees at runtime. A query like `WHERE A = 1 AND B = 2 AND C = 3` produced:

```
BinaryMatch<BinaryMatch<TermMatch, TermMatch, AndMarker>, TermMatch, AndMarker>
```

This had several fundamental problems:

**Generic type explosion.** Each unique nesting produced a distinct generic instantiation that the JIT had to compile separately. Complex queries with many AND/OR clauses created deeply nested types that were expensive to JIT, and the type signatures leaked into the sorting layer, requiring `SortingMatch.Erasure` wrappers just to hide them.

**Streaming merge overhead.** The pipeline merged sorted `Span<long>` arrays at each tree level. Each `BinaryMatch.Fill` called inner `Fill`, then outer `AndWith`, propagating through the entire depth. For a 5-clause AND chain, the same entry IDs were touched 5 times through merge-join at each level. CPU cache behavior and branch prediction were poor because the access pattern was data-dependent.

**No plan reuse.** Every query execution rebuilt the entire generic tree from the RQL AST. The same query with different parameter values did the same parsing, the same optimization, the same generic instantiation, the same JIT warm-up. There was no way to cache the execution strategy.

**Memoization tax.** When the same sub-expression appeared multiple times (e.g., in OR chains), `MemoizationMatch` had to cache and replay results, adding memory allocations and copying. The old optimizer (`CoraxAndQueries`, `CoraxOrQueries`) attempted to flatten and reorder trees, but the fundamental shape was still recursive.

**Allocation pressure.** Each `BinaryMatch` captured `ByteStringContext`, function pointers, and both inner/outer matches by value. `MemoizationMatchProvider` allocated buffers. None of this was pooled.

The new pipeline replaces all of this with a three-phase architecture: **parse once, compile to IL, execute on bitmaps.**

---

## Architecture Overview

### The Three Phases

```
RQL Text
   |
   v
[Phase 1: Parse]  -- once per query text
   |  ParseTemplate() -> ClauseTemplate (cached)
   v
[Phase 2: Plan]   -- once per (query text, parameter types, cardinality ordering)
   |  PopulateClauseValues() -> typed arrays
   |  EstimateCardinality() -> operand sort
   |  EmitPlan() -> PlanOp[]
   |  QueryILEmitter.EmitDelegate() -> IL delegate (cached in PlanCache)
   v
[Phase 3: Execute] -- every query execution
   |  ResolveMatches() -> IQueryMatch[] (spatial, vector, boosted)
   |  ResolveTermSources() -> PostingSource[] (native posting lists)
   |  CompiledQueryMatch.Execute() -> RoaringBitmap result
   v
Bitmap -> Fill()/Contains()/Count -> Results
```

### Three Levels of Caching

| Layer | What's Cached | Key | When Invalidated |
|-------|--------------|-----|-----------------|
| **ClauseTemplate** | Structural query skeleton (fields, clause types, parameter bindings, boolean structure) | `queryText` | GC'd with IndexSearcher when index is replaced |
| **CompiledPlan** | IL delegate + EXPLAIN pseudocode + inspection template | `(queryText, operandOrdering, typeSignature, fullKinds)` | Same -- lives on PlanCache which lives on IndexSearcher |
| **CompiledQueryMatch** | Per-execution: resolved posting sources, typed params, bitmap pool | Not cached | Single query execution, then disposed |

The expensive operations (AST parsing, cardinality estimation, plan emission, IL compilation, JIT) happen at most once per unique query shape. The cheap operations (parameter population, posting list resolution, bitmap execution) happen per execution.

### File Organization

The planner is split into three partial files by **execution frequency**:

| File | Lifecycle | Contents |
|------|-----------|----------|
| `QueryPlanBuilder.cs` | Once per query text | AST parsing (`ParseTemplate`), plan emission (`EmitPlan`), cardinality estimation, dispatch classification |
| `QueryPlanBuilder.Resolution.cs` | Every execution | `BuildAndCompile` entry point, `PopulateClauseValues`, match/posting-source resolution, spatial/vector materialization |
| `QueryPlanBuilder.Inspection.cs` | On demand | Studio visualization tree, built from cached `InspectionTemplate` + runtime telemetry |

---

## Key Design Decisions

### 1. Template / Execution Split

Every clause has two parallel representations:

- **`ClauseInfo`** (immutable, cached on template): field name, clause type, parameter bindings (literal values or parameter names), boolean flags, sub-clause structure.
- **`ClauseExecution`** (per-execution, mutable): resolved `PackedParam` value, cardinality estimate, IN term count, boost factor, spatial/vector params.

The template captures *what* the query asks for. The execution captures *how* to answer it with current parameter values and index statistics. This split is what makes template caching possible -- the same `ClauseInfo` array serves all executions of the same query text.

### 2. PackedParam: Zero-Allocation Parameter Encoding

Each resolved parameter is a 32-bit struct:

```
Bits [31:30] = value type (Long=0, Double=1, String=2, None=3)
Bits [29:15] = first parameter index (0..32767)
Bits [14:0]  = second parameter index (for BETWEEN; 0x7FFF = none)
```

Three parallel typed arrays (`LongValues[]`, `DoubleValues[]`, `StringValues[]`) hold the actual values. `PackedParam` is just a (type, index) pair pointing into the right array. This eliminates the old pattern of `ToString()` on every value followed by `TryParse()` at execution time.

### 3. PlanOp: A Bitmap Virtual Machine

`EmitPlan()` translates the sorted clause list into a linear `PlanOp[]` array -- an instruction sequence for a bitmap virtual machine. The 17 opcodes are:

| Category | Opcodes | Purpose |
|----------|---------|---------|
| **Fill** | `FillFromPostings`, `DirectIterate` | Seed bitmap from a source |
| **Set ops** | `AndWithPostings`, `OrWithPostings`, `AndNotWithPostings`, `LazyOrWithPostings` | Combine a source with the bitmap |
| **Bitmap-to-bitmap** | `AndBitmaps`, `OrBitmaps`, `AndNotBitmaps`, `SwapBitmaps`, `ClearBitmap` | Pure in-memory bitmap operations |
| **Ranges** | `OrRange`, `AndRange` | IN/AllIn: IL-emitted loop over contiguous posting sources |
| **Control** | `CheckEmpty`, `CheckAndMaybeEntryScan`, `IterateInto`, `RepairAfterLazy` | Flow control and adaptive execution |

Each `PlanOp` also carries a **`MatchDispatch`** selecting the runtime primitive:

- **`PostingList`** -- Direct native posting list. Fastest path: decompress + merge, no iteration.
- **`TreeScan`** -- CompactTree walk (StartsWith, EndsWith, Regex, Exists, range). Iterates tree, decodes each posting list.
- **`QueryMatch`** -- General `IQueryMatch.Fill()` (spatial, vector, search, boosted). Slowest but most flexible.

### 4. IL Compilation

The `QueryILEmitter` translates `PlanOp[]` into a `DynamicMethod` that operates directly on `CompiledQueryMatch`:

```csharp
delegate void CompiledExecuteDelegate(CompiledQueryMatch ctx);
```

The emitted IL calls static methods on `QueryPrimitives` (e.g., `CtxFillFromPostingSource`, `CtxAndWithPostingSource`) which operate on `RoaringBitmap` instances via `ctx.Bitmaps[slot]`. The dispatch (PostingList vs TreeScan vs QueryMatch) is resolved at emit time, not at execution time -- no runtime polymorphism.

Key IL features:
- **Bounds-check elimination preamble**: Loads and discards the maximum index of each array so the JIT can hoist bounds checks.
- **OrRange/AndRange loops**: IN/AllIn clauses emit actual IL `for` loops instead of one op per term.
- **Entry scan fallback**: If `CheckAndMaybeEntryScan` triggers, the IL jumps to an inline section that reads individual entries and checks predicates via emitted comparisons (long/double/slice).
- **EXPLAIN pseudocode**: Built in the same pass as the IL, so it cannot drift from the actual execution.

### 5. Three-Bitmap Slot Strategy

`EmitPlan` uses up to 3 bitmap slots:

- **Slot 0** -- Main result. Accumulates the final answer.
- **Slot 1** -- Scratch. Used for AND-chain non-seed IN terms (OR all terms into slot 1, then AND with slot 0) and OR-group accumulation.
- **Slot 2** -- Save slot. Only needed when an OR chain contains multiple AND-groups. Pattern: save slot 0 to slot 2, build AND result fresh in slot 0, OR slot 2 back.

Bitmaps are rented from `ArrayPool<RoaringBitmap>` at execution start and returned in the finally block. Slot 0 is the instance's own `_bitmapData`; slots 1+ are freshly allocated.

### 6. SIMD Plan Cache

`PlanCache` uses a struct-of-arrays layout with SIMD-accelerated lookup:

- Three parallel arrays: `_orderings[32]`, `_typeSignatures[32]`, `_plans[32]`.
- **Vec256 path**: Broadcasts target ordering/typeSignature into `Vector256<int>`, compares 8 slots per iteration (4 iterations for 32 slots). `ExtractMostSignificantBits` + `TrailingZeroCount` walks hits.
- **Two-generation rotation**: When `Current.Count > MaxDistinctQueries/2`, rotate atomically. Previous generation is dropped on next rotation.
- **Chain depth**: Plans sharing the same (ordering, typeSignature) but differing in `FullKinds` (>16 scan predicates) form a linked list, capped at depth 8.

### 7. Adaptive Execution: Entry Scan Heuristic

When the candidate bitmap is small enough, it's cheaper to read individual entries and check predicates than to intersect large posting lists. The heuristic:

```
bitmapCount < 32,768 AND bitmapCount * 64 < postingListSize
```

The IL emitter generates inline comparisons for the entry scan path: `reader.CurrentLong [op] ctx.LongParams[idx]` for numeric fields, `reader.Current.Decoded() SequenceEqual ctx.SliceParams[idx]` for strings. No boxing, no virtual dispatch.

Entry scan is only eligible for simple predicates (Equals, NotEquals, ranges, Between, OrGroup of these). StartsWith, EndsWith, Regex, Search, Spatial, Vector, IN/AllIn, and Exists cannot participate.

### 8. Consumed-After-Use Bitmap Safety

`RoaringBitmap.OrWith`, `AndWith`, and `AndNotWith` are destructive to the right-hand argument (containers are stolen or mutated in-place). A `_consumed` flag (DEBUG-only) is set after each set operation. Any subsequent read on a consumed bitmap asserts immediately, catching double-consume bugs that would otherwise manifest as heap corruption.

---

## Notable Items for Reviewers

### The `allNegated` Check

```csharp
bool allNegated = clauses.Count > 0
    && (clauses[0].IsNegated || clauses[0].ClauseType == ClauseType.NotEquals);
```

This looks like it only checks the first clause, but it's correct because `EmitPlan` runs **after operand sorting**. Negated clauses sort to the end (highest cardinality). If clause[0] is negated after sorting, every subsequent clause must also be negated. The plan then seeds from `AllEntries` and ANDNOTs each clause.

### `IsOrChainNotEquals`

In an OR chain, `Name != 'a'` can't just OR the posting list for `Name = 'a'` (that contains entries WITH 'a'). The flag triggers `ResolveMatches` to pre-materialize `AllEntries ANDNOT TermQuery('a')` into a `BitmapMatch`, which then gets ORed normally.

### `Array.Resize` Pitfall in `AttachPostFilterPhases`

`Array.Resize(ref execs, ...)` creates a new array and assigns to the local variable, but `plan.Executions` must be explicitly updated afterward. The fix (`plan.Executions = execs`) was a real bug caught during this review.

### Timing Overhead

`EmitTimingStart`/`EmitTimingEnd` are emitted for every op unconditionally. The recording methods check `ctx.Timings != null` at runtime. When timings aren't requested, the cost is two `Stopwatch.GetTimestamp()` calls + two null checks per op. This is a conscious trade-off: one compiled delegate serves both timed and untimed executions.

### `SortingMatch` Integration

`SortingMatch` uses `IBitmapQueryMatch` to avoid full materialization. `SortUsingIndexFromBitmap` walks the CompactTree in sorted order, intersecting batches via `AndWith` (which uses `bitmap.Contains()` per entry since entries arrive in sort-field order, not entry-ID order). This stops early once `_take` results are collected.

---

## What We Can Do Now That We Couldn't Before

### Plan Caching

The same query executed with different parameter values reuses the cached IL delegate (if cardinality ordering hasn't changed). This eliminates repeated AST parsing, plan building, and JIT compilation. A query that runs 1000 times with different parameters parses the AST once, compiles IL at most a handful of times (one per distinct ordering), and executes the cached delegate for the rest.

### Adaptive Execution Per Query

The same query text can have multiple cached plans for different cardinality regimes. `WHERE Tag = $tag AND Status = $status` with a rare tag gets a plan that starts with the tag's small posting list. The same query with a common tag gets a different plan that starts with status. The cache key includes the operand ordering, so both plans coexist.

### Entry Scan Fallback

When the intermediate bitmap is small (< 32K entries) and the remaining predicate has a large posting list, the pipeline switches to reading individual entries and checking predicates inline. This was not possible in the old streaming pipeline where every clause had to be evaluated as a posting-list merge.

### Typed Parameters

Parameters are resolved to native types once and stored in typed arrays (`long[]`, `double[]`, `string[]`). The IL emits direct comparisons (`reader.CurrentLong >= ctx.LongParams[3]`) without boxing, parsing, or string conversion. The old pipeline resolved parameters to strings and re-parsed them at multiple points.

### OrRange / AndRange

IN clauses with N terms emit a single IL loop instead of N separate plan ops. The loop body calls `CtxOrFillFromPostingSource(ctx, j, bitmapSlot)` with an incrementing index. This is both more compact in IL and better for the JIT's loop optimizations.

### Inspection Without Re-Execution

The `InspectionTemplate` (stored on `CompiledPlan`) is a structural template for the Studio visualization tree. At query time, it's cloned and populated with per-execution timings and result counts. You don't need to re-execute the query to get the plan -- the cached template shows the structure, and `include timings()` adds runtime data.

---

## Integration Path for Future Features

### WHEN Clauses

WHEN clauses (conditional expressions in queries) would integrate at the `ClauseType` level. Add a `ClauseType.When` variant with sub-clauses for the condition and the body. During `EmitPlan`, a WHEN clause would emit:

1. `CheckAndMaybeEntryScan`-like conditional branch based on the WHEN condition
2. If true: emit the body clause ops
3. If false: skip or emit alternative

The entry scan path already supports `OrBranches` on `ScanPredicateInfo`, which is the same "conditional check" pattern. WHEN is a generalization.

### Constant Propagation

With the template/execution split, constant propagation becomes straightforward:

1. During `PopulateClauseValues`, detect clauses where the parameter resolves to a value that makes the predicate trivially true (e.g., `x >= 0` on a non-negative field) or trivially false (e.g., `IN ()` with no terms).
2. Mark the clause for elimination or replace with `AllEntries`/`Empty`.
3. Re-sort and re-emit the plan.

The existing empty-IN elimination in `EmitPlan` (AND chain returns empty, OR chain removes the clause) is already a form of this. Extending it to numeric ranges and constant comparisons follows the same pattern.

### Compound Field Optimization

The `CompoundField` index feature (multiple fields in a single CompactTree) is already detected by the query plan. Currently, the plan falls back to individual field queries even when a compound field exists. The path to compound field utilization:

1. During `EstimateCardinality`, detect that a group of clauses matches a compound field.
2. Replace the group with a single compound lookup that walks the CompactTree once.
3. The dispatch would be `MatchDispatch.TreeScan` with a compound-aware term provider.

### Streaming Execution (Partial Bitmap)

The `Limit` field on `CompiledQueryMatch` already supports early termination for unsorted queries. This could be extended to a full streaming mode where:

1. The bitmap is built incrementally (first N containers only).
2. Results are yielded as containers complete.
3. If the consumer asks for more, the next batch of containers is processed.

This requires changes to `RoaringBitmap` to support incremental construction, but the plan/execution split means the IL delegate doesn't need to change.

### Per-Field Type Specialization

The `TypeSignature` cache key already distinguishes `Age = 25` (long) from `Age = "25"` (string). This could be extended to generate type-specialized IL: when all parameters for a field are known to be long, the IL could emit long-specialized posting list lookups that skip the type-check dispatch.

---

## Current Limitations

### Cache Scope

The `PlanCache` lives on `IndexSearcher`, which is per-transaction. Compiled delegates are NOT reused across transactions. For high-throughput read workloads, this means the first query of each new transaction pays the full JIT cost. Moving the cache to the index-instance level (shared across transactions, invalidated on index rebuild) would amortize this cost.

### Entry Scan Coverage

Only simple predicates (Equals, NotEquals, numeric ranges, Between, and OrGroups of these) can participate in entry scan. The following clause types force the full bitmap path even when the intermediate result is small:

- StartsWith, EndsWith, Contains, Regex
- Search (full-text)
- Spatial, Vector
- IN, AllIn
- Exists
- AndGroup

### TreeScan Dispatch Not Wired

`MatchDispatch.TreeScan` is defined and the IL emitter has full support for it, but `GetDispatch()` in the plan builder never returns it. All multi-term queries (StartsWith, EndsWith, Regex, Exists, range) currently go through `MatchDispatch.QueryMatch` via `IQueryMatch.Fill()`. Wiring up the TreeScan dispatch would allow the IL to call `QueryPrimitives.CtxFillFromTreeScan` directly, avoiding the virtual dispatch overhead.

### Hard-Coded Heuristic Constants

The entry scan thresholds (`EntryScanCountThreshold = 32K`, `EntryScanCostMultiplier = 64`, `EntryScanBatchSize = 256`, `FillBufferSize = 4096`) are hard-coded private constants. They were tuned on NVMe workloads but are not configurable at runtime. Different storage backends (cloud, HDD, memory-mapped) may have different cost profiles.

### No Intersect Query Support

`Corax.IntersectQuery` throws `NotSupportedException`. This is unchanged from the old pipeline.

### No Explanation Support

`Corax.Explanations` throws `NotSupportedInCoraxException`. The EXPLAIN pseudocode on `CompiledPlan` is a partial substitute but doesn't match the Lucene explanation format.

### Spatial/Vector in OR Chains

Spatial and vector clauses are only extracted to post-filter phases in AND queries. In OR queries, they remain in the clause list and go through the general `IQueryMatch` path. This means `WHERE spatial.within(...) OR Name = 'x'` may be less efficient than the AND equivalent.

### Timing Always Emitted

The IL delegate always contains timing instructions (two `Stopwatch.GetTimestamp()` calls per op + null checks). A future optimization could emit two separate delegates -- one with timings, one without -- and select at execution time. The cache key would need an additional bit.

### `CompiledQueryMatch.AndWith` Linear Scan

The `AndWith` method on `CompiledQueryMatch` uses per-entry `bitmap.Contains()` rather than bitmap intersection. This is necessary because callers (SortingMatch) pass entry IDs in sort-field order, not entry-ID order. For sorted queries with large result sets and many sort iterations, this is O(N) per batch.

---

## Items Still To Do

1. **Wire up `MatchDispatch.TreeScan`**: The infrastructure exists (IL emitter, QueryPrimitives methods, PostingSource types) but `GetDispatch()` never returns `TreeScan`. Multi-term queries (StartsWith, EndsWith, Regex, Exists, ranges) would benefit from direct dispatch instead of going through `IQueryMatch`.

2. **Move PlanCache to index-instance level**: Currently per-transaction (per `IndexSearcher`). Moving it to the index instance would allow compiled delegates to be reused across transactions, eliminating repeated JIT costs for the same queries.

3. **Expand entry scan eligibility**: Add support for StartsWith and Exists predicates in the entry scan path. These can be evaluated by reading the entry's stored fields, similar to how Equals/Range work.

4. **Make heuristic constants configurable**: Expose `EntryScanCountThreshold` and `EntryScanCostMultiplier` as server configuration options, or auto-tune based on observed latency.

5. **Separate timed/untimed delegates**: Emit two versions of the IL delegate -- one with timing instrumentation, one without. Select at execution time based on `include timings()`. Eliminates the per-op `Stopwatch.GetTimestamp()` overhead for normal queries.

6. **Compound field utilization**: When a compound index field covers all predicates in an AND group, replace individual field lookups with a single compound tree scan.

7. **WHEN clause support**: Add `ClauseType.When` and corresponding `EmitPlan` / entry-scan logic for conditional query expressions.

8. **Constant propagation**: Detect and eliminate trivially-true/false clauses during `PopulateClauseValues` (e.g., `x >= 0` on non-negative fields, empty IN, contradictory ranges).

9. **Streaming/incremental bitmap**: For very large result sets without ORDER BY, build the bitmap incrementally and yield results as containers complete, rather than materializing the full bitmap.

10. **Custom OrderBy support**: Currently throws `NotSupportedInCoraxException`. Requires the sorting layer to support user-defined comparators.
