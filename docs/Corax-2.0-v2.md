# Corax 2.0 — Design Document

*Target: RavenDB 8.0. Corax becomes the default indexing engine.*

*Source ground truth: the v7.2 codebase and the `RavenDB-25284` branch
(on `ayende/ravendb`). All type/file references are from the code.*

---

## 1. Executive Summary

Corax 2.0 replaces the query execution model. Two changes ship together:

- **Roaring bitmap as accumulator.** Replace the `IQueryMatch` streaming
  `Fill(Span<long>)` protocol with roaring bitmaps. AND/OR/ANDNOT become
  SIMD container ops. Eliminates re-sorting, re-deduplication, and
  `MemoizationMatch`. Already prototyped on `RavenDB-25284`; 2-4× on
  high-selectivity queries at 10M docs.

- **Compiled query execution via DynamicMethod.** Replace the
  `BinaryMatch<,,>` / `delegate*` / erasure-file match tree with a
  per-query function emitted via `ILGenerator` (~50μs). The generated IL
  calls pre-compiled bitmap primitives directly. No Roslyn, no
  assemblies, no `AssemblyLoadContext`. DynamicMethod delegates are
  GC-collectible.

Together these delete: `IQueryMatch`, the seven `*.Erasure.cs` files,
`MemoizationMatch`, `MergeHelper`, `DeduplicationMatch`,
`RoaringBitmapMatch`, and the `delegate*` function-pointer dispatch.
The compiled path is the only execution model.

---

## 2. Why Corax 2.0

### 2.1 What we preserve

Corax 1.0's architectural wins — no-cold-start, no-merge-pause,
on-disk-final-form, per-index engine choice — are out of scope. **No
on-disk format changes.** Existing indexes work without rebuild. Corax
2.0 is compute-only.

### 2.2 What hurts

**Pain 1 — the streaming contract is leaky.** `IQueryMatch.Fill` may
return duplicates across calls and is not guaranteed sorted between
batches. This forces `MemoizationMatch` (full materialisation),
`MergeHelper` (re-merge sorted batches), and a 128 MB memory cap
(`MaxMemoizationSizeInBytes`) with degraded fallback.

The streaming contract does have one advantage: `LIMIT 25` can stop
after one batch without evaluating the full set. The bitmap design
preserves this via early-termination primitives (§7.4).

**Pain 2 — the type-erasure machinery.** `BinaryMatch<TInner, TOuter,
TMarker>` holds three `delegate*` function pointers — runtime dispatch
despite generics. Seven `*.Erasure.cs` files box generic structs for
mixed collections. `CoraxQueryBuilder` is a twelve-case `if` cascade.
Understanding a query plan requires reading half the codebase.

---

## 3. Goals & Non-Goals

**Goals.**

1. Bitmap-based query evaluation. Intermediate uniqueness, sort, and
   dedup are properties of the bitmap, not caller invariants.
2. Compiled per-query functions via DynamicMethod. Each query is a
   flat IL sequence calling bitmap primitives — no indirect dispatch.
3. Preserve on-disk format and indexing semantics. Corax 2.0 is
   compute-only.
4. Delete `IQueryMatch` and the match-tree code entirely. The compiled
   path is the only execution model. No fallback, no feature toggle.
   DynamicMethod generation at ~50μs eliminates cold-start concerns.

**Non-Goals.** Replacing Voron. Replacing analyzers/pipeline. Sharding-
aware planning. A new query language. Vector/spatial algorithm redesign.

---

## 4. Corax 1.0 Baseline

The reader is assumed familiar with Corax. Key structures the new design
builds on:

- **PostingList** — B+ tree of entry IDs with PFor-compressed leaf pages.
  Three cardinality tiers (Single/Small/Large). The query path uses
  `PostingList.Iterator`. Exact cardinality is available via
  `PostingListState.NumberOfEntries` (O(1) metadata read).
- **CompactTree** — per-field term storage with HOPE encoding. Maps
  `term → postingListReference`. Range scans walk terms in order.
- **Lookup<TKey>** — fixed-size B+ tree for `entryId → blobLocation`.
- **Container** — dense byte-packed storage for entry blobs.
- **SortUsingIndex / SortedIndexReader** — walks a sort-field index in
  order and intersects with a candidate set. Used for
  `WHERE x = a ORDER BY y` when candidates exceed 4096.
- **TermsReader** — `entryId → first-term` lookups per field.
- **EntryTermsReader** — reads all field values from an entry blob.
  Used by `MultiUnaryMatch` for per-entry predicate evaluation.
- **StreamingOptimization** — detects when ORDER BY field aligns with a
  WHERE field and issues an ordered range scan instead of sort.

The match-tree execution model (`IQueryMatch`, `BinaryMatch<,,>`,
erasure files, `delegate*` function pointers) is what Corax 2.0
replaces. It is not described here — see §2.2 for why it hurts.

---

## 5. Architecture Overview

```
              ┌────────────────────────┐
              │  Compiled Query        │  DynamicMethod emits IL per query
              │  Execution             │  shape (~50μs). CLR JITs it.
              │  (DynamicMethod → JIT) │  Generated code calls bitmap
              └────────┬───────────────┘  primitives directly.
                       │ emits calls into
                       ▼
              ┌────────────────────────┐
              │  Roaring Bitmap        │  The intermediate representation.
              │  Accumulator           │  All query ops (AND/OR/ANDNOT)
              │  (compute only)        │  are SIMD container ops on bitmaps.
              └────────┬───────────────┘
                       │ reads from
                       ▼
   ┌──────────────────────────────────────────────────┐
   │       Corax 1.0 storage + sort substrate         │
   │  PostingList, CompactTree, Container, Lookup,    │
   │  TermsReader, SortUsingIndex, EntryTermsReader   │
   └──────────────────────────────────────────────────┘
```

Three execution paths exist in the generated code:

1. **Bitmap operations** — for set-level predicates (term, range, IN,
   AND, OR, ANDNOT). The common path for most queries.
2. **Entry scan (unary matching)** — when the bitmap shrinks below a
   threshold (~32K), remaining predicates are evaluated per-entry via
   `EntryTermsReader` instead of more posting list lookups.
3. **Ordered range scan** — when WHERE field = ORDER BY field with
   LIMIT, walk the CompactTree in sort order directly. No bitmap.

---

## 6. Roaring Bitmap

### 6.1 Implementation

`public ref struct RoaringBitmap : IDisposable` . Allocated through `ByteStringContext`, zero
managed-heap pressure. Compiler-enforced stack lifetime.

Entry IDs are `long` split via `key = value >> 16` into container key + 16-bit
offset. Container lookup is O(1) via flat index (`NativeList<int>`).

Four container types: **Range** (contiguous 0..count, no allocation),
**ArrayUnsorted** (append-only, sorted lazily), **Array** (sorted
`ushort[]`, ≤4096 values), **Bitmap** (fixed 8 KB, >4096 values).

Struct-of-arrays layout (`_entries`, `_types`, `_index`) for
cache-friendly scanning. Free-list tombstone management. SIMD set
operations via `IBitmapOp` with `AndOp`/`OrOp`/`AndNotOp`, dispatched
per vector width (256/128/scalar). Destructive `OrWith` steals container
ownership (zero-copy). SIMD-accelerated `Contains` via `SimdLinearContains`
(≤64 elements) and `SimdQuadContains` (>64, Lemire's quaternary search).

### 6.2 PostingList → bitmap operations

Four operations on `PostingList.Iterator`, working natively in bitmap
space:

- **`FillFromPostings`** — walk leaf pages, decode PFor blocks, set bits.
  Sequential output uses the Range container path.
- **`AndWithPostings`** — galloping page-scan. Combines skip-ahead
  (only visit posting list pages that overlap with set bits in the
  accumulator) with bulk SIMD AND (decode entire page into a temp
  bitmap, AND against accumulator containers):

  1. Find first set bit in accumulator → entry ID X.
  2. Seek posting list B+ tree to the page containing X.
  3. Decode the *entire* page into a reusable temp bitmap.
  4. AND the temp bitmap's containers with the accumulator's containers
     (only overlapping container keys — O(1) per key via flat index).
  5. Find next set bit in accumulator *after* the page's last entry ID.
  6. Gallop forward in B+ tree, repeat from step 2.

  Pages with no accumulator bits are skipped entirely (galloping), and 
  matching pages are processed via SIMD container AND (bulk). The temp 
  bitmap is allocated once per query, `Clear()`'d between pages. PFor 
  decoding is sequential — you can't selectively decode entries from a 
  compressed block, so decoding the  whole page into a temp bitmap costs 
  the same as decoding only the overlapping entries. Cost proportional 
  to *pages that intersect the accumulator*, not the total posting list size.
- **`OrWithPostings`** — walk all leaf pages, set bits. Idempotent.
- **`AndNotWithPostings`** — same galloping shape, calls ANDNOT the temp bitmap
  instead of AND, that is all.

**Page decoding strategy.** A posting list leaf page may contain entry
IDs spanning multiple 16-bit container ranges. When the page range fits
within a single 64K container, decode into a scratch bitmap and AND/OR
with the accumulator container directly. When it spans multiple
containers, decode via `Fill(Span<long>)` into a temporary buffer and
add entries individually. A reusable temp bitmap (allocated once per
query from `ByteStringContext`, cleared between pages) avoids repeated
allocation.

**Lazy OR.** For `IN` clauses with many terms, `LazyOrWithPostings`
skips per-operation cardinality recount (a full `popcount` over 8 KB
per Bitmap container). Container promotion (Array→Bitmap at 4096)
still happens — you can't avoid it structurally. `RepairAfterLazy()`
runs one popcount pass at the end.

### 6.3 Scoring

Scoring splits into two cases with different storage requirements:

**Full-text / BM25 scoring.** BM25 scores are computed from term
frequencies stored in the posting list alongside entry IDs. The key
insight: the boosted posting lists (from `search()` / `boost()` clauses)
are typically much smaller than the full result set. In
`WHERE Category = $c AND boost(search(Title, $t), 10)`, the Category
bitmap may have 1M entries but the Title posting list for term `$t` has
10K entries. We only need frequencies for those 10K.

The flow:

1. Bitmap ops build the candidate set from all predicates (no scoring).
2. For each boosted term, walk its posting list using the galloping
   page-scan pattern (§6.2) — but instead of AND, *collect frequencies*.
   Only visit pages that intersect the bitmap (galloping), and only
   store frequencies for entry IDs present in the bitmap. This produces
   a compact `(entryId, frequency)` array bounded by the *intersection*
   of the boosted posting list with the bitmap — not the bitmap size.
3. `SortByScore` uses a `PriorityQueue<long, float>` of capacity =
   LIMIT. Iterates the bitmap, looks up frequency for each entry (O(1)
   from the collected array), computes BM25, pushes into the queue.
   Entries not in any boosted posting list get the default score
   (`Bm25Relevance.InitialScoreValue`).

NOTE: This is a change from Corax 1.0 - where we did the scoring along side
the call to `Fill()`. Need to make sure if this is a problem performance-wise.

Memory for scoring: O(boosted_intersection + LIMIT), not O(result_set).
For the common case (10M result bitmap, 10K boosted term matches), the
frequency array is ~20 KB (10K × 2 bytes). The priority queue holds
LIMIT entries.

When multiple boosted terms are OR'd (`boost(search(Title, $t), 10) OR
search(Body, $t)`), each term produces its own frequency array. The
scorer combines them per entry: for each entry in the bitmap, look up
its frequency in each boosted term's array, compute BM25 for each,
sum the scores.

**Vector search scoring — bounded by K, not result set.** Vector
similarity search with `k=20` produces exactly 20 `(entryId, distance)`
pairs — the HNSW retriever returns top-K, not the full candidate set.
The `VectorRank` primitive filters the bitmap down to those K entries
and stores K distances (80 bytes for K=20). No scaling problem.

**Spatial distances** — same pattern as vector: the spatial filter
produces distances for matching entries. For `ORDER BY distance()`,
distances are stored only for entries that pass the spatial predicate,
bounded by the bitmap cardinality after filtering.

---

## 7. Execution Model

### 7.1 Why DynamicMethod

The generated query code is 10-30 static method calls into pre-compiled
primitives. The IL is trivial: `ldarg`, `call`, `stloc`, `ret`, with
occasional `if` checks for dynamic unary promotion.

`DynamicMethod` via `ILGenerator` produces this in **~50 microseconds**.
This eliminates:
- Roslyn (70-1000ms compile time, 10-40MB memory per compilation)
- Tiered execution (interpreter → async compile → atomic swap)
- `AssemblyLoadContext` and assembly lifecycle management
- Persistent DLL cache and compile-storm-at-restart concerns

DynamicMethod delegates are GC-collectible. When an index is replaced,
the old plan cache becomes unreferenced and the delegates are collected.
No explicit unloading needed.

**EXPLAIN** generates a C# source string for human reading. This source
is never compiled — it exists for diagnostics and debugging only. For
deep debugging, a developer can copy-paste the EXPLAIN source into a
standalone `.cs` file and step through it.

### 7.2 Primitive vocabulary

One canonical list. These are static methods in `Corax.dll`, SIMD-tuned,
individually benchmarkable. The generated DynamicMethod IL calls them
directly.

```csharp
namespace Corax.Querying.Primitives;

public static class Primitives
{
    // --- Posting list → bitmap ---
    void FillFromPostings(PostingList postings, ref RoaringBitmap bitmap);
    void AndWithPostings(PostingList postings, ref RoaringBitmap bitmap);
    void OrWithPostings(PostingList postings, ref RoaringBitmap bitmap);
    void AndNotWithPostings(PostingList postings, ref RoaringBitmap bitmap);
    void LazyOrWithPostings(PostingList postings, ref RoaringBitmap bitmap);

    // --- Range scan → bitmap ---
    // Handles BETWEEN, >, <, >=, <= via from/to with Min/Max sentinels.
    void FillFromRange(CompactTree tree, Slice from, Slice to,
                       ref RoaringBitmap bitmap);

    // --- Bitmap → output ---
    int IterateInto(ref RoaringBitmap bitmap, Span<long> output,
                    ref int skip);

    // --- Ordered range scan (sort-skip path, no bitmap) ---
    int OrderedRangeScan(CompactTree tree, Slice from, Slice to,
                         Span<long> output, int limit, ref int skip);

    // --- Entry scan (unary matching, no sort) ---
    // Iterates bitmap, reads entry data, evaluates predicates, writes
    // matching IDs to output. Fused loop with early exit.
    int ScanAndFilter(ref RoaringBitmap bitmap,
                      IndexSearcher searcher,
                      MultiUnaryItem[] predicates,
                      Span<long> output, int limit, ref int skip);

    // Same as ScanAndFilter but modifies the bitmap in place — clears
    // bits for entries that fail the predicates. Used when the filtered
    // bitmap must flow into a subsequent primitive (e.g. VectorRank)
    // that requires a bitmap input, not an output span.
    void ScanAndFilterInPlace(ref RoaringBitmap bitmap,
                              IndexSearcher searcher,
                              MultiUnaryItem[] predicates);

    // --- Sort + optional filter ---
    // One primitive for all sorted output. Internally decides strategy
    // via ShouldHeapSortDirectly:
    //   Small bitmap → iterate bitmap, eval predicates, heap-sort
    //   Large bitmap → walk sort index in [rangeStart..rangeEnd],
    //                  check bitmap.Contains(), eval predicates per hit
    // predicates may be empty (all filtering already done via bitmap).
    // rangeStart/rangeEnd: use Slice.BeforeAllKeys/AfterAllKeys for unbounded.
    int SortWithFilter(ref RoaringBitmap bitmap,
                       IndexSearcher searcher,
                       MultiUnaryItem[] predicates,
                       OrderMetadata orderMeta,
                       Slice rangeStart, Slice rangeEnd,
                       Span<long> output, int limit);

    // --- Scoring ---
    // Vector search over bitmap candidates. Filters bitmap IN PLACE to
    // top-K entries, writes (entryId, distance) pairs to DistanceLookup.
    // Internally selects strategy based on bitmap cardinality:
    //   Small bitmap → brute-force exact: read each entry's stored vector
    //     from the blob, compute distance directly. N dot products.
    //   Large bitmap → HNSW graph traversal filtered by bitmap. Approximate
    //     but sublinear.
    // DistanceLookup is a small sorted array — O(K) memory. The projection
    // layer reads @distance per result via DistanceLookup.Get(entryId).
    void VectorRank(VectorIndex index, IndexSearcher searcher,
                    ReadOnlySpan<float> query,
                    ref RoaringBitmap candidates,
                    ref DistanceLookup distances, int k);

    // Spatial: filters bitmap, stores distances for matching entries.
    void SpatialFilter(SpatialIndex index, IShape shape,
                       ref RoaringBitmap candidates);

    // Sort bitmap entries by distance (from VectorRank or SpatialFilter).
    // Reads distances from DistanceLookup, outputs entry IDs in
    // distance order. K is small — trivial heap sort.
    int SortByDistance(ref RoaringBitmap bitmap,
                       ref DistanceLookup distances,
                       Span<long> output, int limit);

    // BM25 sort: collect frequencies from boosted posting lists
    // (galloping scan, only entries in bitmap), then iterate bitmap
    // with PriorityQueue<long, float>(capacity = limit).
    // Memory: O(boosted_intersection + limit).
    int SortByScore(ref RoaringBitmap bitmap, IndexSearcher searcher,
                    Span<BoostingTermInfo> terms, Span<long> output, int limit);
}
```

### 7.3 Dynamic unary promotion (the goto pattern)

The planner orders AND operands by estimated cardinality (smallest
first). But cardinality estimates are imprecise — the actual bitmap size
after each AND step may be much smaller than estimated. Rather than
committing to a fixed split between bitmap ops and entry scan at plan
time, the generated code checks at runtime whether switching to entry
scan would be cheaper.

**The decision isn't just "is the bitmap small" — it's "is reading
bitmap-count entry blobs cheaper than AND-ing with the next posting
list?"** A 100-entry bitmap AND'd with a 3-entry posting list should
use the posting list (trivial). A 100-entry bitmap AND'd with a
10M-entry posting list should scan entries (reading 100 blobs beats
seeking through 10M entries).

The check is an inlined method. The next posting list is already
resolved (we need it for the AND call), so we read its current
`NumberOfEntries` — live data, not a plan-time constant:

```csharp
[MethodImpl(MethodImplOptions.AggressiveInlining)]
static bool ShouldSwitchToEntryScan(ref RoaringBitmap bitmap, PostingList postingList)
{
    // Entry scan: read bitmap.Count blobs, compare fields. ~1-2μs per blob.
    // Bitmap AND: galloping page-scan, cost ~ pages that intersect bitmap.
    // Heuristic: one posting list page-seek + decode ≈ scanning ~64 blobs.
    // Switch when bitmap is small enough AND the posting list is large
    // relative to the bitmap.
    var bitmapCount = bitmap.Count;
    return bitmapCount < 32_000
        && bitmapCount * 64 < postingList.State.NumberOfEntries;
}
```

| bitmap | posting list | decision | why |
|--------|-------------|----------|-----|
| 100 | 10M | scan entries | 100 blobs vs galloping through 10M |
| 100 | 3 | bitmap AND | 3-entry posting list is trivial |
| 10K | 10M | scan entries | 10K blobs vs galloping through 10M |
| 10K | 50K | bitmap AND | posting list is comparable size |
| 50K | anything | bitmap AND | 50K blobs is expensive to scan |

The `64` multiplier and `32K` cap are empirical — benchmark to
calibrate. The check itself is one field read (`bitmap.Count`) + one
multiply + two comparisons. Fully inlineable, no function call overhead.

Same pattern for sort strategy — when the bitmap is small, heap-sorting
it directly from entry blobs is cheaper than walking the sort-field
index:

```csharp
[MethodImpl(MethodImplOptions.AggressiveInlining)]
static bool ShouldHeapSortDirectly(ref RoaringBitmap bitmap, OrderMetadata orderMeta)
{
    // Heap-sort: read bitmap.Count blobs, extract sort field, push into
    // PriorityQueue. Cost ~ bitmap.Count * (blob read + comparison).
    // Index walk: scan sort-field index entries, check bitmap.Contains()
    // per entry. Cost ~ sort-field entries checked until LIMIT satisfied.
    // When bitmap is small relative to the sort-field index, heap-sort wins.
    var bitmapCount = bitmap.Count;
    return bitmapCount < 32_000
        && bitmapCount * 64 < orderMeta.FieldTotalEntries;
}
```

```csharp
static int Execute(ref QueryContext ctx, Span<long> output, ref int skip)
{
    using var bitmap = new RoaringBitmap(ctx.Allocator);

    Primitives.FillFromPostings(ctx.GetPostings(Field_Tag, ctx.P0), ref bitmap);

    var categoryPostings = ctx.GetPostings(Field_Category, ctx.P1);
    if (ShouldSwitchToEntryScan(ref bitmap, categoryPostings))
        goto EntryScan_AfterTag;

    Primitives.AndWithPostings(categoryPostings, ref bitmap);

    var statusPostings = ctx.GetPostings(Field_Status, ctx.P2);
    if (ShouldSwitchToEntryScan(ref bitmap, statusPostings))
        goto EntryScan_AfterCategory;

    Primitives.AndWithPostings(statusPostings, ref bitmap);
    return Primitives.IterateInto(ref bitmap, output, ref skip);

EntryScan_AfterTag:
    // Category + Status still need checking
    return Primitives.ScanAndFilter(ref bitmap, ctx.Searcher,
        unaryItems_CategoryAndStatus, output, ctx.Limit, ref skip);

EntryScan_AfterCategory:
    // Only Status still needs checking
    return Primitives.ScanAndFilter(ref bitmap, ctx.Searcher,
        unaryItems_StatusOnly, output, ctx.Limit, ref skip);
}
```

The posting list reference is read once and used for both the threshold
check and the AND. `NumberOfEntries` is a field in `PostingListState` —
O(1) read of the *current* index state, not a plan-time constant.
No stale cardinality estimates, no wasted work.

Each goto label is a distinct jump target with a statically-known
predicate set — the IL emitter generates one label per AND step, each
with the correct `MultiUnaryItem[]` for exactly the remaining
predicates. For repeated / similar queries, we can lean on the actual
CPU branch predictor here :-).

EXPLAIN records which path was taken (which goto, if any) so the query
plan UI can show the actual execution strategy, not just the planned one.

This pattern adapts at runtime to the actual data distribution. A query
plan compiled for "Tag is usually rare" will dynamically switch to entry
scan even when an unusually popular tag is passed — no re-planning
needed.

### 7.4 LIMIT without ORDER BY

`WHERE Foo = $x LIMIT 5` with no ORDER BY must not materialise the
entire posting list into a bitmap. The limit is checked at natural
processing boundaries that we're already hitting — no extra cost.

**Single operand + LIMIT** (`WHERE Foo = $x LIMIT 5`):
`FillFromPostings` checks the bitmap count after each posting list leaf
page. Pages are the natural I/O boundary — finish decoding the current
page, check `bitmap.Count >= limit`, stop. The first page typically
has ~1K entries, so `LIMIT 5` stops after one page. No 9M-entry bitmap
ever gets built for `WHERE Public = true LIMIT 5`.

**AND chain + LIMIT** (`WHERE Foo = $x AND Bar = $y LIMIT 5`):
Fill the smallest operand fully (it's small — that's why it's first).
Then during `AndWithPostings`, check at each container boundary whether
the *running count of matched entries so far* has reached the limit.
The AND processes containers left-to-right; after each container AND,
we know how many entries survived in that container. Once the cumulative
surviving count reaches LIMIT, stop — don't process remaining
containers.

```csharp
// WHERE Foo = $x AND Bar = $y LIMIT 5
Primitives.FillFromPostings(fooPostings, ref bitmap);
// bitmap has e.g. 10K entries (Foo is the smallest operand)

// AndWithPostings processes containers left to right.
// After each container AND, checks cumulative surviving count.
// Stops as soon as 5 entries have survived across all containers.
Primitives.AndWithPostings(barPostings, ref bitmap, limit: 5);

return Primitives.IterateInto(ref bitmap, output, ref skip);
```

The limit check is one comparison per container — negligible cost.
For most queries, the first few containers of the AND produce enough
results and the rest of the bitmap is never touched.

---

## 8. Query Planner

### 8.1 End-to-end flow

```
RQL text
  → parse → Query AST (QueryMetadata, WhereExpression, OrderBy)
  → QueryPlanBuilder.BuildPlan(planParameters)
    [1] Constant-fold: eliminate boolean literals (true OR X → true, etc.)
    [2] Normalise: flatten AND/OR trees, fuse ranges
    [3] Estimate: read cardinalities (O(1) per term from PostingListState)
    [4] Optimise: reorder operands, detect sort-skip, batch IN clauses
    [5] Lookup plan cache (per index instance)
        → hit: return cached DynamicMethod delegate
        → miss: emit via QueryILEmitter, cache
  → invoke delegate(QueryContext, output, skip)
    → goto checks between AND steps handle unary promotion at runtime (§7.3)
    → LIMIT checks at page/container boundaries handle early exit (§7.4)
  → load documents, yield to client
```

Planning cost is bounded: steps 1-3 are O(number of operands), each
doing at most one O(1) metadata read. The posting list references needed
for cardinality estimation are the same ones the query will use for
execution — no wasted work.

### 8.2 Cardinality estimation

- **Single term**: `PostingListState.NumberOfEntries` — exact, O(1).
- **OR of N terms**: sum of individual cardinalities, capped at index size.
- **AND of N terms**: min of individual cardinalities (upper bound).
- **BETWEEN / range / inequality**: three-tier estimation. Look up
  `from` and `to` in the CompactTree (we do this anyway for execution).
  Note their leaf pages and parent branch pages:

  1. **Same leaf page**: scan terms between `from` and `to` on that
     page, sum `NumberOfEntries`. Exact count, zero extra I/O.
  2. **Different leaf pages, same parent**: the range spans sibling
     leaves under one branch page and are siblings. Scan from `from`'s leaf through
     `to`'s leaf — bounded by max of two pages. Exact count, small bounded I/O.
  3. **Different parents / not next to one another**: the range is wide. Fall back to
     `GetTermAmountInField(field)` — total entry count for the field.
     O(1). Wide ranges are large, so the upper bound is directionally
     correct for operand ordering.

  This gives exact cardinality for narrow ranges (the common case for
  ranges, numeric filters) without ever walking too many terms. For high cardinality cases,
  such as unique date times, we use the full count of the terms in the field instead. The goto
  pattern compensates at runtime if the tier-3 upper bound produces
  suboptimal ordering.
- **IN(t1, ..., tN)**: sum of individual term cardinalities (same as OR).
- **Cardinality = 1**: the posting list is a single-entry inline value
  in the CompactTree. We already know the entry ID — no bitmap needed.

### 8.3 Optimisation passes

**Pass 1 — Constant folding.** Eliminate boolean literals and simplify
dead branches before any other work:
- `true OR X` → `true`
- `false OR X` → `X`
- `true AND X` → `X`
- `false AND X` → `false`
- `NOT true` → `false`, `NOT false` → `true`

Applied recursively bottom-up. If the entire WHERE clause folds to
`true`, the query becomes an all-entries query (no bitmap, just sort
or iterate). If it folds to `false`, return empty result immediately.

For subclauses: `(true OR X) AND Y` → `true AND Y` → `Y`. The
constant propagates through before any cardinality estimation happens,
so the planner never reads posting lists for eliminated operands.

**Pass 2 — Flatten.** Nested binary trees → flat AND/OR lists.
`(A AND (B AND C))` → `AND([A, B, C])`.

**Pass 3 — Range fusion.** Detect comparisons on the same field
(regardless of position in the AND list) and fuse into BETWEEN.
`Age > 18 AND Status = 'active' AND Age < 65` →
`Age BETWEEN 18..65 AND Status = 'active'`.

**Pass 4 — Operand reordering.** Sort AND operands by ascending
estimated cardinality. The bitmap is smallest after the first Fill,
so subsequent AndWith calls do proportionally less work.

**Pass 5 — Sort-skip detection.** If WHERE field = ORDER BY field and
LIMIT is present, emit `OrderedRangeScan` instead of bitmap + sort.

**Pass 6 — IN-clause batching.** `field IN (t1, ..., tN)` →
`LazyOrWithPostings` loop + `RepairAfterLazy()`.

**Pass 7 — Emit.** Generate DynamicMethod IL. The emitter inserts
goto checks between AND steps (§7.3) and LIMIT checks at processing
boundaries (§7.4) — these are runtime mechanisms, not planner
decisions. Generate C# source for EXPLAIN.

### 8.4 Plan cache

The plan cache lives on the index instance (one per `IndexSearcher`).
When the index is replaced (side-by-side), the new instance gets a
fresh cache; the old cache is GC'd with the old instance.

```
Per IndexSearcher:
  ConcurrentDictionary<string queryText, CompiledPlan[]>
```

The key is the RQL query string as-is. The value is a small array of
compiled plans — one per distinct operand ordering seen for this query.

Each `CompiledPlan` carries an `int Ordering` field: the operand sort
order packed at 3 bits per position (values 0-7), up to 10 operands
in 30 bits. E.g. [2, 0, 1] → `(2 << 0) | (0 << 3) | (1 << 6)` = `0x42`.

Lookup:
```csharp
if (_cache.TryGetValue(queryText, out var plans))
{
    for (int i = 0; i < plans.Length; i++)
        if (plans[i].Ordering == ordering)
            return plans[i];
    // no exact match — find closest or generate new
}
```

The array is tiny (1-3 entries typical, 32 max). Linear scan is faster
than a hash lookup at this size. On miss, generate a new DynamicMethod
(~50μs) and add it to the array via `ConcurrentDictionary.AddOrUpdate`.

**Cap: 32 plans per query text.** Beyond 32, pick the plan with the
closest ordering. The goto pattern (§7.3) compensates for any
remaining suboptimality.

In practice, most query shapes see 1-3 orderings (field cardinalities
are stable). 50 query shapes × 2-3 orderings = ~100-150 plans total.
Negligible memory.

---

## 9. API Surface

Key new types (signatures only — implementation detail is out of scope):

```csharp
// The planner — replaces CoraxQueryBuilder for query execution.
class QueryPlanBuilder
{
    CompiledPlan Build(PlanParameters parameters);
}

// A compiled plan — DynamicMethod delegate + EXPLAIN source.
class CompiledPlan
{
    delegate int ExecuteDelegate(ref QueryContext ctx,
        Span<long> output, ref int skip);
    ExecuteDelegate Execute;
    string ExplainSource;
}

// Runtime state bag passed to the compiled function.
ref struct QueryContext
{
    IndexSearcher Searcher;
    ByteStringContext Allocator;
    OrderMetadata OrderMeta;
    int Limit;
    BlittableJsonReaderObject Parameters;  // bound params, as-is
    // Field IDs are resolved at plan time and baked into the IL
    // as constant integers — no string lookups at execution time.
}
```

Fields in generated IL are referenced by constant integer IDs (resolved
at plan time from the index schema), not by string names. Multiple IN
clauses are handled by reading term lists from the `Parameters`
blittable at execution time — no separate `PList` arrays needed.

**Removed types**: `IQueryMatch`, all `*.Erasure.cs`, `BinaryMatch<,,>`,
`MemoizationMatch`, `MergeHelper`, `DeduplicationMatch`,
`RoaringBitmapMatch`, `CoraxQueryBuilder` (replaced by
`QueryPlanBuilder`).

---

## 10. Examples

### 10.1 Simple term with no ORDER BY

```sql
from Users where Status = $p0 limit 10
```

```csharp
static int Execute(ref QueryContext ctx, Span<long> output, ref int skip)
{
    // Single operand, no ORDER BY — no bitmap needed.
    // Iterate the posting list directly.
    var postings = ctx.GetPostings(Field_Status, ctx.P0);
    return postings.Iterator.Fill(output, ref skip);
}
```

Single operand + no ORDER BY always uses direct posting list iteration.
The bitmap would be pure overhead — decode posting list pages into
containers, then iterate containers to extract the same entry IDs.
Two passes over the same data for no benefit. The posting list iterator
already produces sorted, deduplicated entry IDs. LIMIT is handled by
the output span size; OFFSET (skip/paging) is the same cost either
way.

### 10.2 AND chain with dynamic unary promotion

```sql
from Products where Tag = $p0 and Category = $p1 and InStock = $p2
  and Rating >= $p3 limit 50
```

Cardinalities: Tag ~1%, Category ~10%, InStock ~80%, Rating≥4 ~40%.

```csharp
static int Execute(ref QueryContext ctx, Span<long> output, ref int skip)
{
    using var bitmap = new RoaringBitmap(ctx.Allocator);

    Primitives.FillFromPostings(ctx.GetPostings(Field_Tag, ctx.P0), ref bitmap);

    var categoryPostings = ctx.GetPostings(Field_Category, ctx.P1);
    if (ShouldSwitchToEntryScan(ref bitmap, categoryPostings))
        goto EntryScan_AfterTag;

    Primitives.AndWithPostings(categoryPostings, ref bitmap);

    var instockPostings = ctx.GetPostings(Field_InStock, ctx.P2);
    if (ShouldSwitchToEntryScan(ref bitmap, instockPostings))
        goto EntryScan_AfterCategory;

    Primitives.AndWithPostings(instockPostings, ref bitmap);

    var ratingPostings = ctx.GetPostings(Field_Rating_Gte, ctx.P3);
    if (ShouldSwitchToEntryScan(ref bitmap, ratingPostings))
        goto EntryScan_AfterInStock;

    Primitives.AndWithPostings(ratingPostings, ref bitmap);
    return Primitives.IterateInto(ref bitmap, output, ref skip);

EntryScan_AfterTag:
    // Category, InStock, Rating evaluated per-entry
    return Primitives.ScanAndFilter(ref bitmap, ctx.Searcher,
        unaryItems_CategoryInStockRating, output, ctx.Limit, ref skip);

EntryScan_AfterCategory:
    // InStock, Rating evaluated per-entry
    return Primitives.ScanAndFilter(ref bitmap, ctx.Searcher,
        unaryItems_InStockRating, output, ctx.Limit, ref skip);

EntryScan_AfterInStock:
    // Only Rating evaluated per-entry
    return Primitives.ScanAndFilter(ref bitmap, ctx.Searcher,
        unaryItems_RatingOnly, output, ctx.Limit, ref skip);
}
```

If Tag = 'rare-tag' (100 entries), the first check fires — bitmap is
tiny relative to Category's posting list. Category, InStock, and Rating
are all evaluated per-entry. If Tag = 'popular-tag' (500K entries),
bitmap ops continue through Category, and the check after Category may
fire for the remaining predicates.

### 10.3 Range fusion + ORDER BY on different field

```sql
from Products where Price > 10 and Price < 100 and InStock = true
order by Name limit 20
```

Pass 2 fuses `Price > 10 AND Price < 100` → `Price BETWEEN 10..100`.
Cardinality estimated via the three-tier method (§8.2).

```csharp
static int Execute(ref QueryContext ctx, Span<long> output, ref int skip)
{
    using var bitmap = new RoaringBitmap(ctx.Allocator);
    Primitives.FillFromRange(ctx.GetCompactTree(Field_Price),
        ctx.P0, ctx.P1, ref bitmap);

    var instockPostings = ctx.GetPostings(Field_InStock, ctx.TrueSlice);
    if (ShouldSwitchToEntryScan(ref bitmap, instockPostings))
        goto EntryScan_AfterPrice;

    Primitives.AndWithPostings(instockPostings, ref bitmap);

    // All predicates applied — no remaining unary filters
    return Primitives.SortWithFilter(ref bitmap, ctx.Searcher,
        MultiUnaryItem.Empty, ctx.OrderMeta,
        Slice.BeforeAllKeys, Slice.AfterAllKeys, output, 20);

EntryScan_AfterPrice:
    // InStock still needs checking — SortWithFilter handles both
    // the unary evaluation and the sort strategy decision internally.
    return Primitives.SortWithFilter(ref bitmap, ctx.Searcher,
        unaryItems_InStockOnly, ctx.OrderMeta,
        Slice.BeforeAllKeys, Slice.AfterAllKeys, output, 20);
}
```

### 10.4 Sort-skip with additional filters

```sql
from Events where StartDate > '2025-01-01' and Venue = $p1
order by StartDate limit 25
```

The planner detects WHERE StartDate = ORDER BY StartDate. But there's
an additional filter (Venue). We don't need to decide upfront whether
Venue is selective — build the bitmap and let the runtime decide:

```csharp
static int Execute(ref QueryContext ctx, Span<long> output, ref int skip)
{
    using var bitmap = new RoaringBitmap(ctx.Allocator);
    Primitives.FillFromPostings(ctx.GetPostings(Field_Venue, ctx.P1), ref bitmap);

    // Bitmap is the Venue candidate set. SortWithFilter handles both
    // the StartDate range filter (as unary predicate for heap-sort path,
    // or as rangeStart for index-walk path) and the sort strategy.
    return Primitives.SortWithFilter(ref bitmap, ctx.Searcher,
        unaryItems_StartDateGt, ctx.OrderMeta,
        ctx.P0, Slice.AfterAllKeys, output, 25);
}
```

`SortWithFilter` decides internally: for a small bitmap, iterate
entries, check StartDate > '2025-01-01' per-entry, heap-sort. For a
large bitmap, walk the StartDate index from `rangeStart` ('2025-01-01')
forward, check `bitmap.Contains()` per candidate.

`rangeStart` / `rangeEnd` control the index walk bounds. For
`ORDER BY X DESC`, `SortWithFilter` walks backward from `rangeEnd`.
Use `Slice.BeforeAllKeys` / `Slice.AfterAllKeys` for unbounded.

### 10.5 IN clause with lazy OR

```sql
from Articles where Tag in ($t0, $t1, ... $t29) and Published = true
```

IN cardinality = sum of individual term cardinalities (max number of entries in index).

```csharp
static int Execute(ref QueryContext ctx, Span<long> output, ref int skip)
{
    using var bitmap = new RoaringBitmap(ctx.Allocator);
    var tags = ctx.GetTermList("Tag");
    for (int i = 0; i < tags.Length; i++)
        Primitives.LazyOrWithPostings(ctx.GetPostings(Field_Tag, tags[i]),
            ref bitmap);
    // fix cardinality counts that were previously skipped in LazyOrWithPostings
    bitmap.RepairAfterLazy(); 

    var publishedPostings = ctx.GetPostings(Field_Published, ctx.TrueSlice);
    if (ShouldSwitchToEntryScan(ref bitmap, publishedPostings))
        goto EntryScan_AfterTags;

    Primitives.AndWithPostings(publishedPostings, ref bitmap);
    return Primitives.IterateInto(ref bitmap, output, ref skip);

EntryScan_AfterTags:
    // Published evaluated per-entry
    return Primitives.ScanAndFilter(ref bitmap, ctx.Searcher,
        unaryItems_PublishedOnly, output, ctx.Limit, ref skip);
}
```

### 10.6 Complex boolean with dynamic unary promotion

```sql
from Patients
where (Department = 'Cardiology' or Department = 'Neurology')
  and Status != 'Discharged'
  and AdmitDate > '2025-01-01'
order by AdmitDate desc limit 50
```

```csharp
static int Execute(ref QueryContext ctx, Span<long> output, ref int skip)
{
    using var bitmap = new RoaringBitmap(ctx.Allocator);
    Primitives.OrWithPostings(ctx.GetPostings(Field_Dept, ctx.P0), ref bitmap);
    Primitives.OrWithPostings(ctx.GetPostings(Field_Dept, ctx.P1), ref bitmap);

    var dateTree = ctx.GetCompactTree(Field_AdmitDate);
    if (ShouldSwitchToEntryScan(ref bitmap, dateTree))
        goto EntryScan_AfterDept;

    using var dateBitmap = new RoaringBitmap(ctx.Allocator);
    Primitives.FillFromRange(dateTree, ctx.P2, Slice.AfterAllKeys, ref dateBitmap);
    bitmap.AndWith(ref dateBitmap);

    var statusPostings = ctx.GetPostings(Field_Status, ctx.P3);
    if (ShouldSwitchToEntryScan(ref bitmap, statusPostings))
        goto EntryScan_AfterDate;

    Primitives.AndNotWithPostings(statusPostings, ref bitmap);

    // ORDER BY AdmitDate DESC — walk AdmitDate index backward from
    // AfterAllKeys down to '2025-01-01', check bitmap.Contains().
    // All predicates applied via bitmap — no remaining unary filters.
    return Primitives.SortWithFilter(ref bitmap, ctx.Searcher,
        MultiUnaryItem.Empty, ctx.OrderMeta,
        ctx.P2, Slice.AfterAllKeys, output, 50);

EntryScan_AfterDept:
    // AdmitDate range + Status != Discharged still need checking.
    return Primitives.SortWithFilter(ref bitmap, ctx.Searcher,
        unaryItems_DateAndStatus, ctx.OrderMeta,
        ctx.P2, Slice.AfterAllKeys, output, 50);

EntryScan_AfterDate:
    // Only Status != Discharged still needs checking.
    return Primitives.SortWithFilter(ref bitmap, ctx.Searcher,
        unaryItems_StatusOnly, ctx.OrderMeta,
        ctx.P2, Slice.AfterAllKeys, output, 50);
}
```

EXPLAIN records which goto path was taken so the Studio query plan UI
shows the actual execution strategy.

### 10.7 Full-text search with boost and score sort

```sql
from index 'Articles'
where boost(search(Title, $p0), 10) or search(Body, $p1)
order by score() limit 10
```

```csharp
static int Execute(ref QueryContext ctx, Span<long> output, ref int skip)
{
    using var bitmap = new RoaringBitmap(ctx.Allocator);

    // Build candidate set: all entries matching either search clause.
    // No scoring yet — just set bits in the bitmap.
    Primitives.OrWithPostings(ctx.GetSearchPostings(Field_Title, ctx.P0),
        ref bitmap);
    Primitives.OrWithPostings(ctx.GetSearchPostings(Field_Body, ctx.P1),
        ref bitmap);

    // Sort by score: iterates bitmap in batches, re-reads term frequencies
    // from the Title and Body posting lists per entry, computes BM25 with
    // boost factors, feeds a top-10 heap. O(LIMIT) memory, not O(bitmap).
    Span<BoostingTermInfo> terms = stackalloc BoostingTermInfo[] {
        new(Field_Title, ctx.P0, boost: 10.0f),
        new(Field_Body, ctx.P1, boost: 1.0f)
    };
    return Primitives.SortByScore(ref bitmap, ctx.Searcher, terms, output, 10);
}
```

### 10.8 Vector search with pre-filtering

```sql
from index 'ProductSearch'
where Category = $p0 and Active = true
and   vector.search(Embedding, $vec, 20)
order by score()
```

```csharp
static int Execute(ref QueryContext ctx, Span<long> output, ref int skip)
{
    using var bitmap = new RoaringBitmap(ctx.Allocator);

    // Build candidate set from WHERE clause
    Primitives.FillFromPostings(ctx.GetPostings(Field_Category, ctx.P0),
        ref bitmap);

    var activePostings = ctx.GetPostings(Field_Active, ctx.TrueSlice);
    if (ShouldSwitchToEntryScan(ref bitmap, activePostings))
        goto EntryScan_AfterCategory;

    Primitives.AndWithPostings(activePostings, ref bitmap);
    goto VectorSearch;

EntryScan_AfterCategory:
    // Active evaluated per-entry — filter bitmap in place
    Primitives.ScanAndFilterInPlace(ref bitmap, ctx.Searcher,
        unaryItems_ActiveOnly);

VectorSearch:
    // VectorRank filters bitmap in place to top-K, writes distances
    // to ctx.Distances for @distance metadata.
    int k = ctx.GetInt(Param_VectorK);
    Primitives.VectorRank(ctx.GetVectorIndex(Field_Embedding),
        ctx.Searcher, ctx.Vec, ref bitmap, ref ctx.Distances, k);
    // bitmap now has ≤K bits set. ORDER BY score() means sort by
    // distance (best similarity first). ctx.Distances has the values —
    // just sort the ≤K entries by their distance.
    return Primitives.SortByDistance(ref bitmap, ref ctx.Distances,
        output, k);
}
```

### 10.9 Vector search with sort on a different field

```sql
from index 'ProductSearch'
where Category = $p0
and   vector.search(Embedding, $vec, 100)
order by Price asc limit 20
```

Vector search returns top-100 by similarity, then we sort those by Price
and take 20. The vector K (100) is larger than the final LIMIT (20) to
give the Price sort enough candidates.

```csharp
static int Execute(ref QueryContext ctx, Span<long> output, ref int skip)
{
    using var bitmap = new RoaringBitmap(ctx.Allocator);

    Primitives.FillFromPostings(ctx.GetPostings(Field_Category, ctx.P0),
        ref bitmap);

    // VectorRank filters bitmap IN PLACE to top-K by similarity.
    // Clears bits for entries not in top-K. Also writes distances
    // into ctx.Distances — a (entryId, distance) lookup keyed by
    // entry ID, so the projection layer can retrieve @distance
    // metadata per result regardless of subsequent sort order.
    // K is bounded and small — the lookup is O(K) memory.
    int k = ctx.GetInt(Param_VectorK);
    Primitives.VectorRank(ctx.GetVectorIndex(Field_Embedding),
        ctx.Searcher, ctx.Vec, ref bitmap, ref ctx.Distances, k);
    // bitmap now has ≤K bits set.

    // Sort the ≤K vector results by Price. Bitmap is tiny —
    // SortWithFilter will heap-sort from entry blobs.
    return Primitives.SortWithFilter(ref bitmap, ctx.Searcher,
        MultiUnaryItem.Empty, ctx.OrderMeta,
        Slice.BeforeAllKeys, Slice.AfterAllKeys, output, 20);
}
```

`VectorRank` modifies the bitmap in place — no clear + rebuild loop.
Distances are written to `ctx.Distances`, a small lookup
(`(entryId, float)` pairs, sorted by entry ID, allocated from
`ByteStringContext`). The projection layer reads `@distance` per
result via `ctx.Distances.Get(entryId)`. Since K is small (20-100),
the lookup is trivially small.

### 10.10 Constant folding — `true OR` elimination

```sql
from index 'PreOrderAll' as result
where (true or result.HasDebtorLocations = false)
order by result.IsHandled
```

Pass 1 (constant folding) simplifies bottom-up:
```
WHERE: OR(true, HasDebtorLocations = false)
     → true OR X = true
     → WHERE clause eliminated
```

The query reduces to:
```sql
from index 'PreOrderAll' order by result.IsHandled
```

Generated code — no bitmap, no WHERE, just walk the sort index:
```csharp
static int Execute(ref QueryContext ctx, Span<long> output, ref int skip)
{
    // All entries — walk IsHandled index from start.
    return Primitives.OrderedRangeScan(
        ctx.GetCompactTree(Field_IsHandled),
        Slice.BeforeAllKeys, Slice.AfterAllKeys,
        output, ctx.Limit, ref skip);
}
```

For subclauses: `(true OR X) AND Y AND Z` → `true AND Y AND Z` → `AND(Y, Z)`.
The constant propagates through, and the remaining predicates proceed
through the normal planner passes (cardinality estimation, reordering,
etc.) as if the `true OR X` was never there.

---

## 11. Testing & Risks

**Testing.** Result parity with Corax 1.0: the full existing test
corpus runs against the new path before the old code is deleted.
Performance regression bench extended with bitmap benchmarks from
`RavenDB-25284`. CI smoke set on every push; full perf bench nightly.

**Risks.**
- Bitmap allocation pressure on `ByteStringContext` for very large result
  sets. Bounded by `BitmapMemoryRequiredThresholdInBytes` (32 MB).
- Sort stability — bitmap iteration is by entry ID; explicit tie-break
  by entry ID in ORDER BY generation.
- DynamicMethod IL bugs are hard to debug — mitigated by EXPLAIN source
  + comprehensive parity tests.

---

## 12. Future Work

- **Galloping intersection for skewed containers.** When ANDing a tiny
  Array (5 elements) against a large one (4000), galloping beats
  vectorized merge. Add a size-ratio threshold to switch strategies.
- **AVX-512 gating.** Lemire (Feb 2025): `vpcompressw` is 15x slower on
  AMD Zen 4. Gate AVX-512 paths on specific microarchitectures.
- **VPOPCNTDQ for cardinality.** .NET 11 will add AVX-512 VPOPCNTDQ
  intrinsics. Until then, Harley-Seal AVX2 (0.52 cycles/8 bytes).
- **Frozen/memory-mapped bitmaps.** If posting lists were stored in
  roaring format on disk, they could be memory-mapped as read-only
  bitmaps without decoding. Requires on-disk format change — post-2.0.
- **Batch-vectorized iteration.** Emit bitmap results in
  container-aligned batches (Array→bulk copy, Bitmap→SIMD tzcnt loops).
  Lucene 10.3 achieved 40% speedup with this pattern.
