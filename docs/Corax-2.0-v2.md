# Corax 2.0 — Design Document

*Target: RavenDB 8.0. Corax becomes the default indexing engine.*

*Source ground truth: the v7.2 codebase, the `RavenDB-25284` prototype,
and the `RavenDB-25281` implementation branch (on `ayende/ravendb`).
All type/file references are from the code.*

---

## 1. Executive Summary

Corax 2.0 replaces the query execution model. Two changes ship together:

- **Roaring bitmap as accumulator.** Replace the streaming
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

The compiled path is the primary execution model. `IQueryMatch` is
**retained** as the integration seam — new types (`BitmapMatch`,
`CombinedMatch`, `CompiledQueryMatch`, `PostFilterMatch`) all implement
it. A new `IBitmapQueryMatch` extension exposes bitmap-backed matches
for zero-copy borrowing by downstream consumers (sort, vector search,
faceted lookup).

Deleted: `BinaryMatch<,,>`, seven `*.Erasure.cs` files,
`MemoizationMatch`, `MergeHelper`, `DeduplicationMatch`,
`RoaringBitmapMatch`, `IncludeNullMatch`, `IncludeNonExistingMatch`,
and the `delegate*` function-pointer dispatch. The entire
`QueryOptimizer` folder (`CoraxAndQueries`, `CoraxBooleanItem`,
`CoraxBooleanQueryBase`, `CoraxOrQueries`, `CoraxWhenQuery`,
`ICoraxClause`) is replaced by `QueryPlanBuilder`.

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
4. Replace `BinaryMatch<,,>`/erasure-match tree with compiled
   DynamicMethod execution. The compiled path is the primary execution
   model. `IQueryMatch` is retained as integration seam for match
   wrapping (sort, vector, spatial, boosting); the heavyweight
   `BinaryMatch<,,>` / erasure / MemoizationMatch machinery is deleted.
   No feature toggle — the compiled path is always used.

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
   AND, OR, ANDNOT). The common path for most queries. Term ops use
   native `TermSource` dispatch (bypassing `IQueryMatch`); non-term
   ops (search, range, vector) use the `IQueryMatch` bridge path.
2. **Entry scan (unary matching)** — when the bitmap shrinks below a
   threshold (~32K), remaining predicates are evaluated per-entry via
   `EntryTermsReader` instead of more posting list lookups. The
   predicate comparisons are emitted as **direct IL** (long/double/
   slice compare instructions), not through `MultiUnaryItem` delegates.
3. **Ordered range scan** — when WHERE field = ORDER BY field with
   LIMIT, walk the CompactTree in sort order directly. No bitmap.
   Handled by `SortingMatch` wrapping a `CompiledQueryMatch` that
   implements `IBitmapQueryMatch` (the sort path walks the index and
   calls `Contains()` per candidate).

---

## 6. Roaring Bitmap

### 6.1 Implementation

`public struct RoaringBitmap : IDisposable` . Allocated through `ByteStringContext`, zero
managed-heap pressure. Not a `ref struct` — the bitmap is stored in
fields (`BitmapMatch`, `CompiledQueryMatch`) and held in
`Span<RoaringBitmap>` arrays (the `QueryScanContext.Bitmaps` pool).
Lifetime discipline is enforced by convention, not the type system:
each bitmap is disposed when its owning match is disposed.

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
- **`AndWithPostings`** — bounds the posting list scan by the bitmap's
  container key range (min/max), then fills a temp bitmap via the
  iterator's `Seek` + `pruneAfter` and batch-ANDs:

  1. Read the bitmap's `MinContainerKey` and `MaxContainerKey` (trivial
     field reads on the flat index).
  2. Seek the posting list B+ tree to `minKey * ContainerSize` (skip
     all entries below the bitmap's range).
  3. Batch-fill entries from the posting list iterator with
     `pruneAfter = (maxKey + 1) * ContainerSize - 1` (stop reading past
     the bitmap's range).
  4. Decode PFor blocks into a temp bitmap (reused, cleared between ops).
  5. `bitmap.AndWith(ref tempBitmap)` — SIMD container AND over
     overlapping container keys.

  Simpler than per-bit galloping but achieves the same effect: only
  posting list pages whose entry IDs fall within the bitmap's container
  range are ever visited. The temp bitmap is allocated once per query,
  `Clear()`'d between ops. PFor decoding is sequential — you can't
  selectively decode entries from a compressed block, so decoding the
  whole page into a temp bitmap costs the same as decoding only the
  overlapping entries. Cost proportional to *containers that intersect
  the bitmap's range*, not the total posting list size.
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

public static class QueryPrimitives
{
    // --- Posting list → bitmap ---
    // Walks leaf pages, decodes PFor blocks, adds entries to bitmap via
    // batch AddRange. Stops once `limit` entries have been added.
    void FillFromPostings(ref PostingList.Iterator iterator,
                          ref RoaringBitmap bitmap,
                          long limit = long.MaxValue);

    // AND bitmap with posting list using container-key-range bounding.
    // Fills tempBitmap from the posting list (Seek to minKey × 65536,
    // batch-Fill with pruneAfter), then bitmap.AndWith(ref tempBitmap).
    void AndWithPostings(ref PostingList.Iterator iterator,
                         ref RoaringBitmap bitmap,
                         ref RoaringBitmap tempBitmap);

    // OR bitmap with posting list — walk all leaf pages, set bits.
    void OrWithPostings(ref PostingList.Iterator iterator,
                        ref RoaringBitmap bitmap);

    // ANDNOT with posting list — same container-key-range bounding,
    // calls bitmap.AndNotWith(ref tempBitmap).
    void AndNotWithPostings(ref PostingList.Iterator iterator,
                            ref RoaringBitmap bitmap,
                            ref RoaringBitmap tempBitmap);

    // --- IQueryMatch-based overloads ---
    // Fast-path dispatch: IBitmapQueryMatch → steal containers via
    // LazyOrWith + RepairAfterLazy; TermMatch with large posting list
    // → native FillFromPostings; fallback → generic Fill+AddRange.
    void FillFromMatch(IQueryMatch match, ref RoaringBitmap bitmap);
    void AndWithMatch(IQueryMatch match, ref RoaringBitmap bitmap,
                      ref RoaringBitmap tempBitmap);
    void AndNotWithMatch(IQueryMatch match, ref RoaringBitmap bitmap,
                         ref RoaringBitmap tempBitmap);

    // --- Native TermSource dispatch (bypasses IQueryMatch) ---
    // TermSource wraps the three-way CompactTree value encoding:
    // Empty, Single (entry ID inline), SmallPostingList (FastPFor
    // buffer in Container), PostingList (full B+ tree iterator).
    void FillBitmapFromTermSource(ref TermSource source, LowLevelTransaction llt,
                                  ref RoaringBitmap bitmap);
    void AndWithTermSource(ref TermSource source, LowLevelTransaction llt,
                           ref RoaringBitmap bitmap, ref RoaringBitmap tempBitmap);
    void AndNotWithTermSource(ref TermSource source, LowLevelTransaction llt,
                              ref RoaringBitmap bitmap, ref RoaringBitmap tempBitmap);

    // --- Term provider → bitmap (for range/startsWith/regex/contains) ---
    void FillBitmapFromTermProvider(ITermProvider provider,
                                    LowLevelTransaction llt,
                                    ref RoaringBitmap bitmap);

    // --- Bitmap → output ---
    int IterateInto(ref RoaringBitmap bitmap, Span<long> output,
                    ref RoaringBitmapIterator iterator);

    // --- Entry scan (emitted as direct IL in the DynamicMethod) ---
    // The IL emitter generates per-predicate comparisons directly
    // (Long/Double/Slice comparisons, Between, NotEqual, OR-groups)
    // instead of calling through MultiUnaryItem. The scan loop walks
    // the bitmap iterator, calls EntryTermsReader per entry, evaluates
    // each predicate via emitted compare instructions, and collects
    // matches into scratch bitmap slot 1, then swaps bitmaps.
    // (No single ScanAndFilter primitive — the scan is inlined in IL.)

    // --- Runtime strategy helpers ---
    bool ShouldSwitchToEntryScan(ref RoaringBitmap bitmap,
                                 in PostingListState postingListState);
    bool ShouldHeapSortDirectly(ref RoaringBitmap bitmap,
                                long sortFieldTotalEntries);
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
static bool ShouldSwitchToEntryScan(ref RoaringBitmap bitmap, in PostingListState postingListState)
{
    // Entry scan: read bitmap.Count blobs, compare fields. ~1-2μs per blob.
    // Bitmap AND: container-key-range-bounded scan, cost ~ containers that
    //   intersect the bitmap's range.
    // Heuristic: one posting list page-seek + decode ≈ scanning ~64 blobs.
    // Switch when bitmap is small enough AND the posting list is large
    // relative to the bitmap.
    var bitmapCount = bitmap.Count;
    return bitmapCount < 32_000
        && bitmapCount * 64 < postingListState.NumberOfEntries;
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
static bool ShouldHeapSortDirectly(ref RoaringBitmap bitmap, long sortFieldTotalEntries)
{
    // Heap-sort: read bitmap.Count blobs, extract sort field, push into
    // PriorityQueue. Cost ~ bitmap.Count * (blob read + comparison).
    // Index walk: scan sort-field index entries, check bitmap.Contains()
    // per entry. Cost ~ sort-field entries checked until LIMIT satisfied.
    // When bitmap is small relative to the sort-field index, heap-sort wins.
    var bitmapCount = bitmap.Count;
    return bitmapCount < 32_000
        && bitmapCount * 64 < sortFieldTotalEntries;
}
```

```csharp
static void Execute(ref QueryScanContext ctx)
{
    // ctx.Bitmaps[0] = main result bitmap (pre-allocated by CompiledQueryMatch)
    // ctx.TermSources[i] = resolved posting list references (Three-way dispatch:
    //   Empty / Single / SmallPostingList / PostingList)

    QueryPrimitives.FillBitmapFromTermSource(
        ref ctx.TermSources[0], ctx.Llt, ref ctx.Bitmaps[0]);

    // Runtime check: is the bitmap small enough that scanning entries
    // is cheaper than AND-ing with the next posting list?
    if (ShouldSwitchToEntryScan(ref ctx.Bitmaps[0],
            ref ctx.DirectSources[1].Count /* next source cardinality */))
        goto EntryScan_AfterFirst;

    QueryPrimitives.AndWithTermSource(
        ref ctx.TermSources[1], ctx.Llt, ref ctx.Bitmaps[0], ref ctx.Bitmaps[1]);
    if (ctx.Bitmaps[0].IsEmpty) return;

    QueryPrimitives.AndWithTermSource(
        ref ctx.TermSources[2], ctx.Llt, ref ctx.Bitmaps[0], ref ctx.Bitmaps[1]);
    // bitmap[0] holds the final result; CompiledQueryMatch reads it via IterateInto
    return;

EntryScan_AfterFirst:
    // Emitted IL iterates bitmap[0] entries, reads entry data via
    // EntryTermsReader, evaluates remaining predicates with direct
    // comparison instructions (long/double/slice compare baked into IL).
    // Matching entries go into bitmap[1], then SwapContents swaps them.
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
QueryPrimitives.FillFromPostings(ref fooIter, ref bitmap, limit: 5);
// FillFromPostings checks bitmap count after each posting list leaf page
// and stops once `limit` entries have been accumulated. For LIMIT 5, the
// first page (~1K entries) is decoded but only 5 entries are added.
// The remaining pages are never read.

// AndWithPostings uses the container-key-range bounding pattern:
// bitmap now has ≤5 entries across at most 5 containers.
// The posting list scan is bounded to those containers' range.
QueryPrimitives.AndWithPostings(ref barIter, ref bitmap, ref tempBitmap);

// IterateInto produces the final output.
QueryPrimitives.IterateInto(ref bitmap, output, ref iterator);
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
    [5] Resolve: TermSources (native dispatch via CompactTree value encoding)
        and IQueryMatch[] DirectSources (vector/spatial/multi-term matches)
    [6] Lookup plan cache (per index instance, keyed by queryText + ordering + TypeSignature)
        → hit: return cached DynamicMethod delegate
        → miss: emit via QueryILEmitter, cache
    → resolve matches → extract scan parameters (long/double/slice)
    → wrap in CompiledQueryMatch → wrap spatial PostFilterMatch if needed
      → invoke delegate(ref QueryScanContext)
        → goto checks between AND steps handle unary promotion at runtime (§7.3)
        → per-op timing recorded for EXPLAIN diagnostics
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

**Pass 7 — TermSource resolution.** Each term op's posting list
reference is decoded from the CompactTree value: the low 2 bits
distinguish Single / SmallPostingList / PostingList. Single values
have the entry ID inline. SmallPostingLists are FastPFor buffers in
a Container. PostingLists are full B+ tree iterators. This three-way
dispatch lets the emitted IL call native primitives (`FillBitmapFromTermSource`,
`AndWithTermSource`, `AndNotWithTermSource`) instead of going through
the `IQueryMatch` wrapper.

**Pass 8 — Emit.** Generate DynamicMethod IL. The emitter inserts
goto checks between AND steps (§7.3) and LIMIT checks at processing
boundaries (§7.4) — these are runtime mechanisms, not planner
decisions. Generate lazy EXPLAIN source provider.

The emitted IL also records per-op timing (Stopwatch.GetTimestamp
before/after) and result counts (`bitmap[0].Count` after each op)
when telemetry is requested. These feed the EXPLAIN diagnostics and
are visible via `CompiledQueryMatch.Inspect()`.

### 8.4 Plan cache

The plan cache lives on the index instance (one per `IndexSearcher`).
When the index is replaced (side-by-side), the new instance gets a
fresh cache; the old cache is GC'd with the old instance.

```
Per IndexSearcher:
  ConcurrentDictionary<string queryText, PerQueryPlans>
  PerQueryPlans: 32-slot struct-of-arrays
    int[]   _orderings
    int[]   _typesigs
    CompiledPlan[] _plans
```

The key is the RQL query string as-is. The value is a fixed 32-slot
SoA (struct-of-arrays) per query text — parallel arrays of `int` for
orderings and type signatures, with a `CompiledPlan[]` payload.

Each `CompiledPlan` carries an `int Ordering` field: the operand sort
order packed at 3 bits per position (values 0-7), up to 10 operands
in 30 bits. E.g. [2, 0, 1] → `(2 << 0) | (0 << 3) | (1 << 6)` = `0x42`.

Each `CompiledPlan` also carries an `int TypeSignature`: 2 bits per
parameter (0=long, 1=double, 2=string) packing the types of all
bound query parameters. Different parameter types produce different
IL code paths (different comparison instructions), so the cache must
distinguish them. E.g. `WHERE Age > $p0` with `$p0=long` vs
`$p0=double` produce different emitted IL.

Lookup uses SIMD-accelerated vector comparison over the SoA arrays:

```csharp
// Vector256 path: 8 slots per iteration, two Vector256.Equals
//   (one for orderings, one for typesigs), AND, ExtractMostSignificantBits,
//   TrailingZeroCount. Typical 1-3 plans = 1 vector iteration.
if (Vector256.IsHardwareAccelerated)
    return Vec256Lookup(ordering, typesig);
if (Vector128.IsHardwareAccelerated)
    return Vec128Lookup(ordering, typesig);
return ScalarLookup(ordering, typesig);
```

Embedded-key revalidation guards against torn writes: after a slot
match, the plan's own `Ordering` and `TypeSignature` fields are
re-checked before returning. This allows lock-free publishing:
the plan ref is written first (Volatile.Write), then the keys.

On miss, generate a new DynamicMethod (~50μs) and publish it into
the SoA arrays via sequential fill (while < 32) or random eviction.

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
// Lives in Raven.Server, building a QueryPlan from parsed RQL AST.
class QueryPlanBuilder
{
    static QueryPlan BuildPlan(PlanParameters parameters);
    static IQueryMatch BuildAndCompile(PlanParameters planParams, ...);
}

// A compiled plan — DynamicMethod delegate + lazy EXPLAIN source.
class CompiledPlan
{
    // void return: the result is in ctx.Bitmaps[0] after execution.
    // The caller (CompiledQueryMatch) reads it via IterateInto.
    delegate void CompiledExecuteDelegate(ref QueryScanContext ctx);

    CompiledExecuteDelegate CompiledDelegate;
    int Ordering;       // packed operand ordering
    int TypeSignature;  // packed parameter type signature
    Func<string> ExplainSourceProvider;  // lazy, generated on first read
}

// Runtime state bag passed to the compiled function.
ref struct QueryScanContext
{
    Span<RoaringBitmap> Bitmaps;      // [0]=main result, [1..N]=scratch
    IndexSearcher Searcher;
    Span<IQueryMatch> DirectSources;  // vector/spatial/multi-term matches
    Span<TermSource> TermSources;     // native posting-list dispatch
    LowLevelTransaction Llt;          // cached for TermSource dispatch
    Span<ITermProvider> TermProviders;
    Span<long> FieldRootPages;        // for entry scan predicates
    Span<long> LongParams;            // typed parameter values
    Span<double> DoubleParams;
    Span<Slice> SliceParams;
    CancellationToken Token;
    Span<long> Timings;               // per-op Stopwatch ticks
    Span<long> ResultCounts;          // bitmap count after each op
    int EntryScanTakenAtOp;           // -1 if not triggered
}

// Three-way native posting-list source. Mirrors the encoding in the
// CompactTree value's low 2 bits (Single / SmallPostingList / PostingList).
// Resolved up-front by QueryPlanBuilder, bypasses IQueryMatch wrapper
// for term ops (Fill/AndWith/AndNotWith).
struct TermSource
{
    TermSourceKind Kind;  // Empty, Single, SmallPostingList, PostingList
    long SingleEntryId;
    long SmallPostingListId;
    PostingList.Iterator LargeIterator;
}

// Bitmap-backed IQueryMatch. Used when query operations produce a bitmap
// that needs to be wrapped for the rest of the pipeline.
struct BitmapMatch : IQueryMatch, IBitmapQueryMatch, IDisposable
{
    ref RoaringBitmap Bitmap { get; }  // mutable ref for building
    // Implements IQueryMatch.Fill via RoaringBitmapIterator
    // Implements IBitmapQueryMatch.BorrowBitmap() for zero-copy sharing
}

// Lightweight replacement for BinaryMatch. Combines two IQueryMatch
// instances with OR or AND semantics during Fill().
struct CombinedMatch : IQueryMatch
{
    static CombinedMatch Or(IQueryMatch left, IQueryMatch right);
    static CombinedMatch And(IQueryMatch left, IQueryMatch right);
}

// The compiled query entry point. Wraps a CompiledPlan delegate +
// resolved matches + parameters. Executes lazily (deferred until first
// Fill/Count/Contains). Implements IBitmapQueryMatch so downstream
// consumers (SortingMatch, VectorSearch) can borrow the bitmap directly.
struct CompiledQueryMatch : IQueryMatch, IBitmapQueryMatch, IDisposable
{
    // BorrowBitmap() returns the result bitmap directly — downstream
    // consumers (SortingMatch.SortUsingIndex, VectorSearch.LoadFilterMatches)
    // use this to skip re-materialization.
    RoaringBitmap BorrowBitmap();
}

// Chains spatial filters after a compiled bitmap match.
struct PostFilterMatch : IQueryMatch { ... }

// Extension interface for bitmap-backed IQueryMatch. Enables
// SortingMatch to avoid full materialization via MemoizationMatch
// and instead walk the CompactTree index, intersecting batches via
// AndWith() against the bitmap.
interface IBitmapQueryMatch : IQueryMatch
{
    bool Contains(long entryId);
    long MinEntryId { get; }
    long MaxEntryId { get; }
    RoaringBitmap BorrowBitmap();  // zero-copy, caller MUST NOT dispose
}
```

Fields in generated IL are referenced by constant integer IDs (resolved
at plan time from the index schema, not by string names). Term ops
resolve to `TermSource[]` indices for native dispatch; non-term ops
(vector, spatial, multi-term) resolve to `DirectSources[]` indices
into the `IQueryMatch[]` array.

**Removed types**: `BinaryMatch<,,>`, all `*.Erasure.cs`,
`MemoizationMatch` (+ erasure + provider), `MergeHelper`,
`DeduplicationMatch`, `RoaringBitmapMatch`, `IncludeNullMatch`,
`IncludeNonExistingMatch`, `CoraxQueryBuilder.QueryOptimizer` folder
(CoraxAndQueries, CoraxBooleanItem, CoraxBooleanQueryBase,
CoraxOrQueries, CoraxWhenQuery, ICoraxClause).

**New types**: `QueryPlanBuilder`, `CompiledPlan`, `QueryScanContext`,
`QueryILEmitter`, `PlanCache`, `PlanOp`, `TermSource`, `BitmapMatch`,
`CombinedMatch`, `CompiledQueryMatch`, `PostFilterMatch`,
`IBitmapQueryMatch`, `QueryPrimitives`.

---

## 10. Examples

### 10.1 Simple term with no ORDER BY

```sql
from Users where Status = $p0 limit 10
```

```
// EXPLAIN pseudocode (actual execution via DynamicMethod IL)
static void Execute(ref QueryScanContext ctx)
{
    // Single operand, no ORDER BY — no bitmap needed.
    // The TermSource is resolved to the posting list iterator;
    // the caller (CompiledQueryMatch.Fill) reads entries via
    // RoaringBitmapIterator onto the output Span<long>.
    QueryPrimitives.FillBitmapFromTermSource(
        ref ctx.TermSources[0], ctx.Llt, ref ctx.Bitmaps[0]);
    // ctx.Bitmaps[0] now has the result — caller iterates it.
}
```

Single operand + no ORDER BY always uses direct posting list iteration
(through the TermSource native dispatch). The bitmap is still built
(to maintain the uniform pipeline), but the entry IDs flow directly
from the posting list iterator into the bitmap via batch AddRange.
No per-entry overhead, one pass through the data. LIMIT is handled by
the `FillFromPostings` limit parameter, which stops after the first
page when the limit is reached.

### 10.2 AND chain with dynamic unary promotion

```sql
from Products where Tag = $p0 and Category = $p1 and InStock = $p2
  and Rating >= $p3 limit 50
```

Cardinalities: Tag ~1%, Category ~10%, InStock ~80%, Rating≥4 ~40%.

```
// EXPLAIN pseudocode (actual execution via DynamicMethod IL)
static void Execute(ref QueryScanContext ctx)
{
    // ctx.TermSources[0] = Tag = $p0 (smallest — fills first)
    QueryPrimitives.FillBitmapFromTermSource(
        ref ctx.TermSources[0], ctx.Llt, ref ctx.Bitmaps[0]);

    // ctx.DirectSources[1] = Category posting list (IQueryMatch wrapper)
    // Runtime check: bitmap small enough to scan entries instead?
    if (ctx.Bitmaps[0].Count < 32000
        && ctx.Bitmaps[0].Count * 64 < ctx.DirectSources[1].Count)
        goto EntryScan_AfterFirst;

    QueryPrimitives.AndWithMatch(ctx.DirectSources[1],
        ref ctx.Bitmaps[0], ref ctx.Bitmaps[1]);
    if (ctx.Bitmaps[0].IsEmpty) return;

    if (ctx.Bitmaps[0].Count < 32000
        && ctx.Bitmaps[0].Count * 64 < ctx.DirectSources[2].Count)
        goto EntryScan_After2;

    QueryPrimitives.AndWithMatch(ctx.DirectSources[2],
        ref ctx.Bitmaps[0], ref ctx.Bitmaps[1]);
    if (ctx.Bitmaps[0].IsEmpty) return;

    if (ctx.Bitmaps[0].Count < 32000
        && ctx.Bitmaps[0].Count * 64 < ctx.DirectSources[3].Count)
        goto EntryScan_After3;

    QueryPrimitives.AndWithMatch(ctx.DirectSources[3],
        ref ctx.Bitmaps[0], ref ctx.Bitmaps[1]);
    return; // result in Bitmaps[0]

EntryScan_AfterFirst:
    // Emitted IL: iterate bitmap[0], for each entry read EntryTermsReader,
    // emit direct long/double/slice comparisons for Category+InStock+Rating.
    // Matches collected into bitmap[1], SwapContents swaps with bitmap[0].

EntryScan_After2:
    // Emitted IL: same pattern, InStock+Rating remaining.

EntryScan_After3:
    // Emitted IL: same pattern, Rating remaining.
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

```
// EXPLAIN pseudocode (actual execution via DynamicMethod IL)
static void Execute(ref QueryScanContext ctx)
{
    // ctx.DirectSources[0] = Price BETWEEN 10..100 (MultiTermMatch driving
    // a TermRangeProvider, resolved as IQueryMatch, not TermSource)
    QueryPrimitives.FillFromMatch(ctx.DirectSources[0], ref ctx.Bitmaps[0]);

    if (ctx.Bitmaps[0].Count < 32000
        && ctx.Bitmaps[0].Count * 64 < ctx.DirectSources[1].Count)
        goto EntryScan_AfterPrice;

    QueryPrimitives.AndWithMatch(ctx.DirectSources[1],
        ref ctx.Bitmaps[0], ref ctx.Bitmaps[1]);
    return; // result in Bitmaps[0]; sorted by Name via SortingMatch

EntryScan_AfterPrice:
    // Emitted IL: iterate bitmap[0], check InStock per entry,
    // collect matches into bitmap[1], SwapContents.
    // SortingMatch wraps this and sorts the result by Name.
}
```

Sorting is handled by `SortingMatch` wrapping the `CompiledQueryMatch`.
When the inner match implements `IBitmapQueryMatch`, `SortingMatch`
avoids full materialization: it walks the CompactTree index (Name) and
intersects batches via `AndWith()` against the bitmap, stopping early
when the LIMIT (20) is reached.

### 10.4 Sort-skip with additional filters

```sql
from Events where StartDate > '2025-01-01' and Venue = $p1
order by StartDate limit 25
```

The planner detects WHERE StartDate = ORDER BY StartDate. But there's
an additional filter (Venue). We don't need to decide upfront whether
Venue is selective — build the bitmap and let the runtime decide:

```
// EXPLAIN pseudocode
static void Execute(ref QueryScanContext ctx)
{
    QueryPrimitives.FillBitmapFromTermSource(
        ref ctx.TermSources[0], ctx.Llt, ref ctx.Bitmaps[0]);
    // Result: Venue candidate set; StartDate filter + sort by StartDate
    // handled by SortingMatch's IBitmapQueryMatch path.
}
```

SortingMatch wraps the CompiledQueryMatch. Because CompiledQueryMatch
implements `IBitmapQueryMatch`, the sort path checks `bitmap.Contains()`
against each entry as it walks the StartDate index. No separate
SortWithFilter primitive — the sort strategy (heap-sort vs index walk)
is decided inside `SortingMatch.SortUsingIndex<TEntryComparer, TDirection>`
based on the `ShouldHeapSortDirectly` heuristic.

### 10.5 IN clause with lazy OR

```sql
from Articles where Tag in ($t0, $t1, ... $t29) and Published = true
```

IN cardinality = sum of individual term cardinalities (max number of entries in index).

```
// EXPLAIN pseudocode
static void Execute(ref QueryScanContext ctx)
{
    // ctx.TermSources[0..29] = Tag IN ($t0..$t29) — each resolved to
    // a TermSource. Emitted loop calls FillBitmapFromTermSource for each.
    for (int i = 0; i < 30; i++)
        QueryPrimitives.FillBitmapFromTermSource(
            ref ctx.TermSources[i], ctx.Llt, ref ctx.Bitmaps[0]);

    ctx.Bitmaps[0].RepairAfterLazy(); // fix cardinality counts

    QueryPrimitives.AndWithMatch(ctx.DirectSources[0],
        ref ctx.Bitmaps[0], ref ctx.Bitmaps[1]);
    if (ctx.Bitmaps[0].IsEmpty) return;

    return; // result in Bitmaps[0]

    // Entry scan label emitted if ShouldSwitchToEntryScan fires
}
```

Note: IN clause does NOT use `LazyOrWithPostings` as a distinct
primitive call. Instead, the emitted IL loops over resolved
`TermSource` entries and calls `FillBitmapFromTermSource` for each,
which handles all three value encodings (Single/SmallPostingList/
PostingList) natively. The lazy cardinality skip is inherent in the
`AddRange` path — `RepairAfterLazy()` fixes counts at the end.

### 10.6 Complex boolean with dynamic unary promotion

```sql
from Patients
where (Department = 'Cardiology' or Department = 'Neurology')
  and Status != 'Discharged'
  and AdmitDate > '2025-01-01'
order by AdmitDate desc limit 50
```

```
// EXPLAIN pseudocode
static void Execute(ref QueryScanContext ctx)
{
    // Department OR — both terms via TermSource native dispatch
    QueryPrimitives.FillBitmapFromTermSource(
        ref ctx.TermSources[0], ctx.Llt, ref ctx.Bitmaps[0]);
    QueryPrimitives.FillBitmapFromTermSource(
        ref ctx.TermSources[1], ctx.Llt, ref ctx.Bitmaps[0]);

    // ctx.DirectSources[0] = AdmitDate > '2025-01-01' (MultiTermMatch range)
    if (ShouldSwitchToEntryScan(...)) goto EntryScan_AfterDept;

    // Fill from multi-term match into temp, AND with main
    QueryPrimitives.AndWithMatch(ctx.DirectSources[0],
        ref ctx.Bitmaps[0], ref ctx.Bitmaps[1]);
    if (ctx.Bitmaps[0].IsEmpty) return;

    // ctx.DirectSources[1] = Status != 'Discharged' (AndNotMatch)
    if (ShouldSwitchToEntryScan(...)) goto EntryScan_AfterDate;

    QueryPrimitives.AndNotWithMatch(ctx.DirectSources[1],
        ref ctx.Bitmaps[0], ref ctx.Bitmaps[1]);

    // result in Bitmaps[0]; sorted by AdmitDate DESC via SortingMatch
    // wrapper implementing IBitmapQueryMatch path
    return;

EntryScan_AfterDept:
    // Emitted IL: iterate bitmap[0], check AdmitDate+Status per entry

EntryScan_AfterDate:
    // Emitted IL: iterate bitmap[0], check Status per entry
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

```
// EXPLAIN pseudocode
static void Execute(ref QueryScanContext ctx)
{
    // Title search + Body search — OR'd into bitmap via IQueryMatch fill
    // (search produces BoostingMatch/IQueryMatch, not native TermSource)
    QueryPrimitives.FillFromMatch(ctx.DirectSources[0], ref ctx.Bitmaps[0]);
    QueryPrimitives.FillFromMatch(ctx.DirectSources[1], ref ctx.Bitmaps[0]);

    // Scoring handled by BoostingMatch wrapper + SortingMatch's
    // Score path. The compiled query builds the candidate set;
    // scoring and BM25 sort are layered on top via IQueryMatch.Score()
    // calls through the resolved match chain.
    return; // result in Bitmaps[0]
}
```

Scoring is handled by the existing `BoostingMatch`/`Score` machinery
wrapping the `CompiledQueryMatch`. The compiled query produces the
candidate bitmap; `SortingMatch` iterates it and calls `Score()` on
the resolved match chain for BM25 computation. No `SortByScore`
primitive is called from the emitted IL — scoring flows through the
`IQueryMatch.Score()` interface.

### 10.8 Vector search with pre-filtering

```sql
from index 'ProductSearch'
where Category = $p0 and Active = true
and   vector.search(Embedding, $vec, 20)
order by score()
```

```
// EXPLAIN pseudocode
static void Execute(ref QueryScanContext ctx)
{
    // WHERE filter: Category AND Active — built into bitmap[0]
    QueryPrimitives.FillBitmapFromTermSource(
        ref ctx.TermSources[0], ctx.Llt, ref ctx.Bitmaps[0]);

    if (ShouldSwitchToEntryScan(...)) goto EntryScan_AfterCategory;
    QueryPrimitives.AndWithMatch(ctx.DirectSources[0],
        ref ctx.Bitmaps[0], ref ctx.Bitmaps[1]);
    goto VectorSearch;

EntryScan_AfterCategory:
    // Emitted IL: iterate bitmap[0], check Active per entry

VectorSearch:
    // The VectorSearchMatch wraps this CompiledQueryMatch as its
    // filterQuery. Because CompiledQueryMatch implements
    // IBitmapQueryMatch, VectorSearchUtils.LoadFilterMatches
    // borrows the bitmap directly (BorrowBitmap()) instead of
    // re-materializing via Fill(). Zero-copy path.
    return;
}
```

Vector search is handled at the match-object level, not via a
primitive. `BuildAndCompile` produces a `CompiledQueryMatch` and then
a `CoraxVectorItem.Materialize()` wraps it as a `VectorSearchMatch`
with the compiled bitmap as its `filterQuery`. The vector retriever
reads the borrowed bitmap via `IBitmapQueryMatch.BorrowBitmap()`.
Sorting by distance is handled by `SortingMatch`/`SortingMultiMatch`
wrapping the vector result — distances are read from `GrowableBuffer`
inside `VectorSearchMatch`.

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

```
// EXPLAIN pseudocode
static void Execute(ref QueryScanContext ctx)
{
    QueryPrimitives.FillBitmapFromTermSource(
        ref ctx.TermSources[0], ctx.Llt, ref ctx.Bitmaps[0]);
    // result to VectorSearchMatch filter; vector returns top-K,
    // then SortingMultiMatch sorts those ≤K results by Price.
}
```

Same pattern as §10.8: the compiled query builds the WHERE bitmap,
`VectorSearchMatch` borrows it via `IBitmapQueryMatch.BorrowBitmap()`,
performs the ANN/brute-force search, and produces ≤K results.
`SortingMultiMatch` then sorts those ≤K results by Price using heap
sort (the set is tiny). No `SortWithFilter` or `VectorRank` primitive
is called from the emitted IL — these are layered as match wrappers.

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

Generated code — empty plan (no bitmap, no WHERE, no TermSources):
```
// EXPLAIN pseudocode
static void Execute(ref QueryScanContext ctx)
{
    // All entries — no bitmap needed, no WHERE clause.
    // ctx.Bitmaps[0] is empty; the caller (CompiledQueryMatch) sees
    // Count = 0 from the empty bitmap and delegates to the enclosing
    // SortingMatch which walks the IsHandled index directly.
    return;
}
```

For subclauses: `(true OR X) AND Y AND Z` → `true AND Y AND Z` → `AND(Y, Z)`.
The constant propagates through, and the remaining predicates proceed
through the normal planner passes (cardinality estimation, reordering,
etc.) as if the `true OR X` was never there.

---

## 11. Testing & Risks

**Testing.** Result parity with Corax 1.0: the full existing test
corpus runs against the new path. The old code (erasure files,
BinaryMatch, etc.) is deleted — there is no fallback path.
Performance regression bench extended with bitmap benchmarks from
`RavenDB-25284`. CI smoke set on every push; full perf bench nightly.

**Risks.**
- Bitmap allocation pressure on `ByteStringContext` for very large result
  sets. Bounded by `BitmapMemoryRequiredThresholdInBytes` (32 MB). The
  bitmap pool (`QueryScanContext.Bitmaps`) reuses scratch slot [1]
  across all ops, avoiding per-op allocation.
- Sort stability — bitmap iteration is by entry ID; explicit tie-break
  by entry ID in ORDER BY generation.
- DynamicMethod IL bugs are hard to debug — mitigated by EXPLAIN source
  + comprehensive parity tests + per-op timing telemetry visible via
  `CompiledQueryMatch.Inspect()`.
- `CombinedMatch` (replacement for `BinaryMatch` in SearchQuery/InQuery)
  introduces heap-allocated buffers — must be kept on the stack frame
  of a calling method and not escape to the heap. The struct lifetime
  discipline is enforced by convention.

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
- **Plan-shape caching.** The `BuildPlan` AST walk is currently cached
  per `IndexSearcher` via `ConditionalWeakTable` (cloned + refreshed
  per call). Moving to a full plan-shape cache that skips the entire
  AST walk on cache hit is the remaining work (tracking in task #82).
- **Per-op limit propagation.** The `CompiledQueryMatch` constructor
  accepts a `limit` parameter but it is not yet wired through to
  `FillFromPostings` for early termination. Planned with task #84.
- **Multi-field ORDER BY in IBitmapQueryMatch sort path.** The current
  `SortingMatch.IBitmapQueryMatch` path handles single-field sort.
  Multi-field sorting with bitmap-based index walk for tie-breaking
  is not yet implemented.
- **Compound field sort-skip.** Detecting compound field opportunities
  via `Index.HasCompoundField()` and emitting `OrderedRangeScan` on
  the compound field's CompactTree is a gap.
- **Null-first/null-last sort.** The `IncludeNullMatch`/`IncludeNonExistingMatch`
  types are deleted but the null-positioning feature is not yet ported
  to the bitmap path.
