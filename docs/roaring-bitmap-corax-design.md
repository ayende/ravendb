# Roaring Bitmap Integration into Corax: Design Summary

## Purpose

This document summarizes findings from building a roaring bitmap implementation for RavenDB's Corax indexing engine, benchmarking it at multiple levels, and designing how it integrates into Corax's query pipeline. It is intended to guide the full implementation of Corax 2.0's bitmap-based query execution.

## 1. Roaring Bitmap Implementation

### Location
- `src/Corax/Utils/RoaringBitmaps/RoaringBitmap.cs` (~1800 lines)
- `src/Corax/Utils/RoaringBitmaps/RoaringBitmapIterator.cs` (~200 lines)

### Core Design

The bitmap uses RavenDB's `ByteStringContext` for all allocations — zero managed heap pressure. Values are split into container key (value >> 16) and low 16 bits. Container lookup is O(1) via a flat index array.

**Container types:**
- **Range**: Contiguous values 0..count-1. No allocation. Sequential `Add` from 0 is O(1). Count == 65536 means full container.
- **ArrayUnsorted**: Append-only ushort[]. `Add` is O(1). Sorted lazily on `PrepareForReading()`.
- **Array**: Sorted ushort[] for sparse data (cardinality <= 4096, up to 8KB).
- **Bitmap**: 8KB fixed bitmap (1024 ulongs) for dense data (> 4096 values).

**Key implementation details:**
- `ContainerEntry` uses explicit layout (28 bytes). Type stored in a separate parallel `NativeList<ContainerType>` for cache-friendly scanning.
- `ArrayData`/`BitmapData` properties return `Span<T>` in Debug (bounds-checked) and raw pointers in Release. `ArrayPtr`/`BitmapPtr` always return raw pointers for SIMD code.
- Smart sorted append: if the value being added is >= the last element, append directly and stay sorted. Only falls to ArrayUnsorted when insertion would break sort order.
- Bitmap radix sort in `PrepareForReading()`: explode unsorted values into 8KB scratch bitmap, extract sorted. O(n) vs O(n log n). Uses dirtyMap to skip clean chunks.
- SIMD bitmap operations via generic `IBitmapOp` interface with `AndOp`/`OrOp`/`AndNotOp` structs, dispatched per vector width (512/256/128/scalar).
- SIMD galloping for sorted array intersection via `Vector256.EqualsAny`.
- Destructive `OrWith`: steals container ownership from `other` bitmap instead of cloning. Zero-copy for unmatched containers.

### Build Characteristics

For Corax's typical input (sorted posting list IDs):
- `Add()` uses the smart sorted-append path — O(1) per value, stays as Range or sorted Array.
- `PrepareForReading()` is nearly free since arrays are already sorted.
- Build cost is dominated by `ByteStringContext` allocation for container storage.

## 2. Micro-Benchmark Results

### Roaring vs GrowableBitArray vs BCL BitArray

Tested across density (0.0005% to 50%) and range (1M to 1B):

**Where Roaring wins decisively:**
- Large value ranges (100M+): 128MB flat bit array vs 0.08-3MB roaring. Memory difference is 40-1600x.
- Sparse set operations at large range: ANDNOT at 5K/1B is 0.10ms vs 155ms BCL (1500x faster). Roaring only touches containers with data; BCL scans entire 120MB.
- Contains at large range: O(log n) binary search in small container vs random cache misses across 128MB.

**Where Roaring loses:**
- Dense small range (1M at 50%): BCL is one contiguous 125KB allocation — single SIMD sweep. Roaring has per-container overhead (type dispatch, separate 8KB buffers).
- Build at small range: ByteStringContext + container management overhead vs trivial bit-set.

**Crossover point:** Roaring wins when the flat bitmap exceeds L2/L3 cache (~1-4MB), roughly at 10M+ document ranges or when both operands produce large results.

### Profiling (dotnet-trace, 1M values / 100M range)

| Component | % of total time |
|-----------|----------------|
| `Buffer.MemmoveInternal` (memcpy from alloc/copy) | 63.5% |
| `OrWith` total | 45.9% |
| `OrContainerInPlace` | 45.1% |
| `BuildBitmap` (Add loop) | 37.2% |
| `PrepareForReading` / sort | 16.3% |
| `AddToContainer` | 15.8% |
| `IncreaseArrayCapacity` | 11.9% |
| `AddNewContainer` | 0.2% |
| AND + ANDNOT combined | 1.3% |

**Key finding**: AND and ANDNOT are already extremely efficient (1.3% combined). OR is the expensive operation (45.9%) because it's the only one that adds containers and grows buffers. The OR cost is dominated by memory allocation and copying during container merging, not by the set operation logic itself.

### OR Optimizations Attempted

1. **Destructive steal**: Transfer container ownership from right to left instead of clone+copy. Eliminates allocation + 8KB memcpy per unmatched container. Modest win.
2. **Pre-grow index/entries/types**: Single upfront allocation instead of per-container growth checks. No measurable impact (geometric growth already keeps reallocations logarithmic).
3. **Scratch bitmap for Array x Array**: Project both arrays into scratch, decide bitmap vs array based on cardinality. Mixed results — helps for medium density, hurts for sparse (overhead of bitmap projection exceeds sorted merge cost). Reverted for Array x Array; kept for other paths.
4. **Buffer reuse (Array x Bitmap)**: When converting Array to Bitmap for OR, steal right's 8KB buffer if available instead of allocating new. Clean win.

## 3. Corax Integration: Current State

### Prototype (branch: RavenDB-25284-Corax)

Added `RoaringBitmapMatch` implementing `IQueryMatch`, with `BitmapAnd`/`BitmapOr`/`BitmapAndNot` methods on `IndexSearcher`. These materialize both child matches into bitmaps via `Fill(Span<long>)`, perform set ops, and stream results via the bitmap iterator.

### End-to-End Benchmark Results (10M documents)

| Query | Streaming | Bitmap | Speedup |
|-------|-----------|--------|---------|
| Status AND Category (50% n 10%) | 128ms | 53ms | **2.4x** |
| Category AND Tag (10% n 1%) | 10ms | 11ms | 0.87x |
| Category AND Region (10% n 0.1%) | 7ms | 9ms | 0.77x |
| Cat0 OR Cat1 (10% u 10%) | 11ms | 22ms | 0.50x |
| Tag0 OR Tag1 (1% u 1%) | 0.6ms | 2.8ms | 0.21x |
| Status ANDNOT Category (50%-10%) | 257ms | 63ms | **4.1x** |
| Category ANDNOT Tag (10%-1%) | 9ms | 12ms | 0.71x |
| (Cat0 OR Cat1) AND Status | 365ms | 91ms | **4.0x** |

**Pattern**: Bitmap wins big (2-4x) when at least one operand has high selectivity (50% = 5M entries). The streaming approach must scan/merge millions of sorted longs; the bitmap approach processes ~150 containers of 8KB each. Bitmap loses for sparse queries where materialization overhead exceeds the merge cost.

### Why the Prototype is Suboptimal

The prototype materializes via `Fill(Span<long>)` then `Add()` — it goes through the streaming path first, then converts to bitmap. This double-handles every entry ID. The real implementation should populate bitmaps directly from the posting list data, bypassing `Span<long>` entirely.

## 4. Proposed Design: Single Bitmap Pipeline

### Core Concept

Instead of two separate execution strategies (streaming vs bitmap), the bitmap IS the query result that flows through the pipeline. One bitmap is created, passed by reference down the query tree, and each operator modifies it in-place.

```
// Query: Status = 'active' AND Category = 'cat_0' AND Tag = 'tag_0'
// Planner reorders: Tag (1%) first, then Category (10%), then Status (50%)

bitmap = new RoaringBitmap(ctx)
tag_posting_list.Fill(ref bitmap)           // bitmap: 100K entries
category_posting_list.AndWith(ref bitmap)   // bitmap: ~10K entries
status_posting_list.AndWith(ref bitmap)     // bitmap: ~5K entries
// iterate bitmap for results
```

### Posting List Operations on Bitmap

Each posting list operation works at the page level, using the bitmap to guide which pages to read:

#### `PostingList.Fill(ref RoaringBitmap bitmap)`
Populate bitmap from posting list. Walk B+ tree leaf pages, decode PFor-compressed blocks, add entry IDs directly to bitmap. For sequential IDs (common from batch indexing), the Range container path handles this in O(1) per value.

#### `PostingList.AndWith(ref RoaringBitmap bitmap)`
Filter bitmap to only contain entries present in this posting list. The critical optimization is **galloping by bitmap**:

```
1. Find first set bit in bitmap -> entry ID X
2. Seek posting list B+ tree to the page containing X
3. Decode that page into a scratch bitmap (8KB stackalloc)
4. AND the scratch with the corresponding container range in the accumulator
5. Find next set bit after the page's last entry -> entry ID Y
6. If Y skips pages, gallop forward in the B+ tree
7. Repeat from step 2
```

This makes AND cost proportional to the **current bitmap cardinality** (which shrinks at each step), not the posting list size. After ANDing a 100K bitmap with a 1M posting list, we read ~100K worth of posting list pages, not all 1M. And each page AND is a SIMD operation on the container, not per-entry comparison.

#### `PostingList.OrWith(ref RoaringBitmap bitmap)`
Add all entries from this posting list to the bitmap. Walk all leaf pages, decode, set bits. No deduplication needed — setting an already-set bit is a no-op. For multi-term OR (`field IN [t1, t2, ..., t50]`), each posting list's `OrWith` just sets more bits in the same bitmap. No merge, no sorting.

#### `PostingList.AndNotWith(ref RoaringBitmap bitmap)`
Remove entries present in this posting list from the bitmap. Same galloping approach as AndWith — walk bitmap set bits, seek to corresponding posting list pages, clear matching bits.

### Query Planner Integration

The query planner already reorders AND operands by estimated cardinality (smallest first). This optimization carries over directly:

```
// Before (streaming):
small.Fill(buffer)  ->  medium.AndWith(buffer)  ->  large.AndWith(buffer)

// After (bitmap):
small.Fill(ref bitmap)  ->  medium.AndWith(ref bitmap)  ->  large.AndWith(ref bitmap)
```

The bitmap shrinks at each step, so later AndWith operations process fewer entries. The B+ tree seek skips pages that don't overlap with remaining set bits — same galloping benefit as the streaming approach.

### TOP N / ORDER BY Queries

For paginated queries (`WHERE ... ORDER BY ... LIMIT 25`):

**Option A: Bitmap then iterate.** Build the bitmap for the WHERE clause, then iterate the first N results for ORDER BY. The bitmap iteration produces sorted output. For ORDER BY on a different field, iterate bitmap and score/heap like today.

**Option B: Streaming for small takes.** If `LIMIT` is small and the estimated result set is also small, the streaming path may still win. The planner chooses.

In practice, the bitmap path is never *worse* for the WHERE filtering phase. The question is only whether the overhead of bitmap construction exceeds the streaming merge savings. For `LIMIT 25` against a 50K result set, the answer is: bitmap filtering is ~1ms, streaming is ~5ms. Bitmap still wins. For `LIMIT 25` against a 100-entry result set, both are sub-microsecond and it doesn't matter.

### Memory Model

One `RoaringBitmap` per query, allocated from `ByteStringContext` (same arena as all other Corax per-query allocations). At 10M documents:
- 50% selectivity: ~150 containers, mix of Bitmap (8KB) and Range (0 bytes). ~600KB total.
- 1% selectivity: ~150 containers, mostly Array (variable). ~50KB total.
- 0.1% selectivity: ~15 containers. ~5KB total.

Compare to current streaming: `Span<long>` buffers of 8KB-64KB, re-allocated per Fill call, plus MemoizationMatchProvider buffers for AND.

### Composition

A `RoaringBitmapMatch` wrapping the result bitmap can participate as an operand in further query operations:

```
// Complex query: (A OR B) AND (C OR D) AND NOT E
bitmapAB = new RoaringBitmap(ctx)
A.OrWith(ref bitmapAB)
B.OrWith(ref bitmapAB)

bitmapCD = new RoaringBitmap(ctx)
C.OrWith(ref bitmapCD)
D.OrWith(ref bitmapCD)

bitmapAB.AndWith(ref bitmapCD)
bitmapCD.Dispose()

E.AndNotWith(ref bitmapAB)
// bitmapAB is the final result
```

The query planner builds this execution plan. Each intermediate bitmap is disposed after being consumed. For deep query trees, at most O(depth) bitmaps exist simultaneously.

## 5. Implementation Roadmap

### Phase 1: PostingList Bitmap Interface
Add `Fill(ref RoaringBitmap)`, `AndWith(ref RoaringBitmap)`, `OrWith(ref RoaringBitmap)`, `AndNotWith(ref RoaringBitmap)` to `PostingList.Iterator`. These work at the page level with scratch bitmaps.

### Phase 2: Query Match Integration
Create `RoaringBitmapMatch` that wraps a `RoaringBitmap` and implements `IQueryMatch`. The `Fill(Span<long>)` method uses the bitmap iterator. `AndWith(Span<long>, int)` filters the span against the bitmap via `Contains()`.

### Phase 3: Query Planner Decision
Add bitmap path selection to the query planner. When both operands exceed a cardinality threshold (empirically ~50K from benchmarks), route to bitmap-based execution. This can be a simple heuristic initially, refined with real workload profiling.

### Phase 4: Multi-Term Optimization
`MultiTermMatch` (`field IN [...]`) fills a single bitmap via repeated `OrWith`. This eliminates the current approach of merging N sorted spans.

### Phase 5: Galloping Page-Level AndWith
Implement the seek-by-bitmap-bit optimization for `PostingList.AndWith(ref RoaringBitmap)`. This is the key performance win for AND chains — cost proportional to result size, not operand sizes.

## 6. What We Learned

1. **AND and ANDNOT are where bitmap shines in Corax.** The streaming merge must scan both sides linearly. The bitmap AND is SIMD over containers. At 10M docs, 2-4x faster for high-selectivity queries.

2. **OR is harder to win.** The streaming OR already only touches actual entries. The bitmap OR must still touch all entries to set bits. The win comes from avoiding deduplication and merge overhead, but materialization cost offsets this for small sets.

3. **The real win is architectural, not algorithmic.** The bitmap-as-accumulator pattern eliminates intermediate buffers, sorting, deduplication, and merge operations. A single bitmap flows through the pipeline, modified in-place at each step. This is fundamentally simpler than the current streaming merge approach.

4. **Posting list page structure maps naturally to bitmap containers.** Both are keyed by 16-bit prefixes. A posting list page's entry IDs decode into a set of containers. This makes page-level AND/OR with scratch bitmaps a natural operation.

5. **The decision to use bitmap vs streaming should be based on estimated cardinality.** Below ~50K entries, streaming wins (less overhead). Above ~500K, bitmap wins (SIMD container ops scale better than linear span scanning). The 50K-500K range is a grey area where either approach works.

6. **Buffer stealing and destructive OR are important for memory efficiency.** When ORing two bitmaps, transferring container ownership instead of cloning eliminates allocation + 8KB memcpy per container. This matters for multi-term OR where dozens of bitmaps are merged.

## 7. Files and Branches

- **Roaring bitmap implementation**: Branch `RavenDB-25284`, PR #4712
  - `src/Corax/Utils/RoaringBitmaps/RoaringBitmap.cs`
  - `src/Corax/Utils/RoaringBitmaps/RoaringBitmapIterator.cs`
  - `test/FastTests/Corax/RoaringBitmapTests.cs` (30 tests)
  - `bench/Voron.Benchmark/Corax/RoaringBitmapQuickBench.cs`

- **Corax integration prototype**: Branch `RavenDB-25284-Corax`
  - `src/Corax/Querying/Matches/RoaringBitmapMatch.cs`
  - `src/Corax/Querying/IndexSearcher.BinaryMatches.cs` (BitmapAnd/Or/AndNot methods)
  - `bench/Voron.Benchmark/Corax/CoraxQueryBitmapBench.cs`

- **Baseline benchmark results**: `/tmp/corax-query-results.txt`
