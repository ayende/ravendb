# Corax 2.0 — Design Document

*Status: draft for review. This document explores the design surface for the
next major redesign of Corax's query engine. It is deliberately exploratory in
the sections that need to be: where two reasonable directions exist, both are
described and a recommendation is given. The deliverable is a sharable thesis,
not a sprint plan — implementation breakdown, owners, and ticket mapping are
explicitly out of scope.*

*Source ground truth: the v7.2 codebase  and the
`RavenDB-25284` branch (on `ayende/ravendb`). Where this
document refers to a type, file, or behaviour, that reference is from the code,
not from prior design notes. Older design notes informed the questions; the
code answers them.*

---

## 1. Executive Summary

Corax 2.0 is built on **two** pillars that share a common goal — let the
query engine express *more* with *less code*, while also being *faster* on
the queries that hurt today.

:fix: this is fluff, let's go over all of it and NOT do fluff stuff. just core tech talk

- **Pillar A — Bitmap-as-Accumulator.** Replace the streaming `Span<long>`
  result-passing protocol with a single, mutable roaring bitmap that flows
  down the query tree. Eliminates re-sorting, re-deduplication, and the
  `MemoizationMatch` working-buffer growth pattern; turns AND/ANDNOT into
  SIMD container ops. Already prototyped on branch `RavenDB-25284`; 2-4× on
  high-selectivity queries at 10M docs.

- **Pillar B — Compiled Query Execution.** Where today's engine assembles
  a generic-struct match tree dispatched through `delegate*` function
  pointers, Corax 2.0 emits a per-query function and lets the CLR JIT it.
  We choose **Roslyn-emitted C# calling pre-compiled vector primitives** as
  the primary path; the alternatives (Expression Trees, raw IL emit, custom
  interpreter) are documented in §7 with their trade-offs. Reuses the
  existing RavenDB index-compilation infrastructure
  (`IndexCompiler` + `IndexCompilationCache`).

Pillar A is independently shippable. Pillar B *uses* A's bitmap operations
when A is enabled and falls back to the existing match-tree paths
otherwise — it is not hard-blocked on A.

:fix: no, we want to ship both, not each separate part. Don't use Pilar A, Pilar B - say what this means. 

The cross-cutting effect is that the entire `*.Erasure.cs` layer (seven
files of type-erasure plumbing) becomes redundant under Pillar B, and the
function-pointer dispatch in `BinaryMatch<,,>` collapses into JIT-inlined
direct calls. The "complex object graphs … runtime type erasures and
black magic" the team has been writing tooling around go away.

---

## 2. Why Corax 2.0

### 2.1 Where Corax 1.0 wins (and what we don't break)

Corax 1.0 has *already* solved the hardest problems Lucene leaves on the
table. It is online, it has a fixed indexing cost, it stores its data
structures on disk in their final form (no per-segment merges, no cold-start
hydration), and it pays for that with a few extra GBs and computes. That
trade is paying off in unexpected ways: at the 99.99-percentile under
encrypted-at-rest, Lucene takes ~40 seconds while Corax takes ~186 ms — a
~215× gap that comes entirely from Corax touching far fewer pages, which
matters when each page touched requires decryption. Under memory pressure,
that also translates to less page faults.

These wins are *architectural*. They come from on-disk layout choices and
from the per-index engine choice that lets users adopt Corax incrementally.
They are out of scope for 2.0 to revisit, and they constrain 2.0 strongly:
**any 2.0 change must preserve the no-cold-start, no-merge-pause guarantee.
2.0 is a query-engine and sort-storage redesign, not a storage-engine
redesign.**

:fix: we also want to avoid (if possible) - changing the on disk format for corax in a way that would require re-indexing
:fix: the idea is to make this part of RavenDB 8.0 and make Corax the default indexing engine there

### 2.2 Where Corax 1.0 hurts

Two concrete pain points motivate 2.0. They come straight from the code
and the prototype benchmarks on `RavenDB-25284`.

**Pain 1: the streaming protocol is leaky.** The `IQueryMatch` interface in
`src/Corax/Querying/Matches/Meta/IQueryMatch.cs` documents the Fill contract
verbatim:

> The output of Fill will be sorted and deduplicated for the call. *Different
> calls to Fill may return identical values are not guaranteed to be sorted
> between calls.*

:fix: note that this is also an advantage - because it allows us to read N results
:fix: from the match and not have to evaluate all of it

That sentence is the source of half of the engine's intermediate complexity.
`MemoizationMatch` exists to materialise an entire match into a deduplicated
sorted span before reuse. `BinaryMatch` carries a `_fillCallCounter` to flip
its `SkipSortingResult` once Fill is called more than once, on the assumption
that subsequent batches may overlap. `MergeHelper` exists because two
already-sorted batches must be re-merged. The cost is real: the same code
paths that fix the duplicate problem also cap working memory at
`MaxMemoizationSizeInBytes = 128 MB` and bail to a degraded mode beyond that.

:fix: "The cost is real: " - again, blrub that sounds exciting, doesn't add to the tone

**Pain 2: the type-erasure machinery has metastasised.** `BinaryMatch<TInner,
TOuter, TMarker>` is a generic struct holding three `delegate*<>` function
pointers — runtime dispatch despite the generic specialisation. Seven
companion files (`BinaryMatch.Erasure.cs`, `MultiTermMatch.Erasure.cs`,
`MemoizationMatch.Erasure.cs`, `AndNotMatch.Erasure.cs`,
`MultiUnaryMatch.Erasure.cs`, `SortingMatch.Erasure.cs`,
`SortingMultiMatch.Erasure.cs`) exist to box those generic structs into a
non-generic interface so the rest of the engine can hold mixed match types
in collections. The code comment in `BinaryMatch.Fill` is candid: *"Even
though 'and' will always return a sorted array, it only sorts locally."*
Every layer is stitched against every other layer through trampolines. The
`CoraxQueryBuilder.cs` materialisation logic is a twelve-case cascade of
`if (left is CoraxBooleanItem cbi && leftOnlyOptimization.TrySetAsStreamingField(...))`.
This works, but understanding why a query plan emerged the way it did
requires reading half the codebase.

The fact that Corax already has a `StreamingOptimization` struct with
compound-field detection, sort-skip detection, and a
`CoraxBooleanItem.OptimizeCompoundField` method is evidence that *some*
2.0-flavoured planning lives in the source today — what's missing is the
*execution model* that lets these optimisations cash in.

---

## 3. Goals & Non-Goals

**Goals.**

1. Eliminate the leaky streaming contract. Query results are passed as
   bitmaps; intermediate uniqueness, sort, and dedup are properties of the
   data structure, not invariants the caller has to maintain.
2. Replace runtime type erasure with compile-time specialisation. Each
   query produces a tight, JIT-inlined function instead of a tree of
   function-pointer trampolines.
3. Preserve indexing semantics and on-disk format. No change to the
   indexing API; entry IDs stay stable; per-index engine choice stays.
   Posting lists, compact trees, term containers, lookup trees, and the
   sort-side `TermsReader` infrastructure all stay. Corax 2.0 is
   compute-only.
4. Be incrementally rolloutable. Per-query feature flag for Pillar A;
   per-database flag for Pillar B with a budgeted per-query opt-out.

:fix: these both go together, but also need to evaluate how we keep both the current code
:fix: and the new one and keep it in good shape. I _like_ the ability to feature toggle it
:fix: question is if this will cause us pain afterward?

**Non-Goals.**

- Replacing Voron or revisiting the storage engine.
- Replacing the analyzer / pipeline layer (the `Pipeline/` and `Analyzers/`
  directories are stable and the Lucene-analyzer adapter stays).
- Multi-machine query execution / sharding-aware planning. The shard
  coordinator already wraps Corax; that boundary is unchanged.
- A new query language. RQL and the AST in `Raven.Server.Documents.Queries`
  stay; only the AST → execution-plan compiler changes.
- Lucene parity for full-text features beyond what's already in the
  Phrase / Search infrastructure. Vector and spatial stay separate paths.
- A monolithic rewrite. Each pillar ships behind its own gate.

:fix: the last one is wrong

---

## 4. Corax 1.0 in 5 Pages

This section is a *grounded* tour of the engine as it exists today, named
files and types only. It is the baseline §5 changes will be expressed as
deltas against.

### 4.1 Storage primitives (Voron substrate)

Corax sits on Voron primitives. The relevant ones:

- `Voron.Data.PostingLists.PostingList` (`src/Voron/Data/PostingLists/`,
  ~781 LoC for the main file, plus the leaf/branch page files for ~1.5K LoC
  total) is a B+ tree of entry IDs with PFor-compressed leaf pages.
  Posting lists come in three cardinality tiers (Single / Small / Large)
  decided at the term level: a single-entry term is encoded inline in the
  CompactTree value, a small-set term is a tiny dedicated structure, a
  large-set term is a full `PostingList` tree. The query path mostly cares
  about the `PostingList.Iterator` for large posting lists.
- `Voron.Data.Containers.Container` (~1536 LoC) is Corax's existing dense
  byte-packed primitive — *unrelated* to the roaring bitmap container of
  the same conceptual name. The current entry blobs live in containers.
- `Voron.Data.CompactTrees.CompactTree` is the term storage with HOPE
  (High-speed Order-Preserving) encoding. Each field gets its own
  CompactTree mapping `term → postingListReference`. HOPE means search is
  performed on encoded keys without decompression, and the dictionary
  adapts as more terms are inserted.
- `Voron.Data.Lookups.Lookup<TKey>` is a fixed-size B+ tree, used by
  `IndexSearcher._entryIdToLocation` to map an entry ID to its blob
  location, among other things. It's the right shape for "cheap, dense,
  numeric → numeric" lookups.

### 4.2 The `IQueryMatch` streaming contract

`src/Corax/Querying/Matches/Meta/IQueryMatch.cs`:

```csharp
public interface IQueryMatch
{
    long Count { get; }
    SkipSortingResult AttemptToSkipSorting();
    QueryCountConfidence Confidence { get; }
    bool IsBoosting { get; }

    // Guarantees: The output of Fill will be sorted and deduplicated for the call.
    //             Different calls to Fill may return identical values are not guaranteed to be sorted between calls.
    //             0 return means no more matches. 
    int Fill(Span<long> matches);

    // Guarantees: AndWith accepts sorted and returns sorted.
    //             May optimize for continued sorted.
    //             0 return means no more matches from the provided span, and may need to go to the next batch
    // Requirements: Cannot be called with .Fill() from same instance.
    int AndWith(Span<long> buffer, int matches);

    void Score(Span<long> matches, Span<float> scores, float boostFactor);
    QueryInspectionNode Inspect();
    DuplicatesOccurrence DuplicatesOccurrenceStatus { get; }
}
```

The interface is small but the contract is heavy. Three pieces of incidental
complexity follow from it: (a) `MemoizationMatch` to materialise into a
sorted-deduped span when reuse is needed, (b) `MergeHelper` to merge two
sorted spans, (c) `SortingMatch` family to wrap any match with a heap sorter
when an `ORDER BY` is present.

### 4.3 The match-type zoo and its erasure layer

`src/Corax/Querying/Matches/`:

- Flat: `AllEntriesMatch`, `AndNotMatch`, `BinaryMatch` (with `.Boolean.cs`
  variants), `BoostingMatch`, `DeduplicationMatch`, `IncludeNonExistingMatch`,
  `IncludeNullMatch`, `MemoizationMatch`, `MultiTermMatch`, `MultiUnaryMatch`,
  `MultiVectorSearchMatch`, `PharseMatch.cs` (typo — `Phrase`), `TermMatch`,
  `VectorSearchMatch`.
- `SortingMatches/`: `SortingMatch` and `SortingMultiMatch` (each split into
  `.Comparers`, `.BasicsComparers`, `.Erasure`, etc.), the heap sorters
  (`NumericalMaxHeapSorter`, `TextualMaxHeapSorter`, `HeapSorterBuilder`),
  `CompactKeyComparer`, `SortingHelpers`.
- `SpatialMatch/SpatialMatch.cs`.
- `TermProviders/`: `Contains`, `EndsWith`, `Exists`, `In`, `NotContains`,
  `NotEndsWith`, `NotStartsWith`, `NumericRange`, `Regex`, `StartWith`,
  `TermRange`. These feed `MultiTermMatch`. The pattern — separate
  *iterators that produce terms* from *the match that consumes them* — is
  the right pattern; we reuse it for bitmap feeders in §6.
- `Meta/`: the interfaces (`IQueryMatch`, `IBoostingMarker`,
  `IMemoizationMatchSource`, `ITermProvider`), the markers
  (`BinaryMatchOperationMarker`, `RangeMarker`, `UnaryMatchOperation`), and
  the helpers (`MergeHelper`, `SortHelper`, `QueryInspectionNode`).

The erasure layer. `BinaryMatch<TInner, TOuter, TBinaryOperationMarker>`
holds three `delegate*<ref BinaryMatch<...>, Span<long>, int>` function
pointers — `_fillFunc`, `_andWithFunc`, `_inspectFunc`. The companion
`*.Erasure.cs` files are non-generic shells that box specialisations of the
generic struct. There are seven of them. The cost is one indirect call per
operation, plus the box; the benefit is that mixed match types live in a
non-generic collection. With Pillar B, both go away — emit a per-query
function and the indirection is a direct call the JIT inlines.

### 4.4 The query builder layer

`src/Raven.Server/Documents/Indexes/Persistence/Corax/CoraxQueryBuilder.cs`
is the AST → match-tree compiler. It already does a non-trivial amount of
optimisation:

- `StreamingOptimization` struct (in `CoraxQueryBuilder.cs`) detects when
  the order-by field aligns with a where-field of the same type and skips
  the heap-sort stage by issuing an ordered range scan. It handles the
  compound-field case via `Index.HasCompoundField(...)` and the
  multi-term-match streaming case via `TrySetMultiTermMatchAsStreamingField`.
- A `CoraxBooleanItem` / `CoraxBooleanQueryBase` materialisation
  cascade handles AST-level pattern detection during plan construction.

What's *not* there: a cardinality-driven decision between bitmap and
streaming execution; a between-detection optimiser that fuses
`x > a AND x < b`; and any code-emitting layer.

### 4.5 The bench infrastructure

`bench/Voron.Benchmark/Corax/` is the benchmark home; the `RavenDB-25284`
branch adds `RoaringBitmapQuickBench.cs` (micro-bench) and
`CoraxQueryBitmapBench.cs` (end-to-end). The 10M-document end-to-end
results — 2-4× on AND/ANDNOT high-selectivity, slight regressions on small
OR — set the performance baseline for §6.

### 4.6 The sort-by-non-WHERE-field path

`SortingMatch.SortUsingIndex<TEntryComparer, TDirection>`
(`src/Corax/Querying/Matches/SortingMatches/SortingMatch.cs:469`) is the
production code path for `WHERE x = a ORDER BY y` queries when the
candidate set exceeds the `IndexSortingThreshold` (4096 rows, line 203).
The infrastructure it builds on:

- **Term storage in dedicated containers.** Sort-eligible terms live
  outside the per-field CompactTree in their own containers, addressable
  by stable container IDs.
- **`TermsReader`** (`src/Corax/Querying/TermsReader.cs`) — provides
  `entryId → first-term` lookups per field at constant cost, fed by the
  per-field `_entriesToTermsTree` (a `Lookup<Int64LookupKey>` on
  `IndexSearcher.cs:102, 479`).
- **`SortedIndexReader<TDirection>`** (`SortingMatch.cs:260`) — walks
  the sort-field index forward or backward (per `ILookupIterator`),
  emitting entry IDs in field-sort order. The query engine intersects
  this stream with the candidate set rather than reading per-entry
  blobs.
- **Bit-twiddling encoding for direction** — the top and bottom halves
  of 64-bit numbers encode different sort orders, so the system can
  sort once and remember previous orderings.

Sort-time cost is bounded by result-set size, not entry-blob locality.
Corax 2.0 builds on this substrate; §6.5 covers how the bitmap pipeline
composes with `SortUsingIndex`.

### 4.7 A note on existing Roslyn infrastructure

`src/Raven.Server/Documents/Indexes/Static/IndexCompiler.cs`,
`IndexCompilationCache.cs`, and the rewriter set under
`src/Raven.Server/Documents/Indexes/Static/Roslyn/Rewriters/` (~20 files)
together form a mature dynamic-compile pipeline that takes a user's index
definition (an arbitrary C# expression) and produces a compiled assembly
that's loaded into the running server. **This is the hidden gift for
Pillar B.** The compiled-query path can re-use the same Roslyn rig — the
problem of "compile a chunk of generated C# to an assembly and run it" is
already solved here.

---

## 5. The Two Pillars (Map)

```
              ┌────────────────────────┐
              │  Pillar B: Compiled    │
              │  Query Execution       │  ← uses A when present;
              │  (Roslyn → JIT)        │     emits match-tree calls otherwise
              └────────┬───────────────┘
                       │ uses
                       ▼
              ┌────────────────────────┐
              │  Pillar A: Bitmap-as-  │
              │  Accumulator           │
              │  (compute only)        │
              └────────┬───────────────┘
                       │
                       ▼
   ┌──────────────────────────────────────────────────┐
   │       Corax 1.0 storage + sort substrate         │
   │  PostingList, CompactTree, Container,            │
   │  _entriesToTerms / TermsReader / SortUsingIndex  │
   │  IndexCompiler / IndexCompilationCache (Roslyn)  │
   └──────────────────────────────────────────────────┘
```

Pillar A is independently shippable. Pillar B *can* ship without A —
without bitmap operations available, the compiler emits calls into the
existing match-tree primitives (`TermMatch.Fill`, `BinaryMatch.AndWith`,
etc.) rather than into `Primitives.FillFromPostings` /
`Primitives.AndWith`. Sort/projection always uses the existing
`SortUsingIndex` path — neither pillar redesigns it. The rest of the
document is one section per pillar plus cross-cutting concerns.

:fix: no, we want to use both

---

## 6. Pillar A — Bitmap-as-Accumulator

### 6.1 The roaring bitmap (verified against
`\Work\ravendb\RavenDB-25284\src\Corax\Utils\RoaringBitmaps\RoaringBitmap.cs`)

The bitmap is `public unsafe struct RoaringBitmap : IDisposable`, allocated
through `ByteStringContext`, with no managed-heap pressure. It is **not** a
`ref struct` — its lifetime discipline (one bitmap per query branch, disposed
when the wrapping match dies, never escaping the transaction's
`ByteStringContext`) is enforced by convention, not by the type system. This
matters: the bitmap *can* be stored in a field, captured by a lambda, or held
across a yield. The codegen and match-type layers must be careful not to
accidentally extend its lifetime past the transaction. See §6.7 for the
discipline applied to `RoaringBitmapMatch`.

:fix: *should* it be a ref struct? if so, what complicated does it cause us?


Values split via `key = value >> 16` into a 48-bit container key and a 16-bit
within-container offset. Container lookup is O(1) via a flat index
(`_index: NativeList<int>`) sized to `maxKey + 1`. The flat index relies on
entry IDs being dense from 0; at 10M entries the index is ~152 KB, at 1B
~15 MB, both fine; the design assumes RavenDB does not produce entry IDs
above ~2³² in a single index, which holds today and is part of the contract
the indexer maintains.

:fix: what does ravendb does not produce > 4B in an index? that is NOT correct.

Four container types, chosen by data shape:

- **`Range`** — contiguous values `0..count-1`, no allocation. Sequential
  Add from 0 is O(1). Set when `count == 65536`.

:fix: nothing special happens when count == 65536? it just means that it is full.

- **`ArrayUnsorted`** — append-only `ushort[]`. Add is O(1). Sorted lazily
  on first read via `PrepareForReading()`. The key trick: if the appended
  value is `>= last`, append directly and stay sorted; only fall to
  unsorted when an out-of-order insertion is detected. In practice, with
  posting-list-ordered input, this never trips.
- **`Array`** — sorted `ushort[]`, used for sparse data
  (cardinality ≤ 4096, up to 8 KB).
- **`Bitmap`** — fixed 8 KB (1024 ulongs) for dense data (> 4096 values).

The `RoaringBitmap` struct stores three parallel arrays:
`_entries: NativeList<ContainerEntry>`, `_types: NativeList<ContainerType>`,
`_index: NativeList<int>` (the key → entry-slot lookup). Tombstones are
managed via a free list (`_freeListHead`, dead entry's `Cardinality` field
overloads as the next-free index, `-2 = end`). Memory layout is
struct-of-arrays for cache-friendly scanning.

Set operations are SIMD over containers via a generic `IBitmapOp` interface
with `AndOp`/`OrOp`/`AndNotOp` structs, dispatched per vector width
(512/256/128/scalar). Sorted-array intersection uses Vector256 galloping.
Destructive `OrWith` *steals* container ownership from the right-hand
operand (zero-copy for unmatched containers).

The lifecycle contract: one bitmap per query branch, allocated from
`IndexSearcher.Allocator: ByteStringContext` (which returns
`_transaction.Allocator`), disposed when the wrapping match's lifetime ends.
Because the bitmap is *not* a ref struct (see §6.1), each match type that
holds one is responsible for explicit `Dispose()` discipline — the same way
`MemoizationMatch` already manages its own buffer lifecycle. The `IDisposable`
implementation on `RoaringBitmap` does the actual freeing back into the
`ByteStringContext`.

:fix: should we change this?

### 6.2 PostingList × bitmap operations

Pillar A's payoff comes when posting lists work *natively* in bitmap space.
Four operations on `PostingList.Iterator`:

- **`Fill(ref RoaringBitmap)`.** Walk leaf pages, decode PFor blocks, set
  the bits. Sequential output → the `Range` container path applies, so
  the cost is roughly one `memcpy` per leaf page.

- **`AndWith(ref RoaringBitmap)`.** Filter the bitmap by entries in this
  posting list. The galloping-by-bitmap algorithm:

  1. Find the first set bit in the bitmap → entry ID `X`.
  2. Seek the posting list B+ tree to the page containing `X`.
  3. Decode that page into a stack-allocated 8 KB scratch bitmap.
  4. AND the scratch with the corresponding container range in the
     accumulator.
  5. Find the next set bit in the bitmap *after* that page's last entry
     → entry ID `Y`.
  6. If `Y` skips pages, gallop forward in the B+ tree.
  7. Repeat from step 2.

:fix: would it make sense to create a temp roaring bitmap for each leaf page? 
:fix: we can reuse that between calls, I think?

  Cost is proportional to the *current bitmap cardinality*, which shrinks
  at each step. After ANDing a 100K bitmap with a 1M posting list, we
  read ~100K's worth of posting-list pages, not all 1M. Each page-AND is
  a SIMD op on 8 KB.

- **`OrWith(ref RoaringBitmap)`.** Walk all leaf pages, set bits. No dedup
  needed (idempotent). For multi-term OR (`field IN [t1, ..., t50]`),
  each `OrWith` just sets more bits in the same bitmap. No merge, no
  sort.

- **`AndNotWith(ref RoaringBitmap)`.** Same galloping shape as `AndWith`
  but clears bits.

These live in a new partial extension —
`src/Voron/Data/PostingLists/PostingList.Bitmap.cs` — to keep the
1269-LoC main file tidy. Scratch bitmap is `stackalloc` 8 KB
(`SkipLocalsInit` pattern already used elsewhere on the bitmap branch).

The current prototype (branch `RavenDB-25284-Corax`) goes through
`Fill(Span<long>)` then `Add()` on the bitmap — i.e. it streams first and
materialises after. That's the prototype-grade integration; the design here
is the production form, where the bitmap is fed *directly* from the page
data without ever going through a `Span<long>`.

### 6.3 The single-bitmap pipeline

A query like `Status='active' AND Category='cat_0' AND Tag='tag_0'`,
reordered by the planner to `Tag` (1%) → `Category` (10%) → `Status` (50%),
runs as:

```
bitmap = new RoaringBitmap(ctx)
tag_postings.Fill(ref bitmap)            // ~100K bits set
category_postings.AndWith(ref bitmap)    // ~10K bits remain
status_postings.AndWith(ref bitmap)      // ~5K bits remain
// iterate bitmap → results
```

For complex shapes — `(A OR B) AND (C OR D) AND NOT E` — at most O(depth)
bitmaps exist simultaneously. Intermediates dispose as their consumer
finishes:

```
bitmapAB = new RoaringBitmap(ctx)
A_postings.OrWith(ref bitmapAB)
B_postings.OrWith(ref bitmapAB)

bitmapCD = new RoaringBitmap(ctx)
C_postings.OrWith(ref bitmapCD)
D_postings.OrWith(ref bitmapCD)

bitmapAB.AndWith(ref bitmapCD)
bitmapCD.Dispose()

E_postings.AndNotWith(ref bitmapAB)
// bitmapAB is the final result
```

Memory footprint at 10M entries: 50% selectivity → ~150 containers, mostly
8-KB Bitmap form, ~600 KB total; 1% selectivity → ~50 KB; 0.1% → ~5 KB.

### 6.4 Selector: when does the bitmap path engage?

The prototype benchmarks (10M-doc workload) show a clean cardinality
threshold: bitmap wins above ~50 K operand cardinality, streaming wins
below ~10 K, mixed in between. The selector decision needs to be made by
`CoraxQueryBuilder` based on cardinality estimates available from
`PostingList` metadata (term frequency) and from the `Confidence` field
already on `IQueryMatch`.

The shape of the selector is a single addition to
`CoraxQueryBuilder.Parameters.cs`:

```
Parameters.UseBitmapPath: bool   // configurable; default = auto
Parameters.BitmapCardinalityFloor: long  // empirically ~50_000
```

For the auto path, the rule is approximately *"if the smaller AND operand
is above the floor, or any OR operand is above the floor, use bitmap; else
streaming"*. The floor is empirical and per-database tunable. The selector
runs at planner time, not per-Fill batch, so it pays for itself trivially.

:fix: that means that we need to consider query plan caching, same query, different params, different plan

There are *two* tunables, not one: `BitmapCardinalityFloor` for AND
operands (default ~50K) and `BitmapOrFloor` for OR operands (default
~500K — higher because OR's materialisation cost offsets bitmap savings
on small sets). Both live in `Parameters.cs` and are per-database
overridable. The §9 experiment list calibrates both.

:fix: look at the other ways in which Corax can modify the query plan, let's centralize all that logic too.

Cardinality estimates come from `PostingList.NumberOfEntries` for terms
already on disk (exact). For derived operands — an OR of three terms, an
intersection of two ranges — the planner estimates: sum for OR, min for AND,
clamped to the index size. Confidence is `QueryCountConfidence.High` for
single posting-list reads, `Normal` for derived operands. The selector
treats `Low` confidence as "use streaming" by default, since the streaming
path's worst case is gentler than the bitmap path's worst case
(materialising a much-smaller-than-expected result still wastes the
materialisation cost).

### 6.5 Sort, score, LIMIT under a bitmap

Bitmap iteration produces entry IDs in ascending order. For
`ORDER BY entryId` (rare but possible), or for sort-skip when the planner
already arranged entries by sort field via a streaming-sort path
(`StreamingOptimization`), the bitmap *is* the sort. No separate sort step.

:fix: how would this work? where Age > 10 order by Age using bitmap works how?
:fix: give an example please.

For `ORDER BY <other-field>` above the `IndexSortingThreshold` (4096
rows), the bitmap acts as the candidate set fed into the existing
`SortingMatch.SortUsingIndex<TEntryComparer, TDirection>` machinery
(see §4.6). The flow:

1. The bitmap materialises the WHERE clause via the §6.2/6.7 path.
2. `SortUsingIndex` walks the *sort-field index* in order, via
   `SortedIndexReader<TDirection>`, emitting candidate entry IDs in
   sort-field order.
3. Each candidate is intersected with the bitmap (one `Contains` call —
   exactly the path SIMD Quad in §6.8 targets).

:fix: would it make sense to create a temp bitmap here & intersect? or do we need to do each individually? 

4. Matches accumulate until `LIMIT` is satisfied, with the
   `forceUsingOnlyIndex` / "scanned twice the result size without
   filling page" fallback to brute sort that already exists in the code
   (`SortingMatch.cs:490-505`).

Below the threshold, today's `SortResults<TEntryComparer>` brute heap
sort runs over the bitmap iterator output. Same code as today; the
bitmap just supplies a sorted entry-ID stream instead of a possibly-
unsorted streaming-Fill output.

The bitmap pipeline plugs into `SortUsingIndex` at the candidate-set
boundary; it does not change the sort algorithm itself.

For scoring (`BM25`-style boosting), the existing `BoostingMatch` and the
`Score(Span<long>, Span<float>, float)` method on `IQueryMatch` carry over.
Under a bitmap, scoring is computed in batches over the bitmap iterator.
The bridge match in §6.7 implements `Score` over its iterator output so
boosting composes.

For `LIMIT N` with no `ORDER BY`, iterate the bitmap until `N` results are
gathered. With `ORDER BY entryId DESC` and `LIMIT N`, iterate from the
last container backward — the bitmap structure supports reverse iteration
trivially.

### 6.6 Composition with vector / spatial / scoring

Vector and spatial matches today produce *scored* candidate sets, not pure
boolean memberships. They sit naturally as *bitmap consumers*:

- A vector match produces, for a candidate set, a `(entryId, score)`
  stream. The candidate set is the bitmap of the WHERE clause. The vector
  match wraps the bitmap and uses the iterator to feed the
  similarity-search loop. This composes cleanly: complex queries like
  `WHERE x = a AND vector.search(...)` produce a bitmap from `x = a`
  and pass it to the vector match for re-ranking.

:fix: No, the problem is that when we do vector.search, as a _side-effect_, we generate the score / distance
:fix: we want to avoid paying for that again when we do the final result, no? And for full text search, not sure it is easily to recompute the score at all.
:fix: do we want to retain the score somewhere (which I think we do now), or recompute in the end - for sorting on the score, we need to run over a lot of them potentially
:fix: we should probably account for that once, not repeatedly

- A spatial match (`SpatialMatch`) is similar — geo predicates filter to
  a bitmap, then expensive geometric tests run over the bitmap iterator.

In both cases the `RoaringBitmapMatch` wrapper (§6.7) is the integration
seam. No invasive change to vector/spatial code.

### 6.7 Match-type plumbing: bridge first, fold later

Pillar A is shippable *behind* the existing match interface. Phase one
adds a single new match:

```csharp
internal struct RoaringBitmapMatch : IQueryMatch
{
    private RoaringBitmap _bitmap;
    private RoaringBitmapIterator _iterator;

    public int Fill(Span<long> matches) => _iterator.Fill(matches);
    public int AndWith(Span<long> buffer, int matches) =>
        _iterator.FilterByContains(buffer, matches);
    // Score, Inspect, etc. as today.
}
```

Plus `IndexSearcher.BinaryMatches.cs` factory methods (`BitmapAnd`,
`BitmapOr`, `BitmapAndNot`) that return a `RoaringBitmapMatch` over the
result.

This makes the bitmap path *one* member of the existing match-type zoo;
the existing `BinaryMatch`, `MultiTermMatch`, etc. continue to work
unchanged. The selector (§6.4) decides at plan time which path to take.

A small `BitmapFeeders/` directory mirrors `TermProviders/` — one feeder
per term-source pattern (term, multi-term, range, between). Each feeder
knows how to populate a `RoaringBitmap` from posting lists; the consumer
is `RoaringBitmapMatch`.

Phase two — once Pillar B lands and removes the erasure layer — folds the
parallel paths. `RoaringBitmapMatch` becomes the canonical implementation;
`MultiTermMatch`, `BinaryMatch`, and friends become specialisations
emitted by the JIT. But that is Pillar B's problem; Pillar A ships with
the parallel-path arrangement.

:fix: given that we do this at once, give me the final design for how this will look like.

### 6.8 SIMD Quad for Array-container `Contains`

An optimisation that should land alongside Pillar A's stabilisation, but
that is *not* on the bitmap branch today. The current
`ArrayContainerFind` (lines 1159-1178 of `RoaringBitmap.cs`) is plain
scalar binary search:

:fix: pretty sure that we already implemented it now.

```csharp
internal static int ArrayContainerFind(ushort* arr, int count, ushort value)
{
    int lo = 0;
    int hi = count - 1;
    while (lo <= hi) {
        int mid = lo + ((hi - lo) >> 1);
        ushort midVal = arr[mid];
        if (midVal == value) return mid;
        if (midVal < value) lo = mid + 1;
        else hi = mid - 1;
    }
    return ~lo;
}
```

This runs on every `Contains(entryId)` against an Array-typed container
(cardinality 1-4096). It is hot under two regimes:

1. The `RoaringBitmapMatch.AndWith(Span<long> buffer, int matches)` bridge
   path (§6.7), which filters a streaming span against a bitmap by
   per-value `Contains`.
2. Posting-list-fed `AndWith(ref RoaringBitmap)` page operations against
   bitmaps whose containers are still in Array form (sparse case).

Daniel Lemire (the Roaring Bitmap co-creator) recently published a
**SIMD Quad** algorithm that consistently beats scalar binary search on
sorted 16-bit arrays. The technique:

- Divide the array into fixed 16-element blocks (the trailing partial
  block falls back to linear scan).
- Use the *last* element of each block as an interpolation key.
- Perform a *quaternary* (base-4) interpolation search over block boundaries
  — compute three quarter-points, three scalar comparisons, advance the base
  by `(c1 + c2 + c3) * quarter` (zero to three quarter-jumps in one step).
- Once narrowed to a single 16-element block, load the block into two
  Vector128/NEON registers and SIMD-compare all 16 elements to the needle
  in parallel.

Lemire reports >2× over `std::binary_search`:

- Intel/GCC, warm cache: 2× faster across all sizes ≥ 64.
- Apple/LLVM, cold cache: 2× faster.
- Both platforms: never *slower* than binary search.

The "quad" (vs plain "SIMD-bin": SIMD final block + binary block-search)
helps mostly on Intel for large cold-cache arrays, where memory-level
parallelism is exploited by issuing three independent loads
(`carr[(base+q+1)*16-1]`, `carr[(base+2q+1)*16-1]`, `carr[(base+3q+1)*16-1]`)
that the out-of-order core can overlap. On Apple/LLVM the quad part is
near-neutral; the SIMD-final-block is the dominant win.

For Corax: replace `ArrayContainerFind` with a SIMD Quad implementation,
keyed off the existing `IsAccelerated` check on `IndexSearcher` (which
already gates on `AdvInstructionSet.IsAcceleratedVector256`). Fall back
to scalar binary search on platforms without 16-element SIMD compare. The
expected payoff is largest in the two regimes named above.

Lemire's reference implementation is in C++ (NEON / SSE2). The C# port
uses *two* `Vector128<ushort>` compares for the 16-element block
(`Sse2.CompareEqual` / `AdvSimd.CompareEqual` — each instruction handles
8 lanes, not 16), `Sse2.MoveMask` / `AdvSimd.Arm64.MaxAcross` for hit
detection. On AVX2, a single `Vector256<ushort>` compare handles all 16
lanes in one instruction. LoC for the C# port plus tests is *probably
~150*; we'll size it concretely once written.

**Caveats worth stating in the design.**

- *The 2× claim is primitive-level, not query-level.* Lemire's number is
  for `Contains` calls landing on Array-typed containers. Bitmap-typed
  containers (>4096 entries; dense data) use `BitmapContainerContains`,
  which is a single bit test — SIMD Quad doesn't apply there. The
  query-level speedup is bounded by the fraction of `Contains` calls
  that hit Array containers, which depends on workload sparsity. We
  expect substantial wins on sparse fields (typical user-facing
  dimensions: status, tag, country) and minimal on dense fields (e.g.
  any `Public = true` style boolean).
- *Lemire's bench uses random needles.* Corax's bridge path
  (`RoaringBitmapMatch.AndWith(Span<long> buffer, int matches)`) feeds
  needles in *ascending order* — the buffer is a sorted `Span<long>`.
  In that regime, simple sequential cursor advancement (galloping) may
  already be competitive, and SIMD Quad's interpolation jump may
  thrash. The microbench in question N.12 must compare both regimes
  before scheduling; if galloping wins for ascending inputs, SIMD Quad
  applies only to truly-random `Contains` (e.g. point lookups, not
  bulk filtering).
- *Crossover.* Lemire's data shows SIMD Quad wins clearly for arrays
  ≥ 64 elements; below 64 it's roughly neutral with binary search and
  pure linear/SIMD-final-block is enough. The implementation should
  early-out with linear-scan-or-SIMD-only for small arrays and only
  engage the quaternary path above the crossover.

:fix: we implemented direct SIMD linear scan for this. check the resutsl.

This is *Pillar A polish*, not Pillar A core. It does not change the
architecture; it makes the existing architecture faster on a specific
hot inner loop. Worth scheduling in the same release as Pillar A so the
bitmap selector heuristic (§6.4) is calibrated on the *optimised*
`Contains`, not the scalar baseline — but only after the bridge-path
microbench (N.12) confirms SIMD Quad is the right tool for ascending-
needle inputs.

---


## 7. Pillar B — Compiled Query Execution

This is the largest pillar and the one with the widest design space. We
spend more time here because the trade-offs are real.

### 7.1 The case for compilation

Today's match tree is a generic-struct sandwich with `delegate*` function
pointers. In a query like `where x = a and y > b and z in (c, d, e)`, the
JIT sees:

- `BinaryMatch<TInner=BinaryMatch<TermMatch, MultiUnaryMatch, AndMarker>,
  TOuter=MultiTermMatch<InProvider>, AndMarker>.Fill(buffer)`
- which calls `_fillFunc(ref this, buffer)` — an indirect call through a
  function pointer to a static function chosen at construction.

The CLR JIT *can* devirtualise function pointers in some cases but not
reliably across the full match-tree depth. In practice, every `Fill`
boundary is an indirect call. Within a leaf match, the JIT does its job;
across boundaries, it doesn't.

If we instead emit, for the same query, a single function (using the
§7.7 vocabulary):

```csharp
int ExecuteQuery_42(in QueryContext ctx, Span<long> output, int toSkip)
{
    using var bitmap = new RoaringBitmap(ctx.Allocator);
    Primitives.FillFromPostings(ctx.GetPostings("x", ctx.PA), ref bitmap);

    Primitives.FillFromInequality(
        ctx.GetCompactTree("y"), ComparisonMode.GreaterThan, ctx.PB, ref bitmap);

    using var scratch = new RoaringBitmap(ctx.Allocator);
    foreach (var zTerm in ctx.PCList)
        Primitives.OrWithPostings(ctx.GetPostings("z", zTerm), ref scratch);
    Primitives.AndWith(ref bitmap, ref scratch);

    return Primitives.IterateInto(ref bitmap, output, ref toSkip);
}
```

— and the CLR JIT inlines `Fill`, `AndWith`, `OrWith`, etc., we get one
contiguous function with no indirect calls and the JIT's full reach. The
seven `*.Erasure.cs` files plus the `delegate*` machinery vanish. The
shape of the function is determined by the query plan; the *content* is
calls into pre-compiled Corax primitives. We're not generating SIMD
intrinsics from scratch — we're *orchestrating* pre-built ones.

### 7.2 The design space — five options

There are five paths to a per-query compiled function. They sit on two
axes: *interpretation vs JIT* and *what kind of source we author*.

**Option A — Operational primitives interpreter (no codegen).** The
compiler emits a tagged-union `Op[]` array; an interpreter loop
dispatches on op type. Pro: deterministic, no warm-up, AOT-friendly.
Con: an interpreter loop *is* the indirection we're trying to eliminate.
Defeats the purpose.

**Option B — Roslyn dynamic compile (emit C# source, CSC, load assembly).**
Build a C# `string` (or `SyntaxTree`) for the query function, hand it to
Roslyn, get back an assembly, load it, invoke. Pro: full C# expressiveness
— `Span<T>`, `stackalloc`, `ref struct`, SIMD intrinsics, generics, the
works. Pro: RavenDB *already does this for index definitions* via
`IndexCompiler`. Pro: emitted code is debuggable; you can write it to disk
for a query under investigation. Con: ~50–200 ms compile time for cold
queries; needs a query-plan cache (which Corax already has the shape of).
Con: emitted assemblies use memory; need eviction. Con: AOT-incompatible.

**Option C — Expression Trees + `Lambda.Compile()`.** Build a
`System.Linq.Expressions` tree, compile to a delegate, invoke. Pro:
in-tree, no compiler dependency, JIT-compiled by .NET runtime. Con:
Expression Trees cannot express `stackalloc`, `Span<T>` literals,
pointer arithmetic, `ref struct` locals, or `fixed` blocks.
SIMD intrinsics work only via `MethodCallExpression` — every
`Vector256.Add` is a `Call`. Con: introduced 2010-era;
`ref` local support is partial; ref-struct-as-parameter has historically
been awkward. Con: `Lambda.Compile()` produces a `DynamicMethod` whose
JIT-tier optimisations are subtly different from Roslyn-emitted code.
Verdict: too narrow for Corax's needs.

**Option D — Direct IL emit via `DynamicMethod` / `Reflection.Emit`.** Hand-
write CIL into a `DynamicMethod`. Pro: same JIT performance as Roslyn
output. Pro: full IL — `localloc` for stackalloc, `ref struct`, generics,
pointer ops. Pro: in-tree; no Roslyn dependency for the runtime path.
Con: hand-written IL is brutal to maintain; needs an IL-builder
abstraction; debugging is hard (no source). Con: every change to a
primitive's calling convention requires changing IL emit.

**Option E — Hybrid B+D.** Roslyn for the macro shape (the function
body that orchestrates primitive calls); IL emit for per-query
specialisations of inner loops. Possible, but probably overkill.

:fix: the ability to debug / profile / etc queries is imporant - address how that would work in the selected option.
:fix: first time query cost is _meh_ - we can pay for that, but not too much. Would it make sense to have tiered approach? Build the query plan as a code string (for debug), and an ops array for interpreter?
:fix: then when we have enough queries, we'll start async code compilation, then switch the interpeter to the actual code when we have it?
:fix: that may need some perf testing to see how that actually works. 

### 7.3 Why "code-gen + CLR JIT" is the right family

Options B, C, D all hand work to the CLR JIT. Option A doesn't — and
that's what makes it the wrong choice for Corax. The whole point of the
exercise is to get the JIT's reach through the query plan; an interpreter
puts the indirection back.

The user-stated lean — *code-gen then JIT using the CLR* — is correct.
Of the three CLR-JIT options (B, C, D), the trade-off is *expressiveness
vs implementation cost*.

:fix: user-stated lean  - let's avoid stuff like that. be neutral.

### 7.4 C# source vs Expression Trees vs IL emit

Detailed comparison for Corax's specific needs.

| Feature                       | B (Roslyn C#) | C (Expression Trees) | D (IL Emit) |
|-------------------------------|---------------|----------------------|-------------|
| `Span<T>` locals              | ✓             | partial              | ✓           |
| `stackalloc`                  | ✓             | ✗                    | ✓ `localloc`|
| `ref struct` locals           | ✓             | partial              | ✓           |
| `Vector256<T>` arithmetic     | ✓ idiomatic   | as `Call(...)`       | ✓ as opcodes|
| Calling pre-compiled methods  | ✓             | ✓                    | ✓           |
| Generic specialisation        | ✓             | partial              | ✓           |
| `unsafe`/`fixed`              | ✓             | ✗                    | ✓           |
| Cold-start latency            | ~50-200 ms    | ~5-20 ms             | ~1-5 ms     |
| Maintenance cost              | low           | medium               | high        |
| Debuggability of generated    | high (source) | medium (tree)        | low (IL)    |
| RavenDB infra reuse           | yes           | none                 | none        |
| AOT compatibility             | no            | partial              | partial     |

The cold-start gap (B at ~50-200 ms vs D at ~1-5 ms) looks dramatic. In
practice it is paid *once* per query plan; with the plan cache (§7.8) a
repeated query is free. The honest framing of who pays:

- Production query workloads with stable shapes: pay the compile cost
  once per shape per server lifetime. Below 1% overhead at scale.
- Studio's ad-hoc query view, SDK dynamic queries, exploratory reports
  ("let me check this collection real quick"): pay the compile cost on
  every distinct query. This is real and it is the legitimate worst case
  the design must address, not dismiss.

For the second category, the design provides three escape hatches: (a) a
per-database `Corax.UseCompiledQueries` flag, default off until the cache
is warm; (b) an explicit per-query override
(`Hint.NoCompile`) for known-ad-hoc paths; (c) a budget-based fallback
where queries whose compile time exceeds a threshold fall back to
match-tree for the first invocation and continue compiling in the
background (§C.1).

Maintenance cost is the deciding factor for the multi-year horizon. With
B (Roslyn C#), adding a new primitive or planner pattern is "edit the
template"; with D (IL emit), it is "edit the IL emitter". Over five
years and dozens of new patterns, B wins.

:fix: I want to see some ideas about what those patterns looks like with Roslyn
:fix: show me a couple of those.

The maintenance gap (low for B, high for D) is the real differentiator
for a multi-year design. Corax's match repertoire keeps growing; every
new match type or planner pattern needs a code-gen path. With B, that
path is "edit the C# template"; with D, that path is "edit the IL
emitter". Over five years, B wins by a wide margin.

Expression Trees (C) is the wrong middle ground. It misses on
expressiveness exactly where Corax needs it — `stackalloc` for the
scratch buffers, `ref struct` for the bitmap, idiomatic SIMD calls.

### 7.5 The "other IR" — operational primitives — and what we steal from it

The operational-primitives IR proposed in `Corax 2.0 Ideas.txt` is the
*rejected* execution form (because of §7.3). But the *primitive set* it
proposes is exactly what we want as the vocabulary for the C# code-gen
target. The doc's worked example:

```javascript
function executeQueryPlan(p0, p1, p2, p3, p4) {
  const patientId = GetPostings("PatientId", p0);
  const encounterStatus = GetPostings("EncounterStatus", p1);
  const startAtDate = GetPostings("StartAtDate", p2);
  ...
}
```

:fix: don't bother to refer to the ideas docs, or to other docs. assume this is self contained.

Re-cast for Corax's actual machinery:

:fix: show the actual query we are processing here.

```csharp
[Compiled("hash:42")]
static int Execute(in QueryContext ctx, Span<long> output, int toSkip) {
    using var bitmap = new RoaringBitmap(ctx.Allocator);

    var patientPostings  = ctx.GetPostings("PatientId", ctx.P0);
    var statusPostings   = ctx.GetPostings("EncounterStatus", ctx.P1);
    var startAtPostings  = ctx.GetPostings("StartAtDate", ctx.P2);

    patientPostings.Fill(ref bitmap);                 // small list first
    using var scratch = new RoaringBitmap(ctx.Allocator);
    statusPostings.OrWith(ref scratch);
    startAtPostings.OrWith(ref scratch);
    bitmap.AndWith(ref scratch);

    return bitmap.IterateInto(output, ref toSkip);
}
```

:fix: important factor, we can add _comments_ here, which will explain our reasoning and make this a lot more understandable.

The vocabulary is small and stable:

- `ctx.GetPostings(field, term)` → `PostingList` reference.
- `PostingList.Fill / AndWith / OrWith / AndNotWith(ref RoaringBitmap)`.
- `RoaringBitmap.IterateInto(Span<long> output, ref int toSkip)`.
- `RoaringBitmap.AndWith / OrWith / AndNotWith(ref RoaringBitmap)`.
- `SortAndIterate<TEntryComparer, TDirection>(ref candidates, orderMeta,
  output, limit)` — wraps the existing `SortUsingIndex` machinery
  (§4.6) for sort + LIMIT above 4096 candidates.
- `HeapSorter<T>.Push(...)` for sub-threshold ORDER BY + LIMIT.

That's *roughly the full vocabulary*. Most plans are 10-40 lines of
generated C#. The compiler's job is small.

### 7.6 Recommendation: Roslyn-emitted C#, calling pre-compiled vector primitives

The recommended design:

1. **A query plan compiles to a single C# function** authored as
   `SyntaxTree` (not string concatenation; we use Roslyn's syntax API for
   safety and round-tripping). The function takes a `QueryContext`, an
   output `Span<long>`, and limit/skip parameters.

2. **The function body orchestrates calls** into a small, hand-written,
   pre-compiled set of primitive methods (the §7.5 vocabulary). The
   primitives are in `Corax.dll`; the generated function is in a
   per-query dynamic assembly. The primitives carry the SIMD, the
   `stackalloc`, the `ref struct` discipline. The generated code is plain
   C# orchestration.

:fix: will the JIT be able to inline code across assembly / dll boundary?
:fix: do we want to treat this as a _persistent cache_ and store the compilation results somewhere?


3. **Compilation reuses `IndexCompiler`'s Roslyn rig.** The Roslyn
   `CSharpCompilation` setup, the metadata-reference list, the rewriter
   plumbing — all reusable. The cache is **new infrastructure**:
   `IndexCompilationCache` is intentionally an unbounded
   `ConcurrentDictionary` (verified at line 20 of the source) because the
   number of indexes is small. The plan cache will hold thousands of
   entries and needs explicit eviction. Plan keys are content hashes of
   the *normalised* AST + index schema version.

:fix: note that the IndexCompiler is intentionally dynamic as much as possible.
:fix: here we want to go the other way around, need to make sure we aren't intoruding things like that which would hurt perf

4. **Fallback path: any query that fails to compile (or compilation hits
   an error) falls back to today's match-tree execution.** The match-tree
   execution doesn't go away; it's the safe rollback for the per-query
   feature flag.

:fix: if all queries compile, do we still need the match tree? I would really like to remove it in the end.

5. **Diagnostics: `EXPLAIN` returns the generated C# source.** This is a
   gift to operations — a query plan is reviewable by humans the way a
   SQL `EXPLAIN` is. It also makes the design provably correct: if the
   generated source is wrong, it's *visibly* wrong.

This recommendation answers the user's stated uncertainty (C# vs
Expression Trees) cleanly: **C# source via Roslyn**. Expression Trees
fails on expressiveness for Corax's vector/span/stackalloc patterns.

### 7.7 The IR vocabulary

The "IR" in Corax 2.0 is *not* a custom intermediate language. It is the
set of primitive calls the generated C# orchestrates. Treating the
primitive set as the IR has two virtues:

- It is **typed** — the C# compiler enforces shape correctness for free.
- It is **extensible** — adding a new primitive is "add a method to
  Corax.Querying". No grammar updates, no compiler updates.

Primitive set, v0:

```
namespace Corax.Querying.Primitives;

// Posting list × bitmap
static void FillFromPostings(PostingList postings, ref RoaringBitmap bitmap);
static void AndWithPostings(PostingList postings, ref RoaringBitmap bitmap);
static void OrWithPostings(PostingList postings, ref RoaringBitmap bitmap);
static void AndNotWithPostings(PostingList postings, ref RoaringBitmap bitmap);

// Bitmap × bitmap
static void AndWith(ref RoaringBitmap left, ref RoaringBitmap right);
static void OrWith(ref RoaringBitmap left, ref RoaringBitmap right);   // destructive on right
static void AndNotWith(ref RoaringBitmap left, ref RoaringBitmap right);

// Range / between (planner-fused predicates)
static void FillFromRange(CompactTree tree, Slice from, Slice to, ref RoaringBitmap bitmap);
static void FillFromInequality(CompactTree tree, ComparisonMode mode, T threshold,
                                ref RoaringBitmap bitmap);

:fix: do we _need_ Inequality? should it just be from/to, with max / min if we have Inequality? what else is that needed for?

// Scoring (boost composes around the bitmap, not inside the primitives;
// see §6.5/§6.6 — Score is applied by a BoostingMatch wrapper outside the
// generated function, so the primitive set does not include a Score primitive)

// Sort/Project — delegate to the existing SortingMatch.SortUsingIndex
// machinery (PR #16453, see §4.6). Pillar B does NOT replace the sort
// path; it composes with it.
static int SortAndIterate<TEntryComparer, TDirection>(
    ref RoaringBitmap candidates, OrderMetadata orderMeta,
    Span<long> output, int limit)
    where TEntryComparer : struct, IEntryComparer, IComparer<UnmanagedSpan>
    where TDirection : struct, ILookupIterator;
// Internally walks SortedIndexReader<TDirection>, intersects with the
// candidate bitmap, accumulates matches into output. Same algorithm as
// today's SortUsingIndex, just driven from a bitmap instead of an
// IQueryMatch.

// Iterator → output (for queries without ORDER BY or with ORDER BY entryId)
static int IterateInto(ref RoaringBitmap bitmap, Span<long> output, ref int toSkip);

// Vector / Spatial — wrappers around existing match types
static int VectorRank(VectorIndex index, ReadOnlySpan<float> query, ref RoaringBitmap candidates,
                      Span<(long entryId, float score)> output, int k);
static int SpatialFilter(SpatialIndex index, IShape shape, ref RoaringBitmap candidates);

:fix: see my previous comments on scoring - not sure that this is good, and how do we maintain the scoring to return to the caller?
```

Each primitive is hand-written in `Corax.dll`, SIMD-tuned, individually
benchmarkable. The generated query function is a sequence of primitive
calls.

**`QueryContext.GetPostings` and the AST→primitive bridge.** The §7.5
example uses `ctx.GetPostings("PatientId", ctx.P0)` as shorthand. In the
real generated code this becomes a call to the existing posting-list
lookup machinery on `IndexSearcher` — there is no new "GetPostings" the
way there is a new "FillFromPostings". The compiler emits the
`IndexSearcher.GetPostings(field, term)` call directly; `QueryContext`
is the bag of bound parameters and the indexsearcher reference.

**Sort always uses existing infrastructure.** The compiled-query path
emits `SortAndIterate<TEntryComparer, TDirection>(...)` for any query
with `ORDER BY` above the `IndexSortingThreshold` (4096 rows, see §4.6);
the underlying machinery is `SortingMatch.SortUsingIndex` and
`SortedIndexReader<TDirection>`, which already exist in production. For
sub-threshold result sets the compiler emits a heap-sort over
`IterateInto`'s output (matching today's `SortResults<TEntryComparer>`).
Neither path was redesigned in 2.0 — the compiler is a *consumer* of the
existing sort infrastructure, not a re-implementer.

### 7.8 Caching, warm-up, observability

- **Plan cache** keyed by a stable hash of the *normalised AST*: a query
  with `where x = 5 and y = 7` and a query with `where x = 12 and y = 0`
  produce the same plan with different parameters. Parameters are
  hoisted out at compile time so they never flow into emitted source —
  only into the compiled function's runtime arguments. (This is also a
  multi-tenancy invariant: a buggy planner that lets a parameter affect
  codegen could in principle let one tenant's query emit code visible to
  another tenant on the same process. The "no parameter ever flows into
  emitted code" rule must be enforced by code review and a parity test.)
  Cache lives at the database level (per `IndexSearcher`'s parent).

:fix: how does that work when we have different cardinality of the paramters? we need different query plans then, no?

- **Eviction is new infrastructure** — `IndexCompilationCache` does not
  evict (verified). The plan cache implements a true LRU with a
  configurable budget; default 64 MB or 1024 plans, whichever first.
  Compile-on-miss uses `Lazy<CompiledPlan>` with
  `LazyThreadSafetyMode.ExecutionAndPublication` so concurrent misses on
  the same key serialise behind the first compile rather than burning
  N compile costs.

:fix: need to think about making this persistent somehow. 

- **Warm-up**: on database open, the cache pre-loads plans for the top-N
  queries from the previous session (logged as part of the existing
  query-cost telemetry).
- **Compile failures** fall back to match-tree execution and log a
  diagnostic. Repeated failures for the same plan key disable Pillar B
  for that query shape.

:fix: what can cause that?

- **EXPLAIN**: the generated source code is exposed via the existing
  `Studio` query-plan UI as an additional tab.
- **Metrics**: `corax_compiled_query_compile_duration_ms` per plan,
  `corax_compiled_query_invoke_duration_ns` per call,
  `corax_compiled_query_cache_hit_ratio`. The match-tree path keeps its
  existing metrics so before/after deltas are computable.

#### Concurrency and lifetime under high QPS

Corax queries are synchronous, but the generated-assembly story still
needs an explicit concurrency model:

- **Plan-cache reads** are a hot path on every query. Use the
  `ConcurrentDictionary<TKey, Lazy<CompiledPlan>>` pattern;
  `Lazy.ExecutionAndPublication` guarantees one compile per cache miss,
  even under simultaneous arrival. Cost on hit is one dictionary lookup
  and one delegate invoke.
- **LRU update on hit** is the standard contention point for any LRU
  built on `ConcurrentDictionary`. The plan cache uses a CLOCK
  approximation rather than strict LRU: each entry has a one-bit
  reference flag set on hit; eviction sweeps clear bits and evicts the
  next clear-bit entry. Avoids the per-hit linked-list manipulation that
  makes strict LRU contended.
- **Generated-assembly lifetime relative to in-flight queries.** Each
  compiled plan is loaded into a per-database `AssemblyLoadContext`. On
  eviction, we mark the plan dead but do *not* unload the
  `AssemblyLoadContext` until the entry is no longer referenced from any
  in-flight stack frame (tracked via a per-plan reader-counter
  incremented on invoke, decremented on return). For long-running
  queries this means a recently-evicted plan can stay in memory briefly
  past eviction, which is fine.

:fix: why do we need that? adds atomic writes on each query. can't we let the GC handle this?

- **Schema invalidation while a query is in flight.** Schema changes
  bump the index schema version. A query that has already passed plan-
  cache lookup is committed to its compiled plan for the duration of
  that one execution; its results are consistent with the schema at
  query-start time, not query-end time. This matches the existing
  Voron transactional read-isolation guarantee.

:fix: not relevant, the query is bound to an index, and schema changes means new instance.
:fix: do need to consider stuff like side-by-side indexes replacing an index, so the key cannot be the index name.

A worked example: 1000 QPS, 50 distinct plan shapes, 256 MB plan cache
budget. With 50 plans the cache never evicts (256 MB / 50 = ~5 MB per
plan, well above typical generated-assembly size). Cache lookups are
50 ns scale; compile cost (~150 ms once per shape per server lifetime)
amortises to negligible. Cold-cache hit on a new shape: 150 ms latency
spike, single query, then steady-state.

:fix: why 5MB per plan?

### 7.9 Open questions — Pillar B

These are the unresolved decisions for the team. Each is explored in the
companion `corax-2.0-questions.md` file.

1. **Plan eviction unit — resolved.** §7.8 commits to **per-plan
   `AssemblyLoadContext`** with deferred unload via a per-plan reader
   counter. This deviates from `IndexCompiler`'s "one process-wide
   assembly" pattern intentionally: index definitions are bounded in
   number, query plans are not. The cost is one ALC per plan (a few KB
   of metadata each); the benefit is collectible eviction.

2. **Does the generated code call into `IndexSearcher` instance methods,
   or into static primitives?** Static primitives are JIT-friendlier
   (no `this` indirection); instance methods are easier to refactor.
   Lean: static primitives, with `IndexSearcher` passed in as parameter.

3. **How do we handle plan invalidation on schema change?** Each plan
   carries the index schema version in its key; a schema change
   invalidates the relevant cache entries. Already cheap given the cache
   is keyed by hash. (Future-work: question N.6 asks whether finer-grained
   invalidation — by accessed-fields rather than whole-schema-version —
   is worth the complexity.)

:fix: make that schmea version, etc, part of the hash itself, no?

4. **Should compilation be synchronous or background?** First option:
   compile synchronously on first use, hold the request. Second: compile
   in background, use match-tree for the first N invocations. Lean:
   synchronous, behind a per-query latency budget; if compile takes
   longer than 200 ms, fall back to match-tree for that invocation and
   keep compiling in background for the next.

5. **EXPLAIN contract for compiled-but-unused plans.** When a query
   compiled successfully but the runtime chose match-tree (e.g. because
   `Hint.NoCompile` was passed, or the plan cache evicted between
   compilation and invocation), what does EXPLAIN return? Lean: the
   generated source is *always* shown if it compiled, regardless of
   whether this particular invocation ran it; EXPLAIN includes a flag
   indicating which path actually executed.

---

## 8. Cross-Cutting Concerns

### 8.1 Erasure-layer fate

The seven `*.Erasure.cs` files are dead under Pillar B. Once a query
compiles to a generated function, there's no reason to box generic struct
matches; the JIT can specialise directly. Plan: keep the erasure layer
through Pillar A (the bridge match needs it), remove during Pillar B
rollout. This is a pure refactor — no behaviour change — and nets ~1500
LoC of removal.

### 8.2 Explanations (RavenDB-18385)

Explanations get easier under Pillar B because the explanation *is the
generated source code*. The current `QueryInspectionNode` machinery is
useful for the match-tree path; we keep it for backward compatibility but
add a code-gen-aware view for compiled queries.

For Lucene-style "why did this document match" explanations, the bitmap
pipeline doesn't help directly — that requires per-document scoring
state, which is independent of the bitmap. We design Explanations as a
separate match wrapper that records score contributions per document;
under bitmap or compiled execution it's a slight overhead.

### 8.3 Test strategy

Three layers of correctness coverage:

- **Match-tree vs bitmap parity**: every query in the existing test
  corpus must return the same results under both execution paths.
  Implemented as a test attribute that runs the same test twice with
  different `Parameters.UseBitmapPath`.
- **Match-tree vs compiled parity**: same idea for Pillar B. The
  generated source can be diffed against a golden file; the *output*
  must match the match-tree path bit-for-bit.
- **Performance regression bench**: the existing
  `bench/Voron.Benchmark/Corax/` corpus extended with the bitmap
  benchmarks already on `RavenDB-25284`. CI runs a smoke set on every
  push to `v7.2`; full perf bench runs nightly.

Test category-wise: existing
`RavenTestCategory.Corax | RavenTestCategory.Querying` covers the bulk;
add a new `RavenTestCategory.CoraxCompiled` for Pillar B-specific tests
(generated source diffs, plan-cache behaviour, fallback paths).

---

## 9. Performance Model

**Methodology footprint for the numbers below.** All "what we know"
figures come from `bench/Voron.Benchmark/Corax/CoraxQueryBitmapBench.cs`
on the `RavenDB-25284` branch, run on a 10M-document synthetic corpus
with three indexed dimensions of skewed cardinality (`Status` ~50%
selectivity, `Category` ~10%, `Tag`/`Region` ≤1%). Hardware: developer
laptops (Apple M-series and Intel Core); plaintext (no encryption-at-
rest). Cache state: warm for the headline numbers, cold runs reported
separately on the branch. None of these are production-cluster numbers
yet — they are the prototype's evidence, suitable for design conviction
and not for marketing claims.

What we know:

- AND high-selectivity (50% ∩ 10%): bitmap wins ~2.4×.
- ANDNOT high-selectivity (50% − 10%): bitmap wins ~4.1×.
- Composite (Cat0 OR Cat1) AND Status: bitmap wins ~4×.
- AND low-selectivity (10% ∩ 1%): roughly even.
- OR low-selectivity (1% ∪ 1%): streaming wins ~5× (materialisation
  overhead dominates).
- Mixed: bitmap consistently wins above ~50K cardinality, streaming
  below.

What we *don't* know yet, and the Pillar B performance model is
predicated on:

- **Generated code vs match tree on small queries.** On a 100-row result
  set, match-tree is dominated by Fill loop overhead. Compiled-query
  removes that overhead but adds per-invocation dispatch. Hypothesis:
  break-even around 10-100 entries.
- **Plan cache hit ratio in real workloads.** A typical RavenDB
  workload has perhaps 10-100 distinct query *shapes*. Cache hit ratio
  should be >99%; if it isn't, the design assumption breaks.
- **Bitmap-driven sort vs streaming-driven sort.** §4.6's
  `SortUsingIndex` was designed assuming an `IQueryMatch` candidate
  source. When the candidate source is a bitmap, the intersection step
  becomes per-entry `Contains` (the §6.8 SIMD Quad path). Whether
  bitmap-driven sort wins on small candidate sets, large candidate
  sets, or both, is unmeasured. Bench: same `WHERE x = a ORDER BY y`
  query under match-tree-driven sort vs bitmap-driven sort, sweeping
  candidate cardinality.

The first 2.0 release should publish a performance regression bench
suite that the CI runs continuously. Numbers in the design doc become
stale fast; the bench corpus is the live source of truth.

---

## 10. Risks

**Pillar A risks:**

- Bitmap allocation pressure on `ByteStringContext` for queries returning
  whole-database result sets. Mitigation: cap bitmap size; fall back to
  streaming for scans that exceed a configurable threshold.
- Sort stability changes — bitmap iteration is by entry ID; if the
  current path has a sort tie-break that depends on iteration order, that
  changes. Mitigation: explicit tie-break by entry ID in `ORDER BY`
  generation.
- Boost / score contributions composing differently. Mitigation: parity
  tests (§8.3).

**Pillar B risks:**

- Roslyn compilation cost dominates short-lived workloads (e.g.
  test-fixture spin-up). Mitigation: per-database flag to disable
  Pillar B for non-production environments; the match-tree path still
  works.

:fix: no, that is a problem, but we have the iteration mode to resolve that, caching, etc.
:fix: we _must_ ensure that this is tested during the normal course of things.

- Generated assembly leaks. Mitigation: strict eviction policy; LRU with
  hard memory budget.
- AOT incompatibility. RavenDB doesn't ship AOT today, but if it ever
  does (e.g. NativeAOT-compiled Studio), Pillar B is incompatible. We
  document this as a non-goal.

:fix: interpreter would work here?

- Debugging support: a customer report that says "query X returns wrong
  result" is harder to investigate when the engine is per-query
  generated code. Mitigation: EXPLAIN returns the source; the source can
  be reviewed and the parity-test machinery can run the same query under
  the match-tree path. For *failed-to-compile* queries (which fall back
  to match-tree), EXPLAIN should still return the (failing) generated
  source plus the compile error — that's the worst-case ticket and we
  want it self-contained.

:fix: we should make sure that we have an easy way to copy/paste the code for that and run that under a debugger.

**Cross-pillar risks.** The biggest risk this document does not yet have
data on: Pillar A and Pillar B interact. Compiled queries that
orchestrate bitmap primitives can have failure modes that the standalone
Pillar A parity tests do not catch — e.g. a generated function that
holds two `RoaringBitmap` locals and calls destructive `OrWith` on the
wrong one (§6.7's destructive-steal optimisation), or that misses a
`Dispose` on a code path. The cross-pillar test corpus must run the
same query through both compiled+bitmap and match-tree+streaming and
compare results. **Parity tests catch incorrect *results* from
destructive-OR misuse; they do not catch the *misuse itself*. Static
analysis of generated source, or codegen-time invariants on which
primitives may consume which bitmap locals, are needed to prevent the
class of bug rather than just its observable effects.** This is a real
engineering cost the design has not yet sized.

**Operational risks.**

- *Memory-pressure observability.* The plan cache (§7.8, default 64 MB
  / 1024 plans) shares the same RAM as `_entryIdToLocation`,
  `MaxMemoizationSizeInBytes` (per-`IndexSearcher` 128 MB), and the
  bitmap-allocation arena. A naive sum can overshoot. The 2.0 release
  needs a unified memory-pressure controller that flushes the plan
  cache before any of the per-query allocators bail. Without it, a
  plan-cache-heavy workload can starve query memory.
- *Compile-storm at cluster restart.* Fifty-node cluster restarts plus
  incoming traffic = fifty parallel Roslyn compiles for the same
  plans. Per-shape compile cost (~150 ms) bounds the storm, but
  measuring P99 latency during a rolling restart is the right
  validation. A cluster-coordinated plan-cache pre-warm (e.g. each node
  publishes its top-N plan keys to Rachis on graceful shutdown,
  startup re-compiles them ahead of taking traffic) is a reasonable
  future optimisation; not in 2.0 v1.
- *Roslyn version pinning.* `IndexCompiler` references a specific
  `Microsoft.CodeAnalysis.CSharp` package version. Pillar B inherits
  that pin. A Roslyn upgrade for any reason (security, language-feature
  needed for the IR) bumps both subsystems together; expect a coupled
  validation cycle. We accept the coupling; ad-hoc dual-version
  Roslyn is not worth the maintenance load.

---

## 11. Things We Are *Not* Deciding Now

- Whether to keep the per-index Lucene/Corax engine choice in the long
  term. It stays for 2.0.
- Whether to expose Corax outside RavenDB (separate library product). Not
  in this round.
- Whether the IR primitive set should evolve into a public extension
  point. Stays internal in 2.0.
- Whether to support distributed-aware planning. Out of scope.
- Indexing pipeline changes (RavenDB-19665 batch analyzer). The mention in
  some old notes is real but is its own design exercise; not folded
  here.

---

## 12. Pointers

- `corax-2.0-questions.md` — open questions accumulated during writing.
  Tracked separately so this document stays a thesis rather than a
  Q&A.
- `corax-2.0-review-notes.md` and `corax-2.0-review-notes-2.md` — review
  pass notes (produced by separate agent passes after this draft).
- `C:\Work\ravendb\RavenDB-25284\` — canonical bitmap implementation; the
  authoritative source for §6.1.
- `src/Raven.Server/Documents/Indexes/Static/IndexCompiler.cs` and the
  rewriter set under `src/Raven.Server/Documents/Indexes/Static/Roslyn/`
  — the existing Roslyn rig Pillar B reuses.
- `src/Corax/Querying/Matches/SortingMatches/SortingMatch.cs:469`
  (`SortUsingIndex<TEntryComparer, TDirection>`),
  `src/Corax/Querying/TermsReader.cs`, PR
  [#16453](https://github.com/ravendb/ravendb/pull/16453) and
  [#17061](https://github.com/ravendb/ravendb/pull/17061),
  YouTrack [RavenDB-21124](https://issues.hibernatingrhinos.com/issue/RavenDB-21124)
  — the sort-by-non-WHERE-field machinery (§4.6).

*End of design draft.*


**N.5 [risk]** What is the threat model for the generated source under
multi-tenancy? RavenDB databases share a process. If tenant A's query
generates code in a per-process dynamic assembly and tenant B's query
hits a planner bug that lets a parameter affect codegen (rather than
being hoisted), tenant B could in principle emit code into A's space.
The plan-cache key includes index schema version (per C.10) but the
*generation* path needs to be confirmed parameter-sanitised. Pillar C
needs an explicit "no parameter ever flows into emitted code, only into
the compiled function's runtime arguments" invariant.

:fix: there is not treat modeling here. we assume that the code is valid and safe.
:fix: it exposes a limited set of operations only, etc.

