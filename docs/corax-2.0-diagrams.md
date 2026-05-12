# Corax 2.0: Visual Architecture Guide

Companion to `corax-2.0-architecture.md`. This document contains diagrams and
visual walkthroughs of the pipeline — the parts that are best shown, not written.

---

## 1. Old Pipeline vs New Pipeline: Side-by-Side

### Old: Generic Type Tree (Corax 1.x)

For the query `WHERE Tag = 'db' AND Status = 'active' AND Priority > 3`:

```
                    ┌──────────────────────────────────┐
                    │ SortingMatch.Erasure              │  ← type erasure to hide
                    │   (hides the generic nesting)     │     the 3-level deep type
                    └──────────────┬───────────────────┘
                                   │
                    ┌──────────────▼───────────────────┐
                    │ BinaryMatch<                      │
                    │   BinaryMatch<                    │
                    │     TermMatch,        ← Tag='db'  │
                    │     TermMatch,        ← Status    │
                    │     AndMarker>,                    │
                    │   MultiUnaryMatch,    ← Priority>3│
                    │   AndMarker>                       │
                    └──────────────────────────────────┘

Execution:  Fill() calls propagate recursively down the tree.
            Each level does a merge-join on Span<long>.

    BinaryMatch.Fill()
        → inner.Fill()                     [Tag posting list → sorted span]
        → outer.AndWith(innerResults)      [Status posting list → merge]
    BinaryMatch.Fill()
        → inner.Fill()                     [above result]
        → outer.AndWith(innerResults)      [Priority scan → merge]

    Data is touched 3 times. Each merge-join allocates/scans spans.
    The JIT compiles a unique method for each BinaryMatch<A,B,M> instantiation.
```

### New: Flat Plan → IL → Bitmap (Corax 2.0)

Same query:

```
    ┌─────────────────────────────────────────────────────────┐
    │                    PlanOp[] (4 ops)                      │
    │                                                         │
    │  [0] FillFromPostings   Tag='db'     → bitmap[0]        │
    │  [1] AndWithPostings    Status='act' → bitmap[0] ∩ src  │
    │  [2] AndWithPostings    Priority>3   → bitmap[0] ∩ src  │
    │  [3] CheckAndMaybeEntryScan                             │
    └────────────────────────┬────────────────────────────────┘
                             │
                   QueryILEmitter compiles to:
                             │
                             ▼
    ┌─────────────────────────────────────────────────────────┐
    │              DynamicMethod (IL)                          │
    │                                                         │
    │  call CtxFillFromPostingSource(ctx, 0)   // Tag         │
    │  call CtxAndWithPostingSource(ctx, 1)    // Status      │
    │  brfalse.s done           // early exit if empty        │
    │  call CtxAndWithPostingSource(ctx, 2)    // Priority    │
    │  brfalse.s done           // early exit if empty        │
    │  call ShouldSwitchToEntryScan(...)                      │
    │  brtrue.s entryScan                                     │
    │  br.s done                                              │
    │  entryScan:                                             │
    │    ... inline predicate checks ...                      │
    │  done:                                                  │
    │  ret                                                    │
    └─────────────────────────────────────────────────────────┘

Execution:  Single flat pass. Each posting list is merged directly
            into the RoaringBitmap. Data is touched once per source.
            Early exit on empty intermediate result.
            No generic types. No recursive calls. No merge-join.
```

---

## 2. Query Execution Flow

```
  ┌──────────────┐
  │  RQL Query    │   "from Users where Tag = $tag and Status = $status order by Name"
  │  + Params     │   { tag: "db", status: "active" }
  └──────┬───────┘
         │
         ▼
  ┌──────────────────────────────────────────────────────┐
  │  PlanCache.TryGetTemplate("from Users where ...")     │
  │                                                       │
  │  HIT? ──────── yes ──→ use cached ClauseTemplate      │
  │  MISS? ─────── no ──→ ParseTemplate()                 │
  │                          │                             │
  │               ┌──────────▼──────────┐                  │
  │               │ ClauseTemplate      │                  │
  │               │  Clauses:           │                  │
  │               │   [0] Tag, Equals   │                  │
  │               │   [1] Status, Equals│                  │
  │               │  IsOr: false        │                  │
  │               └──────────┬──────────┘                  │
  │                          │                             │
  └──────────────────────────┼─────────────────────────────┘
                             │
         ┌───────────────────▼───────────────────┐
         │     PopulateClauseValues()             │
         │                                        │
         │  For each ClauseInfo:                  │
         │    binding.ParameterName = "tag"        │
         │    → blittable["tag"] = "db"            │
         │    → StringValues[0] = "db"             │
         │    → PackedParam(String, 0)             │
         │                                        │
         │  Result: typed arrays populated         │
         │    LongValues:   []                     │
         │    DoubleValues: []                     │
         │    StringValues: ["db", "active"]       │
         └───────────────────┬───────────────────┘
                             │
         ┌───────────────────▼───────────────────┐
         │     EstimateCardinality()              │
         │                                        │
         │  Tag='db':     150 entries              │
         │  Status='act': 8,000 entries            │
         │                                        │
         │     SortOperands()                     │
         │  → [Tag=150, Status=8000]  (cheapest   │
         │     first for AND chain)               │
         │                                        │
         │  ordering = hash(sorted indices)        │
         └───────────────────┬───────────────────┘
                             │
         ┌───────────────────▼───────────────────┐
         │  PlanCache.Get(text, ordering, typeSig) │
         │                                        │
         │  HIT?  → CompiledPlan (IL delegate)    │────────────┐
         │  MISS? → EmitPlan() + EmitDelegate()   │            │
         │          + PlanCache.Add(...)           │            │
         └───────────────────┬───────────────────┘            │
                             │                                 │
         ┌───────────────────▼───────────────────┐            │
         │  ResolveMatches()    → IQueryMatch[]   │◄───────────┘
         │  ResolveTermSources()→ PostingSource[] │
         │  ExtractScanParams() → long[],double[] │
         └───────────────────┬───────────────────┘
                             │
         ┌───────────────────▼───────────────────┐
         │  new CompiledQueryMatch(               │
         │    compiledPlan, bitmapCount, opCount,  │
         │    resolvedMatches, postingSources,     │
         │    termsProviders, longParams, ...      │
         │  )                                      │
         └───────────────────┬───────────────────┘
                             │
                             │  .Fill() / .Count / .Contains()
                             │  triggers lazy Execute()
                             ▼
         ┌───────────────────────────────────────┐
         │  Execute()                             │
         │                                        │
         │  1. Rent Bitmaps[] from ArrayPool      │
         │  2. _compiledDelegate(this)            │
         │     ↓                                   │
         │     IL calls QueryPrimitives methods    │
         │     operating on this.Bitmaps[0..2]     │
         │     ↓                                   │
         │  3. _bitmapData = Bitmaps[0]           │
         │  4. PrepareForReading()                │
         │  5. _count = bitmap.Count              │
         │  6. Return scratch bitmaps to pool     │
         └───────────────────┬───────────────────┘
                             │
                             ▼
                    RoaringBitmap result
                   (supports Fill, Contains,
                    Count, AndWith for sorting)
```

---

## 3. PlanOp Execution: AND Chain Example

Query: `WHERE Tag = 'db' AND Status = 'active' AND NOT Deleted = true`

After cardinality sort: Tag (150) → Status (8000) → NOT Deleted (90000)

```
  PostingSources:            Bitmaps:
  ┌─────────────┐           ┌─────────────────────┐
  │ [0] Tag='db'│           │ [0] main   (result)  │
  │ [1] Status  │           │ [1] scratch           │
  │ [2] Deleted │           └─────────────────────┘
  └─────────────┘

  Op 0: FillFromPostings [0] → bitmap[0]
  ┌─────────────────────────────────────────────────┐
  │ Read Tag='db' posting list                       │
  │ Decompress into bitmap[0]                        │
  │ bitmap[0] = {3, 17, 42, 55, 91, ...}  (150)     │
  └──────────────────────────┬──────────────────────┘
                             │
  Op 1: AndWithPostings [1] → bitmap[0] ∩ Status
  ┌─────────────────────────────────────────────────┐
  │ Read Status='active' posting list                │
  │ bitmap[0].AndWith(statusPostings)                │
  │ bitmap[0] = {3, 17, 42, 55}           (4)       │
  │                                                  │
  │ if bitmap[0].IsEmpty → early exit (skip rest)    │
  └──────────────────────────┬──────────────────────┘
                             │
  Op 2: AndNotWithPostings [2] → bitmap[0] \ Deleted
  ┌─────────────────────────────────────────────────┐
  │ Read Deleted=true posting list                   │
  │ bitmap[0].AndNotWith(deletedPostings)            │
  │ bitmap[0] = {3, 17, 42}               (3)       │
  └──────────────────────────┬──────────────────────┘
                             │
  Op 3: CheckAndMaybeEntryScan
  ┌─────────────────────────────────────────────────┐
  │ bitmap[0].Count=3, next posting has 90K entries  │
  │ 3 * 64 = 192 < 90000 → YES, entry scan cheaper  │
  │ → jump to entry scan path                        │
  │   (but we already processed all clauses,         │
  │    so entry scan has nothing to check)            │
  └──────────────────────────┬──────────────────────┘
                             │
                             ▼
                   Final: bitmap[0] = {3, 17, 42}
```

---

## 4. PlanOp Execution: IN Clause with OrRange

Query: `WHERE Status IN ('active', 'pending', 'review', null)`

```
  PostingSources:              ResolvedMatches:
  ┌──────────────────┐         ┌──────────────────┐
  │ [0] 'active'     │         │ [3] TermQuery(    │
  │ [1] 'pending'    │         │       Status,null)│
  │ [2] 'review'     │         └──────────────────┘
  └──────────────────┘

  Op 0: FillFromPostings [0] → bitmap[0]
  ┌─────────────────────────────────────────────────┐
  │ bitmap[0] = 'active' posting list                │
  └──────────────────────────┬──────────────────────┘
                             │
  Op 1: OrRange [1], count=2, bitmap[0]
  ┌─────────────────────────────────────────────────┐
  │ IL loop:                                         │
  │   for j = 1 to 2:                                │
  │     bitmap[0].OrWith(PostingSources[j])          │
  │                                                  │
  │ After: bitmap[0] = active ∪ pending ∪ review     │
  └──────────────────────────┬──────────────────────┘
                             │
  Op 2: OrWithPostings [3] → bitmap[0]  (QueryMatch dispatch)
  ┌─────────────────────────────────────────────────┐
  │ FillFromMatch(TermQuery(Status, null))           │
  │ bitmap[0] |= null-term entries                   │
  │                                                  │
  │ (null terms go through IQueryMatch because       │
  │  null has special posting list handling)          │
  └──────────────────────────┬──────────────────────┘
                             │
                             ▼
              bitmap[0] = active ∪ pending ∪ review ∪ null
```

---

## 5. PlanOp Execution: OR Chain with AND Groups (Three-Bitmap Pattern)

Query: `WHERE (A = 1 AND B = 2) OR (C = 3 AND D = 4)`

```
  Bitmap slots:
    [0] = main result
    [1] = scratch
    [2] = save slot    ← only allocated for this pattern

  Op 0: FillFromPostings A=1 → bitmap[0]
  Op 1: AndWithPostings  B=2 → bitmap[0]     // bitmap[0] = A∩B
  ────────── first AND group done ──────────

  Op 2: ClearBitmap [1]
  Op 3: SwapBitmaps [0] ↔ [2]                // save A∩B to slot 2
                                              // slot 0 is now empty (was slot 2)
  Op 4: FillFromPostings C=3 → bitmap[0]
  Op 5: AndWithPostings  D=4 → bitmap[0]     // bitmap[0] = C∩D
  ────────── second AND group done ──────────

  Op 6: OrBitmaps [0] |= [2]                 // bitmap[0] = (C∩D) ∪ (A∩B)

  Final: bitmap[0] = (A∩B) ∪ (C∩D)
```

Why three bitmaps? You can't OR into a bitmap you're also building an AND chain in. Slot 2 saves the prior OR accumulation while slot 0 is reused for the new AND sub-chain.

---

## 6. Entry Scan: Inline Predicate Evaluation

When the intermediate bitmap is small enough, the IL switches to reading individual entries:

```
  bitmap[0] after AND chain: {3, 17, 42}  (3 entries)
  Remaining predicate: Age >= 21 AND Age <= 65

  Entry scan emitted IL (pseudocode):
  ┌─────────────────────────────────────────────────────────┐
  │  clear bitmap[1]   // TempBitmap                        │
  │  iter = bitmap[0].GetIterator()                         │
  │                                                         │
  │  while (batch = iter.Fill(buffer)) > 0:                 │
  │    for each entryId in batch:                           │
  │      reader = searcher.GetEntryTermsReader(entryId)     │
  │      reader.FindNext(fieldRootPages[ageFieldIdx])       │
  │                                                         │
  │      // Inlined IL — no virtual dispatch:               │
  │      long val = reader.CurrentLong                      │
  │      if val < LongParams[0] goto skip   // age < 21    │
  │      if val > LongParams[1] goto skip   // age > 65    │
  │                                                         │
  │      bitmap[1].Add(entryId)  // passed all predicates   │
  │    skip:                                                │
  │                                                         │
  │  swap bitmap[0] ↔ bitmap[1]                             │
  │  clear bitmap[1]                                        │
  └─────────────────────────────────────────────────────────┘

  Cost: 3 entry reads + 3 × (2 long comparisons)
  vs. full bitmap path: decompress Age posting list (maybe 50K entries) + AND
```

---

## 7. PlanCache: SIMD Lookup

```
  PerQueryPlans (32 slots, struct-of-arrays):

  _orderings:      [0x1A3F, 0x2B4C, 0x1A3F, 0x0000, 0x5D2E, ...]
  _typeSignatures: [0x0005, 0x0005, 0x0009, 0x0000, 0x0005, ...]
  _plans:          [plan_A, plan_B, plan_C, null,   plan_D, ...]

  Lookup: ordering=0x1A3F, typeSignature=0x0005

  Vec256 iteration 1 (slots 0-7):
  ┌─────────────────────────────────────────────────────────┐
  │  ordVec = [0x1A3F, 0x1A3F, 0x1A3F, 0x1A3F,             │
  │            0x1A3F, 0x1A3F, 0x1A3F, 0x1A3F]             │
  │                                                         │
  │  ords   = [0x1A3F, 0x2B4C, 0x1A3F, 0x0000,             │
  │            0x5D2E, 0x0000, 0x0000, 0x0000]             │
  │                                                         │
  │  Equals(ords, ordVec) =                                 │
  │           [  1   ,   0   ,   1   ,   0   ,              │
  │              0   ,   0   ,   0   ,   0   ]              │
  │                                                         │
  │  typVec = [0x0005, 0x0005, 0x0005, 0x0005, ...]        │
  │  typs   = [0x0005, 0x0005, 0x0009, 0x0000, ...]        │
  │  Equals  = [  1  ,   1  ,   0  ,   0  , ...]           │
  │                                                         │
  │  Combined (AND):                                        │
  │           [  1   ,   0   ,   0   ,   0   ,              │
  │              0   ,   0   ,   0   ,   0   ]              │
  │                                                         │
  │  Mask = 0b00000001  →  TrailingZeroCount = 0            │
  │  → slot 0 is a candidate                                │
  │  → Volatile.Read(_plans[0]) → plan_A                    │
  │  → ResolveCandidate(plan_A, ordering, typeSig, kinds)   │
  │  → match! return plan_A                                 │
  └─────────────────────────────────────────────────────────┘

  Total: 1 vector load + 2 vector compares + 1 AND + 1 bitmask extract
  For 32 slots: 4 iterations (Vec256) or 8 iterations (Vec128)
```

---

## 8. Template / Execution Split

```
  ┌─────────────────────────────────────┐
  │         ClauseTemplate (cached)      │
  │                                      │
  │  ClauseInfo[0]:                      │
  │    FieldName: "Tag"                  │
  │    ClauseType: Equals                │
  │    Bindings[0]:                      │
  │      ParameterName: "tag"            │  ← knows the NAME
  │      LiteralType: Parameter          │
  │                                      │
  │  ClauseInfo[1]:                      │
  │    FieldName: "Status"               │
  │    ClauseType: Equals                │
  │    Bindings[0]:                      │
  │      LiteralValue: "active"          │  ← literal: value is known
  │      LiteralType: String             │
  └──────────────┬──────────────────────┘
                 │
                 │  PopulateClauseValues(template, queryParameters)
                 │
                 ▼
  ┌─────────────────────────────────────┐
  │       ClauseExecution[] (per-run)    │
  │                                      │
  │  ClauseExecution[0]:                 │
  │    PackedParamValue:                 │
  │      PackedParam(String, idx=0)      │  ← resolved from blittable
  │    Cardinality: 150                  │  ← from index stats
  │    TermValueType: String             │
  │                                      │
  │  ClauseExecution[1]:                 │
  │    PackedParamValue:                 │
  │      PackedParam(String, idx=1)      │  ← literal already resolved
  │    Cardinality: 8000                 │
  │    TermValueType: String             │
  └──────────────┬──────────────────────┘
                 │
                 │  Typed arrays:
                 │    StringValues = ["db", "active"]
                 │    LongValues   = []
                 │    DoubleValues = []
                 │
                 │  PackedParam(String, 0) → StringValues[0] = "db"
                 │  PackedParam(String, 1) → StringValues[1] = "active"
                 ▼
```

---

## 9. PostingSource: Three-Way Dispatch

```
  CompactTree lookup for "Tag" = "db":

  CompactTree node value for "db":
    low 2 bits = TermIdMask

  ┌──────────────┬──────────────────────────────────────────┐
  │ Mask bits    │ PostingSourceKind                         │
  ├──────────────┼──────────────────────────────────────────┤
  │ 00           │ Single: one entry. Value IS the entry ID │
  │              │ → PostingSource.SingleEntryId = decoded   │
  │              │ → bitmap.Add(entryId)                     │
  ├──────────────┼──────────────────────────────────────────┤
  │ 01           │ SmallPostingList: fits in one container   │
  │              │ → PostingSource.SmallPostingListId = cid  │
  │              │ → Container.Get(llt, cid)                 │
  │              │ → FastPFor decode → bitmap.AddRange()     │
  ├──────────────┼──────────────────────────────────────────┤
  │ 10           │ PostingList: large, multi-container       │
  │              │ → PostingSource.LargeIterator = iter      │
  │              │ → iter.Fill(buffer) in batches            │
  │              │ → bitmap.AddRange() per batch             │
  ├──────────────┼──────────────────────────────────────────┤
  │ 11           │ (unused — validated empty in FillBitmap   │
  │              │  FromTreeScan)                            │
  └──────────────┴──────────────────────────────────────────┘

  All three paths end with entries in the RoaringBitmap.
  The dispatch is resolved once in ResolveTermSources(),
  not per-execution. The IL emitter selects the right
  QueryPrimitives method at compile time.
```

---

## 10. RoaringBitmap Container Types

```
  Entry ID space is split into 64K-sized containers:
  Container key = entryId >> 16
  Container value = entryId & 0xFFFF

  ┌───────────────────────────────────────────────────────────┐
  │                    Container Types                         │
  ├──────────────┬────────────────────────────────────────────┤
  │ Range        │ Contiguous values [start, start+count).    │
  │              │ No data allocation. O(1) Add at edges.     │
  │              │ Created automatically for sequential adds.  │
  │              │ count=65536 → full container.               │
  ├──────────────┼────────────────────────────────────────────┤
  │ ArrayUnsorted│ Append-only ushort[]. O(1) Add.            │
  │              │ Sorted lazily on first read                 │
  │              │ (PrepareForReading).                        │
  ├──────────────┼────────────────────────────────────────────┤
  │ Array        │ Sorted ushort[]. For sparse data            │
  │              │ (cardinality ≤ 4096, up to 8KB).            │
  │              │ SIMD linear scan for Contains().             │
  ├──────────────┼────────────────────────────────────────────┤
  │ Bitmap       │ 8KB fixed bitmap (1024 longs).              │
  │              │ For dense data (> 4096 values).             │
  │              │ Bit-level AND/OR/ANDNOT via SIMD.           │
  └──────────────┴────────────────────────────────────────────┘

  Set operations are destructive on the right-hand side:
  OrWith steals containers from the RHS (zero-copy transfer).
  AndWith/AndNotWith may sort RHS arrays in-place.
  After a set op, the RHS is consumed — only Clear() revives it.

  In DEBUG builds, a _consumed flag asserts on any access
  to a consumed bitmap, catching double-use bugs immediately.
```

---

## 11. Cache Key Composition

Why the same query text can have multiple compiled plans:

```
  Query: "from Users where Age > $minAge and Name = $name"

  Execution 1: minAge=90, name="Oren"
  ┌──────────────────────────────────────────────────────────┐
  │ Cardinality: Age>90 = 50,  Name='Oren' = 200            │
  │ Sort: [Age>90, Name='Oren']                              │
  │ Ordering: 0x0001  (Age first)                            │
  │ TypeSig:  0x0004  (long, string)                         │
  │                                                          │
  │ Plan: Fill(Age>90) → And(Name='Oren')                    │
  │ Cache key: ("from Users...", 0x0001, 0x0004)             │
  └──────────────────────────────────────────────────────────┘

  Execution 2: minAge=1, name="Oren"
  ┌──────────────────────────────────────────────────────────┐
  │ Cardinality: Age>1 = 99000,  Name='Oren' = 200          │
  │ Sort: [Name='Oren', Age>1]                               │
  │ Ordering: 0x0010  (Name first)                           │
  │ TypeSig:  0x0004  (string, long)  ← note: also swapped  │
  │                                                          │
  │ Plan: Fill(Name='Oren') → And(Age>1)                     │
  │ Cache key: ("from Users...", 0x0010, 0x0004)             │
  └──────────────────────────────────────────────────────────┘

  Both plans coexist in PlanCache. Future executions with
  similar cardinality distributions hit the right plan.
```

---

## 12. Spatial / Vector Post-Filter Architecture

Spatial and vector clauses are separated from the main bitmap pipeline because they can't be expressed as posting-list intersections:

```
  Query: WHERE Name = 'hotel' AND spatial.within(Location, circle(10, 32.5, 34.9))

  ┌──────────────────────────────────┐
  │ Main bitmap pipeline (AND chain)  │
  │                                   │
  │ [0] FillFromPostings Name='hotel' │
  │     → bitmap: {1, 5, 12, 30, 87} │
  └──────────────┬───────────────────┘
                 │
                 ▼
  ┌──────────────────────────────────┐
  │ Post-filter: Spatial             │
  │                                   │
  │ For each entry in bitmap:         │
  │   spatialField.ReadCircle(...)    │
  │   if point within circle:         │
  │     keep                          │
  │   else:                           │
  │     remove from bitmap            │
  │                                   │
  │ Result: {5, 30}                   │
  └──────────────┬───────────────────┘
                 │
                 ▼
  ┌──────────────────────────────────┐
  │ SortingMatch (if ORDER BY)       │
  │                                   │
  │ Uses bitmap.Contains() to check   │
  │ entries while walking CompactTree │
  │ in sort-field order               │
  └──────────────────────────────────┘

  Note: In OR queries, spatial clauses stay in the main
  clause list (not extracted to post-filter) and go through
  the general IQueryMatch dispatch.
```

---

## 13. Comparison: Old Query Lifecycle vs New

```
  ┌─────────────── OLD (Corax 1.x) ──────────────────────────┐
  │                                                            │
  │  Query arrives                                             │
  │    ├─ Parse RQL AST                        (every time)    │
  │    ├─ CoraxQueryBuilder.ToCoraxQuery()     (every time)    │
  │    │    ├─ Build generic type tree          (every time)    │
  │    │    ├─ Optimizer: flatten, reorder      (every time)    │
  │    │    └─ JIT each new generic combo       (first time)    │
  │    ├─ Execute via recursive Fill/AndWith   (every time)    │
  │    │    ├─ Merge-join at each tree level                    │
  │    │    ├─ MemoizationMatch for shared nodes               │
  │    │    └─ SortingMatch.Erasure to hide types              │
  │    └─ Return results                                       │
  │                                                            │
  │  Cost per execution: O(parse + build + optimize + execute) │
  │  Cached: nothing                                           │
  └────────────────────────────────────────────────────────────┘

  ┌─────────────── NEW (Corax 2.0) ──────────────────────────┐
  │                                                            │
  │  Query arrives                                             │
  │    ├─ PlanCache.TryGetTemplate()                           │
  │    │    HIT  → skip parse                  (amortized O(1))│
  │    │    MISS → ParseTemplate()             (once per text)  │
  │    ├─ PopulateClauseValues()               (every time)    │
  │    ├─ EstimateCardinality + Sort           (every time)    │
  │    ├─ PlanCache.Get(ordering, typeSig)                     │
  │    │    HIT  → skip plan+compile          (amortized O(1)) │
  │    │    MISS → EmitPlan + EmitDelegate    (once per shape)  │
  │    ├─ ResolveMatches/TermSources           (every time)    │
  │    ├─ Execute: single flat bitmap pass     (every time)    │
  │    │    ├─ No recursion, no merge-join                     │
  │    │    ├─ Early exit on empty intermediate                │
  │    │    └─ Adaptive entry scan if bitmap small             │
  │    └─ Return results                                       │
  │                                                            │
  │  Cost per execution: O(populate + resolve + execute)       │
  │  Cached: ClauseTemplate + CompiledPlan (IL delegate)       │
  └────────────────────────────────────────────────────────────┘
```
