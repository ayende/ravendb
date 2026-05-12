# Direct Tree Scan: Unified Design

## Problem Statement

The bitmap pipeline always materializes all matching entries into a `RoaringBitmap`,
then optionally walks a sort tree and `Contains`-filters each entry. This is optimal
when:
- Multiple selective posting lists need intersection (bitmap AND is fast)
- The result set is large relative to the index
- No ORDER BY, or ORDER BY on a different field than WHERE

But it's suboptimal when:
- A tree walk can be bounded (range clause, compound prefix, or just `_take`)
- Additional filters are simple enough for entry scan
- The estimated scan count is small relative to the bitmap pipeline cost

In these cases, walking the tree directly and checking predicates per entry is cheaper
than building a full bitmap.

## Core Insight

The existing `CheckAndMaybeEntryScan` IL heuristic and this optimization are the same
question asked at different times:

| | CheckAndMaybeEntryScan | Direct Tree Scan |
|---|---|---|
| **When** | Execution time (mid-flight) | Plan time (before execution) |
| **Input** | Actual intermediate bitmap count | Estimated cardinality from index stats |
| **Decision** | Switch remaining AND ops to entry scan | Skip bitmap pipeline entirely |
| **Cost model** | `bitmapCount × 64 < postingListSize` | Same constants, same logic |

Both ask: **is reading N individual entries cheaper than decoding posting lists and
doing bitmap set operations?**

They share:
- Cost constants (`EntryScanCostMultiplier`, `EntryScanCountThreshold`)
- Eligibility check (which clause types can be entry-scanned)
- The cost comparison formula

The design unifies them into a single cost model evaluated at two levels:
1. **Plan time** — skip the bitmap pipeline when the estimate is clearly favorable
2. **Execution time** — the existing `CheckAndMaybeEntryScan` IL handles cases where
   the actual bitmap turns out smaller than estimated

## What This Produces

A new `DirectScanMatch` that:
1. Walks a "driving tree" in order (bounded by seek/range, or from the start)
2. For each entry, checks residual predicates via stored field reads
3. Collects matches, stopping at `_take` if set
4. If ORDER BY matches the driving tree's order, results are pre-sorted
   (no `SortingMatch` wrapper needed)
5. Reports telemetry: tree scan cost, entry scan cost, entries scanned/filtered/rejected

This match can replace:
- `CompiledQueryMatch` + `SortingMatch` (when ORDER BY matches the tree)
- `CompiledQueryMatch` alone (when no ORDER BY but tree walk is cheaper)

## Driving Tree Selection

The "driving tree" is the tree that provides the primary iteration order.
It can be:

### A. Simple field tree (sort field = WHERE field)

The field's own CompactTree or Lookup tree. Bounded by range clauses.

| Query | Driving tree | Seek | Stop |
|-------|-------------|------|------|
| `WHERE Foo > 100 ORDER BY Foo ASC` | Foo's Lookup tree | Seek forward to 100 | End of tree or `_take` |
| `WHERE Foo < 100 ORDER BY Foo DESC` | Foo's Lookup tree | Seek backward to 100 | End of tree or `_take` |
| `WHERE Foo BETWEEN 50 AND 150 ORDER BY Foo ASC` | Foo's Lookup tree | Seek to 50 | Stop at 150 or `_take` |
| `WHERE Foo BETWEEN 50 AND 150 ORDER BY Foo DESC` | Foo's Lookup tree | Seek backward to 150 | Stop at 50 or `_take` |
| `WHERE Foo BETWEEN 50 AND 150` (no ORDER BY) | Foo's Lookup tree | Seek to 50 | Stop at 150 |
| `ORDER BY Foo LIMIT 10` (no WHERE) | Foo's Lookup tree | Start of tree | `_take` |

### B. Compound field tree

The compound tree `compound(field1, field2)`. Compound keys sort by field1 then field2.

| Query | Driving tree | Seek | Stop |
|-------|-------------|------|------|
| `WHERE Name = 'X' ORDER BY Birthday ASC` | compound(Name, Birthday) | Prefix 'X' (StartsWith) | End of prefix or `_take` |
| `WHERE Name = 'X' ORDER BY Birthday DESC` | compound(Name, Birthday) | Prefix 'X' (backward) | Start of prefix or `_take` |
| `WHERE Name = 'X' AND Birthday > D ORDER BY Birthday` | compound(Name, Birthday) | Compound key ('X', D) | End of prefix or `_take` |
| `WHERE Name = 'X' ORDER BY Name, Birthday` | compound(Name, Birthday) | Prefix 'X' | End of prefix or `_take` |
| `ORDER BY Name, Birthday LIMIT 10` | compound(Name, Birthday) | Start of tree | `_take` |
| `ORDER BY Name LIMIT 10` | compound(Name, Birthday) | Start of tree | `_take` |
| `WHERE Name >= 'X' ORDER BY Name, Birthday` | compound(Name, Birthday) | Seek to 'X' | End of tree or `_take` |
| `WHERE Name >= 'X' ORDER BY Name ASC` | compound(Name, Birthday) | Seek to 'X' | End of tree or `_take` |

ORDER BY on field1 alone works because all entries with the same field1 value are
grouped contiguously in the compound tree. ORDER BY on both fields is the native
sort order of the compound tree.

### C. Sort field tree (sort field != WHERE field, no compound)

When ORDER BY is on a different field than WHERE, and no compound exists,
the sort field tree can still be used as the driving tree. Entry scan handles
the WHERE predicates. This is the inverse of `SortUsingIndexFromBitmap` — instead
of building a bitmap and Contains-filtering, we walk the sort tree and entry-scan.

| Query | Driving tree | Entry scan |
|-------|-------------|------------|
| `WHERE Status = 'active' ORDER BY Name LIMIT 20` | Name's tree | Check Status = 'active' per entry |

This is only efficient when the cost model says so — when `_take` is small and
the WHERE predicates aren't too selective relative to the total entry count.

## Residual Clauses and Entry Scan Eligibility

Clauses not covered by the driving tree become "residual predicates" checked
via entry scan for each candidate entry.

**Entry-scan eligible** (can be checked by reading stored fields):
- Equals, NotEquals
- GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, Between
- OrGroup, AndGroup (of eligible sub-clauses — just flatten and check each)
- IN, AllIn (set membership check against resolved term list)
- Exists (reader.FindNext succeeds for the field)
- StartsWith, EndsWith, Contains (check against stored term — iterate all terms
  for multi-term fields. **Only valid on exact fields or when the query value is
  analyzed with the same analyzer.** For analyzed fields, the stored value is
  the analyzed form; the entry scan must analyze the query value identically.)
- Regex (regex match against stored term value — same analyzer caveat)

**NOT entry-scan eligible:**
- Search (needs full-text scoring with TF/IDF, term frequency tracking)
- Spatial (could theoretically work — stored coordinates are accessible — but
  the spatial math adds complexity for marginal benefit. Not worth it initially.)
- Vector (needs vector similarity computation against the vector index)

For analyzed fields (StartsWith, EndsWith, Contains, Regex): the query value must
be run through the field's analyzer before comparison. For multi-term fields
(tokenized), iterate all stored terms and check if ANY term matches the predicate.

If any residual clause is not eligible, the optimization cannot be used —
fall back to the bitmap pipeline.

## Cost Model

### Constants (shared with CheckAndMaybeEntryScan)

```
EntryScanCountThreshold = 32,768    // max candidates for entry scan
EntryScanCostMultiplier = 64        // one entry read ≈ 64 posting list decodes
```

### Plan-Time Estimate

The key question: how many tree entries must we scan to produce the results?

```
driving_tree_entries = estimated entries in the bounded range
                       (from cardinality estimation on the driving clause)

// How selective are the residual predicates?
// Use the minimum cardinality among residual clauses — the most selective one
// determines the pass rate.
residual_pass_rate = min_residual_cardinality / numberOfEntries

// How many tree entries to scan to find _take results?
// If 2% of entries pass the residual, we need to scan ~50× more than _take.
entries_to_scan = _take > 0
    ? _take / residual_pass_rate     // estimated scan to fill the page
    : driving_tree_entries           // no limit — scan the whole range

// Cap at driving tree size (can't scan more than the tree has)
entries_to_scan = min(entries_to_scan, driving_tree_entries)

// Direct scan cost
direct_cost = entries_to_scan × EntryScanCostMultiplier

// Bitmap pipeline cost: build bitmap (decode posting lists + AND) + sort walk
bitmap_cost = sum of posting list cardinalities for all clauses

use_direct_scan = entries_to_scan < EntryScanCountThreshold
                  AND direct_cost < bitmap_cost
```

When there are no residual predicates, `residual_pass_rate = 1.0` (every tree entry
is a match), so `entries_to_scan = _take`. This is the best case for direct scan.

When residual predicates are very selective (e.g., cardinality 10 out of 10M),
`residual_pass_rate = 0.000001`, so `entries_to_scan = 25 / 0.000001 = 25M` — far
more than the tree has. The cost model correctly rejects direct scan.

### Execution-Time Fallback

The existing `CheckAndMaybeEntryScan` IL remains for cases where:
- Plan-time estimates were wrong (cardinality estimation is approximate)
- The bitmap pipeline was chosen but the actual intermediate bitmap turns
  out small enough for entry scan

This is the second level of the same optimization, applied mid-flight.

## What Can Be Decided Per Query Text vs Per Execution

| Per query text (cached on ClauseTemplate) | Per execution (per parameter set) |
|---|---|
| Clause structure, field names, clause types | Parameter values |
| Compound field pairings (field1, field2, bindingId) | Cardinality estimates |
| Which clauses are entry-scan eligible | Residual selectivity |
| Residual predicate structure (ScanPredicateInfo[]) | Cost comparison result |
| Driving tree candidates (structural match) | Seek bound values |
| Generated predicate-check delegate (if IL-compiled) | The actual decision: direct scan or bitmap |

The **structural eligibility** (can this query shape potentially use direct scan?) is
determined once per query text and cached. The **cost-based decision** (should this
specific execution use direct scan?) depends on parameter values and index statistics,
so it's per-execution.

## Detection Flow

```
BuildAndCompile() → CompiledQueryMatch + QueryExecution (plan)
GetSortMetadata() → OrderMetadata[] (ORDER BY fields)

┌─ TryDirectTreeScan(plan, orderByFields, builderParams) ─────────────────┐
│                                                                          │
│  1. Identify driving tree candidates:                                    │
│     a. For each ORDER BY field: check if a range clause bounds it       │
│     b. For each (clause, ORDER BY) pair: check compound field exists    │
│     c. ORDER BY alone with _take: unbounded tree walk                   │
│     d. If no ORDER BY: check if any range clause bounds a tree walk     │
│                                                                          │
│  2. For best candidate, split clauses into:                              │
│     - driving clause(s): covered by the tree walk                       │
│     - residual clause(s): need entry scan                               │
│                                                                          │
│  3. Check residual eligibility:                                          │
│     All residual clauses must be entry-scan eligible                    │
│     No boosting on any clause                                           │
│                                                                          │
│  4. Cost comparison:                                                     │
│     Estimate entries_to_scan (from cardinality + residual selectivity)   │
│     Estimate bitmap_cost (sum of posting list cardinalities)            │
│     If direct scan is not clearly cheaper → skip, use bitmap            │
│                                                                          │
│  5. If all checks pass:                                                  │
│     Build DirectScanMatch with:                                         │
│       - tree iterator (simple or compound, with seek bounds)            │
│       - residual predicates (ScanPredicateInfo[])                       │
│       - generated predicate-check delegate (or interpreted)             │
│       - _take limit                                                      │
│       - dedup bitmap (for multi-value fields)                            │
│     If ORDER BY matches tree order: skip SortingMatch wrapper           │
│     Return true + the DirectScanMatch                                   │
│                                                                          │
│  6. If checks fail:                                                      │
│     Return false → use bitmap pipeline + SortUsingIndexFromBitmap       │
│     Log reason on CompiledQueryMatch for diagnostic telemetry            │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
```

## DirectScanMatch Implementation

```csharp
public class DirectScanMatch : IQueryMatch
{
    // Driving tree iteration
    private SortedIndexReader<TDirection> _reader;
    private RoaringBitmap _emittedBitmap;    // dedup for multi-value fields

    // Residual predicate checking — either interpreted or compiled
    private ScanPredicateInfo[] _residualPredicates;  // for interpreted path
    private CheckPredicateDelegate _compiledCheck;     // for compiled path (Phase 2+)
    private long[] _longParams;
    private double[] _doubleParams;
    private Slice[] _sliceParams;
    private long[] _fieldRootPages;

    // Limits
    private int _take;
    private long _totalMatched;

    // Entry reader
    private IndexSearcher _searcher;

    // Telemetry (always collected)
    private long _treeEntriesScanned;
    private long _entriesPassedFilter;
    private long _entriesRejected;
    private long _treeScanTicks;
    private long _entryScanTicks;
    private string _stoppedReason;

    // Diagnostic metadata
    private string _drivingTreeName;
    private string _drivingClause;
    private string _seekBound;
    private string _direction;
    private string _residualDescription;
    private string _reason;          // why direct scan was chosen
    private string _skippedReason;   // or why it was skipped (on CompiledQueryMatch)

    public int Fill(Span<long> matches)
    {
        int count = 0;
        Span<long> batch = stackalloc long[256];

        while (count < matches.Length)
        {
            long t0 = Stopwatch.GetTimestamp();
            int read = _reader.Read(batch);
            _treeScanTicks += Stopwatch.GetTimestamp() - t0;

            if (read == 0) { _stoppedReason = "TreeExhausted"; break; }
            _treeEntriesScanned += read;

            for (int i = 0; i < read && count < matches.Length; i++)
            {
                long entryId = batch[i];

                // Dedup: multi-value fields produce duplicate entry IDs
                if (_emittedBitmap.Contains(entryId))
                    continue;

                if (_residualPredicates != null)
                {
                    long t1 = Stopwatch.GetTimestamp();
                    var reader = _searcher.GetEntryTermsReader(entryId);
                    bool passed = _compiledCheck != null
                        ? _compiledCheck(ref reader, _longParams, _doubleParams, _sliceParams, _fieldRootPages)
                        : CheckAllPredicates(ref reader);
                    _entryScanTicks += Stopwatch.GetTimestamp() - t1;

                    if (!passed) { _entriesRejected++; continue; }
                }

                _emittedBitmap.Add(entryId);
                _entriesPassedFilter++;
                matches[count++] = entryId;
            }

            if (_take > 0 && _totalMatched + count >= _take)
            {
                _stoppedReason = $"_take({_take})";
                break;
            }
        }
        _totalMatched += count;
        return count;
    }

    private bool CheckAllPredicates(ref EntryTermsReader reader)
    {
        // Interpreted path: loop over predicates.
        // For each predicate:
        //   reader.Reset()
        //   if (!reader.FindNext(fieldRootPages[pred.fieldIndex])) → handle missing
        //   For multi-term fields: iterate reader.FindNext in a loop
        //   Compare reader.CurrentLong / CurrentDouble / Current.Decoded()
        //   against the parameter value
        foreach (var pred in _residualPredicates)
        {
            if (!EvaluatePredicate(ref reader, pred))
                return false;
        }
        return true;
    }
}
```

### Compiled predicate check (future optimization)

For frequently-executed queries, the predicate check can be IL-compiled into a
`DynamicMethod` — same pattern as the bitmap pipeline's entry scan. The predicate
structure is known at plan time and cached on the `CompiledPlan`. This eliminates
the per-predicate loop and virtual dispatch.

```csharp
delegate bool CheckPredicateDelegate(
    ref EntryTermsReader reader,
    long[] longParams, double[] doubleParams,
    Slice[] sliceParams, long[] fieldRootPages);
```

The generated IL inlines all comparisons, same as `EmitEntryScan` in `QueryILEmitter`.
Cache the delegate on `CompiledPlan` alongside the bitmap delegate.

## TotalResults / Count

`DirectScanMatch` doesn't know the total count upfront (it hasn't scanned the entire
tree).

**When SkipStatistics = true**: Report `Confidence = Low`, count = number of results
returned so far. Client shows "25+ results" instead of "exactly 1,234 results."

**When SkipStatistics = false** (client wants exact total): Run the bitmap pipeline
in parallel for count. `CompiledQueryMatch` is still compiled and cached — execute it,
read `bitmap.Count`, but use `DirectScanMatch` for the actual sorted result retrieval.
The bitmap pipeline cost is paid for count, but the sort walk cost is avoided — the
direct scan produces sorted results without `SortUsingIndexFromBitmap`.

## Interaction with Existing Pipeline

### With ORDER BY (replaces CompiledQueryMatch + SortingMatch)

```
                ┌──────────────────────────┐
                │    DirectScanMatch        │
                │    (walks sort tree +     │
                │     entry scan filter)    │
                │    Results: pre-sorted    │
                └────────────┬─────────────┘
                             │
                             ▼
                    Fill() → sorted results
                    (no SortingMatch needed)
```

### Without ORDER BY (replaces CompiledQueryMatch)

```
                ┌──────────────────────────┐
                │    DirectScanMatch        │
                │    (walks bounded tree +  │
                │     entry scan filter)    │
                │    Results: tree order    │
                └────────────┬─────────────┘
                             │
                             ▼
                    Fill() → results in tree order
                    (no bitmap, no SortingMatch)
```

### With ORDER BY + exact count needed

```
                ┌──────────────────────────┐
                │    CompiledQueryMatch     │  ← count only (bitmap.Count)
                │    (bitmap pipeline)      │
                └──────────────────────────┘

                ┌──────────────────────────┐
                │    DirectScanMatch        │  ← result retrieval (sorted)
                │    (walks sort tree +     │
                │     entry scan filter)    │
                └────────────┬─────────────┘
                             │
                             ▼
                    Fill() → sorted results
                    TotalResults from bitmap.Count
```

### Fallback (bitmap pipeline, unchanged)

```
                ┌──────────────────────────┐
                │    CompiledQueryMatch     │
                │    (bitmap pipeline)      │
                └────────────┬─────────────┘
                             │
                     ┌───────▼───────┐
                     │  SortingMatch  │ (if ORDER BY)
                     │  walks tree +  │
                     │  Contains()    │
                     └───────┬───────┘
                             │
                             ▼
                    Fill() → sorted results
```

## Compound Key Construction

For compound tree queries, the seek/range keys must be built in the same binary
format used during indexing:

```
[field1_bytes][field2_bytes][field1_len_as_byte]
```

### Building seek keys at query time

For `WHERE Name = 'Corax' AND Birthday > 2020-01-01 ORDER BY Birthday`:

**Low key**: `[analyze("Corax")][SwapBytes(2020-01-01.Ticks)][len("Corax")]`
**High key**: `[analyze("Corax")][0xFF × 8][len("Corax")]`

The `analyze()` step uses field1's analyzer (from `IndexFieldsMapping`).
The `SwapBytes()` step uses the same big-endian encoding as `AppendFieldValue`.

### Utility method

```csharp
static Slice BuildCompoundKey(
    IndexSearcher searcher,
    FieldMetadata field1Meta,
    string field1Value,
    ReadOnlySpan<byte> field2Bytes,   // empty = prefix only (no field2 bound)
    ByteStringContext allocator)
{
    var analyzed = searcher.EncodeAndApplyAnalyzer(field1Meta, field1Value);
    int prefixLen = analyzed.Size;
    int totalLen = prefixLen + field2Bytes.Length + 1; // +1 for trailing length byte

    // Allocate from ByteStringContext — no managed array
    var scope = allocator.Allocate(totalLen, out ByteString bs);
    var dest = new Span<byte>(bs.Ptr, totalLen);
    analyzed.AsReadOnlySpan().CopyTo(dest);
    field2Bytes.CopyTo(dest.Slice(prefixLen));
    dest[^1] = (byte)prefixLen;

    return new Slice(bs);
}
```

## Timings & Telemetry

### What DirectScanMatch reports

```json
{
  "Operation": "DirectScan",
  "Parameters": {
    "DrivingTree": "compound(Name, Birthday)",
    "DrivingClause": "Name = 'Corax'",
    "SeekBound": "'Corax' (prefix, validatePostfixLen)",
    "TreeDirection": "Forward",
    "ResidualPredicates": "Status = 'active', Age >= 21",
    "Reason": "entries_to_scan(1250) × 64 < bitmap_cost(45000)",
    "TreeScan_ms": "0.85",
    "TreeEntriesScanned": "312",
    "EntryScans_ms": "0.42",
    "EntriesPassedFilter": "23",
    "EntriesRejected": "289",
    "TotalResults": "23",
    "StoppedAt": "_take(25)"
  }
}
```

### Telemetry fields

| Field | Type | Description |
|-------|------|-------------|
| `DrivingTree` | string | The tree being walked — field name or `compound(f1, f2)` |
| `DrivingClause` | string | The clause that bounds the tree walk |
| `SeekBound` | string | Where the tree iterator seeks to (value + inclusive/exclusive) |
| `TreeDirection` | string | `Forward` or `Backward` |
| `ResidualPredicates` | string | Comma-separated list of entry-scan predicates |
| `Reason` | string | Why direct scan was chosen — the cost comparison |
| `TreeScan_ms` | double | Time spent walking the tree |
| `EntryScans_ms` | double | Time spent reading entries and checking predicates |
| `TreeEntriesScanned` | long | Total entry IDs produced by the tree walk |
| `EntriesPassedFilter` | long | Entries that passed all residual predicates |
| `EntriesRejected` | long | Entries that failed at least one residual predicate |
| `StoppedAt` | string | Why iteration stopped: `_take(N)`, `TreeExhausted`, `RangeBound` |

### Telemetry is always collected

The cost is two `Stopwatch.GetTimestamp()` calls per tree batch (not per entry)
plus simple counter increments. Negligible compared to I/O cost of reading entries.
`Inspect()` is only called when `include timings()` is active, but counters are
always ready.

### When DirectScan is NOT chosen

Log the reason on `CompiledQueryMatch.Inspect()`:

```json
{
  "Operation": "CompiledQuery",
  "Parameters": {
    "DirectScanSkipped": "entries_to_scan(250000) > threshold(32768); residual Status has cardinality 10/10M",
    ...
  }
}
```

### How it appears in Studio

When direct scan is used (sort eliminated):
```
DirectScan
  DrivingTree: Age (Lookup<Int64LookupKey>)
  DrivingClause: Age > 21
  SeekBound: 21 (exclusive, forward)
  ResidualPredicates: (none)
  TreeScan_ms: 0.12
  TreeEntriesScanned: 25
  EntriesPassedFilter: 25
  StoppedAt: _take(25)
```

When bitmap pipeline is used instead:
```
SortingMatch
  └── CompiledQuery
        Explain: Fill(bitmap[0], ctx.TermSources[0]); ...
        DirectScanSkipped: entries_to_scan(250000) > threshold(32768)
        Op0_ms: 0.042
        Op0_count: 1500
```

The user sees at a glance which path was taken, and when DirectScan was skipped, why.

## Implementation Phases

### Phase 1: Simple field — ORDER BY matches range WHERE clause
- `WHERE Foo > $x ORDER BY Foo` (and all ASC/DESC, GT/GTE/LT/LTE/BETWEEN variants)
- Single field, single clause, no residual predicates
- Uses `SortedIndexReader` with seek (already partially implemented)
- Skip `SortingMatch` when ORDER BY matches
- Cost: minimal new code, biggest impact for common pagination patterns

### Phase 2: Residual predicates via entry scan
- `WHERE Foo > $x AND Bar = $y ORDER BY Foo`
- New `DirectScanMatch` implementation with interpreted entry scan
- Handles: Equals, NotEquals, ranges, Between, IN/AllIn, Exists,
  StartsWith/EndsWith/Contains/Regex (exact fields or with analyzer)
- Cost model includes residual selectivity

### Phase 3: Compound field — prefix scan
- `WHERE Name = $n ORDER BY Birthday` with compound(Name, Birthday)
- Uses `StartWithQuery(compoundField, prefix, validatePostfixLen: true)`
- Already partially implemented (current commit)

### Phase 4: Compound field — range within prefix
- `WHERE Name = $n AND Birthday > $d ORDER BY Birthday`
- Compound key construction for seek bounds
- Uses `TermsRangeProvider` on the compound tree

### Phase 5: Case C — sort tree + entry scan for unrelated WHERE
- `WHERE Status = 'active' ORDER BY Name LIMIT 20`
- Walk Name tree, entry-scan Status per entry
- Only used when cost model says it's cheaper than bitmap

### Phase 6: No ORDER BY — range as direct tree walk
- `WHERE Foo BETWEEN $a AND $b` without ORDER BY
- Walk Foo's tree from $a to $b instead of building bitmap
- Only beneficial when the range is small relative to bitmap cost

### Phase 7: IL-compiled predicate check
- Generate `DynamicMethod` for the predicate check (same pattern as IL entry scan)
- Cache on `CompiledPlan` alongside bitmap delegate
- Eliminates per-predicate loop overhead for hot queries

### Phase 8: Cost model unification
- Share constants, eligibility, and comparison logic with `CheckAndMaybeEntryScan`
- Plan-time estimation drives the choice
- Execution-time `CheckAndMaybeEntryScan` remains as fallback

## Open Questions

1. **Multi-value fields**: Documents with multiple values in a field produce multiple
   entries in the tree for the same document ID. `DirectScanMatch` uses a small
   `RoaringBitmap` for dedup (same as `SortUsingIndexFromBitmap`). The bitmap tracks
   only the `_take` entries — small and fast.

2. **Null handling**: Compound keys encode null as zero bytes (field length = 0).
   `WHERE Name = NULL` needs special handling: either use the null posting list for
   the individual field, or seek to the zero-length prefix in the compound tree with
   a range query bounded by length byte = 0.

3. **Numeric field1 in compound keys**: Compound keys for numeric field1 use
   `Bits.SwapBytes(long)` encoding. The query value needs the same encoding for the
   prefix. Type dispatch in `BuildCompoundKey` based on the field's type metadata.

4. **Analyzer consistency**: The compound key uses field1's analyzer during indexing.
   The query must apply the same analyzer. `EncodeAndApplyAnalyzer` must be called
   with field1's `FieldMetadata`, not the compound field's metadata. For multi-term
   analyzers (tokenization), the entry scan must iterate all stored terms per field.

5. **Cache key impact**: When `DirectScanMatch` is used, the compiled bitmap pipeline
   delegate is still cached (and used for count when `SkipStatistics = false`). The
   detection decision (direct vs bitmap) is per-execution based on cardinality, so
   different parameter values for the same query text can take different paths.
