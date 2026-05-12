# Direct Tree Scan: Unified Design

## Problem Statement

The bitmap pipeline always materializes all matching entries into a `RoaringBitmap`,
then optionally walks a sort tree and `Contains`-filters each entry. This is optimal
when:
- Multiple selective posting lists need intersection (bitmap AND is fast)
- The result set is large relative to the index
- No ORDER BY, or ORDER BY on a different field than WHERE

But it's suboptimal when:
- A single range clause or compound prefix bounds a tree walk
- `_take` is small (first-page queries)
- The bounded tree produces fewer entries than the cheapest posting list
- Additional filters are simple enough for entry scan

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

A new `DirectScanMatch` (or similar) that:
1. Walks a "driving tree" in order (bounded by seek/range)
2. For each entry, checks residual predicates via stored field reads
3. Collects matches, stopping at `_take` if set
4. If ORDER BY matches the driving tree's order, results are pre-sorted
   (no `SortingMatch` wrapper needed)

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
| `WHERE Foo BETWEEN 50 AND 150 ORDER BY Foo` | Foo's Lookup tree | Seek to 50 | Stop at 150 or `_take` |
| `WHERE Foo BETWEEN 50 AND 150` (no ORDER BY) | Foo's Lookup tree | Seek to 50 | Stop at 150 |
| `ORDER BY Foo LIMIT 10` (no WHERE) | Foo's Lookup tree | Start of tree | `_take` |

### B. Compound field tree

The compound tree `compound(field1, field2)`. Compound keys sort by field1 then field2.

| Query | Driving tree | Seek | Stop |
|-------|-------------|------|------|
| `WHERE Name = 'X' ORDER BY Birthday` | compound(Name, Birthday) | Prefix 'X' (StartsWith) | End of prefix or `_take` |
| `WHERE Name = 'X' AND Birthday > D ORDER BY Birthday` | compound(Name, Birthday) | Compound key ('X', D) | End of prefix or `_take` |
| `WHERE Name = 'X' ORDER BY Name, Birthday` | compound(Name, Birthday) | Prefix 'X' | End of prefix or `_take` |
| `ORDER BY Name, Birthday LIMIT 10` | compound(Name, Birthday) | Start of tree | `_take` |
| `WHERE Name >= 'X' ORDER BY Name, Birthday` | compound(Name, Birthday) | Seek to 'X' | End of tree or `_take` |

### C. Sort field tree (sort field != WHERE field, no compound)

When ORDER BY is on a different field than WHERE, and no compound exists,
the sort field tree can still be used as the driving tree. Entry scan handles
the WHERE predicates.

| Query | Driving tree | Entry scan |
|-------|-------------|------------|
| `WHERE Status = 'active' ORDER BY Name LIMIT 20` | Name's tree | Check Status = 'active' per entry |

This is only efficient when `_take` is small and the predicate isn't too
selective (otherwise bitmap is better).

## Residual Clauses and Entry Scan Eligibility

Clauses not covered by the driving tree become "residual predicates" checked
via entry scan for each candidate entry.

**Entry-scan eligible** (can be checked by reading stored fields):
- Equals, NotEquals
- GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, Between
- OrGroup (of eligible clauses)

**NOT entry-scan eligible** (require posting list / tree scan / special logic):
- Search (full-text)
- Spatial, Vector
- IN, AllIn (multiple terms)
- StartsWith, EndsWith, Contains, Regex
- Exists
- AndGroup

If any residual clause is not eligible, the optimization cannot be used —
fall back to the bitmap pipeline.

## Cost Model

### Constants (shared with CheckAndMaybeEntryScan)

```
EntryScanCountThreshold = 32,768    // max candidates for entry scan
EntryScanCostMultiplier = 64        // one entry read ≈ 64 posting list decodes
```

### Plan-Time Estimate

```
driving_tree_entries = estimated entries in the bounded range
                       (from cardinality estimation on the driving clause)
                       capped by _take if set

residual_scan_cost = driving_tree_entries × (1 + number_of_residual_predicates)
                     × EntryScanCostMultiplier

bitmap_cost = sum of posting list sizes for all clauses
              (the cheapest-first AND chain cost)

use_direct_scan = driving_tree_entries < EntryScanCountThreshold
                  AND residual_scan_cost < bitmap_cost
```

When `_take` is set and small (e.g., 25), `driving_tree_entries` is capped at
`_take / selectivity_estimate` — if the driving tree has 10K entries but only
1% pass the residual predicates, we'd scan ~2,500 entries to get 25 results.

### Execution-Time Fallback

The existing `CheckAndMaybeEntryScan` IL remains for cases where:
- Plan-time estimates were wrong (cardinality estimation is approximate)
- The bitmap pipeline was chosen but the actual intermediate bitmap turns
  out small enough for entry scan

This is the second level of the same optimization, applied mid-flight.

## Detection Flow

```
BuildAndCompile() → CompiledQueryMatch + QueryExecution (plan)
GetSortMetadata() → OrderMetadata[] (ORDER BY fields)

┌─ TryDirectTreeScan(plan, orderByFields, builderParams) ─────────────────┐
│                                                                          │
│  1. Identify driving tree candidates:                                    │
│     a. For each ORDER BY field: check if a range clause bounds it       │
│     b. For each (clause, ORDER BY) pair: check compound field exists    │
│     c. If no ORDER BY: check if any range clause bounds a tree walk     │
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
│     Estimate driving_tree_entries (from cardinality, capped by _take)   │
│     Estimate bitmap_cost (sum of posting list cardinalities)            │
│     If direct scan is not clearly cheaper → skip, use bitmap            │
│                                                                          │
│  5. If all checks pass:                                                  │
│     Build DirectScanMatch with:                                         │
│       - tree iterator (simple or compound, with seek bounds)            │
│       - residual predicates (ScanPredicateInfo[])                       │
│       - _take limit                                                      │
│     If ORDER BY matches tree order: skip SortingMatch wrapper           │
│     Return true + the DirectScanMatch                                   │
│                                                                          │
│  6. If checks fail:                                                      │
│     Return false → use bitmap pipeline + SortUsingIndexFromBitmap       │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
```

## DirectScanMatch Implementation

```csharp
public class DirectScanMatch : IQueryMatch
{
    // Driving tree iteration
    private SortedIndexReader<TDirection> _reader;   // walks the tree
    private long _min, _max;                          // entry ID bounds (optional)

    // Residual predicate checking
    private ScanPredicateInfo[] _residualPredicates;
    private long[] _longParams;
    private double[] _doubleParams;
    private Slice[] _sliceParams;
    private long[] _fieldRootPages;

    // Limits
    private int _take;
    private long _totalScanned;
    private long _totalMatched;

    // Entry reader
    private IndexSearcher _searcher;

    public int Fill(Span<long> matches)
    {
        int count = 0;
        Span<long> batch = stackalloc long[256];

        while (count < matches.Length)
        {
            int read = _reader.Read(batch);
            if (read == 0) break;

            for (int i = 0; i < read && count < matches.Length; i++)
            {
                long entryId = batch[i];
                _totalScanned++;

                if (_residualPredicates == null || _residualPredicates.Length == 0)
                {
                    matches[count++] = entryId;
                    continue;
                }

                // Read entry and check all residual predicates
                var reader = _searcher.GetEntryTermsReader(entryId);
                if (CheckAllPredicates(ref reader))
                    matches[count++] = entryId;
            }

            if (_take > 0 && _totalMatched + count >= _take)
                break;
        }

        _totalMatched += count;
        return count;
    }

    private bool CheckAllPredicates(ref EntryTermsReader reader)
    {
        // Same logic as the entry scan IL path, but interpreted.
        // For each predicate: reader.FindNext(fieldRootPage),
        // compare reader.CurrentLong/CurrentDouble/Current.Decoded()
        // against the parameter value.
        foreach (var pred in _residualPredicates)
        {
            if (!EvaluatePredicate(ref reader, pred))
                return false;
        }
        return true;
    }
}
```

### Key difference from the IL entry scan

The IL entry scan path emits inline comparisons at compile time — no virtual dispatch,
no loop over predicates. `DirectScanMatch.CheckAllPredicates` is interpreted (loops over
`ScanPredicateInfo[]`). This is slightly slower per entry but:

1. It's used when entry count is small (bounded range + `_take`)
2. The overhead of the predicate loop is dwarfed by the I/O cost of reading entries
3. It avoids the complexity of emitting another DynamicMethod

If profiling shows the interpreted path is a bottleneck, a future optimization can
emit a specialized delegate (like the IL entry scan) for `DirectScanMatch`.

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

## TotalResults / Count

`DirectScanMatch` doesn't know the total count upfront (it hasn't scanned the entire
tree). Options:

1. **SkipStatistics = true**: Report `Confidence = Low`, count = number of results
   returned so far. Client shows "25+ results" instead of "exactly 1,234 results."

2. **SkipStatistics = false** (client wants exact total): Fall back to the bitmap
   pipeline which knows the exact count from `bitmap.Count`. Or: run the bitmap
   pipeline for count only, use `DirectScanMatch` for result retrieval.

3. **Hybrid**: Use `DirectScanMatch` for the first page, then on subsequent pages
   (or when count is requested), build the bitmap for the exact total.

For the initial implementation, option 1 is simplest. Option 2 can be added later
by running `CompiledQueryMatch.Count` alongside `DirectScanMatch.Fill()`.

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
    byte[] field2Bytes = null,  // null = no field2 bound (prefix only)
    ByteStringContext allocator)
{
    var analyzed = searcher.EncodeAndApplyAnalyzer(field1Meta, field1Value);
    int prefixLen = analyzed.Size;

    int totalLen = prefixLen + (field2Bytes?.Length ?? 0) + 1; // +1 for trailing length byte
    var buffer = new byte[totalLen];
    analyzed.CopyTo(buffer);
    if (field2Bytes != null)
        field2Bytes.CopyTo(buffer.AsSpan(prefixLen));
    buffer[^1] = (byte)prefixLen;

    Slice.From(allocator, buffer, out var result);
    return result;
}
```

## Implementation Phases

### Phase 1: Simple field — ORDER BY matches range WHERE clause
- `WHERE Foo > $x ORDER BY Foo`
- Single field, single clause, no residual predicates
- Uses `SortedIndexReader` with seek (already implemented)
- Skip `SortingMatch` when ORDER BY matches
- Cost: minimal new code, biggest impact for common patterns

### Phase 2: Simple field — ORDER BY with residual predicates
- `WHERE Foo > $x AND Bar = $y ORDER BY Foo`
- Uses `DirectScanMatch` with entry scan for Bar = $y
- New `DirectScanMatch` implementation needed

### Phase 3: Compound field — prefix scan
- `WHERE Name = $n ORDER BY Birthday` with compound(Name, Birthday)
- Uses `StartWithQuery(compoundField, prefix, validatePostfixLen: true)`
- Already partially implemented (current commit)

### Phase 4: Compound field — range within prefix
- `WHERE Name = $n AND Birthday > $d ORDER BY Birthday`
- Needs compound key construction for seek bounds
- Uses `TermsRangeProvider` on the compound tree

### Phase 5: No ORDER BY — range as direct tree walk
- `WHERE Foo BETWEEN $a AND $b` without ORDER BY
- Walk Foo's tree from $a to $b instead of building bitmap from posting list
- Only beneficial when the range is small

### Phase 6: Cost model unification
- Share constants and eligibility checks with `CheckAndMaybeEntryScan`
- Plan-time estimation drives the choice
- Execution-time `CheckAndMaybeEntryScan` remains as fallback

## Open Questions

1. **Multi-value fields**: If field1 has multiple values per document, the compound tree
   has multiple entries per document. The dedup bitmap in `SortedIndexReader` handles this,
   but `DirectScanMatch` would need the same dedup logic.

2. **Null handling**: Compound keys encode null as zero bytes. A `WHERE Name = NULL`
   query would need a zero-length prefix, which matches everything. Need special handling.

3. **Numeric field1**: Compound keys for numeric field1 use `SwapBytes` encoding. The
   query value needs the same encoding for the prefix. This is straightforward but
   needs type dispatch.

4. **Analyzer mismatch**: The compound key uses field1's analyzer during indexing. The
   query must use the same analyzer. Need to ensure `EncodeAndApplyAnalyzer` is called
   with field1's metadata, not the compound field's metadata.

5. **Cache key impact**: If `DirectScanMatch` bypasses the compiled pipeline, the
   `PlanCache` is unused for that query. The detection decision itself should be
   cached (same query text + similar cardinalities should make the same choice).
