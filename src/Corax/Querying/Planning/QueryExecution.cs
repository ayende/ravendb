using System;
using System.Collections.Generic;
using Corax.Mappings;
using Corax.Querying;
using Voron;

namespace Corax.Querying.Planning;

public class QueryExecution
{
    /// <summary>Bit 30 of <see cref="CompiledPlan.Ordering"/>. Set when any clause carries a boost factor.</summary>
    public const int HasBoostBit = 1 << 30;

    /// <summary>Bit 31 of <see cref="CompiledPlan.Ordering"/>. Set when the sort-driving clause's
    /// cardinality is &lt;= <see cref="Corax.Querying.Matches.SortedDrivingWithTieBreakMatch.MaxGroupSize"/>
    /// (16K). Queries under the cliff can use tie-break sorted scan; queries over it cannot.
    /// Different cardinality buckets get different compiled plans (and different optimization
    /// hints), so a plan cached from a small-cardinality execution isn't reused for a
    /// large-cardinality one that needs a different dispatch path.</summary>
    public const int CardinalityCliffBit = 1 << 31;

    /// <summary>Back-reference to the compiled plan this execution belongs to.
    /// Structural fields (AllNegated, OptimizationFlags, SortDrivingClauseIndex,
    /// compound indices, etc.) live on the plan — not duplicated here.</summary>
    public CompiledPlan Plan;

    /// <summary>Per-execution state — parameter values, cardinality, etc.
    /// Each element carries a back-reference to its <see cref="ClauseInfo"/> via
    /// <see cref="ClauseExecution.Clause"/>, so clause metadata is accessible as
    /// <c>Executions[i].Clause</c> without a separate parallel list.</summary>
    public List<ClauseExecution> Executions;
    public bool IsAllEntries;

    /// <summary>Set during clause population/contradiction propagation when we can detect upfront that this query cannot return any results</summary>
    public bool QueryWillReturnNoResults;

    /// <summary>Cardinality of the clause that matches <see cref="PlanTemplate.SortDrivingClauseIndex"/>,
    /// captured during the per-execution cardinality estimation pass. Lets <see cref="CompiledPlan.Ordering"/>'s
    /// cliff-bit decision skip a second walk of the executions list. -1 when there is no sort-driving clause
    /// or no execution matched it.</summary>
    public long DrivingClauseCardinality = -1;

    /// <summary>Typed parameter values for clause resolution. Populated during plan building
    /// from resolved query parameters and literal values. Each clause's PackedParam field
    /// encodes (type, index) pairs pointing into these arrays, so resolution never has to
    /// reparse strings back to their native types.</summary>
    public long[] LongValues;
    public double[] DoubleValues;
    public string[] StringValues;

    /// <summary>Spatial operations to apply after the bitmap filter phase builds the candidate bitmap.
    /// Each spatial match is ANDed with the candidate set.</summary>
    public SpatialFilterOp[] SpatialFilters;

    /// <summary>Vector operations to apply after spatial filtering.
    /// The bitmap-producing CompiledQueryMatch is passed as the filterQuery to VectorSearchMatch.</summary>
    public VectorSearchOp[] VectorSelects;

    /// <summary>Per-execution term counts for OrRange/AndRange ops. Each range op's
    /// ParamIndex2 is an index into this array. The IL reads the count at runtime,
    /// so the same compiled delegate handles different IN parameter array sizes.</summary>
    public int[] InRangeCounts;

    /// <summary>Per-slot planner cardinality estimate consumed by the entry-scan heuristic.
    /// Plan-cached IL holds no per-query numbers, so we attach the estimate per execution and
    /// the IL indexes it by the runtime cursor. One long per match slot (same layout as the
    /// dispatch arrays — IN/AllIn occupy <c>InTermCount + 1</c> consecutive slots).</summary>
    public long[] Cardinalities;

    /// <summary>Flat array of analyzer-encoded string slices referenced by the entry-scan/direct-scan
    /// residual predicates. Sized to match <see cref="ScanPredicateInfo.ParamIndex"/>; the emitted
    /// residual IL reads <c>exec.ResidualSlices[paramIdx]</c> via <c>Ldfld</c>+<c>Ldelema</c>+
    /// <c>AsReadOnlySpan</c>. Populated lazily by <c>PopulateScanParams</c> on first entry-scan
    /// trigger because most queries never reach this path.</summary>
    public Slice[] ResidualSlices;

    /// <summary>Per-predicate field-root page identifiers consumed by the residual IL via
    /// <c>EntryTermsReader.FindNext(fieldRootPage)</c>. Index parallels <see cref="ResidualSlices"/>'s
    /// predicate ordering (one entry per leaf scan predicate, including children of group predicates).
    /// Populated lazily alongside <see cref="ResidualSlices"/>.</summary>
    public long[] FieldRootPages;

    /// <summary>Lazy populate hook for <see cref="ResidualSlices"/>/<see cref="FieldRootPages"/>.
    /// Invoked once on first entry-scan trigger; the bitmap pipeline never pays the analyzer/
    /// field-root cost for queries that complete entirely from the bitmap.</summary>
    public Action PopulateScanParams;

    public bool HasSpatialOrVector => SpatialFilters is { Length: > 0 } || VectorSelects is { Length: > 0 };

    /// <summary>Per-execution cache for analyzer-encoded string slices. Both the bitmap-pipeline
    /// resolution (via <see cref="PackedParam"/> string branches) and the entry-scan residual
    /// extractor (<c>ScanParamExtractor</c>) consult this cache so each <c>(field, stringSlot)</c>
    /// pair is encoded at most once per <c>Build()</c>. Stored as a flat list because the
    /// per-query unique-pair count is small (typically &lt; 20); a linear scan beats hashing
    /// overhead for these sizes and avoids per-lookup allocation.</summary>
    private List<(Slice Field, int Slot, Slice Analyzed)> _analyzedSlices;

    /// <summary>Get the analyzer-encoded slice for <c>(fieldMeta, slot)</c>, materializing once
    /// per execution. Subsequent lookups for the same pair return the cached slice without
    /// re-running the analyzer. The cached slice's lifetime matches the underlying allocator's
    /// transaction.</summary>
    public Slice GetAnalyzedSlice(IndexSearcher indexSearcher, in FieldMetadata fieldMeta, int slot)
    {
        var fieldName = fieldMeta.FieldName;
        if (_analyzedSlices != null)
        {
            // Linear scan — see field comment for the size rationale.
            foreach (var entry in _analyzedSlices)
            {
                if (entry.Slot == slot && SliceComparer.AreEqual(entry.Field, fieldName))
                    return entry.Analyzed;
            }
        }
        else
        {
            _analyzedSlices = new List<(Slice, int, Slice)>();
        }

        var analyzed = indexSearcher.EncodeAndApplyAnalyzer(fieldMeta, StringValues[slot]);
        _analyzedSlices.Add((fieldName, slot, analyzed));
        return analyzed;
    }
}
