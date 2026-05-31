using System;
using System.Collections.Generic;
using Corax.Mappings;
using Voron;

namespace Corax.Querying.Planning;

public class QueryExecution
{
    /// <summary>Bit 30 of <see cref="CompiledPlan.Ordering" />. Set when any clause carries a boost factor.</summary>
    public const int HasBoostBit = 1 << 30;

    /// <summary>
    ///     Bit 31 of <see cref="CompiledPlan.Ordering" />. Set when the sort-driving clause's
    ///     cardinality is greater than (16K). Check whether we can use tie-break sorted scan; queries over it cannot.
    /// </summary>
    public const int CardinalityCliffBit = 1 << 31;

    public long[] Cardinalities;
    
    public double[] DoubleValues;

    public long DrivingClauseCardinality = -1;

    public List<ClauseExecution> Executions;

    public long[] FieldRootPages;

    public int[] InRangeCounts;

    public bool IsAllEntries;

    public long[] LongValues;

    public CompiledPlan Plan;

    public Action PopulateScanParams;

    public bool QueryWillReturnNoResults;

    /// <summary>Analyzer-encoded single-value slices, parallel to <see cref="StringValues"/> and
    /// indexed by <see cref="PackedParam.Param1"/> / <see cref="PackedParam.Param2"/> — the same
    /// addressing scheme <see cref="LongValues"/> / <see cref="DoubleValues"/> use. The single source of
    /// truth for analyzed slices: lazily allocated and filled on first touch by <see cref="GetAnalyzedSlice"/>,
    /// which every consumer routes through — bitmap term/range/between queries, the compound-field key
    /// encoding, and the residual-scan extractor (which forces the slots the IL will read to be populated).
    /// The residual-scan IL then reads <c>AnalyzedSlices[ParamIndex].AsReadOnlySpan()</c> directly. Slot is
    /// 1:1 with field (the append-only ValueWriter never reuses a slot), so the slot index alone is the key
    /// and an unset slot stays <c>default</c> — its <see cref="Slice.HasValue"/> is the "not yet analyzed" flag.</summary>
    public Slice[] AnalyzedSlices;

    /// <summary>Per-IN/ALL-IN residual predicate value sets, materialized per execution and
    /// indexed positionally by the IN-leaf walk order (the same order in which
    /// <see cref="ResidualScanIlEmitter"/> bakes its set index and
    /// <see cref="QueryPlanBuilder"/>'s scan-param extractor fills it). Self-contained
    /// per predicate so the residual IL never has to thread a runtime-variable base offset:
    /// the value count is simply the populated array's length.</summary>
    public ResidualInValues[] ResidualInSets;

    public SpatialFilterOp[] SpatialFilters;
    
    public string[] StringValues;

    public VectorSearchOp[] VectorSelects;

    public bool HasSpatialOrVector => SpatialFilters is { Length: > 0 } || VectorSelects is { Length: > 0 };

    public Slice GetAnalyzedSlice(IndexSearcher indexSearcher, in FieldMetadata fieldMeta, int slot)
    {
        AnalyzedSlices ??= new Slice[StringValues.Length];
        ref Slice analyzed = ref AnalyzedSlices[slot];
        if (analyzed.HasValue == false)
            analyzed = indexSearcher.EncodeAndApplyAnalyzer(fieldMeta, StringValues[slot]);
        return analyzed;
    }
}

/// <summary>Value set for a single IN / ALL IN residual predicate, materialized per execution.
/// Exactly one of <see cref="Slices"/> / <see cref="Longs"/> / <see cref="Doubles"/> is populated,
/// matching the predicate's <see cref="ScanValueType"/>. <see cref="HasNull"/> records whether the
/// IN list contained a null term (so the residual scan can match documents whose field is null,
/// mirroring the bitmap pipeline's null-term posting list).</summary>
public struct ResidualInValues
{
    public Slice[] Slices;
    public long[] Longs;
    public double[] Doubles;
    public bool HasNull;
}
