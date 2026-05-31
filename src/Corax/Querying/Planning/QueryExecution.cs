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

    private List<(Slice Field, int Slot, Slice Analyzed)> _analyzedSlices;

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

    /// <summary>Analyzed single-value slice predicates, parallel to <see cref="StringValues"/> and
    /// indexed by <see cref="PackedParam.Param1"/> / <see cref="PackedParam.Param2"/> — the same
    /// addressing scheme <see cref="LongValues"/> / <see cref="DoubleValues"/> use. Each slot holds the
    /// analyzer-encoded form of <see cref="StringValues"/>[slot]; non-slice slots stay default. The
    /// residual-scan IL reads <c>AnalyzedSlices[ParamIndex].AsReadOnlySpan()</c> directly, so there is no
    /// dense per-scan slice counter and both the bitmap and direct-scan extraction paths fill it identically.</summary>
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
        // count is small (typically &lt; 20); a linear scan beats hashing for these sizes and
        Slice fieldName = fieldMeta.FieldName;
        List<(Slice Field, int Slot, Slice Analyzed)> list = _analyzedSlices ??= [];
        foreach ((Slice field, int slotIdx, Slice slice) in list)
        {
            if (slotIdx == slot && SliceComparer.AreEqual(field, fieldName))
                return slice;
        }

        Slice analyzed = indexSearcher.EncodeAndApplyAnalyzer(fieldMeta, StringValues[slot]);
        _analyzedSlices.Add((fieldName, slot, analyzed));
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
