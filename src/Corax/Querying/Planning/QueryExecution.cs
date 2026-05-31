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

    /// <summary>Holds the analyzed slices for each field, indexed by the field's slot.</summary>
    public Slice[] AnalyzedSlices;

    /// <summary>Per-IN/ALL-IN residual predicate descriptors, indexed positionally by the IN-leaf
    /// walk order (the same order in which <see cref="ResidualScanIlEmitter"/> bakes its set index
    /// and <see cref="QueryPlanBuilder"/>'s scan-param extractor fills it). Each entry only records
    /// where the IN values live in the flat per-execution arrays (<see cref="ResidualInValues.Base"/>
    /// + <see cref="ResidualInValues.Count"/>) and whether the list had a null term — the values
    /// themselves are not copied; the residual IL slices <see cref="LongValues"/> / <see cref="DoubleValues"/>
    /// / <see cref="AnalyzedSlices"/> directly.</summary>
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

/// <summary>Descriptor for a single IN / ALL IN residual predicate. The values themselves are not
/// copied — they already live contiguously in the flat per-execution arrays (<see cref="QueryExecution.LongValues"/>
/// / <see cref="QueryExecution.DoubleValues"/> / <see cref="QueryExecution.AnalyzedSlices"/>) at
/// <c>[Base, Base + Count)</c>, the same slots a single-value predicate would read. <see cref="Base"/>
/// is the first slot (PackedParam.Param1), <see cref="Count"/> the term count (InTermCount), and
/// <see cref="HasNull"/> records whether the IN list contained a null term (so the residual scan can
/// match null-valued fields, mirroring the bitmap pipeline's null-term posting list).</summary>
public struct ResidualInValues
{
    public int Base;
    public int Count;
    public bool HasNull;
}
