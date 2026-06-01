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

    /// <summary>The strategy actually selected at instance-time for THIS execution, after the
    /// per-execution cost gate. May differ from <see cref="CompiledPlan.Strategy"/> (the cached
    /// structural candidacy) when the cost gate falls back to the bitmap pipeline for the current
    /// parameter values — e.g. a plan cached as <see cref="ExecutionStrategy.DirectScan"/> from a
    /// selective parameter, re-run with a non-selective one. <see cref="ExecutionStrategy.NotEvaluated"/>
    /// until Instantiate runs.</summary>
    public ExecutionStrategy ActualStrategy = ExecutionStrategy.NotEvaluated;

    public Action PopulateScanParams;

    public bool QueryWillReturnNoResults;

    /// <summary>Holds the analyzed slices for each field, indexed by the field's slot.</summary>
    public Slice[] AnalyzedSlices;

    /// <summary>Range of values for each IN / ALL IN residual predicate.</summary>
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

public struct ResidualInValues
{
    public int Base;
    public int Count;
    public bool HasNull;
}
