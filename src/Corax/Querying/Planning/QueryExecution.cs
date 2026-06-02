using System;
using System.Collections.Generic;
using Corax.Mappings;
using Voron;

namespace Corax.Querying.Planning;

public class QueryExecution
{
    public long[] Cardinalities;
    
    public double[] DoubleValues;

    public long DrivingClauseCardinality = -1;

    public List<ClauseExecution> Executions;

    // Clause playing each plan-optimization role, captured at execution-creation time by matching the
    // template-space OriginalIndex. Holding the instance (not a post-sort index) makes these
    // sort-order-invariant, so there is no remap step and nothing to recompute on a cache hit. A role is
    // null when it has no template candidate, or when its clause collapsed to a sentinel.
    public ClauseExecution SortDrivingClause;
    public ClauseExecution CompoundExactFirst;
    public ClauseExecution CompoundExactSecond;
    public ClauseExecution CompoundFieldDrivingClause;
    public ClauseExecution CompoundFieldField2Range;
    public ClauseExecution SortSeekClause;

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
    
    public void SetKnownClause(ClauseExecution exec, PlanTemplate t)
    {
        if (exec.IsSentinel)
            return; // sentinel clause is effectively removed, cannot be a known clause
        
        int originalIndex = exec.Clause.OriginalIndex;
        if (originalIndex == t.SortDrivingClauseIndex)
            DrivingClauseCardinality = exec.Cardinality;
        if (originalIndex == t.SortDrivingClauseIndex)
            SortDrivingClause = exec;
        if (originalIndex == t.CompoundExact.First)
            CompoundExactFirst = exec;
        if (originalIndex == t.CompoundExact.Second)
            CompoundExactSecond = exec;
        if (originalIndex == t.CompoundFieldDrivingClause)
            CompoundFieldDrivingClause = exec;       
        if (originalIndex == t.CompoundFieldField2Range)
            CompoundFieldField2Range = exec;
        if (originalIndex == t.SortSeekHintTemplateIdx)
            SortSeekClause = exec;
    }
}

public struct ResidualInValues
{
    public int Base;
    public int Count;
    public bool HasNull;
}
