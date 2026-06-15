using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
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

    /// <summary>The exact number of entries this plan resolves to, known from O(1) metadata
    /// (index / posting-list <c>NumberOfEntries</c>) without materializing the result bitmap.
    /// Set per-execution by <c>BuildResolver.FinalizePlan</c> for the two shapes whose result
    /// cardinality is exactly a single counter: an all-entries plan (no WHERE) and a single
    /// non-negated, non-boosted <c>Equals</c> (one term posting list). -1 when not known cheaply
    /// — every other shape must materialize/scan to count. Read by <c>CoraxIndexReadOperation</c>
    /// to source TotalResults and enable limit push-down even when <c>SkipStatistics</c> is false.</summary>
    public long KnownExactTotal = -1;

    public long[] LongValues;

    public CompiledPlan Plan;

    /// <summary>The strategy actually selected at instance-time for THIS execution, after the
    /// per-execution cost gate. May differ from <see cref="CompiledPlan.Strategy"/> (the cached
    /// structural candidacy) when the cost gate falls back to the bitmap pipeline for the current
    /// parameter values — e.g. a plan cached as <see cref="ExecutionStrategy.FieldSortedScan"/> from a
    /// selective parameter, re-run with a non-selective one. <see cref="ExecutionStrategy.NotEvaluated"/>
    /// until Instantiate runs.</summary>
    public ExecutionStrategy ActualStrategy = ExecutionStrategy.NotEvaluated;

    /// <summary>Per-execution cost-gate verdict for the candidate scan strategy, populated only when timings are
    /// requested. Carries the actual arithmetic the gate compared for THIS parameter set
    /// (entries_to_scan × cost vs bitmap_cost, or a short reason such as "full scan requested"), so the decision
    /// trail can explain why the cached structural candidate (FieldSortedScan / CompoundSortedScan) did or did
    /// not fire this run rather than just stating that a per-execution gate exists. Null when timings are off or
    /// the chosen strategy has no per-execution gate.</summary>
    public string StrategyGateReason;

    public Action PopulateScanParams;

    /// <summary>Holds the analyzed slices for each field, indexed by the field's slot.</summary>
    public Slice[] AnalyzedSlices;

    /// <summary>Range of values for each IN / ALL IN residual predicate.</summary>
    public ResidualInValues[] ResidualInSets;

    public SpatialFilterOp[] SpatialFilters;
    
    public string[] StringValues;

    /// <summary>Builds a <see cref="Regex"/> from a pattern string for a regex tree-scan leaf. Set by the server
    /// (<c>BuildResolver.FinalizePlan</c>) to the index's cached regex factory so a tree-scan leaf reuses the
    /// compiled-and-cached regex with its match timeout, rather than constructing a bare <c>new Regex</c> per query.
    /// Mirrors the way spatial leaves carry their server-resolved <see cref="Mappings.FieldMetadata"/>/field objects.</summary>
    public Func<string, Regex> RegexFactory;

    public VectorSearchOp[] VectorSelects;

    /// <summary>True when a single vector-search post-filter already emits its results in similarity-score
    /// order, making the implicit (or explicit <c>ORDER BY score()</c>) SortingMatch wrapper redundant. When
    /// set, the vector match is told to stream score-ordered output and the sort wrapper is skipped. Set only
    /// on the sorted-query path (BuildSortedQuery); the order-agnostic BuildFilterMatch path (facets / MLT)
    /// leaves it false so those callers keep the entry-id-sorted vector output they rely on.</summary>
    public bool VectorPostFilterProvidesScoreOrder;

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
        {
            DrivingClauseCardinality = exec.Cardinality;
            SortDrivingClause = exec;
        }
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
