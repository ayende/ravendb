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

    /// <summary>True when any clause in this query is a BETWEEN whose sentinel bound ("*"/"NULL") was
    /// delivered by a query parameter. Aggregated during the PopulateClauseValues walk so the plan-cache
    /// key computation can decide on the FullKinds sentinel marker without re-walking the clause tree.</summary>
    public bool HasParameterSentinel;

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
