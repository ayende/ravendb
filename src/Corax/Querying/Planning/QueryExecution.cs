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

    public Slice[] ResidualSlices;

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
