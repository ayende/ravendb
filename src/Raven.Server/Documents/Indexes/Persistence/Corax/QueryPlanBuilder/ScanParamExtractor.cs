using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Corax.Mappings;
using Corax.Querying.Planning;
using IndexSearcher = Corax.Querying.IndexSearcher;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

internal sealed class ScanParamExtractor(QueryExecution exec, IndexSearcher indexSearcher, ResolutionContext walkerCtx)
{
    /// <summary>Extract field-root pages and analyzed slices for a residual scan set (entry-scan,
    /// DirectScan, or CompoundField), populating <see cref="QueryExecution.FieldRootPages"/> and the
    /// analyzed-slice cache in the SAME predicate order the matching delegate was emitted in.
    /// <see cref="ResidualScanSet.ClauseIndices"/> maps each predicate to its post-sort
    /// <see cref="QueryExecution.Executions"/> entry.</summary>
    public static void Extract(QueryExecution exec, IndexSearcher indexSearcher, ResolutionContext walkerCtx, ResidualScanSet set)
    {
        if (set is not { HasPredicates: true })
            return;

        new ScanParamExtractor(exec, indexSearcher, walkerCtx).ExtractAll(set.Predicates, set.ClauseIndices);
    }

    private readonly List<long> _roots = [];
    private readonly List<ResidualInValues> _inSets = [];

    private void ExtractAll(ScanPredicateInfo[] predicates, int[] clauseIndices)
    {
        var execs = exec.Executions;
        for (int p = 0; p < predicates.Length; p++)
        {
            ExtractFromPredicate(predicates[p], execs[clauseIndices[p]]);
        }

        exec.FieldRootPages = _roots.Count > 0 ? _roots.ToArray() : null;
        exec.ResidualInSets = _inSets.Count > 0 ? _inSets.ToArray() : null;
    }

    private ResidualInValues BuildInSet(ScanPredicateInfo pred, ClauseExecution exec1)
    {
        PackedParam packed = exec1.PackedParamValue;
        int baseIdx = packed.Param1;
        int count = exec1.InTermCount;

        if (pred.ValueType is ScanValueType.Slice or ScanValueType.SliceLong)
        {   // we need to analyze the strings
            FieldMetadata fieldMeta = QueryPlanBuilder.ResolveFieldMetadata(exec1.Clause, walkerCtx);
            for (int k = 0; k < count; k++)
            {   // running this for the side effect of setting the AnalyzedSlices value
                _ = exec.GetAnalyzedSlice(indexSearcher, fieldMeta, baseIdx + k);
            }
        }

        return new ResidualInValues { Base = baseIdx, Count = count, HasNull = exec1.HasNullTerm };
    }

    private void ExtractFromPredicate(ScanPredicateInfo pred, ClauseExecution cur)
    {
        RuntimeHelpers.EnsureSufficientExecutionStack();
        if (pred.SubPredicates != null)
        {
            var subExecs = cur.SubExecutions;
            for (int b = 0; b < pred.SubPredicates.Length; b++)
                ExtractFromPredicate(pred.SubPredicates[b], subExecs[b]);
            return;
        }

        // A sentinel leaf (collapsed MatchAll / MatchNothing) has no field — it adds no root page,
        // matching the IL emitter's rootIdx skip (ResidualScanIlEmitter.ConsumesFieldRootPage).
        if (pred.CompareOp is ScanCompareOp.AlwaysTrue or ScanCompareOp.AlwaysFalse)
            return;

        _roots.Add(indexSearcher.FieldCache.GetLookupRootPage(pred.FieldName));

        if (pred.CompareOp is ScanCompareOp.In or ScanCompareOp.AllIn)
        {
            _inSets.Add(BuildInSet(pred, cur));
            return;
        }

        if (cur.PackedParamValue.IsNone ||
            pred.ValueType is not (ScanValueType.Slice or ScanValueType.SliceLong))
            return;

        // ensure that the relevant slices are analyzed
        FieldMetadata fieldMeta = QueryPlanBuilder.ResolveFieldMetadata(cur.Clause, walkerCtx);
        exec.GetAnalyzedSlice(indexSearcher, fieldMeta, cur.PackedParamValue.Param1);
        if (cur.PackedParamValue.Param2 != PackedParam.NoParamValue)
            exec.GetAnalyzedSlice(indexSearcher, fieldMeta, cur.PackedParamValue.Param2);
    }
}
