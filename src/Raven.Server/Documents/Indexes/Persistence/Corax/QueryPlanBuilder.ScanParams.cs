using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Corax.Mappings;
using Corax.Querying.Planning;
using IndexSearcher = Corax.Querying.IndexSearcher;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

internal static partial class QueryPlanBuilder
{
     private sealed class ScanParamExtractor(QueryExecution exec, IndexSearcher indexSearcher, ResolutionContext walkerCtx)
     {
        public static void Extract(QueryExecution exec, IndexSearcher indexSearcher, ResolutionContext walkerCtx)
        {
            var predicates = exec.Plan.ScanPredicateInfos;
            if (predicates == null || predicates.Count == 0)
                return;

            new ScanParamExtractor(exec, indexSearcher, walkerCtx).ExtractAll(predicates);
        }

        private readonly List<long> _roots = [];
        private readonly List<ResidualInValues> _inSets = [];

        private void ExtractAll(List<ScanPredicateInfo> predicates)
        {
            var execs = exec.Executions;
            int[] clauseIndices = exec.Plan.ScanPredicateClauseIndices;
            for (int p = 0; p < predicates.Count; p++)
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
                FieldMetadata fieldMeta = ResolveFieldMetadata(exec1.Clause, walkerCtx);
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
            FieldMetadata fieldMeta = ResolveFieldMetadata(cur.Clause, walkerCtx);
            exec.GetAnalyzedSlice(indexSearcher, fieldMeta, cur.PackedParamValue.Param1);
            if (cur.PackedParamValue.Param2 != PackedParam.NoParamValue)
                exec.GetAnalyzedSlice(indexSearcher, fieldMeta, cur.PackedParamValue.Param2);
        }
    }
}
