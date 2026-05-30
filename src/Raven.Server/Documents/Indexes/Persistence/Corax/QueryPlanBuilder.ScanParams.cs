using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Corax.Mappings;
using Corax.Querying.Planning;
using Sparrow.Server;
using Voron;
using IndexSearcher = Corax.Querying.IndexSearcher;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

internal static partial class QueryPlanBuilder
{
     private sealed class ScanParamExtractor
    {
        public static void Extract(QueryExecution exec, IndexSearcher indexSearcher, ResolutionContext walkerCtx)
        {
            var predicates = exec.Plan.ScanPredicateInfos;
            if (predicates == null || predicates.Count == 0)
                return;

            new ScanParamExtractor(exec, indexSearcher, allocator: null, walkerCtx).ExtractAll(predicates);
        }


        public static void BuildResidual(
            QueryExecution exec, IndexSearcher indexSearcher, ByteStringContext allocator,
            ScanPredicateInfo[] residualArray, int skipClauseIdx1, int skipClauseIdx2)
        {
            var execs = exec.Executions;
            if (residualArray == null || execs == null)
                return;

            new ScanParamExtractor(exec, indexSearcher, allocator, walkerCtx: null).BuildResidualCore(residualArray, skipClauseIdx1, skipClauseIdx2);
        }
        
        private readonly List<Slice> _slices = [];
        private readonly List<long> _roots = [];
        private readonly List<ResidualInValues> _inSets = [];

        private readonly QueryExecution _exec;
        private readonly IndexSearcher _indexSearcher;
        private readonly ByteStringContext _allocator;
        private readonly ResolutionContext _walkerCtx;

        private ScanParamExtractor(QueryExecution exec, IndexSearcher indexSearcher, ByteStringContext allocator, ResolutionContext walkerCtx)
        {
            _exec = exec;
            _indexSearcher = indexSearcher;
            _allocator = allocator;
            _walkerCtx = walkerCtx;
        }

        private void ExtractAll(List<ScanPredicateInfo> predicates)
        {
            // ScanPredicateClauseIndices is parallel to predicates: it carries the exec position each
            // predicate was built from (recorded once at cache-miss in BuildScanPredicates), so there is
            // no need to re-walk the clauses skipping non-scannable ones on every query.
            var execs = _exec.Executions;
            int[] clauseIndices = _exec.Plan.ScanPredicateClauseIndices;
            for (int p = 0; p < predicates.Count; p++)
            {
                ExtractFromPredicate(predicates[p], execs[clauseIndices[p]]);
            }

            StoreResults();
        }

        private void StoreResults()
        {
            _exec.FieldRootPages = _roots.Count > 0 ? _roots.ToArray() : null;
            _exec.ResidualSlices = _slices.Count > 0 ? _slices.ToArray() : null;
            _exec.ResidualInSets = _inSets.Count > 0 ? _inSets.ToArray() : null;
        }

        private void BuildResidualCore(ScanPredicateInfo[] residualArray, int skipClauseIdx1, int skipClauseIdx2)
        {
            var execs = _exec.Executions;

            int residualIdx = 0;
            for (int i = 0; i < execs.Count; i++)
            {
                if (i == skipClauseIdx1 || i == skipClauseIdx2) continue;
                _roots.Add(_indexSearcher.FieldCache.GetLookupRootPage(execs[i].Clause.FieldName));

                ScanPredicateInfo residualPred = residualArray[residualIdx];
                if (residualPred.CompareOp is ScanCompareOp.In or ScanCompareOp.AllIn)
                {
                    _inSets.Add(BuildInSet(residualPred, execs[i], analyzed: false));
                    residualIdx++;
                    continue;
                }

                PackedParam packed = execs[i].PackedParamValue;
                if (packed.IsNone)
                {
                    residualIdx++;
                    continue;
                }

                int idx1 = packed.Param1;
                int idx2 = packed.Param2;
                switch (residualArray[residualIdx].ValueType)
                {
                    case ScanValueType.Slice:
                    case ScanValueType.SliceLong:
                        Slice.From(_allocator, _exec.StringValues[idx1], out Slice s1);
                        _slices.Add(s1);
                        if (idx2 != PackedParam.NoParamValue)
                        {
                            Slice.From(_allocator, _exec.StringValues[idx2], out Slice s2);
                            _slices.Add(s2);
                        }

                        break;
                }

                residualIdx++;
            }

            StoreResults();
        }

        private ResidualInValues BuildInSet(ScanPredicateInfo pred, ClauseExecution exec, bool analyzed)
        {
            PackedParam packed = exec.PackedParamValue;
            int baseIdx = packed.Param1;
            int count = exec.InTermCount;
            var set = new ResidualInValues { HasNull = exec.HasNullTerm };

            switch (pred.ValueType)
            {
                case ScanValueType.Long:
                    var longs = new long[count];
                    for (int k = 0; k < count; k++)
                        longs[k] = _exec.LongValues[baseIdx + k];
                    set.Longs = longs;
                    break;

                case ScanValueType.Double:
                    var doubles = new double[count];
                    for (int k = 0; k < count; k++)
                        doubles[k] = _exec.DoubleValues[baseIdx + k];
                    set.Doubles = doubles;
                    break;

                default:
                    var slices = new Slice[count];
                    if (analyzed)
                    {
                        FieldMetadata fieldMeta = ResolveFieldMetadata(exec.Clause, _walkerCtx);
                        for (int k = 0; k < count; k++)
                            slices[k] = _exec.GetAnalyzedSlice(_indexSearcher, fieldMeta, baseIdx + k);
                    }
                    else
                    {
                        for (int k = 0; k < count; k++)
                        {
                            Slice.From(_allocator, _exec.StringValues[baseIdx + k], out Slice s);
                            slices[k] = s;
                        }
                    }
                    set.Slices = slices;
                    break;
            }

            return set;
        }

        private void ExtractFromPredicate(ScanPredicateInfo pred, ClauseExecution exec)
        {
            RuntimeHelpers.EnsureSufficientExecutionStack();
            if (pred.SubPredicates != null)
            {
                var subExecs = exec.SubExecutions;
                for (int b = 0; b < pred.SubPredicates.Length; b++)
                    ExtractFromPredicate(pred.SubPredicates[b], subExecs[b]);
                return;
            }

            // Resolve field root page
            _roots.Add(_indexSearcher.FieldCache.GetLookupRootPage(pred.FieldName));

            if (pred.CompareOp is ScanCompareOp.In or ScanCompareOp.AllIn)
            {
                _inSets.Add(BuildInSet(pred, exec, analyzed: true));
                return;
            }

            // Slice values flow through the analyzer; long/double are read directly from
            // QueryExecution by the IL using baked PackedParam.Param1 indices, so no copy.
            var packed = exec.PackedParamValue;
            if (packed.IsNone)
                return;
            int idx1 = packed.Param1;
            int idx2 = packed.Param2;

            switch (pred.ValueType)
            {
                case ScanValueType.Slice:
                case ScanValueType.SliceLong:
                    FieldMetadata fieldMeta = ResolveFieldMetadata(exec.Clause, _walkerCtx);
                    _slices.Add(_exec.GetAnalyzedSlice(_indexSearcher, fieldMeta, idx1));
                    if (idx2 != PackedParam.NoParamValue)
                        _slices.Add(_exec.GetAnalyzedSlice(_indexSearcher, fieldMeta, idx2));
                    break;
            }
        }
    }
}
