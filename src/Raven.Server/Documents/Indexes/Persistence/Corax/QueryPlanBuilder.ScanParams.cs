using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Corax.Mappings;
using Corax.Querying.Planning;
using Voron;
using IndexSearcher = Corax.Querying.IndexSearcher;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

internal static partial class QueryPlanBuilder
{
     private sealed class ScanParamExtractor
    {
        /// <summary>Single extraction entry point for both the bitmap pipeline (deferred via
        /// <see cref="QueryExecution.PopulateScanParams"/>) and the direct-scan / compound-field paths
        /// (called eagerly at construct time). Walks the full <see cref="CompiledPlan.ScanPredicateInfos"/>
        /// list recursively — analyzing every slice and IN value — so the per-entry arrays the residual IL
        /// reads (<see cref="QueryExecution.AnalyzedSlices"/>, <see cref="QueryExecution.FieldRootPages"/>,
        /// <see cref="QueryExecution.ResidualInSets"/>) are identical regardless of which path filled them.
        /// The direct paths re-evaluate driving-clause predicates harmlessly (they were already satisfied by
        /// the tree scan), which keeps the dense root/IN-set counters aligned with the shared IL delegate.</summary>
        public static void Extract(QueryExecution exec, IndexSearcher indexSearcher, ResolutionContext walkerCtx)
        {
            var predicates = exec.Plan.ScanPredicateInfos;
            if (predicates == null || predicates.Count == 0)
                return;

            new ScanParamExtractor(exec, indexSearcher, walkerCtx).ExtractAll(predicates);
        }

        private Slice[] _analyzedSlices;
        private readonly List<long> _roots = [];
        private readonly List<ResidualInValues> _inSets = [];

        private readonly QueryExecution _exec;
        private readonly IndexSearcher _indexSearcher;
        private readonly ResolutionContext _walkerCtx;

        private ScanParamExtractor(QueryExecution exec, IndexSearcher indexSearcher, ResolutionContext walkerCtx)
        {
            _exec = exec;
            _indexSearcher = indexSearcher;
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
            _exec.AnalyzedSlices = _analyzedSlices;
            _exec.ResidualInSets = _inSets.Count > 0 ? _inSets.ToArray() : null;
        }

        /// <summary>Analyze the string at <paramref name="slot"/> (a <see cref="QueryExecution.StringValues"/>
        /// index, i.e. a packed Param1/Param2) and store it into <see cref="QueryExecution.AnalyzedSlices"/>
        /// at the same slot, mirroring how long/double values are addressed by their packed index.</summary>
        private void StoreAnalyzedSlice(in FieldMetadata fieldMeta, int slot)
        {
            _analyzedSlices ??= new Slice[_exec.StringValues.Length];
            _analyzedSlices[slot] = _exec.GetAnalyzedSlice(_indexSearcher, fieldMeta, slot);
        }

        private ResidualInValues BuildInSet(ScanPredicateInfo pred, ClauseExecution exec)
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
                    FieldMetadata fieldMeta = ResolveFieldMetadata(exec.Clause, _walkerCtx);
                    for (int k = 0; k < count; k++)
                        slices[k] = _exec.GetAnalyzedSlice(_indexSearcher, fieldMeta, baseIdx + k);
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
                _inSets.Add(BuildInSet(pred, exec));
                return;
            }

            // Slice values flow through the analyzer into AnalyzedSlices[Param1]; long/double are read
            // directly from QueryExecution by the IL using baked PackedParam.Param1 indices, so no copy.
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
                    StoreAnalyzedSlice(fieldMeta, idx1);
                    if (idx2 != PackedParam.NoParamValue)
                        StoreAnalyzedSlice(fieldMeta, idx2);
                    break;
            }
        }
    }
}
