using System.Collections.Generic;
using Corax.Querying.Planning;
using Sparrow.Server;
using Voron;
using IndexSearcher = Corax.Querying.IndexSearcher;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

internal static partial class QueryPlanBuilder
{
    /// <summary>Materializes the four typed scan-parameter arrays (long/double/slice/fieldRoot)
    /// consumed by entry-scan and direct-scan code paths. Two static entry points share the
    /// same accumulator state held on instance fields:
    /// <list type="bullet">
    ///   <item><see cref="Extract"/> — bitmap-pipeline path; encodes slice values through the
    ///   field analyzer.</item>
    ///   <item><see cref="BuildResidual"/> — DirectScan/CompoundField residual path; slice values
    ///   use raw <c>Slice.From</c> (no analyzer) because the residual evaluator compares against
    ///   the entry's stored term directly.</item>
    /// </list>
    /// </summary>
    private sealed class ScanParamExtractor
    {
        private readonly List<long> _longs = [];
        private readonly List<double> _doubles = [];
        private readonly List<Slice> _slices = [];
        private readonly List<long> _roots = [];

        private readonly QueryExecution _exec;
        private readonly IndexSearcher _indexSearcher;
        private readonly ByteStringContext _allocator;

        private ScanParamExtractor(QueryExecution exec, IndexSearcher indexSearcher, ByteStringContext allocator)
        {
            _exec = exec;
            _indexSearcher = indexSearcher;
            _allocator = allocator;
        }

        public static void Extract(QueryExecution exec, IndexSearcher indexSearcher,
            out long[] longParams, out double[] doubleParams, out Slice[] sliceParams, out long[] fieldRootPages)
        {
            var predicates = exec.Plan.ScanPredicateInfos;
            if (predicates == null || predicates.Count == 0)
            {
                longParams = [];
                doubleParams = [];
                sliceParams = [];
                fieldRootPages = [];
                return;
            }

            var x = new ScanParamExtractor(exec, indexSearcher, allocator: null);

            // Walk predicates and clauses in lock-step. BuildScanPredicateInfo skips non-eligible
            // clauses (Search, In, AllIn, Exists, StartsWith, EndsWith, Regex, Spatial, Vector,
            // AndGroup), so we must skip them here too to keep the 1:1 positional mapping.
            int scanStart = exec.Plan.AllNegated ? 0 : 1;
            int clauseIdx = scanStart;
            var execs = exec.Executions;
            foreach (ScanPredicateInfo pred in predicates)
            {
                while (IsScanEligible(execs[clauseIdx]) == false)
                    clauseIdx++;

                x.ExtractFromPredicate(pred, execs[clauseIdx++]);
            }

            longParams = x._longs.Count > 0 ? x._longs.ToArray() : [];
            doubleParams = x._doubles.Count > 0 ? x._doubles.ToArray() : [];
            sliceParams = x._slices.Count > 0 ? x._slices.ToArray() : [];
            fieldRootPages = x._roots.Count > 0 ? x._roots.ToArray() : [];
        }

        public static void BuildResidual(
            QueryExecution exec, IndexSearcher indexSearcher, ByteStringContext allocator,
            ScanPredicateInfo[] residualArray, int skipClauseIdx1, int skipClauseIdx2,
            out long[] longParams, out double[] doubleParams, out Slice[] sliceParams, out long[] fieldRootPages)
        {
            longParams = null;
            doubleParams = null;
            sliceParams = null;
            fieldRootPages = null;

            var execs = exec.Executions;
            if (residualArray == null || execs == null)
                return;

            var x = new ScanParamExtractor(exec, indexSearcher, allocator);

            int residualIdx = 0;
            for (int i = 0; i < execs.Count; i++)
            {
                if (i == skipClauseIdx1 || i == skipClauseIdx2) continue;
                x._roots.Add(indexSearcher.FieldCache.GetLookupRootPage(execs[i].Clause.FieldName));
                var packed = execs[i].PackedParamValue;
                if (packed.IsNone)
                {
                    residualIdx++;
                    continue;
                }

                int idx1 = packed.Param1;
                int idx2 = packed.Param2;
                bool hasBetween = idx2 != PackedParam.NoParamValue;
                switch (residualArray[residualIdx].ValueType)
                {
                    case ScanValueType.Long:
                        x._longs.Add(exec.LongValues[idx1]);
                        if (hasBetween) x._longs.Add(exec.LongValues[idx2]);
                        break;
                    case ScanValueType.Double:
                        x._doubles.Add(exec.DoubleValues[idx1]);
                        if (hasBetween) x._doubles.Add(exec.DoubleValues[idx2]);
                        break;
                    case ScanValueType.Slice:
                    case ScanValueType.SliceLong:
                        Slice.From(allocator, exec.StringValues[idx1], out var s1);
                        x._slices.Add(s1);
                        if (hasBetween)
                        {
                            Slice.From(allocator, exec.StringValues[idx2], out var s2);
                            x._slices.Add(s2);
                        }

                        break;
                }

                residualIdx++;
            }

            longParams = x._longs.Count > 0 ? x._longs.ToArray() : null;
            doubleParams = x._doubles.Count > 0 ? x._doubles.ToArray() : null;
            sliceParams = x._slices.Count > 0 ? x._slices.ToArray() : null;
            fieldRootPages = x._roots.Count > 0 ? x._roots.ToArray() : null;
        }

        private void ExtractFromPredicate(ScanPredicateInfo pred, ClauseExecution exec)
        {
            if (pred.SubPredicates != null)
            {
                // Each sub-predicate corresponds positionally to a sub-execution of the group.
                // BuildScanPredicateInfoCore guarantees pred.SubPredicates.Length == exec.SubExecutions.Count.
                var subExecs = exec.SubExecutions;
                for (int b = 0; b < pred.SubPredicates.Length; b++)
                    ExtractFromPredicate(pred.SubPredicates[b], subExecs[b]);
                return;
            }

            // Resolve field root page
            _roots.Add(_indexSearcher.FieldCache.GetLookupRootPage(pred.FieldName));

            // Read pre-resolved typed values from the queryExec's arrays via packed param.
            var packed = exec.PackedParamValue;
            if (packed.IsNone)
                return;
            int idx1 = packed.Param1;
            int idx2 = packed.Param2;
            bool hasBetween = idx2 != PackedParam.NoParamValue;

            switch (pred.ValueType)
            {
                case ScanValueType.Long:
                    _longs.Add(_exec.LongValues[idx1]);
                    if (hasBetween)
                        _longs.Add(_exec.LongValues[idx2]);
                    break;
                case ScanValueType.Double:
                    _doubles.Add(_exec.DoubleValues[idx1]);
                    if (hasBetween)
                        _doubles.Add(_exec.DoubleValues[idx2]);
                    break;
                case ScanValueType.Slice:
                case ScanValueType.SliceLong:
                    var fieldMeta = _indexSearcher.FieldMetadataBuilder(exec.Clause.FieldName);
                    _slices.Add(_indexSearcher.EncodeAndApplyAnalyzer(fieldMeta, _exec.StringValues[idx1]));
                    if (hasBetween)
                        _slices.Add(_indexSearcher.EncodeAndApplyAnalyzer(fieldMeta, _exec.StringValues[idx2]));
                    break;
            }
        }
    }
}
