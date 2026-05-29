using System.Collections.Generic;
using Corax.Mappings;
using Corax.Querying.Planning;
using Sparrow.Server;
using Voron;
using IndexSearcher = Corax.Querying.IndexSearcher;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

internal static partial class QueryPlanBuilder
{
    /// <summary>Materializes the typed scan-parameter arrays (slice + fieldRoot) consumed by
    /// entry-scan and direct-scan code paths. Long/double parameters are NOT materialized — the
    /// emitted IL reads <c>QueryExecution.LongValues</c>/<c>DoubleValues</c> directly via baked
    /// <see cref="PackedParam.Param1"/> indices. Two static entry points share the same accumulator
    /// state held on instance fields:
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

        public static void Extract(QueryExecution exec, IndexSearcher indexSearcher, ResolutionContext walkerCtx,
            out Slice[] sliceParams, out long[] fieldRootPages)
        {
            var predicates = exec.Plan.ScanPredicateInfos;
            if (predicates == null || predicates.Count == 0)
            {
                sliceParams = [];
                fieldRootPages = [];
                return;
            }

            var x = new ScanParamExtractor(exec, indexSearcher, allocator: null, walkerCtx);

            // Walk predicates and clauses in lock-step. BuildScanPredicateInfo skips non-eligible
            // clauses (Search, Regex, Spatial, Vector, and negated/boosted IN), so we must skip
            // them here too to keep the 1:1 positional mapping.
            int scanStart = exec.Plan.AllNegated ? 0 : 1;
            int clauseIdx = scanStart;
            var execs = exec.Executions;
            foreach (ScanPredicateInfo pred in predicates)
            {
                while (IsScanEligible(execs[clauseIdx]) == false)
                    clauseIdx++;

                x.ExtractFromPredicate(pred, execs[clauseIdx++]);
            }

            sliceParams = x._slices.Count > 0 ? x._slices.ToArray() : [];
            fieldRootPages = x._roots.Count > 0 ? x._roots.ToArray() : [];
            exec.ResidualInSets = x._inSets.Count > 0 ? x._inSets.ToArray() : null;
        }

        public static void BuildResidual(
            QueryExecution exec, IndexSearcher indexSearcher, ByteStringContext allocator,
            ScanPredicateInfo[] residualArray, int skipClauseIdx1, int skipClauseIdx2,
            out Slice[] sliceParams, out long[] fieldRootPages)
        {
            sliceParams = null;
            fieldRootPages = null;

            var execs = exec.Executions;
            if (residualArray == null || execs == null)
                return;

            var x = new ScanParamExtractor(exec, indexSearcher, allocator, walkerCtx: null);

            int residualIdx = 0;
            for (int i = 0; i < execs.Count; i++)
            {
                if (i == skipClauseIdx1 || i == skipClauseIdx2) continue;
                x._roots.Add(indexSearcher.FieldCache.GetLookupRootPage(execs[i].Clause.FieldName));

                ScanPredicateInfo residualPred = residualArray[residualIdx];
                if (residualPred.CompareOp is ScanCompareOp.In or ScanCompareOp.AllIn)
                {
                    x._inSets.Add(x.BuildInSet(residualPred, execs[i], analyzed: false));
                    residualIdx++;
                    continue;
                }

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

            sliceParams = x._slices.Count > 0 ? x._slices.ToArray() : null;
            fieldRootPages = x._roots.Count > 0 ? x._roots.ToArray() : null;
            exec.ResidualInSets = x._inSets.Count > 0 ? x._inSets.ToArray() : null;
        }

        /// <summary>Materialize a self-contained IN / ALL IN value set for one residual predicate.
        /// The IN values live contiguously in the typed value arrays starting at
        /// <see cref="PackedParam.Param1"/> for <see cref="ClauseExecution.InTermCount"/> entries.
        /// Slice values are analyzer-encoded on the bitmap path (<paramref name="analyzed"/> = true)
        /// and raw on the direct-scan path; long/double values are copied verbatim. The
        /// <see cref="ResidualInValues.HasNull"/> flag carries whether the IN list contained a null
        /// term so the residual scan can match null fields, mirroring the bitmap null-term posting list.</summary>
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

            // IN / ALL IN materialize a self-contained per-predicate value set (positionally indexed
            // by the residual IL's set counter, parallel to the field-root index).
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
            bool hasBetween = idx2 != PackedParam.NoParamValue;

            switch (pred.ValueType)
            {
                case ScanValueType.Slice:
                case ScanValueType.SliceLong:
                    // Route through the per-execution analyzed-slice cache. If the bitmap pipeline
                    // already analyzed this (field, slot) pair while building TermMatch/RangeMatch,
                    // the cached Slice is returned without re-running the analyzer.
                    FieldMetadata fieldMeta = ResolveFieldMetadata(exec.Clause, _walkerCtx);
                    _slices.Add(_exec.GetAnalyzedSlice(_indexSearcher, fieldMeta, idx1));
                    if (hasBetween)
                        _slices.Add(_exec.GetAnalyzedSlice(_indexSearcher, fieldMeta, idx2));
                    break;
            }
        }
    }
}
