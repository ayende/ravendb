using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Corax.Querying.Planning;
using IndexSearcher = Corax.Querying.IndexSearcher;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

internal static partial class QueryPlanBuilder
{
    /// <summary>Estimates per-clause cardinality (forward inference, before plan
    /// resolution). The recursive walker reads <see cref="IndexSearcher"/>,
    /// <see cref="ValueWriter"/>, and <see cref="ResolutionContext"/> from instance
    /// fields so the inner recursion doesn't have to thread them through every call.</summary>
    private sealed class CardinalityEstimator
    {
        private readonly IndexSearcher _indexSearcher;
        private readonly ValueWriter _writer;
        private readonly ResolutionContext _walkerCtx;

        private CardinalityEstimator(IndexSearcher indexSearcher, ValueWriter writer, ResolutionContext walkerCtx)
        {
            _indexSearcher = indexSearcher;
            _writer = writer;
            _walkerCtx = walkerCtx;
        }

        public static long Estimate(ClauseExecution exec, IndexSearcher indexSearcher, ValueWriter writer, ResolutionContext walkerCtx)
            => new CardinalityEstimator(indexSearcher, writer, walkerCtx).EstimateClause(exec);

        private long EstimateClause(ClauseExecution exec)
        {
            var clause = exec.Clause;
            switch (clause.ClauseType)
            {
                case ClauseType.Equals:
                {
                    // ResolveFieldMetadata attaches the field's analyzer; FieldMetadataBuilder
                    // does not. Without the analyzer, NumberOfDocumentsUnderSpecificTerm looks
                    // up the term verbatim and misses index-time-normalized matches (e.g.
                    // LowerCaseKeyword turns "Alpha" into "alpha" on the index side).
                    var fieldMeta = ResolveFieldMetadata(clause, _walkerCtx);
                    var p = exec.PackedParamValue;
                    return p.ValueType switch
                    {
                        PackedParam.TypeLong => _indexSearcher.NumberOfDocumentsUnderSpecificTerm(fieldMeta, _writer.GetLong(p.Param1)),
                        PackedParam.TypeDouble => _indexSearcher.NumberOfDocumentsUnderSpecificTerm(fieldMeta, _writer.GetDouble(p.Param1)),
                        _ => _indexSearcher.NumberOfDocumentsUnderSpecificTerm(fieldMeta, _writer.GetString(p.Param1))
                    };
                }

                case ClauseType.NotEquals:
                case ClauseType.GreaterThan:
                case ClauseType.GreaterThanOrEqual:
                case ClauseType.LessThan:
                case ClauseType.LessThanOrEqual:
                case ClauseType.Between:
                case ClauseType.Exists:
                case ClauseType.StartsWith:
                case ClauseType.EndsWith:
                case ClauseType.Search:
                case ClauseType.Regex:
                    // Use field-level cardinality as upper bound
                    return _indexSearcher.GetTermAmountInField(ResolveFieldMetadata(clause, _walkerCtx));

                case ClauseType.In:
                case ClauseType.AllIn:
                    // Sum of individual term cardinalities. ResolveFieldMetadata picks up the
                    // field analyzer so case-folding/keyword normalization applies before the
                    // per-term posting-list lookup — otherwise IN over an analyzed field
                    // returns 0 for every term and the clause is misjudged as trivially small,
                    // which corrupts the cardinality-driven clause ordering.
                    long sum = 0;
                    var meta = ResolveFieldMetadata(clause, _walkerCtx);
                    var ip = exec.PackedParamValue;
                    if (ip.IsNone)
                        return _indexSearcher.NumberOfEntries;

                    int start = ip.Param1;
                    int count = exec.InTermCount;
                    for (int t = 0; t < count; t++)
                    {
                        sum += ip.ValueType switch
                        {
                            PackedParam.TypeLong => _indexSearcher.NumberOfDocumentsUnderSpecificTerm(meta, _writer.GetLong(start + t)),
                            PackedParam.TypeDouble => _indexSearcher.NumberOfDocumentsUnderSpecificTerm(meta, _writer.GetDouble(start + t)),
                            _ => _indexSearcher.NumberOfDocumentsUnderSpecificTerm(meta, _writer.GetString(start + t))
                        };
                    }

                    return Math.Min(sum, _indexSearcher.NumberOfEntries);

                case ClauseType.Spatial:
                case ClauseType.Vector:
                    return _indexSearcher.NumberOfEntries;

                case ClauseType.OrGroup:
                    long orSum = 0;
                    if (exec.SubExecutions == null) return orSum;
                    for (int si = 0; si < clause.SubClauses.Count; si++)
                    {
                        var subExec = exec.SubExecutions[si];
                        if (subExec.Cardinality < 0)
                        {
                            subExec.Cardinality = EstimateClause(subExec);
                        }
                        orSum += subExec.Cardinality;
                    }
                    return Math.Min(orSum, _indexSearcher.NumberOfEntries);

                case ClauseType.AndGroup:
                    long andMin = _indexSearcher.NumberOfEntries;
                    if (exec.SubExecutions == null) return andMin;
                    for (int si = 0; si < clause.SubClauses.Count; si++)
                    {
                        var subExec = exec.SubExecutions[si];
                        if (subExec.Cardinality < 0)
                        {
                            subExec.Cardinality = EstimateClause(subExec);
                        }
                        andMin = Math.Min(andMin, subExec.Cardinality);
                    }
                    return andMin;

                default:
                    return _indexSearcher.NumberOfEntries;
            }
        }
    }

    private static class CardinalityArrayBuilder
    {
        public static int[] BuildInRangeCounts(List<ClauseExecution> executions, int slotCount)
        {
            if (slotCount is 0)
                return Array.Empty<int>();
            
            int[] counts = new int[slotCount];
            int rangeIdx = 0;
    
            Accumulate(executions);
            return counts;

            void Accumulate(List<ClauseExecution> currentExecutions)
            {
                RuntimeHelpers.EnsureSufficientExecutionStack();
                for (int ci = 0; ci < currentExecutions.Count && rangeIdx < counts.Length; ci++)
                {
                    ClauseExecution execution = currentExecutions[ci];
                    switch (execution.Clause.ClauseType)
                    {
                        case ClauseType.OrGroup:
                        case ClauseType.AndGroup:
                            if (execution.SubExecutions is not null)
                            {
                                Accumulate(execution.SubExecutions);
                            }
                            break;

                        case ClauseType.In:
                        case ClauseType.AllIn:
                            counts[rangeIdx++] = execution.InTermCount;
                            break;
                    }
                }
            }
        }

        public static long[] BuildCardinalities(List<ClauseExecution> executions, bool isAllEntries)
        {
            int slotCount = CountMatchSlots(executions, isAllEntries);
            if (slotCount == 0)
                return null;

            long[] cardinalities = new long[slotCount];
            int slot = isAllEntries ? 1 : 0;

            foreach (var exec in executions ?? [])
            {
                Accumulate(exec);
            }

            return cardinalities;

            void Accumulate(ClauseExecution exec)
            {
                RuntimeHelpers.EnsureSufficientExecutionStack();
                switch (exec.ClauseType)
                {
                    case ClauseType.OrGroup:
                    case ClauseType.AndGroup:
                        if (exec.SubExecutions is not null)
                        {
                            foreach (var sub in exec.SubExecutions)
                            {
                                Accumulate(sub);
                            }
                        }
                        break;

                    case ClauseType.In:
                    case ClauseType.AllIn:
                        int n = exec.InTermCount + 1;
                        for (int i = 0; i < n; i++)
                        {
                            cardinalities[slot++] = exec.Cardinality;
                        }
                        break;

                    default:
                        cardinalities[slot++] = exec.Cardinality;
                        break;
                }
            }
        }
    }
}
