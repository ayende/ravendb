using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Corax.Querying.Planning;
using IndexSearcher = Corax.Querying.IndexSearcher;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

internal static partial class QueryPlanBuilder
{
    /// <summary>Estimates per-clause cardinality (forward inference, before plan
    /// resolution). Nested recursion via a local function — the C# compiler keeps the
    /// captures on the stack (struct closure) because the local function is never
    /// turned into a delegate, so the call is allocation-free.</summary>
    private static class CardinalityEstimator
    {
        public static long Estimate(ClauseExecution exec, IndexSearcher indexSearcher, ValueWriter writer, ResolutionContext walkerCtx)
        {
            return EstimateClause(exec);

            long EstimateClause(ClauseExecution exec)
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
                        var fieldMeta = ResolveFieldMetadata(clause, walkerCtx);
                        var p = exec.PackedParamValue;
                        return p.ValueType switch
                        {
                            PackedParam.TypeLong => indexSearcher.NumberOfDocumentsUnderSpecificTerm(fieldMeta, writer.GetLong(p.Param1)),
                            PackedParam.TypeDouble => indexSearcher.NumberOfDocumentsUnderSpecificTerm(fieldMeta, writer.GetDouble(p.Param1)),
                            _ => indexSearcher.NumberOfDocumentsUnderSpecificTerm(fieldMeta, writer.GetString(p.Param1))
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
                        // Total index size is the only honest data-independent upper bound:
                        // GetDistinctTermCountInField counts unique terms (4 for an enum-like
                        // status field over 10M entries), not matching documents — using it
                        // here capped enum-like fields to "this clause can match at most 4 docs"
                        // and corrupted cardinality-driven clause ordering on low-cardinality
                        // fields (see #4861 for the concrete failure modes).
                        return indexSearcher.NumberOfEntries;

                    case ClauseType.In:
                    case ClauseType.AllIn:
                        // Sum of individual term cardinalities. ResolveFieldMetadata picks up the
                        // field analyzer so case-folding/keyword normalization applies before the
                        // per-term posting-list lookup — otherwise IN over an analyzed field
                        // returns 0 for every term and the clause is misjudged as trivially small,
                        // which corrupts the cardinality-driven clause ordering.
                        long sum = 0;
                        var meta = ResolveFieldMetadata(clause, walkerCtx);
                        var ip = exec.PackedParamValue;
                        if (ip.IsNone)
                            return indexSearcher.NumberOfEntries;

                        int start = ip.Param1;
                        int count = exec.InTermCount;
                        for (int t = 0; t < count; t++)
                        {
                            sum += ip.ValueType switch
                            {
                                PackedParam.TypeLong => indexSearcher.NumberOfDocumentsUnderSpecificTerm(meta, writer.GetLong(start + t)),
                                PackedParam.TypeDouble => indexSearcher.NumberOfDocumentsUnderSpecificTerm(meta, writer.GetDouble(start + t)),
                                _ => indexSearcher.NumberOfDocumentsUnderSpecificTerm(meta, writer.GetString(start + t))
                            };
                        }

                        return Math.Min(sum, indexSearcher.NumberOfEntries);

                    case ClauseType.Spatial:
                    case ClauseType.Vector:
                        return indexSearcher.NumberOfEntries;

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
                        return Math.Min(orSum, indexSearcher.NumberOfEntries);

                    case ClauseType.AndGroup:
                        long andMin = indexSearcher.NumberOfEntries;
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
                        return indexSearcher.NumberOfEntries;
                }
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
