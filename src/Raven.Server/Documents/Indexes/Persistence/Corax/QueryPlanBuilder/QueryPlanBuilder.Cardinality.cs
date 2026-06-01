using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Corax.Mappings;
using Corax.Querying.Planning;
using IndexSearcher = Corax.Querying.IndexSearcher;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

internal static partial class QueryPlanBuilder
{
    private static class CardinalityEstimator
    {
        public static long Estimate(ClauseExecution exec, IndexSearcher indexSearcher, ValueWriter writer, ResolutionContext walkerCtx)
        {
            return EstimateClause(exec);

            long EstimateClause(ClauseExecution e)
            {
                RuntimeHelpers.EnsureSufficientExecutionStack();
                ClauseInfo clause = e.Clause;
                switch (clause.ClauseType)
                {
                    case ClauseType.Equals:
                    {
                        FieldMetadata fieldMeta = ResolveFieldMetadata(clause, walkerCtx); // find the relevant analyzer here
                        PackedParam p = e.PackedParamValue;
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
                        return indexSearcher.NumberOfEntries; // Total index size is the only honest data-independent upper bound

                    case ClauseType.In:
                    case ClauseType.AllIn:
                        long sum = 0;
                        PackedParam ip = e.PackedParamValue;
                        if (ip.IsNone)
                            return indexSearcher.NumberOfEntries;

                        FieldMetadata meta = ResolveFieldMetadata(clause, walkerCtx);
                        int start = ip.Param1;
                        int count = e.InTermCount;
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
                        foreach (ClauseExecution subExec in e.SubExecutions)
                        {
                            if (subExec.Cardinality < 0) 
                                subExec.Cardinality = EstimateClause(subExec);
                            orSum += subExec.Cardinality;
                        }
                        return Math.Min(orSum, indexSearcher.NumberOfEntries);
                    case ClauseType.AndGroup:
                        long andMin = indexSearcher.NumberOfEntries;
                        foreach (ClauseExecution subExec in e.SubExecutions)
                        {
                            if (subExec.Cardinality < 0) 
                                subExec.Cardinality = EstimateClause(subExec);
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
        public static void Build(List<ClauseExecution> executions, bool isAllEntries,
            out int[] inRangeCounts, out long[] cardinalities)
        {
            List<int> inRange = [];
            List<long> cards = [];
            if (isAllEntries)
                cards.Add(0); // reserve slot 0 for the synthetic AllEntries match

            foreach (ClauseExecution exec in executions)
            {
                Walk(exec);
            }

            inRangeCounts = inRange.Count == 0 ? Array.Empty<int>() : inRange.ToArray();
            cardinalities = cards.Count == 0 ? null : cards.ToArray();
            return;

            void Walk(ClauseExecution exec)
            {
                RuntimeHelpers.EnsureSufficientExecutionStack();
                switch (exec.ClauseType)
                {
                    case ClauseType.OrGroup:
                    case ClauseType.AndGroup:
                        foreach (ClauseExecution sub in exec.SubExecutions)
                        {
                            Walk(sub);
                        }
                        break;

                    case ClauseType.In:
                    case ClauseType.AllIn:
                        inRange.Add(exec.InTermCount);
                        int n = exec.InTermCount + 1;
                        for (int i = 0; i < n; i++)
                        {
                            cards.Add(exec.Cardinality);
                        }

                        break;

                    default:
                        cards.Add(exec.Cardinality);
                        break;
                }
            }
        }
    }
}
