using System;
using System.Runtime.CompilerServices;
using Corax.Mappings;
using Corax.Querying.Planning;
using IndexSearcher = Corax.Querying.IndexSearcher;
using static Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder.QueryPlanBuilder;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

internal static class CardinalityEstimator
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
