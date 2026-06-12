using System;
using System.Runtime.CompilerServices;
using Corax.Mappings;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Planning;
using IndexSearcher = Corax.Querying.IndexSearcher;

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
            switch (e.ClauseType)
            {
                case ClauseType.MatchAll:
                case ClauseType.MatchNothing:
                    return e.Cardinality; // sentinels carry a preset cardinality (NumberOfEntries / 0); never re-estimated

                case ClauseType.Equals:
                {
                    FieldMetadata fieldMeta = QueryPlanBuilder.ResolveFieldMetadata(clause, walkerCtx); // find the relevant analyzer here
                    PackedParam p = e.PackedParamValue;
                    return p.ValueType switch
                    {
                        PackedParam.TypeLong => indexSearcher.NumberOfDocumentsUnderSpecificTerm(fieldMeta, writer.GetLong(p.Param1)),
                        PackedParam.TypeDouble => indexSearcher.NumberOfDocumentsUnderSpecificTerm(fieldMeta, writer.GetDouble(p.Param1)),
                        _ => indexSearcher.NumberOfDocumentsUnderSpecificTerm(fieldMeta, writer.GetString(p.Param1))
                    };
                }

                case ClauseType.GreaterThan:
                case ClauseType.GreaterThanOrEqual:
                case ClauseType.LessThan:
                case ClauseType.LessThanOrEqual:
                case ClauseType.Between:
                    return EstimateRangeClause(e, e.ClauseType);

                case ClauseType.NotEquals:
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

                    FieldMetadata meta = QueryPlanBuilder.ResolveFieldMetadata(clause, walkerCtx);
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

        // Estimates how many documents a range predicate (BETWEEN / GT / GTE / LT / LTE) matches.
        // Numeric bounds are widened to double (the estimator keys on DoubleLookupKey); open sides use
        // double.MinValue/MaxValue. Textual fields can only be estimated for a fully-bounded BETWEEN -
        // a half-open string range has no concrete opposite bound to sample, so it falls back to the
        // whole-index size. A negative estimate (combiner declined to estimate cheaply) also falls back.
        long EstimateRangeClause(ClauseExecution e, ClauseType type)
        {
            PackedParam p = e.PackedParamValue;
            if (p.IsNone)
                return indexSearcher.NumberOfEntries;

            FieldMetadata fieldMeta = QueryPlanBuilder.ResolveFieldMetadata(e.Clause, walkerCtx);

            if (p.ValueType == PackedParam.TypeString)
            {
                if (type != ClauseType.Between)
                    return indexSearcher.NumberOfEntries;

                long s = indexSearcher.EstimateMatchesInRange(fieldMeta, writer.GetString(p.Param1), writer.GetString(p.Param2),
                    UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThanOrEqual);
                return s < 0 ? indexSearcher.NumberOfEntries : s;
            }

            double Bound(int slot) => p.ValueType == PackedParam.TypeLong ? writer.GetLong(slot) : writer.GetDouble(slot);

            double low, high;
            UnaryMatchOperation left = UnaryMatchOperation.GreaterThanOrEqual;
            UnaryMatchOperation right = UnaryMatchOperation.LessThanOrEqual;

            switch (type)
            {
                case ClauseType.Between:
                    low = Bound(p.Param1);
                    high = Bound(p.Param2);
                    break;
                case ClauseType.GreaterThan:
                    low = Bound(p.Param1);
                    high = double.MaxValue;
                    left = UnaryMatchOperation.GreaterThan;
                    break;
                case ClauseType.GreaterThanOrEqual:
                    low = Bound(p.Param1);
                    high = double.MaxValue;
                    break;
                case ClauseType.LessThan:
                    low = double.MinValue;
                    high = Bound(p.Param1);
                    right = UnaryMatchOperation.LessThan;
                    break;
                default: // LessThanOrEqual
                    low = double.MinValue;
                    high = Bound(p.Param1);
                    break;
            }

            long est = indexSearcher.EstimateMatchesInRange(fieldMeta, low, high, left, right);
            return est < 0 ? indexSearcher.NumberOfEntries : est;
        }
    }
}
