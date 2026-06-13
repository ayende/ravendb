using System;
using System.Runtime.CompilerServices;
using Corax.Mappings;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Planning;
using Voron;
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
                    return EstimateNumberOfDocumentsUnderSpecificTerm(clause, e);

                case ClauseType.GreaterThan:
                case ClauseType.GreaterThanOrEqual:
                case ClauseType.LessThan:
                case ClauseType.LessThanOrEqual:
                case ClauseType.Between:
                    return EstimateRangeClause(e, e.ClauseType);

                case ClauseType.NotEquals:
                {
                    // NotEquals(X) is MatchAll AndNot Equals(X): the count is the whole index minus the docs under
                    // term X - the same O(1) term lookup as Equals, just complemented. A missing packed value (can't
                    // resolve the term) falls back to the whole-index bound.
                    PackedParam p = e.PackedParamValue;
                    if (p.IsNone)
                        return indexSearcher.NumberOfEntries;

                    long eq = EstimateNumberOfDocumentsUnderSpecificTerm(clause, e);
                    return Math.Max(0, indexSearcher.NumberOfEntries - eq);
                }

                case ClauseType.StartsWith:
                {
                    // StartsWith(prefix) is the bounded prefix range [prefix, successor(prefix)): the same two-descent
                    // range estimate as a BETWEEN, not the whole index. A missing/non-string packed value (can't encode
                    // the prefix) falls back to the whole-index bound.
                    PackedParam p = e.PackedParamValue;
                    if (p.IsNone || p.ValueType != PackedParam.TypeString)
                        return indexSearcher.NumberOfEntries;

                    FieldMetadata fieldMeta = QueryPlanBuilder.ResolveFieldMetadata(clause, walkerCtx);
                    return indexSearcher.EstimateStartsWith(fieldMeta, writer.GetString(p.Param1), clause.RangeEstimateCalibration.Factor);
                }

                case ClauseType.Exists:
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

        // Estimates how many documents a range predicate (BETWEEN / GT / GTE / LT / LTE) matches. All three value
        // types share one helper: the per-type fan-out only resolves the concrete bound(s) and the open-side
        // sentinels, then the helper maps the operator onto the (low, high, inclusivity) tuple. Numeric opens use
        // the type's own min/max; string opens use the before/after-all-keys slices. Strings are analyzed with the
        // field's own analyzer here (into the tx allocator, no managed byte[]), so the estimate samples the same
        // byte ordering the real query does instead of the aggregation builder's default-field encoding.
        long EstimateRangeClause(ClauseExecution e, ClauseType type)
        {
            PackedParam p = e.PackedParamValue;
            if (p.IsNone)
                return indexSearcher.NumberOfEntries;

            FieldMetadata fieldMeta = QueryPlanBuilder.ResolveFieldMetadata(e.Clause, walkerCtx);

            bool isBetween = type == ClauseType.Between;
            return p.ValueType switch
            {
                PackedParam.TypeLong => EstimateRange(writer.GetLong(p.Param1), isBetween ? writer.GetLong(p.Param2) : 0, long.MinValue, long.MaxValue),
                PackedParam.TypeDouble => EstimateRange(writer.GetDouble(p.Param1), isBetween ? writer.GetDouble(p.Param2) : 0, double.MinValue, double.MaxValue),
                _ => EstimateRange(
                    indexSearcher.EncodeAndApplyAnalyzer(fieldMeta, writer.GetString(p.Param1)),
                    isBetween ? indexSearcher.EncodeAndApplyAnalyzer(fieldMeta, writer.GetString(p.Param2)) : default,
                    Slices.BeforeAllKeys, Slices.AfterAllKeys)
            };

            long EstimateRange<T>(T value1, T value2, T min, T max)
            {
                var (low, high, left, right) = type switch
                {
                    ClauseType.Between            => (value1, value2, UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThanOrEqual),
                    ClauseType.GreaterThan        => (value1, max,    UnaryMatchOperation.GreaterThan,        UnaryMatchOperation.LessThanOrEqual),
                    ClauseType.GreaterThanOrEqual => (value1, max,    UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThanOrEqual),
                    ClauseType.LessThan           => (min,    value1, UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThan),
                    ClauseType.LessThanOrEqual    => (min,    value1, UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThanOrEqual),
                    _ => throw new ArgumentOutOfRangeException(nameof(type), type, "invalid clause type for range estimation")
                };
                return indexSearcher.EstimateMatchesInRange(fieldMeta, low, high, left, right, e.Clause.RangeEstimateCalibration.Factor);
            }
        }

        long EstimateNumberOfDocumentsUnderSpecificTerm(ClauseInfo clause, ClauseExecution e)
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
    }
}
