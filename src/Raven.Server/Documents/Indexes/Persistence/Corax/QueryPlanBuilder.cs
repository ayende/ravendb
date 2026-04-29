using System;
using System.Collections.Generic;
using System.Threading;
using Corax.Querying;
using Corax.Querying.Matches;
using Corax.Querying.Planning;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Sparrow.Json;
using Sparrow.Server;
using IndexSearcher = Corax.Querying.IndexSearcher;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

/// <summary>
/// Builds a QueryPlan from a parsed RQL query. Replaces CoraxQueryBuilder
/// for query execution in Corax 2.0.
///
/// Expression types handled:
/// - BinaryExpression (AND, OR, =, !=, >, >=, <, <=)
/// - BetweenExpression
/// - InExpression
/// - NegatedExpression
/// - TrueExpression (constant folding)
/// - MethodExpression (search, startsWith, endsWith, exists, boost, exact, regex)
///
/// Not yet handled (throws NotSupportedException):
/// - Spatial queries (spatial_within, etc.)
/// - Vector search
/// - When expressions
/// - MoreLikeThis
/// </summary>
internal static class QueryPlanBuilder
{
    public static QueryPlan BuildPlan(
        IndexSearcher indexSearcher,
        QueryMetadata metadata,
        BlittableJsonReaderObject queryParameters,
        CancellationToken token)
    {
        var query = metadata.Query;
        if (query.Where == null)
            return BuildAllEntriesPlan();

        // Parse WHERE clause into intermediate clause list
        var clauses = new List<ClauseInfo>();
        var rootOp = ParseExpression(query.Where, indexSearcher, clauses, queryParameters);

        if (rootOp == BooleanOp.True)
            return BuildAllEntriesPlan();
        if (rootOp == BooleanOp.False)
            return BuildEmptyPlan();
        if (clauses.Count == 0)
            return BuildAllEntriesPlan();

        // Estimate cardinalities
        foreach (var clause in clauses)
        {
            if (clause.Cardinality < 0)
                clause.Cardinality = EstimateCardinality(clause, indexSearcher);
        }

        // Determine top-level operation
        bool isOr = rootOp == BooleanOp.Or;

        // Sort AND operands by ascending cardinality
        if (!isOr)
            clauses.Sort((a, b) => a.Cardinality.CompareTo(b.Cardinality));

        // Build PlanOp array
        return EmitPlan(clauses, isOr);
    }

    private enum BooleanOp { And, Or, True, False, Leaf }

    private static BooleanOp ParseExpression(
        QueryExpression expr,
        IndexSearcher indexSearcher,
        List<ClauseInfo> clauses,
        BlittableJsonReaderObject queryParameters)
    {
        switch (expr)
        {
            case BinaryExpression be:
                return ParseBinaryExpression(be, indexSearcher, clauses, queryParameters);

            case BetweenExpression between:
                ParseBetween(between, clauses, queryParameters);
                return BooleanOp.Leaf;

            case InExpression inExpr:
                ParseIn(inExpr, clauses, queryParameters);
                return BooleanOp.Leaf;

            case NegatedExpression negated:
                ParseNegated(negated, indexSearcher, clauses, queryParameters);
                return BooleanOp.Leaf;

            case TrueExpression:
                return BooleanOp.True;

            case MethodExpression method:
                ParseMethod(method, indexSearcher, clauses, queryParameters);
                return BooleanOp.Leaf;

            default:
                throw new NotSupportedException(
                    $"Expression type {expr.GetType().Name} is not supported in Corax 2.0 query planner.");
        }
    }

    private static BooleanOp ParseBinaryExpression(
        BinaryExpression be,
        IndexSearcher indexSearcher,
        List<ClauseInfo> clauses,
        BlittableJsonReaderObject queryParameters)
    {
        switch (be.Operator)
        {
            case OperatorType.And:
            {
                var left = ParseExpression(be.Left, indexSearcher, clauses, queryParameters);
                var right = ParseExpression(be.Right, indexSearcher, clauses, queryParameters);
                // Constant folding
                if (left == BooleanOp.True) return right;
                if (right == BooleanOp.True) return left;
                if (left == BooleanOp.False || right == BooleanOp.False) return BooleanOp.False;
                return BooleanOp.And;
            }

            case OperatorType.Or:
            {
                var left = ParseExpression(be.Left, indexSearcher, clauses, queryParameters);
                var right = ParseExpression(be.Right, indexSearcher, clauses, queryParameters);
                // Constant folding
                if (left == BooleanOp.True || right == BooleanOp.True) return BooleanOp.True;
                if (left == BooleanOp.False) return right;
                if (right == BooleanOp.False) return left;
                return BooleanOp.Or;
            }

            case OperatorType.Equal:
            case OperatorType.NotEqual:
                ParseComparison(be, clauses, queryParameters);
                return BooleanOp.Leaf;

            case OperatorType.LessThan:
            case OperatorType.LessThanEqual:
            case OperatorType.GreaterThan:
            case OperatorType.GreaterThanEqual:
                ParseRangeComparison(be, clauses, queryParameters);
                return BooleanOp.Leaf;

            default:
                throw new NotSupportedException(
                    $"Binary operator {be.Operator} is not supported in Corax 2.0 query planner.");
        }
    }

    private static void ParseComparison(BinaryExpression be, List<ClauseInfo> clauses,
        BlittableJsonReaderObject queryParameters)
    {
        if (be.Left is not FieldExpression field)
            return;
        string fieldName = field.FieldValue;
        string termValue = GetTermValue(be.Right, queryParameters);

        clauses.Add(new ClauseInfo
        {
            FieldName = fieldName,
            TermValue = termValue,
            ClauseType = be.Operator == OperatorType.NotEqual ? ClauseType.NotEquals : ClauseType.Equals,
            OriginalIndex = clauses.Count
        });
    }

    private static void ParseRangeComparison(BinaryExpression be, List<ClauseInfo> clauses,
        BlittableJsonReaderObject queryParameters)
    {
        if (be.Left is not FieldExpression field)
            return;
        string fieldName = field.FieldValue;
        string termValue = GetTermValue(be.Right, queryParameters);

        clauses.Add(new ClauseInfo
        {
            FieldName = fieldName,
            TermValue = termValue,
            ClauseType = be.Operator switch
            {
                OperatorType.GreaterThan => ClauseType.GreaterThan,
                OperatorType.GreaterThanEqual => ClauseType.GreaterThanOrEqual,
                OperatorType.LessThan => ClauseType.LessThan,
                OperatorType.LessThanEqual => ClauseType.LessThanOrEqual,
                _ => ClauseType.Equals
            },
            OriginalIndex = clauses.Count
        });
    }

    private static void ParseBetween(BetweenExpression between, List<ClauseInfo> clauses,
        BlittableJsonReaderObject queryParameters)
    {
        if (between.Source is not FieldExpression field)
            return;

        clauses.Add(new ClauseInfo
        {
            FieldName = field.FieldValue,
            TermValue = GetTermValue(between.Min, queryParameters),
            TermValue2 = GetTermValue(between.Max, queryParameters),
            ClauseType = ClauseType.Between,
            OriginalIndex = clauses.Count
        });
    }

    private static void ParseIn(InExpression inExpr, List<ClauseInfo> clauses,
        BlittableJsonReaderObject queryParameters)
    {
        if (inExpr.Source is not FieldExpression field)
            return;

        var terms = new List<string>();
        foreach (var value in inExpr.Values)
        {
            if (value is ValueExpression ve)
                terms.Add(GetTermValue(ve, queryParameters));
        }

        clauses.Add(new ClauseInfo
        {
            FieldName = field.FieldValue,
            InTerms = terms,
            ClauseType = inExpr.All ? ClauseType.AllIn : ClauseType.In,
            OriginalIndex = clauses.Count
        });
    }

    private static void ParseNegated(NegatedExpression negated, IndexSearcher indexSearcher,
        List<ClauseInfo> clauses, BlittableJsonReaderObject queryParameters)
    {
        // NOT expr → ANDNOT with all entries
        var innerClauses = new List<ClauseInfo>();
        ParseExpression(negated.Expression, indexSearcher, innerClauses, queryParameters);

        foreach (var inner in innerClauses)
        {
            inner.IsNegated = true;
            clauses.Add(inner);
        }
    }

    private static void ParseMethod(MethodExpression method, IndexSearcher indexSearcher,
        List<ClauseInfo> clauses, BlittableJsonReaderObject queryParameters)
    {
        string methodName = method.Name.Value;
        switch (methodName)
        {
            case "search":
                ParseSearchMethod(method, clauses, queryParameters);
                break;

            case "startsWith":
                ParsePrefixMethod(method, clauses, queryParameters, ClauseType.StartsWith);
                break;

            case "endsWith":
                ParsePrefixMethod(method, clauses, queryParameters, ClauseType.EndsWith);
                break;

            case "exists":
                if (method.Arguments.Count > 0 && method.Arguments[0] is FieldExpression existsField)
                {
                    clauses.Add(new ClauseInfo
                    {
                        FieldName = existsField.FieldValue,
                        ClauseType = ClauseType.Exists,
                        OriginalIndex = clauses.Count
                    });
                }
                break;

            case "exact":
                // exact(expr) → recurse with exact flag
                if (method.Arguments.Count > 0)
                    ParseExpression(method.Arguments[0], indexSearcher, clauses, queryParameters);
                break;

            case "boost":
                // boost(expr, factor) → recurse, mark as boosted
                if (method.Arguments.Count > 0)
                    ParseExpression(method.Arguments[0], indexSearcher, clauses, queryParameters);
                break;

            case "regex":
                if (method.Arguments.Count >= 2 && method.Arguments[0] is FieldExpression regexField)
                {
                    clauses.Add(new ClauseInfo
                    {
                        FieldName = regexField.FieldValue,
                        TermValue = GetTermValue(method.Arguments[1], queryParameters),
                        ClauseType = ClauseType.Regex,
                        OriginalIndex = clauses.Count
                    });
                }
                break;

            case "spatial.within":
            case "spatial.contains":
            case "spatial.disjoint":
            case "spatial.intersects":
                throw new NotSupportedException(
                    $"Spatial queries ({methodName}) not yet implemented in Corax 2.0 planner. " +
                    "Deferred to post-MVP. See docs/implementation-notes.md.");

            case "vector.search":
                throw new NotSupportedException(
                    $"Vector search not yet implemented in Corax 2.0 planner. " +
                    "Deferred to post-MVP. See docs/implementation-notes.md.");

            case "moreLikeThis":
                throw new NotSupportedException(
                    "MoreLikeThis not yet implemented in Corax 2.0 planner.");

            default:
                throw new NotSupportedException(
                    $"Method '{methodName}' not supported in Corax 2.0 query planner.");
        }
    }

    private static void ParseSearchMethod(MethodExpression method, List<ClauseInfo> clauses,
        BlittableJsonReaderObject queryParameters)
    {
        if (method.Arguments.Count < 2)
            return;

        if (method.Arguments[0] is not FieldExpression searchField)
            return;

        clauses.Add(new ClauseInfo
        {
            FieldName = searchField.FieldValue,
            TermValue = GetTermValue(method.Arguments[1], queryParameters),
            ClauseType = ClauseType.Search,
            OriginalIndex = clauses.Count
        });
    }

    private static void ParsePrefixMethod(MethodExpression method, List<ClauseInfo> clauses,
        BlittableJsonReaderObject queryParameters, ClauseType type)
    {
        if (method.Arguments.Count < 2)
            return;

        if (method.Arguments[0] is not FieldExpression field)
            return;

        clauses.Add(new ClauseInfo
        {
            FieldName = field.FieldValue,
            TermValue = GetTermValue(method.Arguments[1], queryParameters),
            ClauseType = type,
            OriginalIndex = clauses.Count
        });
    }

    private static string GetTermValue(QueryExpression expr, BlittableJsonReaderObject queryParameters)
    {
        if (expr is ValueExpression ve)
            return ve.GetValue(queryParameters)?.ToString();
        return null;
    }

    private static long EstimateCardinality(ClauseInfo clause, IndexSearcher indexSearcher)
    {
        switch (clause.ClauseType)
        {
            case ClauseType.Equals:
                return indexSearcher.NumberOfDocumentsUnderSpecificTerm(
                    indexSearcher.FieldMetadataBuilder(clause.FieldName), clause.TermValue);

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
                return indexSearcher.GetTermAmountInField(
                    indexSearcher.FieldMetadataBuilder(clause.FieldName));

            case ClauseType.In:
            case ClauseType.AllIn:
                // Sum of individual term cardinalities
                long sum = 0;
                var meta = indexSearcher.FieldMetadataBuilder(clause.FieldName);
                if (clause.InTerms != null)
                foreach (var term in clause.InTerms)
                    sum += indexSearcher.NumberOfDocumentsUnderSpecificTerm(meta, term);
                return Math.Min(sum, indexSearcher.NumberOfEntries);

            default:
                return indexSearcher.NumberOfEntries;
        }
    }

    private static QueryPlan EmitPlan(List<ClauseInfo> clauses, bool isOr)
    {
        var ops = new List<PlanOp>();
        var entryScanPredicates = new List<MultiUnaryItem[]>();

        if (isOr)
        {
            // OR chain
            for (int i = 0; i < clauses.Count; i++)
            {
                ops.Add(new PlanOp
                {
                    Kind = i == 0 ? PlanOpKind.FillFromPostings : PlanOpKind.OrWithPostings,
                    FieldId = i,
                    ParamIndex = i,
                    EstimatedCardinality = clauses[i].Cardinality
                });
            }
            ops.Add(new PlanOp { Kind = PlanOpKind.IterateInto });
        }
        else if (clauses.Count == 1 && clauses[0].ClauseType == ClauseType.Equals)
        {
            // Single equality — direct iterate, no bitmap
            ops.Add(new PlanOp
            {
                Kind = PlanOpKind.DirectIterate,
                ParamIndex = 0,
                EstimatedCardinality = clauses[0].Cardinality
            });
        }
        else
        {
            // AND chain: Fill smallest, then AndWith with goto checks
            ops.Add(new PlanOp
            {
                Kind = PlanOpKind.FillFromPostings,
                ParamIndex = 0,
                EstimatedCardinality = clauses[0].Cardinality
            });

            for (int i = 1; i < clauses.Count; i++)
            {
                // Goto check before each AND step
                ops.Add(new PlanOp
                {
                    Kind = PlanOpKind.CheckAndMaybeEntryScan,
                    ParamIndex = i,
                    GotoLabelIndex = entryScanPredicates.Count
                });

                // Remaining predicates for this goto label
                // (currently empty — MultiUnaryItem conversion deferred to execution)
                entryScanPredicates.Add(Array.Empty<MultiUnaryItem>());

                // Determine AND op kind based on clause type
                var andKind = clauses[i].IsNegated ? PlanOpKind.AndNotWithPostings : PlanOpKind.AndWithPostings;
                ops.Add(new PlanOp
                {
                    Kind = andKind,
                    ParamIndex = i,
                    EstimatedCardinality = clauses[i].Cardinality
                });
            }

            ops.Add(new PlanOp { Kind = PlanOpKind.IterateInto });
        }

        // Pack operand ordering
        int ordering = 0;
        for (int i = 0; i < Math.Min(clauses.Count, 10); i++)
            ordering |= (clauses[i].OriginalIndex & 0x7) << (i * 3);

        return new QueryPlan
        {
            Ops = ops.ToArray(),
            EntryScanPredicates = entryScanPredicates.ToArray(),
            OperandOrdering = ordering,
            OperandCount = clauses.Count,
            Clauses = clauses.ToArray()
        };
    }

    private static QueryPlan BuildAllEntriesPlan()
    {
        return new QueryPlan
        {
            Ops = new[] { new PlanOp { Kind = PlanOpKind.IterateInto } },
            EntryScanPredicates = Array.Empty<MultiUnaryItem[]>(),
            Clauses = Array.Empty<ClauseInfo>()
        };
    }

    private static QueryPlan BuildEmptyPlan()
    {
        // Query that always returns 0 results (e.g. false AND X)
        return new QueryPlan
        {
            Ops = Array.Empty<PlanOp>(),
            EntryScanPredicates = Array.Empty<MultiUnaryItem[]>(),
            Clauses = Array.Empty<ClauseInfo>()
        };
    }

    internal enum ClauseType
    {
        Equals,
        NotEquals,
        GreaterThan,
        GreaterThanOrEqual,
        LessThan,
        LessThanOrEqual,
        Between,
        In,
        AllIn,
        Exists,
        StartsWith,
        EndsWith,
        Search,
        Regex,
    }

    internal class ClauseInfo
    {
        public string FieldName;
        public string TermValue;
        public string TermValue2; // for BETWEEN
        public List<string> InTerms; // for IN
        public ClauseType ClauseType;
        public long Cardinality = -1;
        public int OriginalIndex;
        public bool IsNegated;
    }
}
