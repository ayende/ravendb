using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Corax.Querying;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Planning;
using Corax.Mappings;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Sparrow.Json;
using Constants = Corax.Constants;
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
/// MoreLikeThis is handled by a separate execution path (Index.cs → reader.MoreLikeThis())
/// and never reaches this planner.
/// </summary>
internal static class QueryPlanBuilder
{
    /// <summary>
    /// Parameters needed by the planner for field metadata resolution,
    /// analyzer setup, and cardinality estimation.
    /// </summary>
    internal class PlanParameters
    {
        public IndexSearcher IndexSearcher;
        public QueryMetadata Metadata;
        public BlittableJsonReaderObject QueryParameters;
        public Raven.Server.Documents.Indexes.Index Index;
        public global::Corax.Mappings.IndexFieldsMapping IndexFieldsMapping;
        public FieldsToFetch FieldsToFetch;
        public ByteStringContext Allocator;
        public CancellationToken Token;
        public bool HasDynamics;
        public Lazy<List<string>> DynamicFields;
        public bool HasBoost;
    }

    public static QueryPlan BuildPlan(
        IndexSearcher indexSearcher,
        QueryMetadata metadata,
        BlittableJsonReaderObject queryParameters,
        CancellationToken token)
    {
        return BuildPlan(new PlanParameters
        {
            IndexSearcher = indexSearcher,
            Metadata = metadata,
            QueryParameters = queryParameters,
            Token = token
        });
    }

    public static QueryPlan BuildPlan(PlanParameters p)
    {
        var query = p.Metadata.Query;
        var indexSearcher = p.IndexSearcher;
        var queryParameters = p.QueryParameters;
        var token = p.Token;
        var metadata = p.Metadata;
        if (query.Where == null)
            return BuildAllEntriesPlan();

        // Parse WHERE clause into intermediate clause list
        var clauses = new List<ClauseInfo>();
        bool hasMixedAndOr = false;
        var rootOp = ParseExpression(query.Where, indexSearcher, clauses, queryParameters, metadata, ref hasMixedAndOr);

        // Mixed AND/OR trees are handled via OrGroup clauses

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

        // Sort AND operands: non-negated first (ascending cardinality), then negated.
        // Negated clauses (NotEquals, IsNegated) can only subtract from an existing
        // bitmap via ANDNOT — they must never be the seed (first) operand.
        if (!isOr)
        {
            clauses.Sort((a, b) =>
            {
                bool aNeg = a.IsNegated || a.ClauseType == ClauseType.NotEquals;
                bool bNeg = b.IsNegated || b.ClauseType == ClauseType.NotEquals;
                if (aNeg != bNeg)
                    return aNeg ? 1 : -1; // non-negated first
                return a.Cardinality.CompareTo(b.Cardinality);
            });
        }

        // Build PlanOp array
        return EmitPlan(clauses, isOr);
    }

    private enum BooleanOp { And, Or, True, False, Leaf }

    private static BooleanOp ParseExpression(
        QueryExpression expr,
        IndexSearcher indexSearcher,
        List<ClauseInfo> clauses,
        BlittableJsonReaderObject queryParameters,
        QueryMetadata metadata,
        ref bool hasMixedAndOr)
    {
        switch (expr)
        {
            case BinaryExpression be:
                return ParseBinaryExpression(be, indexSearcher, clauses, queryParameters, metadata, ref hasMixedAndOr);

            case BetweenExpression between:
                ParseBetween(between, clauses, queryParameters, metadata);
                return BooleanOp.Leaf;

            case InExpression inExpr:
                ParseIn(inExpr, clauses, queryParameters, metadata);
                return BooleanOp.Leaf;

            case NegatedExpression negated:
                ParseNegated(negated, indexSearcher, clauses, queryParameters, metadata, ref hasMixedAndOr);
                return BooleanOp.Leaf;

            case TrueExpression:
                return BooleanOp.True;

            case MethodExpression method:
                ParseMethod(method, indexSearcher, clauses, queryParameters, metadata, ref hasMixedAndOr);
                return BooleanOp.Leaf;

            default:
                throw new InvalidOperationException(
                    $"Unexpected expression type {expr.GetType().Name} in WHERE clause.");
        }
    }

    private static BooleanOp ParseBinaryExpression(
        BinaryExpression be,
        IndexSearcher indexSearcher,
        List<ClauseInfo> clauses,
        BlittableJsonReaderObject queryParameters,
        QueryMetadata metadata,
        ref bool hasMixedAndOr)
    {
        switch (be.Operator)
        {
            case OperatorType.And:
            {
                // For AND, handle OR sub-expressions as grouped clauses
                BooleanOp left, right;

                if (be.Left is BinaryExpression { Operator: OperatorType.Or })
                {
                    // Left side is OR — parse into a separate clause list and group them
                    var orClauses = new List<ClauseInfo>();
                    left = ParseExpression(be.Left, indexSearcher, orClauses, queryParameters, metadata, ref hasMixedAndOr);
                    clauses.Add(new ClauseInfo
                    {
                        ClauseType = ClauseType.OrGroup,
                        OrSubClauses = orClauses,
                        OriginalIndex = clauses.Count
                    });
                }
                else
                {
                    left = ParseExpression(be.Left, indexSearcher, clauses, queryParameters, metadata, ref hasMixedAndOr);
                }

                if (be.Right is BinaryExpression { Operator: OperatorType.Or })
                {
                    var orClauses = new List<ClauseInfo>();
                    right = ParseExpression(be.Right, indexSearcher, orClauses, queryParameters, metadata, ref hasMixedAndOr);
                    clauses.Add(new ClauseInfo
                    {
                        ClauseType = ClauseType.OrGroup,
                        OrSubClauses = orClauses,
                        OriginalIndex = clauses.Count
                    });
                }
                else
                {
                    right = ParseExpression(be.Right, indexSearcher, clauses, queryParameters, metadata, ref hasMixedAndOr);
                }
                // Constant folding
                if (left == BooleanOp.True) return right;
                if (right == BooleanOp.True) return left;
                if (left == BooleanOp.False || right == BooleanOp.False) return BooleanOp.False;
                return BooleanOp.And;
            }

            case OperatorType.Or:
            {
                var left = ParseExpression(be.Left, indexSearcher, clauses, queryParameters, metadata, ref hasMixedAndOr);
                var right = ParseExpression(be.Right, indexSearcher, clauses, queryParameters, metadata, ref hasMixedAndOr);
                // Constant folding
                if (left == BooleanOp.True || right == BooleanOp.True) return BooleanOp.True;
                if (left == BooleanOp.False) return right;
                if (right == BooleanOp.False) return left;
                return BooleanOp.Or;
            }

            case OperatorType.Equal:
                ParseComparison(be, clauses, queryParameters, metadata);
                return BooleanOp.Leaf;

            case OperatorType.NotEqual:
                ParseComparison(be, clauses, queryParameters, metadata);
                return BooleanOp.Leaf;

            case OperatorType.LessThan:
            case OperatorType.LessThanEqual:
            case OperatorType.GreaterThan:
            case OperatorType.GreaterThanEqual:
                ParseRangeComparison(be, clauses, queryParameters, metadata);
                return BooleanOp.Leaf;

            default:
                throw new InvalidOperationException(
                    $"Unexpected binary operator {be.Operator} in WHERE clause.");
        }
    }

    private static void ParseComparison(BinaryExpression be, List<ClauseInfo> clauses,
        BlittableJsonReaderObject queryParameters, QueryMetadata metadata)
    {
        if (be.Left is not FieldExpression field)
            return;
        string fieldName = GetFieldName(field, metadata, queryParameters);
        string termValue = GetTermValue(be.Right, queryParameters, out var valueType);

        clauses.Add(new ClauseInfo
        {
            FieldName = fieldName,
            TermValue = termValue,
            TermValueType = valueType,
            ClauseType = be.Operator == OperatorType.NotEqual ? ClauseType.NotEquals : ClauseType.Equals,
            OriginalIndex = clauses.Count
        });
    }

    private static void ParseRangeComparison(BinaryExpression be, List<ClauseInfo> clauses,
        BlittableJsonReaderObject queryParameters, QueryMetadata metadata)
    {
        if (be.Left is not FieldExpression field)
            return;
        string fieldName = GetFieldName(field, metadata, queryParameters);
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
        BlittableJsonReaderObject queryParameters, QueryMetadata metadata)
    {
        if (between.Source is not FieldExpression field)
            return;

        clauses.Add(new ClauseInfo
        {
            FieldName = GetFieldName(field, metadata, queryParameters),
            TermValue = GetTermValue(between.Min, queryParameters),
            TermValue2 = GetTermValue(between.Max, queryParameters),
            ClauseType = ClauseType.Between,
            OriginalIndex = clauses.Count
        });
    }

    private static void ParseIn(InExpression inExpr, List<ClauseInfo> clauses,
        BlittableJsonReaderObject queryParameters, QueryMetadata metadata)
    {
        if (inExpr.Source is not FieldExpression field)
            return;

        var terms = new List<string>();
        bool hasTime = false;
        foreach (var value in inExpr.Values)
        {
            if (value is ValueExpression ve)
            {
                var resolvedValue = ve.GetValue(queryParameters);
                if (resolvedValue is Sparrow.Json.BlittableJsonReaderArray arr)
                {
                    for (int a = 0; a < arr.Length; a++)
                    {
                        var elem = arr[a];
                        terms.Add(ConvertInValue(elem, ref hasTime));
                    }
                }
                else
                {
                    terms.Add(ConvertInValue(resolvedValue, ref hasTime));
                }
            }
        }

        clauses.Add(new ClauseInfo
        {
            FieldName = GetFieldName(field, metadata, queryParameters),
            InTerms = terms,
            ClauseType = inExpr.All ? ClauseType.AllIn : ClauseType.In,
            OriginalIndex = clauses.Count
        });
    }

    private static void ParseNegated(NegatedExpression negated, IndexSearcher indexSearcher,
        List<ClauseInfo> clauses, BlittableJsonReaderObject queryParameters, QueryMetadata metadata, ref bool hasMixedAndOr)
    {
        // NOT expr → ANDNOT with all entries
        var innerClauses = new List<ClauseInfo>();
        ParseExpression(negated.Expression, indexSearcher, innerClauses, queryParameters, metadata, ref hasMixedAndOr);

        foreach (var inner in innerClauses)
        {
            inner.IsNegated = true;
            clauses.Add(inner);
        }
    }

    private static void ParseMethod(MethodExpression method, IndexSearcher indexSearcher,
        List<ClauseInfo> clauses, BlittableJsonReaderObject queryParameters, QueryMetadata metadata, ref bool hasMixedAndOr)
    {
        var methodType = QueryMethod.GetMethodType(method.Name.Value);
        switch (methodType)
        {
            case MethodType.Search:
                ParseSearchMethod(method, clauses, queryParameters, metadata);
                break;

            case MethodType.StartsWith:
                ParsePrefixMethod(method, clauses, queryParameters, metadata, ClauseType.StartsWith);
                break;

            case MethodType.EndsWith:
                ParsePrefixMethod(method, clauses, queryParameters, metadata, ClauseType.EndsWith);
                break;

            case MethodType.Exists:
                if (method.Arguments.Count > 0 && method.Arguments[0] is FieldExpression existsField)
                {
                    clauses.Add(new ClauseInfo
                    {
                        FieldName = GetFieldName(existsField, metadata, queryParameters),
                        ClauseType = ClauseType.Exists,
                        OriginalIndex = clauses.Count
                    });
                }
                break;

            case MethodType.Exact:
            {
                // exact(expr) → recurse, then mark all new clauses as exact
                int beforeCount = clauses.Count;
                if (method.Arguments.Count > 0)
                    ParseExpression(method.Arguments[0], indexSearcher, clauses, queryParameters, metadata, ref hasMixedAndOr);
                for (int c = beforeCount; c < clauses.Count; c++)
                    clauses[c].IsExact = true;
                break;
            }

            case MethodType.Boost:
            {
                // boost(expr, factor) → recurse, then set boost factor on new clauses
                int beforeCount = clauses.Count;
                if (method.Arguments.Count > 0)
                    ParseExpression(method.Arguments[0], indexSearcher, clauses, queryParameters, metadata, ref hasMixedAndOr);
                float boostFactor = 1f;
                if (method.Arguments.Count > 1)
                {
                    var factorStr = GetTermValue(method.Arguments[1], queryParameters);
                    if (factorStr != null)
                        float.TryParse(factorStr, System.Globalization.NumberStyles.Float,
                            System.Globalization.CultureInfo.InvariantCulture, out boostFactor);
                }
                for (int c = beforeCount; c < clauses.Count; c++)
                    clauses[c].BoostFactor = boostFactor;
                break;
            }

            case MethodType.Regex:
                if (method.Arguments.Count >= 2 && method.Arguments[0] is FieldExpression regexField)
                {
                    clauses.Add(new ClauseInfo
                    {
                        FieldName = GetFieldName(regexField, metadata, queryParameters),
                        TermValue = GetTermValue(method.Arguments[1], queryParameters),
                        ClauseType = ClauseType.Regex,
                        OriginalIndex = clauses.Count
                    });
                }
                break;

            case MethodType.Spatial_Within:
            case MethodType.Spatial_Contains:
            case MethodType.Spatial_Disjoint:
            case MethodType.Spatial_Intersects:
                // Spatial queries are resolved at execution time via the existing
                // CoraxQueryBuilder.HandleSpatial infrastructure.
                clauses.Add(new ClauseInfo
                {
                    ClauseType = ClauseType.Spatial,
                    MethodExpression = method,
                    OriginalIndex = clauses.Count
                });
                break;

            case MethodType.Vector_Search:
                clauses.Add(new ClauseInfo
                {
                    ClauseType = ClauseType.Vector,
                    MethodExpression = method,
                    OriginalIndex = clauses.Count
                });
                break;

            case MethodType.MoreLikeThis:
                // MoreLikeThis method in a WHERE clause acts as "all entries" —
                // the actual MLT logic is in the separate reader.MoreLikeThis() path.
                // When it appears in a filter expression, treat as no-op (all entries match).
                break;

            case MethodType.When:
            {
                // when(condition, expr) — evaluate the constant condition at plan time.
                // If false, produce no clause (empty result for this branch).
                // If true, recurse into the inner expression.
                if (method.Arguments.Count != 2)
                    break;
                var conditionResult = QueryBuilderHelper.EvaluateConstantExpressionForWhenQuery(
                    (BinaryExpression)method.Arguments[0], queryParameters);
                if (conditionResult)
                    ParseExpression(method.Arguments[1], indexSearcher, clauses, queryParameters, metadata, ref hasMixedAndOr);
                // If false, we simply don't add any clause — the branch is eliminated.
                break;
            }

            default:
                throw new InvalidOperationException(
                    $"Unexpected method '{method.Name.Value}' ({methodType}) in WHERE clause.");
        }
    }

    private static void ParseSearchMethod(MethodExpression method, List<ClauseInfo> clauses,
        BlittableJsonReaderObject queryParameters, QueryMetadata metadata)
    {
        if (method.Arguments.Count < 2)
            return;

        if (method.Arguments[0] is not FieldExpression searchField)
            return;

        clauses.Add(new ClauseInfo
        {
            FieldName = GetFieldName(searchField, metadata, queryParameters),
            TermValue = GetTermValue(method.Arguments[1], queryParameters),
            ClauseType = ClauseType.Search,
            OriginalIndex = clauses.Count
        });
    }

    private static void ParsePrefixMethod(MethodExpression method, List<ClauseInfo> clauses,
        BlittableJsonReaderObject queryParameters, QueryMetadata metadata, ClauseType type)
    {
        if (method.Arguments.Count < 2)
            return;

        if (method.Arguments[0] is not FieldExpression field)
            return;

        clauses.Add(new ClauseInfo
        {
            FieldName = GetFieldName(field, metadata, queryParameters),
            TermValue = GetTermValue(method.Arguments[1], queryParameters),
            ClauseType = type,
            OriginalIndex = clauses.Count
        });
    }

    /// <summary>Convert an IN value to its string representation, handling booleans and dates.</summary>
    private static string ConvertInValue(object value, ref bool hasTime)
    {
        if (value == null)
            return null;
        if (value is bool b)
            return b ? "true" : "false";
        // Check for date/time values — convert to ticks string for Corax
        if (value is DateTime dt)
        {
            hasTime = true;
            return dt.Ticks.ToString(System.Globalization.CultureInfo.InvariantCulture);
        }
        if (value is DateTimeOffset dto)
        {
            hasTime = true;
            return dto.UtcDateTime.Ticks.ToString(System.Globalization.CultureInfo.InvariantCulture);
        }
        // LazyStringValue from Blittable — might be a date string
        var str = value.ToString();
        if (str != null && str.Length > 18 && str.Length < 35 && str.Contains('T')
            && DateTime.TryParse(str, System.Globalization.CultureInfo.InvariantCulture,
                System.Globalization.DateTimeStyles.RoundtripKind, out var parsed))
        {
            hasTime = true;
            return parsed.Ticks.ToString(System.Globalization.CultureInfo.InvariantCulture);
        }
        return str;
    }

    /// <summary>Split search term value respecting quoted phrases.
    /// "nonexists \"second third\" nonexsts" → ["nonexists", "second third", "nonexsts"]
    /// Same logic as old CoraxQueryBuilder.GetValues().</summary>
    private static IEnumerable<string> SplitSearchTerms(string value)
    {
        if (string.IsNullOrEmpty(value))
        {
            yield return value;
            yield break;
        }

        bool quoted = false;
        int lastStart = 0;

        for (int i = 0; i < value.Length; i++)
        {
            char c = value[i];
            if (c == '"')
            {
                if (i > 0 && value[i - 1] == '\\')
                    continue; // escaped quote

                if (lastStart != i)
                    yield return value.Substring(lastStart, i - lastStart);

                quoted = !quoted;
                lastStart = i + 1;
            }
            else if ((c == ' ' || c == '\t') && !quoted)
            {
                if (lastStart != i)
                    yield return value.Substring(lastStart, i - lastStart);
                lastStart = i + 1;
            }
        }

        if (value.Length - lastStart > 0)
            yield return value.Substring(lastStart, value.Length - lastStart);
    }

    /// <summary>Extract field name from a FieldExpression.
    /// Uses the full compound path (Date.Year stays Date.Year).
    /// When metadata is available, uses GetIndexFieldName for proper alias resolution.</summary>
    private static string GetFieldName(FieldExpression field)
    {
        return field.FieldValue;
    }

    /// <summary>Extract field name with proper alias resolution using query metadata.</summary>
    private static string GetFieldName(FieldExpression field, QueryMetadata metadata, BlittableJsonReaderObject queryParameters)
    {
        if (metadata != null)
            return metadata.GetIndexFieldName(field, queryParameters).Value;
        return field.FieldValue;
    }

    private static string GetTermValue(QueryExpression expr, BlittableJsonReaderObject queryParameters)
    {
        return GetTermValue(expr, queryParameters, out _);
    }

    private static string GetTermValue(QueryExpression expr, BlittableJsonReaderObject queryParameters, out ValueTokenType valueType)
    {
        if (expr is ValueExpression ve)
        {
            valueType = ve.Value;
            var value = ve.GetValue(queryParameters);
            if (value is bool b)
                return b ? "true" : "false"; // Corax stores booleans as lowercase
            // For parameters, detect the actual type from the resolved value
            if (valueType == ValueTokenType.Parameter && value != null)
            {
                if (value is bool)
                    valueType = (bool)value ? ValueTokenType.True : ValueTokenType.False;
                else if (value is long or int)
                    valueType = ValueTokenType.Long;
                else if (value is double or float or decimal)
                    valueType = ValueTokenType.Double;
                else if (value is Sparrow.Json.LazyNumberValue lnv)
                {
                    // LazyNumberValue wraps JSON numbers — try long first, then double
                    if (lnv.TryParseLong(out _))
                        valueType = ValueTokenType.Long;
                    else
                        valueType = ValueTokenType.Double;
                }
                else
                    valueType = ValueTokenType.String;
            }
            return value?.ToString();
        }
        valueType = ValueTokenType.Null;
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

            case ClauseType.Spatial:
            case ClauseType.Vector:
                // Spatial and vector can't be estimated cheaply — use field total as upper bound
                return indexSearcher.NumberOfEntries;

            case ClauseType.OrGroup:
                long orSum = 0;
                if (clause.OrSubClauses != null)
                {
                    foreach (var sub in clause.OrSubClauses)
                    {
                        if (sub.Cardinality < 0)
                            sub.Cardinality = EstimateCardinality(sub, indexSearcher);
                        orSum += sub.Cardinality;
                    }
                }
                return Math.Min(orSum, indexSearcher.NumberOfEntries);

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
        else if (clauses.Count == 1 && clauses[0].ClauseType == ClauseType.NotEquals)
        {
            // Standalone NotEquals: AllEntries ANDNOT term
            // ParamIndex 0 = the AllEntries match, ParamIndex 1 = the negated term
            ops.Add(new PlanOp
            {
                Kind = PlanOpKind.FillFromPostings,
                ParamIndex = 0, // Will be resolved to AllEntries
                EstimatedCardinality = long.MaxValue // AllEntries — exact count not needed for plan
            });
            ops.Add(new PlanOp
            {
                Kind = PlanOpKind.AndNotWithPostings,
                ParamIndex = 1, // Will be resolved to the negated term
                EstimatedCardinality = clauses[0].Cardinality
            });
            ops.Add(new PlanOp { Kind = PlanOpKind.IterateInto });

            // Mark clause so ResolveMatches produces [AllEntries, TermMatch]
            clauses[0].IsNegated = true;

            return new QueryPlan
            {
                Ops = ops.ToArray(),
                EntryScanPredicates = Array.Empty<MultiUnaryItem[]>(),
                OperandOrdering = 0,
                OperandCount = 1,
                Clauses = clauses.ToArray()
            };
        }
        else
        {
            // AND chain: Fill smallest non-negated, then AndWith/AndNotWith remaining.
            // If the first clause is negated (all clauses are negated), we need to
            // start from AllEntries and ANDNOT each one.
            bool firstIsNegated = clauses[0].IsNegated || clauses[0].ClauseType == ClauseType.NotEquals;
            int startIndex;

            if (firstIsNegated)
            {
                // All clauses are negated — start from all entries.
                // AllEntries match is appended at index clauses.Count by ResolveMatches.
                ops.Add(new PlanOp
                {
                    Kind = PlanOpKind.FillFromPostings,
                    ParamIndex = clauses.Count, // Index of AllEntries in resolved matches
                    EstimatedCardinality = long.MaxValue
                });
                startIndex = 0; // Process all clauses as ANDNOT
            }
            else
            {
                startIndex = 1;
            }

            // Build match index mapping — OrGroups expand to multiple matches
            // First, compute match index for clause 0
            int matchIndex = 0;
            if (!firstIsNegated)
            {
                if (clauses[0].ClauseType == ClauseType.OrGroup && clauses[0].OrSubClauses != null)
                {
                    var subClauses = clauses[0].OrSubClauses;
                    for (int s = 0; s < subClauses.Count; s++)
                    {
                        ops.Add(new PlanOp
                        {
                            Kind = s == 0 ? PlanOpKind.FillFromPostings : PlanOpKind.OrWithPostings,
                            ParamIndex = matchIndex + s,
                            BitmapLocal = 0,
                            EstimatedCardinality = subClauses[s].Cardinality
                        });
                    }
                    matchIndex += subClauses.Count;
                }
                else if (clauses[0].ClauseType == ClauseType.AllIn && clauses[0].InTerms != null)
                {
                    // First clause is AllIn — fill first term, AND remaining
                    var terms = clauses[0].InTerms;
                    ops.Add(new PlanOp
                    {
                        Kind = PlanOpKind.FillFromPostings,
                        ParamIndex = matchIndex,
                        BitmapLocal = 0,
                        EstimatedCardinality = clauses[0].Cardinality / terms.Count
                    });
                    for (int t = 1; t < terms.Count; t++)
                    {
                        ops.Add(new PlanOp
                        {
                            Kind = PlanOpKind.AndWithPostings,
                            ParamIndex = matchIndex + t,
                            BitmapLocal = 0,
                            EstimatedCardinality = clauses[0].Cardinality / terms.Count
                        });
                        ops.Add(new PlanOp { Kind = PlanOpKind.CheckEmpty, BitmapLocal = 0 });
                    }
                    matchIndex += terms.Count;
                }
                else
                {
                    ops.Add(new PlanOp
                    {
                        Kind = PlanOpKind.FillFromPostings,
                        ParamIndex = 0,
                        EstimatedCardinality = clauses[0].Cardinality
                    });
                    matchIndex = 1;
                }
            }
            // Precheck: can all remaining clauses be converted to entry scan predicates?
            // If any clause (In, AllIn, Spatial, Vector, Search, etc.) can't be scanned,
            // we must not emit CheckAndMaybeEntryScan — entry scan would skip them entirely.
            bool allScanEligible = true;
            {
                int dummyL = 0, dummyD = 0, dummyS = 0;
                for (int j = startIndex; j < clauses.Count; j++)
                {
                    if (BuildScanPredicateInfo(clauses[j], ref dummyL, ref dummyD, ref dummyS) == null)
                    {
                        allScanEligible = false;
                        break;
                    }
                }
            }

            for (int i = startIndex; i < clauses.Count; i++)
            {
                // Goto check before each AND step — only if all remaining clauses
                // can be handled by entry scan predicates
                if (allScanEligible)
                {
                    ops.Add(new PlanOp
                    {
                        Kind = PlanOpKind.CheckAndMaybeEntryScan,
                        ParamIndex = matchIndex,
                        GotoLabelIndex = entryScanPredicates.Count
                    });
                    entryScanPredicates.Add(Array.Empty<MultiUnaryItem>());
                }

                if (clauses[i].ClauseType == ClauseType.OrGroup && clauses[i].OrSubClauses != null)
                {
                    // OrGroup: OR sub-clauses into bitmap[1], then AND with bitmap[0]
                    var subClauses = clauses[i].OrSubClauses;

                    // Clear bitmap[1] (OR accumulator)
                    ops.Add(new PlanOp { Kind = PlanOpKind.ClearBitmap, BitmapLocal = 1 });

                    // Fill each sub-clause into bitmap[1]
                    for (int s = 0; s < subClauses.Count; s++)
                    {
                        ops.Add(new PlanOp
                        {
                            Kind = PlanOpKind.OrWithPostings,
                            ParamIndex = matchIndex + s,
                            BitmapLocal = 1, // target bitmap[1]
                            EstimatedCardinality = subClauses[s].Cardinality
                        });
                    }

                    // AND bitmap[1] into bitmap[0]
                    ops.Add(new PlanOp
                    {
                        Kind = PlanOpKind.AndBitmaps,
                        BitmapLocal = 0,   // target
                        ParamIndex2 = 1    // source (reuse ParamIndex2 for source bitmap)
                    });

                    // Early exit check
                    ops.Add(new PlanOp { Kind = PlanOpKind.CheckEmpty, BitmapLocal = 0 });

                    matchIndex += subClauses.Count;
                }
                else if (clauses[i].ClauseType == ClauseType.AllIn && clauses[i].InTerms != null)
                {
                    // AllIn: AND each term's posting list with bitmap[0]
                    var terms = clauses[i].InTerms;
                    for (int t = 0; t < terms.Count; t++)
                    {
                        ops.Add(new PlanOp
                        {
                            Kind = PlanOpKind.AndWithPostings,
                            ParamIndex = matchIndex + t,
                            BitmapLocal = 0,
                            EstimatedCardinality = clauses[i].Cardinality / terms.Count
                        });
                        ops.Add(new PlanOp { Kind = PlanOpKind.CheckEmpty, BitmapLocal = 0 });
                    }
                    matchIndex += terms.Count;
                }
                else
                {
                    // Simple clause: AND or ANDNOT with bitmap[0]
                    var isNegated = clauses[i].IsNegated || clauses[i].ClauseType == ClauseType.NotEquals;
                    var andKind = isNegated ? PlanOpKind.AndNotWithPostings : PlanOpKind.AndWithPostings;
                    ops.Add(new PlanOp
                    {
                        Kind = andKind,
                        ParamIndex = matchIndex,
                        BitmapLocal = 0,
                        EstimatedCardinality = clauses[i].Cardinality
                    });

                    if (!isNegated)
                        ops.Add(new PlanOp { Kind = PlanOpKind.CheckEmpty, BitmapLocal = 0 });

                    matchIndex++;
                }
            }

            ops.Add(new PlanOp { Kind = PlanOpKind.IterateInto });
        }

        // Pack operand ordering
        int ordering = 0;
        for (int i = 0; i < Math.Min(clauses.Count, 10); i++)
            ordering |= (clauses[i].OriginalIndex & 0x7) << (i * 3);

        // Check if all clauses are negated (first clause after sort is negated)
        bool allNegated = clauses.Count > 0
            && (clauses[0].IsNegated || clauses[0].ClauseType == ClauseType.NotEquals);

        // Build scan predicate infos for entry scan — only for AND chains with simple clauses
        ScanPredicateInfo[] scanPredicateInfos = null;
        if (!isOr && clauses.Count > 1)
        {
            var scanPreds = new List<ScanPredicateInfo>();
            int longIndex = 0, doubleIndex = 0, sliceIndex = 0;

            // Start from clause 1 (clause 0 is the fill operand)
            for (int i = 1; i < clauses.Count; i++)
            {
                var pred = BuildScanPredicateInfo(clauses[i], ref longIndex, ref doubleIndex, ref sliceIndex);
                if (pred != null)
                    scanPreds.Add(pred.Value);
            }

            if (scanPreds.Count > 0)
                scanPredicateInfos = scanPreds.ToArray();
        }

        // Compute type signature from scan predicates
        int typeSignature = 0;
        if (scanPredicateInfos != null)
        {
            for (int i = 0; i < Math.Min(scanPredicateInfos.Length, 16); i++)
                typeSignature |= ((int)scanPredicateInfos[i].ValueType & 0x3) << (i * 2);
        }

        var plan = new QueryPlan
        {
            Ops = ops.ToArray(),
            EntryScanPredicates = entryScanPredicates.ToArray(),
            OperandOrdering = ordering,
            OperandCount = clauses.Count,
            Clauses = clauses.ToArray(),
            AllNegated = allNegated,
            ScanPredicateInfos = scanPredicateInfos,
            TypeSignature = typeSignature
        };

        // Generate EXPLAIN source
        plan.ExplainSource = GenerateExplain(plan, clauses);
        return plan;
    }

    private static string GenerateExplain(QueryPlan plan, List<ClauseInfo> clauses)
    {
        var sb = new System.Text.StringBuilder();
        sb.AppendLine("// Corax 2.0 Bitmap Query Plan");
        sb.AppendLine("// Clauses (sorted by cardinality):");
        foreach (var c in clauses)
        {
            string type = c.ClauseType.ToString();
            string detail = c.ClauseType switch
            {
                ClauseType.Equals => $"{c.FieldName} = '{c.TermValue}'",
                ClauseType.NotEquals => $"{c.FieldName} != '{c.TermValue}'",
                ClauseType.GreaterThan => $"{c.FieldName} > {c.TermValue}",
                ClauseType.LessThan => $"{c.FieldName} < {c.TermValue}",
                ClauseType.Between => $"{c.FieldName} BETWEEN {c.TermValue}..{c.TermValue2}",
                ClauseType.In => $"{c.FieldName} IN ({c.InTerms?.Count ?? 0} terms)",
                ClauseType.StartsWith => $"startsWith({c.FieldName}, '{c.TermValue}')",
                ClauseType.EndsWith => $"endsWith({c.FieldName}, '{c.TermValue}')",
                ClauseType.OrGroup => $"OR group ({c.OrSubClauses?.Count ?? 0} sub-clauses)",
                _ => $"{c.FieldName} {type} '{c.TermValue}'"
            };
            sb.AppendLine($"//   [{c.Cardinality:N0} est] {detail}");
        }
        sb.AppendLine("//");
        sb.AppendLine("// Operations:");
        foreach (var op in plan.Ops)
        {
            sb.AppendLine($"//   {op.Kind} (param={op.ParamIndex}, est={op.EstimatedCardinality:N0})");
        }
        return sb.ToString();
    }

    /// <summary>Extract typed parameter values from clauses for entry scan.
    /// Called per-query at execution time. The values populate the QueryScanContext spans.</summary>
    public static void ExtractScanParameters(QueryPlan plan, IndexSearcher indexSearcher,
        out long[] longParams, out double[] doubleParams, out Voron.Slice[] sliceParams, out long[] fieldRootPages)
    {
        var predicates = plan.ScanPredicateInfos;
        if (predicates == null || predicates.Length == 0)
        {
            longParams = Array.Empty<long>();
            doubleParams = Array.Empty<double>();
            sliceParams = Array.Empty<Voron.Slice>();
            fieldRootPages = Array.Empty<long>();
            return;
        }

        var clauses = plan.Clauses;
        var longs = new List<long>();
        var doubles = new List<double>();
        var slices = new List<Voron.Slice>();
        var roots = new List<long>();

        foreach (var pred in predicates)
        {
            ExtractParamsFromPredicate(pred, clauses, indexSearcher, longs, doubles, slices, roots);
        }

        longParams = longs.Count > 0 ? longs.ToArray() : Array.Empty<long>();
        doubleParams = doubles.Count > 0 ? doubles.ToArray() : Array.Empty<double>();
        sliceParams = slices.Count > 0 ? slices.ToArray() : Array.Empty<Voron.Slice>();
        fieldRootPages = roots.Count > 0 ? roots.ToArray() : Array.Empty<long>();
    }

    private static void ExtractParamsFromPredicate(ScanPredicateInfo pred, object[] clauses,
        IndexSearcher indexSearcher, List<long> longs, List<double> doubles,
        List<Voron.Slice> slices, List<long> roots)
    {
        if (pred.OrBranches != null)
        {
            foreach (var branch in pred.OrBranches)
                ExtractParamsFromPredicate(branch, clauses, indexSearcher, longs, doubles, slices, roots);
            return;
        }

        // Resolve field root page
        roots.Add(indexSearcher.FieldCache.GetLookupRootPage(pred.FieldName));

        // The values are in the clauses — find the matching clause.
        // The scan predicates correspond to clauses 1..N (skipping clause 0 which is the fill).
        // We need to find the clause for this predicate by field name.
        ClauseInfo matchingClause = null;
        if (clauses != null)
        {
            for (int i = 0; i < clauses.Length; i++)
            {
                if (clauses[i] is ClauseInfo ci && ci.FieldName == pred.FieldName)
                {
                    matchingClause = ci;
                    break;
                }
            }
        }

        if (matchingClause == null)
            return;

        switch (pred.ValueType)
        {
            case ScanValueType.Long:
                if (long.TryParse(matchingClause.TermValue, out long lv))
                    longs.Add(lv);
                if (matchingClause.TermValue2 != null && long.TryParse(matchingClause.TermValue2, out long lv2))
                    longs.Add(lv2);
                break;
            case ScanValueType.Double:
                if (double.TryParse(matchingClause.TermValue,
                    System.Globalization.NumberStyles.Float,
                    System.Globalization.CultureInfo.InvariantCulture, out double dv))
                    doubles.Add(dv);
                if (matchingClause.TermValue2 != null && double.TryParse(matchingClause.TermValue2,
                    System.Globalization.NumberStyles.Float,
                    System.Globalization.CultureInfo.InvariantCulture, out double dv2))
                    doubles.Add(dv2);
                break;
            case ScanValueType.Slice:
                var fieldMeta = indexSearcher.FieldMetadataBuilder(matchingClause.FieldName);
                slices.Add(indexSearcher.EncodeAndApplyAnalyzer(fieldMeta, matchingClause.TermValue));
                if (matchingClause.TermValue2 != null)
                    slices.Add(indexSearcher.EncodeAndApplyAnalyzer(fieldMeta, matchingClause.TermValue2));
                break;
        }
    }

    /// <summary>
    /// Populate the highlighting terms dictionary from the plan's clauses.
    /// The old CoraxQueryBuilder did this as a side effect during query building.
    /// The bitmap pipeline must do it explicitly after plan building.
    /// </summary>
    public static void PopulateHighlightingTerms(QueryPlan plan, Dictionary<string, CoraxHighlightingTermIndex> highlightingTerms, QueryMetadata metadata)
    {
        if (highlightingTerms == null || plan.Clauses == null)
            return;

        foreach (var clauseObj in plan.Clauses)
        {
            if (clauseObj is not ClauseInfo clause || clause.FieldName == null)
                continue;

            PopulateHighlightingForClause(clause, highlightingTerms, metadata);

            // Also handle OrGroup sub-clauses
            if (clause.ClauseType == ClauseType.OrGroup && clause.OrSubClauses != null)
            {
                foreach (var sub in clause.OrSubClauses)
                    PopulateHighlightingForClause(sub, highlightingTerms, metadata);
            }
        }
    }

    private static void PopulateHighlightingForClause(ClauseInfo clause, Dictionary<string, CoraxHighlightingTermIndex> highlightingTerms, QueryMetadata metadata)
    {
        string fieldName = clause.FieldName;
        if (fieldName == null)
            return;

        if (highlightingTerms.TryGetValue(fieldName, out var existingTerm))
        {
            // Already populated (e.g., multiple clauses on same field) — update values if needed
            if (existingTerm.Values == null)
                existingTerm.Values = GetHighlightingValues(clause);
            return;
        }

        var term = new CoraxHighlightingTermIndex
        {
            FieldName = fieldName,
            Values = GetHighlightingValues(clause)
        };

        if (metadata.IsDynamic && clause.ClauseType == ClauseType.Search)
            term.DynamicFieldName = AutoIndexField.GetSearchAutoIndexFieldName(fieldName);
        else if (metadata.IsDynamic && clause.IsExact)
            term.DynamicFieldName = AutoIndexField.GetExactAutoIndexFieldName(fieldName);

        highlightingTerms[fieldName] = term;

        // For dynamic indexes, also add the dynamic field name variant
        if (term.DynamicFieldName != null)
            highlightingTerms[term.DynamicFieldName] = term;
    }

    private static object GetHighlightingValues(ClauseInfo clause)
    {
        return clause.ClauseType switch
        {
            ClauseType.Between => clause.TermValue != null && clause.TermValue2 != null
                ? new Tuple<string, string>(clause.TermValue, clause.TermValue2)
                : clause.TermValue,
            ClauseType.In when clause.InTerms != null => clause.InTerms,
            ClauseType.Search => clause.TermValue,
            _ => clause.TermValue
        };
    }

    /// <summary>Convert a ClauseInfo to a ScanPredicateInfo for entry scan IL emission.
    /// Returns null for complex clauses that can't be entry-scanned.</summary>
    private static ScanPredicateInfo? BuildScanPredicateInfo(ClauseInfo clause,
        ref int longIndex, ref int doubleIndex, ref int sliceIndex)
    {
        // Complex clauses can't be entry-scanned
        switch (clause.ClauseType)
        {
            case ClauseType.Search:
            case ClauseType.Regex:
            case ClauseType.Spatial:
            case ClauseType.Vector:
            case ClauseType.In:
            case ClauseType.AllIn:
            case ClauseType.Exists:
            case ClauseType.StartsWith:
            case ClauseType.EndsWith:
                return null;

            case ClauseType.OrGroup:
            {
                if (clause.OrSubClauses == null || clause.OrSubClauses.Count == 0)
                    return null;
                var branches = new List<ScanPredicateInfo>();
                int li = longIndex, di = doubleIndex, si = sliceIndex;
                foreach (var sub in clause.OrSubClauses)
                {
                    var subPred = BuildScanPredicateInfo(sub, ref li, ref di, ref si);
                    if (subPred == null)
                        return null; // Any complex sub-clause → can't entry-scan the whole group
                    branches.Add(subPred.Value);
                }
                longIndex = li; doubleIndex = di; sliceIndex = si;
                return new ScanPredicateInfo
                {
                    FieldName = clause.OrSubClauses[0].FieldName,
                    OrBranches = branches.ToArray()
                };
            }
        }

        // Determine value type and comparison op
        ScanCompareOp compareOp = clause.ClauseType switch
        {
            ClauseType.Equals => ScanCompareOp.Equal,
            ClauseType.NotEquals => ScanCompareOp.NotEqual,
            ClauseType.GreaterThan => ScanCompareOp.GreaterThan,
            ClauseType.GreaterThanOrEqual => ScanCompareOp.GreaterThanOrEqual,
            ClauseType.LessThan => ScanCompareOp.LessThan,
            ClauseType.LessThanOrEqual => ScanCompareOp.LessThanOrEqual,
            ClauseType.Between => ScanCompareOp.Between,
            _ => ScanCompareOp.Equal
        };

        // Determine the value type from the term value
        if (clause.TermValue != null && long.TryParse(clause.TermValue, out _))
        {
            int paramIdx = longIndex++;
            int paramIdx2 = clause.ClauseType == ClauseType.Between && clause.TermValue2 != null
                ? longIndex++ : -1;
            return new ScanPredicateInfo
            {
                FieldName = clause.FieldName,
                ValueType = ScanValueType.Long,
                CompareOp = compareOp,
                ParamIndex = paramIdx,
                ParamIndex2 = paramIdx2
            };
        }

        if (clause.TermValue != null && double.TryParse(clause.TermValue,
            System.Globalization.NumberStyles.Float,
            System.Globalization.CultureInfo.InvariantCulture, out _))
        {
            int paramIdx = doubleIndex++;
            int paramIdx2 = clause.ClauseType == ClauseType.Between && clause.TermValue2 != null
                ? doubleIndex++ : -1;
            return new ScanPredicateInfo
            {
                FieldName = clause.FieldName,
                ValueType = ScanValueType.Double,
                CompareOp = compareOp,
                ParamIndex = paramIdx,
                ParamIndex2 = paramIdx2
            };
        }

        // String/Slice comparison
        int sliceIdx = sliceIndex++;
        int sliceIdx2 = clause.ClauseType == ClauseType.Between && clause.TermValue2 != null
            ? sliceIndex++ : -1;
        return new ScanPredicateInfo
        {
            FieldName = clause.FieldName,
            ValueType = ScanValueType.Slice,
            CompareOp = compareOp,
            ParamIndex = sliceIdx,
            ParamIndex2 = sliceIdx2
        };
    }

    private static QueryPlan BuildAllEntriesPlan()
    {
        // No bitmap needed — AllEntries already implements IQueryMatch.Fill(),
        // so we iterate it directly without materializing into a bitmap first.
        return new QueryPlan
        {
            Ops = new[] { new PlanOp { Kind = PlanOpKind.DirectIterate, ParamIndex = 0 } },
            EntryScanPredicates = Array.Empty<MultiUnaryItem[]>(),
            Clauses = Array.Empty<ClauseInfo>(),
            IsAllEntries = true
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
        Spatial,
        Vector,
        OrGroup, // A group of OR'd sub-clauses
    }

    internal class ClauseInfo
    {
        public string FieldName;
        public string TermValue;
        public ValueTokenType TermValueType; // for type-aware comparison resolution
        public string TermValue2; // for BETWEEN
        public List<string> InTerms; // for IN
        public List<ClauseInfo> OrSubClauses; // for OrGroup
        public MethodExpression MethodExpression; // for Spatial, Vector
        public ClauseType ClauseType;
        public long Cardinality = -1;
        public int OriginalIndex;
        public bool IsNegated;
        public bool IsExact;
        public float BoostFactor;
    }

    /// <summary>
    /// Resolve clause infos to IQueryMatch instances for execution.
    /// Uses existing IndexSearcher methods (TermQuery, etc.) which handle
    /// all the complexity of analyzer application, CompactKey encoding,
    /// posting list resolution, etc.
    /// </summary>
    public static IQueryMatch[] ResolveMatches(QueryPlan plan, IndexSearcher indexSearcher,
        PlanParameters parameters = null, CoraxQueryBuilder.Parameters builderParams = null)
    {
        // All-entries plan: no clauses, single AllEntries match
        if (plan.IsAllEntries)
            return new IQueryMatch[] { indexSearcher.AllEntries() };

        var clauses = plan.Clauses;
        if (clauses == null || clauses.Length == 0)
            return Array.Empty<IQueryMatch>();

        // Check for standalone NotEquals pattern: plan has Fill(AllEntries) + ANDNOT(term)
        if (clauses.Length == 1 && ((ClauseInfo)clauses[0]).IsNegated)
        {
            var clause = (ClauseInfo)clauses[0];
            return new IQueryMatch[]
            {
                indexSearcher.AllEntries(),
                indexSearcher.TermQuery(indexSearcher.FieldMetadataBuilder(clause.FieldName), clause.TermValue)
            };
        }

        // Flatten OrGroups and AllIn: each sub-clause/term becomes a separate match.
        int totalMatches = 0;
        for (int i = 0; i < clauses.Length; i++)
        {
            var clause = (ClauseInfo)clauses[i];
            if (clause.ClauseType == ClauseType.OrGroup && clause.OrSubClauses != null)
                totalMatches += clause.OrSubClauses.Count;
            else if (clause.ClauseType == ClauseType.AllIn && clause.InTerms != null)
                totalMatches += clause.InTerms.Count;
            else
                totalMatches++;
        }
        int extraSlots = plan.AllNegated ? 1 : 0;
        var matches = new IQueryMatch[totalMatches + extraSlots];
        int matchIdx = 0;
        for (int i = 0; i < clauses.Length; i++)
        {
            var clause = (ClauseInfo)clauses[i];
            if (clause.ClauseType == ClauseType.OrGroup && clause.OrSubClauses != null)
            {
                foreach (var sub in clause.OrSubClauses)
                {
                    var match = ResolveClause(sub, indexSearcher, parameters, builderParams);
                    if (sub.BoostFactor > 0)
                        match = indexSearcher.Boost(match, sub.BoostFactor);
                    matches[matchIdx++] = match;
                }
            }
            else if (clause.ClauseType == ClauseType.AllIn && clause.InTerms != null)
            {
                var fieldMeta = indexSearcher.FieldMetadataBuilder(clause.FieldName);
                foreach (var term in clause.InTerms)
                    matches[matchIdx++] = indexSearcher.TermQuery(fieldMeta, term);
            }
            else
            {
                var match = ResolveClause(clause, indexSearcher, parameters, builderParams);
                if (clause.BoostFactor > 0)
                    match = indexSearcher.Boost(match, clause.BoostFactor);
                matches[matchIdx++] = match;
            }
        }
        if (plan.AllNegated)
            matches[matchIdx] = indexSearcher.AllEntries();
        return matches;
    }

    private static IQueryMatch ResolveClause(ClauseInfo clause, IndexSearcher indexSearcher,
        PlanParameters parameters = null, CoraxQueryBuilder.Parameters builderParams = null)
    {
        // Use proper field metadata from the index schema when available.
        // This ensures correct analyzer application for static index fields.
        // Requires all of: Allocator, Index, IndexFieldsMapping to be non-null.
        // Use proper field metadata from the index schema for simple clauses.
        // Spatial/Vector/Search have their own field resolution paths.
        FieldMetadata fieldMeta;
        bool useBuilderParams = builderParams != null
            && clause.ClauseType != ClauseType.Spatial
            && clause.ClauseType != ClauseType.Vector
            && clause.ClauseType != ClauseType.Search;
        if (useBuilderParams)
        {
            string resolvedFieldName = clause.FieldName;
            // For exact queries on auto-indexes, use the _exact field variant
            if (clause.IsExact && builderParams.Metadata.IsDynamic)
                resolvedFieldName = AutoIndexField.GetExactAutoIndexFieldName(resolvedFieldName);
            fieldMeta = QueryBuilderHelper.GetFieldMetadata(in builderParams, resolvedFieldName, exact: clause.IsExact, hasBoost: builderParams.HasBoost);
        }
        else
        {
            fieldMeta = indexSearcher.FieldMetadataBuilder(clause.FieldName, hasBoost: parameters?.HasBoost ?? false);
        }

        switch (clause.ClauseType)
        {
            case ClauseType.Equals:
            case ClauseType.NotEquals:
            {
                // Use the value type to determine the right TermQuery overload
                IQueryMatch eqMatch;
                if (clause.TermValueType == ValueTokenType.Long
                    && long.TryParse(clause.TermValue, out long eqLong))
                    eqMatch = indexSearcher.TermQuery(fieldMeta, eqLong);
                else if (clause.TermValueType == ValueTokenType.Double
                    && double.TryParse(clause.TermValue,
                        System.Globalization.NumberStyles.Float,
                        System.Globalization.CultureInfo.InvariantCulture, out double eqDouble))
                    eqMatch = indexSearcher.TermQuery(fieldMeta, eqDouble);
                else
                    eqMatch = indexSearcher.TermQuery(fieldMeta, clause.TermValue);
                return eqMatch;
            }

            case ClauseType.GreaterThan:
                if (long.TryParse(clause.TermValue, out long gtLong))
                    return indexSearcher.GreaterThanQuery(fieldMeta, gtLong);
                if (double.TryParse(clause.TermValue, System.Globalization.NumberStyles.Float,
                    System.Globalization.CultureInfo.InvariantCulture, out double gtDouble))
                    return indexSearcher.GreaterThanQuery(fieldMeta, gtDouble);
                return indexSearcher.GreaterThanQuery(fieldMeta, clause.TermValue);

            case ClauseType.GreaterThanOrEqual:
                if (long.TryParse(clause.TermValue, out long gteLong))
                    return indexSearcher.GreatThanOrEqualsQuery(fieldMeta, gteLong);
                if (double.TryParse(clause.TermValue, System.Globalization.NumberStyles.Float,
                    System.Globalization.CultureInfo.InvariantCulture, out double gteDouble))
                    return indexSearcher.GreatThanOrEqualsQuery(fieldMeta, gteDouble);
                return indexSearcher.GreatThanOrEqualsQuery(fieldMeta, clause.TermValue);

            case ClauseType.LessThan:
                if (long.TryParse(clause.TermValue, out long ltLong))
                    return indexSearcher.LessThanQuery(fieldMeta, ltLong);
                if (double.TryParse(clause.TermValue, System.Globalization.NumberStyles.Float,
                    System.Globalization.CultureInfo.InvariantCulture, out double ltDouble))
                    return indexSearcher.LessThanQuery(fieldMeta, ltDouble);
                return indexSearcher.LessThanQuery(fieldMeta, clause.TermValue);

            case ClauseType.LessThanOrEqual:
                if (long.TryParse(clause.TermValue, out long lteLong))
                    return indexSearcher.LessThanOrEqualsQuery(fieldMeta, lteLong);
                if (double.TryParse(clause.TermValue, System.Globalization.NumberStyles.Float,
                    System.Globalization.CultureInfo.InvariantCulture, out double lteDouble))
                    return indexSearcher.LessThanOrEqualsQuery(fieldMeta, lteDouble);
                return indexSearcher.LessThanOrEqualsQuery(fieldMeta, clause.TermValue);

            case ClauseType.Between:
                if (long.TryParse(clause.TermValue, out long btwLowLong) &&
                    long.TryParse(clause.TermValue2, out long btwHighLong))
                    return indexSearcher.BetweenQuery(fieldMeta, btwLowLong, btwHighLong);
                if (double.TryParse(clause.TermValue, System.Globalization.NumberStyles.Float,
                    System.Globalization.CultureInfo.InvariantCulture, out double btwLowDouble) &&
                    double.TryParse(clause.TermValue2, System.Globalization.NumberStyles.Float,
                    System.Globalization.CultureInfo.InvariantCulture, out double btwHighDouble))
                    return indexSearcher.BetweenQuery(fieldMeta, btwLowDouble, btwHighDouble);
                return indexSearcher.BetweenQuery(fieldMeta, clause.TermValue, clause.TermValue2);

            case ClauseType.In:
            {
                // Check if terms are numeric — InQuery only handles string terms correctly.
                // For numeric IN values, we need to use TermQuery<long> per term and OR them.
                if (clause.InTerms != null && clause.InTerms.Count > 0
                    && long.TryParse(clause.InTerms[0], out _))
                {
                    // Numeric IN — OR the individual TermQuery<long> results
                    IQueryMatch inMatch = null;
                    foreach (var term in clause.InTerms)
                    {
                        IQueryMatch termMatch;
                        if (long.TryParse(term, out long inLong))
                            termMatch = indexSearcher.TermQuery(fieldMeta, inLong);
                        else if (double.TryParse(term, System.Globalization.NumberStyles.Float,
                            System.Globalization.CultureInfo.InvariantCulture, out double inDouble))
                            termMatch = indexSearcher.TermQuery(fieldMeta, inDouble);
                        else
                            termMatch = indexSearcher.TermQuery(fieldMeta, term);

                        inMatch = inMatch == null ? termMatch : indexSearcher.Or(inMatch, termMatch);
                    }
                    return inMatch ?? TermMatch.CreateEmpty(indexSearcher, indexSearcher.Allocator);
                }
                return indexSearcher.InQuery(fieldMeta, clause.InTerms);
            }

            case ClauseType.AllIn:
                // AllIn sub-terms are expanded into separate matches by ResolveMatches.
                throw new InvalidOperationException(
                    "AllIn should be expanded by ResolveMatches, not resolved as a single clause.");

            case ClauseType.Exists:
                return indexSearcher.ExistsQuery(fieldMeta);

            case ClauseType.StartsWith:
                return indexSearcher.StartWithQuery(fieldMeta, clause.TermValue);

            case ClauseType.EndsWith:
                return indexSearcher.EndsWithQuery(fieldMeta, clause.TermValue);

            case ClauseType.Search:
            {
                // Search needs proper field metadata with search analyzer
                FieldMetadata searchMeta;
                if (builderParams != null)
                {
                    string searchFieldName = clause.FieldName;
                    if (builderParams.Metadata.IsDynamic)
                        searchFieldName = AutoIndexField.GetSearchAutoIndexFieldName(searchFieldName);

                    searchMeta = QueryBuilderHelper.GetFieldMetadata(
                        builderParams.Allocator, searchFieldName, builderParams.Index,
                        builderParams.IndexFieldsMapping, builderParams.FieldsToFetch,
                        builderParams.HasDynamics, builderParams.DynamicFields,
                        handleSearch: true, hasBoost: builderParams.HasBoost);
                }
                else if (parameters?.Index != null && parameters.IndexFieldsMapping != null)
                {
                    string searchFieldName = clause.FieldName;
                    if (parameters.Metadata.IsDynamic)
                        searchFieldName = AutoIndexField.GetSearchAutoIndexFieldName(searchFieldName);

                    searchMeta = QueryBuilderHelper.GetFieldMetadata(
                        parameters.Allocator, searchFieldName, parameters.Index,
                        parameters.IndexFieldsMapping, parameters.FieldsToFetch,
                        parameters.HasDynamics, parameters.DynamicFields,
                        handleSearch: true, hasBoost: parameters.HasBoost);
                }
                else
                {
                    searchMeta = fieldMeta;
                }

                // Determine SearchQueryOptions based on index version
                var indexDef = builderParams?.Index?.Definition ?? parameters?.Index?.Definition;
                IndexSearcher.SearchQueryOptions searchQueryOptions;
                if (indexDef != null && IndexDefinitionBaseServerSide.IndexVersion.IsCoraxSearchWildcardAdjustmentSupported(indexDef.Version))
                    searchQueryOptions = IndexSearcher.SearchQueryOptions.PhraseQueryWithWildcardAdjustments;
                else if (indexDef != null && indexDef.Version >= IndexDefinitionBaseServerSide.IndexVersion.PhraseQuerySupportInCoraxIndexes)
                    searchQueryOptions = IndexSearcher.SearchQueryOptions.PhraseQuery;
                else
                    searchQueryOptions = IndexSearcher.SearchQueryOptions.Legacy;

                // For wildcard queries with WildcardAdjustments, use Legacy mode
                // which handles wildcard analyzer replacement internally
                if (searchQueryOptions == IndexSearcher.SearchQueryOptions.PhraseQueryWithWildcardAdjustments
                    && clause.TermValue != null && clause.TermValue.Length >= 1
                    && (clause.TermValue[0] == '*' || (clause.TermValue.Length >= 2 && clause.TermValue[^1] == '*')))
                {
                    searchQueryOptions = IndexSearcher.SearchQueryOptions.Legacy;
                }

                // Split search value respecting quoted phrases (same as old CoraxQueryBuilder.GetValues)
                var searchValues = SplitSearchTerms(clause.TermValue);

                return indexSearcher.SearchQuery(searchMeta,
                    searchValues,
                    Constants.Search.Operator.Or,
                    searchQueryOptions);
            }

            case ClauseType.Regex:
                return indexSearcher.RegexQuery(fieldMeta,
                    new System.Text.RegularExpressions.Regex(clause.TermValue));

            case ClauseType.Spatial:
            {
                if (builderParams == null || clause.MethodExpression == null)
                    throw new InvalidOperationException("Spatial resolution requires builder parameters");
                var spatialMethod = QueryMethod.GetMethodType(clause.MethodExpression.Name.Value);
                return CoraxQueryBuilder.HandleSpatial(builderParams, clause.MethodExpression, spatialMethod);
            }

            case ClauseType.Vector:
            {
                if (builderParams == null || clause.MethodExpression == null)
                    throw new InvalidOperationException("Vector resolution requires builder parameters");
                var vectorItem = CoraxQueryBuilder.HandleVector(builderParams, clause.MethodExpression, false);
                // Materialize with null inner — the bitmap provides the candidate set
                return vectorItem.Materialize(null);
            }

            case ClauseType.OrGroup:
                // OrGroup sub-clauses are expanded into separate matches by ResolveMatches.
                // This case should not be reached — OrGroups are handled at the ResolveMatches level.
                throw new InvalidOperationException(
                    "OrGroup should be expanded by ResolveMatches, not resolved as a single clause.");

            default:
                throw new InvalidOperationException($"Unexpected ClauseType {clause.ClauseType} in ResolveClause.");
        }
    }
}
