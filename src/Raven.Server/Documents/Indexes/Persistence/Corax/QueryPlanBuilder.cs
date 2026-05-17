using System;
using System.Collections.Generic;
using System.Diagnostics;
using Corax.Querying.Planning;
using Corax.Mappings;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Sparrow.Json;
using Sparrow.Server;
using Constants = Corax.Constants;
using ClientConstants = Raven.Client.Constants;
using IndexSearcher = Corax.Querying.IndexSearcher;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

/// <summary>
/// Builds a QueryExecution from a parsed RQL query.
///
/// The planner has three independent concerns, each in its own partial file:
///
///   QueryPlanBuilder.cs (this file) — structural
///     Data model (ClauseType, ClauseInfo, PlanParameters), RQL AST parsing,
///     cardinality estimation, plan emission (EmitPlan), dispatch classification.
///     Output: a QueryExecution with PlanOp[] — per-execution, consumed by the IL delegate.
///
///   QueryPlanBuilder.Resolution.cs — per-execution
///     BuildAndCompile entry point, match/term-source resolution, scan parameter
///     extraction, highlighting, sorting, spatial/vector materialization.
///     Runs once per query execution; binds concrete posting lists to the cached plan.
///
///   QueryPlanBuilder.Inspection.cs — diagnostic
///     BuildInspectionGraph, BuildInspectionTemplate. Studio visualization tree
///     built from the cached InspectionTemplate plus runtime telemetry.
///
/// Expression types handled:
/// - BinaryExpression (AND, OR, =, !=, >, >=, &lt;, &lt;=)
/// - BetweenExpression
/// - InExpression
/// - NegatedExpression
/// - TrueExpression (constant folding)
/// - MethodExpression (search, startsWith, endsWith, exists, boost, exact, regex)
///
/// MoreLikeThis is handled by a separate execution path (Index.cs → reader.MoreLikeThis())
/// and never reaches this planner.
/// </summary>
internal static partial class QueryPlanBuilder
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
        public Index Index;
        public IndexFieldsMapping IndexFieldsMapping;
        public FieldsToFetch FieldsToFetch;
        public ByteStringContext Allocator;
        public bool HasDynamics;
        public Lazy<List<string>> DynamicFields;
        public bool HasBoost;
    }

    /// <summary>
    /// Accumulates typed parameter values during query plan building.
    /// Each Add call stores the native value in the appropriate-typed list
    /// and returns a <see cref="PackedParam"/> encoding (type + index) for the clause.
    /// Values are stored as their native types — no string round-trips.
    /// </summary>
    private sealed class ValueWriter
    {
        private readonly List<long> _longs = [];
        private readonly List<double> _doubles = [];
        private readonly List<string> _strings = [];

        private PackedParam AddLong(long value)
        {
            _longs.Add(value);
            return new PackedParam(PackedParam.TypeLong, _longs.Count - 1);
        }

        private PackedParam AddDouble(double value)
        {
            _doubles.Add(value);
            return new PackedParam(PackedParam.TypeDouble, _doubles.Count - 1);
        }

        private PackedParam AddString(string value)
        {
            _strings.Add(value);
            return new PackedParam(PackedParam.TypeString, _strings.Count - 1);
        }
        
        /// <summary>Add a resolved value by its detected type. Used by Parse* methods
        /// after <see cref="ResolveTermValue"/> determines the native type.</summary>
        public PackedParam Add(object value, ValueTokenType type)
        {
            return type switch
            {
                ValueTokenType.Long => AddLong(value is long l ? l : Convert.ToInt64(value)),
                ValueTokenType.Double => AddDouble(value is double d ? d : Convert.ToDouble(value)),
                _ => AddString(value?.ToString())
            };
        }

        /// <summary>Add a pair of resolved values (for BETWEEN).</summary>
        public PackedParam AddPair(object value1, object value2, ValueTokenType type)
        {
            return type switch
            {
                ValueTokenType.Long => AddLongPair(
                    value1 is long l1 ? l1 : Convert.ToInt64(value1),
                    value2 is long l2 ? l2 : Convert.ToInt64(value2)),
                ValueTokenType.Double => AddDoublePair(
                    value1 is double d1 ? d1 : Convert.ToDouble(value1),
                    value2 is double d2 ? d2 : Convert.ToDouble(value2)),
                _ => AddStringPair(value1?.ToString(), value2?.ToString())
            };

            PackedParam AddLongPair(long low, long high)
            {
                _longs.Add(low);
                _longs.Add(high);
                return new PackedParam(PackedParam.TypeLong, _longs.Count - 2, _longs.Count - 1);
            }

            PackedParam AddDoublePair(double low, double high)
            {
                _doubles.Add(low);
                _doubles.Add(high);
                return new PackedParam(PackedParam.TypeDouble, _doubles.Count - 2, _doubles.Count - 1);
            }

            PackedParam AddStringPair(string low, string high)
            {
                _strings.Add(low);
                _strings.Add(high);
                return new PackedParam(PackedParam.TypeString, _strings.Count - 2, _strings.Count - 1);
            }
        }

        public int LongCount => _longs.Count;
        public int DoubleCount => _doubles.Count;
        public int StringCount => _strings.Count;

        public long GetLong(int index) => _longs[index];
        public double GetDouble(int index) => _doubles[index];
        public string GetString(int index) => _strings[index];

        public long[] GetLongs() => _longs.Count > 0 ? _longs.ToArray() : [];
        public double[] GetDoubles() => _doubles.Count > 0 ? _doubles.ToArray() : [];
        public string[] GetStrings() => _strings.Count > 0 ? _strings.ToArray() : [];
    }

    private static ParamValueType ToParamValueType(ValueTokenType t) => t switch
    {
        ValueTokenType.Long => ParamValueType.Long,
        ValueTokenType.Double => ParamValueType.Double,
        ValueTokenType.Parameter => ParamValueType.Parameter,
        _ => ParamValueType.String
    };

    private static ValueTokenType ToValueTokenType(ParamValueType t) => t switch
    {
        ParamValueType.Long => ValueTokenType.Long,
        ParamValueType.Double => ValueTokenType.Double,
        ParamValueType.Parameter => ValueTokenType.Parameter,
        _ => ValueTokenType.String
    };

    /// <summary>Type-appropriate "no lower bound" value, used when rewriting BETWEEN with the
    /// "*" sentinel. We still write a real value into ValueWriter so AddPair doesn't crash
    /// (Convert.ToInt64("*") would throw); the actual bound semantics are applied at resolution
    /// via <see cref="ClauseExecution.BetweenLowUnbounded"/>.</summary>
    private static object GetTypeMinSentinel(ParamValueType t) => t switch
    {
        ParamValueType.Long => long.MinValue,
        ParamValueType.Double => double.MinValue,
        _ => string.Empty
    };

    /// <summary>Type-appropriate "no upper bound" value — see <see cref="GetTypeMinSentinel"/>.</summary>
    private static object GetTypeMaxSentinel(ParamValueType t) => t switch
    {
        ParamValueType.Long => long.MaxValue,
        ParamValueType.Double => double.MaxValue,
        _ => "\uFFFF"
    };

    /// <summary>True when a value of type <paramref name="termType"/> cannot be coerced
    /// to <paramref name="dominantType"/> without throwing. Used to filter mixed-type IN
    /// lists: a string term in an otherwise-numeric IN list (e.g. IN(DateTime, "Shalom")
    /// on a DateTime-indexed field) can never match a numeric-indexed term and would
    /// throw on Convert.ToInt64, so it is dropped instead.</summary>
    private static bool IsTypeIncompatible(ParamValueType termType, ParamValueType dominantType)
    {
        if (termType == dominantType) return false;
        // Long and Double are mutually coercible.
        if ((termType == ParamValueType.Long || termType == ParamValueType.Double) &&
            (dominantType == ParamValueType.Long || dominantType == ParamValueType.Double))
            return false;
        // Anything else (string vs numeric, or vice versa) is incompatible.
        return true;
    }

    private static SpatialOperationType ToSpatialOp(MethodType t) => t switch
    {
        MethodType.Spatial_Within => SpatialOperationType.Within,
        MethodType.Spatial_Contains => SpatialOperationType.Contains,
        MethodType.Spatial_Disjoint => SpatialOperationType.Disjoint,
        MethodType.Spatial_Intersects => SpatialOperationType.Intersects,
        _ => SpatialOperationType.Within
    };

    private enum BooleanOp { And, Or, True, False, Leaf }

    /// <summary>Parse the RQL AST into a structural clause template.
    /// Captures field names, clause types, parameter bindings, and literal values.
    /// No cardinality estimation, no sorting, no plan emission.
    /// Those happen in BuildAndCompile after PopulateClauseValues.</summary>
    public static ClauseTemplate ParseTemplate(PlanParameters p)
    {
        var query = p.Metadata.Query;
        var indexSearcher = p.IndexSearcher;
        var queryParameters = p.QueryParameters;
        var metadata = p.Metadata;
        if (query.Where == null)
            return new ClauseTemplate { IsAllEntries = true, Clauses = [] };

        bool hasMixedAndOr = false;
        var clauses = new List<ClauseInfo>();
        var rootOp = ParseExpression(query.Where, indexSearcher, clauses, queryParameters, metadata, ref hasMixedAndOr);

        if (rootOp == BooleanOp.True || clauses.Count == 0)
            return new ClauseTemplate { IsAllEntries = true, Clauses = [] };
        if (rootOp == BooleanOp.False)
            return new ClauseTemplate { Clauses = [] };

        bool isOr = rootOp == BooleanOp.Or;

        // Separate spatial and vector clauses from the filter chain (AND queries only).
        ClauseInfo[] spatialClauses = null;
        ClauseInfo[] vectorClauses = null;
        if (!isOr)
        {
            List<ClauseInfo> spatialList = null, vectorList = null;
            for (int i = clauses.Count - 1; i >= 0; i--)
            {
                switch (clauses[i].ClauseType)
                {
                    case ClauseType.Spatial:
                        spatialList ??= [];
                        spatialList.Add(clauses[i]);
                        clauses.RemoveAt(i);
                        break;
                    case ClauseType.Vector:
                        vectorList ??= [];
                        vectorList.Add(clauses[i]);
                        clauses.RemoveAt(i);
                        break;
                }
            }
            spatialClauses = spatialList?.ToArray();
            vectorClauses = vectorList?.ToArray();

            if (clauses.Count == 0 && (spatialClauses != null || vectorClauses != null))
            {
                return new ClauseTemplate
                {
                    IsAllEntries = true,
                    Clauses = [],
                    IsOr = false,
                    SpatialClauses = spatialClauses,
                    VectorClauses = vectorClauses
                };
            }
        }

        var templateClauses = clauses.ToArray();
        FreezeAll(templateClauses);
        FreezeAll(spatialClauses);
        FreezeAll(vectorClauses);
        return new ClauseTemplate
        {
            Clauses = templateClauses,
            IsAllEntries = false,
            IsOr = isOr,
            SpatialClauses = spatialClauses,
            VectorClauses = vectorClauses
        };
    }

    /// <summary>Recursively freeze every <see cref="ClauseInfo"/> in a template list,
    /// including nested OrSubClauses / AndSubClauses. Called once at the end of
    /// <see cref="ParseTemplate"/> so the entire shared template tree rejects mutation
    /// from per-execution code paths (which must <see cref="ClauseInfo.Clone"/> before
    /// rewriting).</summary>
    private static void FreezeAll(ClauseInfo[] list)
    {
        if (list == null)
            return;
        for (int i = 0; i < list.Length; i++)
        {
            var c = list[i];
            if (c == null || c.IsFrozen)
                continue;
            if (c.OrSubClauses != null)
                for (int j = 0; j < c.OrSubClauses.Count; j++)
                    FreezeRecursive(c.OrSubClauses[j]);
            if (c.AndSubClauses != null)
                for (int j = 0; j < c.AndSubClauses.Count; j++)
                    FreezeRecursive(c.AndSubClauses[j]);
            c.Freeze();
        }
    }

    private static void FreezeRecursive(ClauseInfo c)
    {
        if (c == null || c.IsFrozen)
            return;
        if (c.OrSubClauses != null)
            for (int j = 0; j < c.OrSubClauses.Count; j++)
                FreezeRecursive(c.OrSubClauses[j]);
        if (c.AndSubClauses != null)
            for (int j = 0; j < c.AndSubClauses.Count; j++)
                FreezeRecursive(c.AndSubClauses[j]);
        c.Freeze();
    }

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

            case MethodExpression method:
                ParseMethod(method, indexSearcher, clauses, queryParameters, metadata, ref hasMixedAndOr);
                return BooleanOp.Leaf;

            case NegatedExpression negated:
                ParseNegated(negated, indexSearcher, clauses, queryParameters, metadata, ref hasMixedAndOr);
                return BooleanOp.Leaf;

            case TrueExpression:
                return BooleanOp.True;

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
                if (left == BooleanOp.True) return right;
                if (right == BooleanOp.True) return left;
                if (left == BooleanOp.False || right == BooleanOp.False) return BooleanOp.False;
                return BooleanOp.And;
            }

            case OperatorType.Or:
            {
                BooleanOp left, right;

                if (be.Left is BinaryExpression { Operator: OperatorType.And })
                {
                    var andClauses = new List<ClauseInfo>();
                    left = ParseExpression(be.Left, indexSearcher, andClauses, queryParameters, metadata, ref hasMixedAndOr);
                    clauses.Add(new ClauseInfo
                    {
                        ClauseType = ClauseType.AndGroup,
                        AndSubClauses = andClauses,
                        OriginalIndex = clauses.Count
                    });
                }
                else
                {
                    left = ParseExpression(be.Left, indexSearcher, clauses, queryParameters, metadata, ref hasMixedAndOr);
                }

                if (be.Right is BinaryExpression { Operator: OperatorType.And })
                {
                    var andClauses = new List<ClauseInfo>();
                    right = ParseExpression(be.Right, indexSearcher, andClauses, queryParameters, metadata, ref hasMixedAndOr);
                    clauses.Add(new ClauseInfo
                    {
                        ClauseType = ClauseType.AndGroup,
                        AndSubClauses = andClauses,
                        OriginalIndex = clauses.Count
                    });
                }
                else
                {
                    right = ParseExpression(be.Right, indexSearcher, clauses, queryParameters, metadata, ref hasMixedAndOr);
                }

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
        if (TryGetFieldName(be.Left, metadata, queryParameters, out string fieldName) == false)
            throw new InvalidQueryException($"Comparison left side must be a field expression or id(), but got: {be.Left.Type}");

        clauses.Add(new ClauseInfo
        {
            FieldName = fieldName,
            ClauseType = be.Operator == OperatorType.NotEqual ? ClauseType.NotEquals : ClauseType.Equals,
            OriginalIndex = clauses.Count,
            Bindings = [CreateBinding(be.Right, queryParameters)]
        });
    }

    private static void ParseRangeComparison(BinaryExpression be, List<ClauseInfo> clauses,
        BlittableJsonReaderObject queryParameters, QueryMetadata metadata)
    {
        if (TryGetFieldName(be.Left, metadata, queryParameters, out string fieldName) == false)
            throw new InvalidQueryException($"Range comparison left side must be a field expression or id(), but got: {be.Left.Type}");

        clauses.Add(new ClauseInfo
        {
            FieldName = fieldName,
            Bindings = [CreateBinding(be.Right, queryParameters)],
            ClauseType = be.Operator switch
            {
                OperatorType.GreaterThan => ClauseType.GreaterThan,
                OperatorType.GreaterThanEqual => ClauseType.GreaterThanOrEqual,
                OperatorType.LessThan => ClauseType.LessThan,
                OperatorType.LessThanEqual => ClauseType.LessThanOrEqual,
                _ => ClauseType.Equals
            },
            OriginalIndex = clauses.Count,
        });
    }

    private static void ParseBetween(BetweenExpression between, List<ClauseInfo> clauses,
        BlittableJsonReaderObject queryParameters, QueryMetadata metadata)
    {
        if (TryGetFieldName(between.Source, metadata, queryParameters, out string resolvedFieldName) == false)
            throw new InvalidQueryException($"BETWEEN source must be a field expression or id(), but got: {between.Source.Type}");

        var minBinding = CreateBinding(between.Min, queryParameters);
        var maxBinding = CreateBinding(between.Max, queryParameters);

        // Type validation for literals (parameter types validated in PopulateParameters)
        if (minBinding is { LiteralType: not ParamValueType.Parameter } && maxBinding is { LiteralType: not ParamValueType.Parameter }
            && minBinding.LiteralType != maxBinding.LiteralType)
        {
            throw new InvalidQueryException(
                $"BETWEEN bounds for field '{resolvedFieldName}' have different types: " +
                $"low is {minBinding.LiteralType}, high is {maxBinding.LiteralType}. Both must be the same type.");
        }

        clauses.Add(new ClauseInfo
        {
            FieldName = resolvedFieldName,
            ClauseType = ClauseType.Between,
            OriginalIndex = clauses.Count,
            Bindings = [minBinding, maxBinding]
        });
    }

    private static void ParseIn(InExpression inExpr, List<ClauseInfo> clauses,
        BlittableJsonReaderObject queryParameters, QueryMetadata metadata)
    {
        if (TryGetFieldName(inExpr.Source, metadata, queryParameters, out string resolvedFieldName) == false)
            throw new InvalidQueryException($"IN source must be a field expression or id(), but got: {inExpr.Source.Type}");

        // Capture bindings for each IN term. Array parameters expand at PopulateParameters time.
        var inBindings = new List<ParameterBinding>();
        foreach (var value in inExpr.Values)
        {
            if (value is ValueExpression ve)
            {
                if (ve.Value == ValueTokenType.Parameter)
                {
                    // Parameter — could be a single value or an array. Mark as array-capable.
                    inBindings.Add(new ParameterBinding { ParameterName = ve.Token.Value, LiteralType = ParamValueType.Parameter });
                }
                else
                {
                    // Literal value
                    var rawValue = ve.GetValue(queryParameters);
                    var (resolved, resolvedType) = ResolveInValue(rawValue, ve.Value);
                    inBindings.Add(new ParameterBinding { LiteralValue = resolved, LiteralType = ToParamValueType(resolvedType) });
                }
            }
        }

        // Empty IN() with no bindings still creates an In clause —
        // PopulateClauseValues sets InTermCount=0, EmitPlan handles empty IN.
        clauses.Add(new ClauseInfo
        {
            FieldName = resolvedFieldName,
            ClauseType = inExpr.All ? ClauseType.AllIn : ClauseType.In,
            OriginalIndex = clauses.Count,
            Bindings = inBindings.ToArray()
        });
    }

    private static void ParseNegated(NegatedExpression negated, IndexSearcher indexSearcher,
        List<ClauseInfo> clauses, BlittableJsonReaderObject queryParameters, QueryMetadata metadata, ref bool hasMixedAndOr)
    {
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
            {
                if (method.Arguments.Count == 0)
                    throw new InvalidQueryException("exists() requires a field argument.");
                if (TryGetFieldName(method.Arguments[0], metadata, queryParameters, out var existsFieldName) == false)
                    throw new InvalidQueryException($"exists() argument must be a field name, but got: {method.Arguments[0].Type} ({method.Arguments[0]}).");
                clauses.Add(new ClauseInfo
                {
                    FieldName = existsFieldName,
                    ClauseType = ClauseType.Exists,
                    OriginalIndex = clauses.Count
                });
                break;
            }

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
                // Append boost factor binding to each inner clause's Bindings array.
                ParameterBinding boostBinding = method.Arguments.Count > 1
                    ? CreateBinding(method.Arguments[1], queryParameters) : null;
                if (boostBinding != null)
                {
                    for (int c = beforeCount; c < clauses.Count; c++)
                    {
                        var old = clauses[c].Bindings;
                        var extended = new ParameterBinding[(old?.Length ?? 0) + 1];
                        if (old != null) Array.Copy(old, extended, old.Length);
                        extended[^1] = boostBinding;
                        clauses[c].Bindings = extended;
                        clauses[c].HasBoost = true;
                    }
                }
                break;
            }

            case MethodType.Regex:
            {
                if (method.Arguments.Count < 2)
                    throw new InvalidQueryException($"regex() requires at least 2 arguments (field, pattern), but got {method.Arguments.Count}.");
                if (TryGetFieldName(method.Arguments[0], metadata, queryParameters, out var regexFieldName) == false)
                    throw new InvalidQueryException($"regex() first argument must be a field name, but got: {method.Arguments[0].Type} ({method.Arguments[0]}).");
                clauses.Add(new ClauseInfo
                {
                    FieldName = regexFieldName,
                    ClauseType = ClauseType.Regex,
                    OriginalIndex = clauses.Count,
                    Bindings = [CreateBinding(method.Arguments[1], queryParameters)]
                });
                break;
            }

            case MethodType.Spatial_Within:
            case MethodType.Spatial_Contains:
            case MethodType.Spatial_Disjoint:
            case MethodType.Spatial_Intersects:
            {
                // Capture bindings for all spatial sub-arguments.
                // Shape type and field name are structural; parameter values resolved per-execution.
                string spatialFieldName;
                if (metadata.IsDynamic && method.Arguments[0] is MethodExpression spatialPointExpr)
                    spatialFieldName = metadata.GetSpatialFieldName(spatialPointExpr, queryParameters);
                else if (TryGetFieldName(method.Arguments[0], metadata, queryParameters, out var sfn))
                    spatialFieldName = sfn;
                else
                    spatialFieldName = QueryBuilderHelper.ExtractIndexFieldName(metadata.Query, queryParameters, method.Arguments[0], metadata);

                var shapeExpr = method.Arguments[1] as MethodExpression
                    ?? throw new InvalidQueryException($"Spatial shape argument must be a method expression (spatial.circle or spatial.wkt), but got: {method.Arguments[1].Type}");
                var shapeType = QueryMethod.GetMethodType(shapeExpr.Name.Value);

                // Build spatial bindings: [0]=distErrPct, then shape-specific args
                List<ParameterBinding> spatialBindings =
                [
                    method.Arguments.Count == 3
                        ? CreateBinding(method.Arguments[2], queryParameters)
                        : null
                ];

                bool isCircle = shapeType == MethodType.Spatial_Circle;
                if (isCircle && shapeExpr.Arguments.Count >= 3)
                {
                    spatialBindings.Add(CreateBinding(shapeExpr.Arguments[0], queryParameters)); // radius
                    spatialBindings.Add(CreateBinding(shapeExpr.Arguments[1], queryParameters)); // lat
                    spatialBindings.Add(CreateBinding(shapeExpr.Arguments[2], queryParameters)); // lng
                    spatialBindings.Add(shapeExpr.Arguments.Count == 4 // units (optional)
                        ? CreateBinding(shapeExpr.Arguments[3], queryParameters) : null);
                }
                else if (shapeType == MethodType.Spatial_Wkt && shapeExpr.Arguments.Count >= 1)
                {
                    spatialBindings.Add(CreateBinding(shapeExpr.Arguments[0], queryParameters)); // wkt
                    spatialBindings.Add(shapeExpr.Arguments.Count == 2 // units (optional)
                        ? CreateBinding(shapeExpr.Arguments[1], queryParameters) : null);
                }

                clauses.Add(new ClauseInfo
                {
                    FieldName = spatialFieldName,
                    ClauseType = ClauseType.Spatial,
                    SpatialMethodType = ToSpatialOp(methodType),
                    OriginalIndex = clauses.Count,
                    Bindings = spatialBindings.ToArray()
                });
                break;
            }

            case MethodType.Vector_Search:
            {
                // Capture bindings for vector sub-arguments.
                // Resolve field name (structural — uses metadata for dynamic index field naming).
                string vectorFieldName = metadata.IsDynamic == false
                    ? QueryBuilderHelper.ExtractIndexFieldName(metadata.Query, queryParameters, method.Arguments[0], metadata)
                    : metadata.GetVectorFieldName(method, queryParameters);

                VectorSourceKind vecMethod = VectorSourceKind.Inline;
                ParameterBinding vectorValueBinding = null;
                ParameterBinding aiTaskBinding = null;
                ParameterBinding minimumMatchBinding = null;
                ParameterBinding numberOfCandidatesBinding = null;

                if (method.Arguments.Count > 2)
                    minimumMatchBinding = CreateBinding(method.Arguments[2], queryParameters);
                if (method.Arguments.Count > 3)
                    numberOfCandidatesBinding = CreateBinding(method.Arguments[3], queryParameters);

                QueryExpression srcVector = method.Arguments[1];
                if (srcVector is MethodExpression methodValue)
                {
                    vecMethod = methodValue.Name.ToString() switch
                    {
                        ClientConstants.VectorSearch.EmbeddingForDocument => VectorSourceKind.FromDocument,
                        ClientConstants.VectorSearch.EmbeddingForRaw => VectorSourceKind.Inline,
                        ClientConstants.VectorSearch.EmbeddingText => VectorSourceKind.FromText,
                        _ => VectorSourceKind.Inline
                    };
                    if (methodValue.Arguments.Count > 0)
                        vectorValueBinding = CreateBinding(methodValue.Arguments[0], queryParameters);
                    if (vecMethod == VectorSourceKind.FromText && methodValue.Arguments.Count > 1
                        && methodValue.Arguments[1] is MethodExpression aiMethod && aiMethod.Arguments.Count > 0)
                        aiTaskBinding = CreateBinding(aiMethod.Arguments[0], queryParameters);
                }
                else
                {
                    vectorValueBinding = CreateBinding(srcVector, queryParameters);
                }

                clauses.Add(new ClauseInfo
                {
                    FieldName = vectorFieldName,
                    ClauseType = ClauseType.Vector,
                    OriginalIndex = clauses.Count,
                    Bindings = [vectorValueBinding, minimumMatchBinding, numberOfCandidatesBinding, aiTaskBinding],
                    VectorMethod = vecMethod
                });
                break;
            }

            case MethodType.MoreLikeThis:
                // MoreLikeThis method in a WHERE clause acts as "all entries" —
                // the actual MLT logic is in the separate reader.MoreLikeThis() path.
                // When it appears in a filter expression, treat as no-op (all entries match).
                break;

            case MethodType.When:
            {
                // when(condition, expr) — create a delegate that evaluates the condition
                // against the query's BlittableJsonReaderObject parameters at execution time.
                // Clauses whose condition evaluates to false are eliminated in BuildAndCompile.
                if (method.Arguments.Count != 2)
                    break;
                var conditionExpr = method.Arguments[0];
                int beforeCount = clauses.Count;
                ParseExpression(method.Arguments[1], indexSearcher, clauses, queryParameters, metadata, ref hasMixedAndOr);
                for (int wi = beforeCount; wi < clauses.Count; wi++)
                    clauses[wi].WhenCondition = (queryParams) =>
                        QueryBuilderHelper.EvaluateConstantExpressionForWhenQuery(conditionExpr, metadata.Query, metadata, queryParams);
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
            throw new InvalidQueryException($"search() requires at least 2 arguments (field, term), but got {method.Arguments.Count}.");

        if (TryGetFieldName(method.Arguments[0], metadata, queryParameters, out var fieldName) == false)
            throw new InvalidQueryException($"search() first argument must be a field name, but got: {method.Arguments[0].Type} ({method.Arguments[0]}).");

        var searchOp = Constants.Search.Operator.Or;
        if (method.Arguments.Count >= 3 && method.Arguments[2] is FieldExpression opField
            && opField.Compound.Count == 1)
        {
            var op = opField.Compound[0].Value;
            if (string.Equals("AND", op, StringComparison.OrdinalIgnoreCase))
                searchOp = Constants.Search.Operator.And;
        }

        clauses.Add(new ClauseInfo
        {
            FieldName = fieldName,
            ClauseType = ClauseType.Search,
            SearchOperator = (int)searchOp,
            OriginalIndex = clauses.Count,
            Bindings = [CreateBinding(method.Arguments[1], queryParameters)]
        });
    }

    private static void ParsePrefixMethod(MethodExpression method, List<ClauseInfo> clauses,
        BlittableJsonReaderObject queryParameters, QueryMetadata metadata, ClauseType type)
    {
        if (method.Arguments.Count < 2)
            throw new InvalidQueryException($"{type}() requires at least 2 arguments (field, term), but got {method.Arguments.Count}.");

        if (TryGetFieldName(method.Arguments[0], metadata, queryParameters, out var fieldName) == false)
            throw new InvalidQueryException($"{type}() first argument must be a field name, but got: {method.Arguments[0].Type} ({method.Arguments[0]}).");

        clauses.Add(new ClauseInfo
        {
            FieldName = fieldName,
            ClauseType = type,
            OriginalIndex = clauses.Count,
            Bindings = [CreateBinding(method.Arguments[1], queryParameters)]
        });
    }

    // ── Value extraction helpers ─────────────────────────────────────────

    /// <summary>Extract the field name with proper alias resolution using query metadata.</summary>
    private static string GetFieldName(FieldExpression field, QueryMetadata metadata, BlittableJsonReaderObject queryParameters)
    {
        if (metadata != null)
            return metadata.GetIndexFieldName(field, queryParameters).Value;
        return field.FieldValue;
    }

    /// <summary>Try to extract a field name from a query expression that may be a
    /// <see cref="FieldExpression"/> (normal field), a <see cref="ValueExpression"/>
    /// (quoted field name like <c>'Order'</c>), or a <see cref="MethodExpression"/>
    /// for the <c>id()</c> function. Returns false if the expression is none of these.</summary>
    private static bool TryGetFieldName(QueryExpression expr, QueryMetadata metadata,
        BlittableJsonReaderObject queryParameters, out string fieldName)
    {
        if (expr is FieldExpression fe)
        {
            fieldName = GetFieldName(fe, metadata, queryParameters);
            return true;
        }

        // Quoted field names (e.g. 'Order' for reserved words) are parsed as ValueExpression
        if (expr is ValueExpression ve)
        {
            var resolved = ve.GetValue(queryParameters)?.ToString();
            if (resolved != null)
            {
                fieldName = metadata != null
                    ? metadata.GetIndexFieldName(new QueryFieldName(resolved, ve.Value == ValueTokenType.String), queryParameters).Value
                    : resolved;
                return true;
            }
        }

        if (expr is MethodExpression me && string.Equals(me.Name.Value, "id", StringComparison.OrdinalIgnoreCase))
        {
            fieldName = Client.Constants.Documents.Indexing.Fields.DocumentIdFieldName;
            return true;
        }

        fieldName = null;
        return false;
    }

    /// <summary>Detect the native type of resolved SCALAR parameter value.
    /// Must not be called with arrays or blittable objects — callers must check
    /// and handle those before calling this method.</summary>
    private static (object Value, ValueTokenType Type) ResolveParameterValue(object value)
    {
        Debug.Assert(value is not BlittableJsonReaderArray and not BlittableJsonReaderObject,
            $"ResolveParameterValue called with non-scalar type {value?.GetType().Name}. " +
            "Caller must handle arrays/objects before calling this method.");

        switch (value)
        {
            case long l:
                return (l, ValueTokenType.Long);
            case int i:
                return ((long)i, ValueTokenType.Long);
            case double d:
                return (d, ValueTokenType.Double);
            case float f:
                return ((double)f, ValueTokenType.Double);
            case decimal dec:
                return ((double)dec, ValueTokenType.Double);
            case LazyNumberValue lnv when lnv.TryParseLong(out long lnvLong):
                return (lnvLong, ValueTokenType.Long);
            case LazyNumberValue lnv:
                return ((double)lnv, ValueTokenType.Double);
            default:
            {
                var str = value?.ToString();
                if (str is { Length: > 18 and < 35 } && str.Contains('T')
                    && DateTime.TryParse(str, System.Globalization.CultureInfo.InvariantCulture,
                        System.Globalization.DateTimeStyles.RoundtripKind, out var parsed))
                {
                    return (parsed.Ticks, ValueTokenType.Long);
                }
                return (str, ValueTokenType.String);
            }
        }
    }

    /// <summary>Build a ParameterBinding from a query expression. For parameters, captures only the
    /// name (value resolved later by PopulateParameters). For literals, resolves and caches the
    /// constant value since it never changes. For method expressions (cmpxchg, now, today),
    /// stores the expression for deferred resolution at execution time.</summary>
    private static ParameterBinding CreateBinding(QueryExpression expr, BlittableJsonReaderObject queryParameters)
    {
        if (expr is MethodExpression me)
        {
            // Method expressions like cmpxchg(), now(), today() must be resolved at execution
            // time (not template creation time) because their values can change between executions.
            // Create a closure delegate that captures the AST node and evaluates it with
            // the QueryBuilderParameters provided at invocation time (boxed as object).
            return new ParameterBinding
            {
                DeferredExpression = (builderParamsObj, qp) =>
                {
                    var bp = (QueryBuilderParameters)builderParamsObj;
                    var resolved = QueryBuilderHelper.EvaluateMethod(
                        bp.Query.Metadata.Query,
                        bp.Metadata,
                        bp.ServerContext,
                        bp.DocumentsContext.DocumentDatabase.CompareExchangeStorage,
                        me,
                        qp,
                        bp.QueryTime);
                    if (resolved is ValueExpression ve)
                    {
                        if (ve.Value == ValueTokenType.Null)
                            return null;
                        var value = ve.GetValue(qp);
                        if (value == null)
                            return null;
                        return value;
                    }
                    return null;
                },
                LiteralType = ParamValueType.String
            };
        }

        if (expr is not ValueExpression ve)
            return null;

        if (ve.Value == ValueTokenType.Parameter)
            return new ParameterBinding { ParameterName = ve.Token.Value, LiteralType = ParamValueType.Parameter };

        var value = ve.GetValue(queryParameters);

        if (ve.Value == ValueTokenType.Null || value is null)
            return new ParameterBinding { LiteralValue = null, LiteralType = ParamValueType.String };
        if (value is bool b)
            return new ParameterBinding { LiteralValue = b ? "true" : "false", LiteralType = ParamValueType.String };

        var (resolved, resolvedType) = ResolveParameterValue(value);
        return new ParameterBinding { LiteralValue = resolved, LiteralType = ToParamValueType(resolvedType) };
    }

    /// <summary>Format a value from the plan's typed arrays as a string for display/highlighting.</summary>
    internal static string FormatValueFromPlan(PackedParam packed, QueryExecution plan) => FormatValueFromPlanInternal(packed, plan, packed.Param1);

    /// <summary>Format the second value (BETWEEN high bound) from the plan's typed arrays.</summary>
    internal static string FormatValue2FromPlan(PackedParam packed, QueryExecution plan) => FormatValueFromPlanInternal(packed, plan, packed.Param2);
    
    private static string FormatValueFromPlanInternal(PackedParam packed, QueryExecution plan, int idx)
    {
        if (idx is PackedParam.NoParamValue) return null;
        // An IN clause with all-null terms records InTermCount=0 and writes no values
        // to the typed arrays, but the packed Param1 still points at the (empty) slot.
        // Bounds-check before indexing — return null to indicate "no displayable value".
        return packed.ValueType switch
        {
            PackedParam.TypeLong => idx < (plan.LongValues?.Length ?? 0) ? plan.LongValues[idx].ToString() : null,
            PackedParam.TypeDouble => idx < (plan.DoubleValues?.Length ?? 0) ? plan.DoubleValues[idx].ToString(System.Globalization.CultureInfo.InvariantCulture) : null,
            _ => idx < (plan.StringValues?.Length ?? 0) ? plan.StringValues[idx] : null
        };
    }

    /// <summary>Resolve an IN value to its native type, handling booleans and dates.</summary>
    private static (object Value, ValueTokenType Type) ResolveInValue(object value, ValueTokenType literalType)
    {
        if (value == null)
            return (null, ValueTokenType.String);
        if (value is bool b)
            return (b ? "true" : "false", ValueTokenType.String);
        if (value is DateTime dt)
            return (dt.Ticks, ValueTokenType.Long);
        if (value is DateTimeOffset dto)
            return (dto.UtcDateTime.Ticks, ValueTokenType.Long);
        if (literalType != ValueTokenType.Parameter)
            return (value, literalType);
        return ResolveParameterValue(value);
    }

    // ── Cardinality estimation ───────────────────────────────────────────

    private static long EstimateCardinality(ClauseInfo clause, ClauseExecution exec, IndexSearcher indexSearcher, ValueWriter writer)
    {
        switch (clause.ClauseType)
        {
            case ClauseType.Equals:
            {
                var fieldMeta = indexSearcher.FieldMetadataBuilder(clause.FieldName);
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
                // Use field-level cardinality as upper bound
                return indexSearcher.GetTermAmountInField(
                    indexSearcher.FieldMetadataBuilder(clause.FieldName));

            case ClauseType.In:
            case ClauseType.AllIn:
                // Sum of individual term cardinalities
                long sum = 0;
                var meta = indexSearcher.FieldMetadataBuilder(clause.FieldName);
                var ip = exec.PackedParamValue;
                if (!ip.IsNone)
                {
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
                }

                return Math.Min(sum, indexSearcher.NumberOfEntries);

            case ClauseType.Spatial:
            case ClauseType.Vector:
                return indexSearcher.NumberOfEntries;

            case ClauseType.OrGroup:
                long orSum = 0;
                if (clause.OrSubClauses != null && exec.OrSubExecutions != null)
                {
                    for (int si = 0; si < clause.OrSubClauses.Count; si++)
                    {
                        var subExec = exec.OrSubExecutions[si];
                        if (subExec.Cardinality < 0)
                            subExec.Cardinality = EstimateCardinality(clause.OrSubClauses[si], subExec, indexSearcher, writer);
                        orSum += subExec.Cardinality;
                    }
                }
                return Math.Min(orSum, indexSearcher.NumberOfEntries);

            case ClauseType.AndGroup:
                long andMin = indexSearcher.NumberOfEntries;
                if (clause.AndSubClauses != null && exec.AndSubExecutions != null)
                {
                    for (int si = 0; si < clause.AndSubClauses.Count; si++)
                    {
                        var subExec = exec.AndSubExecutions[si];
                        if (subExec.Cardinality < 0)
                            subExec.Cardinality = EstimateCardinality(clause.AndSubClauses[si], subExec, indexSearcher, writer);
                        if (subExec.Cardinality < andMin)
                            andMin = subExec.Cardinality;
                    }
                }
                return andMin;

            default:
                return indexSearcher.NumberOfEntries;
        }
    }

    // ── Plan emission: clause list → PlanOp[] ────────────────────────────

    /// <summary>Translate sorted clauses into a linear PlanOp[] sequence for IL emission.
    ///
    /// Bitmap slots:
    ///   Slot 0 = main result bitmap (accumulates the final answer).
    ///   Slot 1 = scratch bitmap (used for AND-chain non-seed IN terms and OR-group accumulation).
    ///   Slot 2 = save slot (only allocated when an OR chain contains multiple AND-groups;
    ///            used to save the prior OR accumulation while building a new AND sub-chain
    ///            in slot 0 via SwapBitmaps(0,2), then ORed back).
    ///
    /// AND chain: the first clause seeds slot 0 (FillFromPostings), subsequent clauses narrow it
    /// (AndWithPostings/AndNotWithPostings). IN terms are ORed into slot 1, then ANDed with slot 0.
    ///
    /// OR chain: all terms are ORed into slot 0. AND-groups within an OR use the three-bitmap
    /// swap pattern: save slot 0 → slot 2, build AND result in slot 0, OR slot 2 back.</summary>
    private static QueryExecution EmitPlan(List<ClauseInfo> clauses, ClauseExecution[] executions, bool isOr)
    {
        if (isOr is false)
        {
            // Empty IN clauses: zero results in AND, no-op in OR.
            for (int i = 0; i < clauses.Count; i++)
            {
                if (clauses[i].ClauseType is ClauseType.In or ClauseType.AllIn &&
                    executions[i] is { InTermCount: 0, HasNullTerm: false })
                {
                    return new QueryExecution { Ops = [], IsAllEntries = false, Executions = executions };
                }
            }
        }
        else
        {
            int write = 0;
            for (int i = 0; i < clauses.Count; i++)
            {
                if (clauses[i].ClauseType is ClauseType.In or ClauseType.AllIn &&
                    executions[i] is { InTermCount: 0, HasNullTerm: false })
                    continue;
                clauses[write] = clauses[i];
                executions[write] = executions[i];
                write++;
            }
            if (write < clauses.Count)
            {
                clauses.RemoveRange(write, clauses.Count - write);
                executions = executions[..write];
            }
        }

        List<PlanOp> ops = [];
        List<int> rangeCounts = [];
        bool needsThreeBitmaps = false;

        if (isOr)
        {
            // OR chain — expand In/OrGroup terms into individual OR ops
            int matchIndex = 0;
            for (int ci = 0; ci < clauses.Count; ci++)
            {
                var it = clauses[ci];
                var itExec = executions[ci];

                // Negated clause in an OR chain: emit a single QueryMatch slot whose match
                // is materialized at resolution time as AllEntries ANDNOT(positive form).
                // Covers NotEquals, NOT IN, NOT AllIn, NOT exists(), NOT startsWith(), etc.
                // The raw posting list / range / tree-scan can't deliver the complement,
                // so dispatch is forced to QueryMatch and CreateNotEqualsOrMatch produces
                // a pre-materialized BitmapMatch.
                if (it.IsNegated || it.ClauseType == ClauseType.NotEquals)
                {
                    if (it.IsOrChainNotEquals == false)
                    {
                        // ClauseInfo is shared with the cached plan template; clone before
                        // mutating so the template stays untouched across executions.
                        var cloned = it.Clone();
                        cloned.IsOrChainNotEquals = true;
                        clauses[ci] = cloned;
                        it = cloned;
                    }
                    ops.Add(new PlanOp
                    {
                        Kind = matchIndex == 0 ? PlanOpKind.FillFromPostings : PlanOpKind.OrWithPostings,
                        ParamIndex = matchIndex,
                        EstimatedCardinality = itExec.Cardinality,
                        Dispatch = MatchDispatch.QueryMatch
                    });
                    matchIndex++;
                    continue;
                }

                switch (it.ClauseType)
                {
                    case ClauseType.In or ClauseType.AllIn when (itExec.InTermCount > 0 || itExec.HasNullTerm):
                        EmitInOps(ops, itExec, bitmapLocal: 0, isSeed: matchIndex == 0, ref matchIndex, rangeCounts);
                        break;
                    case ClauseType.OrGroup when it.OrSubClauses is {Count: > 0}:
                    {
                        for (int si = 0; si < it.OrSubClauses.Count; si++)
                        {
                            var sub = it.OrSubClauses[si];
                            var subExec = itExec.OrSubExecutions[si];
                            ops.Add(new PlanOp
                            {
                                Kind = matchIndex == 0 ? PlanOpKind.FillFromPostings : PlanOpKind.OrWithPostings,
                                ParamIndex = matchIndex,
                                EstimatedCardinality = itExec.Cardinality / it.OrSubClauses.Count,
                                Dispatch = GetDispatch(sub, subExec)
                            });
                            matchIndex++;
                        }

                        break;
                    }
                    case ClauseType.AndGroup when it.AndSubClauses is { Count: > 0}:
                    {
                        // AND sub-expression inside an OR chain.
                        // Only supported when the AND group is the first element (matchIndex == 0)
                        // or can be merged into slot 0 via OrBitmaps after computing into slot 1.
                        var subClauses = it.AndSubClauses;
                        var subExecs = itExec.AndSubExecutions;
                        if (matchIndex == 0)
                        {
                            // First element: build the AND chain directly in slot 0.
                            // Slot 1 is free (unused), so AndWithPostings can use it as scratch.
                            // Suppress early-exit on AND steps — the OR chain continues regardless.
                            ops.Add(new PlanOp
                            {
                                Kind = PlanOpKind.FillFromPostings,
                                ParamIndex = matchIndex,
                                EstimatedCardinality = subExecs[0].Cardinality,
                                Dispatch = GetDispatch(subClauses[0], subExecs[0])
                            });
                            for (int s = 1; s < subClauses.Count; s++)
                            {
                                ops.Add(new PlanOp
                                {
                                    Kind = subClauses[s].IsNegated ? PlanOpKind.AndNotWithPostings : PlanOpKind.AndWithPostings,
                                    ParamIndex = matchIndex + s,
                                    EstimatedCardinality = subExecs[s].Cardinality,
                                    Dispatch = GetDispatch(subClauses[s], subExecs[s]),
                                    SkipEarlyExit = true // don't abort on empty — remaining OR terms may still match
                                });
                            }
                        }
                        else
                        {
                            // Non-first AND group: save the accumulated OR result (slot 0) to slot 2,
                            // build this AND sub-chain fresh in slot 0, then OR slot 2 back.
                            needsThreeBitmaps = true;
                            // Uses SwapBitmaps(0, 2): slot 0 ↔ slot 2.
                            // Slot 2 must have been cleared before the swap (it's either the initial
                            // empty state or was cleared at the end of the previous iteration).
                            ops.Add(new PlanOp { Kind = PlanOpKind.ClearBitmap, BitmapLocal = 1 });
                            ops.Add(new PlanOp
                            {
                                Kind = PlanOpKind.SwapBitmaps,
                                BitmapLocal = 0,
                                ParamIndex2 = 2
                            });
                            // Slot 0 is now fresh (was slot 2 = cleared); slot 2 = prior OR accumulation.
                            ops.Add(new PlanOp
                            {
                                Kind = PlanOpKind.FillFromPostings,
                                ParamIndex = matchIndex,
                                EstimatedCardinality = subExecs[0].Cardinality,
                                Dispatch = GetDispatch(subClauses[0], subExecs[0])
                            });
                            for (int s = 1; s < subClauses.Count; s++)
                            {
                                ops.Add(new PlanOp
                                {
                                    Kind = subClauses[s].IsNegated ? PlanOpKind.AndNotWithPostings : PlanOpKind.AndWithPostings,
                                    ParamIndex = matchIndex + s,
                                    BitmapLocal = 0,
                                    EstimatedCardinality = subExecs[s].Cardinality,
                                    Dispatch = GetDispatch(subClauses[s], subExecs[s]),
                                    SkipEarlyExit = true // don't abort — OR chain continues
                                });
                            }
                            ops.Add(new PlanOp
                            {
                                Kind = PlanOpKind.OrBitmaps,
                                BitmapLocal = 0,
                                ParamIndex2 = 2
                            });
                            ops.Add(new PlanOp { Kind = PlanOpKind.ClearBitmap, BitmapLocal = 2 });
                        }
                        matchIndex += subClauses.Count;
                        break;
                    }
                    default:
                    {
                        ops.Add(new PlanOp
                        {
                            Kind = matchIndex == 0 ? PlanOpKind.FillFromPostings : PlanOpKind.OrWithPostings,
                            ParamIndex = matchIndex,
                            EstimatedCardinality = itExec.Cardinality,
                            Dispatch = GetDispatch(it, itExec)
                        });
                        matchIndex++;
                        break;
                    }
                }
            }
            ops.Add(new PlanOp { Kind = PlanOpKind.IterateInto });
        }
        else switch (clauses.Count)
        {
            case 1 when clauses[0].ClauseType == ClauseType.Equals && clauses[0].IsNegated is false:
                ops.Add(new PlanOp
                {
                    Kind = PlanOpKind.DirectIterate,
                    ParamIndex = 0,
                    EstimatedCardinality = executions[0].Cardinality,
                    Dispatch = GetDispatch(clauses[0], executions[0])
                });
                break;
            case 1 when clauses[0].ClauseType == ClauseType.NotEquals
                        || (clauses[0].ClauseType == ClauseType.Equals && clauses[0].IsNegated):
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
                    EstimatedCardinality = executions[0].Cardinality,
                    Dispatch = GetDispatch(clauses[0], executions[0])
                });
                ops.Add(new PlanOp { Kind = PlanOpKind.IterateInto });

                // Mark clause so ResolveMatches produces [AllEntries, TermMatch].
                // The ClauseInfo here is shared with the template; clone before mutating
                // so the cached template stays untouched across executions.
                if (clauses[0].IsNegated == false)
                {
                    var cloned = clauses[0].Clone();
                    cloned.IsNegated = true;
                    clauses[0] = cloned;
                }

                return new QueryExecution
                {
                    Ops = ops.ToArray(),
                    OperandOrdering = 0,
                    Clauses = clauses,
                    Executions = executions
                };
            default:
            {
                // AND chain: Fill the smallest non-negated, then AndWith/AndNotWith remaining.
                // If the first clause is negated (all clauses are negated), we need to
                // start from AllEntries and ANDNOT each one.
                bool firstIsNegated = clauses[0].IsNegated || clauses[0].ClauseType == ClauseType.NotEquals;
                int startIndex;

                if (firstIsNegated)
                {
                    // All clauses are negated — start from all entries.
                    // AllEntries match is appended AFTER all clause-expanded matches by ResolveMatches.
                    ops.Add(new PlanOp
                    {
                        Kind = PlanOpKind.FillFromPostings,
                        ParamIndex = CountMatchSlots(clauses, executions, isAllEntries: false, allNegated: false), // Index of AllEntries in resolved matches
                        EstimatedCardinality = long.MaxValue
                    });
                    startIndex = 0; // Process all clauses as ANDNOT
                }
                else
                {
                    startIndex = 1;
                }

                int matchIndex = 0;
                if (!firstIsNegated)
                {
                    switch (clauses[0].ClauseType)
                    {
                        case ClauseType.OrGroup when clauses[0].OrSubClauses != null:
                        {
                            var subClauses = clauses[0].OrSubClauses;
                            var subExecs = executions[0].OrSubExecutions;
                            for (int s = 0; s < subClauses.Count; s++)
                            {
                                ops.Add(new PlanOp
                                {
                                    Kind = s == 0 ? PlanOpKind.FillFromPostings : PlanOpKind.OrWithPostings,
                                    ParamIndex = matchIndex + s,
                                    BitmapLocal = 0,
                                    EstimatedCardinality = subExecs[s].Cardinality,
                                    Dispatch = GetDispatch(subClauses[s], subExecs[s])
                                });
                            }
                            matchIndex += subClauses.Count;
                            break;
                        }
                        case ClauseType.In when executions[0].InTermCount > 0 || executions[0].HasNullTerm:
                            EmitInOps(ops, executions[0], bitmapLocal: 0, isSeed: true, ref matchIndex, rangeCounts);
                            break;
                        // Use the fixed-shape EmitAllInOps path whenever the clause has any term
                        // (typed OR null). Routing the InTermCount=0/HasNullTerm=true case through
                        // the default branch instead would emit a single QueryMatch-dispatched op,
                        // which has a different cache-key-equivalent shape than the 2-op
                        // (Fill+AndRange, PostingList) shape EmitAllInOps emits — and the cache key
                        // uses only queryText/ordering/sig, so a subsequent execution with a typed
                        // term would receive this 1-op IL and skip the AND-range entirely.
                        case ClauseType.AllIn when executions[0].InTermCount > 0 || executions[0].HasNullTerm:
                            EmitAllInOps(ops, executions[0], ref matchIndex, rangeCounts);
                            break;
                        default:
                            ops.Add(new PlanOp
                            {
                                Kind = PlanOpKind.FillFromPostings,
                                ParamIndex = 0,
                                EstimatedCardinality = executions[0].Cardinality,
                                Dispatch = GetDispatch(clauses[0], executions[0])
                            });
                            matchIndex = 1;
                            break;
                    }
                }
                // Precheck: can all remaining clauses be converted to entry scan predicates?
                bool allScanEligible = AreAllScanEligible(clauses, executions, startIndex);

                for (int i = startIndex; i < clauses.Count; i++)
                {
                    var iExec = executions[i];
                    // Goto check before each AND step — only if all remaining clauses
                    // can be handled by entry scan predicates
                    if (allScanEligible)
                    {
                        ops.Add(new PlanOp
                        {
                            Kind = PlanOpKind.CheckAndMaybeEntryScan,
                            ParamIndex = matchIndex
                        });
                    }

                    switch (clauses[i].ClauseType)
                    {
                        case ClauseType.OrGroup when clauses[i].OrSubClauses != null:
                        {
                            // OrGroup: OR subclauses into bitmap[1], then AND with bitmap[0]
                            var subClauses = clauses[i].OrSubClauses;
                            var subExecs = iExec.OrSubExecutions;

                            // Clear bitmap[1] (OR accumulator)
                            ops.Add(new PlanOp { Kind = PlanOpKind.ClearBitmap, BitmapLocal = 1 });

                            // Fill each subclause into bitmap[1]
                            for (int s = 0; s < subClauses.Count; s++)
                            {
                                ops.Add(new PlanOp
                                {
                                    Kind = PlanOpKind.OrWithPostings,
                                    ParamIndex = matchIndex + s,
                                    BitmapLocal = 1, // target bitmap[1]
                                    EstimatedCardinality = subExecs[s].Cardinality,
                                    Dispatch = GetDispatch(subClauses[s], subExecs[s])
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
                            break;
                        }
                        case ClauseType.In when (iExec.InTermCount > 0 || iExec.HasNullTerm):
                        {
                            // OR all IN terms into bitmap[1], then AND (or ANDNOT) with bitmap[0].
                            // isSeed: false — FillFromPostings always targets bitmap[0], so we use
                            // OrRange which respects bitmapLocal. Bitmap[1] is freshly cleared.
                            ops.Add(new PlanOp { Kind = PlanOpKind.ClearBitmap, BitmapLocal = 1 });
                            EmitInOps(ops, iExec, bitmapLocal: 1, isSeed: false, ref matchIndex, rangeCounts);
                            ops.Add(new PlanOp
                            {
                                Kind = clauses[i].IsNegated ? PlanOpKind.AndNotBitmaps : PlanOpKind.AndBitmaps,
                                BitmapLocal = 0,
                                ParamIndex2 = 1
                            });
                            if (!clauses[i].IsNegated)
                                ops.Add(new PlanOp { Kind = PlanOpKind.CheckEmpty, BitmapLocal = 0 });
                            break;
                        }
                        case ClauseType.AllIn when iExec.InTermCount > 0:
                        {
                            int rangeIdx = rangeCounts.Count;
                            rangeCounts.Add(iExec.InTermCount);
                            ops.Add(new PlanOp
                            {
                                Kind = PlanOpKind.AndRange,
                                ParamIndex = matchIndex,
                                ParamIndex2 = rangeIdx,
                                BitmapLocal = 0,
                                EstimatedCardinality = iExec.Cardinality,
                                Dispatch = MatchDispatch.PostingList
                            });
                            matchIndex += iExec.InTermCount;
                            break;
                        }
                        default:
                        {
                            // Simple clause: AND or ANDNOT with bitmap[0]
                            var isNegated = clauses[i].IsNegated || clauses[i].ClauseType == ClauseType.NotEquals;
                            var andKind = isNegated ? PlanOpKind.AndNotWithPostings : PlanOpKind.AndWithPostings;
                            ops.Add(new PlanOp
                            {
                                Kind = andKind,
                                ParamIndex = matchIndex,
                                BitmapLocal = 0,
                                EstimatedCardinality = iExec.Cardinality,
                                Dispatch = GetDispatch(clauses[i], iExec)
                            });

                            if (!isNegated)
                                ops.Add(new PlanOp { Kind = PlanOpKind.CheckEmpty, BitmapLocal = 0 });

                            matchIndex++;
                            break;
                        }
                    }
                }

                ops.Add(new PlanOp { Kind = PlanOpKind.IterateInto });
                break;
            }
        }

        // Pack operand ordering — encodes clause sort order after cardinality reordering.
        // The plan structure must be the same for all parameter values of the same query text.
        // Variable-count IN terms use InRangeCounts (read at runtime by the IL).
        // Null-term slots are always allocated — the match is a no-op when null isn't present.
        int ordering = 0;
        for (int i = 0; i < Math.Min(clauses.Count, 10); i++)
            ordering |= (clauses[i].OriginalIndex & 0x7) << (i * 3);

        // Check if all clauses are negated (if first clause after sort is negated, all the rest are too)
        bool allNegated = clauses.Count > 0
            && (clauses[0].IsNegated || clauses[0].ClauseType == ClauseType.NotEquals);

        // Build scan predicate infos for entry scan — only for AND chains with simple clauses
        ScanPredicateInfo[] scanPredicateInfos = null;
        if (isOr is false && clauses.Count > 1)
        {
            var scanPreds = new List<ScanPredicateInfo>();
            int longIndex = 0, doubleIndex = 0, sliceIndex = 0;

            // Start from 0 when all clauses are negated (allNegated=true): all are ANDNOT
            // operands and AllEntries is the implicit seed, so every clause needs a predicate.
            // Start from 1 in the normal case: clause 0 is the fill seed.
            int scanStart = allNegated ? 0 : 1;
            for (int i = scanStart; i < clauses.Count; i++)
            {
                var pred = BuildScanPredicateInfo(clauses[i], executions[i], ref longIndex, ref doubleIndex, ref sliceIndex);
                if (pred != null)
                    scanPreds.Add(pred.Value);
            }

            if (scanPreds.Count > 0)
                scanPredicateInfos = scanPreds.ToArray();
        }

        // Compute type signature from scan predicates. The int packs the first 16 kinds
        // (2 bits each). For ≤ 16 predicates this is the exact cache identity. For more,
        // it's a lossy hash, and we attach FullKinds for disambiguation in PlanCache.
        (int typeSignature, byte[] fullKinds) = GetTypeSignature(scanPredicateInfos);

        return new QueryExecution
        {
            Ops = ops.ToArray(),
            OperandOrdering = ordering,
            Clauses = clauses,
            Executions = executions,
            AllNegated = allNegated,
            ScanPredicateInfos = scanPredicateInfos,
            TypeSignature = typeSignature,
            FullKinds = fullKinds,
            RequiredBitmaps = needsThreeBitmaps ? 3 : 2,
            InRangeCounts = rangeCounts.Count > 0 ? rangeCounts.ToArray() : null
        };
    }

    private static (int TypeSignature, byte[] FullKinds) GetTypeSignature(ScanPredicateInfo[] scanPredicateInfos)
    {
        if (scanPredicateInfos == null) 
            return (0, null);
        
        int typeSignature = 0;
        
        int n = scanPredicateInfos.Length;
        int packCount = Math.Min(n, 16);
        for (int i = 0; i < packCount; i++)
        {
            typeSignature |= ((int)scanPredicateInfos[i].ValueType & 0x3) << (i * 2);
        }

        if (n <= 16) return (typeSignature, null);
        
        var fullKinds = new byte[n];
        for (int i = 0; i < n; i++)
        {
            fullKinds[i] = (byte)scanPredicateInfos[i].ValueType;
        }

        return (typeSignature, fullKinds);
    }

    private static bool AreAllScanEligible(List<ClauseInfo> clauses, ClauseExecution[] executions, int startIndex)
    {
        // If any clause (In, AllIn, Spatial, Vector, Search, etc.) can't be scanned, we must not emit CheckAndMaybeEntryScan — entry scan would skip them entirely.
        int dummyL = 0, dummyD = 0, dummyS = 0;
        for (int j = startIndex; j < clauses.Count; j++)
        {
            if (BuildScanPredicateInfo(clauses[j], executions[j], ref dummyL, ref dummyD, ref dummyS) != null) 
                continue;
            return false;
        }
        return true;
    }

    // ── Plan helpers ─────────────────────────────────────────────────────

    /// <summary>Emit ops for an IN clause: Fill the first term + OrRange for the rest, plus null-term if needed.</summary>
    /// <summary>Emit ops for an IN clause: Fill slot 0 + OrRange for the rest.
    /// Fixed 2-op shape regardless of term count or presence of null. Slot 0 holds
    /// the null-term posting list when HasNullTerm, else the first typed term, else
    /// an empty PostingSource. Slots 1..N-1 hold remaining typed terms, dispatched
    /// via OrRange whose count comes from <c>ctx.InRangeCounts[rangeIdx]</c> at
    /// runtime. Keeping the op shape parameter-independent is what allows the plan
    /// cache to share one compiled delegate across executions with different
    /// InTermCount / HasNullTerm values for the same query text.</summary>
    private static void EmitInOps(List<PlanOp> ops, ClauseExecution exec, int bitmapLocal, bool isSeed, ref int matchIndex, List<int> rangeCounts)
    {
        // Always (inTermCount + 1) slots — matches CountMatchSlots and ResolveMatches.
        // Last slot is the null-term slot (empty match when !HasNullTerm).
        int totalSlots = exec.InTermCount + 1;
        // Range iterates over the slots AFTER slot 0 (which Fill handles). When the parameter
        // list has no null, the trailing null slot is Empty — ORing with Empty is a no-op, so
        // we can safely include it (rangeCount = totalSlots - 1). When the list HAS a null
        // term, that slot is non-empty and we want to OR it in. Both cases use the same range.
        int rangeIdx = rangeCounts.Count;
        rangeCounts.Add(totalSlots - 1);

        ops.Add(new PlanOp
        {
            Kind = isSeed ? PlanOpKind.FillFromPostings : PlanOpKind.OrWithPostings,
            ParamIndex = matchIndex,
            BitmapLocal = bitmapLocal,
            EstimatedCardinality = Math.Max(1, exec.Cardinality / totalSlots),
            Dispatch = MatchDispatch.PostingList
        });
        ops.Add(new PlanOp
        {
            Kind = PlanOpKind.OrRange,
            ParamIndex = matchIndex + 1,
            ParamIndex2 = rangeIdx,
            BitmapLocal = bitmapLocal,
            EstimatedCardinality = exec.Cardinality,
            Dispatch = MatchDispatch.PostingList
        });
        matchIndex += totalSlots;
    }

    /// <summary>Emit ops for an AllIn clause (as a seed): Fill slot 0 + AndRange for the rest.
    /// Same fixed shape rationale as <see cref="EmitInOps"/> — the count of remaining
    /// terms lives in <c>ctx.InRangeCounts</c> rather than the op shape itself.</summary>
    private static void EmitAllInOps(List<PlanOp> ops, ClauseExecution exec, ref int matchIndex, List<int> rangeCounts)
    {
        // Always (inTermCount + 1) slots — matches CountMatchSlots and ResolveMatches.
        int totalSlots = exec.InTermCount + 1;
        // For AllIn, ANDing with an Empty PostingSource clears the bitmap — so we MUST
        // exclude the trailing null slot from the range when HasNullTerm is false.
        // Range walks slots [1..) over the typed terms (and the null term if present).
        int rangeCount = exec.InTermCount - 1 + (exec.HasNullTerm ? 1 : 0);
        if (rangeCount < 0) rangeCount = 0; // (HasNullTerm=true, InTermCount=0): no AND step
        int rangeIdx = rangeCounts.Count;
        rangeCounts.Add(rangeCount);

        ops.Add(new PlanOp
        {
            Kind = PlanOpKind.FillFromPostings,
            ParamIndex = matchIndex,
            BitmapLocal = 0,
            EstimatedCardinality = Math.Max(1, exec.Cardinality / totalSlots),
            Dispatch = MatchDispatch.PostingList
        });
        ops.Add(new PlanOp
        {
            Kind = PlanOpKind.AndRange,
            ParamIndex = matchIndex + 1,
            ParamIndex2 = rangeIdx,
            BitmapLocal = 0,
            EstimatedCardinality = exec.Cardinality,
            Dispatch = MatchDispatch.PostingList
        });
        matchIndex += totalSlots;
    }

    private static QueryExecution BuildAllEntriesPlan()
    {
        // No bitmap needed — AllEntries already implements IQueryMatch.Fill(),
        // so we iterate it directly without materializing into a bitmap first.
        return new QueryExecution
        {
            Ops = [new PlanOp { Kind = PlanOpKind.DirectIterate, ParamIndex = 0 }],
            IsAllEntries = true
        };
    }

    /// <summary>Attach spatial and vector post-filter phases to a query plan.
    /// Spatial/vector clauses are stored in the plan's Clauses array at known indices,
    /// and SpatialFilters/VectorSelects reference those indices for resolution at execution time.</summary>
    private static void AttachPostFilterPhases(QueryExecution plan, List<ClauseInfo> spatialClauses, ClauseExecution[] spatialExecs,
        List<ClauseInfo> vectorClauses, ClauseExecution[] vectorExecs)
    {
        if (spatialClauses == null && vectorClauses == null)
            return;

        var clauses = plan.Clauses ??= [];

        // Extend Executions array to match the clauses that will be added
        int extraCount = (spatialClauses?.Count ?? 0) + (vectorClauses?.Count ?? 0);
        var execs = plan.Executions ??= [];
        int execIdx = execs.Length;
        Array.Resize(ref execs, execs.Length + extraCount);

        int matchIndex = CountMatchSlots(clauses, execs, plan.IsAllEntries, plan.AllNegated);

        if (spatialClauses != null)
        {
            plan.SpatialFilters = new SpatialFilterOp[spatialClauses.Count];
            for (int i = 0; i < spatialClauses.Count; i++)
            {
                clauses.Add(spatialClauses[i]);
                var exec = spatialExecs?[i] ?? new ClauseExecution();
                execs[execIdx++] = exec;
                plan.SpatialFilters[i] = new SpatialFilterOp { MatchIndex = matchIndex++, Clause = spatialClauses[i], Exec = exec };
            }
        }

        if (vectorClauses != null)
        {
            plan.VectorSelects = new VectorSearchOp[vectorClauses.Count];
            for (int i = 0; i < vectorClauses.Count; i++)
            {
                clauses.Add(vectorClauses[i]);
                var exec = vectorExecs?[i] ?? new ClauseExecution();
                execs[execIdx++] = exec;
                plan.VectorSelects[i] = new VectorSearchOp {
                    Clause = vectorClauses[i], Exec = exec };
            }
        }

        plan.Executions = execs;
    }

    /// <summary>Count how many IQueryMatch slots a clause list expands to.
    /// OrGroup/AndGroup/In/AllIn each expand to one slot per sub-term.</summary>
    internal static int CountMatchSlots(List<ClauseInfo> clauses, ClauseExecution[] executions, bool isAllEntries, bool allNegated)
    {
        if (clauses == null)
            return isAllEntries ? 1 : 0;

        int count = isAllEntries ? 1 : 0;
        for (int ci = 0; ci < clauses.Count; ci++)
        {
            var clause = clauses[ci];
            var exec = executions != null && ci < executions.Length ? executions[ci] : null;
            int inTermCount = exec?.InTermCount ?? 0;
            bool hasNullTerm = exec?.HasNullTerm ?? false;
            // OR-chain negated clauses (IsOrChainNotEquals) always use a single QueryMatch
            // slot containing AllEntries ANDNOT(positive form) — regardless of underlying
            // clause type (NotEquals, NOT IN, NOT AllIn, NOT exists(), ...).
            if (clause.IsOrChainNotEquals)
            {
                count += 1;
                continue;
            }
            count += clause.ClauseType switch
            {
                ClauseType.OrGroup when clause.OrSubClauses != null => clause.OrSubClauses.Count,
                ClauseType.AndGroup when clause.AndSubClauses != null => clause.AndSubClauses.Count,
                // Always +1 for the null-term slot so the plan structure is parameter-independent.
                // The slot is filled with an empty match when null isn't in the parameter.
                ClauseType.In or ClauseType.AllIn => inTermCount + 1,
                _ => 1
            };
        }
        if (allNegated)
            count++;
        return count;
    }

    // ── Dispatch classification ──────────────────────────────────────────

    /// <summary>Decide whether a clause type can be expressed as a single
    /// <see cref="PostingSource"/>. Boosted clauses go through the IQueryMatch path
    /// even when they're term-shaped, so scoring still works.</summary>
    internal static bool IsTermSourceEligibleClause(ClauseInfo clause, ClauseExecution exec = null)
    {
        if (clause == null)
            return false;
        if (exec is { BoostFactor: > 0 })
            return false;
        if (clause.HasBoost)
            return false;
        return clause.ClauseType is ClauseType.Equals or ClauseType.NotEquals;
    }

    /// <summary>TreeScan-eligible: multi-term clauses that have a direct ITermsProvider
    /// (StartsWith, EndsWith, Exists, Regex, ranges). Boosted clauses go through QueryMatch
    /// for scoring. Contains is excluded because its tree walk pattern doesn't benefit
    /// from the direct dispatch (it walks the full tree regardless).</summary>
    internal static bool IsTreeScanEligibleClause(ClauseInfo clause, ClauseExecution exec = null)
    {
        if (clause == null)
            return false;
        if (exec is { BoostFactor: > 0 })
            return false;
        if (clause.HasBoost)
            return false;
        // BETWEEN with a client-sent null sentinel rewrites at resolution time into
        // LessThanOrEqual / AllEntries-ANDNOT-LessThan — those custom shapes can't be
        // delivered by the TreeScan ITermsProvider dispatch, so force QueryMatch.
        if (clause.ClauseType == ClauseType.Between && exec is { BetweenLowUnbounded: true } or { BetweenHighUnbounded: true })
            return false;
        return clause.ClauseType is ClauseType.StartsWith or ClauseType.EndsWith
            or ClauseType.Exists or ClauseType.Regex
            or ClauseType.GreaterThan or ClauseType.GreaterThanOrEqual
            or ClauseType.LessThan or ClauseType.LessThanOrEqual
            or ClauseType.Between;
    }

    /// <summary>Resolve the <see cref="MatchDispatch"/> mode for a clause at plan-build time.
    /// Equals / NotEquals (unboosted) → <c>PostingList</c> (native posting-list).
    /// Multi-term (unboosted) → <c>TreeScan</c> (direct ITermsProvider, no IQueryMatch wrapper).
    /// All other clause types → <c>QueryMatch</c> (IQueryMatch interface dispatch).</summary>
    private static MatchDispatch GetDispatch(ClauseInfo clause, ClauseExecution exec = null)
    {
        if (IsTermSourceEligibleClause(clause, exec))
            return MatchDispatch.PostingList;
        if (IsTreeScanEligibleClause(clause, exec))
            return MatchDispatch.TreeScan;
        return MatchDispatch.QueryMatch;
    }

    // ── Entry scan predicate building ────────────────────────────────────

    /// <summary>Convert a ClauseInfo to a ScanPredicateInfo for entry scan IL emission.
    /// Returns null for complex clauses that can't be entry-scanned.</summary>
    private static ScanPredicateInfo? BuildScanPredicateInfo(ClauseInfo clause, ClauseExecution exec,
        ref int longIndex, ref int doubleIndex, ref int sliceIndex)
    {
        // Complex clauses can't be entry-scanned
        switch (clause.ClauseType)
        {
            case ClauseType.Search:
            case ClauseType.Regex:
            case ClauseType.Spatial:
            case ClauseType.Vector:
            case ClauseType.StartsWith:
            {
                var packed2 = exec?.PackedParamValue ?? PackedParam.None;
                if (packed2.IsNone || packed2.ValueType != PackedParam.TypeString) return null;
                sliceIndex++;
                return new ScanPredicateInfo
                {
                    FieldName = clause.FieldName,
                    ValueType = ScanValueType.Slice,
                    CompareOp = ScanCompareOp.StartsWith,
                    ParamIndex = sliceIndex - 1
                };
            }
            case ClauseType.In:
            case ClauseType.AllIn:
                // IN/AllIn cannot use the single-slot StartsWith fallback above — they
                // need multi-term OR/AND semantics. Returning null forces these clauses
                // through the regular posting-list pipeline; AreAllScanEligible will see
                // null and disable entry-scan for any AND chain that contains them.
                return null;
            case ClauseType.EndsWith:
            {
                var packed2 = exec?.PackedParamValue ?? PackedParam.None;
                if (packed2.IsNone || packed2.ValueType != PackedParam.TypeString) return null;
                sliceIndex++;
                return new ScanPredicateInfo
                {
                    FieldName = clause.FieldName,
                    ValueType = ScanValueType.Slice,
                    CompareOp = ScanCompareOp.EndsWith,
                    ParamIndex = sliceIndex - 1
                };
            }
            case ClauseType.AndGroup:
            {
                if (clause.AndSubClauses == null || clause.AndSubClauses.Count == 0)
                    return null;
                var branches = new List<ScanPredicateInfo>();
                var subExecs = exec?.AndSubExecutions;
                for (int si = 0; si < clause.AndSubClauses.Count; si++)
                {
                    var sub = clause.AndSubClauses[si];
                    var subExec = subExecs != null && si < subExecs.Length ? subExecs[si] : null;
                    var subPred = BuildScanPredicateInfo(sub, subExec, ref longIndex, ref doubleIndex, ref sliceIndex);
                    if (subPred == null) return null;
                    branches.Add(subPred.Value);
                }
                return new ScanPredicateInfo
                {
                    FieldName = clause.FieldName ?? clause.AndSubClauses[0].FieldName,
                    ValueType = ScanValueType.Long,
                    CompareOp = ScanCompareOp.Equal,
                    SubPredicates = branches.ToArray(),
                    Group = GroupKind.And
                };
            }

            case ClauseType.Exists:
                return new ScanPredicateInfo
                {
                    FieldName = clause.FieldName,
                    ValueType = ScanValueType.Long, // unused for Exists but must be set
                    CompareOp = ScanCompareOp.Exists,
                    ParamIndex = 0 // unused
                };

            case ClauseType.OrGroup:
            {
                if (clause.OrSubClauses == null || clause.OrSubClauses.Count == 0)
                    return null;
                var branches = new List<ScanPredicateInfo>();
                int li = longIndex, di = doubleIndex, si = sliceIndex;
                var subExecs = exec?.OrSubExecutions;
                for (int si2 = 0; si2 < clause.OrSubClauses.Count; si2++)
                {
                    var sub = clause.OrSubClauses[si2];
                    var subExec = subExecs != null && si2 < subExecs.Length ? subExecs[si2] : null;
                    var subPred = BuildScanPredicateInfo(sub, subExec, ref li, ref di, ref si);
                    if (subPred == null)
                        return null; // Any complex subclause → can't entry-scan the whole group
                    branches.Add(subPred.Value);
                }
                longIndex = li; doubleIndex = di; sliceIndex = si;
                return new ScanPredicateInfo
                {
                    FieldName = clause.OrSubClauses[0].FieldName,
                    SubPredicates = branches.ToArray(),
                    Group = GroupKind.Or
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

        // Strong typing: TermValueType is set by GetTermValue from the parser's literal
        // type (for inline values) or the resolved JSON-blittable runtime type (for params).
        // Switch on it directly — no string round-trip / TryParse fallback. A null TermValue
        // (e.g. "exists" check) falls through to Slice.
        ScanValueType valueType;
        switch (exec?.TermValueType ?? ParamValueType.String)
        {
            case ParamValueType.Long:
                valueType = ScanValueType.Long;
                break;
            case ParamValueType.Double:
                valueType = ScanValueType.Double;
                break;
            default:
                // String/True/False/Null/Parameter (when unresolvable) → opaque slice comparison.
                valueType = ScanValueType.Slice;
                break;
        }

        bool isBetween = clause.ClauseType == ClauseType.Between;
        int idx, idx2;
        switch (valueType)
        {
            case ScanValueType.Long:
                idx = longIndex++;
                idx2 = isBetween ? longIndex++ : -1;
                break;
            case ScanValueType.Double:
                idx = doubleIndex++;
                idx2 = isBetween ? doubleIndex++ : -1;
                break;
            default:
                idx = sliceIndex++;
                idx2 = isBetween ? sliceIndex++ : -1;
                break;
        }

        return new ScanPredicateInfo
        {
            FieldName = clause.FieldName,
            ValueType = valueType,
            CompareOp = compareOp,
            ParamIndex = idx,
            ParamIndex2 = idx2
        };
    }
}
