using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Corax.Querying.Planning;
using Corax.Mappings;
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

    /// <summary>True when a value of type <paramref name="termType"/> cannot be coerced
    /// to <paramref name="dominantType"/> without throwing. Used to filter mixed-type IN
    /// lists: a string term in an otherwise-numeric IN list (e.g. IN(DateTime, "Shalom")
    /// on a DateTime-indexed field) can never match a numeric-indexed term and would
    /// throw on Convert.ToInt64, so it is dropped instead.</summary>
    private static bool AreTypesIncompatible(ParamValueType termType, ParamValueType dominantType)
    {
        return termType != dominantType && (
            // Long and Double are mutually coercible.
            termType is not (ParamValueType.Long or ParamValueType.Double) ||
            dominantType is not (ParamValueType.Long or ParamValueType.Double));
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

    /// <summary>Parse the RQL AST into a structural plan template.
    /// Captures field names, clause types, parameter bindings, and literal values.
    /// No cardinality estimation, no sorting, no plan emission.
    /// Those happen in BuildAndCompile after PopulateClauseValues.</summary>
    public static PlanTemplate ParseTemplate(PlanParameters p)
    {
        var query = p.Metadata.Query;
        if (query.Where == null)
            return new PlanTemplate { Clauses = [] };

        // Phase 1: materialize the AST into ClauseInfo[]. Each Parse method validates
        // its own preconditions (field name, argument count, type compatibility) and
        // reports errors to walkerCtx.Errors — they are thrown as a single
        // InvalidQueryException after materialization completes.
        var walkerCtx = new ResolutionContext(p){ Clauses = [] };
        var rootOp = ParseExpression(query.Where, walkerCtx);
        PlanWalker.ThrowIfErrors(walkerCtx);

        if (rootOp == BooleanOp.True || walkerCtx.Clauses.Count == 0)
            return new PlanTemplate { Clauses = [] };

        Debug.Assert(rootOp != BooleanOp.False,
            "No RQL expression currently reduces to BooleanOp.False at template time. " +
            "If a future rewrite introduces this, add an AlwaysEmpty flag to PlanTemplate " +
            "and handle it in Build (return null → empty result, not AllEntries).");

        walkerCtx.IsOr = rootOp == BooleanOp.Or;

        // Phase 3: walker — rewrite/register steps on the materialized ClauseInfo list
        // before freezing. Currently, runs GroupCollapse (spatial/vector partition) then
        // WhenRegister; future steps slot in here.
        PlanWalker.RewriteClauses(walkerCtx);

        // Spatial-only or vector-only AND query with no remaining filter clauses
        // returns an IsAllEntries template that carries the aux arrays only.
        if (walkerCtx.Clauses.Count == 0 && (walkerCtx.SpatialClauses ?? walkerCtx.VectorClauses) is not null)
        {
            FreezeAll(walkerCtx.SpatialClauses);
            FreezeAll(walkerCtx.VectorClauses);
            return new PlanTemplate
            {
                Clauses = [],
                SpatialClauses = walkerCtx.SpatialClauses,
                VectorClauses = walkerCtx.VectorClauses
            };
        }

        FreezeAll(walkerCtx.Clauses);
        FreezeAll(walkerCtx.SpatialClauses);
        FreezeAll(walkerCtx.VectorClauses);

        // Primary ORDER BY field name from the query metadata (null if no ORDER BY or
        // ORDER BY score/random/etc. which have no field name).
        string orderByPrimaryField = p.Metadata.OrderBy is { Length: > 0 }
            ? p.Metadata.OrderBy[0].Name?.Value
            : null;

        var optFlags = ComputeTemplateOptimizations(walkerCtx, p, orderByPrimaryField, out int sortDrivingIdx);

        return new PlanTemplate
        {
            Clauses = walkerCtx.Clauses,
            IsOr = walkerCtx.IsOr,
            SpatialClauses = walkerCtx.SpatialClauses,
            VectorClauses = walkerCtx.VectorClauses,
            WhenCount = walkerCtx.WhenCount,
            OptimizationFlags = optFlags,
            SortDrivingClauseIndex = sortDrivingIdx,
            CompoundExactClauseA = walkerCtx.CompoundExactClauseA,
            CompoundExactClauseB = walkerCtx.CompoundExactClauseB,
            CompoundExactAFirst = walkerCtx.CompoundExactAFirst,
            CompoundFieldDrivingClause = walkerCtx.CompoundFieldDrivingClause,
            CompoundFieldSortName = walkerCtx.CompoundFieldSortName,
            CompoundFieldIsMultiSort = walkerCtx.CompoundFieldIsMultiSort
        };
    }

    [SkipLocalsInit]
    private static PlanOptimizationFlags ComputeTemplateOptimizations(
        ResolutionContext walkerCtx, PlanParameters p, string orderByPrimaryField, out int sortDrivingIdx)
    {
        sortDrivingIdx = -1;
        var flags = PlanOptimizationFlags.None;
        var clauses = walkerCtx.Clauses;

        // Collect non-negated, non-boosted Equals clause indices for compound lookups.
        const int maxStackAllocSize = 64;
        Span<int> eqBuf = clauses.Count <= maxStackAllocSize ? stackalloc int[maxStackAllocSize] : new int[clauses.Count];
        int eqCount = 0;

        for (int i = 0; i < clauses.Count; i++)
        {
            var c = clauses[i];

            if (HasBoostRecursive(c)) // Any boost anywhere rules out DirectScan and CompoundField (no scoring stage).
                return PlanOptimizationFlags.None;

            if (c.IsNegated)
                continue;

            if (c.ClauseType == ClauseType.Equals)
                eqBuf[eqCount++] = i;

            if (orderByPrimaryField is null || c.FieldName != orderByPrimaryField)
                continue;

            if (c.ClauseType is not (
                ClauseType.Equals or ClauseType.GreaterThan or
                ClauseType.GreaterThanOrEqual or ClauseType.LessThan or
                ClauseType.LessThanOrEqual or ClauseType.Between)
               )
                continue;

            flags |= PlanOptimizationFlags.DirectScanCandidate;
            if (sortDrivingIdx == -1) sortDrivingIdx = i;
        }

        if (eqCount >= 2)
            flags |= PlanOptimizationFlags.CompoundExactCandidate;

        if (walkerCtx.IsOr || // cannot optimize: `where a OR b`  
            p.Index is not { HasCompoundFields: true } ||
            eqCount is 0)
            return flags;

        // Compound-exact pair: two Equals clauses whose fields form a compound field.
        if (eqCount >= 2) TryFindCompoundFieldEqualMatches(eqBuf);

        // Compound-field candidate: Equals clause + ORDER BY field forming a compound field.
        switch (p.Metadata.OrderBy)
        {
            // Case 1: OrderBy has exactly 2 elements with non-null names
            case [{ Name.Value: { } f1 }, { Name.Value: { } f2 }] when p.Index.HasCompoundField(p.Allocator, f1, f2):
                for (int e = 0; e < eqCount; e++)
                {
                    if (clauses[eqBuf[e]].FieldName != f1) continue;
                    walkerCtx.CompoundFieldDrivingClause = eqBuf[e];
                    walkerCtx.CompoundFieldSortName = f2;
                    walkerCtx.CompoundFieldIsMultiSort = true;
                    break;
                }

                break;

            // Case 2: OrderBy has exactly 1 element with a non-null name
            case [{ Name.Value: { } sf }]:
                for (int e = 0; e < eqCount; e++)
                {
                    string ef = clauses[eqBuf[e]].ResolvedFieldName ?? clauses[eqBuf[e]].FieldName;
                    if (p.Index.HasCompoundField(p.Allocator, ef, sf) == false) continue;
                    walkerCtx.CompoundFieldDrivingClause = eqBuf[e];
                    walkerCtx.CompoundFieldSortName = sf;
                    break;
                }

                break;
        }

        return flags;

        static bool HasBoostRecursive(ClauseInfo c)
        {
            if (c.HasBoost)
                return true;
            foreach (var t in c.OrSubClauses ?? c.AndSubClauses ?? [])
            {
                if (HasBoostRecursive(t))
                    return true;
            }

            return false;
        }

        void TryFindCompoundFieldEqualMatches(Span<int> eqBuf)
        {
            for (int a = 0; a < eqCount && walkerCtx.CompoundExactClauseA < 0; a++)
            {
                var c1 = clauses[eqBuf[a]];
                string f1 = c1.ResolvedFieldName ?? c1.FieldName;
                for (int b = a + 1; b < eqCount; b++)
                {
                    var c2 = clauses[eqBuf[b]];
                    string f2 = c2.ResolvedFieldName ?? c2.FieldName;
                    using (Voron.Slice.From(p.Allocator, f1, out var s1))
                    using (Voron.Slice.From(p.Allocator, f2, out var s2))
                    {
                        if (p.Index.HasCompoundField(s1, s2))
                        {
                            walkerCtx.CompoundExactClauseA = eqBuf[a];
                            walkerCtx.CompoundExactClauseB = eqBuf[b];
                            walkerCtx.CompoundExactAFirst = true;
                            break;
                        }

                        if (p.Index.HasCompoundField(s2, s1))
                        {
                            walkerCtx.CompoundExactClauseA = eqBuf[a];
                            walkerCtx.CompoundExactClauseB = eqBuf[b];
                            walkerCtx.CompoundExactAFirst = false;
                            break;
                        }
                    }
                }
            }
        }
    }

    private static void FreezeAll(List<ClauseInfo> clauses)
    {
        RuntimeHelpers.EnsureSufficientExecutionStack();

        if (clauses is not { Count: > 0 })
            return;

        foreach (var c in clauses)
        {
            if (c == null || c.IsFrozen)
                continue;

            FreezeAll(c.OrSubClauses);
            FreezeAll(c.AndSubClauses);
            c.Freeze();
        }
    }

    private static BooleanOp ParseExpression(QueryExpression expr, ResolutionContext walkerCtx)
    {
        RuntimeHelpers.EnsureSufficientExecutionStack();
        switch (expr)
        {
            case BinaryExpression be:
                return ParseBinaryExpression(be, walkerCtx);

            case BetweenExpression between:
                ParseBetween(between, walkerCtx);
                return BooleanOp.Leaf;

            case InExpression inExpr:
                ParseIn(inExpr, walkerCtx);
                return BooleanOp.Leaf;

            case MethodExpression method:
                return ParseMethod(method, walkerCtx);

            case NegatedExpression negated:
                ParseNegated(negated, walkerCtx);
                return BooleanOp.Leaf;

            case TrueExpression:
                return BooleanOp.True;

            default:
                throw new InvalidOperationException(
                    $"Unexpected expression type {expr.GetType().Name} in WHERE clause.");
        }
    }

    private static BooleanOp ParseBinaryExpression(BinaryExpression be, ResolutionContext walkerCtx)
    {
        switch (be.Operator)
        {
            case OperatorType.And:
            {
                // For AND, handle OR sub-expressions as grouped clauses
                var left = be.Left is BinaryExpression { Operator: OperatorType.Or } ? HandleGroup(be.Left, ClauseType.OrGroup) : ParseExpression(be.Left, walkerCtx);

                var right = be.Right is BinaryExpression { Operator: OperatorType.Or } ? HandleGroup(be.Right, ClauseType.OrGroup) : ParseExpression(be.Right, walkerCtx);

                return (left, right) switch
                {
                    (BooleanOp.True, _) => right,
                    (_, BooleanOp.True) => left,
                    (BooleanOp.False, BooleanOp.False) => BooleanOp.False,
                    _ => BooleanOp.And
                };
            }

            case OperatorType.Or:
            {
                var left = be.Left is BinaryExpression { Operator: OperatorType.And } ? HandleGroup(be.Left, ClauseType.AndGroup) : ParseExpression(be.Left, walkerCtx);

                var right = be.Right is BinaryExpression { Operator: OperatorType.And } ? HandleGroup(be.Right, ClauseType.AndGroup) : ParseExpression(be.Right, walkerCtx);

                return (left, right) switch
                {
                    (BooleanOp.True, _) => BooleanOp.True,
                    (_, BooleanOp.True) => BooleanOp.True,
                    (BooleanOp.False, BooleanOp.False) => BooleanOp.False,
                    _ => BooleanOp.Or
                };
            }

            case OperatorType.Equal:
                ParseComparison(be, walkerCtx);
                return BooleanOp.Leaf;

            case OperatorType.NotEqual:
                ParseComparison(be, walkerCtx);
                return BooleanOp.Leaf;

            case OperatorType.LessThan:
            case OperatorType.LessThanEqual:
            case OperatorType.GreaterThan:
            case OperatorType.GreaterThanEqual:
                ParseRangeComparison(be, walkerCtx);
                return BooleanOp.Leaf;

            default:
                throw new InvalidOperationException(
                    $"Unexpected binary operator {be.Operator} in WHERE clause.");
        }

        BooleanOp HandleGroup(QueryExpression queryExpression, ClauseType clauseType)
        {
            var saved = walkerCtx.Clauses;
            walkerCtx.Clauses = [];
            var expr = ParseExpression(queryExpression, walkerCtx);
            var clauses = walkerCtx.Clauses;
            walkerCtx.Clauses = saved;

            ClauseInfo clauseInfo = new() { ClauseType = clauseType, OriginalIndex = walkerCtx.Clauses.Count };
            switch (clauseType)
            {
                case ClauseType.OrGroup:
                    clauseInfo.OrSubClauses = clauses;
                    break;
                case ClauseType.AndGroup:
                    clauseInfo.AndSubClauses = clauses;
                    break;
                default:
                    throw new InvalidOperationException($"Unexpected clause type: {clauseType}");
            }

            walkerCtx.Clauses.Add(clauseInfo);
            return expr;
        }
    }

    private static void ParseComparison(BinaryExpression be, ResolutionContext walkerCtx)
    {
        if (TryGetFieldName(be.Left, walkerCtx, out string fieldName) == false)
        {
            walkerCtx.Report($"Comparison left side must be a field expression or id(), but got: {be.Left.Type}");
            return;
        }

        walkerCtx.Clauses.Add(new ClauseInfo
        {
            FieldName = fieldName,
            ClauseType = be.Operator == OperatorType.NotEqual ? ClauseType.NotEquals : ClauseType.Equals,
            OriginalIndex = walkerCtx.Clauses.Count,
            Bindings = [CreateBinding(be.Right, walkerCtx)]
        });
    }

    private static void ParseRangeComparison(BinaryExpression be, ResolutionContext walkerCtx)
    {
        if (TryGetFieldName(be.Left, walkerCtx, out string fieldName) == false)
        {
            walkerCtx.Report($"Range comparison left side must be a field expression or id(), but got: {be.Left.Type}");
            return;
        }

        walkerCtx.Clauses.Add(new ClauseInfo
        {
            FieldName = fieldName,
            Bindings = [CreateBinding(be.Right, walkerCtx)],
            ClauseType = be.Operator switch
            {
                OperatorType.GreaterThan => ClauseType.GreaterThan,
                OperatorType.GreaterThanEqual => ClauseType.GreaterThanOrEqual,
                OperatorType.LessThan => ClauseType.LessThan,
                OperatorType.LessThanEqual => ClauseType.LessThanOrEqual,
                _ => ClauseType.Equals
            },
            OriginalIndex = walkerCtx.Clauses.Count,
        });
    }

    private static void ParseBetween(BetweenExpression between, ResolutionContext walkerCtx)
    {
        if (TryGetFieldName(between.Source, walkerCtx, out string resolvedFieldName) == false)
        {
            walkerCtx.Report($"BETWEEN source must be a field expression or id(), but got: {between.Source.Type}");
            return;
        }

        var minBinding = CreateBinding(between.Min, walkerCtx);
        var maxBinding = CreateBinding(between.Max, walkerCtx);

        // Validate literal-type compatibility. Parameter-typed bindings are validated later
        // in PopulateParameters, when the actual value is known. Sentinel bounds ("*"/"NULL")
        // are allowed with any other type — they're rewritten away by BetweenRewriteSentinels.
        bool minIsSentinel = minBinding is { LiteralType: ParamValueType.String, LiteralValue: Client.Constants.Documents.Querying.Terms.LeftNullValueOfBetweenQuery };
        bool maxIsSentinel = maxBinding is { LiteralType: ParamValueType.String, LiteralValue: Client.Constants.Documents.Querying.Terms.RightNullValueOfBetweenQuery };
        bool bothAstStrings = between is { Min.Value: ValueTokenType.String, Max.Value: ValueTokenType.String };
        if (minIsSentinel == false && maxIsSentinel == false
                                   && bothAstStrings == false
                                   && minBinding is { LiteralType: not ParamValueType.Parameter }
                                   && maxBinding is { LiteralType: not ParamValueType.Parameter }
                                   && minBinding.LiteralType != maxBinding.LiteralType)
        {
            walkerCtx.Report(
                $"BETWEEN bounds for field '{resolvedFieldName}' have different types: " +
                $"low is {minBinding.LiteralType}, high is {maxBinding.LiteralType}. Both must be the same type.");
            return;
        }

        walkerCtx.Clauses.Add(new ClauseInfo
        {
            FieldName = resolvedFieldName,
            ClauseType = ClauseType.Between,
            OriginalIndex = walkerCtx.Clauses.Count,
            Bindings = [minBinding, maxBinding]
        });
    }

    private static void ParseIn(InExpression inExpr, ResolutionContext walkerCtx)
    {
        if (TryGetFieldName(inExpr.Source, walkerCtx, out string resolvedFieldName) == false)
        {
            walkerCtx.Report($"IN source must be a field expression or id(), but got: {inExpr.Source.Type}");
            return;
        }

        if (inExpr.Values.Count == 0)
        {
            walkerCtx.Report("IN/ALL IN with an empty value list is a syntax error.");
            return;
        }

        // Capture bindings for each IN term. Array parameters expand at PopulateParameters time.
        var inBindings = new List<ParameterBinding>();
        foreach (var value in inExpr.Values)
        {
            if (CreateBinding(value, walkerCtx) is { } binding)
                inBindings.Add(binding);
        }

        // Empty IN() with no bindings still creates an In clause —
        // PopulateClauseValues sets InTermCount=0, EmitPlan handles empty IN.
        walkerCtx.Clauses.Add(new ClauseInfo
        {
            FieldName = resolvedFieldName,
            ClauseType = inExpr.All ? ClauseType.AllIn : ClauseType.In,
            OriginalIndex = walkerCtx.Clauses.Count,
            Bindings = inBindings.ToArray()
        });
    }

    private static void ParseNegated(NegatedExpression negated, ResolutionContext walkerCtx)
    {
        var saved = walkerCtx.Clauses;
        walkerCtx.Clauses = [];
        ParseExpression(negated.Expression, walkerCtx);
        var innerClauses = walkerCtx.Clauses;
        walkerCtx.Clauses = saved;
        foreach (var inner in innerClauses)
        {
            inner.IsNegated = true;
            walkerCtx.Clauses.Add(inner);
        }
    }

    private static BooleanOp ParseMethod(MethodExpression method, ResolutionContext walkerCtx)
    {
        var methodType = QueryMethod.GetMethodType(method.Name.Value);
        switch (methodType)
        {
            case MethodType.Search:
                ParseSearchMethod(method, walkerCtx);
                break;

            case MethodType.StartsWith:
                ParsePrefixMethod(method, ClauseType.StartsWith, walkerCtx);
                break;

            case MethodType.EndsWith:
                ParsePrefixMethod(method, ClauseType.EndsWith, walkerCtx);
                break;

            case MethodType.Exists:
            {
                if (method.Arguments.Count == 0)
                {
                    walkerCtx.Report("exists() requires a field argument.");
                    break;
                }

                if (TryGetFieldName(method.Arguments[0], walkerCtx, out var existsFieldName) == false)
                {
                    walkerCtx.Report($"exists() argument must be a field name, but got: {method.Arguments[0].Type} ({method.Arguments[0]}).");
                    break;
                }

                walkerCtx.Clauses.Add(new ClauseInfo
                {
                    FieldName = existsFieldName,
                    ClauseType = ClauseType.Exists,
                    OriginalIndex = walkerCtx.Clauses.Count
                });
                break;
            }

            case MethodType.Exact:
            {
                // exact(expr) → recurse, then mark all new clauses as exact.
                // Propagate the inner BooleanOp so that exact(A OR B) is still detected as OR at
                // the root — otherwise the outer ParseExpression would see Leaf and compile in
                // AND mode, intersecting the OR branches.
                int beforeCount = walkerCtx.Clauses.Count;
                BooleanOp innerOp = BooleanOp.Leaf;
                if (method.Arguments.Count > 0)
                {
                    innerOp = ParseExpression(method.Arguments[0], walkerCtx);
                }

                for (int c = beforeCount; c < walkerCtx.Clauses.Count; c++)
                {
                    walkerCtx.Clauses[c].IsExact = true;
                }

                return innerOp;
            }

            case MethodType.Boost:
            {
                // boost(expr, factor) → recurse, then capture the clauses' instances for later call to BoostPropagate.
                // even if they are moved by GroupCollapse or AnalyzerRewrite later on, the instances are the same
                int beforeCount = walkerCtx.Clauses.Count;
                if (method.Arguments.Count is 0)
                    return BooleanOp.Leaf;
                var innerOp = ParseExpression(method.Arguments[0], walkerCtx);
                if (method.Arguments.Count is 1 ||
                    walkerCtx.Clauses.Count == beforeCount ||
                    CreateBinding(method.Arguments[1], walkerCtx) is not { } boostBinding)
                    return innerOp;

                var inner = new ClauseInfo[walkerCtx.Clauses.Count - beforeCount];
                for (int c = 0; c < inner.Length; c++)
                {
                    inner[c] = walkerCtx.Clauses[beforeCount + c];
                }

                walkerCtx.RecordPendingBoost(inner, boostBinding);
                return innerOp;
            }

            case MethodType.Regex:
            {
                if (method.Arguments.Count < 2)
                {
                    walkerCtx.Report($"regex() requires at least 2 arguments (field, pattern), but got {method.Arguments.Count}.");
                    break;
                }

                if (TryGetFieldName(method.Arguments[0], walkerCtx, out var regexFieldName) == false)
                {
                    walkerCtx.Report($"regex() first argument must be a field name, but got: {method.Arguments[0].Type} ({method.Arguments[0]}).");
                    break;
                }

                walkerCtx.Clauses.Add(new ClauseInfo
                {
                    FieldName = regexFieldName,
                    ClauseType = ClauseType.Regex,
                    OriginalIndex = walkerCtx.Clauses.Count,
                    Bindings = [CreateBinding(method.Arguments[1], walkerCtx)]
                });
                break;
            }

            case MethodType.Spatial_Within:
            case MethodType.Spatial_Contains:
            case MethodType.Spatial_Disjoint:
            case MethodType.Spatial_Intersects:
            {
                if (method.Arguments is [_, not MethodExpression, ..])
                {
                    walkerCtx.Report($"Spatial shape argument must be a method expression (spatial.circle or spatial.wkt), but got: {method.Arguments[1].Type}");
                    break;
                }

                // Capture bindings for all spatial sub-arguments.
                // Shape type and field name are structural; parameter values resolved per-execution.
                string spatialFieldName;
                if (walkerCtx.Metadata.IsDynamic && method.Arguments[0] is MethodExpression spatialPointExpr)
                {
                    spatialFieldName = walkerCtx.Metadata.GetSpatialFieldName(spatialPointExpr, walkerCtx.QueryParameters);
                }
                else if (TryGetFieldName(method.Arguments[0], walkerCtx, out var sfn))
                {
                    spatialFieldName = sfn;
                }
                else
                {
                    spatialFieldName = QueryBuilderHelper.ExtractIndexFieldName(walkerCtx.Metadata.Query, walkerCtx.QueryParameters, method.Arguments[0], walkerCtx.Metadata);
                }

                var shapeExpr = (MethodExpression)method.Arguments[1];
                var shapeType = QueryMethod.GetMethodType(shapeExpr.Name.Value);

                // Build spatial bindings: [0]=distErrPct, then shape-specific args
                List<ParameterBinding> spatialBindings =
                [
                    method.Arguments.Count == 3
                        ? CreateBinding(method.Arguments[2], walkerCtx)
                        : null
                ];

                switch (shapeType, shapeExpr.Arguments.Count)
                {
                    case (MethodType.Spatial_Circle, >= 3):
                        spatialBindings.Add(CreateBinding(shapeExpr.Arguments[0], walkerCtx)); // radius
                        spatialBindings.Add(CreateBinding(shapeExpr.Arguments[1], walkerCtx)); // lat
                        spatialBindings.Add(CreateBinding(shapeExpr.Arguments[2], walkerCtx)); // lng
                        spatialBindings.Add(shapeExpr.Arguments.Count == 4 // units (optional)
                            ? CreateBinding(shapeExpr.Arguments[3], walkerCtx)
                            : null);
                        break;
                    case (MethodType.Spatial_Wkt, >= 1):
                        spatialBindings.Add(CreateBinding(shapeExpr.Arguments[0], walkerCtx)); // wkt
                        spatialBindings.Add(shapeExpr.Arguments.Count == 2 // units (optional)
                            ? CreateBinding(shapeExpr.Arguments[1], walkerCtx)
                            : null);
                        break;
                }

                walkerCtx.Clauses.Add(new ClauseInfo
                {
                    FieldName = spatialFieldName,
                    ClauseType = ClauseType.Spatial,
                    SpatialMethodType = ToSpatialOp(methodType),
                    OriginalIndex = walkerCtx.Clauses.Count,
                    Bindings = spatialBindings.ToArray()
                });
                break;
            }

            case MethodType.Vector_Search:
            {
                // Capture bindings for vector sub-arguments.
                // Resolve field name (structural — uses metadata for dynamic index field naming).
                string vectorFieldName = walkerCtx.Metadata.IsDynamic
                    ? walkerCtx.Metadata.GetVectorFieldName(method, walkerCtx.QueryParameters)
                    : QueryBuilderHelper.ExtractIndexFieldName(walkerCtx.Metadata.Query, walkerCtx.QueryParameters, method.Arguments[0], walkerCtx.Metadata);

                VectorSourceKind vecMethod = VectorSourceKind.Inline;
                ParameterBinding vectorValueBinding = null;
                ParameterBinding aiTaskBinding = null;

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
                    {
                        vectorValueBinding = CreateBinding(methodValue.Arguments[0], walkerCtx);
                    }

                    if (vecMethod == VectorSourceKind.FromText && methodValue.Arguments.Count > 1
                                                               && methodValue.Arguments[1] is MethodExpression aiMethod && aiMethod.Arguments.Count > 0)
                    {
                        aiTaskBinding = CreateBinding(aiMethod.Arguments[0], walkerCtx);
                    }
                }
                else
                {
                    vectorValueBinding = CreateBinding(srcVector, walkerCtx);
                }

                ParameterBinding minimumMatchBinding = method.Arguments.Count <= 2 ? null : CreateBinding(method.Arguments[2], walkerCtx);
                ParameterBinding numberOfCandidatesBinding = method.Arguments.Count <= 3 ? null : CreateBinding(method.Arguments[3], walkerCtx);
                walkerCtx.Clauses.Add(new ClauseInfo
                {
                    FieldName = vectorFieldName,
                    ClauseType = ClauseType.Vector,
                    OriginalIndex = walkerCtx.Clauses.Count,
                    Bindings = [vectorValueBinding, minimumMatchBinding, numberOfCandidatesBinding, aiTaskBinding],
                    VectorMethod = vecMethod
                });
                break;
            }

            case MethodType.MoreLikeThis:
                // MoreLikeThis method in a WHERE clause acts as "all entries" —
                // the actual MLT logic is in the separate reader.MoreLikeThis() path.
                // When it appears in a filter expression, treat it as no-op (all entries match).
                break;

            case MethodType.When:
            {
                // when(condition, expr) — create a delegate that evaluates the condition
                // against the query's BlittableJsonReaderObject parameters at execution time.
                // Clauses whose condition evaluates to false are eliminated in BuildAndCompile.
                //
                // Propagate the inner BooleanOp so that when(c, A OR B) preserves the OR shape
                // for rootOp detection (parallels Boost/Exact wrappers).
                if (method.Arguments.Count != 2)
                {
                    break;
                }

                var conditionExpr = method.Arguments[0];
                int beforeCount = walkerCtx.Clauses.Count;
                BooleanOp innerOp = ParseExpression(method.Arguments[1], walkerCtx);
                for (int wi = beforeCount; wi < walkerCtx.Clauses.Count; wi++)
                {
                    walkerCtx.Clauses[wi].WhenCondition = (queryParams) =>
                        QueryBuilderHelper.EvaluateConstantExpressionForWhenQuery(conditionExpr, walkerCtx.Metadata.Query, walkerCtx.Metadata, queryParams);
                }

                return innerOp;
            }

            default:
                throw new InvalidOperationException(
                    $"Unexpected method '{method.Name.Value}' ({methodType}) in WHERE clause.");
        }

        // Leaf methods (Search, StartsWith, EndsWith, Exists, Regex, Spatial_*, Vector.*, MoreLikeThis, etc.)
        // and the malformed-When fallback all add at most one ClauseInfo with no nested boolean structure.
        return BooleanOp.Leaf;
    }

    private static void ParseSearchMethod(MethodExpression method, ResolutionContext walkerCtx)
    {
        if (method.Arguments.Count < 2)
        {
            walkerCtx.Report($"search() requires at least 2 arguments (field, term), but got {method.Arguments.Count}.");
            return;
        }

        if (TryGetFieldName(method.Arguments[0], walkerCtx, out var fieldName) == false)
        {
            walkerCtx.Report($"search() first argument must be a field name, but got: {method.Arguments[0].Type} ({method.Arguments[0]}).");
            return;
        }

        var searchOp = Constants.Search.Operator.Or;
        if (method.Arguments.Count >= 3 && method.Arguments[2] is FieldExpression opField
                                        && opField.Compound.Count == 1)
        {
            var op = opField.Compound[0].Value;
            if (string.Equals("AND", op, StringComparison.OrdinalIgnoreCase))
            {
                searchOp = Constants.Search.Operator.And;
            }
        }

        walkerCtx.Clauses.Add(new ClauseInfo
        {
            FieldName = fieldName,
            ClauseType = ClauseType.Search,
            SearchOperator = (int)searchOp,
            OriginalIndex = walkerCtx.Clauses.Count,
            Bindings = [CreateBinding(method.Arguments[1], walkerCtx)]
        });
    }

    private static void ParsePrefixMethod(MethodExpression method, ClauseType type, ResolutionContext walkerCtx)
    {
        if (method.Arguments.Count < 2)
        {
            walkerCtx.Report($"{type.ToString().ToLowerInvariant()}() requires at least 2 arguments (field, prefix), but got {method.Arguments.Count}.");
            return;
        }

        if (TryGetFieldName(method.Arguments[0], walkerCtx, out var fieldName) == false)
        {
            walkerCtx.Report($"{type.ToString().ToLowerInvariant()}() first argument must be a field name, but got: {method.Arguments[0].Type} ({method.Arguments[0]}).");
            return;
        }

        walkerCtx.Clauses.Add(new ClauseInfo
        {
            FieldName = fieldName,
            ClauseType = type,
            OriginalIndex = walkerCtx.Clauses.Count,
            Bindings = [CreateBinding(method.Arguments[1], walkerCtx)]
        });
    }

    /// <summary>Extract the field name with proper alias resolution using query metadata.</summary>
    private static string GetFieldName(FieldExpression field, QueryMetadata metadata, BlittableJsonReaderObject queryParameters)
    {
        return metadata != null ? metadata.GetIndexFieldName(field, queryParameters).Value : field.FieldValue;
    }

    /// <summary>Try to extract a field name from a query expression that may be a
    /// <see cref="FieldExpression"/> (normal field), a <see cref="ValueExpression"/>
    /// (quoted field name like <c>'Order'</c>), or a <see cref="MethodExpression"/>
    /// for the <c>id()</c> function. Returns false if the expression is none of these.</summary>
    private static bool TryGetFieldName(QueryExpression expr, ResolutionContext ctx, out string fieldName)
    {
        if (expr is FieldExpression fe)
        {
            fieldName = GetFieldName(fe, ctx.Metadata, ctx.QueryParameters);
            return true;
        }

        // Quoted field names (e.g. 'Order' for reserved words) are parsed as ValueExpression
        if (expr is ValueExpression ve)
        {
            var resolved = ve.GetValue(ctx.QueryParameters)?.ToString();
            if (resolved != null)
            {
                fieldName = ctx.Metadata != null
                    ? ctx.Metadata.GetIndexFieldName(new QueryFieldName(resolved, ve.Value == ValueTokenType.String), ctx.QueryParameters).Value
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
            case null:
                return (null, ValueTokenType.String);
            case bool b:
                return (b ? "true" : "false", ValueTokenType.String);
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
            case DateTime dt:
                return (dt.Ticks, ValueTokenType.Long);
            case DateTimeOffset dto:
                return (dto.UtcDateTime.Ticks, ValueTokenType.Long);
            case LazyNumberValue lnv when lnv.TryParseLong(out long lnvLong):
                return (lnvLong, ValueTokenType.Long);
            case LazyNumberValue lnv:
                return ((double)lnv, ValueTokenType.Double);
            default:
            {
                var str = value.ToString();
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
    private static ParameterBinding CreateBinding(QueryExpression expr, ResolutionContext ctx)
    {
        switch (expr)
        {
            case MethodExpression me:
                // Method expressions like cmpxchg(), now(), today() must be resolved at execution
                // time (not template creation time) because their values can change between executions.
                // Create a closure delegate that captures the AST node and evaluates it with
                // the QueryBuilderParameters provided at invocation time (boxed as object).
                return new ParameterBinding
                {
                    Source = BindingSource.DeferredMethod,
                    DeferredExpression = (builderParamsObj, qp) =>
                    {
                        var bp = (QueryBuilderParameters)builderParamsObj;
                        var resolvedExpr = QueryBuilderHelper.EvaluateMethod(
                            bp.Query.Metadata.Query,
                            bp.Metadata,
                            bp.ServerContext,
                            bp.DocumentsContext.DocumentDatabase.CompareExchangeStorage,
                            me, qp, bp.QueryTime);
                        if (resolvedExpr is not ValueExpression valueExpression || valueExpression.Value == ValueTokenType.Null)
                            return null;

                        return valueExpression.GetValue(qp);
                    },
                    LiteralType = ParamValueType.String
                };
            case ValueExpression ve:
                if (ve.Value == ValueTokenType.Parameter)
                    return new ParameterBinding { Source = BindingSource.QueryParameter, ParameterName = ve.Token.Value, LiteralType = ParamValueType.Parameter };
                var value = ve.GetValue(ctx.QueryParameters);

                if (ve.Value == ValueTokenType.Null || value is null)
                    return new ParameterBinding { Source = BindingSource.Literal, LiteralValue = null, LiteralType = ParamValueType.String };

                if (value is bool b)
                    return new ParameterBinding { Source = BindingSource.Literal, LiteralValue = b ? "true" : "false", LiteralType = ParamValueType.String };

                var (resolved, resolvedType) = ResolveParameterValue(value);
                return new ParameterBinding { Source = BindingSource.Literal, LiteralValue = resolved, LiteralType = ToParamValueType(resolvedType) };
            default:
                return null;
        }
    }

    /// <summary>Derive the term value type from a clause's binding. Used for sub-clauses
    /// in OrGroup/AndGroup where the caller doesn't have per-subclause term types.</summary>
    private static ParamValueType InferTermType(ClauseInfo clause)
    {
        if (clause.Bindings is [{ LiteralType: not ParamValueType.Parameter } lit, ..])
            return lit.LiteralType;
        return ParamValueType.String;
    }
}
