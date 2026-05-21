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

        // Phase 1: walker — validate AST shape. Accumulates every shape error into
        // walkerCtx.Errors so the user sees them all at once.
        var walkerCtx = new ResolutionContext(p);
        PlanWalker.ValidateAst(query.Where, walkerCtx);

        // Phase 2: materialize the AST into ClauseInfo[]. walkerCtx is threaded
        // through the recursive helpers, so deferred steps (e.g. BoostPropagate)
        // can record per-template metadata for later processing.
        walkerCtx.Clauses = [];
        var rootOp = ParseExpression(query.Where, walkerCtx);

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
            
            if(c.IsNegated == false)
                continue;

            if (c.ClauseType == ClauseType.Equals)
                eqBuf[eqCount++] = i;
            
            if(orderByPrimaryField is null || c.FieldName != orderByPrimaryField)
                continue;
            
            if(c.ClauseType is not (
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
            p.Index is not {HasCompoundFields: true} || 
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
        
        if (clauses is not {Count: > 0})
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
                var left = be.Left is BinaryExpression { Operator: OperatorType.Or }  ? 
                    HandleGroup(be.Left, ClauseType.OrGroup) : 
                    ParseExpression(be.Left, walkerCtx);
                
                var right = be.Right is BinaryExpression { Operator: OperatorType.Or }  ? 
                    HandleGroup(be.Right, ClauseType.OrGroup) : 
                    ParseExpression(be.Right, walkerCtx);

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
                var left = be.Left is BinaryExpression { Operator: OperatorType.And } ? 
                    HandleGroup(be.Left, ClauseType.AndGroup) :
                    ParseExpression(be.Left, walkerCtx);

                var right = be.Right is BinaryExpression { Operator: OperatorType.And } ? 
                    HandleGroup(be.Right, ClauseType.AndGroup) :
                    ParseExpression(be.Right, walkerCtx);

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
            BooleanOp expr;
            List<ClauseInfo> clauses;
            using (walkerCtx.SubExpressionScope(out clauses)) 
                expr = ParseExpression(queryExpression, walkerCtx);

            ClauseInfo clauseInfo = new() { ClauseType = clauseType,OriginalIndex = walkerCtx.Clauses.Count };
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
        TryGetFieldName(be.Left, walkerCtx, out string fieldName);
        Debug.Assert(fieldName != null, "PlanWalker.AstShapeValidate must reject comparisons with non-field LHS before materialization");

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
        TryGetFieldName(be.Left, walkerCtx, out string fieldName);
        Debug.Assert(fieldName != null, "PlanWalker.AstShapeValidate must reject range comparisons with non-field LHS before materialization");

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
        TryGetFieldName(between.Source, walkerCtx, out string resolvedFieldName);
        Debug.Assert(resolvedFieldName != null, "PlanWalker.AstShapeValidate must reject BETWEEN with non-field source before materialization");

        var minBinding = CreateBinding(between.Min, walkerCtx);
        var maxBinding = CreateBinding(between.Max, walkerCtx);

        AssertBetweenBindingsCompatibility();

        walkerCtx.Clauses.Add(new ClauseInfo
        {
            FieldName = resolvedFieldName,
            ClauseType = ClauseType.Between,
            OriginalIndex = walkerCtx.Clauses.Count,
            Bindings = [minBinding, maxBinding]
        });

        void AssertBetweenBindingsCompatibility()
        {
            // Literal-type symmetry is also enforced by AstShapeValidate; we only assert
            // here as a defensive backstop. Parameter-typed bindings are validated later
            // in PopulateParameters, when the actual value is known. Sentinel bounds ("*"/"NULL")
            // are allowed with any other type — they're rewritten away by BetweenRewriteSentinels.
            Debug.Assert(
                minBinding is not { LiteralType: not ParamValueType.Parameter }
                || maxBinding is not { LiteralType: not ParamValueType.Parameter }
                || minBinding.LiteralType == maxBinding.LiteralType
                || (minBinding is { LiteralType: ParamValueType.String, LiteralValue: Client.Constants.Documents.Querying.Terms.LeftNullValueOfBetweenQuery })
                || (maxBinding is { LiteralType: ParamValueType.String, LiteralValue: Client.Constants.Documents.Querying.Terms.RightNullValueOfBetweenQuery }),
                "PlanWalker.AstShapeValidate must reject mixed-type BETWEEN literal bounds before materialization");
        }
    }

    private static void ParseIn(InExpression inExpr, ResolutionContext walkerCtx)
    {
        TryGetFieldName(inExpr.Source, walkerCtx, out string resolvedFieldName);
        Debug.Assert(resolvedFieldName != null, "PlanWalker.AstShapeValidate must reject IN with non-field source before materialization");

        // Capture bindings for each IN term. Array parameters expand at PopulateParameters time.
        var inBindings = new List<ParameterBinding>();
        foreach (var value in inExpr.Values)
        {
            if (CreateBinding(value, walkerCtx) is {} binding) 
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
        List<ClauseInfo> innerClauses;
        using (walkerCtx.SubExpressionScope(out innerClauses))
            ParseExpression(negated.Expression, walkerCtx);

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
                Debug.Assert(method.Arguments.Count > 0, "PlanWalker.AstShapeValidate must reject exists() without a field argument before materialization");
                TryGetFieldName(method.Arguments[0], walkerCtx, out var existsFieldName);
                Debug.Assert(existsFieldName != null, "PlanWalker.AstShapeValidate must reject exists() with a non-field argument before materialization");
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
                if(method.Arguments.Count is 1 || 
                   walkerCtx.Clauses.Count  == beforeCount || 
                   CreateBinding(method.Arguments[1], walkerCtx) is not {} boostBinding)
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
                Debug.Assert(method.Arguments.Count >= 2, "PlanWalker.AstShapeValidate must reject regex() with fewer than 2 arguments before materialization");
                TryGetFieldName(method.Arguments[0], walkerCtx, out var regexFieldName);
                Debug.Assert(regexFieldName != null, "PlanWalker.AstShapeValidate must reject regex() with a non-field first argument before materialization");
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
        Debug.Assert(method.Arguments.Count >= 2, "PlanWalker.AstShapeValidate must reject search() with fewer than 2 arguments before materialization");
        TryGetFieldName(method.Arguments[0], walkerCtx, out var fieldName);
        Debug.Assert(fieldName != null, "PlanWalker.AstShapeValidate must reject search() with a non-field first argument before materialization");

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
        Debug.Assert(method.Arguments.Count >= 2, "PlanWalker.AstShapeValidate must reject startsWith/endsWith with fewer than 2 arguments before materialization");
        TryGetFieldName(method.Arguments[0], walkerCtx, out var fieldName);
        Debug.Assert(fieldName != null, "PlanWalker.AstShapeValidate must reject startsWith/endsWith with a non-field first argument before materialization");

        walkerCtx.Clauses.Add(new ClauseInfo
        {
            FieldName = fieldName,
            ClauseType = type,
            OriginalIndex = walkerCtx.Clauses.Count,
            Bindings = [CreateBinding(method.Arguments[1], walkerCtx)]
        });
    }

    // ── Value extraction helpers ─────────────────────────────────────────

    /// <summary>Extract the field name with proper alias resolution using query metadata.</summary>
    private static string GetFieldName(FieldExpression field, QueryMetadata metadata, BlittableJsonReaderObject queryParameters)
    {
        if (metadata != null)
        {
            return metadata.GetIndexFieldName(field, queryParameters).Value;
        }

        return field.FieldValue;
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
        if (expr is MethodExpression me)
        {
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
        }

        if (expr is not ValueExpression ve)
        {
            return null;
        }

        if (ve.Value == ValueTokenType.Parameter)
        {
            return new ParameterBinding { Source = BindingSource.QueryParameter, ParameterName = ve.Token.Value, LiteralType = ParamValueType.Parameter };
        }

        var value = ve.GetValue(ctx.QueryParameters);

        if (ve.Value == ValueTokenType.Null || value is null)
        {
            return new ParameterBinding { Source = BindingSource.Literal, LiteralValue = null, LiteralType = ParamValueType.String };
        }

        if (value is bool b)
        {
            return new ParameterBinding { Source = BindingSource.Literal, LiteralValue = b ? "true" : "false", LiteralType = ParamValueType.String };
        }

        var (resolved, resolvedType) = ResolveParameterValue(value);
        return new ParameterBinding { Source = BindingSource.Literal, LiteralValue = resolved, LiteralType = ToParamValueType(resolvedType) };
    }

    /// <summary>Format a value from the plan's typed arrays as a string for display/highlighting.</summary>
    internal static string FormatValueFromPlan(PackedParam packed, QueryExecution plan) => FormatValueFromPlanInternal(packed, plan, packed.Param1);

    /// <summary>Format the second value (BETWEEN high bound) from the plan's typed arrays.</summary>
    internal static string FormatValue2FromPlan(PackedParam packed, QueryExecution plan) => FormatValueFromPlanInternal(packed, plan, packed.Param2);
    
    private static string FormatValueFromPlanInternal(PackedParam packed, QueryExecution plan, int idx)
    {
        if (idx is PackedParam.NoParamValue)
        {
            return null;
        }

        // An IN clause with all-null terms records InTermCount=0 and writes no values
        // to the typed arrays, but the packed Param1 still points at the (empty) slot.
        // Bounds-check before indexing — return null to indicate "no displayable value".
        return packed.ValueType switch
        {
            PackedParam.TypeLong => idx < plan.LongValues?.Length  ? plan.LongValues[idx].ToString() : null,
            PackedParam.TypeDouble => idx < plan.DoubleValues?.Length ? plan.DoubleValues[idx].ToString(System.Globalization.CultureInfo.InvariantCulture) : null,
            _ => idx < plan.StringValues?.Length ? plan.StringValues[idx] : null
        };
    }

    private static long EstimateCardinality(ClauseInfo clause, ClauseExecution exec, IndexSearcher indexSearcher, ValueWriter writer, ResolutionContext walkerCtx)
    {
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
                // Use field-level cardinality as upper bound
                return indexSearcher.GetTermAmountInField(
                    ResolveFieldMetadata(clause, walkerCtx));

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
                        {
                            subExec.Cardinality = EstimateCardinality(clause.OrSubClauses[si], subExec, indexSearcher, writer, walkerCtx);
                        }

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
                        {
                            subExec.Cardinality = EstimateCardinality(clause.AndSubClauses[si], subExec, indexSearcher, writer, walkerCtx);
                        }

                        if (subExec.Cardinality < andMin)
                        {
                            andMin = subExec.Cardinality;
                        }
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
    /// AND chain: the first clause seeds slot 0 (FillFromPostings), the following clauses narrow it
    /// (AndWithPostings/AndNotWithPostings). IN terms are ORed into slot 1, then ANDed with slot 0.
    ///
    /// OR chain: all terms are ORed into slot 0. AND-groups within an OR use the three-bitmap
    /// swap pattern: save slot 0 → slot 2, build AND result in slot 0, OR slot 2 back.</summary>
    private static QueryExecution EmitPlan(List<ClauseInfo> clauses, long[] cardinalities, ParamValueType[] termTypes, bool isOr, ClauseExecution[] executions)
    {
        // "Empty IN" check — must consult runtime InTermCount when executions are
        // available. Bindings.Length is the *structural* count (one slot per
        // ValueExpression in the IN literal); when a single parameter binding
        // expands to a runtime array, InTermCount is the true element count and
        // can be 0 even though Bindings.Length is 1.
        //
        // HasNullTerm must also block the empty-IN path: a list whose only entry
        // is null arrives as InTermCount=0+HasNullTerm=true and still has to match
        // docs with a null in that field via the null-term posting list (Fill@0
        // reads the null PL, OrRange/AndRange becomes a runtime no-op when
        // InRangeCounts[rangeIdx] resolves to 0).
        static bool IsEmptyIn(ClauseInfo c, ClauseExecution e) =>
            c.ClauseType is ClauseType.In or ClauseType.AllIn &&
            ((e?.InTermCount ?? c.Bindings?.Length ?? 0) == 0) &&
            e?.HasNullTerm != true;

        if (isOr is false)
        {
            // Empty IN clauses: zero results in AND, no-op in OR.
            for (int i = 0; i < clauses.Count; i++)
            {
                if (IsEmptyIn(clauses[i], executions != null && i < executions.Length ? executions[i] : null))
                {
                    return new QueryExecution { Ops = [], IsAllEntries = false };
                }
            }
        }
        else
        {
            int write = 0;
            for (int i = 0; i < clauses.Count; i++)
            {
                var execI = executions != null && i < executions.Length ? executions[i] : null;
                if (IsEmptyIn(clauses[i], execI))
                {
                    continue;
                }

                clauses[write] = clauses[i];
                cardinalities[write] = cardinalities[i];
                termTypes[write] = termTypes[i];
                if (executions != null)
                {
                    executions[write] = executions[i];
                }

                write++;
            }
            if (write < clauses.Count)
            {
                clauses.RemoveRange(write, clauses.Count - write);
                cardinalities = cardinalities[..write];
                termTypes = termTypes[..write];
                if (executions != null)
                {
                    Array.Resize(ref executions, write);
                }
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

                // Negated clause in an OR chain: emit a single QueryMatch slot whose match
                // is materialized at resolution time as AllEntries ANDNOT(positive form).
                // Covers NotEquals, NOT IN, NOT AllIn, NOT exists(), NOT startsWith(), etc.
                // The raw posting list / range / tree-scan can't deliver the complement,
                // so dispatch is forced to QueryMatch and CreateNotEqualsOrMatch produces
                // a pre-materialized BitmapMatch. The IsOrChainNotEquals flag is set at
                // template build time by PlanWalker.NotCanonicalize.
                if (it.IsNegated || it.ClauseType == ClauseType.NotEquals)
                {
                    Debug.Assert(it.IsOrChainNotEquals,
                        "PlanWalker.NotCanonicalize must mark every negated OR-chain clause with IsOrChainNotEquals=true before template freeze.");
                    ops.Add(new PlanOp
                    {
                        Kind = matchIndex == 0 ? PlanOpKind.FillFromPostings : PlanOpKind.OrWithPostings,
                        ParamIndex = matchIndex,
                        EstimatedCardinality = cardinalities[ci],
                        Dispatch = MatchDispatch.QueryMatch
                    });
                    matchIndex++;
                    continue;
                }

                switch (it.ClauseType)
                {
                    case ClauseType.In or ClauseType.AllIn:
                    {
                        // OR chain uses EmitInOps for both In and AllIn (range is OR'd regardless).
                        EmitInOps(ops, it, cardinalities[ci], bitmapLocal: 0, isSeed: matchIndex == 0, ref matchIndex, rangeCounts);
                        break;
                    }
                    case ClauseType.OrGroup when it.OrSubClauses is {Count: > 0}:
                    {
                        int subCount = it.OrSubClauses.Count;
                        for (int si = 0; si < subCount; si++)
                        {
                            var sub = it.OrSubClauses[si];
                            ops.Add(new PlanOp
                            {
                                Kind = matchIndex == 0 ? PlanOpKind.FillFromPostings : PlanOpKind.OrWithPostings,
                                ParamIndex = matchIndex,
                                EstimatedCardinality = cardinalities[ci] / subCount,
                                Dispatch = GetDispatch(sub)
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
                        int subCount = subClauses.Count;
                        long subCardinality = cardinalities[ci] / Math.Max(1, subCount);
                        if (matchIndex == 0)
                        {
                            // First element: build the AND chain directly in slot 0.
                            // Slot 1 is free (unused), so AndWithPostings can use it as scratch.
                            // Suppress early-exit on AND steps — the OR chain continues regardless.
                            ops.Add(new PlanOp
                            {
                                Kind = PlanOpKind.FillFromPostings,
                                ParamIndex = matchIndex,
                                EstimatedCardinality = subCardinality,
                                Dispatch = GetDispatch(subClauses[0])
                            });
                            for (int s = 1; s < subClauses.Count; s++)
                            {
                                ops.Add(new PlanOp
                                {
                                    Kind = subClauses[s].IsNegated ? PlanOpKind.AndNotWithPostings : PlanOpKind.AndWithPostings,
                                    ParamIndex = matchIndex + s,
                                    EstimatedCardinality = subCardinality,
                                    Dispatch = GetDispatch(subClauses[s]),
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
                                EstimatedCardinality = subCardinality,
                                Dispatch = GetDispatch(subClauses[0])
                            });
                            for (int s = 1; s < subClauses.Count; s++)
                            {
                                ops.Add(new PlanOp
                                {
                                    Kind = subClauses[s].IsNegated ? PlanOpKind.AndNotWithPostings : PlanOpKind.AndWithPostings,
                                    ParamIndex = matchIndex + s,
                                    BitmapLocal = 0,
                                    EstimatedCardinality = subCardinality,
                                    Dispatch = GetDispatch(subClauses[s]),
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
                            EstimatedCardinality = cardinalities[ci],
                            Dispatch = GetDispatch(it)
                        });
                        matchIndex++;
                        break;
                    }
                }
            }
            ops.Add(new PlanOp { Kind = PlanOpKind.IterateInto });
        }
        else
        {
            switch (clauses.Count)
            {
                case 1 when clauses[0].ClauseType == ClauseType.Equals && clauses[0].IsNegated is false:
                    ops.Add(new PlanOp
                    {
                        Kind = PlanOpKind.DirectIterate,
                        ParamIndex = 0,
                        EstimatedCardinality = cardinalities[0],
                        Dispatch = GetDispatch(clauses[0])
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
                        EstimatedCardinality = cardinalities[0],
                        Dispatch = GetDispatch(clauses[0])
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
                        Clauses = clauses
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
                        // FillAllEntries doesn't need a slot index — it calls indexSearcher.AllEntries()
                        // directly. This sidesteps the structural-vs-runtime slot-index mismatch that
                        // occurs when an IN clause's runtime InTermCount differs from its template
                        // Bindings.Length (e.g. NOT IN with a parameter that expands to an array).
                        ops.Add(new PlanOp
                        {
                            Kind = PlanOpKind.FillAllEntries,
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
                                int subCount = subClauses.Count;
                                for (int s = 0; s < subCount; s++)
                                {
                                    ops.Add(new PlanOp
                                    {
                                        Kind = s == 0 ? PlanOpKind.FillFromPostings : PlanOpKind.OrWithPostings,
                                        ParamIndex = matchIndex + s,
                                        BitmapLocal = 0,
                                        EstimatedCardinality = cardinalities[0] / Math.Max(1, subCount),
                                        Dispatch = GetDispatch(subClauses[s])
                                    });
                                }
                                matchIndex += subCount;
                                break;
                            }
                            case ClauseType.In:
                            {
                                EmitInOps(ops, clauses[0], cardinalities[0], bitmapLocal: 0, isSeed: true, ref matchIndex, rangeCounts);
                                break;
                            }
                            // Use the fixed-shape EmitAllInOps path for AllIn clauses.
                            case ClauseType.AllIn:
                            {
                                EmitAllInOps(ops, clauses[0], cardinalities[0], ref matchIndex, rangeCounts);
                                break;
                            }
                            default:
                                ops.Add(new PlanOp
                                {
                                    Kind = PlanOpKind.FillFromPostings,
                                    ParamIndex = 0,
                                    EstimatedCardinality = cardinalities[0],
                                    Dispatch = GetDispatch(clauses[0])
                                });
                                matchIndex = 1;
                                break;
                        }
                    }
                    // Precheck: can all remaining clauses be converted to entry scan predicates?
                    bool allScanEligible = AreAllScanEligible(clauses, termTypes, startIndex);

                    for (int i = startIndex; i < clauses.Count; i++)
                    {
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
                                int subCount = subClauses.Count;

                                // Clear bitmap[1] (OR accumulator)
                                ops.Add(new PlanOp { Kind = PlanOpKind.ClearBitmap, BitmapLocal = 1 });

                                // Fill each subclause into bitmap[1]
                                for (int s = 0; s < subCount; s++)
                                {
                                    ops.Add(new PlanOp
                                    {
                                        Kind = PlanOpKind.OrWithPostings,
                                        ParamIndex = matchIndex + s,
                                        BitmapLocal = 1, // target bitmap[1]
                                        EstimatedCardinality = cardinalities[i] / Math.Max(1, subCount),
                                        Dispatch = GetDispatch(subClauses[s])
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

                                matchIndex += subCount;
                                break;
                            }
                            case ClauseType.In:
                            {
                                // OR all IN terms into bitmap[1], then AND (or ANDNOT) with bitmap[0].
                                // isSeed: false — FillFromPostings always targets bitmap[0], so we use
                                // OrRange which respects bitmapLocal. Bitmap[1] is freshly cleared.
                                ops.Add(new PlanOp { Kind = PlanOpKind.ClearBitmap, BitmapLocal = 1 });
                                EmitInOps(ops, clauses[i], cardinalities[i], bitmapLocal: 1, isSeed: false, ref matchIndex, rangeCounts);
                                ops.Add(new PlanOp
                                {
                                    Kind = clauses[i].IsNegated ? PlanOpKind.AndNotBitmaps : PlanOpKind.AndBitmaps,
                                    BitmapLocal = 0,
                                    ParamIndex2 = 1
                                });
                                if (!clauses[i].IsNegated)
                                {
                                    ops.Add(new PlanOp { Kind = PlanOpKind.CheckEmpty, BitmapLocal = 0 });
                                }

                                break;
                            }
                            case ClauseType.AllIn:
                            {
                                int inTermCount = (clauses[i].Bindings?.Length ?? 0);
                                int rangeIdx = rangeCounts.Count;
                                rangeCounts.Add(inTermCount);
                                ops.Add(new PlanOp
                                {
                                    Kind = PlanOpKind.AndRange,
                                    ParamIndex = matchIndex,
                                    ParamIndex2 = rangeIdx,
                                    BitmapLocal = 0,
                                    EstimatedCardinality = cardinalities[i],
                                    Dispatch = MatchDispatch.PostingList
                                });
                                matchIndex += inTermCount;
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
                                    EstimatedCardinality = cardinalities[i],
                                    Dispatch = GetDispatch(clauses[i])
                                });

                                if (!isNegated)
                                {
                                    ops.Add(new PlanOp { Kind = PlanOpKind.CheckEmpty, BitmapLocal = 0 });
                                }

                                matchIndex++;
                                break;
                            }
                        }
                    }

                    ops.Add(new PlanOp { Kind = PlanOpKind.IterateInto });
                    break;
                }
            }
        }

        // Pack operand ordering — encodes clause sort order after cardinality reordering.
        // The plan structure must be the same for all parameter values of the same query text.
        // Variable-count IN terms use InRangeCounts (read at runtime by the IL).
        // Null-term slots are always allocated — the match is a no-op when null isn't present.
        int ordering = 0;
        for (int i = 0; i < Math.Min(clauses.Count, 10); i++)
        {
            ordering |= (clauses[i].OriginalIndex & 0x7) << (i * 3);
        }

        // Check if all clauses are negated (if first clause after sort is negated, all the rest are too)
        bool allNegated = clauses.Count > 0
            && (clauses[0].IsNegated || clauses[0].ClauseType == ClauseType.NotEquals);

        return new QueryExecution
        {
            Ops = ops.ToArray(),
            OperandOrdering = ordering,
            Clauses = clauses,
            AllNegated = allNegated,
            RequiredBitmaps = needsThreeBitmaps ? 3 : 2,
            InRangeCounts = rangeCounts.Count > 0 ? rangeCounts.ToArray() : null
        };
    }

    /// <summary>Compute TypeSignature from scan predicates. Packs 2 bits per predicate (first 16)
    /// into an int. For Slice-typed predicates, checks the resolved string value's UTF-8 byte
    /// count against the compound-index segment limit (255 bytes) to produce
    /// <see cref="ScanValueType.Slice"/> (≤ 255) vs <see cref="ScanValueType.SliceLong"/> (&gt; 255).
    /// This ensures compound-index-eligible string plans don't share a cache entry with
    /// ineligible ones.</summary>
    private static (int TypeSignature, byte[] FullKinds) GetTypeSignature(ScanPredicateInfo[] scanPredicateInfos, string[] stringValues)
    {
        if (scanPredicateInfos == null)
        {
            return (0, null);
        }

        int typeSignature = 0;

        int n = scanPredicateInfos.Length;
        int packCount = Math.Min(n, 16);
        for (int i = 0; i < packCount; i++)
        {
            int kind = (int)ResolveSliceKind(scanPredicateInfos[i], stringValues) & 0x3;
            typeSignature |= kind << (i * 2);
        }

        if (n <= 16)
        {
            return (typeSignature, null);
        }

        var fullKinds = new byte[n];
        for (int i = 0; i < n; i++)
        {
            fullKinds[i] = (byte)ResolveSliceKind(scanPredicateInfos[i], stringValues);
        }

        return (typeSignature, fullKinds);
    }

    /// <summary>For Slice predicates, check if the resolved string exceeds 255 UTF-8 bytes.
    /// Returns SliceLong if so — separating compound-eligible from ineligible in the cache key.
    /// Non-Slice types pass through unchanged.</summary>
    private static ScanValueType ResolveSliceKind(ScanPredicateInfo pred, string[] stringValues)
    {
        if (pred.ValueType != ScanValueType.Slice)
        {
            return pred.ValueType;
        }

        if (stringValues != null && pred.ParamIndex >= 0 && pred.ParamIndex < stringValues.Length)
        {
            string val = stringValues[pred.ParamIndex];
            if (val != null && System.Text.Encoding.UTF8.GetByteCount(val) > byte.MaxValue)
            {
                return ScanValueType.SliceLong;
            }
        }
        return ScanValueType.Slice;
    }

    private static bool AreAllScanEligible(List<ClauseInfo> clauses, ParamValueType[] termTypes, int startIndex)
    {
        // If any clause (In, AllIn, Spatial, Vector, Search, etc.) can't be scanned, we must not emit CheckAndMaybeEntryScan — entry scan would skip them entirely.
        int dummyL = 0, dummyD = 0, dummyS = 0;
        for (int j = startIndex; j < clauses.Count; j++)
        {
            if (BuildScanPredicateInfo(clauses[j], termTypes[j], ref dummyL, ref dummyD, ref dummyS) != null)
            {
                continue;
            }

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
    /// runtime.</summary>
    private static void EmitInOps(List<PlanOp> ops, ClauseInfo clause, long cardinality, int bitmapLocal, bool isSeed, ref int matchIndex, List<int> rangeCounts)
    {
        int totalSlots = (clause.Bindings?.Length ?? 0) + 1;
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
            EstimatedCardinality = Math.Max(1, cardinality / totalSlots),
            Dispatch = MatchDispatch.PostingList
        });
        ops.Add(new PlanOp
        {
            Kind = PlanOpKind.OrRange,
            ParamIndex = matchIndex + 1,
            ParamIndex2 = rangeIdx,
            BitmapLocal = bitmapLocal,
            EstimatedCardinality = cardinality,
            Dispatch = MatchDispatch.PostingList
        });
        matchIndex += totalSlots;
    }

    /// <summary>Emit ops for an AllIn clause (as a seed): Fill slot 0 + AndRange for the rest.
    /// Same fixed shape rationale as <see cref="EmitInOps"/> — the count of remaining
    /// terms lives in <c>ctx.InRangeCounts</c> rather than the op shape itself.</summary>
    private static void EmitAllInOps(List<PlanOp> ops, ClauseInfo clause, long cardinality, ref int matchIndex, List<int> rangeCounts)
    {
        int totalSlots = (clause.Bindings?.Length ?? 0) + 1;
        // For AllIn, ANDing with an Empty PostingSource clears the bitmap — so the
        // null-term slot is always included in the range. At runtime, when no null term
        // is present the slot holds an empty match (AND with empty = clear), which is
        // correct for AllIn semantics. The range count covers all slots after slot 0.
        int rangeCount = totalSlots - 1;
        int rangeIdx = rangeCounts.Count;
        rangeCounts.Add(rangeCount);

        ops.Add(new PlanOp
        {
            Kind = PlanOpKind.FillFromPostings,
            ParamIndex = matchIndex,
            BitmapLocal = 0,
            EstimatedCardinality = Math.Max(1, cardinality / totalSlots),
            Dispatch = MatchDispatch.PostingList
        });
        ops.Add(new PlanOp
        {
            Kind = PlanOpKind.AndRange,
            ParamIndex = matchIndex + 1,
            ParamIndex2 = rangeIdx,
            BitmapLocal = 0,
            EstimatedCardinality = cardinality,
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
    private static void AttachPostFilterPhases(QueryExecution plan, ClauseInfo[] spatialClauses, ClauseExecution[] spatialExecs,
        ClauseInfo[] vectorClauses, ClauseExecution[] vectorExecs)
    {
        if (spatialClauses == null && vectorClauses == null)
        {
            return;
        }

        var clauses = plan.Clauses ??= [];

        // Extend Executions array to match the clauses that will be added
        int extraCount = (spatialClauses?.Length ?? 0) + (vectorClauses?.Length ?? 0);
        var execs = plan.Executions ??= [];
        int execIdx = execs.Length;
        Array.Resize(ref execs, execs.Length + extraCount);

        int matchIndex = CountMatchSlots(clauses, execs, plan.IsAllEntries, plan.AllNegated);

        if (spatialClauses != null)
        {
            plan.SpatialFilters = new SpatialFilterOp[spatialClauses.Length];
            for (int i = 0; i < spatialClauses.Length; i++)
            {
                clauses.Add(spatialClauses[i]);
                var exec = spatialExecs?[i] ?? new ClauseExecution();
                execs[execIdx++] = exec;
                plan.SpatialFilters[i] = new SpatialFilterOp { MatchIndex = matchIndex++, Clause = spatialClauses[i], Exec = exec };
            }
        }

        if (vectorClauses != null)
        {
            plan.VectorSelects = new VectorSearchOp[vectorClauses.Length];
            for (int i = 0; i < vectorClauses.Length; i++)
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
    internal static int CountMatchSlots(List<ClauseInfo> clauses, bool isAllEntries, bool allNegated)
    {
        if (clauses == null)
            return isAllEntries ? 1 : 0;

        int count = isAllEntries ? 1 : 0;
        foreach (var clause in clauses)
        {
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
                ClauseType.In or ClauseType.AllIn => (clause.Bindings?.Length ?? 0) + 1,
                _ => 1
            };
        }
        if (allNegated)
        {
            count++;
        }

        return count;
    }

    /// <summary>Resolution-time overload: uses <paramref name="executions"/> for IN/AllIn
    /// term counts (which may differ from template-time Bindings.Length when a single
    /// parameter binding expands to an array at execution time).</summary>
    internal static int CountMatchSlots(List<ClauseInfo> clauses, ClauseExecution[] executions, bool isAllEntries, bool allNegated)
    {
        if (clauses == null)
        {
            return isAllEntries ? 1 : 0;
        }

        int count = isAllEntries ? 1 : 0;
        for (int ci = 0; ci < clauses.Count; ci++)
        {
            var clause = clauses[ci];
            var exec = executions != null && ci < executions.Length ? executions[ci] : null;
            if (clause.IsOrChainNotEquals)
            {
                count += 1;
                continue;
            }
            count += clause.ClauseType switch
            {
                ClauseType.OrGroup when clause.OrSubClauses != null => clause.OrSubClauses.Count,
                ClauseType.AndGroup when clause.AndSubClauses != null => clause.AndSubClauses.Count,
                ClauseType.In or ClauseType.AllIn => (exec?.InTermCount ?? clause.Bindings?.Length ?? 0) + 1,
                _ => 1
            };
        }
        if (allNegated)
        {
            count++;
        }

        return count;
    }

    /// <summary>For an OrGroup or AndGroup clause, returns the parallel (sub-clauses, sub-executions)
    /// arrays that callers iterate to fan out one match slot per sub-term. Returns false for any
    /// other clause type, or for empty groups. <paramref name="subExecs"/> is null when
    /// <paramref name="exec"/> is null (TermsProviders path tolerates that).</summary>
    internal static bool TryGetGroupFanOut(ClauseInfo clause, ClauseExecution exec,
        out List<ClauseInfo> subClauses, out ClauseExecution[] subExecs)
    {
        if (clause.ClauseType == ClauseType.OrGroup && clause.OrSubClauses is { Count: > 0 })
        {
            subClauses = clause.OrSubClauses;
            subExecs = exec?.OrSubExecutions;
            return true;
        }
        if (clause.ClauseType == ClauseType.AndGroup && clause.AndSubClauses is { Count: > 0 })
        {
            subClauses = clause.AndSubClauses;
            subExecs = exec?.AndSubExecutions;
            return true;
        }
        subClauses = null;
        subExecs = null;
        return false;
    }

    // ── Dispatch classification ──────────────────────────────────────────

    /// <summary>Decide whether a clause type can be expressed as a single
    /// <see cref="PostingSource"/>. Boosted clauses go through the IQueryMatch path
    /// even when they're term-shaped, so scoring still works.</summary>
    internal static bool IsTermSourceEligibleClause(ClauseInfo clause)
    {
        if (clause == null)
        {
            return false;
        }

        if (clause.HasBoost)
        {
            return false;
        }

        return clause.ClauseType is ClauseType.Equals or ClauseType.NotEquals;
    }

    /// <summary>TreeScan-eligible: multi-term clauses that have a direct ITermsProvider
    /// (StartsWith, EndsWith, Exists, Regex, ranges). Boosted clauses go through QueryMatch
    /// for scoring. Contains is excluded because its tree walk pattern doesn't benefit
    /// from the direct dispatch (it walks the full tree regardless).</summary>
    internal static bool IsTreeScanEligibleClause(ClauseInfo clause)
    {
        if (clause == null)
        {
            return false;
        }

        if (clause.HasBoost)
        {
            return false;
        }

        // Parameter-bound BETWEEN sentinels use QueryMatch dispatch, not TreeScan.
        if (clause.ClauseType == ClauseType.Between)
        {
            foreach (var t in clause.Bindings)
            {
                if (t is { Source: BindingSource.QueryParameter })
                    return false;
            }
        }
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
    private static MatchDispatch GetDispatch(ClauseInfo clause)
    {
        if (IsTermSourceEligibleClause(clause))
        {
            return MatchDispatch.PostingList;
        }

        if (IsTreeScanEligibleClause(clause))
        {
            return MatchDispatch.TreeScan;
        }

        return MatchDispatch.QueryMatch;
    }

    // ── Entry scan predicate building ────────────────────────────────────

    /// <summary>Derive the term value type from a clause's binding. Used for sub-clauses
    /// in OrGroup/AndGroup where the caller doesn't have per-subclause term types.</summary>
    private static ParamValueType InferTermType(ClauseInfo clause)
    {
        var bindings = clause.Bindings;
        if (bindings == null || bindings.Length == 0)
        {
            return ParamValueType.String;
        }

        var lit = bindings[0].LiteralType;
        return lit == ParamValueType.Parameter ? ParamValueType.String : lit;
    }

    /// <summary>Template-time overload: caller supplies <paramref name="termType"/> directly.
    /// Group-clause recursion falls back to <see cref="InferTermType"/> per sub-clause since
    /// no ClauseExecution is available. Used by <see cref="AreAllScanEligible"/> at plan time.</summary>
    private static ScanPredicateInfo? BuildScanPredicateInfo(ClauseInfo clause, ParamValueType termType,
        ref int longIndex, ref int doubleIndex, ref int sliceIndex)
        => BuildScanPredicateInfoCore(clause, exec: null, termType,
            ref longIndex, ref doubleIndex, ref sliceIndex);

    /// <summary>Resolution-time overload: derives term type from <paramref name="exec"/>
    /// and recurses into subclauses using sub-execution types. Used when actual resolved
    /// types are available (per-execution, after PopulateClauseValues).</summary>
    internal static ScanPredicateInfo? BuildScanPredicateInfo(ClauseInfo clause, ClauseExecution exec,
        ref int longIndex, ref int doubleIndex, ref int sliceIndex)
        => BuildScanPredicateInfoCore(clause, exec, exec?.TermValueType ?? ParamValueType.String,
            ref longIndex, ref doubleIndex, ref sliceIndex);

    /// <summary>Single walker shared by both overloads. <paramref name="exec"/> is non-null on
    /// the resolution path and supplies per-sub TermValueType during group recursion; on the
    /// template path it is null and recursion falls back to InferTermType.</summary>
    private static ScanPredicateInfo? BuildScanPredicateInfoCore(ClauseInfo clause, ClauseExecution exec,
        ParamValueType termType, ref int longIndex, ref int doubleIndex, ref int sliceIndex)
    {
        switch (clause.ClauseType)
        {
            case ClauseType.Search:
            case ClauseType.Regex:
            case ClauseType.Spatial:
            case ClauseType.Vector:
            case ClauseType.StartsWith:
            {
                if (termType != ParamValueType.String)
                {
                    return null;
                }

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
                if (termType != ParamValueType.String)
                {
                    return null;
                }

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
                if (clause.AndSubClauses is not { Count: > 0 } subs)
                {
                    return null;
                }

                var subExecs = exec?.AndSubExecutions;
                var branches = new List<ScanPredicateInfo>();
                for (int si = 0; si < subs.Count; si++)
                {
                    var sub = subs[si];
                    var subExec = subExecs != null && si < subExecs.Length ? subExecs[si] : null;
                    var subTermType = subExec?.TermValueType ?? InferTermType(sub);
                    var subPred = BuildScanPredicateInfoCore(sub, subExec, subTermType,
                        ref longIndex, ref doubleIndex, ref sliceIndex);
                    if (subPred == null)
                    {
                        return null;
                    }

                    branches.Add(subPred.Value);
                }
                return new ScanPredicateInfo
                {
                    FieldName = clause.FieldName ?? subs[0].FieldName,
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
                if (clause.OrSubClauses is not { Count: > 0 } subs)
                {
                    return null;
                }

                var subExecs = exec?.OrSubExecutions;
                var branches = new List<ScanPredicateInfo>();
                int li = longIndex, di = doubleIndex, slc = sliceIndex;
                for (int si = 0; si < subs.Count; si++)
                {
                    var sub = subs[si];
                    var subExec = subExecs != null && si < subExecs.Length ? subExecs[si] : null;
                    var subTermType = subExec?.TermValueType ?? InferTermType(sub);
                    var subPred = BuildScanPredicateInfoCore(sub, subExec, subTermType,
                        ref li, ref di, ref slc);
                    if (subPred == null)
                    {
                        return null; // Any complex subclause → can't entry-scan the whole group
                    }

                    branches.Add(subPred.Value);
                }
                longIndex = li; doubleIndex = di; sliceIndex = slc;
                return new ScanPredicateInfo
                {
                    FieldName = subs[0].FieldName,
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

        // Strong typing: termType is set by GetTermValue from the parser's literal
        // type (for inline values) or the resolved JSON-blittable runtime type (for params).
        // Switch on it directly — no string round-trip / TryParse fallback.
        ScanValueType valueType;
        switch (termType)
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
