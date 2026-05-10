using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Corax.Querying.Planning;
using Corax.Mappings;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Sparrow.Json;
using Sparrow.Server;
using Constants = Corax.Constants;
using IndexSearcher = Corax.Querying.IndexSearcher;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

/// <summary>
/// Builds a QueryPlan from a parsed RQL query.
///
/// The planner has three independent concerns, each in its own partial file:
///
///   QueryPlanBuilder.cs (this file) — structural
///     Data model (ClauseType, ClauseInfo, PlanParameters), RQL AST parsing,
///     cardinality estimation, plan emission (EmitPlan), dispatch classification.
///     Output: a QueryPlan with PlanOp[] — cacheable, query-text-keyed.
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
    // ── Data model ───────────────────────────────────────────────────────

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

    // ── Packed parameter encoding ──────────────────────────────────────

    /// <summary>
    /// Packed parameter reference encoding.
    ///
    /// Each clause value is stored in a typed array (long[], double[], string[])
    /// on the QueryPlan and referenced by a 32-bit packed integer:
    ///   bits [31:30] = value type (Long=0, Double=1, String=2, None=3)
    ///   bits [29:15] = first parameter index (0..32767)
    ///   bits [14:0]  = second parameter index (0..32767, 0x7FFF = no second param)
    ///
    /// For BETWEEN: param1 = low bound index, param2 = high bound index (same typed array).
    /// For simple predicates: param1 = value index, param2 = NoParam.
    /// For parameterless clauses (Exists, Spatial, Vector): type = None.
    ///
    /// Maximum 32,767 parameters of each type per query.
    /// </summary>
    internal static class PackedParam
    {
        public const int NoParam = 0x7FFF;
        public const int MaxIndex = 0x7FFF; // 32767

        public const int TypeLong = 0;
        public const int TypeDouble = 1;
        public const int TypeString = 2;
        public const int TypeNone = 3;

        /// <summary>Sentinel: no parameter (Exists, Spatial, Vector clauses).</summary>
        public const int None = (TypeNone << 30) | (NoParam << 15) | NoParam;

        public static int Encode(int type, int param1, int param2 = NoParam)
            => (type << 30) | ((param1 & 0x7FFF) << 15) | (param2 & 0x7FFF);

        public static int GetValueType(int packed) => (packed >>> 30) & 0x3;
        public static int GetParam1(int packed) => (packed >>> 15) & 0x7FFF;
        public static int GetParam2(int packed) => packed & 0x7FFF;
    }

    /// <summary>
    /// Accumulates typed parameter values during query plan building.
    /// Each Add call stores the value in the appropriate typed list and
    /// returns a packed int encoding (type + index) for the clause.
    /// </summary>
    private sealed class ValueWriter
    {
        private readonly List<long> _longs = new();
        private readonly List<double> _doubles = new();
        private readonly List<string> _strings = new();

        public int Add(string value, ValueTokenType type)
        {
            switch (type)
            {
                case ValueTokenType.Long when long.TryParse(value, out long l):
                    CheckLimit(_longs.Count);
                    _longs.Add(l);
                    return PackedParam.Encode(PackedParam.TypeLong, _longs.Count - 1);
                case ValueTokenType.Double when double.TryParse(value,
                    System.Globalization.NumberStyles.Float,
                    System.Globalization.CultureInfo.InvariantCulture, out double d):
                    CheckLimit(_doubles.Count);
                    _doubles.Add(d);
                    return PackedParam.Encode(PackedParam.TypeDouble, _doubles.Count - 1);
                default:
                    CheckLimit(_strings.Count);
                    _strings.Add(value);
                    return PackedParam.Encode(PackedParam.TypeString, _strings.Count - 1);
            }
        }

        public int AddPair(string value1, string value2, ValueTokenType type)
        {
            switch (type)
            {
                case ValueTokenType.Long when long.TryParse(value1, out long l1) && long.TryParse(value2, out long l2):
                    CheckLimit(_longs.Count + 1);
                    _longs.Add(l1);
                    _longs.Add(l2);
                    return PackedParam.Encode(PackedParam.TypeLong, _longs.Count - 2, _longs.Count - 1);
                case ValueTokenType.Double when double.TryParse(value1,
                    System.Globalization.NumberStyles.Float,
                    System.Globalization.CultureInfo.InvariantCulture, out double d1)
                    && double.TryParse(value2,
                    System.Globalization.NumberStyles.Float,
                    System.Globalization.CultureInfo.InvariantCulture, out double d2):
                    CheckLimit(_doubles.Count + 1);
                    _doubles.Add(d1);
                    _doubles.Add(d2);
                    return PackedParam.Encode(PackedParam.TypeDouble, _doubles.Count - 2, _doubles.Count - 1);
                default:
                    CheckLimit(_strings.Count + 1);
                    _strings.Add(value1);
                    _strings.Add(value2);
                    return PackedParam.Encode(PackedParam.TypeString, _strings.Count - 2, _strings.Count - 1);
            }
        }

        public long[] GetLongs() => _longs.Count > 0 ? _longs.ToArray() : [];
        public double[] GetDoubles() => _doubles.Count > 0 ? _doubles.ToArray() : [];
        public string[] GetStrings() => _strings.Count > 0 ? _strings.ToArray() : [];

        private static void CheckLimit(int currentCount)
        {
            if (currentCount >= PackedParam.MaxIndex)
                throw new InvalidOperationException(
                    $"Query exceeds maximum parameter count ({PackedParam.MaxIndex}). " +
                    "Simplify the query or reduce the number of IN terms.");
        }
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
        OrGroup,  // A group of OR'd sub-clauses
        AndGroup, // A group of AND'd sub-clauses inside an OR chain
        EmptyIn,  // IN() with empty list — matches nothing
    }

    /// <summary>
    /// Intermediate representation of a single WHERE predicate, between the RQL AST
    /// and the PlanOp[] execution plan.
    ///
    /// Why not reuse the AST directly?
    /// - The AST is a recursive tree (AND(AND(A,B),C)); ClauseInfo is a flat list suitable for
    ///   plan emission. Mixed AND/OR trees are flattened into OrGroup/AndGroup sub-lists.
    /// - Field names are resolved (alias substitution, id() expansion, quoted-name handling).
    /// - Parameter values are resolved from the blittable and stored as native types in the
    ///   plan's typed arrays (LongValues, DoubleValues, StringValues). PackedParam encodes
    ///   (type, index) so resolution never re-parses strings.
    /// - Clause type is classified into a flat enum — downstream code switches on one value
    ///   instead of pattern-matching AST node types and method names.
    /// - Planning annotations (Cardinality, IsExact, BoostFactor, IsNegated) are attached per
    ///   clause for operand reordering, dispatch classification, and entry-scan eligibility.
    /// </summary>
    internal class ClauseInfo
    {
        public string FieldName;
        public string TermValue;          // string form — used for display, highlighting, cardinality estimation
        public ValueTokenType TermValueType; // for type-aware scan predicate building
        public string TermValue2;         // for BETWEEN (string form)
        public List<string> InTerms;      // for IN (string forms)
        public List<ValueTokenType> InTermTypes; // parallel to InTerms — literal type per term

        /// <summary>Packed (type, index) into QueryPlan.LongValues/DoubleValues/StringValues.
        /// For BETWEEN: encodes both low and high indices. See <see cref="PackedParam"/>.</summary>
        public int PackedParamValue = PackedParam.None;

        /// <summary>Per-term packed params for IN/AllIn. Parallel to <see cref="InTerms"/>.
        /// Each entry encodes (type, index) for one IN term.</summary>
        public int[] InPackedParams;

        public List<ClauseInfo> OrSubClauses;  // for OrGroup
        public bool IsOrChainNotEquals; // Set for NotEquals in OR chains; ResolveMatches creates AllEntries ANDNOT TermQuery
        public List<ClauseInfo> AndSubClauses; // for AndGroup (AND sub-expression inside OR)
        public MethodExpression MethodExpression; // for Spatial, Vector
        public ClauseType ClauseType;
        public long Cardinality = -1;
        public int OriginalIndex;
        public bool IsNegated;
        public bool IsExact;
        public float BoostFactor;

        public Constants.Search.Operator SearchOperator; // for Search (AND/OR)
    }

    private enum BooleanOp { And, Or, True, False, Leaf }

    // ── Entry point ──────────────────────────────────────────────────────

    public static QueryPlan BuildPlan(PlanParameters p)
    {
        var query = p.Metadata.Query;
        var indexSearcher = p.IndexSearcher;
        var queryParameters = p.QueryParameters;
        var metadata = p.Metadata;
        if (query.Where == null)
            return BuildAllEntriesPlan();

        bool hasMixedAndOr = false;
        var clauses = new List<ClauseInfo>();
        var writer = new ValueWriter();
        var rootOp = ParseExpression(query.Where, indexSearcher, clauses, queryParameters, metadata, ref hasMixedAndOr, writer);

        // Mixed AND/OR trees are handled via OrGroup clauses

        if (rootOp == BooleanOp.True)
            return BuildAllEntriesPlan();
        if (rootOp == BooleanOp.False)
            return BuildEmptyPlan();
        if (clauses.Count == 0)
            return BuildAllEntriesPlan();

        foreach (var clause in clauses)
        {
            if (clause.Cardinality < 0)
                clause.Cardinality = EstimateCardinality(clause, indexSearcher);
        }

        bool isOr = rootOp == BooleanOp.Or;

        // Separate spatial and vector clauses from the filter chain.
        // For AND queries, spatial/vector execute AFTER the bitmap filter:
        //   FilterOps -> SpatialFilters -> VectorSelects
        // For OR queries, spatial/vector remain in the flat chain (they produce
        // candidate sets that are OR'd together).
        List<ClauseInfo> spatialClauses = null;
        List<ClauseInfo> vectorClauses = null;
        if (!isOr)
        {
            for (int i = clauses.Count - 1; i >= 0; i--)
            {
                if (clauses[i].ClauseType == ClauseType.Spatial)
                {
                    spatialClauses ??= [];
                    spatialClauses.Add(clauses[i]);
                    clauses.RemoveAt(i);
                }
                else if (clauses[i].ClauseType == ClauseType.Vector)
                {
                    vectorClauses ??= [];
                    vectorClauses.Add(clauses[i]);
                    clauses.RemoveAt(i);
                }
            }

            if (clauses.Count == 0)
            {
                // No filter clauses remain — only spatial and/or vector.
                // Use AllEntries as the base and attach post-filter phases.
                // Vector clauses must NOT be put back into the flat plan because:
                // (a) the bitmap pipeline would lose distance ordering (bitmaps
                //     iterate in entry-ID order, not distance order), and
                // (b) ResolveClause would fail on vector clauses that have no FieldName.
                if (spatialClauses != null || vectorClauses != null)
                {
                    var plan = BuildAllEntriesPlan();
                    AttachPostFilterPhases(plan, spatialClauses, vectorClauses);
                    return plan;
                }
            }
        }

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
        else
        {
            // For OR chains: move AndGroups to the front so the AND sub-chain always
            // seeds slot 0 directly (no third bitmap slot needed for scratch).
            // OR is commutative — reordering is safe.
            int insertPos = 0;
            for (int j = 0; j < clauses.Count; j++)
            {
                if (clauses[j].ClauseType == ClauseType.AndGroup)
                {
                    ClauseInfo ag = clauses[j];
                    clauses.RemoveAt(j);
                    clauses.Insert(insertPos++, ag);
                }
            }
        }

        var result = EmitPlan(clauses, isOr);
        result.LongValues = writer.GetLongs();
        result.DoubleValues = writer.GetDoubles();
        result.StringValues = writer.GetStrings();

        if (spatialClauses != null || vectorClauses != null)
            AttachPostFilterPhases(result, spatialClauses, vectorClauses);

        return result;
    }

    // ── Parsing: RQL AST → flat clause list ──────────────────────────────

    private static BooleanOp ParseExpression(
        QueryExpression expr,
        IndexSearcher indexSearcher,
        List<ClauseInfo> clauses,
        BlittableJsonReaderObject queryParameters,
        QueryMetadata metadata,
        ref bool hasMixedAndOr,
        ValueWriter writer)
    {
        switch (expr)
        {
            case BinaryExpression be:
                return ParseBinaryExpression(be, indexSearcher, clauses, queryParameters, metadata, ref hasMixedAndOr, writer);

            case BetweenExpression between:
                ParseBetween(between, clauses, queryParameters, metadata, writer);
                return BooleanOp.Leaf;

            case InExpression inExpr:
                ParseIn(inExpr, clauses, queryParameters, metadata, writer);
                return BooleanOp.Leaf;

            case NegatedExpression negated:
                ParseNegated(negated, indexSearcher, clauses, queryParameters, metadata, ref hasMixedAndOr, writer);
                return BooleanOp.Leaf;

            case TrueExpression:
                return BooleanOp.True;

            case MethodExpression method:
                ParseMethod(method, indexSearcher, clauses, queryParameters, metadata, ref hasMixedAndOr, writer);
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
        ref bool hasMixedAndOr,
        ValueWriter writer)
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
                    left = ParseExpression(be.Left, indexSearcher, orClauses, queryParameters, metadata, ref hasMixedAndOr, writer);
                    clauses.Add(new ClauseInfo
                    {
                        ClauseType = ClauseType.OrGroup,
                        OrSubClauses = orClauses,
                        OriginalIndex = clauses.Count
                    });
                }
                else
                {
                    left = ParseExpression(be.Left, indexSearcher, clauses, queryParameters, metadata, ref hasMixedAndOr, writer);
                }

                if (be.Right is BinaryExpression { Operator: OperatorType.Or })
                {
                    var orClauses = new List<ClauseInfo>();
                    right = ParseExpression(be.Right, indexSearcher, orClauses, queryParameters, metadata, ref hasMixedAndOr, writer);
                    clauses.Add(new ClauseInfo
                    {
                        ClauseType = ClauseType.OrGroup,
                        OrSubClauses = orClauses,
                        OriginalIndex = clauses.Count
                    });
                }
                else
                {
                    right = ParseExpression(be.Right, indexSearcher, clauses, queryParameters, metadata, ref hasMixedAndOr, writer);
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
                    left = ParseExpression(be.Left, indexSearcher, andClauses, queryParameters, metadata, ref hasMixedAndOr, writer);
                    clauses.Add(new ClauseInfo
                    {
                        ClauseType = ClauseType.AndGroup,
                        AndSubClauses = andClauses,
                        OriginalIndex = clauses.Count
                    });
                }
                else
                {
                    left = ParseExpression(be.Left, indexSearcher, clauses, queryParameters, metadata, ref hasMixedAndOr, writer);
                }

                if (be.Right is BinaryExpression { Operator: OperatorType.And })
                {
                    var andClauses = new List<ClauseInfo>();
                    right = ParseExpression(be.Right, indexSearcher, andClauses, queryParameters, metadata, ref hasMixedAndOr, writer);
                    clauses.Add(new ClauseInfo
                    {
                        ClauseType = ClauseType.AndGroup,
                        AndSubClauses = andClauses,
                        OriginalIndex = clauses.Count
                    });
                }
                else
                {
                    right = ParseExpression(be.Right, indexSearcher, clauses, queryParameters, metadata, ref hasMixedAndOr, writer);
                }

                if (left == BooleanOp.True || right == BooleanOp.True) return BooleanOp.True;
                if (left == BooleanOp.False) return right;
                if (right == BooleanOp.False) return left;
                return BooleanOp.Or;
            }

            case OperatorType.Equal:
                ParseComparison(be, clauses, queryParameters, metadata, writer);
                return BooleanOp.Leaf;

            case OperatorType.NotEqual:
                ParseComparison(be, clauses, queryParameters, metadata, writer);
                return BooleanOp.Leaf;

            case OperatorType.LessThan:
            case OperatorType.LessThanEqual:
            case OperatorType.GreaterThan:
            case OperatorType.GreaterThanEqual:
                ParseRangeComparison(be, clauses, queryParameters, metadata, writer);
                return BooleanOp.Leaf;

            default:
                throw new InvalidOperationException(
                    $"Unexpected binary operator {be.Operator} in WHERE clause.");
        }
    }

    private static void ParseComparison(BinaryExpression be, List<ClauseInfo> clauses,
        BlittableJsonReaderObject queryParameters, QueryMetadata metadata, ValueWriter writer)
    {
        if (TryGetFieldName(be.Left, metadata, queryParameters, out string fieldName) == false)
            throw new InvalidQueryException($"Comparison left side must be a field expression or id(), but got: {be.Left.Type}");
        string termValue = GetTermValue(be.Right, queryParameters, out var valueType);

        clauses.Add(new ClauseInfo
        {
            FieldName = fieldName,
            TermValue = termValue,
            TermValueType = valueType,
            PackedParamValue = writer.Add(termValue, valueType),
            ClauseType = be.Operator == OperatorType.NotEqual ? ClauseType.NotEquals : ClauseType.Equals,
            OriginalIndex = clauses.Count
        });
    }

    private static void ParseRangeComparison(BinaryExpression be, List<ClauseInfo> clauses,
        BlittableJsonReaderObject queryParameters, QueryMetadata metadata, ValueWriter writer)
    {
        if (TryGetFieldName(be.Left, metadata, queryParameters, out string fieldName) == false)
            throw new InvalidQueryException($"Range comparison left side must be a field expression or id(), but got: {be.Left.Type}");
        string termValue = GetTermValue(be.Right, queryParameters, out var valueType);

        clauses.Add(new ClauseInfo
        {
            FieldName = fieldName,
            TermValue = termValue,
            TermValueType = valueType,
            PackedParamValue = writer.Add(termValue, valueType),
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
        BlittableJsonReaderObject queryParameters, QueryMetadata metadata, ValueWriter writer)
    {
        if (TryGetFieldName(between.Source, metadata, queryParameters, out string resolvedFieldName) == false)
            throw new InvalidQueryException($"BETWEEN source must be a field expression or id(), but got: {between.Source.Type}");

        var minValue = GetTermValue(between.Min, queryParameters, out var minType);
        var maxValue = GetTermValue(between.Max, queryParameters, out _);
        clauses.Add(new ClauseInfo
        {
            FieldName = resolvedFieldName,
            TermValue = minValue,
            TermValue2 = maxValue,
            TermValueType = minType,
            PackedParamValue = writer.AddPair(minValue, maxValue, minType),
            ClauseType = ClauseType.Between,
            OriginalIndex = clauses.Count
        });
    }

    private static void ParseIn(InExpression inExpr, List<ClauseInfo> clauses,
        BlittableJsonReaderObject queryParameters, QueryMetadata metadata, ValueWriter writer)
    {
        if (TryGetFieldName(inExpr.Source, metadata, queryParameters, out string resolvedFieldName) == false)
            throw new InvalidQueryException($"IN source must be a field expression or id(), but got: {inExpr.Source.Type}");

        var terms = new List<string>();
        var termTypes = new List<ValueTokenType>();
        bool hasTime = false;
        foreach (var value in inExpr.Values)
        {
            if (value is ValueExpression ve)
            {
                var resolvedValue = ve.GetValue(queryParameters);
                if (resolvedValue is BlittableJsonReaderArray arr)
                {
                    foreach (var it in arr)
                    {
                        var (elemVal, elemTyp) = ConvertInValue(it, ValueTokenType.Parameter, ref hasTime);
                        terms.Add(elemVal);
                        termTypes.Add(elemTyp);
                    }
                }
                else
                {
                    var (resVal, resTyp) = ConvertInValue(resolvedValue, ve.Value, ref hasTime);
                    terms.Add(resVal);
                    termTypes.Add(resTyp);
                }
            }
        }

        if (terms.Count == 0)
        {
            clauses.Add(new ClauseInfo
            {
                FieldName = resolvedFieldName,
                ClauseType = ClauseType.EmptyIn,
                OriginalIndex = clauses.Count
            });
            return;
        }

        var inPacked = new int[terms.Count];
        for (int i = 0; i < terms.Count; i++)
            inPacked[i] = writer.Add(terms[i], termTypes[i]);

        clauses.Add(new ClauseInfo
        {
            FieldName = resolvedFieldName,
            InTerms = terms,
            InTermTypes = termTypes,
            InPackedParams = inPacked,
            ClauseType = inExpr.All ? ClauseType.AllIn : ClauseType.In,
            OriginalIndex = clauses.Count
        });
    }

    private static void ParseNegated(NegatedExpression negated, IndexSearcher indexSearcher,
        List<ClauseInfo> clauses, BlittableJsonReaderObject queryParameters, QueryMetadata metadata, ref bool hasMixedAndOr, ValueWriter writer)
    {
        var innerClauses = new List<ClauseInfo>();
        ParseExpression(negated.Expression, indexSearcher, innerClauses, queryParameters, metadata, ref hasMixedAndOr, writer);

        foreach (var inner in innerClauses)
        {
            inner.IsNegated = true;
            clauses.Add(inner);
        }
    }

    private static void ParseMethod(MethodExpression method, IndexSearcher indexSearcher,
        List<ClauseInfo> clauses, BlittableJsonReaderObject queryParameters, QueryMetadata metadata, ref bool hasMixedAndOr, ValueWriter writer)
    {
        var methodType = QueryMethod.GetMethodType(method.Name.Value);
        switch (methodType)
        {
            case MethodType.Search:
                ParseSearchMethod(method, clauses, queryParameters, metadata, writer);
                break;

            case MethodType.StartsWith:
                ParsePrefixMethod(method, clauses, queryParameters, metadata, ClauseType.StartsWith, writer);
                break;

            case MethodType.EndsWith:
                ParsePrefixMethod(method, clauses, queryParameters, metadata, ClauseType.EndsWith, writer);
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
                    ParseExpression(method.Arguments[0], indexSearcher, clauses, queryParameters, metadata, ref hasMixedAndOr, writer);
                for (int c = beforeCount; c < clauses.Count; c++)
                    clauses[c].IsExact = true;
                break;
            }

            case MethodType.Boost:
            {
                // boost(expr, factor) → recurse, then set boost factor on new clauses
                int beforeCount = clauses.Count;
                if (method.Arguments.Count > 0)
                    ParseExpression(method.Arguments[0], indexSearcher, clauses, queryParameters, metadata, ref hasMixedAndOr, writer);
                float boostFactor = 1f;
                if (method.Arguments.Count > 1)
                {
                    var factorStr = GetTermValue(method.Arguments[1], queryParameters);
                    if (factorStr != null)
                        float.TryParse(factorStr, System.Globalization.NumberStyles.Float,
                            System.Globalization.CultureInfo.InvariantCulture, out boostFactor);
                }
                for (int c = beforeCount; c < clauses.Count; c++)
                {
                    clauses[c].BoostFactor = boostFactor;
                }
                break;
            }

            case MethodType.Regex:
            {
                if (method.Arguments.Count < 2)
                    throw new InvalidQueryException($"regex() requires at least 2 arguments (field, pattern), but got {method.Arguments.Count}.");
                if (TryGetFieldName(method.Arguments[0], metadata, queryParameters, out var regexFieldName) == false)
                    throw new InvalidQueryException($"regex() first argument must be a field name, but got: {method.Arguments[0].Type} ({method.Arguments[0]}).");
                var regexTerm = GetTermValue(method.Arguments[1], queryParameters);
                clauses.Add(new ClauseInfo
                {
                    FieldName = regexFieldName,
                    TermValue = regexTerm,
                    PackedParamValue = writer.Add(regexTerm, ValueTokenType.String),
                    ClauseType = ClauseType.Regex,
                    OriginalIndex = clauses.Count
                });
                break;
            }

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
                    ParseExpression(method.Arguments[1], indexSearcher, clauses, queryParameters, metadata, ref hasMixedAndOr, writer);
                // If false, we simply don't add any clause — the branch is eliminated.
                break;
            }

            default:
                throw new InvalidOperationException(
                    $"Unexpected method '{method.Name.Value}' ({methodType}) in WHERE clause.");
        }
    }

    private static void ParseSearchMethod(MethodExpression method, List<ClauseInfo> clauses,
        BlittableJsonReaderObject queryParameters, QueryMetadata metadata, ValueWriter writer)
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

        var searchTerm = GetTermValue(method.Arguments[1], queryParameters);
        clauses.Add(new ClauseInfo
        {
            FieldName = fieldName,
            TermValue = searchTerm,
            PackedParamValue = writer.Add(searchTerm, ValueTokenType.String),
            ClauseType = ClauseType.Search,
            SearchOperator = searchOp,
            OriginalIndex = clauses.Count
        });
    }

    private static void ParsePrefixMethod(MethodExpression method, List<ClauseInfo> clauses,
        BlittableJsonReaderObject queryParameters, QueryMetadata metadata, ClauseType type, ValueWriter writer)
    {
        if (method.Arguments.Count < 2)
            throw new InvalidQueryException($"{type}() requires at least 2 arguments (field, term), but got {method.Arguments.Count}.");

        if (TryGetFieldName(method.Arguments[0], metadata, queryParameters, out var fieldName) == false)
            throw new InvalidQueryException($"{type}() first argument must be a field name, but got: {method.Arguments[0].Type} ({method.Arguments[0]}).");

        var prefixTerm = GetTermValue(method.Arguments[1], queryParameters);
        clauses.Add(new ClauseInfo
        {
            FieldName = fieldName,
            TermValue = prefixTerm,
            PackedParamValue = writer.Add(prefixTerm, ValueTokenType.String),
            ClauseType = type,
            OriginalIndex = clauses.Count
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
                switch (value)
                {
                    case long or int:
                        valueType = ValueTokenType.Long;
                        break;
                    case double or float or decimal:
                        valueType = ValueTokenType.Double;
                        break;
                    // LazyNumberValue wraps JSON numbers — try long first, then double
                    case LazyNumberValue lnv when lnv.TryParseLong(out _):
                        valueType = ValueTokenType.Long;
                        break;
                    case LazyNumberValue:
                        valueType = ValueTokenType.Double;
                        break;
                    default:
                    {
                        // DateTime/DateTimeOffset parameters arrive as LazyStringValue from JSON.
                        // Detect date strings and convert to ticks so range queries hit the numeric tree.
                        var str = value.ToString();
                        if (str is { Length: > 18 and < 35 } && str.Contains('T')
                                                             && DateTime.TryParse(str, System.Globalization.CultureInfo.InvariantCulture,
                                                                 System.Globalization.DateTimeStyles.RoundtripKind, out var parsed))
                        {
                            valueType = ValueTokenType.Long;
                            return parsed.Ticks.ToString(System.Globalization.CultureInfo.InvariantCulture);
                        }
                        valueType = ValueTokenType.String;
                        break;
                    }
                }
            }
            return value?.ToString();
        }
        valueType = ValueTokenType.Null;
        return null;
    }

    /// <summary>Convert an IN value to its string representation, handling booleans and dates.</summary>
    private static (string Value, ValueTokenType Type) ConvertInValue(object value, ValueTokenType literalType, ref bool hasTime)
    {
        if (value == null)
            return (null, ValueTokenType.String);
        if (value is bool b)
            return (b ? "true" : "false", b ? ValueTokenType.True : ValueTokenType.False);
        if (value is DateTime dt)
        {
            hasTime = true;
            return (dt.Ticks.ToString(System.Globalization.CultureInfo.InvariantCulture), ValueTokenType.Long);
        }
        if (value is DateTimeOffset dto)
        {
            hasTime = true;
            return (dto.UtcDateTime.Ticks.ToString(System.Globalization.CultureInfo.InvariantCulture), ValueTokenType.Long);
        }
        if (literalType != ValueTokenType.Parameter)
            return (value.ToString(), literalType);
        if (value is long or int)
            return (value.ToString(), ValueTokenType.Long);
        if (value is double or float or decimal)
            return (((IConvertible)value).ToString(System.Globalization.CultureInfo.InvariantCulture), ValueTokenType.Double);
        if (value is LazyNumberValue lnv)
            return (value.ToString(), lnv.TryParseLong(out _) ? ValueTokenType.Long : ValueTokenType.Double);
        var str = value.ToString();
        if (str is { Length: > 18 and < 35 } &&
            DateTime.TryParse(str, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.RoundtripKind, out var parsed))
        {
            hasTime = true;
            return (parsed.Ticks.ToString(System.Globalization.CultureInfo.InvariantCulture), ValueTokenType.Long);
        }
        return (str, ValueTokenType.String);
    }

    // ── Cardinality estimation ───────────────────────────────────────────

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
                {
                    foreach (var term in clause.InTerms)
                        sum += indexSearcher.NumberOfDocumentsUnderSpecificTerm(meta, term);
                }

                return Math.Min(sum, indexSearcher.NumberOfEntries);

            case ClauseType.Spatial:
            case ClauseType.Vector:
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

            case ClauseType.AndGroup:
                // AND of sub-clauses: cardinality is bounded by the minimum sub-clause cardinality.
                long andMin = indexSearcher.NumberOfEntries;
                if (clause.AndSubClauses != null)
                {
                    foreach (var sub in clause.AndSubClauses)
                    {
                        if (sub.Cardinality < 0)
                            sub.Cardinality = EstimateCardinality(sub, indexSearcher);
                        if (sub.Cardinality < andMin)
                            andMin = sub.Cardinality;
                    }
                }
                return andMin;

            default:
                return indexSearcher.NumberOfEntries;
        }
    }

    // ── Plan emission: clause list → PlanOp[] ────────────────────────────

    private static QueryPlan EmitPlan(List<ClauseInfo> clauses, bool isOr)
    {
        // Empty IN() in an AND chain means zero results — emit an empty plan.
        // In an OR chain, just remove the EmptyIn clauses (they contribute nothing).
        for (int i = clauses.Count - 1; i >= 0; i--)
        {
            if (clauses[i].ClauseType != ClauseType.EmptyIn)
                continue;

            if (isOr == false)
                return new QueryPlan { Ops = [], IsAllEntries = false };

            clauses.RemoveAt(i);
        }

        var ops = new List<PlanOp>();
        bool needsThreeBitmaps = false;

        if (isOr)
        {
            // OR chain — expand In/OrGroup terms into individual OR ops
            int matchIndex = 0;
            foreach (var it in clauses)
            {
                if ((it.ClauseType == ClauseType.In || it.ClauseType == ClauseType.AllIn) && it.InTerms != null)
                {
                    // Each IN term is a single-term lookup → eligible for native dispatch.
                    foreach (var _ in it.InTerms)
                    {
                        ops.Add(new PlanOp
                        {
                            Kind = matchIndex == 0 ? PlanOpKind.FillFromPostings : PlanOpKind.OrWithPostings,
                            ParamIndex = matchIndex,
                            EstimatedCardinality = it.Cardinality / it.InTerms.Count,
                            Dispatch = MatchDispatch.TermSource
                        });
                        matchIndex++;
                    }
                }
                else if (it.ClauseType == ClauseType.OrGroup && it.OrSubClauses != null)
                {
                    foreach (var sub in it.OrSubClauses)
                    {
                        ops.Add(new PlanOp
                        {
                            Kind = matchIndex == 0 ? PlanOpKind.FillFromPostings : PlanOpKind.OrWithPostings,
                            ParamIndex = matchIndex,
                            EstimatedCardinality = it.Cardinality / it.OrSubClauses.Count,
                            Dispatch = GetDispatch(sub)
                        });
                        matchIndex++;
                    }
                }
                else if (it.ClauseType == ClauseType.AndGroup && it.AndSubClauses != null)
                {
                    // AND sub-expression inside an OR chain.
                    // Only supported when the AND group is the first element (matchIndex == 0)
                    // or can be merged into slot 0 via OrBitmaps after computing into slot 1.
                    var subClauses = it.AndSubClauses;
                    if (matchIndex == 0)
                    {
                        // First element: build the AND chain directly in slot 0.
                        // Slot 1 is free (unused) so AndWithPostings can use it as scratch.
                        // Suppress early-exit on AND steps — the OR chain continues regardless.
                        ops.Add(new PlanOp
                        {
                            Kind = PlanOpKind.FillFromPostings,
                            ParamIndex = matchIndex,
                            EstimatedCardinality = subClauses[0].Cardinality,
                            Dispatch = GetDispatch(subClauses[0])
                        });
                        for (int s = 1; s < subClauses.Count; s++)
                        {
                            ops.Add(new PlanOp
                            {
                                Kind = subClauses[s].IsNegated ? PlanOpKind.AndNotWithPostings : PlanOpKind.AndWithPostings,
                                ParamIndex = matchIndex + s,
                                EstimatedCardinality = subClauses[s].Cardinality,
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
                        // empty state, or was cleared at the end of the previous iteration).
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
                            EstimatedCardinality = subClauses[0].Cardinality,
                            Dispatch = GetDispatch(subClauses[0])
                        });
                        for (int s = 1; s < subClauses.Count; s++)
                        {
                            ops.Add(new PlanOp
                            {
                                Kind = subClauses[s].IsNegated ? PlanOpKind.AndNotWithPostings : PlanOpKind.AndWithPostings,
                                ParamIndex = matchIndex + s,
                                BitmapLocal = 0,
                                EstimatedCardinality = subClauses[s].Cardinality,
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
                }
                else
                {
                    bool isNotEqualsInOr = it.ClauseType == ClauseType.NotEquals;
                    if (isNotEqualsInOr)
                    {
                        // NotEquals in OR chain: OR of NOT(X) clauses cannot use the raw term posting list
                        // (FillBitmapFromTermSource would add entries WITH X, not entries WITHOUT X).
                        // Mark the clause so ResolveMatches creates AllEntries ANDNOT TermQuery instead.
                        it.IsOrChainNotEquals = true;
                    }
                    ops.Add(new PlanOp
                    {
                        Kind = matchIndex == 0 ? PlanOpKind.FillFromPostings : PlanOpKind.OrWithPostings,
                        ParamIndex = matchIndex,
                        EstimatedCardinality = it.Cardinality,
                        Dispatch = isNotEqualsInOr ? MatchDispatch.DirectSource : GetDispatch(it)
                    });
                    matchIndex++;
                }
            }
            ops.Add(new PlanOp { Kind = PlanOpKind.IterateInto });
        }
        else if (clauses.Count == 1 && clauses[0].ClauseType == ClauseType.Equals && clauses[0].IsNegated == false)
        {
            ops.Add(new PlanOp
            {
                Kind = PlanOpKind.DirectIterate,
                ParamIndex = 0,
                EstimatedCardinality = clauses[0].Cardinality,
                Dispatch = GetDispatch(clauses[0])
            });
        }
        else if (clauses.Count == 1 && (clauses[0].ClauseType == ClauseType.NotEquals
            || (clauses[0].ClauseType == ClauseType.Equals && clauses[0].IsNegated)))
        {
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
                EstimatedCardinality = clauses[0].Cardinality,
                Dispatch = GetDispatch(clauses[0])
            });
            ops.Add(new PlanOp { Kind = PlanOpKind.IterateInto });

            // Mark clause so ResolveMatches produces [AllEntries, TermMatch]
            clauses[0].IsNegated = true;

            return new QueryPlan
            {
                Ops = ops.ToArray(),
                OperandOrdering = 0,
                QueryBuilderPlanState = clauses
            };
        }
        else
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
                    ParamIndex = CountMatchSlots(clauses, isAllEntries: false, allNegated: false), // Index of AllEntries in resolved matches
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
                            EstimatedCardinality = subClauses[s].Cardinality,
                            Dispatch = GetDispatch(subClauses[s])
                        });
                    }
                    matchIndex += subClauses.Count;
                }
                else if (clauses[0].ClauseType == ClauseType.In && clauses[0].InTerms != null)
                {
                    // IN at seed: OR all terms into bitmap[0]. Each IN term is a single-term lookup.
                    var terms = clauses[0].InTerms;
                    for (int t = 0; t < terms.Count; t++)
                    {
                        ops.Add(new PlanOp
                        {
                            Kind = t == 0 ? PlanOpKind.FillFromPostings : PlanOpKind.OrWithPostings,
                            ParamIndex = matchIndex + t,
                            BitmapLocal = 0,
                            EstimatedCardinality = clauses[0].Cardinality / terms.Count,
                            Dispatch = MatchDispatch.TermSource
                        });
                    }
                    matchIndex += terms.Count;
                }
                else if (clauses[0].ClauseType == ClauseType.AllIn && clauses[0].InTerms != null)
                {
                    // First clause is AllIn — fill first term, AND remaining. Each term is a single-term lookup.
                    var terms = clauses[0].InTerms;
                    ops.Add(new PlanOp
                    {
                        Kind = PlanOpKind.FillFromPostings,
                        ParamIndex = matchIndex,
                        BitmapLocal = 0,
                        EstimatedCardinality = clauses[0].Cardinality / terms.Count,
                        Dispatch = MatchDispatch.TermSource
                    });
                    for (int t = 1; t < terms.Count; t++)
                    {
                        ops.Add(new PlanOp
                        {
                            Kind = PlanOpKind.AndWithPostings,
                            ParamIndex = matchIndex + t,
                            BitmapLocal = 0,
                            EstimatedCardinality = clauses[0].Cardinality / terms.Count,
                            Dispatch = MatchDispatch.TermSource
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
                        EstimatedCardinality = clauses[0].Cardinality,
                        Dispatch = GetDispatch(clauses[0])
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
                        ParamIndex = matchIndex
                    });
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
                            EstimatedCardinality = subClauses[s].Cardinality,
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

                    matchIndex += subClauses.Count;
                }
                else if (clauses[i].ClauseType == ClauseType.In && clauses[i].InTerms != null)
                {
                    // IN in AND chain: OR all terms into bitmap[1], then AND (or ANDNOT for negated) with bitmap[0].
                    // Each IN term is a single-term lookup.
                    var terms = clauses[i].InTerms;
                    ops.Add(new PlanOp { Kind = PlanOpKind.ClearBitmap, BitmapLocal = 1 });
                    for (int t = 0; t < terms.Count; t++)
                    {
                        ops.Add(new PlanOp
                        {
                            Kind = PlanOpKind.OrWithPostings,
                            ParamIndex = matchIndex + t,
                            BitmapLocal = 1,
                            EstimatedCardinality = clauses[i].Cardinality / terms.Count,
                            Dispatch = MatchDispatch.TermSource
                        });
                    }
                    // Negated In (NOT IN): subtract the OR'd terms from the result bitmap.
                    var bitmapCombineKind = clauses[i].IsNegated ? PlanOpKind.AndNotBitmaps : PlanOpKind.AndBitmaps;
                    ops.Add(new PlanOp
                    {
                        Kind = bitmapCombineKind,
                        BitmapLocal = 0,
                        ParamIndex2 = 1
                    });
                    if (!clauses[i].IsNegated)
                        ops.Add(new PlanOp { Kind = PlanOpKind.CheckEmpty, BitmapLocal = 0 });
                    matchIndex += terms.Count;
                }
                else if (clauses[i].ClauseType == ClauseType.AllIn && clauses[i].InTerms != null)
                {
                    // AllIn: AND each term's posting list with bitmap[0]. Each term is single-term.
                    var terms = clauses[i].InTerms;
                    for (int t = 0; t < terms.Count; t++)
                    {
                        ops.Add(new PlanOp
                        {
                            Kind = PlanOpKind.AndWithPostings,
                            ParamIndex = matchIndex + t,
                            BitmapLocal = 0,
                            EstimatedCardinality = clauses[i].Cardinality / terms.Count,
                            Dispatch = MatchDispatch.TermSource
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
                        EstimatedCardinality = clauses[i].Cardinality,
                        Dispatch = GetDispatch(clauses[i])
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

            // Start from 0 when all clauses are negated (allNegated=true): all are ANDNOT
            // operands and AllEntries is the implicit seed, so every clause needs a predicate.
            // Start from 1 in the normal case: clause 0 is the fill seed.
            int scanStart = allNegated ? 0 : 1;
            for (int i = scanStart; i < clauses.Count; i++)
            {
                var pred = BuildScanPredicateInfo(clauses[i], ref longIndex, ref doubleIndex, ref sliceIndex);
                if (pred != null)
                    scanPreds.Add(pred.Value);
            }

            if (scanPreds.Count > 0)
                scanPredicateInfos = scanPreds.ToArray();
        }

        // Compute type signature from scan predicates. The int packs the first 16 kinds
        // (2 bits each). For ≤ 16 predicates this is the exact cache identity. For more,
        // it's a lossy hash and we attach FullKinds for disambiguation in PlanCache.
        int typeSignature = 0;
        byte[] fullKinds = null;
        if (scanPredicateInfos != null)
        {
            int n = scanPredicateInfos.Length;
            int packCount = Math.Min(n, 16);
            for (int i = 0; i < packCount; i++)
                typeSignature |= ((int)scanPredicateInfos[i].ValueType & 0x3) << (i * 2);
            if (n > 16)
            {
                fullKinds = new byte[n];
                for (int i = 0; i < n; i++)
                    fullKinds[i] = (byte)scanPredicateInfos[i].ValueType;
            }
        }

        var plan = new QueryPlan
        {
            Ops = ops.ToArray(),
            OperandOrdering = ordering,
            QueryBuilderPlanState = clauses,
            AllNegated = allNegated,
            ScanPredicateInfos = scanPredicateInfos,
            TypeSignature = typeSignature,
            FullKinds = fullKinds,
            RequiredBitmaps = needsThreeBitmaps ? 3 : 2
        };
        return plan;
    }

    // ── Plan helpers ─────────────────────────────────────────────────────

    private static QueryPlan BuildAllEntriesPlan()
    {
        // No bitmap needed — AllEntries already implements IQueryMatch.Fill(),
        // so we iterate it directly without materializing into a bitmap first.
        return new QueryPlan
        {
            Ops = [new PlanOp { Kind = PlanOpKind.DirectIterate, ParamIndex = 0 }],
            IsAllEntries = true
        };
    }

    private static QueryPlan BuildEmptyPlan()
    {
        // Query that always returns 0 results (e.g. false AND X)
        return new QueryPlan
        {
            Ops = [],
        };
    }

    /// <summary>Attach spatial and vector post-filter phases to a query plan.
    /// Spatial/vector clauses are stored in the plan's Clauses array at known indices,
    /// and SpatialFilters/VectorSelects reference those indices for resolution at execution time.</summary>
    private static void AttachPostFilterPhases(QueryPlan plan, List<ClauseInfo> spatialClauses, List<ClauseInfo> vectorClauses)
    {
        if (spatialClauses == null && vectorClauses == null)
            return;

        var clauses = plan.QueryBuilderPlanState as List<ClauseInfo> ?? [];
        plan.QueryBuilderPlanState = clauses;

        int matchIndex = CountMatchSlots(clauses, plan.IsAllEntries, plan.AllNegated);

        if (spatialClauses != null)
        {
            plan.SpatialFilters = new SpatialFilterOp[spatialClauses.Count];
            for (int i = 0; i < spatialClauses.Count; i++)
            {
                clauses.Add(spatialClauses[i]);
                plan.SpatialFilters[i] = new SpatialFilterOp { MatchIndex = matchIndex++, Clause = spatialClauses[i] };
            }
        }

        if (vectorClauses != null)
        {
            plan.VectorSelects = new VectorSelectOp[vectorClauses.Count];
            for (int i = 0; i < vectorClauses.Count; i++)
            {
                clauses.Add(vectorClauses[i]);
                plan.VectorSelects[i] = new VectorSelectOp { MatchIndex = matchIndex++, Clause = vectorClauses[i] };
            }
        }
    }

    /// <summary>Count how many IQueryMatch slots a clause list expands to.
    /// OrGroup/AndGroup/In/AllIn each expand to one slot per sub-term.</summary>
    internal static int CountMatchSlots(List<ClauseInfo> clauses, bool isAllEntries, bool allNegated)
    {
        if (clauses == null)
            return isAllEntries ? 1 : 0;

        int count = isAllEntries ? 1 : 0;
        foreach (var ci in clauses)
        {
            count += ci.ClauseType switch
            {
                ClauseType.OrGroup when ci.OrSubClauses != null => ci.OrSubClauses.Count,
                ClauseType.AndGroup when ci.AndSubClauses != null => ci.AndSubClauses.Count,
                ClauseType.In or ClauseType.AllIn when ci.InTerms != null => ci.InTerms.Count,
                _ => 1
            };
        }
        if (allNegated)
            count++;
        return count;
    }

    // ── Dispatch classification ──────────────────────────────────────────

    /// <summary>Decide whether a clause type can be expressed as a single
    /// <see cref="TermSource"/>. Boosted clauses go through the IQueryMatch path
    /// even when they're term-shaped, so scoring still works.</summary>
    internal static bool IsTermSourceEligibleClause(ClauseInfo clause)
    {
        if (clause == null)
            return false;
        if (clause.BoostFactor > 0)
            return false;
        return clause.ClauseType is ClauseType.Equals or ClauseType.NotEquals;
    }

    /// <summary>Resolve the <see cref="MatchDispatch"/> mode for a clause at plan-build time.
    /// Equals / NotEquals (unboosted) → <c>TermSource</c> (native posting-list, no IQueryMatch wrapper).
    /// All other clause types → <c>DirectSource</c> (IQueryMatch interface dispatch).</summary>
    private static MatchDispatch GetDispatch(ClauseInfo clause)
    {
        if (IsTermSourceEligibleClause(clause))
            return MatchDispatch.TermSource;
        return MatchDispatch.DirectSource;
    }

    // ── Entry scan predicate building ────────────────────────────────────

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
            case ClauseType.AndGroup: // AND-groups inside OR chains are handled at the bitmap level
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

        // Strong typing: TermValueType is set by GetTermValue from the parser's literal
        // type (for inline values) or the resolved JSON-blittable runtime type (for params).
        // Switch on it directly — no string round-trip / TryParse fallback. A null TermValue
        // (e.g. "exists" check) falls through to Slice.
        ScanValueType valueType;
        switch (clause.TermValueType)
        {
            case ValueTokenType.Long:
                valueType = ScanValueType.Long;
                break;
            case ValueTokenType.Double:
                valueType = ScanValueType.Double;
                break;
            default:
                // String/True/False/Null/Parameter (when unresolvable) → opaque slice comparison.
                valueType = ScanValueType.Slice;
                break;
        }

        bool isBetween = clause.ClauseType == ClauseType.Between && clause.TermValue2 != null;
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
