using System;
using System.Collections.Generic;
using Corax.Querying.Planning;
using Corax.Mappings;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Sparrow.Json;
using Sparrow.Server;
using Constants = Corax.Constants;
using ClientConstants = Raven.Client.Constants;
using SpatialUnits = Raven.Client.Documents.Indexes.Spatial.SpatialUnits;
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

    // ── Cached clause template ────────────────────────────────────────

    /// <summary>Immutable structural template built on first execution of a query text.
    /// Cached on PerQueryPlans.Template (as object, since PlanCache is in Corax).
    /// On cache hit, PopulateParameters re-resolves parameter values from the blittable
    /// using the bindings, skipping AST parsing entirely.</summary>
    /// <summary>Cached per-query-text template. Each ClauseInfo carries its own Binding
    /// describing how to re-resolve its value from the blittable on subsequent executions.
    /// On cache hit, the clauses are cloned (shallow) and their per-execution fields
    /// (PackedParamValue, TermValueType, Cardinality, Spatial, Vector) are overwritten
    /// by PopulateParameters.</summary>
    internal sealed class ClauseTemplate
    {
        public ClauseInfo[] Clauses;
        public bool IsAllEntries;
        public bool IsOr;              // root boolean operator
        public bool HasSpatialFilters; // had spatial post-filter clauses
        public bool HasVectorSelects;  // had vector post-filter clauses
        /// <summary>Spatial clauses separated from the main filter chain (AND queries only).</summary>
        public ClauseInfo[] SpatialClauses;
        /// <summary>Vector clauses separated from the main filter chain (AND queries only).</summary>
        public ClauseInfo[] VectorClauses;
    }

    /// <summary>Describes how to resolve a clause's parameter value without the AST.
    /// For literals: the native value is cached directly.
    /// For parameters: the parameter name is stored for blittable lookup.</summary>
    internal sealed class ParameterBinding
    {
        public bool IsLiteral;
        public object LiteralValue;       // cached native value for literals (long/double/string)
        public ValueTokenType LiteralType;
        public string ParameterName;      // for parameters: name to look up in blittable ("p0")
        public bool IsArrayParameter;    // true if this parameter may resolve to an array (IN terms)

        /// <summary>Second binding for BETWEEN high bound. Null for non-BETWEEN clauses.</summary>
        public ParameterBinding Second;

        /// <summary>For IN/AllIn: bindings for each term. Null for non-IN clauses.
        /// Each entry is either a literal or parameter-array-element.</summary>
        public ParameterBinding[] InBindings;

        /// <summary>For Spatial: bindings for shape arguments.</summary>
        public ParameterBinding[] SpatialBindings;

        /// <summary>For Vector: binding for the vector value argument.</summary>
        public ParameterBinding VectorValueBinding;
    }

    /// <summary>
    /// Packed parameter reference — a 32-bit value encoding the type and index(es)
    /// of a clause's resolved value within the plan's typed arrays
    /// (QueryPlan.LongValues / DoubleValues / StringValues).
    ///
    ///   bits [31:30] = value type (Long=0, Double=1, String=2, None=3)
    ///   bits [29:15] = first parameter index (0..32767)
    ///   bits [14:0]  = second parameter index (0..32767, 0x7FFF = no second param)
    ///
    /// For simple predicates (Equals, GT, LT, etc.): Param1 = value index, Param2 = NoParam.
    /// For BETWEEN: Param1 = low-bound index, Param2 = high-bound index (same-typed array).
    /// For IN/AllIn: Param1 = start index into the typed array. Term count is stored separately
    ///   in ClauseInfo.InTermCount (not packed) because IN can exceed 32K terms.
    /// For parameterless clauses (Exists): None sentinel.
    /// </summary>
    internal readonly struct PackedParam
    {
        public const int NoParamValue = 0x7FFF;
        private const int MaxIndex = 0x7FFF; // 32767

        public const int TypeLong = 0;
        public const int TypeDouble = 1;
        public const int TypeString = 2;
        private const int TypeNone = 3;

        /// <summary>Sentinel: no parameter ("exists(Field)" clauses). Spatial and vector clauses
        /// store their numeric parameters directly on ClauseInfo fields instead.</summary>
        public static readonly PackedParam None = new((TypeNone << 30) | (NoParamValue << 15) | NoParamValue);

        private readonly int _value;

        private PackedParam(int raw) => _value = raw;

        public PackedParam(int type, int param1, int param2 = NoParamValue)
        {
            if (param1 > MaxIndex)
                ThrowLimitExceeded(param1);
            if (param2 != NoParamValue && param2 > MaxIndex)
                ThrowLimitExceeded(param2);
            _value = (type << 30) | ((param1 & 0x7FFF) << 15) | (param2 & 0x7FFF);
        }

        public int ValueType => (_value >>> 30) & 0x3;
        public int Param1 => (_value >>> 15) & 0x7FFF;
        public int Param2 => _value & 0x7FFF;
        public bool IsNone => _value == None._value;

        private static void ThrowLimitExceeded(int index)
        {
            throw new InvalidOperationException(
                $"Query parameter index {index} exceeds maximum ({MaxIndex}). " +
                "Simplify the query or reduce the number of IN terms.");
        }
    }

    /// <summary>
    /// Accumulates typed parameter values during query plan building.
    /// Each Add call stores the native value in the appropriate-typed list
    /// and returns a <see cref="PackedParam"/> encoding (type + index) for the clause.
    /// Values are stored as their native types — no string round-trips.
    /// </summary>
    private sealed class ValueWriter
    {
        private readonly List<long> _longs = new();
        private readonly List<double> _doubles = new();
        private readonly List<string> _strings = new();

        public PackedParam AddLong(long value)
        {
            _longs.Add(value);
            return new PackedParam(PackedParam.TypeLong, _longs.Count - 1);
        }

        public PackedParam AddDouble(double value)
        {
            _doubles.Add(value);
            return new PackedParam(PackedParam.TypeDouble, _doubles.Count - 1);
        }

        public PackedParam AddString(string value)
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
        OrGroup,  // A group of OR'd subclauses
        AndGroup, // A group of AND'd subclauses inside an OR chain
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
    ///   (type, index), so resolution never reparses strings.
    /// - A clause type is classified into a flat enum — downstream code switches on one value
    ///   instead of pattern-matching AST node types and method names.
    /// - Planning annotations (Cardinality, IsExact, BoostFactor, IsNegated) are attached per
    ///   clause for operand reordering, dispatch classification, and entry-scan eligibility.
    /// </summary>
    internal class ClauseInfo
    {
        public string FieldName;
        public ValueTokenType TermValueType; // for type-aware scan predicate building

        /// <summary>Packed (type, index) into QueryPlan.LongValues/DoubleValues/StringValues.
        /// For BETWEEN: Param1 = low-bound index, Param2 = high-bound index.
        /// For IN/AllIn: Param1 = start index into the typed array. Count is in <see cref="InTermCount"/>.
        /// See <see cref="PackedParam"/>.</summary>
        public PackedParam PackedParamValue = PackedParam.None;

        /// <summary>Number of IN/AllIn terms stored contiguously starting at PackedParamValue.Param1.
        /// Stored separately (not in PackedParam.Param2) because IN clauses can have more than
        /// 32K terms (e.g. a large array parameter), exceeding the 15-bit Param2 limit.</summary>
        public int InTermCount;

        /// <summary>Indices of null terms within the IN list. Null for most queries.
        /// When present, resolution uses string-null lookup for these positions instead
        /// of reading from the typed array (where null is stored as a 0 sentinel).</summary>
        public HashSet<int> InNullTermIndices;

        public List<ClauseInfo> OrSubClauses;  // for OrGroup
        /// <summary>Set for NotEquals clauses appearing in OR chains.
        /// Example: WHERE Name != 'a' OR Age = 25
        /// The NOT(Name='a') term cannot use the raw posting list (which contains entries
        /// WITH 'a', not entries WITHOUT 'a'). Instead, ResolveMatches pre-materializes
        /// AllEntries ANDNOT TermQuery('a') into a BitmapMatch, so FillFromMatch during
        /// execution correctly ORs in the complement set.</summary>
        public bool IsOrChainNotEquals;
        public List<ClauseInfo> AndSubClauses; // for AndGroup (AND sub-expression inside OR)

        /// <summary>Pre-resolved spatial parameters. Null for non-spatial clauses.</summary>
        public SpatialParams Spatial;
        public MethodType SpatialMethodType; // for spatial: Within/Contains/Disjoint/Intersects

        /// <summary>Pre-resolved vector parameters. Null for non-vector clauses.</summary>
        public VectorParams Vector;
        public ClauseType ClauseType;
        public long Cardinality = -1;
        public int OriginalIndex;
        public bool IsNegated;
        public bool IsExact;
        public float BoostFactor;

        public Constants.Search.Operator SearchOperator; // for Search (AND/OR)

        /// <summary>How to re-resolve this clause's value from the blittable on cache hit.
        /// Null for parameterless clauses (Exists). Set during first parse, immutable after.</summary>
        public ParameterBinding Binding;

        /// <summary>How to re-resolve the boost factor, if this clause is wrapped in boost().
        /// Null for non-boosted clauses. The actual factor is resolved per-execution.</summary>
        public ParameterBinding BoostBinding;

        /// <summary>Clone structural fields for cache-hit reuse. Per-execution fields
        /// (PackedParamValue, TermValueType, Cardinality, Spatial, Vector, InTermCount,
        /// InNullTermIndices) will be overwritten by PopulateParameters.</summary>
        public ClauseInfo CloneStructure()
        {
            return new ClauseInfo
            {
                FieldName = FieldName,
                ClauseType = ClauseType,
                OriginalIndex = OriginalIndex,
                IsNegated = IsNegated,
                IsExact = IsExact,
                BoostFactor = BoostFactor,
                SearchOperator = SearchOperator,
                IsOrChainNotEquals = IsOrChainNotEquals,
                SpatialMethodType = SpatialMethodType,
                Binding = Binding,
                BoostBinding = BoostBinding,
                OrSubClauses = OrSubClauses?.ConvertAll(c => c.CloneStructure()),
                AndSubClauses = AndSubClauses?.ConvertAll(c => c.CloneStructure()),
                Cardinality = -1, // re-estimated per execution
            };
        }
    }

    /// <summary>Pre-resolved spatial query parameters. All values are extracted from the AST
    /// during parsing so execution never calls GetValue back on the blittable.
    /// Shape construction (spatialField.ReadCircle / ReadShape) still runs at execution time
    /// because it needs the spatial field factory from builderParameters.</summary>
    internal sealed class SpatialParams
    {
        public double DistanceErrorPct = -1; // -1 = use default
        public bool IsCircle;                // true = circle, false = WKT
        // Circle parameters
        public double CircleRadius;
        public double CircleLatitude;
        public double CircleLongitude;
        // WKT parameter
        public string Wkt;
        // Shared
        public SpatialUnits? Units;
    }

    /// <summary>Pre-resolved vector query parameters. All scalar values and the raw vector
    /// payload are extracted from the AST during parsing. Embedding construction
    /// (base64 decode, AI embedding generation) still runs at execution time.</summary>
    internal sealed class VectorParams
    {
        public float MinimumMatch = -1;      // -1 = use index default
        public int NumberOfCandidates = -1;  // -1 = use index default
        public object ResolvedValue;         // the raw vector parameter (string, BlittableJsonReaderArray, etc.)
        public ValueTokenType ResolvedValueType;
        public VectorMethodKind Method;      // which embedding method, if any
        public string AiTaskName;            // AI task identifier for embedding.text
    }

    internal enum VectorMethodKind : byte
    {
        None,           // direct value (not a method call)
        ForDocument,    // embedding.forDoc(docId)
        ForRaw,         // embedding.forRaw(base64)
        EmbeddingText,  // embedding.text(text, ai.task(taskName))
    }

    private enum BooleanOp { And, Or, True, False, Leaf }

    // ── Entry point ──────────────────────────────────────────────────────

    /// <summary>Parse the RQL AST into a structural clause template.
    /// No value resolution, no cardinality estimation, no sorting, no plan emission.
    /// Those happen in BuildAndCompile after PopulateParameters.</summary>
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
                if (clauses[i].ClauseType == ClauseType.Spatial)
                {
                    spatialList ??= [];
                    spatialList.Add(clauses[i]);
                    clauses.RemoveAt(i);
                }
                else if (clauses[i].ClauseType == ClauseType.Vector)
                {
                    vectorList ??= [];
                    vectorList.Add(clauses[i]);
                    clauses.RemoveAt(i);
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
                    VectorClauses = vectorClauses,
                    HasSpatialFilters = spatialClauses != null,
                    HasVectorSelects = vectorClauses != null
                };
            }
        }

        return new ClauseTemplate
        {
            Clauses = clauses.ToArray(),
            IsAllEntries = false,
            IsOr = isOr,
            SpatialClauses = spatialClauses,
            VectorClauses = vectorClauses,
            HasSpatialFilters = spatialClauses != null,
            HasVectorSelects = vectorClauses != null
        };
    }

    // ── Parsing: RQL AST → flat clause list ──────────────────────────────

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
            Binding = CreateBinding(be.Right, queryParameters)
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
            Binding = CreateBinding(be.Right, queryParameters),
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
        if (minBinding is { IsLiteral: true } && maxBinding is { IsLiteral: true }
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
            Binding = new ParameterBinding
            {
                IsLiteral = minBinding?.IsLiteral ?? true,
                LiteralValue = minBinding?.LiteralValue,
                LiteralType = minBinding?.LiteralType ?? ValueTokenType.String,
                ParameterName = minBinding?.ParameterName,
                Second = maxBinding
            }
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
                    inBindings.Add(new ParameterBinding { ParameterName = ve.Token.Value, IsArrayParameter = true });
                }
                else
                {
                    // Literal value
                    var rawValue = ve.GetValue(queryParameters);
                    bool hasTime = false;
                    var (resolved, resolvedType) = ResolveInValue(rawValue, ve.Value, ref hasTime);
                    inBindings.Add(new ParameterBinding { IsLiteral = true, LiteralValue = resolved, LiteralType = resolvedType });
                }
            }
        }

        if (inBindings.Count == 0)
        {
            clauses.Add(new ClauseInfo
            {
                FieldName = resolvedFieldName,
                ClauseType = ClauseType.EmptyIn,
                OriginalIndex = clauses.Count
            });
            return;
        }

        clauses.Add(new ClauseInfo
        {
            FieldName = resolvedFieldName,
            ClauseType = inExpr.All ? ClauseType.AllIn : ClauseType.In,
            OriginalIndex = clauses.Count,
            Binding = new ParameterBinding { InBindings = inBindings.ToArray() }
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
                // Boost factor is per-execution (could be parameterized).
                // Store the binding on each inner clause; PopulateClauseValues resolves the factor.
                ParameterBinding boostBinding = null;
                if (method.Arguments.Count > 1)
                    boostBinding = CreateBinding(method.Arguments[1], queryParameters);
                for (int c = beforeCount; c < clauses.Count; c++)
                {
                    clauses[c].BoostFactor = 1f; // mark as boosted; actual value set by PopulateClauseValues
                    clauses[c].BoostBinding = boostBinding;
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
                    Binding = CreateBinding(method.Arguments[1], queryParameters)
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

                var shapeExpr = method.Arguments[1] as MethodExpression;
                var shapeType = shapeExpr != null ? QueryMethod.GetMethodType(shapeExpr.Name.Value) : MethodType.Unknown;

                // Build spatial bindings: [0]=distErrPct, then shape-specific args
                var spatialBindings = new List<ParameterBinding>();
                // distanceErrorPct (3rd argument, optional)
                spatialBindings.Add(method.Arguments.Count == 3
                    ? CreateBinding(method.Arguments[2], queryParameters)
                    : null);

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
                    SpatialMethodType = methodType,
                    OriginalIndex = clauses.Count,
                    Binding = new ParameterBinding
                    {
                        SpatialBindings = spatialBindings.ToArray(),
                        // Store shape type as literal metadata
                        IsLiteral = isCircle, // reuse: true = circle, false = WKT
                    }
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

                VectorMethodKind vecMethod = VectorMethodKind.None;
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
                        ClientConstants.VectorSearch.EmbeddingForDocument => VectorMethodKind.ForDocument,
                        ClientConstants.VectorSearch.EmbeddingForRaw => VectorMethodKind.ForRaw,
                        ClientConstants.VectorSearch.EmbeddingText => VectorMethodKind.EmbeddingText,
                        _ => VectorMethodKind.None
                    };
                    if (methodValue.Arguments.Count > 0)
                        vectorValueBinding = CreateBinding(methodValue.Arguments[0], queryParameters);
                    if (vecMethod == VectorMethodKind.EmbeddingText && methodValue.Arguments.Count > 1
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
                    Binding = new ParameterBinding
                    {
                        VectorValueBinding = vectorValueBinding,
                        // Store other vector bindings on SpatialBindings (reuse the array)
                        // [0]=minimumMatch, [1]=numberOfCandidates, [2]=aiTask
                        SpatialBindings = new[] { minimumMatchBinding, numberOfCandidatesBinding, aiTaskBinding },
                    },
                    Vector = new VectorParams { Method = vecMethod } // structural: method kind doesn't change
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
            SearchOperator = searchOp,
            OriginalIndex = clauses.Count,
            Binding = CreateBinding(method.Arguments[1], queryParameters)
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
            Binding = CreateBinding(method.Arguments[1], queryParameters)
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

    /// <summary>Resolve a query expression to its native typed value + type tag.
    /// Parameters are resolved from the blittable; literals are returned directly.
    /// No ToString — callers that need a string form call <see cref="FormatValue"/>.</summary>
    private static (object Value, ValueTokenType Type) ResolveTermValue(QueryExpression expr, BlittableJsonReaderObject queryParameters)
    {
        return ResolveTermValue(expr, queryParameters, out _);
    }

    /// <summary>Resolve a query expression to its native typed value + type tag,
    /// also returning the parameter name (if the expression is a parameter reference).
    /// <paramref name="parameterName"/> is null for literals, non-null for $parameters.</summary>
    private static (object Value, ValueTokenType Type) ResolveTermValue(QueryExpression expr, BlittableJsonReaderObject queryParameters, out string parameterName)
    {
        parameterName = null;
        if (expr is ValueExpression ve)
        {
            var valueType = ve.Value;
            var value = ve.GetValue(queryParameters);
            if (value is bool b)
                return (b ? "true" : "false", ValueTokenType.String); // Corax stores booleans as lowercase strings
            if (valueType == ValueTokenType.Parameter)
            {
                parameterName = ve.Token.Value;
                if (value != null)
                    return ResolveParameterValue(value);
                return (null, ValueTokenType.String);
            }
            // For non-parameter literals, coerce to native type when the token says so
            if (valueType == ValueTokenType.Long && value != null)
            {
                if (value is long l) return (l, ValueTokenType.Long);
                if (long.TryParse(value.ToString(), out long parsed)) return (parsed, ValueTokenType.Long);
            }
            if (valueType == ValueTokenType.Double && value != null)
            {
                if (value is double d) return (d, ValueTokenType.Double);
                if (double.TryParse(value.ToString(), System.Globalization.NumberStyles.Float,
                    System.Globalization.CultureInfo.InvariantCulture, out double parsed))
                    return (parsed, ValueTokenType.Double);
            }
            return (value?.ToString(), valueType);
        }
        return (null, ValueTokenType.Null);
    }

    /// <summary>Detect the native type of a resolved parameter value.
    /// Shared by <see cref="ResolveTermValue"/> and <see cref="ResolveInValue"/>.</summary>
    private static (object Value, ValueTokenType Type) ResolveParameterValue(object value)
    {
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
    /// constant value since it never changes.</summary>
    private static ParameterBinding CreateBinding(QueryExpression expr, BlittableJsonReaderObject queryParameters)
    {
        if (expr is not ValueExpression ve)
            return null;

        if (ve.Value == ValueTokenType.Parameter)
            return new ParameterBinding { ParameterName = ve.Token.Value };

        // Null literal — preserve as actual null, not the string "null"
        if (ve.Value == ValueTokenType.Null)
            return new ParameterBinding { IsLiteral = true, LiteralValue = null, LiteralType = ValueTokenType.String };

        // Literal — resolve the constant value once
        var value = ve.GetValue(queryParameters);
        if (value is bool b)
            return new ParameterBinding { IsLiteral = true, LiteralValue = b ? "true" : "false", LiteralType = ValueTokenType.String };
        if (value == null)
            return new ParameterBinding { IsLiteral = true, LiteralValue = null, LiteralType = ValueTokenType.String };

        var (resolved, resolvedType) = ResolveParameterValue(value);
        return new ParameterBinding { IsLiteral = true, LiteralValue = resolved, LiteralType = resolvedType };
    }

    /// <summary>Format a value from the plan's typed arrays as a string for display/highlighting.</summary>
    internal static string FormatValueFromPlan(PackedParam packed, QueryPlan plan)
    {
        if (packed.IsNone) return null;
        int idx = packed.Param1;
        return packed.ValueType switch
        {
            PackedParam.TypeLong => plan.LongValues[idx].ToString(),
            PackedParam.TypeDouble => plan.DoubleValues[idx].ToString(System.Globalization.CultureInfo.InvariantCulture),
            _ => plan.StringValues[idx]
        };
    }

    /// <summary>Format the second value (BETWEEN high bound) from the plan's typed arrays.</summary>
    internal static string FormatValue2FromPlan(PackedParam packed, QueryPlan plan)
    {
        if (packed.IsNone) return null;
        int idx = packed.Param2;
        if (idx == PackedParam.NoParamValue) return null;
        return packed.ValueType switch
        {
            PackedParam.TypeLong => plan.LongValues[idx].ToString(),
            PackedParam.TypeDouble => plan.DoubleValues[idx].ToString(System.Globalization.CultureInfo.InvariantCulture),
            _ => plan.StringValues[idx]
        };
    }

    /// <summary>Convenience: resolve and format as string in one call. Used by methods that
    /// only need the string form (e.g. boost factor parsing).</summary>
    private static string GetTermValue(QueryExpression expr, BlittableJsonReaderObject queryParameters)
    {
        var (value, _) = ResolveTermValue(expr, queryParameters);
        return value?.ToString();
    }

    /// <summary>Resolve an IN value to its native type, handling booleans and dates.</summary>
    private static (object Value, ValueTokenType Type) ResolveInValue(object value, ValueTokenType literalType, ref bool hasTime)
    {
        if (value == null)
            return (null, ValueTokenType.String);
        if (value is bool b)
            return (b ? "true" : "false", ValueTokenType.String); // booleans → lowercase strings
        if (value is DateTime dt)
        {
            hasTime = true;
            return (dt.Ticks, ValueTokenType.Long);
        }
        if (value is DateTimeOffset dto)
        {
            hasTime = true;
            return (dto.UtcDateTime.Ticks, ValueTokenType.Long);
        }
        if (literalType != ValueTokenType.Parameter)
            return (value.ToString(), literalType);
        // Parameter: detect native type
        var (resolved, resolvedType) = ResolveParameterValue(value);
        if (resolvedType == ValueTokenType.Long && value is DateTime or DateTimeOffset)
            hasTime = true;
        return (resolved, resolvedType);
    }

    // ── Cardinality estimation ───────────────────────────────────────────

    private static long EstimateCardinality(ClauseInfo clause, IndexSearcher indexSearcher, ValueWriter writer)
    {
        switch (clause.ClauseType)
        {
            case ClauseType.Equals:
            {
                var fieldMeta = indexSearcher.FieldMetadataBuilder(clause.FieldName);
                var p = clause.PackedParamValue;
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
                var ip = clause.PackedParamValue;
                if (!ip.IsNone)
                {
                    int start = ip.Param1;
                    int count = clause.InTermCount;
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
                if (clause.OrSubClauses != null)
                {
                    foreach (var sub in clause.OrSubClauses)
                    {
                        if (sub.Cardinality < 0)
                            sub.Cardinality = EstimateCardinality(sub, indexSearcher, writer);
                        orSum += sub.Cardinality;
                    }
                }
                return Math.Min(orSum, indexSearcher.NumberOfEntries);

            case ClauseType.AndGroup:
                long andMin = indexSearcher.NumberOfEntries;
                if (clause.AndSubClauses != null)
                {
                    foreach (var sub in clause.AndSubClauses)
                    {
                        if (sub.Cardinality < 0)
                            sub.Cardinality = EstimateCardinality(sub, indexSearcher, writer);
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
                if ((it.ClauseType == ClauseType.In || it.ClauseType == ClauseType.AllIn) && it.InTermCount > 0)
                {
                    // Each IN term is a single-term lookup → eligible for native dispatch.
                    for (int t = 0; t < it.InTermCount; t++)
                    {
                        ops.Add(new PlanOp
                        {
                            Kind = matchIndex == 0 ? PlanOpKind.FillFromPostings : PlanOpKind.OrWithPostings,
                            ParamIndex = matchIndex,
                            EstimatedCardinality = it.Cardinality / it.InTermCount,
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
                else if (clauses[0].ClauseType == ClauseType.In && clauses[0].InTermCount > 0)
                {
                    // IN at seed: OR all terms into bitmap[0]. Each IN term is a single-term lookup.
                    var terms = clauses[0].InTermCount;
                    for (int t = 0; t < terms; t++)
                    {
                        ops.Add(new PlanOp
                        {
                            Kind = t == 0 ? PlanOpKind.FillFromPostings : PlanOpKind.OrWithPostings,
                            ParamIndex = matchIndex + t,
                            BitmapLocal = 0,
                            EstimatedCardinality = clauses[0].Cardinality / terms,
                            Dispatch = MatchDispatch.TermSource
                        });
                    }
                    matchIndex += terms;
                }
                else if (clauses[0].ClauseType == ClauseType.AllIn && clauses[0].InTermCount > 0)
                {
                    // First clause is AllIn — fill first term, AND remaining. Each term is a single-term lookup.
                    var terms = clauses[0].InTermCount;
                    ops.Add(new PlanOp
                    {
                        Kind = PlanOpKind.FillFromPostings,
                        ParamIndex = matchIndex,
                        BitmapLocal = 0,
                        EstimatedCardinality = clauses[0].Cardinality / terms,
                        Dispatch = MatchDispatch.TermSource
                    });
                    for (int t = 1; t < terms; t++)
                    {
                        ops.Add(new PlanOp
                        {
                            Kind = PlanOpKind.AndWithPostings,
                            ParamIndex = matchIndex + t,
                            BitmapLocal = 0,
                            EstimatedCardinality = clauses[0].Cardinality / terms,
                            Dispatch = MatchDispatch.TermSource
                        });
                        ops.Add(new PlanOp { Kind = PlanOpKind.CheckEmpty, BitmapLocal = 0 });
                    }
                    matchIndex += terms;
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
                else if (clauses[i].ClauseType == ClauseType.In && clauses[i].InTermCount > 0)
                {
                    // IN in AND chain: OR all terms into bitmap[1], then AND (or ANDNOT for negated) with bitmap[0].
                    // Each IN term is a single-term lookup.
                    var terms = clauses[i].InTermCount;
                    ops.Add(new PlanOp { Kind = PlanOpKind.ClearBitmap, BitmapLocal = 1 });
                    for (int t = 0; t < terms; t++)
                    {
                        ops.Add(new PlanOp
                        {
                            Kind = PlanOpKind.OrWithPostings,
                            ParamIndex = matchIndex + t,
                            BitmapLocal = 1,
                            EstimatedCardinality = clauses[i].Cardinality / terms,
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
                    matchIndex += terms;
                }
                else if (clauses[i].ClauseType == ClauseType.AllIn && clauses[i].InTermCount > 0)
                {
                    // AllIn: AND each term's posting list with bitmap[0]. Each term is single-term.
                    var terms = clauses[i].InTermCount;
                    for (int t = 0; t < terms; t++)
                    {
                        ops.Add(new PlanOp
                        {
                            Kind = PlanOpKind.AndWithPostings,
                            ParamIndex = matchIndex + t,
                            BitmapLocal = 0,
                            EstimatedCardinality = clauses[i].Cardinality / terms,
                            Dispatch = MatchDispatch.TermSource
                        });
                        ops.Add(new PlanOp { Kind = PlanOpKind.CheckEmpty, BitmapLocal = 0 });
                    }
                    matchIndex += terms;
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
                ClauseType.In or ClauseType.AllIn when ci.InTermCount > 0 => ci.InTermCount,
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
