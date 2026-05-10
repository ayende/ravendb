using System.Collections.Generic;

namespace Corax.Querying.Planning;

/// <summary>Value type for resolved parameters. Corax-side equivalent of
/// Raven.Server's ValueTokenType — only the subset needed for clause resolution.</summary>
public enum ParamValueType : byte
{
    String,
    Long,
    Double,
    Null,
    /// <summary>Blittable object or array parameter — preserved as-is for vector/spatial resolution.
    /// Maps to ValueTokenType.Parameter at the Raven.Server boundary.</summary>
    Parameter
}

/// <summary>Spatial operation type. Corax-side equivalent of the Raven.Server
/// MethodType enum — only the spatial operations.</summary>
public enum SpatialOperationType : byte
{
    Within,
    Contains,
    Disjoint,
    Intersects
}

/// <summary>Where the vector data for a vector.search() query comes from.</summary>
public enum VectorSourceKind : byte
{
    Inline,         // direct value or embedding.forRaw — vector data provided in the query
    FromDocument,   // embedding.forDoc(docId) — vector copied from another document
    FromText,       // embedding.text(text, ai.task(task)) — server generates embedding from text
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
public readonly struct PackedParam
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
        throw new System.InvalidOperationException(
            $"Query parameter index {index} exceeds maximum ({MaxIndex}). " +
            "Simplify the query or reduce the number of IN terms.");
    }
}

/// <summary>Single parameter reference — either a literal (value cached) or a parameter
/// name for blittable lookup. Leaf type with no nesting.</summary>
public sealed class ParameterBinding
{
    /// <summary>Cached native value for literals (long/double/string). Null for parameters
    /// and for literal nulls. Only valid when LiteralType != Parameter.</summary>
    public object LiteralValue;

    /// <summary>Type of the value. ParamValueType.Parameter means look up ParameterName
    /// in the blittable at execution time. Other values mean LiteralValue is cached.</summary>
    public ParamValueType LiteralType;

    /// <summary>For parameters: name to look up in blittable ("p0"). Null for literals.</summary>
    public string ParameterName;
}

/// <summary>Named binding indices per clause type. Each clause type stores its
/// parameter bindings in a flat array at these known positions.</summary>
public static class BindingIndex
{
    // Equals, NotEquals, Range (GT/GTE/LT/LTE), StartsWith, EndsWith, Search, Regex:
    public const int Value = 0;

    // Between:
    public const int BetweenLow = 0;
    public const int BetweenHigh = 1;

    // Spatial circle: [0]=distErrPct, [1]=radius, [2]=lat, [3]=lng, [4]=units
    public const int SpatialCircleBindingCount = 5; // distErrPct + radius + lat + lng + units
    public const int SpatialDistErrPct = 0;
    public const int SpatialRadius = 1;
    public const int SpatialLatitude = 2;
    public const int SpatialLongitude = 3;
    public const int SpatialUnits = 4;
    // Spatial WKT: [0]=distErrPct, [1]=wkt, [2]=units
    public const int SpatialWkt = 1;
    public const int SpatialWktUnits = 2;

    // Vector: [0]=vectorValue, [1]=minimumMatch, [2]=numberOfCandidates, [3]=aiTaskName
    public const int VectorValue = 0;
    public const int VectorMinMatch = 1;
    public const int VectorCandidates = 2;
    public const int VectorAiTask = 3;

    // IN/AllIn: [0..N] = each term binding (array params expand at resolution time)
}

/// <summary>Predicate types for the query plan clause list.</summary>
public enum ClauseType : byte
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
/// <summary>
/// Structural template for a single WHERE predicate. Immutable after first parse,
/// cached on ClauseTemplate, shared across all executions of the same query text.
/// Per-execution data lives in the parallel <see cref="ClauseExecution"/> array.
/// </summary>
public sealed class ClauseInfo
{
    public string FieldName;
    public ClauseType ClauseType;
    public int OriginalIndex;
    public bool IsNegated;
    public bool IsExact;
    public int SearchOperator; // for Search (AND=1/OR=0)
    public SpatialOperationType SpatialMethodType;
    public VectorSourceKind VectorMethod;

    /// <summary>Set for NotEquals clauses appearing in OR chains.
    /// Example: WHERE Name != 'a' OR Age = 25
    /// The NOT(Name='a') term cannot use the raw posting list (which contains entries
    /// WITH 'a', not entries WITHOUT 'a'). Instead, ResolveMatches pre-materializes
    /// AllEntries ANDNOT TermQuery('a') into a BitmapMatch, so FillFromMatch during
    /// execution correctly ORs in the complement set.</summary>
    public bool IsOrChainNotEquals;

    public List<ClauseInfo> OrSubClauses;
    public List<ClauseInfo> AndSubClauses;

    /// <summary>Parameter bindings indexed by <see cref="BindingIndex"/> constants.</summary>
    public ParameterBinding[] Bindings;

    /// <summary>Binding for the boost factor (if wrapped in boost()). Null if not boosted.</summary>
    public ParameterBinding BoostBinding;
}

/// <summary>Per-execution state for a clause. Parallel to <see cref="ClauseInfo"/>[] —
/// populated by PopulateClauseValues each execution, never cached.
/// Also used for operand reordering (Cardinality) and plan emission (InTermCount).</summary>
public sealed class ClauseExecution
{
    public PackedParam PackedParamValue = PackedParam.None;
    public ParamValueType TermValueType;
    public long Cardinality = -1;
    public int InTermCount;
    public bool HasNullTerm;
    public float BoostFactor;
    public SpatialParams Spatial;
    public VectorParams Vector;

    /// <summary>Per-execution state for OrGroup sub-clauses. Parallel to <see cref="ClauseInfo.OrSubClauses"/>.</summary>
    public ClauseExecution[] OrSubExecutions;

    /// <summary>Per-execution state for AndGroup sub-clauses. Parallel to <see cref="ClauseInfo.AndSubClauses"/>.</summary>
    public ClauseExecution[] AndSubExecutions;
}

/// <summary>Per-execution spatial query parameters. Resolved from ParameterBinding during
/// PopulateClauseValues — scalar params looked up by name in the blittable.
/// Shape construction (spatialField.ReadCircle / ReadShape) also runs at execution time
/// because it needs the spatial field factory from builderParameters.</summary>
public sealed class SpatialParams
{
    public double DistanceErrorPct = -1; // -1 = use default
    public bool IsCircle;                // true = circle, false = WKT
    // Circle parameters
    public double CircleRadius;
    public double CircleLatitude;
    public double CircleLongitude;
    // WKT parameter
    public string Wkt;
    public Corax.Utils.Spatial.SpatialUnits? Units;
}

/// <summary>Per-execution vector query parameters. Resolved from ParameterBinding during
/// PopulateClauseValues — scalar params from blittable lookup, vector payload passed
/// through as-is. Embedding construction (base64 decode, AI embedding generation)
/// also runs at execution time.</summary>
public sealed class VectorParams
{
    public float MinimumMatch = -1;      // -1 = use index default
    public int NumberOfCandidates = -1;  // -1 = use index default
    public object ResolvedValue;         // the raw vector parameter (string, BlittableJsonReaderArray, etc.)
    public ParamValueType ResolvedValueType;
    public VectorSourceKind Method;      // which embedding method, if any
    public string AiTaskName;            // AI task identifier for embedding.text
}

/// <summary>Immutable structural template built on first execution of a query text.
/// Cached on PerQueryPlans.Template. On cache hit, clauses are cloned and their
/// per-execution fields overwritten by PopulateParameters.</summary>
public sealed class ClauseTemplate
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
