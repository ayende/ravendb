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

/// <summary>Embedding method kind for vector queries.</summary>
public enum VectorMethodKind : byte
{
    None,           // direct value (not a method call)
    ForDocument,    // embedding.forDoc(docId)
    ForRaw,         // embedding.forRaw(base64)
    EmbeddingText,  // embedding.text(text, ai.task(taskName))
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

/// <summary>Describes how to resolve a clause's parameter value without the AST.
/// For literals: the native value is cached directly.
/// For parameters: the parameter name is stored for blittable lookup.</summary>
public sealed class ParameterBinding
{
    public bool IsLiteral;
    public object LiteralValue;       // cached native value for literals (long/double/string)
    public ParamValueType LiteralType;
    public string ParameterName;      // for parameters: name to look up in blittable ("p0")
    public bool IsArrayParameter;     // true if this parameter may resolve to an array (IN terms)

    /// <summary>Second binding for BETWEEN high bound. Null for non-BETWEEN clauses.</summary>
    public ParameterBinding Second;

    /// <summary>For IN/AllIn: bindings for each term. Null for non-IN clauses.</summary>
    public ParameterBinding[] InBindings;

    /// <summary>For Spatial: bindings for shape arguments.
    /// For Vector (reused): [0]=minimumMatch, [1]=numberOfCandidates, [2]=aiTask.</summary>
    public ParameterBinding[] SpatialBindings;

    /// <summary>For Vector: binding for the vector value argument.</summary>
    public ParameterBinding VectorValueBinding;
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
public sealed class ClauseInfo
{
    public string FieldName;
    public ParamValueType TermValueType; // for type-aware scan predicate building

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
    public SpatialOperationType SpatialMethodType; // for spatial: Within/Contains/Disjoint/Intersects

    /// <summary>Pre-resolved vector parameters. Null for non-vector clauses.</summary>
    public VectorParams Vector;
    public ClauseType ClauseType;
    public long Cardinality = -1;
    public int OriginalIndex;
    public bool IsNegated;
    public bool IsExact;
    public float BoostFactor;

    public int SearchOperator; // for Search (AND=1/OR=0) — uses int to avoid Corax.Constants dependency

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
            Vector = Vector != null ? new VectorParams { Method = Vector.Method } : null,
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
    // Shared — stored as enum value from Raven.Client.Documents.Indexes.Spatial.SpatialUnits
    // (Kilometers=0, Miles=1). Null = use spatial field default.
    public int? Units;
}

/// <summary>Pre-resolved vector query parameters. All scalar values and the raw vector
/// payload are extracted from the AST during parsing. Embedding construction
/// (base64 decode, AI embedding generation) still runs at execution time.</summary>
public sealed class VectorParams
{
    public float MinimumMatch = -1;      // -1 = use index default
    public int NumberOfCandidates = -1;  // -1 = use index default
    public object ResolvedValue;         // the raw vector parameter (string, BlittableJsonReaderArray, etc.)
    public ParamValueType ResolvedValueType;
    public VectorMethodKind Method;      // which embedding method, if any
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
