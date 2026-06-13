using System;
using Sparrow.Json;

namespace Corax.Querying.Planning;

/// <summary>Explicit classification of a binding's resolution path, set at template-creation time.</summary>
public enum BindingSource : byte
{
    /// <summary>LiteralValue + LiteralType hold the resolved value. No runtime resolution needed.</summary>
    Literal,
    /// <summary>ParameterName → blittable lookup at execution time. May resolve to scalar or array.</summary>
    QueryParameter,
    /// <summary>DeferredExpression → evaluate at execution time (cmpxchg, now, today).</summary>
    DeferredMethod,
}

/// <summary>Single parameter reference — either a literal (value cached) or a parameter
/// name for blittable lookup. Leaf type with no nesting.</summary>
public sealed class ParameterBinding
{
    /// <summary>How this binding resolves.</summary>
    public BindingSource Source;
    /// <summary>Cached native value for literals (long/double/string). Null for parameters
    /// and for literal nulls. Only valid when LiteralType != Parameter.</summary>
    public object LiteralValue;

    /// <summary>Type of the value. ParamValueType.Parameter means look up ParameterName
    /// in the blittable at execution time. Other values mean LiteralValue is cached.</summary>
    public ParamValueType LiteralType;

    /// <summary>For parameters: name to look up in blittable ("p0"). Null for literals.</summary>
    public string ParameterName;

    /// <summary>Index of this binding's parameter in <see cref="PlanTemplate.ParameterSlots"/>, assigned
    /// once at template-build time. -1 for literal/deferred bindings (no slot).</summary>
    public int ParameterSlot = -1;

    /// <summary>Position of this binding in the canonical value-leaf order (the left-to-right DFS order in
    /// which <c>CreateBinding</c> is invoked while parsing the WHERE expression). Every value-bearing binding —
    /// literal, query parameter, or deferred method — gets a sequential hole index. This is the auto-parameterization
    /// slot: the shared template carries only the hole index, while each query's per-text slot vector (built by the
    /// same DFS) supplies the actual literal value / parameter name / deferred expression at <c>HoleIndex</c>.
    /// -1 until assigned.</summary>
    public int HoleIndex = -1;

    /// <summary>For deferred method expressions (e.g. cmpxchg(), now(), today()) that must
    /// be resolved at execution time rather than template creation time. The first parameter
    /// is the QueryBuilderParameters boxed as object (cast at the resolution site in
    /// Raven.Server); the second is the query's BlittableJsonReaderObject.
    /// Returns the resolved native value, or null for null values.</summary>
    public Func<object, BlittableJsonReaderObject, object> DeferredExpression;
}
