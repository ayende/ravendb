using System;
using Sparrow.Json;

namespace Corax.Querying.Planning;

/// <summary>Explicit classification of a binding's resolution path. Set at template
/// creation time. Eliminates the need for callers to infer source kind from
/// field combinations (LiteralType, ParameterName, DeferredExpression).</summary>
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
    /// <summary>How this binding resolves. Set at creation time in ParseComparison / ParseIn /
    /// CreateBinding. Replaces the implicit inference from LiteralType == Parameter or
    /// DeferredExpression != null.</summary>
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
    /// once at template-build time. -1 for literal/deferred bindings (no slot). Lets the FullKinds sentinel
    /// marker write directly into the slot byte without an Array.IndexOf lookup at query time.</summary>
    public int ParameterSlot = -1;

    /// <summary>For deferred method expressions (e.g. cmpxchg(), now(), today()) that must
    /// be resolved at execution time rather than template creation time. The first parameter
    /// is the QueryBuilderParameters boxed as object (cast at the resolution site in
    /// Raven.Server); the second is the query's BlittableJsonReaderObject.
    /// Returns the resolved native value, or null for null values.</summary>
    public Func<object, BlittableJsonReaderObject, object> DeferredExpression;
}