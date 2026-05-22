using System;
using Sparrow.Json;

namespace Corax.Querying.Planning;

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

    /// <summary>For deferred method expressions (e.g. cmpxchg(), now(), today()) that must
    /// be resolved at execution time rather than template creation time. The first parameter
    /// is the QueryBuilderParameters boxed as object (cast at the resolution site in
    /// Raven.Server); the second is the query's BlittableJsonReaderObject.
    /// Returns the resolved native value, or null for null values.</summary>
    public Func<object, BlittableJsonReaderObject, object> DeferredExpression;
}