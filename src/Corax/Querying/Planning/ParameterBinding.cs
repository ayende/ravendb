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
}