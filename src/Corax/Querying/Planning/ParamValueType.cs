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
    /// Maps to ValueTokenType.Parameter at the `Raven.Server` boundary.</summary>
    Parameter
}
