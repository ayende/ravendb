namespace Corax.Querying.Planning;

/// <summary>Spatial operation type. Corax-side equivalent of the Raven.Server
/// MethodType enum — only the spatial operations.</summary>
public enum SpatialOperationType : byte
{
    Within,
    Contains,
    Disjoint,
    Intersects
}