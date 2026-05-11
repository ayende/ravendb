namespace Corax.Querying.Planning;

public enum SpatialShapeType : byte
{
    Circle,
    Wkt
}

/// <summary>Per-execution spatial query parameters. Resolved from ParameterBinding during
/// PopulateClauseValues — scalar params looked up by name in the blittable.
/// Shape construction (spatialField.ReadCircle / ReadShape) also runs at execution time
/// because it needs the spatial field factory from builderParameters.</summary>
public sealed class SpatialParams
{
    public double DistanceErrorPct = -1; // -1 = use default
    public SpatialShapeType ShapeType;
    // Circle parameters
    public double CircleRadius;
    public double CircleLatitude;
    public double CircleLongitude;
    // WKT parameter
    public string Wkt;
    public Utils.Spatial.SpatialUnits? Units;
}
