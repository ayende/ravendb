namespace Corax.Utils.Spatial;

/// <summary>Simple value-type (longitude/latitude) point used in ORDER BY distance
/// computations. Replaces the <c>Spatial4n.Shapes.IPoint</c> reference type in
/// <see cref="Corax.Utils.OrderMetadata"/> so that spatial ordering does not carry
/// a heap-allocated Spatial4n shape object.</summary>
public readonly struct SpatialPoint
{
    public readonly double X; // longitude
    public readonly double Y; // latitude

    public SpatialPoint(double x, double y)
    {
        X = x;
        Y = y;
    }

    public override string ToString() =>
        string.Format("Pt(x={0:0.0#############},y={1:0.0#############})", X, Y);
}
