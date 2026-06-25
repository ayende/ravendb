using Corax.Mappings;
using Corax.Utils;

namespace Corax.Querying.Planning;

public sealed class SortMetadataTemplate
{
    public bool NoSort { get; init; }

    /// <summary>True when no ORDER BY but <c>HasBoost</c>.</summary>
    public bool ImplicitScore { get; init; }

    public bool HasVectorSearch { get; init; }

    public OrderMetadata[] Prebuilt { get; init; }

    /// <summary>Per-query patches for slots whose <see cref="OrderMetadata"/> must be re-resolved each query.</summary>
    public SortSlotPatch[] Patches { get; init; }
}

public delegate OrderMetadata SortDistanceMetadataBuilder(object runtimeContext, FieldMetadata fieldMeta);

public struct SortSlotPatch
{
    public SortSlotPatchKind Kind;
    public string FieldName;
    /// <summary>
    ///  To compute distance for spatial queries, we need to resolve the per-query center point from the parameters.  
    /// </summary>
    public SortDistanceMetadataBuilder DistanceBuilder;
}

public enum SortSlotPatchKind : byte
{
    /// <summary>Slot is fully baked — runtime returns the prefab entry verbatim.</summary>
    None = 0,

    /// <summary>Field-backed sort slot holds transaction-bound slices, so it must be re-resolved every query.</summary>
    FieldRuntimeResolve,

    /// <summary>Random ordering with no Arguments — need a new seed each query.</summary>
    RandomFreshSeed,

    /// <summary>Random ordering seeded by a query parameter (<c>random($p)</c>), need to read that via the FieldName.</summary>
    RandomSeededByParam,

    /// <summary>Distance ordering whose lat/lng/wkt arguments are parameter-bound. If the values are literals in the query, the sort kind is None.</summary>
    DistanceRuntime,
}
