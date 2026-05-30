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

    /// <summary>
    /// If we need to update the slot's <see cref="OrderMetadata"/> per query, this is stored here
    /// </summary>
    public SortSlotPatch[] Patches { get; init; }
}

public delegate OrderMetadata SortDistanceMetadataBuilder(object runtimeContext, FieldMetadata fieldMeta, bool fieldIsEmpty);

public struct SortSlotPatch
{
    public SortSlotPatchKind Kind;
    public string FieldName;
    /// <summary>
    ///  To compute distance for spatial queries, we need to resolve the per-query center point from the parameters.  
    /// </summary>
    public SortDistanceMetadataBuilder DistanceBuilder;
}

/// <summary>Per-slot patch kind. See <see cref="SortSlotPatch"/>.</summary>
public enum SortSlotPatchKind : byte
{
    /// <summary>Slot is fully baked — runtime returns the prefab entry verbatim.</summary>
    None = 0,

    /// <summary>Field slot may have zero distinct terms in the index. We need to check this at runtime.</summary>
    FieldEmptyCheck,

    /// <summary>Random ordering with no Arguments — need a new seed each query.</summary>
    RandomFreshSeed,

    /// <summary>Distance ordering whose lat/lng/wkt arguments are parameter-bound. If the values are fixed to the query, the sort kind is None.</summary>
    DistanceRuntime,
}
