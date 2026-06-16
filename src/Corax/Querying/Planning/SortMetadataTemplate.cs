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

    /// <summary>
    /// Per-position flag (aligned to <see cref="Prebuilt"/>): true when this ORDER BY key is a field sort
    /// pinned to a constant by a top-level, unguarded, non-negated equality clause on the same field (with
    /// an AND root, not OR). Every result then shares one value for that key, so it contributes nothing to
    /// the order. When EVERY position is pinned AND each pinned field is single-valued at query time, the
    /// whole sort is a no-op and is elided (<c>MaterializeSortMetadata</c> returns null, dropping the
    /// SortingMatch wrapper). Single-valuedness is baked into the plan cache key, so a field that later
    /// becomes multi-valued re-plans rather than reusing this elision. Null when no position is pinnable.
    /// </summary>
    public bool[] PinnedByEquality { get; init; }
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

/// <summary>Per-slot patch kind. See <see cref="SortSlotPatch"/>.</summary>
public enum SortSlotPatchKind : byte
{
    /// <summary>Slot is fully baked — runtime returns the prefab entry verbatim.</summary>
    None = 0,

    /// <summary>Field-backed sort slot. <see cref="FieldMetadata"/> holds transaction-bound slices, so it must be
    /// re-resolved every query. If the field has zero distinct terms, the slot is flagged
    /// <see cref="OrderMetadata.MayHaveMissingEntries"/> so SortingMatch routes through InMemorySort (every doc
    /// is treated as missing) instead of walking a non-existent term tree.</summary>
    FieldRuntimeResolve,

    /// <summary>Random ordering with no Arguments — need a new seed each query.</summary>
    RandomFreshSeed,

    /// <summary>Random ordering seeded by a query parameter (<c>random($p)</c>) — the seed depends on the
    /// per-query parameter value, so it cannot be baked. <see cref="SortSlotPatch.FieldName"/> holds the parameter name.</summary>
    RandomSeededByParam,

    /// <summary>Distance ordering whose lat/lng/wkt arguments are parameter-bound. If the values are fixed to the query, the sort kind is None.</summary>
    DistanceRuntime,
}
