using Corax.Mappings;
using Corax.Utils;

namespace Corax.Querying.Planning;

/// <summary>Per-template snapshot of sort-metadata work. Built once at template-build
/// time (by Raven.Server's <c>QueryPlanBuilder.BuildSortMetadataTemplate</c>); consumed
/// per query by the matching <c>MaterializeSortMetadata</c>. All template-stable
/// decisions (field-metadata resolution, ordering-type classification, nulls-sort-mode
/// derivation, the implicit-time→long rewrite, spatial-unit parsing for WKT constants,
/// the per-field <c>FieldId</c> dynamic-field check) are baked into <see cref="Prebuilt"/>.
/// Per-query runtime work is reduced to: PageSize == 0 short-circuit, optional per-field
/// empty-term check, fresh <c>Random.Shared.Next()</c> seed when the query uses
/// argument-less random ordering, and parameter resolution for Distance args that
/// reference query parameters.</summary>
public sealed class SortMetadataTemplate
{
    /// <summary>True when the query has no ORDER BY at all and the implicit-score
    /// auto-promotion path doesn't apply — runtime returns null without further work.</summary>
    public bool NoSort { get; init; }

    /// <summary>True when no ORDER BY but <c>HasBoost</c> + config flag would auto-promote
    /// to ORDER BY score(). Runtime allocates the singleton score array and asserts
    /// (unless the query is a vector search — see <see cref="ImplicitScoreSkipAssert"/>).</summary>
    public bool ImplicitScore { get; init; }

    /// <summary>When <see cref="ImplicitScore"/> is true, indicates the assertion can be
    /// skipped because the query already uses vector search (which has its own
    /// score-ordering semantics).</summary>
    public bool ImplicitScoreSkipAssert { get; init; }

    /// <summary>Pre-built per-slot OrderMetadata. Slots that need runtime patching
    /// are stored with their template-stable defaults (e.g. <c>FieldHasNoTerms = false</c>
    /// for field slots that still need the empty-term check). When no slot needs
    /// any runtime patching and the query has no PageSize-driven short circuit,
    /// this array can be returned directly.</summary>
    public OrderMetadata[] Prebuilt { get; init; }

    /// <summary>Per-slot patch directives parallel to <see cref="Prebuilt"/>.
    /// Null when no slot needs patching (hot path — return <see cref="Prebuilt"/> directly).</summary>
    public SortSlotPatch[] Patches { get; init; }

    /// <summary>True when at least one <see cref="Patches"/> entry has
    /// <see cref="SortSlotPatchKind.FieldEmptyCheck"/>. Lets the runtime materializer
    /// skip the patch loop entirely when no empty checks are pending.</summary>
    public bool AnyEmptyCheckPending { get; init; }
}

/// <summary>Per-query Distance ordering builder. Closed over template-time data
/// (the source ORDER BY descriptor + resolved field metadata) at template-build time;
/// invoked at runtime with the per-query opaque context (a <c>QueryBuilderParameters</c>
/// on the Raven.Server side) and the empty-field outcome to produce the slot's
/// <see cref="OrderMetadata"/>. The opaque <see cref="object"/> context keeps this
/// delegate Corax-clean — the consumer-side knows the concrete type.</summary>
public delegate OrderMetadata SortDistanceMetadataBuilder(object runtimeContext, bool fieldIsEmpty);

/// <summary>Per-slot runtime patch directive. Populated only when at least one slot
/// needs runtime work; otherwise the entire array stays null and the caller returns
/// <see cref="SortMetadataTemplate.Prebuilt"/> directly.</summary>
public struct SortSlotPatch
{
    /// <summary>Patch kind. <see cref="SortSlotPatchKind.None"/> when this slot is
    /// fully baked and the corresponding <see cref="SortMetadataTemplate.Prebuilt"/>
    /// entry can be used as-is.</summary>
    public SortSlotPatchKind Kind;

    /// <summary>For <see cref="SortSlotPatchKind.FieldEmptyCheck"/> and
    /// <see cref="SortSlotPatchKind.DistanceRuntime"/>: field metadata to query against
    /// <c>IndexSearcher.GetDistinctTermCountInField</c>. Already resolved at template
    /// time so the runtime patch avoids the <c>GetFieldIdForOrderBy</c> allocator +
    /// dynamic-field lookup.</summary>
    public FieldMetadata FieldMeta;

    /// <summary>For <see cref="SortSlotPatchKind.DistanceRuntime"/>: closure over
    /// template-time data (the source <c>OrderByField</c>) that resolves per-query
    /// parameter values and returns the slot's <see cref="OrderMetadata"/>. Null for
    /// non-Distance patches.</summary>
    public SortDistanceMetadataBuilder DistanceBuilder;
}

/// <summary>Per-slot patch kind. See <see cref="SortSlotPatch"/>.</summary>
public enum SortSlotPatchKind : byte
{
    /// <summary>Slot is fully baked — runtime returns the prefab entry verbatim.</summary>
    None = 0,

    /// <summary>Field slot may have zero distinct terms in the index. Runtime calls
    /// <c>GetDistinctTermCountInField</c> with <see cref="SortSlotPatch.FieldMeta"/>;
    /// if zero, the slot rebuilds with <c>FieldHasNoTerms = true</c> (and is either
    /// dropped from the result or marked, per sharded/non-sharded policy).</summary>
    FieldEmptyCheck,

    /// <summary>Random ordering with no Arguments — runtime fills the slot with
    /// <c>new OrderMetadata(Random.Shared.Next())</c> each query (intentional per-query
    /// shuffle).</summary>
    RandomFreshSeed,

    /// <summary>Distance ordering whose lat/lng/wkt arguments are parameter-bound.
    /// Runtime invokes <see cref="SortSlotPatch.DistanceBuilder"/> with the per-query
    /// context. Distance slots with constant arguments are baked into
    /// <see cref="SortMetadataTemplate.Prebuilt"/> and use <see cref="None"/> instead.</summary>
    DistanceRuntime,
}
