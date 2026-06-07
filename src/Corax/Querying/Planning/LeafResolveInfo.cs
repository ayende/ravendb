using Corax.Mappings;

namespace Corax.Querying.Planning;

/// <summary>How a leaf slot's bitmap source is materialized at execution time from
/// <see cref="LeafResolveInfo"/>. The IL pipeline resolves the concrete posting source
/// / terms provider lazily inside the Ctx* primitives, so Raven.Server only hands over
/// the value-independent metadata (field + packed parameter) instead of pre-decoding
/// every posting list up front.</summary>
public enum LeafResolveKind : byte
{
    /// <summary>Slot is served from <see cref="CompiledQueryMatch.ResolvedMatches"/> — the
    /// IQueryMatch path (spatial, vector, search, boosted). <see cref="LeafResolveInfo"/>
    /// carries no resolution data for these slots.</summary>
    PreResolved,

    /// <summary>Native posting list for a concrete term — resolve via
    /// <c>Packed.GetTermPostingListId</c> then <c>PostingSource.Decode</c>.</summary>
    TermPosting,

    /// <summary>Null-term posting list — resolve via <c>IndexSearcher.TryGetPostingListForNull</c>.
    /// Empty when the field has no null posting list.</summary>
    NullPosting,

    /// <summary>Universal pass-through (AllIn's null slot when the clause has no null term):
    /// AND-shaped ops treat it as a no-op.</summary>
    AllPosting,

    /// <summary>Empty posting source (IN's null slot when the clause has no null term, or a
    /// non-existent term): OR-shaped ops no-op, AND-shaped ops clear.</summary>
    EmptyPosting,

    /// <summary>CompactTree scan — resolve an <see cref="ITermsProvider"/> from the clause type
    /// (StartsWith / EndsWith / Exists / Regex / range / non-sentinel Between).</summary>
    TreeScan,
}

/// <summary>Per-leaf resolution descriptor produced by Raven.Server and consumed by the
/// compiled IL pipeline. The value-independent <see cref="FieldMeta"/> and <see cref="Packed"/>
/// are computed once, and the posting source / terms provider is materialized lazily inside
/// <c>QueryPrimitives</c> when the slot is consumed. Parallel to
/// <see cref="CompiledQueryMatch.ResolvedMatches"/>; slots whose <see cref="Kind"/> is
/// <see cref="LeafResolveKind.PreResolved"/> carry no data here.</summary>
public struct LeafResolveInfo
{
    public LeafResolveKind Kind;
    public ClauseType ClauseType;
    public PackedParam Packed;
    public FieldMetadata FieldMeta;
}
