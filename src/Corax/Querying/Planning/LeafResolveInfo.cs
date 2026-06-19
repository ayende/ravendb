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

    /// <summary>Range-cardinality calibration EWMA for this leaf's clause, set only for calibrated
    /// range tree-scans (BETWEEN / GT / GTE / LT / LTE / StartsWith). Null for every other leaf
    /// (including the other tree-scan shapes — Exists / EndsWith / Regex — whose estimate is just
    /// NumberOfEntries, with no EstimateMatchesInRange to calibrate). When non-null and the tree-scan
    /// fill is unbounded, the consuming primitive feeds the measured over-counting postings tally back
    /// via <c>RangeCalibration.Observe(tally, RangeEstimate)</c> so the estimator self-corrects over
    /// time. See <see cref="InflationEwma"/> and IndexSearcher.EstimateMatchesInRange.</summary>
    public InflationEwma RangeCalibration;

    /// <summary>The cardinality the estimator predicted for this clause — the over-counting postings
    /// sum EstimateMatchesInRange returns, the same quantity the fill tally measures. Paired with the
    /// measured tally as the "predicted" argument of <see cref="RangeCalibration"/>.Observe. Meaningful
    /// only when <see cref="RangeCalibration"/> is non-null.</summary>
    public long RangeEstimate;
}
