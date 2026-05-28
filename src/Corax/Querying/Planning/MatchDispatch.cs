namespace Corax.Querying.Planning;

/// <summary>Emit-time-only selector for a leaf op's execution-time source. No longer stored
/// on <see cref="PlanOp"/> or <see cref="CompiledPlan"/> — it is folded into the concrete
/// <see cref="PlanOpKind"/> (the <c>…From{PostingSource,TreeScan,Match}</c> families) by
/// Raven.Server's <c>ToPlanOpKind</c>, and consumed transiently inside the emitter to pick the
/// right array for range bounds-check hints.</summary>
public enum MatchDispatch : byte
{
    /// <summary>IQueryMatch.Fill() dispatch — the general-purpose path for spatial,
    /// vector, search, boosted, and any clause that can't be expressed as a posting
    /// list or tree scan.</summary>
    QueryMatch,

    /// <summary>Native posting-list dispatch — a single resolved posting list
    /// (Single / SmallPostingList / PostingList.Iterator). The fastest path.
    /// Used for Equals and NotEquals clauses.</summary>
    PostingList,

    /// <summary>CompactTree scan — iterates the tree at execution time, decoding each
    /// matching posting list directly into the bitmap. Used for StartsWith, EndsWith,
    /// Contains, Exists, Regex, and range clauses.</summary>
    TreeScan,
}