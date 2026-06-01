namespace Corax.Querying.Planning;

/// <summary>Predicate types for the query plan clause list.</summary>
public enum ClauseType : byte
{
    Equals,
    NotEquals,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    Between,
    In,
    AllIn,
    Exists,
    StartsWith,
    EndsWith,
    Search,
    Regex,
    Spatial,
    Vector,
    OrGroup,  // A group of OR'd subclauses
    AndGroup, // A group of AND'd subclauses inside an OR chain

    // Per-execution collapse sentinels. Never present on a template ClauseInfo; stamped onto a
    // ClauseExecution when the clause statically resolves to "matches every doc" (MatchAll) or
    // "matches no doc" (MatchNothing) — e.g. WHEN(false), a statically-true exists()/NOT exists(),
    // an empty IN, or a contradictory BETWEEN. The plan emitter turns them into a bitmap
    // FillAllEntries / ClearBitmap, so they consume no match leaf and no cardinality slot.
    MatchAll,
    MatchNothing,
}