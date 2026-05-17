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
}