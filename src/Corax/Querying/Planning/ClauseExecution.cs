using System;
using System.Collections.Generic;

namespace Corax.Querying.Planning;

/// <summary>Per-execution state for a clause. Holds a back-reference to its
/// <see cref="ClauseInfo"/> so the pair can be sorted / reordered without maintaining
/// parallel arrays. Populated by PopulateClauseValues each execution (not cached).
/// Implements <see cref="IComparable{ClauseExecution}"/> for cardinality-based
/// operand reordering (negated clauses sort last, ties broken by ascending cardinality).</summary>
public sealed class ClauseExecution(ClauseInfo clause) : IComparable<ClauseExecution>
{
    /// <summary>Back-reference to the template clause this execution belongs to.</summary>
    public readonly ClauseInfo Clause = clause;

    public PackedParam PackedParamValue = PackedParam.None;
    public ParamValueType TermValueType;
    public long Cardinality = -1;
    public int InTermCount;
    public bool HasNullTerm;
    public float BoostFactor;
    public SpatialParams Spatial;
    public VectorParams Vector;

    public ClauseType? SentinelRewriteType;

    /// <summary>Clause type for this execution. Initialized from
    /// <see cref="ClauseInfo.ClauseType"/> at creation; mutable for per-execution rewrites
    /// (e.g. contradictory BETWEEN → empty-IN).</summary>
    public ClauseType ClauseType
    {
        get;
        set
        {
            if (value is ClauseType.NotEquals)
                IsNegated = true;
            field = value;
        }
    } = clause.ClauseType;

    /// <summary>Negation flag for this execution. Initialized from
    /// <see cref="ClauseInfo.IsNegated"/> at creation; mutable for per-execution rewrites
    /// (e.g. standalone NotEquals marking).</summary>
    public bool IsNegated = clause.IsNegated;

    /// <summary>Per-execution state for OrGroup/AndGroup subclauses. Parallel to <see cref="ClauseInfo.SubClauses"/>.</summary>
    public List<ClauseExecution> SubExecutions;

    /// <summary>Negated clauses sort last; ties broken by ascending cardinality.</summary>
    public int CompareTo(ClauseExecution other)
    {
        if (IsNegated != other.IsNegated)
            return IsNegated ? 1 : -1;
        return Cardinality.CompareTo(other.Cardinality);
    }
}
