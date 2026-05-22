using System;

namespace Corax.Querying.Planning;

/// <summary>Per-execution state for a clause. Holds a back-reference to its
/// <see cref="ClauseInfo"/> so the pair can be sorted / reordered without maintaining
/// parallel arrays. Populated by PopulateClauseValues each execution (not cached).
/// Implements <see cref="IComparable{ClauseExecution}"/> for cardinality-based
/// operand reordering (negated clauses sort last, ties broken by ascending cardinality).</summary>
public sealed class ClauseExecution : IComparable<ClauseExecution>
{
    /// <summary>Back-reference to the template clause this execution belongs to.</summary>
    public ClauseInfo Clause;

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
    public ClauseType EffectiveClauseType;

    /// <summary>Negation flag for this execution. Initialized from
    /// <see cref="ClauseInfo.IsNegated"/> at creation; mutable for per-execution rewrites
    /// (e.g. standalone NotEquals marking).</summary>
    public bool EffectiveIsNegated;

    /// <summary>Per-execution state for OrGroup subclauses. Parallel to <see cref="ClauseInfo.OrSubClauses"/>.</summary>
    public ClauseExecution[] OrSubExecutions;

    /// <summary>Per-execution state for AndGroup subclauses. Parallel to <see cref="ClauseInfo.AndSubClauses"/>.</summary>
    public ClauseExecution[] AndSubExecutions;

    /// <summary>Negated clauses sort last; ties broken by ascending cardinality.</summary>
    public int CompareTo(ClauseExecution other)
    {
        bool aNeg = EffectiveIsNegated || EffectiveClauseType == ClauseType.NotEquals;
        bool bNeg = other.EffectiveIsNegated || other.EffectiveClauseType == ClauseType.NotEquals;
        if (aNeg != bNeg)
            return aNeg ? 1 : -1;
        return Cardinality.CompareTo(other.Cardinality);
    }
}
