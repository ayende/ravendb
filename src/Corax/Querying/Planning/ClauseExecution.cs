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

    /// <summary>Per-execution override for <see cref="ClauseInfo.ClauseType"/>. When non-null,
    /// execution code uses this instead of the template's ClauseType. Set by
    /// <c>PropagateBetweenContradictions</c> to rewrite a contradictory BETWEEN to
    /// empty-IN without cloning the frozen template ClauseInfo.</summary>
    public ClauseType? EffectiveClauseType;

    /// <summary>Per-execution override for <see cref="ClauseInfo.IsNegated"/>. When non-null,
    /// execution code uses this instead of the template's IsNegated. Set by
    /// <c>EmitPlan</c> for standalone NotEquals so that downstream ResolveMatches /
    /// ResolveTermSources see the clause as negated without cloning the frozen template.</summary>
    public bool? EffectiveIsNegated;

    /// <summary>Per-execution state for OrGroup subclauses. Parallel to <see cref="ClauseInfo.OrSubClauses"/>.</summary>
    public ClauseExecution[] OrSubExecutions;

    /// <summary>Per-execution state for AndGroup subclauses. Parallel to <see cref="ClauseInfo.AndSubClauses"/>.</summary>
    public ClauseExecution[] AndSubExecutions;

    /// <summary>Negated clauses sort last; ties broken by ascending cardinality.</summary>
    public int CompareTo(ClauseExecution other)
    {
        bool aNeg = Clause.IsNegated || Clause.ClauseType == ClauseType.NotEquals;
        bool bNeg = other.Clause.IsNegated || other.Clause.ClauseType == ClauseType.NotEquals;
        if (aNeg != bNeg)
            return aNeg ? 1 : -1;
        return Cardinality.CompareTo(other.Cardinality);
    }
}
