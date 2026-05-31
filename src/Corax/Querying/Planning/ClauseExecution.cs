using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Corax.Querying.Planning;

/// <summary>Per-execution state for a clause. Holds a back-reference to its
/// <see cref="ClauseInfo"/> so the pair can be sorted / reordered without maintaining
/// parallel arrays. Populated by PopulateClauseValues each execution (not cached).
/// Implements <see cref="IComparable{ClauseExecution}"/> for cardinality-based
/// operand reordering (negated clauses sort last, ties broken by ascending cardinality).</summary>
public sealed class ClauseExecution : IComparable<ClauseExecution>
{
    /// <summary>Back-reference to the template clause this execution belongs to.</summary>
    public readonly ClauseInfo Clause;

    public PackedParam PackedParamValue = PackedParam.None;
    public ParamValueType TermValueType;
    public long Cardinality = -1;
    public int InTermCount;
    public bool HasNullTerm;
    public float BoostFactor;
    public SpatialParams Spatial;
    public VectorParams Vector;

    public ClauseType? SentinelRewriteType;

    /// <summary>True when this clause — or a clause in its subtree — is a BETWEEN whose sentinel bound ("*"/"NULL") was delivered by a query parameter. This changes the relevant cached query plan</summary>
    public bool HasParameterSentinel;

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
    }

    /// <summary>Negation flag for this execution. Initialized from
    /// <see cref="ClauseInfo.IsNegated"/> at creation; mutable for per-execution rewrites
    /// (e.g. standalone NotEquals marking).</summary>
    public bool IsNegated;

    /// <summary>Per-execution state for OrGroup/AndGroup subclauses. Parallel to <see cref="ClauseInfo.SubClauses"/>.</summary>
    public List<ClauseExecution> SubExecutions;

    public ClauseExecution(ClauseInfo clause)
    {
        Clause = clause;
        IsNegated = clause.IsNegated;
        ClauseType = clause.ClauseType;
    }

    /// <summary>Negated clauses sort last; ties broken by ascending cardinality.</summary>
    public int CompareTo(ClauseExecution other)
    {
        if (IsNegated != other.IsNegated)
            return IsNegated ? 1 : -1;
        return Cardinality.CompareTo(other.Cardinality);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public long GetEffectiveCardinality(IndexSearcher indexSearcher) => Cardinality > 0 ? Cardinality : indexSearcher.NumberOfEntries;
}
