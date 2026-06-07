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

    /// <summary>True when this execution has been collapsed to a <see cref="ClauseType.MatchAll"/> or
    /// <see cref="ClauseType.MatchNothing"/> sentinel. Sentinels emit no match leaf, consume no
    /// cardinality slot, and resolve to a bitmap fill/clear in the plan emitter — so every leaf-counting
    /// pass (emitter match cursor, leaf resolution, cardinality array, inspection flattening) must skip them.</summary>
    public bool IsSentinel => ClauseType is ClauseType.MatchAll or ClauseType.MatchNothing;

    /// <summary>Collapse this execution to a sentinel: a clause that statically resolves to match-all or
    /// match-nothing. Clears <see cref="IsNegated"/> because the sentinel already subsumes the clause's
    /// polarity (a dropped clause's negation is resolved into the MatchAll/MatchNothing choice), and presets
    /// <see cref="Cardinality"/> so the cardinality estimator is skipped (MatchNothing sorts first → AND
    /// short-circuits; MatchAll sorts last → AND no-op / OR absorb).</summary>
    public void MarkAsSentinel(ClauseType sentinel, long cardinality)
    {
        ClauseType = sentinel;
        IsNegated = false;
        Cardinality = cardinality;
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
