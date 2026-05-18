using System;

namespace Corax.Querying.Planning;

/// <summary>Structural optimization applicability flags. Set at template construction
/// time by analyzing the clause list shape. Used at Instantiate time to skip
/// <c>Try*</c> optimization methods that are guaranteed to fail for this template,
/// avoiding per-query work (Slice allocations, compound-field lookups, etc.).</summary>
[Flags]
public enum PlanOptFlags : byte
{
    None = 0,

    /// <summary>Template has ≥2 non-negated, non-boosted Equals clauses — structural
    /// prerequisite for <c>TryCreateCompoundExactMatch</c>. When clear, the O(n²)
    /// compound-exact scan is skipped entirely.</summary>
    CompoundExactCandidate = 1,

    /// <summary>Template has at least one non-negated, non-boosted range or Equals clause
    /// (GreaterThan, LessThan, Between, Equals, etc.) — structural prerequisite for
    /// <c>TryCreateSimpleFieldDirectScan</c> and <c>TryCreateCompoundFieldMatch</c>.
    /// When clear, both are skipped.</summary>
    DirectScanCandidate = 2,
}

/// <summary>Immutable structural template built on the first execution of a query text.
/// Cached on PerQueryPlans.Template. On cache hit, clauses are cloned and their
/// per-execution fields overwritten by PopulateParameters.</summary>
public sealed class PlanTemplate
{
    /// <summary>Hard cap on WHEN-bearing clauses per template. Bit <c>i</c> of
    /// <see cref="QueryExecution.WhenFlags"/> tracks the <c>i</c>-th WHEN clause's
    /// survival under bound params; with <c>int</c> as the carrier, the maximum
    /// safe count is 32. Realistic workloads have far fewer (~10 in the worst
    /// optional-filter LINQ shapes).</summary>
    public const int MaxWhenClauses = 32;

    public ClauseInfo[] Clauses;
    public bool IsAllEntries;
    public bool IsOr;              // root boolean operator

    /// <summary>Spatial clauses separated from the main filter chain (AND queries only).</summary>
    public ClauseInfo[] SpatialClauses;
    /// <summary>Vector clauses separated from the main filter chain (AND queries only).</summary>
    public ClauseInfo[] VectorClauses;

    /// <summary>Plan-time structural optimization flags. Computed at template
    /// construction and checked at Instantiate time to skip inapplicable Try* methods.</summary>
    public PlanOptFlags OptimizationFlags;

    /// <summary>Count of clauses that carry a <see cref="ClauseInfo.WhenCondition"/>,
    /// in template traversal order. Computed once at template construction. If this
    /// exceeds <see cref="MaxWhenClauses"/>, template construction throws
    /// <see cref="System.NotSupportedException"/> — the bit position used in
    /// <see cref="QueryExecution.WhenFlags"/> would otherwise wrap silently.</summary>
    public int WhenCount;
}
