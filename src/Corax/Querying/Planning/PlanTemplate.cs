using System;
using System.Collections.Generic;

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

    public List<ClauseInfo> Clauses;
    public bool IsOr;              // root boolean operator

    /// <summary>Spatial clauses separated from the main filter chain (AND queries only).</summary>
    public List<ClauseInfo> SpatialClauses;
    /// <summary>Vector clauses separated from the main filter chain (AND queries only).</summary>
    public List<ClauseInfo> VectorClauses;

    /// <summary>Plan-time structural optimization flags. Computed at template
    /// construction and checked at Instantiate time to skip inapplicable Try* methods.</summary>
    public PlanOptFlags OptimizationFlags;

    /// <summary>Template-position index of the clause that can drive a sorted scan
    /// (range/eq on the primary ORDER BY field, non-negated, non-boosted). -1 when no
    /// such clause exists or the query has no ORDER BY. Pre-computed at template time
    /// so TryCreateSimpleFieldDirectScan skips the per-execution clause scan loop.
    /// NOTE: after WHEN elimination + cardinality sort, the runtime clause index may
    /// differ — Build must remap via OriginalIndex.</summary>
    public int SortDrivingClauseIndex = -1;

    /// <summary>Pre-identified compound-exact-match clause pair (template-position indices).
    /// -1/-1 when no qualifying pair exists. Set at template time after checking the index's
    /// compound field configuration. <see cref="CompoundExactAFirst"/> encodes the compound
    /// field ordering. Eliminates the O(n²) scan + Slice allocation + HasCompoundField check
    /// on every execution.</summary>
    public int CompoundExactClauseA = -1;
    public int CompoundExactClauseB = -1;
    /// <summary>True when compound field order is (A, B); false when (B, A).</summary>
    public bool CompoundExactAFirst;

    /// <summary>Pre-identified compound-field-match (WHERE Equals + ORDER BY) driving clause
    /// index (template position). -1 when no qualifying clause/compound-field pair exists.
    /// Eliminates per-execution clause scan + HasCompoundField Slice allocations.</summary>
    public int CompoundFieldDrivingClause = -1;
    /// <summary>Sort field name for the compound-field match (the second field in the compound pair).
    /// Null when CompoundFieldDrivingClause is -1.</summary>
    public string CompoundFieldSortName;
    /// <summary>True when compound-field match uses two ORDER BY fields (multi-sort mode).</summary>
    public bool CompoundFieldIsMultiSort;

    /// <summary>Count of clauses that carry a <see cref="ClauseInfo.WhenCondition"/>,
    /// in template traversal order. Computed once at template construction. If this
    /// exceeds <see cref="MaxWhenClauses"/>, template construction throws
    /// <see cref="System.NotSupportedException"/> — the bit position used in
    /// <see cref="QueryExecution.WhenFlags"/> would otherwise wrap silently.</summary>
    public int WhenCount;

    /// <summary>True when the WHERE clause reduces to a contradiction at template time
    /// (e.g. <c>WHERE true AND false</c>). The query can never match any documents —
    /// <see cref="QueryPlanBuilder"/> short-circuits to an empty result without building
    /// or caching a compiled plan.</summary>
    public bool AlwaysEmpty;
}
