using System;
using System.Collections.Generic;

namespace Corax.Querying.Planning;

/// <summary>Structural optimization applicability flags. Set at template construction
/// time by analyzing the clause list shape. Used at Instantiate time to skip
/// <c>Try*</c> optimization methods that are guaranteed to fail for this template,
/// avoiding per-query work (Slice allocations, compound-field lookups, etc.).</summary>
[Flags]
public enum PlanOptimizationFlags : byte
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
    public List<ClauseInfo> Clauses;
    public bool IsOr;              // root boolean operator

    /// <summary>Spatial clauses separated from the main filter chain (AND queries only).</summary>
    public List<ClauseInfo> SpatialClauses;
    /// <summary>Vector clauses separated from the main filter chain (AND queries only).</summary>
    public List<ClauseInfo> VectorClauses;

    /// <summary>Plan-time structural optimization flags. Computed at template
    /// construction and checked at Instantiate time to skip inapplicable Try* methods.</summary>
    public PlanOptimizationFlags OptimizationFlags;

    /// <summary>Template-position index of the clause that can drive a sorted scan
    /// (range/eq on the primary ORDER BY field, non-negated, non-boosted). -1 when no
    /// such clause exists or the query has no ORDER BY. Pre-computed at template time
    /// so TryCreateSimpleFieldDirectScan skips the per-execution clause scan loop.
    /// NOTE: after WHEN elimination + cardinality sort, the runtime clause index may
    /// differ — Build must remap via OriginalIndex.</summary>
    public int SortDrivingClauseIndex = -1;

    /// <summary>Pre-identified compound-exact-match clause pair (template-position indices).</summary>
    public (int First, int Second) CompoundExact = (-1, -1);
    
    /// <summary>True when compound field order is (A, B); false when (B, A).</summary>
    public bool CompoundExactAFirst;
    /// <summary>Pre-built <c>compound({firstField},{secondField})</c> tree name for the compound-exact match.
    /// Field names are template-stable, so this is baked here instead of being interpolated on every
    /// execution in <c>ConstructCompoundExact</c>. Null when no qualifying pair exists.</summary>
    public string CompoundExactName;

    /// <summary>Pre-identified compound-field-match (WHERE Equals + ORDER BY) driving clause
    /// index (template position). -1 when no qualifying clause/compound-field pair exists.
    /// Eliminates per-execution clause scan + HasCompoundField Slice allocations.</summary>
    public int CompoundFieldDrivingClause = -1;
    /// <summary>Sort field name for the compound-field match (the second field in the compound pair).
    /// Null when CompoundFieldDrivingClause is -1.</summary>
    public string CompoundFieldSortName;
    /// <summary>Pre-built <c>compound({field1},{sortField})</c> tree name for the compound-field match.
    /// Field names are template-stable, so this is baked here instead of being interpolated on every
    /// execution in <c>ConstructCompoundField</c>. Null when CompoundFieldDrivingClause is -1.</summary>
    public string CompoundFieldName;
    /// <summary>True when compound-field match uses two ORDER BY fields (multi-sort mode).</summary>
    public bool CompoundFieldIsMultiSort;

    /// <summary>Pre-identified optional field2 range narrowing clause (template position) — a
    /// GT/GTE/LT/LTE/Between on <see cref="CompoundFieldSortName"/> that narrows the compound
    /// prefix scan. -1 when none. Structural (clause-type + field name), so it is template-stable.
    /// Build remaps this to the post-sort runtime index on <see cref="CompiledPlan.CompoundFieldField2RangeIdx"/>.</summary>
    public int CompoundFieldField2Range = -1;

    /// <summary>Count of clauses that carry a <see cref="ClauseInfo.WhenCondition"/>,
    /// in template traversal order. Computed once at template construction. If this
    /// exceeds <see cref="MaxWhenClauses"/>, template construction throws
    /// <see cref="System.NotSupportedException"/> — the bit position used in
    /// the WHEN-survival mask folded into the plan-cache key would otherwise wrap silently.</summary>
    public int WhenCount;

    /// <summary>Deduplicated, ordered list of query parameter names referenced by this
    /// template's clause bindings (<see cref="BindingSource.QueryParameter"/> only).
    /// Literals are excluded since their types are fixed at template time.
    /// Used to compute the TypeSignature cache-key component cheaply at execution time
    /// by classifying each parameter's runtime blittable type, instead of walking the
    /// full clause/execution list.</summary>
    public string[] ParameterSlots = [];

    /// <summary>Template-position index of the clause that supplies the seek value for
    /// <c>TrySetSortSeekHint</c> — a non-negated range predicate on the primary
    /// <c>ORDER BY</c> field with a direction-compatible clause type. -1 when no such
    /// clause exists, the query has no <c>ORDER BY</c>, or the primary ORDER BY field
    /// has no name (Score/Random/Distance). Pre-computed at template time so
    /// <c>TrySetSortSeekHint</c> avoids a per-execution clause-scan + per-clause
    /// Slice.ToString() allocation.</summary>
    public int SortSeekHintTemplateIdx = -1;

    /// <summary>For the BETWEEN seek hint: true when descending order (read Param2 = upper bound),
    /// false when ascending (read Param1 = lower bound). For GT/GTE/LT/LTE this is always
    /// false — they only have Param1. Meaningful only when <see cref="SortSeekHintTemplateIdx"/>
    /// is non-negative.</summary>
    public bool SortSeekUseParam2;

    /// <summary>Pre-computed sort-metadata template. Built once at template-build time by
    /// <c>QueryPlanBuilder.BuildSortMetadataTemplate</c> (in Raven.Server); the runtime
    /// <c>MaterializeSortMetadata</c> reads the prebuilt <c>OrderMetadata[]</c> + per-slot
    /// patch directives. Null for templates built by callers that don't supply a
    /// <c>QueryBuilderParameters</c> (direct-planner tests).</summary>
    public SortMetadataTemplate SortMetadataTemplate;
}
