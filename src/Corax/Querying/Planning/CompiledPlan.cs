using System.Collections.Generic;

namespace Corax.Querying.Planning;

/// <summary>Execution strategy chosen for a CompiledPlan. Determined once at cache-miss time
/// (when Try* discovery runs in Build) and baked into the CompiledPlan. On every subsequent
/// cache hit, Instantiate dispatches directly on Strategy without re-running Try*.</summary>
public enum ExecutionStrategy : byte
{
    /// <summary>The first execution hasn't completed yet — discovery has not run.
    /// Treated as "run discovery" by the dispatch path.</summary>
    NotEvaluated = 0,
    /// <summary>No ORDER BY optimization applies. Use the bitmap pipeline (CompiledQueryMatch)
    /// and wrap with SortingMatch when an ORDER BY is present.</summary>
    BitmapSort,
    /// <summary>Single compound-tree exact-term lookup. No ORDER BY — replaces an AND of two
    /// Equals on the compound field's component clauses with one TermQuery on the composite key.</summary>
    CompoundExact,
    /// <summary>Compound-tree range scan that emits docs in ORDER BY order. Driving clause is
    /// an Equals on the prefix field of a compound (Equals + ORDER BY sort field).</summary>
    CompoundField,
    /// <summary>SortedDrivingMatch on the sort field. Streams entries in sort order from a
    /// single-field tree without materializing a bitmap.</summary>
    DirectScan,
}

public sealed class CompiledPlan
{
    /// <summary>The template this plan was compiled from. Provides access to structural
    /// template-level data (OptimizationFlags, ParameterSlots, etc.) without copying.</summary>
    public PlanTemplate Template { get; init; }

    /// <summary>IL-emitted delegate that executes the posting-list scan plan (no timing instrumentation).</summary>
    public QueryIlEmitter.CompiledExecuteDelegate CompiledDelegate { get; init; }

    /// <summary>IL-emitted delegate with per-op timing instrumentation: `include timings()`.</summary>
    public QueryIlEmitter.CompiledExecuteDelegate CompiledTimedDelegate { get; init; }

    /// <summary>IL-emitted per-entry predicate evaluator for the entry-scan path.
    /// Bakes in the plan's full ScanPredicateInfo[] structure. Null when the plan
    /// has no entry-scan predicates.</summary>
    public ResidualScanIlEmitter.ResidualScanPredicate CompiledEntryPredicate { get; init; }
    
    /// <summary>
    /// Packed operand ordering used as part of the cache key.
    ///
    ///  A single query may be represented by different compiled plans because the shape
    ///  of the data is different. Consider `WHERE Tag = $tag and Published = $published`.
    ///  If $tag is a popular term, and $published is true, that usually means that we
    ///      need to generate a plan that would:
    ///  - Read the posting list for $tag
    ///  - AND that posting list with the Published=true posting list.
    ///      On the other hand, if we want all the _unpublished_ items in a popular tag, we can check
    ///      that Published=false is a small amount, then start from that, then we find that we have
    ///      low enough results that we are going to just scan through them, instead of going through
    ///      the posting list.
    ///  In other words, the parameters we use for the query impact the query plan.
    ///  The `Ordering` field is the order of steps in the query plan, and we use that as a cache key for disambiguation.  
    /// </summary>
    public int Ordering { get; init; }
    
    /// <summary>
    /// Packed parameter type signature (2 bits per param for first 16).
    /// The same query may be called with parameters of different types:
    /// Age = "25" vs. Age = 25 vs. Age = 25.0
    /// Each one of them has a different posting list that they use, and that matters, so we
    /// need a different compiled plan for each set of ordering.
    /// </summary>
    public int TypeSignature { get; init; }
    
    /// <summary>
    /// Full per-predicate kind vector for >16 typed scan predicates.
    ///
    /// The `TypeSignature` here is able to hold up to 16 parameter types, with 2 bits per parameter.
    /// Users may want to use queries with > 16 parameters, and we need to respect the same problem that
    /// `TypeSignature` is solving. In those cases, we use `FullKinds` to store the full kind vector for
    /// the full check.                                                                                 
    /// </summary>
    public byte[] FullKinds { get; init; }

    /// <summary>Per-execution WHEN-clause survival bitmask. Joins
    /// (<see cref="Ordering"/>, <see cref="TypeSignature"/>, <see cref="FullKinds"/>)
    /// as part of the cache key. Zero for queries with no WHEN clauses (the common
    /// case). Bit <c>i</c> = "the <c>i</c>-th WHEN clause in template traversal order
    /// evaluated true under the bound parameters."</summary>
    public int WhenFlags { get; init; }

    /// <summary>Execution strategy chosen for this compiled plan. Set once at cache-miss
    /// time after Try* discovery (volatile store), then read-only — safe for concurrent readers.
    /// Cache hits dispatch on this field without re-running Try*.</summary>
    public volatile ExecutionStrategy Strategy;

    public PlanDecisionTrail DecisionTrail;

    /// <summary>Chain pointer for hash-collision disambiguation in PlanCache.</summary>
    public CompiledPlan Next;

    /// <summary>Depth of this entry in the chain (0 for head). Used by TryChainPrepend
    /// to limit chain growth — when depth exceeds MaxChainDepth, the chain is replaced.</summary>
    public int ChainDepth;

    /// <summary>C# source string mirroring emitted IL.</summary>
    public string Source { get; init; }

    /// <summary>Roslyn-normalized version of <see cref="Source"/>.</summary>
    public string FormattedSource => field ??= CSharpFormatter.Format(Source);

    /// <summary>Template inspection nodes built during IL emission.
    /// At query time, cloned and populated with per-execution telemetry
    /// (timings, result counts, scanned entries) from CompiledQueryMatch.</summary>
    public InspectionOp[] InspectionTemplate { get; init; }

    /// <summary>Number of PlanOps in the plan. Stored on the CompiledPlan so that
    /// cache hits can size timing arrays without re-running EmitPlan.</summary>
    public int OpCount { get; init; }

    /// <summary>Number of bitmaps the plan needs at execution time (2 or 3).
    /// Stored on the CompiledPlan so cache hits can allocate bitmaps without
    /// re-running EmitPlan.</summary>
    public int RequiredBitmaps { get; init; }

    /// <summary>Structural scan predicate metadata cached from the first compilation.
    /// Field names, param indices, compare ops do not change across executions.
    /// Used by <c>ExtractScanParameters</c> on every execution (both cache hit and miss)
    /// to avoid rebuilding the <see cref="ScanPredicateInfo"/> array per query.
    /// Null when the plan has no entry-scan predicates (single-clause, OR, etc.).</summary>
    public List<ScanPredicateInfo> ScanPredicateInfos { get; init; }

    /// <summary>Residual entry-scan predicates for the CompoundField strategy, filtered once at
    /// cache-miss time (after <c>RemapOptimizationIndices</c>) from the per-clause predicate array
    /// against the plan-stable exclusion set {<see cref="CompoundFieldDrivingClause"/>,
    /// <see cref="CompoundFieldField2RangeIdx"/>}. The filter result is itself plan-stable, so it is
    /// computed once instead of per query.</summary>
    public ResidualPredicateSet CompoundFieldResiduals { get; set; }

    /// <summary>Residual entry-scan predicates for the DirectScan dispatch path, filtered once at
    /// cache-miss time from the per-clause predicate array against the plan-stable exclusion of
    /// {<see cref="SortDrivingClauseIndex"/>}. Plan-stable, so computed once instead of per query.</summary>
    public ResidualPredicateSet DirectScanResiduals { get; set; }

    // ── Structural fields moved from QueryExecution ─────────────────────
    // Set once at cache-miss time (by Build + RemapOptimizationIndices) then
    // read-only on every subsequent cache hit. Not duplicated on QueryExecution.

    /// <summary>True when every clause in the execution is negated (NOT pattern).
    /// Determines whether a trailing AllEntries slot is appended during resolution.
    /// This is per-CompiledPlan (not per-template) because WHEN elimination can remove
    /// all non-negated clauses, leaving only negated ones. Different WHEN outcomes produce
    /// different WhenFlags → different CompiledPlan entries, so AllNegated is stable
    /// within a single cached plan.</summary>
    public bool AllNegated { get; init; }

    /// <summary>Post-sort runtime index of the clause identified at plan time as the
    /// sort-driving candidate (range/eq on ORDER BY field). -1 when none.
    /// Remapped from template position by RemapOptimizationIndices.</summary>
    public int SortDrivingClauseIndex { get; set; } = -1;

    /// <summary>Pre-identified compound-exact-match clause pair (runtime indices, remapped
    /// from template via OriginalIndex). -1/-1 when no qualifying pair exists.</summary>
    public int CompoundExactClauseA { get; set; } = -1;
    /// <inheritdoc cref="CompoundExactClauseA"/>
    public int CompoundExactClauseB { get; set; } = -1;
    /// <summary>Pre-identified compound-field-match (WHERE Equals + ORDER BY) driving clause
    /// index (runtime, remapped from template). -1 when none.</summary>
    public int CompoundFieldDrivingClause { get; set; } = -1;

    /// <summary>Optional field2 range narrowing clause (runtime exec-position index, remapped from
    /// <see cref="PlanTemplate.CompoundFieldField2Range"/>). -1 when none. A GT/GTE/LT/LTE/Between
    /// on the compound sort field that narrows the prefix scan.</summary>
    public int CompoundFieldField2RangeIdx { get; set; } = -1;

    /// <summary>Runtime exec-position index of the sort-seek-hint clause (remapped from
    /// <see cref="PlanTemplate.SortSeekHintTemplateIdx"/>). -1 when no hint applies for
    /// this template, or when WHEN elimination dropped the candidate clause for this
    /// specific execution.</summary>
    public int SortSeekClauseExecIdx { get; set; } = -1;
}

/// <summary>Plan-stable residual entry-scan predicates for one strategy, filtered once at
/// cache-miss time from the per-clause predicate array against that strategy's exclusion set.
/// <see cref="Predicates"/> is null when no residual clauses remain (the strategy degrades to a
/// simple driving scan). <see cref="Unscannable"/> is true when a non-excluded clause is not
/// expressible as an entry-scan predicate, so the strategy must abort to the bitmap pipeline — a
/// defensive state that discovery's IsScanEligible check prevents for a correctly-chosen strategy.
/// NOTE: slice <see cref="ScanPredicateInfo.ParamIndex"/> values in <see cref="Predicates"/> are
/// per-clause local, NOT the IL-aligned dense indices carried by <see cref="CompiledPlan.ScanPredicateInfos"/>
/// (the entry-scan IL). The DirectScan/CompoundField residual paths read slice/numeric values from
/// <see cref="QueryExecution"/> directly (see ScanParamExtractor.BuildResidual) and never consume
/// this array's ParamIndex — only FieldName/ValueType/CompareOp/SubPredicates/Group and null-ness.</summary>
public readonly struct ResidualPredicateSet
{
    public ResidualPredicateSet(ScanPredicateInfo[] predicates, bool unscannable)
    {
        Predicates = predicates;
        Unscannable = unscannable;
    }

    public ScanPredicateInfo[] Predicates { get; }
    public bool Unscannable { get; }
}
