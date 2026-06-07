using System.Runtime.Intrinsics;

namespace Corax.Querying.Planning;

/// <summary>Execution strategy chosen for a CompiledPlan. Determined once at cache-miss time
/// (when Try* discovery runs in Build) and baked into the CompiledPlan. On every subsequent
/// cache hit, Instantiate dispatches directly on Strategy without re-running Try*.</summary>
public enum ExecutionStrategy : byte
{
    /// <summary>The first execution hasn't completed yet — discovery has not run.
    /// Treated as "run discovery" by the dispatch path.</summary>
    NotEvaluated = 0,
    /// <summary>The general default: build the result by intersecting/unioning posting-list bitmaps
    /// (CompiledQueryMatch), then wrap with SortingMatch only when an ORDER BY is present. No scan
    /// optimization applies. The "Pipeline" name signals that sorting is a separate wrapper, not part
    /// of this strategy — for a no-ORDER-BY query nothing sorts.</summary>
    BitmapPipeline,
    /// <summary>Single compound-tree exact-term lookup (a point lookup). No ORDER BY — replaces an AND
    /// of two Equals on the compound field's component clauses with one TermQuery on the composite key.</summary>
    CompoundKeyLookup,
    /// <summary>Compound-tree range scan that emits docs already in ORDER BY order (streamed sorted).
    /// Driving clause is an Equals on the prefix field of a compound (Equals + ORDER BY sort field).</summary>
    CompoundSortedScan,
    /// <summary>SortedDrivingMatch on the sort field — streams entries in sort order from a
    /// single-field tree without materializing a bitmap (streamed sorted, single-field flavor).</summary>
    FieldSortedScan,
}

public sealed class CompiledPlan
{
    /// <summary>The template this plan was compiled from.</summary>
    public PlanTemplate Template { get; init; }

    /// <summary>IL-emitted delegate that executes the posting-list scan plan (no timing instrumentation).</summary>
    public QueryIlEmitter.CompiledExecuteDelegate CompiledDelegate { get; init; }

    /// <summary>IL-emitted delegate with per-op timing instrumentation: `include timings()`.</summary>
    public QueryIlEmitter.CompiledExecuteDelegate CompiledTimedDelegate { get; init; }

    /// <summary>
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
    /// 
    ///  In other words, the parameters we use for the query impact the query plan. The digest folds
    ///  every disambiguating dimension (operand ordering, per-parameter runtime type, BETWEEN
    ///  sentinel marks, WHEN-clause survival, boost/cardinality-cliff flags) into one 256-bit value
    ///  used as the cache key — see <see cref="PlanCacheKeyBuilder"/> for the serialization.
    /// </summary>
    public Vector256<long> CacheKeyHash { get; init; }

    /// <summary>Execution strategy chosen for this compiled plan. Set once at cache-miss
    /// time after Try* discovery (volatile store), then read-only — safe for concurrent readers.
    /// Cache hits dispatch on this field without re-running Try*.</summary>
    public volatile ExecutionStrategy Strategy;

    public PlanDecisionTrail DecisionTrail;

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

    /// <summary>Predicate set for the bitmap entry-scan path (excludes clause 0, the bitmap seed).
    /// This is used when we have small enough set of results that we can scan them, rather then search.</summary>
    public ResidualScanSet EntryScanSet { get; init; }

    /// <summary>Predicate set for the CompoundSortedScan strategy, null when a non-scannable clause makes the path ineligible.</summary>
    public ResidualScanSet CompoundFieldResidualSet { get; set; }

    /// <summary>Predicate set for the DirectScan dispatch path, null when a non-scannable clause makes it ineligible.</summary>
    public ResidualScanSet DirectScanResidualSet { get; set; }

    /// <summary>True when every clause in the execution is negated (NOT pattern).
    /// This is per-CompiledPlan (not per-template) because WHEN elimination can remove all non-negated clauses, leaving only negated ones.</summary>
    public bool AllNegated { get; init; }
}
