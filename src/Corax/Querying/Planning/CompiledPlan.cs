namespace Corax.Querying.Planning;

/// <summary>Execution strategy chosen for a CompiledPlan. Determined once at cache-miss time
/// (when Try* discovery runs in Build) and baked into the CompiledPlan. On every subsequent
/// cache hit, Instantiate dispatches directly on Strategy without re-running Try*.</summary>
public enum ExecutionStrategy : byte
{
    /// <summary>The first execution hasn't completed yet — Try* discovery has not run.
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

    /// <summary>Number of IN/AllIn range-count slots in the plan. Each IN or AllIn
    /// clause produces one slot whose runtime value comes from the fixup loop.
    /// Zero when the plan has no IN/AllIn clauses. Stored on the CompiledPlan so
    /// cache hits can allocate and fill InRangeCounts without EmitPlan.</summary>
    public int InRangeSlotCount { get; init; }

    /// <summary>Structural scan predicate metadata cached from the first compilation.
    /// Field names, param indices, compare ops do not change across executions.
    /// Used by <c>ExtractScanParameters</c> on every execution (both cache hit and miss)
    /// to avoid rebuilding the <see cref="ScanPredicateInfo"/> array per query.
    /// Null when the plan has no entry-scan predicates (single-clause, OR, etc.).</summary>
    public ScanPredicateInfo[] ScanPredicateInfos { get; init; }
}
