namespace Corax.Querying.Planning;

/// <summary>Execution strategy chosen for a CompiledPlan. Determined once at cache-miss time
/// (when Try* discovery runs in Build) and baked into the CompiledPlan. On every subsequent
/// cache hit, Instantiate dispatches directly on Strategy without re-running Try*.</summary>
public enum ExecutionStrategy : byte
{
    /// <summary>First execution hasn't completed yet — Try* discovery has not run.
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

/// <summary>Cache-level plan data for the CompoundExact strategy. Captures the structural
/// facts the Try*Discover step established once at cache-miss time, so that Instantiate on
/// each subsequent execution can build the live TermQuery directly.</summary>
public readonly record struct CompoundExactPlan(int ClauseA, int ClauseB, bool AFirst);

/// <summary>Cache-level plan data for the CompoundField strategy. Captures the driving
/// clause's runtime index plus the sort-field/multi-sort facts that were validated at
/// cache-miss time.</summary>
public readonly record struct CompoundFieldPlan(int DrivingClauseIdx, string SortFieldName, bool IsMultiSort);

/// <summary>Cache-level plan data for the DirectScan strategy. The driving clause index
/// is the sole structural fact needed at Instantiate time; the clause itself (boost/IsNone)
/// is still re-validated per execution because parameter values can shift.</summary>
public readonly record struct DirectScanPlan(int DrivingClauseIdx);

public sealed class CompiledPlan
{
    /// <summary>IL-emitted delegate that executes the posting-list scan plan (no timing instrumentation).</summary>
    public QueryIlEmitter.CompiledExecuteDelegate CompiledDelegate { get; init; }

    /// <summary>IL-emitted delegate with per-op timing instrumentation. Compiled lazily on
    /// first `include timings()` request. Null until then.</summary>
    public QueryIlEmitter.CompiledExecuteDelegate CompiledTimedDelegate { get; set; }

    /// <summary>IL-emitted per-entry predicate evaluator for the entry-scan path.
    /// Bakes in the plan's full ScanPredicateInfo[] structure. Null when the plan
    /// has no entry-scan predicates.</summary>
    public ResidualScanIlEmitter.ResidualScanPredicate CompiledEntryPredicate { get; init; }



    /* A single query may be represented by different compiled plans, because the shape
     * of the data is different. Consider `WHERE Tag = $tag and Published = $published`.
     * If $tag is a popular term, and $published is true, that usually means that we
     * need to generate a plan that would:
     * - Read the posting list for $tag
     * - AND that posting list with the Published=true posting list.
     *
     * On the other hand, if we want all the _unpublished_ items in a popular tag, we can check
     * that Published=false is a small amount, then start from that, then we find that we have
     * low enough results that we are going to just scan through them, instead of going through
     * the posting list.
     *
     * In other words, the parameters we use for the query impact the query plan.
     * The `Ordering` field is the order of steps in the query plan, and we use that as a cache key for disambiguation.
     */
    
    /// <summary>Packed operand ordering used as part of the cache key.</summary>
    public int Ordering { get; init; }

    /* The same query may be called with parameters of different types:
     *      Age = "25" vs. Age = 25 vs. Age = 25.0
     * Each one of them has a different posting list that they use, and that matters, so we
     * need a different compiled plan for each set of ordering.
     */
    
    /// <summary>Packed parameter type signature (2 bits per param for first 16).</summary>
    public int TypeSignature { get; init; }

    /* The `TypeSignature` here is able to hold up to 16 parameter types, with 2 bits per parameter.
     * Users may want to use queries with > 16 parameters, and we need to respect the same problem that
     * `TypeSignature` is solving. In those cases, we use `FullKinds` to store the full kind vector for
     * the full check.
     */
    
    /// <summary>Full per-predicate kind vector for >16 typed scan predicates.</summary>
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

    /// <summary>Cache-level plan data for the CompoundExact strategy. Populated only when
    /// <see cref="Strategy"/> is <see cref="ExecutionStrategy.CompoundExact"/>.</summary>
    public CompoundExactPlan CompoundExactData;

    /// <summary>Cache-level plan data for the CompoundField strategy. Populated only when
    /// <see cref="Strategy"/> is <see cref="ExecutionStrategy.CompoundField"/>.</summary>
    public CompoundFieldPlan CompoundFieldData;

    /// <summary>Cache-level plan data for the DirectScan strategy. Populated only when
    /// <see cref="Strategy"/> is <see cref="ExecutionStrategy.DirectScan"/>.</summary>
    public DirectScanPlan DirectScanData;

    public PlanDecisionTrail DecisionTrail;

    /// <summary>Chain pointer for hash-collision disambiguation in PlanCache.</summary>
    public CompiledPlan Next;

    /// <summary>Depth of this entry in the chain (0 for head). Used by TryChainPrepend
    /// to limit chain growth — when depth exceeds MaxChainDepth, the chain is replaced.</summary>
    public int ChainDepth;

    /// <summary>EXPLAIN pseudocode. Generated in the same pass as IL emission.</summary>
    public string ExplainSource { get; init; }

    /// <summary>Compilable C# source string mirroring emitted IL.</summary>
    public string CSharpSource { get; init; }

    private string _cSharpSourceFormatted;
    /// <summary>Roslyn-normalized version of <see cref="CSharpSource"/>. Computed lazily on first
    /// access and cached. Benign races between concurrent readers produce the same string so no
    /// lock is needed.</summary>
    public string CSharpSourceFormatted => _cSharpSourceFormatted ??= CSharpFormatter.Format(CSharpSource);

    /// <summary>Template inspection nodes built during IL emission.
    /// At query time, cloned and populated with per-execution telemetry
    /// (timings, result counts, scanned entries) from CompiledQueryMatch.</summary>
    public InspectionOp[] InspectionTemplate { get; init; }
}
