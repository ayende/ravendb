namespace Corax.Querying.Planning;

public sealed class CompiledPlan
{
    /// <summary>IL-emitted delegate that executes the posting-list scan plan (no timing instrumentation).</summary>
    public QueryIlEmitter.CompiledExecuteDelegate CompiledDelegate { get; init; }

    /// <summary>IL-emitted delegate with per-op timing instrumentation. Compiled lazily on
    /// first `include timings()` request. Null until then.</summary>
    public QueryIlEmitter.CompiledExecuteDelegate CompiledTimedDelegate { get; set; }

    /// <summary>IL-emitted per-entry predicate evaluator used by the entry-scan path.
    /// Bakes in the ScanPredicateInfo[] structure (value types, compare ops, AND/OR
    /// sub-groups, fieldRootPages indexing) so the per-entry hot path has no switches.
    /// Null when the plan has no entry-scan predicates.</summary>
    public EntryScanIlEmitter.CompiledEntryPredicate CompiledEntryPredicate { get; init; }

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

    /// <summary>Chain pointer for hash-collision disambiguation in PlanCache.</summary>
    public CompiledPlan Next;

    /// <summary>Depth of this entry in the chain (0 for head). Used by TryChainPrepend
    /// to limit chain growth — when depth exceeds MaxChainDepth, the chain is replaced.</summary>
    public int ChainDepth;

    /// <summary>EXPLAIN pseudocode. Generated in the same pass as IL emission.</summary>
    public string ExplainSource { get; init; }

    /// <summary>Template inspection nodes built during IL emission.
    /// At query time, cloned and populated with per-execution telemetry
    /// (timings, result counts, scanned entries) from CompiledQueryMatch.</summary>
    public InspectionOp[] InspectionTemplate { get; init; }
}
