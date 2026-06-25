using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using Corax.Mappings;
using Corax.Querying.Planning;
using Corax.Querying.Primitives;
using Sparrow;
using IndexSearcher = Corax.Querying.IndexSearcher;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

ref struct BuildResolver(PlanTemplate template, PlanParameters planParams, QueryBuilderParameters builderParameters, ResolutionContext walkerCtx)
{
    private readonly IndexSearcher _indexSearcher = planParams.IndexSearcher;

    private PlanCacheKeyBuilder _builder = new();
    private readonly ValueWriter _writer = new();
    private byte[] _sentinelFull = null;

    private QueryExecution _exec;

    public QueryExecution Resolve()
    {
        _exec = CreateQueryExecution();
        var cacheKeyHash = ComputeCacheKeyHash();

        // BuildTemplate already resolved the per-query bucket for this structural key;
        // we only probe/publish the runtime variant within that bucket here for the parameters types, cardinalities, etc
        return planParams.Bucket.TryLookup(cacheKeyHash) is { } cachedPlan 
            ? FinalizePlan(cachedPlan) 
            : BuildOnCacheMiss(cacheKeyHash);
    }

    private QueryExecution BuildOnCacheMiss(in Vector256<long> cacheKeyHash)
    {
        var (scanSet, perClause, scanEligible) = BuildScanPredicates();
        var (ops, requiredBitmaps) = PlanEmitter.Emit(template, _exec.Executions, planParams, scanEligible);
        // The entry-scan delegate is emitted only when the scan path is viable (BuildScanPredicates cleared the
        // predicates otherwise). MaybeEntryScan is gated on the same verdict, so a non-viable plan never needs it.
        string scanCsharp = null;
        if (scanSet.HasPredicates)
            scanSet.Compiled = ResidualScanIlEmitter.EmitDelegate(scanSet.Predicates, out scanCsharp);
        // DirectScan / CompoundField walk the driving clause via the tree and filter every OTHER clause per-entry. Their residual set excludes the DRIVING clause,
        // whereas the entry-scan set excludes clause[0] (the bitmap seed). Those differ whenever the driving clause is not the smallest-cardinality clause (always, for a range-driven scan).
        var compoundFieldResidualSet = _exec.CompoundFieldDrivingClause is not null
            ? BuildResidualSet(_exec.Executions, perClause, _exec.CompoundFieldDrivingClause, _exec.CompoundFieldField2Range)
            : null;
        var directScanResidualSet = _exec.SortDrivingClause is not null
            ? BuildResidualSet(_exec.Executions, perClause, _exec.SortDrivingClause, skip2: null)
            : null;

        string directScanCsharp = null, compoundCsharp = null;
        if (directScanResidualSet is { HasPredicates: true } directSet)
            directSet.Compiled = ResidualScanIlEmitter.EmitDelegate(directSet.Predicates, out directScanCsharp);
        if (compoundFieldResidualSet is { HasPredicates: true } compoundSet)
            compoundSet.Compiled = ResidualScanIlEmitter.EmitDelegate(compoundSet.Predicates, out compoundCsharp);

        var plan = new CompiledPlan
        {
            CompiledDelegate = QueryIlEmitter.EmitDelegate(ops, out var csharpText),
            Template = template,
            Source = ComposePlanSource(csharpText, scanCsharp, directScanCsharp, compoundCsharp),
            CacheKeyHash = cacheKeyHash,
            OpCount = ops.Length,
            RequiredBitmaps = requiredBitmaps,
            InspectionTemplate = QueryPlanBuilder.BuildInspectionTemplate(ops, _exec.Executions),
            EntryScanSet = scanSet,
            CompoundFieldResidualSet = compoundFieldResidualSet,
            DirectScanResidualSet = directScanResidualSet,
            AllNegated = CheckAllNegated(),
        };

        planParams.Bucket.Publish(plan);

        return FinalizePlan(plan);
    }

    private static string ComposePlanSource(string queryCsharp, string entryScanCsharp, string directScanCsharp, string compoundCsharp)
    {
        string result = queryCsharp ?? string.Empty;
        var seen = new HashSet<string>();

        result = AddResidualSection(result, seen, "Entry-scan per-entry residual filter (bitmap cost-gate path)", entryScanCsharp);
        result = AddResidualSection(result, seen, "Direct-scan per-entry residual filter (FieldSortedScan path)", directScanCsharp);
        result = AddResidualSection(result, seen, "Compound-field per-entry residual filter (CompoundSortedScan path)", compoundCsharp);
        return result;

        static string AddResidualSection(string acc, HashSet<string> seen, string header, string csharp)
        {
            if (string.IsNullOrEmpty(csharp) || seen.Add(csharp) == false)
                return acc;
            return acc + Environment.NewLine + "// --- " + header + " ---" + Environment.NewLine + csharp;
        }
    }

    private QueryExecution FinalizePlan(CompiledPlan plan)
    {
        _exec.Plan = plan;
        (_exec.InRangeCounts, _exec.Cardinalities) = CardinalityArrayBuilder.Build(_exec.Executions, _exec.IsAllEntries);
        _exec.KnownExactTotal = ComputeKnownExactTotal();

        QueryPlanBuilder.AttachSpatialAndVectorClauses(_exec, template, planParams, builderParameters, _writer);
        _writer.SetValues(_exec);
        _exec.RegexFactory = builderParameters.Factories?.GetRegexFactory;
        return _exec;
    }

    /// <summary> Try to compute the exact number of results if we can do that cheaply, -1 otherwise. Allows to avoid materializing the full bitmap when we need the count. </summary>
    /// <returns></returns>
    private long ComputeKnownExactTotal()
    {
        if (_exec.HasSpatialOrVector)
            return -1; // those don't have a good way to say how much we'll get

        if (_exec.IsAllEntries) // the whole of the index
            return _indexSearcher.NumberOfEntries;
        
        if (_exec.Executions is not [{ } only]) 
            return -1; // we can't detect if we have more than a single clause

        if (only.IsSentinel)// a single when() clause, etc...
            return only.Cardinality;// The sentinel already carries its exact O(1) count in Cardinality (NumberOfEntries / 0)

        // A single Equals / NotEquals has an exactly-known result count the cardinality estimator already computed
        // from O(1) metadata: Equals -> the term posting list's NumberOfEntries; NotEquals -> index NumberOfEntries
        // minus that exact term count. Boost is NOT a guard here — it never changes the matched set, so the count is
        // identical; whether the page may be truncated to a limit is a separate concern owned by the consumer
        // (CoraxIndexReadOperation gates the limit push-down on HasBoost). Cardinality is either a real count (never
        // negative for Equals/NotEquals) or the -1 "not estimated" sentinel — which is exactly this method's
        // "unknown" return, so return it directly.
        bool exactEquals = only.ClauseType == ClauseType.Equals && only.IsNegated == false;
        bool exactNotEquals = only.ClauseType == ClauseType.NotEquals && only.IsNegated;
        if (only.PackedParamValue.IsNone == false && (exactEquals || exactNotEquals))
            return only.Cardinality;

        // A single exists() has an exactly-known total the estimator does NOT supply (it returns the whole-index upper bound for Exists).
        // Only valid for fields without multiple terms per field (empty array is consider to exists(), but wouldn't be properly counted).
        if (only.ClauseType == ClauseType.Exists && only.IsNegated == false)
        {
            FieldMetadata existsField = QueryPlanBuilder.ResolveFieldMetadata(only.Clause, walkerCtx);
            if (_indexSearcher.HasMultipleTermsInField(existsField) == false)
                return _indexSearcher.NumberOfEntriesForExists(existsField);
        }

        return -1;
    }

    private QueryExecution CreateQueryExecution()
    {
        // A clause that collapses (WHEN(false), a statically-true exists()/NOT exists(), an empty IN, a contradictory BETWEEN, etc) is replaced IN PLACE by a MatchAll / MatchNothing sentinel. 
        var execList = new List<ClauseExecution>(template.Clauses.Count);
        QueryExecution queryExecution = new();
        foreach (var cached in template.Clauses)
        {
            var exec = QueryPlanBuilder.CreateExecution(cached);
            ApplyFate(exec, cached);
            if (exec.IsSentinel == false)
            {
                QueryPlanBuilder.PopulateClauseValues(exec, planParams.SlotBindings, planParams.QueryParameters, _writer, builderParameters, template.ParameterSlots.Length, ref _sentinelFull);
                QueryPlanBuilder.PropagateBetweenContradiction(exec, _writer); // a contradictory BETWEEN collapses to MatchNothing
                if (IsEmptyIn(exec))
                    exec.MarkAsSentinel(ClauseType.MatchNothing, 0); // an empty IN matches nothing

                if (exec.Cardinality < 0)
                    exec.Cardinality = CardinalityEstimator.Estimate(exec, _indexSearcher, _writer, walkerCtx);
            }
            AppendSentinelCodes(exec);
            queryExecution.SetKnownClause(exec, template);
            execList.Add(exec);
        }

        execList.Sort(); // sort executions by cardinality (smaller clauses first)

        queryExecution.Executions = execList;
        queryExecution.IsAllEntries = execList.Count is 0;
        return queryExecution;
    }

    private void AppendSentinelCodes(ClauseExecution exec)
    {
        // Every clause contributes a 2-bit sentinel outcome (Keep / MatchAll / MatchNothing) in template order.
        // This is important so we can generate a different query plan for each final query output
        RuntimeHelpers.EnsureSufficientExecutionStack();
        var val = exec.ClauseType switch
        {
            ClauseType.MatchAll => 0b00,
            ClauseType.MatchNothing => 0b01,
            _ => 0b10 // Keep
        };
        _builder.Append(val, 2);
        foreach (var sub in exec.SubExecutions ?? [])
        {
            AppendSentinelCodes(sub);
        }
    }

    private void ApplyFate(ClauseExecution exec, ClauseInfo clause)
    {
        if (template.WhenCount is 0)
            return;

        if (clause.WhenCondition is not { } predicate || predicate(planParams.QueryParameters))
            return;

        // WHEN(false): the guard is off, so the whole guarded clause (its negation included) does not filter.
        // It collapses to the identity of its enclosing boolean operator: MatchAll under AND, MatchNothing under OR.
        if (template.IsOr)
            exec.MarkAsSentinel(ClauseType.MatchNothing, 0);
        else
            exec.MarkAsSentinel(ClauseType.MatchAll, _indexSearcher.NumberOfEntries);
    }

    private static bool IsEmptyIn(ClauseExecution e) =>
        e.ClauseType is ClauseType.In or ClauseType.AllIn &&
        e.InTermCount == 0 &&
        e.HasNullTerm is false;

    // Consider the query: FROM Posts WHERE Tags = 'good' AND Status = 'Public', Tags = 'good' has 100 results, Status = 'Public' (may has 1 million)
    // it is cheaper to evaluate 100 entries to find if Status = 'Public' directly.
    private (ResidualScanSet ScanSet, ScanPredicateInfo?[] PerClause, bool ScanEligible) BuildScanPredicates()
    {
        var perClause = new ScanPredicateInfo?[_exec.Executions.Count];

        // Scan predicates only apply to multi-clause AND chains (clause 0 is the seed, 1..N are evaluated per-entry).
        bool hasScanList = template.IsOr == false && _exec.Executions.Count > 1;
        // Skip clause 0 (the seed) unless all clauses are negated (then we start from AllEntries, so every clause would be a scan predicate).
        int scanStart = CheckAllNegated() ? 0 : 1;

        List<ScanPredicateInfo> scanList = hasScanList ? [] : null;
        List<int> clauseIndices = hasScanList ? [] : null;

        for (int i = 0; i < _exec.Executions.Count; i++)
        {
            bool isScanCandidate = hasScanList && i >= scanStart;

            ClauseExecution clauseExec = _exec.Executions[i];
            ScanPredicateInfo? pred = BuildScanPredicateInfoCore(clauseExec, clauseExec.TermValueType);

            // A TOP-LEVEL MatchNothing (AlwaysFalse) empties the whole AND; nested AlwaysFalse is handled in its group
            if (pred is { CompareOp: ScanCompareOp.AlwaysFalse })
                pred = null;
            perClause[i] = pred;

            // AlwaysTrue (MatchAll sentinel) stays in perClause so it counts as scan-eligible, but it
            // carries no predicate to evaluate, so it is never added to the residual list.
            if (isScanCandidate is false || pred is not { } p || p.CompareOp == ScanCompareOp.AlwaysTrue)
                continue;

            scanList.Add(p);
            clauseIndices.Add(i);
        }

        // The entry scan over clause 0's seed bitmap (1..N evaluated per-entry) is viable only when EVERY clause in
        // the scan range can be expressed as a predicate. A null there means an unsupported clause or a top-level
        // AlwaysFalse collapsed to null above — and since scanList silently drops nulls, a delegate built from the
        // survivors would be missing that filter. PlanEmitter gates MaybeEntryScan on the same condition, so today
        // such a delegate is dead, but we refuse to bake a partial (invalid) delegate at all rather than rely on it
        // never being invoked. (Clause 0 is the seed, run via the bitmap pipeline, so a null there is irrelevant —
        // mirror PlanEmitter's [1..] window.)
        bool scanEligible = hasScanList && perClause.AsSpan()[1..].Contains(null) == false;

        return (new ResidualScanSet
        {
            Predicates = scanEligible ? scanList?.ToArray() : null,
            ClauseIndices = scanEligible ? clauseIndices?.ToArray() : null
        }, perClause, scanEligible);
    }

    private ScanPredicateInfo? BuildScanPredicateInfoCore(ClauseExecution exec, ParamValueType termType)
    {
        var clause = exec.Clause;
        // Single-valued ⟺ the field holds at most one term per entry. The straight-line residual IL
        // (DirectScan/CompoundScan/entry-scan) reads exactly one term instead of walking the term list.
        // Folded into the structural plan key (ComputeStructuralKey) so a later single→multi flip selects
        // a different bucket and re-plans rather than reusing this template built under the single-valued assumption.
        bool singleValued = clause.FieldName is { } fieldName && _indexSearcher.HasMultipleTermsInField(fieldName) == false;

        // The residual-scan IL only encodes negation for IN / ALL IN (via ScanPredicateInfo.Negated, which the
        // emitter inverts) and for NotEquals (the NotEqual compare op is inherently the negation). Any other
        // negated clause (negated Equals, ranges, StartsWith/EndsWith/Exists, groups) would be emitted as its
        // POSITIVE predicate and filter incorrectly — so disqualify the scan and let the bitmap pipeline, which
        // builds the correct complement, handle it.
        if (exec.IsNegated
            && exec.ClauseType is not (ClauseType.In or ClauseType.AllIn or ClauseType.NotEquals))
            return null;

        switch (exec.ClauseType)
        {
            // x ∧ ALL = x: a MatchAll sentinel is always true, so it filters nothing. Emit an
            // AlwaysTrue placeholder (non-null → keeps the clause scan-eligible) that the residual-set
            // builders drop before IL emission. This lets a query keep the entry-scan tail even when a
            // top-level clause has collapsed to match-all (e.g. WHEN(false)).
            case ClauseType.MatchAll:
                return new ScanPredicateInfo { CompareOp = ScanCompareOp.AlwaysTrue };

            // x ∧ ∅ = ∅: a MatchNothing sentinel is always false. As an AlwaysFalse placeholder it lets
            // the group recursion below apply boolean algebra (x∨∅=x, x∧∅=∅). At the TOP level
            // BuildScanPredicates collapses AlwaysFalse back to a disqualifier so the bitmap pipeline
            // empties the whole AND via ClearBitmap + GotoDoneIfEmpty (faster than any scan).
            case ClauseType.MatchNothing:
                return new ScanPredicateInfo { CompareOp = ScanCompareOp.AlwaysFalse };

            // These clause types cannot be expressed as entry-scan predicates.
            case ClauseType.Search:
            case ClauseType.Regex:
            case ClauseType.Spatial:
            case ClauseType.Vector:
                return null;

            case ClauseType.In:
            case ClauseType.AllIn:
            {
                // Boosted IN stays on the scoring bitmap path (a complement has no match to score).
                // Negation is fine: the per-entry helper returns a membership boolean we simply invert.
                if (clause.HasBoost)
                    return null;

                return new ScanPredicateInfo
                {
                    FieldName = clause.FieldName,
                    ValueType = exec.PackedParamValue.ValueType switch
                    {
                        PackedParam.TypeLong => ScanValueType.Long,
                        PackedParam.TypeDouble => ScanValueType.Double,
                        _ => ScanValueType.Slice
                    },
                    CompareOp = exec.ClauseType == ClauseType.In ? ScanCompareOp.In : ScanCompareOp.AllIn,
                    ParamIndex = 0,
                    Negated = exec.IsNegated,
                    IsSingleValued = singleValued
                };
            }

            case ClauseType.StartsWith:
                if (termType != ParamValueType.String)
                    return null;
                return new ScanPredicateInfo
                {
                    FieldName = clause.FieldName,
                    ValueType = ScanValueType.Slice,
                    CompareOp = ScanCompareOp.StartsWith,
                    ParamIndex = exec.PackedParamValue.Param1,
                    IsSingleValued = singleValued
                };
            case ClauseType.EndsWith:
                if (termType != ParamValueType.String)
                    return null;
                return new ScanPredicateInfo
                {
                    FieldName = clause.FieldName,
                    ValueType = ScanValueType.Slice,
                    CompareOp = ScanCompareOp.EndsWith,
                    ParamIndex = exec.PackedParamValue.Param1,
                    IsSingleValued = singleValued
                };
            case ClauseType.Exists:
                return new ScanPredicateInfo
                {
                    FieldName = clause.FieldName,
                    CompareOp = ScanCompareOp.Exists,
                    IsSingleValued = singleValued
                };

            case ClauseType.AndGroup:
            case ClauseType.OrGroup:
            {
                var branches = new List<ScanPredicateInfo>();
                foreach (var it in exec.SubExecutions)
                {
                    var subTermType = it.TermValueType;
                    var subPred = BuildScanPredicateInfoCore(it, subTermType);
                    // Only a genuinely unsupported sub-clause (Search/Regex/Spatial/Vector/boosted-IN)
                    // disqualifies the whole scan. A sentinel sub-clause is kept as an AlwaysTrue /
                    // AlwaysFalse marker: the predicate tree stays 1:1 with the SubExecutions tree (so
                    // ScanParamExtractor and the IL emitter walk it in lockstep) and the IL bakes the
                    // group-local boolean identity (x∧ALL=x, x∨ALL=ALL, x∧∅=∅, x∨∅=x).
                    if (subPred is not { } sp)
                        return null;
                    branches.Add(sp);
                }

                return new ScanPredicateInfo
                {
                    FieldName = clause.FieldName ?? exec.SubExecutions[0].Clause.FieldName,
                    SubPredicates = branches.ToArray(),
                    Group = clause.ClauseType == ClauseType.AndGroup ? GroupKind.And : GroupKind.Or
                };
            }
        }

        return new ScanPredicateInfo
        {
            FieldName = clause.FieldName,
            ValueType = termType switch
            {
                ParamValueType.Long => ScanValueType.Long,
                ParamValueType.Double => ScanValueType.Double,
                _ => ScanValueType.Slice // String/True/False/Null/Parameter (when unresolvable) → opaque slice comparison.
            },
            CompareOp = exec.ClauseType switch
            {
                ClauseType.Equals => ScanCompareOp.Equal,
                ClauseType.NotEquals => ScanCompareOp.NotEqual,
                ClauseType.GreaterThan => ScanCompareOp.GreaterThan,
                ClauseType.GreaterThanOrEqual => ScanCompareOp.GreaterThanOrEqual,
                ClauseType.LessThan => ScanCompareOp.LessThan,
                ClauseType.LessThanOrEqual => ScanCompareOp.LessThanOrEqual,
                ClauseType.Between => ScanCompareOp.Between,
                _ => ScanCompareOp.Equal
            },
            ParamIndex = exec.PackedParamValue.Param1,
            ParamIndex2 = exec.PackedParamValue.Param2 != PackedParam.NoParamValue ? exec.PackedParamValue.Param2 : -1,
            IsSingleValued = singleValued
        };
    }

    // Returns null when a non-scannable residual clause makes the path ineligible (caller falls back
    // to the bitmap pipeline); otherwise a set whose Predicates may be empty (driving clause is the
    // only clause → no per-entry filter needed). The Compiled delegate is baked by the caller.
    private static ResidualScanSet BuildResidualSet(List<ClauseExecution> execs, ScanPredicateInfo?[] perClause, ClauseExecution skip1, ClauseExecution skip2)
    {
        var residuals = new List<ScanPredicateInfo>();
        var indices = new List<int>();
        for (int i = 0; i < perClause.Length; i++)
        {
            // skip1/skip2 may be null (role has no candidate) — ReferenceEquals against null skips nothing,
            // matching the old skip == -1 sentinel.
            if (ReferenceEquals(execs[i], skip1) || ReferenceEquals(execs[i], skip2))
                continue;
            if (perClause[i] is not { } pred)
                return null;
            // AlwaysTrue (MatchAll sentinel) is scan-eligible but has no predicate to evaluate — skip it
            // without disqualifying the set.
            if (pred.CompareOp == ScanCompareOp.AlwaysTrue)
                continue;
            residuals.Add(pred);
            indices.Add(i);
        }

        return new ResidualScanSet { Predicates = residuals.ToArray(), ClauseIndices = indices.ToArray() };
    }

    // ClauseExecution.CompareTo sorts negated clauses LAST, so if even the FIRST clause is negated then every
    // clause must be — checking the head is enough to decide "all negated".
    private readonly bool CheckAllNegated() => _exec.Executions is [{ IsNegated: true }, ..];

    // Single canonical serialization of every plan-disambiguating dimension, digested to a 256-bit cache key.
    // Used for BOTH the cache probe and the on-miss store, so this appends sequence is the one source of truth —
    // adding a dimension is one more Append.
    private Vector256<long> ComputeCacheKeyHash()
    {
        var execs = _exec.Executions;

        _builder.Append(execs.Count, 16); // length prefixed to ensure consistency
        foreach (var e in execs)
        {
            _builder.Append(e.Clause.OriginalIndex, 16);
        }

        // Boost + cardinality-cliff flags: queries on either side of the cliff get distinct plans.
        int flags = planParams.HasBoost.ToInt32() << 1 |
                    (_exec.DrivingClauseCardinality is >= 0 and <= QueryPrimitives.TieBreakGroupInitialCapacity).ToInt32();
        _builder.Append(flags, 2);

        // Per-parameter runtime type (bits 0-1) OR-ed with the BETWEEN-sentinel mark (bit 2 of
        // sentinelFull). A parameter-bound BETWEEN sentinel ("*"/"NULL") must go to a
        // QueryMatch-dispatched plan while a non-sentinel BETWEEN of the same query text can be
        // TreeScan-dispatched, so the mark forces a distinct key.
        _builder.Append((ushort)template.ParameterSlots.Length, 16);
        for (int i = 0; i < template.ParameterSlots.Length; i++)
        {
            int kind = (int)QueryPlanBuilder.ClassifyParamType(planParams.QueryParameters, template.ParameterSlots[i]) & 0b11;
            if (_sentinelFull != null)
                kind |= _sentinelFull[i]; // SentinelParamMark is already (1 << 2), i.e. bit 2 — OR it in directly; a further << 2 would push it out of the 3-bit payload
            _builder.Append(kind, 3);
        }

        return _builder.ToHash();
    }
}
