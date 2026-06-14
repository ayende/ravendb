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

        // BuildTemplate already resolved (or created) the per-query bucket for this structural key and stashed it
        // // on planParams; we only probe/publish the runtime variant (inner 256-bit key) within that bucket here.
        return planParams.Bucket.TryLookup(cacheKeyHash) is { } cachedPlan ? FinalizePlan(cachedPlan) : BuildOnCacheMiss(cacheKeyHash);// Cache miss — full exec emission
    }

    private QueryExecution BuildOnCacheMiss(in Vector256<long> cacheKeyHash)
    {
        var (scanSet, perClause) = BuildScanPredicates();
        var (ops, requiredBitmaps) = PlanEmitter.Emit(template, _exec.Executions, planParams, perClause);
        // The entry-scan delegate is always emitted (an empty predicate set when there is no entry-scan path); its C# mirror joins the plan Source.
        scanSet.Compiled = ResidualScanIlEmitter.EmitDelegate(scanSet.Predicates, out var scanCsharp);

        // DirectScan / CompoundField walk the driving clause via the tree and filter every OTHER clause per-entry. Their residual set excludes the DRIVING clause, whereas the entry-scan
        // set excludes clause[0] (the bitmap seed). Those differ whenever the driving clause is not the smallest-cardinality clause (always, for a range-driven scan).
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
            CompiledDelegate = QueryIlEmitter.EmitDelegate(ops, out var csharpText, emitTimings: false),
            CompiledTimedDelegate = QueryIlEmitter.EmitDelegate(ops, out _, emitTimings: true),
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
        CardinalityArrayBuilder.Build(_exec.Executions, _exec.IsAllEntries, out var inRange, out var cards);
        _exec.InRangeCounts = inRange;
        _exec.Cardinalities = cards;

        QueryPlanBuilder.AttachSpatialAndVectorClauses(_exec, template, planParams, builderParameters, _writer);
        _writer.SetValues(_exec);
        return _exec;
    }

    private QueryExecution CreateQueryExecution()
    {
        // A clause that collapses (WHEN(false), a statically-true exists()/NOT exists(), an empty IN, a
        // contradictory BETWEEN, etc) is replaced IN PLACE by a MatchAll / MatchNothing sentinel. 
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
            ClauseType.MatchAll => 1,
            ClauseType.MatchNothing => 2,
            _ => 0 // Keep
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

        if (clause.WhenCondition is not { } predicate ||
            predicate(planParams.QueryParameters))
            return;

        // WHEN(false): the guard is off, so the whole guarded clause (its negation included) does not
        // filter. It collapses to the identity of its enclosing boolean operator: MatchAll (the universe,
        // x ∧ ALL = x) under AND, MatchNothing (the empty set, x ∨ ∅ = x) under OR. The polarity is purely
        // operator-driven — a dropped clause's own negation is subsumed, since a removed filter contributes
        // its parent's identity regardless of how it was written (e.g. `A and not when(false, X)` => A).
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
    private (ResidualScanSet ScanSet, ScanPredicateInfo?[] PerClause) BuildScanPredicates()
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

            // A TOP-LEVEL MatchNothing (AlwaysFalse) empties the whole AND; disqualify the entry-scan so
            // the bitmap pipeline empties it via ClearBitmap + GotoDoneIfEmpty. (Nested AlwaysFalse stays
            // inside its group predicate, where the IL bakes the boolean identity.)
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

        return (new ResidualScanSet { Predicates = scanList?.ToArray(), ClauseIndices = clauseIndices?.ToArray() }, perClause);
    }

    private static ScanPredicateInfo? BuildScanPredicateInfoCore(ClauseExecution exec, ParamValueType termType)
    {
        var clause = exec.Clause;
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
                    Negated = exec.IsNegated
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
                    ParamIndex = exec.PackedParamValue.Param1
                };
            case ClauseType.EndsWith:
                if (termType != ParamValueType.String)
                    return null;
                return new ScanPredicateInfo
                {
                    FieldName = clause.FieldName,
                    ValueType = ScanValueType.Slice,
                    CompareOp = ScanCompareOp.EndsWith,
                    ParamIndex = exec.PackedParamValue.Param1
                };
            case ClauseType.Exists:
                return new ScanPredicateInfo
                {
                    FieldName = clause.FieldName,
                    CompareOp = ScanCompareOp.Exists,
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
            ParamIndex2 = exec.PackedParamValue.Param2 != PackedParam.NoParamValue ? exec.PackedParamValue.Param2 : -1
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

    private readonly bool CheckAllNegated() => _exec.Executions is [{ IsNegated: true }, ..]; // negated clauses are always sorted first, so we can just check the first

    // Single canonical serialization of every plan-disambiguating dimension, digested to a
    // 256-bit cache key. Used for BOTH the cache probe and the on-miss store, so the Append
    // sequence here is the one source of truth — adding a new dimension is one more Append.
    // Unlike the former packed-int fields, nothing is truncated: all clauses and all
    // parameters contribute, so the old 10-clause / 16-param ceilings are gone.
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
                kind |= _sentinelFull[i] << 2;
            _builder.Append(kind, 3);
        }

        return _builder.ToHash();
    }
}
