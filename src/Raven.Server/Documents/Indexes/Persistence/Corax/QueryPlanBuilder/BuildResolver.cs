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

ref struct BuildResolver(PlanTemplate template, PlanParameters planParams, QueryBuilderParameters builderParameters, ResolutionContext walkerCtx, Span<byte> scratch)
{
    private readonly IndexSearcher _indexSearcher = planParams.IndexSearcher;

    private PlanCacheKeyBuilder _builder = new(scratch);
    private readonly ValueWriter _writer = new();
    private byte[] _sentinelFull = null;

    private QueryExecution _exec;
    private Vector256<long> _cacheKeyHash;
    private CompiledPlan _compiledPlan;

    public (CompiledPlan, QueryExecution) Resolve()
    {
        _exec = CreateQueryExecution();
        _cacheKeyHash = ComputeCacheKeyHash();

        // A query that collapses to no results is no longer special-cased: its clauses survive as
        // MatchNothing sentinels and the emitted plan produces an empty bitmap. That plan caches and
        // executes like any other, so the per-clause sentinel code in the key keeps it distinct.

        if (_indexSearcher.PlanCache.Get(planParams.CacheKey, _cacheKeyHash) is { } cachedPlan)
        {
            _compiledPlan = cachedPlan; // use cached plan
            return FinalizePlan();
        }

        return BuildOnCacheMiss(); // Cache miss — full exec emission
    }

    private (CompiledPlan, QueryExecution) BuildOnCacheMiss()
    {
        var (scanSet, perClause) = BuildScanPredicates();
        var (ops, requiredBitmaps) = PlanEmitter.Emit(template, _exec.Executions, planParams, perClause);
        // The entry-scan delegate is always emitted (an empty predicate set when there is no
        // entry-scan path); its C# mirror joins the plan Source.
        scanSet.Compiled = ResidualScanIlEmitter.EmitDelegate(scanSet.Predicates, out var scanCsharp);
        _compiledPlan = new CompiledPlan
        {
            CompiledDelegate = QueryIlEmitter.EmitDelegate(ops, out var csharpText, emitTimings: false),
            CompiledTimedDelegate = QueryIlEmitter.EmitDelegate(ops, out _, emitTimings: true),

            Template = template,
            Source = csharpText + Environment.NewLine + scanCsharp,
            CacheKeyHash = _cacheKeyHash,
            OpCount = ops.Length,
            RequiredBitmaps = requiredBitmaps,
            InspectionTemplate = QueryPlanBuilder.BuildInspectionTemplate(ops, _exec.Executions),
            EntryScanSet = scanSet,
            AllNegated = CheckAllNegated(),
        };
        RemapOptimizationIndices();

        // DirectScan / CompoundField walk the driving clause via the tree and filter every OTHER
        // clause per-entry. Their residual set excludes the DRIVING clause, whereas the entry-scan
        // set excludes clause[0] (the bitmap seed). Those differ whenever the driving clause is not
        // the smallest-cardinality clause (always, for a range-driven scan), so each path bakes its
        // own delegate from its own residual set.
        _compiledPlan.CompoundFieldResidualSet = BuildResidualSet(perClause, _compiledPlan.CompoundField.DrivingClause, _compiledPlan.CompoundField.Field2Range);
        _compiledPlan.DirectScanResidualSet = BuildResidualSet(perClause, _compiledPlan.SortDrivingClauseIndex, skip2: -1);

        if (_compiledPlan.DirectScanResidualSet is { HasPredicates: true } directSet)
            directSet.Compiled = ResidualScanIlEmitter.EmitDelegate(directSet.Predicates, out _);
        if (_compiledPlan.CompoundFieldResidualSet is { HasPredicates: true } compoundSet)
            compoundSet.Compiled = ResidualScanIlEmitter.EmitDelegate(compoundSet.Predicates, out _);

        _indexSearcher.PlanCache.Add(planParams.CacheKey, _compiledPlan, template);

        return FinalizePlan();
    }

    private (CompiledPlan, QueryExecution) FinalizePlan()
    {
        _exec.Plan = _compiledPlan;
        CardinalityArrayBuilder.Build(_exec.Executions, _exec.IsAllEntries, out var inRange, out var cards);
        _exec.InRangeCounts = inRange;
        _exec.Cardinalities = cards;

        QueryPlanBuilder.AttachSpatialAndVectorClauses(_exec, template, planParams, builderParameters, _writer);
        _writer.SetValues(_exec);
        return (_compiledPlan, _exec);
    }

    private QueryExecution CreateQueryExecution()
    {
        // A clause is "gated" when its effective shape can vary per query: a WHEN(...) guard, or an
        // exists()/NOT exists() leaf (at any nesting depth) that may statically collapse against the live
        // NonExisting posting list. exists() eligibility (dynamic fields write no NonExisting markers;
        // pre-feature index versions have no list) is structural, so it is stable per query text. The root
        // operator is irrelevant: a collapsed leaf becomes a sentinel that the emitter's merge algebra
        // simplifies correctly under OR/AND and inside nested groups.
        bool existsEligible = template.ExistsCollapseEligible;
        bool gated = template.WhenCount != 0 || existsEligible;

        // The execution list mirrors the template clause list one-for-one — clauses are never removed.
        // A clause that collapses (WHEN(false), a statically-true exists()/NOT exists(), an empty IN, a
        // contradictory BETWEEN) is replaced IN PLACE by a MatchAll / MatchNothing sentinel. The plan
        // emitter's merge algebra performs the boolean simplification (∨/∧ against the universe/∅), which
        // also makes nesting fall out for free, so there is no list-shape reconciliation to do.
        var execList = new List<ClauseExecution>(template.Clauses.Count);

        int sortDrivingIdx = template.SortDrivingClauseIndex;
        long drivingClauseCardinality = -1;
        long numberOfEntries = _indexSearcher.NumberOfEntries;

        foreach (var cached in template.Clauses)
        {
            var it = QueryPlanBuilder.CreateExecution(cached);

            ApplyFate(it, gated ? GateClause(cached, existsEligible) : ClauseFate.Keep);
            if (it.IsSentinel == false)
            {
                // A kept top-level clause may still wrap a group with nested exists() leaves that
                // collapse independently against the live NonExisting list — gate them before
                // populating values so each becomes its own MatchAll/MatchNothing sentinel.
                if (existsEligible)
                    GateNestedExists(it);

                QueryPlanBuilder.PopulateClauseValues(it, planParams.QueryParameters, _writer, builderParameters, template.ParameterSlots.Length, ref _sentinelFull);
                QueryPlanBuilder.PropagateBetweenContradiction(it, _writer); // a contradictory BETWEEN collapses to MatchNothing
                if (IsEmptyIn(it))
                    it.MarkAsSentinel(ClauseType.MatchNothing, 0); // an empty IN matches nothing

                if (it.Cardinality < 0)
                    it.Cardinality = CardinalityEstimator.Estimate(it, _indexSearcher, _writer, walkerCtx);
                if (sortDrivingIdx >= 0 && it.Clause.OriginalIndex == sortDrivingIdx)
                    drivingClauseCardinality = it.Cardinality;
            }

            // Every clause contributes a 2-bit sentinel outcome (Keep / MatchAll / MatchNothing) in template
            // order, recursing into groups so a nested exists() collapse is captured too. With the list shape
            // frozen, the OriginalIndex enumeration in ComputeCacheKeyHash no longer encodes collapse, so this
            // code is the sole disambiguator: WHEN+exists on one clause can yield MatchAll OR MatchNothing (one
            // bit is not enough), and the now-cached empty-IN / contradictory-BETWEEN no-result variants must
            // resolve to distinct plans from their populated counterparts.
            AppendSentinelCodes(it);

            execList.Add(it);
        }

        execList.Sort(); // sort executions by cardinality (smaller clauses first)

        return new QueryExecution
        {
            Executions = execList,
            // Only a genuinely clause-less query (no WHERE) is match-all here; an all-collapsed query keeps its
            // MatchAll/MatchNothing sentinels and lets the emitter produce the right bitmap.
            IsAllEntries = execList.Count is 0,
            DrivingClauseCardinality = drivingClauseCardinality,
        };
    }

    /// <summary>Append the 2-bit collapse code for <paramref name="exec"/> and every nested sub-execution, in
    /// template traversal order: 0 = kept (real leaf), 1 = MatchAll, 2 = MatchNothing. The recursion mirrors the
    /// frozen tree shape so a nested exists() collapse outcome (which varies with the live NonExisting list) lands
    /// in the plan-cache key and never collides with a populated counterpart.</summary>
    private void AppendSentinelCodes(ClauseExecution exec)
    {
        RuntimeHelpers.EnsureSufficientExecutionStack();
        _builder.Append(SentinelCode(exec), 2);
        foreach (var sub in exec.SubExecutions ?? [])
            AppendSentinelCodes(sub);
    }

    private static int SentinelCode(ClauseExecution exec) => exec.ClauseType switch
    {
        ClauseType.MatchAll => 1,
        ClauseType.MatchNothing => 2,
        _ => 0
    };

    private void ApplyFate(ClauseExecution exec, ClauseFate fate)
    {
        switch (fate)
        {
            case ClauseFate.Drop:
                exec.MarkAsSentinel(ClauseType.MatchAll, _indexSearcher.NumberOfEntries); // WHEN(false) / statically-true exists()
                break;
            case ClauseFate.CollapseToNoResults:
                exec.MarkAsSentinel(ClauseType.MatchNothing, 0); // statically-true NOT exists()
                break;
        }
    }

    // Decide the fate of a single clause during the resolution pass. WHEN(false) and a statically-true
    // exists() are match-all (Drop); a statically-true NOT exists() is match-nothing (CollapseToNoResults).
    private ClauseFate GateClause(ClauseInfo cached, bool existsEligible)
    {
        if (cached.WhenCondition is { } predicate && predicate(planParams.QueryParameters) == false)
            return ClauseFate.Drop;

        return GateExists(cached, existsEligible);
    }

    // exists() collapses only when the field has NO missing entries: every doc has it, so exists()
    // is statically true. Then exists() -> Drop (match-all) and NOT exists() -> match-nothing.
    // When some docs miss the field, the result is data-dependent and stays a runtime term-walk.
    private ClauseFate GateExists(ClauseInfo cached, bool existsEligible)
    {
        if (existsEligible is false || cached.ClauseType != ClauseType.Exists)
            return ClauseFate.Keep;

        FieldMetadata fieldMeta = QueryPlanBuilder.ResolveFieldMetadata(cached, walkerCtx);
        if (_indexSearcher.HasAnyNonExistingEntries(in fieldMeta))
            return ClauseFate.Keep;

        return cached.IsNegated ? ClauseFate.CollapseToNoResults : ClauseFate.Drop;
    }

    // Recurse through a kept clause's groups and collapse any nested exists()/NOT exists() leaf whose field
    // has no missing entries. A group node itself is never gated — only its leaves — and the emitter's merge
    // algebra folds the resulting sentinels into the surrounding Or/AndGroup correctly.
    private void GateNestedExists(ClauseExecution exec)
    {
        RuntimeHelpers.EnsureSufficientExecutionStack();
        foreach (var sub in exec.SubExecutions ?? [])
        {
            if (sub.SubExecutions is { Count: > 0 })
            {
                GateNestedExists(sub);
                continue;
            }

            ApplyFate(sub, GateExists(sub.Clause, existsEligible: true));
        }
    }

    private static bool IsEmptyIn(ClauseExecution e) =>
        e.ClauseType is ClauseType.In or ClauseType.AllIn &&
        e.InTermCount == 0 &&
        e.HasNullTerm is false;

    private void RemapOptimizationIndices()
    {
        for (int i = 0; i < _exec.Executions.Count; i++)
        {
            ClauseExecution it = _exec.Executions[i];
            // A collapsed clause keeps its slot but drives no optimization: leave the index disabled (-1),
            // matching the pre-frozen-shape behavior where the clause was removed and never matched by OriginalIndex.
            if (it.IsSentinel)
                continue;
            if (it.Clause.OriginalIndex == template.SortDrivingClauseIndex)
                _compiledPlan.SortDrivingClauseIndex = i;
            if (it.Clause.OriginalIndex == template.CompoundExact.First)
                _compiledPlan.CompoundExact = (i, _compiledPlan.CompoundExact.Second);
            if (it.Clause.OriginalIndex == template.CompoundExact.Second)
                _compiledPlan.CompoundExact = (_compiledPlan.CompoundExact.First, i);
            if (it.Clause.OriginalIndex == template.CompoundFieldDrivingClause)
                _compiledPlan.CompoundField = (i, _compiledPlan.CompoundField.Field2Range);
            if (it.Clause.OriginalIndex == template.CompoundFieldField2Range)
                _compiledPlan.CompoundField = (_compiledPlan.CompoundField.DrivingClause, i);
            if (it.Clause.OriginalIndex == template.SortSeekHintTemplateIdx)
                _compiledPlan.SortSeekClauseExecIdx = i;
        }
    }

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
            perClause[i] = pred;

            if (isScanCandidate is false || pred is not { } p)
                continue;

            scanList.Add(p);
            clauseIndices.Add(i);
        }

        return (new ResidualScanSet { Predicates = scanList?.ToArray(), ClauseIndices = clauseIndices?.ToArray() }, perClause);
    }

    private static ScanPredicateInfo? BuildScanPredicateInfoCore(ClauseExecution exec, ParamValueType termType)
    {
        var clause = exec.Clause;
        // Switch on the EFFECTIVE per-execution type, not the frozen template type: PropagateBetweenContradiction
        // and the collapse stamps rewrite exec.ClauseType, and a sentinel has no scan predicate.
        switch (exec.ClauseType)
        {
            // A collapse sentinel is not a scannable residual; null forces the bitmap pipeline (it also clears
            // allScanEligible, so a query with any sentinel never takes the entry-scan tail).
            case ClauseType.MatchAll:
            case ClauseType.MatchNothing:
                return null;

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
                    if (subPred == null)
                        return null;
                    branches.Add(subPred.Value);
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
    private static ResidualScanSet BuildResidualSet(ScanPredicateInfo?[] perClause, int skip1, int skip2)
    {
        var residuals = new List<ScanPredicateInfo>();
        var indices = new List<int>();
        for (int i = 0; i < perClause.Length; i++)
        {
            if (i == skip1 || i == skip2)
                continue;
            if (perClause[i] is not { } pred)
                return null;
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
