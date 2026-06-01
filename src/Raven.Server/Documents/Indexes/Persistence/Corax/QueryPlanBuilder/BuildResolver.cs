using System;
using System.Collections.Generic;
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

        if (_exec.QueryWillReturnNoResults) // there are no results here..., return immediately
            return default;

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
        // A clause is "gated" when its presence in the execution can vary per query: a WHEN(...) guard,
        // or a top-level exists()/NOT exists() leaf that may statically collapse against the live
        // NonExisting posting list. When any clause is gated we length-prefix the key and emit one
        // per-clause survival bit so the kept/removed shape resolves to a distinct cached plan.
        // exists() eligibility (dynamic fields write no NonExisting markers; pre-feature index versions
        // have no list; OR roots make the collapse a no-op) is structural, so it is stable per query text
        // and the gated/ungated key shape never diverges between executions of the same query.
        bool existsEligible = template.ExistsCollapseCandidateCount > 0
                              && template.IsOr == false
                              && builderParameters.HasDynamics == false
                              && IndexDefinitionBaseServerSide.IndexVersion.IsNonExistingPostingListSupported(planParams.Index.Definition.Version);
        bool gated = template.WhenCount != 0 || existsEligible;

        var execList = new List<ClauseExecution>(template.Clauses.Count);
        if (gated)
            _builder.Append(template.Clauses.Count, 16);

        bool collapseToNoResults = false;
        int sortDrivingIdx = template.SortDrivingClauseIndex;
        long drivingClauseCardinality = -1;

        foreach (var cached in template.Clauses)
        {
            if (gated)
            {
                ClauseFate fate = GateClause(cached, existsEligible);
                // Kept = survival bit 1. Both Drop (match-all) and CollapseToNoResults (match-nothing)
                // remove the clause from execution and emit bit 0; the two zero-bit fates never share a
                // cached plan because CollapseToNoResults sets QueryWillReturnNoResults and the caller
                // returns before touching the cache.
                _builder.Append((fate == ClauseFate.Keep).ToInt32(), 1);
                switch (fate)
                {
                    case ClauseFate.Drop:
                        continue;
                    case ClauseFate.CollapseToNoResults:
                        collapseToNoResults = true;
                        continue;
                }
            }

            var it = QueryPlanBuilder.CreateExecution(cached);
            QueryPlanBuilder.PopulateClauseValues(it, planParams.QueryParameters, _writer, builderParameters, template.ParameterSlots.Length, ref _sentinelFull);
            QueryPlanBuilder.PropagateBetweenContradiction(it, _writer); // a contradictory BETWEEN is rewritten into an empty-IN
            collapseToNoResults |= IsEmptyIn(it);

            if (it.Cardinality < 0)
                it.Cardinality = CardinalityEstimator.Estimate(it, _indexSearcher, _writer, walkerCtx);
            if (sortDrivingIdx >= 0 && it.Clause.OriginalIndex == sortDrivingIdx)
                drivingClauseCardinality = it.Cardinality;

            execList.Add(it);
        }

        execList.Sort(); // sort executions by cardinality (smaller clauses first)

        return new QueryExecution
        {
            Executions = execList,
            // Empty-IN and a collapsed NOT exists() (no missing entries -> match-nothing) only short-circuit an
            // AND chain; in an OR chain they're no-op clauses. collapseToNoResults is only ever set in an AND root.
            QueryWillReturnNoResults = collapseToNoResults && template.IsOr is false,
            IsAllEntries = execList.Count is 0,
            DrivingClauseCardinality = drivingClauseCardinality,
        };
    }

    // Decide the fate of a single clause during the resolution pass. WHEN(false) and a statically-true
    // exists() are match-all (Drop); a statically-true NOT exists() is match-nothing (CollapseToNoResults).
    private ClauseFate GateClause(ClauseInfo cached, bool existsEligible)
    {
        if (cached.WhenCondition is { } predicate && predicate(planParams.QueryParameters) == false)
            return ClauseFate.Drop;

        // exists() collapses only when the field has NO missing entries: every doc has it, so exists()
        // is statically true. Then exists() -> Drop (match-all) and NOT exists() -> match-nothing.
        // When some docs miss the field, the result is data-dependent and stays a runtime term-walk.

        if (existsEligible is false || cached.ClauseType != ClauseType.Exists)
            return ClauseFate.Keep;

        FieldMetadata fieldMeta = QueryPlanBuilder.ResolveFieldMetadata(cached, walkerCtx);
        if (_indexSearcher.HasAnyNonExistingEntries(in fieldMeta) == false)
            return ClauseFate.Keep;

        return cached.IsNegated ? ClauseFate.CollapseToNoResults : ClauseFate.Drop;
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
        switch (clause.ClauseType)
        {
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
                    CompareOp = clause.ClauseType == ClauseType.In ? ScanCompareOp.In : ScanCompareOp.AllIn,
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
            CompareOp = clause.ClauseType switch
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
