using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using Corax.Mappings;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.SortingMatches.Meta;
using Corax.Querying.Planning;
using Corax.Querying.Primitives;
using Corax.Utils;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Indexes.Persistence.Corax.QueryOptimizer;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Server;
using Spatial4n.Shapes;
using Voron;
using Voron.Data.RoaringBitmaps;
using Voron.Impl;
using Constants = Corax.Constants;
using RavenConstants = Raven.Client.Constants;
using IndexSearcher = Corax.Querying.IndexSearcher;
using Range = Corax.Querying.Matches.Meta.Range;
using SpatialRelation = Spatial4n.Shapes.SpatialRelation;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

internal static partial class QueryPlanBuilder
{
    internal readonly record struct CompiledQuery(
        IQueryMatch QueryMatch,
        IQueryMatch ExecutedMatch,
        IQueryMatch SortingWrapper,
        CompiledPlan CompiledPlan,
        QueryExecution Execution,
        QueryBuilderParameters QueryBuilderParams,
        OrderMetadata[] OrderByFields) : IDisposable
    {
        public void Dispose()
        {
            (QueryMatch as IDisposable)?.Dispose();
            (SortingWrapper as IDisposable)?.Dispose();
        }
    }

    private ref struct InstCtx(CompiledPlan plan, QueryExecution exec, OrderMetadata[] orderByFields, PlanParameters planParams, QueryBuilderParameters builderParams, bool wantTimings)
    {
        public readonly CompiledPlan Plan = plan;
        public readonly QueryExecution Exec = exec;
        public readonly OrderMetadata[] OrderByFields = orderByFields; // may be null when PageSize == 0
        public readonly PlanParameters PlanParams = planParams;
        public readonly QueryBuilderParameters BuilderParams = builderParams;

        public readonly bool WantTimings = wantTimings;

        public string RejectReason;
    }

    private enum MergeKind
    {
        Fill, // slot 0 ← clause result. First op of an OR chain or first non-negated element of an AND chain
        OrInto, // slot 0 ← slot 0 ∪ clause. Subsequent OR-chain elements
        AndInto, // slot 0 ← slot 0 ∩ clause. Subsequent positive AND-chain elements
        AndNotInto // slot 0 ← slot 0 \ clause. Negated AND-chain elements
    }

    public static PlanTemplate BuildTemplate(PlanParameters planParams)
    {
        var queryText = planParams.CacheKey;
        var planCache = planParams.IndexSearcher.PlanCache;
        if (planCache.TryGetTemplate(queryText) is { } template)
            return template;

        template = ParseTemplate(planParams);
        template.SortMetadataTemplate = BuildSortMetadataTemplate(planParams);
        return template;
    }

    /// <summary>
    /// This gets the query match without any sorting. This is used by callers who care about the results but not the order.
    /// For example, facets, more-like-this, etc.
    /// </summary>
    public static IQueryMatch BuildFilterMatch(
        PlanParameters planParams,
        QueryBuilderParameters builderParameters,
        out QueryExecution exec,
        out CompiledPlan compiledPlanOut,
        Dictionary<string, CoraxHighlightingTermIndex> highlightingTerms,
        bool wantTimings,
        CancellationToken token)
    {
        var indexSearcher = planParams.IndexSearcher;
        var walkerCtx = new ResolutionContext(builderParameters);

        var template = BuildTemplate(planParams);

        (compiledPlanOut, exec) = Build(template, planParams, builderParameters, walkerCtx);
        if (compiledPlanOut == null)
            return TermMatch.CreateEmpty(indexSearcher, indexSearcher.Allocator);

        return InstantiateBitmapPipeline(compiledPlanOut, exec, planParams, builderParameters, walkerCtx, highlightingTerms, wantTimings, token);
    }

    public static CompiledQuery BuildSortedQuery(PlanParameters planParams,
        QueryBuilderParameters builderParameters,
        Dictionary<string, CoraxHighlightingTermIndex> highlightingTerms,
        bool wantTimings,
        CancellationToken token)
    {
        var indexSearcher = planParams.IndexSearcher;
        var walkerCtx = new ResolutionContext(builderParameters);

        var template = BuildTemplate(planParams);

        var (plan, exec) = Build(template, planParams, builderParameters, walkerCtx);
        if (plan == null)
        {
            var emptyMatch = TermMatch.CreateEmpty(indexSearcher, indexSearcher.Allocator);
            return new(emptyMatch, emptyMatch, null, null, null, builderParameters, null);
        }

        var orderByFields = GetSortMetadata(builderParameters, plan.Template, out var hasEmptySorts);
        var queryMatch = Instantiate(plan, exec, orderByFields, hasEmptySorts,
            planParams, builderParameters, walkerCtx, highlightingTerms, wantTimings, out var innerMatch, token);
        return new(queryMatch, innerMatch, queryMatch == innerMatch ? null : queryMatch, plan, exec, builderParameters, orderByFields);
    }


    private static (CompiledPlan, QueryExecution) Build(PlanTemplate template, PlanParameters planParams, QueryBuilderParameters builderParameters, ResolutionContext walkerCtx)
    {
        var indexSearcher = planParams.IndexSearcher;
        
        var (executions, whenFlags) = EvaluateWhenAndFilterClauses(); // evaluating WHEN clauses against bound parameters as we go.

        var writer = new ValueWriter();
        QueryExecution exec = CreateQueryExecution();

        if (exec.QueryWillReturnNoResults) // there are no results here..., return immediately
            return default;

        int operandOrdering = ComputeOperandOrdering();
        (int typeSignature, byte[] fullKinds) = ComputeTypeSignature();
        if (indexSearcher.PlanCache.Get(planParams.CacheKey, operandOrdering, typeSignature, fullKinds, whenFlags) is { } compiledPlan)
            return FinalizePlan(); // use cached plan
        
        return BuildOnCacheMiss(); // Cache miss — full exec emission

        (CompiledPlan, QueryExecution) BuildOnCacheMiss()
        {
            var (scanPredicates, scanClauseIndices, perClause) = BuildScanPredicates();
            var (ops, requiredBitmaps) = PlanEmitter.Emit(template, executions, planParams, perClause);
            compiledPlan = new CompiledPlan
            {
                CompiledDelegate = QueryIlEmitter.EmitDelegate(ops, out var csharpText, emitTimings: false),
                CompiledTimedDelegate = QueryIlEmitter.EmitDelegate(ops, out _, emitTimings: true),
                CompiledEntryPredicate = ResidualScanIlEmitter.EmitDelegate(CollectionsMarshal.AsSpan(scanPredicates), out var scanCsharp),

                Template = template,
                Source = csharpText + Environment.NewLine + scanCsharp,
                Ordering = operandOrdering,
                TypeSignature = typeSignature,
                FullKinds = fullKinds,
                WhenFlags = whenFlags,
                OpCount = ops.Length,
                RequiredBitmaps = requiredBitmaps,
                InspectionTemplate = BuildInspectionTemplate(ops, executions),
                ScanPredicateInfos = scanPredicates,
                ScanPredicateClauseIndices = scanClauseIndices,
                AllNegated = CheckAllNegated(),
            };
            RemapOptimizationIndices();

            compiledPlan.CompoundFieldResiduals = BuildResidualSet(perClause, compiledPlan.CompoundFieldDrivingClause, compiledPlan.CompoundFieldField2RangeIdx);
            compiledPlan.DirectScanResiduals = BuildResidualSet(perClause, compiledPlan.SortDrivingClauseIndex, skip2: -1);

            indexSearcher.PlanCache.Add(planParams.CacheKey, compiledPlan, template);

            return FinalizePlan();
        }

        (CompiledPlan, QueryExecution ) FinalizePlan()
        {
            exec.Plan = compiledPlan;
            CardinalityArrayBuilder.Build(executions, exec.IsAllEntries, out var inRange, out var cards);
            exec.InRangeCounts = inRange;
            exec.Cardinalities = cards;

            AttachSpatialAndVectorClauses(exec, template, planParams, builderParameters, writer);
            writer.SetValues(exec);
            return (compiledPlan, exec);
        } 

        QueryExecution CreateQueryExecution()
        {
            bool hasEmptyIn = false;
            int sortDrivingIdx = template.SortDrivingClauseIndex;
            long drivingClauseCardinality = -1;
            foreach (var it in executions)
            {
                PopulateClauseValues(it, planParams.QueryParameters, writer, builderParameters);
                PropagateBetweenContradiction(it, writer); // a contradictory BETWEEN is rewritten into an empty-IN
                hasEmptyIn |= IsEmptyIn(it);

                if (it.Cardinality < 0)
                    it.Cardinality = CardinalityEstimator.Estimate(it, indexSearcher, writer, walkerCtx);
                if (sortDrivingIdx >= 0 && it.Clause.OriginalIndex == sortDrivingIdx)
                    drivingClauseCardinality = it.Cardinality;
            }

            executions.Sort(); // sort executions by cardinality (smaller clauses first)

            return new QueryExecution
            {
                Executions = executions,
                QueryWillReturnNoResults = hasEmptyIn && template.IsOr is false, // Empty-IN only short-circuits an AND chain; in an OR chain it's a no-op clause.
                IsAllEntries = executions.Count is 0,
                DrivingClauseCardinality = drivingClauseCardinality,
            };
        }

        static bool IsEmptyIn(ClauseExecution e) =>
            e.ClauseType is ClauseType.In or ClauseType.AllIn &&
            (e.InTermCount == 0) &&
            e.HasNullTerm is false;

        void RemapOptimizationIndices()
        {
            for (int i = 0; i < executions.Count; i++)
            {
                ClauseExecution it = executions[i];
                if (it.Clause.OriginalIndex == template.SortDrivingClauseIndex)
                    compiledPlan.SortDrivingClauseIndex = i;
                if (it.Clause.OriginalIndex == template.CompoundExactClauseA)
                    compiledPlan.CompoundExactClauseA = i;
                if (it.Clause.OriginalIndex == template.CompoundExactClauseB)
                    compiledPlan.CompoundExactClauseB = i;
                if (it.Clause.OriginalIndex == template.CompoundFieldDrivingClause)
                    compiledPlan.CompoundFieldDrivingClause = i;
                if (it.Clause.OriginalIndex == template.CompoundFieldField2Range)
                    compiledPlan.CompoundFieldField2RangeIdx = i;
                if (it.Clause.OriginalIndex == template.SortSeekHintTemplateIdx)
                    compiledPlan.SortSeekClauseExecIdx = i;
            }
        }

        (List<ClauseExecution> Executions, int WhenFlags)  EvaluateWhenAndFilterClauses()
        {
            var execList = new List<ClauseExecution>(template.Clauses.Count);
            if (template.WhenCount == 0) // Fast path: no WHEN clauses anywhere in the template — skip the per-clause
            {
                foreach (var clause in template.Clauses)
                {
                    execList.Add(CreateExecution(clause));
                }

                return (execList, 0);
            }

            int flags = 0;
            int whenBit = 0;
            foreach (var cached in template.Clauses)
            {
                if (cached.WhenCondition is { } predicate)
                {
                    if (predicate(planParams.QueryParameters) == false)
                    {
                        whenBit++;
                        continue;
                    }

                    flags |= 1 << whenBit;
                    whenBit++;
                }

                execList.Add(CreateExecution(cached));
            }

            return (execList, flags);
        }

        // Consider the query: FROM Posts WHERE Tags = 'good' AND Status = 'Public', Tags = 'good' has 100 results, Status = 'Public' (may has 1 million)
        // it is cheaper to evaluate 100 entries to find if Status = 'Public' directly.
        (List<ScanPredicateInfo> ScanList, int[] ClauseIndices, ScanPredicateInfo?[] PerClause) BuildScanPredicates()
        {
            var perClause = new ScanPredicateInfo?[executions.Count];

            // Scan predicates only apply to multi-clause AND chains (clause 0 is the seed, 1..N are evaluated per-entry).
            bool hasScanList = template.IsOr == false && executions.Count > 1;
            // Skip clause 0 (the seed) unless all clauses are negated (then we start from AllEntries, so every clause would be a scan predicate).
            int scanStart = CheckAllNegated() ? 0 : 1;

            List<ScanPredicateInfo> scanList = hasScanList ? [] : null;
            List<int> clauseIndices = hasScanList ? [] : null;

            for (int i = 0; i < executions.Count; i++)
            {
                bool isScanCandidate = hasScanList && i >= scanStart;

                ClauseExecution clauseExec = executions[i];
                ScanPredicateInfo? pred = BuildScanPredicateInfoCore(clauseExec, clauseExec.TermValueType);
                perClause[i] = pred;

                if (isScanCandidate is false || pred is not { } p) 
                    continue;
                
                scanList.Add(p);
                clauseIndices.Add(i);
            }

            return (scanList, clauseIndices?.ToArray(), perClause);

            static ScanPredicateInfo? BuildScanPredicateInfoCore(ClauseExecution exec, ParamValueType termType)
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
                        // Negated or boosted IN falls back to the bitmap posting-list union
                        if (exec.IsNegated || clause.HasBoost)
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
                            ParamIndex = 0
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
        }
           
        static ScanPredicateInfo[] BuildResidualSet(ScanPredicateInfo?[] perClause, int skip1, int skip2)
        {
            var residuals = new List<ScanPredicateInfo>();
            for (int i = 0; i < perClause.Length; i++)
            {
                if (i == skip1 || i == skip2)
                    continue;
                if (perClause[i] is not { } pred)
                    return null;
                residuals.Add(pred);
            }

            return residuals.ToArray();
        }

        bool CheckAllNegated() => executions is [{ IsNegated: true }, ..]; // negated clauses are always sorted first, so we can just check the first

        int ComputeOperandOrdering()
        {
            var execs = exec.Executions;
            int ordering = 0;

            for (int i = 0; i < Math.Min(execs.Count, 10); i++)
                ordering |= (execs[i].Clause.OriginalIndex & 0x7) << (i * 3);

            if (planParams.HasBoost)
                ordering |= QueryExecution.HasBoostBit;

            // Cardinality cliff bit: queries under vs. over the cliff get different compiled plans, so the bit is part of the cache key and we give them different plans
            long drivingCard = exec.DrivingClauseCardinality;
            if (drivingCard is >= 0 and <= QueryPrimitives.TieBreakGroupInitialCapacity)
                ordering |= QueryExecution.CardinalityCliffBit;
            return ordering;
        }

        (int TypeSignature, byte[] FullKinds) ComputeTypeSignature()
        {
            int types = 0; // Each unique query parameter contributes 2 bits (its runtime type: long/double/slice/sliceLong), literals are handled via the query text (separate)

            // A parameter-bound BETWEEN sentinel ("*"/"NULL") is QueryMatch-dispatched (that dispatch is
            // baked into the compiled IL), while a non-sentinel BETWEEN of the same query text is
            // TreeScan-dispatched. For numeric fields the sentinel string already shifts the type
            // signature, but for string fields the sentinel value classifies identically to a real bound,
            // so the two would collide on one cache entry. We reuse the FullKinds escape array (otherwise
            // only allocated for >16 params) as the carrier and mark the bound parameter slots, forcing a
            // distinct compiled plan. Sentinel queries are rare, so the extra allocation is acceptable.
            bool hasSentinel = HasParameterSentinelBetween(exec.Executions);
            var full = template.ParameterSlots.Length > 16 || hasSentinel ? new byte[template.ParameterSlots.Length] : null;
            for (int i = 0; i < template.ParameterSlots.Length; i++)
            {
                int kind = (int)ClassifyParamType(planParams.QueryParameters, template.ParameterSlots[i]) & 0x3;
                full?[i] = (byte)kind;
                if (i > 15) continue; // param 15 fills bits 30-31; index 16 would shift by 32 and wrap (mod-32) back onto param 0
                types |= kind << (i * 2);
            }

            if (hasSentinel)
                MarkSentinelSlots(exec.Executions, full, template.ParameterSlots);

            return (types, full);
        }
    }

    private static ClauseExecution CreateExecution(ClauseInfo clause)
    {
        RuntimeHelpers.EnsureSufficientExecutionStack();
        var exec = new ClauseExecution(clause);

        if (clause.SubClauses is null)
            return exec;

        exec.SubExecutions = new List<ClauseExecution>(clause.SubClauses.Count);
        foreach (var it in clause.SubClauses)
        {
            exec.SubExecutions.Add(CreateExecution(it));
        }

        return exec;
    }

    /// <summary>Marker bit OR-ed into a sentinel-bound parameter's FullKinds byte (kind occupies bits 0-1).
    /// Forces a distinct plan-cache entry for parameter-bound BETWEEN sentinels — see ComputeTypeSignature.</summary>
    private const byte SentinelParamMark = 1 << 2;

    /// <summary>True if any clause (recursively) is a parameter-bound BETWEEN that PopulateClauseValues
    /// rewrote to a sentinel form. Literal sentinels are excluded — those are already encoded in the
    /// query text (and rewritten away at template time), so they need no cache-key marker.</summary>
    private static bool HasParameterSentinelBetween(List<ClauseExecution> executions)
    {
        foreach (var e in executions)
        {
            if (IsParameterSentinelBetween(e))
                return true;
            if (e.SubExecutions is { Count: > 0 } && HasParameterSentinelBetween(e.SubExecutions))
                return true;
        }

        return false;
    }

    private static bool IsParameterSentinelBetween(ClauseExecution e)
    {
        if (e.ClauseType != ClauseType.Between || e.SentinelRewriteType == null)
            return false;

        foreach (var b in e.Clause.Bindings ?? [])
        {
            if (b is { Source: BindingSource.QueryParameter })
                return true;
        }

        return false;
    }

    /// <summary>Set <see cref="SentinelParamMark"/> on the FullKinds byte of every query-parameter slot
    /// bound by a sentinel-rewritten BETWEEN (recursively). This keeps sentinel and non-sentinel plans
    /// for the same query text on separate cache entries.</summary>
    private static void MarkSentinelSlots(List<ClauseExecution> executions, byte[] full, string[] parameterSlots)
    {
        foreach (var e in executions)
        {
            if (IsParameterSentinelBetween(e))
            {
                foreach (var b in e.Clause.Bindings)
                {
                    if (b is { Source: BindingSource.QueryParameter, ParameterName: not null })
                    {
                        int slot = Array.IndexOf(parameterSlots, b.ParameterName);
                        if (slot >= 0)
                            full[slot] |= SentinelParamMark;
                    }
                }
            }

            if (e.SubExecutions is { Count: > 0 })
                MarkSentinelSlots(e.SubExecutions, full, parameterSlots);
        }
    }


    private static void PopulateClauseValues(ClauseExecution exec, BlittableJsonReaderObject queryParameters, ValueWriter writer, QueryBuilderParameters builderParameters)
    {
        RuntimeHelpers.EnsureSufficientExecutionStack();
        foreach (var it in exec.SubExecutions ?? [])
        {   // Always recurse into subclauses first (OrGroup/AndGroup have no binding of their own)
            PopulateClauseValues(it, queryParameters, writer, builderParameters);
        }

        if (exec.Clause is { HasBoost: true, Bindings.Length: > 0 })
        {
            ResolveBoostFactor(exec, queryParameters);
        }

        switch (exec.Clause.ClauseType) // Spatial and vector resolve via their binding array. 
        {
            case ClauseType.Spatial when exec.Clause.Bindings is { Length: > 0 }:
                ResolveSpatialFromBindings(exec, queryParameters);
                return;
            case ClauseType.Vector when exec.Clause.Bindings is { Length: > 0 }:
                ResolveVectorFromBindings(exec, queryParameters);
                return;
        }

        if (exec.Clause.Bindings is not { Length: > 0 })
            return;

        var bindings = exec.Clause.Bindings;
        switch (exec.Clause.ClauseType)
        {
            case ClauseType.Between: // BETWEEN: Literal sentinel bounds are rewritten at template time. Parameter-bound sentinels are detected here at execution time. 
            {
                var (low, lowType) = ResolveBindingScalar(bindings[BindingIndex.BetweenLow], queryParameters, builderParameters);
                var (high, highType) = ResolveBindingScalar(bindings[BindingIndex.BetweenHigh], queryParameters, builderParameters);
                bool lowIsSentinel = low is RavenConstants.Documents.Querying.Terms.LeftNullValueOfBetweenQuery;
                bool highIsSentinel = high is RavenConstants.Documents.Querying.Terms.RightNullValueOfBetweenQuery;
                switch (lowIsSentinel, highIsSentinel)
                {
                    case (true, true):
                        exec.SentinelRewriteType = ClauseType.Exists;
                        return;
                    case (true, false):
                        exec.SentinelRewriteType = ClauseType.LessThanOrEqual;
                        exec.TermValueType = highType;
                        exec.PackedParamValue = writer.Add(high, ToValueTokenType(highType));
                        return;
                    case (false, true):
                        exec.SentinelRewriteType = ClauseType.GreaterThanOrEqual;
                        exec.TermValueType = lowType;
                        exec.PackedParamValue = writer.Add(low, ToValueTokenType(lowType));
                        return;
                    case (false, false):
                        exec.TermValueType = lowType;
                        exec.PackedParamValue = writer.AddPair(low, high, ToValueTokenType(lowType));
                        return;
                }
            }
            case ClauseType.In or ClauseType.AllIn:
                Span<ParameterBinding> inBindings =  bindings;
                if(exec.Clause.HasBoost)
                {   // Boosted clauses store the boost factor in the trailing binding (read by ResolveBoostFactor via Bindings[^1]); exclude it from the IN-term walk.
                    inBindings = inBindings[..^1];
                }
                ResolveInFromBindings(exec, queryParameters, writer, inBindings, builderParameters);
                break;
            default: // Simple clause (Equals, Range, Search, Regex, etc.): single value at Bindings[0]
                var (value, valueType) = ResolveBindingScalar(bindings[BindingIndex.Value], queryParameters, builderParameters);
                if (value == null && exec.Clause.ClauseType is ClauseType.StartsWith or ClauseType.EndsWith or ClauseType.Search or ClauseType.Regex)
                {
                    throw new InvalidQueryException(  // reject null (matches Lucene behavior).
                        $"Method {exec.Clause.ClauseType}() expects to get an argument of type String while it got Null");
                }

                exec.TermValueType = valueType;
                exec.PackedParamValue = writer.Add(value, ToValueTokenType(valueType));
                break;
        }
    }


    private static void ResolveInFromBindings(ClauseExecution exec, BlittableJsonReaderObject queryParameters, ValueWriter writer,
        Span<ParameterBinding> bindings, QueryBuilderParameters builderParameters)
    {
        var resolvedValues = new List<object>(bindings.Length);
        var termTypes = new List<ParamValueType>(bindings.Length);
        bool hasNullTerm = false;

        foreach (var it in bindings)
        {
            if (it.Source == BindingSource.QueryParameter // handle array-valued query parameters
                && queryParameters.TryGet(it.ParameterName, out object raw)
                && raw is BlittableJsonReaderArray arr)
            {
                foreach (var elem in arr)
                {
                    var (elemVal, elemType) = ResolveParameterValue(elem);
                    AddInValue(elemVal, ToParamValueType(elemType));
                }

                continue;
            }

            var (val, type) = ResolveBindingScalar(it, queryParameters, builderParameters); // normal parameter
            AddInValue(val, type);
        }

        ParamValueType dominantType = resolvedValues.Count > 0 ? termTypes[0] : ParamValueType.String;
        EmitInTerms(exec, writer, dominantType, resolvedValues, hasNullTerm);

        void AddInValue(object val, ParamValueType type)
        {
            if (val == null)
            {
                hasNullTerm = true;
                return;
            }

            resolvedValues.Add(val);
            termTypes.Add(type);
        }
    }

    private static void EmitInTerms(ClauseExecution exec, ValueWriter writer, ParamValueType dominantType, List<object> values, bool hasNullTerm)
    {
        var (packedType, startIdx) = writer.ResolveInSlot(dominantType);
        var dominantTokenType = ToValueTokenType(dominantType);

        int written = 0;
        for (int i = 0; i < values.Count; i++)
        {
            // Mixed-type IN: (IN [long, "Shalom"]). Silently drop it instead of throwing, Matches Lucene's behavior.
            if (writer.TryAdd(values[i], dominantTokenType) is null)
                continue;
            written++;
        }

        exec.PackedParamValue = new PackedParam(packedType, startIdx);
        exec.InTermCount = written;
        exec.HasNullTerm = hasNullTerm;
    }

    private static (object Value, ParamValueType Type) ResolveBindingScalar(ParameterBinding binding, BlittableJsonReaderObject queryParameters, QueryBuilderParameters builderParameters)
    {
        switch (binding.Source)
        {
            case BindingSource.Literal:
                return (binding.LiteralValue, binding.LiteralType);

            case BindingSource.DeferredMethod:
            {
                var value = binding.DeferredExpression(builderParameters, queryParameters);
                if (value == null)
                    return (null, ParamValueType.Null);
                var (val, valType) = ResolveParameterValue(value);
                return (val, ToParamValueType(valType));
            }

            case BindingSource.QueryParameter:
            default:
                if (queryParameters.TryGet(binding.ParameterName, out object raw) && raw != null)
                {
                    var (val, type) = ResolveParameterValue(raw);
                    return (val, ToParamValueType(type));
                }

                return (null, ParamValueType.Null);
        }
    }

    private static void ResolveBoostFactor(ClauseExecution exec, BlittableJsonReaderObject queryParameters)
    {
        var (boostVal, boostType) = ResolveBindingScalar(exec.Clause.Bindings[^1], queryParameters, builderParameters: null);
        if (boostVal == null) return;

        exec.BoostFactor = boostType switch
        {
            ParamValueType.Double => (float)(double)boostVal,
            _ => boostType switch
            {
                ParamValueType.Long => (long)boostVal,
                _ when float.TryParse(boostVal.ToString(), NumberStyles.Float, CultureInfo.InvariantCulture, out float parsed) => parsed,
                _ => 1f
            }
        };
    }

    /// <summary>
    /// Foo BETWEEN $x AND $y - where $y > $x - returns nothing, this re-writes the clause so we can optimize this 
    /// </summary>
    private static void PropagateBetweenContradiction(ClauseExecution exec, ValueWriter writer)
    {
        var p = exec.PackedParamValue;
        if (exec.Clause.ClauseType != ClauseType.Between || p.Param2 is PackedParam.NoParamValue)
            return;

        bool contradictory = p.ValueType switch
        {
            PackedParam.TypeLong => writer.GetLong(p.Param1) > writer.GetLong(p.Param2),
            PackedParam.TypeDouble => writer.GetDouble(p.Param1) > writer.GetDouble(p.Param2),
            _ => false // for strings, we have to consider analyzers, so we can't tell
        };
        if (!contradictory)
            return;

        exec.Cardinality = 0;
        exec.InTermCount = 0;
        exec.HasNullTerm = false;
        exec.ClauseType = ClauseType.In; // Reuse empty-IN elimination in EmitPlan
    }

    private static ScanValueType ClassifyParamType(BlittableJsonReaderObject queryParams, string name)
    {
        if (queryParams.TryGet(name, out object raw) == false || raw == null)
            return ScanValueType.Slice;
        return ClassifyValue(raw);

        static ScanValueType ClassifyValue(object raw)
        {
            RuntimeHelpers.EnsureSufficientExecutionStack();
            return raw switch
            {
                long => ScanValueType.Long,
                double => ScanValueType.Double,
                LazyNumberValue lnv => lnv.TryParseLong(out _) ? ScanValueType.Long : ScanValueType.Double,
                string { Length: < 83 } => ScanValueType.Slice, // statically skip Encoding.UTF8.GetByteCount() < 255 here, since we _know_ it's < 255 regardless
                string s when Encoding.UTF8.GetByteCount(s) < byte.MaxValue => ScanValueType.Slice,
                // we distinguish between strings > 255 because they cannot use compound field optimizations, so this ensures that we have a separate plan for them 
                string => ScanValueType.SliceLong,
                LazyStringValue lsv => lsv.Size > byte.MaxValue ? ScanValueType.SliceLong : ScanValueType.Slice,
                BlittableJsonReaderArray arr => arr.Length > 0 ? ClassifyValue(arr[0]) : ScanValueType.Slice,
                _ => ScanValueType.Slice
            };
        }
    }

    private static IQueryMatch Instantiate(
        CompiledPlan compiledPlan,
        QueryExecution exec,
        OrderMetadata[] orderByFields,
        bool hasEmptySorts,
        PlanParameters planParams,
        QueryBuilderParameters builderParameters,
        ResolutionContext walkerCtx,
        Dictionary<string, CoraxHighlightingTermIndex> highlightingTerms,
        bool wantTimings,
        out IQueryMatch innerMatch,
        CancellationToken token)
    {
        var ctx = new InstCtx(compiledPlan, exec, orderByFields, planParams, builderParameters, wantTimings);
        if (compiledPlan.Strategy == ExecutionStrategy.NotEvaluated)
            SelectExecutionStrategy(ref ctx);

        switch (compiledPlan.Strategy)
        {
            case ExecutionStrategy.CompoundExact:
                innerMatch = ConstructCompoundExact(ref ctx);
                if (innerMatch is null) goto default;
                return innerMatch;
            // orderByFields can be null when page size is 0, in which case, we need to get the actual total count
            // no advantage of using compound field here, since we can't stop midway (like we do with paging)
            case ExecutionStrategy.CompoundField when orderByFields != null:
                if (CompoundFieldCostEffective(ref ctx, out long cfEntriesToScan, out long cfBitmapCost) == false)
                    goto default; // if this isn't expected to benefit us, just use a bitmap query option
                innerMatch = ConstructCompoundField(ref ctx, walkerCtx, ctx.Exec.Plan.CompoundFieldField2RangeIdx, cfEntriesToScan, cfBitmapCost);
                if (innerMatch is null) goto default;
                return OrderBy(builderParameters, innerMatch, orderByFields, hasEmptySorts);
            case ExecutionStrategy.DirectScan when orderByFields != null:
                var execs = exec.Executions;
                bool isFullScan = execs is not { Count: > 0 };
                if (DirectScanCostEffective(ref ctx, isFullScan, out var directScanReason))
                {
                    bool hasTieBreak = orderByFields.Length == 2;
                    int drivingIdx = exec.Plan.SortDrivingClauseIndex;
                    innerMatch = ConstructDirectScan(ref ctx, walkerCtx, drivingIdx, isFullScan, hasTieBreak, directScanReason);
                    if (innerMatch is not null) 
                        return innerMatch;
                }
                goto default;
            case ExecutionStrategy.BitmapSort:
            default: // may either be the selected strategy or a one-off (because of bad parameters preventing a faster strategy)
                innerMatch = InstantiateBitmapPipeline(ctx.Plan, ctx.Exec, ctx.PlanParams, ctx.BuilderParams, walkerCtx, highlightingTerms, wantTimings, token);
                if (ctx.OrderByFields == null) return innerMatch;
                if (innerMatch is CompiledQueryMatch seekMatch)
                    TrySetSortSeekHint(ctx.Plan, ctx.Exec, seekMatch);
                return OrderBy(ctx.BuilderParams, innerMatch, ctx.OrderByFields, hasEmptySorts);
        }

        static void SelectExecutionStrategy(ref InstCtx ctx)
        {
            // ── Slow path: cache-miss, run Try* discovery chain ──
            ctx.Plan.DecisionTrail = new();
            ctx.Plan.Strategy = ExecutionStrategy.BitmapSort; // if nothing else overrides it

            if (ctx.Plan.Template.OptimizationFlags.HasFlag(PlanOptimizationFlags.CompoundExactCandidate))
            {
                if (TryCreateCompoundExactMatch(ref ctx, out ctx.RejectReason))
                {
                    ctx.Plan.Strategy = ExecutionStrategy.CompoundExact;
                    ctx.Plan.DecisionTrail.Record("CompoundExact", true, "compound exact-term lookup");
                    return;
                }

                ctx.Plan.DecisionTrail.Record("CompoundExact", false, ctx.RejectReason ?? "rejected");
            }

            if (ctx.OrderByFields is null)
            {
                ctx.Plan.DecisionTrail.Record("NoOrderBy", true, "no ORDER BY");
                return;
            }

            if (ctx.Plan.Template.OptimizationFlags.HasFlag(PlanOptimizationFlags.DirectScanCandidate))
            {
                if (TryCreateCompoundFieldMatch(ref ctx, out ctx.RejectReason))
                {
                    ctx.Plan.Strategy = ExecutionStrategy.CompoundField;
                    ctx.Plan.DecisionTrail.Record("CompoundField", true, "compound tree scan candidate (cost gated per-execution)");
                    return;
                }

                ctx.Plan.DecisionTrail.Record("CompoundField", false, ctx.RejectReason ?? "rejected");

                if (TryCreateSimpleFieldDirectScan(ref ctx, out ctx.RejectReason))
                {
                    ctx.Plan.Strategy = ExecutionStrategy.DirectScan;
                    ctx.Plan.DecisionTrail.Record("DirectScan", true, "direct tree scan candidate on sort field (cost gated per-execution)");
                    return;
                }

                ctx.Plan.DecisionTrail.Record("DirectScan", false, ctx.RejectReason ?? "rejected");
            }

            ctx.Plan.DecisionTrail.Record("BitmapSort", true, "bitmap pipeline with SortingMatch fallback");
        }
        
        static bool IsDirectScanCostEffective(long entriesToScan, long bitmapCost)
        {
            long directCost =  entriesToScan > long.MaxValue / QueryPrimitives.EntryScanCostMultiplier
                ? long.MaxValue // avoid overflow
                : entriesToScan * QueryPrimitives.EntryScanCostMultiplier;
            // check what will be more costly, and set a hard limit (32K) to how many entries we may scan
            return directCost < bitmapCost && entriesToScan <= QueryPrimitives.EntryScanCountThreshold;
        }
        
        static bool CompoundFieldCostEffective(ref InstCtx ctx, out long entriesToScan, out long bitmapCost)
        {
            entriesToScan = 0;
            bitmapCost = 0;
            var execs  = ctx.Exec.Executions;
            int drivingIdx = ctx.Exec.Plan.CompoundFieldDrivingClause;

            var drivingExec = execs[drivingIdx];
            if (drivingExec.PackedParamValue.IsNone)
                return false;

            var indexSearcher = ctx.PlanParams.IndexSearcher;
            int field2RangeIdx = ctx.Exec.Plan.CompoundFieldField2RangeIdx;
            int residualCount = 0;
            for (int i = 0; i < execs.Count; i++)
            {
                bitmapCost += execs[i].GetEffectiveCardinality(indexSearcher);
                if (i == drivingIdx || i == field2RangeIdx)
                    continue;
                residualCount++;
            }

            long drivingCardinality = drivingExec.GetEffectiveCardinality(indexSearcher);
            entriesToScan = residualCount > 0
                ? ComputeNumberOfEntriesQueryLikelyToScan(execs, drivingIdx, drivingCardinality, ctx.BuilderParams.Query.PageSize, indexSearcher)
                : drivingCardinality;

            return IsDirectScanCostEffective(entriesToScan, bitmapCost);
        }
        
        static bool DirectScanCostEffective(ref InstCtx ctx, bool isFullScan, out string directScanReason)
        {
            if (isFullScan)
            {
                directScanReason = "full scan requested";
                return true;
            }

            directScanReason = null;
        
            var execs = ctx.Exec.Executions;
            int drivingIdx = ctx.Exec.Plan.SortDrivingClauseIndex;
            if (drivingIdx < 0 || drivingIdx >= execs.Count)
                return false;
            if (execs[drivingIdx].PackedParamValue.IsNone)
                return false;

            long bitmapCost = 0;
            var indexSearcher = ctx.PlanParams.IndexSearcher;
            foreach (var it in execs)
            {
                bitmapCost += it.GetEffectiveCardinality(indexSearcher);
            }

            long drivingCard = execs[drivingIdx].GetEffectiveCardinality(indexSearcher);
            var entriesToScan = execs.Count > 1
                ? ComputeNumberOfEntriesQueryLikelyToScan(execs, drivingIdx, drivingCard, ctx.BuilderParams.Query.PageSize, indexSearcher)
                : drivingCard;

            if (ctx.WantTimings)
                directScanReason = $"entries_to_scan({entriesToScan}) × {QueryPrimitives.EntryScanCostMultiplier} < bitmap_cost({bitmapCost})";
            return IsDirectScanCostEffective(entriesToScan, bitmapCost);
        }

        static long ComputeNumberOfEntriesQueryLikelyToScan(List<ClauseExecution> execs,
            int drivingIdx, long drivingCard, long pageSize, IndexSearcher indexSearcher)
        {
            long resultsWanted = Math.Min(drivingCard, pageSize);

            long minResidual = long.MaxValue;
            for (int i = 0; i < execs.Count; i++)
            {
                if (i == drivingIdx) continue;
                long c = execs[i].GetEffectiveCardinality(indexSearcher);
                minResidual = Math.Min(c, minResidual);
            }

            if (minResidual > 0 && minResidual < indexSearcher.NumberOfEntries)
            {
                // here we check what is the pass rate of the most selective residual clause (i.e, 1% of entries matched, etc)
                double passRate = (double)minResidual / indexSearcher.NumberOfEntries;
                if (passRate > 0)
                {
                    // if the pass rate is 1%, we have to scan through 10_000 entries to get 100, etc, so we need to inflate the costs.
                    // We inflate the *results wanted* (page-bounded) rather than the full driving cardinality: filling a 10-row
                    // page through a 1%-selective residual means scanning ~1_000 entries, regardless of how large the driving set is.
                    return (long)(resultsWanted / passRate);
                }
            }
            return resultsWanted;
        }
    }

    private static IQueryMatch InstantiateBitmapPipeline(
        CompiledPlan compiledPlan,
        QueryExecution exec,
        PlanParameters planParams,
        QueryBuilderParameters builderParameters,
        ResolutionContext walkerCtx,
        Dictionary<string, CoraxHighlightingTermIndex> highlightingTerms,
        bool wantTimings,
        CancellationToken token)
    {
        var indexSearcher = planParams.IndexSearcher;

        // Spatial / Vector queries with no other clauses ( WHERE spatial.within() / WHERE vector.search() )
        // use a dedicated code path to avoid AllEntries + post-filters
        if (exec is { IsAllEntries: true, HasSpatialOrVector: true })
            return InstantiateAllEntriesPostFilter(exec, builderParameters, walkerCtx, wantTimings);

        var (resolvedMatches, leaves) = ResolveAllSlots(exec, walkerCtx, planParams.HasBoost);

        if (highlightingTerms != null)
            PopulateHighlightingTerms(exec, highlightingTerms, planParams.Metadata);

        var compiledMatch = new CompiledQueryMatch(
            compiledPlan, exec, compiledPlan.RequiredBitmaps, compiledPlan.OpCount, resolvedMatches, leaves,
            indexSearcher, planParams.Allocator, wantTimings, token)
        {
            InRangeCounts = exec.InRangeCounts,
            Cardinalities = exec.Cardinalities,
        };

        if (exec.Plan.ScanPredicateInfos is { Count: > 0 })
        {
            exec.PopulateScanParams = () => ScanParamExtractor.Extract(exec, indexSearcher, walkerCtx);
        }

        IQueryMatch[] spatialMatches = null;
        if (exec.SpatialFilters is { Length: > 0 })
        {
            spatialMatches = new IQueryMatch[exec.SpatialFilters.Length];
            for (int sf = 0; sf < exec.SpatialFilters.Length; sf++)
                spatialMatches[sf] = resolvedMatches[exec.SpatialFilters[sf].MatchIndex];
        }

        return ApplyPostFilters(compiledMatch, spatialMatches, exec, builderParameters, wantTimings);
    }

    private static IQueryMatch ApplyPostFilters(
        IQueryMatch source, IQueryMatch[] spatialMatches,
        QueryExecution exec, QueryBuilderParameters builderParameters, bool wantTimings)
    {
        IQueryMatch result = source;

        if (spatialMatches is { Length: > 0 })
        {
            result = result is null
                ? new PostFilterMatch(spatialMatches[0], spatialMatches.Length is 1 ? [] : spatialMatches[1..], wantTimings)
                : new PostFilterMatch(result, spatialMatches, wantTimings);
        }

        if (exec.VectorSelects is { Length: > 0 })
        {
            foreach (var item in ResolveVectorItems(exec, builderParameters))
            {
                result = item.Materialize(result);
            }
        }

        return result;
    }

    /// <summary>
    /// Bypass path for queries with no real WHERE clauses — only spatial filters and/or  vector selects. 
    /// </summary>
    private static IQueryMatch InstantiateAllEntriesPostFilter(QueryExecution exec, QueryBuilderParameters builderParameters, ResolutionContext walkerCtx, bool wantTimings)
    {
        // No real WHERE clause, so the spatial clauses aren't in resolvedMatches — resolve them directly.
        IQueryMatch[] spatialMatches = null;
        if (exec.SpatialFilters is { Length: > 0 })
        {
            spatialMatches = new IQueryMatch[exec.SpatialFilters.Length];
            for (int i = 0; i < exec.SpatialFilters.Length; i++)
                spatialMatches[i] = ResolveClause(exec.SpatialFilters[i].Exec, exec, walkerCtx);
        }

        return ApplyPostFilters(source: null, spatialMatches, exec, builderParameters, wantTimings);
    }


    
    public static IQueryMatch BuildQueryForMoreLikeThis(QueryBuilderParameters builderParams, QueryExpression expression)
    {
        const string moreLikeThisCacheKeyPrefix = "$mlt$:";

        return BuildFilterMatch(new PlanParameters
        {
            IndexSearcher = builderParams.IndexSearcher,
            Metadata = builderParams.Query.Metadata,
            QueryParameters = builderParams.QueryParameters,
            Index = builderParams.Index,
            IndexFieldsMapping = builderParams.IndexFieldsMapping,
            Allocator = builderParams.Allocator,
            HasDynamics = builderParams.HasDynamics,
            DynamicFields = builderParams.DynamicFields,
            HasBoost = builderParams.HasBoost,
            WhereOverride = expression,
            CacheKeyOverride = moreLikeThisCacheKeyPrefix + expression.GetText(builderParams.Query),
        }, builderParams, out _, out _, highlightingTerms: null, wantTimings: false, builderParams.Token);
    }

    
    private static bool TryCreateCompoundExactMatch(ref InstCtx ctx, out string rejectReason)
    {
        if (ctx.PlanParams.Index is null || ctx.Exec is not
            {
                Executions: { Count: >= 2 } executions,
                Plan:
                {
                    AllNegated: false,
                    CompoundExactClauseA: var a and >= 0,
                    CompoundExactClauseB: var b and >= 0
                }
            } || a >= executions.Count || b >= executions.Count)
        {
            rejectReason = "no compound-exact clause pair identified at template time";
            return false;
        }

        if (IsClauseBoosted(executions[a]) || executions[a].PackedParamValue.IsNone ||
            IsClauseBoosted(executions[b]) || executions[b].PackedParamValue.IsNone)
        {
            rejectReason = "composite key encoding failed or exceeded max term length, or clause is boosted";
            return false;
        }

        rejectReason = null;
        return true;
    }

    private static IQueryMatch ConstructCompoundExact(ref InstCtx ctx)
    {
        var execs = ctx.Exec.Executions;
        var indexSearcher = ctx.PlanParams.IndexSearcher;
        var eA = execs[ctx.Exec.Plan.CompoundExactClauseA];
        var eB = execs[ctx.Exec.Plan.CompoundExactClauseB];

        var (firstField, secondField, firstExec, secondExec) = ctx.Exec.Plan.Template.CompoundExactAFirst
            ? (eA.Clause.ResolvedFieldName ?? eA.Clause.FieldName, eB.Clause.ResolvedFieldName ?? eB.Clause.FieldName, eA, eB)
            : (eB.Clause.ResolvedFieldName ?? eB.Clause.FieldName, eA.Clause.ResolvedFieldName ?? eA.Clause.FieldName, eB, eA);
        
        if (TryGetCompoundFieldEncoding(ref ctx, firstField, firstExec.PackedParamValue, firstExec.PackedParamValue.Param1, out var enc1) == false || 
            TryGetCompoundFieldEncoding(ref ctx, secondField, secondExec.PackedParamValue, secondExec.PackedParamValue.Param1, out var enc2) == false)
            return null;

        int totalLen = enc1.Size + enc2.Size + 1;
        if (totalLen > Constants.Terms.MaxLength) 
            return null;

        ctx.PlanParams.Allocator.Allocate(totalLen, out ByteString keyBuf);
        var keySpan = keyBuf.ToSpan();
        WriteCompoundFieldEncoding(keySpan.Slice(0, enc1.Size), enc1, ctx.Exec);
        WriteCompoundFieldEncoding(keySpan.Slice(enc1.Size, enc2.Size), enc2, ctx.Exec);
        keySpan[totalLen - 1] = (byte)enc1.Size;

        var compoundFieldMeta = indexSearcher.FieldMetadataBuilder(ctx.Exec.Plan.Template.CompoundExactName, hasBoost: false);

        return indexSearcher.TermQuery(compoundFieldMeta, new Slice(keyBuf));
    }

    private static bool TryCreateCompoundFieldMatch(ref InstCtx ctx, out string rejectReason)
    {
        if (ctx.Exec.Plan.CompoundFieldDrivingClause < 0 || ctx.Exec.Plan.Template.CompoundFieldSortName is null)
        {
            rejectReason = "no compound-field candidate identified at template time";
            return false;
        }

        if (ctx.Exec.Plan.AllNegated)
        {
            rejectReason = "all clauses are negated";
            return false;
        }

        var execs = ctx.Exec.Executions;
        for (int i = 0; i < execs.Count; i++)
        {
            if (i == ctx.Exec.Plan.CompoundFieldDrivingClause || i == ctx.Exec.Plan.CompoundFieldField2RangeIdx)
                continue;
            if (IsClauseBoosted(execs[i]))
            {
                rejectReason = "boosted clause found";
                return false;
            }
        }

        if (ctx.Exec.Plan.CompoundFieldResiduals is null)
        {
            rejectReason = "scan predicate info is null";
            return false;
        }

        rejectReason = null;
        return true;
    }

    private static IQueryMatch ConstructCompoundField(ref InstCtx ctx, ResolutionContext walkerCtx, int field2RangeIdx, long entriesToScan, long bitmapCost)
    {
        var execs = ctx.Exec.Executions;
        var indexSearcher = ctx.PlanParams.IndexSearcher;
        int drivingClauseIdx = ctx.Exec.Plan.CompoundFieldDrivingClause;

        var packed = execs[drivingClauseIdx].PackedParamValue;

        if (ctx.Exec.Plan.CompoundFieldResiduals is null)
            return null;

        string field1Name = execs[drivingClauseIdx].Clause.FieldName;
        string compoundFieldName = ctx.Exec.Plan.Template.CompoundFieldName;
        var compoundFieldMeta = indexSearcher.FieldMetadataBuilder(compoundFieldName, hasBoost: false);

        // Build the prefix bytes for field1's value.
        Slice analyzedPrefix = BuildField1Prefix(ref ctx, field1Name, packed, out string field1ValueStr);
        if (analyzedPrefix.HasValue == false || analyzedPrefix.Size > byte.MaxValue) // if too long, cannot be used for compound
            return null; // fall back to bitmap

        ScanParamExtractor.Extract(ctx.Exec, indexSearcher, walkerCtx);

        IQueryMatch drivingMatch = CreateDrivingMatch(ref ctx);
        DirectScanMatchBase directScan = (ctx.Exec.Plan.CompoundFieldResiduals is null or { Length: 0 }
            ? new DirectScanSimpleMatch(indexSearcher, drivingMatch, take: -1)
            : new DirectScanFilteredMatch(indexSearcher, drivingMatch, ctx.Exec, take: -1, precompiledDelegate: ctx.Plan.CompiledEntryPredicate));

        if (ctx.WantTimings) // only used when we use include timings()
            SetDirectScanPropertiesForIntrospection(ref ctx);

        return directScan;

        IQueryMatch CreateDrivingMatch(ref InstCtx context)
        {
            string fieldName = context.Exec.Plan.Template.CompoundFieldSortName;
            if (field2RangeIdx >= 0 && 
                TryBuildCompositeRangeKeys(ref context, analyzedPrefix, fieldName, execs[field2RangeIdx], out var lowSlice, out var highSlice))
            {
                return indexSearcher.RangeBuilder<Range.Inclusive, Range.Inclusive>(
                    compoundFieldMeta, lowSlice, highSlice,
                    forward: context.OrderByFields[0].Ascending, CancellationToken.None);
            }

            // No field2 narrowing available: run a prefix scan on field1 only and let entry-scan residuals filter the rest.
            return indexSearcher.StartWithQuery(compoundFieldMeta, analyzedPrefix,
                isNegated: false, forward: context.OrderByFields[0].Ascending,
                validatePostfixLen: true);
        }

        void SetDirectScanPropertiesForIntrospection(ref InstCtx context)
        {
            directScan.DrivingTreeName = compoundFieldName;
            directScan.DrivingClause = $"{field1Name} = '{field1ValueStr}'";
            directScan.SeekBound = $"'{field1ValueStr}' (prefix, validatePostfixLen)";
            directScan.Direction = context.OrderByFields[0].Ascending ? "Forward" : "Backward";
            directScan.ResidualDescription = context.Exec.Plan.CompoundFieldResiduals == null ? null : string.Join(", ", Array.ConvertAll(context.Exec.Plan.CompoundFieldResiduals, p => $"{p.FieldName} {p.CompareOp}"));
            directScan.Reason = $"entries_to_scan({entriesToScan}) × {QueryPrimitives.EntryScanCostMultiplier} < bitmap_cost({bitmapCost})";
        }
    }

    private static Slice BuildField1Prefix(ref InstCtx ctx, string field1Name, PackedParam packed, out string field1ValueStrForIntrospection)
    {
        var indexSearcher = ctx.PlanParams.IndexSearcher;
        switch (packed.ValueType)
        {
            case PackedParam.TypeString:
            {
                field1ValueStrForIntrospection = ctx.Exec.StringValues[packed.Param1];
                var field1Meta = QueryBuilderHelper.GetFieldMetadata(in ctx.BuilderParams, field1Name, hasBoost: false);
                return ctx.Exec.GetAnalyzedSlice(indexSearcher, field1Meta, packed.Param1);
            }
            case PackedParam.TypeLong:
            {
                // skip the ToString allocation unless this is an inspected query.
                field1ValueStrForIntrospection = ctx.WantTimings ? ctx.Exec.LongValues[packed.Param1].ToString() : null;
                ctx.PlanParams.Allocator.Allocate(sizeof(long), out ByteString buf);
                EncodeNumericValue(buf.ToSpan(), PackedParam.TypeLong, packed.Param1, ctx.Exec);
                return new Slice(buf);
            }
            case PackedParam.TypeDouble:
            {
                field1ValueStrForIntrospection = ctx.WantTimings ? ctx.Exec.DoubleValues[packed.Param1].ToString(CultureInfo.InvariantCulture) : null;
                ctx.PlanParams.Allocator.Allocate(sizeof(long), out ByteString buf);
                EncodeNumericValue(buf.ToSpan(), PackedParam.TypeDouble, packed.Param1, ctx.Exec);
                return new Slice(buf);
            }
            default:
                field1ValueStrForIntrospection = null;
                return default;
        }
    }

    private static bool TryBuildCompositeRangeKeys(ref InstCtx ctx, Slice analyzedPrefix, string sortFieldName,
        ClauseExecution field2Exec, out Slice lowSlice, out Slice highSlice)
    {
        lowSlice = default;
        highSlice = default;

        var field2Packed = field2Exec.PackedParamValue;
        if (field2Packed.IsNone)
            return false;

        if (TryGetCompoundFieldEncoding(ref ctx, sortFieldName, field2Packed, field2Packed.Param1, out var encLow) == false)
            return false;
        
        CompoundFieldEncoding encHigh = default;
        if (field2Exec.Clause.ClauseType is ClauseType.Between && 
            TryGetCompoundFieldEncoding(ref ctx, sortFieldName, field2Packed, field2Packed.Param2, out encHigh) == false)
            return false;

        var (lowEnc, highEnc, lowSuffixSize, highSuffixSize) = field2Exec.Clause.ClauseType switch
        {
            // e.g. WHERE field1 = X AND field2 BETWEEN Y AND Z ORDER BY field1, field2
            ClauseType.Between => (encLow, encHigh, encLow.Size, encHigh.Size),
            // e.g. WHERE field1 = X AND field2 > Y (or >=) ORDER BY field1, field2
            ClauseType.GreaterThan or ClauseType.GreaterThanOrEqual => (encLow, default, encLow.Size, encLow.Size),
            // e.g. WHERE field1 = X AND field2 < Y (or <=) ORDER BY field1, field2
            ClauseType.LessThan or ClauseType.LessThanOrEqual =>  (default, encLow, encLow.Size, encLow.Size),
            // Fall back to a prefix-only scan, will fail the length check
            _ => (default(CompoundFieldEncoding), default(CompoundFieldEncoding), Constants.Terms.MaxLength, Constants.Terms.MaxLength)
        };

        if (analyzedPrefix.Size + lowSuffixSize + 1 > Constants.Terms.MaxLength ||
            analyzedPrefix.Size + highSuffixSize + 1 > Constants.Terms.MaxLength)
            return false;

        lowSlice = WriteCompositeRangeKey(ref ctx, analyzedPrefix, lowSuffixSize, in lowEnc, openFill: 0x00);
        highSlice = WriteCompositeRangeKey(ref ctx, analyzedPrefix, highSuffixSize, in highEnc, openFill: 0xFF);
        return true;
        
        static Slice WriteCompositeRangeKey(ref InstCtx ctx, Slice analyzedPrefix, int suffixSize, in CompoundFieldEncoding  suffixEncoding, byte openFill)
        {
            int len = analyzedPrefix.Size + suffixSize + 1;
            ctx.PlanParams.Allocator.Allocate(len, out ByteString buf);
            Span<byte> span = buf.ToSpan();
            analyzedPrefix.CopyTo(span);

            Span<byte> suffix = span.Slice(analyzedPrefix.Size, suffixSize);
            if (suffixEncoding is { } enc)
                WriteCompoundFieldEncoding(suffix, enc, ctx.Exec);
            else
                suffix.Fill(openFill);

            span[len - 1] = (byte)analyzedPrefix.Size;
            return new Slice(buf);
        }
    }
    
    private static bool TryCreateSimpleFieldDirectScan(ref InstCtx ctx, out string rejectReason)
    {
        if (ctx.OrderByFields is not { Length: not 0 })
        {
            rejectReason = "no ORDER BY fields";
            return false;
        }

        if (ctx.OrderByFields.Length > 2)
        {
            rejectReason = "ORDER BY has too many fields (max 2 for direct scan)";
            return false;
        }

        bool hasTieBreak = ctx.OrderByFields.Length == 2;
        if (hasTieBreak)
        {
            var tieBreakType = ctx.OrderByFields[1].FieldType;
            if (tieBreakType is not (MatchCompareFieldType.Integer or MatchCompareFieldType.Floating or MatchCompareFieldType.Sequence))
            {
                rejectReason = "tie-break field type isn't numeric or string";
                return false;
            }
        }

        var execs = ctx.Exec.Executions;
        bool isFullScan = execs is not { Count: not 0 };

        if (isFullScan)
        {
            if (ctx.Exec.Plan.AllNegated)
            {
                rejectReason = "all clauses are negated";
                return false;
            }
            if (ctx.OrderByFields[0].MayHaveMissingEntries)
            {
                rejectReason = "sort field may have missing entries";
                return false;
            }
            if (ctx.OrderByFields[0].FieldType is not (MatchCompareFieldType.Sequence or MatchCompareFieldType.Integer or MatchCompareFieldType.Floating))
            {
                rejectReason = "full-scan sort field type is not numeric or string";
                return false;
            }
            rejectReason = null;
            return true;
        }

        int drivingIdx = ctx.Exec.Plan.SortDrivingClauseIndex;
        if (drivingIdx < 0)
        {
            rejectReason = "no range/equals clause on sort field (or WHEN eliminated the candidate)";
            return false;
        }

        if (ctx.Exec.Plan.DirectScanResiduals is null)
        {
            rejectReason = "non-scannable residual clause";
            return false;
        }

        rejectReason = null;
        return true;
    }

    private static IQueryMatch ConstructDirectScan(ref InstCtx ctx, ResolutionContext walkerCtx,
        int drivingIdx, bool isFullScan, bool hasTieBreak, string reasonForInspection)
    {
        var indexSearcher = ctx.PlanParams.IndexSearcher;
        string sortFieldName = ctx.WantTimings ? ctx.OrderByFields[0].Field.FieldName.ToString() : null;
        bool forward = ctx.OrderByFields[0].Ascending;

        if (ctx.Exec.Plan.DirectScanResiduals is null)
            return null;

        var (drivingMatchProvider, drivingClauseDescription) = isFullScan ? 
            ResolveFullScanDrivingProvider(ref ctx, forward) : 
            ResolveDrivingProvider(ref ctx, walkerCtx, drivingIdx, forward);
        
        if (drivingMatchProvider is not TermsProviderMatch tpm)
            return null; // can happen if we have no entries for this field

        bool nullFirst = ResolveNullFirst(ctx.OrderByFields[0], ctx.BuilderParams.Index.Configuration.NullsSortMode, forward);
        IQueryMatch drivingMatch = hasTieBreak
            ? BuildSortedDrivingWithTieBreakMatch(ctx, tpm.Provider, tpm.Llt, ctx.BuilderParams.Index.Configuration.NullsSortMode, indexSearcher, nullFirst)
            : new SortedDrivingMatch(tpm.Provider, tpm.Llt, ctx.PlanParams.Allocator, indexSearcher, ctx.OrderByFields[0].Field, nullFirst);

        ScanParamExtractor.Extract(ctx.Exec, indexSearcher, walkerCtx);
        DirectScanMatchBase ds = ctx.Exec.Plan.DirectScanResiduals is null or { Length: 0 }
            ? new DirectScanSimpleMatch(indexSearcher, drivingMatch, take: Constants.IndexSearcher.TakeAll)
            : new DirectScanFilteredMatch(indexSearcher, drivingMatch, ctx.Exec, take: Constants.IndexSearcher.TakeAll, precompiledDelegate: ctx.Plan.CompiledEntryPredicate);

        if (ctx.WantTimings)
        {
            PopulateDirectScanInspection(ds, sortFieldName, drivingClauseDescription, forward, ctx.Exec.Plan.DirectScanResiduals,
                isFullScan ? "full index-only scan (no WHERE clause)" : reasonForInspection);
        }
        return ds;
        
        static (IQueryMatch, string) ResolveDrivingProvider(ref InstCtx ctx, ResolutionContext walkerCtx, int drivingIdx, bool forward)
        {
            var drivingExec = ctx.Exec.Executions[drivingIdx];
            var match = drivingExec.ClauseType == ClauseType.Equals
                ? ResolveEqualsClauseWithDirection(drivingExec, ctx.Exec, forward, walkerCtx)
                : ResolveRangeClauseWithDirection(drivingExec, ctx.Exec, forward, walkerCtx);
        
            return (match, ctx.WantTimings ? $"{drivingExec.Clause.FieldName} {drivingExec.ClauseType}" : null);
        }
        
        
        static IQueryMatch ResolveEqualsClauseWithDirection(ClauseExecution drivingExec, QueryExecution queryExec, bool forward, ResolutionContext walkerCtx)
        {
            var indexSearcher = walkerCtx.IndexSearcher;
            FieldMetadata fieldMeta = ResolveFieldMetadata(drivingExec.Clause, walkerCtx);
            var packed = drivingExec.PackedParamValue;
            return packed.ValueType switch
            {
                PackedParam.TypeLong => indexSearcher.BetweenQuery(fieldMeta, queryExec.LongValues[packed.Param1], queryExec.LongValues[packed.Param1], forward: forward),
                PackedParam.TypeDouble => indexSearcher.BetweenQuery(fieldMeta, queryExec.DoubleValues[packed.Param1], queryExec.DoubleValues[packed.Param1], forward: forward),
                _ => indexSearcher.BetweenQuery(fieldMeta, queryExec.StringValues[packed.Param1], queryExec.StringValues[packed.Param1], forward: forward)
            };
        }

        static IQueryMatch ResolveRangeClauseWithDirection(ClauseExecution drivingExec, QueryExecution queryExec, bool forward, ResolutionContext walkerCtx)
        {
            var indexSearcher = walkerCtx.IndexSearcher;
            FieldMetadata fieldMeta = ResolveFieldMetadata(drivingExec.Clause, walkerCtx);
            var packed = drivingExec.PackedParamValue;

            return drivingExec.ClauseType switch
            {
                ClauseType.GreaterThan or ClauseType.GreaterThanOrEqual or ClauseType.LessThan or ClauseType.LessThanOrEqual
                    => packed.RangeQuery(drivingExec.ClauseType, fieldMeta, indexSearcher, queryExec, forward),
                ClauseType.Between when drivingExec.SentinelRewriteType != null =>
                    ResolveSentinelRewrittenBetween(drivingExec, fieldMeta, indexSearcher, queryExec),
                ClauseType.Between => packed.BetweenQuery(fieldMeta, indexSearcher, queryExec, forward),
                _ => ResolveClause(drivingExec, queryExec, walkerCtx) // fallback
            };
        }

        static (IQueryMatch, string) ResolveFullScanDrivingProvider(ref InstCtx ctx, bool forward)
        {
            var indexSearcher = ctx.PlanParams.IndexSearcher;
            var fieldMeta = ctx.OrderByFields[0].Field;
            var sortFieldType = ctx.OrderByFields[0].FieldType;
            var match = sortFieldType switch
            {
                MatchCompareFieldType.Integer => indexSearcher.BetweenQuery(fieldMeta, long.MinValue, long.MaxValue, forward: forward),
                MatchCompareFieldType.Floating => indexSearcher.BetweenQuery(fieldMeta, double.MinValue, double.MaxValue, forward: forward),
                _ => indexSearcher.ExistsQuery(fieldMeta, forward: forward)
            };
            return (match, ctx.WantTimings ? $"{fieldMeta.FieldName} [all]" : null);
        }
        
        static void PopulateDirectScanInspection(DirectScanMatchBase ds, string sortFieldName, string drivingClauseDescription, bool forward,
            ScanPredicateInfo[] residualArray, string reason)
        {
            ds.DrivingTreeName = sortFieldName;
            ds.DrivingClause = drivingClauseDescription;
            ds.Direction = forward ? "Forward" : "Backward";
            ds.ResidualDescription = residualArray == null ? null : string.Join(", ", Array.ConvertAll(residualArray, p => $"{p.FieldName} {p.CompareOp}"));
            ds.Reason = reason;
        }
    }

    private static bool ResolveNullFirst(in OrderMetadata orderByField, NullsSortMode indexDefault, bool forward)
    {
        bool nullIsSmallest = (orderByField.NullsSortMode ?? indexDefault) == NullsSortMode.NullsSmallest;
        return forward ? nullIsSmallest : nullIsSmallest is false;
    }

    private static IQueryMatch BuildSortedDrivingWithTieBreakMatch(InstCtx ctx, ITermsProvider provider, LowLevelTransaction llt, NullsSortMode indexDefaultNullsSortMode,
        IndexSearcher indexSearcher, bool nullFirst)
    {
        bool secondaryNullIsSmallest = (ctx.OrderByFields[1].NullsSortMode ?? indexDefaultNullsSortMode) == NullsSortMode.NullsSmallest;
        int take = ctx.BuilderParams?.Take ?? Constants.IndexSearcher.TakeAll;
        return new SortedDrivingWithTieBreakMatch(
            provider, llt, ctx.PlanParams.Allocator, indexSearcher,
            ctx.OrderByFields[0].Field, ctx.OrderByFields[1].Field,
            ctx.OrderByFields[1].FieldType, secondaryDescending: !ctx.OrderByFields[1].Ascending,
            nullFirst: nullFirst, nullIsSmallest: secondaryNullIsSmallest,
            take: take);
    }

    private static (IQueryMatch[], LeafResolveInfo[]) ResolveAllSlots(QueryExecution exec, ResolutionContext walkerCtx, bool planHasBoost)
    {
        Debug.Assert((exec.IsAllEntries && exec.HasSpatialOrVector) is false);

        if (exec.IsAllEntries) // nothing to do here
            return ( [walkerCtx.IndexSearcher.AllEntries()], [new LeafResolveInfo { Kind = LeafResolveKind.PreResolved }]);

        if (exec.Executions is not { Count: > 0 })
            return ([], []);

        var matchList = new List<IQueryMatch>();
        var leafList = new List<LeafResolveInfo>();
        foreach (var clauseExec in exec.Executions)
        {
            ResolveLeafIntoAll(walkerCtx, clauseExec, exec, planHasBoost, matchList, leafList);
        }

        return (matchList.ToArray(), leafList.ToArray());
    }

    private static void ResolveLeafIntoAll(ResolutionContext walkerCtx,
        ClauseExecution clauseExec, QueryExecution root, bool planHasBoost,
        List<IQueryMatch> matches, List<LeafResolveInfo> leaves)
    {
        RuntimeHelpers.EnsureSufficientExecutionStack();
        switch (clauseExec.ClauseType)
        {
            case ClauseType.OrGroup or ClauseType.AndGroup:
                foreach (var it in clauseExec.SubExecutions)
                {
                    ResolveLeafIntoAll(walkerCtx, it, root, planHasBoost, matches, leaves);
                }
                break;
            case ClauseType.AllIn or ClauseType.In:
            {
                MatchDispatch dispatch = planHasBoost ? MatchDispatch.QueryMatch : MatchDispatch.PostingList;
                for (int i = 0; i < clauseExec.InTermCount; i++)
                {
                    AddInTermSlot(dispatch,  i);
                }
                AddNullTermSlot(dispatch); // Null-term slot is always allocated; dispatch decides how it resolves.
                break;
            }
            default:
            {
                AddDefaultSlot(planHasBoost ? MatchDispatch.QueryMatch : GetDispatch(clauseExec));
                break;
            }
        }
        
        void AddInTermSlot(MatchDispatch dispatch, int termIndex)
        {
            switch (dispatch)
            {
                case MatchDispatch.QueryMatch:
                    matches.Add(ResolveInTerm(clauseExec, termIndex, root, walkerCtx));
                    leaves.Add(new LeafResolveInfo { Kind = LeafResolveKind.PreResolved });
                    return;
                default: 
                    matches.Add(null);
                    leaves.Add(new LeafResolveInfo
                    {
                        Kind = LeafResolveKind.TermPosting,
                        ClauseType = clauseExec.ClauseType,
                        Packed = clauseExec.PackedParamValue.WithTermOffset(termIndex),
                        FieldMeta = ResolveFieldMetadata(clauseExec.Clause, walkerCtx)
                    });
                    break;
            }
        }
        
        void AddNullTermSlot(MatchDispatch dispatch)
        {
            switch (dispatch)
            {
                case MatchDispatch.QueryMatch:
                    var indexSearcher = walkerCtx.IndexSearcher;
                    FieldMetadata nullMeta = ResolveFieldMetadata(clauseExec.Clause, walkerCtx);
                    matches.Add(clauseExec.HasNullTerm
                        ? indexSearcher.TermQuery(nullMeta, null)
                        : TermMatch.CreateEmpty(indexSearcher, indexSearcher.Allocator));
                    leaves.Add(new LeafResolveInfo { Kind = LeafResolveKind.PreResolved });
                    return;
                default: 
                    matches.Add(null);
                    LeafResolveInfo ret = clauseExec.HasNullTerm is false
                        ? new LeafResolveInfo
                        {
                            Kind = clauseExec.ClauseType == ClauseType.AllIn ? LeafResolveKind.AllPosting : LeafResolveKind.EmptyPosting
                        }
                        : new LeafResolveInfo
                        {
                            Kind = LeafResolveKind.NullPosting,
                            ClauseType = clauseExec.ClauseType,
                            FieldMeta = ResolveFieldMetadata(clauseExec.Clause, walkerCtx)
                        };

                    leaves.Add(ret);
                    break;
            }
        }
        
        void AddDefaultSlot(MatchDispatch dispatch)
        {
            switch (dispatch)
            {
                case MatchDispatch.QueryMatch:
                {
                    IQueryMatch match = ResolveClause(clauseExec, root, walkerCtx);
                    if (clauseExec.BoostFactor is not 0)
                        match = walkerCtx.IndexSearcher.Boost(match, clauseExec.BoostFactor);
                    matches.Add(match);
                    leaves.Add(new LeafResolveInfo { Kind = LeafResolveKind.PreResolved });
                    break;
                }
                case MatchDispatch.PostingList:
                    matches.Add(null);
                    leaves.Add(new LeafResolveInfo
                    {
                        Kind = LeafResolveKind.TermPosting,
                        ClauseType = clauseExec.ClauseType,
                        Packed = clauseExec.PackedParamValue,
                        FieldMeta = ResolveFieldMetadata(clauseExec.Clause, walkerCtx)
                    });
                    break;
                case MatchDispatch.TreeScan:
                    matches.Add(null);
                    leaves.Add(new LeafResolveInfo
                    {
                        Kind = LeafResolveKind.TreeScan,
                        ClauseType = clauseExec.ClauseType,
                        Packed = clauseExec.PackedParamValue,
                        FieldMeta = ResolveFieldMetadata(clauseExec.Clause, walkerCtx)
                    });
                    break;
                default:
                    throw new ArgumentOutOfRangeException(dispatch.ToString()); 
            }
        }
    }

    private static IQueryMatch ResolveClause(ClauseExecution cur, QueryExecution root, ResolutionContext walkerCtx)
    {
        var clause = cur.Clause;
        var indexSearcher = walkerCtx.IndexSearcher;
        var builderParams = walkerCtx.BuilderParams;
      
      
        FieldMetadata fieldMeta = default;
        // Spatial/Vector/Search have their own field resolution paths.
        if (clause.ClauseType is not ClauseType.Spatial and not ClauseType.Vector and not ClauseType.Search)
        {
            fieldMeta = ResolveFieldMetadata(clause, walkerCtx);
        }

        var packed = cur.PackedParamValue;

        switch (clause.ClauseType)
        {
            case ClauseType.Equals:
            case ClauseType.NotEquals:
                return packed.TermQuery(fieldMeta, indexSearcher, root);

            case ClauseType.GreaterThan:
            case ClauseType.GreaterThanOrEqual:
            case ClauseType.LessThan:
            case ClauseType.LessThanOrEqual:
                return packed.RangeQuery(clause.ClauseType, fieldMeta, indexSearcher, root);

            case ClauseType.Between:
                if (cur.SentinelRewriteType != null)
                    return ResolveSentinelRewrittenBetween(cur, fieldMeta, indexSearcher, root);
                return packed.BetweenQuery(fieldMeta, indexSearcher, root);

            case ClauseType.In:
            case ClauseType.AllIn:
                throw new InvalidOperationException(
                    "In/AllIn should be expanded by ResolveMatches (per-term slot loop), not resolved as a single clause.");

            case ClauseType.Exists:
                return indexSearcher.ExistsQuery(fieldMeta);

            case ClauseType.StartsWith:
                return indexSearcher.StartWithQuery(fieldMeta, root.StringValues[packed.Param1]);

            case ClauseType.EndsWith:
                return indexSearcher.EndsWithQuery(fieldMeta, root.StringValues[packed.Param1]);

            case ClauseType.Search:
                return HandleSearch();

            case ClauseType.Regex:
                return indexSearcher.RegexQuery(fieldMeta, new Regex(root.StringValues[packed.Param1]));

            case ClauseType.Spatial:
                return HandleSpatial(clause.SpatialMethodType);

            case ClauseType.Vector:
                return HandleVector(builderParams, cur).Materialize(null);
            case ClauseType.OrGroup:
                throw new InvalidOperationException(
                    "OrGroup should be expanded by ResolveMatches, not resolved as a single clause.");

            case ClauseType.AndGroup:
                throw new InvalidOperationException(
                    "AndGroup should be expanded by ResolveMatches, not resolved as a single clause.");

            default:
                throw new InvalidOperationException($"Unexpected ClauseType {clause.ClauseType} in ResolveClause.");
        }

        IQueryMatch HandleSpatial(SpatialOperationType spatialMethod)
        {
            var index = builderParams.Index;
            var allocator = builderParams.Allocator;
        
            string fieldName = cur.Clause.FieldName 
                               ?? throw new InvalidOperationException("Spatial clause has no pre-resolved field name.");

            var fieldMetadata = QueryBuilderHelper.GetFieldMetadata(allocator, fieldName, index, builderParams.IndexFieldsMapping,
                builderParams.HasDynamics, builderParams.DynamicFields, hasBoost: builderParams.HasBoost);

            var sp = cur.Spatial;
            var distanceErrorPct = sp.DistanceErrorPct >= 0
                ? sp.DistanceErrorPct
                : RavenConstants.Documents.Indexing.Spatial.DefaultDistanceErrorPct;

            var spatialField = builderParams.Factories.GetSpatialFieldFactory(fieldName);

            IShape shape;
            SpatialUnits? units = sp.Units.HasValue ? (SpatialUnits)sp.Units.Value : null;
            if (sp.ShapeType == SpatialShapeType.Circle)
            {
                shape = spatialField.ReadCircle(sp.CircleRadius, sp.CircleLatitude, sp.CircleLongitude, units);
            }
            else if (sp.Wkt != null)
            {
                shape = spatialField.ReadShape(sp.Wkt, units);
            }
            else
            {
                throw new InvalidOperationException("Spatial clause has no pre-resolved shape parameters.");
            }

            return builderParams.IndexSearcher.SpatialQuery(fieldMetadata, distanceErrorPct, shape, spatialField.GetContext(), (global::Corax.Utils.Spatial.SpatialRelation)spatialMethod, token: builderParams.Token);
        }
        
        IQueryMatch HandleSearch()
        {
            string searchFieldName = clause.ResolvedFieldName ?? clause.FieldName;
            bool forceSearch = builderParams.HasDynamics
                               && builderParams.Index.Configuration.UseSearchAnalyzerForDynamicFieldsIfNotSetExplicitlyInSearchQuery;
            FieldMetadata searchMeta = QueryBuilderHelper.GetFieldMetadata(
                builderParams.Allocator, searchFieldName, builderParams.Index,
                builderParams.IndexFieldsMapping,
                builderParams.HasDynamics, builderParams.DynamicFields,
                handleSearch: true, hasBoost: builderParams.HasBoost,
                forceDefaultSearchAnalyzer: forceSearch);

            var searchTerm = root.StringValues[packed.Param1];
            if (builderParams.Index.CoraxSearchQueryOptions == IndexSearcher.SearchQueryOptions.PhraseQueryWithWildcardAdjustments
                && searchTerm is { Length: >= 1 }
                && (searchTerm[0] == '*' || (searchTerm.Length >= 2 && searchTerm[^1] == '*')))
            {
                searchMeta = ReplaceAnalyzerForWildcardQueries(searchMeta, walkerCtx);
            }

            return indexSearcher.SearchQuery(searchMeta,
                QueryBuilderHelper.SplitSearchValue(searchTerm),
                (Constants.Search.Operator)clause.SearchOperator,
                builderParams.Index.CoraxSearchQueryOptions);
        }
    }

    private static IQueryMatch ResolveSentinelRewrittenBetween(ClauseExecution exec, FieldMetadata fieldMeta,
        IndexSearcher indexSearcher, QueryExecution queryExec)
    {
        if (exec.SentinelRewriteType == ClauseType.Exists)
            return indexSearcher.AllEntries();
        if (exec.SentinelRewriteType == ClauseType.LessThanOrEqual)
            return exec.PackedParamValue.RangeQuery(ClauseType.LessThanOrEqual, fieldMeta, indexSearcher, queryExec);

        Debug.Assert(exec.SentinelRewriteType == ClauseType.GreaterThanOrEqual);
        IQueryMatch rangeMatch = exec.PackedParamValue.RangeQuery(ClauseType.GreaterThanOrEqual, fieldMeta, indexSearcher, queryExec);
        if (indexSearcher.TryGetPostingListForNull(in fieldMeta, out _) is false) 
            return rangeMatch;
        
        // BETWEEN low AND 'NULL' must include null-valued docs (Lucene parity)
        return new LazyOrMatch(indexSearcher.Allocator, rangeMatch, indexSearcher.TermQuery(fieldMeta, null));
    }

    private static IQueryMatch ResolveInTerm(ClauseExecution exec, int termIndex, QueryExecution queryExec, ResolutionContext walkerCtx)
    {
        FieldMetadata fieldMeta = ResolveFieldMetadata(exec.Clause, walkerCtx);
        var termPacked = exec.PackedParamValue.WithTermOffset(termIndex);
        return termPacked.TermQuery(fieldMeta, walkerCtx.IndexSearcher, queryExec);
    }

    private static FieldMetadata ResolveFieldMetadata(ClauseInfo clause, ResolutionContext walkerCtx)
    {
        var builderParams = walkerCtx.BuilderParams;
        string resolvedFieldName = clause.ResolvedFieldName ?? clause.FieldName;
        bool forceSearchAnalyzer = builderParams.HasDynamics
                                   && !clause.IsExact
                                   && clause.ClauseType != ClauseType.Search
                                   && builderParams.Index.Configuration.UseSearchAnalyzerForDynamicFieldsIfNotSetExplicitlyInSearchQuery;
        
        return QueryBuilderHelper.GetFieldMetadata(in builderParams, resolvedFieldName, exact: clause.IsExact,
            hasBoost: builderParams.HasBoost, forceDefaultSearchAnalyzer: forceSearchAnalyzer);
    }

    private static bool IsClauseBoosted(ClauseExecution exec)
        => exec.Clause.HasBoost || exec.BoostFactor > 0;

    private static void EncodeNumericValue(Span<byte> dest, int valueType, int paramIdx, QueryExecution exec)
    {
        long raw = valueType == PackedParam.TypeDouble
            ? Bits.DoubleToSortableLong(exec.DoubleValues[paramIdx])
            : exec.LongValues[paramIdx];
        BinaryPrimitives.WriteInt64BigEndian(dest, Bits.SwapBytes(raw));
    }

    private struct CompoundFieldEncoding
    {
        public PackedParam Packed;
        public Slice Analyzed;
        public int SourceSlot;
        public int Size;
    }

    private static bool TryGetCompoundFieldEncoding(ref InstCtx ctx, string fieldName, PackedParam packed, int paramSlot, out CompoundFieldEncoding encoding)
    {
        encoding = default;
        encoding.Packed = packed;
        encoding.SourceSlot = paramSlot;

        switch (packed.ValueType)
        {
            case PackedParam.TypeString:
            {
                var meta = QueryBuilderHelper.GetFieldMetadata(in ctx.BuilderParams, fieldName, hasBoost: false);
                encoding.Analyzed = ctx.Exec.GetAnalyzedSlice(ctx.PlanParams.IndexSearcher, meta, paramSlot);
                encoding.Size = encoding.Analyzed.Size;
                return encoding.Size <= byte.MaxValue;
            }
            case PackedParam.TypeLong or PackedParam.TypeDouble:
                encoding.Size = sizeof(long);
                return true;
            default:
                return false;
        }
    }
    
    private static void WriteCompoundFieldEncoding(Span<byte> dest, CompoundFieldEncoding encoding, QueryExecution exec)
    {
        if (encoding.Packed.ValueType == PackedParam.TypeString)
        {
            encoding.Analyzed.CopyTo(dest);
            return;
        }
        EncodeNumericValue(dest, encoding.Packed.ValueType, encoding.SourceSlot, exec);
    }

    /// <summary>TreeScan-eligible: multi-term clauses with a direct ITermsProvider (StartsWith,
    /// EndsWith, Exists, Regex, ranges, BETWEEN). Boosted clauses go through QueryMatch for scoring.
    /// Sentinel-rewritten BETWEEN is handled by GetDispatch, not here, because it needs the
    /// per-execution SentinelRewriteType.</summary>
    internal static bool IsTreeScanEligibleClause(ClauseInfo clause)
    {
        if (clause.HasBoost)
            return false;

        return clause.ClauseType is ClauseType.StartsWith or ClauseType.EndsWith
            or ClauseType.Exists or ClauseType.Regex
            or ClauseType.GreaterThan or ClauseType.GreaterThanOrEqual
            or ClauseType.LessThan or ClauseType.LessThanOrEqual
            or ClauseType.Between;
    }

    /// <summary>Resolve the <see cref="MatchDispatch"/> mode for a clause execution at plan-build time.
    /// Equals / NotEquals (unboosted) → <c>PostingList</c>. Multi-term (unboosted) → <c>TreeScan</c>.
    /// All other clause types → <c>QueryMatch</c>. A sentinel-rewritten BETWEEN ("*"/"NULL" bounds)
    /// always takes the QueryMatch path: ResolveSentinelRewrittenBetween reads SentinelRewriteType at
    /// resolve time and may fold in the null posting list, so it cannot be expressed as a plain TreeScan.</summary>
    private static MatchDispatch GetDispatch(ClauseExecution exec)
    {
        var clause = exec.Clause;
        if (clause is { HasBoost: false, ClauseType: ClauseType.Equals or ClauseType.NotEquals })
            return MatchDispatch.PostingList;

        if (exec.SentinelRewriteType != null)
            return MatchDispatch.QueryMatch;

        if (IsTreeScanEligibleClause(clause))
            return MatchDispatch.TreeScan;

        return MatchDispatch.QueryMatch;
    }

    private static string FormatValueFromPlan(PackedParam packed, QueryExecution exec, int idx)
    {
        if (idx is PackedParam.NoParamValue)
            return null;
        // An IN clause with all-null terms records InTermCount=0 and writes no values
        // to the typed arrays, but the packed Param1 still points at the (empty) slot.
        // Bounds-check before indexing — return null to indicate "no displayable value".
        return packed.ValueType switch
        {
            PackedParam.TypeLong => idx < exec.LongValues.Length ? exec.LongValues[idx].ToString() : null,
            PackedParam.TypeDouble => idx < exec.DoubleValues.Length ? exec.DoubleValues[idx].ToString(CultureInfo.InvariantCulture) : null,
            _ => idx < exec.StringValues.Length ? exec.StringValues[idx] : null
        };
    }
}
