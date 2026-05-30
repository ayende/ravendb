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
using Raven.Client.Exceptions;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Server;
using Voron;
using Voron.Data.RoaringBitmaps;
using Voron.Impl;
using Constants = Corax.Constants;
using RavenConstants = Raven.Client.Constants;
using IndexSearcher = Corax.Querying.IndexSearcher;
using Range = Corax.Querying.Matches.Meta.Range;

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

        /// <summary>True only for `include timings()` / explain queries (queryTimings != null), the
        /// only case where a match's Inspect() is read. Free-form DirectScan inspection strings are
        /// built solely under this flag, so the common query path skips their per-execution allocation.</summary>
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
        var queryText = planParams.Metadata.Query.QueryText;
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
        if (indexSearcher.PlanCache.Get(planParams.Metadata.Query.QueryText, operandOrdering, typeSignature, fullKinds, whenFlags) is { } compiledPlan)
            return FinalizePlan(); // use cached plan
        
        return BuildOnCacheMiss(); // Cache miss — full exec emission

        (CompiledPlan, QueryExecution) BuildOnCacheMiss()
        {
            var (ops, requiredBitmaps) = PlanEmitter.Emit(template, executions, planParams);

            var scanPredicates = CreateScanPredicates();
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
                PerClauseScanPredicates = BuildPerClauseScanPredicates(),
                AllNegated = CheckAllNegated(),
            };
            RemapOptimizationIndices();
            indexSearcher.PlanCache.Add(planParams.Metadata.Query.QueryText, compiledPlan, template);

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
                PropagateBetweenContradiction(it, writer); // a contradictory BETWEEN rewrites into the empty-IN flavor
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
        // it is cheaper to evaluate 100 entries to find if Status = 'Public' directly, this runs _once_ per cached compiled plan
        List<ScanPredicateInfo> CreateScanPredicates()
        {
            // Scan predicates only apply to multi-clause AND chains (clause 0 is the seed, 1..N are evaluated per-entry).
            if (template.IsOr || executions.Count <= 1)
                return null;

            var allNegated = CheckAllNegated();
            List<ScanPredicateInfo> predicates = [];
            int scanStart = allNegated ? 0 : 1; // Skip clause 0 (the seed) unless all clauses are negated (then we start from AllEntries, so every clause is a scan predicate).
            int sliceIndex = 0;
            for (int i = scanStart; i < executions.Count; i++)
            {
                if (BuildScanPredicateInfo(executions[i], ref sliceIndex) is { } pred)
                    predicates.Add(pred);
            }

            return predicates;
        }

        // One ScanPredicateInfo per execution (in post-sort order), built once per compiled plan.
        // The DirectScan and CompoundField construct paths filter this by their exclusion set every
        // query rather than re-running the BuildScanPredicateInfo switch. Slice ParamIndex here is
        // per-clause local (unused by those residual paths — see CompiledPlan.PerClauseScanPredicates).
        ScanPredicateInfo?[] BuildPerClauseScanPredicates()
        {
            var perClause = new ScanPredicateInfo?[executions.Count];
            for (int i = 0; i < executions.Count; i++)
            {
                int sliceIndex = 0;
                perClause[i] = BuildScanPredicateInfo(executions[i], ref sliceIndex);
            }

            return perClause;
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
            var full = template.ParameterSlots.Length > 16 ? new byte[template.ParameterSlots.Length] : null;
            for (int i = 0; i < template.ParameterSlots.Length; i++)
            {
                int kind = (int)ClassifyParamType(planParams.QueryParameters, template.ParameterSlots[i]) & 0x3;
                full?[i] = (byte)kind;
                if (i > 16) continue;
                types |= kind << (i * 2);
            }

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
                    ThrowInvalidMethodArgument(exec.Clause); // reject null (matches Lucene behavior).

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

    private static (object Value, ParamValueType Type) ResolveBindingRaw(ParameterBinding binding, BlittableJsonReaderObject queryParameters)
    {
        if (binding.LiteralType != ParamValueType.Parameter)
            return (binding.LiteralValue, binding.LiteralType);
        if (queryParameters.TryGet(binding.ParameterName, out object raw) && raw != null)
            return (raw, ParamValueType.Parameter); // raw from blittable — caller decides how to interpret
        return (null, ParamValueType.Null);
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

    private static void ThrowInvalidMethodArgument(ClauseInfo clause)
    {
        string methodName = clause.ClauseType switch
        {
            ClauseType.StartsWith => "startsWith",
            ClauseType.EndsWith => "endsWith",
            ClauseType.Search => "search",
            ClauseType.Regex => "regex",
            _ => clause.ClauseType.ToString()
        };
        throw new InvalidQueryException(
            $"Method {methodName}() expects to get an argument of type String while it got Null");
    }

    /// <summary>
    /// Foo BETWEEN $x AND $y - where $y > $x - returns nothing, this re-writes the clause to directly reflect this  
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
                innerMatch = ConstructCompoundField(ref ctx, ctx.Exec.Plan.CompoundFieldField2RangeIdx, cfEntriesToScan, cfBitmapCost);
                if (innerMatch is null) goto default;
                return OrderBy(builderParameters, innerMatch, orderByFields, hasEmptySorts);
            case ExecutionStrategy.DirectScan when orderByFields != null:
                // orderByFields can be null on a per-execution PageSize==0 reuse of a cached
                // DirectScan plan — PageSize is not part of the plan cache key.
                // Like CompoundField, DirectScan is a structural candidate; the cost gate runs fresh
                // per-execution here (RavenDB #4852). Full scans have no cost gate and always pass.
                var execs = exec.Executions;
                bool isFullScan = execs is not { Count: > 0 };
                if (DirectScanCostEffective(ref ctx, isFullScan, out long dsEntriesToScan, out long dsBitmapCost))
                {
                    bool hasTieBreak = orderByFields.Length == 2;
                    string dsReason = ctx.WantTimings
                        ? $"entries_to_scan({dsEntriesToScan}) × {QueryPrimitives.EntryScanCostMultiplier} < bitmap_cost({dsBitmapCost})"
                        : null;
                    innerMatch = ConstructDirectScan(ref ctx, exec.Plan.SortDrivingClauseIndex, isFullScan, hasTieBreak,
                        reasonForInspection: dsReason);
                    if (innerMatch is not null) return innerMatch;
                }

                goto default;
            case ExecutionStrategy.BitmapSort:
            default: // may either be the selected strategy or a one-off (because of bad parameters preventing a faster strategy)
                innerMatch = InstantiateBitmapPipeline(ctx.Plan, ctx.Exec, ctx.PlanParams, ctx.BuilderParams, walkerCtx, highlightingTerms, wantTimings, token);
                if (ctx.OrderByFields == null) return innerMatch;
                if (innerMatch is CompiledQueryMatch seekMatch)
                    TrySetSortSeekHint(seekMatch, ctx.Plan, ctx.Exec);
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

        ResolveAllSlots(exec, walkerCtx, planParams.HasBoost, out var resolvedMatches, out var leaves);

        if (highlightingTerms != null)
            PopulateHighlightingTerms(exec, highlightingTerms, planParams.Metadata);

        var compiledMatch = new CompiledQueryMatch(
            compiledPlan, exec, compiledPlan.RequiredBitmaps, compiledPlan.OpCount, resolvedMatches, leaves,
            indexSearcher, planParams.Allocator, wantTimings, token)
        {
            InRangeCounts = exec.InRangeCounts,
            Cardinalities = exec.Cardinalities,
        };

        // Attach the deferred scan-param populate on QueryExecution only when the plan actually has
        // scan-eligible predicates. Most queries don't, so the closure allocation is skipped on the
        // common path. LongValues/DoubleValues are already populated on exec; only ResidualSlices
        // and FieldRootPages need lazy materialization (they trigger analyzer + field-root lookups).
        if (exec.Plan.ScanPredicateInfos is { Count: > 0 })
        {
            exec.PopulateScanParams = () =>
            {
                ScanParamExtractor.Extract(exec, indexSearcher, walkerCtx);
            };
        }

        IQueryMatch result = compiledMatch;

        // Spatial post-filter phase: AND each spatial match with the candidate bitmap.
        if (exec.SpatialFilters is { Length: > 0 })
        {
            var spatialFilters = new IQueryMatch[exec.SpatialFilters.Length];
            for (int sf = 0; sf < exec.SpatialFilters.Length; sf++)
            {
                spatialFilters[sf] = resolvedMatches[exec.SpatialFilters[sf].MatchIndex];
            }

            result = new PostFilterMatch(result, spatialFilters, wantTimings);
        }

        // Vector select phase: each vector wraps the bitmap so far as its filter source.
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
        IQueryMatch result = null;

        // Spatial: resolve each spatial clause directly, then chain via PostFilterMatch.
        if (exec.SpatialFilters is { Length: > 0 })
        {
            var primary = ResolveClause(exec.SpatialFilters[0].Exec, exec, walkerCtx);
            var rest = exec.SpatialFilters.Length is 1 ? Array.Empty<IQueryMatch>() : new IQueryMatch[exec.SpatialFilters.Length - 1];
            for (int i = 1; i < exec.SpatialFilters.Length; i++)
            {
                rest[i - 1] = ResolveClause(exec.SpatialFilters[i].Exec, exec, walkerCtx);
            }

            result = new PostFilterMatch(primary, rest, wantTimings);
        }

        // Vector: each vector wraps the (possibly null) filter so far.
        if (exec.VectorSelects is { Length: > 0 })
        {
            foreach (var item in ResolveVectorItems(exec, builderParameters))
            {
                result = item.Materialize(result);
            }
        }

        return result;
    }


    public static IQueryMatch BuildQueryForMoreLikeThis(QueryBuilderParameters builderParams, QueryExpression expression)
    {
        // Sub-expression entry point: run the same phases as ParseTemplate.
        // Validation is inline in the Parse methods; errors accumulate in walkerCtx.Errors.
        // The whole build is measured so query introspection can report per-MLT-base build
        // cost (parse + resolve + AND chain) instead of folding it into the outer query's
        // opaque "everything else" budget.
        long buildStart = Stopwatch.GetTimestamp();
        var walkerCtx = new ResolutionContext(builderParams);
        var indexSearcher = walkerCtx.IndexSearcher;
        walkerCtx.Clauses = [];
        ParseExpression(expression, walkerCtx);
        PlanWalker.ThrowIfErrors(walkerCtx);
        PlanWalker.RewriteClauses(walkerCtx);

        if (walkerCtx.Clauses.Count == 0)
            return indexSearcher.AllEntries();

        // Populate parameters for the sub-expression clauses
        var writer = new ValueWriter();
        var subExecs = new List<ClauseExecution>(walkerCtx.Clauses.Count);
        foreach (var it in walkerCtx.Clauses)
        {
            var item = CreateExecution(it);
            subExecs.Add(item);
            PopulateClauseValues(item, builderParams.QueryParameters, writer, builderParams);
        }

        var subPlan = new QueryExecution
        {
            Executions = subExecs
        };
        writer.SetValues(subPlan);

        int clauseCount = walkerCtx.Clauses.Count;
        var clauseTicks = new long[clauseCount];
        var clauseCardinality = new long[clauseCount];
        var childInspections = new QueryInspectionNode[clauseCount];

        if (clauseCount == 1)
        {
            long t0 = Stopwatch.GetTimestamp();
            var single = ResolveClause(subExecs[0], subPlan, walkerCtx);
            clauseTicks[0] = Stopwatch.GetTimestamp() - t0;
            clauseCardinality[0] = single.Count;
            childInspections[0] = single.Inspect();
            long buildTicks = Stopwatch.GetTimestamp() - buildStart;
            return new MoreLikeThisBaseMatch(single, buildTicks, clauseTicks, clauseCardinality,
                bitmapAfterAnd: null, capturedChildInspections: childInspections);
        }

        // Multiple clauses (AND chain) — resolve each and AND them via bitmap.
        // Capture each sub-match's Count + Inspect() before it's consumed by Or/AndWith
        // (the matches go out of scope immediately after consumption with no further hook
        // for introspection — see #4856).
        var bitmap = new BitmapMatch(indexSearcher.Allocator);
        var bitmapAfterStep = new long[clauseCount];

        {
            long t0 = Stopwatch.GetTimestamp();
            var first = ResolveClause(subExecs[0], subPlan, walkerCtx);
            clauseTicks[0] = Stopwatch.GetTimestamp() - t0;
            clauseCardinality[0] = first.Count;
            childInspections[0] = first.Inspect();
            QueryPrimitives.OrWithMatch(first, ref bitmap.BitmapState);
            bitmapAfterStep[0] = bitmap.BitmapState.Count;
        }

        var temp = new RoaringBitmap(indexSearcher.Allocator);
        try
        {
            for (int i = 1; i < clauseCount; i++)
            {
                long t0 = Stopwatch.GetTimestamp();
                var sub = ResolveClause(subExecs[i], subPlan, walkerCtx);
                clauseTicks[i] = Stopwatch.GetTimestamp() - t0;
                clauseCardinality[i] = sub.Count;
                childInspections[i] = sub.Inspect();
                QueryPrimitives.AndWithMatch(sub, ref bitmap.BitmapState, ref temp);
                bitmapAfterStep[i] = bitmap.BitmapState.Count;
            }
        }
        finally
        {
            temp.Dispose();
        }

        long totalBuildTicks = Stopwatch.GetTimestamp() - buildStart;
        return new MoreLikeThisBaseMatch(bitmap, totalBuildTicks, clauseTicks, clauseCardinality,
            bitmapAfterStep, childInspections);
    }

    private static bool TryCreateCompoundExactMatch(
        ref InstCtx ctx, out string rejectReason)
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

    /// <summary>Phase 5 bake: construction-only path for the CompoundExact hint.
    /// Assumes structural discovery has already validated this optimization applies
    /// (called either right after <see cref="TryCreateCompoundExactMatch"/>'s checks pass
    /// on compile-miss, or directly on cache-hit when <c>compiledPlan.Strategy == ExecutionStrategy.CompoundExact</c>).
    /// Returns null when a per-execution byte-length check fails — the caller must fall
    /// back to the next optimization (or bitmap). No cost gates here — those are encoded
    /// in the plan-cache key (cardinality cliff bit 31 of Ordering).</summary>
    private static IQueryMatch ConstructCompoundExact(ref InstCtx ctx)
    {
        var execs = ctx.Exec.Executions;
        var indexSearcher = ctx.PlanParams.IndexSearcher;
        int idxA = ctx.Exec.Plan.CompoundExactClauseA;
        int idxB = ctx.Exec.Plan.CompoundExactClauseB;
        var eA = execs[idxA];
        var eB = execs[idxB];

        string firstField, secondField;
        ClauseExecution firstExec, secondExec;
        if (ctx.Exec.Plan.Template.CompoundExactAFirst)
        {
            firstField = eA.Clause.ResolvedFieldName ?? eA.Clause.FieldName;
            secondField = eB.Clause.ResolvedFieldName ?? eB.Clause.FieldName;
            firstExec = eA;
            secondExec = eB;
        }
        else
        {
            firstField = eB.Clause.ResolvedFieldName ?? eB.Clause.FieldName;
            secondField = eA.Clause.ResolvedFieldName ?? eA.Clause.FieldName;
            firstExec = eB;
            secondExec = eA;
        }

        if (TryGetCompoundFieldEncoding(firstField, firstExec.PackedParamValue,
                firstExec.PackedParamValue.Param1, ref ctx, out var enc1) == false
            || enc1.Size > byte.MaxValue)
            return null;

        if (TryGetCompoundFieldEncoding(secondField, secondExec.PackedParamValue,
                secondExec.PackedParamValue.Param1, ref ctx, out var enc2) == false)
            return null;

        int totalLen = enc1.Size + enc2.Size + 1;
        if (totalLen > Constants.Terms.MaxLength) return null;

        // Single allocator-backed buffer; write each field directly into its slice.
        // The trailing byte stores field1 length (used at scan time to split the composite key).
        ctx.PlanParams.Allocator.Allocate(totalLen, out ByteString keyBuf);
        var keySpan = keyBuf.ToSpan();
        WriteCompoundFieldEncoding(keySpan.Slice(0, enc1.Size), enc1, ctx.Exec);
        WriteCompoundFieldEncoding(keySpan.Slice(enc1.Size, enc2.Size), enc2, ctx.Exec);
        keySpan[totalLen - 1] = (byte)enc1.Size;

        var compoundFieldName = $"compound({firstField},{secondField})";
        var compoundFieldMeta = indexSearcher.FieldMetadataBuilder(compoundFieldName, hasBoost: false);

        return indexSearcher.TermQuery(compoundFieldMeta, new Slice(keyBuf));
    }

    /// <summary>STRUCTURAL eligibility for the CompoundField candidate: compound(field1, field2)
    /// exists in the index, the query is not all-negated, and every residual clause is non-boosted
    /// and entry-scan eligible. Baked once at cache-miss into <see cref="ExecutionStrategy.CompoundField"/>.
    /// The per-execution cost gate lives in <see cref="CompoundFieldCostEffective"/>.</summary>
    private static bool TryCreateCompoundFieldMatch(ref InstCtx ctx, out string rejectReason)
    {
        if (ctx.Exec.Plan.CompoundFieldDrivingClause < 0 || ctx.Exec.Plan.Template.CompoundFieldSortName is null)
        {
            rejectReason = "no compound-field candidate identified at template time";
            return false;
        }

        var execs = ctx.Exec.Executions;
        if (ctx.Exec.Plan.CompoundFieldDrivingClause >= execs.Count || ctx.Exec.Plan.AllNegated)
        {
            rejectReason = "all clauses are negated";
            return false;
        }

        // Optional field2 range narrowing clause — baked at template time
        // (structural; same for all executions of this template).
        int field2RangeIdx = ctx.Exec.Plan.CompoundFieldField2RangeIdx;

        // Residual scannability — structural only (clause-type / boost based, stable across
        // executions). The bitmap-vs-direct-scan COST gate and the parameter-dependent
        // PackedParamValue.IsNone guard are deliberately NOT evaluated here: both depend on the
        // bound-parameter cardinalities and run per-execution in CompoundFieldCostEffective, so a
        // cached plan never reuses a stale cost decision (RavenDB #4852).
        for (int i = 0; i < execs.Count; i++)
        {
            if (i == ctx.Exec.Plan.CompoundFieldDrivingClause || i == field2RangeIdx)
                continue;
            if (IsClauseBoosted(execs[i]))
            {
                rejectReason = "boosted clause found";
                return false;
            }

            if (IsScanEligible(execs[i]) == false)
            {
                rejectReason = "scan predicate info is null";
                return false;
            }
        }

        rejectReason = null;
        return true;
    }

    private static IQueryMatch ConstructCompoundField(ref InstCtx ctx, int field2RangeIdx, long entriesToScan, long bitmapCost)
    {
        var execs = ctx.Exec.Executions;
        var indexSearcher = ctx.PlanParams.IndexSearcher;
        var allocator = ctx.PlanParams.Allocator;
        int drivingClauseIdx = ctx.Exec.Plan.CompoundFieldDrivingClause;
        string sortFieldName = ctx.Exec.Plan.Template.CompoundFieldSortName;

        var drivingClause = execs[drivingClauseIdx].Clause;
        var drivingExec = execs[drivingClauseIdx];
        var packed = drivingExec.PackedParamValue;

        // Residual predicates: filter the per-clause predicates precomputed at plan-build time,
        // excluding the driving clause and the field2-range narrowing clause. A null entry means a
        // non-scannable clause — abort to the bitmap pipeline.
        var perClause = ctx.Exec.Plan.PerClauseScanPredicates;
        var residualPreds = new List<ScanPredicateInfo>();
        for (int i = 0; i < execs.Count; i++)
        {
            if (i == drivingClauseIdx || i == field2RangeIdx)
                continue;
            if (perClause[i] is not { } pred)
                return null;
            residualPreds.Add(pred);
        }

        string field1Name = drivingClause.FieldName;
        string compoundFieldName = ctx.Exec.Plan.Template.CompoundFieldName;
        var compoundFieldMeta = indexSearcher.FieldMetadataBuilder(compoundFieldName, hasBoost: false);

        // Build the prefix bytes for field1's value.
        // String: analyzed via field1's analyzer (cached). Numeric: 8-byte big-endian sortable encoding
        // written directly into a single allocator-backed ByteString (no managed byte[] hop).
        Slice analyzedPrefix = BuildField1Prefix(ref ctx, field1Name, packed, out string field1ValueStr);
        if (analyzedPrefix.HasValue == false)
            return null;

        // Compound key trailing byte stores field1 length as a single byte.
        // If the analyzed prefix exceeds 255 bytes, the compound key format can't represent it.
        // Fall back to the bitmap pipeline which queries individual fields normally.
        if (analyzedPrefix.Size > byte.MaxValue)
            return null;

        bool ascending = ctx.OrderByFields[0].Ascending;

        IQueryMatch drivingMatch;
        if (field2RangeIdx >= 0
            && TryBuildCompositeRangeKeys(ref ctx, analyzedPrefix, sortFieldName,
                execs[field2RangeIdx], out var lowSlice, out var highSlice))
        {
            drivingMatch = indexSearcher.RangeBuilder<Range.Inclusive, Range.Inclusive>(
                compoundFieldMeta, lowSlice, highSlice,
                forward: ascending, CancellationToken.None);
        }
        else
        {
            // No field2 narrowing (or it would overflow / use an unsupported value type):
            // run a prefix scan on field1 only and let entry-scan residuals filter the rest.
            drivingMatch = indexSearcher.StartWithQuery(compoundFieldMeta, analyzedPrefix,
                isNegated: false, forward: ascending,
                validatePostfixLen: true);
        }

        // Extract scan parameters for residual predicates and stash them on the exec — the residual
        // IL reads exec.ResidualSlices / exec.FieldRootPages directly. Longs/Doubles already live
        // on exec; the IL reads them by baked PackedParam.Param1 indices.
        ScanPredicateInfo[] residualArray = residualPreds.Count > 0 ? residualPreds.ToArray() : null;
        ScanParamExtractor. BuildResidual(ctx.Exec, indexSearcher, allocator, residualArray,
            drivingClauseIdx, field2RangeIdx);

        var directScan = BuildDirectScan(
            indexSearcher, drivingMatch, ctx.Exec,
            ctx.Plan.CompiledEntryPredicate, residualArray);

        // Free-form inspection strings are read only by Inspect() on `include timings()` / explain
        // queries (queryTimings != null). Skip building them — and the field1ValueStr formatting —
        // on the common query path.
        if (ctx.WantTimings)
        {
            directScan.DrivingTreeName = compoundFieldName;
            directScan.DrivingClause = $"{field1Name} = '{field1ValueStr}'";
            directScan.SeekBound = $"'{field1ValueStr}' (prefix, validatePostfixLen)";
            directScan.Direction = ctx.OrderByFields[0].Ascending ? "Forward" : "Backward";
            directScan.ResidualDescription = residualArray != null
                ? string.Join(", ", residualPreds.ConvertAll(p => $"{p.FieldName} {p.CompareOp}"))
                : null;
            directScan.Reason = $"entries_to_scan({entriesToScan}) × {QueryPrimitives.EntryScanCostMultiplier} < bitmap_cost({bitmapCost})";
        }

        return directScan;
    }

    /// <summary>Build the field1 prefix <see cref="Slice"/> for the CompoundField driving match.
    /// String case returns the cached analyzed slice directly (no allocation). Numeric cases
    /// allocate a single 8-byte allocator-backed buffer and write the big-endian sortable
    /// encoding straight in — no managed <c>byte[]</c> intermediate. Unsupported value types
    /// return <c>default(Slice)</c>; caller treats that as a failure.</summary>
    private static Slice BuildField1Prefix(ref InstCtx ctx, string field1Name, PackedParam packed,
        out string field1ValueStr)
    {
        var indexSearcher = ctx.PlanParams.IndexSearcher;
        switch (packed.ValueType)
        {
            case PackedParam.TypeString:
            {
                field1ValueStr = ctx.Exec.StringValues[packed.Param1];
                var field1Meta = QueryBuilderHelper.GetFieldMetadata(in ctx.BuilderParams, field1Name, hasBoost: false);
                return ctx.Exec.GetAnalyzedSlice(indexSearcher, field1Meta, packed.Param1);
            }
            case PackedParam.TypeLong:
            {
                // field1ValueStr feeds only the inspection strings (see ConstructCompoundField); skip
                // the ToString allocation unless this is an inspected query.
                field1ValueStr = ctx.WantTimings ? ctx.Exec.LongValues[packed.Param1].ToString() : null;
                ctx.PlanParams.Allocator.Allocate(sizeof(long), out ByteString buf);
                EncodeNumericValue(buf.ToSpan(), PackedParam.TypeLong, packed.Param1, ctx.Exec);
                return new Slice(buf);
            }
            case PackedParam.TypeDouble:
            {
                field1ValueStr = ctx.WantTimings ? ctx.Exec.DoubleValues[packed.Param1].ToString(CultureInfo.InvariantCulture) : null;
                ctx.PlanParams.Allocator.Allocate(sizeof(long), out ByteString buf);
                EncodeNumericValue(buf.ToSpan(), PackedParam.TypeDouble, packed.Param1, ctx.Exec);
                return new Slice(buf);
            }
            default:
                field1ValueStr = null;
                return default;
        }
    }

    /// <summary>Build the composite low/high <see cref="Slice"/> keys for a CompoundField range scan.
    /// Returns false (and the caller falls back to the prefix-only StartsWith path) when:
    ///   - the field2 packed param is None,
    ///   - field2 is a String value that exceeds 255 bytes (low or high bound),
    ///   - field2 is an unsupported value type,
    ///   - the resulting composite key (prefix + suffix + 1 length byte) exceeds
    ///     <see cref="Constants.Terms.MaxLength"/>.
    /// Each successful return allocates exactly two allocator-backed buffers (one per slice);
    /// the field2 bytes are written directly into them with no managed <c>byte[]</c> hop.
    /// </summary>
    private static bool TryBuildCompositeRangeKeys(
        ref InstCtx ctx,
        Slice analyzedPrefix,
        string sortFieldName,
        ClauseExecution field2Exec,
        out Slice lowSlice,
        out Slice highSlice)
    {
        lowSlice = default;
        highSlice = default;

        var field2Packed = field2Exec.PackedParamValue;
        if (field2Packed.IsNone)
            return false;

        var field2ClauseType = field2Exec.Clause.ClauseType;
        bool isBetween = field2ClauseType == ClauseType.Between;
        bool isGt = field2ClauseType is ClauseType.GreaterThan or ClauseType.GreaterThanOrEqual;
        bool isLt = field2ClauseType is ClauseType.LessThan or ClauseType.LessThanOrEqual;

        // Resolve low (and high for Between) encodings. String slots reject early on >255 bytes.
        if (TryGetCompoundFieldEncoding(sortFieldName, field2Packed, field2Packed.Param1, ref ctx, out var encLow) == false)
            return false;
        if (field2Packed.ValueType == PackedParam.TypeString && encLow.Size > byte.MaxValue)
            return false;

        CompoundFieldEncoding encHigh = default;
        if (isBetween)
        {
            if (TryGetCompoundFieldEncoding(sortFieldName, field2Packed, field2Packed.Param2, ref ctx, out encHigh) == false)
                return false;
            if (field2Packed.ValueType == PackedParam.TypeString && encHigh.Size > byte.MaxValue)
                return false;
        }

        int prefixSize = analyzedPrefix.Size;
        int lowSuffixSize = encLow.Size;
        int highSuffixSize = isBetween ? encHigh.Size : encLow.Size;
        int lowLen = prefixSize + lowSuffixSize + 1;
        int highLen = prefixSize + highSuffixSize + 1;

        if (lowLen > Constants.Terms.MaxLength || highLen > Constants.Terms.MaxLength)
            return false;

        var allocator = ctx.PlanParams.Allocator;
        var prefixSpan = analyzedPrefix.AsReadOnlySpan();

        // Low key. GT/GTE → low suffix = field2 bound; LT/LTE → low suffix = 0x00; Between → low = low bound.
        allocator.Allocate(lowLen, out ByteString lowBuf);
        var lowSpan = lowBuf.ToSpan();
        prefixSpan.CopyTo(lowSpan);
        if (isGt || isBetween)
            WriteCompoundFieldEncoding(lowSpan.Slice(prefixSize, lowSuffixSize), encLow, ctx.Exec);
        else
            lowSpan.Slice(prefixSize, lowSuffixSize).Clear();
        lowSpan[lowLen - 1] = (byte)prefixSize;

        // High key. LT/LTE → high suffix = field2 bound; GT/GTE → high suffix = 0xFF; Between → high = high bound.
        allocator.Allocate(highLen, out ByteString highBuf);
        var highSpan = highBuf.ToSpan();
        prefixSpan.CopyTo(highSpan);
        if (isLt)
            WriteCompoundFieldEncoding(highSpan.Slice(prefixSize, highSuffixSize), encLow, ctx.Exec);
        else if (isBetween)
            WriteCompoundFieldEncoding(highSpan.Slice(prefixSize, highSuffixSize), encHigh, ctx.Exec);
        else
            highSpan.Slice(prefixSize, highSuffixSize).Fill(0xFF);
        highSpan[highLen - 1] = (byte)prefixSize;

        lowSlice = new Slice(lowBuf);
        highSlice = new Slice(highBuf);
        return true;
    }

    /// <summary>Structural eligibility for serving an ORDER BY via a direct tree scan
    /// instead of the bitmap pipeline (the range/equals query already walks the tree in
    /// sort order, so no SortingMatch wrapper is needed). This checks only template-stable
    /// shape: ORDER BY arity, tie-break field type, sort-driving clause presence, full-scan
    /// eligibility, and residual scannability. The cost gate (cardinality-vs-bitmap, which
    /// is parameter-dependent) is deliberately NOT here — it runs fresh every execution in
    /// <see cref="DirectScanCostEffective"/> so a cached <see cref="ExecutionStrategy.DirectScan"/>
    /// candidate cannot go stale across bound-parameter values (RavenDB #4852).</summary>
    private static bool TryCreateSimpleFieldDirectScan(ref InstCtx ctx, out string rejectReason)
    {
        if (ctx.OrderByFields == null || ctx.OrderByFields.Length == 0)
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
                rejectReason = "tie-break field type is not numeric (must be Integer, Floating, or Sequence)";
                return false;
            }
        }

        var sortFieldType = ctx.OrderByFields[0].FieldType;
        var execs = ctx.Exec.Executions;
        bool isFullScan = execs == null || execs.Count == 0;

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
            if (sortFieldType is not (MatchCompareFieldType.Sequence or MatchCompareFieldType.Integer or MatchCompareFieldType.Floating))
            {
                rejectReason = "full-scan sort field type is not Sequence/Integer/Floating";
                return false;
            }
            rejectReason = null;
            return true;
        }

        // SortDrivingClauseIndex was pre-identified at template time (excluding clauses
        // with WHEN conditions — see ComputeTemplateOptimizations) and remapped to its
        // post-sort index during Build. A value of -1 here means either no candidate
        // exists, or WHEN eliminated the candidate at runtime; both fall back to bitmap.
        int drivingIdx = ctx.Exec.Plan.SortDrivingClauseIndex;
        if (drivingIdx < 0)
        {
            rejectReason = "no range/equals clause on sort field (or WHEN eliminated the candidate)";
            return false;
        }

        // Boost is ruled out at template time (see ComputeOptFlags); here we only
        // confirm every non-driving residual is scannable.
        for (int i = 0; i < execs.Count; i++)
        {
            if (i == drivingIdx) continue;
            if (IsScanEligible(execs[i]) == false)
            {
                rejectReason = "non-scannable residual clause";
                return false;
            }
        }

        rejectReason = null;
        return true;
    }

    /// <summary>Phase 5 bake: construction-only path for the DirectScan hint.
    /// Discovery (clause selection, cost gate, residual scannability) already passed
    /// in either TryCreateSimpleFieldDirectScan or by virtue of a cached
    /// <see cref="ExecutionStrategy.DirectScan"/>. Returns null when a per-execution
    /// runtime check fails (e.g. driving match resolution returns non-TermsProviderMatch
    /// because the field has no terms in this index).</summary>
    /// <param name="reasonForInspection">Free-form decision-trail string surfaced through
    /// <c>DirectScanMatchBase.Reason</c>; isFullScan overrides it with a fixed string.</param>
    private static IQueryMatch ConstructDirectScan(
        ref InstCtx ctx,
        int drivingIdx, bool isFullScan, bool hasTieBreak,
        string reasonForInspection,
        List<ScanPredicateInfo> preBuiltResiduals = null)
    {
        var indexSearcher = ctx.PlanParams.IndexSearcher;
        // sortFieldName feeds only the inspection node; skip the ToString on the common path.
        string sortFieldName = ctx.WantTimings ? ctx.OrderByFields[0].Field.FieldName.ToString() : null;
        bool forward = ctx.OrderByFields[0].Ascending;

        // ── 1. Driving provider (term iterator wrapped as TermsProviderMatch) ──
        if (ResolveDrivingProvider(ref ctx, drivingIdx, isFullScan, forward,
                out ITermsProvider provider, out LowLevelTransaction llt, out string drivingClauseDescription) == false)
            return null;

        // ── 2. Residual predicates (rebuilt on cache-hit path; passed through on the discovery path) ──
        if (TryGetResidualPredicates(ref ctx, drivingIdx, isFullScan, preBuiltResiduals, out List<ScanPredicateInfo> residualPreds) == false)
            return null;

        // ── 3. Driving match: SortedDrivingMatch or its tie-break variant ──
        // BetweenQuery and StartWithQuery don't include nulls in their term output,
        // so SortedDrivingMatch must drain them itself (respecting nullFirst direction).
        IQueryMatch drivingMatch = BuildSortedDrivingMatch(ref ctx, provider, llt, hasTieBreak, forward);

        // ── 4. Residual scan parameters ── stash on exec (residual IL reads them from there).
        ScanPredicateInfo[] residualArray = residualPreds is { Count: > 0 } ? residualPreds.ToArray() : null;
        ScanParamExtractor.BuildResidual(ctx.Exec, indexSearcher, ctx.PlanParams.Allocator, residualArray,
            drivingIdx, -1);
 
        var ds = BuildDirectScan(
            indexSearcher, drivingMatch, ctx.Exec,
            ctx.Plan.CompiledEntryPredicate, residualArray);

        // Inspection node is read only on `include timings()` / explain queries.
        if (ctx.WantTimings)
            PopulateDirectScanInspection(ds, sortFieldName, drivingClauseDescription, forward, residualArray, residualPreds,
                isFullScan ? "full index-only scan (no WHERE clause)" : reasonForInspection);
        return ds;
    }

    /// <summary>Resolve the term iterator that walks the sort field in sort order, for full-scan or
    /// from the driving range/equals clause. Returns false when the underlying factory yielded a
    /// non-<see cref="TermsProviderMatch"/> (typically because the field has no terms in this index).</summary>
    private static bool ResolveDrivingProvider(
        ref InstCtx ctx, int drivingIdx, bool isFullScan, bool forward,
        out ITermsProvider provider, out LowLevelTransaction llt, out string drivingClauseDescription)
    {
        var indexSearcher = ctx.PlanParams.IndexSearcher;

        if (isFullScan)
        {
            var fieldMeta = ctx.OrderByFields[0].Field;
            var sortFieldType = ctx.OrderByFields[0].FieldType;
            IQueryMatch fullScanMatch = sortFieldType switch
            {
                MatchCompareFieldType.Integer => indexSearcher.BetweenQuery(fieldMeta, long.MinValue, long.MaxValue, forward: forward),
                MatchCompareFieldType.Floating => indexSearcher.BetweenQuery(fieldMeta, double.MinValue, double.MaxValue, forward: forward),
                _ => indexSearcher.ExistsQuery(fieldMeta, forward: forward)
            };
            if (fullScanMatch is not TermsProviderMatch tpm)
            {
                provider = null; llt = null; drivingClauseDescription = null;
                return false;
            }
            provider = tpm.Provider;
            llt = tpm.Llt;
            drivingClauseDescription = ctx.WantTimings ? $"{fieldMeta.FieldName} [all]" : null;
            return true;
        }

        var walkerCtx = new ResolutionContext(ctx.BuilderParams);
        var drivingExec = ctx.Exec.Executions[drivingIdx];
        IQueryMatch drivingMatch = drivingExec.ClauseType == ClauseType.Equals
            ? ResolveEqualsClauseWithDirection(drivingExec, ctx.Exec, forward, walkerCtx)
            : ResolveRangeClauseWithDirection(drivingExec, ctx.Exec, forward, walkerCtx);

        if (drivingMatch is not TermsProviderMatch resolved)
        {
            provider = null; llt = null; drivingClauseDescription = null;
            return false;
        }
        provider = resolved.Provider;
        llt = resolved.Llt;
        drivingClauseDescription = ctx.WantTimings ? $"{drivingExec.Clause.FieldName} {drivingExec.ClauseType}" : null;
        return true;
    }

    /// <summary>Residual predicates list. On the discovery path (<see cref="TryCreateSimpleFieldDirectScan"/>)
    /// the list is built during cost analysis and threaded through; on the cache-hit dispatch path
    /// (<see cref="BuildSortedQuery"/>) the list is rebuilt here.
    /// Returns false when a residual predicate is non-scannable — a structural property that didn't
    /// change since the decision was cached, so the assertion of consistency here is genuine.</summary>
    private static bool TryGetResidualPredicates(
        ref InstCtx ctx, int drivingIdx, bool isFullScan,
        List<ScanPredicateInfo> preBuiltResiduals, out List<ScanPredicateInfo> residualPreds)
    {
        residualPreds = preBuiltResiduals;
        if (residualPreds != null || isFullScan)
            return true;

        var execs = ctx.Exec.Executions;
        var perClause = ctx.Exec.Plan.PerClauseScanPredicates;
        for (int i = 0; i < execs.Count; i++)
        {
            if (i == drivingIdx) continue;
            if (perClause[i] is not { } pred)
                return false;
            residualPreds ??= new List<ScanPredicateInfo>();
            residualPreds.Add(pred);
        }
        return true;
    }

    /// <summary>Resolve "nulls first vs last" for a single ORDER BY field given its NullsSortMode
    /// (per-field override or index default) and the scan direction.</summary>
    private static bool ResolveNullFirst(in OrderMetadata orderByField, NullsSortMode indexDefault, bool forward)
    {
        bool nullIsSmallest = (orderByField.NullsSortMode ?? indexDefault) == NullsSortMode.NullsSmallest;
        return forward ? nullIsSmallest : !nullIsSmallest;
    }

    /// <summary>Build the SortedDrivingMatch (or its tie-break variant) that walks the term tree
    /// in sort order, draining the per-field null list itself.</summary>
    private static IQueryMatch BuildSortedDrivingMatch(
        ref InstCtx ctx, ITermsProvider provider, LowLevelTransaction llt, bool hasTieBreak, bool forward)
    {
        var indexSearcher = ctx.PlanParams.IndexSearcher;
        var indexDefaultNullsSortMode = ctx.BuilderParams.Index.Configuration.NullsSortMode;
        bool nullFirst = ResolveNullFirst(ctx.OrderByFields[0], indexDefaultNullsSortMode, forward);

        if (hasTieBreak)
        {
            // Secondary field uses its own NullsSortMode — distinct from the primary field's.
            // ResolveNullFirst's "forward" arg is the primary scan direction; the secondary's nullIsSmallest
            // is passed directly through (the tie-break match interprets it relative to its own descending flag).
            bool secondaryNullIsSmallest = (ctx.OrderByFields[1].NullsSortMode ?? indexDefaultNullsSortMode) == NullsSortMode.NullsSmallest;
            int take = ctx.BuilderParams?.Take ?? Constants.IndexSearcher.TakeAll;
            return new SortedDrivingWithTieBreakMatch(
                provider, llt, ctx.PlanParams.Allocator, indexSearcher,
                ctx.OrderByFields[0].Field, ctx.OrderByFields[1].Field,
                ctx.OrderByFields[1].FieldType, secondaryDescending: !ctx.OrderByFields[1].Ascending,
                nullFirst: nullFirst, nullIsSmallest: secondaryNullIsSmallest,
                take: take);
        }

        return new SortedDrivingMatch(provider, llt, ctx.PlanParams.Allocator,
            indexSearcher, ctx.OrderByFields[0].Field, nullFirst);
    }

    /// <summary>Populate the free-form inspection strings on a constructed DirectScan match. The
    /// match's Fill/AndWith path never reads these — they exist solely for query introspection.</summary>
    private static void PopulateDirectScanInspection(
        DirectScanMatchBase ds, string sortFieldName, string drivingClauseDescription, bool forward,
        ScanPredicateInfo[] residualArray, List<ScanPredicateInfo> residualPreds, string reason)
    {
        ds.DrivingTreeName = sortFieldName;
        ds.DrivingClause = drivingClauseDescription;
        ds.Direction = forward ? "Forward" : "Backward";
        ds.ResidualDescription = residualArray != null
            ? string.Join(", ", residualPreds.ConvertAll(p => $"{p.FieldName} {p.CompareOp}"))
            : null;
        ds.Reason = reason;
    }

    /// <summary>Create the appropriate DirectScan match based on whether residual predicates exist.</summary>
    private static DirectScanMatchBase BuildDirectScan(
        IndexSearcher searcher, IQueryMatch drivingMatch,
        QueryExecution exec,
        ResidualScanIlEmitter.ResidualScanPredicate residualDelegate,
        ScanPredicateInfo[] residualArray)
    {
        if (residualArray == null)
            return new DirectScanSimpleMatch(searcher, drivingMatch, take: -1);

        return new DirectScanFilteredMatch(
            searcher, drivingMatch, exec,
            take: -1, precompiledDelegate: residualDelegate);
    }

    /// <summary>Single leaf walk that produces the two parallel slot arrays at once:
    /// <paramref name="matches"/> (eagerly-resolved IQueryMatch slots — spatial/vector/search/
    /// boosted) and <paramref name="leaves"/> (value-independent resolve metadata for
    /// PostingSource / TreeScan slots, materialized lazily inside the IL pipeline). Each leaf
    /// derives its effective dispatch inline (see <see cref="ResolveLeafIntoAll"/>). Slot layout
    /// (per-leaf, IN/AllIn → InTermCount+1, groups expanded) is identical to the IL emitter's
    /// leaf walk and <see cref="CardinalityArrayBuilder.Build"/>, keeping IL slot indices
    /// end-to-end consistent. The arrays stay length-equal because every add appends to both.</summary>
    private static void ResolveAllSlots(QueryExecution exec, ResolutionContext walkerCtx, bool planHasBoost,
        out IQueryMatch[] matches, out LeafResolveInfo[] leaves)
    {
        Debug.Assert(!(exec.IsAllEntries && exec.HasSpatialOrVector),
            "ResolveAllSlots reached with IsAllEntries && HasSpatialOrVector — InstantiateAllEntriesPostFilter bypass should have handled this.");

        // IsAllEntries plans never emit leaf ops — match[0] is AllEntries (served from
        // ResolvedMatches via FillFromMatch), post-filter slots are spatial/vector. The
        // parallel Leaves slot is PreResolved and never consumed.
        if (exec.IsAllEntries)
        {
            matches = [walkerCtx.IndexSearcher.AllEntries()];
            leaves = [new LeafResolveInfo { Kind = LeafResolveKind.PreResolved }];
            return;
        }

        if (exec.Executions is not { Count: > 0 })
        {
            matches = [];
            leaves = [];
            return;
        }

        var matchList = new List<IQueryMatch>();
        var leafList = new List<LeafResolveInfo>();
        foreach (var clauseExec in exec.Executions)
        {
            ResolveLeafIntoAll(walkerCtx, clauseExec, exec, planHasBoost, matchList, leafList);
        }

        matches = matchList.ToArray();
        leaves = leafList.ToArray();
    }

    /// <summary>Recursive leaf walker for <see cref="ResolveAllSlots"/>. Groups expand to
    /// their leaves. The per-leaf dispatch is derived inline (mirroring the emitter): a boosted
    /// plan forces every leaf through <see cref="MatchDispatch.QueryMatch"/> (so the matching
    /// <c>*FromMatch</c> ops read <see cref="CompiledQueryMatch.ResolvedMatches"/>); IN/AllIn are
    /// always <see cref="MatchDispatch.PostingList"/>; every other clause uses
    /// <see cref="GetDispatch"/>. Match-dispatch slots are resolved eagerly into
    /// <paramref name="matches"/>; PostingList / TreeScan slots store value-independent metadata
    /// in <paramref name="leaves"/> for lazy resolution inside the IL pipeline. Boost on a negated
    /// leaf is silently ignored — matches Lucene, where boosting a negation has no effect.</summary>
    private static void ResolveLeafIntoAll(ResolutionContext walkerCtx,
        ClauseExecution clauseExec, QueryExecution root, bool planHasBoost,
        List<IQueryMatch> matches, List<LeafResolveInfo> leaves)
    {
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
                    AddInTermSlot(dispatch, clauseExec, i, root, walkerCtx, matches, leaves);
                }

                // Null-term slot is always allocated; dispatch decides how it resolves.
                AddNullTermSlot(dispatch, clauseExec, walkerCtx, matches, leaves);
                break;
            }
            default:
            {
                MatchDispatch dispatch = planHasBoost ? MatchDispatch.QueryMatch : GetDispatch(clauseExec.Clause);
                AddDefaultSlot(dispatch, clauseExec, root, walkerCtx, matches, leaves);
                break;
            }
        }
    }

    /// <summary>Append one IN/AllIn term slot. Match dispatch resolves the term query eagerly;
    /// PostingList dispatch stores the per-term packed parameter + field metadata for lazy
    /// resolution. The slot count stays in lockstep with the IL emitter's leaf walk.</summary>
    private static void AddInTermSlot(MatchDispatch dispatch, ClauseExecution clauseExec, int termIndex,
        QueryExecution root, ResolutionContext walkerCtx,
        List<IQueryMatch> matches, List<LeafResolveInfo> leaves)
    {
        if (dispatch == MatchDispatch.QueryMatch)
        {
            matches.Add(ResolveInTerm(clauseExec, termIndex, root, walkerCtx));
            leaves.Add(new LeafResolveInfo { Kind = LeafResolveKind.PreResolved });
            return;
        }

        // PostingList — IN/AllIn have no tree-scan form.
        matches.Add(null);
        leaves.Add(new LeafResolveInfo
        {
            Kind = LeafResolveKind.TermPosting,
            ClauseType = clauseExec.ClauseType,
            Packed = clauseExec.PackedParamValue.WithTermOffset(termIndex),
            FieldMeta = ResolveFieldMetadata(clauseExec.Clause, walkerCtx)
        });
    }

    /// <summary>Append the always-allocated null-term slot. Match dispatch emits a real match
    /// (TermQuery(null) or CreateEmpty) so the OR/AND step is a no-op when there is no null term;
    /// PostingList dispatch stores a null/all/empty resolve descriptor for lazy resolution.</summary>
    private static void AddNullTermSlot(MatchDispatch dispatch, ClauseExecution clauseExec,
        ResolutionContext walkerCtx,
        List<IQueryMatch> matches, List<LeafResolveInfo> leaves)
    {
        if (dispatch == MatchDispatch.QueryMatch)
        {
            var indexSearcher = walkerCtx.IndexSearcher;
            FieldMetadata nullMeta = ResolveFieldMetadata(clauseExec.Clause, walkerCtx);
            matches.Add(clauseExec.HasNullTerm
                ? indexSearcher.TermQuery(nullMeta, null)
                : TermMatch.CreateEmpty(indexSearcher, indexSearcher.Allocator));
            leaves.Add(new LeafResolveInfo { Kind = LeafResolveKind.PreResolved });
            return;
        }

        // PostingList
        matches.Add(null);
        leaves.Add(ResolveNullTermLeaf(clauseExec, walkerCtx));
    }

    /// <summary>Build the null-term leaf descriptor. When the clause carries no null term,
    /// AllIn resolves to All (so AND-ing the null slot is a no-op) and IN resolves to Empty
    /// (so OR-ing is a no-op). Otherwise NullPosting carries the field metadata so the IL
    /// pipeline can resolve the null posting list lazily. <see cref="AccumulateInRangeCounts"/>
    /// always uses InTermCount as the AllIn range, mirroring this.</summary>
    private static LeafResolveInfo ResolveNullTermLeaf(ClauseExecution clauseExec, ResolutionContext walkerCtx)
    {
        if (!clauseExec.HasNullTerm)
        {
            return new LeafResolveInfo
            {
                Kind = clauseExec.ClauseType == ClauseType.AllIn ? LeafResolveKind.AllPosting : LeafResolveKind.EmptyPosting
            };
        }

        return new LeafResolveInfo
        {
            Kind = LeafResolveKind.NullPosting,
            ClauseType = clauseExec.ClauseType,
            FieldMeta = ResolveFieldMetadata(clauseExec.Clause, walkerCtx)
        };
    }

    /// <summary>Append a scalar (non-IN) leaf slot. Match dispatch resolves the clause eagerly
    /// (wrapping in Boost when a boost factor is set); PostingList / TreeScan dispatch store
    /// value-independent metadata (field + packed parameter) for lazy resolution inside the IL
    /// pipeline.</summary>
    private static void AddDefaultSlot(MatchDispatch dispatch, ClauseExecution clauseExec,
        QueryExecution root, ResolutionContext walkerCtx,
        List<IQueryMatch> matches, List<LeafResolveInfo> leaves)
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
            default: // TreeScan
                matches.Add(null);
                leaves.Add(new LeafResolveInfo
                {
                    Kind = LeafResolveKind.TreeScan,
                    ClauseType = clauseExec.ClauseType,
                    Packed = clauseExec.PackedParamValue,
                    FieldMeta = ResolveFieldMetadata(clauseExec.Clause, walkerCtx)
                });
                break;
        }
    }

    private static IQueryMatch ResolveClause(ClauseExecution cur, QueryExecution root, ResolutionContext walkerCtx)
    {
        var clause = cur.Clause;
        var indexSearcher = walkerCtx.IndexSearcher;
        var builderParams = walkerCtx.BuilderParams;
        // ResolveClause is invoked per leaf only. OrGroup/AndGroup are decomposed
        // by ResolveClauseLeavesInto / EmitClauseInto upstream; if one reaches here
        // it falls through to the switch default which throws "Unexpected ClauseType".

        // Spatial/Vector/Search have their own field resolution paths.
        FieldMetadata fieldMeta = default;
        bool needsFieldMeta = clause.ClauseType != ClauseType.Spatial
                              && clause.ClauseType != ClauseType.Vector
                              && clause.ClauseType != ClauseType.Search;
        if (needsFieldMeta)
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
            {
                if (cur.SentinelRewriteType != null)
                    return ResolveSentinelRewrittenBetween(cur, fieldMeta, indexSearcher, root);
                return packed.BetweenQuery(fieldMeta, indexSearcher, root);
            }

            case ClauseType.In:
            case ClauseType.AllIn:
                throw new InvalidOperationException(
                    "In/AllIn should be expanded by ResolveMatches (per-term slot loop), " +
                    "not resolved as a single clause.");

            case ClauseType.Exists:
                return indexSearcher.ExistsQuery(fieldMeta);

            case ClauseType.StartsWith:
                return indexSearcher.StartWithQuery(fieldMeta, root.StringValues[packed.Param1]);

            case ClauseType.EndsWith:
                return indexSearcher.EndsWithQuery(fieldMeta, root.StringValues[packed.Param1]);

            case ClauseType.Search:
            {
                FieldMetadata searchMeta;
                // Dynamic field name variants (search(FieldName) for auto-indexes) are
                // pre-resolved by the DynamicFieldNameResolve walker step at template time.
                string searchFieldName = clause.ResolvedFieldName ?? clause.FieldName;
                {
                    // Search clause is unreachable from the direct-test path (tests that use
                    // the test-only QueryBuilderParameters ctor never construct Search clauses),
                    // so Index is always non-null here.
                    bool forceSearch = builderParams.HasDynamics
                                       && builderParams.Index.Configuration.UseSearchAnalyzerForDynamicFieldsIfNotSetExplicitlyInSearchQuery;
                    searchMeta = QueryBuilderHelper.GetFieldMetadata(
                        builderParams.Allocator, searchFieldName, builderParams.Index,
                        builderParams.IndexFieldsMapping,
                        builderParams.HasDynamics, builderParams.DynamicFields,
                        handleSearch: true, hasBoost: builderParams.HasBoost,
                        forceDefaultSearchAnalyzer: forceSearch);
                }

                var indexDef = builderParams.Index.Definition;
                IndexSearcher.SearchQueryOptions searchQueryOptions;
                if (IndexDefinitionBaseServerSide.IndexVersion.IsCoraxSearchWildcardAdjustmentSupported(indexDef.Version))
                    searchQueryOptions = IndexSearcher.SearchQueryOptions.PhraseQueryWithWildcardAdjustments;
                else if (indexDef.Version >= IndexDefinitionBaseServerSide.IndexVersion.PhraseQuerySupportInCoraxIndexes)
                    searchQueryOptions = IndexSearcher.SearchQueryOptions.PhraseQuery;
                else
                    searchQueryOptions = IndexSearcher.SearchQueryOptions.Legacy;

                var searchTerm = root.StringValues[packed.Param1];
                if (searchQueryOptions == IndexSearcher.SearchQueryOptions.PhraseQueryWithWildcardAdjustments
                    && searchTerm is { Length: >= 1 }
                    && (searchTerm[0] == '*' || (searchTerm.Length >= 2 && searchTerm[^1] == '*')))
                {
                    searchMeta = ReplaceAnalyzerForWildcardQueries(searchMeta, walkerCtx);
                }

                var searchValues = QueryBuilderHelper.SplitSearchValue(searchTerm);

                return indexSearcher.SearchQuery(searchMeta,
                    searchValues,
                    (Constants.Search.Operator)clause.SearchOperator,
                    searchQueryOptions);
            }

            case ClauseType.Regex:
                return indexSearcher.RegexQuery(fieldMeta,
                    new Regex(root.StringValues[packed.Param1]));

            case ClauseType.Spatial:
            {
                return HandleSpatial(builderParams, cur, clause.SpatialMethodType);
            }

            case ClauseType.Vector:
            {
                var vectorItem = HandleVector(builderParams, cur);
                return vectorItem.Materialize(null);
            }

            case ClauseType.OrGroup:
                throw new InvalidOperationException(
                    "OrGroup should be expanded by ResolveMatches, not resolved as a single clause.");

            case ClauseType.AndGroup:
                throw new InvalidOperationException(
                    "AndGroup should be expanded by ResolveMatches, not resolved as a single clause.");

            default:
                throw new InvalidOperationException($"Unexpected ClauseType {clause.ClauseType} in ResolveClause.");
        }
    }

    /// <summary>Converts an Equals clause into a BetweenQuery(low==high==value) so
    /// it produces a TermsProviderMatch that SortedDrivingMatch can walk in sort order.</summary>
    private static IQueryMatch ResolveEqualsClauseWithDirection(ClauseExecution exec,
        QueryExecution queryExec, bool forward, ResolutionContext walkerCtx)
    {
        var indexSearcher = walkerCtx.IndexSearcher;
        FieldMetadata fieldMeta = ResolveFieldMetadata(exec.Clause, walkerCtx);
        var packed = exec.PackedParamValue;
        return packed.ValueType switch
        {
            PackedParam.TypeLong => indexSearcher.BetweenQuery(fieldMeta, queryExec.LongValues[packed.Param1], queryExec.LongValues[packed.Param1], forward: forward),
            PackedParam.TypeDouble => indexSearcher.BetweenQuery(fieldMeta, queryExec.DoubleValues[packed.Param1], queryExec.DoubleValues[packed.Param1], forward: forward),
            _ => indexSearcher.BetweenQuery(fieldMeta, queryExec.StringValues[packed.Param1], queryExec.StringValues[packed.Param1], forward: forward)
        };
    }


    private static IQueryMatch ResolveRangeClauseWithDirection(ClauseExecution exec,
        QueryExecution queryExec, bool forward, ResolutionContext walkerCtx)
    {
        var indexSearcher = walkerCtx.IndexSearcher;
        FieldMetadata fieldMeta = ResolveFieldMetadata(exec.Clause, walkerCtx);
        var packed = exec.PackedParamValue;

        return exec.ClauseType switch
        {
            ClauseType.GreaterThan or ClauseType.GreaterThanOrEqual or ClauseType.LessThan or ClauseType.LessThanOrEqual
                => packed.RangeQuery(exec.ClauseType, fieldMeta, indexSearcher, queryExec, forward),
            ClauseType.Between when exec.SentinelRewriteType != null =>
                ResolveSentinelRewrittenBetween(exec, fieldMeta, indexSearcher, queryExec),
            ClauseType.Between => packed.BetweenQuery(fieldMeta, indexSearcher, queryExec, forward),
            _ => ResolveClause(exec, queryExec, walkerCtx) // fallback
        };
    }


    private static IQueryMatch ResolveSentinelRewrittenBetween(ClauseExecution exec, FieldMetadata fieldMeta,
        IndexSearcher indexSearcher, QueryExecution queryExec)
    {
        if (exec.SentinelRewriteType == ClauseType.Exists)
            return indexSearcher.AllEntries();
        var packed = exec.PackedParamValue;
        if (exec.SentinelRewriteType == ClauseType.LessThanOrEqual)
            return packed.RangeQuery(ClauseType.LessThanOrEqual, fieldMeta, indexSearcher, queryExec);

        Debug.Assert(exec.SentinelRewriteType == ClauseType.GreaterThanOrEqual);
        IQueryMatch rangeMatch = packed.RangeQuery(ClauseType.GreaterThanOrEqual, fieldMeta, indexSearcher, queryExec);
        // BETWEEN low AND 'NULL' must include null-valued docs (Lucene parity)
        if (indexSearcher.TryGetPostingListForNull(in fieldMeta, out _))
        {
            var bm = new BitmapMatch(indexSearcher.Allocator);
            QueryPrimitives.OrWithMatch(rangeMatch, ref bm.BitmapState);
            QueryPrimitives.OrWithMatch(indexSearcher.TermQuery(fieldMeta, null), ref bm.BitmapState);
            return bm;
        }

        return rangeMatch;
    }

    private static IQueryMatch ResolveInTerm(ClauseExecution exec, int termIndex,
        QueryExecution queryExec, ResolutionContext walkerCtx)
    {
        var (fieldMeta, termPacked) = ResolveInTermParam(exec, termIndex, walkerCtx);
        return termPacked.TermQuery(fieldMeta, walkerCtx.IndexSearcher, queryExec);
    }

    // ── Term-source resolution ───────────────────────────────────────────

    /// <summary>Compute the field metadata and packed parameter for an IN term at the given index.
    /// Shared by <see cref="ResolveInTerm"/> (bitmap path) and <see cref="ResolveInTermSource"/>
    /// (posting-list path) to ensure field resolution and index arithmetic stay in sync.</summary>
    private static (FieldMetadata FieldMeta, PackedParam TermPacked) ResolveInTermParam(
        ClauseExecution exec, int termIndex, ResolutionContext walkerCtx)
    {
        FieldMetadata fieldMeta = ResolveFieldMetadata(exec.Clause, walkerCtx);
        return (fieldMeta, exec.PackedParamValue.WithTermOffset(termIndex));
    }

    /// <summary>Resolve field metadata for a term-source clause. Mirrors the
    /// non-Spatial/Vector/Search branch of <see cref="ResolveClause"/>.</summary>
    private static FieldMetadata ResolveFieldMetadata(ClauseInfo clause, ResolutionContext walkerCtx)
    {
        var builderParams = walkerCtx.BuilderParams;
        // Dynamic field name variants are pre-resolved by DynamicFieldNameResolve at template time.
        string resolvedFieldName = clause.ResolvedFieldName ?? clause.FieldName;

        // When forceDefaultSearchAnalyzer is enabled for indexes with dynamic fields (CreateField),
        // non-exact non-search clauses should use the search analyzer (#4778 fix).
        // HasDynamics short-circuits the Index dereference for the direct-test path.
        bool forceSearchAnalyzer = builderParams.HasDynamics
                                   && !clause.IsExact
                                   && clause.ClauseType != ClauseType.Search
                                   && builderParams.Index.Configuration.UseSearchAnalyzerForDynamicFieldsIfNotSetExplicitlyInSearchQuery;
        return QueryBuilderHelper.GetFieldMetadata(in builderParams, resolvedFieldName, exact: clause.IsExact,
            hasBoost: builderParams.HasBoost, forceDefaultSearchAnalyzer: forceSearchAnalyzer);
    }

    // ── Scan parameter extraction ────────────────────────────────────────

    // ── Execution-phase methods (moved from QueryPlanBuilder.cs) ──────────

    /// <summary>Cardinality used for cost estimation; <c>NumberOfEntries</c> is the
    /// fallback when a clause hasn't computed a cardinality yet (e.g. multi-term or
    /// regex). Callers treat the fallback as "could match everything."</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long EffectiveCardinality(in ClauseExecution exec, IndexSearcher indexSearcher)
        => exec.Cardinality > 0 ? exec.Cardinality : indexSearcher.NumberOfEntries;

    private static long ComputeNumberOfEntriesQueryLikelyToScan(List<ClauseExecution> execs,
        int drivingIdx, long drivingCard, long pageSize, IndexSearcher indexSearcher)
    {
        // The scan walks the driving stream in sort order and stops once the page is full, so the
        // number of *results* we actually need is bounded by the page size — there is no point in
        // costing the scan as if it produced every driving match when the caller only asked for the
        // first `pageSize` of them. A `pageSize` of int.MaxValue ("get everything") leaves the
        // driving cardinality as the binding limit, so this only narrows the estimate for top-N queries.
        long resultsWanted = Math.Min(drivingCard, pageSize);

        // first, we find the most selective residual clause
        long minResidual = long.MaxValue;
        for (int i = 0; i < execs.Count; i++)
        {
            if (i == drivingIdx) continue;
            long c = EffectiveCardinality(execs[i], indexSearcher);
            minResidual = Math.Min(c, minResidual);
        }

        if (minResidual > 0 && minResidual < indexSearcher.NumberOfEntries)
        {
            // here we, check what is the pass rate of the most selective residual clause (i.e, 1% of entries matched, etc)
            double passRate = (double)minResidual / indexSearcher.NumberOfEntries;
            if (passRate > 0)
            {
                // if pass rate is 1%, we have to scan through 10_000 entries to get 100, etc, so we need to inflate the costs.
                // We inflate the *results wanted* (page-bounded) rather than the full driving cardinality: filling a 10-row
                // page through a 1%-selective residual means scanning ~1_000 entries, regardless of how large the driving set is.
                return (long)(resultsWanted / passRate);
            }
        }

        return resultsWanted;
    }

    /// <summary>
    /// Are we expecting to go through more entries by weighted cost or via bitmap
    /// And if we are going through direct cost, will we need to read more than 32K  entries
    /// </summary>
    private static bool IsDirectScanCostEffective(long entriesToScan, long bitmapCost)
    {
        long directCost =  entriesToScan > long.MaxValue / QueryPrimitives.EntryScanCostMultiplier
            ? long.MaxValue // avoid overflow
            : entriesToScan * QueryPrimitives.EntryScanCostMultiplier;
        return directCost < bitmapCost && entriesToScan <= QueryPrimitives.EntryScanCountThreshold;
    }

    /// <summary>Per-execution cost gate for the CompoundField candidate. Structural eligibility
    /// (driving clause, compound sort field, non-boosted scannable residuals) was baked at
    /// cache-miss by <see cref="TryCreateCompoundFieldMatch"/>; this re-evaluates the
    /// bitmap-vs-direct-scan cost using the CURRENT bound-parameter cardinalities so a cached plan
    /// never reuses a stale strategy decision.</summary>
    private static bool CompoundFieldCostEffective(ref InstCtx ctx, out long entriesToScan, out long bitmapCost)
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
            bitmapCost += EffectiveCardinality(execs[i], indexSearcher);
            if (i == drivingIdx || i == field2RangeIdx)
                continue;
            residualCount++;
        }

        long drivingCardinality = EffectiveCardinality(drivingExec, indexSearcher);
        entriesToScan = residualCount > 0
            ? ComputeNumberOfEntriesQueryLikelyToScan(execs, drivingIdx, drivingCardinality, ctx.BuilderParams.Query.PageSize, indexSearcher)
            : drivingCardinality;

        return IsDirectScanCostEffective(entriesToScan, bitmapCost);
    }

    /// <summary>Per-execution cost gate for the DirectScan candidate. Structural eligibility
    /// (ORDER BY shape, sort-driving clause, scannable residuals) was baked at cache-miss by
    /// <see cref="TryCreateSimpleFieldDirectScan"/>; this re-evaluates the cost using the CURRENT
    /// bound-parameter cardinalities (RavenDB #4852). Full scans have no cost gate — the sort-field
    /// tree is walked directly — so they always pass. <paramref name="entriesToScan"/> and
    /// <paramref name="bitmapCost"/> are surfaced for the inspection Reason string.</summary>
    private static bool DirectScanCostEffective(ref InstCtx ctx, bool isFullScan, out long entriesToScan, out long bitmapCost)
    {
        entriesToScan = 0;
        bitmapCost = 0;
        if (isFullScan)
            return true;

        var execs = ctx.Exec.Executions;
        int drivingIdx = ctx.Exec.Plan.SortDrivingClauseIndex;
        if (drivingIdx < 0 || drivingIdx >= execs.Count)
            return false;
        if (execs[drivingIdx].PackedParamValue.IsNone)
            return false;

        var indexSearcher = ctx.PlanParams.IndexSearcher;
        foreach (var it in execs)
        {
            bitmapCost += EffectiveCardinality(it, indexSearcher);
        }

        long drivingCard = EffectiveCardinality(execs[drivingIdx], indexSearcher);
        entriesToScan = execs.Count > 1
            ? ComputeNumberOfEntriesQueryLikelyToScan(execs, drivingIdx, drivingCard, ctx.BuilderParams.Query.PageSize, indexSearcher)
            : drivingCard;

        return IsDirectScanCostEffective(entriesToScan, bitmapCost);
    }


    private static bool IsEmptyIn(ClauseExecution e) =>
        // HasNullTerm must also block the empty-IN path: a list whose only entry
        // is null arrives as InTermCount=0+HasNullTerm=true and still has to match
        // docs with a null in that field via the null-term posting list (Fill@0
        // reads the null PL, OrRange/AndRange becomes a runtime no-op when
        // InRangeCounts[rangeIdx] resolves to 0).
        e.ClauseType is ClauseType.In or ClauseType.AllIn &&
        (e.InTermCount == 0) &&
        e.HasNullTerm is false;

    // ── Plan helpers ─────────────────────────────────────────────────────

    /// <summary>True iff <paramref name="exec"/> carries any boost — either annotated at
    /// template time (<see cref="ClauseInfo.HasBoost"/>) or with a resolved runtime
    /// factor (<see cref="ClauseExecution.BoostFactor"/> &gt; 0). Compound-key and
    /// direct-scan paths can't propagate scores, so they reject any boosted clause.</summary>
    private static bool IsClauseBoosted(ClauseExecution exec)
        => exec.Clause.HasBoost || exec.BoostFactor > 0;

    /// <summary>Encode a numeric (long/double) field value at <paramref name="paramIdx"/>
    /// into 8 big-endian sortable bytes — the same encoding indexing uses for compound-key
    /// long/double fields. Doubles map through <see cref="Bits.DoubleToSortableLong"/>
    /// first so that descending order matches IEEE-754 semantics. Writes directly into
    /// <paramref name="dest"/> (which must be exactly <c>sizeof(long)</c> bytes), so the
    /// caller controls the allocation strategy and no managed <c>byte[]</c> is needed.</summary>
    private static void EncodeNumericValue(Span<byte> dest, int valueType, int paramIdx, QueryExecution exec)
    {
        long raw = valueType == PackedParam.TypeDouble
            ? Bits.DoubleToSortableLong(exec.DoubleValues[paramIdx])
            : exec.LongValues[paramIdx];
        BinaryPrimitives.WriteInt64BigEndian(dest, Bits.SwapBytes(raw));
    }

    /// <summary>One value-slot of a compound key: the resolved size plus the source
    /// (typed packed-param, plus the cached analyzed <see cref="Slice"/> for the string
    /// case so <c>EncodeAndApplyAnalyzer</c> doesn't run twice — once to size, once to
    /// write). Numeric slots carry only <see cref="Packed"/>; <see cref="Analyzed"/>
    /// stays <c>default</c>.</summary>
    private struct CompoundFieldEncoding
    {
        public PackedParam Packed;
        /// <summary>String case only: analyzed value (from the per-execution analyzed-slice
        /// cache). For Between, <see cref="Packed.Param2"/> selects the high bound — but
        /// this struct only holds one bound, so callers build two encodings.</summary>
        public Slice Analyzed;
        /// <summary>For the string case, the slot index inside the typed-value array that
        /// <see cref="Analyzed"/> was resolved from. Used when the caller wants the high
        /// bound (Between) and asks for the encoding at <see cref="PackedParam.Param2"/>
        /// rather than <c>Param1</c>.</summary>
        public int SourceSlot;
        public int Size;
    }

    /// <summary>Resolve a compound-key value slot to its size + write-source.
    /// String slots run the analyzer (cached) and reject sizes &gt;255 (the trailing
    /// length byte in the compound-key format is a single byte). Numeric slots return
    /// <c>sizeof(long)</c>. Any other ValueType returns false.</summary>
    private static bool TryGetCompoundFieldEncoding(string fieldName, PackedParam packed, int paramSlot,
        ref InstCtx ctx, out CompoundFieldEncoding encoding)
    {
        encoding = default;
        encoding.Packed = packed;
        encoding.SourceSlot = paramSlot;

        if (packed.ValueType == PackedParam.TypeString)
        {
            var meta = QueryBuilderHelper.GetFieldMetadata(in ctx.BuilderParams, fieldName, hasBoost: false);
            encoding.Analyzed = ctx.Exec.GetAnalyzedSlice(ctx.PlanParams.IndexSearcher, meta, paramSlot);
            encoding.Size = encoding.Analyzed.Size;
            return true;
        }

        if (packed.ValueType is PackedParam.TypeLong or PackedParam.TypeDouble)
        {
            encoding.Size = sizeof(long);
            return true;
        }

        return false;
    }

    /// <summary>Write the encoded bytes of a compound-key slot into <paramref name="dest"/>.
    /// <paramref name="dest"/> must be exactly <see cref="CompoundFieldEncoding.Size"/> bytes long.
    /// String: copies the cached analyzed bytes. Numeric: delegates to
    /// <see cref="EncodeNumericValue"/> for 8-byte big-endian sortable encoding.</summary>
    private static void WriteCompoundFieldEncoding(Span<byte> dest, CompoundFieldEncoding encoding, QueryExecution exec)
    {
        if (encoding.Packed.ValueType == PackedParam.TypeString)
        {
            encoding.Analyzed.AsReadOnlySpan().CopyTo(dest);
            return;
        }
        EncodeNumericValue(dest, encoding.Packed.ValueType, encoding.SourceSlot, exec);
    }


    /// <summary>Decide whether a clause type can be expressed as a single
    /// <see cref="PostingSource"/>. Boosted clauses go through the IQueryMatch path
    /// even when they're term-shaped, so scoring still works.</summary>
    internal static bool IsTermSourceEligibleClause(ClauseInfo clause)
    {
        return clause is { HasBoost: false, ClauseType: ClauseType.Equals or ClauseType.NotEquals };
    }

    /// <summary>TreeScan-eligible: multi-term clauses that have a direct ITermsProvider
    /// (StartsWith, EndsWith, Exists, Regex, ranges). Boosted clauses go through QueryMatch
    /// for scoring. Contains is excluded because its tree walk pattern doesn't benefit
    /// from the direct dispatch (it walks the full tree regardless).</summary>
    internal static bool IsTreeScanEligibleClause(ClauseInfo clause)
    {
        if (clause.HasBoost)
            return false;

        if (clause.ClauseType is ClauseType.StartsWith or ClauseType.EndsWith
            or ClauseType.Exists or ClauseType.Regex
            or ClauseType.GreaterThan or ClauseType.GreaterThanOrEqual
            or ClauseType.LessThan or ClauseType.LessThanOrEqual)
            return true;

        if (clause.ClauseType != ClauseType.Between)
            return false;

        // Parameter-bound BETWEEN sentinels use QueryMatch dispatch, not TreeScan.
        // Because we have to deal with sentinals (NULL/*) in the parameters, which change how
        // we process the query (may need to also include from the null posting list, etc. 
        foreach (var t in clause.Bindings)
        {
            if (t is { Source: BindingSource.QueryParameter })
                return false;
        }

        return true;
    }

    /// <summary>Resolve the <see cref="MatchDispatch"/> mode for a clause at plan-build time.
    /// Equals / NotEquals (unboosted) → <c>PostingList</c> (native posting-list).
    /// Multi-term (unboosted) → <c>TreeScan</c> (direct ITermsProvider, no IQueryMatch wrapper).
    /// All other clause types → <c>QueryMatch</c> (IQueryMatch interface dispatch).</summary>
    private static MatchDispatch GetDispatch(ClauseInfo clause)
    {
        if (IsTermSourceEligibleClause(clause))
            return MatchDispatch.PostingList;

        if (IsTreeScanEligibleClause(clause))
            return MatchDispatch.TreeScan;

        return MatchDispatch.QueryMatch;
    }

    internal static ScanPredicateInfo? BuildScanPredicateInfo(ClauseExecution exec, ref int sliceIndex)
        => BuildScanPredicateInfoCore(exec, exec.TermValueType, ref sliceIndex);

    private static bool IsScanEligible(ClauseExecution exec)
    {
        int s = 0;
        return BuildScanPredicateInfo(exec, ref s) is not null;
    }

    /// <summary>Build a <see cref="ScanPredicateInfo"/> for a single clause execution. For numeric
    /// (long/double) predicates, <c>ParamIndex</c>/<c>ParamIndex2</c> are the original
    /// <see cref="PackedParam.Param1"/>/<see cref="PackedParam.Param2"/> indices into
    /// <see cref="QueryExecution.LongValues"/>/<see cref="QueryExecution.DoubleValues"/> — the
    /// emitted IL reads those arrays directly via baked literals (no per-residual array copy).
    /// Slice predicates retain a dense local index because their values flow through the analyzer
    /// and live in a separate analyzed-slice array.</summary>
    private static ScanPredicateInfo? BuildScanPredicateInfoCore(ClauseExecution exec, ParamValueType termType, ref int sliceIndex)
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
                // Negated or boosted IN falls back to the bitmap posting-list union: the residual
                // scan expresses neither negation nor boost scoring. The value set itself is
                // materialized per execution into QueryExecution.ResidualInSets (see ScanParamExtractor);
                // the residual IL only carries the field + compare op + value-type, all cache-stable
                // (the runtime value count is the materialized array's length).
                if (exec.IsNegated || clause.HasBoost)
                    return null;

                ScanValueType inValueType = exec.PackedParamValue.ValueType switch
                {
                    PackedParam.TypeLong => ScanValueType.Long,
                    PackedParam.TypeDouble => ScanValueType.Double,
                    _ => ScanValueType.Slice
                };
                return new ScanPredicateInfo
                {
                    FieldName = clause.FieldName,
                    ValueType = inValueType,
                    CompareOp = clause.ClauseType == ClauseType.In ? ScanCompareOp.In : ScanCompareOp.AllIn,
                    ParamIndex = 0
                };
            }

            case ClauseType.StartsWith:
                if (termType != ParamValueType.String)
                    return null;
                sliceIndex++;
                return new ScanPredicateInfo
                {
                    FieldName = clause.FieldName,
                    ValueType = ScanValueType.Slice,
                    CompareOp = ScanCompareOp.StartsWith,
                    ParamIndex = sliceIndex - 1
                };
            case ClauseType.EndsWith:
                if (termType != ParamValueType.String)
                    return null;
                sliceIndex++;
                return new ScanPredicateInfo
                {
                    FieldName = clause.FieldName,
                    ValueType = ScanValueType.Slice,
                    CompareOp = ScanCompareOp.EndsWith,
                    ParamIndex = sliceIndex - 1
                };
            case ClauseType.Exists:
                return new ScanPredicateInfo
                {
                    FieldName = clause.FieldName,
                    ValueType = ScanValueType.Long,
                    CompareOp = ScanCompareOp.Exists,
                    ParamIndex = 0
                };

            case ClauseType.AndGroup:
            case ClauseType.OrGroup:
            {
                var subExecs = exec.SubExecutions;
                var branches = new List<ScanPredicateInfo>();
                // Save slice index so we can roll back if any subclause is unscannable.
                int slc = sliceIndex;
                for (int si = 0; si < subExecs.Count; si++)
                {
                    var subTermType = subExecs[si].TermValueType;
                    var subPred = BuildScanPredicateInfoCore(subExecs[si], subTermType, ref slc);
                    if (subPred == null)
                        return null;
                    branches.Add(subPred.Value);
                }

                sliceIndex = slc;
                return new ScanPredicateInfo
                {
                    FieldName = clause.FieldName ?? subExecs[0].Clause.FieldName,
                    SubPredicates = branches.ToArray(),
                    Group = clause.ClauseType == ClauseType.AndGroup ? GroupKind.And : GroupKind.Or
                };
            }
        }

        // Determine value type and comparison op
        ScanCompareOp compareOp = clause.ClauseType switch
        {
            ClauseType.Equals => ScanCompareOp.Equal,
            ClauseType.NotEquals => ScanCompareOp.NotEqual,
            ClauseType.GreaterThan => ScanCompareOp.GreaterThan,
            ClauseType.GreaterThanOrEqual => ScanCompareOp.GreaterThanOrEqual,
            ClauseType.LessThan => ScanCompareOp.LessThan,
            ClauseType.LessThanOrEqual => ScanCompareOp.LessThanOrEqual,
            ClauseType.Between => ScanCompareOp.Between,
            _ => ScanCompareOp.Equal
        };

        ScanValueType valueType = termType switch
        {
            ParamValueType.Long => ScanValueType.Long,
            ParamValueType.Double => ScanValueType.Double,
            _ => ScanValueType.Slice // String/True/False/Null/Parameter (when unresolvable) → opaque slice comparison.
        };

        // Numeric predicates use the packed source indices directly (PackedParam.Param1/Param2 into
        // QueryExecution.LongValues/DoubleValues). Plan-cache key invariance: whenFlags + fullKinds
        // captures sentinel rewrites and clause-type variants, so for a given cache key the writer
        // emits the same per-clause slot assignment across executions.
        // Slice predicates keep a dense local index because their values are analyzer-encoded and live
        // in a separate ResidualParams.Slices array.
        var packed = exec.PackedParamValue;
        int idx, idx2;
        if (valueType == ScanValueType.Slice)
        {
            bool isBetween = clause.ClauseType == ClauseType.Between;
            idx = sliceIndex++;
            idx2 = isBetween ? sliceIndex++ : -1;
        }
        else
        {
            idx = packed.Param1;
            idx2 = packed.Param2 != PackedParam.NoParamValue ? packed.Param2 : -1;
        }

        return new ScanPredicateInfo
        {
            FieldName = clause.FieldName,
            ValueType = valueType,
            CompareOp = compareOp,
            ParamIndex = idx,
            ParamIndex2 = idx2
        };
    }


    internal static string FormatValueFromPlan(PackedParam packed, QueryExecution exec) => FormatValueFromPlanInternal(packed, exec, packed.Param1);

    /// <summary>Format the second value (BETWEEN high bound) from the exec's typed arrays.</summary>
    internal static string FormatValue2FromPlan(PackedParam packed, QueryExecution exec) => FormatValueFromPlanInternal(packed, exec, packed.Param2);


    private static string FormatValueFromPlanInternal(PackedParam packed, QueryExecution exec, int idx)
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
