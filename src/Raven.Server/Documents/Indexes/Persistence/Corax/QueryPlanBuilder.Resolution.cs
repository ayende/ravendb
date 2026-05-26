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
using Corax.Indexing;
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

/// <summary>
/// Per-execution resolution: compiles plans, resolves matches and term sources
/// from clause metadata, extracts typed scan parameters, handles highlighting,
/// sorting, and spatial/vector materialization.
///
/// Methods here run once per query execution (not cached).
/// </summary>
internal static partial class QueryPlanBuilder
{

    internal readonly record struct BuildCompileAndOptimizeResult(
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


    private ref struct InstCtx(CompiledPlan plan, QueryExecution exec, OrderMetadata[] orderByFields, PlanParameters planParams, QueryBuilderParameters builderParams)
    {
        public readonly CompiledPlan Plan = plan;
        public readonly QueryExecution Exec = exec;
        public readonly OrderMetadata[] OrderByFields = orderByFields; // may be null when PageSize == 0
        public readonly PlanParameters PlanParams = planParams;
        public readonly QueryBuilderParameters BuilderParams = builderParams;

        public string RejectReason;
    }

    private enum MergeKind
    {
        /// <summary>Slot 0 ← clause result. First op of an OR chain or first
        /// non-negated element of an AND chain.</summary>
        Fill,
        /// <summary>slot 0 ← slot 0 ∪ clause. Subsequent OR-chain elements.</summary>
        OrInto,
        /// <summary>slot 0 ← slot 0 ∩ clause. Subsequent positive AND-chain elements.</summary>
        AndInto,
        /// <summary>slot 0 ← slot 0 \ clause. Negated AND-chain elements.</summary>
        AndNotInto
    }


    private interface ISlotResolver<TSelf, out TSlot>
        where TSelf : ISlotResolver<TSelf, TSlot>
    {
        static abstract MatchDispatch TargetDispatch { get; }

        static abstract TSlot ResolveInTermSlot(ClauseExecution clauseExec, int termIndex, QueryExecution exec, ResolutionContext ctx);

        static abstract TSlot ResolveNullTermSlot(ClauseExecution clauseExec, ResolutionContext ctx);

        static abstract TSlot ResolveDefaultSlot(ClauseExecution clauseExec, QueryExecution exec, ResolutionContext ctx);
    }


    private readonly struct MatchResolver : ISlotResolver<MatchResolver, IQueryMatch>
    {
        public static MatchDispatch TargetDispatch => MatchDispatch.QueryMatch;

        public static IQueryMatch ResolveInTermSlot(ClauseExecution clauseExec, int termIndex,
            QueryExecution exec, ResolutionContext ctx)
            => ResolveInTerm(clauseExec, termIndex, exec, ctx);

        public static IQueryMatch ResolveNullTermSlot(ClauseExecution clauseExec, ResolutionContext ctx)
        {
            // Always emit a match for the null-term slot: TermQuery(null) when the
            // clause actually carries a null term, otherwise CreateEmpty so the OR/AND
            // step the IL emits against this slot is a no-op.
            var indexSearcher = ctx.IndexSearcher;
            FieldMetadata nullMeta = ResolveFieldMetadata(clauseExec.Clause, ctx);
            return clauseExec.HasNullTerm
                ? indexSearcher.TermQuery(nullMeta, null)
                : TermMatch.CreateEmpty(indexSearcher, indexSearcher.Allocator);
        }

        public static IQueryMatch ResolveDefaultSlot(ClauseExecution clauseExec,
            QueryExecution exec, ResolutionContext ctx)
        {
            IQueryMatch match = ResolveClause(clauseExec, exec, ctx);
            if (clauseExec.BoostFactor is 0) 
                return match;
            return ctx.IndexSearcher.Boost(match, clauseExec.BoostFactor);
        }
    }


    private readonly struct TermSourceResolver : ISlotResolver<TermSourceResolver, PostingSource>
    {
        public static MatchDispatch TargetDispatch => MatchDispatch.PostingList;

        public static PostingSource ResolveInTermSlot(ClauseExecution clauseExec, int termIndex,
            QueryExecution exec, ResolutionContext ctx)
            => ResolveInTermSource(clauseExec, termIndex, exec, ctx);

        public static PostingSource ResolveNullTermSlot(ClauseExecution clauseExec, ResolutionContext ctx)
        {
            if (!clauseExec.HasNullTerm)
            {
                // For IN: OR with PostingSource.Empty is already a no-op — default is fine.
                // For AllIn: AND with PostingSource.Empty would clear the bitmap; return All
                // so the AND range loop can always cover inTermCount slots (including the null
                // slot) without corrupting the result. AccumulateInRangeCounts mirrors this by
                // always using inTermCount as the range for AllIn.
                return clauseExec.ClauseType == ClauseType.AllIn
                    ? new PostingSource { Kind = PostingSourceKind.All }
                    : default;
            }

            FieldMetadata nullMeta = ResolveFieldMetadata(clauseExec.Clause, ctx);
            return ctx.IndexSearcher.TryGetPostingListForNull(in nullMeta, out long nullPlId)
                ? DecodePostingListId(nullPlId, ctx.IndexSearcher)
                : default;
        }

        public static PostingSource ResolveDefaultSlot(ClauseExecution clauseExec,
            QueryExecution exec, ResolutionContext ctx)
            => ResolveSingleTermSource(clauseExec, exec, ctx);
    }


    private readonly struct TermsProviderResolver : ISlotResolver<TermsProviderResolver, ITermsProvider>
    {
        public static MatchDispatch TargetDispatch => MatchDispatch.TreeScan;

        public static ITermsProvider ResolveInTermSlot(ClauseExecution clauseExec, int termIndex,
            QueryExecution exec, ResolutionContext ctx) => null;

        public static ITermsProvider ResolveNullTermSlot(ClauseExecution clauseExec, ResolutionContext ctx) => null;

        public static ITermsProvider ResolveDefaultSlot(ClauseExecution clauseExec,
            QueryExecution exec, ResolutionContext ctx)
            => ResolveSingleTermsProvider(clauseExec, exec, ctx);
    }


    private sealed class EmptyTermsProviderInstance : ITermsProvider
    {
        public static readonly EmptyTermsProviderInstance Instance = new();
        public int FillPostingListIds(Span<long> postingListIds) => 0;
        public void Reset() { }

        public bool Next(out TermMatch term)
        {
            term = default;
            return false;
        }

        public QueryInspectionNode Inspect() => new("EmptyTermsProvider");
    }


    public static PlanTemplate BuildTemplate(PlanParameters planParams)
    {
        var queryText = planParams.Metadata.Query.QueryText;
        var planCache = planParams.IndexSearcher.PlanCache;
        return planCache.TryGetTemplate(queryText) ?? ParseTemplate(planParams);
    }

    public static IQueryMatch BuildAndCompile(
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

        // Phase 1: structural template (cached per queryText).
        var template = BuildTemplate(planParams);

        // Phase 2: parameter resolution, exec emission, IL compile (with cache miss handling).
        (compiledPlanOut, exec) = Build(template, planParams, builderParameters, walkerCtx);
        if (compiledPlanOut == null)
            return TermMatch.CreateEmpty(indexSearcher, indexSearcher.Allocator);

        // Phase 3: live binding (resolved matches, term sources, spatial/vector/highlighting wrappers).
        // BuildAndCompile uses the unconditional bitmap path — ORDER BY optimization dispatch
        // (CompoundExact / CompoundField / DirectScan) belongs to BuildCompileAndOptimize.
        return InstantiateBitmapPipeline(compiledPlanOut, exec, planParams, builderParameters, walkerCtx, highlightingTerms, wantTimings, token);
    }


    public static BuildCompileAndOptimizeResult BuildCompileAndOptimize(PlanParameters planParams,
        QueryBuilderParameters builderParameters,
        Dictionary<string, CoraxHighlightingTermIndex> highlightingTerms,
        bool wantTimings,
        CancellationToken token)
    {
        var indexSearcher = planParams.IndexSearcher;
        var walkerCtx = new ResolutionContext(builderParameters);

        // Phase 1: structural template (cached per queryText).
        var template = BuildTemplate(planParams);

        // Phase 2: parameter resolution, plan emission, IL compile (with cache-miss handling).
        var (plan, exec) = Build(template, planParams, builderParameters, walkerCtx);
        if (plan == null)
        {
            var emptyMatch = TermMatch.CreateEmpty(indexSearcher, indexSearcher.Allocator);
            return new(emptyMatch, emptyMatch, null, null, null, builderParameters, null);
        }

        // Phase 3a: resolve ORDER BY metadata (needed by Instantiate's strategy dispatch).
        var orderByFields = GetSortMetadata(builderParameters, out var hasEmptySorts);
        // Phase 3b: dispatch on the cached ExecutionStrategy (fast path) or run  discovery (cache-miss only). 
        var queryMatch = Instantiate(plan, exec, orderByFields, hasEmptySorts,
            planParams, builderParameters, walkerCtx, highlightingTerms, wantTimings, out var innerMatch, token);
        return new (queryMatch, innerMatch, queryMatch == innerMatch ? null : queryMatch, plan, exec, builderParameters, orderByFields);
    }


    private static (CompiledPlan,  QueryExecution) Build(PlanTemplate template, PlanParameters planParams, QueryBuilderParameters builderParameters, ResolutionContext walkerCtx)
    {
        var indexSearcher = planParams.IndexSearcher;
        var queryText = planParams.Metadata.Query.QueryText;
        var planCache = indexSearcher.PlanCache;

        // Step 2: Build the per-execution exec list from the template, evaluating
        // WHEN clauses against bound parameters as we go.
        var (executions, whenFlags) = EvaluateWhenAndFilterClauses(template, planParams);

        // Step 3: Populate parameter values into typed arrays
        var writer = new ValueWriter();
        foreach (var it in executions)
        {
            PopulateClauseValues(it, planParams.QueryParameters, writer, builderParameters);
        }

        // Step 3b: Constant propagation — simplify trivially-false/simple clauses.
        bool isOr = template.IsOr;
        PropagateBetweenContradictions(executions, writer);

        // Step 4: Estimate cardinality (needs populated values).
        foreach (var it in executions)
        {
            if (it.Cardinality >= 0) continue;
            it.Cardinality = EstimateCardinality(it, indexSearcher, writer, walkerCtx);
        }

        // Step 5: sort executions by cardinality
        executions.Sort();

        var exec = new QueryExecution { Executions = executions };

        if(executions.Count is 0)
        {
            if (template.Clauses.Count > 0)
            {
                // Consider the following two queries, when both $a and $b are false:
                // FROM Orders WHERE when($a, Name = 'x') AND when($b, Price > 10)
                // FROM Orders WHERE when($a, Name = 'x') OR when($b, Price > 10)
                // Regardless of the query, we have nothing _to_ select here, so we return nothing
                return default; // Caller will use TermMatch.CreateEmpty.
            }

            // FROM Post - i.e, query with no where clauses, still needs a compiled delegate, so we go generate a cached exec for it
            exec.IsAllEntries = true;
        }

        // ── Step 6: Compute cache key components (cheap) ────────────────────
        int operandOrdering = ComputeOperandOrdering(template, planParams, executions);

        (int typeSignature, byte[] fullKinds) = ComputeTypeSignature(template, planParams);

        if(planCache.Get(queryText, operandOrdering, typeSignature, fullKinds, whenFlags) is {} compiledPlan)
            return FinalizePlan(); // use cached plan
        

        // ── Step 7: Cache miss — full exec emission ─────────────────────────

        // Per-leaf effective dispatch in resolver-walk order, with the boost-override
        // pre-applied. ResolveClauseLeavesInto reads compiledPlan.ClauseDispatch[i] to
        // decide whether to populate slot[i] for its TargetDispatch — boost handling
        // becomes a no-op inside the resolver because every entry is QueryMatch under boost.
        MatchDispatch[] clauseDispatch = ComputeClauseDispatch(executions, planParams.HasBoost, template);

        var (ops, requiredBitmaps, inRangeCounts) = EmitPlan(isOr, executions);

        // Boost handling: force every op to QueryMatch dispatch so the IL emitter
        // generates IQueryMatch-based methods that accumulate scores.
        for (int i = 0; i < ops.Length && planParams.HasBoost; i++)
        {
            ops[i].Dispatch = MatchDispatch.QueryMatch;
        }

        // Compile and cache. Structural fields (AllNegated, OptimizationFlags, remapped
        // indices, ScanPredicateInfos) are stored on the CompiledPlan, not on QueryExecution.
        var scanPredicates = isOr is false && executions.Count > 1 ? CreateScanPredicates(executions) : null;
        compiledPlan = new CompiledPlan
        {
            CompiledDelegate = QueryIlEmitter.EmitDelegate(ops, out var csharpText, emitTimings: false),
            CompiledTimedDelegate = QueryIlEmitter.EmitDelegate(ops, out _, emitTimings: true),
            CompiledEntryPredicate = ResidualScanIlEmitter.EmitDelegate(scanPredicates, out var scanCsharp),

            Template = template,
            Source = csharpText + "\n" + scanCsharp,
            Ordering = operandOrdering,
            TypeSignature = typeSignature,
            FullKinds = fullKinds,
            WhenFlags = whenFlags,
            OpCount = ops.Length,
            RequiredBitmaps = requiredBitmaps,
            InRangeSlotCount = inRangeCounts?.Length ?? 0,
            InspectionTemplate = BuildInspectionTemplate(ops, executions),
            ScanPredicateInfos = scanPredicates,
            AllNegated =  CheckAllNegated(executions),
            ClauseDispatch = clauseDispatch,
        };
        RemapOptimizationIndices(compiledPlan, executions);
        planCache.Add(queryText, compiledPlan, template);

        return FinalizePlan();

        (CompiledPlan, QueryExecution ) FinalizePlan()
        {
            exec.Plan = compiledPlan;
            if(compiledPlan.InRangeSlotCount is not 0)
                exec.InRangeCounts = BuildInRangeCounts(executions, compiledPlan.InRangeSlotCount);

            AttachSpatialAndVectorClauses(exec, template, planParams, builderParameters, writer);
            // Re-snapshot typed arrays into exec: AttachSpatialAndVectorClauses appended
            writer.SetValues(exec);
            return (compiledPlan, exec);
        }
    }
    
    
    /// <summary>
    /// Phase 3 dispatcher: produce the final <see cref="IQueryMatch"/> for a compiled plan,
    /// applying ORDER BY when present. On the first execution (<see cref="ExecutionStrategy.NotEvaluated"/>)
    /// runs the Try* discovery chain — CompoundExact → CompoundField → DirectScan — and caches
    /// the winner's strategy + structural facts on <paramref name="compiledPlan"/>. Subsequent
    /// executions read the cached <see cref="CompiledPlan.Strategy"/> and dispatch straight to
    /// the matching Construct* helper, skipping discovery.
    ///
    /// The bitmap pipeline (<see cref="InstantiateBitmapPipeline"/>) is the last fallback —
    /// reached either when all Try* methods reject (cache-miss path) or when a cached Construct*
    /// returns null on per-execution rejection (e.g. byte-length overflow for CompoundExact).
    /// </summary>
    /// <param name="innerMatch">Pre-wrap inner match: same as the return value for the no-wrap
    /// strategies (CompoundExact / DirectScan / no ORDER BY), the compound match for CompoundField,
    /// or the bitmap CompiledQueryMatch for BitmapSort. The caller uses this for inspection-graph
    /// construction and deterministic disposal of the IL-emitted match.</param>


    private static (List<ClauseExecution> ExecList, int WhenFlags) EvaluateWhenAndFilterClauses(PlanTemplate template, PlanParameters planParams)
    {
        var execList = new List<ClauseExecution>(template.Clauses.Count);
        int whenFlags = 0;
        if (template.WhenCount == 0)
        {
            // Fast path: no WHEN clauses anywhere in the template — skip the per-clause
            // WhenCondition null check. Common case for non-conditional queries.
            foreach (var cached in template.Clauses)
            {
                execList.Add(CreateExecution(cached));
            }
            return (execList, whenFlags);
        }

        int whenBit = 0;
        foreach (var cached in template.Clauses)
        {
            if (cached.WhenCondition is {} predicate)
            {
                if (predicate(planParams.QueryParameters) == false)
                {
                    whenBit++;
                    continue;
                }

                whenFlags |= 1 << whenBit;
                whenBit++;
            }

            execList.Add(CreateExecution(cached));
        }

        return (execList, whenFlags);
    }


    private static ClauseExecution CreateExecution(ClauseInfo clause)
    {
        var exec = new ClauseExecution(clause);
        
        if (clause.SubClauses is not { Count: > 0 }) 
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
        // Always recurse into subclauses first (OrGroup/AndGroup have no binding of their own)
        foreach (var it in exec.SubExecutions ?? [])
        {
            PopulateClauseValues(it, queryParameters, writer, builderParameters);
        }

        // Resolve boost factor if this clause is boosted
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
            // BETWEEN: Literal sentinel bounds are rewritten at template time.
            // Parameter-bound sentinels are detected here at execution time.
            case ClauseType.Between:
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
                    case (false,false):
                        exec.TermValueType = lowType;
                        exec.PackedParamValue = writer.AddPair(low, high, ToValueTokenType(lowType));
                        return;
                }
            }
            case ClauseType.In or ClauseType.AllIn:
                // Boosted clauses store the boost factor in the trailing binding (read by
                // ResolveBoostFactor via Bindings[^1]); exclude it from the IN-term walk.
                var inBindings = exec.Clause.HasBoost
                    ? bindings.AsSpan(0, bindings.Length - 1).ToArray()
                    : bindings;
                ResolveInFromBindings(exec, queryParameters, writer, inBindings, builderParameters);
                break;
            default:
                // Simple clause (Equals, Range, Search, Regex, etc.): single value at Bindings[0]
                var (value, valueType) = ResolveBindingScalar(bindings[BindingIndex.Value], queryParameters, builderParameters);
                // startsWith/endsWith/search/regex require a String argument — reject Null (matches Lucene behavior).
                if (value == null && exec.Clause.ClauseType is ClauseType.StartsWith or ClauseType.EndsWith or ClauseType.Search or ClauseType.Regex) 
                    ThrowInvalidMethodArgument(exec.Clause);

                exec.TermValueType = valueType;
                exec.PackedParamValue = writer.Add(value, ToValueTokenType(valueType));
                break;
        }
    }


    private static void ResolveInFromBindings(ClauseExecution exec, BlittableJsonReaderObject queryParameters, ValueWriter writer, 
        ParameterBinding[] bindings, QueryBuilderParameters builderParameters)
    {
        var resolvedValues = new List<object>(bindings.Length);
        var termTypes = new List<ParamValueType>(bindings.Length);
        bool hasNullTerm = false;

        foreach (var it in bindings)
        {
            switch (it.Source)
            {
                case BindingSource.Literal:
                    if (it.LiteralValue == null)
                    {
                        hasNullTerm = true; 
                        continue;
                    }
                    resolvedValues.Add(it.LiteralValue);
                    termTypes.Add(it.LiteralType);
                    break;

                case BindingSource.QueryParameter:
                {
                    // Parameter — resolve from blittable. May be scalar or array.
                    queryParameters.TryGet(it.ParameterName, out object inRaw);
                    if (inRaw is BlittableJsonReaderArray arr)
                    {
                        foreach (var elem in arr)
                        {
                            var (elemVal, elemType) = ResolveParameterValue(elem);
                            if (elemVal == null)
                            {
                                hasNullTerm = true; 
                                continue;
                            }
                            resolvedValues.Add(elemVal);
                            termTypes.Add(ToParamValueType(elemType));
                        }
                    }
                    else if (inRaw != null)
                    {
                        var (singleVal, singleType) = ResolveParameterValue(inRaw);
                        if (singleVal == null)
                        {
                            hasNullTerm = true; 
                            continue;
                        }
                        resolvedValues.Add(singleVal);
                        termTypes.Add(ToParamValueType(singleType));
                    }
                    else
                    {
                        hasNullTerm = true;
                    }
                    break;
                }

                case BindingSource.DeferredMethod:
                {
                    var (val, type) = ResolveBindingScalar(it, queryParameters, builderParameters);
                    if (val == null)
                    {
                        hasNullTerm = true; 
                        continue;
                    }
                    resolvedValues.Add(val);
                    termTypes.Add(type);
                    break;
                }
            }
        }

        ParamValueType dominantType = resolvedValues.Count > 0 ? termTypes[0] : ParamValueType.String;
        EmitInTerms(exec, writer, dominantType, resolvedValues, hasNullTerm);
    }


    private static void EmitInTerms(ClauseExecution exec, ValueWriter writer, ParamValueType dominantType, List<object> values, bool hasNullTerm)
    {
        var (packedType, startIdx) = writer.ResolveInSlot(dominantType);
        var dominantTokenType = ToValueTokenType(dominantType);

        int written = 0;
        for (int i = 0; i < values.Count; i++)
        {
            writer.Add(values[i], dominantTokenType);
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
            $"Method {methodName}() expects to get an argument of type String while it got null");
    }


    private static void PropagateBetweenContradictions(List<ClauseExecution> execList, ValueWriter writer)
    {
        foreach (var exec in execList)
        {
            var p = exec.PackedParamValue;

            if (exec.Clause.ClauseType != ClauseType.Between || p.Param2 is PackedParam.NoParamValue)
                continue;


            bool contradictory = p.ValueType switch
            {
                PackedParam.TypeLong => writer.GetLong(p.Param1) > writer.GetLong(p.Param2),
                PackedParam.TypeDouble => writer.GetDouble(p.Param1) > writer.GetDouble(p.Param2),
                _ => false // for strings, we have to consider analyzers, so we can't tell
            };
            if (!contradictory)
                continue;

            exec.Cardinality = 0;
            exec.InTermCount = 0;
            exec.HasNullTerm = false;
            exec.ClauseType = ClauseType.In; // Reuse empty-IN elimination in EmitPlan
        }
    }


    private static ScanPredicateInfo[] CreateScanPredicates(List<ClauseExecution> executions)
    {
        var allNegated = CheckAllNegated(executions);
        // Scan predicates allow us to do a direct scan of the entires to reduce the number of document loads
        // Consider the query: FROM Posts WHERE Tags = 'good' AND Status = 'Public'
        // If Tags = 'good' gave us 100 items, we don't want to do an AndWith Status = 'Public' (may have 1M items)
        // it is cheaper to evaluate 100 entries to find if Status = 'Public' directly
        List<ScanPredicateInfo> predicates = []; 
        int scanStart = allNegated ? 0 : 1; // Skip clause 0 (the seed) unless all clauses are negated (then we start from AllEntries, so every clause is a scan predicate).
        int longIndex = 0, doubleIndex = 0, sliceIndex = 0;
        for (int si2 = scanStart; si2 < executions.Count; si2++)
        {
            if (BuildScanPredicateInfo(executions[si2], ref longIndex, ref doubleIndex, ref sliceIndex) is {} pred)
                predicates.Add(pred);
        }
        return predicates.Count > 0 ? predicates.ToArray() : null;
    }


    private static int ComputeOperandOrdering(PlanTemplate template, PlanParameters planParams, List<ClauseExecution> executions)
    {
        // Empty-IN in AND: guaranteed zero results (AND with empty = empty).
        //   $p = [] - empty vs. $p = ['news'] → has results - we need to tell the differernce.
        if (template.IsOr is false && HasEmptyIn(executions))
            return QueryExecution.EmptyInOrdering;
        
        int operandOrdering = 0;
        
        for (int i = 0; i < Math.Min(executions.Count, 10); i++)
            operandOrdering |= (executions[i].Clause.OriginalIndex & 0x7) << (i * 3);
        
        if (planParams.HasBoost)
            operandOrdering |= QueryExecution.HasBoostBit;

        // Cardinality cliff bit: queries under vs. over the cliff get different compiled plans, so the bit is part of the cache key.
        if (template.SortDrivingClauseIndex >= 0) 
            operandOrdering |= SetCardinalityCliffBit(executions, template.SortDrivingClauseIndex);
        return operandOrdering;
    }


    private static int SetCardinalityCliffBit(List<ClauseExecution> executions, int templateIdx)
    {
        foreach (var execution in executions)
        {
            if (execution.Clause.OriginalIndex != templateIdx) 
                continue;
                
            long drivingCard = execution.Cardinality;
            if (drivingCard is >= 0 and <= QueryPrimitives.TieBreakGroupInitialCapacity)
                return QueryExecution.CardinalityCliffBit;
            break;
        }
        return 0;
    }


    private static void RemapOptimizationIndices(CompiledPlan plan, List<ClauseExecution> executions)
    {
        var template = plan.Template;
        for (int i = 0; i < executions.Count; i++)
        {
            ClauseExecution exec = executions[i];
            if (exec.Clause.OriginalIndex == template.SortDrivingClauseIndex)
                plan.SortDrivingClauseIndex = i;
            if (exec.Clause.OriginalIndex == template.CompoundExactClauseA)
                plan.CompoundExactClauseA = i;
            if (exec.Clause.OriginalIndex == template.CompoundExactClauseB)
                plan.CompoundExactClauseB = i;
            if (exec.Clause.OriginalIndex == template.CompoundFieldDrivingClause)
                plan.CompoundFieldDrivingClause = i;
        }
    }


    private static (int TypeSignature, byte[] FullKinds) ComputeTypeSignature(PlanTemplate template, PlanParameters planParams)
    {
        // Each unique query parameter contributes 2 bits (its runtime type: long/double/slice/sliceLong). Literals are excluded — their types are fixed at template time.
        int typeSignature = 0;
        var fullKinds = template.ParameterSlots.Length > 16 ? new byte[template.ParameterSlots.Length] : null;
        for (int i = 0; i < template.ParameterSlots.Length; i++)
        {
            int kind = (int)ClassifyParamType(planParams.QueryParameters, template.ParameterSlots[i]) & 0x3;
            fullKinds?[i] = (byte)kind;
            if (i > 16) continue;
            typeSignature |= kind << (i * 2); 
        }
        return (typeSignature, fullKinds);
    }

    /// <summary>Classify a query parameter's runtime type from the blittable JSON value.
    /// Mirrors the type-branching in <see cref="ResolveParameterValue"/> — long, double,
    /// string, or SliceLong (string exceeding 255 UTF-8 bytes). Used to compute the
    /// TypeSignature cache-key component cheaply from <see cref="PlanTemplate.ParameterSlots"/>
    /// without walking the full clause/execution list.</summary>


    private static ScanValueType ClassifyParamType(BlittableJsonReaderObject queryParams, string name)
    {
        if (queryParams.TryGet(name, out object raw) == false || raw == null)
            return ScanValueType.Slice;
        return raw switch
        {
            long => ScanValueType.Long,
            double => ScanValueType.Double,
            LazyNumberValue lnv => lnv.TryParseLong(out _) ? ScanValueType.Long : ScanValueType.Double,
            string { Length: < 83 } => ScanValueType.Slice, // statically skip Encoding.UTF8.GetByteCount() < 255 here, since we _know_ it's < 255 regardless
            string s when Encoding.UTF8.GetByteCount(s) < byte.MaxValue => ScanValueType.Slice, 
            string => ScanValueType.SliceLong,
            LazyStringValue lsv => lsv.Size > byte.MaxValue ? ScanValueType.SliceLong : ScanValueType.Slice,
            BlittableJsonReaderArray arr => arr.Length > 0 ? ClassifyParamTypeFirstElement(arr[0]) : ScanValueType.Slice,
            _ => ScanValueType.Slice
        };
    }

    /// <summary>Classify the first element of a parameter array (for IN/AllIn parameter bindings).
    /// Arrays are typed by their first element in the same manner as <see cref="ResolveParameterValue"/>.</summary>


    private static ScanValueType ClassifyParamTypeFirstElement(object element)
    {
        return element switch
        {
            long => ScanValueType.Long,
            double => ScanValueType.Double,
            LazyNumberValue lnv => lnv.TryParseLong(out _) ? ScanValueType.Long : ScanValueType.Double,
            _ => ScanValueType.Slice
        };
    }


    private static (PlanOp[] Ops, int RequiredBitmaps, int[] InRangeCounts) EmitPlan(bool isOr, List<ClauseExecution> executions)
    {
        if (executions.Count is 0)
            return (BuildAllEntriesPlan(), 2, null);
        
        // Empty IN in AND → zero results. The reserved EmptyInOrdering cache key ensures this caches separately from real plans.
        if (isOr is false && HasEmptyIn(executions))
            return ([], 2, null);

        int write = 0;
        for (int i = 0; i < executions.Count; i++)
        {
            if (IsEmptyIn(executions[i]))
                continue;

            executions[write++] = executions[i];
        }
        return isOr ? EmitOrPlan(executions) : EmitAndPlan(executions);
    }

    /// <summary>Emit the PlanOp sequence for an OR chain. All clauses are merged into
    /// slot 0: the first via Fill, the rest via OrInto. Groups recurse through
    /// <see cref="EmitClauseInto"/>, allocating scratch slots on demand. SkipEarlyExit
    /// is forced on every AND-step because remaining OR terms may still match.
    /// Returns RequiredBitmaps = max(2, deepest scratch slot used + 1).</summary>


    private static (PlanOp[] Ops, int RequiredBitmaps, int[] InRangeCounts) EmitOrPlan(List<ClauseExecution> executions)
    {
        List<PlanOp> ops = [];
        List<int> rangeCounts = [];
        int matchIndex = 0;
        int nextScratch = 2;
        int maxScratchUsed = 1;

        for (int i = 0; i < executions.Count; i++)
        {
            var exec = executions[i];
            EmitClauseInto(exec,
                i == 0 ? MergeKind.Fill : MergeKind.OrInto,
                exec.Cardinality, suppressEarlyExit: true,
                ref matchIndex, ref nextScratch, ref maxScratchUsed,
                ops, rangeCounts);
        }

        ops.Add(new PlanOp { Kind = PlanOpKind.IterateInto });

        return (ops.ToArray(), Math.Max(2, maxScratchUsed + 1), rangeCounts.Count > 0 ? rangeCounts.ToArray() : null);
    }

    /// <summary>Emit the PlanOp sequence for an AND chain. Single-clause Equals/NotEquals
    /// retain their specialised plans (DirectIterate, FillAllEntries+AndNot). Otherwise
    /// the first non-negated clause seeds slot 0 (Fill) and each subsequent clause is
    /// merged via AndInto or AndNotInto through <see cref="EmitClauseInto"/>. When every
    /// clause is negated we seed with FillAllEntries instead and AndNot all of them.
    /// CheckAndMaybeEntryScan is emitted before each iteration when remaining clauses
    /// are scan-eligible; CheckEmpty follows each non-negated step.</summary>


    private static (PlanOp[] Ops, int RequiredBitmaps, int[] InRangeCounts) EmitAndPlan(List<ClauseExecution> executions)
    {
        List<PlanOp> ops = [];
        List<int> rangeCounts = [];

        var e0 = executions[0];
        switch (executions.Count)
        {
            case 1 when e0.ClauseType == ClauseType.Equals && e0.IsNegated is false:
                ops.Add(new PlanOp
                {
                    Kind = PlanOpKind.DirectIterate,
                    ParamIndex = 0,
                    EstimatedCardinality = e0.Cardinality,
                    Dispatch = GetDispatch(e0.Clause)
                });
                return (ops.ToArray(), 2, null);
            case 1 when e0.ClauseType == ClauseType.NotEquals
                        || (e0.ClauseType == ClauseType.Equals && e0.IsNegated):
                ops.Add(new PlanOp
                {
                    Kind = PlanOpKind.FillAllEntries,
                    EstimatedCardinality = long.MaxValue
                });
                ops.Add(new PlanOp
                {
                    Kind = PlanOpKind.AndNotWithPostings,
                    EstimatedCardinality = e0.Cardinality,
                    Dispatch = GetDispatch(e0.Clause)
                });
                ops.Add(new PlanOp { Kind = PlanOpKind.IterateInto });

                // Mark clause as negated so ResolveMatches/ResolveTermSources
                // produce [AllEntries, TermMatch].
                if (!e0.IsNegated)
                {
                    e0.IsNegated = true;
                }

                return (ops.ToArray(), 2, null);
        }

        int matchIndex = 0;
        int nextScratch = 2;
        int maxScratchUsed = 1;

        // AND chain: Fill the smallest non-negated, then AndWith/AndNotWith the rest.
        // If the first clause is negated (cardinality sort puts negated clauses last,
        // so first-negated ⇒ all-negated) we seed with FillAllEntries instead and
        // AndNot every clause. FillAllEntries calls indexSearcher.AllEntries() directly,
        // avoiding the structural-vs-runtime slot-index mismatch that bites IN with a
        // parameter-bound array of different length.
        bool firstIsNegated = e0.IsNegated || e0.ClauseType == ClauseType.NotEquals;
        int startIndex;

        if (firstIsNegated)
        {
            ops.Add(new PlanOp { Kind = PlanOpKind.FillAllEntries, EstimatedCardinality = long.MaxValue });
            startIndex = 0;
        }
        else
        {
            EmitClauseInto(e0, MergeKind.Fill, e0.Cardinality, suppressEarlyExit: false,
                ref matchIndex, ref nextScratch, ref maxScratchUsed, ops, rangeCounts);
            startIndex = 1;
        }

        // Precheck: can every remaining clause be evaluated by an entry-scan predicate?
        bool allScanEligible = AreAllScanEligible(executions, startIndex);

        for (int i = startIndex; i < executions.Count; i++)
        {
            // CheckAndMaybeEntryScan emits a runtime branch into the entry-scan
            // fallback when bitmap[0] is small relative to remaining IQueryMatch
            // counts. Only safe to emit when AreAllScanEligible reports every
            // remaining clause has a scan predicate.
            if (allScanEligible)
            {
                ops.Add(new PlanOp
                {
                    Kind = PlanOpKind.CheckAndMaybeEntryScan,
                    ParamIndex = matchIndex
                });
            }

            var execI = executions[i];
            bool stepNegated = execI.IsNegated || execI.ClauseType == ClauseType.NotEquals;
            MergeKind merge = stepNegated ? MergeKind.AndNotInto : MergeKind.AndInto;

            EmitClauseInto(execI, merge, execI.Cardinality, suppressEarlyExit: false,
                ref matchIndex, ref nextScratch, ref maxScratchUsed, ops, rangeCounts);

            // CheckEmpty: short-circuit when slot 0 became empty after a positive
            // intersection. Negated steps don't justify the check — they remove
            // entries, so an empty result still represents a valid finished plan.
            if (!stepNegated)
            {
                ops.Add(new PlanOp { Kind = PlanOpKind.CheckEmpty, BitmapLocal = 0 });
            }
        }

        ops.Add(new PlanOp { Kind = PlanOpKind.IterateInto });

        return (ops.ToArray(), Math.Max(2, maxScratchUsed + 1), rangeCounts.Count > 0 ? rangeCounts.ToArray() : null);
    }

    /// <summary>True when all clauses are negated (1+ clauses, first is negated after
    /// cardinality sort — since negated sort last, if the first is negated, all are).
    /// The single-clause NotEquals path in <see cref="EmitAndPlan"/> sets <c>IsNegated = true</c>
    /// before this is called, so the slot layout is <c>[AllEntries, TermMatch]</c>.</summary>


    private static void EmitClauseInto(
        ClauseExecution exec,
        MergeKind merge, long cardinality,
        bool suppressEarlyExit,
        ref int matchIndex, ref int nextScratch, ref int maxScratchUsed,
        List<PlanOp> ops, List<int> rangeCounts)
    {
        // Negated leaf of an OR chain. Build the complement at IL time via FillAllEntries +
        // AndNot of the positive form (single term, IN union, or AllIn intersection). The slot
        // footprint follows the POSITIVE form's layout — CountClauseLeaves and
        // ResolveClauseLeavesInto agree. Cancellation and timing come for free from the per-term
        // cursor machinery.
        //
        // Boost on a negated leaf is intentionally a no-op (matches Lucene): boosting is
        // scoring for a match, and a negation produces a complement, not a match — there's
        // nothing to score. The BoostFactor on such a clause is silently ignored.
        if (exec.Clause.IsOrChainNotEquals)
        {
            EmitNegatedLeafInto(exec, merge, cardinality,
                ref matchIndex, ref nextScratch, ref maxScratchUsed, ops, rangeCounts);
            return;
        }

        if (TryGetGroupFanOut(exec.Clause, exec, out _, out var subExecs))
        {
            EmitGroupInto(exec, subExecs, merge, suppressEarlyExit,
                ref matchIndex, ref nextScratch, ref maxScratchUsed, ops, rangeCounts);
            return;
        }

        if (exec.ClauseType is ClauseType.In)
        {
            EmitInLeaf(exec, cardinality, merge, ref matchIndex, ref maxScratchUsed, ops, rangeCounts);
            return;
        }

        if (exec.ClauseType is ClauseType.AllIn)
        {
            EmitAllInLeaf(exec, cardinality, merge, suppressEarlyExit,
                ref matchIndex, ref nextScratch, ref maxScratchUsed, ops, rangeCounts);
            return;
        }

        EmitLeafMergeOp(merge, matchIndex, cardinality, GetDispatch(exec.Clause), suppressEarlyExit, ops);
        matchIndex++;
    }

    /// <summary>Emit a group (OrGroup or AndGroup) merged into slot 0. For Fill merge,
    /// build directly in slot 0. For non-Fill, save slot 0 to a scratch slot, build
    /// the group fresh in slot 0, then merge with the saved accumulator via the
    /// matching bitmap-pair op.</summary>


    private static void EmitGroupInto(
        ClauseExecution exec,
        List<ClauseExecution> subExecs,
        MergeKind merge, bool suppressEarlyExit,
        ref int matchIndex, ref int nextScratch, ref int maxScratchUsed,
        List<PlanOp> ops, List<int> rangeCounts)
    {
        if (merge == MergeKind.Fill)
        {
            EmitGroupContentsInSlot0(exec, subExecs, suppressEarlyExit,
                ref matchIndex, ref nextScratch, ref maxScratchUsed, ops, rangeCounts);
            return;
        }

        int saveSlot = nextScratch++;
        if (saveSlot > maxScratchUsed) maxScratchUsed = saveSlot;

        ops.Add(new PlanOp { Kind = PlanOpKind.ClearBitmap, BitmapLocal = saveSlot });
        ops.Add(new PlanOp { Kind = PlanOpKind.SwapBitmaps, BitmapLocal = 0, ParamIndex2 = saveSlot });

        // Inside the saved context, AndWithPostings/AndRange MUST NOT early-exit to
        // doneLabel — that would skip the merge-back below and leak the saved value.
        EmitGroupContentsInSlot0(exec, subExecs, suppressEarlyExit: true,
            ref matchIndex, ref nextScratch, ref maxScratchUsed, ops, rangeCounts);

        switch (merge)
        {
            case MergeKind.OrInto:
                ops.Add(new PlanOp { Kind = PlanOpKind.OrBitmaps, BitmapLocal = 0, ParamIndex2 = saveSlot });
                break;
            case MergeKind.AndInto:
                ops.Add(new PlanOp { Kind = PlanOpKind.AndBitmaps, BitmapLocal = 0, ParamIndex2 = saveSlot });
                break;
            case MergeKind.AndNotInto:
                // AndNotBitmaps[0, saveSlot] = slot 0 \ saveSlot. After build,
                // slot 0 = group result, saveSlot = original accumulator. We
                // want (orig \ group) so swap operands back first.
                ops.Add(new PlanOp { Kind = PlanOpKind.SwapBitmaps, BitmapLocal = 0, ParamIndex2 = saveSlot });
                ops.Add(new PlanOp { Kind = PlanOpKind.AndNotBitmaps, BitmapLocal = 0, ParamIndex2 = saveSlot });
                break;
        }

        ops.Add(new PlanOp { Kind = PlanOpKind.ClearBitmap, BitmapLocal = saveSlot });
        nextScratch--;
    }

    /// <summary>Build a group's complete result in slot 0 (slot 0 must be empty/usable
    /// on entry; the caller arranges this either by being the seed Fill or by swapping
    /// the live accumulator out). OrGroup: Fill first sub, OR rest. AndGroup: Fill
    /// first sub (or FillAllEntries if first is negated), AND/ANDNOT rest.</summary>


    private static void EmitGroupContentsInSlot0(
        ClauseExecution exec,
        List<ClauseExecution> subExecs,
        bool suppressEarlyExit,
        ref int matchIndex, ref int nextScratch, ref int maxScratchUsed,
        List<PlanOp> ops, List<int> rangeCounts)
    {
        int subCount = subExecs.Count;
        long subCard = exec.Cardinality / Math.Max(1, subCount);
        bool isOr = exec.ClauseType == ClauseType.OrGroup;

        if (isOr)
        {
            for (int si = 0; si < subCount; si++)
            {
                EmitClauseInto(subExecs[si],
                    si == 0 ? MergeKind.Fill : MergeKind.OrInto,
                    subCard, suppressEarlyExit,
                    ref matchIndex, ref nextScratch, ref maxScratchUsed, ops, rangeCounts);
            }
            return;
        }

        // AndGroup
        bool firstIsNeg = subExecs[0].IsNegated || subExecs[0].ClauseType == ClauseType.NotEquals;
        int start;
        if (firstIsNeg)
        {
            ops.Add(new PlanOp { Kind = PlanOpKind.FillAllEntries, EstimatedCardinality = long.MaxValue });
            start = 0;
        }
        else
        {
            EmitClauseInto(subExecs[0], MergeKind.Fill, subCard, suppressEarlyExit,
                ref matchIndex, ref nextScratch, ref maxScratchUsed, ops, rangeCounts);
            start = 1;
        }
        for (int si = start; si < subCount; si++)
        {
            bool subNeg = subExecs[si].IsNegated || subExecs[si].ClauseType == ClauseType.NotEquals;
            EmitClauseInto(subExecs[si],
                subNeg ? MergeKind.AndNotInto : MergeKind.AndInto,
                subCard, suppressEarlyExit,
                ref matchIndex, ref nextScratch, ref maxScratchUsed, ops, rangeCounts);
        }
    }

    /// <summary>Emit one PlanOp for a simple leaf clause according to <paramref name="merge"/>.
    /// Sets SkipEarlyExit on AndWithPostings when inside a saved-swap context.</summary>


    private static void EmitLeafMergeOp(
        MergeKind merge, int matchIndex, long cardinality, MatchDispatch dispatch,
        bool suppressEarlyExit, List<PlanOp> ops)
    {
        PlanOpKind kind = merge switch
        {
            MergeKind.Fill => PlanOpKind.FillFromPostings,
            MergeKind.OrInto => PlanOpKind.OrWithPostings,
            MergeKind.AndInto => PlanOpKind.AndWithPostings,
            MergeKind.AndNotInto => PlanOpKind.AndNotWithPostings,
            _ => throw new InvalidOperationException($"Unhandled MergeKind: {merge}")
        };
        ops.Add(new PlanOp
        {
            Kind = kind,
            ParamIndex = matchIndex,
            BitmapLocal = 0,
            EstimatedCardinality = cardinality,
            Dispatch = dispatch,
            SkipEarlyExit = kind == PlanOpKind.AndWithPostings && suppressEarlyExit
        });
    }

    /// <summary>IN clause leaf — logically (term0 ∪ term1 ∪ … ∪ termN). For Fill/OrInto
    /// merges, build directly in slot 0 via EmitInOps. For AndInto/AndNotInto, build
    /// the union in slot 1 (slot 1 is freshly cleared first), then merge into slot 0
    /// via AndBitmaps/AndNotBitmaps. OrRange ignores SkipEarlyExit so suppression
    /// doesn't need to propagate here.</summary>


    private static void EmitInLeaf(
        ClauseExecution exec, long cardinality, MergeKind merge,
        ref int matchIndex, ref int maxScratchUsed,
        List<PlanOp> ops, List<int> rangeCounts)
    {
        int inTermCount = exec.InTermCount;
        if (merge is MergeKind.Fill or MergeKind.OrInto)
        {
            EmitInOps(ops, inTermCount, cardinality, bitmapLocal: 0, isSeed: merge == MergeKind.Fill, ref matchIndex, rangeCounts);
            return;
        }

        // AndInto / AndNotInto: union IN terms in slot 1, then merge with slot 0.
        if (1 > maxScratchUsed) maxScratchUsed = 1;
        ops.Add(new PlanOp { Kind = PlanOpKind.ClearBitmap, BitmapLocal = 1 });
        EmitInOps(ops, inTermCount, cardinality, bitmapLocal: 1, isSeed: false, ref matchIndex, rangeCounts);
        ops.Add(new PlanOp
        {
            Kind = merge == MergeKind.AndInto ? PlanOpKind.AndBitmaps : PlanOpKind.AndNotBitmaps,
            BitmapLocal = 0,
            ParamIndex2 = 1
        });
    }

    /// <summary>AllIn clause leaf — logically (term0 ∩ term1 ∩ … ∩ termN). For Fill merge,
    /// build directly in slot 0. For OrInto/AndInto/AndNotInto, save slot 0 to a scratch
    /// slot, build the intersection in slot 0 (Fill + AndRange), then merge back. The
    /// AndRange op honors SkipEarlyExit; in a saved context we must set it so the loop
    /// doesn't jump to doneLabel mid-intersection.</summary>


    private static void EmitAllInLeaf(
        ClauseExecution exec, long cardinality, MergeKind merge,
        bool suppressEarlyExit,
        ref int matchIndex, ref int nextScratch, ref int maxScratchUsed,
        List<PlanOp> ops, List<int> rangeCounts)
    {
        int inTermCount = exec.InTermCount;
        if (merge == MergeKind.Fill)
        {
            EmitAllInOps(ops, inTermCount, cardinality, 0, ref matchIndex, rangeCounts);
            if (suppressEarlyExit)
                SetLastAndRangeSkipEarlyExit(ops);
            return;
        }

        int saveSlot = nextScratch++;
        if (saveSlot > maxScratchUsed) maxScratchUsed = saveSlot;

        ops.Add(new PlanOp { Kind = PlanOpKind.ClearBitmap, BitmapLocal = saveSlot });
        ops.Add(new PlanOp { Kind = PlanOpKind.SwapBitmaps, BitmapLocal = 0, ParamIndex2 = saveSlot });
        EmitAllInOps(ops, inTermCount, cardinality, 0, ref matchIndex, rangeCounts);
        // Inside save-swap: AndRange must not jump to doneLabel — that would skip
        // the merge-back and leak the saved accumulator.
        SetLastAndRangeSkipEarlyExit(ops);

        switch (merge)
        {
            case MergeKind.OrInto:
                ops.Add(new PlanOp { Kind = PlanOpKind.OrBitmaps, BitmapLocal = 0, ParamIndex2 = saveSlot });
                break;
            case MergeKind.AndInto:
                ops.Add(new PlanOp { Kind = PlanOpKind.AndBitmaps, BitmapLocal = 0, ParamIndex2 = saveSlot });
                break;
            case MergeKind.AndNotInto:
                ops.Add(new PlanOp { Kind = PlanOpKind.SwapBitmaps, BitmapLocal = 0, ParamIndex2 = saveSlot });
                ops.Add(new PlanOp { Kind = PlanOpKind.AndNotBitmaps, BitmapLocal = 0, ParamIndex2 = saveSlot });
                break;
        }
        ops.Add(new PlanOp { Kind = PlanOpKind.ClearBitmap, BitmapLocal = saveSlot });
        nextScratch--;
    }

    /// <summary>Set <see cref="PlanOp.SkipEarlyExit"/>=true on the most recent
    /// <see cref="PlanOpKind.AndRange"/> in <paramref name="ops"/>. The op was emitted
    /// by <see cref="EmitAllInOps"/> as the last entry of the AllIn pair; mutating it
    /// here avoids threading the flag through that helper's signature.</summary>


    private static void EmitNegatedLeafInto(
        ClauseExecution exec,
        MergeKind merge, long cardinality,
        ref int matchIndex, ref int nextScratch, ref int maxScratchUsed,
        List<PlanOp> ops, List<int> rangeCounts)
    {
        Debug.Assert(merge is MergeKind.Fill or MergeKind.OrInto,
            $"IsOrChainNotEquals only appears in OR chains; got merge={merge}");

        if (merge == MergeKind.Fill)
        {
            ops.Add(new PlanOp { Kind = PlanOpKind.FillAllEntries, EstimatedCardinality = long.MaxValue });
            EmitComplementBody(exec, cardinality, ref matchIndex, ref maxScratchUsed, ops, rangeCounts);
            return;
        }

        // OrInto: save the accumulator out, build a fresh complement in slot 0, OR back.
        // Mirrors the save-swap pattern in EmitGroupInto / EmitAllInLeaf.
        int saveSlot = nextScratch++;
        if (saveSlot > maxScratchUsed) maxScratchUsed = saveSlot;

        ops.Add(new PlanOp { Kind = PlanOpKind.ClearBitmap, BitmapLocal = saveSlot });
        ops.Add(new PlanOp { Kind = PlanOpKind.SwapBitmaps, BitmapLocal = 0, ParamIndex2 = saveSlot });

        ops.Add(new PlanOp { Kind = PlanOpKind.FillAllEntries, EstimatedCardinality = long.MaxValue });
        EmitComplementBody(exec, cardinality, ref matchIndex, ref maxScratchUsed, ops, rangeCounts);

        ops.Add(new PlanOp { Kind = PlanOpKind.OrBitmaps, BitmapLocal = 0, ParamIndex2 = saveSlot });
        ops.Add(new PlanOp { Kind = PlanOpKind.ClearBitmap, BitmapLocal = saveSlot });
        nextScratch--;
    }

    /// <summary>Turn slot 0 (currently <see cref="PlanOpKind.FillAllEntries"/>) into the
    /// complement of <paramref name="exec"/>'s positive form. IN unions the terms into slot 1
    /// then AndNotBitmaps(0, 1); AllIn intersects into slot 1 then AndNotBitmaps(0, 1).
    /// Scalar / Exists / Range clauses use AndNotWithPostings directly (the
    /// <see cref="PlanOp.Dispatch"/> follows <see cref="GetDispatch"/> for the positive form).
    /// Advances <paramref name="matchIndex"/> past the clause's slot footprint.</summary>


    private static void EmitComplementBody(
        ClauseExecution exec, long cardinality,
        ref int matchIndex, ref int maxScratchUsed,
        List<PlanOp> ops, List<int> rangeCounts)
    {
        if (exec.ClauseType is ClauseType.In)
        {
            if (1 > maxScratchUsed) maxScratchUsed = 1;
            // isSeed:true so FillFromPostings overwrites slot 1 — no ClearBitmap needed.
            EmitInOps(ops, exec.InTermCount, cardinality, bitmapLocal: 1, isSeed: true, ref matchIndex, rangeCounts);
            ops.Add(new PlanOp { Kind = PlanOpKind.AndNotBitmaps, BitmapLocal = 0, ParamIndex2 = 1 });
            return;
        }

        if (exec.ClauseType is ClauseType.AllIn)
        {
            if (1 > maxScratchUsed) maxScratchUsed = 1;
            EmitAllInOps(ops, exec.InTermCount, cardinality, 1, ref matchIndex, rangeCounts);
            // AndRange would early-exit to doneLabel if slot 1 empties mid-intersection,
            // skipping our AndNotBitmaps and the rest of the OR chain. Suppress it.
            SetLastAndRangeSkipEarlyExit(ops);
            ops.Add(new PlanOp { Kind = PlanOpKind.AndNotBitmaps, BitmapLocal = 0, ParamIndex2 = 1 });
            return;
        }

        // Single-term positive form (Equals/NotEquals/Exists/StartsWith/range/...).
        // AndNotWithPostings reads matchIndex per Dispatch and removes those entries.
        ops.Add(new PlanOp
        {
            Kind = PlanOpKind.AndNotWithPostings,
            ParamIndex = matchIndex,
            BitmapLocal = 0,
            EstimatedCardinality = cardinality,
            Dispatch = GetDispatch(exec.Clause)
        });
        matchIndex++;
    }

    /// <summary>Translate sorted clauses into a linear PlanOp[] sequence for IL emission.
    /// Dispatches to <see cref="EmitOrPlan"/> or <see cref="EmitAndPlan"/> after shared
    /// empty-IN handling.</summary>


    private static void SetLastAndRangeSkipEarlyExit(List<PlanOp> ops)
    {
        for (int i = ops.Count - 1; i >= 0; i--)
        {
            if (ops[i].Kind == PlanOpKind.AndRange)
            {
                var op = ops[i];
                op.SkipEarlyExit = true;
                ops[i] = op;
                return;
            }
        }
    }

    /// <summary>IsOrChainNotEquals leaf — fold AllEntries ANDNOT(positive form) into slot 0
    /// at IL time. The per-term loops route through the cursor machinery so cancellation
    /// and timing are covered. Only invoked with merge ∈ {Fill, OrInto} because
    /// IsOrChainNotEquals only appears as an OR-chain leaf. Boost on such a clause is
    /// silently ignored (matches Lucene — negation produces a complement, not a match to score).
    /// Fill: build the complement directly in slot 0 (FillAllEntries + complement body).
    /// OrInto: save slot 0 to a scratch, rebuild complement in slot 0, OR back.</summary>


    private static void EmitInOps(List<PlanOp> ops, int inTermCount, long cardinality, int bitmapLocal, bool isSeed, ref int matchIndex, List<int> rangeCounts)
    {
        int totalSlots = inTermCount + 1; // inTermCount non-null terms + 1 null-term slot
        // Range iterates over the slots AFTER slot 0 (which Fill handles). When the parameter
        // list has no null, the trailing null slot is Empty — ORing with Empty is a no-op, so
        // we can safely include it (rangeCount = totalSlots - 1). When the list HAS a null
        // term, that slot is non-empty and we want to OR it in. Both cases use the same range.
        int rangeIdx = rangeCounts.Count;
        rangeCounts.Add(totalSlots - 1);

        ops.Add(new PlanOp
        {
            Kind = isSeed ? PlanOpKind.FillFromPostings : PlanOpKind.OrWithPostings,
            ParamIndex = matchIndex,
            BitmapLocal = bitmapLocal,
            EstimatedCardinality = Math.Max(1, cardinality / totalSlots),
            Dispatch = MatchDispatch.PostingList
        });
        ops.Add(new PlanOp
        {
            Kind = PlanOpKind.OrRange,
            ParamIndex = matchIndex + 1,
            ParamIndex2 = rangeIdx,
            BitmapLocal = bitmapLocal,
            EstimatedCardinality = cardinality,
            Dispatch = MatchDispatch.PostingList
        });
        matchIndex += totalSlots;
    }

    /// <summary>Emit ops for an AllIn clause (as a seed): Fill slot 0 + AndRange for the rest.
    /// Same fixed shape rationale as <see cref="EmitInOps"/> — the count of remaining
    /// terms lives in <c>ctx.InRangeCounts</c> rather than the op shape itself.
    /// <paramref name="inTermCount"/> must match <c>exec.InTermCount</c> so the slot
    /// layout agrees with the resolver walk.</summary>


    private static void EmitAllInOps(List<PlanOp> ops, int inTermCount, long cardinality, int bitmapLocal, ref int matchIndex, List<int> rangeCounts)
    {
        int totalSlots = inTermCount + 1; // inTermCount non-null terms + 1 null-term slot
        // Fill consumes slot 0, AndRange iterates the rest. The range count
        // covers all slots after slot 0 (including the null-term slot).
        int rangeCount = totalSlots - 1;
        int rangeIdx = rangeCounts.Count;
        rangeCounts.Add(rangeCount);

        ops.Add(new PlanOp
        {
            Kind = PlanOpKind.FillFromPostings,
            ParamIndex = matchIndex,
            BitmapLocal = bitmapLocal,
            EstimatedCardinality = Math.Max(1, cardinality / totalSlots),
            Dispatch = MatchDispatch.PostingList
        });
        ops.Add(new PlanOp
        {
            Kind = PlanOpKind.AndRange,
            ParamIndex = matchIndex + 1,
            ParamIndex2 = rangeIdx,
            BitmapLocal = bitmapLocal,
            EstimatedCardinality = cardinality,
            Dispatch = MatchDispatch.PostingList
        });
        matchIndex += totalSlots;
    }


    private static PlanOp[] BuildAllEntriesPlan()
    {
        // No bitmap needed — AllEntries already implements IQueryMatch.Fill(),
        // so we iterate it directly without materializing into a bitmap first.
        return [new PlanOp { Kind = PlanOpKind.DirectIterate, ParamIndex = 0 }];
    }

    /// <summary>Cardinality used for cost estimation; <c>NumberOfEntries</c> is the
    /// fallback when a clause hasn't computed a cardinality yet (e.g. multi-term or
    /// regex). Callers treat the fallback as "could match everything."</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]


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
        var ctx = new InstCtx(compiledPlan, exec, orderByFields, planParams, builderParameters);

        if (compiledPlan.Strategy == ExecutionStrategy.NotEvaluated)
            SelectExecutionStrategy(ref ctx);

        switch (compiledPlan.Strategy)
        {
            // ── Fast path: cached strategy, dispatch directly to Construct* ──
            case ExecutionStrategy.CompoundExact:
                innerMatch = ConstructCompoundExact(ref ctx);
                if (innerMatch is null) goto default;
                return innerMatch;
            case ExecutionStrategy.CompoundField when orderByFields != null:
                // orderByFields can be null on a per-execution PageSize==0 reuse of a cached
                // CompoundField plan — PageSize is not part of the plan cache key.
                innerMatch = ConstructCompoundField(ref ctx, FindCompoundFieldField2Range(ref ctx), entriesToScan: 0, bitmapCost: 0);
                if (innerMatch is null) goto default;
                return OrderBy(builderParameters, innerMatch, orderByFields, hasEmptySorts);
            case ExecutionStrategy.DirectScan when orderByFields is { Length: <= 2 }:
                // orderByFields can be null on a per-execution PageSize==0 reuse of a cached
                // DirectScan plan — PageSize is not part of the plan cache key.
                var execs = exec.Executions;
                bool isFullScan = execs is not { Count: > 0 };
                if (isFullScan ||
                    exec.Plan.SortDrivingClauseIndex >= 0 && exec.Executions[exec.Plan.SortDrivingClauseIndex].PackedParamValue.IsNone is false)
                {
                    bool hasTieBreak = orderByFields.Length == 2;
                    innerMatch = ConstructDirectScan(ref ctx, exec.Plan.SortDrivingClauseIndex, isFullScan, hasTieBreak, entriesToScan: 0, bitmapCost: 0);
                    if (innerMatch is not null) return innerMatch;
                }
                goto default;
            case ExecutionStrategy.BitmapSort:
            default: // may either be the selected strategy, or a one-off (because of bad parameters preventing a faster strategy)
                innerMatch = InstantiateBitmapPipeline(ctx.Plan, ctx.Exec, ctx.PlanParams, ctx.BuilderParams, walkerCtx, highlightingTerms, wantTimings, token);
                if (ctx.OrderByFields == null) return innerMatch;
                if (innerMatch is CompiledQueryMatch seekMatch)
                    TrySetSortSeekHint(seekMatch, ctx.Exec, ctx.OrderByFields);
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
                    ctx.Plan.DecisionTrail.Record("CompoundField", true, "compound tree scan with ORDER BY");
                    return;
                }
                ctx.Plan.DecisionTrail.Record("CompoundField", false, ctx.RejectReason ?? "rejected");

                if (TryCreateSimpleFieldDirectScan(ref ctx, out ctx.RejectReason))
                {
                    ctx.Plan.Strategy = ExecutionStrategy.DirectScan;
                    ctx.Plan.DecisionTrail.Record("DirectScan", true, "direct tree scan on sort field");
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
            return InstantiateAllEntriesPostFilter(exec, builderParameters, walkerCtx);

        var resolvedMatches = ResolveMatches(exec, walkerCtx);
        var termSources = ResolveTermSources(exec, walkerCtx);
        var termsProviders = ResolveTermsProviders(exec, walkerCtx);
        ExtractScanParameters(exec, indexSearcher,
            out var longParams, out var doubleParams, out var sliceParams, out var fieldRootPages);

        if (highlightingTerms != null)
            PopulateHighlightingTerms(exec, highlightingTerms, planParams.Metadata);

        var compiledMatch = new CompiledQueryMatch(
            compiledPlan, compiledPlan.RequiredBitmaps, compiledPlan.OpCount, resolvedMatches, termSources, termsProviders,
            indexSearcher, planParams.Allocator, wantTimings, token)
        {
            InRangeCounts = exec.InRangeCounts,
            ResidualLongParams = longParams,
            ResidualDoubleParams = doubleParams,
            ResidualSliceParams = sliceParams,
            ResidualFieldRootPages = fieldRootPages
        };
        IQueryMatch result = compiledMatch;

        // Spatial post-filter phase: AND each spatial match with the candidate bitmap.
        if (exec.SpatialFilters is { Length: > 0 })
        {
            var spatialFilters = new IQueryMatch[exec.SpatialFilters.Length];
            for (int sf = 0; sf < exec.SpatialFilters.Length; sf++)
            {
                spatialFilters[sf] = resolvedMatches[exec.SpatialFilters[sf].MatchIndex];
            }

            result = new PostFilterMatch(result, spatialFilters);
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


    private static IQueryMatch InstantiateAllEntriesPostFilter(QueryExecution exec, QueryBuilderParameters builderParameters, ResolutionContext walkerCtx)
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
            result = new PostFilterMatch(primary, rest);

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

        if (walkerCtx.Clauses.Count == 1)
            return ResolveClause(subExecs[0], subPlan, walkerCtx);

        // Multiple clauses (AND chain) — resolve each and AND them via bitmap.
        var bitmap = new BitmapMatch(indexSearcher.Allocator);
        if (walkerCtx.Clauses.Count == 0)
            return bitmap;
        
        QueryPrimitives.OrWithMatch(ResolveClause(subExecs[0], subPlan, walkerCtx), ref bitmap.BitmapState);

        var temp = new RoaringBitmap(indexSearcher.Allocator);
        try
        {
            for (int i = 1; i < walkerCtx.Clauses.Count; i++)
            {
                QueryPrimitives.AndWithMatch(ResolveClause(subExecs[i], subPlan, walkerCtx), ref bitmap.BitmapState, ref temp);
            }
        }
        finally
        {
            temp.Dispose();
        }

        return bitmap;
    }

    /// <summary>
    /// Used to get the active list of clauses and a mask on when flags to tell between the different plans based on the when conditions 
    /// </summary>


    private static bool TryCreateCompoundExactMatch(
        ref InstCtx ctx, out string rejectReason)
    {
        if (ctx.PlanParams.Index is null || ctx.Exec is not {
                Executions: { Count: >= 2 } executions,
                Plan: {
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

        byte[] field1Bytes = BuildCompoundFieldBytes(firstField, firstExec, indexSearcher, ctx.Exec);
        if (field1Bytes == null || field1Bytes.Length > byte.MaxValue) return null;

        byte[] field2Bytes = BuildCompoundFieldBytes(secondField, secondExec, indexSearcher, ctx.Exec);
        if (field2Bytes == null) return null;

        int totalLen = field1Bytes.Length + field2Bytes.Length + 1;
        if (totalLen > Constants.Terms.MaxLength) return null;

        var compositeKey = new byte[totalLen];
        field1Bytes.CopyTo(compositeKey, 0);
        field2Bytes.CopyTo(compositeKey.AsSpan(field1Bytes.Length));
        compositeKey[^1] = (byte)field1Bytes.Length;

        var compoundFieldName = $"compound({firstField},{secondField})";
        var compoundFieldMeta = indexSearcher.FieldMetadataBuilder(compoundFieldName, hasBoost: false);
        Slice.From(ctx.PlanParams.Allocator, compositeKey, out var keySlice);

        return indexSearcher.TermQuery(compoundFieldMeta, keySlice);
    }


    private static byte[] BuildCompoundFieldBytes(string fieldName, ClauseExecution exec,
        IndexSearcher indexSearcher, QueryExecution queryExec)
    {
        var p = exec.PackedParamValue;
        if (p.ValueType == PackedParam.TypeString)
        {
            var meta = indexSearcher.FieldMetadataBuilder(fieldName, hasBoost: false);
            var analyzed = indexSearcher.EncodeAndApplyAnalyzer(meta, queryExec.StringValues[p.Param1]);
            if (analyzed.Size > byte.MaxValue) return null;
            var bytes = new byte[analyzed.Size];
            analyzed.CopyTo(bytes);
            return bytes;
        }

        if (p.ValueType == PackedParam.TypeLong)
        {
            var bytes = new byte[sizeof(long)];
            BinaryPrimitives.WriteInt64BigEndian(
                bytes, Bits.SwapBytes(queryExec.LongValues[p.Param1]));
            return bytes;
        }

        if (p.ValueType == PackedParam.TypeDouble)
        {
            var bytes = new byte[sizeof(long)];
            long sortable = Bits.DoubleToSortableLong(queryExec.DoubleValues[p.Param1]);
            BinaryPrimitives.WriteInt64BigEndian(
                bytes, Bits.SwapBytes(sortable));
            return bytes;
        }

        return null;
    }

    /// <summary>compound(field1, field2) exists in the index, and any residual clauses are
    /// entry-scan eligible.
    /// Returns a DirectScanMatch wrapping a compound tree StartsWith with optional
    /// residual predicate checking.</summary>


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

        var indexSearcher = ctx.PlanParams.IndexSearcher;

        var drivingExec = execs[ctx.Exec.Plan.CompoundFieldDrivingClause];
        if (drivingExec.PackedParamValue.IsNone)
        {
            rejectReason = "driving clause has no packed param value";
            return false;
        }

        // Find optional field2 range narrowing clause (structural — same for all
        // executions of this template).
        int field2RangeIdx = FindCompoundFieldField2Range(ref ctx);

        // Residual scannability + cost check
        long bitmapCost = 0;
        int residualCount = 0;
        for (int i = 0; i < execs.Count; i++)
        {
            bitmapCost += EffectiveCardinality(execs[i], indexSearcher);
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
            residualCount++;
        }

        long drivingCardinality = EffectiveCardinality(drivingExec, indexSearcher);
        long entriesToScan = residualCount > 0
            ? AdjustEntriesToScanByMinResidual(execs, ctx.Exec.Plan.CompoundFieldDrivingClause, drivingCardinality, indexSearcher)
            : drivingCardinality;

        if (IsDirectScanCostEffective(entriesToScan, bitmapCost) == false)
        {
            rejectReason = "cost check failed (bitmap is cheaper), non-scannable residual, or prefix too long";

            return false;
        }

        rejectReason = null;
        return true;
    }

    /// <summary>Locate an optional GT/GTE/LT/LTE/Between clause on the sort field
    /// that can narrow the compound prefix scan. Structural — same for all executions
    /// of a given template, but cheap enough to recompute on each Construct call
    /// rather than threading another field through QueryExecution.</summary>


    private static int FindCompoundFieldField2Range(ref InstCtx ctx)
    {
        var executions = ctx.Exec.Executions;
        int drivingClauseIdx = ctx.Plan.CompoundFieldDrivingClause;
        var sortFieldName = ctx.Plan.Template.CompoundFieldSortName;
        
        for (int i = 0; i < executions.Count; i++)
        {
            if (i == drivingClauseIdx) continue;
            var cl = executions[i].Clause;
            if (cl.FieldName != sortFieldName) continue;
            if (cl.ClauseType is ClauseType.GreaterThan or ClauseType.GreaterThanOrEqual
                or ClauseType.LessThan or ClauseType.LessThanOrEqual or ClauseType.Between)
                return i;
        }

        return -1;
    }

    /// <summary>Phase 5 bake: construction-only path for the CompoundField hint.
    /// Caller has either run TryCreateCompoundFieldMatch's discovery (compile-miss)
    /// or read the cached ExecutionStrategy and is dispatching directly.
    /// Returns null on per-execution failure (e.g. analyzed prefix exceeds 255 bytes);
    /// caller falls back to the next optimization or bitmap.</summary>


    private static IQueryMatch ConstructCompoundField(
        ref InstCtx ctx,
        int field2RangeIdx, long entriesToScan, long bitmapCost)
    {
        var execs = ctx.Exec.Executions;
        var indexSearcher = ctx.PlanParams.IndexSearcher;
        var allocator = ctx.PlanParams.Allocator;
        int drivingClauseIdx = ctx.Exec.Plan.CompoundFieldDrivingClause;
        string sortFieldName = ctx.Exec.Plan.Template.CompoundFieldSortName;

        var drivingClause = execs[drivingClauseIdx].Clause;
        var drivingExec = execs[drivingClauseIdx];
        var packed = drivingExec.PackedParamValue;

        // Rebuild residual predicates (Construct rebuilds; the structural shape is
        // identical to what discovery just walked, so List growth is bounded).
        var residualPreds = new List<ScanPredicateInfo>();
        int rLongIdx = 0, rDoubleIdx = 0, rSliceIdx = 0;
        for (int i = 0; i < execs.Count; i++)
        {
            if (i == drivingClauseIdx || i == field2RangeIdx)
                continue;
            var pred = BuildScanPredicateInfo(execs[i], ref rLongIdx, ref rDoubleIdx, ref rSliceIdx);
            if (pred == null)
                return null;
            residualPreds.Add(pred.Value);
        }

        string field1Name = drivingClause.FieldName;
        var compoundFieldName = $"compound({field1Name},{sortFieldName})";
        var compoundFieldMeta = indexSearcher.FieldMetadataBuilder(compoundFieldName, hasBoost: false);

        // Build the prefix bytes for field1's value.
        // String: analyzed via field1's analyzer. Numeric: Bits.SwapBytes big-endian encoding.
        Slice analyzedPrefix;
        string field1ValueStr;
        switch (packed.ValueType)
        {
            case PackedParam.TypeString:
            {
                field1ValueStr = ctx.Exec.StringValues[packed.Param1];
                var field1Meta = QueryBuilderHelper.GetFieldMetadata(in ctx.BuilderParams, field1Name, hasBoost: false);
                analyzedPrefix = indexSearcher.EncodeAndApplyAnalyzer(field1Meta, field1ValueStr);
                break;
            }
            case PackedParam.TypeLong:
            {
                long longVal = ctx.Exec.LongValues[packed.Param1];
                field1ValueStr = longVal.ToString();
                var bytes = new byte[sizeof(long)];
                BinaryPrimitives.WriteInt64BigEndian(bytes, Bits.SwapBytes(longVal));
                Slice.From(allocator, bytes, out analyzedPrefix);
                break;
            }
            case PackedParam.TypeDouble:
            {
                double dblVal = ctx.Exec.DoubleValues[packed.Param1];
                field1ValueStr = dblVal.ToString(CultureInfo.InvariantCulture);
                long sortable = Bits.DoubleToSortableLong(dblVal);
                var bytes = new byte[sizeof(long)];
                BinaryPrimitives.WriteInt64BigEndian(bytes, Bits.SwapBytes(sortable));
                Slice.From(allocator, bytes, out analyzedPrefix);
                break;
            }
            default:
                return null;
        }

        // Compound key trailing byte stores field1 length as a single byte.
        // If the analyzed prefix exceeds 255 bytes, the compound key format can't represent it.
        // Fall back to the bitmap pipeline which queries individual fields normally.
        if (analyzedPrefix.Size > byte.MaxValue)
            return null;

        IQueryMatch drivingMatch = null;
        if (field2RangeIdx >= 0)
        {
            // Compound range: build composite low/high keys incorporating the field2 bound
            var field2Exec = execs[field2RangeIdx];
            var field2Clause = field2Exec.Clause;
            var field2Packed = field2Exec.PackedParamValue;

            if (!field2Packed.IsNone)
            {
                // Encode field2 bound value into bytes (same encoding as indexing).
                // Long/Double: Bits.SwapBytes big-endian. String: analyze with field2's analyzer.
                byte[] field2Bytes = null;
                byte[] field2HighBytes = null;
                bool usePrefix = false;

                if (field2Packed.ValueType is PackedParam.TypeLong or PackedParam.TypeDouble)
                {
                    field2Bytes = EncodeNumericBoundBigEndian(ctx.Exec, field2Packed.ValueType, field2Packed.Param1);
                    if (field2Clause.ClauseType == ClauseType.Between)
                        field2HighBytes = EncodeNumericBoundBigEndian(ctx.Exec, field2Packed.ValueType, field2Packed.Param2);
                }
                else if (field2Packed.ValueType == PackedParam.TypeString)
                {
                    // Analyze field2's value with the sort field's analyzer (same as indexing)
                    var field2Meta = QueryBuilderHelper.GetFieldMetadata(in ctx.BuilderParams, sortFieldName, hasBoost: false);
                    var analyzed = indexSearcher.EncodeAndApplyAnalyzer(field2Meta, ctx.Exec.StringValues[field2Packed.Param1]);
                    if (analyzed.Size > byte.MaxValue)
                        usePrefix = true;
                    else
                    {
                        field2Bytes = new byte[analyzed.Size];
                        analyzed.CopyTo(field2Bytes);
                        if (field2Clause.ClauseType == ClauseType.Between)
                        {
                            var analyzedHigh = indexSearcher.EncodeAndApplyAnalyzer(field2Meta, ctx.Exec.StringValues[field2Packed.Param2]);
                            if (analyzedHigh.Size > byte.MaxValue)
                                usePrefix = true;
                            else
                            {
                                field2HighBytes = new byte[analyzedHigh.Size];
                                analyzedHigh.CopyTo(field2HighBytes);
                            }
                        }
                    }
                }
                else
                {
                    usePrefix = true;
                }

                if (usePrefix)
                {
                    // Field2 value too long or unsupported type — fall back to prefix-only
                    drivingMatch = indexSearcher.StartWithQuery(compoundFieldMeta, analyzedPrefix,
                        isNegated: false, forward: ctx.OrderByFields[0].Ascending,
                        validatePostfixLen: true);
                }
                else
                {

                    // Build low- and high-composite keys
                    int prefixLen = analyzedPrefix.Size;
                    int field2Len = field2Bytes.Length;
                    int keyLen = prefixLen + field2Len + 1; // +1 for field1 length byte
                    int highField2Len = field2HighBytes?.Length ?? field2Len;
                    int highKeyLen = prefixLen + highField2Len + 1;

                    // Check total key length against max
                    if (keyLen > Constants.Terms.MaxLength || highKeyLen > Constants.Terms.MaxLength)
                    {
                        drivingMatch = indexSearcher.StartWithQuery(compoundFieldMeta, analyzedPrefix,
                            isNegated: false, forward: ctx.OrderByFields[0].Ascending, validatePostfixLen: true);
                        goto DrivingMatchReady;
                    }

                    byte[] lowKeyBytes = new byte[keyLen];
                    byte[] highKeyBytes = new byte[highKeyLen];

                    analyzedPrefix.CopyTo(lowKeyBytes);
                    analyzedPrefix.CopyTo(highKeyBytes);

                    // Low key: either the field2 bound or min value (0x00s)
                    // High key: either the field2 bound or max value (0xFFs)
                    bool isGt = field2Clause.ClauseType is ClauseType.GreaterThan or ClauseType.GreaterThanOrEqual;
                    if (isGt || field2Clause.ClauseType == ClauseType.Between)
                    {
                        field2Bytes.CopyTo(lowKeyBytes.AsSpan(prefixLen));
                    }
                    // else: low = field1 prefix + 0x00s (already zeroed)

                    if (field2Clause.ClauseType is ClauseType.LessThan or ClauseType.LessThanOrEqual || field2Clause.ClauseType == ClauseType.Between)
                    {
                        var highBytes = field2HighBytes ?? field2Bytes;
                        highBytes.CopyTo(highKeyBytes.AsSpan(prefixLen));
                    }
                    else
                    {
                        // GT/GTE: high = field1 prefix + 0xFF...FF
                        highKeyBytes.AsSpan(prefixLen, highField2Len).Fill(0xFF);
                    }

                    // Trailing field1 length byte
                    lowKeyBytes[^1] = (byte)prefixLen;
                    highKeyBytes[^1] = (byte)prefixLen;

                    Slice.From(allocator, lowKeyBytes, out var lowSlice);
                    Slice.From(allocator, highKeyBytes, out var highSlice);

                    drivingMatch = indexSearcher.RangeBuilder<Range.Inclusive, Range.Inclusive>(
                        compoundFieldMeta, lowSlice, highSlice,
                        forward: ctx.OrderByFields[0].Ascending, CancellationToken.None);
                }
            }
        }
        else
        {
            // Pure prefix scan (no field2 constraint)
            drivingMatch = indexSearcher.StartWithQuery(compoundFieldMeta, analyzedPrefix,
                isNegated: false, forward: ctx.OrderByFields[0].Ascending,
                validatePostfixLen: true);
        }

        DrivingMatchReady:

        // Extract scan parameters for residual predicates
        ScanPredicateInfo[] residualArray = residualPreds.Count > 0 ? residualPreds.ToArray() : null;
        BuildResidualScanParams(ctx.Exec, indexSearcher, allocator, residualArray,
            drivingClauseIdx, field2RangeIdx,
            out var longParams, out var doubleParams, out var sliceParams, out var fieldRootPages);

        var directScan = BuildDirectScan(
            indexSearcher, drivingMatch, longParams, doubleParams, sliceParams, fieldRootPages,
            ctx.Plan.CompiledEntryPredicate, residualArray);
        directScan.DrivingTreeName = compoundFieldName;
        directScan.DrivingClause = $"{field1Name} = '{field1ValueStr}'";
        directScan.SeekBound = $"'{field1ValueStr}' (prefix, validatePostfixLen)";
        directScan.Direction = ctx.OrderByFields[0].Ascending ? "Forward" : "Backward";
        directScan.ResidualDescription = residualArray != null
            ? string.Join(", ", residualPreds.ConvertAll(p => $"{p.FieldName} {p.CompareOp}"))
            : null;
        directScan.Reason = $"entries_to_scan({entriesToScan}) × {QueryPrimitives.EntryScanCostMultiplier} < bitmap_cost({bitmapCost})";

        return directScan;
    }

    /// <summary>Check if a range clause on the ORDER BY field can be served by a direct</summary>


    private static bool TryCreateSimpleFieldDirectScan(
        ref InstCtx ctx, out string rejectReason)
    {
        rejectReason = null;
        bool result = TryCreateSimpleFieldDirectScan(ref ctx, out IQueryMatch directMatch);
        if (!result)
        {
            if (ctx.OrderByFields == null || ctx.OrderByFields.Length == 0)
                rejectReason = "no ORDER BY fields";
            else if (ctx.OrderByFields.Length > 2)
                rejectReason = "ORDER BY has too many fields (max 2 for direct scan)";
            else if (ctx.OrderByFields.Length == 2 && ctx.OrderByFields[1].FieldType is not (MatchCompareFieldType.Integer or MatchCompareFieldType.Floating))
                rejectReason = "tie-break field type is not numeric (must be Integer or Floating)";
            else if (ctx.Exec.Executions is { Count: > 0 } && ctx.Exec.Plan.SortDrivingClauseIndex < 0)
                rejectReason = "no range/equals clause on sort field";
            else
                rejectReason = "cost check failed (bitmap is cheaper), non-scannable residual, or cardinality too high for tie-break";
        }

        return result;
    }

    /// <summary>tree scan instead of the bitmap pipeline. The range query already walks the tree
    /// in sort order, so no SortingMatch wrapper is needed.</summary>


    private static bool TryCreateSimpleFieldDirectScan(ref InstCtx ctx, out IQueryMatch directMatch)
    {
        directMatch = null;

        // Discovery: ORDER BY shape, sort-driving clause selection, residual
        // scannability + cost check. Per Phase 5, all per-execution rebuilds of
        // the live match are routed through ConstructDirectScan; this method
        // only validates the runtime state is compatible and then delegates.
        if (ctx.OrderByFields == null || ctx.OrderByFields.Length == 0)
            return false;

        if (ctx.OrderByFields.Length > 2)
            return false;

        bool hasTieBreak = ctx.OrderByFields.Length == 2;
        if (hasTieBreak)
        {
            var tieBreakType = ctx.OrderByFields[1].FieldType;
            if (tieBreakType is not (MatchCompareFieldType.Integer or MatchCompareFieldType.Floating or MatchCompareFieldType.Sequence))
                return false;
        }

        var indexSearcher = ctx.PlanParams.IndexSearcher;
        string sortFieldName = ctx.OrderByFields[0].Field.FieldName.ToString();
        var sortFieldType = ctx.OrderByFields[0].FieldType;

        var execs = ctx.Exec.Executions;
        bool isFullScan = execs == null || execs.Count == 0;

        if (isFullScan && ctx.Exec.Plan.AllNegated)
            return false;

        // ── Discovery: drivingIdx + cost gate ──
        int drivingIdx = -1;
        long entriesToScan = 0, bitmapCost = 0;
        List<ScanPredicateInfo> preBuiltResiduals = null;
        if (!isFullScan)
        {
            // SortDrivingClauseIndex pre-identified at template time and remapped to
            // post-sort index during Build — skip the per-execution clause scan.
            drivingIdx = ctx.Exec.Plan.SortDrivingClauseIndex;
            if (drivingIdx == -1)
            {
                // Fallback: template didn't identify a candidate (e.g. WHEN eliminated the
                // clause, or sort field didn't match any template clause). Boost is ruled
                // out at template time, so we don't recheck BoostFactor here.
                for (int i = 0; i < execs.Count; i++)
                {
                    var cl = execs[i].Clause;
                    if (cl.FieldName != sortFieldName)
                        continue;
                    if (cl.ClauseType is not (ClauseType.GreaterThan or ClauseType.GreaterThanOrEqual
                        or ClauseType.LessThan or ClauseType.LessThanOrEqual or ClauseType.Between
                        or ClauseType.Equals))
                        continue;
                    if (cl.IsNegated)
                        continue;
                    drivingIdx = i;
                    break;
                }
            }

            if (drivingIdx == -1)
                return false;

            if (execs[drivingIdx].PackedParamValue.IsNone)
                return false;

            // Residual scannability + bitmap cost summation in one pass.
            // The ScanPredicateInfo array built here is the same array ConstructDirectScan
            // needs, so we collect it now and pass it forward instead of rebuilding.
            int rlongIdx = 0, rdoubleIdx = 0, rsliceIdx = 0;
            for (int i = 0; i < execs.Count; i++)
            {
                bitmapCost += EffectiveCardinality(execs[i], indexSearcher);
                if (i == drivingIdx) continue;
                // Boost is ruled out at template time (see ComputeOptFlags).
                var pred = BuildScanPredicateInfo(execs[i], ref rlongIdx, ref rdoubleIdx, ref rsliceIdx);
                if (pred == null)
                    return false;
                preBuiltResiduals ??= new List<ScanPredicateInfo>();
                preBuiltResiduals.Add(pred.Value);
            }

            long drivingCard = EffectiveCardinality(execs[drivingIdx], indexSearcher);
            entriesToScan = preBuiltResiduals is { Count: > 0 }
                ? AdjustEntriesToScanByMinResidual(execs, drivingIdx, drivingCard, indexSearcher)
                : drivingCard;

            if (IsDirectScanCostEffective(entriesToScan, bitmapCost) == false)
                return false;
        }
        else
        {
            // Full-scan structural eligibility checks (would-cause-empty paths).
            if (ctx.OrderByFields[0].MayHaveMissingEntries)
                return false;
            if (sortFieldType is not (MatchCompareFieldType.Sequence or MatchCompareFieldType.Integer or MatchCompareFieldType.Floating))
                return false;
        }

        directMatch = ConstructDirectScan(ref ctx, drivingIdx, isFullScan, hasTieBreak, entriesToScan, bitmapCost, preBuiltResiduals);
        return directMatch != null;
    }

    /// <summary>Phase 5 bake: construction-only path for the DirectScan hint.
    /// Discovery (clause selection, cost gate, residual scannability) already passed
    /// in either TryCreateSimpleFieldDirectScan or by virtue of a cached
    /// <see cref="ExecutionStrategy.DirectScan"/>. Returns null when a per-execution
    /// runtime check fails (e.g. driving match resolution returns non-TermsProviderMatch
    /// or tie-break group cap exceeded by current parameter cardinality).</summary>


    private static IQueryMatch ConstructDirectScan(
        ref InstCtx ctx,
        int drivingIdx, bool isFullScan, bool hasTieBreak,
        long entriesToScan, long bitmapCost,
        List<ScanPredicateInfo> preBuiltResiduals = null)
    {
        var indexSearcher = ctx.PlanParams.IndexSearcher;
        var walkerCtx = new ResolutionContext(ctx.BuilderParams);
        string sortFieldName = ctx.OrderByFields[0].Field.FieldName.ToString();
        bool forward = ctx.OrderByFields[0].Ascending;
        var sortFieldType = ctx.OrderByFields[0].FieldType;
        var execs = ctx.Exec.Executions;

        ITermsProvider provider;
        LowLevelTransaction llt;
        string drivingClauseDescription;

        if (isFullScan)
        {
            var fieldMeta = ctx.OrderByFields[0].Field;
            IQueryMatch fullScanMatch;
            if (sortFieldType == MatchCompareFieldType.Integer)
                fullScanMatch = indexSearcher.BetweenQuery(fieldMeta, long.MinValue, long.MaxValue, forward: forward);
            else if (sortFieldType == MatchCompareFieldType.Floating)
                fullScanMatch = indexSearcher.BetweenQuery(fieldMeta, double.MinValue, double.MaxValue, forward: forward);
            else
                fullScanMatch = indexSearcher.ExistsQuery(fieldMeta, forward: forward);
            if (fullScanMatch is not TermsProviderMatch tpm)
                return null;
            provider = tpm.Provider;
            llt = tpm.Llt;
            drivingClauseDescription = $"{sortFieldName} [all]";
        }
        else
        {
            var drivingExec = execs[drivingIdx];

            TermsProviderMatch tpm;
            if (drivingExec.ClauseType == ClauseType.Equals)
            {
                var eqMatch = ResolveEqualsClauseWithDirection(drivingExec, ctx.Exec, forward, walkerCtx);
                if (eqMatch is not TermsProviderMatch eq)
                    return null;
                tpm = eq;
            }
            else
            {
                var match = ResolveRangeClauseWithDirection(drivingExec, ctx.Exec, forward, walkerCtx);
                if (match is not TermsProviderMatch m)
                    return null;
                tpm = m;
            }

            provider = tpm.Provider;
            llt = tpm.Llt;
            drivingClauseDescription = $"{drivingExec.Clause.FieldName} {drivingExec.ClauseType}";
        }

        // Residual predicates: reuse the list built during discovery when available;
        // the cached strategy dispatch path passes null and we build it here.
        List<ScanPredicateInfo> residualPreds = preBuiltResiduals;
        if (residualPreds == null && !isFullScan)
        {
            int longIdx = 0, doubleIdx = 0, sliceIdx = 0;
            for (int i = 0; i < execs.Count; i++)
            {
                if (i == drivingIdx) continue;
                var pred = BuildScanPredicateInfo(execs[i], ref longIdx, ref doubleIdx, ref sliceIdx);
                if (pred == null)
                    return null;
                residualPreds ??= new List<ScanPredicateInfo>();
                residualPreds.Add(pred.Value);
            }
        }

        // ── Create the driving match ──
        // BetweenQuery and StartWithQuery don't include nulls in their term output,
        // so SortedDrivingMatch must drain them itself (respecting nullFirst direction).
        bool nullIsSmallest = (ctx.OrderByFields[0].NullsSortMode ?? ctx.BuilderParams.Index.Configuration.NullsSortMode) == NullsSortMode.NullsSmallest;
        bool nullFirst = forward ? nullIsSmallest : !nullIsSmallest;
        IQueryMatch drivingMatch;
        if (hasTieBreak)
        {
            // Secondary field uses its own NullsSortMode — distinct from the primary field's.
            bool secondaryNullIsSmallest = (ctx.OrderByFields[1].NullsSortMode ?? ctx.BuilderParams.Index.Configuration.NullsSortMode) == NullsSortMode.NullsSmallest;
            int take = ctx.BuilderParams?.Take ?? Constants.IndexSearcher.TakeAll;
            drivingMatch = new SortedDrivingWithTieBreakMatch(
                provider, llt, ctx.PlanParams.Allocator, indexSearcher,
                ctx.OrderByFields[0].Field, ctx.OrderByFields[1].Field,
                ctx.OrderByFields[1].FieldType, secondaryDescending: !ctx.OrderByFields[1].Ascending,
                nullFirst: nullFirst, nullIsSmallest: secondaryNullIsSmallest,
                take: take);
        }
        else
        {
            drivingMatch = new SortedDrivingMatch(provider, llt, ctx.PlanParams.Allocator,
                indexSearcher, ctx.OrderByFields[0].Field, nullFirst);
        }

        // ── Residual scan parameters ──
        ScanPredicateInfo[] residualArray = residualPreds is { Count: > 0 } ? residualPreds.ToArray() : null;
        BuildResidualScanParams(ctx.Exec, indexSearcher, ctx.PlanParams.Allocator, residualArray,
            drivingIdx, -1,
            out var longParams, out var doubleParams, out var sliceParams, out var fieldRootPages);

        var ds = BuildDirectScan(
            indexSearcher, drivingMatch, longParams, doubleParams, sliceParams, fieldRootPages,
            ctx.Plan.CompiledEntryPredicate, residualArray);
        ds.DrivingTreeName = sortFieldName;
        ds.DrivingClause = drivingClauseDescription;
        ds.Direction = ctx.OrderByFields[0].Ascending ? "Forward" : "Backward";
        ds.ResidualDescription = residualArray != null
            ? string.Join(", ", residualPreds.ConvertAll(p => $"{p.FieldName} {p.CompareOp}"))
            : null;
        ds.Reason = isFullScan
            ? "full index-only scan (no WHERE clause)"
            : $"entries_to_scan({entriesToScan}) × {QueryPrimitives.EntryScanCostMultiplier} < bitmap_cost({bitmapCost})";
        return ds;
    }

    /// <summary>Create the appropriate DirectScan match based on whether residual predicates exist.</summary>


    private static DirectScanMatchBase BuildDirectScan(
        IndexSearcher searcher, IQueryMatch drivingMatch,
        long[] longParams, double[] doubleParams, Slice[] sliceParams, long[] fieldRootPages,
        ResidualScanIlEmitter.ResidualScanPredicate residualDelegate,
        ScanPredicateInfo[] residualArray)
    {
        if (residualArray == null)
            return new DirectScanSimpleMatch(searcher, drivingMatch, take: -1);

        return new DirectScanFilteredMatch(
            searcher, drivingMatch, longParams, doubleParams, sliceParams, fieldRootPages,
            take: -1, precompiledDelegate: residualDelegate);
    }

    /// <summary>Singleton no-op ITermsProvider for TreeScan slots where the field doesn't exist.
    /// FillPostingListIds returns 0 immediately, so the bitmap op is a no-op.</summary>


    private static IQueryMatch[] ResolveMatches(QueryExecution exec, ResolutionContext walkerCtx)
    {
        Debug.Assert(!exec.HasSpatialOrVector,
            "ResolveMatches reached with IsAllEntries && HasSpatialOrVector — InstantiateAllEntriesPostFilter bypass should have handled this.");

        return (exec.IsAllEntries, exec.Executions) switch
        {
            (true, _) => [walkerCtx.IndexSearcher.AllEntries()],
            (false, []) => [],
            _ => ResolveSlots<MatchResolver, IQueryMatch>(walkerCtx, exec)
        };
    }

    /// <summary>Produces a slot per LEAF position from QueryExecution's clauses. Slot layout
    /// matches <see cref="CountMatchSlots"/> exactly — both walk the clause tree via the
    /// same recursion (<see cref="ResolveClauseLeavesInto{TResolver, TSlot}"/> here,
    /// <see cref="CountClauseLeaves"/> there). The emit helpers consume leaves in the
    /// same order, keeping IL slot indices end-to-end consistent.
    ///
    /// <para><paramref name="clauseDispatch"/> is the per-leaf effective dispatch vector
    /// computed at plan-build time (parallel to the recursion). A slot is populated only
    /// when the leaf's dispatch matches <c>TResolver.TargetDispatch</c>; mismatched slots
    /// stay at <c>default(TSlot)</c>. This avoids the wasted work of building three
    /// parallel arrays where IL only ever reads one per slot.</para>
    /// </summary>


    private static PostingSource[] ResolveTermSources(QueryExecution exec, ResolutionContext walkerCtx)
    {
        // IsAllEntries plans never emit term ops (FillFromPostings / AndWith / etc.) —
        // their match[0] is AllEntries, post-filter slots are spatial/vector. No
        // PostingSource population is needed.
        if (exec.IsAllEntries || exec.Executions is not { Count: > 0 })
            return [];

        return ResolveSlots<TermSourceResolver, PostingSource>(walkerCtx, exec);
    }

    /// <summary>Resolve TreeScan-eligible clauses to ITermsProvider instances for direct
    /// tree-scan dispatch in the compiled pipeline. Slot indexing is parallel to
    /// ResolveMatches/ResolveTermSources. Per-leaf dispatch filtering handles the
    /// post-boost-override semantics — when boost is on every TreeScan-shaped
    /// clause becomes QueryMatch and no TreeScan slot is populated.</summary>


    private static ITermsProvider[] ResolveTermsProviders(QueryExecution exec, ResolutionContext walkerCtx)
    {
        var execs = exec.Executions;
        if (exec.IsAllEntries || execs is not { Count: > 0 })
            return null;

        return ResolveSlots<TermsProviderResolver, ITermsProvider>(walkerCtx, exec);
    }

    /// <summary>Resolve a single TreeScan-eligible clause to its raw ITermsProvider.
    /// Returns null for non-TreeScan clauses or when the field doesn't exist in the
    /// index (factory method returned TermMatch.Empty instead of TermsProviderMatch).
    /// Null slots cause the IL to fall through to the QueryMatch dispatch path.</summary>


    private static TSlot[] ResolveSlots<TResolver, TSlot>(ResolutionContext walkerCtx, QueryExecution exec)
        where TResolver : ISlotResolver<TResolver, TSlot>
    {
        var slots = new TSlot[CountMatchSlots(exec.Executions, exec.IsAllEntries)];
        int matchIdx = 0;
        int clauseIdx = 0;
        foreach (var clauseExec in exec.Executions)
        {
            ResolveClauseLeavesInto<TResolver, TSlot>(walkerCtx, clauseExec, exec, slots, ref matchIdx, ref clauseIdx);
        }
        return slots;
    }

    /// <summary>Recursive leaf walker shared by all three <see cref="ISlotResolver{TSelf, TSlot}"/>
    /// implementations. Groups expand to their leaves. <see cref="ClauseInfo.IsOrChainNotEquals"/>
    /// clauses walk their positive form (IN/AllIn → InTermCount+1 slots, scalar → 1 slot); the IL
    /// emitter handles the complement via FillAllEntries + AndNot, picking up cancellation/timing
    /// for free. Boost on a negated leaf is silently ignored — matches Lucene, where boosting a
    /// negation has no effect because there is no match to score.</summary>


    private static void ResolveClauseLeavesInto<TResolver, TSlot>(ResolutionContext walkerCtx,
        ClauseExecution clauseExec, QueryExecution root,
        TSlot[] slots, ref int matchIdx, ref int clauseIdx)
        where TResolver : ISlotResolver<TResolver, TSlot>
    {
        var clauseDispatch = root.Plan.ClauseDispatch;
        switch (clauseExec.ClauseType)
        {
            case ClauseType.OrGroup or ClauseType.AndGroup:
                foreach (var it in clauseExec.SubExecutions)
                {
                    ResolveClauseLeavesInto<TResolver, TSlot>(walkerCtx, it, root, slots, ref matchIdx, ref clauseIdx);
                }
                break;
            case ClauseType.AllIn or ClauseType.In:
            {
                bool matches = clauseDispatch[clauseIdx++] == TResolver.TargetDispatch;
                if (!matches)
                {
                    matchIdx += clauseExec.InTermCount + 1; // +1 for the null slot
                    break;
                }

                for (int i = 0; i < clauseExec.InTermCount; i++)
                {
                    slots[matchIdx++] = TResolver.ResolveInTermSlot(clauseExec, i, root, walkerCtx);
                }

                // Null-term slot is always allocated; resolver decides whether to populate.
                slots[matchIdx++] = TResolver.ResolveNullTermSlot(clauseExec, walkerCtx);
                break;
            }
            default:
            {
                bool matches = clauseDispatch[clauseIdx++] == TResolver.TargetDispatch;
                if (!matches)
                {
                    matchIdx++;
                    break;
                }
                slots[matchIdx++] = TResolver.ResolveDefaultSlot(clauseExec, root, walkerCtx);
                break;
            }
        }
    }

    /// <summary>Static abstracts keep the dispatch monomorphic — the JIT specializes  resolver.</summary>


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
                var vectorItem = HandleVector(builderParams, cur, false);
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

    /// <summary>Compute the field metadata and packed parameter for an IN term at the given index.
    /// Shared by <see cref="ResolveInTerm"/> (bitmap path) and <see cref="ResolveInTermSource"/>
    /// (posting-list path) to ensure field resolution and index arithmetic stay in sync.</summary>


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

    /// <summary>Converts an Equals clause into a BetweenQuery(low==high==value) so
    /// it produces a TermsProviderMatch that SortedDrivingMatch can walk in sort order.</summary>


    private static IQueryMatch ResolveInTerm(ClauseExecution exec, int termIndex,
        QueryExecution queryExec, ResolutionContext walkerCtx)
    {
        var (fieldMeta, termPacked) = ResolveInTermParam(exec, termIndex, walkerCtx);
        return termPacked.TermQuery(fieldMeta, walkerCtx.IndexSearcher, queryExec);
    }

    // ── Term-source resolution ───────────────────────────────────────────

    /// <summary>
    /// Resolve clause infos to <see cref="PostingSource"/> instances for the native
    /// posting-list dispatch path. Parallels <see cref="ResolveMatches"/> — the
    /// returned array uses the same indexing scheme. Slots whose underlying
    /// clause is multi-term / non-term-shaped (Spatial, Vector, Search, Range,
    /// StartsWith, EndsWith, Regex, AllEntries) keep <c>Kind == PostingSourceKind.Empty</c>;
    /// only Equals / NotEquals / In / AllIn / OrGroup-of-(Not)Equals slots populate.
    /// The IL emitter consults <see cref="PlanOp.Dispatch"/> to decide which
    /// array to read.
    /// </summary>


    private static (FieldMetadata FieldMeta, PackedParam TermPacked) ResolveInTermParam(
        ClauseExecution exec, int termIndex, ResolutionContext walkerCtx)
    {
        FieldMetadata fieldMeta = ResolveFieldMetadata(exec.Clause, walkerCtx);
        return (fieldMeta, exec.PackedParamValue.WithTermOffset(termIndex));
    }

    /// <summary>Resolve a single IN term to a typed TermQuery (bitmap path).
    /// IN terms are stored contiguously: PackedParamValue.Param1 = start index, InTermCount = count.
    /// Only non-null terms are in the typed array. Null is handled separately via HasNullTerm.</summary>


    private static ITermsProvider ResolveSingleTermsProvider(ClauseExecution exec,
        QueryExecution queryExec, ResolutionContext walkerCtx)
    {
        if (IsTreeScanEligibleClause(exec.Clause) == false)
            return null;

        // Create the match via the existing factory methods, then extract the provider.
        // The factory methods handle all complexity (analyzer, CompactKey, tree lookup).
        var match = ResolveClause(exec, queryExec, walkerCtx);
        if (match is TermsProviderMatch tpm)
            return tpm.Provider;

        // Factory returned something other than TermsProviderMatch (e.g. TermMatch.Empty
        // when the field doesn't exist). Return an empty provider so the IL's TreeScan
        // dispatch gets a valid (no-op) provider instead of null.
        return EmptyTermsProviderInstance.Instance;
    }

    /// <summary>Resolve a single Equals / NotEquals clause to a posting-list ID and
    /// decode it into a <see cref="PostingSource"/>. Returns Empty when the clause
    /// is non-term-shaped or the term doesn't exist in the index.</summary>


    private static PostingSource ResolveSingleTermSource(ClauseExecution exec,
        QueryExecution queryExec, ResolutionContext walkerCtx)
    {
        if (IsTermSourceEligibleClause(exec.Clause) == false)
            return default; // Kind == Empty

        FieldMetadata fieldMeta = ResolveFieldMetadata(exec.Clause, walkerCtx);
        long postingListId = exec.PackedParamValue.GetTermPostingListId(fieldMeta, walkerCtx.IndexSearcher, queryExec);
        return DecodePostingListId(postingListId, walkerCtx.IndexSearcher);
    }

    /// <summary>Resolve a single In/AllIn term to a posting-list source (posting-list path).
    /// Uses <see cref="ResolveInTermParam"/> for field resolution and index arithmetic.</summary>


    private static PostingSource ResolveInTermSource(ClauseExecution exec, int termIndex,
        QueryExecution queryExec, ResolutionContext walkerCtx)
    {
        var (fieldMeta, termPacked) = ResolveInTermParam(exec, termIndex, walkerCtx);
        return DecodePostingListId(termPacked.GetTermPostingListId(fieldMeta, walkerCtx.IndexSearcher, queryExec), walkerCtx.IndexSearcher);
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

    /// <summary>Decode a raw posting-list ID (with TermIdMask bits) into a
    /// <see cref="PostingSource"/>. Returns Empty when the term doesn't exist (-1).
    /// For PostingList kind, opens a fresh iterator on the underlying set.</summary>


    private static PostingSource DecodePostingListId(long postingListId, IndexSearcher indexSearcher)
    {
        if (postingListId == -1)
        {
            return default; // Kind == Empty
        }

        var termType = (TermIdMask)postingListId & TermIdMask.EnsureIsSingleMask;
        switch (termType)
        {
            case TermIdMask.Single:
                return new PostingSource
                {
                    Kind = PostingSourceKind.Single,
                    SingleEntryId = (long)EntryIdEncodings.GetContainerId(postingListId),
                };

            case TermIdMask.SmallPostingList:
                return new PostingSource
                {
                    Kind = PostingSourceKind.SmallPostingList,
                    SmallPostingListId = (long)EntryIdEncodings.GetContainerId(postingListId),
                };

            case TermIdMask.PostingList:
            {
                var postingList = indexSearcher.GetPostingList(postingListId);
                return new PostingSource
                {
                    Kind = PostingSourceKind.PostingList,
                    LargeIterator = postingList.Iterate(),
                };
            }

            default:
                return default;
        }
    }

    // ── Scan parameter extraction ────────────────────────────────────────

    /// <summary>Extract typed parameter values from clauses for entry scan.
    /// Called per-query at execution time. The values populate the CompiledQueryMatch arrays.</summary>


    private static void ExtractScanParameters(QueryExecution exec, IndexSearcher indexSearcher,
        out long[] longParams, out double[] doubleParams, out Slice[] sliceParams, out long[] fieldRootPages)
    {
        var predicates = exec.Plan.ScanPredicateInfos;
        if (predicates == null || predicates.Length == 0)
        {
            longParams = [];
            doubleParams = [];
            sliceParams = [];
            fieldRootPages = [];
            return;
        }

        var longs = new List<long>();
        var doubles = new List<double>();
        var slices = new List<Slice>();
        var roots = new List<long>();

        // Walk predicates and clauses in lock-step. BuildScanPredicateInfo skips non-eligible
        // clauses (Search, In, AllIn, Exists, StartsWith, EndsWith, Regex, Spatial, Vector,
        // AndGroup), so we must skip them here too to keep the 1:1 positional mapping.
        int scanStart = exec.Plan.AllNegated ? 0 : 1;
        int clauseIdx = scanStart;
        var execs = exec.Executions;
        foreach (ScanPredicateInfo pred in predicates)
        {
            // Advance past clauses that BuildScanPredicateInfo would have skipped.
            while (IsScanEligible(execs[clauseIdx]) == false)
                clauseIdx++;

            ExtractParamsFromPredicate(pred, execs[clauseIdx++], indexSearcher, exec, longs, doubles, slices, roots);
        }

        longParams = longs.Count > 0 ? longs.ToArray() : [];
        doubleParams = doubles.Count > 0 ? doubles.ToArray() : [];
        sliceParams = slices.Count > 0 ? slices.ToArray() : [];
        fieldRootPages = roots.Count > 0 ? roots.ToArray() : [];
    }

    /// <summary>Materialize residual scan parameter arrays for a DirectScan/CompoundField driving
    /// match. Walks all clauses except the driving (and optional secondary) index, mirroring
    /// <paramref name="residualArray"/> positionally. Used by both DirectScan and CompoundField
    /// construction; both feed the resulting arrays straight into <see cref="BuildDirectScan"/>.
    ///
    /// Unlike the bitmap-pipeline <see cref="ExtractScanParameters"/> path, slice values here
    /// use raw <c>Slice.From</c> (no analyzer) because the residual evaluator compares against
    /// the entry's stored term directly.</summary>


    private static void BuildResidualScanParams(
        QueryExecution exec, IndexSearcher indexSearcher, ByteStringContext allocator,
        ScanPredicateInfo[] residualArray, int skipClauseIdx1, int skipClauseIdx2,
        out long[] longParams, out double[] doubleParams, out Slice[] sliceParams, out long[] fieldRootPages)
    {
        longParams = null;
        doubleParams = null;
        sliceParams = null;
        fieldRootPages = null;

        var execs = exec.Executions;
        if (residualArray == null || execs == null)
            return;

        var longs = new List<long>();
        var doubles = new List<double>();
        var slices = new List<Slice>();
        var roots = new List<long>();

        int residualIdx = 0;
        for (int i = 0; i < execs.Count; i++)
        {
            if (i == skipClauseIdx1 || i == skipClauseIdx2) continue;
            roots.Add(indexSearcher.FieldCache.GetLookupRootPage(execs[i].Clause.FieldName));
            var packed = execs[i].PackedParamValue;
            if (packed.IsNone)
            {
                residualIdx++;
                continue;
            }

            int idx1 = packed.Param1;
            int idx2 = packed.Param2;
            bool hasBetween = idx2 != PackedParam.NoParamValue;
            switch (residualArray[residualIdx].ValueType)
            {
                case ScanValueType.Long:
                    longs.Add(exec.LongValues[idx1]);
                    if (hasBetween) longs.Add(exec.LongValues[idx2]);
                    break;
                case ScanValueType.Double:
                    doubles.Add(exec.DoubleValues[idx1]);
                    if (hasBetween) doubles.Add(exec.DoubleValues[idx2]);
                    break;
                case ScanValueType.Slice:
                case ScanValueType.SliceLong:
                    Slice.From(allocator, exec.StringValues[idx1], out var s1);
                    slices.Add(s1);
                    if (hasBetween)
                    {
                        Slice.From(allocator, exec.StringValues[idx2], out var s2);
                        slices.Add(s2);
                    }

                    break;
            }

            residualIdx++;
        }

        longParams = longs.Count > 0 ? longs.ToArray() : null;
        doubleParams = doubles.Count > 0 ? doubles.ToArray() : null;
        sliceParams = slices.Count > 0 ? slices.ToArray() : null;
        fieldRootPages = roots.Count > 0 ? roots.ToArray() : null;
    }


    private static void ExtractParamsFromPredicate(ScanPredicateInfo pred, ClauseExecution exec,
        IndexSearcher indexSearcher, QueryExecution queryExec, List<long> longs, List<double> doubles,
        List<Slice> slices, List<long> roots)
    {
        if (pred.SubPredicates != null)
        {
            // Each sub-predicate corresponds positionally to a sub-execution of the group.
            // BuildScanPredicateInfoCore guarantees pred.SubPredicates.Length == exec.SubExecutions.Count.
            var subExecs = exec.SubExecutions;
            for (int b = 0; b < pred.SubPredicates.Length; b++)
                ExtractParamsFromPredicate(pred.SubPredicates[b], subExecs[b], indexSearcher, queryExec, longs, doubles, slices, roots);
            return;
        }

        // Resolve field root page
        roots.Add(indexSearcher.FieldCache.GetLookupRootPage(pred.FieldName));

        // Read pre-resolved typed values from the queryExec's arrays via packed param.
        var packed = exec.PackedParamValue;
        if (packed.IsNone)
            return;
        int idx1 = packed.Param1;
        int idx2 = packed.Param2;
        bool hasBetween = idx2 != PackedParam.NoParamValue;

        switch (pred.ValueType)
        {
            case ScanValueType.Long:
                longs.Add(queryExec.LongValues[idx1]);
                if (hasBetween)
                    longs.Add(queryExec.LongValues[idx2]);
                break;
            case ScanValueType.Double:
                doubles.Add(queryExec.DoubleValues[idx1]);
                if (hasBetween)
                    doubles.Add(queryExec.DoubleValues[idx2]);
                break;
            case ScanValueType.Slice:
            case ScanValueType.SliceLong:
                var fieldMeta = indexSearcher.FieldMetadataBuilder(exec.Clause.FieldName);
                slices.Add(indexSearcher.EncodeAndApplyAnalyzer(fieldMeta, queryExec.StringValues[idx1]));
                if (hasBetween)
                    slices.Add(indexSearcher.EncodeAndApplyAnalyzer(fieldMeta, queryExec.StringValues[idx2]));
                break;
        }
    }

    // ── Compound field exact match (no ORDER BY) ─────────────────────────


    private static long EstimateCardinality(ClauseExecution exec, IndexSearcher indexSearcher, ValueWriter writer, ResolutionContext walkerCtx)
    {
        var clause = exec.Clause;
        switch (clause.ClauseType)
        {
            case ClauseType.Equals:
            {
                // ResolveFieldMetadata attaches the field's analyzer; FieldMetadataBuilder
                // does not. Without the analyzer, NumberOfDocumentsUnderSpecificTerm looks
                // up the term verbatim and misses index-time-normalized matches (e.g.
                // LowerCaseKeyword turns "Alpha" into "alpha" on the index side).
                var fieldMeta = ResolveFieldMetadata(clause, walkerCtx);
                var p = exec.PackedParamValue;
                return p.ValueType switch
                {
                    PackedParam.TypeLong => indexSearcher.NumberOfDocumentsUnderSpecificTerm(fieldMeta, writer.GetLong(p.Param1)),
                    PackedParam.TypeDouble => indexSearcher.NumberOfDocumentsUnderSpecificTerm(fieldMeta, writer.GetDouble(p.Param1)),
                    _ => indexSearcher.NumberOfDocumentsUnderSpecificTerm(fieldMeta, writer.GetString(p.Param1))
                };
            }

            case ClauseType.NotEquals:
            case ClauseType.GreaterThan:
            case ClauseType.GreaterThanOrEqual:
            case ClauseType.LessThan:
            case ClauseType.LessThanOrEqual:
            case ClauseType.Between:
            case ClauseType.Exists:
            case ClauseType.StartsWith:
            case ClauseType.EndsWith:
            case ClauseType.Search:
            case ClauseType.Regex:
                // Use field-level cardinality as upper bound
                return indexSearcher.GetTermAmountInField(ResolveFieldMetadata(clause, walkerCtx));

            case ClauseType.In:
            case ClauseType.AllIn:
                // Sum of individual term cardinalities. ResolveFieldMetadata picks up the
                // field analyzer so case-folding/keyword normalization applies before the
                // per-term posting-list lookup — otherwise IN over an analyzed field
                // returns 0 for every term and the clause is misjudged as trivially small,
                // which corrupts the cardinality-driven clause ordering.
                long sum = 0;
                var meta = ResolveFieldMetadata(clause, walkerCtx);
                var ip = exec.PackedParamValue;
                if (ip.IsNone)
                    return indexSearcher.NumberOfEntries;

                int start = ip.Param1;
                int count = exec.InTermCount;
                for (int t = 0; t < count; t++)
                {
                    sum += ip.ValueType switch
                    {
                        PackedParam.TypeLong => indexSearcher.NumberOfDocumentsUnderSpecificTerm(meta, writer.GetLong(start + t)),
                        PackedParam.TypeDouble => indexSearcher.NumberOfDocumentsUnderSpecificTerm(meta, writer.GetDouble(start + t)),
                        _ => indexSearcher.NumberOfDocumentsUnderSpecificTerm(meta, writer.GetString(start + t))
                    };
                }

                return Math.Min(sum, indexSearcher.NumberOfEntries);

            case ClauseType.Spatial:
            case ClauseType.Vector:
                return indexSearcher.NumberOfEntries;

            case ClauseType.OrGroup:
                long orSum = 0;
                if (exec.SubExecutions == null)  return orSum;
                for (int si = 0; si < clause.SubClauses.Count; si++)
                {
                    var subExec = exec.SubExecutions[si];
                    if (subExec.Cardinality < 0)
                    {
                        subExec.Cardinality = EstimateCardinality(subExec, indexSearcher, writer, walkerCtx);
                    }
                    orSum += subExec.Cardinality;
                }
                return Math.Min(orSum, indexSearcher.NumberOfEntries);

            case ClauseType.AndGroup:
                long andMin = indexSearcher.NumberOfEntries;
                if (exec.SubExecutions == null) return andMin;
                for (int si = 0; si < clause.SubClauses.Count; si++)
                {
                    var subExec = exec.SubExecutions[si];
                    if (subExec.Cardinality < 0)
                    {
                        subExec.Cardinality = EstimateCardinality(subExec, indexSearcher, writer, walkerCtx);
                    }
                    andMin = Math.Min(andMin, subExec.Cardinality);
                }
                return andMin;

            default:
                return indexSearcher.NumberOfEntries;
        }
    }

    // ── Execution-phase methods (moved from QueryPlanBuilder.cs) ──────────

    /// <summary>Format a value from the plan's typed arrays as a string for display/highlighting.</summary>


    private static long EffectiveCardinality(in ClauseExecution exec, IndexSearcher indexSearcher)
        => exec.Cardinality > 0 ? exec.Cardinality : indexSearcher.NumberOfEntries;

    /// <summary>Adjust the driving-clause cardinality by the most-selective residual's
    /// pass rate to estimate how many entries the scan will actually touch. The min
    /// residual is the tightest filter we'll apply during the scan — its selectivity
    /// shrinks the effective entries-to-scan. Skips only <paramref name="drivingIdx"/>
    /// (other "structural" clauses like a field2 range still count as residuals here,
    /// matching pre-extraction behavior).</summary>


    private static long AdjustEntriesToScanByMinResidual(List<ClauseExecution> execs,
        int drivingIdx, long drivingCard, IndexSearcher indexSearcher)
    {
        long minResidual = long.MaxValue;
        for (int i = 0; i < execs.Count; i++)
        {
            if (i == drivingIdx) continue;
            long c = EffectiveCardinality(execs[i], indexSearcher);
            if (c < minResidual) minResidual = c;
        }

        if (minResidual > 0 && minResidual < indexSearcher.NumberOfEntries)
        {
            double passRate = (double)minResidual / indexSearcher.NumberOfEntries;
            if (passRate > 0)
                return (long)(drivingCard / passRate);
        }

        return drivingCard;
    }

    /// <summary>True iff <paramref name="exec"/> carries any boost — either annotated at
    /// template time (<see cref="ClauseInfo.HasBoost"/>) or with a resolved runtime
    /// factor (<see cref="ClauseExecution.BoostFactor"/> &gt; 0). Compound-key and
    /// direct-scan paths can't propagate scores, so they reject any boosted clause.
    ///
    /// HasBoost is normally filtered upstream at plan time
    /// (see <c>HasBoostRecursive</c> in QueryPlanBuilder.cs), so the second disjunct
    /// catches edge cases where a runtime factor is set without the template flag
    /// (e.g. wrapper paths that materialize a BoostingMatch directly).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]


    private static bool IsDirectScanCostEffective(long entriesToScan, long bitmapCost)
    {
        long directCost = entriesToScan > long.MaxValue / QueryPrimitives.EntryScanCostMultiplier
            ? long.MaxValue
            : entriesToScan * QueryPrimitives.EntryScanCostMultiplier;
        return directCost < bitmapCost && entriesToScan <= QueryPrimitives.EntryScanCountThreshold;
    }

    /// <summary>Count IL match slots a query consumes. Nested groups expand to one slot
    /// PER LEAF (recursively); the recursive walk is shared with <see cref="ResolveSlots"/>
    /// and the emit helpers so slot indices stay aligned end-to-end.</summary>


    private static bool CheckAllNegated(List<ClauseExecution> executions) => executions is [{ IsNegated: true }, ..];


    private static bool IsEmptyIn(ClauseExecution e) =>
        // HasNullTerm must also block the empty-IN path: a list whose only entry
        // is null arrives as InTermCount=0+HasNullTerm=true and still has to match
        // docs with a null in that field via the null-term posting list (Fill@0
        // reads the null PL, OrRange/AndRange becomes a runtime no-op when
        // InRangeCounts[rangeIdx] resolves to 0).
        e.ClauseType is ClauseType.In or ClauseType.AllIn &&
        (e.InTermCount == 0) &&
        e.HasNullTerm is false;
    
    /// <summary>True when any clause in the array is an empty IN/AllIn (InTermCount=0,
    /// no null term). Used to detect guaranteed-empty AND chains before the cache lookup.</summary>


    private static bool HasEmptyIn(List<ClauseExecution> executions)
    {
        foreach (var exec in executions)
        {
            if (IsEmptyIn(exec))
                return true;
        }
        return false;
    }

    /// <summary>Build the per-execution InRangeCounts array. Each IN/AllIn clause gets one
    /// slot whose value is the number of posting-source slots the compiled IL's OrRange/AndRange
    /// op will iterate. The IL reads these at runtime so the same compiled delegate handles
    /// different parameter array sizes across executions of the same query text.</summary>


    private static int[] BuildInRangeCounts(List<ClauseExecution> executions,  int slotCount)
    {
        var counts = new int[slotCount];
        int rangeIdx = 0;
        AccumulateInRangeCounts(executions, counts, ref rangeIdx);
        return counts;
    }


    private static void AccumulateInRangeCounts(List<ClauseExecution> executions, int[] counts, ref int rangeIdx)
    {
        for (int ci = 0; ci < executions.Count && rangeIdx < counts.Length; ci++)
        {
            ClauseExecution execution = executions[ci];
            switch (execution.Clause.ClauseType)
            {
                case ClauseType.OrGroup:
                case ClauseType.AndGroup:
                    if (execution.SubExecutions is not null)
                        AccumulateInRangeCounts(execution.SubExecutions, counts, ref rangeIdx);
                    break;

                // IN: EmitInOps emits Fill + OrRange. Fill consumed slot 0,
                // range = InTermCount (ORing with empty null slot is a no-op).
                case ClauseType.In:
                    counts[rangeIdx++] = execution.InTermCount;
                    break;

                // AllIn: EmitAllInOps emits Fill + AndRange over inTermCount slots (all typed terms
                // + the null-term slot). The null-term slot is always iterated; when HasNullTerm=false
                // ResolveNullTermSlot returns PostingSourceKind.All so the AND is a no-op rather than
                // clearing the bitmap. The cursor always advances inTermCount positions past Fill,
                // landing at inTermCount+1 = CountClauseLeaves(AllIn) — consistent with the slot layout.
                case ClauseType.AllIn:
                    counts[rangeIdx++] = execution.InTermCount;
                    break;
            }
        }
    }


    private static bool AreAllScanEligible(List<ClauseExecution> executions, int startIndex)
    {
        // If any clause (In, AllIn, Spatial, Vector, Search, etc.) can't be scanned, we must not emit CheckAndMaybeEntryScan — entry scan would skip them entirely.
        for (int j = startIndex; j < executions.Count; j++)
        {
            if (IsScanEligible(executions[j]) == false)
                return false;
        }

        return true;
    }

    // ── Plan helpers ─────────────────────────────────────────────────────

    /// <summary>Emit ops for an IN clause: Fill slot 0 + OrRange for the rest.
    /// Fixed 2-op shape regardless of term count or presence of null. Slot 0 holds
    /// the null-term posting list when HasNullTerm, else the first typed term, else
    /// an empty PostingSource. Slots 1..inTermCount hold remaining typed terms and
    /// the null-term, dispatched via OrRange whose count comes from
    /// <c>ctx.InRangeCounts[rangeIdx]</c> at runtime. <paramref name="inTermCount"/>
    /// must match <c>exec.InTermCount</c> so the emitter slot layout agrees with the
    /// resolver's <see cref="ResolveClauseLeavesInto{TResolver,TSlot}"/> walk.</summary>


    private static bool IsClauseBoosted(ClauseExecution exec)
        => exec.Clause.HasBoost || exec.BoostFactor > 0;

    /// <summary>Encode a numeric (long/double) field value at <paramref name="paramIdx"/>
    /// into 8 big-endian sortable bytes — the same encoding indexing uses for compound-key
    /// long/double fields. Doubles map through <see cref="Bits.DoubleToSortableLong"/>
    /// first so that descending order matches IEEE-754 semantics.</summary>


    private static byte[] EncodeNumericBoundBigEndian(QueryExecution exec, int valueType, int paramIdx)
    {
        long raw = valueType == PackedParam.TypeDouble
            ? Bits.DoubleToSortableLong(exec.DoubleValues[paramIdx])
            : exec.LongValues[paramIdx];
        var buf = new byte[sizeof(long)];
        BinaryPrimitives.WriteInt64BigEndian(buf, Bits.SwapBytes(raw));
        return buf;
    }

    /// <summary>Cost-gate shared by direct-scan eligibility checks: rejects when the
    /// estimated direct-scan cost (entries × multiplier) is no cheaper than the
    /// bitmap-pipeline cost, or when the scan would touch more entries than the
    /// hard threshold. Returns <c>true</c> when direct scan is the right pick.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]


    internal static int CountMatchSlots(List<ClauseExecution> executions, bool isAllEntries)
    {
        int count = isAllEntries ? 1 : 0;
        foreach (var exec in executions ?? [])
            count += CountClauseLeaves(exec);
        return count;
    }

    /// <summary><see cref="ClauseInfo.IsOrChainNotEquals"/> leaves walk their positive form
    /// (1 slot for scalar/exists, InTermCount+1 for IN/AllIn) and the IL emitter materialises
    /// the complement via FillAllEntries + AndNot. Boost on a negated leaf is ignored (matches
    /// Lucene), so HasBoost has no effect on the slot count here.</summary>


    private static int CountClauseLeaves(ClauseExecution exec)
    {
        switch (exec.ClauseType)
        {
            case ClauseType.OrGroup or ClauseType.AndGroup:
                int sum = 0;
                foreach (var it in exec.SubExecutions)
                {
                    sum += CountClauseLeaves(it);
                }

                return sum;
            case ClauseType.In or ClauseType.AllIn:
                return exec.InTermCount + 1;
            default:
                return 1;
        }
    }

    /// <summary>For an OrGroup or AndGroup clause, returns the parallel (sub-clauses, sub-executions)
    /// arrays that callers iterate to fan out one match slot per sub-term. Returns false for any
    /// other clause type, or for empty groups.</summary>


    internal static bool TryGetGroupFanOut(ClauseInfo clause, ClauseExecution exec,
        out List<ClauseInfo> subClauses, out List<ClauseExecution> subExecs)
    {
        if (clause.ClauseType is ClauseType.OrGroup or ClauseType.AndGroup && clause.SubClauses is { Count: > 0 })
        {
            subClauses = clause.SubClauses;
            subExecs = exec.SubExecutions;
            return true;
        }

        subClauses = null;
        subExecs = null;
        return false;
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

    /// <summary>Per-leaf effective dispatch in the same recursive walk order as
    /// <see cref="ResolveClauseLeavesInto{TResolver, TSlot}"/> and
    /// <see cref="CountClauseLeaves"/>. Or/AndGroup recurses into sub-executions; IN/AllIn
    /// collapses to one entry (its dispatch applies to every term + null slot the clause
    /// emits); every other clause type contributes one entry. Boost forces every entry to
    /// <see cref="MatchDispatch.QueryMatch"/> — mirroring the boost-override loop that
    /// already promoted every <see cref="PlanOp.Dispatch"/>. Empty for IsAllEntries plans
    /// and for plans with no executions.</summary>


    private static MatchDispatch[] ComputeClauseDispatch(List<ClauseExecution> executions, bool planHasBoost, PlanTemplate template)
    {
        if (executions is null || executions.Count == 0)
            return [];

        var list = new List<MatchDispatch>(executions.Count);
        foreach (var clauseExec in executions)
            AppendClauseDispatch(clauseExec, planHasBoost, list);

        // Spatial/vector clauses are separated by GroupCollapse at template time and
        // appended to exec.Executions later by AttachSpatialAndVectorClauses. They
        // always resolve through IQueryMatch (no PostingList / TreeScan fast path).
        int postFilterCount = (template.SpatialClauses?.Count ?? 0) + (template.VectorClauses?.Count ?? 0);
        for (int i = 0; i < postFilterCount; i++)
            list.Add(MatchDispatch.QueryMatch);

        return list.ToArray();
    }


    private static void AppendClauseDispatch(ClauseExecution clauseExec, bool planHasBoost, List<MatchDispatch> list)
    {
        switch (clauseExec.ClauseType)
        {
            case ClauseType.OrGroup or ClauseType.AndGroup:
                foreach (var sub in clauseExec.SubExecutions)
                    AppendClauseDispatch(sub, planHasBoost, list);
                break;
            // IN/AllIn always resolve as individual posting-list lookups (EmitInOps /
            // EmitAllInOps hardcode PostingList on the emitted ops). GetDispatch would
            // return QueryMatch for the parent clause type, causing a mismatch.
            case ClauseType.In or ClauseType.AllIn:
                list.Add(planHasBoost ? MatchDispatch.QueryMatch : MatchDispatch.PostingList);
                break;
            default:
                list.Add(planHasBoost ? MatchDispatch.QueryMatch : GetDispatch(clauseExec.Clause));
                break;
        }
    }
    
    /// <summary>Resolution-time overload: derives term type from <paramref name="exec"/>
    /// and recurses into subclauses using sub-execution types. Used when actual resolved
    /// types are available (per-execution, after PopulateClauseValues).</summary>


    internal static ScanPredicateInfo? BuildScanPredicateInfo(ClauseExecution exec, ref int longIndex, ref int doubleIndex, ref int sliceIndex)
        => BuildScanPredicateInfoCore(exec, exec.TermValueType, ref longIndex, ref doubleIndex, ref sliceIndex);

    /// <summary>Eligibility-only probe — defined as "would <see cref="BuildScanPredicateInfo"/>
    /// return non-null?" so the two cannot drift. The throwaway index counters are discarded;
    /// the only cost over a hand-rolled walk is the List+ToArray allocation in the Group case,
    /// which is acceptable because callers process eligible clauses by calling Build immediately
    /// after anyway.</summary>


    private static bool IsScanEligible(ClauseExecution exec)
    {
        int l = 0, d = 0, s = 0;
        return BuildScanPredicateInfo(exec, ref l, ref d, ref s) is not null;
    }

    /// <summary>Single walker shared by both overloads. <paramref name="exec"/> is non-null on
    /// the resolution path and supplies per-sub TermValueType during group recursion; on the
    /// template path it is null and recursion falls back to InferTermType.</summary>


    private static ScanPredicateInfo? BuildScanPredicateInfoCore( ClauseExecution exec, ParamValueType termType, ref int longIndex, ref int doubleIndex, ref int sliceIndex)
    {
        var clause = exec.Clause;
        switch (clause.ClauseType)
        {
            // These clause types cannot be expressed as entry-scan predicates.
            case ClauseType.Search:
            case ClauseType.Regex:
            case ClauseType.Spatial:
            case ClauseType.Vector:
            case ClauseType.In:
            case ClauseType.AllIn:
                return null;

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
                if (clause.SubClauses is not { Count: > 0 } subs)
                    return null;

                var subExecs = exec.SubExecutions;
                var branches = new List<ScanPredicateInfo>();
                // Save indices so we can roll back if any subclause is unscannable.
                int li = longIndex, di = doubleIndex, slc = sliceIndex;
                for (int si = 0; si < subs.Count; si++)
                {
                    var subTermType = subExecs[si].TermValueType;
                    var subPred = BuildScanPredicateInfoCore(subExecs[si], subTermType, ref li, ref di, ref slc);
                    if (subPred == null)
                        return null;
                    branches.Add(subPred.Value);
                }
                longIndex = li; doubleIndex = di; sliceIndex = slc;
                return new ScanPredicateInfo
                {
                    FieldName = clause.FieldName ?? subs[0].FieldName,
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
            _ => ScanValueType.Slice  // String/True/False/Null/Parameter (when unresolvable) → opaque slice comparison.
        };

        bool isBetween = clause.ClauseType == ClauseType.Between;
        var (idx, idx2) = valueType switch
        {
            ScanValueType.Long => (longIndex++, isBetween ? longIndex++ : -1),
            ScanValueType.Double => (doubleIndex++, isBetween ? doubleIndex++ : -1),
            _ => (sliceIndex++, isBetween ? sliceIndex++ : -1)
        };

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
