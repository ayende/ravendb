using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Threading;
using Corax.Mappings;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.SortingMatches.Meta;
using Corax.Querying.Planning;
using Corax.Utils;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Server;
using Voron;
using Voron.Impl;
using Constants = Corax.Constants;
using RavenConstants = Raven.Client.Constants;
using IndexSearcher = Corax.Querying.IndexSearcher;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

internal static partial class QueryPlanBuilder
{
    public static PlanTemplate BuildTemplate(PlanParameters planParams)
    {
        var planCache = planParams.IndexSearcher.PlanCache;
        var metadata = planParams.Metadata;

        var generation = planCache.GenerationIdx;

        if (metadata.CachedPlanMemo is { } memo
            && memo.PlanCacheGeneration == generation
            && memo.Bucket.TryGetTarget(out var warmBucket))
        {
            return Finalize(warmBucket);
        }

        var structuralKey = ComputeStructuralKey(planParams);
        if (planCache.GetBucket(structuralKey) is { } existing)
        {
            metadata.CachedPlanMemo = new QueryMetadata.PlanMemo(generation, existing);
            return Finalize(existing);
        }

        var template = ParseTemplate(planParams);
        template.SortMetadataTemplate = BuildSortMetadataTemplate(planParams, template);
        var bucket = planCache.GetOrAddBucket(structuralKey, template, planParams.CacheKey);
        metadata.CachedPlanMemo = new QueryMetadata.PlanMemo(generation, bucket);
        return Finalize(bucket);

        PlanTemplate Finalize(PlanCache.PerQueryPlans b)
        {
            // ExtractSlotBindings is only called on fresh metadata instances, that tends to be rare, since 
            // we cache the metadata instances at the database level
            var bindings = metadata.CachedSlotBindings ??= ExtractSlotBindings(planParams);
            planParams.SlotBindings = bindings;
            planParams.Bucket = b;
            AssertSlotBindingsMatchTemplate(b.Template, bindings);
            return b.Template;
        }
    }

    [Conditional("DEBUG")]
    private static void AssertSlotBindingsMatchTemplate(PlanTemplate template, ParameterBinding[] slotBindings)
    {
        Debug.Assert(template.ValueOrdinalCount == slotBindings.Length,
            $"Slot-binding vector length ({slotBindings.Length}) must equal the template value-ordinal count " +
            $"({template.ValueOrdinalCount}). Both come from the same canonical WHERE walk, so a mismatch means the " +
            "template parse and the per-query slot-vector parse diverged.");
    }

    /// <summary>
    /// This gets the query match without any sorting. This is used by callers who care about the results but not the order.
    /// For example, facets, more-like-this, etc.
    /// </summary>
    public static IQueryMatch BuildFilterMatch(
        PlanParameters planParams,
        QueryBuilderParameters builderParameters,
        out QueryExecution exec,
        Dictionary<string, CoraxHighlightingTermIndex> highlightingTerms,
        bool wantTimings,
        CancellationToken token)
    {
        var indexSearcher = planParams.IndexSearcher;
        var walkerCtx = new ResolutionContext(builderParameters);

        var template = BuildTemplate(planParams);

        exec = Build(template, planParams, builderParameters, walkerCtx);
        return InstantiateBitmapPipeline(exec.Plan, exec, planParams, builderParameters, walkerCtx, highlightingTerms, wantTimings, token);
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

        var exec = Build(template, planParams, builderParameters, walkerCtx);
        var orderByFields = GetSortMetadata(builderParameters, exec.Plan.Template);
        // A single vector-search post-filter already streams its HNSW output in score order. When that matches what
        // the query asks for, skip the redundant SortingMatch wrapper: stream score order in ApplyPostFilters, skip
        // the wrapper in Instantiate. The order-agnostic BuildFilterMatch path never reaches here (facets / MLT keep
        // entry-id-sorted vector output).
        exec.VectorPostFilterProvidesScoreOrder = VectorPostFilterProvidesResultOrder(exec, builderParameters, orderByFields);
        var queryMatch = Instantiate(exec, orderByFields,
            planParams, builderParameters, walkerCtx, highlightingTerms, wantTimings, out var innerMatch, token);
        return new(queryMatch, innerMatch, queryMatch == innerMatch ? null : queryMatch, exec, builderParameters, orderByFields);
    }


    private static QueryExecution Build(PlanTemplate template, PlanParameters planParams, QueryBuilderParameters builderParameters, ResolutionContext walkerCtx)
    {
        return new BuildResolver(template, planParams, builderParameters, walkerCtx).Resolve();
    }

    internal static ClauseExecution CreateExecution(ClauseInfo clause)
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

    /// <summary>Mark a parameter-bound BETWEEN sentinel's slot in the FullKinds carrier, lazily allocating it on first use.
    /// No-op for literal/deferred bounds (ParameterSlot == -1 — the sentinel is encoded in the query text, no marker needed).</summary>
    private static void MarkSentinel(ref byte[] full, int parameterSlotCount, ParameterBinding binding)
    {
        if (binding.ParameterSlot < 0 || parameterSlotCount is 0)
            return;
        full ??= new byte[parameterSlotCount];
        full[binding.ParameterSlot] |= SentinelParamMark;
    }

    internal static void PopulateClauseValues(ClauseExecution exec, ParameterBinding[] slotBindings, BlittableJsonReaderObject queryParameters, ValueWriter writer, QueryBuilderParameters builderParameters,
        int parameterSlotCount, ref byte[] full)
    {
        RuntimeHelpers.EnsureSufficientExecutionStack();
        foreach (var it in exec.SubExecutions ?? [])
        {   // Always recurse into subclauses first (OrGroup/AndGroup have no binding of their own)
            PopulateClauseValues(it, slotBindings, queryParameters, writer, builderParameters, parameterSlotCount, ref full);
        }

        if (exec.Clause is { HasBoost: true, Bindings.Length: > 0 })
        {
            ResolveBoostFactor(exec, slotBindings, queryParameters);
        }

        switch (exec.Clause.ClauseType) // Spatial and vector resolve via their binding array.
        {
            case ClauseType.Spatial when exec.Clause.Bindings is { Length: > 0 }:
                ResolveSpatialFromBindings(exec, slotBindings, queryParameters);
                return;
            case ClauseType.Vector when exec.Clause.Bindings is { Length: > 0 }:
                ResolveVectorFromBindings(exec, slotBindings, queryParameters);
                return;
        }

        if (exec.Clause.Bindings is not { Length: > 0 })
            return;

        var bindings = exec.Clause.Bindings;
        switch (exec.Clause.ClauseType)
        {
            case ClauseType.Between: // BETWEEN: open-range "*"/"NULL" sentinel bounds (literal or parameter-bound) are detected here and rewritten to the equivalent half-open range / match-all leaf.
            {
                var (low, lowType) = ResolveBindingScalar(bindings[BindingIndex.BetweenLow], slotBindings, queryParameters, builderParameters);
                var (high, highType) = ResolveBindingScalar(bindings[BindingIndex.BetweenHigh], slotBindings, queryParameters, builderParameters);
                bool lowIsSentinel = low is RavenConstants.Documents.Querying.Terms.LeftNullValueOfBetweenQuery;
                bool highIsSentinel = high is RavenConstants.Documents.Querying.Terms.RightNullValueOfBetweenQuery;
                switch (lowIsSentinel, highIsSentinel)
                {
                    case (true, true):
                        exec.SentinelRewriteType = ClauseType.Exists;
                        MarkSentinel(ref full, parameterSlotCount, bindings[BindingIndex.BetweenLow]);
                        MarkSentinel(ref full, parameterSlotCount, bindings[BindingIndex.BetweenHigh]);
                        return;
                    case (true, false):
                        exec.SentinelRewriteType = ClauseType.LessThanOrEqual;
                        MarkSentinel(ref full, parameterSlotCount, bindings[BindingIndex.BetweenLow]);
                        exec.TermValueType = highType;
                        exec.PackedParamValue = writer.Add(high, ToValueTokenType(highType));
                        return;
                    case (false, true):
                        exec.SentinelRewriteType = ClauseType.GreaterThanOrEqual;
                        MarkSentinel(ref full, parameterSlotCount, bindings[BindingIndex.BetweenHigh]);
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
                ResolveInFromBindings(exec, slotBindings, queryParameters, writer, inBindings, builderParameters);
                break;
            default: // Simple clause (Equals, Range, Search, Regex, etc.): single value at Bindings[0]
                var (value, valueType) = ResolveBindingScalar(bindings[BindingIndex.Value], slotBindings, queryParameters, builderParameters);
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

    private static (object Value, ParamValueType Type) ResolveBindingScalar(ParameterBinding binding, ParameterBinding[] slotBindings, BlittableJsonReaderObject queryParameters, QueryBuilderParameters builderParameters)
    {
        // The template binding supplies only structure plus its canonical ValueOrdinal; the value for THIS query
        // lives in the per-query slot vector at that ordinal (so value/name/param variants share one template).
        // Redirect to the slot binding before reading any value. (Idempotent: slotBindings[b.ValueOrdinal] == b.)
        binding = slotBindings[binding.ValueOrdinal];
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
                if (queryParameters == null) // query text references $param but no parameters were supplied
                    QueryBuilderHelper.ThrowParametersWereNotProvided(builderParameters?.Metadata?.QueryText);

                if (queryParameters.TryGet(binding.ParameterName, out object raw) == false) // referenced parameter is absent from the supplied set
                    QueryBuilderHelper.ThrowParameterValueWasNotProvided(binding.ParameterName, builderParameters?.Metadata?.QueryText, queryParameters);

                if (raw == null) // explicit null value is allowed (matches null terms)
                    return (null, ParamValueType.Null);

                var (paramVal, paramType) = ResolveParameterValue(raw);
                return (paramVal, ToParamValueType(paramType));
        }
    }

    private static void ResolveBoostFactor(ClauseExecution exec, ParameterBinding[] slotBindings, BlittableJsonReaderObject queryParameters)
    {
        var (boostVal, boostType) = ResolveBindingScalar(exec.Clause.Bindings[^1], slotBindings, queryParameters, builderParameters: null);
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
    /// Foo BETWEEN $x AND $y - where $x > $y - returns nothing, this collapses the clause to a
    /// MatchNothing sentinel so the plan emitter bakes an empty bitmap for it.
    /// </summary>
    internal static void PropagateBetweenContradiction(ClauseExecution exec, ValueWriter writer)
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

        exec.MarkAsSentinel(ClauseType.MatchNothing, 0);
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

        if (exec.Plan.EntryScanSet is { HasPredicates: true })
        {
            exec.PopulateScanParams = () => ScanParamExtractor.Extract(exec, indexSearcher, walkerCtx, exec.Plan.EntryScanSet);
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
            // These spatial matches were lifted to top-level post-filters by the planner (AND context). Record the
            // role on each one so inspection reports it as a post-filter rather than re-deriving it from the type
            // (a spatial leaf inside an OR is NOT a post-filter — see IPostFilterMatch).
            foreach (var spatialMatch in spatialMatches)
            {
                if (spatialMatch is IPostFilterMatch postFilter)
                    postFilter.IsPostFilter = true;
            }

            result = result is null
                ? new PostFilterMatch(spatialMatches[0], spatialMatches.Length is 1 ? [] : spatialMatches[1..], wantTimings)
                : new PostFilterMatch(result, spatialMatches, wantTimings);
        }

        if (exec.VectorSelects is { Length: > 0 })
        {
            foreach (var item in ResolveVectorItems(exec, builderParameters))
            {
                result = item.Materialize(result, isPostFilter: true, streamScoreOrder: exec.VectorPostFilterProvidesScoreOrder);
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
    
    public static IQueryMatch BuildQueryForMoreLikeThis(QueryBuilderParameters builderParams, MethodExpression mltCall, QueryExpression expression)
    {
        mltCall.MoreLikeThisExpression ??= CreateQueryMetadataForMoreLikeThis();
        return BuildFilterMatch(new PlanParameters
        {
            IndexSearcher = builderParams.IndexSearcher,
            Metadata =  mltCall.MoreLikeThisExpression,
            QueryParameters = builderParams.QueryParameters,
            Index = builderParams.Index,
            IndexFieldsMapping = builderParams.IndexFieldsMapping,
            Allocator = builderParams.Allocator,
            HasDynamics = builderParams.HasDynamics,
            DynamicFields = builderParams.DynamicFields,
            HasBoost = builderParams.HasBoost,
        }, builderParams, out _, highlightingTerms: null, wantTimings: false, builderParams.Token);

        QueryMetadata CreateQueryMetadataForMoreLikeThis()
        {
            // The base-document sub-expression is compiled as its own standalone query rather than as a special case grafted onto the outer query.
            // We clone the outer query, swap in just this WHERE and drop ORDER BY (the result is an unsorted filter), and build a fresh QueryMetadata for it.
            var subQuery = builderParams.Query.Metadata.Query.ShallowCopy();
            subQuery.Where = expression;
            subQuery.OrderBy = null;
            return new QueryMetadata(subQuery, builderParams.QueryParameters, cacheKey: 0, addSpatialProperties: false);
        }
    }

    
    private static bool TryCreateCompoundExactMatch(ref InstCtx ctx, out string rejectReason)
    {
        // The only thing still unknown is value-dependent — a bound parameter can resolve to "none" (null/missing), which has no composite-key encoding.
        if (ctx.Exec.CompoundExactFirst.PackedParamValue.IsNone || 
            ctx.Exec.CompoundExactSecond.PackedParamValue.IsNone)
        {
            rejectReason = "the combined-key lookup needs both values, but one is null or missing";
            return false;
        }

        rejectReason = null;
        return true;
    }

    private static IQueryMatch ConstructCompoundExact(ref InstCtx ctx)
    {
        var indexSearcher = ctx.PlanParams.IndexSearcher;
        var eA = ctx.Exec.CompoundExactFirst;
        var eB = ctx.Exec.CompoundExactSecond;

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
        var compoundNumericXorMask = ctx.BuilderParams.CompoundFieldNumericXorMask;
        WriteCompoundFieldEncoding(keySpan.Slice(0, enc1.Size), enc1, ctx.Exec, compoundNumericXorMask);
        WriteCompoundFieldEncoding(keySpan.Slice(enc1.Size, enc2.Size), enc2, ctx.Exec, compoundNumericXorMask);
        keySpan[totalLen - 1] = (byte)enc1.Size;

        var compoundFieldMeta = indexSearcher.FieldMetadataBuilder(ctx.Exec.Plan.Template.CompoundExactName, hasBoost: false);

        return indexSearcher.TermQuery(compoundFieldMeta, new Slice(keyBuf));
    }

    private static bool TryCreateCompoundFieldMatch(ref InstCtx ctx, out string rejectReason)
    {
        if (ctx.Exec.CompoundFieldDrivingClause is null || ctx.Exec.Plan.Template.CompoundFieldSortName is null)
        {
            rejectReason = "no compound field matches this query's filter-and-sort shape";
            return false;
        }

        if (ctx.Exec.Plan.AllNegated)
        {
            rejectReason = "every filter is a negation (not/!=), so there is no term to drive the scan";
            return false;
        }

        var driving = ctx.Exec.CompoundFieldDrivingClause;
        var field2Range = ctx.Exec.CompoundFieldField2Range;
        var execs = ctx.Exec.Executions;
        for (int i = 0; i < execs.Count; i++)
        {
            if (ReferenceEquals(execs[i], driving) || ReferenceEquals(execs[i], field2Range))
                continue;
            if (IsClauseBoosted(execs[i]))
            {
                rejectReason = "a filter uses boosting, which needs scoring this scan can't do";
                return false;
            }
        }

        if (ctx.Exec.Plan.CompoundFieldResidualSet is null)
        {
            rejectReason = "a filter can't be checked per-document during the scan";
            return false;
        }

        // Null / missing guard for the bare shape (equality on field1, ORDER BY field2, no clause on field2). The
        // compound walk emits field2's null/missing docs where their marker sorts in the tree (after real values),
        // which does NOT match NullsSortMode / the SortingMatch fallback (nulls-first by default). A field2 range
        // clause excludes nulls, so the walk is safe; otherwise fall back to bitmap + SortingMatch. (Mirrors the
        // single-field MayHaveMissingEntries guard, extended to nulls since the compound scan skips both the null
        // and non-existing posting-list merges SortedDrivingMatch would apply.)
        if (field2Range is null && ctx.OrderByFields is { Length: 1 })
        {
            var sortField = ctx.OrderByFields[0];
            if (sortField.MayHaveMissingEntries ||
                ctx.PlanParams.IndexSearcher.TryGetPostingListForNull(in sortField.Field, out _))
            {
                rejectReason = "the sort field has null/missing values and no range filter to exclude them, so the scan would order nulls wrong";
                return false;
            }
        }

        rejectReason = null;
        return true;
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
                EncodeNumericValue(buf.ToSpan(), PackedParam.TypeLong, packed.Param1, ctx.Exec, ctx.BuilderParams.CompoundFieldNumericXorMask);
                return new Slice(buf);
            }
            case PackedParam.TypeDouble:
            {
                field1ValueStrForIntrospection = ctx.WantTimings ? ctx.Exec.DoubleValues[packed.Param1].ToString(CultureInfo.InvariantCulture) : null;
                ctx.PlanParams.Allocator.Allocate(sizeof(long), out ByteString buf);
                EncodeNumericValue(buf.ToSpan(), PackedParam.TypeDouble, packed.Param1, ctx.Exec, ctx.BuilderParams.CompoundFieldNumericXorMask);
                return new Slice(buf);
            }
            default:
                field1ValueStrForIntrospection = null;
                return default;
        }
    }

    private static bool TryCreateSimpleFieldDirectScan(ref InstCtx ctx, out string rejectReason)
    {
        if (ctx.OrderByFields is not { Length: not 0 })
        {
            rejectReason = "the query has no ORDER BY for the scan to follow";
            return false;
        }

        if (ctx.OrderByFields.Length > 2)
        {
            rejectReason = "ORDER BY has more than 2 fields (a direct scan supports at most 2)";
            return false;
        }

        bool hasTieBreak = ctx.OrderByFields.Length == 2;
        if (hasTieBreak)
        {
            var tieBreakType = ctx.OrderByFields[1].FieldType;
            if (tieBreakType is not (MatchCompareFieldType.Integer or MatchCompareFieldType.Floating or MatchCompareFieldType.Sequence))
            {
                rejectReason = "the secondary (tie-break) ORDER BY field isn't a number or string";
                return false;
            }
        }

        var execs = ctx.Exec.Executions;
        bool isFullScan = execs is not { Count: not 0 };

        if (isFullScan)
        {
            if (ctx.Exec.Plan.AllNegated)
            {
                rejectReason = "every filter is a negation (not/!=), so there is no term to drive the scan";
                return false;
            }
            if (ctx.OrderByFields[0].MayHaveMissingEntries)
            {
                rejectReason = "some documents have no value for the sort field (a direct scan can't place them in order)";
                return false;
            }
            if (ctx.OrderByFields[0].FieldType is not (MatchCompareFieldType.Sequence or MatchCompareFieldType.Integer or MatchCompareFieldType.Floating))
            {
                rejectReason = "the sort field isn't a number or string type";
                return false;
            }
            rejectReason = null;
            return true;
        }

        if (ctx.Exec.SortDrivingClause is null)
        {
            rejectReason = "no equals/range filter on the sort field to drive the scan";
            return false;
        }

        // A driving clause on a multi-valued sort field cannot be elided from the residual: the
        // residual set excludes the driving clause assuming the in-order tree walk enforces it, but
        // SortedDrivingMatch walks every posting of a multi-valued field, so documents matching the
        // driving term under one value AND a different value elsewhere are emitted unfiltered. Fall
        // back to bitmap + SortingMatch, which applies the clause as a real filter.
        if (ctx.PlanParams.IndexSearcher.HasMultipleTermsInField(ctx.OrderByFields[0].Field))
        {
            rejectReason = "the sort field holds multiple values per document, so its filter can't be safely skipped during the walk";
            return false;
        }

        if (ctx.Exec.Plan.DirectScanResidualSet is null)
        {
            rejectReason = "a filter can't be checked per-document during the scan";
            return false;
        }

        rejectReason = null;
        return true;
    }

    private static bool ResolveNullFirst(in OrderMetadata orderByField, NullsSortMode indexDefault, bool forward)
    {
        bool nullIsSmallest = (orderByField.NullsSortMode ?? indexDefault) == NullsSortMode.NullsSmallest;
        return forward ? nullIsSmallest : nullIsSmallest is false;
    }

    /// <summary>
    /// Resolves the entry budget for a sorted index-only scan. Normally the driving match yields entries already in
    /// ORDER BY order, so the first <c>take</c> (= pageSize + start) survivors ARE the answer and the scan can stop
    /// early. Two situations break that assumption and require streaming the whole sorted tree (TakeAll):
    /// <list type="bullet">
    /// <item>A server-side <c>filter</c> clause is applied AFTER the index produces results, so an entry the tree
    /// yields is only a *candidate* — the index must keep streaming until the filter has accepted enough (bounded
    /// server-side by FilterLimit), else filtered+sorted queries truncate before reaching matching documents.</item>
    /// <item>The client requested statistics (<c>SkipStatistics == false</c>) or this is a count query: the read
    /// operation needs the exact <c>TotalResults</c>, which it derives by draining the match. Early-stopping at
    /// <c>take</c> would report only the page-sized prefix as the total. For a no-residual full scan this drain
    /// reads no entries (it just enumerates ids), matching the old SortingMatch behaviour.</item>
    /// </list>
    /// </summary>
    private static int ResolveSortedScanTake(QueryBuilderParameters builderParams)
    {
        // Early-stopping at the page limit is only safe when nothing forces a full drain: no post-index filter to
        // satisfy, and no exact total to report. Either one means stream the whole sorted tree.
        if (HasServerSideFilter(builderParams) || ConsumesExactTotal(builderParams))
            return Constants.IndexSearcher.TakeAll;

        return builderParams?.Take ?? Constants.IndexSearcher.TakeAll;
    }

    /// <summary>
    /// Whether the up-front known-total optimisation (resolving TotalResults from posting-list headers / O(1)
    /// metadata instead of draining the scan to count it) is applicable for this read, independent of the plan
    /// shape. It pays off only when the read actually <see cref="ConsumesExactTotal">consumes the total</see> and
    /// stays correct only when there is no <see cref="HasServerSideFilter">server-side filter</see> (which would
    /// make the header count overcount the survivors). When false, the total must come from draining the scan.
    /// </summary>
    private static bool CanResolveKnownTotal(QueryBuilderParameters builderParams)
        => ConsumesExactTotal(builderParams) && HasServerSideFilter(builderParams) == false;

    /// <summary>
    /// The read operation needs the exact <c>TotalResults</c>: it answers a count query, or it reports statistics
    /// (<c>SkipStatistics == false</c>, the default). When false the page bound is the only limit that matters, so
    /// neither does a sorted scan have to drain past it nor is an up-front total worth computing.
    /// </summary>
    private static bool ConsumesExactTotal(QueryBuilderParameters builderParams)
        => builderParams?.Query is { IsCountQuery: true } or { SkipStatistics: false };

    /// <summary>
    /// A server-side <c>filter</c> clause runs AFTER the index produces results, so an index hit is only a
    /// candidate until the filter accepts it. That makes any index-side count (e.g. posting-list headers) an
    /// overcount of the surviving documents, and forces a sorted scan to keep streaming until the filter has
    /// accepted enough (bounded server-side by FilterLimit).
    /// </summary>
    private static bool HasServerSideFilter(QueryBuilderParameters builderParams)
        => builderParams?.Metadata?.Query?.Filter != null;

    /// <summary>
    /// Runs the header-only <see cref="IAggregationProvider.CountPostingsInRange"/> probe on a throwaway
    /// <paramref name="countMatch"/> provider (which it disposes), timing the walk and reporting how many in-range
    /// terms it visited. Returns the summed posting total, or -1 when the match is not a countable aggregation
    /// provider. The probe exhausts the provider's iterator, so <paramref name="countMatch"/> must be a fresh
    /// instance — never the one feeding the scan, which still has to read.
    /// </summary>
    private static long ProbeCountPostingsInRange(IQueryMatch countMatch, out long probeTicks, out int probeTerms)
    {
        probeTicks = -1; // -1 ticks marks "no probe ran" (the match was not countable).
        probeTerms = 0;
        try
        {
            if (countMatch is TermsProviderMatch { Provider: IAggregationProvider agg })
            {
                long t0 = Stopwatch.GetTimestamp();
                var stats = agg.CountPostingsInRange(0);
                probeTicks = Stopwatch.GetTimestamp() - t0;
                probeTerms = stats.Terms;
                return stats.Postings;
            }

            return -1;
        }
        finally
        {
            (countMatch as IDisposable)?.Dispose();
        }
    }

    /// <summary>
    /// The page size the residual-DirectScan cost model is allowed to assume. A residual scan only
    /// early-terminates at the page boundary when the executor's take is page-bounded; <see cref="ResolveSortedScanTake"/>
    /// returns <c>TakeAll</c> whenever a total-result count must be reported (<c>SkipStatistics == false</c>, the
    /// default), the query is a count query, or a post-filter is present. In those cases the scan enumerates every
    /// matching entry — doing a stored-entry read per entry — so the page no longer bounds the work. Modelling the
    /// page bound there would let the cost gate price a handful of reads when the scan actually reads the whole
    /// driving tree, so report the full matching set (<see cref="long.MaxValue"/>, clamped to the driving
    /// cardinality by the caller) instead.
    /// </summary>
    private static long ResolveEffectiveScanPageSize(QueryBuilderParameters builderParams)
    {
        return ResolveSortedScanTake(builderParams) == Constants.IndexSearcher.TakeAll
            ? long.MaxValue
            : builderParams.Query.PageSize;
    }

    private static string DescribeUnboundedScanTake(QueryBuilderParameters builderParams)
    {
        return builderParams switch
        {
            { Metadata.Query.Filter: not null } => "post-filter present", 
            { Query.IsCountQuery: true } => "count query",
            { Query.SkipStatistics: false } => "statistics requested (SkipStatistics=false, requires count)",
            _ => null
        };
    }

    private static IQueryMatch BuildSortedDrivingWithTieBreakMatch(InstCtx ctx, ITermsProvider provider, LowLevelTransaction llt, NullsSortMode indexDefaultNullsSortMode,
        IndexSearcher indexSearcher, bool nullFirst, int take)
    {
        bool secondaryNullIsSmallest = (ctx.OrderByFields[1].NullsSortMode ?? indexDefaultNullsSortMode) == NullsSortMode.NullsSmallest;
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

    // forward defaults to true for the bitmap/unsorted resolve path; the sorted direct-scan path passes the
    // ORDER BY direction so a sentinel-rewritten BETWEEN (>= / <=) drives its provider in the right direction.
    private static IQueryMatch ResolveSentinelRewrittenBetween(ClauseExecution exec, FieldMetadata fieldMeta,
        IndexSearcher indexSearcher, QueryExecution queryExec, bool forward = true)
    {
        if (exec.SentinelRewriteType == ClauseType.Exists)
            return indexSearcher.AllEntries();
        if (exec.SentinelRewriteType == ClauseType.LessThanOrEqual)
            return exec.PackedParamValue.RangeQuery(ClauseType.LessThanOrEqual, fieldMeta, indexSearcher, queryExec, forward);

        Debug.Assert(exec.SentinelRewriteType == ClauseType.GreaterThanOrEqual);
        IQueryMatch rangeMatch = exec.PackedParamValue.RangeQuery(ClauseType.GreaterThanOrEqual, fieldMeta, indexSearcher, queryExec, forward);
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

    internal static FieldMetadata ResolveFieldMetadata(ClauseInfo clause, ResolutionContext walkerCtx)
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

    private static void EncodeNumericValue(Span<byte> dest, int valueType, int paramIdx, QueryExecution exec, long numericXorMask)
    {
        // The double path is order-preserving via DoubleToSortableLong; only the raw signed-long path needs the
        // sign-flip mask, which must mirror the indexer (CoraxDocumentConverterBase) for the queried index.
        long raw = valueType == PackedParam.TypeDouble
            ? Bits.DoubleToSortableLong(exec.DoubleValues[paramIdx])
            : exec.LongValues[paramIdx] ^ numericXorMask;
        // Must produce byte-for-byte the key the indexer wrote. CoraxDocumentConverterBase.AppendLong stores
        // `SwapBytes(l ^ mask)` little-endian (i.e. big-endian/sortable order of `l ^ mask`); mirror it exactly —
        // a big-endian write here would re-swap the bytes and the seek would never match (zero rows).
        BinaryPrimitives.WriteInt64LittleEndian(dest, Bits.SwapBytes(raw));
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
    
    private static void WriteCompoundFieldEncoding(Span<byte> dest, CompoundFieldEncoding encoding, QueryExecution exec, long numericXorMask)
    {
        if (encoding.Packed.ValueType == PackedParam.TypeString)
        {
            encoding.Analyzed.CopyTo(dest);
            return;
        }
        EncodeNumericValue(dest, encoding.Packed.ValueType, encoding.SourceSlot, exec, numericXorMask);
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
    internal static MatchDispatch GetDispatch(ClauseExecution exec)
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
