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

        var orderByFields = GetSortMetadata(builderParameters, plan.Template);
        var queryMatch = Instantiate(plan, exec, orderByFields,
            planParams, builderParameters, walkerCtx, highlightingTerms, wantTimings, out var innerMatch, token);
        return new(queryMatch, innerMatch, queryMatch == innerMatch ? null : queryMatch, plan, exec, builderParameters, orderByFields);
    }


    private static (CompiledPlan, QueryExecution) Build(PlanTemplate template, PlanParameters planParams, QueryBuilderParameters builderParameters, ResolutionContext walkerCtx)
    {
        Span<byte> scratch = stackalloc byte[128];
        return new BuildResolver(template, planParams, builderParameters, walkerCtx, scratch).Resolve();
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

    internal static void PopulateClauseValues(ClauseExecution exec, BlittableJsonReaderObject queryParameters, ValueWriter writer, QueryBuilderParameters builderParameters,
        int parameterSlotCount, ref byte[] full)
    {
        RuntimeHelpers.EnsureSufficientExecutionStack();
        foreach (var it in exec.SubExecutions ?? [])
        {   // Always recurse into subclauses first (OrGroup/AndGroup have no binding of their own)
            PopulateClauseValues(it, queryParameters, writer, builderParameters, parameterSlotCount, ref full);
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
            // The cache key must capture the expression STRUCTURE (parameter names like $p0), not the
            // bound values: the compiled plan reads its operands from QueryParameters by name at
            // instantiation, so two MLT queries whose base-document expression differs only in the bound
            // value (e.g. id() = 'users/1' vs id() = 'users/2') legitimately share a plan, while two
            // queries that resolve to the same value but reference different parameter names (e.g.
            // id() = $p1 with options vs id() = $p0 without) must NOT share — otherwise the cached plan
            // reads the wrong parameter slot. GetTextWithAlias(parent: null) renders parameters as $pN.
            CacheKeyOverride = moreLikeThisCacheKeyPrefix + expression.GetTextWithAlias(parent: null),
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
                    CompoundExact: (> 0, > 0) and var (a, b)
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
        var eA = execs[ctx.Exec.Plan.CompoundExact.First];
        var eB = execs[ctx.Exec.Plan.CompoundExact.Second];

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
        if (ctx.Exec.Plan.CompoundField.DrivingClause < 0 || ctx.Exec.Plan.Template.CompoundFieldSortName is null)
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
            if (i == ctx.Exec.Plan.CompoundField.DrivingClause || i == ctx.Exec.Plan.CompoundField.Field2Range)
                continue;
            if (IsClauseBoosted(execs[i]))
            {
                rejectReason = "boosted clause found";
                return false;
            }
        }

        if (ctx.Exec.Plan.CompoundFieldResidualSet is null)
        {
            rejectReason = "scan predicate info is null";
            return false;
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

        if (ctx.Exec.Plan.DirectScanResidualSet is null)
        {
            rejectReason = "non-scannable residual clause";
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
