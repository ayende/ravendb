using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Primitives;
using Voron.Data.RoaringBitmaps;
using Corax.Querying.Matches.SortingMatches.Meta;
using Corax.Querying.Planning;
using Corax.Mappings;
using Corax.Utils;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Corax;
using VectorOptions = Raven.Client.Documents.Indexes.Vector.VectorOptions;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;
using Raven.Server.Documents.Indexes.Persistence.Corax.QueryOptimizer;
using Raven.Server.Documents.Indexes.VectorSearch;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Spatial4n.Shapes;
using Sparrow;
using Sparrow.Json;
using Constants = Corax.Constants;
using RavenConstants = Raven.Client.Constants;
using SpatialUnits = Raven.Client.Documents.Indexes.Spatial.SpatialUnits;
using IndexSearcher = Corax.Querying.IndexSearcher;
using SpatialRelation = Corax.Utils.Spatial.SpatialRelation;

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
    // ── Entry points ─────────────────────────────────────────────────────

    /// <summary>
    /// Plan → compile → resolve pipeline. On cache hit (template exists for this query text),
    /// skips AST parsing — re-resolves parameter values from the blittable, re-estimates
    /// cardinality, re-sorts, and it looks up the compiled delegate by ordering.
    /// On cache miss, parses the AST into a template and caches it.
    /// Both paths then: populate → sort → emit → compile (if needed) → resolve.
    /// </summary>
    public static IQueryMatch BuildAndCompile(
        PlanParameters planParams,
        QueryBuilderParameters builderParameters,
        out QueryExecution plan,
        out CompiledPlan compiledPlanOut,
        Dictionary<string, CoraxHighlightingTermIndex> highlightingTerms,
        bool wantTimings,
        CancellationToken token)
    {
        var indexSearcher = planParams.IndexSearcher;
        var queryText = planParams.Metadata.Query.QueryText;
        var planCache = indexSearcher.PlanCache;

        // Step 1: Get or build the clause template (structural, no values).
        // cmpxchg()/now()/today() are safe to cache: the template stores a DeferredExpression
        // delegate on the binding, and ResolveBindingScalar invokes it per execution with the
        // current builderParameters/queryParameters — the resolved value lives on ClauseExecution,
        // not on the cached binding.
        var template = planCache.TryGetTemplate(queryText) ?? ParseTemplate(planParams);

        // Step 2: Create per-execution state for each clause (template is immutable, not cloned)
        var clauses = new List<ClauseInfo>(template.Clauses.Length);
        var execList = new List<ClauseExecution>(template.Clauses.Length);
        foreach (var cached in template.Clauses)
        {
            clauses.Add(cached);
            execList.Add(CreateExecution(cached));
        }

        // Step 2b: Evaluate WHEN conditions and eliminate inactive clauses.
        //
        // Track which WHEN-guarded clauses survived in a bitmask keyed by template
        // position. Plans built from the same queryText but different WHEN-survival
        // subsets can produce the same (OperandOrdering, TypeSignature) cache key —
        // for example, [Attach==true, Number!=1] sorted with Attach first gives
        // ordering=1, which collides with the single-clause [Attach==true] plan that
        // arises when WHEN eliminates the NotEquals clause. We mix this survival mask
        // into the cache key below so each survival pattern gets its own slot.
        //
        // We index by the loop counter (template position), not by ClauseInfo.OriginalIndex:
        // negated/nested clauses (e.g. "not when(...)") carry the index from their local
        // inner sub-list, not the outer template position. Iterating clauses in reverse
        // and removing only later indices keeps `ci` aligned with the template position
        // at the point of removal.
        int whenSurvivalMask = 0;
        bool templateHasWhen = false;
        for (int ti = 0; ti < template.Clauses.Length; ti++)
        {
            if (template.Clauses[ti].WhenCondition == null)
                continue;
            templateHasWhen = true;
            whenSurvivalMask |= 1 << (ti & 31);
        }
        for (int ci = clauses.Count - 1; ci >= 0; ci--)
        {
            var whenCondition = clauses[ci].WhenCondition;
            if (whenCondition == null)
                continue;
            if (whenCondition(planParams.QueryParameters) == false)
            {
                whenSurvivalMask &= ~(1 << (ci & 31));
                clauses.RemoveAt(ci);
                execList.RemoveAt(ci);
            }
        }

        // Step 3: Populate parameter values into typed arrays
        var writer = new ValueWriter();
        for (int ci = 0; ci < clauses.Count; ci++)
            PopulateClauseValues(clauses[ci], execList[ci], planParams.QueryParameters, writer, builderParameters);

        // Step 3b: Constant propagation — simplify trivially-false/simple clauses
        bool isOr = template.IsOr;
        for (int ci = clauses.Count - 1; ci >= 0; ci--)
        {
            var c = clauses[ci];
            var e = execList[ci];

            // Contradictory BETWEEN: low > high → clause matches nothing
            if (c.ClauseType == ClauseType.Between && e.PackedParamValue.IsNone == false)
            {
                var p = e.PackedParamValue;
                if (p.Param2 != PackedParam.NoParamValue)
                {
                    bool contradictory = p.ValueType switch
                    {
                        PackedParam.TypeLong => writer.GetLongs()[p.Param1] > writer.GetLongs()[p.Param2],
                        PackedParam.TypeDouble => writer.GetDoubles()[p.Param1] > writer.GetDoubles()[p.Param2],
                        PackedParam.TypeString => string.Compare(writer.GetStrings()[p.Param1], writer.GetStrings()[p.Param2], StringComparison.Ordinal) > 0,
                        _ => false
                    };
                    if (contradictory)
                    {
                        // Mark with zero cardinality. EmitPlan handles:
                        // AND chain: zero-cardinality → empty result.
                        // OR chain: remove the clause (contributes nothing).
                        e.Cardinality = 0;
                        e.InTermCount = 0;
                        e.HasNullTerm = false;
                        c.ClauseType = ClauseType.In; // Reuse empty-IN elimination in EmitPlan
                    }
                }
            }

            // Single-value IN → convert to Equals (simpler plan, single PostingList lookup)
            if (c.ClauseType == ClauseType.In && e.InTermCount == 1 && e.HasNullTerm == false)
            {
                c.ClauseType = ClauseType.Equals;
            }
            // Null-only IN (InTermCount=0, HasNullTerm=true): already optimized —
            // EmitInOps skips OrRange (no non-null terms) and emits only the null-term
            // OrWithPostings op. No conversion needed.
        }

        // Step 4: Estimate cardinality (needs populated values)
        for (int ci = 0; ci < clauses.Count; ci++)
        {
            if (execList[ci].Cardinality < 0)
                execList[ci].Cardinality = EstimateCardinality(clauses[ci], execList[ci], indexSearcher, writer);
        }

        var executions = execList.ToArray();

        // Step 5: Sort operands by cardinality (sort clauses and executions in lockstep)
        if (!isOr)
        {
            // Build index array, sort by cardinality, then reorder both arrays
            var indices = new int[clauses.Count];
            for (int j = 0; j < indices.Length; j++) indices[j] = j;
            Array.Sort(indices, (a, b) =>
            {
                bool aNeg = clauses[a].IsNegated || clauses[a].ClauseType == ClauseType.NotEquals;
                bool bNeg = clauses[b].IsNegated || clauses[b].ClauseType == ClauseType.NotEquals;
                if (aNeg != bNeg)
                    return aNeg ? 1 : -1;
                return executions[a].Cardinality.CompareTo(executions[b].Cardinality);
            });
            var sortedClauses = new List<ClauseInfo>(clauses.Count);
            var sortedExecs = new ClauseExecution[clauses.Count];
            for (int j = 0; j < indices.Length; j++)
            {
                sortedClauses.Add(clauses[indices[j]]);
                sortedExecs[j] = executions[indices[j]];
            }
            clauses = sortedClauses;
            executions = sortedExecs;
        }
        else
        {
            int insertPos = 0;
            for (int j = 0; j < clauses.Count; j++)
            {
                if (clauses[j].ClauseType == ClauseType.AndGroup)
                {
                    ClauseInfo ag = clauses[j];
                    ClauseExecution agExec = executions[j];
                    clauses.RemoveAt(j);
                    var execListTmp = new List<ClauseExecution>(executions);
                    execListTmp.RemoveAt(j);
                    clauses.Insert(insertPos, ag);
                    execListTmp.Insert(insertPos, agExec);
                    executions = execListTmp.ToArray();
                    insertPos++;
                }
            }
        }

        // Step 6: Emit plan ops + attach spatial/vector post-filters.
        // clauses.Count == 0 can happen either because the template is AllEntries (no WHERE)
        // or because every WHERE clause was eliminated by a false WHEN condition — both reduce
        // to "match all entries".
        if (clauses.Count == 0)
        {
            plan = BuildAllEntriesPlan();
            plan.Executions = executions;
        }
        else
        {
            plan = EmitPlan(clauses, executions, isOr);
        }
        plan.LongValues = writer.GetLongs();
        plan.DoubleValues = writer.GetDoubles();
        plan.StringValues = writer.GetStrings();

        if (template.SpatialClauses != null || template.VectorClauses != null)
        {
            var spatialList = template.SpatialClauses != null ? new List<ClauseInfo>(template.SpatialClauses.Length) : null;
            var vectorList = template.VectorClauses != null ? new List<ClauseInfo>(template.VectorClauses.Length) : null;
            ClauseExecution[] spatialExecs = null;
            ClauseExecution[] vectorExecs = null;
            if (spatialList != null)
            {
                spatialExecs = new ClauseExecution[template.SpatialClauses.Length];
                for (int si = 0; si < template.SpatialClauses.Length; si++)
                {
                    var sc = template.SpatialClauses[si];
                    var scExec = new ClauseExecution();
                    PopulateClauseValues(sc, scExec, planParams.QueryParameters, writer, builderParameters);
                    spatialList.Add(sc);
                    spatialExecs[si] = scExec;
                }
            }
            if (vectorList != null)
            {
                vectorExecs = new ClauseExecution[template.VectorClauses.Length];
                for (int vi = 0; vi < template.VectorClauses.Length; vi++)
                {
                    var vc = template.VectorClauses[vi];
                    var vcExec = new ClauseExecution();
                    PopulateClauseValues(vc, vcExec, planParams.QueryParameters, writer, builderParameters);
                    vectorList.Add(vc);
                    vectorExecs[vi] = vcExec;
                }
            }
            AttachPostFilterPhases(plan, spatialList, spatialExecs, vectorList, vectorExecs);
            // Re-store typed arrays after spatial/vector population may have added more
            plan.LongValues = writer.GetLongs();
            plan.DoubleValues = writer.GetDoubles();
            plan.StringValues = writer.GetStrings();
        }

        // Step 7: Boost handling
        if (planParams.HasBoost)
        {
            var ops = plan.Ops;
            if (ops != null)
                for (int i = 0; i < ops.Length; i++)
                    ops[i].Dispatch = MatchDispatch.QueryMatch;
            plan.OperandOrdering |= (1 << 30);
        }

        // Step 8: Look up or compile the delegate for this ordering.
        // When the template has WHEN clauses, prepend 4 bytes of the survival mask
        // to FullKinds so plans with different surviving subsets get distinct cache slots
        // — otherwise [Attach, Number!=1] (sorted, ord=1) collides with the 1-clause
        // [Attach] survivor (also ord=1) since both share the same queryText.
        if (templateHasWhen)
        {
            var existing = plan.FullKinds ?? Array.Empty<byte>();
            var combined = new byte[4 + existing.Length];
            combined[0] = (byte)(whenSurvivalMask & 0xFF);
            combined[1] = (byte)((whenSurvivalMask >> 8) & 0xFF);
            combined[2] = (byte)((whenSurvivalMask >> 16) & 0xFF);
            combined[3] = (byte)((whenSurvivalMask >> 24) & 0xFF);
            existing.CopyTo(combined, 4);
            plan.FullKinds = combined;
        }
        var compiledPlan = planCache.Get(queryText, plan.OperandOrdering, plan.TypeSignature, plan.FullKinds);
        if (compiledPlan == null)
        {
            compiledPlan = new CompiledPlan
            {
                CompiledDelegate = QueryIlEmitter.EmitDelegate(plan, out var explainText, emitTimings: false),
                CompiledTimedDelegate = QueryIlEmitter.EmitDelegate(plan, out _, emitTimings: true),
                CompiledEntryPredicate = ResidualScanIlEmitter.EmitDelegate(plan.ScanPredicateInfos, out var scanExplain),

                ExplainSource = explainText + "\n" + scanExplain,
                Ordering = plan.OperandOrdering,
                TypeSignature = plan.TypeSignature,
                FullKinds = plan.FullKinds,
                InspectionTemplate = BuildInspectionTemplate(plan)
            };
            planCache.Add(queryText, compiledPlan, template);
        }

        compiledPlanOut = compiledPlan;
        var resolvedMatches = ResolveMatches(plan, indexSearcher, planParams, builderParameters);
        var termSources = ResolveTermSources(plan, indexSearcher, planParams, builderParameters);
        var termsProviders = ResolveTermsProviders(plan, indexSearcher, planParams, builderParameters);
        ExtractScanParameters(plan, indexSearcher,
            out var longParams, out var doubleParams, out var sliceParams, out var fieldRootPages);

        if (highlightingTerms != null)
            PopulateHighlightingTerms(plan, highlightingTerms, planParams.Metadata);

        var compiledMatch = new CompiledQueryMatch(
            compiledPlan, plan.RequiredBitmaps, plan.Ops?.Length ?? 0, resolvedMatches, termSources, termsProviders,
            indexSearcher, planParams.Allocator, wantTimings, token)
        {
            InRangeCounts = plan.InRangeCounts,
            ScanPredicateInfos = plan.ScanPredicateInfos,
            ScanLongParams = longParams,
            ScanDoubleParams = doubleParams,
            ScanSliceParams = sliceParams,
            ScanFieldRootPages = fieldRootPages
        };
        IQueryMatch result = compiledMatch;

        // Spatial post-filter phase: AND each spatial match with the candidate bitmap.
        if (plan.SpatialFilters is { Length: > 0 })
        {
            var spatialFilters = new IQueryMatch[plan.SpatialFilters.Length];
            for (int sf = 0; sf < plan.SpatialFilters.Length; sf++)
                spatialFilters[sf] = resolvedMatches[plan.SpatialFilters[sf].MatchIndex];
            result = new PostFilterMatch(result, spatialFilters);
        }

        // Vector select phase: each vector wraps the bitmap so far as its filter source.
        if (plan.VectorSelects is { Length: > 0 } && builderParameters != null)
        {
            var vectorItems = ResolveVectorItems(plan, builderParameters);
            bool hasActualFilter = !plan.IsAllEntries || plan.SpatialFilters is { Length: > 0 };
            IQueryMatch vectorFilter = hasActualFilter ? result : null;
            foreach (var item in vectorItems)
                result = item.Materialize(vectorFilter);
        }

        return result;
    }

    /// <summary>
    /// Build a query match from a sub-expression (e.g. the inner BinaryExpression
    /// of a moreLikeThis clause). Parses and resolves the expression directly
    /// without going through the full plan/compile pipeline.
    /// </summary>
    public static IQueryMatch BuildFromSubExpression(QueryBuilderParameters builderParams, QueryExpression expression)
    {
        var indexSearcher = builderParams.IndexSearcher;
        var clauses = new List<ClauseInfo>();
        bool hasMixed = false;
        ParseExpression(expression, indexSearcher, clauses, builderParams.QueryParameters,
            builderParams.Metadata, ref hasMixed);

        if (clauses.Count == 0)
            return indexSearcher.AllEntries();

        // Populate parameters for the sub-expression clauses
        var writer = new ValueWriter();
        var subExecs = new ClauseExecution[clauses.Count];
        for (int ci = 0; ci < clauses.Count; ci++)
        {
            subExecs[ci] = CreateExecution(clauses[ci]);
            PopulateClauseValues(clauses[ci], subExecs[ci], builderParams.QueryParameters, writer, builderParams);
        }

        var subPlan = new QueryExecution
        {
            LongValues = writer.GetLongs(),
            DoubleValues = writer.GetDoubles(),
            StringValues = writer.GetStrings(),
            Executions = subExecs
        };

        if (clauses.Count == 1)
            return ResolveClause(clauses[0], subExecs[0], indexSearcher, subPlan, builderParams: builderParams);

        // Multiple clauses (AND chain) — resolve each and AND them via bitmap
        var bitmap = new BitmapMatch(indexSearcher.Allocator);
        var temp = new RoaringBitmap(indexSearcher.Allocator);
        bool first = true;
        for (int ci2 = 0; ci2 < clauses.Count; ci2++)
        {
            var clause = clauses[ci2];
            var match = ResolveClause(clause, subExecs[ci2], indexSearcher, subPlan, builderParams: builderParams);
            if (first)
            {
                QueryPrimitives.FillFromMatch(match, ref bitmap.BitmapState);
                first = false;
            }
            else
            {
                QueryPrimitives.AndWithMatch(match, ref bitmap.BitmapState, ref temp);
            }
        }
        temp.Dispose();
        return bitmap;
    }

    // ── Template caching ──────────────────────────────────────────────

    /// <summary>Create a ClauseExecution for a clause, including sub-executions for OrGroup/AndGroup.</summary>
    private static ClauseExecution CreateExecution(ClauseInfo clause)
    {
        var exec = new ClauseExecution();
        if (clause.OrSubClauses is { Count: > 0 })
        {
            exec.OrSubExecutions = new ClauseExecution[clause.OrSubClauses.Count];
            for (int i = 0; i < clause.OrSubClauses.Count; i++)
                exec.OrSubExecutions[i] = CreateExecution(clause.OrSubClauses[i]);
        }
        if (clause.AndSubClauses is { Count: > 0 })
        {
            exec.AndSubExecutions = new ClauseExecution[clause.AndSubClauses.Count];
            for (int i = 0; i < clause.AndSubClauses.Count; i++)
                exec.AndSubExecutions[i] = CreateExecution(clause.AndSubClauses[i]);
        }
        return exec;
    }

    /// <summary>Resolve a single clause's parameter value using its cached binding.
    /// Called for each clause during parameter population (both first execution and cache hit).
    /// The optional builderParameters is needed to resolve deferred method expressions (cmpxchg, now, today).</summary>
    private static void PopulateClauseValues(ClauseInfo clause, ClauseExecution exec, BlittableJsonReaderObject queryParameters, ValueWriter writer, QueryBuilderParameters builderParameters = null)
    {
        // Always recurse into subclauses first (OrGroup/AndGroup have no binding of their own)
        if (clause.OrSubClauses != null && exec.OrSubExecutions != null)
            for (int si = 0; si < clause.OrSubClauses.Count; si++)
                PopulateClauseValues(clause.OrSubClauses[si], exec.OrSubExecutions[si], queryParameters, writer, builderParameters);
        if (clause.AndSubClauses != null && exec.AndSubExecutions != null)
            for (int si = 0; si < clause.AndSubClauses.Count; si++)
                PopulateClauseValues(clause.AndSubClauses[si], exec.AndSubExecutions[si], queryParameters, writer, builderParameters);

        // Resolve boost factor if this clause is boosted
        if (clause.HasBoost && clause.Bindings is { Length: > 0 })
        {
            ResolveBoostFactor(clause, exec, queryParameters);
        }

        switch (clause.ClauseType)
        {
            // Spatial and vector resolve via their binding array.
            case ClauseType.Spatial when clause.Bindings is { Length: > 0 }:
                ResolveSpatialFromBindings(clause, exec, queryParameters);
                return;
            case ClauseType.Vector when clause.Bindings is { Length: > 0 }:
                ResolveVectorFromBindings(clause, exec, queryParameters);
                return;
        }

        var bindings = clause.Bindings;
        if (bindings == null || bindings.Length == 0)
            return;

        switch (clause.ClauseType)
        {
            // BETWEEN: two values at Bindings[0] (low) and Bindings[1] (high)
            case ClauseType.Between:
                var (low, lowType) = ResolveBindingScalar(bindings[BindingIndex.BetweenLow], queryParameters, builderParameters);
                var (high, _) = ResolveBindingScalar(bindings[BindingIndex.BetweenHigh], queryParameters, builderParameters);
                exec.TermValueType = lowType;
                exec.PackedParamValue = writer.AddPair(low, high, ToValueTokenType(lowType));
                return;
            case ClauseType.In or ClauseType.AllIn:
                // IN/AllIn: each binding is a term (literal or parameter, possibly array-expanding)
                ResolveInFromBindings(exec, queryParameters, writer, bindings);
                break;
            default:
                // Simple clause (Equals, Range, Search, Regex, etc.): single value at Bindings[0]
                var (value, valueType) = ResolveBindingScalar(bindings[BindingIndex.Value], queryParameters, builderParameters);
                exec.TermValueType = valueType;
                exec.PackedParamValue = writer.Add(value, ToValueTokenType(valueType));
                break;
        }
    }

    private static void ResolveBoostFactor(ClauseInfo clause, ClauseExecution exec, BlittableJsonReaderObject queryParameters)
    {
        var (boostVal, boostType) = ResolveBindingScalar(clause.Bindings[^1], queryParameters);
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

    private static void ResolveInFromBindings(ClauseExecution exec, BlittableJsonReaderObject queryParameters, ValueWriter writer, ParameterBinding[] bindings)
    {
        var termTypes = new List<ParamValueType>();
        var resolvedValues = new List<object>();
        bool hasNullTerm = false;

        foreach (var it in bindings)
        {
            if (it.LiteralType != ParamValueType.Parameter)
            {
                resolvedValues.Add(it.LiteralValue);
                termTypes.Add(it.LiteralType);
                if (it.LiteralValue == null)
                {
                    hasNullTerm = true;
                }
            }
            else
            {
                // Parameter — resolve from blittable. It may be scalar or array.
                object inRaw = null;
                queryParameters?.TryGet(it.ParameterName, out inRaw);
                if (inRaw is BlittableJsonReaderArray arr)
                {
                    // Array parameter — expand each element
                    foreach (var elem in arr)
                    {
                        var (elemVal, elemType) = ResolveInValue(elem, ValueTokenType.Parameter);
                        resolvedValues.Add(elemVal);
                        termTypes.Add(ToParamValueType(elemType));
                        if (elemVal == null)
                            hasNullTerm = true;
                    }
                }
                else if (inRaw != null)
                {
                    // Scalar parameter — single term
                    var (singleVal, singleType) = ResolveInValue(inRaw, ValueTokenType.Parameter);
                    resolvedValues.Add(singleVal);
                    termTypes.Add(ToParamValueType(singleType));
                }
                else
                {
                    resolvedValues.Add(null);
                    termTypes.Add(ParamValueType.Null);
                    hasNullTerm = true;
                }
            }
        }

        // Determine dominant type
        ParamValueType dominantType = ParamValueType.Null;
        for (int i = 0; i < termTypes.Count; i++)
        {
            if (resolvedValues[i] == null) continue;
            if (dominantType == ParamValueType.Null)  
                dominantType = termTypes[i];
        }
        if (dominantType == ParamValueType.Null) 
            dominantType = ParamValueType.String;

        int packedType = dominantType switch
        {
            ParamValueType.Long => PackedParam.TypeLong,
            ParamValueType.Double => PackedParam.TypeDouble,
            _ => PackedParam.TypeString
        };
        int startIdx = packedType switch
        {
            PackedParam.TypeLong => writer.LongCount,
            PackedParam.TypeDouble => writer.DoubleCount,
            _ => writer.StringCount
        };
        // Only store non-null values in the typed array. Null terms are handled
        // separately via HasNullTerm — one null-term lookup covers all nulls.
        int nonNullCount = 0;
        for (int i = 0; i < resolvedValues.Count; i++)
        {
            if (resolvedValues[i] != null)
            {
                writer.Add(resolvedValues[i], ToValueTokenType(dominantType));
                nonNullCount++;
            }
        }

        exec.PackedParamValue = new PackedParam(packedType, startIdx);
        exec.InTermCount = nonNullCount;
        exec.HasNullTerm = hasNullTerm;
    }

    /// <summary>Resolve spatial parameters from cached bindings (no MethodExpression dependency).</summary>
    private static void ResolveSpatialFromBindings(ClauseInfo clause, ClauseExecution exec, BlittableJsonReaderObject queryParameters)
    {
        var bindings = clause.Bindings;
        var sp = new SpatialParams();

        // [0] = distanceErrorPct
        if (bindings.Length > 0 && bindings[BindingIndex.SpatialDistErrPct] != null)
        {
            var (depVal, _) = ResolveBindingScalar(bindings[BindingIndex.SpatialDistErrPct], queryParameters);
            sp.DistanceErrorPct = depVal != null ? Convert.ToDouble(depVal) : -1;
        }

        // Shape type determined by the number of bindings:
        // circle has 5 (distErrPct, radius, lat, lng, units), WKT has 3 (distErrPct, wkt, units)
        if (bindings.Length >= BindingIndex.SpatialCircleBindingCount - 1) // circle: at least distErrPct + radius + lat + lng
        {
            sp.ShapeType = SpatialShapeType.Circle;
            var (r, _) = ResolveBindingScalar(bindings[BindingIndex.SpatialRadius], queryParameters);
            var (lat, _) = ResolveBindingScalar(bindings[BindingIndex.SpatialLatitude], queryParameters);
            var (lng, _) = ResolveBindingScalar(bindings[BindingIndex.SpatialLongitude], queryParameters);
            sp.CircleRadius = Convert.ToDouble(r);
            sp.CircleLatitude = Convert.ToDouble(lat);
            sp.CircleLongitude = Convert.ToDouble(lng);
            if (bindings.Length > BindingIndex.SpatialUnits && bindings[BindingIndex.SpatialUnits] != null)
            {
                var (u, _) = ResolveBindingScalar(bindings[BindingIndex.SpatialUnits], queryParameters);
                if (u != null && Enum.TryParse(typeof(SpatialUnits), u.ToString(), true, out var su))
                    sp.Units = (SpatialUnits)su == SpatialUnits.Kilometers
                            ? global::Corax.Utils.Spatial.SpatialUnits.Kilometers
                            : global::Corax.Utils.Spatial.SpatialUnits.Miles;
            }
        }
        else // WKT: distErrPct, wkt, [units]
        {
            sp.ShapeType = SpatialShapeType.Wkt;
            if (bindings.Length > BindingIndex.SpatialWkt && bindings[BindingIndex.SpatialWkt] != null)
            {
                var (wkt, _) = ResolveBindingScalar(bindings[BindingIndex.SpatialWkt], queryParameters);
                sp.Wkt = wkt?.ToString();
                if (bindings.Length > BindingIndex.SpatialWktUnits && bindings[BindingIndex.SpatialWktUnits] != null)
                {
                    var (u, _) = ResolveBindingScalar(bindings[BindingIndex.SpatialWktUnits], queryParameters);
                    if (u != null && Enum.TryParse(typeof(SpatialUnits), u.ToString(), true, out var su))
                        sp.Units = (SpatialUnits)su == SpatialUnits.Kilometers
                            ? global::Corax.Utils.Spatial.SpatialUnits.Kilometers
                            : global::Corax.Utils.Spatial.SpatialUnits.Miles;
                }
            }
        }

        exec.Spatial = sp;
    }

    /// <summary>Resolve vector parameters from cached bindings (no MethodExpression dependency).</summary>
    private static void ResolveVectorFromBindings(ClauseInfo clause, ClauseExecution exec, BlittableJsonReaderObject queryParameters)
    {
        var bindings = clause.Bindings;
        var vec = new VectorParams { Method = clause.VectorMethod };

        // [1]=minimumMatch, [2]=numberOfCandidates, [3]=aiTask
        if (bindings.Length > BindingIndex.VectorMinMatch && bindings[BindingIndex.VectorMinMatch] != null)
        {
            var (simVal, simType) = ResolveBindingScalar(bindings[BindingIndex.VectorMinMatch], queryParameters);
            if (simVal != null && simType != ParamValueType.Null)
                vec.MinimumMatch = simType == ParamValueType.Double ? (float)(double)simVal
                    : simType == ParamValueType.Long ? (long)simVal : -1;
        }
        if (bindings.Length > BindingIndex.VectorCandidates && bindings[BindingIndex.VectorCandidates] != null)
        {
            var (candVal, candType) = ResolveBindingScalar(bindings[BindingIndex.VectorCandidates], queryParameters);
            if (candVal != null && candType != ParamValueType.Null)
                vec.NumberOfCandidates = Convert.ToInt32(candVal);
        }
        if (bindings.Length > BindingIndex.VectorAiTask && bindings[BindingIndex.VectorAiTask] != null)
        {
            var (taskVal, _) = ResolveBindingScalar(bindings[BindingIndex.VectorAiTask], queryParameters);
            vec.AiTaskName = taskVal?.ToString();
        }

        // [0]=vector value (may be scalar, array, or blittable object)
        if (bindings.Length > BindingIndex.VectorValue && bindings[BindingIndex.VectorValue] != null)
        {
            var (val, valType) = ResolveBindingRaw(bindings[BindingIndex.VectorValue], queryParameters);
            vec.ResolvedValue = val;
            vec.ResolvedValueType = valType;
            // For scalar parameters, resolve the native type
            if (valType == ParamValueType.Parameter && val is not (BlittableJsonReaderArray or BlittableJsonReaderObject))
            {
                var (resolved, resolvedType) = ResolveParameterValue(val);
                vec.ResolvedValue = resolved;
                vec.ResolvedValueType = ToParamValueType(resolvedType);
            }
        }

        exec.Vector = vec;
    }

    /// <summary>Look up a binding's value from the blittable. Returns the RAW value —
    /// callers must check for arrays/objects before calling ResolveParameterValue.</summary>
    private static (object Value, ParamValueType Type) ResolveBindingRaw(ParameterBinding binding, BlittableJsonReaderObject queryParameters)
    {
        if (binding.LiteralType != ParamValueType.Parameter)
            return (binding.LiteralValue, binding.LiteralType);
        if (queryParameters != null && queryParameters.TryGet(binding.ParameterName, out object raw) && raw != null)
            return (raw, ParamValueType.Parameter); // raw from blittable — caller decides how to interpret
        return (null, ParamValueType.Null);
    }

    /// <summary>Resolve a binding to a scalar value. Asserts the result is not an array/object.
    /// For parameters that might be arrays, use ResolveBindingRaw and handle arrays first.
    /// The optional builderParameters is needed to resolve deferred method expressions (cmpxchg, now, today).</summary>
    private static (object Value, ParamValueType Type) ResolveBindingScalar(ParameterBinding binding, BlittableJsonReaderObject queryParameters, QueryBuilderParameters builderParameters = null)
    {
        // Handle deferred method expressions (cmpxchg, now, today) — resolve at execution time
        if (binding.DeferredExpression != null)
        {
            var value = binding.DeferredExpression(builderParameters, queryParameters);
            if (value == null)
                return (null, ParamValueType.Null);
            var (val, valType) = ResolveParameterValue(value);
            return (val, ToParamValueType(valType));
        }

        if (binding.LiteralType != ParamValueType.Parameter)
            return (binding.LiteralValue, binding.LiteralType);
        if (queryParameters != null && queryParameters.TryGet(binding.ParameterName, out object raw) && raw != null)
        {
            var (val, type) = ResolveParameterValue(raw); // asserts not array/object
            return (val, ToParamValueType(type));
        }
        return (null, ParamValueType.Null);
    }

    // ── Typed dispatch helpers ───────────────────────────────────────────

    /// <summary>Create a TermQuery using the pre-resolved typed value from the plan's arrays.</summary>
    private static IQueryMatch TermQueryFromParam(PackedParam packed, FieldMetadata fieldMeta,
        IndexSearcher indexSearcher, QueryExecution plan)
    {
        int idx = packed.Param1;
        return packed.ValueType switch
        {
            PackedParam.TypeLong => indexSearcher.TermQuery(fieldMeta, plan.LongValues[idx]),
            PackedParam.TypeDouble => indexSearcher.TermQuery(fieldMeta, plan.DoubleValues[idx]),
            _ => indexSearcher.TermQuery(fieldMeta, plan.StringValues[idx])
        };
    }

    /// <summary>Get a posting-list ID using the pre-resolved typed value.</summary>
    private static long GetTermPostingListIdFromParam(PackedParam packed, FieldMetadata fieldMeta,
        IndexSearcher indexSearcher, QueryExecution plan)
    {
        int idx = packed.Param1;
        return packed.ValueType switch
        {
            PackedParam.TypeLong => indexSearcher.GetTermPostingListId(fieldMeta, plan.LongValues[idx]),
            PackedParam.TypeDouble => indexSearcher.GetTermPostingListId(fieldMeta, plan.DoubleValues[idx]),
            _ => indexSearcher.GetTermPostingListId(fieldMeta, plan.StringValues[idx])
        };
    }

    // ── Match resolution ─────────────────────────────────────────────────

    /// <summary>
    /// Resolve clause infos to IQueryMatch instances for execution.
    /// Uses existing IndexSearcher methods (TermQuery, etc.) which handle
    /// all the complexity of analyzer application, CompactKey encoding,
    /// posting list resolution, etc.
    /// </summary>
    private static IQueryMatch[] ResolveMatches(QueryExecution plan, IndexSearcher indexSearcher,
        PlanParameters parameters = null, QueryBuilderParameters builderParams = null)
    {
        var clauses = plan.Clauses ?? [];
        // All-entries plan with possible post-filter phases
        if (plan.IsAllEntries)
        {
            int spatialCount = plan.SpatialFilters?.Length ?? 0;
            int vectorCount = plan.VectorSelects?.Length ?? 0;
            int totalExtra = spatialCount + vectorCount;
            if (totalExtra == 0)
                return [indexSearcher.AllEntries()];

            var allEntriesMatches = new IQueryMatch[1 + totalExtra];
            allEntriesMatches[0] = indexSearcher.AllEntries();
            int matchOfs = 1;
            if (plan.SpatialFilters != null)
            {
                for (int i = 0; i < plan.SpatialFilters.Length; i++)
                    allEntriesMatches[matchOfs++] = ResolveClause(plan.SpatialFilters[i].Clause, plan.SpatialFilters[i].Exec ?? new ClauseExecution(), indexSearcher, plan, parameters, builderParams);
            }
            if (plan.VectorSelects != null)
            {
                for (int i = 0; i < plan.VectorSelects.Length; i++)
                    allEntriesMatches[matchOfs++] = ResolveClause(plan.VectorSelects[i].Clause, plan.VectorSelects[i].Exec ?? new ClauseExecution(), indexSearcher, plan, parameters, builderParams);
            }
            return allEntriesMatches;
        }

        if (clauses.Count == 0)
            return [];

        var execs = plan.Executions;

        // Standalone NotEquals pattern: Fill(AllEntries) + ANDNOT(term).
        if (clauses.Count == 1 && clauses[0].IsNegated && !plan.AllNegated)
        {
            var clause = clauses[0];
            var exec0 = execs[0];
            return
            [
                indexSearcher.AllEntries(),
                TermQueryFromParam(exec0.PackedParamValue, indexSearcher.FieldMetadataBuilder(clause.FieldName), indexSearcher, plan)
            ];
        }

        var matches = new IQueryMatch[CountMatchSlots(clauses, execs, plan.IsAllEntries, plan.AllNegated)];
        int matchIdx = 0;
        for (int ci = 0; ci < clauses.Count; ci++)
        {
            ClauseInfo clause = clauses[ci];
            ClauseExecution exec = execs[ci];
            switch (clause.ClauseType)
            {
                case ClauseType.OrGroup when clause.OrSubClauses is { Count: > 0 }:
                {
                    for (int si = 0; si < clause.OrSubClauses.Count; si++)
                    {
                        var sub = clause.OrSubClauses[si];
                        var subExec = exec.OrSubExecutions[si];
                        var match = ResolveClause(sub, subExec, indexSearcher, plan, parameters, builderParams);
                        if (subExec.BoostFactor > 0)
                            match = indexSearcher.Boost(match, subExec.BoostFactor);
                        matches[matchIdx++] = match;
                    }

                    break;
                }
                case ClauseType.AndGroup when clause.AndSubClauses is { Count : > 0 }:
                {
                    for (int si = 0; si < clause.AndSubClauses.Count; si++)
                    {
                        var sub = clause.AndSubClauses[si];
                        var subExec = exec.AndSubExecutions[si];
                        var match = ResolveClause(sub, subExec, indexSearcher, plan, parameters, builderParams);
                        if (subExec.BoostFactor > 0)
                            match = indexSearcher.Boost(match, subExec.BoostFactor);
                        matches[matchIdx++] = match;
                    }

                    break;
                }
                case ClauseType.AllIn or ClauseType.In:
                {
                    for (int t = 0; t < exec.InTermCount; t++)
                        matches[matchIdx++] = ResolveInTerm(clause, exec, t, indexSearcher, plan, parameters, builderParams);
                    // Always allocate the null-term slot (plan structure is parameter-independent).
                    // When HasNullTerm is false, fill with a TermQuery(null) that resolves to an
                    // empty posting list — the OR with an empty match is a no-op.
                    {
                        FieldMetadata nullMeta = builderParams != null
                            ? QueryBuilderHelper.GetFieldMetadata(in builderParams, clause.FieldName, hasBoost: builderParams.HasBoost)
                            : indexSearcher.FieldMetadataBuilder(clause.FieldName, hasBoost: parameters?.HasBoost ?? false);
                        matches[matchIdx++] = exec.HasNullTerm
                            ? indexSearcher.TermQuery(nullMeta, null)
                            : TermMatch.CreateEmpty(indexSearcher, indexSearcher.Allocator);
                    }
                    break;
                }
                default:
                {
                    IQueryMatch match = clause.IsOrChainNotEquals switch
                    {
                        true => CreateNotEqualsOrMatch(clause, exec, indexSearcher, plan, parameters, builderParams),
                        false => ResolveClause(clause, exec, indexSearcher, plan, parameters, builderParams)
                    };
                    if (exec.BoostFactor > 0)
                        match = indexSearcher.Boost(match, exec.BoostFactor);
                    matches[matchIdx++] = match;
                    break;
                }
            }
        }

        if (plan.AllNegated)
            matches[matchIdx] = indexSearcher.AllEntries();
        return matches;
    }

    private static IQueryMatch ResolveRangeClauseWithDirection(ClauseInfo clause, ClauseExecution exec,
        IndexSearcher indexSearcher, QueryExecution plan, PlanParameters parameters, QueryBuilderParameters builderParams, bool forward)
    {
        FieldMetadata fieldMeta = ResolveFieldMetadata(clause, indexSearcher, parameters, builderParams);
        var packed = exec.PackedParamValue;

        return clause.ClauseType switch
        {
            ClauseType.GreaterThan => packed.ValueType switch
            {
                PackedParam.TypeLong => indexSearcher.GreaterThanQuery(fieldMeta, plan.LongValues[packed.Param1], forward),
                PackedParam.TypeDouble => indexSearcher.GreaterThanQuery(fieldMeta, plan.DoubleValues[packed.Param1], forward),
                _ => indexSearcher.GreaterThanQuery(fieldMeta, plan.StringValues[packed.Param1], forward)
            },
            ClauseType.GreaterThanOrEqual => packed.ValueType switch
            {
                PackedParam.TypeLong => indexSearcher.GreaterThanOrEqualsQuery(fieldMeta, plan.LongValues[packed.Param1], forward),
                PackedParam.TypeDouble => indexSearcher.GreaterThanOrEqualsQuery(fieldMeta, plan.DoubleValues[packed.Param1], forward),
                _ => indexSearcher.GreaterThanOrEqualsQuery(fieldMeta, plan.StringValues[packed.Param1], forward)
            },
            ClauseType.LessThan => packed.ValueType switch
            {
                PackedParam.TypeLong => indexSearcher.LessThanQuery(fieldMeta, plan.LongValues[packed.Param1], forward),
                PackedParam.TypeDouble => indexSearcher.LessThanQuery(fieldMeta, plan.DoubleValues[packed.Param1], forward),
                _ => indexSearcher.LessThanQuery(fieldMeta, plan.StringValues[packed.Param1], forward)
            },
            ClauseType.LessThanOrEqual => packed.ValueType switch
            {
                PackedParam.TypeLong => indexSearcher.LessThanOrEqualsQuery(fieldMeta, plan.LongValues[packed.Param1], forward),
                PackedParam.TypeDouble => indexSearcher.LessThanOrEqualsQuery(fieldMeta, plan.DoubleValues[packed.Param1], forward),
                _ => indexSearcher.LessThanOrEqualsQuery(fieldMeta, plan.StringValues[packed.Param1], forward)
            },
            ClauseType.Between => packed.ValueType switch
            {
                PackedParam.TypeLong => indexSearcher.BetweenQuery(fieldMeta, plan.LongValues[packed.Param1], plan.LongValues[packed.Param2], forward: forward),
                PackedParam.TypeDouble => indexSearcher.BetweenQuery(fieldMeta, plan.DoubleValues[packed.Param1], plan.DoubleValues[packed.Param2], forward: forward),
                _ => indexSearcher.BetweenQuery(fieldMeta, plan.StringValues[packed.Param1], plan.StringValues[packed.Param2], forward: forward)
            },
            _ => ResolveClause(clause, exec, indexSearcher, plan, parameters, builderParams) // fallback
        };
    }

    /// <summary>Converts an Equals clause into a BetweenQuery(low==high==value) so
    /// it produces a TermsProviderMatch that SortedDrivingMatch can walk in sort order.</summary>
    private static IQueryMatch ResolveEqualsClauseWithDirection(ClauseInfo clause, ClauseExecution exec,
        IndexSearcher indexSearcher, QueryExecution plan, PlanParameters parameters, QueryBuilderParameters builderParams, bool forward)
    {
        FieldMetadata fieldMeta = ResolveFieldMetadata(clause, indexSearcher, parameters, builderParams);
        var packed = exec.PackedParamValue;
        return packed.ValueType switch
        {
            PackedParam.TypeLong => indexSearcher.BetweenQuery(fieldMeta, plan.LongValues[packed.Param1], plan.LongValues[packed.Param1],
                UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThanOrEqual, forward: forward),
            PackedParam.TypeDouble => indexSearcher.BetweenQuery(fieldMeta, plan.DoubleValues[packed.Param1], plan.DoubleValues[packed.Param1],
                UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThanOrEqual, forward: forward),
            _ => indexSearcher.BetweenQuery(fieldMeta, plan.StringValues[packed.Param1], plan.StringValues[packed.Param1],
                UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThanOrEqual, forward: forward)
        };
    }

    private static IQueryMatch ResolveClause(ClauseInfo clause, ClauseExecution exec, IndexSearcher indexSearcher,
        QueryExecution plan, PlanParameters parameters = null, QueryBuilderParameters builderParams = null)
    {
        if (clause.ClauseType == ClauseType.OrGroup && clause.OrSubClauses != null)
        {
            var bm = new BitmapMatch(indexSearcher.Allocator);
            var temp = new RoaringBitmap(indexSearcher.Allocator);
            for (int si = 0; si < clause.OrSubClauses.Count; si++)
            {
                var subExec = exec.OrSubExecutions[si];
                var subMatch = ResolveClause(clause.OrSubClauses[si], subExec, indexSearcher, plan, parameters, builderParams);
                QueryPrimitives.FillFromMatch(subMatch, ref bm.BitmapState);
            }
            temp.Dispose();
            return bm;
        }
        if (clause.ClauseType == ClauseType.AndGroup && clause.AndSubClauses != null)
        {
            var bm = new BitmapMatch(indexSearcher.Allocator);
            var temp = new RoaringBitmap(indexSearcher.Allocator);
            bool first = true;
            for (int si = 0; si < clause.AndSubClauses.Count; si++)
            {
                var sub = clause.AndSubClauses[si];
                var subExec = exec.AndSubExecutions[si];
                var subMatch = ResolveClause(sub, subExec, indexSearcher, plan, parameters, builderParams);
                if (first)
                {
                    QueryPrimitives.FillFromMatch(subMatch, ref bm.BitmapState);
                    first = false;
                }
                else if (sub.IsNegated)
                    QueryPrimitives.AndNotWithMatch(subMatch, ref bm.BitmapState, ref temp);
                else
                    QueryPrimitives.AndWithMatch(subMatch, ref bm.BitmapState, ref temp);
            }
            temp.Dispose();
            return bm;
        }

        // Spatial/Vector/Search have their own field resolution paths.
        FieldMetadata fieldMeta = default;
        bool needsFieldMeta = clause.ClauseType != ClauseType.Spatial
            && clause.ClauseType != ClauseType.Vector
            && clause.ClauseType != ClauseType.Search;
        if (needsFieldMeta)
        {
            if (builderParams != null)
            {
                string resolvedFieldName = clause.FieldName;
                if (clause.IsExact && builderParams.Metadata.IsDynamic)
                    resolvedFieldName = AutoIndexField.GetExactAutoIndexFieldName(resolvedFieldName);
                fieldMeta = QueryBuilderHelper.GetFieldMetadata(in builderParams, resolvedFieldName, exact: clause.IsExact, hasBoost: builderParams.HasBoost);
            }
            else
            {
                fieldMeta = indexSearcher.FieldMetadataBuilder(clause.FieldName, hasBoost: parameters?.HasBoost ?? false);
            }
        }

        var packed = exec.PackedParamValue;

        switch (clause.ClauseType)
        {
            case ClauseType.Equals:
            case ClauseType.NotEquals:
                return TermQueryFromParam(packed, fieldMeta, indexSearcher, plan);

            case ClauseType.GreaterThan:
            {
                int idx = packed.Param1;
                return packed.ValueType switch
                {
                    PackedParam.TypeLong => indexSearcher.GreaterThanQuery(fieldMeta, plan.LongValues[idx]),
                    PackedParam.TypeDouble => indexSearcher.GreaterThanQuery(fieldMeta, plan.DoubleValues[idx]),
                    _ => indexSearcher.GreaterThanQuery(fieldMeta, plan.StringValues[idx])
                };
            }

            case ClauseType.GreaterThanOrEqual:
            {
                int idx = packed.Param1;
                return packed.ValueType switch
                {
                    PackedParam.TypeLong => indexSearcher.GreaterThanOrEqualsQuery(fieldMeta, plan.LongValues[idx]),
                    PackedParam.TypeDouble => indexSearcher.GreaterThanOrEqualsQuery(fieldMeta, plan.DoubleValues[idx]),
                    _ => indexSearcher.GreaterThanOrEqualsQuery(fieldMeta, plan.StringValues[idx])
                };
            }

            case ClauseType.LessThan:
            {
                int idx = packed.Param1;
                return packed.ValueType switch
                {
                    PackedParam.TypeLong => indexSearcher.LessThanQuery(fieldMeta, plan.LongValues[idx]),
                    PackedParam.TypeDouble => indexSearcher.LessThanQuery(fieldMeta, plan.DoubleValues[idx]),
                    _ => indexSearcher.LessThanQuery(fieldMeta, plan.StringValues[idx])
                };
            }

            case ClauseType.LessThanOrEqual:
            {
                int idx = packed.Param1;
                return packed.ValueType switch
                {
                    PackedParam.TypeLong => indexSearcher.LessThanOrEqualsQuery(fieldMeta, plan.LongValues[idx]),
                    PackedParam.TypeDouble => indexSearcher.LessThanOrEqualsQuery(fieldMeta, plan.DoubleValues[idx]),
                    _ => indexSearcher.LessThanOrEqualsQuery(fieldMeta, plan.StringValues[idx])
                };
            }

            case ClauseType.Between:
            {
                int idx1 = packed.Param1;
                int idx2 = packed.Param2;
                return packed.ValueType switch
                {
                    PackedParam.TypeLong => indexSearcher.BetweenQuery(fieldMeta, plan.LongValues[idx1], plan.LongValues[idx2]),
                    PackedParam.TypeDouble => indexSearcher.BetweenQuery(fieldMeta, plan.DoubleValues[idx1], plan.DoubleValues[idx2]),
                    _ => indexSearcher.BetweenQuery(fieldMeta, plan.StringValues[idx1], plan.StringValues[idx2])
                };
            }

            case ClauseType.In:
            case ClauseType.AllIn:
            {
                // IN/AllIn inside an AndGroup reaches here as a single clause.
                // Expand each term into a TermQuery and merge via bitmap, same as the
                // top-level plan does with FillFromPostings/OrWithPostings/AndWithPostings.
                if (exec.InTermCount == 0)
                    return indexSearcher.EmptyMatch();
                var bm = new BitmapMatch(indexSearcher.Allocator);
                var temp = new RoaringBitmap(indexSearcher.Allocator);
                for (int t = 0; t < exec.InTermCount; t++)
                {
                    var termMatch = ResolveInTerm(clause, exec, t, indexSearcher, plan, parameters, builderParams);
                    if (clause.ClauseType == ClauseType.AllIn && t > 0)
                        QueryPrimitives.AndWithMatch(termMatch, ref bm.BitmapState, ref temp);
                    else
                        QueryPrimitives.FillFromMatch(termMatch, ref bm.BitmapState);
                }
                temp.Dispose();
                return bm;
            }

            case ClauseType.Exists:
                return indexSearcher.ExistsQuery(fieldMeta);

            case ClauseType.StartsWith:
                return indexSearcher.StartWithQuery(fieldMeta, plan.StringValues[packed.Param1]);

            case ClauseType.EndsWith:
                return indexSearcher.EndsWithQuery(fieldMeta, plan.StringValues[packed.Param1]);

            case ClauseType.Search:
            {
                FieldMetadata searchMeta;
                if (builderParams != null)
                {
                    string searchFieldName = clause.FieldName;
                    if (builderParams.Metadata.IsDynamic)
                        searchFieldName = AutoIndexField.GetSearchAutoIndexFieldName(searchFieldName);

                    searchMeta = QueryBuilderHelper.GetFieldMetadata(
                        builderParams.Allocator, searchFieldName, builderParams.Index,
                        builderParams.IndexFieldsMapping, builderParams.FieldsToFetch,
                        builderParams.HasDynamics, builderParams.DynamicFields,
                        handleSearch: true, hasBoost: builderParams.HasBoost);
                }
                else if (parameters is { Index: not null, IndexFieldsMapping: not null })
                {
                    string searchFieldName = clause.FieldName;
                    if (parameters.Metadata.IsDynamic)
                        searchFieldName = AutoIndexField.GetSearchAutoIndexFieldName(searchFieldName);

                    searchMeta = QueryBuilderHelper.GetFieldMetadata(
                        parameters.Allocator, searchFieldName, parameters.Index,
                        parameters.IndexFieldsMapping, parameters.FieldsToFetch,
                        parameters.HasDynamics, parameters.DynamicFields,
                        handleSearch: true, hasBoost: parameters.HasBoost);
                }
                else
                {
                    searchMeta = fieldMeta;
                }

                var indexDef = builderParams?.Index?.Definition ?? parameters?.Index?.Definition;
                IndexSearcher.SearchQueryOptions searchQueryOptions;
                if (indexDef != null && IndexDefinitionBaseServerSide.IndexVersion.IsCoraxSearchWildcardAdjustmentSupported(indexDef.Version))
                    searchQueryOptions = IndexSearcher.SearchQueryOptions.PhraseQueryWithWildcardAdjustments;
                else if (indexDef is { Version: >= IndexDefinitionBaseServerSide.IndexVersion.PhraseQuerySupportInCoraxIndexes })
                    searchQueryOptions = IndexSearcher.SearchQueryOptions.PhraseQuery;
                else
                    searchQueryOptions = IndexSearcher.SearchQueryOptions.Legacy;

                var searchTerm = plan.StringValues[packed.Param1];
                if (searchQueryOptions == IndexSearcher.SearchQueryOptions.PhraseQueryWithWildcardAdjustments
                    && searchTerm is { Length: >= 1 }
                    && (searchTerm[0] == '*' || (searchTerm.Length >= 2 && searchTerm[^1] == '*')))
                {
                    searchMeta = ReplaceAnalyzerForWildcardQueries(searchMeta, builderParams, parameters);
                }

                var searchValues = SplitSearchTerms(searchTerm);

                return indexSearcher.SearchQuery(searchMeta,
                    searchValues,
                    (Constants.Search.Operator)clause.SearchOperator,
                    searchQueryOptions);
            }

            case ClauseType.Regex:
                return indexSearcher.RegexQuery(fieldMeta,
                    new System.Text.RegularExpressions.Regex(plan.StringValues[packed.Param1]));

            case ClauseType.Spatial:
            {
                if (builderParams == null)
                    throw new InvalidOperationException("Spatial resolution requires builder parameters");
                return HandleSpatial(builderParams, clause, exec, clause.SpatialMethodType);
            }

            case ClauseType.Vector:
            {
                if (builderParams == null)
                    throw new InvalidOperationException("Vector resolution requires builder parameters");
                var vectorItem = HandleVector(builderParams, clause, exec, false);
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

    /// <summary>Resolve a single IN term to a typed TermQuery.
    /// IN terms are stored contiguously: PackedParamValue.Param1 = start index, InTermCount = count.
    /// Only non-null terms are in the typed array. Null is handled separately via HasNullTerm.</summary>
    private static IQueryMatch ResolveInTerm(ClauseInfo clause, ClauseExecution exec, int termIndex,
        IndexSearcher indexSearcher, QueryExecution plan,
        PlanParameters parameters, QueryBuilderParameters builderParams)
    {
        FieldMetadata fieldMeta = builderParams != null ?
            QueryBuilderHelper.GetFieldMetadata(in builderParams, clause.FieldName, hasBoost: builderParams.HasBoost) :
            indexSearcher.FieldMetadataBuilder(clause.FieldName, hasBoost: parameters?.HasBoost ?? false);

        var p = exec.PackedParamValue;
        int idx = p.Param1 + termIndex;
        var termPacked = new PackedParam(p.ValueType, idx);
        return TermQueryFromParam(termPacked, fieldMeta, indexSearcher, plan);
    }

    /// <summary>Create a pre-materialized <see cref="BitmapMatch"/> for a NotEquals clause
    /// appearing in an OR chain. OR(NOT X, NOT Y, ...) cannot use the raw term posting list
    /// (FillBitmapFromPostingSource would add entries WITH X, not WITHOUT X). Instead, we
    /// pre-compute AllEntries ANDNOT TermQuery(X) into a BitmapMatch so that FillFromMatch
    /// during execution correctly ORs in the set of entries NOT having X.</summary>
    private static IQueryMatch CreateNotEqualsOrMatch(ClauseInfo clause, ClauseExecution exec, IndexSearcher indexSearcher,
        QueryExecution plan, PlanParameters parameters, QueryBuilderParameters builderParams)
    {
        FieldMetadata fieldMeta = ResolveFieldMetadata(clause, indexSearcher, parameters, builderParams);
        IQueryMatch termMatch = TermQueryFromParam(exec.PackedParamValue, fieldMeta, indexSearcher, plan);

        var bitmapMatch = new BitmapMatch(indexSearcher.Allocator);
        var tempData = new RoaringBitmap(indexSearcher.Allocator);
        QueryPrimitives.FillFromMatch(indexSearcher.AllEntries(), ref bitmapMatch.BitmapState);
        QueryPrimitives.AndNotWithMatch(termMatch, ref bitmapMatch.BitmapState, ref tempData);
        tempData.Dispose();
        return bitmapMatch;
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
    private static PostingSource[] ResolveTermSources(QueryExecution plan, IndexSearcher indexSearcher,
        PlanParameters parameters = null, QueryBuilderParameters builderParams = null)
    {
        // IsAllEntries plans never emit term ops (FillFromPostings / AndWith / etc.) —
        // their match[0] is AllEntries, post-filter slots are spatial/vector. No
        // PostingSource population is needed.
        if (plan.IsAllEntries)
            return [];

        if (plan.Clauses is not { Count: > 0 } clauses)
            return [];

        var execs = plan.Executions;

        // Standalone NotEquals: matches[0] = AllEntries (NOT a term source),
        // matches[1] = the negated term. Mirror that layout.
        if (clauses.Count == 1 && clauses[0].IsNegated && !plan.AllNegated)
        {
            var sources = new PostingSource[2];
            sources[1] = ResolveSingleTermSource(clauses[0], execs[0], indexSearcher, plan, parameters, builderParams);
            return sources;
        }

        var termSources = new PostingSource[CountMatchSlots(clauses, execs, plan.IsAllEntries, plan.AllNegated)];
        int matchIdx = 0;
        for (int ci = 0; ci < clauses.Count; ci++)
        {
            ClauseInfo clause = clauses[ci];
            ClauseExecution exec = execs[ci];
            switch (clause.ClauseType)
            {
                case ClauseType.OrGroup when clause.OrSubClauses is { Count: > 0 }:
                {
                    for (int si = 0; si < clause.OrSubClauses.Count; si++)
                    {
                        var sub = clause.OrSubClauses[si];
                        var subExec = exec.OrSubExecutions[si];
                        if (subExec.BoostFactor > 0)
                        {
                            matchIdx++;
                            continue;
                        }
                        termSources[matchIdx++] = ResolveSingleTermSource(sub, subExec, indexSearcher, plan, parameters, builderParams);
                    }

                    break;
                }
                case ClauseType.AndGroup when clause.AndSubClauses is { Count: > 0 }:
                {
                    for (int si = 0; si < clause.AndSubClauses.Count; si++)
                    {
                        var sub = clause.AndSubClauses[si];
                        var subExec = exec.AndSubExecutions[si];
                        if (subExec.BoostFactor > 0)
                        {
                            matchIdx++;
                            continue;
                        }
                        termSources[matchIdx++] = ResolveSingleTermSource(sub, subExec, indexSearcher, plan, parameters, builderParams);
                    }

                    break;
                }
                case ClauseType.AllIn or ClauseType.In:
                {
                    for (int t = 0; t < exec.InTermCount; t++)
                        termSources[matchIdx++] = ResolveInTermSource(clause, exec, t, indexSearcher, plan, parameters, builderParams);
                    matchIdx++; // null-term slot — always allocated, stays Empty in TermSources (uses QueryMatch path)
                    break;
                }
                default:
                {
                    if (exec.BoostFactor > 0)
                    {
                        matchIdx++;
                        continue;
                    }
                    termSources[matchIdx++] = ResolveSingleTermSource(clause, exec, indexSearcher, plan, parameters, builderParams);
                    break;
                }
            }
        }
        // AllNegated extra slot is AllEntries — stays Empty in TermSources.
        return termSources;
    }

    /// <summary>Resolve TreeScan-eligible clauses to ITermsProvider instances for direct
    /// tree-scan dispatch in the compiled pipeline. Slot indexing is parallel to
    /// ResolveMatches/ResolveTermSources. Returns null if no TreeScan clauses exist.</summary>
    private static ITermsProvider[] ResolveTermsProviders(QueryExecution plan, IndexSearcher indexSearcher,
        PlanParameters parameters = null, QueryBuilderParameters builderParams = null)
    {
        if (plan.IsAllEntries || plan.Clauses is not { Count: > 0 } clauses)
            return null;

        var execs = plan.Executions;
        bool hasAnyTreeScan = false;

        // Quick check: do we have any TreeScan clauses at all?
        for (int ci = 0; ci < clauses.Count; ci++)
        {
            var clause = clauses[ci];
            var exec = execs != null && ci < execs.Length ? execs[ci] : null;

            if (IsTreeScanEligibleClause(clause, exec))
            {
                hasAnyTreeScan = true;
                break;
            }

            // Check subclauses
            if (clause.OrSubClauses != null)
            {
                for (int si = 0; si < clause.OrSubClauses.Count; si++)
                {
                    var subExec = exec?.OrSubExecutions?[si];
                    if (IsTreeScanEligibleClause(clause.OrSubClauses[si], subExec))
                    {
                        hasAnyTreeScan = true;
                        break;
                    }
                }
            }
            if (hasAnyTreeScan) break;

            if (clause.AndSubClauses != null)
            {
                for (int si = 0; si < clause.AndSubClauses.Count; si++)
                {
                    var subExec = exec?.AndSubExecutions?[si];
                    if (IsTreeScanEligibleClause(clause.AndSubClauses[si], subExec))
                    {
                        hasAnyTreeScan = true;
                        break;
                    }
                }
            }
            if (hasAnyTreeScan) break;
        }

        if (!hasAnyTreeScan)
            return null;

        int totalSlots = CountMatchSlots(clauses, execs, plan.IsAllEntries, plan.AllNegated);
        var providers = new ITermsProvider[totalSlots];
        int matchIdx = 0;

        for (int ci = 0; ci < clauses.Count; ci++)
        {
            ClauseInfo clause = clauses[ci];
            ClauseExecution exec = execs != null && ci < execs.Length ? execs[ci] : null;

            switch (clause.ClauseType)
            {
                case ClauseType.OrGroup when clause.OrSubClauses is { Count: > 0 }:
                    for (int si = 0; si < clause.OrSubClauses.Count; si++)
                    {
                        var sub = clause.OrSubClauses[si];
                        var subExec = exec?.OrSubExecutions?[si];
                        providers[matchIdx] = ResolveSingleTermsProvider(sub, subExec, indexSearcher, plan, parameters, builderParams);
                        matchIdx++;
                    }
                    break;

                case ClauseType.AndGroup when clause.AndSubClauses is { Count: > 0 }:
                    for (int si = 0; si < clause.AndSubClauses.Count; si++)
                    {
                        var sub = clause.AndSubClauses[si];
                        var subExec = exec?.AndSubExecutions?[si];
                        providers[matchIdx] = ResolveSingleTermsProvider(sub, subExec, indexSearcher, plan, parameters, builderParams);
                        matchIdx++;
                    }
                    break;

                case ClauseType.AllIn or ClauseType.In:
                    // IN terms use PostingList dispatch, not TreeScan. +1 for null-term slot (always allocated).
                    matchIdx += (exec?.InTermCount ?? 0) + 1;
                    break;

                default:
                    providers[matchIdx] = ResolveSingleTermsProvider(clause, exec, indexSearcher, plan, parameters, builderParams);
                    matchIdx++;
                    break;
            }
        }

        return providers;
    }

    /// <summary>Resolve a single TreeScan-eligible clause to its raw ITermsProvider.
    /// Returns null for non-TreeScan clauses or when the field doesn't exist in the
    /// index (factory method returned TermMatch.Empty instead of TermsProviderMatch).
    /// Null slots cause the IL to fall through to the QueryMatch dispatch path.</summary>
    private static ITermsProvider ResolveSingleTermsProvider(ClauseInfo clause, ClauseExecution exec,
        IndexSearcher indexSearcher, QueryExecution plan, PlanParameters parameters, QueryBuilderParameters builderParams)
    {
        if (IsTreeScanEligibleClause(clause, exec) == false)
            return null;

        // Create the match via the existing factory methods, then extract the provider.
        // The factory methods handle all complexity (analyzer, CompactKey, tree lookup).
        var match = ResolveClause(clause, exec ?? new ClauseExecution(), indexSearcher, plan, parameters, builderParams);
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
    private static PostingSource ResolveSingleTermSource(ClauseInfo clause, ClauseExecution exec, IndexSearcher indexSearcher,
        QueryExecution plan, PlanParameters parameters, QueryBuilderParameters builderParams)
    {
        if (IsTermSourceEligibleClause(clause, exec) == false)
            return default; // Kind == Empty

        FieldMetadata fieldMeta = ResolveFieldMetadata(clause, indexSearcher, parameters, builderParams);
        long postingListId = GetTermPostingListIdFromParam(exec.PackedParamValue, fieldMeta, indexSearcher, plan);
        return DecodePostingListId(postingListId, indexSearcher);
    }

    /// <summary>Resolve a single In/AllIn term to a posting-list ID.
    /// Uses <paramref name="termIndex"/> into <see cref="ClauseInfo.InTerms"/> /
    /// <see cref="ClauseInfo.InTermTypes"/> to pick the correct numeric vs. string
    /// overload — avoids the long.TryParse false-positive on zero-padded string
    /// values like "000001" (parses as 1L but is indexed as the string "000001").</summary>
    private static PostingSource ResolveInTermSource(ClauseInfo clause, ClauseExecution exec, int termIndex, IndexSearcher indexSearcher,
        QueryExecution plan, PlanParameters parameters, QueryBuilderParameters builderParams)
    {
        FieldMetadata fieldMeta = builderParams != null ?
            QueryBuilderHelper.GetFieldMetadata(in builderParams, clause.FieldName, hasBoost: builderParams.HasBoost) :
            indexSearcher.FieldMetadataBuilder(clause.FieldName, hasBoost: parameters?.HasBoost ?? false);

        var p = exec.PackedParamValue;
        int idx = p.Param1 + termIndex;
        var termPacked = new PackedParam(p.ValueType, idx);
        long postingListId = GetTermPostingListIdFromParam(termPacked, fieldMeta, indexSearcher, plan);
        return DecodePostingListId(postingListId, indexSearcher);
    }

    /// <summary>Resolve field metadata for a term-source clause. Mirrors the
    /// non-Spatial/Vector/Search branch of <see cref="ResolveClause"/>.</summary>
    private static FieldMetadata ResolveFieldMetadata(ClauseInfo clause, IndexSearcher indexSearcher,
        PlanParameters parameters, QueryBuilderParameters builderParams)
    {
        if (builderParams != null)
        {
            string resolvedFieldName = clause.FieldName;
            if (clause.IsExact && builderParams.Metadata.IsDynamic)
                resolvedFieldName = AutoIndexField.GetExactAutoIndexFieldName(resolvedFieldName);
            return QueryBuilderHelper.GetFieldMetadata(in builderParams, resolvedFieldName, exact: clause.IsExact, hasBoost: builderParams.HasBoost);
        }

        return indexSearcher.FieldMetadataBuilder(clause.FieldName, hasBoost: parameters?.HasBoost ?? false);
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

        var termType = (global::Corax.Indexing.TermIdMask)postingListId & global::Corax.Indexing.TermIdMask.EnsureIsSingleMask;
        switch (termType)
        {
            case global::Corax.Indexing.TermIdMask.Single:
                return new PostingSource
                {
                    Kind = PostingSourceKind.Single,
                    SingleEntryId = (long)EntryIdEncodings.GetContainerId(postingListId),
                };

            case global::Corax.Indexing.TermIdMask.SmallPostingList:
                return new PostingSource
                {
                    Kind = PostingSourceKind.SmallPostingList,
                    SmallPostingListId = (long)EntryIdEncodings.GetContainerId(postingListId),
                };

            case global::Corax.Indexing.TermIdMask.PostingList:
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
    private static void ExtractScanParameters(QueryExecution plan, IndexSearcher indexSearcher,
        out long[] longParams, out double[] doubleParams, out Voron.Slice[] sliceParams, out long[] fieldRootPages)
    {
        var predicates = plan.ScanPredicateInfos;
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
        var slices = new List<Voron.Slice>();
        var roots = new List<long>();

        // Walk predicates and clauses in lock-step. BuildScanPredicateInfo skips non-eligible
        // clauses (Search, In, AllIn, Exists, StartsWith, EndsWith, Regex, Spatial, Vector,
        // AndGroup), so we must skip them here too to keep the 1:1 positional mapping.
        int scanStart = plan.AllNegated ? 0 : 1;
        int clauseIdx = scanStart;
        var clauses = plan.Clauses;
        var execs = plan.Executions;
        int dummyL = 0, dummyD = 0, dummyS = 0;
        foreach (ScanPredicateInfo pred in predicates)
        {
            // Advance past clauses that BuildScanPredicateInfo would have skipped (returned null).
            while (clauseIdx < clauses.Count &&
                   BuildScanPredicateInfo(clauses[clauseIdx], execs != null && clauseIdx < execs.Length ? execs[clauseIdx] : null,
                       ref dummyL, ref dummyD, ref dummyS) == null)
            {
                clauseIdx++;
            }

            ClauseInfo matchingClause = clauseIdx < clauses.Count ? clauses[clauseIdx] : null;
            ClauseExecution matchingExec = execs != null && clauseIdx < execs.Length ? execs[clauseIdx] : null;
            clauseIdx++;
            ExtractParamsFromPredicate(pred, matchingClause, matchingExec, indexSearcher, plan, longs, doubles, slices, roots);
        }

        longParams = longs.Count > 0 ? longs.ToArray() : [];
        doubleParams = doubles.Count > 0 ? doubles.ToArray() : [];
        sliceParams = slices.Count > 0 ? slices.ToArray() : [];
        fieldRootPages = roots.Count > 0 ? roots.ToArray() : [];
    }

    private static void ExtractParamsFromPredicate(ScanPredicateInfo pred, ClauseInfo clause, ClauseExecution exec,
        IndexSearcher indexSearcher, QueryExecution plan, List<long> longs, List<double> doubles,
        List<Voron.Slice> slices, List<long> roots)
    {
        if (pred.SubPredicates != null)
        {
            // Each OrBranch corresponds to a subclause of the OrGroup.
            // Pass subclauses positionally to avoid the same field-name ambiguity.
            List<ClauseInfo> subClauses = clause?.OrSubClauses;
            ClauseExecution[] subExecs = exec?.OrSubExecutions;
            for (int b = 0; b < pred.SubPredicates.Length; b++)
            {
                ClauseInfo subClause = (subClauses != null && b < subClauses.Count) ? subClauses[b] : null;
                ClauseExecution subExec = (subExecs != null && b < subExecs.Length) ? subExecs[b] : null;
                ExtractParamsFromPredicate(pred.SubPredicates[b], subClause, subExec, indexSearcher, plan, longs, doubles, slices, roots);
            }
            return;
        }

        // Resolve field root page
        roots.Add(indexSearcher.FieldCache.GetLookupRootPage(pred.FieldName));

        if (clause == null || exec == null)
            return;

        // Read pre-resolved typed values from the plan's arrays via packed param.
        var packed = exec.PackedParamValue;
        if (packed.IsNone)
            return;
        int idx1 = packed.Param1;
        int idx2 = packed.Param2;
        bool hasBetween = idx2 != PackedParam.NoParamValue;

        switch (pred.ValueType)
        {
            case ScanValueType.Long:
                longs.Add(plan.LongValues[idx1]);
                if (hasBetween)
                    longs.Add(plan.LongValues[idx2]);
                break;
            case ScanValueType.Double:
                doubles.Add(plan.DoubleValues[idx1]);
                if (hasBetween)
                    doubles.Add(plan.DoubleValues[idx2]);
                break;
            case ScanValueType.Slice:
                var fieldMeta = indexSearcher.FieldMetadataBuilder(clause.FieldName);
                slices.Add(indexSearcher.EncodeAndApplyAnalyzer(fieldMeta, plan.StringValues[idx1]));
                if (hasBetween)
                    slices.Add(indexSearcher.EncodeAndApplyAnalyzer(fieldMeta, plan.StringValues[idx2]));
                break;
        }
    }

    // ── Highlighting ─────────────────────────────────────────────────────

    /// <summary>
    /// Populate the highlighting terms dictionary from the plan's clauses.
    /// The old CoraxQueryBuilder did this as a side effect during query building.
    /// The bitmap pipeline must do it explicitly after plan building.
    /// </summary>
    private static void PopulateHighlightingTerms(QueryExecution plan, Dictionary<string, CoraxHighlightingTermIndex> highlightingTerms, QueryMetadata metadata)
    {
        if (highlightingTerms == null || plan.Clauses is not { Count: > 0 } clauses)
            return;

        var execs = plan.Executions;
        for (int ci = 0; ci < clauses.Count; ci++)
        {
            var clauseObj = clauses[ci];
            var exec = execs != null && ci < execs.Length ? execs[ci] : null;
            if (clauseObj?.FieldName == null)
                continue;

            PopulateHighlightingForClause(clauseObj, exec, highlightingTerms, metadata, plan);

            switch (clauseObj.ClauseType)
            {
                case ClauseType.OrGroup when clauseObj.OrSubClauses is { Count: > 0 }:
                {
                    for (int si = 0; si < clauseObj.OrSubClauses.Count; si++)
                    {
                        var subExec = exec?.OrSubExecutions != null && si < exec.OrSubExecutions.Length ? exec.OrSubExecutions[si] : null;
                        PopulateHighlightingForClause(clauseObj.OrSubClauses[si], subExec, highlightingTerms, metadata, plan);
                    }
                    break;
                }
                case ClauseType.AndGroup when clauseObj.AndSubClauses is { Count: > 0 }:
                {
                    for (int si = 0; si < clauseObj.AndSubClauses.Count; si++)
                    {
                        var subExec = exec?.AndSubExecutions != null && si < exec.AndSubExecutions.Length ? exec.AndSubExecutions[si] : null;
                        PopulateHighlightingForClause(clauseObj.AndSubClauses[si], subExec, highlightingTerms, metadata, plan);
                    }
                    break;
                }
            }
        }
    }

    private static void PopulateHighlightingForClause(ClauseInfo clause, ClauseExecution exec, Dictionary<string, CoraxHighlightingTermIndex> highlightingTerms, QueryMetadata metadata, QueryExecution plan)
    {
        string fieldName = clause.FieldName;
        if (fieldName == null)
            return;

        if (highlightingTerms.TryGetValue(fieldName, out var existingTerm))
        {
            // Already populated (e.g., multiple clauses on same field) — update values if needed
            existingTerm.Values ??= GetHighlightingValues(clause, exec, plan);
            return;
        }

        var term = new CoraxHighlightingTermIndex
        {
            FieldName = fieldName,
            Values = GetHighlightingValues(clause, exec, plan)
        };

        if (metadata.IsDynamic && clause.ClauseType == ClauseType.Search)
            term.DynamicFieldName = AutoIndexField.GetSearchAutoIndexFieldName(fieldName);
        else if (metadata.IsDynamic && clause.IsExact)
            term.DynamicFieldName = AutoIndexField.GetExactAutoIndexFieldName(fieldName);

        highlightingTerms[fieldName] = term;

        // For dynamic indexes, also add the dynamic field name variant
        if (term.DynamicFieldName != null)
            highlightingTerms[term.DynamicFieldName] = term;
    }

    private static object GetHighlightingValues(ClauseInfo clause, ClauseExecution exec, QueryExecution plan)
    {
        var packed = exec?.PackedParamValue ?? PackedParam.None;
        if (clause.ClauseType == ClauseType.Between)
        {
            return new Tuple<string, string>(
                FormatValueFromPlan(packed, plan),
                FormatValue2FromPlan(packed, plan));
        }

        int inTermCount = exec?.InTermCount ?? 0;
        bool hasNullTerm = exec?.HasNullTerm ?? false;
        if (clause.ClauseType is ClauseType.In or ClauseType.AllIn && (inTermCount > 0 || hasNullTerm))
        {
            var p = packed;
            var terms = new List<string>(inTermCount + (hasNullTerm ? 1 : 0));
            for (int t = 0; t < inTermCount; t++)
                terms.Add(FormatValueFromPlan(new PackedParam(p.ValueType, p.Param1 + t), plan));
            if (hasNullTerm)
                terms.Add(null);
            return terms;
        }

        return FormatValueFromPlan(packed, plan);
    }

    // ── Vector / Spatial resolution ──────────────────────────────────────

    /// <summary>
    /// Resolve vector select operations from the plan into CoraxVectorItem instances.
    /// These are NOT materialized yet — the caller materializes them with the bitmap-producing
    /// match as the filterQuery. Returns null if the plan has no vectors.
    /// </summary>
    private static CoraxVectorItem[] ResolveVectorItems(QueryExecution plan, QueryBuilderParameters builderParams = null)
    {
        if (plan.VectorSelects == null || plan.VectorSelects.Length == 0)
            return null;

        var items = new CoraxVectorItem[plan.VectorSelects.Length];
        for (int i = 0; i < plan.VectorSelects.Length; i++)
        {
            var clause = plan.VectorSelects[i].Clause;
            var exec = plan.VectorSelects[i].Exec;
            if (clause == null || clause.ClauseType != ClauseType.Vector || builderParams == null)
                throw new InvalidOperationException("Vector select references an invalid clause at index " + i);

            items[i] = HandleVector(builderParams, clause, exec, false);
        }
        return items;
    }

    private static IQueryMatch HandleSpatial(QueryBuilderParameters builderParameters, ClauseInfo clause, ClauseExecution exec, SpatialOperationType spatialMethod)
    {
        var index = builderParameters.Index;
        var allocator = builderParameters.Allocator;

        // Field name was pre-resolved during parsing.
        string fieldName = clause.FieldName
            ?? throw new InvalidOperationException("Spatial clause has no pre-resolved field name.");

        var fieldMetadata = QueryBuilderHelper.GetFieldMetadata(allocator, fieldName, index, builderParameters.IndexFieldsMapping,
            builderParameters.FieldsToFetch, builderParameters.HasDynamics, builderParameters.DynamicFields, hasBoost: builderParameters.HasBoost);

        var sp = exec.Spatial;
        var distanceErrorPct = sp.DistanceErrorPct >= 0
            ? sp.DistanceErrorPct
            : RavenConstants.Documents.Indexing.Spatial.DefaultDistanceErrorPct;

        var spatialField = builderParameters.Factories.GetSpatialFieldFactory(fieldName);

        // Build shape from pre-resolved parameters — no GetValue calls
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

        return builderParameters.IndexSearcher.SpatialQuery(fieldMetadata, distanceErrorPct, shape, spatialField.GetContext(), (SpatialRelation)spatialMethod, token: builderParameters.Token);
    }

    private static CoraxVectorItem HandleVector(QueryBuilderParameters builderParameters, ClauseInfo clause, ClauseExecution exec, bool exact)
    {
        IndexField indexField;
        string embeddingsGenerationTaskIdentifier;

        var vec = exec.Vector;
        var minimumMatch = vec.MinimumMatch >= 0
            ? vec.MinimumMatch
            : builderParameters.Index.Configuration.CoraxVectorSearchDefaultMinimumSimilarity;

        int numberOfCandidates = vec.NumberOfCandidates >= 0
            ? vec.NumberOfCandidates
            : builderParameters.Index.Configuration.CoraxVectorDefaultNumberOfCandidatesForQuerying;

        var fieldName = clause.FieldName
            ?? throw new InvalidOperationException("Vector clause has no pre-resolved field name.");

        var fieldMetadata = QueryBuilderHelper.GetFieldMetadata(builderParameters, fieldName, hasBoost: builderParameters.HasBoost);

        // Use pre-resolved vector value and method kind from parsing
        object methodParameter = vec.ResolvedValue;
        ValueTokenType valueTokenType = ToValueTokenType(vec.ResolvedValueType);

        if (vec.Method != VectorSourceKind.Inline)
        {
            var method = vec.Method switch
            {
                VectorSourceKind.FromDocument => VectorHelpers.MethodVectorValue.ForDocument,
                VectorSourceKind.FromText => VectorHelpers.MethodVectorValue.EmbeddingText,
                _ => throw new InvalidDataException($"Unknown vector source kind: {vec.Method}")
            };

            if (method is not VectorHelpers.MethodVectorValue.EmbeddingText)
            {
                return (method, methodParameter) switch
                {
                    (method: VectorHelpers.MethodVectorValue.ForDocument, string docId) => CoraxVectorItem.BuildForDocVector(builderParameters, fieldMetadata, docId, numberOfCandidates, minimumMatch, exact),
                    (method: VectorHelpers.MethodVectorValue.ForDocument, StringSegment docIdSegment) => CoraxVectorItem.BuildForDocVector(builderParameters, fieldMetadata, docIdSegment.Value, numberOfCandidates, minimumMatch, exact),
                    (method: VectorHelpers.MethodVectorValue.ForRaw, string vectorAsBase64) => CoraxVectorItem.BuildSingleVector(builderParameters, fieldMetadata, GenerateEmbeddings.FromBase64Array(VectorOptions.Default, builderParameters.Allocator, vectorAsBase64), numberOfCandidates, minimumMatch, exact),
                    (method: VectorHelpers.MethodVectorValue.ForRaw, StringSegment stringSegmentAsBase64) => CoraxVectorItem.BuildSingleVector(builderParameters, fieldMetadata, GenerateEmbeddings.FromBase64Array(VectorOptions.Default, builderParameters.Allocator, stringSegmentAsBase64.ToString()), numberOfCandidates, minimumMatch, exact),
                    (_, BlittableJsonReaderArray { Length: > 0 }) => throw new InvalidDataException("Cannot perform search on empty value."),
                    _ => throw new InvalidQueryException(
                        $"Unknown method in value ({vec.Method}. Parameter type: {methodParameter?.GetType().FullName}, Value: {methodParameter}")
                };
            }

            embeddingsGenerationTaskIdentifier = vec.AiTaskName;
            var vectorOptions = VectorHelpers.GetExplicitVectorOptions(builderParameters, fieldName, out indexField);
            if (vectorOptions != null)
            {
                vectorOptions = new VectorOptions()
                {
                    DestinationEmbeddingType = vectorOptions.DestinationEmbeddingType,
                    Dimensions = vectorOptions.Dimensions,
                    SourceEmbeddingType = VectorEmbeddingType.Text,
                    NumberOfCandidatesForIndexing = vectorOptions.NumberOfCandidatesForIndexing,
                    NumberOfEdges = vectorOptions.NumberOfEdges
                };
            }

            var vector = VectorHelpers.GetEmbeddingsForQueryParameter(builderParameters, valueTokenType, methodParameter, embeddingsGenerationTaskIdentifier, vectorOptions, fieldName);

            if (vector.SingleVector != null)
                return CoraxVectorItem.BuildSingleVector(builderParameters, fieldMetadata, vector.SingleVector.Value, numberOfCandidates, minimumMatch, exact);

            return CoraxVectorItem.BuildMultiVector(builderParameters, fieldMetadata, vector.MultiVector, numberOfCandidates, minimumMatch, exact);
        }

        // Direct value (not a method call) — use pre-resolved value
        var value = methodParameter;
        var valueType = valueTokenType;

        (VectorValue? SingleVector, VectorValue[] MultiVector) transformedEmbeddings = (null, null);
        int numberOfDimensions;
        if (VectorHelpers.TryRetrieveEmbeddingsGenerationTaskIdentifier(builderParameters, fieldName, out embeddingsGenerationTaskIdentifier))
        {
            var vectorOptions = VectorHelpers.GetExplicitVectorOptions(builderParameters, fieldName, out indexField);
            transformedEmbeddings = VectorHelpers.GetEmbeddingsForQueryParameter(builderParameters, valueType, value, embeddingsGenerationTaskIdentifier, vectorOptions, fieldName);
        }
        else
        {
            VectorOptions vectorOptions = VectorHelpers.GetOptions(builderParameters, fieldName, out indexField);

            if (builderParameters.Index.IndexFieldsPersistence.TryReadNumberOfDimensions(fieldName, out numberOfDimensions) == false)
                return CoraxVectorItem.BuildEmpty(builderParameters); // no vector indexed
            if (vectorOptions.SourceEmbeddingType is VectorEmbeddingType.Text)
            {
                transformedEmbeddings = VectorHelpers.GetVectorValueForTextualInput(builderParameters, vectorOptions, valueType, value);
            }
            else
            {
                switch (value)
                {
                    case string s:
                        transformedEmbeddings.SingleVector = GenerateEmbeddings.FromBase64Array(vectorOptions, builderParameters.Allocator, s);
                        break;
                    case StringSegment stringSegment:
                        transformedEmbeddings.SingleVector = GenerateEmbeddings.FromBase64Array(vectorOptions, builderParameters.Allocator, stringSegment.ToString());
                        break;
                    case BlittableJsonReaderObject bjro:
                        transformedEmbeddings.SingleVector = VectorHelpers.GetVectorValueFromRavenVector(builderParameters, bjro, vectorOptions);
                        break;
                    case BlittableJsonReaderArray { Length: > 0 } bjra:
                    {
                        var isRavenVector = bjra[0] is BlittableJsonReaderObject;
                        var isStringArray = bjra[0] is string or StringSegment or LazyStringValue;
                        var isArray = bjra[0] is BlittableJsonReaderArray;

                        if (isRavenVector == false && isStringArray == false && isArray == false)
                        {
                            transformedEmbeddings.SingleVector = VectorHelpers.GetVectorValueFromNumericalBlittableArray(builderParameters, bjra, vectorOptions);
                        }
                        else
                        {
                            var embeddings = new VectorValue[bjra.Length];
                            for (int i = 0; i < bjra.Length; ++i)
                            {
                                if (isRavenVector)
                                    embeddings[i] = VectorHelpers.GetVectorValueFromRavenVector(builderParameters, (BlittableJsonReaderObject)bjra[i], vectorOptions);
                                else if (isStringArray)
                                    embeddings[i] = GenerateEmbeddings.FromBase64Array(vectorOptions, builderParameters.Allocator, bjra[i].ToString());
                                else
                                    embeddings[i] = VectorHelpers.GetVectorValueFromNumericalBlittableArray(builderParameters, (BlittableJsonReaderArray)bjra[i],
                                        vectorOptions);
                            }

                            transformedEmbeddings.MultiVector = embeddings;
                        }

                        break;
                    }
                    default:
                        PortableExceptions.Throw<InvalidDataException>("We expected to get vector(s), however got: " + value.GetType().Name);
                        break;
                }
            }
        }

        if (builderParameters.Index.IndexFieldsPersistence.TryReadNumberOfDimensions(fieldName, out numberOfDimensions) == false)
            return CoraxVectorItem.BuildEmpty(builderParameters); // no vector indexed

        if (transformedEmbeddings.SingleVector != null)
        {
            var singleVector = transformedEmbeddings.SingleVector.Value;

            if (indexField != null)
                AssertDimensions(singleVector);
            return CoraxVectorItem.BuildSingleVector(builderParameters, fieldMetadata, singleVector, numberOfCandidates, minimumMatch, exact);
        }

        if (transformedEmbeddings.MultiVector != null)
        {
            var multiVector = transformedEmbeddings.MultiVector;

            if (indexField != null)
            {
                foreach (var vector in multiVector)
                    AssertDimensions(vector);
            }
            return CoraxVectorItem.BuildMultiVector(builderParameters, fieldMetadata, multiVector, numberOfCandidates, minimumMatch, exact);
        }

        throw new InvalidDataException("Expected to get single or multiple embeddings of VectorValue type but none was provided");

        void AssertDimensions(in VectorValue vector)
        {
            if (numberOfDimensions != vector.Length)
            {
                using (vector)
                    VectorHelpers.ThrowDifferentNumberOfDimensions(indexField, fieldName, vector, numberOfDimensions);
            }
        }
    }

    private static class VectorHelpers
    {
        public enum MethodVectorValue
        {
            ForDocument,
            ForRaw,
            EmbeddingText
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryRetrieveEmbeddingsGenerationTaskIdentifier(QueryBuilderParameters builderParameters, in string fieldName, out string embeddingsGenerationTaskIdentifier)
        {
            var existsInPersistence =
                builderParameters.Index.IndexFieldsPersistence.TryReadEmbeddingsGenerationTaskIdentifier(fieldName, out embeddingsGenerationTaskIdentifier);

            if (builderParameters.Metadata.IsDynamic == false)
                return existsInPersistence;

            if (((builderParameters.FieldsToFetch != null && builderParameters.FieldsToFetch.IndexFields.TryGetValue(fieldName, out var indexField)) || (builderParameters.Index.Definition.IndexFields.TryGetValue(fieldName, out indexField))) && indexField.Vector is AutoVectorOptions avo)
            {
                embeddingsGenerationTaskIdentifier = avo.EmbeddingsGenerationTaskIdentifier;
                return string.IsNullOrEmpty(avo.EmbeddingsGenerationTaskIdentifier) == false;
            }

            embeddingsGenerationTaskIdentifier = null;
            return false;
        }

        internal static (VectorValue? SingleVector, VectorValue[] MultiVector) GetVectorValueForTextualInput(QueryBuilderParameters parameters, VectorOptions vectorOptions, ValueTokenType valueType, object value)
        {
            if (valueType is ValueTokenType.String)
                return (GenerateEmbeddings.FromText(parameters.Allocator, vectorOptions, value.ToString()), null);

            if (valueType is not ValueTokenType.Parameter)
                PortableExceptions.Throw<InvalidDataException>($"Cannot use vector.search() on a text field with a non-string value. Got {valueType}");

            if (value is BlittableJsonReaderArray valueAsList)
            {
                var embeddings = new VectorValue[valueAsList.Length];
                for (var i = 0; i < valueAsList.Length; ++i)
                    embeddings[i] = GenerateEmbeddings.FromText(parameters.Allocator, vectorOptions, valueAsList[i].ToString());

                return (null, embeddings);
            }

            PortableExceptions.Throw<InvalidDataException>($"Cannot use vector.search() on a text field with a non-string value(s). Got {valueType}");
            return (null, null);
        }

        internal static VectorValue GetVectorValueFromRavenVector(QueryBuilderParameters parameters, BlittableJsonReaderObject json, VectorOptions vectorOptions)
        {
            var vectorObjectFound = json.TryGetMember(Sparrow.Global.Constants.Naming.VectorPropertyName, out var vectorObject);
            PortableExceptions.ThrowIfNot<InvalidDataException>(vectorObjectFound, "Cannot find vector property in the object.");

            var vectorReader = (BlittableJsonReaderVector)vectorObject;
            return QueryBuilderHelper.GetVectorValueFromBlittableJsonVectorReader(parameters.Allocator, vectorOptions, vectorReader);
        }

        internal static VectorValue GetVectorValueFromNumericalBlittableArray(QueryBuilderParameters parameters, BlittableJsonReaderArray array, VectorOptions vectorOptions)
        {
            var bytesUsed = array.Length * (vectorOptions.SourceEmbeddingType is VectorEmbeddingType.Single ? sizeof(float) : 1);
            var memScope = parameters.Allocator.Allocate(bytesUsed, out Memory<byte> mem);
            ref var floatRef = ref MemoryMarshal.GetReference(MemoryMarshal.Cast<byte, float>(mem.Span));
            ref var sbyteRef = ref MemoryMarshal.GetReference(MemoryMarshal.Cast<byte, sbyte>(mem.Span));
            ref var byteRef = ref MemoryMarshal.GetReference(mem.Span);

            for (int i = 0; i < array.Length; ++i)
            {
                switch (vectorOptions.SourceEmbeddingType)
                {
                    case VectorEmbeddingType.Single:
                        Unsafe.Add(ref floatRef, i) = array.GetByIndex<float>(i);
                        break;
                    case VectorEmbeddingType.Int8:
                        Unsafe.Add(ref sbyteRef, i) = array.GetByIndex<sbyte>(i);
                        break;
                    default:
                        Unsafe.AddByteOffset(ref byteRef, i) = array.GetByIndex<byte>(i);
                        break;
                }
            }

            return GenerateEmbeddings.FromArray(parameters.Allocator, memScope, mem, vectorOptions, bytesUsed);
        }

        internal static VectorOptions GetExplicitVectorOptions(QueryBuilderParameters builderParameters, in string fieldName, out IndexField indexField)
        {
            if ((builderParameters.FieldsToFetch != null && builderParameters.FieldsToFetch.IndexFields.TryGetValue(fieldName, out indexField)) == false
                && (builderParameters.Index.Definition.IndexFields.TryGetValue(fieldName, out indexField)) == false)
                PortableExceptions.Throw<InvalidDataException>($"Cannot find `{fieldName}` field in the index.");

            return indexField.Vector;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static VectorOptions GetOptions(QueryBuilderParameters builderParameters, in string fieldName, out IndexField indexField)
        {
            if ((builderParameters.FieldsToFetch != null && builderParameters.FieldsToFetch.IndexFields.TryGetValue(fieldName, out indexField)) == false
                && (builderParameters.Index.Definition.IndexFields.TryGetValue(fieldName, out indexField)) == false)
                PortableExceptions.Throw<InvalidDataException>($"Cannot find `{fieldName}` field in the index.");

            // VectorOptions can be null when a user does not specify the configuration.
            // In such cases, we will choose the input depending on the value type (similar to how we handle it during indexing).
            if (indexField.Vector != null)
                return indexField.Vector;

            builderParameters.Index.IndexFieldsPersistence.TryReadVectorSourceEmbeddingType(fieldName, out var vectorSourceEmbeddingType);

            var defaultVectorOptions = vectorSourceEmbeddingType switch
            {
                VectorEmbeddingType.Single => VectorOptions.Default,
                VectorEmbeddingType.Text => VectorOptions.DefaultText,
                _ => throw new InvalidDataException(
                    $"Unknown vector source embedding type: {vectorSourceEmbeddingType}. Implicit configuration support only single and text vector source embedding types.")
            };

            indexField.Vector = defaultVectorOptions;

            return defaultVectorOptions;
        }

        internal static void ThrowDifferentNumberOfDimensions(in IndexField indexField, in string fieldName, in VectorValue transformedEmbedding,
            in int numberOfDimensions)
        {
            var (storedDimensions, inputDimensions) = indexField.Vector.DestinationEmbeddingType switch
            {
                VectorEmbeddingType.Single => (numberOfDimensions / sizeof(float), transformedEmbedding.Length / sizeof(float)),
                VectorEmbeddingType.Int8 => (numberOfDimensions - sizeof(float), transformedEmbedding.Length - sizeof(float)),
                VectorEmbeddingType.Binary => (numberOfDimensions, transformedEmbedding.Length),
                _ => throw new InvalidDataException($"Unexpected embedding type - {numberOfDimensions}.")
            };

            PortableExceptions.Throw<InvalidDataException>(
                $"Vector field `{fieldName}` has {storedDimensions} dimensions, but the vector passed to vector.search() has {inputDimensions} dimensions.");
        }

        internal static (VectorValue? SingleVector, VectorValue[] MultiVector) GetEmbeddingsForQueryParameter(QueryBuilderParameters builderParameters, ValueTokenType valueType,
            object value,
            string embeddingsGenerationTaskIdentifier, VectorOptions vectorOptions, string fieldName)
        {
            var database = builderParameters.Index.DocumentDatabase;

            var embeddingsTaskId = new EmbeddingsGenerationTaskIdentifier(embeddingsGenerationTaskIdentifier);

            var embeddingsGenerator = database.EmbeddingsGeneratorQueries;

            var sourceEmbeddingType = embeddingsGenerator.GetQuantizationOf(embeddingsTaskId);

            // Quantized dynamic field indicates that the task generated embeddings with different quantization than requested in the index
            // In this case we want to use quantization defined in dynamic field (which was set in CurrentIndexingScope.GetLoadVectorField)
            VectorEmbeddingType destinationEmbeddingType;
            if (builderParameters.Metadata.IsDynamic)
            {
                destinationEmbeddingType = sourceEmbeddingType is not VectorEmbeddingType.Single ? 
                    sourceEmbeddingType : 
                    vectorOptions!.DestinationEmbeddingType;
            }
            else
            {
                destinationEmbeddingType = vectorOptions?.DestinationEmbeddingType ?? sourceEmbeddingType;
            }

            ReadOnlyMemory<ReadOnlyMemory<byte>> embeddingValues;

            switch (valueType)
            {
                case ValueTokenType.String:
                    embeddingValues = embeddingsGenerator
                        .GetEmbeddingsForQuery(builderParameters.DocumentsContext, embeddingsTaskId, value.ToString());
                    break;
                case ValueTokenType.Parameter:
                {
                    if (value is not BlittableJsonReaderArray bjra)
                        throw new InvalidQueryException($"Expected array as parameter of vector.search({fieldName}) method, got '{value.GetType().FullName}' type instead.");

                    var values = new string[bjra.Length];

                    for (var i = 0; i < values.Length; i++)
                        values[i] = bjra[i].ToString();

                    embeddingValues = embeddingsGenerator
                        .GetEmbeddingsForQuery(builderParameters.DocumentsContext, embeddingsTaskId, values);
                    break;
                }
                default:
                    throw new NotSupportedException($"Unexpected value type provided as parameter to vector.search({fieldName}) method. Got '{value.GetType().FullName}' type.");
            }

            var queryingVectorOption = new VectorOptions
            {
                SourceEmbeddingType = sourceEmbeddingType,
                DestinationEmbeddingType = destinationEmbeddingType
            };

            if (embeddingValues.Length == 1)
            {
                var embeddingValue = embeddingValues.Span[0];

                return (GenerateEmbeddings.FromArray(builderParameters.Allocator, embeddingValue.Span, queryingVectorOption), null);
            }
            else
            {
                var vectorValues = new VectorValue[embeddingValues.Length];

                for (int i = 0; i < embeddingValues.Length; i++)
                {
                    var embeddingValue = embeddingValues.Span[i];

                    vectorValues[i] = GenerateEmbeddings.FromArray(builderParameters.Allocator, embeddingValue.Span, queryingVectorOption);
                }

                return (null, vectorValues);
            }
        }
    }

    // ── Compound field merging (WHERE only, no ORDER BY) ──────────────────

    // ── Compound field exact match (no ORDER BY) ─────────────────────────

    /// <summary>Check if two Equals clauses on (field1, field2) match a compound field.
    /// If so, build a single TermQuery on the compound tree with the composite key.
    /// One tree lookup instead of two posting list intersections.</summary>
    public static bool TryCreateCompoundExactMatch(
        QueryExecution plan, PlanParameters planParams, QueryBuilderParameters builderParams,
        out IQueryMatch compoundMatch)
    {
        compoundMatch = null;
        if (plan.Clauses == null || plan.Clauses.Count < 2 || plan.AllNegated)
            return false;
        if (planParams.Index == null)
            return false;

        var clauses = plan.Clauses;
        var execs = plan.Executions;
        var index = planParams.Index;
        var indexSearcher = planParams.IndexSearcher;
        var allocator = planParams.Allocator;

        // Find two unboosted Equals clauses that match a compound field
        for (int i = 0; i < clauses.Count; i++)
        {
            var c1 = clauses[i];
            if (c1.ClauseType != ClauseType.Equals || c1.IsNegated || c1.HasBoost)
                continue;
            var e1 = execs[i];
            if (e1.BoostFactor > 0 || e1.PackedParamValue.IsNone)
                continue;

            for (int j = i + 1; j < clauses.Count; j++)
            {
                var c2 = clauses[j];
                if (c2.ClauseType != ClauseType.Equals || c2.IsNegated || c2.HasBoost)
                    continue;
                var e2 = execs[j];
                if (e2.BoostFactor > 0 || e2.PackedParamValue.IsNone)
                    continue;

                // Check both orderings
                string firstField = null, secondField = null;
                ClauseExecution firstExec = null, secondExec = null;

                using (Voron.Slice.From(allocator, c1.FieldName, out var s1))
                using (Voron.Slice.From(allocator, c2.FieldName, out var s2))
                {
                    if (index.HasCompoundField(s1, s2, out _))
                    {
                        firstField = c1.FieldName; secondField = c2.FieldName;
                        firstExec = e1; secondExec = e2;
                    }
                    else if (index.HasCompoundField(s2, s1, out _))
                    {
                        firstField = c2.FieldName; secondField = c1.FieldName;
                        firstExec = e2; secondExec = e1;
                    }
                }

                if (firstField == null) continue;

                // Build field1 bytes
                byte[] field1Bytes = BuildCompoundFieldBytes(firstField, firstExec, indexSearcher, plan);
                if (field1Bytes == null || field1Bytes.Length > byte.MaxValue) continue;

                // Build field2 bytes
                byte[] field2Bytes = BuildCompoundFieldBytes(secondField, secondExec, indexSearcher, plan);
                if (field2Bytes == null) continue;

                // Build composite key
                int totalLen = field1Bytes.Length + field2Bytes.Length + 1;
                if (totalLen > Constants.Terms.MaxLength) continue;

                var compositeKey = new byte[totalLen];
                field1Bytes.CopyTo(compositeKey, 0);
                field2Bytes.CopyTo(compositeKey.AsSpan(field1Bytes.Length));
                compositeKey[^1] = (byte)field1Bytes.Length;

                var compoundFieldName = $"compound({firstField},{secondField})";
                var compoundFieldMeta = indexSearcher.FieldMetadataBuilder(compoundFieldName, hasBoost: false);
                Voron.Slice.From(allocator, compositeKey, out var keySlice);

                // Single exact-term lookup on the compound tree
                compoundMatch = indexSearcher.TermQuery(compoundFieldMeta, keySlice);
                return true;
            }
        }

        return false;
    }

    private static byte[] BuildCompoundFieldBytes(string fieldName, ClauseExecution exec,
        IndexSearcher indexSearcher, QueryExecution plan)
    {
        var p = exec.PackedParamValue;
        if (p.ValueType == PackedParam.TypeString)
        {
            var meta = indexSearcher.FieldMetadataBuilder(fieldName, hasBoost: false);
            var analyzed = indexSearcher.EncodeAndApplyAnalyzer(meta, plan.StringValues[p.Param1]);
            if (analyzed.Size > byte.MaxValue) return null;
            var bytes = new byte[analyzed.Size];
            analyzed.CopyTo(bytes);
            return bytes;
        }
        if (p.ValueType == PackedParam.TypeLong)
        {
            var bytes = new byte[sizeof(long)];
            System.Buffers.Binary.BinaryPrimitives.WriteInt64BigEndian(
                bytes, Sparrow.Binary.Bits.SwapBytes(plan.LongValues[p.Param1]));
            return bytes;
        }
        if (p.ValueType == PackedParam.TypeDouble)
        {
            var bytes = new byte[sizeof(long)];
            long sortable = Sparrow.Binary.Bits.DoubleToSortableLong(plan.DoubleValues[p.Param1]);
            System.Buffers.Binary.BinaryPrimitives.WriteInt64BigEndian(
                bytes, Sparrow.Binary.Bits.SwapBytes(sortable));
            return bytes;
        }
        return null;
    }

    // ── Compound field optimization (with ORDER BY) ─────────────────────

    /// <summary>Check if WHERE + ORDER BY can be served by a compound tree scan.
    /// Condition: an Equals clause on field1, ORDER BY on field2 (or field1, or both),
    /// compound(field1, field2) exists in the index, and any residual clauses are
    /// entry-scan eligible.
    /// Returns a DirectScanMatch wrapping a compound tree StartsWith with optional
    /// residual predicate checking.</summary>
    public static bool TryCreateCompoundFieldMatch(
        QueryExecution plan, OrderMetadata[] orderByFields,
        PlanParameters planParams, QueryBuilderParameters builderParams,
        CompiledPlan compiledPlan, out IQueryMatch compoundMatch)
    {
        compoundMatch = null;

        if (orderByFields == null || orderByFields.Length == 0)
            return false;
        if (plan.Clauses == null || plan.Clauses.Count == 0 || plan.AllNegated)
            return false;

        var clauses = plan.Clauses;
        var execs = plan.Executions;
        var index = planParams.Index;
        var indexSearcher = planParams.IndexSearcher;
        var allocator = planParams.Allocator;

        // Determine which field to pair with the Equals clause for compound lookup.
        // Single ORDER BY: compound(equalsField, sortField)
        // Two ORDER BY fields: compound(orderBy[0], orderBy[1]) — the Equals must be on orderBy[0]
        string sortFieldName;
        string compoundField1ForMultiSort = null;
        if (orderByFields.Length == 1)
        {
            sortFieldName = orderByFields[0].Field.FieldName.ToString();
        }
        else if (orderByFields.Length == 2)
        {
            // Check if compound(orderBy[0], orderBy[1]) exists
            string f1 = orderByFields[0].Field.FieldName.ToString();
            string f2 = orderByFields[1].Field.FieldName.ToString();
            using (Voron.Slice.From(allocator, f1, out var s1))
            using (Voron.Slice.From(allocator, f2, out var s2))
            {
                if (index.HasCompoundField(s1, s2, out _))
                {
                    sortFieldName = f2; // The compound tree sorts by f1 then f2
                    compoundField1ForMultiSort = f1; // The Equals clause must be on f1
                }
                else
                {
                    return false; // No compound field for this ORDER BY pair
                }
            }
        }
        else
        {
            return false; // >2 ORDER BY fields not supported
        }

        // Find an Equals clause that, paired with the ORDER BY field, matches a compound field
        int drivingClauseIdx = -1;
        for (int i = 0; i < clauses.Count; i++)
        {
            var c = clauses[i];
            if (c.ClauseType != ClauseType.Equals || c.IsNegated || c.HasBoost)
                continue;
            var e = execs[i];
            if (e.BoostFactor > 0)
                continue;

            if (compoundField1ForMultiSort != null)
            {
                // Multi-field ORDER BY: the Equals must be on the first ORDER BY field
                if (c.FieldName == compoundField1ForMultiSort)
                {
                    drivingClauseIdx = i;
                    break;
                }
            }
            else
            {
                // Single-field ORDER BY: check compound(equalsField, sortField)
                using (Voron.Slice.From(allocator, c.FieldName, out var f1Slice))
                using (Voron.Slice.From(allocator, sortFieldName, out var sortSlice))
                {
                    if (index.HasCompoundField(f1Slice, sortSlice, out _))
                    {
                        drivingClauseIdx = i;
                        break;
                    }
                }
            }
        }

        if (drivingClauseIdx == -1)
            return false;

        var drivingClause = clauses[drivingClauseIdx];
        var drivingExec = execs[drivingClauseIdx];
        var packed = drivingExec.PackedParamValue;
        if (packed.IsNone)
            return false;

        // Look for an optional range clause on field2 (the sort field) — narrows the compound scan
        int field2RangeIdx = -1;
        for (int i = 0; i < clauses.Count; i++)
        {
            if (i == drivingClauseIdx) continue;
            if (clauses[i].FieldName != sortFieldName) continue;
            if (clauses[i].ClauseType is ClauseType.GreaterThan or ClauseType.GreaterThanOrEqual
                or ClauseType.LessThan or ClauseType.LessThanOrEqual or ClauseType.Between)
            {
                field2RangeIdx = i;
                break;
            }
        }

        // Check residual clauses are entry-scan eligible
        var residualPreds = new List<ScanPredicateInfo>();
        int longIdx = 0, doubleIdx = 0, sliceIdx = 0;
        for (int i = 0; i < clauses.Count; i++)
        {
            if (i == drivingClauseIdx || i == field2RangeIdx)
                continue;
            if (clauses[i].HasBoost || (execs[i] is { BoostFactor: > 0 }))
                return false; // boosted clauses need scoring — can't entry scan
            var pred = BuildScanPredicateInfo(clauses[i], execs[i], ref longIdx, ref doubleIdx, ref sliceIdx);
            if (pred == null)
                return false; // non-scannable residual → fall back to bitmap
            residualPreds.Add(pred.Value);
        }

        // Cost check: estimate scan count vs bitmap cost
        long drivingCardinality = drivingExec.Cardinality > 0 ? drivingExec.Cardinality : indexSearcher.NumberOfEntries;
        long bitmapCost = 0;
        for (int i = 0; i < clauses.Count; i++)
            bitmapCost += execs[i].Cardinality > 0 ? execs[i].Cardinality : indexSearcher.NumberOfEntries;

        // Estimate entries to scan (accounting for residual selectivity)
        long entriesToScan = drivingCardinality;
        if (residualPreds.Count > 0)
        {
            long minResidualCardinality = long.MaxValue;
            for (int i = 0; i < clauses.Count; i++)
            {
                if (i == drivingClauseIdx) continue;
                long card = execs[i].Cardinality > 0 ? execs[i].Cardinality : indexSearcher.NumberOfEntries;
                if (card < minResidualCardinality)
                    minResidualCardinality = card;
            }
            if (minResidualCardinality > 0 && minResidualCardinality < indexSearcher.NumberOfEntries)
            {
                double passRate = (double)minResidualCardinality / indexSearcher.NumberOfEntries;
                if (passRate > 0)
                    entriesToScan = (long)(drivingCardinality / passRate);
            }
        }

        long directCost = entriesToScan > long.MaxValue / QueryPrimitives.EntryScanCostMultiplier ? long.MaxValue : entriesToScan * QueryPrimitives.EntryScanCostMultiplier;
        if (directCost >= bitmapCost || entriesToScan > QueryPrimitives.EntryScanCountThreshold)
            return false; // bitmap is cheaper

        // Build the compound tree match
        string field1Name = drivingClause.FieldName;
        var compoundFieldName = $"compound({field1Name},{sortFieldName})";
        var compoundFieldMeta = indexSearcher.FieldMetadataBuilder(compoundFieldName, hasBoost: false);

        // Build the prefix bytes for field1's value.
        // String: analyzed via field1's analyzer. Numeric: Bits.SwapBytes big-endian encoding.
        Voron.Slice analyzedPrefix;
        string field1ValueStr;
        switch (packed.ValueType)
        {
            case PackedParam.TypeString:
            {
                field1ValueStr = plan.StringValues[packed.Param1];
                var field1Meta = builderParams != null
                    ? QueryBuilderHelper.GetFieldMetadata(in builderParams, field1Name, hasBoost: false)
                    : indexSearcher.FieldMetadataBuilder(field1Name, hasBoost: false);
                analyzedPrefix = indexSearcher.EncodeAndApplyAnalyzer(field1Meta, field1ValueStr);
                break;
            }
            case PackedParam.TypeLong:
            {
                long longVal = plan.LongValues[packed.Param1];
                field1ValueStr = longVal.ToString();
                var bytes = new byte[sizeof(long)];
                System.Buffers.Binary.BinaryPrimitives.WriteInt64BigEndian(bytes, Sparrow.Binary.Bits.SwapBytes(longVal));
                Voron.Slice.From(allocator, bytes, out analyzedPrefix);
                break;
            }
            case PackedParam.TypeDouble:
            {
                double dblVal = plan.DoubleValues[packed.Param1];
                field1ValueStr = dblVal.ToString(CultureInfo.InvariantCulture);
                long sortable = Sparrow.Binary.Bits.DoubleToSortableLong(dblVal);
                var bytes = new byte[sizeof(long)];
                System.Buffers.Binary.BinaryPrimitives.WriteInt64BigEndian(bytes, Sparrow.Binary.Bits.SwapBytes(sortable));
                Voron.Slice.From(allocator, bytes, out analyzedPrefix);
                break;
            }
            default:
                return false;
        }

        // Compound key trailing byte stores field1 length as a single byte.
        // If the analyzed prefix exceeds 255 bytes, the compound key format can't represent it.
        // Fall back to the bitmap pipeline which queries individual fields normally.
        if (analyzedPrefix.Size > byte.MaxValue)
            return false;

        IQueryMatch drivingMatch = null;
        if (field2RangeIdx >= 0)
        {
            // Compound range: build composite low/high keys incorporating the field2 bound
            var field2Exec = execs[field2RangeIdx];
            var field2Clause = clauses[field2RangeIdx];
            var field2Packed = field2Exec.PackedParamValue;

            if (field2Packed.IsNone == false)
            {
                // Encode field2 bound value into bytes (same encoding as indexing).
                // Long/Double: Bits.SwapBytes big-endian. String: analyze with field2's analyzer.
                byte[] field2Bytes = null;
                byte[] field2HighBytes = null;
                bool usePrefix = false;

                if (field2Packed.ValueType == PackedParam.TypeLong)
                {
                    field2Bytes = new byte[sizeof(long)];
                    System.Buffers.Binary.BinaryPrimitives.WriteInt64BigEndian(
                        field2Bytes, Sparrow.Binary.Bits.SwapBytes(plan.LongValues[field2Packed.Param1]));
                    if (field2Clause.ClauseType == ClauseType.Between)
                    {
                        field2HighBytes = new byte[sizeof(long)];
                        System.Buffers.Binary.BinaryPrimitives.WriteInt64BigEndian(
                            field2HighBytes, Sparrow.Binary.Bits.SwapBytes(plan.LongValues[field2Packed.Param2]));
                    }
                }
                else if (field2Packed.ValueType == PackedParam.TypeDouble)
                {
                    field2Bytes = new byte[sizeof(long)];
                    long sortable = Sparrow.Binary.Bits.DoubleToSortableLong(plan.DoubleValues[field2Packed.Param1]);
                    System.Buffers.Binary.BinaryPrimitives.WriteInt64BigEndian(
                        field2Bytes, Sparrow.Binary.Bits.SwapBytes(sortable));
                    if (field2Clause.ClauseType == ClauseType.Between)
                    {
                        field2HighBytes = new byte[sizeof(long)];
                        long highSortable = Sparrow.Binary.Bits.DoubleToSortableLong(plan.DoubleValues[field2Packed.Param2]);
                        System.Buffers.Binary.BinaryPrimitives.WriteInt64BigEndian(
                            field2HighBytes, Sparrow.Binary.Bits.SwapBytes(highSortable));
                    }
                }
                else if (field2Packed.ValueType == PackedParam.TypeString)
                {
                    // Analyze field2's value with the sort field's analyzer (same as indexing)
                    var field2Meta = builderParams != null
                        ? QueryBuilderHelper.GetFieldMetadata(in builderParams, sortFieldName, hasBoost: false)
                        : indexSearcher.FieldMetadataBuilder(sortFieldName, hasBoost: false);
                    var analyzed = indexSearcher.EncodeAndApplyAnalyzer(field2Meta, plan.StringValues[field2Packed.Param1]);
                    if (analyzed.Size > byte.MaxValue)
                        usePrefix = true;
                    else
                    {
                        field2Bytes = new byte[analyzed.Size];
                        analyzed.CopyTo(field2Bytes);
                        if (field2Clause.ClauseType == ClauseType.Between)
                        {
                            var analyzedHigh = indexSearcher.EncodeAndApplyAnalyzer(field2Meta, plan.StringValues[field2Packed.Param2]);
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

                if (usePrefix || field2Bytes == null)
                {
                    // Field2 value too long or unsupported type — fall back to prefix-only
                    drivingMatch = indexSearcher.StartWithQuery(compoundFieldMeta, analyzedPrefix,
                        isNegated: false, forward: orderByFields[0].Ascending,
                        validatePostfixLen: true);
                }
                else
                {

                    // Build low and high composite keys
                    int prefixLen = analyzedPrefix.Size;
                    int field2Len = field2Bytes.Length;
                    int keyLen = prefixLen + field2Len + 1; // +1 for field1 length byte
                    int highField2Len = field2HighBytes?.Length ?? field2Len;
                    int highKeyLen = prefixLen + highField2Len + 1;

                    // Check total key length against max
                    if (keyLen > Constants.Terms.MaxLength || highKeyLen > Constants.Terms.MaxLength)
                    {
                        drivingMatch = indexSearcher.StartWithQuery(compoundFieldMeta, analyzedPrefix,
                            isNegated: false, forward: orderByFields[0].Ascending, validatePostfixLen: true);
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

                    Voron.Slice.From(allocator, lowKeyBytes, out var lowSlice);
                    Voron.Slice.From(allocator, highKeyBytes, out var highSlice);

                    drivingMatch = indexSearcher.RangeBuilder<global::Corax.Querying.Matches.Meta.Range.Inclusive, global::Corax.Querying.Matches.Meta.Range.Inclusive>(
                        compoundFieldMeta, lowSlice, highSlice,
                        forward: orderByFields[0].Ascending, CancellationToken.None);
                }
            }
        }
        else
        {
            // Pure prefix scan (no field2 constraint)
            drivingMatch = indexSearcher.StartWithQuery(compoundFieldMeta, analyzedPrefix,
                isNegated: false, forward: orderByFields[0].Ascending,
                validatePostfixLen: true);
        }
        DrivingMatchReady:

        // Extract scan parameters for residual predicates
        ScanPredicateInfo[] residualArray = residualPreds.Count > 0 ? residualPreds.ToArray() : null;
        long[] longParams = null;
        double[] doubleParams = null;
        Voron.Slice[] sliceParams = null;
        long[] fieldRootPages = null;

        if (residualArray != null)
        {
            var longs = new List<long>();
            var doubles = new List<double>();
            var slices = new List<Voron.Slice>();
            var roots = new List<long>();

            int residualIdx = 0;
            for (int i = 0; i < clauses.Count; i++)
            {
                if (i == drivingClauseIdx || i == field2RangeIdx) continue;
                var matchingExec = execs[i];

                // Add field root page
                roots.Add(indexSearcher.FieldCache.GetLookupRootPage(clauses[i].FieldName));

                var predPacked = matchingExec.PackedParamValue;
                if (predPacked.IsNone) { residualIdx++; continue; }

                int idx1 = predPacked.Param1;
                int idx2 = predPacked.Param2;
                bool hasBetween = idx2 != PackedParam.NoParamValue;

                switch (residualArray[residualIdx].ValueType)
                {
                    case ScanValueType.Long:
                        longs.Add(plan.LongValues[idx1]);
                        if (hasBetween) longs.Add(plan.LongValues[idx2]);
                        break;
                    case ScanValueType.Double:
                        doubles.Add(plan.DoubleValues[idx1]);
                        if (hasBetween) doubles.Add(plan.DoubleValues[idx2]);
                        break;
                    case ScanValueType.Slice:
                    {
                        Voron.Slice.From(allocator, plan.StringValues[idx1], out var s1);
                        slices.Add(s1);
                        if (hasBetween)
                        {
                            Voron.Slice.From(allocator, plan.StringValues[idx2], out var s2);
                            slices.Add(s2);
                        }
                        break;
                    }
                }
                residualIdx++;
            }

            longParams = longs.Count > 0 ? longs.ToArray() : null;
            doubleParams = doubles.Count > 0 ? doubles.ToArray() : null;
            sliceParams = slices.Count > 0 ? slices.ToArray() : null;
            fieldRootPages = roots.Count > 0 ? roots.ToArray() : null;
        }

        var directScan = BuildDirectScan(
            indexSearcher, drivingMatch, longParams, doubleParams, sliceParams, fieldRootPages,
            compiledPlan.CompiledEntryPredicate, residualArray);
        directScan.DrivingTreeName = compoundFieldName;
        directScan.DrivingClause = $"{field1Name} = '{field1ValueStr}'";
        directScan.SeekBound = $"'{field1ValueStr}' (prefix, validatePostfixLen)";
        directScan.Direction = orderByFields[0].Ascending ? "Forward" : "Backward";
        directScan.ResidualDescription = residualArray != null
            ? string.Join(", ", residualPreds.ConvertAll(p => $"{p.FieldName} {p.CompareOp}"))
            : null;
        directScan.Reason = $"entries_to_scan({entriesToScan}) × {QueryPrimitives.EntryScanCostMultiplier} < bitmap_cost({bitmapCost})";

        compoundMatch = directScan;
        return true;
    }

    // ── Simple field direct scan ──────────────────────────────────────────

    /// <summary>Check if a range clause on the ORDER BY field can be served by a direct
    /// tree scan instead of the bitmap pipeline. The range query already walks the tree
    /// in sort order, so no SortingMatch wrapper is needed.</summary>
    public static bool TryCreateSimpleFieldDirectScan(
        QueryExecution plan, OrderMetadata[] orderByFields,
        PlanParameters planParams, QueryBuilderParameters builderParams,
        CompiledPlan compiledPlan, out IQueryMatch directMatch)
    {
        directMatch = null;

        if (orderByFields == null || orderByFields.Length != 1)
            return false;

        var indexSearcher = planParams.IndexSearcher;
        string sortFieldName = orderByFields[0].Field.FieldName.ToString();
        bool forward = orderByFields[0].Ascending;
        var sortFieldType = orderByFields[0].FieldType;

        var clauses = plan.Clauses;
        var execs = plan.Executions;
        bool isFullScan = clauses == null || clauses.Count == 0;

        if (isFullScan && plan.AllNegated)
            return false;

        ITermsProvider provider;
        Voron.Impl.LowLevelTransaction llt;
        long drivingCardinality;
        string drivingClauseDescription;
        int drivingIdx = -1;

        if (isFullScan)
        {
            // ── No WHERE at all, only ORDER BY ──
            // Full field tree walk — use BetweenQuery for numeric fields (correct numeric
            // comparison) and ExistsQuery for string/sequence fields.
            // ExistsQuery uses CompactKeyLookup byte comparison which doesn't match numeric order.
            // Only index-walkable field types qualify.
            if (sortFieldType is not (MatchCompareFieldType.Sequence or MatchCompareFieldType.Integer or MatchCompareFieldType.Floating))
                return false;
            var fieldMeta = orderByFields[0].Field;
            IQueryMatch fullScanMatch;
            if (sortFieldType == MatchCompareFieldType.Integer)
                fullScanMatch = indexSearcher.BetweenQuery(fieldMeta, long.MinValue, long.MaxValue, forward: forward);
            else if (sortFieldType == MatchCompareFieldType.Floating)
                fullScanMatch = indexSearcher.BetweenQuery(fieldMeta, double.MinValue, double.MaxValue, forward: forward);
            else
                fullScanMatch = indexSearcher.ExistsQuery(fieldMeta, forward: forward);
            if (fullScanMatch is not TermsProviderMatch tpm)
                return false;
            provider = tpm.Provider;
            llt = tpm.Llt;
            drivingCardinality = 0; // not used (cost check skipped)
            drivingClauseDescription = $"{sortFieldName} [all]";
        }
        else
        {
            // ── Find a range or equals clause on the sort field ──
            for (int i = 0; i < clauses.Count; i++)
            {
                if (clauses[i].FieldName != sortFieldName)
                    continue;
                if (clauses[i].ClauseType is not (ClauseType.GreaterThan or ClauseType.GreaterThanOrEqual
                    or ClauseType.LessThan or ClauseType.LessThanOrEqual or ClauseType.Between
                    or ClauseType.Equals))
                    continue;
                if (clauses[i].HasBoost || (execs[i] is { BoostFactor: > 0 }))
                    continue;
                drivingIdx = i;
                break;
            }

            if (drivingIdx == -1)
                return false;

            var drivingPacked = execs[drivingIdx].PackedParamValue;
            if (drivingPacked.IsNone)
                return false;

            var drivingClause = clauses[drivingIdx];
            var drivingExec = execs[drivingIdx];

            TermsProviderMatch tpm;
            if (drivingClause.ClauseType == ClauseType.Equals)
            {
                var eqMatch = ResolveEqualsClauseWithDirection(drivingClause, drivingExec, indexSearcher, plan, planParams, builderParams, forward);
                if (eqMatch is not TermsProviderMatch eq)
                    return false;
                tpm = eq;
            }
            else
            {
                var match = ResolveRangeClauseWithDirection(drivingClause, drivingExec, indexSearcher, plan, planParams, builderParams, forward);
                if (match is not TermsProviderMatch m)
                    return false;
                tpm = m;
            }

            provider = tpm.Provider;
            llt = tpm.Llt;
            drivingCardinality = drivingExec.Cardinality > 0 ? drivingExec.Cardinality : indexSearcher.NumberOfEntries;
            drivingClauseDescription = $"{drivingClause.FieldName} {drivingClause.ClauseType}";
        }

        // ── Residual predicates ──
        int longIdx = 0, doubleIdx = 0, sliceIdx = 0;
        var residualPreds = new List<ScanPredicateInfo>();
        if (isFullScan == false)
        {
            for (int i = 0; i < clauses.Count; i++)
            {
                if (i == drivingIdx) continue;
                if (clauses[i].HasBoost || (execs[i] is { BoostFactor: > 0 }))
                    return false;
                var pred = BuildScanPredicateInfo(clauses[i], execs[i], ref longIdx, ref doubleIdx, ref sliceIdx);
                if (pred == null)
                    return false;
                residualPreds.Add(pred.Value);
            }
        }

        // ── Cost check (skip for full scan — always beneficial) ──
        long entriesToScan = 0, bitmapCost = 0;
        if (isFullScan == false)
        {
            for (int i = 0; i < clauses.Count; i++)
                bitmapCost += execs[i].Cardinality > 0 ? execs[i].Cardinality : indexSearcher.NumberOfEntries;

            entriesToScan = drivingCardinality;
            if (residualPreds.Count > 0)
            {
                long minResidual = long.MaxValue;
                for (int i = 0; i < clauses.Count; i++)
                {
                    if (i == drivingIdx) continue;
                    long c = execs[i].Cardinality > 0 ? execs[i].Cardinality : indexSearcher.NumberOfEntries;
                    if (c < minResidual) minResidual = c;
                }
                if (minResidual > 0 && minResidual < indexSearcher.NumberOfEntries)
                {
                    double passRate = (double)minResidual / indexSearcher.NumberOfEntries;
                    if (passRate > 0) entriesToScan = (long)(drivingCardinality / passRate);
                }
            }

            long directCost = entriesToScan > long.MaxValue / QueryPrimitives.EntryScanCostMultiplier ? long.MaxValue : entriesToScan * QueryPrimitives.EntryScanCostMultiplier;
            if (directCost >= bitmapCost || entriesToScan > QueryPrimitives.EntryScanCountThreshold)
                return false;
        }

        // ── Create the driving match ──
        bool nullIsSmallest = (orderByFields[0].NullsSortMode ?? builderParams.Index.Configuration.NullsSortMode) == NullsSortMode.NullsSmallest;
        bool nullFirst = forward ? nullIsSmallest : !nullIsSmallest;
        // BetweenQuery and StartWithQuery don't include nulls in their term output,
        // so SortedDrivingMatch must drain them itself (respecting nullFirst direction).
        bool drainNulls = true;
        var drivingMatch = new SortedDrivingMatch(provider, llt, planParams.Allocator,
            indexSearcher, orderByFields[0].Field, nullFirst, drainNulls);

        // ── Residual scan parameters ──
        ScanPredicateInfo[] residualArray = residualPreds.Count > 0 ? residualPreds.ToArray() : null;
        long[] longParams = null;
        double[] doubleParams = null;
        Voron.Slice[] sliceParams = null;
        long[] fieldRootPages = null;

        if (residualArray != null && clauses != null)
        {
            var longs = new List<long>();
            var doubles = new List<double>();
            var slices = new List<Voron.Slice>();
            var roots = new List<long>();

            int residualIdx = 0;
            for (int i = 0; i < clauses.Count; i++)
            {
                if (i == drivingIdx) continue;
                roots.Add(indexSearcher.FieldCache.GetLookupRootPage(clauses[i].FieldName));
                var predPacked = execs[i].PackedParamValue;
                if (predPacked.IsNone) { residualIdx++; continue; }
                int idx1 = predPacked.Param1;
                int idx2 = predPacked.Param2;
                bool hasBetween = idx2 != PackedParam.NoParamValue;
                switch (residualArray[residualIdx].ValueType)
                {
                    case ScanValueType.Long:
                        longs.Add(plan.LongValues[idx1]);
                        if (hasBetween) longs.Add(plan.LongValues[idx2]);
                        break;
                    case ScanValueType.Double:
                        doubles.Add(plan.DoubleValues[idx1]);
                        if (hasBetween) doubles.Add(plan.DoubleValues[idx2]);
                        break;
                    case ScanValueType.Slice:
                        Voron.Slice.From(planParams.Allocator, plan.StringValues[idx1], out var s1);
                        slices.Add(s1);
                        if (hasBetween) { Voron.Slice.From(planParams.Allocator, plan.StringValues[idx2], out var s2); slices.Add(s2); }
                        break;
                }
                residualIdx++;
            }
            longParams = longs.Count > 0 ? longs.ToArray() : null;
            doubleParams = doubles.Count > 0 ? doubles.ToArray() : null;
            sliceParams = slices.Count > 0 ? slices.ToArray() : null;
            fieldRootPages = roots.Count > 0 ? roots.ToArray() : null;
        }

        var ds = BuildDirectScan(
            indexSearcher, drivingMatch, longParams, doubleParams, sliceParams, fieldRootPages,
            compiledPlan.CompiledEntryPredicate, residualArray);
        ds.DrivingTreeName = sortFieldName;
        ds.DrivingClause = drivingClauseDescription;
        ds.Direction = orderByFields[0].Ascending ? "Forward" : "Backward";
        ds.ResidualDescription = residualArray != null
            ? string.Join(", ", residualPreds.ConvertAll(p => $"{p.FieldName} {p.CompareOp}"))
            : null;
        ds.Reason = isFullScan
            ? "full index-only scan (no WHERE clause)"
            : $"entries_to_scan({entriesToScan}) × {QueryPrimitives.EntryScanCostMultiplier} < bitmap_cost({bitmapCost})";
        directMatch = ds;
        return true;
    }

    // ── Sort seek hint ────────────────────────────────────────────────────

    /// <summary>If the first clause is a range predicate on the same field as the first
    /// ORDER BY field, set a seek hint on the CompiledQueryMatch so SortedIndexReader
    /// can skip walking irrelevant tree terms.</summary>
    public static void TrySetSortSeekHint(CompiledQueryMatch match,
        QueryExecution plan, OrderMetadata[] orderByFields)
    {
        if (orderByFields == null || orderByFields.Length == 0)
            return;

        var clauses = plan.Clauses;
        var execs = plan.Executions;
        if (clauses == null || clauses.Count == 0 || execs == null || execs.Length == 0)
            return;

        // Only consider the first ORDER BY field
        var sortField = orderByFields[0].Field.FieldName;

        // Find a range clause on the same field (scan all clauses, not just first — the sort-eligible
        // clause may not be the cheapest and thus not clause[0]).
        for (int i = 0; i < clauses.Count; i++)
        {
            var clause = clauses[i];
            var exec = execs[i];

            if (clause.FieldName != sortField.ToString())
                continue;

            // Range clauses on the sort field — supports long, double, and string
            if (clause.ClauseType is not (ClauseType.GreaterThan or ClauseType.GreaterThanOrEqual
                or ClauseType.LessThan or ClauseType.LessThanOrEqual or ClauseType.Between))
                continue;

            var packed = exec.PackedParamValue;
            if (packed.IsNone)
                continue;

            // For ascending order: seek to the lower bound (GT/GTE value)
            // For descending order: seek to the upper bound (LT/LTE value)
            bool ascending = orderByFields[0].Ascending;
            object seekValue = null;
            bool inclusive = false;

            if (ascending && clause.ClauseType is ClauseType.GreaterThan or ClauseType.GreaterThanOrEqual)
            {
                seekValue = packed.ValueType switch
                {
                    PackedParam.TypeLong => plan.LongValues[packed.Param1],
                    PackedParam.TypeDouble => plan.DoubleValues[packed.Param1],
                    PackedParam.TypeString => plan.StringValues[packed.Param1],
                    _ => null
                };
                inclusive = clause.ClauseType == ClauseType.GreaterThanOrEqual;
            }
            else if (ascending == false && clause.ClauseType is ClauseType.LessThan or ClauseType.LessThanOrEqual)
            {
                seekValue = packed.ValueType switch
                {
                    PackedParam.TypeLong => plan.LongValues[packed.Param1],
                    PackedParam.TypeDouble => plan.DoubleValues[packed.Param1],
                    PackedParam.TypeString => plan.StringValues[packed.Param1],
                    _ => null
                };
                inclusive = clause.ClauseType == ClauseType.LessThanOrEqual;
            }
            else if (clause.ClauseType == ClauseType.Between)
            {
                // Between: seek to the lower bound for ASC, upper bound for DESC
                int idx = ascending ? packed.Param1 : packed.Param2;
                seekValue = packed.ValueType switch
                {
                    PackedParam.TypeLong => plan.LongValues[idx],
                    PackedParam.TypeDouble => plan.DoubleValues[idx],
                    PackedParam.TypeString => plan.StringValues[idx],
                    _ => null
                };
                inclusive = true; // Between is always inclusive on both sides
            }

            if (seekValue != null)
            {
                match.SortHint = new SortHint(clause.FieldName, seekValue, inclusive);
                return;
            }
        }
    }

    // ── Sorting / Ordering ───────────────────────────────────────────────

    public static OrderMetadata[] GetSortMetadata(QueryBuilderParameters builderParameters, out bool hasEmpty)
    {
        hasEmpty = false;
        var query = builderParameters.Query;
        var index = builderParameters.Index;
        var getSpatialField = builderParameters.Factories?.GetSpatialFieldFactory;
        var indexMapping = builderParameters.IndexFieldsMapping;
        var queryMapping = builderParameters.FieldsToFetch;
        var allocator = builderParameters.Allocator;
        if (query.PageSize == 0) // no need to sort when counting only
        {
            return null;
        }

        var orderByFields = query.Metadata.OrderBy;

        if (orderByFields == null)
        {
            if (builderParameters.HasBoost && (
                    index.Configuration.OrderByScoreAutomaticallyWhenBoostingIsInvolved
                    || index.Configuration.CoraxVectorSearchOrderByScoreAutomatically))
            {
                if (builderParameters.Metadata.HasVectorSearch == false)
                    builderParameters.IndexReadOperation?.AssertCanOrderByScoreAutomaticallyWhenBoostingOrVectorSearchIsInvolved();

                return [new OrderMetadata(true, MatchCompareFieldType.Score)];
            }

            return null;
        }

        int sortIndex = 0;
        var sortArray = new OrderMetadata[16];

        if (orderByFields.Length > sortArray.Length)
            throw new InvalidOperationException($"Corax does not support ordering by more than {sortArray.Length} properties.");

        foreach (var field in orderByFields)
        {
            var nullsSortMode = (field.NullsOrdering, field.Ascending) switch
            {
                (NullsOrderingType.First, Ascending: true) => NullsSortMode.NullsSmallest,
                (NullsOrderingType.First, Ascending: false) => NullsSortMode.NullsLargest,
                (NullsOrderingType.Last, Ascending: true) => NullsSortMode.NullsLargest,
                (NullsOrderingType.Last, Ascending: false) => NullsSortMode.NullsSmallest,
                _ => (NullsSortMode?)null
            };

            if (field.OrderingType == OrderByFieldType.Random)
            {
                var seed = field.Arguments is { Length: > 0 } ?
                    (int)Hashing.XXHash32.CalculateRaw(field.Arguments[0].NameOrValue) :
                    Random.Shared.Next();
                sortArray[sortIndex++] = new OrderMetadata(seed);
                continue;
            }

            if (field.OrderingType == OrderByFieldType.Score)
            {
                // EntryComparerByScore.Compare is intentionally inverted (returns y.CompareTo(x)),
                // so ascending=true -> highest scores first (the default "most relevant first" search engine order).
                // ascending=false -> Descending<EntryComparerByScore> -> lowest scores first.
                sortArray[sortIndex++] = new OrderMetadata(true, MatchCompareFieldType.Score, field.Ascending);

                continue;
            }

            var fieldMetadata = QueryBuilderHelper.GetFieldIdForOrderBy(allocator, field.Name, index, builderParameters.HasDynamics,
                builderParameters.DynamicFields, indexMapping, queryMapping, false);

            bool fieldIsEmpty = builderParameters.IndexSearcher.GetTermAmountInField(fieldMetadata) == 0;
            if (fieldIsEmpty)
            {
                if (builderParameters.IndexReadOperation.IsSharded == false)
                    continue;
                hasEmpty = true;
            }

            if (field.OrderingType == OrderByFieldType.Distance)
            {
                var spatialField = getSpatialField(field.Name);

                int lastArgument;
                IPoint point;
                switch (field.Method)
                {
                    case MethodType.Spatial_Circle:
                        var cLatitude = field.Arguments[1].GetDouble(query.QueryParameters);
                        var cLongitude = field.Arguments[2].GetDouble(query.QueryParameters);
                        lastArgument = 2;
                        point = spatialField.ReadPoint(cLatitude, cLongitude).Center;
                        break;
                    case MethodType.Spatial_Wkt:
                        var wkt = field.Arguments[0].GetString(query.QueryParameters);
                        SpatialUnits? spatialUnits = null;
                        lastArgument = 1;
                        if (field.Arguments.Length > 1)
                        {
                            spatialUnits = Enum.Parse<SpatialUnits>(field.Arguments[1].GetString(query.QueryParameters), ignoreCase: true);
                            lastArgument = 2;
                        }

                        point = spatialField.ReadShape(wkt, spatialUnits).Center;
                        break;
                    case MethodType.Spatial_Point:
                        var pLatitude = field.Arguments[0].GetDouble(query.QueryParameters);
                        var pLongitude = field.Arguments[1].GetDouble(query.QueryParameters);
                        lastArgument = 2;
                        point = spatialField.ReadPoint(pLatitude, pLongitude).Center;
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }

                var roundTo = field.Arguments.Length > lastArgument
                    ? field.Arguments[lastArgument].GetDouble(query.QueryParameters)
                    : 0D;

                sortArray[sortIndex++] = new OrderMetadata(fieldMetadata, field.Ascending, MatchCompareFieldType.Spatial, point, roundTo,
                    spatialField.Units is SpatialUnits.Kilometers
                        ? global::Corax.Utils.Spatial.SpatialUnits.Kilometers
                        : global::Corax.Utils.Spatial.SpatialUnits.Miles, fieldIsEmpty, nullsSortMode);
                continue;
            }

            var orderingType = field.OrderingType;
            if (orderingType is OrderByFieldType.Implicit && index.Configuration.OrderByTicksAutomaticallyWhenDatesAreInvolved && index.IndexFieldsPersistence.HasTimeValues(field.Name.Value))
                orderingType = OrderByFieldType.Long;

            var metadataField = QueryBuilderHelper.GetFieldIdForOrderBy(allocator, field.Name.Value, index, builderParameters.HasDynamics,
                builderParameters.DynamicFields,
                indexMapping, queryMapping, false);
            OrderMetadata? temporaryOrder = null;
            switch (orderingType)
            {
                case OrderByFieldType.Custom:
                    throw new NotSupportedInCoraxException($"{nameof(Corax)} doesn't support Custom OrderBy.");
                case OrderByFieldType.AlphaNumeric:
                    sortArray[sortIndex++] = new OrderMetadata(metadataField, field.Ascending, MatchCompareFieldType.Alphanumeric, fieldIsEmpty, nullsSortMode);
                    continue;
                case OrderByFieldType.Long:
                    temporaryOrder = new OrderMetadata(metadataField, field.Ascending, MatchCompareFieldType.Integer, fieldIsEmpty, nullsSortMode);
                    break;
                case OrderByFieldType.Double:
                    temporaryOrder = new OrderMetadata(metadataField, field.Ascending, MatchCompareFieldType.Floating, fieldIsEmpty, nullsSortMode);
                    break;
            }

            sortArray[sortIndex++] = temporaryOrder ?? new OrderMetadata(metadataField, field.Ascending, MatchCompareFieldType.Sequence, fieldIsEmpty, nullsSortMode);
        }

        return sortArray[0..sortIndex];
    }

    public static IQueryMatch OrderBy(QueryBuilderParameters builderParameters, IQueryMatch match, in OrderMetadata[] orderMetadataSource, bool hasEmptySortingMatches)
    {
        RuntimeHelpers.EnsureSufficientExecutionStack();
        var indexSearcher = builderParameters.IndexSearcher;
        var take = builderParameters.Take;
        OrderMetadata[] orderMetadata = null;
        if (hasEmptySortingMatches == false)
            orderMetadata = orderMetadataSource;
        else
        {
            var currentIdx = 0;
            foreach (var orderMetadataItem in orderMetadataSource)
            {
                if (orderMetadataItem.FieldHasNoTerms)
                    continue;

                orderMetadata ??= new OrderMetadata[orderMetadataSource.Length];
                orderMetadata[currentIdx++] = orderMetadataItem;
            }

            orderMetadata = currentIdx == 0 ? [] : orderMetadata![..currentIdx];
        }

        switch (orderMetadata.Length)
        {
            case 0:
                return match;
            case 1:
                return indexSearcher.OrderBy(match, orderMetadata[0], builderParameters.Index.Configuration.NullsSortMode, take, builderParameters.Token);
            default:
                return indexSearcher.OrderBy(match, orderMetadata, builderParameters.Index.Configuration.NullsSortMode, take, builderParameters.Token);
        }
    }

    /// <summary>Apply ORDER BY from plan metadata when a full <see cref="QueryBuilderParameters"/> is not
    /// available (e.g., direct tests). Handles <c>ORDER BY score()</c> only — callers that need
    /// field / spatial / alphanumeric sorts must use the full
    /// <see cref="OrderBy(QueryBuilderParameters,IQueryMatch,in OrderMetadata[],bool)"/> overload.</summary>
    public static IQueryMatch ApplyScoreOrdering(PlanParameters planParams, IQueryMatch match, long take, CancellationToken token = default)
    {
        OrderByField[] orderByFields = planParams.Metadata.OrderBy;
        if (orderByFields == null || orderByFields.Length == 0)
            return match;

        var indexSearcher = planParams.IndexSearcher;
        int takeInt = take > int.MaxValue ? Constants.IndexSearcher.TakeAll : (int)take;

        for (int i = 0; i < orderByFields.Length; i++)
        {
            if (orderByFields[i].OrderingType == OrderByFieldType.Score)
            {
                var meta = new OrderMetadata(true, MatchCompareFieldType.Score, orderByFields[i].Ascending);
                return indexSearcher.OrderBy(match, meta, NullsSortMode.NullsLargest, take: takeInt, token: token);
            }
        }

        return match;
    }

    // ── Search helpers (used by ResolveClause for Search clause type) ────

    /// <summary>
    /// Replaces the search analyzer with an appropriate wildcard analyzer.
    /// LuceneAnalyzerAdapter wrapping KeywordAnalyzer has IsExactAnalyzer=false
    /// (because LuceneAnalyzerAdapter passes NoTransformers), so the generic
    /// CreateWildcardAnalyzer in the Legacy path would incorrectly lowercase the term.
    /// This matches the old CoraxQueryBuilder.ReplaceAnalyzerForWildcardQueries logic.
    /// </summary>
    private static FieldMetadata ReplaceAnalyzerForWildcardQueries(
        FieldMetadata searchMeta,
        QueryBuilderParameters builderParams,
        PlanParameters parameters)
    {
        var result = searchMeta;
        var indexFieldsMapping = builderParams?.IndexFieldsMapping ?? parameters?.IndexFieldsMapping;

        if (searchMeta.IsDynamic && indexFieldsMapping != null)
            result = searchMeta.ChangeAnalyzer(searchMeta.Mode, indexFieldsMapping.SearchAnalyzer(searchMeta.FieldName.ToString()));

        if (searchMeta.Analyzer is Lucene.LuceneAnalyzerAdapter laa && indexFieldsMapping != null)
        {
            global::Corax.Analyzers.Analyzer replacementAnalyzer = laa.Analyzer switch
            {
                global::Lucene.Net.Analysis.KeywordAnalyzer => indexFieldsMapping.ExactAnalyzer(searchMeta.FieldName.ToString()),
                Lucene.Analyzers.RavenStandardAnalyzer
                    or Lucene.Analyzers.NGramAnalyzer => indexFieldsMapping.DefaultAnalyzer,
                global::Lucene.Net.Analysis.Standard.StandardAnalyzer when laa.Analyzer.GetType() == typeof(global::Lucene.Net.Analysis.Standard.StandardAnalyzer)
                    => indexFieldsMapping.DefaultAnalyzer,
                Lucene.Analyzers.LowerCaseKeywordAnalyzer
                    or Lucene.Analyzers.Collation.CollationAnalyzer => indexFieldsMapping.DefaultAnalyzer,
                _ => null
            };

            if (replacementAnalyzer != null)
                result = searchMeta.ChangeAnalyzer(global::Corax.FieldIndexingMode.Search, replacementAnalyzer);
        }

        return result;
    }

    /// <summary>Split search term value respecting quoted phrases.
    /// "nonexists \"second third\" nonexsts" -> ["nonexists", "second third", "nonexsts"]
    /// Same logic as old CoraxQueryBuilder.GetValues().</summary>
    private static IEnumerable<string> SplitSearchTerms(string value)
    {
        if (string.IsNullOrEmpty(value))
        {
            yield return value;
            yield break;
        }

        bool quoted = false;
        int lastStart = 0;

        for (int i = 0; i < value.Length; i++)
        {
            char c = value[i];
            if (c == '"')
            {
                if (i > 0 && value[i - 1] == '\\')
                    continue; // escaped quote

                if (lastStart != i)
                    yield return value.Substring(lastStart, i - lastStart);

                quoted = !quoted;
                lastStart = i + 1;
            }
            else if ((c == ' ' || c == '\t') && !quoted)
            {
                if (lastStart != i)
                    yield return value.Substring(lastStart, i - lastStart);
                lastStart = i + 1;
            }
        }

        if (value.Length - lastStart > 0)
            yield return value.Substring(lastStart, value.Length - lastStart);
    }

    /// <summary>Create the appropriate DirectScan match based on whether residual predicates exist.</summary>
    private static DirectScanMatchBase BuildDirectScan(
        IndexSearcher searcher, IQueryMatch drivingMatch,
        long[] longParams, double[] doubleParams, Voron.Slice[] sliceParams, long[] fieldRootPages,
        ResidualScanIlEmitter.ResidualScanPredicate residualDelegate,
        ScanPredicateInfo[]? residualArray)
    {
        if (residualArray == null) 
            return new DirectScanSimpleMatch(searcher, drivingMatch, take: -1);
        
        return new DirectScanFilteredMatch(
            searcher, drivingMatch, longParams, doubleParams, sliceParams, fieldRootPages,
            take: -1, precompiledDelegate: residualDelegate);
    }

    /// <summary>Singleton no-op ITermsProvider for TreeScan slots where the field doesn't exist.
    /// FillPostingListIds returns 0 immediately, so the bitmap op is a no-op.</summary>
    private sealed class EmptyTermsProviderInstance : ITermsProvider
    {
        public static readonly EmptyTermsProviderInstance Instance = new();
        public bool IsFillSupported => false;
        public int Fill(Span<long> containers) => 0;
        public int FillPostingListIds(Span<long> postingListIds) => 0;
        public void Reset() { }
        public bool Next(out TermMatch term) { term = default; return false; }
        public QueryInspectionNode Inspect() => new("EmptyTermsProvider");
    }
}
