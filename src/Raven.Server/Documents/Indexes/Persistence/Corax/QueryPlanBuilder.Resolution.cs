using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
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
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Corax;
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

    /// <summary>Maximum number of ORDER BY fields supported by Corax.</summary>
    private const int MaxSortFields = 16;

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

        // Phase 1: structural template (cached per queryText).
        var template = BuildTemplate(planParams);

        // Phase 2: parameter resolution, plan emission, IL compile (with cache miss handling).
        compiledPlanOut = Build(template, planParams, builderParameters, out plan);
        if (compiledPlanOut == null)
            return TermMatch.CreateEmpty(indexSearcher, indexSearcher.Allocator);

        // Phase 3: live binding (resolved matches, term sources, spatial/vector/highlighting wrappers).
        // BuildAndCompile uses the unconditional bitmap path — ORDER BY optimization dispatch
        // (CompoundExact / CompoundField / DirectScan) belongs to BuildCompileAndOptimize.
        return InstantiateBitmapPipeline(compiledPlanOut, plan, planParams, builderParameters, highlightingTerms, wantTimings, token);
    }

    /// <summary>
    /// Full pipeline: build, compile, optimize, and apply ORDER BY — returns the final
    /// <see cref="IQueryMatch"/> ready for result iteration. Encapsulates the optimization
    /// dispatch (compound-exact, compound-field, direct-scan) that previously lived in
    /// <c>CoraxIndexReadOperation</c>. The <see cref="ExecutionStrategy"/> on the compiled plan
    /// caches which optimization succeeded on the first execution; subsequent cache-hit
    /// executions skip the Try* chain entirely.
    /// </summary>
    /// <returns>
    /// A result object containing the final query match, the resolved plan, the inner match,
    /// the compiled plan, and resolved ORDER BY metadata.
    /// </returns>
    internal readonly record struct BuildCompileAndOptimizeResult(
        IQueryMatch QueryMatch,
        IQueryMatch ExecutedMatch,
        IQueryMatch SortingWrapper,
        CompiledPlan CompiledPlan,
        QueryBuilderParameters QueryBuilderParams,
        OrderMetadata[] OrderByFields) : IDisposable
    {
        public void Dispose()
        {
            (QueryMatch as IDisposable)?.Dispose();
            (SortingWrapper as IDisposable)?.Dispose();
        }
    }

    public static BuildCompileAndOptimizeResult BuildCompileAndOptimize(PlanParameters planParams,
        QueryBuilderParameters builderParameters,
        Dictionary<string, CoraxHighlightingTermIndex> highlightingTerms,
        bool wantTimings,
        CancellationToken token)
    {
        var indexSearcher = planParams.IndexSearcher;

        // Phase 1: structural template (cached per queryText).
        var template = BuildTemplate(planParams);

        // Phase 2: parameter resolution, plan emission, IL compile (with cache-miss handling).
        var compiledPlan = Build(template, planParams, builderParameters, out var plan);
        if (compiledPlan == null)
        {
            var emptyMatch = TermMatch.CreateEmpty(indexSearcher, indexSearcher.Allocator);
            return new BuildCompileAndOptimizeResult(emptyMatch, emptyMatch, null, null, builderParameters, null);
        }

        // Phase 3a: resolve ORDER BY metadata (needed by Instantiate's strategy dispatch).
        var orderByFields = GetSortMetadata(builderParameters, out var hasEmptySorts);
        // Phase 3b: dispatch on the cached ExecutionStrategy (fast path) or run Try* discovery
        // (slow path, cache-miss only). Instantiate falls through to the bitmap pipeline as the
        // last resort. All four strategies — CompoundExact / CompoundField / DirectScan / BitmapSort
        // — are produced here.
        var queryMatch = Instantiate(compiledPlan, plan, orderByFields, hasEmptySorts,
            planParams, builderParameters, highlightingTerms, wantTimings, out var innerMatch, token);
        return new BuildCompileAndOptimizeResult(queryMatch, innerMatch, queryMatch == innerMatch ? null : queryMatch, compiledPlan, builderParameters, orderByFields);
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
    private static IQueryMatch Instantiate(
        CompiledPlan compiledPlan,
        QueryExecution plan,
        OrderMetadata[] orderByFields,
        bool hasEmptySorts,
        PlanParameters planParams,
        QueryBuilderParameters builderParameters,
        Dictionary<string, CoraxHighlightingTermIndex> highlightingTerms,
        bool wantTimings,
        out IQueryMatch innerMatch,
        CancellationToken token)
    {
        // ── Fast path: cached strategy, dispatch directly to Construct* ──
        var strategy = compiledPlan.Strategy;
        if (strategy != ExecutionStrategy.NotEvaluated)
        {
            IQueryMatch built = null;
            switch (strategy)
            {
                case ExecutionStrategy.CompoundExact:
                    if (plan != null)
                        built = ConstructCompoundExact(plan, planParams);
                    if (built != null)
                    {
                        innerMatch = built;
                        return built;
                    }

                    break;
                case ExecutionStrategy.CompoundField:
                    if (plan != null && orderByFields != null)
                    {
                        int f2 = FindCompoundFieldField2Range(plan.Executions, plan.Plan.CompoundFieldDrivingClause, plan.Plan.CompoundFieldSortName);
                        // Cost facts (entriesToScan, bitmapCost) are diagnostic-only inside
                        // Construct (used for the Reason string). Pass zeros — the cliff bit
                        // in the cache key already segregates cost buckets.
                        built = ConstructCompoundField(plan, orderByFields, planParams, builderParameters,
                            compiledPlan, f2, entriesToScan: 0, bitmapCost: 0);
                    }

                    if (built != null)
                    {
                        innerMatch = built;
                        return OrderBy(builderParameters, built, orderByFields, hasEmptySorts);
                    }

                    break;
                case ExecutionStrategy.DirectScan:
                    if (plan != null && orderByFields is { Length: <= 2 })
                    {
                        var execs2 = plan.Executions;
                        bool isFullScan = execs2 == null || execs2.Length == 0;
                        bool hasTieBreak = orderByFields.Length == 2;
                        // Re-resolve drivingIdx (cheap structural lookup; cached at template time
                        // via SortDrivingClauseIndex). Skip cost gating — strategy cache encodes
                        // the cost decision via cliff bit in Ordering.
                        int drivingIdx = -1;
                        if (isFullScan == false)
                        {
                            drivingIdx = plan.Plan.SortDrivingClauseIndex;
                            // BoostFactor cannot land here: ComputeOptFlags clears
                            // DirectScanCandidate template-wide for any boost; only IsNone
                            // (parameter resolved to null) can still invalidate at runtime.
                            if (drivingIdx >= 0 && plan.Executions[drivingIdx].PackedParamValue.IsNone)
                                drivingIdx = -1;
                        }

                        if (isFullScan || drivingIdx >= 0)
                            built = ConstructDirectScan(plan, orderByFields, planParams, builderParameters,
                                compiledPlan, drivingIdx, isFullScan, hasTieBreak,
                                entriesToScan: 0, bitmapCost: 0);
                    }

                    if (built != null)
                    {
                        innerMatch = built;
                        return built;
                    }

                    break;
            }

            // Cached non-BitmapSort strategy failed at construction time (e.g. per-execution
            // byte-length overflow). Fall back to bitmap+sort. The cached strategy stays valid
            // for the next execution; this one had unlucky parameter values.
            return InstantiateBitmapFallback(compiledPlan, plan, orderByFields, hasEmptySorts,
                planParams, builderParameters, highlightingTerms, wantTimings, out innerMatch, token);
        }

        // ── Slow path: cache-miss, run Try* discovery chain ──
        // Each Try* method performs structural + cost checks and, on success, constructs the
        // live match. We record the outcome on a DecisionTrail for diagnostics, then bake the
        // winning strategy + *Data onto compiledPlan so future executions skip discovery.
        bool needsFullChain = true;
        var resultStrategy = ExecutionStrategy.BitmapSort;
        var trail = new PlanDecisionTrail();
        IQueryMatch queryMatch = null;
        innerMatch = null;

        if (plan == null)
            trail.Record("CompoundExact", false, "no plan available");
        else if ((compiledPlan.OptimizationFlags & PlanOptimizationFlags.CompoundExactCandidate) == 0)
            trail.Record("CompoundExact", false, "template has no compound-exact candidate");
        else if (TryCreateCompoundExactMatch(plan, planParams, builderParameters, out var compoundExact, out var ceReason))
        {
            queryMatch = compoundExact;
            innerMatch = compoundExact;
            resultStrategy = ExecutionStrategy.CompoundExact;
            needsFullChain = false;
            trail.Record("CompoundExact", true, "compound exact-term lookup");
        }
        else
            trail.Record("CompoundExact", false, ceReason ?? "rejected");

        if (orderByFields != null)
        {
            if (needsFullChain)
            {
                if (plan == null)
                    trail.Record("CompoundField", false, "no plan available");
                else if ((compiledPlan.OptimizationFlags & PlanOptimizationFlags.DirectScanCandidate) == 0)
                    trail.Record("CompoundField", false, "template has no direct-scan candidate");
                else if (TryCreateCompoundFieldMatch(plan, orderByFields, planParams, builderParameters, compiledPlan, out var compoundMatch, out var cfReason))
                {
                    innerMatch = compoundMatch;
                    queryMatch = OrderBy(builderParameters, compoundMatch, orderByFields, hasEmptySorts);
                    resultStrategy = ExecutionStrategy.CompoundField;
                    needsFullChain = false;
                    trail.Record("CompoundField", true, "compound tree scan with ORDER BY");
                }
                else
                    trail.Record("CompoundField", false, cfReason ?? "rejected");
            }

            if (needsFullChain)
            {
                if (plan == null)
                    trail.Record("DirectScan", false, "no plan available");
                else if ((compiledPlan.OptimizationFlags & PlanOptimizationFlags.DirectScanCandidate) == 0)
                    trail.Record("DirectScan", false, "template has no direct-scan candidate");
                else if (TryCreateSimpleFieldDirectScan(plan, orderByFields, planParams, builderParameters, compiledPlan, out var directMatch, out var dsReason))
                {
                    queryMatch = directMatch;
                    innerMatch = directMatch;
                    resultStrategy = ExecutionStrategy.DirectScan;
                    needsFullChain = false;
                    trail.Record("DirectScan", true, "direct tree scan on sort field");
                }
                else
                    trail.Record("DirectScan", false, dsReason ?? "rejected");
            }

            if (needsFullChain)
            {
                // All Try* failed → fall back to bitmap pipeline.
                var bitmapMatch = InstantiateBitmapPipeline(compiledPlan, plan, planParams, builderParameters, highlightingTerms, wantTimings, token);
                innerMatch = bitmapMatch;
                if (bitmapMatch is CompiledQueryMatch seekMatch)
                    TrySetSortSeekHint(seekMatch, plan, orderByFields);
                queryMatch = OrderBy(builderParameters, bitmapMatch, orderByFields, hasEmptySorts);
                resultStrategy = ExecutionStrategy.BitmapSort;
                trail.Record("BitmapSort", true, "bitmap pipeline with SortingMatch fallback");
            }
        }
        else
        {
            trail.Record("NoOrderBy", true, "no ORDER BY");
            if (queryMatch == null)
            {
                // No ORDER BY and no Try* winner — use bitmap match.
                queryMatch = InstantiateBitmapPipeline(compiledPlan, plan, planParams, builderParameters, highlightingTerms, wantTimings, token);
                innerMatch = queryMatch;
            }
        }

        compiledPlan.Strategy = resultStrategy;
        compiledPlan.DecisionTrail = trail;

        return queryMatch;
    }

    /// <summary>Bitmap-pipeline fallback used when a cached non-BitmapSort strategy fails at
    /// construction time (per-execution rejection, not a structural rejection). Applies ORDER BY
    /// when present and sets the seek hint on the inner CompiledQueryMatch. The cached strategy
    /// is intentionally NOT downgraded — the next execution's parameters may construct
    /// successfully.</summary>
    private static IQueryMatch InstantiateBitmapFallback(
        CompiledPlan compiledPlan, QueryExecution plan,
        OrderMetadata[] orderByFields, bool hasEmptySorts,
        PlanParameters planParams, QueryBuilderParameters builderParameters,
        Dictionary<string, CoraxHighlightingTermIndex> highlightingTerms,
        bool wantTimings,
        out IQueryMatch innerMatch,
        CancellationToken token)
    {
        var fallbackMatch = InstantiateBitmapPipeline(compiledPlan, plan, planParams, builderParameters, highlightingTerms, wantTimings, token);
        innerMatch = fallbackMatch;
        if (orderByFields != null)
        {
            if (fallbackMatch is CompiledQueryMatch seekMatch)
                TrySetSortSeekHint(seekMatch, plan, orderByFields);
            return OrderBy(builderParameters, fallbackMatch, orderByFields, hasEmptySorts);
        }

        return fallbackMatch;
    }

    /// <summary>
    /// Phase 1: build (or fetch from the plan cache) the structural template for a query
    /// text. Captures field names, clause types, parameter bindings, and literal values.
    /// No cardinality estimation, no parameter values, no IL.
    ///
    /// Safe to call without a live transaction once the schema is known —
    /// cmpxchg()/now()/today() bindings store a DeferredExpression that resolves per
    /// execution, so the template itself remains parameter-independent.
    /// </summary>
    public static PlanTemplate BuildTemplate(PlanParameters planParams)
    {
        var queryText = planParams.Metadata.Query.QueryText;
        var planCache = planParams.IndexSearcher.PlanCache;
        return planCache.TryGetTemplate(queryText) ?? ParseTemplate(planParams);
    }

    /// <summary>
    /// Phase 2: bind parameter values to the structural template, estimate cardinality,
    /// sort clauses, emit plan ops, and look up or compile the IL delegate via the plan cache.
    ///
    /// Returns the (cached or newly compiled) <see cref="CompiledPlan"/> together with the
    /// per-execution <see cref="QueryExecution"/> via <paramref name="plan"/>.
    /// Returns <c>null</c> when the plan reduces to an empty match without spatial/vector
    /// post-filters (e.g. an empty IN clause inside an AND chain) — the caller must produce
    /// an explicit empty result rather than caching this shape under the wrong key.
    /// </summary>
    private static CompiledPlan Build(PlanTemplate template, PlanParameters planParams, QueryBuilderParameters builderParameters, out QueryExecution plan)
    {
        var indexSearcher = planParams.IndexSearcher;
        var queryText = planParams.Metadata.Query.QueryText;
        var planCache = indexSearcher.PlanCache;

        // Step 2: Build the per-execution exec list from the template, evaluating
        // WHEN clauses against bound parameters as we go.
        var (execList, whenFlags) = EvaluateWhenAndFilterClauses(template, planParams);

        // Step 3: Populate parameter values into typed arrays
        var writer = new ValueWriter();
        foreach (var it in execList)
        {
            PopulateClauseValues(it, planParams.QueryParameters, writer, builderParameters);
        }

        // Step 3b: Constant propagation — simplify trivially-false/simple clauses.
        bool isOr = template.IsOr;
        PropagateBetweenContradictions(execList, writer);

        // Step 4: Estimate cardinality (needs populated values)
        var cardinalityCtx = builderParameters != null
            ? new ResolutionContext(builderParameters)
            : new ResolutionContext(planParams);
        
        foreach (var it in execList)
        {
            if (it.Cardinality >= 0) continue;
            it.Cardinality = EstimateCardinality(it, indexSearcher, writer, cardinalityCtx);
        }

        // Step 5: Sort operands by cardinality (sort executions by cardinality).
        ClauseExecution[] executions = SortClausesByCardinality(execList, isOr);

        plan = new QueryExecution();
        
        if(executions.Length is 0)
        {
            if (template.Clauses.Count > 0)
            {
                // Consider the following two queries, when both $a and $b are false:
                // FROM Orders WHERE when($a, Name = 'x') AND when($b, Price > 10)
                // FROM Orders WHERE when($a, Name = 'x') OR when($b, Price > 10)
                // Regardless of the query, we have nothing _to_ select here, so we return nothing
                plan = null; // indicates that we should use TermMatch.CreateEmpty();
                return null;
            }

            // FROM Post - i.e, query with no where clauses, still needs a compiled delegate, so we go generate a cached plan for it 
            plan = BuildAllEntriesPlan();
            plan.Executions = executions;
        }
        

        // ── Step 6: Compute cache key components (cheap) ────────────────────
        int operandOrdering = ComputeOperandOrdering(executions);

        var (isSingleNegatedEquals, allNegated) = CheckNegatedClauses(executions);

        if (planParams.HasBoost)
            operandOrdering |= QueryExecution.HasBoostBit;

        // Cardinality cliff bit: queries under vs. over the cliff get different compiled plans, so the bit is part of the cache key.
        if (template.SortDrivingClauseIndex >= 0)
        {
            operandOrdering |= SetCardinalityCliffBit(executions, template.SortDrivingClauseIndex);
        }

        (int typeSignature, byte[] fullKinds) = ComputeTypeSignature(template, planParams);

        if(planCache.Get(queryText, operandOrdering, typeSignature, fullKinds, whenFlags) is {} compiledPlan)
        {
            plan.Plan = compiledPlan;
            plan.Executions = executions;

            // Build InRangeCounts from executions (same fixup as cache-miss path
            // but without the structural array from EmitPlan).
            plan.InRangeCounts = BuildInRangeCounts(executions, isOr, compiledPlan.InRangeSlotCount);

            // The single-clause NotEquals path in EmitPlan sets e0.IsNegated=true
            // as a side effect needed by ResolveMatches/ResolveTermSources.
            if (isSingleNegatedEquals && executions[0].IsNegated == false)
                executions[0].IsNegated = true;

            AttachSpatialAndVectorClauses(plan, compiledPlan.AllNegated, template, planParams, builderParameters, writer);

            plan.LongValues = writer.GetLongs();
            plan.DoubleValues = writer.GetDoubles();
            plan.StringValues = writer.GetStrings();

            // Structural fields (AllNegated, OptimizationFlags, SortDrivingClauseIndex,
            // compound indices) are already on the cached compiledPlan — no
            // RemapOptimizationIndices call needed on cache hit.
            return compiledPlan;
        }

        // ── Step 8: Cache miss — full plan emission ─────────────────────────

        // Build ScanPredicateInfo[] — structural metadata for the entry-scan path.
        // Only needed on cache miss: the array is cached on CompiledPlan for future hits.
        ScanPredicateInfo[] scanPreds = null;
        bool needScanPreds = isOr == false && executions.Length > 1;
        if (needScanPreds)
        {
            int longIndex = 0, doubleIndex = 0, sliceIndex = 0;
            int scanStart = allNegated ? 0 : 1;
            int maxPreds = executions.Length - scanStart;
            scanPreds = new ScanPredicateInfo[maxPreds];
            int scanPredCount = 0;
            for (int si2 = scanStart; si2 < executions.Length; si2++)
            {
                var pred = BuildScanPredicateInfo(executions[si2].Clause, executions[si2], ref longIndex, ref doubleIndex, ref sliceIndex);
                if (pred != null)
                    scanPreds[scanPredCount++] = pred.Value;
            }

            if (scanPredCount > 0)
            {
                if (scanPredCount < maxPreds)
                    Array.Resize(ref scanPreds, scanPredCount);
            }
            else
            {
                scanPreds = null;
            }
        }

        if (executions.Length > 0)
        {
            plan = EmitPlan(isOr, executions);
            plan.Executions = executions;

            // Fixup InRangeCounts from actual runtime InTermCount / HasNullTerm.
            // EmitPlan uses Bindings.Length (structural) for range counts, but runtime
            // InTermCount may differ when a single parameter binding expands to an array.
            if (plan.InRangeCounts != null)
            {
                int rangeIdx = 0;
                for (int ci = 0; ci < executions.Length && rangeIdx < plan.InRangeCounts.Length; ci++)
                {
                    var cl = executions[ci].Clause;
                    if (cl.ClauseType == ClauseType.In)
                    {
                        plan.InRangeCounts[rangeIdx] = executions[ci].InTermCount;
                        rangeIdx++;
                    }
                    else if (cl.ClauseType == ClauseType.AllIn)
                    {
                        int inCount = executions[ci].InTermCount;
                        bool hasNull = executions[ci].HasNullTerm;
                        if (ci == 0 && !isOr)
                            plan.InRangeCounts[rangeIdx] = Math.Max(0, inCount - 1 + (hasNull ? 1 : 0));
                        else if (isOr)
                            plan.InRangeCounts[rangeIdx] = inCount;
                        else
                            plan.InRangeCounts[rangeIdx] = inCount;
                        rangeIdx++;
                    }
                }
            }
        }

        // Empty-IN short-circuit: EmitPlan returns Ops=[] for an AND chain containing
        // an empty IN clause (e.g. `Names in ()`), and the resulting plan has
        // the default OperandOrdering=0 and typeSignature=0. That cache key collides
        // with single-clause "default" plans (e.g. a one-term Equals after constant
        // propagation), so a subsequent execution against the same queryText would
        // receive the cached empty IL and produce zero results for a real query.
        // Return an explicit empty match here without touching the cache. Bail only
        // when there are no spatial/vector post-filters — those phases still need to
        // run (AND with empty is empty for spatial, but vector with a null filter
        // would otherwise return unfiltered top-K).
        if (plan.Ops is { Length: 0 } && plan.IsAllEntries == false
                                      && template.SpatialClauses == null && template.VectorClauses == null)
        {
            // Caller (BuildAndCompile facade) converts null into TermMatch.CreateEmpty.
            return null;
        }

        AttachSpatialAndVectorClauses(plan, allNegated, template, planParams, builderParameters, writer);

        // Store typed arrays once after all clauses (including spatial/vector) are populated.
        plan.LongValues = writer.GetLongs();
        plan.DoubleValues = writer.GetDoubles();
        plan.StringValues = writer.GetStrings();

        // Boost handling: force every op to QueryMatch dispatch so scores are accumulated.
        if (planParams.HasBoost)
        {
            var ops = plan.Ops;
            if (ops != null)
                for (int i = 0; i < ops.Length; i++)
                    ops[i].Dispatch = MatchDispatch.QueryMatch;
        }

        var remapped = RemapOptimizationIndices(template, executions);

        // Compile and cache. Structural fields (AllNegated, OptimizationFlags, remapped
        // indices, ScanPredicateInfos) are stored on the CompiledPlan, not on QueryExecution.
        compiledPlan = new CompiledPlan
        {
            CompiledDelegate = QueryIlEmitter.EmitDelegate(plan, out var csharpText, emitTimings: false),
            CompiledTimedDelegate = QueryIlEmitter.EmitDelegate(plan, out _, emitTimings: true),
            CompiledEntryPredicate = ResidualScanIlEmitter.EmitDelegate(scanPreds, out var scanCsharp),

            Source = csharpText + "\n" + scanCsharp,
            Ordering = operandOrdering,
            TypeSignature = typeSignature,
            FullKinds = fullKinds,
            WhenFlags = whenFlags,
            OpCount = plan.Ops?.Length ?? 0,
            RequiredBitmaps = plan.RequiredBitmaps,
            InRangeSlotCount = plan.InRangeCounts?.Length ?? 0,
            InspectionTemplate = BuildInspectionTemplate(plan),
            ScanPredicateInfos = scanPreds,

            AllNegated = allNegated,
            OptimizationFlags = template.OptimizationFlags,
            SortDrivingClauseIndex = remapped.SortDriving,
            CompoundExactClauseA = remapped.ExactA,
            CompoundExactClauseB = remapped.ExactB,
            CompoundExactAFirst = remapped.ExactAFirst,
            CompoundFieldDrivingClause = remapped.FieldDriving,
            CompoundFieldSortName = remapped.FieldSortName
        };
        plan.Plan = compiledPlan;
        planCache.Add(queryText, compiledPlan, template);

        return compiledPlan;
    }

    private static int SetCardinalityCliffBit(ClauseExecution[] executions, int templateIdx)
    {
        foreach (var t in executions)
        {
            if (t.Clause.OriginalIndex != templateIdx) 
                continue;
                
            long drivingCard = t.Cardinality;
            if (drivingCard is >= 0 and <= QueryPrimitives.TieBreakGroupInitialCapacity)
                return QueryExecution.CardinalityCliffBit;
            break;
        }
        return 0;
    }

    /// <summary>
    /// Bitmap-pipeline match allocator: resolves matches and term sources from the live
    /// transaction, extracts scan parameters, optionally populates highlighting terms,
    /// builds the <see cref="CompiledQueryMatch"/>, then applies the mandatory spatial
    /// post-filter and vector-select wrappers.
    ///
    /// This is the unconditional bitmap path. <see cref="Instantiate"/> calls it as the
    /// last fallback when no strategy (CompoundExact / CompoundField / DirectScan) wins;
    /// <see cref="BuildAndCompile"/> calls it directly (no ORDER BY optimization dispatch).
    /// The wrappers are correctness, not optional decoration — vector selects produce
    /// unfiltered top-K without the filter source they're given here.
    /// </summary>
    private static IQueryMatch InstantiateBitmapPipeline(
        CompiledPlan compiledPlan,
        QueryExecution plan,
        QueryPlanBuilder.PlanParameters planParams,
        QueryBuilderParameters builderParameters,
        Dictionary<string, CoraxHighlightingTermIndex> highlightingTerms,
        bool wantTimings,
        CancellationToken token)
    {
        var indexSearcher = planParams.IndexSearcher;
        var walkerCtx = builderParameters != null
            ? new ResolutionContext(builderParameters)
            : new ResolutionContext(planParams);
        var resolvedMatches = ResolveMatches(plan, walkerCtx);
        var termSources = ResolveTermSources(plan, walkerCtx);
        var termsProviders = ResolveTermsProviders(plan, walkerCtx);
        ExtractScanParameters(plan, indexSearcher,
            out var longParams, out var doubleParams, out var sliceParams, out var fieldRootPages);

        if (highlightingTerms != null)
            PopulateHighlightingTerms(plan, highlightingTerms, planParams.Metadata);

        var compiledMatch = new CompiledQueryMatch(
            compiledPlan, compiledPlan.RequiredBitmaps, compiledPlan.OpCount, resolvedMatches, termSources, termsProviders,
            indexSearcher, planParams.Allocator, wantTimings, token)
        {
            InRangeCounts = plan.InRangeCounts,
            ResidualLongParams = longParams,
            ResidualDoubleParams = doubleParams,
            ResidualSliceParams = sliceParams,
            ResidualFieldRootPages = fieldRootPages
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
        var subExecs = new ClauseExecution[walkerCtx.Clauses.Count];
        for (int ci = 0; ci < walkerCtx.Clauses.Count; ci++)
        {
            subExecs[ci] = CreateExecution(walkerCtx.Clauses[ci]);
            PopulateClauseValues(subExecs[ci], builderParams.QueryParameters, writer, builderParams);
        }

        var subPlan = new QueryExecution
        {
            LongValues = writer.GetLongs(),
            DoubleValues = writer.GetDoubles(),
            StringValues = writer.GetStrings(),
            Executions = subExecs
        };

        if (walkerCtx.Clauses.Count == 1)
            return ResolveClause(walkerCtx.Clauses[0], subExecs[0], subPlan, walkerCtx);

        // Multiple clauses (AND chain) — resolve each and AND them via bitmap.
        // RoaringBitmap is passed as `ref` to AndWithMatch, so using var is not legal here;
        // use try/finally to guarantee disposal.
        var bitmap = new BitmapMatch(indexSearcher.Allocator);
        var temp = new RoaringBitmap(indexSearcher.Allocator);
        try
        {
            bool first = true;
            for (int ci2 = 0; ci2 < walkerCtx.Clauses.Count; ci2++)
            {
                var clause = walkerCtx.Clauses[ci2];
                var match = ResolveClause(clause, subExecs[ci2], subPlan, walkerCtx);
                if (first)
                {
                    QueryPrimitives.OrWithMatch(match, ref bitmap.BitmapState);
                    first = false;
                }
                else
                {
                    QueryPrimitives.AndWithMatch(match, ref bitmap.BitmapState, ref temp);
                }
            }
        }
        finally
        {
            temp.Dispose();
        }

        return bitmap;
    }

    // ── Build helpers ─────────────────────────────────────────────────

    /// <summary>Step 2 of <see cref="Build"/>: walk the template clauses, evaluating each
    /// WHEN condition against bound parameters, and collect the surviving clauses with their
    /// freshly-allocated <see cref="ClauseExecution"/> slots.
    ///
    /// WhenFlags layout: bit <c>i</c> = "the i-th WHEN clause in template traversal order
    /// evaluated true." This is a stable, parameter-independent ordinal (PlanTemplate
    /// construction enforced MaxWhenClauses=32 already). Plans built from the same
    /// queryText but different WHEN-survival subsets must end up with different cache
    /// keys — e.g. [Attach==true, Number!=1] sorted Attach-first gives ordering=1, which
    /// collides with the single-clause [Attach==true] survivor (also ord=1). WhenFlags
    /// joins (Ordering, TypeSignature) in the plan-cache key so each survival pattern
    /// gets its own cached compiled plan.</summary>
    private static (List<ClauseExecution> ExecList, int WhenFlags) EvaluateWhenAndFilterClauses(PlanTemplate template, QueryPlanBuilder.PlanParameters planParams)
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
            if (cached.WhenCondition != null)
            {
                if (cached.WhenCondition(planParams.QueryParameters) == false)
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

    /// <summary>Step 3b of <see cref="Build"/>: constant propagation for trivially-false
    /// clauses. Today only BETWEEN with low &gt; high is detected; the contradictory clause
    /// is rewritten to an empty IN (zero cardinality) so that EmitPlan's empty-IN handling
    /// covers it uniformly — AND chain → empty result, OR chain → clause dropped.
    ///
    /// Strings are intentionally skipped: per-field analyzers rewrite both bounds inside
    /// BetweenQuery, so a raw-bounds ordinal compare here would misfire (see RavenDB-23642).
    ///
    /// Note: single-value IN is NOT rewritten to Equals here. The plan cache is keyed by
    /// (queryText, OperandOrdering), but IN parameter cardinality changes between executions
    /// of the same queryText. Rewriting to Equals on first execution would cache an
    /// Equals-shaped IL that would silently drop terms on subsequent N&gt;1 executions
    /// (RavenDB-17423). The uniform IN shape lets EmitInOps emit a single OrRange whose
    /// term count is read from InRangeCounts at runtime.</summary>
    private static void PropagateBetweenContradictions(
        List<ClauseExecution> execList, ValueWriter writer)
    {
        for (int ci = execList.Count - 1; ci >= 0; ci--)
        {
            var e = execList[ci];
            var c = e.Clause;

            var p = e.PackedParamValue;

            if (c.ClauseType != ClauseType.Between || p.Param2 is PackedParam.NoParamValue)
                continue;


            bool contradictory = p.ValueType switch
            {
                PackedParam.TypeLong => writer.GetLongs()[p.Param1] > writer.GetLongs()[p.Param2],
                PackedParam.TypeDouble => writer.GetDoubles()[p.Param1] > writer.GetDoubles()[p.Param2],
                _ => false // for strings, we have to consider analyzers, so we can't tell
            };
            if (contradictory == false)
                continue;

            // Mark with zero cardinality. EmitPlan handles:
            //   AND chain: zero-cardinality → empty result.
            //   OR  chain: remove the clause (contributes nothing).
            e.Cardinality = 0;
            e.InTermCount = 0;
            e.HasNullTerm = false;
            e.ClauseType = ClauseType.In; // Reuse empty-IN elimination in EmitPlan
        }
    }

    /// <summary>Step 5 of <see cref="Build"/>: reorder clauses + execList so that the
    /// outermost AND chain is sorted by ascending cardinality (smallest driving posting
    /// list first), or so that AndGroup clauses are hoisted to the front of an OR chain
    /// (where they act as cheap mandatory pre-filters).
    ///
    /// Each <see cref="ClauseExecution"/> carries a back-reference to its
    /// <see cref="ClauseExecution.Clause"/>, so sorting the execution list and rebuilding
    /// the clause list from it keeps both in sync without parallel-array bookkeeping.
    ///
    /// AND-chain: sort executions via <see cref="ClauseExecution.CompareTo"/> (negated
    /// last, ascending cardinality).
    /// OR-chain: partition AndGroup clauses to the front (preserving relative order).</summary>
    private static ClauseExecution[] SortClausesByCardinality(
        List<ClauseExecution> execList, bool isOr)
    {
        if (isOr == false)
        {
            CollectionsMarshal.AsSpan(execList).Sort();
        }
        else
        {
            // Move AndGroup clauses to the front (preserving relative order).
            int insertPos = 0;
            for (int j = 0; j < execList.Count; j++)
            {
                if (execList[j].ClauseType == ClauseType.AndGroup)
                {
                    if (j != insertPos)
                    {
                        var ag = execList[j];
                        execList.RemoveAt(j);
                        execList.Insert(insertPos, ag);
                    }

                    insertPos++;
                }
            }
        }

        return execList.ToArray();
    }

    /// <summary>Populate ClauseExecution slots for any template-recorded spatial / vector
    /// post-filter clauses (each a separate phase outside the main bitmap pipeline), then
    /// call <see cref="AttachPostFilterPhases"/> to wire them onto the plan. No-op when
    /// the template has neither.</summary>
    private static void AttachSpatialAndVectorClauses(
        QueryExecution plan, bool allNegated, PlanTemplate template, QueryPlanBuilder.PlanParameters planParams,
        QueryBuilderParameters builderParameters, ValueWriter writer)
    {
        if (template.SpatialClauses == null && template.VectorClauses == null)
            return;

        ClauseInfo[] spatialArr = null;
        ClauseInfo[] vectorArr = null;
        ClauseExecution[] spatialExecs = null;
        ClauseExecution[] vectorExecs = null;

        if (template.SpatialClauses != null)
        {
            int sLen = template.SpatialClauses.Count;
            spatialArr = new ClauseInfo[sLen];
            spatialExecs = new ClauseExecution[sLen];
            for (int si = 0; si < sLen; si++)
            {
                var sc = template.SpatialClauses[si];
                var scExec = new ClauseExecution(sc);
                PopulateClauseValues(scExec, planParams.QueryParameters, writer, builderParameters);
                spatialArr[si] = sc;
                spatialExecs[si] = scExec;
            }
        }

        if (template.VectorClauses != null)
        {
            int vLen = template.VectorClauses.Count;
            vectorArr = new ClauseInfo[vLen];
            vectorExecs = new ClauseExecution[vLen];
            for (int vi = 0; vi < vLen; vi++)
            {
                var vc = template.VectorClauses[vi];
                var vcExec = new ClauseExecution(vc);
                PopulateClauseValues(vcExec, planParams.QueryParameters, writer, builderParameters);
                vectorArr[vi] = vc;
                vectorExecs[vi] = vcExec;
            }
        }

        AttachPostFilterPhases(plan, allNegated, spatialArr, spatialExecs, vectorArr, vectorExecs);
    }

    /// <summary>Remap template-position optimization indices (sort-driving, compound-exact pair,
    /// compound-field driver) to post-sort runtime indices. After WHEN elimination + cardinality
    /// sort, clauses are reordered relative to the template; each clause carries its
    /// template-position in <see cref="ClauseInfo.OriginalIndex"/> so a single pass over the
    /// (already-sorted) clause list is enough to find the four targets. Returns the remapped
    /// values to be stored on <see cref="CompiledPlan"/>.
    ///
    /// Note: <see cref="QueryExecution.CardinalityCliffBit"/> is computed in <see cref="Build"/>
    /// before the cache lookup (it is part of the cache key).</summary>
    private static (int SortDriving, int ExactA, int ExactB, bool ExactAFirst,
                     int FieldDriving, string FieldSortName)
        RemapOptimizationIndices(PlanTemplate template, ClauseExecution[] executions)
    {
        int sortDriving = -1;
        int exactA = -1;
        int exactB = -1;
        int fieldDriving = -1;

        int needSort = template.SortDrivingClauseIndex >= 0 ? 1 : 0;
        int needExactA = template.CompoundExactClauseA >= 0 ? 1 : 0;
        int needExactB = template.CompoundExactClauseB >= 0 ? 1 : 0;
        int needField = template.CompoundFieldDrivingClause >= 0 ? 1 : 0;
        int remaining = needSort + needExactA + needExactB + needField;
        for (int i = 0; i < executions.Length && remaining > 0; i++)
        {
            int origIdx = executions[i].Clause.OriginalIndex;
            if (needSort > 0 && origIdx == template.SortDrivingClauseIndex)
            {
                sortDriving = i;
                remaining--;
            }

            if (needExactA > 0 && origIdx == template.CompoundExactClauseA)
            {
                exactA = i;
                remaining--;
            }
            else if (needExactB > 0 && origIdx == template.CompoundExactClauseB)
            {
                exactB = i;
                remaining--;
            }

            if (needField > 0 && origIdx == template.CompoundFieldDrivingClause)
            {
                fieldDriving = i;
                remaining--;
            }
        }

        // NOTE: CardinalityCliffBit is now computed in Build() before the cache lookup
        // (it's part of the cache key). RemapOptimizationIndices no longer sets it.
        return (SortDriving: sortDriving, ExactA: exactA, ExactB: exactB, ExactAFirst: template.CompoundExactAFirst,
                FieldDriving: fieldDriving, FieldSortName: template.CompoundFieldSortName);
    }

    /// <summary>Create a ClauseExecution for a clause, including sub-executions for OrGroup/AndGroup.
    /// Sets the <see cref="ClauseExecution.Clause"/> back-reference on every instance.</summary>
    private static ClauseExecution CreateExecution(ClauseInfo clause)
    {
        var exec = new ClauseExecution(clause);
        if (clause.SubClauses is { Count: > 0 })
        {
            exec.SubExecutions = new ClauseExecution[clause.SubClauses.Count];
            for (int i = 0; i < clause.SubClauses.Count; i++)
                exec.SubExecutions[i] = CreateExecution(clause.SubClauses[i]);
        }

        return exec;
    }

    /// <summary>Resolve a single clause's parameter value using its cached binding.
    /// Called for each clause during parameter population (both first execution and cache hit).
    /// The optional builderParameters is needed to resolve deferred method expressions (cmpxchg, now, today).</summary>
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
                // IN/AllIn: each binding is a term (literal or parameter, possibly array-expanding)
                ResolveInFromBindings(exec.Clause, exec, queryParameters, writer, bindings);
                break;
            default:
                // Simple clause (Equals, Range, Search, Regex, etc.): single value at Bindings[0]
                var (value, valueType) = ResolveBindingScalar(bindings[BindingIndex.Value], queryParameters, builderParameters);
                // startsWith/endsWith/search/regex require a String argument — reject Null (matches Lucene behavior).
                if (value == null && exec.Clause.ClauseType is ClauseType.StartsWith or ClauseType.EndsWith or ClauseType.Search or ClauseType.Regex)
                {
                    ThrowInvalidMethodArgument(exec.Clause);
                }

                exec.TermValueType = valueType;
                exec.PackedParamValue = writer.Add(value, ToValueTokenType(valueType));
                break;
        }
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

    /// <summary>Populate <paramref name="exec"/>'s typed-parameter slot for an IN/AllIn
    /// clause. Two paths share the same tail (<see cref="EmitInTerms"/>): the
    /// allocation-free literal fast path reads directly from <see cref="ParameterBinding"/>
    /// when <see cref="ClauseInfo.AllBindingsAreLiteral"/> is set (dominantType was pre-computed
    /// by InPreClassify at template time); the parameter-bound slow path resolves each
    /// binding (potentially array-expanding) into <see cref="List{T}"/> buffers, infers
    /// the dominant type at runtime, and then runs the same emit logic.</summary>
    private static void ResolveInFromBindings(ClauseInfo clause, ClauseExecution exec, BlittableJsonReaderObject queryParameters, ValueWriter writer, ParameterBinding[] bindings)
    {
        if (clause.AllBindingsAreLiteral)
        {
            EmitInTerms(exec, writer, clause.InDominantType, bindings, hasNullTerm: false);
            return;
        }

        var resolvedValues = new List<object>(bindings.Length);
        var termTypes = new List<ParamValueType>(bindings.Length);
        bool hasNullTerm = false;

        foreach (var it in bindings)
        {
            switch (it.Source)
            {
                case BindingSource.Literal:
                    resolvedValues.Add(it.LiteralValue);
                    termTypes.Add(it.LiteralType);
                    if (it.LiteralValue == null)
                        hasNullTerm = true;
                    break;

                case BindingSource.QueryParameter:
                {
                    // Parameter — resolve from blittable. May be scalar or array.
                    object inRaw = null;
                    queryParameters?.TryGet(it.ParameterName, out inRaw);
                    if (inRaw is BlittableJsonReaderArray arr)
                    {
                        foreach (var elem in arr)
                        {
                            var (elemVal, elemType) = ResolveParameterValue(elem);
                            resolvedValues.Add(elemVal);
                            termTypes.Add(ToParamValueType(elemType));
                            if (elemVal == null)
                                hasNullTerm = true;
                        }
                    }
                    else if (inRaw != null)
                    {
                        var (singleVal, singleType) = ResolveParameterValue(inRaw);
                        resolvedValues.Add(singleVal);
                        termTypes.Add(ToParamValueType(singleType));
                    }
                    else
                    {
                        resolvedValues.Add(null);
                        termTypes.Add(ParamValueType.Null);
                        hasNullTerm = true;
                    }

                    break;
                }

                case BindingSource.DeferredMethod:
                    // Deferred bindings (cmpxchg, now, today) shouldn't appear in IN lists,
                    // but handle gracefully: resolve as null.
                    resolvedValues.Add(null);
                    termTypes.Add(ParamValueType.Null);
                    hasNullTerm = true;
                    break;
            }
        }

        ParamValueType dominantType = ParamValueType.Null;
        for (int i = 0; i < resolvedValues.Count; i++)
        {
            if (resolvedValues[i] == null) continue;
            dominantType = termTypes[i];
            break;
        }

        if (dominantType == ParamValueType.Null)
            dominantType = ParamValueType.String;

        EmitInTerms(exec, writer, dominantType, resolvedValues, termTypes, hasNullTerm);
    }

    /// <summary>Compute the (packedType, startIdx) pair for a writer slot keyed by
    /// <paramref name="dominantType"/>. Shared header for both IN emit paths.</summary>
    private static (int PackedType, int StartIdx) ResolveInWriterSlot(ValueWriter writer, ParamValueType dominantType)
    {
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
        return (packedType, startIdx);
    }

    /// <summary>All-literal fast-path emit: read values straight from
    /// <paramref name="bindings"/>, write each dominantType-compatible non-null
    /// value into <paramref name="writer"/>, then stamp the exec slot.</summary>
    private static void EmitInTerms(ClauseExecution exec, ValueWriter writer, ParamValueType dominantType,
        ParameterBinding[] bindings, bool hasNullTerm)
    {
        var (packedType, startIdx) = ResolveInWriterSlot(writer, dominantType);
        var dominantTokenType = ToValueTokenType(dominantType);

        int nonNullCount = 0;
        foreach (var it in bindings)
        {
            var value = it.LiteralValue;
            if (value == null)
            {
                hasNullTerm = true;
                continue;
            }

            if (it.LiteralType != dominantType && AreTypesIncompatible(it.LiteralType, dominantType))
                continue;
            writer.Add(value, dominantTokenType);
            nonNullCount++;
        }

        exec.PackedParamValue = new PackedParam(packedType, startIdx);
        exec.InTermCount = nonNullCount;
        exec.HasNullTerm = hasNullTerm;
    }

    /// <summary>Parameter-bound slow-path emit: iterate the pre-resolved
    /// <paramref name="values"/> / <paramref name="types"/> buffers, filter to the
    /// inferred dominant type, write into <paramref name="writer"/>, then stamp the
    /// exec slot.
    ///
    /// Values whose individual type can't be coerced to the dominant type are dropped
    /// (e.g. IN(DateTime, "Shalom") on a DateTime-indexed field — dominant = Long,
    /// "Shalom" never matches a long-indexed term, so dropping it produces the correct
    /// empty/partial result matching Lucene). Without this guard, Convert.ToInt64
    /// would throw FormatException.</summary>
    private static void EmitInTerms(ClauseExecution exec, ValueWriter writer, ParamValueType dominantType,
        List<object> values, List<ParamValueType> types, bool hasNullTerm)
    {
        var (packedType, startIdx) = ResolveInWriterSlot(writer, dominantType);
        var dominantTokenType = ToValueTokenType(dominantType);

        int nonNullCount = 0;
        for (int i = 0; i < values.Count; i++)
        {
            var value = values[i];
            if (value == null) continue;
            if (types[i] != dominantType && AreTypesIncompatible(types[i], dominantType))
                continue;
            writer.Add(value, dominantTokenType);
            nonNullCount++;
        }

        exec.PackedParamValue = new PackedParam(packedType, startIdx);
        exec.InTermCount = nonNullCount;
        exec.HasNullTerm = hasNullTerm;
    }

    /// <summary>Resolve spatial parameters from cached bindings (no MethodExpression dependency).</summary>
    private static void ResolveSpatialFromBindings(ClauseExecution exec, BlittableJsonReaderObject queryParameters)
    {
        var bindings = exec.Clause.Bindings;
        var sp = new SpatialParams();

        // [0] = distanceErrorPct
        if (bindings.Length > 0 && bindings[BindingIndex.SpatialDistErrPct] != null)
        {
            var (depVal, _) = ResolveBindingScalar(bindings[BindingIndex.SpatialDistErrPct], queryParameters, builderParameters: null);
            sp.DistanceErrorPct = depVal != null ? Convert.ToDouble(depVal) : -1;
        }

        // Shape type determined by the number of bindings:
        // circle has 5 (distErrPct, radius, lat, lng, units), WKT has 3 (distErrPct, wkt, units)
        if (bindings.Length >= BindingIndex.SpatialCircleBindingCount - 1) // circle: at least distErrPct + radius + lat + lng
        {
            sp.ShapeType = SpatialShapeType.Circle;
            var (r, _) = ResolveBindingScalar(bindings[BindingIndex.SpatialRadius], queryParameters, builderParameters: null);
            var (lat, _) = ResolveBindingScalar(bindings[BindingIndex.SpatialLatitude], queryParameters, builderParameters: null);
            var (lng, _) = ResolveBindingScalar(bindings[BindingIndex.SpatialLongitude], queryParameters, builderParameters: null);
            sp.CircleRadius = Convert.ToDouble(r);
            sp.CircleLatitude = Convert.ToDouble(lat);
            sp.CircleLongitude = Convert.ToDouble(lng);
            if (bindings.Length > BindingIndex.SpatialUnits && bindings[BindingIndex.SpatialUnits] != null)
            {
                var (u, _) = ResolveBindingScalar(bindings[BindingIndex.SpatialUnits], queryParameters, builderParameters: null);
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
                var (wkt, _) = ResolveBindingScalar(bindings[BindingIndex.SpatialWkt], queryParameters, builderParameters: null);
                sp.Wkt = wkt?.ToString();
                if (bindings.Length > BindingIndex.SpatialWktUnits && bindings[BindingIndex.SpatialWktUnits] != null)
                {
                    var (u, _) = ResolveBindingScalar(bindings[BindingIndex.SpatialWktUnits], queryParameters, builderParameters: null);
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
    private static void ResolveVectorFromBindings(ClauseExecution exec, BlittableJsonReaderObject queryParameters)
    {
        var bindings = exec.Clause.Bindings;

        var vec = exec.Vector = new VectorParams { Method = exec.Clause.VectorMethod };

        // [1]=minimumMatch, [2]=numberOfCandidates, [3]=aiTask
        if (bindings.Length > BindingIndex.VectorMinMatch && bindings[BindingIndex.VectorMinMatch] != null)
        {
            var (simVal, _) = ResolveBindingScalar(bindings[BindingIndex.VectorMinMatch], queryParameters, builderParameters: null);
            vec.MinimumMatch = simVal switch
            {
                double d => (float)d,
                long l => l,
                _ => -1
            };
        }

        if (bindings.Length > BindingIndex.VectorCandidates && bindings[BindingIndex.VectorCandidates] != null)
        {
            var (candVal, candType) = ResolveBindingScalar(bindings[BindingIndex.VectorCandidates], queryParameters, builderParameters: null);
            if (candType != ParamValueType.Null)
                vec.NumberOfCandidates = Convert.ToInt32(candVal);
        }

        if (bindings.Length > BindingIndex.VectorAiTask && bindings[BindingIndex.VectorAiTask] != null)
        {
            var (taskVal, _) = ResolveBindingScalar(bindings[BindingIndex.VectorAiTask], queryParameters, builderParameters: null);
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
                if (queryParameters != null && queryParameters.TryGet(binding.ParameterName, out object raw) && raw != null)
                {
                    var (val, type) = ResolveParameterValue(raw);
                    return (val, ToParamValueType(type));
                }

                return (null, ParamValueType.Null);
        }
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
    private static IQueryMatch[] ResolveMatches(QueryExecution plan, ResolutionContext walkerCtx)
    {
        var indexSearcher = walkerCtx.IndexSearcher;
        var execs = plan.Executions ?? [];
        // IsAllEntries + spatial/vector occurs when the query's only predicates are
        // vector.search() or spatial clauses with no other WHERE terms. GroupCollapse
        // partitions them into SpatialClauses/VectorClauses, leaving the main clause
        // list empty → IsAllEntries=true. The AllEntries bitmap feeds the post-filter
        // phases (spatial AND, then vector select with null filter for unfiltered top-K).
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
                {
                    ClauseExecution exec = plan.SpatialFilters[i].Exec ?? new ClauseExecution(plan.SpatialFilters[i].Clause);
                    allEntriesMatches[matchOfs++] = ResolveClause(plan.SpatialFilters[i].Clause, exec, plan, walkerCtx);
                }
            }

            if (plan.VectorSelects != null)
            {
                for (int i = 0; i < plan.VectorSelects.Length; i++)
                {
                    ClauseExecution exec = plan.VectorSelects[i].Exec ?? new ClauseExecution(plan.VectorSelects[i].Clause);
                    allEntriesMatches[matchOfs++] = ResolveClause(plan.VectorSelects[i].Clause, exec, plan, walkerCtx);
                }
            }

            return allEntriesMatches;
        }

        if (execs.Length == 0)
            return [];

        // Standalone NotEquals pattern: FillAllEntries (no slot) + ANDNOT(term at slot 0).
        // Exclude IN/AllIn — they need N+1 slots for multi-term OR+ANDNOT, not 1.
        if (execs.Length == 1 && execs[0].IsNegated && !plan.Plan.AllNegated
            && execs[0].Clause.ClauseType is not (ClauseType.In or ClauseType.AllIn))
        {
            var clause = execs[0].Clause;
            var exec0 = execs[0];
            return [ResolveClause(clause, exec0, plan, walkerCtx)];
        }

        var matches = new IQueryMatch[CountMatchSlots(execs, plan.IsAllEntries, plan.Plan.AllNegated)];
        int matchIdx = 0;
        for (int ci = 0; ci < execs.Length; ci++)
        {
            ClauseInfo clause = execs[ci].Clause;
            ClauseExecution exec = execs[ci];
            if (TryGetGroupFanOut(clause, exec, out var subClauses, out var subExecs))
            {
                for (int si = 0; si < subClauses.Count; si++)
                {
                    var sub = subClauses[si];
                    var subExec = subExecs[si];
                    var match = ResolveClause(sub, subExec, plan, walkerCtx);
                    if (subExec.BoostFactor > 0)
                        match = indexSearcher.Boost(match, subExec.BoostFactor);
                    matches[matchIdx++] = match;
                }

                continue;
            }

            switch (clause.ClauseType)
            {
                case ClauseType.AllIn or ClauseType.In:
                {
                    for (int t = 0; t < exec.InTermCount; t++)
                        matches[matchIdx++] = ResolveInTerm(clause, exec, t, plan, walkerCtx);
                    // Always allocate the null-term slot (plan structure is parameter-independent).
                    // When HasNullTerm is false, fill with a TermQuery(null) that resolves to an
                    // empty posting list — the OR with an empty match is a no-op.
                    {
                        FieldMetadata nullMeta = ResolveFieldMetadata(clause, walkerCtx);
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
                        true => CreateNotEqualsOrMatch(clause, exec, plan, walkerCtx),
                        false => ResolveClause(clause, exec, plan, walkerCtx)
                    };
                    if (exec.BoostFactor > 0)
                        match = indexSearcher.Boost(match, exec.BoostFactor);
                    matches[matchIdx++] = match;
                    break;
                }
            }
        }

        if (plan.Plan.AllNegated)
            matches[matchIdx] = indexSearcher.AllEntries();
        return matches;
    }

    private static IQueryMatch ResolveRangeClauseWithDirection(ClauseInfo clause, ClauseExecution exec,
        QueryExecution plan, bool forward, ResolutionContext walkerCtx)
    {
        var indexSearcher = walkerCtx.IndexSearcher;
        FieldMetadata fieldMeta = ResolveFieldMetadata(clause, walkerCtx);
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
            ClauseType.Between when exec.SentinelRewriteType != null =>
                ResolveSentinelRewrittenBetween(exec, fieldMeta, indexSearcher, plan),
            ClauseType.Between => ResolveBetweenWithDirection(exec, fieldMeta, indexSearcher, plan, forward),
            _ => ResolveClause(clause, exec, plan, walkerCtx) // fallback
        };
    }

    /// <summary>Resolve a BETWEEN clause for sort-driving (TermsProviderMatch) paths.
    /// Sentinel bounds are rewritten at template time, so remaining BETWEEN clauses
    /// always have genuine bounds.</summary>
    private static IQueryMatch ResolveBetweenWithDirection(ClauseExecution exec, FieldMetadata fieldMeta,
        IndexSearcher indexSearcher, QueryExecution plan, bool forward)
    {
        var packed = exec.PackedParamValue;
        return packed.ValueType switch
        {
            PackedParam.TypeLong => indexSearcher.BetweenQuery(fieldMeta, plan.LongValues[packed.Param1], plan.LongValues[packed.Param2], forward: forward),
            PackedParam.TypeDouble => indexSearcher.BetweenQuery(fieldMeta, plan.DoubleValues[packed.Param1], plan.DoubleValues[packed.Param2], forward: forward),
            _ => indexSearcher.BetweenQuery(fieldMeta, plan.StringValues[packed.Param1], plan.StringValues[packed.Param2], forward: forward)
        };
    }

    private static IQueryMatch ResolveSentinelRewrittenBetween(ClauseExecution exec, FieldMetadata fieldMeta,
        IndexSearcher indexSearcher, QueryExecution plan)
    {
        if (exec.SentinelRewriteType == ClauseType.Exists)
            return indexSearcher.AllEntries();
        var packed = exec.PackedParamValue;
        int idx = packed.Param1;
        if (exec.SentinelRewriteType == ClauseType.LessThanOrEqual)
        {
            return packed.ValueType switch
            {
                PackedParam.TypeLong => indexSearcher.LessThanOrEqualsQuery(fieldMeta, plan.LongValues[idx]),
                PackedParam.TypeDouble => indexSearcher.LessThanOrEqualsQuery(fieldMeta, plan.DoubleValues[idx]),
                _ => indexSearcher.LessThanOrEqualsQuery(fieldMeta, plan.StringValues[idx])
            };
        }

        Debug.Assert(exec.SentinelRewriteType == ClauseType.GreaterThanOrEqual);
        IQueryMatch rangeMatch = packed.ValueType switch
        {
            PackedParam.TypeLong => indexSearcher.GreaterThanOrEqualsQuery(fieldMeta, plan.LongValues[idx]),
            PackedParam.TypeDouble => indexSearcher.GreaterThanOrEqualsQuery(fieldMeta, plan.DoubleValues[idx]),
            _ => indexSearcher.GreaterThanOrEqualsQuery(fieldMeta, plan.StringValues[idx])
        };
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
    private static IQueryMatch ResolveEqualsClauseWithDirection(ClauseInfo clause, ClauseExecution exec,
        QueryExecution plan, bool forward, ResolutionContext walkerCtx)
    {
        var indexSearcher = walkerCtx.IndexSearcher;
        FieldMetadata fieldMeta = ResolveFieldMetadata(clause, walkerCtx);
        var packed = exec.PackedParamValue;
        return packed.ValueType switch
        {
            PackedParam.TypeLong => indexSearcher.BetweenQuery(fieldMeta, plan.LongValues[packed.Param1], plan.LongValues[packed.Param1], forward: forward),
            PackedParam.TypeDouble => indexSearcher.BetweenQuery(fieldMeta, plan.DoubleValues[packed.Param1], plan.DoubleValues[packed.Param1], forward: forward),
            _ => indexSearcher.BetweenQuery(fieldMeta, plan.StringValues[packed.Param1], plan.StringValues[packed.Param1], forward: forward)
        };
    }

    private static IQueryMatch ResolveClause(ClauseInfo clause, ClauseExecution exec,
        QueryExecution plan, ResolutionContext walkerCtx)
    {
        var indexSearcher = walkerCtx.IndexSearcher;
        var builderParams = walkerCtx.BuilderParams;
        if (clause.ClauseType == ClauseType.OrGroup && clause.SubClauses != null)
        {
            var bm = new BitmapMatch(indexSearcher.Allocator);
            var temp = new RoaringBitmap(indexSearcher.Allocator);
            for (int si = 0; si < clause.SubClauses.Count; si++)
            {
                var subExec = exec.SubExecutions[si];
                var subMatch = ResolveClause(clause.SubClauses[si], subExec, plan, walkerCtx);
                QueryPrimitives.OrWithMatch(subMatch, ref bm.BitmapState);
            }

            temp.Dispose();
            return bm;
        }

        if (clause.ClauseType == ClauseType.AndGroup && clause.SubClauses != null)
        {
            var bm = new BitmapMatch(indexSearcher.Allocator);
            var temp = new RoaringBitmap(indexSearcher.Allocator);
            bool first = true;
            for (int si = 0; si < clause.SubClauses.Count; si++)
            {
                var sub = clause.SubClauses[si];
                var subExec = exec.SubExecutions[si];
                var subMatch = ResolveClause(sub, subExec, plan, walkerCtx);
                if (first)
                {
                    QueryPrimitives.OrWithMatch(subMatch, ref bm.BitmapState);
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
            fieldMeta = ResolveFieldMetadata(clause, walkerCtx);
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
                if (exec.SentinelRewriteType != null)
                    return ResolveSentinelRewrittenBetween(exec, fieldMeta, indexSearcher, plan);
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
                    var termMatch = ResolveInTerm(clause, exec, t, plan, walkerCtx);
                    if (clause.ClauseType == ClauseType.AllIn && t > 0)
                        QueryPrimitives.AndWithMatch(termMatch, ref bm.BitmapState, ref temp);
                    else
                        QueryPrimitives.OrWithMatch(termMatch, ref bm.BitmapState);
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
                // Dynamic field name variants (search(FieldName) for auto-indexes) are
                // pre-resolved by the DynamicFieldNameResolve walker step at template time.
                string searchFieldName = clause.ResolvedFieldName ?? clause.FieldName;
                {
                    bool forceSearch = builderParams.HasDynamics
                                       && (builderParams.Index?.Configuration?.UseSearchAnalyzerForDynamicFieldsIfNotSetExplicitlyInSearchQuery ?? false);
                    searchMeta = QueryBuilderHelper.GetFieldMetadata(
                        builderParams.Allocator, searchFieldName, builderParams.Index,
                        builderParams.IndexFieldsMapping, builderParams.FieldsToFetch,
                        builderParams.HasDynamics, builderParams.DynamicFields,
                        handleSearch: true, hasBoost: builderParams.HasBoost,
                        forceDefaultSearchAnalyzer: forceSearch);
                }

                var indexDef = builderParams.Index?.Definition;
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
                    new System.Text.RegularExpressions.Regex(plan.StringValues[packed.Param1]));

            case ClauseType.Spatial:
            {
                return HandleSpatial(builderParams, clause, exec, clause.SpatialMethodType);
            }

            case ClauseType.Vector:
            {
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

    /// <summary>Compute the field metadata and packed parameter for an IN term at the given index.
    /// Shared by <see cref="ResolveInTerm"/> (bitmap path) and <see cref="ResolveInTermSource"/>
    /// (posting-list path) to ensure field resolution and index arithmetic stay in sync.</summary>
    private static (FieldMetadata FieldMeta, PackedParam TermPacked) ResolveInTermParam(
        ClauseInfo clause, ClauseExecution exec, int termIndex, ResolutionContext walkerCtx)
    {
        // ResolveFieldMetadata picks up the exact/search field name variant for dynamic indexes (#4777 fix).
        FieldMetadata fieldMeta = ResolveFieldMetadata(clause, walkerCtx);
        var p = exec.PackedParamValue;
        return (fieldMeta, new PackedParam(p.ValueType, p.Param1 + termIndex));
    }

    /// <summary>Resolve a single IN term to a typed TermQuery (bitmap path).
    /// IN terms are stored contiguously: PackedParamValue.Param1 = start index, InTermCount = count.
    /// Only non-null terms are in the typed array. Null is handled separately via HasNullTerm.</summary>
    private static IQueryMatch ResolveInTerm(ClauseInfo clause, ClauseExecution exec, int termIndex,
        QueryExecution plan, ResolutionContext walkerCtx)
    {
        var (fieldMeta, termPacked) = ResolveInTermParam(clause, exec, termIndex, walkerCtx);
        return TermQueryFromParam(termPacked, fieldMeta, walkerCtx.IndexSearcher, plan);
    }

    /// <summary>Create a pre-materialized <see cref="BitmapMatch"/> for a negated clause
    /// appearing in an OR chain. OR(NOT X, NOT Y, ...) cannot use the raw term posting list
    /// (FillBitmapFromPostingSource would add entries WITH X, not WITHOUT X). Instead, we
    /// pre-compute AllEntries ANDNOT (positive form) into a BitmapMatch so that OrWithMatch
    /// during execution correctly ORs in the set of entries NOT matching the positive predicate.
    /// Handles NOT EQUALS (single term), NOT EXISTS (ExistsQuery), and single-term NOT IN/AllIn.</summary>
    private static IQueryMatch CreateNotEqualsOrMatch(ClauseInfo clause, ClauseExecution exec,
        QueryExecution plan, ResolutionContext walkerCtx)
    {
        // Resolve the positive form of the match. For IN/AllIn clauses, ResolveClause
        // handles multi-term expansion correctly. For simple Equals/NotEquals, the
        // single-term TermQueryFromParam suffices. EXISTS clauses carry no PackedParam.
        var indexSearcher = walkerCtx.IndexSearcher;
        IQueryMatch termMatch;
        if (clause.ClauseType is ClauseType.In or ClauseType.AllIn)
        {
            termMatch = ResolveClause(clause, exec, plan, walkerCtx);
        }
        else if (clause.ClauseType == ClauseType.Exists)
        {
            FieldMetadata fieldMeta = ResolveFieldMetadata(clause, walkerCtx);
            termMatch = indexSearcher.ExistsQuery(fieldMeta);
        }
        else
        {
            FieldMetadata fieldMeta = ResolveFieldMetadata(clause, walkerCtx);
            termMatch = TermQueryFromParam(exec.PackedParamValue, fieldMeta, indexSearcher, plan);
        }

        var bitmapMatch = new BitmapMatch(indexSearcher.Allocator);
        var tempData = new RoaringBitmap(indexSearcher.Allocator);
        QueryPrimitives.OrWithMatch(indexSearcher.AllEntries(), ref bitmapMatch.BitmapState);
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
    private static PostingSource[] ResolveTermSources(QueryExecution plan, ResolutionContext walkerCtx)
    {
        var indexSearcher = walkerCtx.IndexSearcher;
        // IsAllEntries plans never emit term ops (FillFromPostings / AndWith / etc.) —
        // their match[0] is AllEntries, post-filter slots are spatial/vector. No
        // PostingSource population is needed.
        if (plan.IsAllEntries)
            return [];

        var execs = plan.Executions;
        if (execs is not { Length: > 0 })
            return [];

        // Standalone NotEquals: FillAllEntries (no slot) + ANDNOT at slot 0.
        // Exclude IN/AllIn — they need N+1 slots for multi-term OR+ANDNOT, not 1.
        if (execs.Length == 1 && execs[0].IsNegated && !plan.Plan.AllNegated
            && execs[0].Clause.ClauseType is not (ClauseType.In or ClauseType.AllIn))
        {
            return [ResolveSingleTermSource(execs[0].Clause, execs[0], plan, walkerCtx)];
        }

        var termSources = new PostingSource[CountMatchSlots(execs, plan.IsAllEntries, plan.Plan.AllNegated)];
        int matchIdx = 0;
        for (int ci = 0; ci < execs.Length; ci++)
        {
            ClauseInfo clause = execs[ci].Clause;
            ClauseExecution exec = execs[ci];
            if (TryGetGroupFanOut(clause, exec, out var subClauses, out var subExecs))
            {
                for (int si = 0; si < subClauses.Count; si++)
                {
                    var sub = subClauses[si];
                    var subExec = subExecs[si];
                    if (subExec.BoostFactor > 0)
                    {
                        matchIdx++;
                        continue;
                    }

                    termSources[matchIdx++] = ResolveSingleTermSource(sub, subExec, plan, walkerCtx);
                }

                continue;
            }

            switch (clause.ClauseType)
            {
                case ClauseType.AllIn or ClauseType.In:
                {
                    for (int t = 0; t < exec.InTermCount; t++)
                        termSources[matchIdx++] = ResolveInTermSource(clause, exec, t, plan, walkerCtx);
                    // Null-term slot: resolve via the null posting list so that the PostingList
                    // dispatch path (used by EmitInOps/EmitAllInOps) can read it.  When HasNullTerm
                    // is false the slot stays Empty and the compiled OR/AND step is a no-op.
                    if (exec.HasNullTerm)
                    {
                        FieldMetadata nullMeta = ResolveFieldMetadata(clause, walkerCtx);
                        if (indexSearcher.TryGetPostingListForNull(in nullMeta, out long nullPlId))
                            termSources[matchIdx] = DecodePostingListId(nullPlId, indexSearcher);
                    }

                    matchIdx++;
                    break;
                }
                default:
                {
                    if (exec.BoostFactor > 0)
                    {
                        matchIdx++;
                        continue;
                    }

                    termSources[matchIdx++] = ResolveSingleTermSource(clause, exec, plan, walkerCtx);
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
    private static ITermsProvider[] ResolveTermsProviders(QueryExecution plan, ResolutionContext walkerCtx)
    {
        var execs = plan.Executions;
        if (plan.IsAllEntries || execs is not { Length: > 0 })
            return null;

        bool hasAnyTreeScan = false;

        // Quick check: do we have any TreeScan clauses at all?
        foreach (var ex in execs)
        {
            var cl = ex.Clause;
            if (IsTreeScanEligibleClause(cl))
            {
                hasAnyTreeScan = true;
                break;
            }

            // Check subclauses
            if (cl.SubClauses != null)
            {
                foreach (var t in cl.SubClauses)
                {
                    if (IsTreeScanEligibleClause(t))
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

        int totalSlots = CountMatchSlots(execs, plan.IsAllEntries, plan.Plan.AllNegated);
        var providers = new ITermsProvider[totalSlots];
        int matchIdx = 0;

        for (int ci = 0; ci < execs.Length; ci++)
        {
            ClauseInfo clause = execs[ci].Clause;
            ClauseExecution exec = execs[ci];

            if (TryGetGroupFanOut(clause, exec, out var subClauses, out var subExecs))
            {
                for (int si = 0; si < subClauses.Count; si++)
                {
                    var sub = subClauses[si];
                    var subExec = subExecs?[si];
                    providers[matchIdx] = ResolveSingleTermsProvider(sub, subExec, plan, walkerCtx);
                    matchIdx++;
                }

                continue;
            }

            switch (clause.ClauseType)
            {
                case ClauseType.AllIn or ClauseType.In:
                    // IN terms use PostingList dispatch, not TreeScan. +1 for null-term slot (always allocated).
                    matchIdx += (exec?.InTermCount ?? 0) + 1;
                    break;

                default:
                    providers[matchIdx] = ResolveSingleTermsProvider(clause, exec, plan, walkerCtx);
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
        QueryExecution plan, ResolutionContext walkerCtx)
    {
        if (IsTreeScanEligibleClause(clause) == false)
            return null;

        // Create the match via the existing factory methods, then extract the provider.
        // The factory methods handle all complexity (analyzer, CompactKey, tree lookup).
        var match = ResolveClause(clause, exec ?? new ClauseExecution(clause), plan, walkerCtx);
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
    private static PostingSource ResolveSingleTermSource(ClauseInfo clause, ClauseExecution exec,
        QueryExecution plan, ResolutionContext walkerCtx)
    {
        if (IsTermSourceEligibleClause(clause) == false)
            return default; // Kind == Empty

        FieldMetadata fieldMeta = ResolveFieldMetadata(clause, walkerCtx);
        long postingListId = GetTermPostingListIdFromParam(exec.PackedParamValue, fieldMeta, walkerCtx.IndexSearcher, plan);
        return DecodePostingListId(postingListId, walkerCtx.IndexSearcher);
    }

    /// <summary>Resolve a single In/AllIn term to a posting-list source (posting-list path).
    /// Uses <see cref="ResolveInTermParam"/> for field resolution and index arithmetic.</summary>
    private static PostingSource ResolveInTermSource(ClauseInfo clause, ClauseExecution exec, int termIndex,
        QueryExecution plan, ResolutionContext walkerCtx)
    {
        var (fieldMeta, termPacked) = ResolveInTermParam(clause, exec, termIndex, walkerCtx);
        return DecodePostingListId(GetTermPostingListIdFromParam(termPacked, fieldMeta, walkerCtx.IndexSearcher, plan), walkerCtx.IndexSearcher);
    }

    /// <summary>Resolve field metadata for a term-source clause. Mirrors the
    /// non-Spatial/Vector/Search branch of <see cref="ResolveClause"/>.</summary>
    private static FieldMetadata ResolveFieldMetadata(ClauseInfo clause, ResolutionContext walkerCtx)
    {
        var builderParams = walkerCtx.BuilderParams;
        // Dynamic field name variants are pre-resolved by DynamicFieldNameResolve at template time.
        string resolvedFieldName = clause.ResolvedFieldName ?? clause.FieldName;

        if (builderParams == null)
        {
            // Direct-test path (no QueryBuilderParameters): use IndexSearcher's FieldMetadataBuilder directly.
            return walkerCtx.IndexSearcher.FieldMetadataBuilder(resolvedFieldName, hasBoost: walkerCtx.HasBoost);
        }

        // When forceDefaultSearchAnalyzer is enabled for indexes with dynamic fields (CreateField),
        // non-exact non-search clauses should use the search analyzer (#4778 fix).
        bool forceSearchAnalyzer = builderParams.HasDynamics
                                   && clause.IsExact == false
                                   && clause.ClauseType != ClauseType.Search
                                   && (builderParams.Index?.Configuration?.UseSearchAnalyzerForDynamicFieldsIfNotSetExplicitlyInSearchQuery ?? false);
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
        var predicates = plan.Plan.ScanPredicateInfos;
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
        int scanStart = plan.Plan.AllNegated ? 0 : 1;
        int clauseIdx = scanStart;
        var execs = plan.Executions;
        int dummyL = 0, dummyD = 0, dummyS = 0;
        foreach (ScanPredicateInfo pred in predicates)
        {
            // Advance past clauses that BuildScanPredicateInfo would have skipped (returned null).
            while (clauseIdx < execs.Length &&
                   BuildScanPredicateInfo(execs[clauseIdx].Clause, clauseIdx < execs.Length ? execs[clauseIdx] : null,
                       ref dummyL, ref dummyD, ref dummyS) == null)
            {
                clauseIdx++;
            }

            ClauseInfo matchingClause = clauseIdx < execs.Length ? execs[clauseIdx].Clause : null;
            ClauseExecution matchingExec = clauseIdx < execs.Length ? execs[clauseIdx] : null;
            clauseIdx++;
            ExtractParamsFromPredicate(pred, matchingClause, matchingExec, indexSearcher, plan, longs, doubles, slices, roots);
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
        QueryExecution plan, IndexSearcher indexSearcher, Sparrow.Server.ByteStringContext allocator,
        ScanPredicateInfo[] residualArray, int skipClauseIdx1, int skipClauseIdx2,
        out long[] longParams, out double[] doubleParams, out Voron.Slice[] sliceParams, out long[] fieldRootPages)
    {
        longParams = null;
        doubleParams = null;
        sliceParams = null;
        fieldRootPages = null;

        var execs = plan.Executions;
        if (residualArray == null || execs == null)
            return;

        var longs = new List<long>();
        var doubles = new List<double>();
        var slices = new List<Voron.Slice>();
        var roots = new List<long>();

        int residualIdx = 0;
        for (int i = 0; i < execs.Length; i++)
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
                    longs.Add(plan.LongValues[idx1]);
                    if (hasBetween) longs.Add(plan.LongValues[idx2]);
                    break;
                case ScanValueType.Double:
                    doubles.Add(plan.DoubleValues[idx1]);
                    if (hasBetween) doubles.Add(plan.DoubleValues[idx2]);
                    break;
                case ScanValueType.Slice:
                case ScanValueType.SliceLong:
                    Voron.Slice.From(allocator, plan.StringValues[idx1], out var s1);
                    slices.Add(s1);
                    if (hasBetween)
                    {
                        Voron.Slice.From(allocator, plan.StringValues[idx2], out var s2);
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

    private static void ExtractParamsFromPredicate(ScanPredicateInfo pred, ClauseInfo clause, ClauseExecution exec,
        IndexSearcher indexSearcher, QueryExecution plan, List<long> longs, List<double> doubles,
        List<Voron.Slice> slices, List<long> roots)
    {
        if (pred.SubPredicates != null)
        {
            // Each OrBranch corresponds to a subclause of the OrGroup.
            // Pass subclauses positionally to avoid the same field-name ambiguity.
            List<ClauseInfo> subClauses = clause?.SubClauses;
            ClauseExecution[] subExecs = exec?.SubExecutions;
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
            case ScanValueType.SliceLong:
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
        var execs = plan.Executions;
        if (highlightingTerms == null || execs is not { Length: > 0 })
            return;

        for (int ci = 0; ci < execs.Length; ci++)
        {
            var clauseObj = execs[ci].Clause;
            var exec = execs[ci];

            // Recurse into sub-clauses before checking FieldName: OrGroup/AndGroup have
            // FieldName==null (they are structural wrappers, not field clauses), so a
            // FieldName-first guard would skip their children entirely.
            switch (clauseObj?.ClauseType)
            {
                case ClauseType.OrGroup or ClauseType.AndGroup when clauseObj.SubClauses is { Count: > 0 }:
                {
                    for (int si = 0; si < clauseObj.SubClauses.Count; si++)
                    {
                        var subExec = exec?.SubExecutions != null && si < exec.SubExecutions.Length ? exec.SubExecutions[si] : null;
                        PopulateHighlightingForClause(clauseObj.SubClauses[si], subExec, highlightingTerms, metadata, plan);
                    }

                    break;
                }
            }

            if (clauseObj?.FieldName == null)
                continue;

            PopulateHighlightingForClause(clauseObj, exec, highlightingTerms, metadata, plan);
        }
    }

    private static void PopulateHighlightingForClause(ClauseInfo clause, ClauseExecution exec, Dictionary<string, CoraxHighlightingTermIndex> highlightingTerms, QueryMetadata metadata, QueryExecution plan)
    {
        string fieldName = clause.FieldName;
        if (fieldName == null)
            return;

        // Skip highlighting for null-valued clauses (e.g. WHERE City == null).
        // Null is not a search term — there's nothing to highlight. Without this
        // guard, the highlighter produces a spurious result for null fields (#4781).
        if (clause.ClauseType is ClauseType.Equals or ClauseType.NotEquals)
        {
            var packed = exec?.PackedParamValue ?? PackedParam.None;
            if (packed.IsNone || (packed is { ValueType: PackedParam.TypeString, Param1: >= 0 } && plan.StringValues != null
                                                                                                && packed.Param1 < plan.StringValues.Length
                                                                                                && plan.StringValues[packed.Param1] == null))
            {
                return;
            }
        }

        if (highlightingTerms.TryGetValue(fieldName, out var existingTerm))
        {
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
    private static CoraxVectorItem[] ResolveVectorItems(QueryExecution plan, QueryBuilderParameters builderParams)
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
                    (method: VectorHelpers.MethodVectorValue.ForRaw, string vectorAsBase64) => CoraxVectorItem.BuildSingleVector(builderParameters, fieldMetadata,
                        GenerateEmbeddings.FromBase64Array(VectorOptions.Default, builderParameters.Allocator, vectorAsBase64), numberOfCandidates, minimumMatch, exact),
                    (method: VectorHelpers.MethodVectorValue.ForRaw, StringSegment stringSegmentAsBase64) => CoraxVectorItem.BuildSingleVector(builderParameters, fieldMetadata,
                        GenerateEmbeddings.FromBase64Array(VectorOptions.Default, builderParameters.Allocator, stringSegmentAsBase64.ToString()), numberOfCandidates, minimumMatch, exact),
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

            if (((builderParameters.FieldsToFetch != null && builderParameters.FieldsToFetch.IndexFields.TryGetValue(fieldName, out var indexField)) || (builderParameters.Index.Definition.IndexFields.TryGetValue(fieldName, out indexField))) &&
                indexField.Vector is AutoVectorOptions avo)
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

            // Hoist the per-element type switch out of the loop: SourceEmbeddingType is
            // constant for the whole array, so dispatching once and then running a tight
            // typed copy loop avoids repeating the branch (and the three ref captures) per
            // element.
            switch (vectorOptions.SourceEmbeddingType)
            {
                case VectorEmbeddingType.Single:
                    CopyFloats(array, MemoryMarshal.Cast<byte, float>(mem.Span));
                    break;
                case VectorEmbeddingType.Int8:
                    CopyInt8(array, MemoryMarshal.Cast<byte, sbyte>(mem.Span));
                    break;
                default:
                    CopyBytes(array, mem.Span);
                    break;
            }

            return GenerateEmbeddings.FromArray(parameters.Allocator, memScope, mem, vectorOptions, bytesUsed);

            static void CopyFloats(BlittableJsonReaderArray src, Span<float> dst)
            {
                ref var dstRef = ref MemoryMarshal.GetReference(dst);
                for (int i = 0; i < src.Length; ++i)
                    Unsafe.Add(ref dstRef, i) = src.GetByIndex<float>(i);
            }

            static void CopyInt8(BlittableJsonReaderArray src, Span<sbyte> dst)
            {
                ref var dstRef = ref MemoryMarshal.GetReference(dst);
                for (int i = 0; i < src.Length; ++i)
                    Unsafe.Add(ref dstRef, i) = src.GetByIndex<sbyte>(i);
            }

            static void CopyBytes(BlittableJsonReaderArray src, Span<byte> dst)
            {
                ref var dstRef = ref MemoryMarshal.GetReference(dst);
                for (int i = 0; i < src.Length; ++i)
                    Unsafe.AddByteOffset(ref dstRef, i) = src.GetByIndex<byte>(i);
            }
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
                destinationEmbeddingType = sourceEmbeddingType is not VectorEmbeddingType.Single ? sourceEmbeddingType : vectorOptions!.DestinationEmbeddingType;
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

    public static bool TryCreateCompoundExactMatch(
        QueryExecution plan, QueryPlanBuilder.PlanParameters planParams, QueryBuilderParameters builderParams,
        out IQueryMatch compoundMatch, out string rejectReason)
    {
        rejectReason = null;
        bool result = TryCreateCompoundExactMatch(plan, planParams, builderParams, out compoundMatch);
        if (!result)
        {
            if (plan.Executions == null || plan.Executions.Length < 2)
                rejectReason = $"fewer than 2 clauses ({plan.Executions?.Length ?? 0})";
            else if (plan.Plan.AllNegated)
                rejectReason = "all clauses are negated";
            else if (planParams.Index == null)
                rejectReason = "no index available";
            else if (plan.Plan.CompoundExactClauseA < 0 || plan.Plan.CompoundExactClauseB < 0)
                rejectReason = "no compound-exact clause pair identified at template time";
            else
                rejectReason = "composite key encoding failed or exceeded max term length";
        }

        return result;
    }

    /// <summary>Check if two Equals clauses on (field1, field2) match a compound field.
    /// If so, build a single TermQuery on the compound tree with the composite key.
    /// One tree lookup instead of two posting list intersections.</summary>
    public static bool TryCreateCompoundExactMatch(
        QueryExecution plan, QueryPlanBuilder.PlanParameters planParams, QueryBuilderParameters builderParams,
        out IQueryMatch compoundMatch)
    {
        compoundMatch = null;
        // Discovery: structural rejection. All structural facts (CompoundExactClauseA/B,
        // CompoundExactAFirst) were pre-classified at template time; this method only
        // confirms the runtime state is compatible.
        if (plan.Executions == null || plan.Executions.Length < 2 || plan.Plan.AllNegated)
            return false;
        if (planParams.Index == null)
            return false;

        int idxA = plan.Plan.CompoundExactClauseA;
        int idxB = plan.Plan.CompoundExactClauseB;
        if (idxA < 0 || idxB < 0 || idxA >= plan.Executions.Length || idxB >= plan.Executions.Length)
            return false;

        var eA = plan.Executions[idxA];
        var eB = plan.Executions[idxB];
        if (eA.BoostFactor > 0 || eA.PackedParamValue.IsNone)
            return false;
        if (eB.BoostFactor > 0 || eB.PackedParamValue.IsNone)
            return false;

        compoundMatch = ConstructCompoundExact(plan, planParams);
        return compoundMatch != null;
    }

    /// <summary>Phase 5 bake: construction-only path for the CompoundExact hint.
    /// Assumes structural discovery has already validated this optimization applies
    /// (called either right after <see cref="TryCreateCompoundExactMatch"/>'s checks pass
    /// on compile-miss, or directly on cache-hit when <c>compiledPlan.Strategy == ExecutionStrategy.CompoundExact</c>).
    /// Returns null when a per-execution byte-length check fails — the caller must fall
    /// back to the next optimization (or bitmap). No cost gates here — those are encoded
    /// in the plan-cache key (cardinality cliff bit 31 of Ordering).</summary>
    private static IQueryMatch ConstructCompoundExact(QueryExecution plan, QueryPlanBuilder.PlanParameters planParams)
    {
        var execs = plan.Executions;
        var indexSearcher = planParams.IndexSearcher;
        int idxA = plan.Plan.CompoundExactClauseA;
        int idxB = plan.Plan.CompoundExactClauseB;
        var eA = execs[idxA];
        var eB = execs[idxB];

        string firstField, secondField;
        ClauseExecution firstExec, secondExec;
        if (plan.Plan.CompoundExactAFirst)
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

        byte[] field1Bytes = BuildCompoundFieldBytes(firstField, firstExec, indexSearcher, plan);
        if (field1Bytes == null || field1Bytes.Length > byte.MaxValue) return null;

        byte[] field2Bytes = BuildCompoundFieldBytes(secondField, secondExec, indexSearcher, plan);
        if (field2Bytes == null) return null;

        int totalLen = field1Bytes.Length + field2Bytes.Length + 1;
        if (totalLen > Constants.Terms.MaxLength) return null;

        var compositeKey = new byte[totalLen];
        field1Bytes.CopyTo(compositeKey, 0);
        field2Bytes.CopyTo(compositeKey.AsSpan(field1Bytes.Length));
        compositeKey[^1] = (byte)field1Bytes.Length;

        var compoundFieldName = $"compound({firstField},{secondField})";
        var compoundFieldMeta = indexSearcher.FieldMetadataBuilder(compoundFieldName, hasBoost: false);
        Voron.Slice.From(planParams.Allocator, compositeKey, out var keySlice);

        return indexSearcher.TermQuery(compoundFieldMeta, keySlice);
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

    /// <summary>Check if WHERE + ORDER BY can be served by a compound tree scan.
    /// Condition: an Equals clause on field1, ORDER BY on field2 (or field1, or both),
    /// </summary>
    public static bool TryCreateCompoundFieldMatch(
        QueryExecution plan, OrderMetadata[] orderByFields,
        QueryPlanBuilder.PlanParameters planParams, QueryBuilderParameters builderParams,
        CompiledPlan compiledPlan, out IQueryMatch compoundMatch, out string rejectReason)
    {
        if (TryCreateCompoundFieldMatch(plan, orderByFields, planParams, builderParams, compiledPlan, out compoundMatch))
        {
            rejectReason = null;
            return true;
        }

        if (plan.Plan.CompoundFieldDrivingClause < 0 || plan.Plan.CompoundFieldSortName == null)
            rejectReason = "no compound-field candidate identified at template time";
        else if (plan.Plan.AllNegated)
            rejectReason = "all clauses are negated";
        else
            rejectReason = "cost check failed (bitmap is cheaper), non-scannable residual, or prefix too long";
        return false;
    }

    /// <summary>compound(field1, field2) exists in the index, and any residual clauses are
    /// entry-scan eligible.
    /// Returns a DirectScanMatch wrapping a compound tree StartsWith with optional
    /// residual predicate checking.</summary>
    public static bool TryCreateCompoundFieldMatch(
        QueryExecution plan, OrderMetadata[] orderByFields,
        QueryPlanBuilder.PlanParameters planParams, QueryBuilderParameters builderParams,
        CompiledPlan compiledPlan, out IQueryMatch compoundMatch)
    {
        compoundMatch = null;

        // Discovery: structural rejection + cost gate. Structural facts
        // (CompoundFieldDrivingClause, CompoundFieldSortName) were pre-classified at
        // template time. This method runs cost estimation and residual-scannability
        // checks that are deterministic for the cache key (cardinality cliff bit 31
        // in Ordering segregates cliff buckets, so cost outcome is stable per key).
        int drivingClauseIdx = plan.Plan.CompoundFieldDrivingClause;
        string sortFieldName = plan.Plan.CompoundFieldSortName;
        if (drivingClauseIdx < 0 || sortFieldName == null)
            return false;
        var execs = plan.Executions;
        if (execs == null || drivingClauseIdx >= execs.Length || plan.Plan.AllNegated)
            return false;

        var indexSearcher = planParams.IndexSearcher;

        var drivingExec = execs[drivingClauseIdx];
        if (drivingExec.PackedParamValue.IsNone)
            return false;

        // Find optional field2 range narrowing clause (structural — same for all
        // executions of this template).
        int field2RangeIdx = FindCompoundFieldField2Range(execs, drivingClauseIdx, sortFieldName);

        // Residual scannability + cost check
        int longIdx = 0, doubleIdx = 0, sliceIdx = 0;
        long bitmapCost = 0;
        int residualCount = 0;
        for (int i = 0; i < execs.Length; i++)
        {
            bitmapCost += execs[i].Cardinality > 0 ? execs[i].Cardinality : indexSearcher.NumberOfEntries;
            if (i == drivingClauseIdx || i == field2RangeIdx)
                continue;
            if (execs[i].Clause.HasBoost || (execs[i] is { BoostFactor: > 0 }))
                return false;
            var pred = BuildScanPredicateInfo(execs[i].Clause, execs[i], ref longIdx, ref doubleIdx, ref sliceIdx);
            if (pred == null)
                return false;
            residualCount++;
        }

        long drivingCardinality = drivingExec.Cardinality > 0 ? drivingExec.Cardinality : indexSearcher.NumberOfEntries;
        long entriesToScan = drivingCardinality;
        if (residualCount > 0)
        {
            long minResidualCardinality = long.MaxValue;
            for (int i = 0; i < execs.Length; i++)
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
            return false;

        compoundMatch = ConstructCompoundField(plan, orderByFields, planParams, builderParams, compiledPlan,
            field2RangeIdx, entriesToScan, bitmapCost);
        return compoundMatch != null;
    }

    /// <summary>Locate an optional GT/GTE/LT/LTE/Between clause on the sort field
    /// that can narrow the compound prefix scan. Structural — same for all executions
    /// of a given template, but cheap enough to recompute on each Construct call
    /// rather than threading another field through QueryExecution.</summary>
    private static int FindCompoundFieldField2Range(ClauseExecution[] executions, int drivingClauseIdx, string sortFieldName)
    {
        for (int i = 0; i < executions.Length; i++)
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
        QueryExecution plan, OrderMetadata[] orderByFields,
        QueryPlanBuilder.PlanParameters planParams, QueryBuilderParameters builderParams, CompiledPlan compiledPlan,
        int field2RangeIdx, long entriesToScan, long bitmapCost)
    {
        var execs = plan.Executions;
        var indexSearcher = planParams.IndexSearcher;
        var allocator = planParams.Allocator;
        int drivingClauseIdx = plan.Plan.CompoundFieldDrivingClause;
        string sortFieldName = plan.Plan.CompoundFieldSortName;

        var drivingClause = execs[drivingClauseIdx].Clause;
        var drivingExec = execs[drivingClauseIdx];
        var packed = drivingExec.PackedParamValue;

        // Rebuild residual predicates (Construct rebuilds; the structural shape is
        // identical to what discovery just walked, so List growth is bounded).
        var residualPreds = new List<ScanPredicateInfo>();
        int rLongIdx = 0, rDoubleIdx = 0, rSliceIdx = 0;
        for (int i = 0; i < execs.Length; i++)
        {
            if (i == drivingClauseIdx || i == field2RangeIdx)
                continue;
            var pred = BuildScanPredicateInfo(execs[i].Clause, execs[i], ref rLongIdx, ref rDoubleIdx, ref rSliceIdx);
            if (pred == null)
                return null;
            residualPreds.Add(pred.Value);
        }

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

                if (usePrefix)
                {
                    // Field2 value too long or unsupported type — fall back to prefix-only
                    drivingMatch = indexSearcher.StartWithQuery(compoundFieldMeta, analyzedPrefix,
                        isNegated: false, forward: orderByFields[0].Ascending,
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
        BuildResidualScanParams(plan, indexSearcher, allocator, residualArray,
            drivingClauseIdx, field2RangeIdx,
            out var longParams, out var doubleParams, out var sliceParams, out var fieldRootPages);

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

        return directScan;
    }

    /// <summary>Check if a range clause on the ORDER BY field can be served by a direct</summary>
    public static bool TryCreateSimpleFieldDirectScan(
        QueryExecution plan, OrderMetadata[] orderByFields,
        QueryPlanBuilder.PlanParameters planParams, QueryBuilderParameters builderParams,
        CompiledPlan compiledPlan, out IQueryMatch directMatch, out string rejectReason)
    {
        rejectReason = null;
        bool result = TryCreateSimpleFieldDirectScan(plan, orderByFields, planParams, builderParams, compiledPlan, out directMatch);
        if (!result)
        {
            if (orderByFields == null || orderByFields.Length == 0)
                rejectReason = "no ORDER BY fields";
            else if (orderByFields.Length > 2)
                rejectReason = $"ORDER BY has {orderByFields.Length} fields (max 2 for direct scan)";
            else if (orderByFields.Length == 2 && orderByFields[1].FieldType is not (MatchCompareFieldType.Integer or MatchCompareFieldType.Floating))
                rejectReason = $"tie-break field type {orderByFields[1].FieldType} is not numeric";
            else if (plan.Executions is { Length: > 0 } && plan.Plan.SortDrivingClauseIndex < 0)
                rejectReason = $"no range/equals clause on sort field '{orderByFields[0].Field.FieldName}'";
            else
                rejectReason = "cost check failed (bitmap is cheaper), non-scannable residual, or cardinality too high for tie-break";
        }

        return result;
    }

    /// <summary>tree scan instead of the bitmap pipeline. The range query already walks the tree
    /// in sort order, so no SortingMatch wrapper is needed.</summary>
    public static bool TryCreateSimpleFieldDirectScan(
        QueryExecution plan, OrderMetadata[] orderByFields,
        QueryPlanBuilder.PlanParameters planParams, QueryBuilderParameters builderParams,
        CompiledPlan compiledPlan, out IQueryMatch directMatch)
    {
        directMatch = null;

        // Discovery: ORDER BY shape, sort-driving clause selection, residual
        // scannability + cost check. Per Phase 5, all per-execution rebuilds of
        // the live match are routed through ConstructDirectScan; this method
        // only validates the runtime state is compatible and then delegates.
        if (orderByFields == null || orderByFields.Length == 0)
            return false;

        if (orderByFields.Length > 2)
            return false;

        bool hasTieBreak = orderByFields.Length == 2;
        if (hasTieBreak)
        {
            var tieBreakType = orderByFields[1].FieldType;
            if (tieBreakType is not (MatchCompareFieldType.Integer or MatchCompareFieldType.Floating or MatchCompareFieldType.Sequence))
                return false;
        }

        var indexSearcher = planParams.IndexSearcher;
        string sortFieldName = orderByFields[0].Field.FieldName.ToString();
        var sortFieldType = orderByFields[0].FieldType;

        var execs = plan.Executions;
        bool isFullScan = execs == null || execs.Length == 0;

        if (isFullScan && plan.Plan.AllNegated)
            return false;

        // ── Discovery: drivingIdx + cost gate ──
        int drivingIdx = -1;
        long entriesToScan = 0, bitmapCost = 0;
        List<ScanPredicateInfo> preBuiltResiduals = null;
        if (isFullScan == false)
        {
            // SortDrivingClauseIndex pre-identified at template time and remapped to
            // post-sort index during Build — skip the per-execution clause scan.
            drivingIdx = plan.Plan.SortDrivingClauseIndex;
            if (drivingIdx == -1)
            {
                // Fallback: template didn't identify a candidate (e.g. WHEN eliminated the
                // clause, or sort field didn't match any template clause). Boost is ruled
                // out at template time, so we don't recheck BoostFactor here.
                for (int i = 0; i < execs.Length; i++)
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
            for (int i = 0; i < execs.Length; i++)
            {
                bitmapCost += execs[i].Cardinality > 0 ? execs[i].Cardinality : indexSearcher.NumberOfEntries;
                if (i == drivingIdx) continue;
                // Boost is ruled out at template time (see ComputeOptFlags).
                var pred = BuildScanPredicateInfo(execs[i].Clause, execs[i], ref rlongIdx, ref rdoubleIdx, ref rsliceIdx);
                if (pred == null)
                    return false;
                preBuiltResiduals ??= new List<ScanPredicateInfo>();
                preBuiltResiduals.Add(pred.Value);
            }

            long drivingCard = execs[drivingIdx].Cardinality > 0 ? execs[drivingIdx].Cardinality : indexSearcher.NumberOfEntries;
            entriesToScan = drivingCard;
            if (preBuiltResiduals is { Count: > 0 })
            {
                long minResidual = long.MaxValue;
                for (int i = 0; i < execs.Length; i++)
                {
                    if (i == drivingIdx) continue;
                    long c = execs[i].Cardinality > 0 ? execs[i].Cardinality : indexSearcher.NumberOfEntries;
                    if (c < minResidual) minResidual = c;
                }

                if (minResidual > 0 && minResidual < indexSearcher.NumberOfEntries)
                {
                    double passRate = (double)minResidual / indexSearcher.NumberOfEntries;
                    if (passRate > 0) entriesToScan = (long)(drivingCard / passRate);
                }
            }

            long directCost = entriesToScan > long.MaxValue / QueryPrimitives.EntryScanCostMultiplier ? long.MaxValue : entriesToScan * QueryPrimitives.EntryScanCostMultiplier;
            if (directCost >= bitmapCost || entriesToScan > QueryPrimitives.EntryScanCountThreshold)
                return false;
        }
        else
        {
            // Full-scan structural eligibility checks (would-cause-empty paths).
            if (orderByFields[0].MayHaveMissingEntries)
                return false;
            if (sortFieldType is not (MatchCompareFieldType.Sequence or MatchCompareFieldType.Integer or MatchCompareFieldType.Floating))
                return false;
        }

        directMatch = ConstructDirectScan(plan, orderByFields, planParams, builderParams, compiledPlan,
            drivingIdx, isFullScan, hasTieBreak, entriesToScan, bitmapCost, preBuiltResiduals);
        return directMatch != null;
    }

    /// <summary>Phase 5 bake: construction-only path for the DirectScan hint.
    /// Discovery (clause selection, cost gate, residual scannability) already passed
    /// in either TryCreateSimpleFieldDirectScan or by virtue of a cached
    /// <see cref="ExecutionStrategy.DirectScan"/>. Returns null when a per-execution
    /// runtime check fails (e.g. driving match resolution returns non-TermsProviderMatch
    /// or tie-break group cap exceeded by current parameter cardinality).</summary>
    private static IQueryMatch ConstructDirectScan(
        QueryExecution plan, OrderMetadata[] orderByFields,
        QueryPlanBuilder.PlanParameters planParams, QueryBuilderParameters builderParams, CompiledPlan compiledPlan,
        int drivingIdx, bool isFullScan, bool hasTieBreak,
        long entriesToScan, long bitmapCost,
        List<ScanPredicateInfo> preBuiltResiduals = null)
    {
        var indexSearcher = planParams.IndexSearcher;
        var walkerCtx = new ResolutionContext(builderParams);
        string sortFieldName = orderByFields[0].Field.FieldName.ToString();
        bool forward = orderByFields[0].Ascending;
        var sortFieldType = orderByFields[0].FieldType;
        var execs = plan.Executions;

        ITermsProvider provider;
        Voron.Impl.LowLevelTransaction llt;
        string drivingClauseDescription;

        if (isFullScan)
        {
            var fieldMeta = orderByFields[0].Field;
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
            var drivingClause = execs[drivingIdx].Clause;
            var drivingExec = execs[drivingIdx];

            TermsProviderMatch tpm;
            if (drivingClause.ClauseType == ClauseType.Equals)
            {
                var eqMatch = ResolveEqualsClauseWithDirection(drivingClause, drivingExec, plan, forward, walkerCtx);
                if (eqMatch is not TermsProviderMatch eq)
                    return null;
                tpm = eq;
            }
            else
            {
                var match = ResolveRangeClauseWithDirection(drivingClause, drivingExec, plan, forward, walkerCtx);
                if (match is not TermsProviderMatch m)
                    return null;
                tpm = m;
            }

            provider = tpm.Provider;
            llt = tpm.Llt;
            drivingClauseDescription = $"{drivingClause.FieldName} {drivingClause.ClauseType}";
        }

        // Residual predicates: reuse the list built during discovery when available;
        // the cached strategy dispatch path passes null and we build it here.
        List<ScanPredicateInfo> residualPreds = preBuiltResiduals;
        if (residualPreds == null && isFullScan == false)
        {
            int longIdx = 0, doubleIdx = 0, sliceIdx = 0;
            for (int i = 0; i < execs.Length; i++)
            {
                if (i == drivingIdx) continue;
                var pred = BuildScanPredicateInfo(execs[i].Clause, execs[i], ref longIdx, ref doubleIdx, ref sliceIdx);
                if (pred == null)
                    return null;
                residualPreds ??= new List<ScanPredicateInfo>();
                residualPreds.Add(pred.Value);
            }
        }

        // ── Create the driving match ──
        // BetweenQuery and StartWithQuery don't include nulls in their term output,
        // so SortedDrivingMatch must drain them itself (respecting nullFirst direction).
        bool nullIsSmallest = (orderByFields[0].NullsSortMode ?? builderParams.Index.Configuration.NullsSortMode) == NullsSortMode.NullsSmallest;
        bool nullFirst = forward ? nullIsSmallest : !nullIsSmallest;
        IQueryMatch drivingMatch;
        if (hasTieBreak)
        {
            // Secondary field uses its own NullsSortMode — distinct from the primary field's.
            bool secondaryNullIsSmallest = (orderByFields[1].NullsSortMode ?? builderParams.Index.Configuration.NullsSortMode) == NullsSortMode.NullsSmallest;
            int take = builderParams?.Take ?? Constants.IndexSearcher.TakeAll;
            drivingMatch = new SortedDrivingWithTieBreakMatch(
                provider, llt, planParams.Allocator, indexSearcher,
                orderByFields[0].Field, orderByFields[1].Field,
                orderByFields[1].FieldType, secondaryDescending: orderByFields[1].Ascending == false,
                nullFirst: nullFirst, nullIsSmallest: secondaryNullIsSmallest,
                take: take);
        }
        else
        {
            drivingMatch = new SortedDrivingMatch(provider, llt, planParams.Allocator,
                indexSearcher, orderByFields[0].Field, nullFirst);
        }

        // ── Residual scan parameters ──
        ScanPredicateInfo[] residualArray = residualPreds is { Count: > 0 } ? residualPreds.ToArray() : null;
        BuildResidualScanParams(plan, indexSearcher, planParams.Allocator, residualArray,
            drivingIdx, -1,
            out var longParams, out var doubleParams, out var sliceParams, out var fieldRootPages);

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
        return ds;
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

        var execs = plan.Executions;
        if (execs == null || execs.Length == 0)
            return;

        // Only consider the first ORDER BY field
        var sortField = orderByFields[0].Field.FieldName;

        // Find a range clause on the same field (scan all clauses, not just first — the sort-eligible
        // clause may not be the cheapest and thus not clause[0]).
        for (int i = 0; i < execs.Length; i++)
        {
            var clause = execs[i].Clause;
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

            if (ascending && clause.ClauseType is ClauseType.GreaterThan or ClauseType.GreaterThanOrEqual)
            {
                seekValue = packed.ValueType switch
                {
                    PackedParam.TypeLong => plan.LongValues[packed.Param1],
                    PackedParam.TypeDouble => plan.DoubleValues[packed.Param1],
                    PackedParam.TypeString => plan.StringValues[packed.Param1],
                    _ => null
                };
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
            }

            if (seekValue != null)
            {
                match.SortHint = new SortHint(clause.FieldName, seekValue);
                return;
            }
        }
    }

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

        if (orderByFields.Length == 0)
            return null;

        if (orderByFields.Length > MaxSortFields)
            throw new InvalidOperationException($"Corax does not support ordering by more than {MaxSortFields} properties.");

        // OrderMetadata contains a managed IPoint reference field, so stackalloc is not
        // possible here (only unmanaged structs may be stack-allocated in C#).
        int sortIndex = 0;
        var sortArray = new OrderMetadata[MaxSortFields];

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
                var seed = field.Arguments is { Length: > 0 } ? (int)Hashing.XXHash32.CalculateRaw(field.Arguments[0].NameOrValue) : Random.Shared.Next();
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
            // Dynamic CreateField fields: no IndexFieldsMapping entry, FieldId == DynamicField (-2).
            // Such fields are written per-document only when the index function emits CreateField;
            // docs that don't emit the field have NO entry (not even a NonExisting marker) in the
            // field's tree, so StreamAndIntersect (which walks tree + null/nonExisting lists) would
            // silently drop them. Route through ExtractAndSort instead — see SortingMatch.Fill.
            bool mayHaveMissingEntries = metadataField.FieldId == Constants.IndexWriter.DynamicField;
            OrderMetadata? temporaryOrder = null;
            switch (orderingType)
            {
                case OrderByFieldType.Custom:
                    throw new NotSupportedInCoraxException($"{nameof(Corax)} doesn't support Custom OrderBy.");
                case OrderByFieldType.AlphaNumeric:
                    sortArray[sortIndex++] = new OrderMetadata(metadataField, field.Ascending, MatchCompareFieldType.Alphanumeric, fieldIsEmpty, nullsSortMode, mayHaveMissingEntries);
                    continue;
                case OrderByFieldType.Long:
                    temporaryOrder = new OrderMetadata(metadataField, field.Ascending, MatchCompareFieldType.Integer, fieldIsEmpty, nullsSortMode, mayHaveMissingEntries);
                    break;
                case OrderByFieldType.Double:
                    temporaryOrder = new OrderMetadata(metadataField, field.Ascending, MatchCompareFieldType.Floating, fieldIsEmpty, nullsSortMode, mayHaveMissingEntries);
                    break;
            }

            sortArray[sortIndex++] = temporaryOrder ?? new OrderMetadata(metadataField, field.Ascending, MatchCompareFieldType.Sequence, fieldIsEmpty, nullsSortMode, mayHaveMissingEntries);
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
    public static IQueryMatch ApplyScoreOrdering(QueryPlanBuilder.PlanParameters planParams, IQueryMatch match, long take, CancellationToken token = default)
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
        ResolutionContext walkerCtx)
    {
        var result = searchMeta;
        var indexFieldsMapping = walkerCtx.BuilderParams.IndexFieldsMapping;

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

    /// <summary>Create the appropriate DirectScan match based on whether residual predicates exist.</summary>
    private static DirectScanMatchBase BuildDirectScan(
        IndexSearcher searcher, IQueryMatch drivingMatch,
        long[] longParams, double[] doubleParams, Voron.Slice[] sliceParams, long[] fieldRootPages,
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
    internal static string FormatValueFromPlan(PackedParam packed, QueryExecution plan) => FormatValueFromPlanInternal(packed, plan, packed.Param1);

    /// <summary>Format the second value (BETWEEN high bound) from the plan's typed arrays.</summary>
    internal static string FormatValue2FromPlan(PackedParam packed, QueryExecution plan) => FormatValueFromPlanInternal(packed, plan, packed.Param2);

    private static string FormatValueFromPlanInternal(PackedParam packed, QueryExecution plan, int idx)
    {
        if (idx is PackedParam.NoParamValue)
            return null;
        // An IN clause with all-null terms records InTermCount=0 and writes no values
        // to the typed arrays, but the packed Param1 still points at the (empty) slot.
        // Bounds-check before indexing — return null to indicate "no displayable value".
        return packed.ValueType switch
        {
            PackedParam.TypeLong => idx < plan.LongValues?.Length ? plan.LongValues[idx].ToString() : null,
            PackedParam.TypeDouble => idx < plan.DoubleValues?.Length ? plan.DoubleValues[idx].ToString(CultureInfo.InvariantCulture) : null,
            _ => idx < plan.StringValues?.Length ? plan.StringValues[idx] : null
        };
    }

    /// <summary>Translate sorted clauses into a linear PlanOp[] sequence for IL emission.
    ///
    /// Bitmap slots:
    ///   Slot 0 = main result bitmap (accumulates the final answer).
    ///   Slot 1 = scratch bitmap (used for AND-chain non-seed IN terms and OR-group accumulation).
    ///   Slot 2 = save slot (only allocated when an OR chain contains multiple AND-groups;
    ///            used to save the prior OR accumulation while building a new AND sub-chain
    ///            in slot 0 via SwapBitmaps(0,2), then ORed back).
    ///
    /// AND chain: the first clause seeds slot 0 (FillFromPostings), the following clauses narrow it
    /// (AndWithPostings/AndNotWithPostings). IN terms are ORed into slot 1, then ANDed with slot 0.
    ///
    /// OR chain: all terms are ORed into slot 0. AND-groups within an OR use the three-bitmap
    /// swap pattern: save slot 0 → slot 2, build AND result in slot 0, OR slot 2 back.</summary>
    private static QueryExecution EmitPlan(bool isOr, ClauseExecution[] executions)
    {
        // "Empty IN" check — must consult runtime InTermCount when executions are
        // available. Bindings.Length is the *structural* count (one slot per
        // ValueExpression in the IN literal); when a single parameter binding
        // expands to a runtime array, InTermCount is the true element count and
        // can be 0 even though Bindings.Length is 1.
        //
        // HasNullTerm must also block the empty-IN path: a list whose only entry
        // is null arrives as InTermCount=0+HasNullTerm=true and still has to match
        // docs with a null in that field via the null-term posting list (Fill@0
        // reads the null PL, OrRange/AndRange becomes a runtime no-op when
        // InRangeCounts[rangeIdx] resolves to 0).
        static bool IsEmptyIn(ClauseExecution e) =>
            e.ClauseType is ClauseType.In or ClauseType.AllIn &&
            (e.InTermCount == 0) &&
            e.HasNullTerm != true;

        if (isOr is false)
        {
            // Empty IN clauses: zero results in AND, no-op in OR.
            for (int i = 0; i < executions.Length; i++)
            {
                if (IsEmptyIn(executions[i]))
                {
                    return new QueryExecution { Ops = [], IsAllEntries = false };
                }
            }
        }
        else
        {
            int write = 0;
            for (int i = 0; i < executions.Length; i++)
            {
                if (IsEmptyIn(executions[i]))
                {
                    continue;
                }

                executions[write] = executions[i];
                write++;
            }

            if (write < executions.Length)
            {
                Array.Resize(ref executions, write);
            }
        }

        List<PlanOp> ops = [];
        List<int> rangeCounts = [];
        bool needsThreeBitmaps = false;

        if (isOr)
        {
            // OR chain — expand In/OrGroup terms into individual OR ops
            int matchIndex = 0;
            for (int ci = 0; ci < executions.Length; ci++)
            {
                var it = executions[ci].Clause;

                // Negated clause in an OR chain: emit a single QueryMatch slot whose match
                // is materialized at resolution time as AllEntries ANDNOT(positive form).
                // Covers NotEquals, NOT IN, NOT AllIn, NOT exists(), NOT startsWith(), etc.
                // The raw posting list / range / tree-scan can't deliver the complement,
                // so dispatch is forced to QueryMatch and CreateNotEqualsOrMatch produces
                // a pre-materialized BitmapMatch. The IsOrChainNotEquals flag is set at
                // template build time by PlanWalker.NotCanonicalize.
                if (it.IsNegated || it.ClauseType == ClauseType.NotEquals)
                {
                    Debug.Assert(it.IsOrChainNotEquals,
                        "PlanWalker.NotCanonicalize must mark every negated OR-chain clause with IsOrChainNotEquals=true before template freeze.");
                    ops.Add(new PlanOp
                    {
                        Kind = matchIndex == 0 ? PlanOpKind.FillFromPostings : PlanOpKind.OrWithPostings,
                        ParamIndex = matchIndex,
                        EstimatedCardinality = executions[ci].Cardinality,
                        Dispatch = MatchDispatch.QueryMatch
                    });
                    matchIndex++;
                    continue;
                }

                switch (it.ClauseType)
                {
                    case ClauseType.In or ClauseType.AllIn:
                    {
                        // OR chain uses EmitInOps for both In and AllIn (range is OR'd regardless).
                        EmitInOps(ops, it, executions[ci].Cardinality, bitmapLocal: 0, isSeed: matchIndex == 0, ref matchIndex, rangeCounts);
                        break;
                    }
                    case ClauseType.OrGroup when it.SubClauses is { Count: > 0 }:
                    {
                        int subCount = it.SubClauses.Count;
                        for (int si = 0; si < subCount; si++)
                        {
                            var sub = it.SubClauses[si];
                            ops.Add(new PlanOp
                            {
                                Kind = matchIndex == 0 ? PlanOpKind.FillFromPostings : PlanOpKind.OrWithPostings,
                                ParamIndex = matchIndex,
                                EstimatedCardinality = executions[ci].Cardinality / subCount,
                                Dispatch = GetDispatch(sub)
                            });
                            matchIndex++;
                        }

                        break;
                    }
                    case ClauseType.AndGroup when it.SubClauses is { Count: > 0 }:
                    {
                        // AND sub-expression inside an OR chain.
                        // Only supported when the AND group is the first element (matchIndex == 0)
                        // or can be merged into slot 0 via OrBitmaps after computing into slot 1.
                        var subClauses = it.SubClauses;
                        int subCount = subClauses.Count;
                        long subCardinality = executions[ci].Cardinality / Math.Max(1, subCount);
                        if (matchIndex == 0)
                        {
                            // First element: build the AND chain directly in slot 0.
                            // Slot 1 is free (unused), so AndWithPostings can use it as scratch.
                            // Suppress early-exit on AND steps — the OR chain continues regardless.
                            ops.Add(new PlanOp
                            {
                                Kind = PlanOpKind.FillFromPostings,
                                ParamIndex = matchIndex,
                                EstimatedCardinality = subCardinality,
                                Dispatch = GetDispatch(subClauses[0])
                            });
                            for (int s = 1; s < subClauses.Count; s++)
                            {
                                ops.Add(new PlanOp
                                {
                                    Kind = subClauses[s].IsNegated ? PlanOpKind.AndNotWithPostings : PlanOpKind.AndWithPostings,
                                    ParamIndex = matchIndex + s,
                                    EstimatedCardinality = subCardinality,
                                    Dispatch = GetDispatch(subClauses[s]),
                                    SkipEarlyExit = true // don't abort on empty — remaining OR terms may still match
                                });
                            }
                        }
                        else
                        {
                            // Non-first AND group: save the accumulated OR result (slot 0) to slot 2,
                            // build this AND sub-chain fresh in slot 0, then OR slot 2 back.
                            needsThreeBitmaps = true;
                            // Uses SwapBitmaps(0, 2): slot 0 ↔ slot 2.
                            // Slot 2 must have been cleared before the swap (it's either the initial
                            // empty state or was cleared at the end of the previous iteration).
                            ops.Add(new PlanOp { Kind = PlanOpKind.ClearBitmap, BitmapLocal = 1 });
                            ops.Add(new PlanOp
                            {
                                Kind = PlanOpKind.SwapBitmaps,
                                BitmapLocal = 0,
                                ParamIndex2 = 2
                            });
                            // Slot 0 is now fresh (was slot 2 = cleared); slot 2 = prior OR accumulation.
                            ops.Add(new PlanOp
                            {
                                Kind = PlanOpKind.FillFromPostings,
                                ParamIndex = matchIndex,
                                EstimatedCardinality = subCardinality,
                                Dispatch = GetDispatch(subClauses[0])
                            });
                            for (int s = 1; s < subClauses.Count; s++)
                            {
                                ops.Add(new PlanOp
                                {
                                    Kind = subClauses[s].IsNegated ? PlanOpKind.AndNotWithPostings : PlanOpKind.AndWithPostings,
                                    ParamIndex = matchIndex + s,
                                    BitmapLocal = 0,
                                    EstimatedCardinality = subCardinality,
                                    Dispatch = GetDispatch(subClauses[s]),
                                    SkipEarlyExit = true // don't abort — OR chain continues
                                });
                            }

                            ops.Add(new PlanOp
                            {
                                Kind = PlanOpKind.OrBitmaps,
                                BitmapLocal = 0,
                                ParamIndex2 = 2
                            });
                            ops.Add(new PlanOp { Kind = PlanOpKind.ClearBitmap, BitmapLocal = 2 });
                        }

                        matchIndex += subClauses.Count;
                        break;
                    }
                    default:
                    {
                        ops.Add(new PlanOp
                        {
                            Kind = matchIndex == 0 ? PlanOpKind.FillFromPostings : PlanOpKind.OrWithPostings,
                            ParamIndex = matchIndex,
                            EstimatedCardinality = executions[ci].Cardinality,
                            Dispatch = GetDispatch(it)
                        });
                        matchIndex++;
                        break;
                    }
                }
            }

            ops.Add(new PlanOp { Kind = PlanOpKind.IterateInto });
        }
        else
        {
            var c0 = executions[0].Clause;
            var e0 = executions[0];
            switch (executions.Length)
            {
                case 1 when e0.ClauseType == ClauseType.Equals && e0.IsNegated is false:
                    ops.Add(new PlanOp
                    {
                        Kind = PlanOpKind.DirectIterate,
                        ParamIndex = 0,
                        EstimatedCardinality = e0.Cardinality,
                        Dispatch = GetDispatch(c0)
                    });
                    break;
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
                        Dispatch = GetDispatch(c0)
                    });
                    ops.Add(new PlanOp { Kind = PlanOpKind.IterateInto });

                    // Mark clause as negated so ResolveMatches/ResolveTermSources
                    // produce [AllEntries, TermMatch].
                    if (e0.IsNegated == false)
                    {
                        e0.IsNegated = true;
                    }

                    return new QueryExecution
                    {
                        Ops = ops.ToArray()
                    };
                default:
                {
                    // AND chain: Fill the smallest non-negated, then AndWith/AndNotWith remaining.
                    // If the first clause is negated (all clauses are negated), we need to
                    // start from AllEntries and ANDNOT each one.
                    bool firstIsNegated = e0.IsNegated || e0.ClauseType == ClauseType.NotEquals;
                    int startIndex;

                    if (firstIsNegated)
                    {
                        // All clauses are negated — start from all entries.
                        // FillAllEntries doesn't need a slot index — it calls indexSearcher.AllEntries()
                        // directly. This sidesteps the structural-vs-runtime slot-index mismatch that
                        // occurs when an IN clause's runtime InTermCount differs from its template
                        // Bindings.Length (e.g. NOT IN with a parameter that expands to an array).
                        ops.Add(new PlanOp
                        {
                            Kind = PlanOpKind.FillAllEntries,
                            EstimatedCardinality = long.MaxValue
                        });
                        startIndex = 0; // Process all clauses as ANDNOT
                    }
                    else
                    {
                        startIndex = 1;
                    }

                    int matchIndex = 0;
                    if (!firstIsNegated)
                    {
                        switch (e0.ClauseType)
                        {
                            case ClauseType.OrGroup when c0.SubClauses != null:
                            {
                                var subClauses = c0.SubClauses;
                                int subCount = subClauses.Count;
                                for (int s = 0; s < subCount; s++)
                                {
                                    ops.Add(new PlanOp
                                    {
                                        Kind = s == 0 ? PlanOpKind.FillFromPostings : PlanOpKind.OrWithPostings,
                                        ParamIndex = matchIndex + s,
                                        BitmapLocal = 0,
                                        EstimatedCardinality = executions[0].Cardinality / Math.Max(1, subCount),
                                        Dispatch = GetDispatch(subClauses[s])
                                    });
                                }

                                matchIndex += subCount;
                                break;
                            }
                            case ClauseType.In:
                            {
                                EmitInOps(ops, c0, executions[0].Cardinality, bitmapLocal: 0, isSeed: true, ref matchIndex, rangeCounts);
                                break;
                            }
                            // Use the fixed-shape EmitAllInOps path for AllIn clauses.
                            case ClauseType.AllIn:
                            {
                                EmitAllInOps(ops, c0, executions[0].Cardinality, ref matchIndex, rangeCounts);
                                break;
                            }
                            default:
                                ops.Add(new PlanOp
                                {
                                    Kind = PlanOpKind.FillFromPostings,
                                    ParamIndex = 0,
                                    EstimatedCardinality = executions[0].Cardinality,
                                    Dispatch = GetDispatch(c0)
                                });
                                matchIndex = 1;
                                break;
                        }
                    }

                    // Precheck: can all remaining clauses be converted to entry scan predicates?
                    bool allScanEligible = AreAllScanEligible(executions, startIndex);

                    for (int i = startIndex; i < executions.Length; i++)
                    {
                        // Goto check before each AND step — only if all remaining clauses
                        // can be handled by entry scan predicates
                        if (allScanEligible)
                        {
                            ops.Add(new PlanOp
                            {
                                Kind = PlanOpKind.CheckAndMaybeEntryScan,
                                ParamIndex = matchIndex
                            });
                        }

                        var ci2 = executions[i].Clause;
                        switch (ci2.ClauseType)
                        {
                            case ClauseType.OrGroup when ci2.SubClauses != null:
                            {
                                // OrGroup: OR subclauses into bitmap[1], then AND with bitmap[0]
                                var subClauses = ci2.SubClauses;
                                int subCount = subClauses.Count;

                                // Clear bitmap[1] (OR accumulator)
                                ops.Add(new PlanOp { Kind = PlanOpKind.ClearBitmap, BitmapLocal = 1 });

                                // Fill each subclause into bitmap[1]
                                for (int s = 0; s < subCount; s++)
                                {
                                    ops.Add(new PlanOp
                                    {
                                        Kind = PlanOpKind.OrWithPostings,
                                        ParamIndex = matchIndex + s,
                                        BitmapLocal = 1, // target bitmap[1]
                                        EstimatedCardinality = executions[i].Cardinality / Math.Max(1, subCount),
                                        Dispatch = GetDispatch(subClauses[s])
                                    });
                                }

                                // AND bitmap[1] into bitmap[0]
                                ops.Add(new PlanOp
                                {
                                    Kind = PlanOpKind.AndBitmaps,
                                    BitmapLocal = 0, // target
                                    ParamIndex2 = 1 // source (reuse ParamIndex2 for source bitmap)
                                });

                                // Early exit check
                                ops.Add(new PlanOp { Kind = PlanOpKind.CheckEmpty, BitmapLocal = 0 });

                                matchIndex += subCount;
                                break;
                            }
                            case ClauseType.In:
                            {
                                // OR all IN terms into bitmap[1], then AND (or ANDNOT) with bitmap[0].
                                // isSeed: false — FillFromPostings always targets bitmap[0], so we use
                                // OrRange which respects bitmapLocal. Bitmap[1] is freshly cleared.
                                ops.Add(new PlanOp { Kind = PlanOpKind.ClearBitmap, BitmapLocal = 1 });
                                EmitInOps(ops, ci2, executions[i].Cardinality, bitmapLocal: 1, isSeed: false, ref matchIndex, rangeCounts);
                                ops.Add(new PlanOp
                                {
                                    Kind = ci2.IsNegated ? PlanOpKind.AndNotBitmaps : PlanOpKind.AndBitmaps,
                                    BitmapLocal = 0,
                                    ParamIndex2 = 1
                                });
                                if (!ci2.IsNegated)
                                {
                                    ops.Add(new PlanOp { Kind = PlanOpKind.CheckEmpty, BitmapLocal = 0 });
                                }

                                break;
                            }
                            case ClauseType.AllIn:
                            {
                                int inTermCount = (ci2.Bindings?.Length ?? 0);
                                int rangeIdx = rangeCounts.Count;
                                rangeCounts.Add(inTermCount);
                                ops.Add(new PlanOp
                                {
                                    Kind = PlanOpKind.AndRange,
                                    ParamIndex = matchIndex,
                                    ParamIndex2 = rangeIdx,
                                    BitmapLocal = 0,
                                    EstimatedCardinality = executions[i].Cardinality,
                                    Dispatch = MatchDispatch.PostingList
                                });
                                matchIndex += inTermCount;
                                break;
                            }
                            default:
                            {
                                // Simple clause: AND or ANDNOT with bitmap[0]
                                var isNegated = ci2.IsNegated || ci2.ClauseType == ClauseType.NotEquals;
                                var andKind = isNegated ? PlanOpKind.AndNotWithPostings : PlanOpKind.AndWithPostings;
                                ops.Add(new PlanOp
                                {
                                    Kind = andKind,
                                    ParamIndex = matchIndex,
                                    BitmapLocal = 0,
                                    EstimatedCardinality = executions[i].Cardinality,
                                    Dispatch = GetDispatch(ci2)
                                });

                                if (!isNegated)
                                {
                                    ops.Add(new PlanOp { Kind = PlanOpKind.CheckEmpty, BitmapLocal = 0 });
                                }

                                matchIndex++;
                                break;
                            }
                        }
                    }

                    ops.Add(new PlanOp { Kind = PlanOpKind.IterateInto });
                    break;
                }
            }
        }

        return new QueryExecution
        {
            Ops = ops.ToArray(),
            RequiredBitmaps = needsThreeBitmaps ? 3 : 2,
            InRangeCounts = rangeCounts.Count > 0 ? rangeCounts.ToArray() : null
        };
    }

    /// <summary>Encode clause sort order into a 30-bit integer for the plan-cache key.
    /// Up to 10 clauses × 3 bits each; slot <c>i</c> holds
    /// <c>clauses[i].OriginalIndex &amp; 0x7</c> shifted by <c>i*3</c>.</summary>
    private static int ComputeOperandOrdering(ClauseExecution[] executions)
    {
        int ordering = 0;
        for (int i = 0; i < Math.Min(executions.Length, 10); i++)
            ordering |= (executions[i].Clause.OriginalIndex & 0x7) << (i * 3);
        return ordering;
    }

    private static (bool IsSingleNegatedEquals, bool AllNegated) CheckNegatedClauses(ClauseExecution[] executions)
    {
        // Matches exactly 1 element that is negated (requires standalone optimization path)
        var isSingleNegatedEquals = executions is [{ IsNegated: true }];
        // Matches >= 2 elements where the first is negated (cardinality sort guarantees subsequent ones are too)
        var allNegated = executions is [{ IsNegated: true }, _, ..]; // here we heck 
        return (isSingleNegatedEquals, allNegated);
    }

    /// <summary>Count the number of IN/AllIn range-count slots in the given executions.
    /// Mirrors the <c>rangeCounts</c> list built inside <see cref="EmitPlan"/>.</summary>
    private static int CountInRangeSlots(ClauseExecution[] executions)
    {
        int count = 0;
        for (int i = 0; i < executions.Length; i++)
        {
            if (executions[i].Clause.ClauseType is ClauseType.In or ClauseType.AllIn)
                count++;
        }
        return count;
    }

    /// <summary>Build the per-execution InRangeCounts array from executions without
    /// needing the structural array from EmitPlan. Returns null when there are no
    /// IN/AllIn clauses.</summary>
    private static int[] BuildInRangeCounts(ClauseExecution[] executions, bool isOr, int slotCount)
    {
        if (slotCount == 0)
            return null;

        var counts = new int[slotCount];
        int rangeIdx = 0;
        for (int ci = 0; ci < executions.Length && rangeIdx < counts.Length; ci++)
        {
            var cl = executions[ci].Clause;
            if (cl.ClauseType == ClauseType.In)
            {
                counts[rangeIdx] = executions[ci].InTermCount;
                rangeIdx++;
            }
            else if (cl.ClauseType == ClauseType.AllIn)
            {
                int inCount = executions[ci].InTermCount;
                bool hasNull = executions[ci].HasNullTerm;
                if (ci == 0 && !isOr)
                    counts[rangeIdx] = Math.Max(0, inCount - 1 + (hasNull ? 1 : 0));
                else if (isOr)
                    counts[rangeIdx] = inCount;
                else
                    counts[rangeIdx] = inCount;
                rangeIdx++;
            }
        }
        return counts;
    }

    private static bool AreAllScanEligible(ClauseExecution[] executions, int startIndex)
    {
        // If any clause (In, AllIn, Spatial, Vector, Search, etc.) can't be scanned, we must not emit CheckAndMaybeEntryScan — entry scan would skip them entirely.
        int dummyL = 0, dummyD = 0, dummyS = 0;
        for (int j = startIndex; j < executions.Length; j++)
        {
            ParamValueType termType = executions[j].TermValueType;
            if (BuildScanPredicateInfoCore(executions[j].Clause, exec: null, termType,
                    ref dummyL, ref dummyD, ref dummyS) != null)
            {
                continue;
            }

            return false;
        }

        return true;
    }

    // ── Plan helpers ─────────────────────────────────────────────────────

    /// <summary>Emit ops for an IN clause: Fill the first term + OrRange for the rest, plus null-term if needed.</summary>
    /// <summary>Emit ops for an IN clause: Fill slot 0 + OrRange for the rest.
    /// Fixed 2-op shape regardless of term count or presence of null. Slot 0 holds
    /// the null-term posting list when HasNullTerm, else the first typed term, else
    /// an empty PostingSource. Slots 1..N-1 hold remaining typed terms, dispatched
    /// via OrRange whose count comes from <c>ctx.InRangeCounts[rangeIdx]</c> at
    /// runtime.</summary>
    private static void EmitInOps(List<PlanOp> ops, ClauseInfo clause, long cardinality, int bitmapLocal, bool isSeed, ref int matchIndex, List<int> rangeCounts)
    {
        int totalSlots = (clause.Bindings?.Length ?? 0) + 1;
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
    /// terms lives in <c>ctx.InRangeCounts</c> rather than the op shape itself.</summary>
    private static void EmitAllInOps(List<PlanOp> ops, ClauseInfo clause, long cardinality, ref int matchIndex, List<int> rangeCounts)
    {
        int totalSlots = (clause.Bindings?.Length ?? 0) + 1;
        // For AllIn, ANDing with an Empty PostingSource clears the bitmap — so the
        // null-term slot is always included in the range. At runtime, when no null term
        // is present the slot holds an empty match (AND with empty = clear), which is
        // correct for AllIn semantics. The range count covers all slots after slot 0.
        int rangeCount = totalSlots - 1;
        int rangeIdx = rangeCounts.Count;
        rangeCounts.Add(rangeCount);

        ops.Add(new PlanOp
        {
            Kind = PlanOpKind.FillFromPostings,
            ParamIndex = matchIndex,
            BitmapLocal = 0,
            EstimatedCardinality = Math.Max(1, cardinality / totalSlots),
            Dispatch = MatchDispatch.PostingList
        });
        ops.Add(new PlanOp
        {
            Kind = PlanOpKind.AndRange,
            ParamIndex = matchIndex + 1,
            ParamIndex2 = rangeIdx,
            BitmapLocal = 0,
            EstimatedCardinality = cardinality,
            Dispatch = MatchDispatch.PostingList
        });
        matchIndex += totalSlots;
    }

    private static QueryExecution BuildAllEntriesPlan()
    {
        // No bitmap needed — AllEntries already implements IQueryMatch.Fill(),
        // so we iterate it directly without materializing into a bitmap first.
        return new QueryExecution
        {
            Ops = [new PlanOp { Kind = PlanOpKind.DirectIterate, ParamIndex = 0 }],
            IsAllEntries = true
        };
    }

    private static void AttachPostFilterPhases(QueryExecution plan, bool allNegated,
        ClauseInfo[] spatialClauses, ClauseExecution[] spatialExecs,
        ClauseInfo[] vectorClauses, ClauseExecution[] vectorExecs)
    {
        if (spatialClauses == null && vectorClauses == null)
        {
            return;
        }

        // Extend Executions array to include spatial/vector post-filter clauses.
        int extraCount = (spatialClauses?.Length ?? 0) + (vectorClauses?.Length ?? 0);
        var execs = plan.Executions ??= [];
        int execIdx = execs.Length;
        Array.Resize(ref execs, execs.Length + extraCount);

        int matchIndex = CountMatchSlots(execs, plan.IsAllEntries, allNegated);

        if (spatialClauses != null)
        {
            plan.SpatialFilters = new SpatialFilterOp[spatialClauses.Length];
            for (int i = 0; i < spatialClauses.Length; i++)
            {
                var exec = spatialExecs?[i] ?? new ClauseExecution(spatialClauses[i]);
                execs[execIdx++] = exec;
                plan.SpatialFilters[i] = new SpatialFilterOp { MatchIndex = matchIndex++, Clause = spatialClauses[i], Exec = exec };
            }
        }

        if (vectorClauses != null)
        {
            plan.VectorSelects = new VectorSearchOp[vectorClauses.Length];
            for (int i = 0; i < vectorClauses.Length; i++)
            {
                var exec = vectorExecs?[i] ?? new ClauseExecution(vectorClauses[i]);
                execs[execIdx++] = exec;
                plan.VectorSelects[i] = new VectorSearchOp
                {
                    Clause = vectorClauses[i], Exec = exec
                };
            }
        }

        plan.Executions = execs;
    }


    internal static int CountMatchSlots(ClauseExecution[] executions, bool isAllEntries, bool allNegated)
    {
        if (executions == null || executions.Length == 0)
        {
            return isAllEntries ? 1 : 0;
        }

        int count = isAllEntries ? 1 : 0;
        for (int ci = 0; ci < executions.Length; ci++)
        {
            var clause = executions[ci].Clause;
            var exec = executions[ci];
            if (clause.IsOrChainNotEquals)
            {
                count += 1;
                continue;
            }

            count += clause.ClauseType switch
            {
                ClauseType.OrGroup or ClauseType.AndGroup when clause.SubClauses != null => clause.SubClauses.Count,
                ClauseType.In or ClauseType.AllIn => (exec.InTermCount > 0 ? exec.InTermCount : clause.Bindings?.Length ?? 0) + 1,
                _ => 1
            };
        }

        if (allNegated)
        {
            count++;
        }

        return count;
    }

    /// <summary>For an OrGroup or AndGroup clause, returns the parallel (sub-clauses, sub-executions)
    /// arrays that callers iterate to fan out one match slot per sub-term. Returns false for any
    /// other clause type, or for empty groups. <paramref name="subExecs"/> is null when
    /// <paramref name="exec"/> is null (TermsProviders path tolerates that).</summary>
    internal static bool TryGetGroupFanOut(ClauseInfo clause, ClauseExecution exec,
        out List<ClauseInfo> subClauses, out ClauseExecution[] subExecs)
    {
        if (clause.ClauseType is ClauseType.OrGroup or ClauseType.AndGroup && clause.SubClauses is { Count: > 0 })
        {
            subClauses = clause.SubClauses;
            subExecs = exec?.SubExecutions;
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
        return clause is { HasBoost: true, ClauseType: ClauseType.Equals or ClauseType.NotEquals };
    }

    /// <summary>TreeScan-eligible: multi-term clauses that have a direct ITermsProvider
    /// (StartsWith, EndsWith, Exists, Regex, ranges). Boosted clauses go through QueryMatch
    /// for scoring. Contains is excluded because its tree walk pattern doesn't benefit
    /// from the direct dispatch (it walks the full tree regardless).</summary>
    internal static bool IsTreeScanEligibleClause(ClauseInfo clause)
    {
        if (clause is null or { HasBoost: true })
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
    
    /// <summary>Resolution-time overload: derives term type from <paramref name="exec"/>
    /// and recurses into subclauses using sub-execution types. Used when actual resolved
    /// types are available (per-execution, after PopulateClauseValues).</summary>
    internal static ScanPredicateInfo? BuildScanPredicateInfo(ClauseInfo clause, ClauseExecution exec,
        ref int longIndex, ref int doubleIndex, ref int sliceIndex)
        => BuildScanPredicateInfoCore(clause, exec, exec?.TermValueType ?? ParamValueType.String,
            ref longIndex, ref doubleIndex, ref sliceIndex);

    /// <summary>Single walker shared by both overloads. <paramref name="exec"/> is non-null on
    /// the resolution path and supplies per-sub TermValueType during group recursion; on the
    /// template path it is null and recursion falls back to InferTermType.</summary>
    private static ScanPredicateInfo? BuildScanPredicateInfoCore(ClauseInfo clause, ClauseExecution exec,
        ParamValueType termType, ref int longIndex, ref int doubleIndex, ref int sliceIndex)
    {
        switch (clause.ClauseType)
        {
            case ClauseType.Search:
            case ClauseType.Regex:
            case ClauseType.Spatial:
            case ClauseType.Vector:
            case ClauseType.StartsWith:
            {
                if (termType != ParamValueType.String)
                {
                    return null;
                }

                sliceIndex++;
                return new ScanPredicateInfo
                {
                    FieldName = clause.FieldName,
                    ValueType = ScanValueType.Slice,
                    CompareOp = ScanCompareOp.StartsWith,
                    ParamIndex = sliceIndex - 1
                };
            }
            case ClauseType.In:
            case ClauseType.AllIn:
                // IN/AllIn cannot use the single-slot StartsWith fallback above — they
                // need multi-term OR/AND semantics. Returning null forces these clauses
                // through the regular posting-list pipeline; AreAllScanEligible will see
                // null and disable entry-scan for any AND chain that contains them.
                return null;
            case ClauseType.EndsWith:
            {
                if (termType != ParamValueType.String)
                {
                    return null;
                }

                sliceIndex++;
                return new ScanPredicateInfo
                {
                    FieldName = clause.FieldName,
                    ValueType = ScanValueType.Slice,
                    CompareOp = ScanCompareOp.EndsWith,
                    ParamIndex = sliceIndex - 1
                };
            }
            case ClauseType.AndGroup:
            {
                if (clause.SubClauses is not { Count: > 0 } subs)
                {
                    return null;
                }

                var subExecs = exec?.SubExecutions;
                var branches = new List<ScanPredicateInfo>();
                for (int si = 0; si < subs.Count; si++)
                {
                    var sub = subs[si];
                    var subExec = subExecs != null && si < subExecs.Length ? subExecs[si] : null;
                    var subTermType = subExec?.TermValueType ?? InferTermType(sub);
                    var subPred = BuildScanPredicateInfoCore(sub, subExec, subTermType,
                        ref longIndex, ref doubleIndex, ref sliceIndex);
                    if (subPred == null)
                    {
                        return null;
                    }

                    branches.Add(subPred.Value);
                }
                return new ScanPredicateInfo
                {
                    FieldName = clause.FieldName ?? subs[0].FieldName,
                    ValueType = ScanValueType.Long,
                    CompareOp = ScanCompareOp.Equal,
                    SubPredicates = branches.ToArray(),
                    Group = GroupKind.And
                };
            }

            case ClauseType.Exists:
                return new ScanPredicateInfo
                {
                    FieldName = clause.FieldName,
                    ValueType = ScanValueType.Long, // unused for Exists but must be set
                    CompareOp = ScanCompareOp.Exists,
                    ParamIndex = 0 // unused
                };

            case ClauseType.OrGroup:
            {
                if (clause.SubClauses is not { Count: > 0 } subs)
                {
                    return null;
                }

                var subExecs = exec?.SubExecutions;
                var branches = new List<ScanPredicateInfo>();
                int li = longIndex, di = doubleIndex, slc = sliceIndex;
                for (int si = 0; si < subs.Count; si++)
                {
                    var sub = subs[si];
                    var subExec = subExecs != null && si < subExecs.Length ? subExecs[si] : null;
                    var subTermType = subExec?.TermValueType ?? InferTermType(sub);
                    var subPred = BuildScanPredicateInfoCore(sub, subExec, subTermType,
                        ref li, ref di, ref slc);
                    if (subPred == null)
                    {
                        return null; // Any complex subclause → can't entry-scan the whole group
                    }

                    branches.Add(subPred.Value);
                }
                longIndex = li; doubleIndex = di; sliceIndex = slc;
                return new ScanPredicateInfo
                {
                    FieldName = subs[0].FieldName,
                    SubPredicates = branches.ToArray(),
                    Group = GroupKind.Or
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

        // Strong typing: termType is set by GetTermValue from the parser's literal
        // type (for inline values) or the resolved JSON-blittable runtime type (for params).
        // Switch on it directly — no string round-trip / TryParse fallback.
        ScanValueType valueType;
        switch (termType)
        {
            case ParamValueType.Long:
                valueType = ScanValueType.Long;
                break;
            case ParamValueType.Double:
                valueType = ScanValueType.Double;
                break;
            default:
                // String/True/False/Null/Parameter (when unresolvable) → opaque slice comparison.
                valueType = ScanValueType.Slice;
                break;
        }

        bool isBetween = clause.ClauseType == ClauseType.Between;
        int idx, idx2;
        switch (valueType)
        {
            case ScanValueType.Long:
                idx = longIndex++;
                idx2 = isBetween ? longIndex++ : -1;
                break;
            case ScanValueType.Double:
                idx = doubleIndex++;
                idx2 = isBetween ? doubleIndex++ : -1;
                break;
            default:
                idx = sliceIndex++;
                idx2 = isBetween ? sliceIndex++ : -1;
                break;
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
        if (queryParams == null || queryParams.TryGet(name, out object raw) == false || raw == null)
            return ScanValueType.Slice;
        return raw switch
        {
            long => ScanValueType.Long,
            double => ScanValueType.Double,
            LazyNumberValue lnv => lnv.TryParseLong(out _) ? ScanValueType.Long : ScanValueType.Double,
            string { Length: < 83 } => ScanValueType.Slice, // statically skip Encoding.UTF8.GetByteCount() < 255 here, since we _know_ it's < 255 regardless
            string s when Encoding.UTF8.GetByteCount(s) < byte.MaxValue => ScanValueType.Slice, 
            string s => ScanValueType.SliceLong,
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
}
