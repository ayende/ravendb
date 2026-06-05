using System;
using System.Collections.Generic;
using System.Threading;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Planning;
using Corax.Querying.Primitives;
using Corax.Utils;
using Raven.Client.Exceptions;
using Sparrow.Json;
using IndexSearcher = Corax.Querying.IndexSearcher;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

internal static partial class QueryPlanBuilder
{
    private static IQueryMatch Instantiate(
        CompiledPlan compiledPlan,
        QueryExecution exec,
        OrderMetadata[] orderByFields,
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

        // A query may pin a specific execution strategy via the reserved $rvn_corax_strategy parameter.
        // Forcing bypasses the per-execution cost gate but NOT structural validity: a strategy that is
        // structurally impossible for this query still falls back to BitmapPipeline when its Construct*
        // returns null. The cached CompiledPlan.Strategy (the natural choice) is never mutated — forcing
        // only redirects this one execution. This exists so every strategy can be exercised under test.
        ExecutionStrategy? forced = TryGetForcedStrategy(ctx.PlanParams.QueryParameters);
        ExecutionStrategy effective = forced ?? compiledPlan.Strategy;

        switch (effective)
        {
            case ExecutionStrategy.CompoundKeyLookup:
                innerMatch = ConstructCompoundExact(ref ctx);
                if (innerMatch is null) goto default;
                exec.ActualStrategy = ExecutionStrategy.CompoundKeyLookup;
                return innerMatch;
            // orderByFields can be null when page size is 0, in which case, we need to get the actual total count
            // no advantage of using compound field here, since we can't stop midway (like we do with paging)
            case ExecutionStrategy.CompoundSortedScan when orderByFields != null:
                // Always compute the cost estimate: ConstructCompoundField consumes entriesToScan/bitmapCost.
                // When forced, we ignore the gate's verdict but still need those values.
                bool cfEffective = CompoundFieldCostEffective(ref ctx, out long cfEntriesToScan, out long cfBitmapCost);
                if (forced is null && cfEffective == false)
                    goto default; // if this isn't expected to benefit us, just use a bitmap query option
                innerMatch = ConstructCompoundField(ref ctx, walkerCtx, ctx.Exec.CompoundFieldField2Range, cfEntriesToScan, cfBitmapCost);
                if (innerMatch is null) goto default;
                exec.ActualStrategy = ExecutionStrategy.CompoundSortedScan;
                return OrderBy(builderParameters, innerMatch, orderByFields);
            case ExecutionStrategy.FieldSortedScan when orderByFields != null:
                var execs = exec.Executions;
                bool isFullScan = execs is not { Count: > 0 };
                string directScanReason = forced is not null ? "forced via $rvn_corax_strategy" : null;
                if (forced is not null || DirectScanCostEffective(ref ctx, isFullScan, out directScanReason))
                {
                    bool hasTieBreak = orderByFields.Length == 2;
                    innerMatch = ConstructDirectScan(ref ctx, walkerCtx, exec.SortDrivingClause, isFullScan, hasTieBreak, directScanReason);
                    if (innerMatch is not null)
                    {
                        exec.ActualStrategy = ExecutionStrategy.FieldSortedScan;
                        return innerMatch;
                    }
                }
                goto default;
            case ExecutionStrategy.BitmapPipeline:
            default: // may either be the selected strategy or a one-off (because of bad parameters preventing a faster strategy)
                exec.ActualStrategy = ExecutionStrategy.BitmapPipeline;
                innerMatch = InstantiateBitmapPipeline(ctx.Plan, ctx.Exec, ctx.PlanParams, ctx.BuilderParams, walkerCtx, highlightingTerms, wantTimings, token);
                if (ctx.OrderByFields == null) return innerMatch;
                if (innerMatch is CompiledQueryMatch seekMatch)
                    TrySetSortSeekHint(ctx.Plan, ctx.Exec, seekMatch);
                return OrderBy(ctx.BuilderParams, innerMatch, ctx.OrderByFields);
        }

        static void SelectExecutionStrategy(ref InstCtx ctx)
        {
            // ── Slow path: cache-miss, run Try* discovery chain ──
            ctx.Plan.DecisionTrail = new();
            ctx.Plan.Strategy = ExecutionStrategy.BitmapPipeline; // if nothing else overrides it

            if (ctx.Plan.Template.OptimizationFlags.HasFlag(PlanOptimizationFlags.CompoundExactCandidate))
            {
                if (TryCreateCompoundExactMatch(ref ctx, out ctx.RejectReason))
                {
                    // No trail entry on success: CompoundKeyLookup has no per-execution cost gate, so there is
                    // no decision to record — the chosen strategy is already surfaced via StrategyCandidate.
                    // A rejection IS recorded below: it explains why a structurally-available optimization
                    // did not apply (encoding failed / boosted clause).
                    ctx.Plan.Strategy = ExecutionStrategy.CompoundKeyLookup;
                    return;
                }

                ctx.Plan.DecisionTrail.Record("CompoundKeyLookup", false, ctx.RejectReason ?? "rejected");
            }

            // No ORDER BY: nothing to decide about a sort strategy, so no trail entry — just stop here.
            if (ctx.OrderByFields is null)
                return;

            if (ctx.Plan.Template.OptimizationFlags.HasFlag(PlanOptimizationFlags.DirectScanCandidate))
            {
                if (TryCreateCompoundFieldMatch(ref ctx, out ctx.RejectReason))
                {
                    ctx.Plan.Strategy = ExecutionStrategy.CompoundSortedScan;
                    ctx.Plan.DecisionTrail.Record("CompoundSortedScan", true, "compound tree scan candidate (cost gated per-execution)");
                    return;
                }

                ctx.Plan.DecisionTrail.Record("CompoundSortedScan", false, ctx.RejectReason ?? "rejected");

                if (TryCreateSimpleFieldDirectScan(ref ctx, out ctx.RejectReason))
                {
                    ctx.Plan.Strategy = ExecutionStrategy.FieldSortedScan;
                    ctx.Plan.DecisionTrail.Record("FieldSortedScan", true, "direct tree scan candidate on sort field (cost gated per-execution)");
                    return;
                }

                ctx.Plan.DecisionTrail.Record("FieldSortedScan", false, ctx.RejectReason ?? "rejected");
            }

            ctx.Plan.DecisionTrail.Record("BitmapPipeline", true, "bitmap pipeline with SortingMatch fallback");
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
            var drivingExec = ctx.Exec.CompoundFieldDrivingClause;

            if (drivingExec.PackedParamValue.IsNone)
                return false;

            var indexSearcher = ctx.PlanParams.IndexSearcher;
            var field2Range = ctx.Exec.CompoundFieldField2Range;
            int residualCount = 0;
            for (int i = 0; i < execs.Count; i++)
            {
                bitmapCost += execs[i].GetEffectiveCardinality(indexSearcher);
                if (ReferenceEquals(execs[i], drivingExec) || ReferenceEquals(execs[i], field2Range))
                    continue;
                residualCount++;
            }

            long drivingCardinality = drivingExec.GetEffectiveCardinality(indexSearcher);
            // No residual filter: the compound subtree is walked in ORDER BY order and the walk stops once
            // the page is filled, so we never read more than the page's worth of entries regardless of how
            // large the driving set is. Page-bound the estimate (a non-paged query keeps the full cardinality
            // — there is no early stop, so the bitmap+sort path wins as the gate below will then decide).
            // With a residual present we must over-scan to fill the page; ComputeNumberOfEntriesQueryLikelyToScan
            // does that page-bounding internally and inflates by the residual's selectivity.
            entriesToScan = residualCount > 0
                ? ComputeNumberOfEntriesQueryLikelyToScan(execs, drivingExec, drivingCardinality, ctx.BuilderParams.Query.PageSize, indexSearcher)
                : Math.Min(drivingCardinality, ctx.BuilderParams.Query.PageSize);

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
            var drivingExec = ctx.Exec.SortDrivingClause;
            if (drivingExec is null)
                return false;
            if (drivingExec.PackedParamValue.IsNone)
                return false;

            long bitmapCost = 0;
            var indexSearcher = ctx.PlanParams.IndexSearcher;
            foreach (var it in execs)
            {
                bitmapCost += it.GetEffectiveCardinality(indexSearcher);
            }

            long drivingCard = drivingExec.GetEffectiveCardinality(indexSearcher);
            // No residual filter beyond the driving clause: the sorted single-field tree is streamed in
            // ORDER BY order and the stream stops once the page is filled, so the scan is page-bounded
            // regardless of how large the driving set is. Page-bound the estimate (a non-paged query keeps
            // the full cardinality — no early stop — and the gate below then prefers bitmap+sort).
            var entriesToScan = execs.Count > 1
                ? ComputeNumberOfEntriesQueryLikelyToScan(execs, drivingExec, drivingCard, ctx.BuilderParams.Query.PageSize, indexSearcher)
                : Math.Min(drivingCard, ctx.BuilderParams.Query.PageSize);

            if (ctx.WantTimings)
                directScanReason = $"entries_to_scan({entriesToScan}) × {QueryPrimitives.EntryScanCostMultiplier} < bitmap_cost({bitmapCost})";
            return IsDirectScanCostEffective(entriesToScan, bitmapCost);
        }

        static long ComputeNumberOfEntriesQueryLikelyToScan(List<ClauseExecution> execs,
            ClauseExecution drivingClause, long drivingCard, long pageSize, IndexSearcher indexSearcher)
        {
            long resultsWanted = Math.Min(drivingCard, pageSize);

            long minResidual = long.MaxValue;
            for (int i = 0; i < execs.Count; i++)
            {
                if (ReferenceEquals(execs[i], drivingClause)) continue;
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

    /// <summary>
    /// Reserved query-parameter name used to pin a query to a specific execution strategy. Supplied as
    /// <c>$rvn_corax_strategy</c> in RQL (stored without the leading <c>$</c> in the parameters object).
    /// Intended for tests that must exercise a particular strategy regardless of the cost gate's verdict.
    /// </summary>
    private const string ForceStrategyParameterName = "rvn_corax_strategy";

    /// <summary>
    /// Reads the reserved <see cref="ForceStrategyParameterName"/> parameter, if present, and maps it to an
    /// <see cref="ExecutionStrategy"/>. Accepts the exact enum names (case-insensitive) plus a few friendly
    /// aliases. Returns <c>null</c> when the parameter is absent. Throws on an unrecognized value so a typo
    /// in a test fails loudly rather than silently falling back to the natural strategy.
    /// </summary>
    private static ExecutionStrategy? TryGetForcedStrategy(BlittableJsonReaderObject queryParameters)
    {
        if (queryParameters is null)
            return null;
        if (queryParameters.TryGet(ForceStrategyParameterName, out string value) == false || string.IsNullOrEmpty(value))
            return null;

        switch (value.ToLowerInvariant())
        {
            case "bitmap":
            case "bitmappipeline":
                return ExecutionStrategy.BitmapPipeline;
            case "compoundkey":
            case "compoundexact":
            case "compoundkeylookup":
                return ExecutionStrategy.CompoundKeyLookup;
            case "compoundsort":
            case "compoundscan":
            case "compoundsortedscan":
                return ExecutionStrategy.CompoundSortedScan;
            case "directsort":
            case "directscan":
            case "fieldsort":
            case "fieldsortedscan":
                return ExecutionStrategy.FieldSortedScan;
            default:
                throw new InvalidQueryException(
                    $"The reserved query parameter '${ForceStrategyParameterName}' has an unrecognized value '{value}'. " +
                    "Valid values are: BitmapPipeline, CompoundKeyLookup, CompoundSortedScan, FieldSortedScan " +
                    "(aliases: Bitmap; CompoundKey/CompoundExact; CompoundSort/CompoundScan; DirectSort/DirectScan/FieldSort).");
        }
    }
}
