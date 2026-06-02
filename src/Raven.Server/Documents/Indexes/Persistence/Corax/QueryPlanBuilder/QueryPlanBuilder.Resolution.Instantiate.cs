using System;
using System.Collections.Generic;
using System.Threading;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Planning;
using Corax.Querying.Primitives;
using Corax.Utils;
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

        switch (compiledPlan.Strategy)
        {
            case ExecutionStrategy.CompoundExact:
                innerMatch = ConstructCompoundExact(ref ctx);
                if (innerMatch is null) goto default;
                exec.ActualStrategy = ExecutionStrategy.CompoundExact;
                return innerMatch;
            // orderByFields can be null when page size is 0, in which case, we need to get the actual total count
            // no advantage of using compound field here, since we can't stop midway (like we do with paging)
            case ExecutionStrategy.CompoundField when orderByFields != null:
                if (CompoundFieldCostEffective(ref ctx, out long cfEntriesToScan, out long cfBitmapCost) == false)
                    goto default; // if this isn't expected to benefit us, just use a bitmap query option
                innerMatch = ConstructCompoundField(ref ctx, walkerCtx, ctx.Exec.CompoundFieldField2Range, cfEntriesToScan, cfBitmapCost);
                if (innerMatch is null) goto default;
                exec.ActualStrategy = ExecutionStrategy.CompoundField;
                return OrderBy(builderParameters, innerMatch, orderByFields);
            case ExecutionStrategy.DirectScan when orderByFields != null:
                var execs = exec.Executions;
                bool isFullScan = execs is not { Count: > 0 };
                if (DirectScanCostEffective(ref ctx, isFullScan, out var directScanReason))
                {
                    bool hasTieBreak = orderByFields.Length == 2;
                    innerMatch = ConstructDirectScan(ref ctx, walkerCtx, exec.SortDrivingClause, isFullScan, hasTieBreak, directScanReason);
                    if (innerMatch is not null)
                    {
                        exec.ActualStrategy = ExecutionStrategy.DirectScan;
                        return innerMatch;
                    }
                }
                goto default;
            case ExecutionStrategy.BitmapSort:
            default: // may either be the selected strategy or a one-off (because of bad parameters preventing a faster strategy)
                exec.ActualStrategy = ExecutionStrategy.BitmapSort;
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
            ctx.Plan.Strategy = ExecutionStrategy.BitmapSort; // if nothing else overrides it

            if (ctx.Plan.Template.OptimizationFlags.HasFlag(PlanOptimizationFlags.CompoundExactCandidate))
            {
                if (TryCreateCompoundExactMatch(ref ctx, out ctx.RejectReason))
                {
                    // No trail entry on success: CompoundExact has no per-execution cost gate, so there is
                    // no decision to record — the chosen strategy is already surfaced via StrategyCandidate.
                    // A rejection IS recorded below: it explains why a structurally-available optimization
                    // did not apply (encoding failed / boosted clause).
                    ctx.Plan.Strategy = ExecutionStrategy.CompoundExact;
                    return;
                }

                ctx.Plan.DecisionTrail.Record("CompoundExact", false, ctx.RejectReason ?? "rejected");
            }

            // No ORDER BY: nothing to decide about a sort strategy, so no trail entry — just stop here.
            if (ctx.OrderByFields is null)
                return;

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
            entriesToScan = residualCount > 0
                ? ComputeNumberOfEntriesQueryLikelyToScan(execs, drivingExec, drivingCardinality, ctx.BuilderParams.Query.PageSize, indexSearcher)
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
            var entriesToScan = execs.Count > 1
                ? ComputeNumberOfEntriesQueryLikelyToScan(execs, drivingExec, drivingCard, ctx.BuilderParams.Query.PageSize, indexSearcher)
                : drivingCard;

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
}
