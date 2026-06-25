using System;
using System.Collections.Generic;
using System.Threading;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.SortingMatches;
using Corax.Querying.Planning;
using Corax.Querying.Primitives;
using Corax.Utils;
using Raven.Client.Exceptions;
using Sparrow.Json;
using IndexSearcher = Corax.Querying.IndexSearcher;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

internal static partial class QueryPlanBuilder
{
    private static (IQueryMatch Exec, IQueryMatch Inner) Instantiate(
        QueryExecution exec,
        OrderMetadata[] orderByFields,
        PlanParameters planParams,
        QueryBuilderParameters builderParameters,
        ResolutionContext walkerCtx,
        Dictionary<string, CoraxHighlightingTermIndex> highlightingTerms,
        bool wantTimings,
        CancellationToken token)
    {
        var compiledPlan = exec.Plan;
        var ctx = new InstantiateContext(compiledPlan, exec, orderByFields, planParams, builderParameters, wantTimings);
        if (compiledPlan.Strategy == ExecutionStrategy.NotEvaluated)
            SelectExecutionStrategy(ref ctx);

        // A query may pin a specific execution strategy via the reserved $rvn_corax_strategy parameter.
        // Forcing bypasses the per-execution cost gate but NOT structural validity. This exists so every strategy can be exercised under user's explicit request.
        ExecutionStrategy? forced = TryGetForcedStrategy(ctx.PlanParams.QueryParameters);
        ExecutionStrategy effective = forced ?? compiledPlan.Strategy;

        // Independent of the bitmap-pipeline strategy above: a query may also pin the SortingMatch's sort
        // strategy via $rvn_corax_sort. Applied to whichever SortingMatch the dispatch below produces.
        CoraxSortingStrategy? forcedSort = TryGetForcedSortStrategy(ctx.PlanParams.QueryParameters);

        switch (effective)
        {
            case ExecutionStrategy.CompoundKeyLookup:
            {
                var innerMatch = ConstructCompoundExact(ref ctx);
                if (innerMatch is null) goto default;
                exec.ActualStrategy = ExecutionStrategy.CompoundKeyLookup;
                return (innerMatch, innerMatch);
            }
            // orderByFields can be null when page size is 0, in which case, we need to get the actual total count
            // no advantage of using compound field here, since we can't stop midway (like we do with paging)
            case ExecutionStrategy.CompoundSortedScan when orderByFields != null:
            {
                // Always compute the cost estimate: ConstructCompoundField consumes entriesToScan/bitmapCost.
                // When forced, we ignore the gate's verdict but still need those values.
                bool cfEffective = CompoundFieldCostEffective(ref ctx, out long cfEntriesToScan, out long cfBitmapCost);
                if (wantTimings)
                    exec.StrategyGateReason = $"entries_to_scan({cfEntriesToScan}) × {QueryPrimitives.EntryScanCostMultiplier} {(cfEffective ? "<" : ">=")} bitmap_cost({cfBitmapCost})";
                if (forced is null && cfEffective == false)
                    goto default; // if this isn't expected to benefit us, just use a bitmap query option
                // Single-field order (== the compound's field2): the DirectScan already emits in that order, so
                // elide the wrapper and push a page-bounded Take into the scan. Independent of $rvn_corax_sort —
                // a sorted scan IS the order, so a SortingMatch would only re-sort and force a TakeAll drain (the
                // sort hint only applies in the bitmap pipeline). Mirrors the FieldSortedScan path.
                bool canElideCompoundSort = orderByFields.Length == 1;
                var innerMatch = ConstructCompoundField(ref ctx, walkerCtx, ctx.Exec.CompoundFieldField2Range, cfEntriesToScan, cfBitmapCost, canElideCompoundSort);
                if (innerMatch is null) goto default;
                exec.ActualStrategy = ExecutionStrategy.CompoundSortedScan;
                var outer = canElideCompoundSort
                    ? innerMatch // already in field2 order; DirectScan handles Take itself 
                    : ApplyForcedSort(OrderBy(builderParameters, innerMatch, orderByFields), forcedSort);
                return (outer, innerMatch);
            }
            case ExecutionStrategy.FieldSortedScan when orderByFields != null:
            {
                var execs = exec.Executions;
                bool isFullScan = execs is not { Count: > 0 };
                string directScanReason = forced is not null ? "forced via $rvn_corax_strategy" : null;
                bool directScanEffective = forced is not null || DirectScanCostEffective(ref ctx, isFullScan, out directScanReason);
                exec.StrategyGateReason = directScanReason;
                if (directScanEffective)
                {
                    bool hasTieBreak = orderByFields.Length == 2;
                    var innerMatch = ConstructDirectScan(ref ctx, walkerCtx, exec.SortDrivingClause, isFullScan, hasTieBreak, directScanReason);
                    if (innerMatch is not null)
                    {
                        exec.ActualStrategy = ExecutionStrategy.FieldSortedScan;
                        return (innerMatch, innerMatch);
                    }
                }

                goto default;
            }
            case ExecutionStrategy.BitmapPipeline:
            default: // may either be the selected strategy or a one-off (because of bad parameters preventing a faster strategy)
            {
                exec.ActualStrategy = ExecutionStrategy.BitmapPipeline;
                var innerMatch = InstantiateBitmapPipeline(ctx.Plan, ctx.Exec, ctx.PlanParams, ctx.BuilderParams, walkerCtx, highlightingTerms, wantTimings, token);
                if (innerMatch is CompiledQueryMatch forcedScanMatch)
                    forcedScanMatch.ForcedEntryScanGate = TryGetForcedEntryScanGate(ctx.PlanParams.QueryParameters);
                // no ordering or already streams its results in right order — return the match as is.
                if (ctx.OrderByFields == null || ctx.Exec.VectorPostFilterProvidesScoreOrder) 
                    return (innerMatch, innerMatch);
                if (innerMatch is CompiledQueryMatch seekMatch)
                    TrySetSortSeekHint(ctx.Plan, ctx.Exec, seekMatch);
                return (ApplyForcedSort(OrderBy(ctx.BuilderParams, innerMatch, ctx.OrderByFields), forcedSort), innerMatch);
            }
        }

        static void SelectExecutionStrategy(ref InstantiateContext ctx)
        {
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
        
        static bool CompoundFieldCostEffective(ref InstantiateContext ctx, out long entriesToScan, out long bitmapCost)
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

            if (residualCount == 0)
            {
                // No residual filter: the compound (field1, field2) subtree is walked in ORDER BY order and its
                // entry ids are emitted directly — DirectScanSimpleMatch reads no stored entries at all. That is
                // exactly the posting-list work the bitmap path does, minus the sort the bitmap path still has to
                // perform. So the sorted walk is unconditionally cheaper than build-bitmap-then-sort, paged or not.
                entriesToScan = Math.Min(drivingCardinality, ctx.BuilderParams.Query.PageSize); // for diagnostics only
                return true;
            }

            // Residual present: DirectScanFilteredMatch must read each scanned entry's stored fields
            // (EntryTermsReader, EntryScanCostMultiplier× a posting decode) to test the residual, and it over-scans
            // the sorted stream to fill the page because only a fraction of scanned entries survive. Estimate that
            // over-scan and let the gate decide whether it still beats the bitmap pipeline.
            entriesToScan = ComputeNumberOfEntriesQueryLikelyToScan(execs, drivingExec, drivingCardinality, ResolveEffectiveScanPageSize(ctx.BuilderParams), indexSearcher);
            bitmapCost += SurvivorSortCost(EstimateSurvivors(execs, indexSearcher)); // add the bitmap's survivor-sort cost
            return IsDirectScanCostEffective(entriesToScan, bitmapCost);
        }
        
        static bool DirectScanCostEffective(ref InstantiateContext ctx, bool isFullScan, out string directScanReason)
        {
            if (isFullScan)
            {
                directScanReason = "no filter — walking the whole index in sort order";
                return true;
            }

            directScanReason = null;

            var execs = ctx.Exec.Executions;
            var drivingExec = ctx.Exec.SortDrivingClause;
            if (drivingExec is null)
                return false;
            if (drivingExec.PackedParamValue.IsNone)
                return false;

            var indexSearcher = ctx.PlanParams.IndexSearcher;

            if (execs.Count <= 1)
            {
                directScanReason = "sorted index walk with no extra filters to apply, so sorting is free";
                return true;
            }

            long bitmapCost = 0;
            foreach (var it in execs)
            {
                bitmapCost += it.GetEffectiveCardinality(indexSearcher);
            }

            // The bitmap path doesn't just decode the posting lists (Σ above) — it SORTS the surviving
            // intersection. Add that sort cost, else few-survivor queries look expensive (Σ dominated by a broad
            // clause) and the gate wrongly picks a scan that over-reads stored fields. See EntryScanSurvivorSortFactor.
            bitmapCost += SurvivorSortCost(EstimateSurvivors(execs, indexSearcher));

            // Residual present: the scan reads each scanned entry's stored fields to test the residual and
            // over-scans the sorted stream to fill the page. Estimate the over-scan and let the gate compare
            // it to the cost of building and sorting the bitmap instead.
            long drivingCard = drivingExec.GetEffectiveCardinality(indexSearcher);
            var entriesToScan = ComputeNumberOfEntriesQueryLikelyToScan(execs, drivingExec, drivingCard, ResolveEffectiveScanPageSize(ctx.BuilderParams), indexSearcher);

            bool effective = IsDirectScanCostEffective(entriesToScan, bitmapCost);
            if (ctx.WantTimings)
                directScanReason = FormatGateReason(entriesToScan, bitmapCost, DescribeUnboundedScanTake(ctx.BuilderParams));
            return effective;
        }
        
        static long CalculateDirectCost(long entriesToScan)
        {
            return entriesToScan > long.MaxValue / QueryPrimitives.EntryScanCostMultiplier
                ? long.MaxValue // avoid overflow
                : entriesToScan * QueryPrimitives.EntryScanCostMultiplier;
        }

        static bool IsDirectScanCostEffective(long entriesToScan, long bitmapCost)
        {
            long directCost = CalculateDirectCost(entriesToScan);

            // check what will be more costly, and set a hard limit (32K) to how many entries we may scan
            return directCost < bitmapCost && entriesToScan <= QueryPrimitives.EntryScanCountThreshold;
        }

        // Independence (product) estimate of the AND intersection: N * Π(card_i / N), clamped to [0, N]. This is
        // the number of documents the bitmap pipeline must SORT, the cost a pure Σ-cardinalities bitmapCost omits.
        static long EstimateSurvivors(List<ClauseExecution> execs, IndexSearcher indexSearcher)
        {
            double n = indexSearcher.NumberOfEntries;
            if (n <= 0)
                return 0;

            double survivors = n;
            foreach (var it in execs)
                survivors *= it.GetEffectiveCardinality(indexSearcher) / n;

            return (long)Math.Clamp(survivors, 0, n);
        }

        static long SurvivorSortCost(long survivors)
            => survivors > long.MaxValue / QueryPrimitives.EntryScanSurvivorSortFactor
                ? long.MaxValue // avoid overflow
                : survivors * QueryPrimitives.EntryScanSurvivorSortFactor;

        static string FormatGateReason(long entriesToScan, long bitmapCost, string unboundedReason)
        {
            string suffix = unboundedReason is null ? "" : $" [page unbounded: {unboundedReason}]";
    
            if (entriesToScan > QueryPrimitives.EntryScanCountThreshold)
                return $"entries_to_scan({entriesToScan}) > cap({QueryPrimitives.EntryScanCountThreshold}) → bitmap{suffix}";

            long directCost = CalculateDirectCost(entriesToScan);
    
            return directCost < bitmapCost
                ? $"entries_to_scan({entriesToScan}) × {QueryPrimitives.EntryScanCostMultiplier} = {directCost} < bitmap_cost({bitmapCost}) → scan{suffix}"
                : $"entries_to_scan({entriesToScan}) × {QueryPrimitives.EntryScanCostMultiplier} = {directCost} >= bitmap_cost({bitmapCost}) → bitmap{suffix}";
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

    private const string ForceStrategyParameterName = "rvn_corax_strategy";

    private static ExecutionStrategy? TryGetForcedStrategy(BlittableJsonReaderObject queryParameters)
    {
        if (queryParameters is null)
            return null;
        if (queryParameters.TryGet(ForceStrategyParameterName, out string value) == false || string.IsNullOrEmpty(value))
            return null;

        if(Enum.TryParse(value, out ExecutionStrategy result) is false)
            throw new InvalidQueryException(
                $"The reserved query parameter '${ForceStrategyParameterName}' has an unrecognized value '{value}'. Expected one of: {string.Join(", ", Enum.GetNames<ExecutionStrategy>())}");
        return result;

    }

    private const string ForceSortParameterName = "rvn_corax_sort";

    // A query may pin the SortingMatch sort strategy via $rvn_corax_sort (e.g. "IndexOrderStreaming" to
    // force the index walk, "InMemorySort" to force the bounded materialize-and-sort). Read at
    // instantiation time, never part of the plan-cache key. The pin is honored only where a runtime
    // choice exists (see SortingMatch.Fill); a pin that can't apply to the query shape is ignored.
    private static CoraxSortingStrategy? TryGetForcedSortStrategy(BlittableJsonReaderObject queryParameters)
    {
        if (queryParameters is null)
            return null;
        if (queryParameters.TryGet(ForceSortParameterName, out string value) == false || string.IsNullOrEmpty(value))
            return null;

        if (Enum.TryParse(value, out CoraxSortingStrategy result) is false)
            throw new InvalidQueryException(
                $"The reserved query parameter '${ForceSortParameterName}' has an unrecognized value '{value}'. Expected one of: {string.Join(", ", Enum.GetNames<CoraxSortingStrategy>())}");
        return result;
    }

    private static IQueryMatch ApplyForcedSort(IQueryMatch match, CoraxSortingStrategy? forcedSort)
    {
        if (forcedSort is { } strategy && match is SortingMatch sortingMatch)
            sortingMatch.ForcedStrategy = strategy;
        return match;
    }

    private const string ForceEntryScanParameterName = "rvn_corax_entry_scan";

    // A query may force or disable the entry-scan gate via $rvn_corax_entry_scan: the op-index of the
    // gate to force (matches the EntryScanAt the plan reports), -1 to disable every gate, or absent to
    // leave the runtime cost gate in charge. Read at instantiation time, never part of the plan-cache key.
    private static int TryGetForcedEntryScanGate(BlittableJsonReaderObject queryParameters)
    {
        if (queryParameters is null)
            return QueryPrimitives.EntryScanGateUnset;
        if (queryParameters.TryGet(ForceEntryScanParameterName, out long value) == false)
            return QueryPrimitives.EntryScanGateUnset;
        if (value < int.MinValue || value > int.MaxValue)
            throw new InvalidQueryException($"The reserved query parameter '${ForceEntryScanParameterName}' must fit in a 32-bit integer, but got '{value}'.");
        return (int)value;
    }
}
