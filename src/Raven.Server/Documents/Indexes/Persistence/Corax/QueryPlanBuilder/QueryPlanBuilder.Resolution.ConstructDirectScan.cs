using System;
using Corax.Mappings;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.SortingMatches.Meta;
using Corax.Querying.Planning;
using Constants = Corax.Constants;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

internal static partial class QueryPlanBuilder
{
    private static IQueryMatch ConstructDirectScan(ref InstantiateContext ctx, ResolutionContext walkerCtx,
        ClauseExecution drivingClause, bool isFullScan, bool hasTieBreak, string reasonForInspection)
    {
        var indexSearcher = ctx.PlanParams.IndexSearcher;
        string sortFieldName = ctx.WantTimings ? ctx.OrderByFields[0].Field.FieldName.ToString() : null;
        bool forward = ctx.OrderByFields[0].Ascending;

        // FROM Posts ORDER BY PublishedAt DESC - full scan, no WHERE, has no residuals. This check ensure we construct a sorted driving match for this
        if (isFullScan == false && ctx.Exec.Plan.DirectScanResidualSet is null)
            return null; 

        var (drivingMatchProvider, drivingClauseDescription) = isFullScan ?
            ResolveFullScanDrivingProvider(ref ctx, forward) :
            ResolveDrivingProvider(ref ctx, walkerCtx, drivingClause, forward);
        
        if (drivingMatchProvider is not TermsProviderMatch tpm)
            return null; // can happen if we have no entries for this field

        bool nullFirst = ResolveNullFirst(ctx.OrderByFields[0], ctx.BuilderParams.Index.Configuration.NullsSortMode, forward);

        bool hasResidual = ctx.Exec.Plan.DirectScanResidualSet is { HasPredicates: true };

        // For a no-residual single-valued scan the exact TotalResults equals the driving provider's posting count.
        // Resolving it up front (O(distinct terms)) lets the read skip the count drain AND page-bound the scan even
        // under statistics (the drain is no longer the count source; ResolveSortedScanTake would otherwise force
        // TakeAll). A residual filter's count depends on draining survivors, so it can't be bounded (knownTotal = -1).
        long probeTicks = -1; // Stopwatch ticks the CountPostingsInRange header walk took (-1 = no probe ran).
        int probeTerms = 0;
        long knownTotal = hasResidual ? -1 : TryResolveDirectScanKnownTotal(ref ctx, walkerCtx, drivingClause, isFullScan, forward, out probeTicks, out probeTerms);
        // The take threaded into the driving match. When knownTotal resolves, the scan is page-bounded even
        // under statistics, so the inner SortedDrivingWithTieBreakMatch can bound its per-group top-K heap.
        int take = knownTotal >= 0 ? ctx.BuilderParams.Take : ResolveSortedScanTake(ctx.BuilderParams);

        IQueryMatch drivingMatch = hasTieBreak
            ? BuildSortedDrivingWithTieBreakMatch(ctx, tpm.Provider, tpm.Llt, ctx.BuilderParams.Index.Configuration.NullsSortMode, indexSearcher, nullFirst, take)
            : new SortedDrivingMatch(tpm.Provider, tpm.Llt, ctx.PlanParams.Allocator, indexSearcher, ctx.OrderByFields[0].Field, nullFirst);

        DirectScanMatchBase ds;
        if (hasResidual)
        {
            // Filter every clause EXCEPT the sort-driving clause (walked by the tree).
            ScanParamExtractor.Extract(ctx.Exec, indexSearcher, walkerCtx, ctx.Exec.Plan.DirectScanResidualSet);
            ds = new DirectScanFilteredMatch(indexSearcher, drivingMatch, ctx.Exec, take: take, precompiledDelegate: ctx.Plan.DirectScanResidualSet.Compiled);
        }
        else
        {   // Nothing to filter, just match.
            ds = new DirectScanSimpleMatch(indexSearcher, drivingMatch, take: take)
            {
                KnownExactTotal = knownTotal, KnownTotalProbeTicks = probeTicks, KnownTotalProbeTerms = probeTerms
            };
        }

        if (ctx.WantTimings)
        {
            PopulateDirectScanInspection(ds, sortFieldName, drivingClauseDescription, forward, ctx.Exec.Plan.DirectScanResidualSet?.Predicates,
                isFullScan ? "full index-only scan (no WHERE clause)" : reasonForInspection);
        }
        return ds;
        
        static (IQueryMatch, string) ResolveDrivingProvider(ref InstantiateContext ctx, ResolutionContext walkerCtx, ClauseExecution drivingExec, bool forward)
        {
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
                    ResolveSentinelRewrittenBetween(drivingExec, fieldMeta, indexSearcher, queryExec, forward),
                ClauseType.Between => packed.BetweenQuery(fieldMeta, indexSearcher, queryExec, forward),
                _ => ResolveClause(drivingExec, queryExec, walkerCtx) // fallback
            };
        }

        static (IQueryMatch, string) ResolveFullScanDrivingProvider(ref InstantiateContext ctx, bool forward)
        {
            var indexSearcher = ctx.PlanParams.IndexSearcher;
            var fieldMeta = ctx.OrderByFields[0].Field;
            var sortFieldType = ctx.OrderByFields[0].FieldType;
            var match = sortFieldType switch
            {
                MatchCompareFieldType.Integer => indexSearcher.BetweenQuery(fieldMeta, long.MinValue, long.MaxValue, forward: forward),
                MatchCompareFieldType.Floating => indexSearcher.BetweenQuery(fieldMeta, double.MinValue, double.MaxValue, forward: forward),
                _ => indexSearcher.ExistsQueryForSortedScan(fieldMeta, forward: forward)
            };
            return (match, ctx.WantTimings ? $"{fieldMeta.FieldName} [all]" : null);
        }
        
        // For a no-residual DirectScanSimpleMatch the exact TotalResults is the driving provider's posting
        // count, readable without draining Fill. Returns -1 (fall back to the drain) unless every condition
        // that keeps "postings == documents" holds.
        static long TryResolveDirectScanKnownTotal(ref InstantiateContext ctx, ResolutionContext walkerCtx, ClauseExecution drivingClause, bool isFullScan, bool forward,
            out long probeTicks, out int probeTerms)
        {
            // -1 ticks marks the O(1) resolutions (NumberOfEntries below, or no resolution at all); only the
            // CountPostingsInRange header walk records a real duration + term count.
            probeTicks = -1;
            probeTerms = 0;

            // The total is only worth resolving when the read consumes it (count / statistics) and no server-side
            // filter would make the header count overcount the survivors.
            if (CanResolveKnownTotal(ctx.BuilderParams) == false)
                return -1;

            // A full index-only scan (no WHERE) emits every document once, so the exact total is the index entry
            // count. Summing the driving exists provider's postings would undercount by the null/non-existing
            // groups (it is built skipNulls), so read the O(1) entry count directly — also correct for multi-valued
            // fields and matching the compiled-plan IsAllEntries known-total.
            if (isFullScan)
                return ctx.PlanParams.IndexSearcher.NumberOfEntries;

            // Multi-valued fields place a document under several terms; DirectScanSimpleMatch dedups those via
            // EmittedBitmap, so the summed posting count would overcount documents. Single-valued fields only.
            if (ctx.PlanParams.IndexSearcher.HasMultipleTermsInField(ctx.OrderByFields[0].Field))
                return -1;

            // CountPostingsInRange advances (and exhausts) a provider's iterator, so it must run on a throwaway
            // provider resolved with the same bounds — never the one feeding the SortedDrivingMatch above, which
            // would leave that scan with nothing to read. The range / numeric driving providers tally their
            // postings from posting-list headers alone (no id decode); the multi-valued overcount is excluded
            // above, so the summed posting count is the exact document total.
            var (countMatch, _) = ResolveDrivingProvider(ref ctx, walkerCtx, drivingClause, forward);
            return TryCountPostingsInRange(countMatch, out probeTicks, out probeTerms);
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
}
