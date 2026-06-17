using System;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Planning;
using Corax.Querying.Primitives;
using Voron;
using Constants = Corax.Constants;
using Range = Corax.Querying.Matches.Meta.Range;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

internal static partial class QueryPlanBuilder
{
    private static IQueryMatch ConstructCompoundField(ref InstCtx ctx, ResolutionContext walkerCtx, ClauseExecution field2Range, long entriesToScan, long bitmapCost, bool canElideCompoundSort)
    {
        var indexSearcher = ctx.PlanParams.IndexSearcher;
        var drivingClause = ctx.Exec.CompoundFieldDrivingClause;

        var packed = drivingClause.PackedParamValue;

        if (ctx.Exec.Plan.CompoundFieldResidualSet is null)
            return null;

        string field1Name = drivingClause.Clause.FieldName;
        string compoundFieldName = ctx.Exec.Plan.Template.CompoundFieldName;
        var compoundFieldMeta = indexSearcher.FieldMetadataBuilder(compoundFieldName, hasBoost: false);

        // Build the prefix bytes for field1's value.
        Slice analyzedPrefix = BuildField1Prefix(ref ctx, field1Name, packed, out string field1ValueStr);
        if (analyzedPrefix.HasValue == false || analyzedPrefix.Size > byte.MaxValue) // if too long, cannot be used for compound
            return null; // fall back to bitmap

        bool forward = ctx.OrderByFields[0].Ascending;

        // Records which seek CreateDrivingMatch actually built, so the introspection below reports the real bound
        // (a field2 composite range vs. the field1-only prefix fallback) instead of a hard-coded prefix string.
        // The composite slices are captured so the known-total probe can rebuild a throwaway count provider.
        bool usedCompositeRange = false;
        Slice compositeLow = default, compositeHigh = default;
        IQueryMatch drivingMatch = CreateDrivingMatch(ref ctx);

        bool hasResidual = ctx.Exec.Plan.CompoundFieldResidualSet is { HasPredicates: true };

        // Page-bound + exact-total parity with the single-field DirectScan: when the sort wrapper is elided and the
        // driving match is the EXACT composite range (field1 pinned, field2 bounded - e.g. the seek ('en', 60)), the
        // emitted set is exactly that range's postings. We can read the first page and derive TotalResults from the
        // range's posting headers (CountPostingsInRange) instead of draining the whole scan. A residual filter rejects
        // candidates after the fact, so its surviving count needs the drain (knownTotal = -1).
        long knownProbeTicks = -1; // Stopwatch ticks the CountPostingsInRange header walk took (-1 = no probe ran).
        int knownProbeTerms = 0;
        long knownTotal = hasResidual ? -1 : TryResolveCompoundKnownTotal(ref ctx);

        // When knownTotal resolves the scan is page-bounded even under statistics (the drain is no longer the count
        // source); otherwise ResolveSortedScanTake forces TakeAll so the drain can report the total. A multi-sort /
        // forced-sort plan keeps the SortingMatch wrapper (which drains regardless), so TakeAll there.
        int take = canElideCompoundSort
            ? (knownTotal >= 0 ? ctx.BuilderParams.Take : ResolveSortedScanTake(ctx.BuilderParams))
            : Constants.IndexSearcher.TakeAll;

        DirectScanMatchBase directScan;
        if (hasResidual)
        {
            // Filter every clause EXCEPT {driving, field2Range} (both enforced by the compound key).
            ScanParamExtractor.Extract(ctx.Exec, indexSearcher, walkerCtx, ctx.Exec.Plan.CompoundFieldResidualSet);
            directScan = new DirectScanFilteredMatch(indexSearcher, drivingMatch, ctx.Exec, take: take, precompiledDelegate: ctx.Plan.CompoundFieldResidualSet.Compiled);
        }
        else
        {   // nothing to filter, just scan...
            directScan = new DirectScanSimpleMatch(indexSearcher, drivingMatch, take: take)
            {
                KnownExactTotal = knownTotal, KnownTotalProbeTicks = knownProbeTicks, KnownTotalProbeTerms = knownProbeTerms
            };
        }

        if (ctx.WantTimings) // only used when we use include timings()
            SetDirectScanPropertiesForIntrospection(ref ctx);

        return directScan;

        IQueryMatch CreateDrivingMatch(ref InstCtx context)
        {
            string fieldName = context.Exec.Plan.Template.CompoundFieldSortName;
            if (field2Range is not null &&
                TryBuildCompositeRangeKeys(ref context, analyzedPrefix, fieldName, field2Range, out var lowSlice, out var highSlice))
            {
                usedCompositeRange = true;
                compositeLow = lowSlice;
                compositeHigh = highSlice;
                return BuildCompositeRangeMatch(lowSlice, highSlice);
            }

            // No field2 narrowing available: run a prefix scan on field1 only and let entry-scan residuals filter the rest.
            return indexSearcher.StartWithQuery(compoundFieldMeta, analyzedPrefix,
                isNegated: false, forward: forward,
                validatePostfixLen: true);
        }

        // Builds the compound-field composite range over [low, high]. The composite key's field2 suffix encodes the
        // bound value exactly, so the clause's strict/inclusive semantics map straight onto the range markers:
        // a strict bound (>, <) must exclude the term equal to the boundary, otherwise the boundary value's documents
        // leak into the result (e.g. Age > 18 wrongly keeping Age = 18). The open side (filled 0x00/0xFF) has no
        // matching term, so its marker is irrelevant. Each call builds a fresh provider, so the count probe below can
        // run on its own throwaway instance without disturbing the scan's iterator.
        IQueryMatch BuildCompositeRangeMatch(Slice low, Slice high)
        {
            return field2Range.Clause.ClauseType switch
            {
                ClauseType.GreaterThan => indexSearcher.RangeBuilder<Range.Exclusive, Range.Inclusive>(compoundFieldMeta, low, high, forward),
                ClauseType.LessThan => indexSearcher.RangeBuilder<Range.Inclusive, Range.Exclusive>(compoundFieldMeta, low, high, forward),
                _ => indexSearcher.RangeBuilder<Range.Inclusive, Range.Inclusive>(compoundFieldMeta, low, high, forward)
            };
        }

        // For a no-residual elided scan over the exact composite range, the emitted set is exactly that range's
        // postings, so TotalResults can be read from posting-list headers (header-only walk) instead of draining.
        // Returns -1 (drain) unless every precondition that keeps "postings == documents" holds.
        long TryResolveCompoundKnownTotal(ref InstCtx context)
        {
            // Only the elided single-order path consults KnownExactTotal (a SortingMatch wrapper would drain anyway),
            // and only the exact composite range is countable: the prefix fallback rejects longer field1 matches via
            // validatePostfixLen during the walk, but a header-only count would include them and overcount.
            if (canElideCompoundSort == false || usedCompositeRange == false)
                return -1;

            var bp = context.BuilderParams;

            // The total is only consumed when the read operation reports statistics or answers a count query;
            // otherwise the scan is already page-bounded and never drained, so computing it would be wasted work.
            if (bp?.Query is not ({ IsCountQuery: true } or { SkipStatistics: false }))
                return -1;

            // A server-side filter rejects index candidates after the fact, so the posting count would overcount.
            if (bp.Metadata?.Query?.Filter != null)
                return -1;

            // A multi-valued compound field places a document under several compound terms; DirectScanSimpleMatch
            // dedups via EmittedBitmap, so the summed posting count would overcount documents. Single-valued only.
            if (indexSearcher.HasMultipleTermsInField(compoundFieldMeta))
                return -1;

            // CountPostingsInRange advances (and exhausts) the provider's iterator, so it runs on a throwaway provider
            // built with the same bounds - never drivingMatch, which still has to feed the scan.
            var countMatch = BuildCompositeRangeMatch(compositeLow, compositeHigh);
            try
            {
                if (countMatch is TermsProviderMatch countTpm && countTpm.Provider is IAggregationProvider agg)
                {
                    long t0 = System.Diagnostics.Stopwatch.GetTimestamp();
                    var stats = agg.CountPostingsInRange(0);
                    knownProbeTicks = System.Diagnostics.Stopwatch.GetTimestamp() - t0;
                    knownProbeTerms = stats.Terms;
                    return stats.Postings;
                }
                return -1;
            }
            finally
            {
                (countMatch as IDisposable)?.Dispose();
            }
        }

        void SetDirectScanPropertiesForIntrospection(ref InstCtx context)
        {
            directScan.DrivingTreeName = compoundFieldName;
            directScan.DrivingClause = $"{field1Name} = '{field1ValueStr}'";
            directScan.SeekBound = usedCompositeRange
                ? $"'{field1ValueStr}' + {field2Range.Clause.FieldName} {field2Range.Clause.ClauseType} (composite range)"
                : $"'{field1ValueStr}' (prefix, validatePostfixLen)";
            directScan.Direction = context.OrderByFields[0].Ascending ? "Forward" : "Backward";
            directScan.ResidualDescription = context.Exec.Plan.CompoundFieldResidualSet?.Predicates is { } cfr ? string.Join(", ", Array.ConvertAll(cfr, p => $"{p.FieldName} {p.CompareOp}")) : null;
            directScan.Reason = $"entries_to_scan({entriesToScan}) × {QueryPrimitives.EntryScanCostMultiplier} < bitmap_cost({bitmapCost})";
        }
    }
}
