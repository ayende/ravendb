using System;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Planning;
using Corax.Querying.Primitives;
using Voron;
using Range = Corax.Querying.Matches.Meta.Range;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

internal static partial class QueryPlanBuilder
{
    private static IQueryMatch ConstructCompoundField(ref InstCtx ctx, ResolutionContext walkerCtx, ClauseExecution field2Range, long entriesToScan, long bitmapCost)
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

        // Records which seek CreateDrivingMatch actually built, so the introspection below reports the real bound
        // (a field2 composite range vs. the field1-only prefix fallback) instead of a hard-coded prefix string.
        bool usedCompositeRange = false;
        IQueryMatch drivingMatch = CreateDrivingMatch(ref ctx);
        DirectScanMatchBase directScan;
        if (ctx.Exec.Plan.CompoundFieldResidualSet is { HasPredicates: true })
        {
            // Filter every clause EXCEPT {driving, field2Range} (both enforced by the compound key).
            ScanParamExtractor.Extract(ctx.Exec, indexSearcher, walkerCtx, ctx.Exec.Plan.CompoundFieldResidualSet);
            directScan = new DirectScanFilteredMatch(indexSearcher, drivingMatch, ctx.Exec, take: -1, precompiledDelegate: ctx.Plan.CompoundFieldResidualSet.Compiled);
        }
        else
        {   // nothing to filter, just scan...
            directScan = new DirectScanSimpleMatch(indexSearcher, drivingMatch, take: -1);
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
                bool forward = context.OrderByFields[0].Ascending;
                usedCompositeRange = true;
                // The composite key's field2 suffix encodes the bound value exactly, so the clause's
                // strict/inclusive semantics map straight onto the range markers on the compound key:
                // a strict bound (>, <) must exclude the term equal to the boundary, otherwise the
                // boundary value's documents leak into the result (e.g. Age > 18 wrongly keeping Age = 18).
                // The open side (filled 0x00/0xFF) has no matching term, so its marker is irrelevant.
                return field2Range.Clause.ClauseType switch
                {
                    ClauseType.GreaterThan => indexSearcher.RangeBuilder<Range.Exclusive, Range.Inclusive>(compoundFieldMeta, lowSlice, highSlice, forward),
                    ClauseType.LessThan => indexSearcher.RangeBuilder<Range.Inclusive, Range.Exclusive>(compoundFieldMeta, lowSlice, highSlice, forward),
                    _ => indexSearcher.RangeBuilder<Range.Inclusive, Range.Inclusive>(compoundFieldMeta, lowSlice, highSlice, forward)
                };
            }

            // No field2 narrowing available: run a prefix scan on field1 only and let entry-scan residuals filter the rest.
            return indexSearcher.StartWithQuery(compoundFieldMeta, analyzedPrefix,
                isNegated: false, forward: context.OrderByFields[0].Ascending,
                validatePostfixLen: true);
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
