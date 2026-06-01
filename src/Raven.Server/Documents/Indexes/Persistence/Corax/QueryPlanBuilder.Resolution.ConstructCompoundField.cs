using System;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using Corax.Mappings;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.SortingMatches.Meta;
using Corax.Querying.Planning;
using Corax.Querying.Primitives;
using Corax.Utils;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Server;
using Spatial4n.Shapes;
using Voron;
using Voron.Impl;
using Constants = Corax.Constants;
using RavenConstants = Raven.Client.Constants;
using IndexSearcher = Corax.Querying.IndexSearcher;
using Range = Corax.Querying.Matches.Meta.Range;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

internal static partial class QueryPlanBuilder
{
    private static IQueryMatch ConstructCompoundField(ref InstCtx ctx, ResolutionContext walkerCtx, int field2RangeIdx, long entriesToScan, long bitmapCost)
    {
        var execs = ctx.Exec.Executions;
        var indexSearcher = ctx.PlanParams.IndexSearcher;
        int drivingClauseIdx = ctx.Exec.Plan.CompoundField.DrivingClause;

        var packed = execs[drivingClauseIdx].PackedParamValue;

        if (ctx.Exec.Plan.CompoundFieldResidualSet is null)
            return null;

        string field1Name = execs[drivingClauseIdx].Clause.FieldName;
        string compoundFieldName = ctx.Exec.Plan.Template.CompoundFieldName;
        var compoundFieldMeta = indexSearcher.FieldMetadataBuilder(compoundFieldName, hasBoost: false);

        // Build the prefix bytes for field1's value.
        Slice analyzedPrefix = BuildField1Prefix(ref ctx, field1Name, packed, out string field1ValueStr);
        if (analyzedPrefix.HasValue == false || analyzedPrefix.Size > byte.MaxValue) // if too long, cannot be used for compound
            return null; // fall back to bitmap

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
            if (field2RangeIdx >= 0 && 
                TryBuildCompositeRangeKeys(ref context, analyzedPrefix, fieldName, execs[field2RangeIdx], out var lowSlice, out var highSlice))
            {
                return indexSearcher.RangeBuilder<Range.Inclusive, Range.Inclusive>(
                    compoundFieldMeta, lowSlice, highSlice,
                    forward: context.OrderByFields[0].Ascending);
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
            directScan.SeekBound = $"'{field1ValueStr}' (prefix, validatePostfixLen)";
            directScan.Direction = context.OrderByFields[0].Ascending ? "Forward" : "Backward";
            directScan.ResidualDescription = context.Exec.Plan.CompoundFieldResidualSet?.Predicates is { } cfr ? string.Join(", ", Array.ConvertAll(cfr, p => $"{p.FieldName} {p.CompareOp}")) : null;
            directScan.Reason = $"entries_to_scan({entriesToScan}) × {QueryPrimitives.EntryScanCostMultiplier} < bitmap_cost({bitmapCost})";
        }
    }
}
