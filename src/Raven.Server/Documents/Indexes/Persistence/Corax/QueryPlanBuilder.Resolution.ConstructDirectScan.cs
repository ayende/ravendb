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
    private static IQueryMatch ConstructDirectScan(ref InstCtx ctx, ResolutionContext walkerCtx,
        int drivingIdx, bool isFullScan, bool hasTieBreak, string reasonForInspection)
    {
        var indexSearcher = ctx.PlanParams.IndexSearcher;
        string sortFieldName = ctx.WantTimings ? ctx.OrderByFields[0].Field.FieldName.ToString() : null;
        bool forward = ctx.OrderByFields[0].Ascending;

        if (ctx.Exec.Plan.DirectScanResidualSet is null)
            return null;

        var (drivingMatchProvider, drivingClauseDescription) = isFullScan ?
            ResolveFullScanDrivingProvider(ref ctx, forward) : 
            ResolveDrivingProvider(ref ctx, walkerCtx, drivingIdx, forward);
        
        if (drivingMatchProvider is not TermsProviderMatch tpm)
            return null; // can happen if we have no entries for this field

        bool nullFirst = ResolveNullFirst(ctx.OrderByFields[0], ctx.BuilderParams.Index.Configuration.NullsSortMode, forward);
        IQueryMatch drivingMatch = hasTieBreak
            ? BuildSortedDrivingWithTieBreakMatch(ctx, tpm.Provider, tpm.Llt, ctx.BuilderParams.Index.Configuration.NullsSortMode, indexSearcher, nullFirst)
            : new SortedDrivingMatch(tpm.Provider, tpm.Llt, ctx.PlanParams.Allocator, indexSearcher, ctx.OrderByFields[0].Field, nullFirst);

        DirectScanMatchBase ds;
        if (ctx.Exec.Plan.DirectScanResidualSet is { HasPredicates: true })
        {
            // Filter every clause EXCEPT the sort-driving clause (walked by the tree).
            ScanParamExtractor.Extract(ctx.Exec, indexSearcher, walkerCtx, ctx.Exec.Plan.DirectScanResidualSet);
            ds = new DirectScanFilteredMatch(indexSearcher, drivingMatch, ctx.Exec, take: Constants.IndexSearcher.TakeAll, precompiledDelegate: ctx.Plan.DirectScanResidualSet.Compiled);
        }
        else
        {   // Nothing to filter, just match...
            ds = new DirectScanSimpleMatch(indexSearcher, drivingMatch, take: Constants.IndexSearcher.TakeAll);
        }

        if (ctx.WantTimings)
        {
            PopulateDirectScanInspection(ds, sortFieldName, drivingClauseDescription, forward, ctx.Exec.Plan.DirectScanResidualSet?.Predicates,
                isFullScan ? "full index-only scan (no WHERE clause)" : reasonForInspection);
        }
        return ds;
        
        static (IQueryMatch, string) ResolveDrivingProvider(ref InstCtx ctx, ResolutionContext walkerCtx, int drivingIdx, bool forward)
        {
            var drivingExec = ctx.Exec.Executions[drivingIdx];
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
                    ResolveSentinelRewrittenBetween(drivingExec, fieldMeta, indexSearcher, queryExec),
                ClauseType.Between => packed.BetweenQuery(fieldMeta, indexSearcher, queryExec, forward),
                _ => ResolveClause(drivingExec, queryExec, walkerCtx) // fallback
            };
        }

        static (IQueryMatch, string) ResolveFullScanDrivingProvider(ref InstCtx ctx, bool forward)
        {
            var indexSearcher = ctx.PlanParams.IndexSearcher;
            var fieldMeta = ctx.OrderByFields[0].Field;
            var sortFieldType = ctx.OrderByFields[0].FieldType;
            var match = sortFieldType switch
            {
                MatchCompareFieldType.Integer => indexSearcher.BetweenQuery(fieldMeta, long.MinValue, long.MaxValue, forward: forward),
                MatchCompareFieldType.Floating => indexSearcher.BetweenQuery(fieldMeta, double.MinValue, double.MaxValue, forward: forward),
                _ => indexSearcher.ExistsQuery(fieldMeta, forward: forward)
            };
            return (match, ctx.WantTimings ? $"{fieldMeta.FieldName} [all]" : null);
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
