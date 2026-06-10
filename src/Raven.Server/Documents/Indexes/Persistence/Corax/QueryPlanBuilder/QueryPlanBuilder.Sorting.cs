using System;
using System.Runtime.CompilerServices;
using Corax.Mappings;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.SortingMatches.Meta;
using Corax.Querying.Planning;
using Corax.Utils;
using Raven.Client.Exceptions.Corax;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Sparrow;
using Spatial4n.Shapes;
using Constants = Corax.Constants;
using SpatialUnits = Raven.Client.Documents.Indexes.Spatial.SpatialUnits;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

internal static partial class QueryPlanBuilder
{
    /// <summary>Maximum number of ORDER BY fields supported by Corax.</summary>
    private const int MaxSortFields = 16;

    /// <summary>At template time, we detect that we can optimize using sort hint. At query time, we just need to read the runtime value.</summary>
    private static void TrySetSortSeekHint(CompiledPlan plan, QueryExecution exec, CompiledQueryMatch match)
    {
        var sortExec = exec.SortSeekClause;
        if (sortExec is null)
            return;

        if (sortExec.PackedParamValue.IsNone)
            return;

        int paramIdx = plan.Template.SortSeekUseParam2 ? sortExec.PackedParamValue.Param2 : sortExec.PackedParamValue.Param1;
        object seekValue = sortExec.PackedParamValue.ValueType switch
        {
            PackedParam.TypeLong => exec.LongValues[paramIdx],
            PackedParam.TypeDouble => exec.DoubleValues[paramIdx],
            PackedParam.TypeString => exec.StringValues[paramIdx],
            _ => null
        };

        if (seekValue != null)
            match.SortHint = new SortHint(sortExec.Clause.FieldName, seekValue);
    }

    /// <summary>
    /// True when a single vector-search post-filter is the sole scoring source AND the only ordering the query
    /// needs is similarity-score order — which the vector's HNSW output already streams. In that case the
    /// implicit (or explicit <c>ORDER BY score()</c>) SortingMatch wrapper is redundant: the match can stream
    /// its score-ordered results directly and the wrapper is skipped. Covers both the bare
    /// <c>WHERE vector.search(...)</c> case and the AND-filtered <c>WHERE x = $v AND vector.search(...)</c> case
    /// (both lift the vector to a top-level post-filter, so it appears in <see cref="QueryExecution.VectorSelects"/>;
    /// OR-branch vectors are leaves, never post-filters, so they never qualify and keep their sort).
    /// </summary>
    private static bool VectorPostFilterProvidesResultOrder(QueryExecution exec, QueryBuilderParameters bp, OrderMetadata[] orderByFields)
    {
        // Exactly one vector post-filter — with two, the outermost vector's order is not a single native order.
        if (exec.VectorSelects is not { Length: 1 })
            return false;

        // The vector distance must be the SOLE scoring contributor. Any index-static boost or explicit boost()
        // in the query composes with the distance, so the final order is the SUM and a real sort is required.
        if (bp.Index is not { HasBoostedFields: false })
            return false;
        if (bp.Metadata.HasBoost)
            return false;

        // Per-document boosting (a stored boost tree) multiplies the similarity, so the final order is no longer
        // the vector's native distance order and the wrapper's BoostDocuments pass is required. Keep the sort.
        if (bp.IndexSearcher.DocumentsAreBoosted)
            return false;

        // The resolved sort is exactly a single score() sort — whether requested explicitly (ORDER BY score())
        // or auto-promoted at template time (the ImplicitScore branch of BuildSortMetadataTemplate, gated on
        // HasBoost + config opt-in). Either way it is precisely the order the vector's HNSW output already
        // streams, so the SortingMatch wrapper is redundant. We read the already-materialized sort shape here
        // instead of re-deriving the promotion gate.
        return orderByFields is { Length: 1 } && orderByFields[0].FieldType == MatchCompareFieldType.Score;
    }

    private static SortMetadataTemplate BuildSortMetadataTemplate(PlanParameters p)
    {
        var orderByFields = p.Metadata.OrderBy;

        if (orderByFields is null)
        {
            // Auto-promote to ORDER BY score() when boosting is involved and the index/config opt in.
            // Index configuration is template-stable; HasBoost is template-stable per PlanParameters.
            if (p.HasBoost && p.Index is { } indexForScore &&
                (indexForScore.Configuration.OrderByScoreAutomaticallyWhenBoostingIsInvolved || indexForScore.Configuration.CoraxVectorSearchOrderByScoreAutomatically))
            {
                return new SortMetadataTemplate
                {
                    ImplicitScore = true,
                    HasVectorSearch = p.Metadata.HasVectorSearch,
                    Prebuilt = [new OrderMetadata(true, MatchCompareFieldType.Score)],
                };
            }

            return new SortMetadataTemplate { NoSort = true };
        }

        if (orderByFields.Length == 0)
            return new SortMetadataTemplate { NoSort = true };

        if (orderByFields.Length > MaxSortFields)
            throw new InvalidOperationException($"Corax does not support ordering by more than {MaxSortFields} properties.");

        var prebuilt = new OrderMetadata[orderByFields.Length];
        var patches = new SortSlotPatch[orderByFields.Length];
        bool anyPatch = false;

        for (int i = 0; i < orderByFields.Length; i++)
        {
            var field = orderByFields[i];

            var orderingType = field.OrderingType;
            switch (field.OrderingType)
            {
                case OrderByFieldType.Random:
                    if (field.Arguments is { Length: > 0 }) // we have a seed to use
                    {
                        var seedArg = field.Arguments[0];
                        if (seedArg.Type == ValueTokenType.Parameter)
                        {
                            // The seed comes from a query parameter value, which is not known at template time
                            // and differs per execution, so it cannot be baked. Resolve it in MaterializeSortMetadata.
                            prebuilt[i] = new OrderMetadata(0);
                            patches[i].Kind = SortSlotPatchKind.RandomSeededByParam;
                            patches[i].FieldName = seedArg.NameOrValue; // the parameter name
                            anyPatch = true;
                        }
                        else
                        {
                            var seed = (int)Hashing.XXHash32.CalculateRaw(seedArg.NameOrValue);
                            prebuilt[i] = new OrderMetadata(seed);
                        }
                    }
                    else
                    {
                        prebuilt[i] = new OrderMetadata(0);
                        patches[i].Kind = SortSlotPatchKind.RandomFreshSeed;
                        anyPatch = true;
                    }
                    continue;
                case OrderByFieldType.Score:
                    prebuilt[i] = new OrderMetadata(true, MatchCompareFieldType.Score, field.Ascending);
                    continue;
                case OrderByFieldType.Distance:
                    patches[i].Kind = SortSlotPatchKind.DistanceRuntime;
                    patches[i].FieldName = field.Name;
                    patches[i].DistanceBuilder = GetDistanceBuilder(field);
                    anyPatch = true;
                    continue;
                case OrderByFieldType.Implicit when p.Index.Configuration.OrderByTicksAutomaticallyWhenDatesAreInvolved && p.Index.IndexFieldsPersistence.HasTimeValues(field.Name.Value):
                    orderingType = OrderByFieldType.Long;
                    break;
            }
            
            var fieldMetadata = QueryBuilderHelper.GetFieldIdForOrderBy(p.Allocator, field.Name, p.Index,
                p.HasDynamics, p.DynamicFields, p.IndexFieldsMapping, false);

            prebuilt[i] = new OrderMetadata(fieldMetadata, field.Ascending, 
                GetMatchCompareFieldType(orderingType), GetNullsSortMode(field), 
                mayHaveMissingEntries: fieldMetadata.FieldId == Constants.IndexWriter.DynamicField);
            patches[i].Kind = SortSlotPatchKind.FieldRuntimeResolve;
            patches[i].FieldName = field.Name;
            anyPatch = true;
        }

        return new SortMetadataTemplate
        {
            Prebuilt = prebuilt,
            Patches = anyPatch ? patches : null,
        };
        
        SortDistanceMetadataBuilder GetDistanceBuilder(OrderByField field) => // side effect for this method, we aren't accidently capturing the wrong instance in a loop
            (ctx, fieldMeta) => BuildDistanceOrderMetadata((QueryBuilderParameters)ctx, field, fieldMeta);

        MatchCompareFieldType GetMatchCompareFieldType(OrderByFieldType orderingType)
        {
            var compareType = orderingType switch
            {
                OrderByFieldType.Custom => throw new NotSupportedInCoraxException($"{nameof(Corax)} doesn't support Custom OrderBy."),
                OrderByFieldType.AlphaNumeric => MatchCompareFieldType.Alphanumeric,
                OrderByFieldType.Long => MatchCompareFieldType.Integer,
                OrderByFieldType.Double => MatchCompareFieldType.Floating,
                _ => MatchCompareFieldType.Sequence,
            };
            return compareType;
        }
    }
    
    private static NullsSortMode? GetNullsSortMode(OrderByField field)
    {
        var nullsSortMode = (field.NullsOrdering, field.Ascending) switch
        {
            (NullsOrderingType.First, Ascending: true) => NullsSortMode.NullsSmallest,
            (NullsOrderingType.First, Ascending: false) => NullsSortMode.NullsLargest,
            (NullsOrderingType.Last, Ascending: true) => NullsSortMode.NullsLargest,
            (NullsOrderingType.Last, Ascending: false) => NullsSortMode.NullsSmallest,
            _ => (NullsSortMode?)null
        };
        return nullsSortMode;
    }


    private static OrderMetadata[] GetSortMetadata(QueryBuilderParameters builderParameters, PlanTemplate planTemplate)
    {
        // PageSize == 0 (count-only) is per-query and short-circuits all the sort work.
        if (builderParameters.Query.PageSize == 0)
            return null;

        return MaterializeSortMetadata(planTemplate.SortMetadataTemplate, builderParameters);
    }

    /// <summary>Runtime materializer: apply per-query patches to the template's prebuilt array.</summary>
    private static OrderMetadata[] MaterializeSortMetadata(SortMetadataTemplate template,
        QueryBuilderParameters builderParameters)
    {
        if (template.NoSort)
            return null;

        if (template.ImplicitScore)
        {
            builderParameters.IndexReadOperation.AssertCanOrderByScoreAutomaticallyWhenBoostingOrVectorSearchIsInvolved(template.HasVectorSearch);
            return template.Prebuilt;
        }

        // Hot path: nothing to patch — return the prebuilt array directly.
        if (template.Patches is null)
            return template.Prebuilt;

        var indexSearcher = builderParameters.IndexSearcher;

        var result = new OrderMetadata[template.Prebuilt.Length];

        for (int i = 0; i < template.Prebuilt.Length; i++)
        {
            ref var patch = ref template.Patches[i];
            switch (patch.Kind)
            {
                case SortSlotPatchKind.None:
                    result[i] = template.Prebuilt[i];
                    break;

                case SortSlotPatchKind.RandomFreshSeed:
                    result[i] = new OrderMetadata(Random.Shared.Next());
                    break;

                case SortSlotPatchKind.RandomSeededByParam:
                {
                    builderParameters.Query.QueryParameters.TryGet(patch.FieldName, out string seedValue);
                    var seed = (int)Hashing.XXHash32.CalculateRaw(seedValue ?? string.Empty);
                    result[i] = new OrderMetadata(seed);
                    break;
                }

                case SortSlotPatchKind.FieldRuntimeResolve:
                {
                    // FieldMetadata holds transaction-bound slices, so re-resolve it per query
                    var fieldMeta = ResolveSortFieldMeta(builderParameters, patch.FieldName);
                    var p = template.Prebuilt[i];
                    // The field "may have missing entries" when some documents carry no value for it.
                    // GetDistinctTermCountInField == 0 catches a fully-empty field; HasAnyNonExistingEntries
                    // catches the partial case (some docs have a value, some are in the non-existing posting list).
                    // Either way the sort must surface those docs null-adjacent, which routes the single-field
                    // full-scan away from the streaming DirectScan and through SortingMatch (InMemorySort).
                    bool mayHaveMissingEntries = p.MayHaveMissingEntries
                        || indexSearcher.GetDistinctTermCountInField(fieldMeta) == 0
                        || indexSearcher.HasAnyNonExistingEntries(fieldMeta);
                    result[i] = new OrderMetadata(fieldMeta, p.Ascending, p.FieldType, p.NullsSortMode, mayHaveMissingEntries);
                    break;
                }

                case SortSlotPatchKind.DistanceRuntime:
                {
                    // Spatial sort has no term tree to iterate, so it always materializes the whole bitmap and
                    // tolerates docs (or an empty field) that carry no spatial value — no emptiness check needed.
                    var fieldMeta = ResolveSortFieldMeta(builderParameters, patch.FieldName);
                    result[i] = patch.DistanceBuilder(builderParameters, fieldMeta);
                    break;
                }
            }
        }

        return result;
    }

    private static FieldMetadata ResolveSortFieldMeta(QueryBuilderParameters builderParameters, string fieldName)
    {
        // FieldMetadata hold slices, which are tied to the current transaction, so have to do this per query 
        return QueryBuilderHelper.GetFieldIdForOrderBy(builderParameters.Allocator, fieldName, builderParameters.Index,
            builderParameters.HasDynamics, builderParameters.DynamicFields, builderParameters.IndexFieldsMapping, false);
    }

    private static OrderMetadata BuildDistanceOrderMetadata(QueryBuilderParameters builderParameters,
        OrderByField field, FieldMetadata fieldMetadata)
    {
        var query = builderParameters.Query;
        var getSpatialField = builderParameters.Factories.GetSpatialFieldFactory;
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
                throw new ArgumentOutOfRangeException(field.Method.ToString());
        }

        var roundTo = field.Arguments.Length > lastArgument
            ? field.Arguments[lastArgument].GetDouble(query.QueryParameters)
            : 0D;

        return new OrderMetadata(fieldMetadata, field.Ascending, MatchCompareFieldType.Spatial, point, roundTo,
            spatialField.Units is SpatialUnits.Kilometers
                ? global::Corax.Utils.Spatial.SpatialUnits.Kilometers
                : global::Corax.Utils.Spatial.SpatialUnits.Miles, GetNullsSortMode(field));
    }

    private static IQueryMatch OrderBy(QueryBuilderParameters builderParameters, IQueryMatch match, in OrderMetadata[] orderMetadata)
    {
        RuntimeHelpers.EnsureSufficientExecutionStack();
        var indexSearcher = builderParameters.IndexSearcher;
        var take = builderParameters.Take;

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


    private static FieldMetadata ReplaceAnalyzerForWildcardQueries(FieldMetadata searchMeta, ResolutionContext walkerCtx)
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
}
