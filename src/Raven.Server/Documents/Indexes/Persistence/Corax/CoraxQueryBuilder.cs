using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Corax;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.SortingMatches.Meta;
using Corax.Utils;
using Raven.Client.Exceptions.Corax;
using Raven.Server.Documents.Indexes.Persistence.Lucene;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Json;
using Spatial4n.Shapes;
using RavenConstants = Raven.Client.Constants;
using IndexSearcher = Corax.Querying.IndexSearcher;
using CoraxConstants = Corax.Constants;
using SpatialUnits = Raven.Client.Documents.Indexes.Spatial.SpatialUnits;
using MoreLikeThisQuery = Raven.Server.Documents.Queries.MoreLikeThis.Corax;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

public static partial class CoraxQueryBuilder
{
    internal const int TakeAll = -1;

    public static MoreLikeThisQuery.MoreLikeThisQuery BuildMoreLikeThisQuery(Parameters builderParameters, QueryExpression whereExpression)
    {
        using (CultureHelper.EnsureInvariantCulture())
        {
            var indexSearcher = builderParameters.IndexSearcher;
            var metadata = builderParameters.Metadata;
            var queryParameters = builderParameters.QueryParameters;
            var context = builderParameters.DocumentsContext;

            var moreLikeThisExpression = QueryBuilderHelper.FindMoreLikeThisExpression(whereExpression);
            if (moreLikeThisExpression == null)
                throw new InvalidOperationException("Query does not contain MoreLikeThis method expression");

            BlittableJsonReaderObject options = null;
            if (moreLikeThisExpression.Arguments.Count == 2)
            {
                var value = QueryBuilderHelper.GetValue(metadata.Query, metadata, queryParameters, moreLikeThisExpression.Arguments[1], allowObjectsInParameters: true);
                if (value.Type == ValueTokenType.String)
                    options = IndexOperationBase.ParseJsonStringIntoBlittable(QueryBuilderHelper.GetValueAsString(value.Value), context);
                else
                    options = value.Value as BlittableJsonReaderObject;
            }

            string baseDocument = null;
            IQueryMatch baseDocumentQuery = null;
            var firstArgument = moreLikeThisExpression.Arguments[0];
            if (firstArgument is BinaryExpression)
            {
                // Use the bitmap path for the base document query
                baseDocumentQuery = BuildCompiledQueryMatch(builderParameters);
            }
            else
            {
                var firstArgumentValue = QueryBuilderHelper.GetValueAsString(QueryBuilderHelper.GetValue(metadata.Query, metadata, queryParameters, firstArgument).Value);
                if (bool.TryParse(firstArgumentValue, out var firstArgumentBool))
                {
                    baseDocumentQuery = firstArgumentBool
                        ? indexSearcher.AllEntries()
                        : indexSearcher.EmptyMatch();
                }
                else
                {
                    baseDocument = firstArgumentValue;
                }
            }

            // Use the bitmap path for the filter query
            var filterQuery = BuildCompiledQueryMatch(builderParameters);

            return new MoreLikeThisQuery.MoreLikeThisQuery
            {
                BaseDocument = baseDocument,
                BaseDocumentQuery = baseDocumentQuery,
                FilterQuery = filterQuery,
                Options = options
            };
        }
    }

    internal static IQueryMatch BuildCompiledQueryMatch(Parameters builderParameters)
    {
        var planParams = new QueryPlanBuilder.PlanParameters
        {
            IndexSearcher = builderParameters.IndexSearcher,
            Metadata = builderParameters.Query.Metadata,
            QueryParameters = builderParameters.QueryParameters,
            Index = builderParameters.Index,
            IndexFieldsMapping = builderParameters.IndexFieldsMapping,
            Allocator = builderParameters.Allocator,
            Token = builderParameters.Token,
            HasDynamics = builderParameters.HasDynamics,
            DynamicFields = builderParameters.DynamicFields,
            HasBoost = builderParameters.HasBoost
        };
        var plan = QueryPlanBuilder.BuildPlan(planParams);
        string queryText = builderParameters.Query.Metadata.Query.QueryText;
        var planCache = builderParameters.IndexSearcher.PlanCache;
        var compiledPlan = planCache.Get(queryText, plan.OperandOrdering);
        if (compiledPlan == null)
        {
            compiledPlan = new global::Corax.Querying.Planning.CompiledPlan
            {
                CompiledDelegate = global::Corax.Querying.Planning.QueryILEmitter.EmitDelegate(plan),
                ExplainSource = plan.ExplainSource ?? global::Corax.Querying.Planning.QueryILEmitter.GenerateExplainSource(plan),
                Ordering = plan.OperandOrdering
            };
            planCache.Add(queryText, compiledPlan);
        }
        var resolvedMatches = QueryPlanBuilder.ResolveMatches(plan, builderParameters.IndexSearcher, planParams, builderParameters);
        QueryPlanBuilder.ExtractScanParameters(plan, builderParameters.IndexSearcher,
            out var longParams, out var doubleParams, out var sliceParams, out var fieldRootPages);
        return new global::Corax.Querying.Matches.CompiledQueryMatch(
            compiledPlan, resolvedMatches,
            longParams, doubleParams, sliceParams, fieldRootPages,
            builderParameters.IndexSearcher, builderParameters.Allocator, long.MaxValue, builderParameters.Token);
    }

    internal static IQueryMatch HandleSpatial(Parameters builderParameters, MethodExpression expression, MethodType spatialMethod)
    {
        var metadata = builderParameters.Metadata;
        var queryParameters = builderParameters.QueryParameters;
        var index = builderParameters.Index;
        var indexFieldsMapping = builderParameters.IndexFieldsMapping;
        var fieldsToFetch = builderParameters.FieldsToFetch;
        var allocator = builderParameters.Allocator;

        string fieldName;
        if (metadata.IsDynamic == false)
            fieldName = QueryBuilderHelper.ExtractIndexFieldName(metadata.Query, queryParameters, expression.Arguments[0], metadata);
        else
        {
            var spatialExpression = (MethodExpression)expression.Arguments[0];
            fieldName = metadata.GetSpatialFieldName(spatialExpression, builderParameters.QueryParameters);
        }

        var fieldMetadata = QueryBuilderHelper.GetFieldMetadata(allocator, fieldName, index, indexFieldsMapping, fieldsToFetch, builderParameters.HasDynamics,
            builderParameters.DynamicFields, hasBoost: builderParameters.HasBoost);
        var shapeExpression = (MethodExpression)expression.Arguments[1];

        var distanceErrorPct = RavenConstants.Documents.Indexing.Spatial.DefaultDistanceErrorPct;
        if (expression.Arguments.Count == 3)
        {
            var distanceErrorPctValue = QueryBuilderHelper.GetValue(metadata.Query, metadata, queryParameters, (ValueExpression)expression.Arguments[2]);
            QueryBuilderHelper.AssertValueIsNumber(fieldName, distanceErrorPctValue.Type);

            distanceErrorPct = Convert.ToDouble(distanceErrorPctValue.Value);
        }

        var spatialField = builderParameters.Factories.GetSpatialFieldFactory(fieldName);

        var methodName = shapeExpression.Name;
        var methodType = QueryMethod.GetMethodType(methodName.Value);

        IShape shape = null;
        switch (methodType)
        {
            case MethodType.Spatial_Circle:
                shape = QueryBuilderHelper.HandleCircle(metadata.Query, shapeExpression, metadata, queryParameters, fieldName, spatialField, out _);
                break;
            case MethodType.Spatial_Wkt:
                shape = QueryBuilderHelper.HandleWkt(builderParameters, fieldName, shapeExpression, spatialField, out _);
                break;
            default:
                QueryMethod.ThrowMethodNotSupported(methodType, metadata.QueryText, builderParameters.QueryParameters);
                break;
        }

        Debug.Assert(shape != null);

        var operation = spatialMethod switch
        {
            MethodType.Spatial_Within => global::Corax.Utils.Spatial.SpatialRelation.Within,
            MethodType.Spatial_Disjoint => global::Corax.Utils.Spatial.SpatialRelation.Disjoint,
            MethodType.Spatial_Intersects => global::Corax.Utils.Spatial.SpatialRelation.Intersects,
            MethodType.Spatial_Contains => global::Corax.Utils.Spatial.SpatialRelation.Contains,
            _ => (global::Corax.Utils.Spatial.SpatialRelation)QueryMethod.ThrowMethodNotSupported(spatialMethod, metadata.QueryText, builderParameters.QueryParameters)
        };


        //var args = new SpatialArgs(operation, shape) {DistErrPct = distanceErrorPct};

        return builderParameters.IndexSearcher.SpatialQuery(fieldMetadata, distanceErrorPct, shape, spatialField.GetContext(), operation, token: builderParameters.Token);
    }

    public static OrderMetadata[] GetSortMetadata(Parameters builderParameters, out bool hasEmpty)
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
                // in case when we've single vector clause and we exose the score, we have to go through
                // order by primitive to retrieve them; however scores are detected as natively sorted
                if (builderParameters.IsVectorSingleClause && index.Configuration.CoraxIncludeDocumentScore == false)
                    return null;

                if (builderParameters.Metadata.HasVectorSearch == false)
                    builderParameters.IndexReadOperation?.AssertCanOrderByScoreAutomaticallyWhenBoostingOrVectorSearchIsInvolved();

                return new[] { new OrderMetadata(true, MatchCompareFieldType.Score) };
            }

            return null;
        }

        int sortIndex = 0;
        var sortArray = new OrderMetadata[16];

        if (orderByFields.Length > sortArray.Length)
            throw new InvalidOperationException($"Corax does not support ordering by more than {sortArray.Length} properties.");

        foreach (var field in orderByFields)
        {


            if (field.OrderingType == OrderByFieldType.Random)
            {
                var seed = field.Arguments is { Length: > 0 } ?
                    (int)Hashing.XXHash32.CalculateRaw(field.Arguments[0].NameOrValue) :
                    Random.Shared.Next(); // use a random seed if none is provided
                sortArray[sortIndex++] = new OrderMetadata(seed);
                continue;
            }

            if (field.OrderingType == OrderByFieldType.Score)
            {
                if (field.Ascending)
                    sortArray[sortIndex++] = new OrderMetadata(true, MatchCompareFieldType.Score);
                else
                    sortArray[sortIndex++] = new OrderMetadata(true, MatchCompareFieldType.Score, ascending: false);

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
                        : global::Corax.Utils.Spatial.SpatialUnits.Miles, fieldIsEmpty);
                continue;
            }

            var orderingType = field.OrderingType;
            if (orderingType is OrderByFieldType.Implicit && index.Configuration.OrderByTicksAutomaticallyWhenDatesAreInvolved && index.IndexFieldsPersistence.HasTimeValues(field.Name.Value))
                orderingType = OrderByFieldType.Long;

            var metadataField = QueryBuilderHelper.GetFieldIdForOrderBy(allocator, field.Name.Value, index, builderParameters.HasDynamics,
                builderParameters.DynamicFields,
                indexMapping, queryMapping, false);
            OrderMetadata? temporaryOrder = null;
            switch (orderingType)
            {
                case OrderByFieldType.Custom:
                    throw new NotSupportedInCoraxException($"{nameof(Corax)} doesn't support Custom OrderBy.");
                case OrderByFieldType.AlphaNumeric:
                    sortArray[sortIndex++] = new OrderMetadata(metadataField, field.Ascending, MatchCompareFieldType.Alphanumeric, fieldIsEmpty);
                    continue;
                case OrderByFieldType.Long:
                    temporaryOrder = new OrderMetadata(metadataField, field.Ascending, MatchCompareFieldType.Integer, fieldIsEmpty);
                    break;
                case OrderByFieldType.Double:
                    temporaryOrder = new OrderMetadata(metadataField, field.Ascending, MatchCompareFieldType.Floating, fieldIsEmpty);
                    break;
            }

            sortArray[sortIndex++] = temporaryOrder ?? new OrderMetadata(metadataField, field.Ascending, MatchCompareFieldType.Sequence, fieldIsEmpty);
        }

        return sortArray[0..sortIndex];
    }

    internal static IQueryMatch OrderBy(Parameters builderParameters, IQueryMatch match, in OrderMetadata[] orderMetadataSource, bool hasEmptySortingMatches)
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
                return indexSearcher.OrderBy(match, orderMetadata[0], builderParameters.Index.Configuration.NullFirst, take, builderParameters.Token);
            default:
                return indexSearcher.OrderBy(match, orderMetadata, builderParameters.Index.Configuration.NullFirst, take, builderParameters.Token);
        }
    }
}
