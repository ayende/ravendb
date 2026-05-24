using System;
using System.Runtime.CompilerServices;
using System.Threading;
using Corax.Mappings;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.SortingMatches.Meta;
using Corax.Querying.Planning;
using Corax.Utils;
using Raven.Client.Exceptions.Corax;
using Raven.Server.Documents.Indexes.Persistence.Corax.QueryOptimizer;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Spatial4n.Shapes;
using Sparrow;
using Constants = Corax.Constants;
using IndexSearcher = Corax.Querying.IndexSearcher;
using SpatialUnits = Raven.Client.Documents.Indexes.Spatial.SpatialUnits;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

/// <summary>
/// Sorting / ORDER BY resolution: validates ORDER BY fields, resolves field
/// metadata, classifies direction/analyzer, applies SortingMatch wrappers,
/// and provides seek-hint optimizations.
/// </summary>
internal static partial class QueryPlanBuilder
{
    /// <summary>Maximum number of ORDER BY fields supported by Corax.</summary>
    private const int MaxSortFields = 16;

    // ── Sort seek hint ────────────────────────────────────────────────────

    /// <summary>If the first clause is a range predicate on the same field as the first
    /// ORDER BY field, set a seek hint on the CompiledQueryMatch so SortedIndexReader
    /// can skip walking irrelevant tree terms.</summary>
    public static void TrySetSortSeekHint(CompiledQueryMatch match,
        QueryExecution exec, OrderMetadata[] orderByFields)
    {
        if (orderByFields == null || orderByFields.Length == 0)
            return;

        var execs = exec.Executions;
        if (execs == null || execs.Count == 0)
            return;

        // Only consider the first ORDER BY field
        var sortField = orderByFields[0].Field.FieldName;

        // Find a range clause on the same field (scan all clauses, not just first — the sort-eligible
        // clause may not be the cheapest and thus not clause[0]).
        for (int i = 0; i < execs.Count; i++)
        {
            var clause = execs[i].Clause;
            var sortExec = execs[i];

            if (clause.FieldName != sortField.ToString())
                continue;

            // Range clauses on the sort field — supports long, double, and string
            if (clause.ClauseType is not (ClauseType.GreaterThan or ClauseType.GreaterThanOrEqual
                or ClauseType.LessThan or ClauseType.LessThanOrEqual or ClauseType.Between))
                continue;

            var packed = sortExec.PackedParamValue;
            if (packed.IsNone)
                continue;

            // For ascending order: seek to the lower bound (GT/GTE value)
            // For descending order: seek to the upper bound (LT/LTE value)
            bool ascending = orderByFields[0].Ascending;
            object seekValue = null;

            if (ascending && clause.ClauseType is ClauseType.GreaterThan or ClauseType.GreaterThanOrEqual)
            {
                seekValue = packed.ValueType switch
                {
                    PackedParam.TypeLong => exec.LongValues[packed.Param1],
                    PackedParam.TypeDouble => exec.DoubleValues[packed.Param1],
                    PackedParam.TypeString => exec.StringValues[packed.Param1],
                    _ => null
                };
            }
            else if (ascending == false && clause.ClauseType is ClauseType.LessThan or ClauseType.LessThanOrEqual)
            {
                seekValue = packed.ValueType switch
                {
                    PackedParam.TypeLong => exec.LongValues[packed.Param1],
                    PackedParam.TypeDouble => exec.DoubleValues[packed.Param1],
                    PackedParam.TypeString => exec.StringValues[packed.Param1],
                    _ => null
                };
            }
            else if (clause.ClauseType == ClauseType.Between)
            {
                // Between: seek to the lower bound for ASC, upper bound for DESC
                int idx = ascending ? packed.Param1 : packed.Param2;
                seekValue = packed.ValueType switch
                {
                    PackedParam.TypeLong => exec.LongValues[idx],
                    PackedParam.TypeDouble => exec.DoubleValues[idx],
                    PackedParam.TypeString => exec.StringValues[idx],
                    _ => null
                };
            }

            if (seekValue != null)
            {
                match.SortHint = new SortHint(clause.FieldName, seekValue);
                return;
            }
        }
    }

    public static OrderMetadata[] GetSortMetadata(QueryBuilderParameters builderParameters, out bool hasEmpty)
    {
        hasEmpty = false;
        var query = builderParameters.Query;
        var index = builderParameters.Index;
        var getSpatialField = builderParameters.Factories?.GetSpatialFieldFactory;
        var indexMapping = builderParameters.IndexFieldsMapping;
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
                if (builderParameters.Metadata.HasVectorSearch == false)
                    builderParameters.IndexReadOperation?.AssertCanOrderByScoreAutomaticallyWhenBoostingOrVectorSearchIsInvolved();

                return [new OrderMetadata(true, MatchCompareFieldType.Score)];
            }

            return null;
        }

        if (orderByFields.Length == 0)
            return null;

        if (orderByFields.Length > MaxSortFields)
            throw new InvalidOperationException($"Corax does not support ordering by more than {MaxSortFields} properties.");

        // OrderMetadata contains a managed IPoint reference field, so stackalloc is not
        // possible here (only unmanaged structs may be stack-allocated in C#).
        int sortIndex = 0;
        var sortArray = new OrderMetadata[MaxSortFields];

        foreach (var field in orderByFields)
        {
            var nullsSortMode = (field.NullsOrdering, field.Ascending) switch
            {
                (NullsOrderingType.First, Ascending: true) => NullsSortMode.NullsSmallest,
                (NullsOrderingType.First, Ascending: false) => NullsSortMode.NullsLargest,
                (NullsOrderingType.Last, Ascending: true) => NullsSortMode.NullsLargest,
                (NullsOrderingType.Last, Ascending: false) => NullsSortMode.NullsSmallest,
                _ => (NullsSortMode?)null
            };

            if (field.OrderingType == OrderByFieldType.Random)
            {
                var seed = field.Arguments is { Length: > 0 } ? (int)Hashing.XXHash32.CalculateRaw(field.Arguments[0].NameOrValue) : Random.Shared.Next();
                sortArray[sortIndex++] = new OrderMetadata(seed);
                continue;
            }

            if (field.OrderingType == OrderByFieldType.Score)
            {
                // EntryComparerByScore.Compare is intentionally inverted (returns y.CompareTo(x)),
                // so ascending=true -> highest scores first (the default "most relevant first" search engine order).
                // ascending=false -> Descending<EntryComparerByScore> -> lowest scores first.
                sortArray[sortIndex++] = new OrderMetadata(true, MatchCompareFieldType.Score, field.Ascending);

                continue;
            }

            var fieldMetadata = QueryBuilderHelper.GetFieldIdForOrderBy(allocator, field.Name, index, builderParameters.HasDynamics,
                builderParameters.DynamicFields, indexMapping, false);

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
                        : global::Corax.Utils.Spatial.SpatialUnits.Miles, fieldIsEmpty, nullsSortMode);
                continue;
            }

            var orderingType = field.OrderingType;
            if (orderingType is OrderByFieldType.Implicit && index.Configuration.OrderByTicksAutomaticallyWhenDatesAreInvolved && index.IndexFieldsPersistence.HasTimeValues(field.Name.Value))
                orderingType = OrderByFieldType.Long;

            var metadataField = QueryBuilderHelper.GetFieldIdForOrderBy(allocator, field.Name.Value, index, builderParameters.HasDynamics,
                builderParameters.DynamicFields,
                indexMapping, false);
            // Dynamic CreateField fields: no IndexFieldsMapping entry, FieldId == DynamicField (-2).
            // Such fields are written per-document only when the index function emits CreateField;
            // docs that don't emit the field have NO entry (not even a NonExisting marker) in the
            // field's tree, so StreamAndIntersect (which walks tree + null/nonExisting lists) would
            // silently drop them. Route through ExtractAndSort instead — see SortingMatch.Fill.
            bool mayHaveMissingEntries = metadataField.FieldId == Constants.IndexWriter.DynamicField;
            OrderMetadata? temporaryOrder = null;
            switch (orderingType)
            {
                case OrderByFieldType.Custom:
                    throw new NotSupportedInCoraxException($"{nameof(Corax)} doesn't support Custom OrderBy.");
                case OrderByFieldType.AlphaNumeric:
                    sortArray[sortIndex++] = new OrderMetadata(metadataField, field.Ascending, MatchCompareFieldType.Alphanumeric, fieldIsEmpty, nullsSortMode, mayHaveMissingEntries);
                    continue;
                case OrderByFieldType.Long:
                    temporaryOrder = new OrderMetadata(metadataField, field.Ascending, MatchCompareFieldType.Integer, fieldIsEmpty, nullsSortMode, mayHaveMissingEntries);
                    break;
                case OrderByFieldType.Double:
                    temporaryOrder = new OrderMetadata(metadataField, field.Ascending, MatchCompareFieldType.Floating, fieldIsEmpty, nullsSortMode, mayHaveMissingEntries);
                    break;
            }

            sortArray[sortIndex++] = temporaryOrder ?? new OrderMetadata(metadataField, field.Ascending, MatchCompareFieldType.Sequence, fieldIsEmpty, nullsSortMode, mayHaveMissingEntries);
        }

        return sortArray[0..sortIndex];
    }

    public static IQueryMatch OrderBy(QueryBuilderParameters builderParameters, IQueryMatch match, in OrderMetadata[] orderMetadataSource, bool hasEmptySortingMatches)
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
                return indexSearcher.OrderBy(match, orderMetadata[0], builderParameters.Index.Configuration.NullsSortMode, take, builderParameters.Token);
            default:
                return indexSearcher.OrderBy(match, orderMetadata, builderParameters.Index.Configuration.NullsSortMode, take, builderParameters.Token);
        }
    }

    /// <summary>Apply ORDER BY from plan metadata when a full <see cref="QueryBuilderParameters"/> is not
    /// available (e.g., direct tests). Handles <c>ORDER BY score()</c> only — callers that need
    /// field / spatial / alphanumeric sorts must use the full
    /// <see cref="OrderBy(QueryBuilderParameters,IQueryMatch,in OrderMetadata[],bool)"/> overload.</summary>
    public static IQueryMatch ApplyScoreOrdering(QueryPlanBuilder.PlanParameters planParams, IQueryMatch match, long take, CancellationToken token = default)
    {
        OrderByField[] orderByFields = planParams.Metadata.OrderBy;
        if (orderByFields == null || orderByFields.Length == 0)
            return match;

        var indexSearcher = planParams.IndexSearcher;
        int takeInt = take > int.MaxValue ? Constants.IndexSearcher.TakeAll : (int)take;

        for (int i = 0; i < orderByFields.Length; i++)
        {
            if (orderByFields[i].OrderingType == OrderByFieldType.Score)
            {
                var meta = new OrderMetadata(true, MatchCompareFieldType.Score, orderByFields[i].Ascending);
                return indexSearcher.OrderBy(match, meta, NullsSortMode.NullsLargest, take: takeInt, token: token);
            }
        }

        return match;
    }

    // ── Search helpers (used by ResolveClause for Search clause type) ────

    /// <summary>
    /// Replaces the search analyzer with an appropriate wildcard analyzer.
    /// LuceneAnalyzerAdapter wrapping KeywordAnalyzer has IsExactAnalyzer=false
    /// (because LuceneAnalyzerAdapter passes NoTransformers), so the generic
    /// CreateWildcardAnalyzer in the Legacy path would incorrectly lowercase the term.
    /// This matches the old CoraxQueryBuilder.ReplaceAnalyzerForWildcardQueries logic.
    /// </summary>
    private static FieldMetadata ReplaceAnalyzerForWildcardQueries(
        FieldMetadata searchMeta,
        ResolutionContext walkerCtx)
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
