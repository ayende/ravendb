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

    /// <summary>O(1) seek-hint dispatch. The eligible clause (range predicate on the primary
    /// ORDER BY field with a direction-compatible operator) was identified once at template
    /// build time (<see cref="PlanTemplate.SortSeekHintTemplateIdx"/>) and remapped to an
    /// execution-position index on the <see cref="CompiledPlan"/>
    /// (<see cref="CompiledPlan.SortSeekClauseExecIdx"/>). At query time we only need to:
    /// (1) verify <c>orderByFields[0]</c> still matches the primary field — <c>GetSortMetadata</c>
    /// may have dropped the primary OrderBy when the index has zero terms in that field
    /// (non-sharded empty-field-skip case), promoting the secondary to slot 0; and
    /// (2) read the runtime parameter value through the baked <c>PackedParam</c> + <c>SortSeekUseParam2</c>
    /// pair. No clause scan, no per-clause <c>FieldName.ToString()</c>.</summary>
    public static void TrySetSortSeekHint(CompiledQueryMatch match, CompiledPlan plan,
        QueryExecution exec, OrderMetadata[] orderByFields)
    {
        int idx = plan.SortSeekClauseExecIdx;
        if (idx < 0)
            return;

        // Empty-field-skip guard: in the non-sharded case, GetSortMetadata drops an
        // OrderBy whose field has zero distinct terms in this index. If that happens to
        // the primary ORDER BY, orderByFields[0] is now the second template ORDER BY,
        // and the cached hint (computed against the template's primary) is invalid.
        var primaryName = plan.Template.SortSeekPrimaryOrderByFieldName;
        if (primaryName is null || orderByFields is null || orderByFields.Length == 0)
            return;
        if (orderByFields[0].Field.FieldName.ToString() != primaryName)
            return;

        var sortExec = exec.Executions[idx];
        var packed = sortExec.PackedParamValue;
        if (packed.IsNone)
            return;

        int paramIdx = plan.Template.SortSeekUseParam2 ? packed.Param2 : packed.Param1;
        object seekValue = packed.ValueType switch
        {
            PackedParam.TypeLong => exec.LongValues[paramIdx],
            PackedParam.TypeDouble => exec.DoubleValues[paramIdx],
            PackedParam.TypeString => exec.StringValues[paramIdx],
            _ => null
        };

        if (seekValue != null)
            match.SortHint = new SortHint(sortExec.Clause.FieldName, seekValue);
    }

    /// <summary>Compute the per-template sort-metadata snapshot. Resolves <see cref="FieldMetadata"/>,
    /// classifies ordering types, derives <c>nullsSortMode</c>, rewrites Implicit→Long for time
    /// fields, and bakes random seeds for constant-arg <c>random()</c> ordering — once per template,
    /// not once per query. Slots that depend on per-query state (argument-less random seed, the
    /// data-dependent empty-term check, parameter-bound Distance args) are emitted as
    /// <see cref="SortSlotPatch"/> directives for the runtime materializer.</summary>
    internal static SortMetadataTemplate BuildSortMetadataTemplate(QueryPlanBuilder.PlanParameters p)
    {
        var orderByFields = p.Metadata.OrderBy;

        if (orderByFields is null)
        {
            // Auto-promote to ORDER BY score() when boosting is involved and the index/config opt in.
            // Index configuration is template-stable; HasBoost is template-stable per PlanParameters.
            if (p.HasBoost && p.Index is { } indexForScore &&
                (indexForScore.Configuration.OrderByScoreAutomaticallyWhenBoostingIsInvolved
                 || indexForScore.Configuration.CoraxVectorSearchOrderByScoreAutomatically))
            {
                return new SortMetadataTemplate
                {
                    ImplicitScore = true,
                    ImplicitScoreSkipAssert = p.Metadata.HasVectorSearch,
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
        bool anyEmptyCheck = false;

        for (int i = 0; i < orderByFields.Length; i++)
        {
            var field = orderByFields[i];

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
                if (field.Arguments is { Length: > 0 })
                {
                    // Hash of the textual argument — fully template-stable (NameOrValue is the literal
                    // or parameter *name*, not the resolved value).
                    var seed = (int)Hashing.XXHash32.CalculateRaw(field.Arguments[0].NameOrValue);
                    prebuilt[i] = new OrderMetadata(seed);
                }
                else
                {
                    // No args → fresh Random.Shared.Next() per query. Prefab is a placeholder
                    // (zero seed); runtime materializer rebuilds the slot.
                    prebuilt[i] = new OrderMetadata(0);
                    patches[i].Kind = SortSlotPatchKind.RandomFreshSeed;
                    anyPatch = true;
                }
                continue;
            }

            if (field.OrderingType == OrderByFieldType.Score)
            {
                // EntryComparerByScore.Compare is intentionally inverted (returns y.CompareTo(x)),
                // so ascending=true -> highest scores first (the default "most relevant first" search engine order).
                // ascending=false -> Descending<EntryComparerByScore> -> lowest scores first.
                prebuilt[i] = new OrderMetadata(true, MatchCompareFieldType.Score, field.Ascending);
                continue;
            }

            // Field-based ordering. Index / IndexFieldsMapping are required from here on; PlanParameters
            // built from the direct-planner test constructor doesn't carry them, in which case we leave
            // the slot un-prebuilt and the runtime materializer falls back to the legacy path.
            if (p.Index is null)
                return null;

            var fieldMetadata = QueryBuilderHelper.GetFieldIdForOrderBy(p.Allocator, field.Name, p.Index,
                p.HasDynamics, p.DynamicFields, p.IndexFieldsMapping, false);

            if (field.OrderingType == OrderByFieldType.Distance)
            {
                // Distance ordering: spatial factory + per-method point/round resolution. Even if
                // all arguments are constant, the spatial factory only exists at execution time
                // (it's on QueryBuilderParameters.Factories, not PlanParameters). Defer to runtime.
                patches[i].Kind = SortSlotPatchKind.DistanceRuntime;
                patches[i].FieldMeta = fieldMetadata;
                patches[i].Source = field;
                anyPatch = true;
                // Distance slots are still subject to the empty-field-skip rule — record the field
                // metadata so the runtime materializer can call GetDistinctTermCountInField without
                // re-resolving. (We keep the empty check inside DistanceRuntime patching.)
                continue;
            }

            var orderingType = field.OrderingType;
            if (orderingType is OrderByFieldType.Implicit
                && p.Index.Configuration.OrderByTicksAutomaticallyWhenDatesAreInvolved
                && p.Index.IndexFieldsPersistence.HasTimeValues(field.Name.Value))
                orderingType = OrderByFieldType.Long;

            // Dynamic CreateField fields: no IndexFieldsMapping entry, FieldId == DynamicField (-2).
            // Such fields are written per-document only when the index function emits CreateField;
            // docs that don't emit the field have NO entry (not even a NonExisting marker) in the
            // field's tree, so StreamAndIntersect (which walks tree + null/nonExisting lists) would
            // silently drop them. Route through ExtractAndSort instead — see SortingMatch.Fill.
            bool mayHaveMissingEntries = fieldMetadata.FieldId == Constants.IndexWriter.DynamicField;

            var compareType = orderingType switch
            {
                OrderByFieldType.Custom => throw new NotSupportedInCoraxException($"{nameof(Corax)} doesn't support Custom OrderBy."),
                OrderByFieldType.AlphaNumeric => MatchCompareFieldType.Alphanumeric,
                OrderByFieldType.Long => MatchCompareFieldType.Integer,
                OrderByFieldType.Double => MatchCompareFieldType.Floating,
                _ => MatchCompareFieldType.Sequence,
            };

            // FieldHasNoTerms baked as false; runtime patch may rebuild with true.
            prebuilt[i] = new OrderMetadata(fieldMetadata, field.Ascending, compareType,
                fieldHasNoTerms: false, nullsSortMode, mayHaveMissingEntries);
            patches[i].Kind = SortSlotPatchKind.FieldEmptyCheck;
            patches[i].FieldMeta = fieldMetadata;
            anyPatch = true;
            anyEmptyCheck = true;
        }

        return new SortMetadataTemplate
        {
            Prebuilt = prebuilt,
            Patches = anyPatch ? patches : null,
            AnyEmptyCheckPending = anyEmptyCheck,
        };
    }

    public static OrderMetadata[] GetSortMetadata(QueryBuilderParameters builderParameters, PlanTemplate planTemplate, out bool hasEmpty)
    {
        hasEmpty = false;

        // PageSize == 0 (count-only) is per-query and short-circuits all sort work.
        if (builderParameters.Query.PageSize == 0)
            return null;

        // Production path: pre-built sort-metadata template from ParseTemplate.
        if (planTemplate?.SortMetadataTemplate is SortMetadataTemplate template)
            return MaterializeSortMetadata(template, builderParameters, out hasEmpty);

        // Fallback path: direct-planner tests construct PlanParameters without Index/IndexFieldsMapping
        // (so BuildSortMetadataTemplate returns null). Use the same per-query computation as before.
        return ComputeSortMetadataLegacy(builderParameters, out hasEmpty);
    }

    /// <summary>Runtime materializer: apply per-query patches to the template's prebuilt array.
    /// Hot path (no patches needed) returns the prebuilt array directly. The slow path walks only
    /// the slots that carry a patch directive — typically the empty-term check.</summary>
    private static OrderMetadata[] MaterializeSortMetadata(SortMetadataTemplate template,
        QueryBuilderParameters builderParameters, out bool hasEmpty)
    {
        hasEmpty = false;

        if (template.NoSort)
            return null;

        if (template.ImplicitScore)
        {
            if (template.ImplicitScoreSkipAssert == false)
                builderParameters.IndexReadOperation?.AssertCanOrderByScoreAutomaticallyWhenBoostingOrVectorSearchIsInvolved();
            return template.Prebuilt;
        }

        var prebuilt = template.Prebuilt;
        var patches = template.Patches;

        // Hot path: nothing to patch — return the prebuilt array directly.
        if (patches is null)
            return prebuilt;

        var indexSearcher = builderParameters.IndexSearcher;
        bool isSharded = builderParameters.IndexReadOperation?.IsSharded ?? false;

        // Result is up to prebuilt.Length, but may be shorter when non-sharded empty-field-skip drops slots.
        var result = new OrderMetadata[prebuilt.Length];
        int outIdx = 0;

        for (int i = 0; i < prebuilt.Length; i++)
        {
            ref var patch = ref patches[i];
            switch (patch.Kind)
            {
                case SortSlotPatchKind.None:
                    result[outIdx++] = prebuilt[i];
                    break;

                case SortSlotPatchKind.RandomFreshSeed:
                    result[outIdx++] = new OrderMetadata(Random.Shared.Next());
                    break;

                case SortSlotPatchKind.FieldEmptyCheck:
                {
                    bool fieldIsEmpty = indexSearcher.GetDistinctTermCountInField(patch.FieldMeta) == 0;
                    if (fieldIsEmpty == false)
                    {
                        result[outIdx++] = prebuilt[i];
                        break;
                    }

                    if (isSharded == false)
                        continue; // non-sharded empty-field-skip: drop this slot entirely

                    hasEmpty = true;
                    // Rebuild slot with FieldHasNoTerms = true; preserve all other prebuilt fields.
                    var p = prebuilt[i];
                    result[outIdx++] = new OrderMetadata(p.Field, p.Ascending, p.FieldType,
                        fieldHasNoTerms: true, p.NullsSortMode, p.MayHaveMissingEntries);
                    break;
                }

                case SortSlotPatchKind.DistanceRuntime:
                {
                    bool fieldIsEmpty = indexSearcher.GetDistinctTermCountInField(patch.FieldMeta) == 0;
                    if (fieldIsEmpty)
                    {
                        if (isSharded == false)
                            continue;
                        hasEmpty = true;
                    }

                    result[outIdx++] = BuildDistanceOrderMetadata(builderParameters, patch.Source, patch.FieldMeta, fieldIsEmpty);
                    break;
                }
            }
        }

        return outIdx == prebuilt.Length ? result : result[..outIdx];
    }

    /// <summary>Distance ordering construction extracted so both the runtime patcher and the
    /// legacy fallback share one code path.</summary>
    private static OrderMetadata BuildDistanceOrderMetadata(QueryBuilderParameters builderParameters,
        OrderByField field, FieldMetadata fieldMetadata, bool fieldIsEmpty)
    {
        var query = builderParameters.Query;
        var getSpatialField = builderParameters.Factories?.GetSpatialFieldFactory;
        var spatialField = getSpatialField(field.Name);

        var nullsSortMode = (field.NullsOrdering, field.Ascending) switch
        {
            (NullsOrderingType.First, Ascending: true) => NullsSortMode.NullsSmallest,
            (NullsOrderingType.First, Ascending: false) => NullsSortMode.NullsLargest,
            (NullsOrderingType.Last, Ascending: true) => NullsSortMode.NullsLargest,
            (NullsOrderingType.Last, Ascending: false) => NullsSortMode.NullsSmallest,
            _ => (NullsSortMode?)null
        };

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

        return new OrderMetadata(fieldMetadata, field.Ascending, MatchCompareFieldType.Spatial, point, roundTo,
            spatialField.Units is SpatialUnits.Kilometers
                ? global::Corax.Utils.Spatial.SpatialUnits.Kilometers
                : global::Corax.Utils.Spatial.SpatialUnits.Miles, fieldIsEmpty, nullsSortMode);
    }

    /// <summary>Legacy per-query path retained for direct-planner tests that build
    /// <see cref="QueryBuilderParameters"/> without a full <see cref="Index"/> / <see cref="IndexFieldsMapping"/>
    /// stack — in those cases <c>BuildSortMetadataTemplate</c> returns null and we fall back here.
    /// Behaviorally equivalent to the pre-template implementation.</summary>
    private static OrderMetadata[] ComputeSortMetadataLegacy(QueryBuilderParameters builderParameters, out bool hasEmpty)
    {
        hasEmpty = false;
        var query = builderParameters.Query;
        var index = builderParameters.Index;
        var indexMapping = builderParameters.IndexFieldsMapping;
        var allocator = builderParameters.Allocator;

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
                sortArray[sortIndex++] = new OrderMetadata(true, MatchCompareFieldType.Score, field.Ascending);
                continue;
            }

            var fieldMetadata = QueryBuilderHelper.GetFieldIdForOrderBy(allocator, field.Name, index, builderParameters.HasDynamics,
                builderParameters.DynamicFields, indexMapping, false);

            bool fieldIsEmpty = builderParameters.IndexSearcher.GetDistinctTermCountInField(fieldMetadata) == 0;
            if (fieldIsEmpty)
            {
                if (builderParameters.IndexReadOperation.IsSharded == false)
                    continue;
                hasEmpty = true;
            }

            if (field.OrderingType == OrderByFieldType.Distance)
            {
                sortArray[sortIndex++] = BuildDistanceOrderMetadata(builderParameters, field, fieldMetadata, fieldIsEmpty);
                continue;
            }

            var orderingType = field.OrderingType;
            if (orderingType is OrderByFieldType.Implicit && index.Configuration.OrderByTicksAutomaticallyWhenDatesAreInvolved && index.IndexFieldsPersistence.HasTimeValues(field.Name.Value))
                orderingType = OrderByFieldType.Long;

            bool mayHaveMissingEntries = fieldMetadata.FieldId == Constants.IndexWriter.DynamicField;
            OrderMetadata? temporaryOrder = null;
            switch (orderingType)
            {
                case OrderByFieldType.Custom:
                    throw new NotSupportedInCoraxException($"{nameof(Corax)} doesn't support Custom OrderBy.");
                case OrderByFieldType.AlphaNumeric:
                    sortArray[sortIndex++] = new OrderMetadata(fieldMetadata, field.Ascending, MatchCompareFieldType.Alphanumeric, fieldIsEmpty, nullsSortMode, mayHaveMissingEntries);
                    continue;
                case OrderByFieldType.Long:
                    temporaryOrder = new OrderMetadata(fieldMetadata, field.Ascending, MatchCompareFieldType.Integer, fieldIsEmpty, nullsSortMode, mayHaveMissingEntries);
                    break;
                case OrderByFieldType.Double:
                    temporaryOrder = new OrderMetadata(fieldMetadata, field.Ascending, MatchCompareFieldType.Floating, fieldIsEmpty, nullsSortMode, mayHaveMissingEntries);
                    break;
            }

            sortArray[sortIndex++] = temporaryOrder ?? new OrderMetadata(fieldMetadata, field.Ascending, MatchCompareFieldType.Sequence, fieldIsEmpty, nullsSortMode, mayHaveMissingEntries);
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
