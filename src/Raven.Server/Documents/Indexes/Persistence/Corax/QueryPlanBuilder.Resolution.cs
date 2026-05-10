using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Primitives;
using Voron.Data.RoaringBitmaps;
using Corax.Querying.Matches.SortingMatches.Meta;
using Corax.Querying.Planning;
using Corax.Mappings;
using Corax.Utils;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Corax;
using VectorOptions = Raven.Client.Documents.Indexes.Vector.VectorOptions;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;
using Raven.Server.Documents.Indexes.Persistence.Corax.QueryOptimizer;
using Raven.Server.Documents.Indexes.VectorSearch;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Spatial4n.Shapes;
using Sparrow;
using Sparrow.Json;
using Constants = Corax.Constants;
using RavenConstants = Raven.Client.Constants;
using ClientConstants = Raven.Client.Constants;
using Sparrow.Server;
using SpatialUnits = Raven.Client.Documents.Indexes.Spatial.SpatialUnits;
using IndexSearcher = Corax.Querying.IndexSearcher;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

/// <summary>
/// Per-execution resolution: compiles plans, resolves matches and term sources
/// from clause metadata, extracts typed scan parameters, handles highlighting,
/// sorting, and spatial/vector materialization.
///
/// Methods here run once per query execution (not cached).
/// </summary>
internal static partial class QueryPlanBuilder
{
    // ── Entry points ─────────────────────────────────────────────────────

    /// <summary>
    /// Plan → compile → resolve pipeline: builds the plan, caches compiled delegates
    /// by query text, resolves matches and term sources, extracts scan parameters,
    /// wraps with post-filter phases (spatial, vector).
    /// </summary>
    public static IQueryMatch BuildAndCompile(
        PlanParameters planParams,
        QueryBuilderParameters builderParameters,
        long take,
        out QueryPlan plan,
        out CompiledPlan compiledPlanOut,
        Dictionary<string, CoraxHighlightingTermIndex> highlightingTerms,
        CancellationToken token)
    {
        plan = BuildPlan(planParams);
        var indexSearcher = planParams.IndexSearcher;
        var queryText = planParams.Metadata.Query.QueryText;

        if (planParams.HasBoost)
        {
            // When BM25 scoring is needed, TermSource dispatch skips Fill() on the
            // TermMatch objects in _resolvedMatches, leaving Bm25Relevance._matchBuffer
            // empty. Score() binary-searches an empty buffer → returns 0 for every entry,
            // producing arbitrary (wrong) sort order.
            // Force all ops through the IQueryMatch path so TermMatch.Fill() is called
            // and score buffers are populated before SortingMatch invokes Score().
            // Bit 30 of OperandOrdering differentiates cached boosted plans from
            // non-boosted ones so they don't share a compiled delegate.
            var ops = plan.Ops;
            if (ops != null)
                for (int i = 0; i < ops.Length; i++)
                    ops[i].Dispatch = MatchDispatch.DirectSource;
            plan.OperandOrdering |= (1 << 30);
        }

        var planCache = indexSearcher.PlanCache;
        var compiledPlan = planCache.Get(queryText, plan.OperandOrdering, plan.TypeSignature, plan.FullKinds);
        if (compiledPlan == null)
        {
            compiledPlan = new CompiledPlan
            {
                CompiledDelegate = QueryIlEmitter.EmitDelegate(plan, out var explainText),
                ExplainSource = explainText,
                Ordering = plan.OperandOrdering,
                TypeSignature = plan.TypeSignature,
                FullKinds = plan.FullKinds,
                InspectionTemplate = BuildInspectionTemplate(plan)
            };
            planCache.Add(queryText, compiledPlan);
        }

        compiledPlanOut = compiledPlan;
        var resolvedMatches = ResolveMatches(plan, indexSearcher, planParams, builderParameters);
        var termSources = ResolveTermSources(plan, indexSearcher, planParams, builderParameters);
        ExtractScanParameters(plan, indexSearcher,
            out var longParams, out var doubleParams, out var sliceParams, out var fieldRootPages);

        if (highlightingTerms != null)
            PopulateHighlightingTerms(plan, highlightingTerms, planParams.Metadata);

        IQueryMatch result = new CompiledQueryMatch(
            compiledPlan, plan.RequiredBitmaps, plan.Ops?.Length ?? 0, resolvedMatches, termSources, null,
            longParams, doubleParams, sliceParams, fieldRootPages,
            indexSearcher, planParams.Allocator, token);

        // Spatial post-filter phase: AND each spatial match with the candidate bitmap.
        if (plan.SpatialFilters is { Length: > 0 })
        {
            var spatialFilters = new IQueryMatch[plan.SpatialFilters.Length];
            for (int sf = 0; sf < plan.SpatialFilters.Length; sf++)
                spatialFilters[sf] = resolvedMatches[plan.SpatialFilters[sf].MatchIndex];
            result = new PostFilterMatch(result, spatialFilters);
        }

        // Vector select phase: each vector wraps the bitmap so far as its filter source.
        if (plan.VectorSelects is { Length: > 0 } && builderParameters != null)
        {
            var vectorItems = ResolveVectorItems(plan, indexSearcher, planParams, builderParameters);
            bool hasActualFilter = !plan.IsAllEntries || plan.SpatialFilters is { Length: > 0 };
            IQueryMatch vectorFilter = hasActualFilter ? result : null;
            foreach (var item in vectorItems)
                result = item.Materialize(vectorFilter);
        }

        return result;
    }

    /// <summary>
    /// Build a query match from a sub-expression (e.g. the inner BinaryExpression
    /// of a moreLikeThis clause). Parses and resolves the expression directly
    /// without going through the full plan/compile pipeline.
    /// </summary>
    public static IQueryMatch BuildFromSubExpression(QueryBuilderParameters builderParams, QueryExpression expression)
    {
        var indexSearcher = builderParams.IndexSearcher;
        var clauses = new List<ClauseInfo>();
        var writer = new ValueWriter();
        bool hasMixed = false;
        ParseExpression(expression, indexSearcher, clauses, builderParams.QueryParameters,
            builderParams.Metadata, ref hasMixed, writer);

        if (clauses.Count == 0)
            return indexSearcher.AllEntries();

        // Create a mini plan to carry the typed arrays for resolution
        var subPlan = new QueryPlan
        {
            LongValues = writer.GetLongs(),
            DoubleValues = writer.GetDoubles(),
            StringValues = writer.GetStrings()
        };

        if (clauses.Count == 1)
            return ResolveClause(clauses[0], indexSearcher, subPlan, builderParams: builderParams);

        // Multiple clauses (AND chain) — resolve each and AND them via bitmap
        var bitmap = new BitmapMatch(indexSearcher.Allocator);
        var temp = new RoaringBitmap(indexSearcher.Allocator);
        bool first = true;
        foreach (var clause in clauses)
        {
            var match = ResolveClause(clause, indexSearcher, subPlan, builderParams: builderParams);
            if (first)
            {
                QueryPrimitives.FillFromMatch(match, ref bitmap.BitmapState);
                first = false;
            }
            else
            {
                QueryPrimitives.AndWithMatch(match, ref bitmap.BitmapState, ref temp);
            }
        }
        temp.Dispose();
        return bitmap;
    }

    // ── Typed dispatch helpers ───────────────────────────────────────────

    /// <summary>Create a TermQuery using the pre-resolved typed value from the plan's arrays.</summary>
    private static IQueryMatch TermQueryFromParam(PackedParam packed, FieldMetadata fieldMeta,
        IndexSearcher indexSearcher, QueryPlan plan)
    {
        int idx = packed.Param1;
        return packed.ValueType switch
        {
            PackedParam.TypeLong => indexSearcher.TermQuery(fieldMeta, plan.LongValues[idx]),
            PackedParam.TypeDouble => indexSearcher.TermQuery(fieldMeta, plan.DoubleValues[idx]),
            _ => indexSearcher.TermQuery(fieldMeta, plan.StringValues[idx])
        };
    }

    /// <summary>Get a posting-list ID using the pre-resolved typed value.</summary>
    private static long GetTermPostingListIdFromParam(PackedParam packed, FieldMetadata fieldMeta,
        IndexSearcher indexSearcher, QueryPlan plan)
    {
        int idx = packed.Param1;
        return packed.ValueType switch
        {
            PackedParam.TypeLong => indexSearcher.GetTermPostingListId(fieldMeta, plan.LongValues[idx]),
            PackedParam.TypeDouble => indexSearcher.GetTermPostingListId(fieldMeta, plan.DoubleValues[idx]),
            _ => indexSearcher.GetTermPostingListId(fieldMeta, plan.StringValues[idx])
        };
    }

    // ── Match resolution ─────────────────────────────────────────────────

    /// <summary>
    /// Resolve clause infos to IQueryMatch instances for execution.
    /// Uses existing IndexSearcher methods (TermQuery, etc.) which handle
    /// all the complexity of analyzer application, CompactKey encoding,
    /// posting list resolution, etc.
    /// </summary>
    public static IQueryMatch[] ResolveMatches(QueryPlan plan, IndexSearcher indexSearcher,
        PlanParameters parameters = null, QueryBuilderParameters builderParams = null)
    {
        var clauses = plan.QueryBuilderPlanState as List<ClauseInfo> ?? [];
        // All-entries plan with possible post-filter phases
        if (plan.IsAllEntries)
        {
            int spatialCount = plan.SpatialFilters?.Length ?? 0;
            int vectorCount = plan.VectorSelects?.Length ?? 0;
            int totalExtra = spatialCount + vectorCount;
            if (totalExtra == 0)
                return [indexSearcher.AllEntries()];

            var allEntriesMatches = new IQueryMatch[1 + totalExtra];
            allEntriesMatches[0] = indexSearcher.AllEntries();
            for (int i = 0; i < clauses.Count; i++)
            {
                ClauseInfo clause = clauses[i];
                allEntriesMatches[1 + i] = ResolveClause(clause, indexSearcher, plan, parameters, builderParams);
            }
            return allEntriesMatches;
        }

        if (clauses.Count == 0)
            return [];

        // Standalone NotEquals pattern: Fill(AllEntries) + ANDNOT(term).
        if (clauses.Count == 1 && clauses[0].IsNegated && !plan.AllNegated)
        {
            var clause = clauses[0];
            return
            [
                indexSearcher.AllEntries(),
                TermQueryFromParam(clause.PackedParamValue, indexSearcher.FieldMetadataBuilder(clause.FieldName), indexSearcher, plan)
            ];
        }

        var matches = new IQueryMatch[CountMatchSlots(clauses, plan.IsAllEntries, plan.AllNegated)];
        int matchIdx = 0;
        foreach (ClauseInfo clause in clauses)
        {
            if (clause.ClauseType == ClauseType.OrGroup && clause.OrSubClauses != null)
            {
                foreach (var sub in clause.OrSubClauses)
                {
                    var match = ResolveClause(sub, indexSearcher, plan, parameters, builderParams);
                    if (sub.BoostFactor > 0)
                        match = indexSearcher.Boost(match, sub.BoostFactor);
                    matches[matchIdx++] = match;
                }
            }
            else if (clause.ClauseType == ClauseType.AndGroup && clause.AndSubClauses != null)
            {
                foreach (var sub in clause.AndSubClauses)
                {
                    var match = ResolveClause(sub, indexSearcher, plan, parameters, builderParams);
                    if (sub.BoostFactor > 0)
                        match = indexSearcher.Boost(match, sub.BoostFactor);
                    matches[matchIdx++] = match;
                }
            }
            else if ((clause.ClauseType == ClauseType.AllIn || clause.ClauseType == ClauseType.In) && clause.InTerms != null)
            {
                for (int t = 0; t < clause.InTerms.Count; t++)
                    matches[matchIdx++] = ResolveInTerm(clause, t, indexSearcher, plan, parameters, builderParams);
            }
            else
            {
                IQueryMatch match = clause.IsOrChainNotEquals ?
                    CreateNotEqualsOrMatch(clause, indexSearcher, plan, parameters, builderParams) :
                    ResolveClause(clause, indexSearcher, plan, parameters, builderParams);
                if (clause.BoostFactor > 0)
                    match = indexSearcher.Boost(match, clause.BoostFactor);
                matches[matchIdx++] = match;
            }
        }
        if (plan.AllNegated)
            matches[matchIdx] = indexSearcher.AllEntries();
        return matches;
    }

    private static IQueryMatch ResolveClause(ClauseInfo clause, IndexSearcher indexSearcher,
        QueryPlan plan, PlanParameters parameters = null, QueryBuilderParameters builderParams = null)
    {
        if (clause.ClauseType == ClauseType.OrGroup && clause.OrSubClauses != null)
        {
            var bm = new BitmapMatch(indexSearcher.Allocator);
            var temp = new RoaringBitmap(indexSearcher.Allocator);
            foreach (var sub in clause.OrSubClauses)
            {
                var subMatch = ResolveClause(sub, indexSearcher, plan, parameters, builderParams);
                QueryPrimitives.FillFromMatch(subMatch, ref bm.BitmapState);
            }
            temp.Dispose();
            return bm;
        }
        if (clause.ClauseType == ClauseType.AndGroup && clause.AndSubClauses != null)
        {
            var bm = new BitmapMatch(indexSearcher.Allocator);
            var temp = new RoaringBitmap(indexSearcher.Allocator);
            bool first = true;
            foreach (var sub in clause.AndSubClauses)
            {
                var subMatch = ResolveClause(sub, indexSearcher, plan, parameters, builderParams);
                if (first)
                {
                    QueryPrimitives.FillFromMatch(subMatch, ref bm.BitmapState);
                    first = false;
                }
                else if (sub.IsNegated)
                    QueryPrimitives.AndNotWithMatch(subMatch, ref bm.BitmapState, ref temp);
                else
                    QueryPrimitives.AndWithMatch(subMatch, ref bm.BitmapState, ref temp);
            }
            temp.Dispose();
            return bm;
        }

        // Spatial/Vector/Search have their own field resolution paths.
        FieldMetadata fieldMeta = default;
        bool needsFieldMeta = clause.ClauseType != ClauseType.Spatial
            && clause.ClauseType != ClauseType.Vector
            && clause.ClauseType != ClauseType.Search;
        if (needsFieldMeta)
        {
            if (builderParams != null)
            {
                string resolvedFieldName = clause.FieldName;
                if (clause.IsExact && builderParams.Metadata.IsDynamic)
                    resolvedFieldName = AutoIndexField.GetExactAutoIndexFieldName(resolvedFieldName);
                fieldMeta = QueryBuilderHelper.GetFieldMetadata(in builderParams, resolvedFieldName, exact: clause.IsExact, hasBoost: builderParams.HasBoost);
            }
            else
            {
                fieldMeta = indexSearcher.FieldMetadataBuilder(clause.FieldName, hasBoost: parameters?.HasBoost ?? false);
            }
        }

        var packed = clause.PackedParamValue;

        switch (clause.ClauseType)
        {
            case ClauseType.Equals:
            case ClauseType.NotEquals:
                return TermQueryFromParam(packed, fieldMeta, indexSearcher, plan);

            case ClauseType.GreaterThan:
            {
                int idx = packed.Param1;
                return packed.ValueType switch
                {
                    PackedParam.TypeLong => indexSearcher.GreaterThanQuery(fieldMeta, plan.LongValues[idx]),
                    PackedParam.TypeDouble => indexSearcher.GreaterThanQuery(fieldMeta, plan.DoubleValues[idx]),
                    _ => indexSearcher.GreaterThanQuery(fieldMeta, plan.StringValues[idx])
                };
            }

            case ClauseType.GreaterThanOrEqual:
            {
                int idx = packed.Param1;
                return packed.ValueType switch
                {
                    PackedParam.TypeLong => indexSearcher.GreatThanOrEqualsQuery(fieldMeta, plan.LongValues[idx]),
                    PackedParam.TypeDouble => indexSearcher.GreatThanOrEqualsQuery(fieldMeta, plan.DoubleValues[idx]),
                    _ => indexSearcher.GreatThanOrEqualsQuery(fieldMeta, plan.StringValues[idx])
                };
            }

            case ClauseType.LessThan:
            {
                int idx = packed.Param1;
                return packed.ValueType switch
                {
                    PackedParam.TypeLong => indexSearcher.LessThanQuery(fieldMeta, plan.LongValues[idx]),
                    PackedParam.TypeDouble => indexSearcher.LessThanQuery(fieldMeta, plan.DoubleValues[idx]),
                    _ => indexSearcher.LessThanQuery(fieldMeta, plan.StringValues[idx])
                };
            }

            case ClauseType.LessThanOrEqual:
            {
                int idx = packed.Param1;
                return packed.ValueType switch
                {
                    PackedParam.TypeLong => indexSearcher.LessThanOrEqualsQuery(fieldMeta, plan.LongValues[idx]),
                    PackedParam.TypeDouble => indexSearcher.LessThanOrEqualsQuery(fieldMeta, plan.DoubleValues[idx]),
                    _ => indexSearcher.LessThanOrEqualsQuery(fieldMeta, plan.StringValues[idx])
                };
            }

            case ClauseType.Between:
            {
                int idx1 = packed.Param1;
                int idx2 = packed.Param2;
                return packed.ValueType switch
                {
                    PackedParam.TypeLong => indexSearcher.BetweenQuery(fieldMeta, plan.LongValues[idx1], plan.LongValues[idx2]),
                    PackedParam.TypeDouble => indexSearcher.BetweenQuery(fieldMeta, plan.DoubleValues[idx1], plan.DoubleValues[idx2]),
                    _ => indexSearcher.BetweenQuery(fieldMeta, plan.StringValues[idx1], plan.StringValues[idx2])
                };
            }

            case ClauseType.In:
            case ClauseType.AllIn:
                if (clause.InTerms is { Count: > 0 })
                    return indexSearcher.InQuery(fieldMeta, clause.InTerms);
                return indexSearcher.EmptyMatch();

            case ClauseType.Exists:
                return indexSearcher.ExistsQuery(fieldMeta);

            case ClauseType.StartsWith:
                return indexSearcher.StartWithQuery(fieldMeta, plan.StringValues[packed.Param1]);

            case ClauseType.EndsWith:
                return indexSearcher.EndsWithQuery(fieldMeta, plan.StringValues[packed.Param1]);

            case ClauseType.Search:
            {
                FieldMetadata searchMeta;
                if (builderParams != null)
                {
                    string searchFieldName = clause.FieldName;
                    if (builderParams.Metadata.IsDynamic)
                        searchFieldName = AutoIndexField.GetSearchAutoIndexFieldName(searchFieldName);

                    searchMeta = QueryBuilderHelper.GetFieldMetadata(
                        builderParams.Allocator, searchFieldName, builderParams.Index,
                        builderParams.IndexFieldsMapping, builderParams.FieldsToFetch,
                        builderParams.HasDynamics, builderParams.DynamicFields,
                        handleSearch: true, hasBoost: builderParams.HasBoost);
                }
                else if (parameters is { Index: not null, IndexFieldsMapping: not null })
                {
                    string searchFieldName = clause.FieldName;
                    if (parameters.Metadata.IsDynamic)
                        searchFieldName = AutoIndexField.GetSearchAutoIndexFieldName(searchFieldName);

                    searchMeta = QueryBuilderHelper.GetFieldMetadata(
                        parameters.Allocator, searchFieldName, parameters.Index,
                        parameters.IndexFieldsMapping, parameters.FieldsToFetch,
                        parameters.HasDynamics, parameters.DynamicFields,
                        handleSearch: true, hasBoost: parameters.HasBoost);
                }
                else
                {
                    searchMeta = fieldMeta;
                }

                var indexDef = builderParams?.Index?.Definition ?? parameters?.Index?.Definition;
                IndexSearcher.SearchQueryOptions searchQueryOptions;
                if (indexDef != null && IndexDefinitionBaseServerSide.IndexVersion.IsCoraxSearchWildcardAdjustmentSupported(indexDef.Version))
                    searchQueryOptions = IndexSearcher.SearchQueryOptions.PhraseQueryWithWildcardAdjustments;
                else if (indexDef is { Version: >= IndexDefinitionBaseServerSide.IndexVersion.PhraseQuerySupportInCoraxIndexes })
                    searchQueryOptions = IndexSearcher.SearchQueryOptions.PhraseQuery;
                else
                    searchQueryOptions = IndexSearcher.SearchQueryOptions.Legacy;

                var searchTerm = plan.StringValues[packed.Param1];
                if (searchQueryOptions == IndexSearcher.SearchQueryOptions.PhraseQueryWithWildcardAdjustments
                    && searchTerm is { Length: >= 1 }
                    && (searchTerm[0] == '*' || (searchTerm.Length >= 2 && searchTerm[^1] == '*')))
                {
                    searchMeta = ReplaceAnalyzerForWildcardQueries(searchMeta, builderParams, parameters);
                }

                var searchValues = SplitSearchTerms(searchTerm);

                return indexSearcher.SearchQuery(searchMeta,
                    searchValues,
                    clause.SearchOperator,
                    searchQueryOptions);
            }

            case ClauseType.Regex:
                return indexSearcher.RegexQuery(fieldMeta,
                    new System.Text.RegularExpressions.Regex(plan.StringValues[packed.Param1]));

            case ClauseType.Spatial:
            {
                if (builderParams == null || clause.MethodExpression == null)
                    throw new InvalidOperationException("Spatial resolution requires builder parameters");
                var spatialMethod = QueryMethod.GetMethodType(clause.MethodExpression.Name.Value);
                return HandleSpatial(builderParams, clause, spatialMethod);
            }

            case ClauseType.Vector:
            {
                if (builderParams == null || clause.MethodExpression == null)
                    throw new InvalidOperationException("Vector resolution requires builder parameters");
                var vectorItem = HandleVector(builderParams, clause, false);
                return vectorItem.Materialize(null);
            }

            case ClauseType.OrGroup:
                throw new InvalidOperationException(
                    "OrGroup should be expanded by ResolveMatches, not resolved as a single clause.");

            case ClauseType.AndGroup:
                throw new InvalidOperationException(
                    "AndGroup should be expanded by ResolveMatches, not resolved as a single clause.");

            default:
                throw new InvalidOperationException($"Unexpected ClauseType {clause.ClauseType} in ResolveClause.");
        }
    }

    /// <summary>Resolve a single IN term to a typed TermQuery.
    /// IN terms are stored contiguously: PackedParamValue.Param1 = start index, Param2 = count.
    /// Null terms (InTerms[i] == null) use the string TermQuery path regardless of packed type.</summary>
    private static IQueryMatch ResolveInTerm(ClauseInfo clause, int termIndex,
        IndexSearcher indexSearcher, QueryPlan plan,
        PlanParameters parameters, QueryBuilderParameters builderParams)
    {
        FieldMetadata fieldMeta = builderParams != null ?
            QueryBuilderHelper.GetFieldMetadata(in builderParams, clause.FieldName, hasBoost: builderParams.HasBoost) :
            indexSearcher.FieldMetadataBuilder(clause.FieldName, hasBoost: parameters?.HasBoost ?? false);

        // Null IN terms use string null lookup regardless of the dominant type
        if (clause.InTerms != null && termIndex < clause.InTerms.Count && clause.InTerms[termIndex] == null)
            return indexSearcher.TermQuery(fieldMeta, (string)null);

        var p = clause.PackedParamValue;
        int idx = p.Param1 + termIndex;
        var termPacked = new PackedParam(p.ValueType, idx);
        return TermQueryFromParam(termPacked, fieldMeta, indexSearcher, plan);
    }

    /// <summary>Create a pre-materialized <see cref="BitmapMatch"/> for a NotEquals clause
    /// appearing in an OR chain. OR(NOT X, NOT Y, ...) cannot use the raw term posting list
    /// (FillBitmapFromTermSource would add entries WITH X, not WITHOUT X). Instead, we
    /// pre-compute AllEntries ANDNOT TermQuery(X) into a BitmapMatch so that FillFromMatch
    /// during execution correctly ORs in the set of entries NOT having X.</summary>
    private static IQueryMatch CreateNotEqualsOrMatch(ClauseInfo clause, IndexSearcher indexSearcher,
        QueryPlan plan, PlanParameters parameters, QueryBuilderParameters builderParams)
    {
        FieldMetadata fieldMeta = ResolveFieldMetadata(clause, indexSearcher, parameters, builderParams);
        IQueryMatch termMatch = TermQueryFromParam(clause.PackedParamValue, fieldMeta, indexSearcher, plan);

        var bitmapMatch = new BitmapMatch(indexSearcher.Allocator);
        var tempData = new RoaringBitmap(indexSearcher.Allocator);
        QueryPrimitives.FillFromMatch(indexSearcher.AllEntries(), ref bitmapMatch.BitmapState);
        QueryPrimitives.AndNotWithMatch(termMatch, ref bitmapMatch.BitmapState, ref tempData);
        tempData.Dispose();
        return bitmapMatch;
    }

    // ── Term-source resolution ───────────────────────────────────────────

    /// <summary>
    /// Resolve clause infos to <see cref="TermSource"/> instances for the native
    /// posting-list dispatch path. Parallels <see cref="ResolveMatches"/> — the
    /// returned array uses the same indexing scheme. Slots whose underlying
    /// clause is multi-term / non-term-shaped (Spatial, Vector, Search, Range,
    /// StartsWith, EndsWith, Regex, AllEntries) keep <c>Kind == TermSourceKind.Empty</c>;
    /// only Equals / NotEquals / In / AllIn / OrGroup-of-(Not)Equals slots populate.
    /// The IL emitter consults <see cref="PlanOp.Dispatch"/> to decide which
    /// array to read.
    /// </summary>
    public static TermSource[] ResolveTermSources(QueryPlan plan, IndexSearcher indexSearcher,
        PlanParameters parameters = null, QueryBuilderParameters builderParams = null)
    {
        // IsAllEntries plans never emit term ops (FillFromPostings / AndWith / etc.) —
        // their match[0] is AllEntries, post-filter slots are spatial/vector. No
        // TermSource population is needed.
        if (plan.IsAllEntries)
            return [];

        if (plan.QueryBuilderPlanState is not List<ClauseInfo>  clauses  || clauses.Count == 0)
            return [];

        // Standalone NotEquals: matches[0] = AllEntries (NOT a term source),
        // matches[1] = the negated term. Mirror that layout.
        if (clauses.Count == 1 && clauses[0].IsNegated && !plan.AllNegated)
        {
            var sources = new TermSource[2];
            sources[1] = ResolveSingleTermSource(clauses[0], indexSearcher, plan, parameters, builderParams);
            return sources;
        }

        var termSources = new TermSource[CountMatchSlots(clauses, plan.IsAllEntries, plan.AllNegated)];
        int matchIdx = 0;
        foreach (ClauseInfo clause in clauses)
        {
            if (clause.ClauseType == ClauseType.OrGroup && clause.OrSubClauses != null)
            {
                foreach (var sub in clause.OrSubClauses)
                {
                    if (sub.BoostFactor > 0)
                    {
                        matchIdx++;
                        continue;
                    }
                    termSources[matchIdx++] = ResolveSingleTermSource(sub, indexSearcher, plan, parameters, builderParams);
                }
            }
            else if (clause.ClauseType == ClauseType.AndGroup && clause.AndSubClauses != null)
            {
                foreach (var sub in clause.AndSubClauses)
                {
                    if (sub.BoostFactor > 0)
                    {
                        matchIdx++;
                        continue;
                    }
                    termSources[matchIdx++] = ResolveSingleTermSource(sub, indexSearcher, plan, parameters, builderParams);
                }
            }
            else if ((clause.ClauseType == ClauseType.AllIn || clause.ClauseType == ClauseType.In) && clause.InTerms != null)
            {
                for (int t = 0; t < clause.InTerms.Count; t++)
                    termSources[matchIdx++] = ResolveInTermSource(clause, t, indexSearcher, plan, parameters, builderParams);
            }
            else
            {
                if (clause.BoostFactor > 0)
                {
                    matchIdx++;
                    continue;
                }
                termSources[matchIdx++] = ResolveSingleTermSource(clause, indexSearcher, plan, parameters, builderParams);
            }
        }
        // AllNegated extra slot is AllEntries — stays Empty in TermSources.
        return termSources;
    }

    /// <summary>Resolve a single Equals / NotEquals clause to a posting-list ID and
    /// decode it into a <see cref="TermSource"/>. Returns Empty when the clause
    /// is non-term-shaped or the term doesn't exist in the index.</summary>
    private static TermSource ResolveSingleTermSource(ClauseInfo clause, IndexSearcher indexSearcher,
        QueryPlan plan, PlanParameters parameters, QueryBuilderParameters builderParams)
    {
        if (IsTermSourceEligibleClause(clause) == false)
            return default; // Kind == Empty

        FieldMetadata fieldMeta = ResolveFieldMetadata(clause, indexSearcher, parameters, builderParams);
        long postingListId = GetTermPostingListIdFromParam(clause.PackedParamValue, fieldMeta, indexSearcher, plan);
        return DecodePostingListId(postingListId, indexSearcher);
    }

    /// <summary>Resolve a single In/AllIn term to a posting-list ID.
    /// Uses <paramref name="termIndex"/> into <see cref="ClauseInfo.InTerms"/> /
    /// <see cref="ClauseInfo.InTermTypes"/> to pick the correct numeric vs. string
    /// overload — avoids the long.TryParse false-positive on zero-padded string
    /// values like "000001" (parses as 1L but is indexed as the string "000001").</summary>
    private static TermSource ResolveInTermSource(ClauseInfo clause, int termIndex, IndexSearcher indexSearcher,
        QueryPlan plan, PlanParameters parameters, QueryBuilderParameters builderParams)
    {
        FieldMetadata fieldMeta = builderParams != null ?
            QueryBuilderHelper.GetFieldMetadata(in builderParams, clause.FieldName, hasBoost: builderParams.HasBoost) :
            indexSearcher.FieldMetadataBuilder(clause.FieldName, hasBoost: parameters?.HasBoost ?? false);

        // Null IN terms use string null lookup
        if (clause.InTerms != null && termIndex < clause.InTerms.Count && clause.InTerms[termIndex] == null)
        {
            long nullPostingListId = indexSearcher.GetTermPostingListId(fieldMeta, (string)null);
            return DecodePostingListId(nullPostingListId, indexSearcher);
        }

        var p = clause.PackedParamValue;
        int idx = p.Param1 + termIndex;
        var termPacked = new PackedParam(p.ValueType, idx);
        long postingListId = GetTermPostingListIdFromParam(termPacked, fieldMeta, indexSearcher, plan);
        return DecodePostingListId(postingListId, indexSearcher);
    }

    /// <summary>Resolve field metadata for a term-source clause. Mirrors the
    /// non-Spatial/Vector/Search branch of <see cref="ResolveClause"/>.</summary>
    private static FieldMetadata ResolveFieldMetadata(ClauseInfo clause, IndexSearcher indexSearcher,
        PlanParameters parameters, QueryBuilderParameters builderParams)
    {
        if (builderParams != null)
        {
            string resolvedFieldName = clause.FieldName;
            if (clause.IsExact && builderParams.Metadata.IsDynamic)
                resolvedFieldName = AutoIndexField.GetExactAutoIndexFieldName(resolvedFieldName);
            return QueryBuilderHelper.GetFieldMetadata(in builderParams, resolvedFieldName, exact: clause.IsExact, hasBoost: builderParams.HasBoost);
        }

        return indexSearcher.FieldMetadataBuilder(clause.FieldName, hasBoost: parameters?.HasBoost ?? false);
    }

    /// <summary>Decode a raw posting-list ID (with TermIdMask bits) into a
    /// <see cref="TermSource"/>. Returns Empty when the term doesn't exist (-1).
    /// For PostingList kind, opens a fresh iterator on the underlying set.</summary>
    private static TermSource DecodePostingListId(long postingListId, IndexSearcher indexSearcher)
    {
        if (postingListId == -1)
        {
            return default; // Kind == Empty
        }

        var termType = (global::Corax.Indexing.TermIdMask)postingListId & global::Corax.Indexing.TermIdMask.EnsureIsSingleMask;
        switch (termType)
        {
            case global::Corax.Indexing.TermIdMask.Single:
                return new TermSource
                {
                    Kind = TermSourceKind.Single,
                    SingleEntryId = (long)EntryIdEncodings.GetContainerId(postingListId),
                };

            case global::Corax.Indexing.TermIdMask.SmallPostingList:
                return new TermSource
                {
                    Kind = TermSourceKind.SmallPostingList,
                    SmallPostingListId = (long)EntryIdEncodings.GetContainerId(postingListId),
                };

            case global::Corax.Indexing.TermIdMask.PostingList:
            {
                var postingList = indexSearcher.GetPostingList(postingListId);
                return new TermSource
                {
                    Kind = TermSourceKind.PostingList,
                    LargeIterator = postingList.Iterate(),
                };
            }

            default:
                return default;
        }
    }

    // ── Scan parameter extraction ────────────────────────────────────────

    /// <summary>Extract typed parameter values from clauses for entry scan.
    /// Called per-query at execution time. The values populate the CompiledQueryMatch arrays.</summary>
    public static void ExtractScanParameters(QueryPlan plan, IndexSearcher indexSearcher,
        out long[] longParams, out double[] doubleParams, out Voron.Slice[] sliceParams, out long[] fieldRootPages)
    {
        var predicates = plan.ScanPredicateInfos;
        if (predicates == null || predicates.Length == 0)
        {
            longParams = [];
            doubleParams = [];
            sliceParams = [];
            fieldRootPages = [];
            return;
        }

        var longs = new List<long>();
        var doubles = new List<double>();
        var slices = new List<Voron.Slice>();
        var roots = new List<long>();

        // Walk predicates and clauses in lock-step (same order as BuildScanPredicateInfo visited them).
        // Using field-name search instead would incorrectly return the first matching clause for
        // every predicate when multiple clauses share the same field (e.g. Name != 'a' AND Name != 'b').
        int scanStart = plan.AllNegated ? 0 : 1;
        int clauseIdx = scanStart;
        var clauses = plan.QueryBuilderPlanState as List<ClauseInfo>;
        foreach (ScanPredicateInfo pred in predicates)
        {
            // Advance to the next eligible clause for this predicate.
            ClauseInfo matchingClause =  clauses?[clauseIdx++];
            ExtractParamsFromPredicate(pred, matchingClause, indexSearcher, longs, doubles, slices, roots);
        }

        longParams = longs.Count > 0 ? longs.ToArray() : [];
        doubleParams = doubles.Count > 0 ? doubles.ToArray() : [];
        sliceParams = slices.Count > 0 ? slices.ToArray() : [];
        fieldRootPages = roots.Count > 0 ? roots.ToArray() : [];
    }

    private static void ExtractParamsFromPredicate(ScanPredicateInfo pred, ClauseInfo clause,
        IndexSearcher indexSearcher, List<long> longs, List<double> doubles,
        List<Voron.Slice> slices, List<long> roots)
    {
        if (pred.OrBranches != null)
        {
            // Each OrBranch corresponds to a subclause of the OrGroup.
            // Pass subclauses positionally to avoid the same field-name ambiguity.
            List<ClauseInfo> subClauses = clause?.OrSubClauses;
            for (int b = 0; b < pred.OrBranches.Length; b++)
            {
                ClauseInfo subClause = (subClauses != null && b < subClauses.Count) ? subClauses[b] : null;
                ExtractParamsFromPredicate(pred.OrBranches[b], subClause, indexSearcher, longs, doubles, slices, roots);
            }
            return;
        }

        // Resolve field root page
        roots.Add(indexSearcher.FieldCache.GetLookupRootPage(pred.FieldName));

        if (clause == null)
            return;

        switch (pred.ValueType)
        {
            case ScanValueType.Long:
                if (long.TryParse(clause.TermValue, out long lv))
                    longs.Add(lv);
                if (clause.TermValue2 != null && long.TryParse(clause.TermValue2, out long lv2))
                    longs.Add(lv2);
                break;
            case ScanValueType.Double:
                if (double.TryParse(clause.TermValue,
                    System.Globalization.NumberStyles.Float,
                    System.Globalization.CultureInfo.InvariantCulture, out double dv))
                    doubles.Add(dv);
                if (clause.TermValue2 != null && double.TryParse(clause.TermValue2,
                    System.Globalization.NumberStyles.Float,
                    System.Globalization.CultureInfo.InvariantCulture, out double dv2))
                    doubles.Add(dv2);
                break;
            case ScanValueType.Slice:
                var fieldMeta = indexSearcher.FieldMetadataBuilder(clause.FieldName);
                slices.Add(indexSearcher.EncodeAndApplyAnalyzer(fieldMeta, clause.TermValue));
                if (clause.TermValue2 != null)
                    slices.Add(indexSearcher.EncodeAndApplyAnalyzer(fieldMeta, clause.TermValue2));
                break;
        }
    }

    // ── Highlighting ─────────────────────────────────────────────────────

    /// <summary>
    /// Populate the highlighting terms dictionary from the plan's clauses.
    /// The old CoraxQueryBuilder did this as a side effect during query building.
    /// The bitmap pipeline must do it explicitly after plan building.
    /// </summary>
    public static void PopulateHighlightingTerms(QueryPlan plan, Dictionary<string, CoraxHighlightingTermIndex> highlightingTerms, QueryMetadata metadata)
    {
        if (highlightingTerms == null || plan.QueryBuilderPlanState is not List<ClauseInfo> clauses)
            return;

        foreach (var clauseObj in clauses)
        {
            if (clauseObj?.FieldName == null)
                continue;

            PopulateHighlightingForClause(clauseObj, highlightingTerms, metadata);

            switch (clauseObj.ClauseType)
            {
                // Also handle OrGroup and AndGroup subclauses
                case ClauseType.OrGroup when clauseObj.OrSubClauses != null:
                {
                    foreach (var sub in clauseObj.OrSubClauses)
                        PopulateHighlightingForClause(sub, highlightingTerms, metadata);
                    break;
                }
                case ClauseType.AndGroup when clauseObj.AndSubClauses != null:
                {
                    foreach (var sub in clauseObj.AndSubClauses)
                        PopulateHighlightingForClause(sub, highlightingTerms, metadata);
                    break;
                }
            }
        }
    }

    private static void PopulateHighlightingForClause(ClauseInfo clause, Dictionary<string, CoraxHighlightingTermIndex> highlightingTerms, QueryMetadata metadata)
    {
        string fieldName = clause.FieldName;
        if (fieldName == null)
            return;

        if (highlightingTerms.TryGetValue(fieldName, out var existingTerm))
        {
            // Already populated (e.g., multiple clauses on same field) — update values if needed
            existingTerm.Values ??= GetHighlightingValues(clause);
            return;
        }

        var term = new CoraxHighlightingTermIndex
        {
            FieldName = fieldName,
            Values = GetHighlightingValues(clause)
        };

        if (metadata.IsDynamic && clause.ClauseType == ClauseType.Search)
            term.DynamicFieldName = AutoIndexField.GetSearchAutoIndexFieldName(fieldName);
        else if (metadata.IsDynamic && clause.IsExact)
            term.DynamicFieldName = AutoIndexField.GetExactAutoIndexFieldName(fieldName);

        highlightingTerms[fieldName] = term;

        // For dynamic indexes, also add the dynamic field name variant
        if (term.DynamicFieldName != null)
            highlightingTerms[term.DynamicFieldName] = term;
    }

    private static object GetHighlightingValues(ClauseInfo clause)
    {
        return clause.ClauseType switch
        {
            ClauseType.Between => clause.TermValue != null && clause.TermValue2 != null
                ? new Tuple<string, string>(clause.TermValue, clause.TermValue2)
                : clause.TermValue,
            ClauseType.In when clause.InTerms != null => clause.InTerms,
            _ => clause.TermValue
        };
    }

    // ── Vector / Spatial resolution ──────────────────────────────────────

    /// <summary>
    /// Resolve vector select operations from the plan into CoraxVectorItem instances.
    /// These are NOT materialized yet — the caller materializes them with the bitmap-producing
    /// match as the filterQuery. Returns null if the plan has no vector selects.
    /// </summary>
    public static CoraxVectorItem[] ResolveVectorItems(QueryPlan plan, IndexSearcher indexSearcher,
        PlanParameters parameters = null, QueryBuilderParameters builderParams = null)
    {
        if (plan.VectorSelects == null || plan.VectorSelects.Length == 0)
            return null;

        var items = new CoraxVectorItem[plan.VectorSelects.Length];
        for (int i = 0; i < plan.VectorSelects.Length; i++)
        {
            var clause = plan.VectorSelects[i].Clause as ClauseInfo;
            if (clause == null || clause.ClauseType != ClauseType.Vector || builderParams == null || clause.MethodExpression == null)
                throw new InvalidOperationException("Vector select references an invalid clause at index " + i);

            items[i] = HandleVector(builderParams, clause, false);
        }
        return items;
    }

    private static IQueryMatch HandleSpatial(QueryBuilderParameters builderParameters, ClauseInfo clause, MethodType spatialMethod)
    {
        var metadata = builderParameters.Metadata;
        var index = builderParameters.Index;
        var allocator = builderParameters.Allocator;

        // Field name was pre-resolved during parsing; fall back to AST resolution for
        // dynamic indexes where GetSpatialFieldName needs the spatial sub-expression.
        string fieldName = clause.FieldName;
        if (fieldName == null)
        {
            var expression = clause.MethodExpression;
            if (metadata.IsDynamic == false)
                fieldName = QueryBuilderHelper.ExtractIndexFieldName(metadata.Query, builderParameters.QueryParameters, expression.Arguments[0], metadata);
            else
            {
                var spatialExpression = (MethodExpression)expression.Arguments[0];
                fieldName = metadata.GetSpatialFieldName(spatialExpression, builderParameters.QueryParameters);
            }
        }

        var fieldMetadata = QueryBuilderHelper.GetFieldMetadata(allocator, fieldName, index, builderParameters.IndexFieldsMapping,
            builderParameters.FieldsToFetch, builderParameters.HasDynamics, builderParameters.DynamicFields, hasBoost: builderParameters.HasBoost);

        var distanceErrorPct = clause.SpatialDistanceErrorPct >= 0
            ? clause.SpatialDistanceErrorPct
            : RavenConstants.Documents.Indexing.Spatial.DefaultDistanceErrorPct;

        var spatialField = builderParameters.Factories.GetSpatialFieldFactory(fieldName);
        var units = clause.SpatialUnits ?? spatialField.Units;

        // Build shape from pre-resolved parameters — no GetValue calls
        IShape shape;
        if (clause.IsSpatialCircle)
        {
            shape = spatialField.ReadCircle(clause.SpatialCircleRadius, clause.SpatialCircleLatitude,
                clause.SpatialCircleLongitude, clause.SpatialUnits);
        }
        else if (clause.SpatialWkt != null)
        {
            shape = spatialField.ReadShape(clause.SpatialWkt, clause.SpatialUnits);
        }
        else
        {
            throw new InvalidOperationException("Spatial clause has no pre-resolved shape parameters.");
        }

        var operation = spatialMethod switch
        {
            MethodType.Spatial_Within => global::Corax.Utils.Spatial.SpatialRelation.Within,
            MethodType.Spatial_Disjoint => global::Corax.Utils.Spatial.SpatialRelation.Disjoint,
            MethodType.Spatial_Intersects => global::Corax.Utils.Spatial.SpatialRelation.Intersects,
            MethodType.Spatial_Contains => global::Corax.Utils.Spatial.SpatialRelation.Contains,
            _ => (global::Corax.Utils.Spatial.SpatialRelation)QueryMethod.ThrowMethodNotSupported(spatialMethod, metadata.QueryText, builderParameters.QueryParameters)
        };

        return builderParameters.IndexSearcher.SpatialQuery(fieldMetadata, distanceErrorPct, shape, spatialField.GetContext(), operation, token: builderParameters.Token);
    }

    private static CoraxVectorItem HandleVector(QueryBuilderParameters builderParameters, ClauseInfo clause, bool exact)
    {
        var metadata = builderParameters.Metadata;
        var me = clause.MethodExpression;
        IndexField indexField;
        string embeddingsGenerationTaskIdentifier;

        var minimumMatch = clause.VectorMinimumMatch >= 0
            ? clause.VectorMinimumMatch
            : builderParameters.Index.Configuration.CoraxVectorSearchDefaultMinimumSimilarity;

        int numberOfCandidates = clause.VectorNumberOfCandidates >= 0
            ? clause.VectorNumberOfCandidates
            : builderParameters.Index.Configuration.CoraxVectorDefaultNumberOfCandidatesForQuerying;

        var fieldName = metadata.IsDynamic == false
            ? QueryBuilderHelper.ExtractIndexFieldName(metadata.Query, builderParameters.QueryParameters, me.Arguments[0], metadata)
            : metadata.GetVectorFieldName(me, builderParameters.QueryParameters);

        var fieldMetadata = QueryBuilderHelper.GetFieldMetadata(builderParameters, fieldName, hasBoost: builderParameters.HasBoost);

        // Use pre-resolved vector value and method kind from parsing
        object methodParameter = clause.ResolvedVectorValue;
        ValueTokenType valueTokenType = clause.ResolvedVectorValueType;

        if (clause.VectorMethod != VectorMethodKind.None)
        {
            var method = clause.VectorMethod switch
            {
                VectorMethodKind.ForDocument => VectorHelpers.MethodVectorValue.ForDocument,
                VectorMethodKind.ForRaw => VectorHelpers.MethodVectorValue.ForRaw,
                VectorMethodKind.EmbeddingText => VectorHelpers.MethodVectorValue.EmbeddingText,
                _ => throw new InvalidDataException($"Unknown vector method kind: {clause.VectorMethod}")
            };

            if (method is not VectorHelpers.MethodVectorValue.EmbeddingText)
            {
                return (method, methodParameter) switch
                {
                    (method: VectorHelpers.MethodVectorValue.ForDocument, string docId) => CoraxVectorItem.BuildForDocVector(builderParameters, fieldMetadata, docId, numberOfCandidates, minimumMatch, exact),
                    (method: VectorHelpers.MethodVectorValue.ForDocument, StringSegment docIdSegment) => CoraxVectorItem.BuildForDocVector(builderParameters, fieldMetadata, docIdSegment.Value, numberOfCandidates, minimumMatch, exact),
                    (method: VectorHelpers.MethodVectorValue.ForRaw, string vectorAsBase64) => CoraxVectorItem.BuildSingleVector(builderParameters, fieldMetadata, GenerateEmbeddings.FromBase64Array(VectorOptions.Default, builderParameters.Allocator, vectorAsBase64), numberOfCandidates, minimumMatch, exact),
                    (method: VectorHelpers.MethodVectorValue.ForRaw, StringSegment stringSegmentAsBase64) => CoraxVectorItem.BuildSingleVector(builderParameters, fieldMetadata, GenerateEmbeddings.FromBase64Array(VectorOptions.Default, builderParameters.Allocator, stringSegmentAsBase64.ToString()), numberOfCandidates, minimumMatch, exact),
                    (_, BlittableJsonReaderArray { Length: > 0 }) => throw new InvalidDataException("Cannot perform search on empty value."),
                    _ => throw new InvalidQueryException(
                        $"Unknown method in value ({clause.VectorMethod}. Parameter type: {methodParameter?.GetType().FullName}, Value: {methodParameter}")
                };
            }

            embeddingsGenerationTaskIdentifier = clause.VectorAiTaskName;
            var vectorOptions = VectorHelpers.GetExplicitVectorOptions(builderParameters, fieldName, out indexField);
            if (vectorOptions != null)
            {
                vectorOptions = new VectorOptions()
                {
                    DestinationEmbeddingType = vectorOptions.DestinationEmbeddingType,
                    Dimensions = vectorOptions.Dimensions,
                    SourceEmbeddingType = VectorEmbeddingType.Text,
                    NumberOfCandidatesForIndexing = vectorOptions.NumberOfCandidatesForIndexing,
                    NumberOfEdges = vectorOptions.NumberOfEdges
                };
            }

            var vector = VectorHelpers.GetEmbeddingsForQueryParameter(builderParameters, valueTokenType, methodParameter, embeddingsGenerationTaskIdentifier, vectorOptions, fieldName);

            if (vector.SingleVector != null)
                return CoraxVectorItem.BuildSingleVector(builderParameters, fieldMetadata, vector.SingleVector.Value, numberOfCandidates, minimumMatch, exact);

            return CoraxVectorItem.BuildMultiVector(builderParameters, fieldMetadata, vector.MultiVector, numberOfCandidates, minimumMatch, exact);
        }

        // Direct value (not a method call) — use pre-resolved value
        var value = methodParameter;
        var valueType = valueTokenType;

        (VectorValue? SingleVector, VectorValue[] MultiVector) transformedEmbeddings = (null, null);
        int numberOfDimensions;
        if (VectorHelpers.TryRetrieveEtlTaskName(builderParameters, fieldName, out embeddingsGenerationTaskIdentifier))
        {
            var vectorOptions = VectorHelpers.GetExplicitVectorOptions(builderParameters, fieldName, out indexField);
            transformedEmbeddings = VectorHelpers.GetEmbeddingsForQueryParameter(builderParameters, valueType, value, embeddingsGenerationTaskIdentifier, vectorOptions, fieldName);
        }
        else
        {
            VectorOptions vectorOptions = VectorHelpers.GetOptions(builderParameters, fieldName, out indexField);

            if (builderParameters.Index.IndexFieldsPersistence.TryReadNumberOfDimensions(fieldName, out numberOfDimensions) == false)
                return CoraxVectorItem.BuildEmpty(builderParameters); // no vector indexed
            if (vectorOptions.SourceEmbeddingType is VectorEmbeddingType.Text)
            {
                transformedEmbeddings = VectorHelpers.GetVectorValueForTextualInput(builderParameters, vectorOptions, valueType, value);
            }
            else
            {
                switch (value)
                {
                    case string s:
                        transformedEmbeddings.SingleVector = GenerateEmbeddings.FromBase64Array(vectorOptions, builderParameters.Allocator, s);
                        break;
                    case StringSegment stringSegment:
                        transformedEmbeddings.SingleVector = GenerateEmbeddings.FromBase64Array(vectorOptions, builderParameters.Allocator, stringSegment.ToString());
                        break;
                    case BlittableJsonReaderObject bjro:
                        transformedEmbeddings.SingleVector = VectorHelpers.GetVectorValueFromRavenVector(builderParameters, bjro, vectorOptions);
                        break;
                    case BlittableJsonReaderArray { Length: > 0 } bjra:
                    {
                        var isRavenVector = bjra[0] is BlittableJsonReaderObject;
                        var isStringArray = bjra[0] is string or StringSegment or LazyStringValue;
                        var isArray = bjra[0] is BlittableJsonReaderArray;

                        if (isRavenVector == false && isStringArray == false && isArray == false)
                        {
                            transformedEmbeddings.SingleVector = VectorHelpers.GetVectorValueFromNumericalBlittableArray(builderParameters, bjra, vectorOptions);
                        }
                        else
                        {
                            var embeddings = new VectorValue[bjra.Length];
                            for (int i = 0; i < bjra.Length; ++i)
                            {
                                if (isRavenVector)
                                    embeddings[i] = VectorHelpers.GetVectorValueFromRavenVector(builderParameters, (BlittableJsonReaderObject)bjra[i], vectorOptions);
                                else if (isStringArray)
                                    embeddings[i] = GenerateEmbeddings.FromBase64Array(vectorOptions, builderParameters.Allocator, bjra[i].ToString());
                                else
                                    embeddings[i] = VectorHelpers.GetVectorValueFromNumericalBlittableArray(builderParameters, (BlittableJsonReaderArray)bjra[i],
                                        vectorOptions);
                            }

                            transformedEmbeddings.MultiVector = embeddings;
                        }

                        break;
                    }
                    default:
                        PortableExceptions.Throw<InvalidDataException>("We expected to get vector(s), however got: " + value.GetType().Name);
                        break;
                }
            }
        }

        if (builderParameters.Index.IndexFieldsPersistence.TryReadNumberOfDimensions(fieldName, out numberOfDimensions) == false)
            return CoraxVectorItem.BuildEmpty(builderParameters); // no vector indexed

        if (transformedEmbeddings.SingleVector != null)
        {
            var singleVector = transformedEmbeddings.SingleVector.Value;

            if (indexField != null)
                AssertDimensions(singleVector);
            return CoraxVectorItem.BuildSingleVector(builderParameters, fieldMetadata, singleVector, numberOfCandidates, minimumMatch, exact);
        }

        if (transformedEmbeddings.MultiVector != null)
        {
            var multiVector = transformedEmbeddings.MultiVector;

            if (indexField != null)
            {
                foreach (var vector in multiVector)
                    AssertDimensions(vector);
            }
            return CoraxVectorItem.BuildMultiVector(builderParameters, fieldMetadata, multiVector, numberOfCandidates, minimumMatch, exact);
        }

        throw new InvalidDataException("Expected to get single or multiple embeddings of VectorValue type but none was provided");

        void AssertDimensions(in VectorValue vector)
        {
            if (numberOfDimensions != vector.Length)
            {
                using (vector)
                    VectorHelpers.ThrowDifferentNumberOfDimensions(indexField, fieldName, vector, numberOfDimensions);
            }
        }
    }

    private static class VectorHelpers
    {
        public enum MethodVectorValue
        {
            ForDocument,
            ForRaw,
            EmbeddingText
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryRetrieveEtlTaskName(QueryBuilderParameters builderParameters, in string fieldName, out string embeddingsGenerationTaskIdentifier)
        {
            var existsInPersistence =
                builderParameters.Index.IndexFieldsPersistence.TryReadEmbeddingsGenerationTaskIdentifier(fieldName, out embeddingsGenerationTaskIdentifier);

            if (builderParameters.Metadata.IsDynamic == false)
                return existsInPersistence;

            if (((builderParameters.FieldsToFetch != null && builderParameters.FieldsToFetch.IndexFields.TryGetValue(fieldName, out var indexField)) || (builderParameters.Index.Definition.IndexFields.TryGetValue(fieldName, out indexField))) && indexField.Vector is AutoVectorOptions avo)
            {
                embeddingsGenerationTaskIdentifier = avo.EmbeddingsGenerationTaskIdentifier;
                return string.IsNullOrEmpty(avo.EmbeddingsGenerationTaskIdentifier) == false;
            }

            embeddingsGenerationTaskIdentifier = null;
            return false;
        }

        internal static (VectorValue? SingleVector, VectorValue[] MultiVector) GetVectorValueForTextualInput(QueryBuilderParameters parameters, VectorOptions vectorOptions, ValueTokenType valueType, object value)
        {
            if (valueType is ValueTokenType.String)
                return (GenerateEmbeddings.FromText(parameters.Allocator, vectorOptions, value.ToString()), null);

            if (valueType is not ValueTokenType.Parameter)
                PortableExceptions.Throw<InvalidDataException>($"Cannot use vector.search() on a text field with a non-string value. Got {valueType}");

            if (value is BlittableJsonReaderArray valueAsList)
            {
                var embeddings = new VectorValue[valueAsList.Length];
                for (var i = 0; i < valueAsList.Length; ++i)
                    embeddings[i] = GenerateEmbeddings.FromText(parameters.Allocator, vectorOptions, valueAsList[i].ToString());

                return (null, embeddings);
            }

            PortableExceptions.Throw<InvalidDataException>($"Cannot use vector.search() on a text field with a non-string value(s). Got {valueType}");
            return (null, null);
        }

        internal static VectorValue GetVectorValueFromRavenVector(QueryBuilderParameters parameters, BlittableJsonReaderObject json, VectorOptions vectorOptions)
        {
            var vectorObjectFound = json.TryGetMember(Sparrow.Global.Constants.Naming.VectorPropertyName, out var vectorObject);
            PortableExceptions.ThrowIfNot<InvalidDataException>(vectorObjectFound, "Cannot find vector property in the object.");

            var vectorReader = (BlittableJsonReaderVector)vectorObject;
            return QueryBuilderHelper.GetVectorValueFromBlittableJsonVectorReader(parameters.Allocator, vectorOptions, vectorReader);
        }

        internal static VectorValue GetVectorValueFromNumericalBlittableArray(QueryBuilderParameters parameters, BlittableJsonReaderArray array, VectorOptions vectorOptions)
        {
            var bytesUsed = array.Length * (vectorOptions.SourceEmbeddingType is VectorEmbeddingType.Single ? sizeof(float) : 1);
            var memScope = parameters.Allocator.Allocate(bytesUsed, out Memory<byte> mem);
            ref var floatRef = ref MemoryMarshal.GetReference(MemoryMarshal.Cast<byte, float>(mem.Span));
            ref var sbyteRef = ref MemoryMarshal.GetReference(MemoryMarshal.Cast<byte, sbyte>(mem.Span));
            ref var byteRef = ref MemoryMarshal.GetReference(mem.Span);

            for (int i = 0; i < array.Length; ++i)
            {
                switch (vectorOptions.SourceEmbeddingType)
                {
                    case VectorEmbeddingType.Single:
                        Unsafe.Add(ref floatRef, i) = array.GetByIndex<float>(i);
                        break;
                    case VectorEmbeddingType.Int8:
                        Unsafe.Add(ref sbyteRef, i) = array.GetByIndex<sbyte>(i);
                        break;
                    default:
                        Unsafe.AddByteOffset(ref byteRef, i) = array.GetByIndex<byte>(i);
                        break;
                }
            }

            return GenerateEmbeddings.FromArray(parameters.Allocator, memScope, mem, vectorOptions, bytesUsed);
        }

        internal static VectorOptions GetExplicitVectorOptions(QueryBuilderParameters builderParameters, in string fieldName, out IndexField indexField)
        {
            if ((builderParameters.FieldsToFetch != null && builderParameters.FieldsToFetch.IndexFields.TryGetValue(fieldName, out indexField)) == false
                && (builderParameters.Index.Definition.IndexFields.TryGetValue(fieldName, out indexField)) == false)
                PortableExceptions.Throw<InvalidDataException>($"Cannot find `{fieldName}` field in the index.");

            return indexField.Vector;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static VectorOptions GetOptions(QueryBuilderParameters builderParameters, in string fieldName, out IndexField indexField)
        {
            if ((builderParameters.FieldsToFetch != null && builderParameters.FieldsToFetch.IndexFields.TryGetValue(fieldName, out indexField)) == false
                && (builderParameters.Index.Definition.IndexFields.TryGetValue(fieldName, out indexField)) == false)
                PortableExceptions.Throw<InvalidDataException>($"Cannot find `{fieldName}` field in the index.");

            // VectorOptions can be null when a user does not specify the configuration.
            // In such cases, we will choose the input depending on the value type (similar to how we handle it during indexing).
            if (indexField.Vector != null)
                return indexField.Vector;

            builderParameters.Index.IndexFieldsPersistence.TryReadVectorSourceEmbeddingType(fieldName, out var vectorSourceEmbeddingType);

            var defaultVectorOptions = vectorSourceEmbeddingType switch
            {
                VectorEmbeddingType.Single => VectorOptions.Default,
                VectorEmbeddingType.Text => VectorOptions.DefaultText,
                _ => throw new InvalidDataException(
                    $"Unknown vector source embedding type: {vectorSourceEmbeddingType}. Implicit configuration support only single and text vector source embedding types.")
            };

            indexField.Vector = defaultVectorOptions;

            return defaultVectorOptions;
        }

        internal static void ThrowDifferentNumberOfDimensions(in IndexField indexField, in string fieldName, in VectorValue transformedEmbedding,
            in int numberOfDimensions)
        {
            var (storedDimensions, inputDimensions) = indexField.Vector.DestinationEmbeddingType switch
            {
                VectorEmbeddingType.Single => (numberOfDimensions / sizeof(float), transformedEmbedding.Length / sizeof(float)),
                VectorEmbeddingType.Int8 => (numberOfDimensions - sizeof(float), transformedEmbedding.Length - sizeof(float)),
                VectorEmbeddingType.Binary => (numberOfDimensions, transformedEmbedding.Length),
                _ => throw new InvalidDataException($"Unexpected embedding type - {numberOfDimensions}.")
            };

            PortableExceptions.Throw<InvalidDataException>(
                $"Vector field `{fieldName}` has {storedDimensions} dimensions, but the vector passed to vector.search() has {inputDimensions} dimensions.");
        }

        internal static (VectorValue? SingleVector, VectorValue[] MultiVector) GetEmbeddingsForQueryParameter(QueryBuilderParameters builderParameters, ValueTokenType valueType,
            object value,
            string embeddingsGenerationTaskIdentifier, VectorOptions vectorOptions, string fieldName)
        {
            var database = builderParameters.Index.DocumentDatabase;

            var embeddingsTaskId = new EmbeddingsGenerationTaskIdentifier(embeddingsGenerationTaskIdentifier);

            var embeddingsGenerator = database.EmbeddingsGeneratorQueries;

            var sourceEmbeddingType = embeddingsGenerator.GetQuantizationOf(embeddingsTaskId);

            // Quantized dynamic field indicates that the task generated embeddings with different quantization than requested in the index
            // In this case we want to use quantization defined in dynamic field (which was set in CurrentIndexingScope.GetLoadVectorField)
            VectorEmbeddingType destinationEmbeddingType;
            if (builderParameters.Metadata.IsDynamic)
            {
                if (sourceEmbeddingType is not VectorEmbeddingType.Single)
                    destinationEmbeddingType = sourceEmbeddingType;
                else
                    destinationEmbeddingType = vectorOptions!.DestinationEmbeddingType;
            }
            else
            {
                destinationEmbeddingType = vectorOptions?.DestinationEmbeddingType ?? sourceEmbeddingType;
            }

            ReadOnlyMemory<ReadOnlyMemory<byte>> embeddingValues;

            switch (valueType)
            {
                case ValueTokenType.String:
                    embeddingValues = embeddingsGenerator
                        .GetEmbeddingsForQuery(builderParameters.DocumentsContext, embeddingsTaskId, value.ToString());
                    break;
                case ValueTokenType.Parameter:
                {
                    if (value is not BlittableJsonReaderArray bjra)
                        throw new InvalidQueryException($"Expected array as parameter of vector.search({fieldName}) method, got '{value.GetType().FullName}' type instead.");

                    var values = new string[bjra.Length];

                    for (var i = 0; i < values.Length; i++)
                        values[i] = bjra[i].ToString();

                    embeddingValues = embeddingsGenerator
                        .GetEmbeddingsForQuery(builderParameters.DocumentsContext, embeddingsTaskId, values);
                    break;
                }
                default:
                    throw new NotSupportedException($"Unexpected value type provided as parameter to vector.search({fieldName}) method. Got '{value.GetType().FullName}' type.");
            }

            var queryingVectorOption = new VectorOptions
            {
                SourceEmbeddingType = sourceEmbeddingType,
                DestinationEmbeddingType = destinationEmbeddingType
            };

            if (embeddingValues.Length == 1)
            {
                var embeddingValue = embeddingValues.Span[0];

                return (GenerateEmbeddings.FromArray(builderParameters.Allocator, embeddingValue.Span, queryingVectorOption), null);
            }
            else
            {
                var vectorValues = new VectorValue[embeddingValues.Length];

                for (int i = 0; i < embeddingValues.Length; i++)
                {
                    var embeddingValue = embeddingValues.Span[i];

                    vectorValues[i] = GenerateEmbeddings.FromArray(builderParameters.Allocator, embeddingValue.Span, queryingVectorOption);
                }

                return (null, vectorValues);
            }
        }
    }

    // ── Sorting / Ordering ───────────────────────────────────────────────

    public static OrderMetadata[] GetSortMetadata(QueryBuilderParameters builderParameters, out bool hasEmpty)
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
                if (builderParameters.Metadata.HasVectorSearch == false)
                    builderParameters.IndexReadOperation?.AssertCanOrderByScoreAutomaticallyWhenBoostingOrVectorSearchIsInvolved();

                return [new OrderMetadata(true, MatchCompareFieldType.Score)];
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
                    Random.Shared.Next();
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
                return indexSearcher.OrderBy(match, orderMetadata[0], builderParameters.Index.Configuration.NullFirst, take, builderParameters.Token);
            default:
                return indexSearcher.OrderBy(match, orderMetadata, builderParameters.Index.Configuration.NullFirst, take, builderParameters.Token);
        }
    }

    /// <summary>Apply ORDER BY from plan metadata when a full <see cref="QueryBuilderParameters"/> is not
    /// available (e.g., direct tests). Handles <c>ORDER BY score()</c> only — callers that need
    /// field / spatial / alphanumeric sorts must use the full
    /// <see cref="OrderBy(QueryBuilderParameters,IQueryMatch,in OrderMetadata[],bool)"/> overload.</summary>
    public static IQueryMatch ApplyScoreOrdering(PlanParameters planParams, IQueryMatch match, long take, CancellationToken token = default)
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
                return indexSearcher.OrderBy(match, meta, nullFirst: false, take: takeInt, token: token);
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
        QueryBuilderParameters builderParams,
        PlanParameters parameters)
    {
        var result = searchMeta;
        var indexFieldsMapping = builderParams?.IndexFieldsMapping ?? parameters?.IndexFieldsMapping;

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

    /// <summary>Split search term value respecting quoted phrases.
    /// "nonexists \"second third\" nonexsts" -> ["nonexists", "second third", "nonexsts"]
    /// Same logic as old CoraxQueryBuilder.GetValues().</summary>
    private static IEnumerable<string> SplitSearchTerms(string value)
    {
        if (string.IsNullOrEmpty(value))
        {
            yield return value;
            yield break;
        }

        bool quoted = false;
        int lastStart = 0;

        for (int i = 0; i < value.Length; i++)
        {
            char c = value[i];
            if (c == '"')
            {
                if (i > 0 && value[i - 1] == '\\')
                    continue; // escaped quote

                if (lastStart != i)
                    yield return value.Substring(lastStart, i - lastStart);

                quoted = !quoted;
                lastStart = i + 1;
            }
            else if ((c == ' ' || c == '\t') && !quoted)
            {
                if (lastStart != i)
                    yield return value.Substring(lastStart, i - lastStart);
                lastStart = i + 1;
            }
        }

        if (value.Length - lastStart > 0)
            yield return value.Substring(lastStart, value.Length - lastStart);
    }
}
