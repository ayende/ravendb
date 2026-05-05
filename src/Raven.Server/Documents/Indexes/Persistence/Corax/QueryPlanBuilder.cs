using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Corax.Querying;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Primitives;
using Voron.Data.RoaringBitmaps;
using Corax.Querying.Matches.SortingMatches.Meta;
using Corax.Querying.Planning;
using Corax.Mappings;
using Corax.Utils;
using Raven.Client;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Corax;
using VectorOptions = Raven.Client.Documents.Indexes.Vector.VectorOptions;
using Raven.Server.Documents.AI.Embeddings;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;
using Raven.Server.Documents.Indexes;
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
/// Builds a QueryPlan from a parsed RQL query. Replaces CoraxQueryBuilder
/// for query execution in Corax 2.0.
///
/// Expression types handled:
/// - BinaryExpression (AND, OR, =, !=, >, >=, <, <=)
/// - BetweenExpression
/// - InExpression
/// - NegatedExpression
/// - TrueExpression (constant folding)
/// - MethodExpression (search, startsWith, endsWith, exists, boost, exact, regex)
///
/// MoreLikeThis is handled by a separate execution path (Index.cs → reader.MoreLikeThis())
/// and never reaches this planner.
/// </summary>
internal static class QueryPlanBuilder
{
    /// <summary>
    /// Parameters needed by the planner for field metadata resolution,
    /// analyzer setup, and cardinality estimation.
    /// </summary>
    internal class PlanParameters
    {
        public IndexSearcher IndexSearcher;
        public QueryMetadata Metadata;
        public BlittableJsonReaderObject QueryParameters;
        public Raven.Server.Documents.Indexes.Index Index;
        public global::Corax.Mappings.IndexFieldsMapping IndexFieldsMapping;
        public FieldsToFetch FieldsToFetch;
        public ByteStringContext Allocator;
        public CancellationToken Token;
        public bool HasDynamics;
        public Lazy<List<string>> DynamicFields;
        public bool HasBoost;
    }

    public static QueryPlan BuildPlan(
        IndexSearcher indexSearcher,
        QueryMetadata metadata,
        BlittableJsonReaderObject queryParameters,
        CancellationToken token)
    {
        return BuildPlan(new PlanParameters
        {
            IndexSearcher = indexSearcher,
            Metadata = metadata,
            QueryParameters = queryParameters,
            Token = token
        });
    }

    /// <summary>
    /// One-stop entry point that runs the full plan-build → cache-lookup → IL-emit →
    /// match-resolve → param-extract → wrap-with-post-filters pipeline. Replaces five
    /// hand-rolled call sites in CoraxIndexReadOperation, CoraxIndexFacetedReadOperation,
    /// and CoraxQueryBuilder which all did the exact same dance.
    ///
    /// Today this is a strict refactor — BuildPlan still runs every call. The
    /// master/compiled split where the AST-walk and clause-extraction are cached by
    /// queryText is task #82's remaining stages and lands later.
    /// </summary>
    public static IQueryMatch BuildAndCompile(
        PlanParameters planParams,
        QueryBuilderParameters builderParameters,
        long take,
        out QueryPlan plan,
        Dictionary<string, CoraxHighlightingTermIndex> highlightingTerms,
        CancellationToken token)
    {
        plan = BuildPlan(planParams);
        var indexSearcher = planParams.IndexSearcher;
        var queryText = planParams.Metadata.Query.QueryText;

        if (planParams.HasBoost)
        {
            // When BM25 scoring is needed, UseTermSource=true skips Fill() on the
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
                    ops[i].UseTermSource = false;
            plan.OperandOrdering |= (1 << 30);
        }

        var planCache = indexSearcher.PlanCache;
        var compiledPlan = planCache.Get(queryText, plan.OperandOrdering, plan.TypeSignature, plan.FullKinds);
        if (compiledPlan == null)
        {
            // Capture the QueryPlan in a closure so the EXPLAIN string is generated only
            // when something reads it (Inspect() / EXPLAIN diagnostics). Most queries
            // never pay this cost.
            var capturedPlan = plan;
            compiledPlan = new CompiledPlan
            {
                CompiledDelegate = QueryILEmitter.EmitDelegate(plan),
                ExplainSourceProvider = () => QueryILEmitter.GenerateExplainSource(capturedPlan),
                Ordering = plan.OperandOrdering,
                TypeSignature = plan.TypeSignature,
                FullKinds = plan.FullKinds
            };
            planCache.Add(queryText, compiledPlan);
        }

        var resolvedMatches = ResolveMatches(plan, indexSearcher, planParams, builderParameters);
        var termSources = ResolveTermSources(plan, indexSearcher, planParams, builderParameters);
        ExtractScanParameters(plan, indexSearcher,
            out var longParams, out var doubleParams, out var sliceParams, out var fieldRootPages);

        if (highlightingTerms != null)
            PopulateHighlightingTerms(plan, highlightingTerms, planParams.Metadata);

        IQueryMatch result = new CompiledQueryMatch(
            compiledPlan, plan.RequiredBitmaps, plan.Ops?.Length ?? 0, resolvedMatches, termSources,
            longParams, doubleParams, sliceParams, fieldRootPages,
            indexSearcher, planParams.Allocator, take, token);

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
            for (int vs = 0; vs < vectorItems.Length; vs++)
                result = vectorItems[vs].Materialize(vectorFilter);
        }

        return result;
    }

    /// <summary>
    /// Master-plan-shape cache (#82 α). Per-IndexSearcher map of query text → cached
    /// parsed shape (List<ClauseInfo> with AST refs preserved, no resolved values
    /// from the live params). The IndexSearcher is held via a CWT-style shape table:
    /// each call to BuildPlan looks up the per-searcher cache lazily and creates it
    /// if absent. When the IndexSearcher dies, the per-searcher cache becomes
    /// unreachable and is reclaimed by GC.
    ///
    /// Cardinality estimation, clause sorting, and op emission still run live every
    /// call — only the AST walk and ClauseInfo construction are cached.
    /// </summary>
    private static readonly System.Runtime.CompilerServices.ConditionalWeakTable<IndexSearcher, System.Collections.Concurrent.ConcurrentDictionary<string, ParsedShape>> _shapeCachesBySearcher = new();

    private sealed class ParsedShape
    {
        public List<ClauseInfo> Clauses;
        public BooleanOp RootOp;
        public bool HasMixedAndOr;
    }

    public static QueryPlan BuildPlan(PlanParameters p)
    {
        var query = p.Metadata.Query;
        var indexSearcher = p.IndexSearcher;
        var queryParameters = p.QueryParameters;
        var token = p.Token;
        var metadata = p.Metadata;
        if (query.Where == null)
            return BuildAllEntriesPlan();

        var queryText = query.QueryText;
        var perSearcherCache = _shapeCachesBySearcher.GetValue(indexSearcher,
            _ => new System.Collections.Concurrent.ConcurrentDictionary<string, ParsedShape>());

        List<ClauseInfo> clauses;
        bool hasMixedAndOr;
        BooleanOp rootOp;
        if (perSearcherCache.TryGetValue(queryText, out var cachedShape))
        {
            // Cache hit — clone the parsed shape and refresh values from the call's params.
            // The parse result is structural (AST refs + clause types); the per-call
            // values (TermValue / InTerms / BoostFactor / FieldName-when-dynamic) are
            // re-derived against queryParameters here.
            clauses = new List<ClauseInfo>(cachedShape.Clauses.Count);
            foreach (var src in cachedShape.Clauses)
                clauses.Add(CloneAndRefreshClause(src, queryParameters, metadata));
            rootOp = cachedShape.RootOp;
            hasMixedAndOr = cachedShape.HasMixedAndOr;
        }
        else
        {
            hasMixedAndOr = false;
            clauses = new List<ClauseInfo>();
            rootOp = ParseExpression(query.Where, indexSearcher, clauses, queryParameters, metadata, ref hasMixedAndOr);

            // Constant-folded queries (True/False) skip caching — they're handled by the
            // direct All-Entries / Empty-plan paths below and never reach the cardinality /
            // ordering / emit path that would benefit from shape reuse.
            if (rootOp != BooleanOp.True && rootOp != BooleanOp.False && clauses.Count > 0)
            {
                // Cache an immutable deep-clone so subsequent in-flight Cardinality /
                // sort mutations on `clauses` do not alias the cached shape.
                var cachedList = new List<ClauseInfo>(clauses.Count);
                foreach (var c in clauses)
                    cachedList.Add(CloneClauseTemplate(c));
                perSearcherCache.TryAdd(queryText, new ParsedShape
                {
                    Clauses = cachedList,
                    RootOp = rootOp,
                    HasMixedAndOr = hasMixedAndOr
                });
            }
        }

        // Mixed AND/OR trees are handled via OrGroup clauses

        if (rootOp == BooleanOp.True)
            return BuildAllEntriesPlan();
        if (rootOp == BooleanOp.False)
            return BuildEmptyPlan();
        if (clauses.Count == 0)
            return BuildAllEntriesPlan();

        // Estimate cardinalities
        foreach (var clause in clauses)
        {
            if (clause.Cardinality < 0)
                clause.Cardinality = EstimateCardinality(clause, indexSearcher);
        }

        // Determine top-level operation
        bool isOr = rootOp == BooleanOp.Or;

        // Separate spatial and vector clauses from the filter chain.
        // For AND queries, spatial/vector execute AFTER the bitmap filter:
        //   FilterOps -> SpatialFilters -> VectorSelects
        // For OR queries, spatial/vector remain in the flat chain (they produce
        // candidate sets that are OR'd together).
        List<ClauseInfo> spatialClauses = null;
        List<ClauseInfo> vectorClauses = null;
        if (!isOr)
        {
            for (int i = clauses.Count - 1; i >= 0; i--)
            {
                if (clauses[i].ClauseType == ClauseType.Spatial)
                {
                    spatialClauses ??= new List<ClauseInfo>();
                    spatialClauses.Add(clauses[i]);
                    clauses.RemoveAt(i);
                }
                else if (clauses[i].ClauseType == ClauseType.Vector)
                {
                    vectorClauses ??= new List<ClauseInfo>();
                    vectorClauses.Add(clauses[i]);
                    clauses.RemoveAt(i);
                }
            }

            if (clauses.Count == 0)
            {
                if (spatialClauses != null)
                {
                    // Purely spatial (or spatial + vector): AllEntries as filter base,
                    // spatial narrows it, then vector selects from the result.
                    var plan = BuildAllEntriesPlan();
                    AttachPostFilterPhases(plan, spatialClauses, vectorClauses);
                    return plan;
                }

                // Purely vector with no other filter clauses: put vector clauses back
                // into the flat plan. The bitmap pipeline handles deduplication for
                // multi-vector results, and VectorSearchMatch runs unfiltered.
                if (vectorClauses != null)
                {
                    for (int i = vectorClauses.Count - 1; i >= 0; i--)
                        clauses.Add(vectorClauses[i]);
                    vectorClauses = null;
                }
            }
        }

        // Sort AND operands: non-negated first (ascending cardinality), then negated.
        // Negated clauses (NotEquals, IsNegated) can only subtract from an existing
        // bitmap via ANDNOT — they must never be the seed (first) operand.
        if (!isOr)
        {
            clauses.Sort((a, b) =>
            {
                bool aNeg = a.IsNegated || a.ClauseType == ClauseType.NotEquals;
                bool bNeg = b.IsNegated || b.ClauseType == ClauseType.NotEquals;
                if (aNeg != bNeg)
                    return aNeg ? 1 : -1; // non-negated first
                return a.Cardinality.CompareTo(b.Cardinality);
            });
        }
        else
        {
            // For OR chains: move AndGroups to the front so the AND sub-chain always
            // seeds slot 0 directly (no third bitmap slot needed for scratch).
            // OR is commutative — reordering is safe.
            int insertPos = 0;
            for (int j = 0; j < clauses.Count; j++)
            {
                if (clauses[j].ClauseType == ClauseType.AndGroup)
                {
                    ClauseInfo ag = clauses[j];
                    clauses.RemoveAt(j);
                    clauses.Insert(insertPos++, ag);
                }
            }
        }

        // Build PlanOp array
        var result = EmitPlan(clauses, isOr);

        // Attach spatial/vector post-filter phases to the plan
        if (spatialClauses != null || vectorClauses != null)
            AttachPostFilterPhases(result, spatialClauses, vectorClauses);

        return result;
    }

    /// <summary>Attach spatial and vector post-filter phases to a query plan.
    /// Spatial/vector clauses are stored in the plan's Clauses array at known indices,
    /// and SpatialFilters/VectorSelects reference those indices for resolution at execution time.</summary>
    private static void AttachPostFilterPhases(QueryPlan plan, List<ClauseInfo> spatialClauses, List<ClauseInfo> vectorClauses)
    {
        if (spatialClauses == null && vectorClauses == null)
            return;

        // Append spatial and vector clauses to the plan's Clauses array.
        // Their match indices are computed relative to the existing clauses.
        var existingClauses = plan.Clauses ?? Array.Empty<object>();
        int existingCount = existingClauses.Length;

        // Count the total match slots already used by existing clauses
        // (OrGroups/In/AllIn expand to multiple matches)
        int existingMatchCount = 0;
        // AllEntries plans have an implicit match at index 0 not in Clauses
        if (plan.IsAllEntries)
            existingMatchCount = 1;
        for (int i = 0; i < existingCount; i++)
        {
            if (existingClauses[i] is ClauseInfo ci)
            {
                if (ci.ClauseType == ClauseType.OrGroup && ci.OrSubClauses != null)
                    existingMatchCount += ci.OrSubClauses.Count;
                else if (ci.ClauseType == ClauseType.AndGroup && ci.AndSubClauses != null)
                    existingMatchCount += ci.AndSubClauses.Count;
                else if ((ci.ClauseType == ClauseType.AllIn || ci.ClauseType == ClauseType.In) && ci.InTerms != null)
                    existingMatchCount += ci.InTerms.Count;
                else
                    existingMatchCount++;
            }
        }
        // Account for AllNegated extra slot (AllEntries appended by ResolveMatches)
        if (plan.AllNegated)
            existingMatchCount++;

        int spatialCount = spatialClauses?.Count ?? 0;
        int vectorCount = vectorClauses?.Count ?? 0;
        int totalExtra = spatialCount + vectorCount;

        var newClauses = new object[existingCount + totalExtra];
        Array.Copy(existingClauses, newClauses, existingCount);

        int matchIndex = existingMatchCount;

        if (spatialClauses != null)
        {
            plan.SpatialFilters = new SpatialFilterOp[spatialCount];
            for (int i = 0; i < spatialCount; i++)
            {
                newClauses[existingCount + i] = spatialClauses[i];
                plan.SpatialFilters[i] = new SpatialFilterOp { MatchIndex = matchIndex++, Clause = spatialClauses[i] };
            }
        }

        if (vectorClauses != null)
        {
            plan.VectorSelects = new VectorSelectOp[vectorCount];
            for (int i = 0; i < vectorCount; i++)
            {
                newClauses[existingCount + spatialCount + i] = vectorClauses[i];
                plan.VectorSelects[i] = new VectorSelectOp { MatchIndex = matchIndex++, Clause = vectorClauses[i] };
            }
        }

        plan.Clauses = newClauses;
    }

    private enum BooleanOp { And, Or, True, False, Leaf }

    private static BooleanOp ParseExpression(
        QueryExpression expr,
        IndexSearcher indexSearcher,
        List<ClauseInfo> clauses,
        BlittableJsonReaderObject queryParameters,
        QueryMetadata metadata,
        ref bool hasMixedAndOr)
    {
        switch (expr)
        {
            case BinaryExpression be:
                return ParseBinaryExpression(be, indexSearcher, clauses, queryParameters, metadata, ref hasMixedAndOr);

            case BetweenExpression between:
                ParseBetween(between, clauses, queryParameters, metadata);
                return BooleanOp.Leaf;

            case InExpression inExpr:
                ParseIn(inExpr, clauses, queryParameters, metadata);
                return BooleanOp.Leaf;

            case NegatedExpression negated:
                ParseNegated(negated, indexSearcher, clauses, queryParameters, metadata, ref hasMixedAndOr);
                return BooleanOp.Leaf;

            case TrueExpression:
                return BooleanOp.True;

            case MethodExpression method:
                ParseMethod(method, indexSearcher, clauses, queryParameters, metadata, ref hasMixedAndOr);
                return BooleanOp.Leaf;

            default:
                throw new InvalidOperationException(
                    $"Unexpected expression type {expr.GetType().Name} in WHERE clause.");
        }
    }

    private static BooleanOp ParseBinaryExpression(
        BinaryExpression be,
        IndexSearcher indexSearcher,
        List<ClauseInfo> clauses,
        BlittableJsonReaderObject queryParameters,
        QueryMetadata metadata,
        ref bool hasMixedAndOr)
    {
        switch (be.Operator)
        {
            case OperatorType.And:
            {
                // For AND, handle OR sub-expressions as grouped clauses
                BooleanOp left, right;

                if (be.Left is BinaryExpression { Operator: OperatorType.Or })
                {
                    // Left side is OR — parse into a separate clause list and group them
                    var orClauses = new List<ClauseInfo>();
                    left = ParseExpression(be.Left, indexSearcher, orClauses, queryParameters, metadata, ref hasMixedAndOr);
                    clauses.Add(new ClauseInfo
                    {
                        ClauseType = ClauseType.OrGroup,
                        OrSubClauses = orClauses,
                        OriginalIndex = clauses.Count
                    });
                }
                else
                {
                    left = ParseExpression(be.Left, indexSearcher, clauses, queryParameters, metadata, ref hasMixedAndOr);
                }

                if (be.Right is BinaryExpression { Operator: OperatorType.Or })
                {
                    var orClauses = new List<ClauseInfo>();
                    right = ParseExpression(be.Right, indexSearcher, orClauses, queryParameters, metadata, ref hasMixedAndOr);
                    clauses.Add(new ClauseInfo
                    {
                        ClauseType = ClauseType.OrGroup,
                        OrSubClauses = orClauses,
                        OriginalIndex = clauses.Count
                    });
                }
                else
                {
                    right = ParseExpression(be.Right, indexSearcher, clauses, queryParameters, metadata, ref hasMixedAndOr);
                }
                // Constant folding
                if (left == BooleanOp.True) return right;
                if (right == BooleanOp.True) return left;
                if (left == BooleanOp.False || right == BooleanOp.False) return BooleanOp.False;
                return BooleanOp.And;
            }

            case OperatorType.Or:
            {
                BooleanOp left, right;

                if (be.Left is BinaryExpression { Operator: OperatorType.And })
                {
                    // Left is an AND sub-expression — parse into a separate list and wrap as AndGroup
                    var andClauses = new List<ClauseInfo>();
                    left = ParseExpression(be.Left, indexSearcher, andClauses, queryParameters, metadata, ref hasMixedAndOr);
                    clauses.Add(new ClauseInfo
                    {
                        ClauseType = ClauseType.AndGroup,
                        AndSubClauses = andClauses,
                        OriginalIndex = clauses.Count
                    });
                }
                else
                {
                    left = ParseExpression(be.Left, indexSearcher, clauses, queryParameters, metadata, ref hasMixedAndOr);
                }

                if (be.Right is BinaryExpression { Operator: OperatorType.And })
                {
                    var andClauses = new List<ClauseInfo>();
                    right = ParseExpression(be.Right, indexSearcher, andClauses, queryParameters, metadata, ref hasMixedAndOr);
                    clauses.Add(new ClauseInfo
                    {
                        ClauseType = ClauseType.AndGroup,
                        AndSubClauses = andClauses,
                        OriginalIndex = clauses.Count
                    });
                }
                else
                {
                    right = ParseExpression(be.Right, indexSearcher, clauses, queryParameters, metadata, ref hasMixedAndOr);
                }

                // Constant folding
                if (left == BooleanOp.True || right == BooleanOp.True) return BooleanOp.True;
                if (left == BooleanOp.False) return right;
                if (right == BooleanOp.False) return left;
                return BooleanOp.Or;
            }

            case OperatorType.Equal:
                ParseComparison(be, clauses, queryParameters, metadata);
                return BooleanOp.Leaf;

            case OperatorType.NotEqual:
                ParseComparison(be, clauses, queryParameters, metadata);
                return BooleanOp.Leaf;

            case OperatorType.LessThan:
            case OperatorType.LessThanEqual:
            case OperatorType.GreaterThan:
            case OperatorType.GreaterThanEqual:
                ParseRangeComparison(be, clauses, queryParameters, metadata);
                return BooleanOp.Leaf;

            default:
                throw new InvalidOperationException(
                    $"Unexpected binary operator {be.Operator} in WHERE clause.");
        }
    }

    /// <summary>
    /// Build a clone of <paramref name="src"/> suitable for storage in the master-plan
    /// shape cache: copies all the AST refs and structural fields, but resets per-call
    /// state (Cardinality, resolved values) so the cached entry stays a clean template.
    /// Recurses through OrSubClauses so nested OrGroup shapes are also templates.
    /// </summary>
    private static ClauseInfo CloneClauseTemplate(ClauseInfo src)
    {
        var dst = new ClauseInfo
        {
            FieldName = src.FieldName,
            ClauseType = src.ClauseType,
            OriginalIndex = src.OriginalIndex,
            IsNegated = src.IsNegated,
            IsExact = src.IsExact,
            BoostFactor = src.BoostFactor,
            MethodExpression = src.MethodExpression,
            FieldExpr = src.FieldExpr,
            TermExpr = src.TermExpr,
            TermExpr2 = src.TermExpr2,
            InTermExprs = src.InTermExprs,
            BoostFactorExpr = src.BoostFactorExpr,
            // Cardinality intentionally left at -1 (default) — re-estimated per call.
        };
        if (src.OrSubClauses != null)
        {
            dst.OrSubClauses = new List<ClauseInfo>(src.OrSubClauses.Count);
            foreach (var sub in src.OrSubClauses)
                dst.OrSubClauses.Add(CloneClauseTemplate(sub));
        }
        if (src.AndSubClauses != null)
        {
            dst.AndSubClauses = new List<ClauseInfo>(src.AndSubClauses.Count);
            foreach (var sub in src.AndSubClauses)
                dst.AndSubClauses.Add(CloneClauseTemplate(sub));
        }
        return dst;
    }

    /// <summary>
    /// Produce a per-call ClauseInfo from a cached shape entry: copy AST refs and
    /// structural fields, then re-resolve every parameter-dependent value
    /// (FieldName, TermValue, TermValue2, InTerms, BoostFactor) against the live
    /// queryParameters. Cardinality is left at -1 so the existing estimation pass
    /// runs with current data.
    /// </summary>
    private static ClauseInfo CloneAndRefreshClause(ClauseInfo src,
        BlittableJsonReaderObject queryParameters, QueryMetadata metadata)
    {
        var dst = new ClauseInfo
        {
            ClauseType = src.ClauseType,
            OriginalIndex = src.OriginalIndex,
            IsNegated = src.IsNegated,
            IsExact = src.IsExact,
            MethodExpression = src.MethodExpression,
            FieldExpr = src.FieldExpr,
            TermExpr = src.TermExpr,
            TermExpr2 = src.TermExpr2,
            InTermExprs = src.InTermExprs,
            BoostFactorExpr = src.BoostFactorExpr,
        };

        // Field name: dynamic when the AST has a FieldExpression with parameter parts;
        // for the common static case, GetFieldName returns the same string every call.
        dst.FieldName = src.FieldExpr is FieldExpression fe
            ? GetFieldName(fe, metadata, queryParameters)
            : src.FieldName;

        if (src.TermExpr != null)
        {
            dst.TermValue = GetTermValue(src.TermExpr, queryParameters, out var tt);
            dst.TermValueType = tt;
        }
        else
        {
            dst.TermValue = src.TermValue;
            dst.TermValueType = src.TermValueType;
        }

        if (src.TermExpr2 != null)
            dst.TermValue2 = GetTermValue(src.TermExpr2, queryParameters);
        else
            dst.TermValue2 = src.TermValue2;

        if (src.InTermExprs != null)
        {
            var terms = new List<string>(src.InTermExprs.Count);
            var termTypes = new List<ValueTokenType>(src.InTermExprs.Count);
            bool hasTime = false;
            foreach (var expr in src.InTermExprs)
            {
                if (expr is ValueExpression ve)
                {
                    var resolved = ve.GetValue(queryParameters);
                    if (resolved is Sparrow.Json.BlittableJsonReaderArray arr)
                    {
                        for (int a = 0; a < arr.Length; a++)
                        {
                            var elem = arr[a];
                            terms.Add(ConvertInValue(elem, ref hasTime));
                            termTypes.Add(GetInTermValueTokenType(elem, ValueTokenType.Parameter));
                        }
                    }
                    else
                    {
                        terms.Add(ConvertInValue(resolved, ref hasTime));
                        termTypes.Add(GetInTermValueTokenType(resolved, ve.Value));
                    }
                }
            }
            dst.InTerms = terms;
            dst.InTermTypes = termTypes;
        }

        if (src.BoostFactorExpr != null)
        {
            var factorStr = GetTermValue(src.BoostFactorExpr, queryParameters);
            float boostFactor = 1f;
            if (factorStr != null)
                float.TryParse(factorStr,
                    System.Globalization.NumberStyles.Float,
                    System.Globalization.CultureInfo.InvariantCulture, out boostFactor);
            dst.BoostFactor = boostFactor;
        }
        else
        {
            dst.BoostFactor = src.BoostFactor;
        }

        if (src.OrSubClauses != null)
        {
            dst.OrSubClauses = new List<ClauseInfo>(src.OrSubClauses.Count);
            foreach (var sub in src.OrSubClauses)
                dst.OrSubClauses.Add(CloneAndRefreshClause(sub, queryParameters, metadata));
        }

        if (src.AndSubClauses != null)
        {
            dst.AndSubClauses = new List<ClauseInfo>(src.AndSubClauses.Count);
            foreach (var sub in src.AndSubClauses)
                dst.AndSubClauses.Add(CloneAndRefreshClause(sub, queryParameters, metadata));
        }

        return dst;
    }

    private static void ParseComparison(BinaryExpression be, List<ClauseInfo> clauses,
        BlittableJsonReaderObject queryParameters, QueryMetadata metadata)
    {
        if (be.Left is not FieldExpression field)
            return;
        string fieldName = GetFieldName(field, metadata, queryParameters);
        string termValue = GetTermValue(be.Right, queryParameters, out var valueType);

        clauses.Add(new ClauseInfo
        {
            FieldName = fieldName,
            TermValue = termValue,
            TermValueType = valueType,
            ClauseType = be.Operator == OperatorType.NotEqual ? ClauseType.NotEquals : ClauseType.Equals,
            OriginalIndex = clauses.Count,
            FieldExpr = field,
            TermExpr = be.Right
        });
    }

    private static void ParseRangeComparison(BinaryExpression be, List<ClauseInfo> clauses,
        BlittableJsonReaderObject queryParameters, QueryMetadata metadata)
    {
        if (be.Left is not FieldExpression field)
            return;
        string fieldName = GetFieldName(field, metadata, queryParameters);
        string termValue = GetTermValue(be.Right, queryParameters, out var valueType);

        clauses.Add(new ClauseInfo
        {
            FieldName = fieldName,
            TermValue = termValue,
            TermValueType = valueType,
            ClauseType = be.Operator switch
            {
                OperatorType.GreaterThan => ClauseType.GreaterThan,
                OperatorType.GreaterThanEqual => ClauseType.GreaterThanOrEqual,
                OperatorType.LessThan => ClauseType.LessThan,
                OperatorType.LessThanEqual => ClauseType.LessThanOrEqual,
                _ => ClauseType.Equals
            },
            OriginalIndex = clauses.Count,
            FieldExpr = field,
            TermExpr = be.Right
        });
    }

    private static void ParseBetween(BetweenExpression between, List<ClauseInfo> clauses,
        BlittableJsonReaderObject queryParameters, QueryMetadata metadata)
    {
        if (between.Source is not FieldExpression field)
            return;

        var minValue = GetTermValue(between.Min, queryParameters, out var minType);
        var maxValue = GetTermValue(between.Max, queryParameters, out _);
        clauses.Add(new ClauseInfo
        {
            FieldName = GetFieldName(field, metadata, queryParameters),
            TermValue = minValue,
            TermValue2 = maxValue,
            TermValueType = minType,
            ClauseType = ClauseType.Between,
            OriginalIndex = clauses.Count,
            FieldExpr = field,
            TermExpr = between.Min,
            TermExpr2 = between.Max
        });
    }

    private static void ParseIn(InExpression inExpr, List<ClauseInfo> clauses,
        BlittableJsonReaderObject queryParameters, QueryMetadata metadata)
    {
        if (inExpr.Source is not FieldExpression field)
            return;

        var terms = new List<string>();
        var termTypes = new List<ValueTokenType>();
        var inTermExprs = new List<QueryExpression>();
        bool hasTime = false;
        foreach (var value in inExpr.Values)
        {
            if (value is ValueExpression ve)
            {
                inTermExprs.Add(ve);
                var resolvedValue = ve.GetValue(queryParameters);
                if (resolvedValue is Sparrow.Json.BlittableJsonReaderArray arr)
                {
                    for (int a = 0; a < arr.Length; a++)
                    {
                        var elem = arr[a];
                        terms.Add(ConvertInValue(elem, ref hasTime));
                        termTypes.Add(GetInTermValueTokenType(elem, ValueTokenType.Parameter));
                    }
                }
                else
                {
                    terms.Add(ConvertInValue(resolvedValue, ref hasTime));
                    termTypes.Add(GetInTermValueTokenType(resolvedValue, ve.Value));
                }
            }
        }

        clauses.Add(new ClauseInfo
        {
            FieldName = GetFieldName(field, metadata, queryParameters),
            InTerms = terms,
            InTermTypes = termTypes,
            ClauseType = inExpr.All ? ClauseType.AllIn : ClauseType.In,
            OriginalIndex = clauses.Count,
            FieldExpr = field,
            InTermExprs = inTermExprs
        });
    }

    private static void ParseNegated(NegatedExpression negated, IndexSearcher indexSearcher,
        List<ClauseInfo> clauses, BlittableJsonReaderObject queryParameters, QueryMetadata metadata, ref bool hasMixedAndOr)
    {
        // NOT expr → ANDNOT with all entries
        var innerClauses = new List<ClauseInfo>();
        ParseExpression(negated.Expression, indexSearcher, innerClauses, queryParameters, metadata, ref hasMixedAndOr);

        foreach (var inner in innerClauses)
        {
            inner.IsNegated = true;
            clauses.Add(inner);
        }
    }

    private static void ParseMethod(MethodExpression method, IndexSearcher indexSearcher,
        List<ClauseInfo> clauses, BlittableJsonReaderObject queryParameters, QueryMetadata metadata, ref bool hasMixedAndOr)
    {
        var methodType = QueryMethod.GetMethodType(method.Name.Value);
        switch (methodType)
        {
            case MethodType.Search:
                ParseSearchMethod(method, clauses, queryParameters, metadata);
                break;

            case MethodType.StartsWith:
                ParsePrefixMethod(method, clauses, queryParameters, metadata, ClauseType.StartsWith);
                break;

            case MethodType.EndsWith:
                ParsePrefixMethod(method, clauses, queryParameters, metadata, ClauseType.EndsWith);
                break;

            case MethodType.Exists:
                if (method.Arguments.Count > 0 && method.Arguments[0] is FieldExpression existsField)
                {
                    clauses.Add(new ClauseInfo
                    {
                        FieldName = GetFieldName(existsField, metadata, queryParameters),
                        ClauseType = ClauseType.Exists,
                        OriginalIndex = clauses.Count,
                        FieldExpr = existsField
                    });
                }
                break;

            case MethodType.Exact:
            {
                // exact(expr) → recurse, then mark all new clauses as exact
                int beforeCount = clauses.Count;
                if (method.Arguments.Count > 0)
                    ParseExpression(method.Arguments[0], indexSearcher, clauses, queryParameters, metadata, ref hasMixedAndOr);
                for (int c = beforeCount; c < clauses.Count; c++)
                    clauses[c].IsExact = true;
                break;
            }

            case MethodType.Boost:
            {
                // boost(expr, factor) → recurse, then set boost factor on new clauses
                int beforeCount = clauses.Count;
                if (method.Arguments.Count > 0)
                    ParseExpression(method.Arguments[0], indexSearcher, clauses, queryParameters, metadata, ref hasMixedAndOr);
                float boostFactor = 1f;
                QueryExpression boostFactorExpr = null;
                if (method.Arguments.Count > 1)
                {
                    boostFactorExpr = method.Arguments[1];
                    var factorStr = GetTermValue(boostFactorExpr, queryParameters);
                    if (factorStr != null)
                        float.TryParse(factorStr, System.Globalization.NumberStyles.Float,
                            System.Globalization.CultureInfo.InvariantCulture, out boostFactor);
                }
                for (int c = beforeCount; c < clauses.Count; c++)
                {
                    clauses[c].BoostFactor = boostFactor;
                    clauses[c].BoostFactorExpr = boostFactorExpr;
                }
                break;
            }

            case MethodType.Regex:
                if (method.Arguments.Count >= 2 && method.Arguments[0] is FieldExpression regexField)
                {
                    clauses.Add(new ClauseInfo
                    {
                        FieldName = GetFieldName(regexField, metadata, queryParameters),
                        TermValue = GetTermValue(method.Arguments[1], queryParameters),
                        ClauseType = ClauseType.Regex,
                        OriginalIndex = clauses.Count,
                        FieldExpr = regexField,
                        TermExpr = method.Arguments[1]
                    });
                }
                break;

            case MethodType.Spatial_Within:
            case MethodType.Spatial_Contains:
            case MethodType.Spatial_Disjoint:
            case MethodType.Spatial_Intersects:
                // Spatial queries are resolved at execution time via the existing
                // CoraxQueryBuilder.HandleSpatial infrastructure.
                clauses.Add(new ClauseInfo
                {
                    ClauseType = ClauseType.Spatial,
                    MethodExpression = method,
                    OriginalIndex = clauses.Count
                });
                break;

            case MethodType.Vector_Search:
                clauses.Add(new ClauseInfo
                {
                    ClauseType = ClauseType.Vector,
                    MethodExpression = method,
                    OriginalIndex = clauses.Count
                });
                break;

            case MethodType.MoreLikeThis:
                // MoreLikeThis method in a WHERE clause acts as "all entries" —
                // the actual MLT logic is in the separate reader.MoreLikeThis() path.
                // When it appears in a filter expression, treat as no-op (all entries match).
                break;

            case MethodType.When:
            {
                // when(condition, expr) — evaluate the constant condition at plan time.
                // If false, produce no clause (empty result for this branch).
                // If true, recurse into the inner expression.
                if (method.Arguments.Count != 2)
                    break;
                var conditionResult = QueryBuilderHelper.EvaluateConstantExpressionForWhenQuery(
                    (BinaryExpression)method.Arguments[0], queryParameters);
                if (conditionResult)
                    ParseExpression(method.Arguments[1], indexSearcher, clauses, queryParameters, metadata, ref hasMixedAndOr);
                // If false, we simply don't add any clause — the branch is eliminated.
                break;
            }

            default:
                throw new InvalidOperationException(
                    $"Unexpected method '{method.Name.Value}' ({methodType}) in WHERE clause.");
        }
    }

    private static void ParseSearchMethod(MethodExpression method, List<ClauseInfo> clauses,
        BlittableJsonReaderObject queryParameters, QueryMetadata metadata)
    {
        if (method.Arguments.Count < 2)
            return;

        if (method.Arguments[0] is not FieldExpression searchField)
            return;

        clauses.Add(new ClauseInfo
        {
            FieldName = GetFieldName(searchField, metadata, queryParameters),
            TermValue = GetTermValue(method.Arguments[1], queryParameters),
            ClauseType = ClauseType.Search,
            OriginalIndex = clauses.Count,
            FieldExpr = searchField,
            TermExpr = method.Arguments[1]
        });
    }

    private static void ParsePrefixMethod(MethodExpression method, List<ClauseInfo> clauses,
        BlittableJsonReaderObject queryParameters, QueryMetadata metadata, ClauseType type)
    {
        if (method.Arguments.Count < 2)
            return;

        if (method.Arguments[0] is not FieldExpression field)
            return;

        clauses.Add(new ClauseInfo
        {
            FieldName = GetFieldName(field, metadata, queryParameters),
            TermValue = GetTermValue(method.Arguments[1], queryParameters),
            ClauseType = type,
            OriginalIndex = clauses.Count,
            FieldExpr = field,
            TermExpr = method.Arguments[1]
        });
    }

    /// <summary>Convert an IN value to its string representation, handling booleans and dates.</summary>
    private static string ConvertInValue(object value, ref bool hasTime)
    {
        if (value == null)
            return null;
        if (value is bool b)
            return b ? "true" : "false";
        // Check for date/time values — convert to ticks string for Corax
        if (value is DateTime dt)
        {
            hasTime = true;
            return dt.Ticks.ToString(System.Globalization.CultureInfo.InvariantCulture);
        }
        if (value is DateTimeOffset dto)
        {
            hasTime = true;
            return dto.UtcDateTime.Ticks.ToString(System.Globalization.CultureInfo.InvariantCulture);
        }
        // LazyStringValue from Blittable — might be a date string
        var str = value.ToString();
        if (str != null && str.Length > 18 && str.Length < 35 && str.Contains('T')
            && DateTime.TryParse(str, System.Globalization.CultureInfo.InvariantCulture,
                System.Globalization.DateTimeStyles.RoundtripKind, out var parsed))
        {
            hasTime = true;
            return parsed.Ticks.ToString(System.Globalization.CultureInfo.InvariantCulture);
        }
        return str;
    }

    /// <summary>Split search term value respecting quoted phrases.
    /// "nonexists \"second third\" nonexsts" → ["nonexists", "second third", "nonexsts"]
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

    /// <summary>Extract field name from a FieldExpression.
    /// Uses the full compound path (Date.Year stays Date.Year).
    /// When metadata is available, uses GetIndexFieldName for proper alias resolution.</summary>
    private static string GetFieldName(FieldExpression field)
    {
        return field.FieldValue;
    }

    /// <summary>Extract field name with proper alias resolution using query metadata.</summary>
    private static string GetFieldName(FieldExpression field, QueryMetadata metadata, BlittableJsonReaderObject queryParameters)
    {
        if (metadata != null)
            return metadata.GetIndexFieldName(field, queryParameters).Value;
        return field.FieldValue;
    }

    /// <summary>Determine the <see cref="ValueTokenType"/> for an individual In-clause term.
    /// <paramref name="literalType"/> is the AST token type; for Parameter tokens the type
    /// is inferred from the resolved value (matches the logic in <see cref="ConvertInValue"/>).</summary>
    private static ValueTokenType GetInTermValueTokenType(object resolvedValue, ValueTokenType literalType)
    {
        if (literalType != ValueTokenType.Parameter)
            return literalType;
        // Parameter expansion — infer from runtime value
        if (resolvedValue is bool b) return b ? ValueTokenType.True : ValueTokenType.False;
        // Date/time values are stored as ticks (long) in Corax — must match ConvertInValue logic
        if (resolvedValue is DateTime or DateTimeOffset) return ValueTokenType.Long;
        if (resolvedValue is long or int) return ValueTokenType.Long;
        if (resolvedValue is double or float or decimal) return ValueTokenType.Double;
        if (resolvedValue is Sparrow.Json.LazyNumberValue lnv)
            return lnv.TryParseLong(out _) ? ValueTokenType.Long : ValueTokenType.Double;
        // LazyStringValue or plain string — check if it's a date string (same pattern as ConvertInValue)
        var str = resolvedValue?.ToString();
        if (str != null && str.Length > 18 && str.Length < 35 && str.Contains('T')
            && DateTime.TryParse(str, System.Globalization.CultureInfo.InvariantCulture,
                System.Globalization.DateTimeStyles.RoundtripKind, out _))
            return ValueTokenType.Long;
        return ValueTokenType.String;
    }

    private static string GetTermValue(QueryExpression expr, BlittableJsonReaderObject queryParameters)
    {
        return GetTermValue(expr, queryParameters, out _);
    }

    private static string GetTermValue(QueryExpression expr, BlittableJsonReaderObject queryParameters, out ValueTokenType valueType)
    {
        if (expr is ValueExpression ve)
        {
            valueType = ve.Value;
            var value = ve.GetValue(queryParameters);
            if (value is bool b)
                return b ? "true" : "false"; // Corax stores booleans as lowercase
            // For parameters, detect the actual type from the resolved value
            if (valueType == ValueTokenType.Parameter && value != null)
            {
                if (value is bool)
                    valueType = (bool)value ? ValueTokenType.True : ValueTokenType.False;
                else if (value is long or int)
                    valueType = ValueTokenType.Long;
                else if (value is double or float or decimal)
                    valueType = ValueTokenType.Double;
                else if (value is Sparrow.Json.LazyNumberValue lnv)
                {
                    // LazyNumberValue wraps JSON numbers — try long first, then double
                    if (lnv.TryParseLong(out _))
                        valueType = ValueTokenType.Long;
                    else
                        valueType = ValueTokenType.Double;
                }
                else
                    valueType = ValueTokenType.String;
            }
            return value?.ToString();
        }
        valueType = ValueTokenType.Null;
        return null;
    }

    private static long EstimateCardinality(ClauseInfo clause, IndexSearcher indexSearcher)
    {
        switch (clause.ClauseType)
        {
            case ClauseType.Equals:
                return indexSearcher.NumberOfDocumentsUnderSpecificTerm(
                    indexSearcher.FieldMetadataBuilder(clause.FieldName), clause.TermValue);

            case ClauseType.NotEquals:
            case ClauseType.GreaterThan:
            case ClauseType.GreaterThanOrEqual:
            case ClauseType.LessThan:
            case ClauseType.LessThanOrEqual:
            case ClauseType.Between:
            case ClauseType.Exists:
            case ClauseType.StartsWith:
            case ClauseType.EndsWith:
            case ClauseType.Search:
            case ClauseType.Regex:
                // Use field-level cardinality as upper bound
                return indexSearcher.GetTermAmountInField(
                    indexSearcher.FieldMetadataBuilder(clause.FieldName));

            case ClauseType.In:
            case ClauseType.AllIn:
                // Sum of individual term cardinalities
                long sum = 0;
                var meta = indexSearcher.FieldMetadataBuilder(clause.FieldName);
                if (clause.InTerms != null)
                foreach (var term in clause.InTerms)
                    sum += indexSearcher.NumberOfDocumentsUnderSpecificTerm(meta, term);
                return Math.Min(sum, indexSearcher.NumberOfEntries);

            case ClauseType.Spatial:
            case ClauseType.Vector:
                // Spatial and vector can't be estimated cheaply — use field total as upper bound
                return indexSearcher.NumberOfEntries;

            case ClauseType.OrGroup:
                long orSum = 0;
                if (clause.OrSubClauses != null)
                {
                    foreach (var sub in clause.OrSubClauses)
                    {
                        if (sub.Cardinality < 0)
                            sub.Cardinality = EstimateCardinality(sub, indexSearcher);
                        orSum += sub.Cardinality;
                    }
                }
                return Math.Min(orSum, indexSearcher.NumberOfEntries);

            case ClauseType.AndGroup:
                // AND of sub-clauses: cardinality is bounded by the minimum sub-clause cardinality.
                long andMin = indexSearcher.NumberOfEntries;
                if (clause.AndSubClauses != null)
                {
                    foreach (var sub in clause.AndSubClauses)
                    {
                        if (sub.Cardinality < 0)
                            sub.Cardinality = EstimateCardinality(sub, indexSearcher);
                        if (sub.Cardinality < andMin)
                            andMin = sub.Cardinality;
                    }
                }
                return andMin;

            default:
                return indexSearcher.NumberOfEntries;
        }
    }

    private static QueryPlan EmitPlan(List<ClauseInfo> clauses, bool isOr)
    {
        var ops = new List<PlanOp>();
        bool needsThreeBitmaps = false;

        if (isOr)
        {
            // OR chain — expand In/OrGroup terms into individual OR ops
            int matchIndex = 0;
            for (int i = 0; i < clauses.Count; i++)
            {
                if ((clauses[i].ClauseType == ClauseType.In || clauses[i].ClauseType == ClauseType.AllIn) && clauses[i].InTerms != null)
                {
                    // Each IN term is a single-term lookup → eligible for native dispatch.
                    foreach (var _ in clauses[i].InTerms)
                    {
                        ops.Add(new PlanOp
                        {
                            Kind = matchIndex == 0 ? PlanOpKind.FillFromPostings : PlanOpKind.OrWithPostings,
                            ParamIndex = matchIndex,
                            EstimatedCardinality = clauses[i].Cardinality / clauses[i].InTerms.Count,
                            UseTermSource = true
                        });
                        matchIndex++;
                    }
                }
                else if (clauses[i].ClauseType == ClauseType.OrGroup && clauses[i].OrSubClauses != null)
                {
                    foreach (var sub in clauses[i].OrSubClauses)
                    {
                        ops.Add(new PlanOp
                        {
                            Kind = matchIndex == 0 ? PlanOpKind.FillFromPostings : PlanOpKind.OrWithPostings,
                            ParamIndex = matchIndex,
                            EstimatedCardinality = clauses[i].Cardinality / clauses[i].OrSubClauses.Count,
                            UseTermSource = IsTermSourceEligibleClause(sub)
                        });
                        matchIndex++;
                    }
                }
                else if (clauses[i].ClauseType == ClauseType.AndGroup && clauses[i].AndSubClauses != null)
                {
                    // AND sub-expression inside an OR chain.
                    // Only supported when the AND group is the first element (matchIndex == 0)
                    // or can be merged into slot 0 via OrBitmaps after computing into slot 1.
                    var subClauses = clauses[i].AndSubClauses;
                    if (matchIndex == 0)
                    {
                        // First element: build the AND chain directly in slot 0.
                        // Slot 1 is free (unused) so AndWithPostings can use it as scratch.
                        // Suppress early-exit on AND steps — the OR chain continues regardless.
                        ops.Add(new PlanOp
                        {
                            Kind = PlanOpKind.FillFromPostings,
                            ParamIndex = matchIndex,
                            EstimatedCardinality = subClauses[0].Cardinality,
                            UseTermSource = IsTermSourceEligibleClause(subClauses[0])
                        });
                        for (int s = 1; s < subClauses.Count; s++)
                        {
                            ops.Add(new PlanOp
                            {
                                Kind = PlanOpKind.AndWithPostings,
                                ParamIndex = matchIndex + s,
                                EstimatedCardinality = subClauses[s].Cardinality,
                                UseTermSource = IsTermSourceEligibleClause(subClauses[s]),
                                SkipEarlyExit = true // don't abort on empty — remaining OR terms may still match
                            });
                        }
                    }
                    else
                    {
                        // Non-first AND group: save the accumulated OR result (slot 0) to slot 2,
                        // build this AND sub-chain fresh in slot 0, then OR slot 2 back.
                        needsThreeBitmaps = true;
                        // Uses SwapBitmaps(0, 2): slot 0 ↔ slot 2.
                        // Slot 2 must have been cleared before the swap (it's either the initial
                        // empty state, or was cleared at the end of the previous iteration).
                        ops.Add(new PlanOp { Kind = PlanOpKind.ClearBitmap, BitmapLocal = 1 });
                        ops.Add(new PlanOp
                        {
                            Kind = PlanOpKind.SwapBitmaps,
                            BitmapLocal = 0,
                            ParamIndex2 = 2
                        });
                        // Slot 0 is now fresh (was slot 2 = cleared); slot 2 = prior OR accumulation.
                        ops.Add(new PlanOp
                        {
                            Kind = PlanOpKind.FillFromPostings,
                            ParamIndex = matchIndex,
                            EstimatedCardinality = subClauses[0].Cardinality,
                            UseTermSource = IsTermSourceEligibleClause(subClauses[0])
                        });
                        for (int s = 1; s < subClauses.Count; s++)
                        {
                            ops.Add(new PlanOp
                            {
                                Kind = PlanOpKind.AndWithPostings,
                                ParamIndex = matchIndex + s,
                                BitmapLocal = 0,
                                EstimatedCardinality = subClauses[s].Cardinality,
                                UseTermSource = IsTermSourceEligibleClause(subClauses[s]),
                                SkipEarlyExit = true // don't abort — OR chain continues
                            });
                        }
                        // OR the saved prior accumulation (slot 2) back into the AND result (slot 0).
                        ops.Add(new PlanOp
                        {
                            Kind = PlanOpKind.OrBitmaps,
                            BitmapLocal = 0,
                            ParamIndex2 = 2
                        });
                        // Clear slot 2 so it's clean for the next non-first AndGroup iteration.
                        ops.Add(new PlanOp { Kind = PlanOpKind.ClearBitmap, BitmapLocal = 2 });
                    }
                    matchIndex += subClauses.Count;
                }
                else
                {
                    bool isNotEqualsInOr = clauses[i].ClauseType == ClauseType.NotEquals;
                    if (isNotEqualsInOr)
                    {
                        // NotEquals in OR chain: OR of NOT(X) clauses cannot use the raw term posting list
                        // (FillBitmapFromTermSource would add entries WITH X, not entries WITHOUT X).
                        // Mark the clause so ResolveMatches creates AllEntries ANDNOT TermQuery instead.
                        clauses[i].IsOrChainNotEquals = true;
                    }
                    ops.Add(new PlanOp
                    {
                        Kind = matchIndex == 0 ? PlanOpKind.FillFromPostings : PlanOpKind.OrWithPostings,
                        ParamIndex = matchIndex,
                        EstimatedCardinality = clauses[i].Cardinality,
                        UseTermSource = isNotEqualsInOr ? false : IsTermSourceEligibleClause(clauses[i])
                    });
                    matchIndex++;
                }
            }
            ops.Add(new PlanOp { Kind = PlanOpKind.IterateInto });
        }
        else if (clauses.Count == 1 && clauses[0].ClauseType == ClauseType.Equals)
        {
            // Single equality — direct iterate, no bitmap
            ops.Add(new PlanOp
            {
                Kind = PlanOpKind.DirectIterate,
                ParamIndex = 0,
                EstimatedCardinality = clauses[0].Cardinality
            });
        }
        else if (clauses.Count == 1 && clauses[0].ClauseType == ClauseType.NotEquals)
        {
            // Standalone NotEquals: AllEntries ANDNOT term
            // ParamIndex 0 = the AllEntries match (NOT a term source — keep on IQueryMatch path),
            // ParamIndex 1 = the negated term (eligible for native dispatch).
            ops.Add(new PlanOp
            {
                Kind = PlanOpKind.FillFromPostings,
                ParamIndex = 0, // Will be resolved to AllEntries
                EstimatedCardinality = long.MaxValue // AllEntries — exact count not needed for plan
            });
            ops.Add(new PlanOp
            {
                Kind = PlanOpKind.AndNotWithPostings,
                ParamIndex = 1, // Will be resolved to the negated term
                EstimatedCardinality = clauses[0].Cardinality,
                UseTermSource = IsTermSourceEligibleClause(clauses[0])
            });
            ops.Add(new PlanOp { Kind = PlanOpKind.IterateInto });

            // Mark clause so ResolveMatches produces [AllEntries, TermMatch]
            clauses[0].IsNegated = true;

            return new QueryPlan
            {
                Ops = ops.ToArray(),
                OperandOrdering = 0,
                Clauses = clauses.ToArray()
            };
        }
        else
        {
            // AND chain: Fill smallest non-negated, then AndWith/AndNotWith remaining.
            // If the first clause is negated (all clauses are negated), we need to
            // start from AllEntries and ANDNOT each one.
            bool firstIsNegated = clauses[0].IsNegated || clauses[0].ClauseType == ClauseType.NotEquals;
            int startIndex;

            if (firstIsNegated)
            {
                // All clauses are negated — start from all entries.
                // AllEntries match is appended AFTER all clause-expanded matches by ResolveMatches.
                // Compute the total match count (In/OrGroup/AndGroup expand to multiple slots).
                int totalMatchCount = 0;
                foreach (var c in clauses)
                {
                    if ((c.ClauseType == ClauseType.In || c.ClauseType == ClauseType.AllIn) && c.InTerms != null)
                        totalMatchCount += c.InTerms.Count;
                    else if (c.ClauseType == ClauseType.OrGroup && c.OrSubClauses != null)
                        totalMatchCount += c.OrSubClauses.Count;
                    else if (c.ClauseType == ClauseType.AndGroup && c.AndSubClauses != null)
                        totalMatchCount += c.AndSubClauses.Count;
                    else
                        totalMatchCount++;
                }
                ops.Add(new PlanOp
                {
                    Kind = PlanOpKind.FillFromPostings,
                    ParamIndex = totalMatchCount, // Index of AllEntries in resolved matches
                    EstimatedCardinality = long.MaxValue
                });
                startIndex = 0; // Process all clauses as ANDNOT
            }
            else
            {
                startIndex = 1;
            }

            // Build match index mapping — OrGroups expand to multiple matches
            // First, compute match index for clause 0
            int matchIndex = 0;
            if (!firstIsNegated)
            {
                if (clauses[0].ClauseType == ClauseType.OrGroup && clauses[0].OrSubClauses != null)
                {
                    var subClauses = clauses[0].OrSubClauses;
                    for (int s = 0; s < subClauses.Count; s++)
                    {
                        ops.Add(new PlanOp
                        {
                            Kind = s == 0 ? PlanOpKind.FillFromPostings : PlanOpKind.OrWithPostings,
                            ParamIndex = matchIndex + s,
                            BitmapLocal = 0,
                            EstimatedCardinality = subClauses[s].Cardinality,
                            UseTermSource = IsTermSourceEligibleClause(subClauses[s])
                        });
                    }
                    matchIndex += subClauses.Count;
                }
                else if (clauses[0].ClauseType == ClauseType.In && clauses[0].InTerms != null)
                {
                    // IN at seed: OR all terms into bitmap[0]. Each IN term is a single-term lookup.
                    var terms = clauses[0].InTerms;
                    for (int t = 0; t < terms.Count; t++)
                    {
                        ops.Add(new PlanOp
                        {
                            Kind = t == 0 ? PlanOpKind.FillFromPostings : PlanOpKind.OrWithPostings,
                            ParamIndex = matchIndex + t,
                            BitmapLocal = 0,
                            EstimatedCardinality = clauses[0].Cardinality / terms.Count,
                            UseTermSource = true
                        });
                    }
                    matchIndex += terms.Count;
                }
                else if (clauses[0].ClauseType == ClauseType.AllIn && clauses[0].InTerms != null)
                {
                    // First clause is AllIn — fill first term, AND remaining. Each term is a single-term lookup.
                    var terms = clauses[0].InTerms;
                    ops.Add(new PlanOp
                    {
                        Kind = PlanOpKind.FillFromPostings,
                        ParamIndex = matchIndex,
                        BitmapLocal = 0,
                        EstimatedCardinality = clauses[0].Cardinality / terms.Count,
                        UseTermSource = true
                    });
                    for (int t = 1; t < terms.Count; t++)
                    {
                        ops.Add(new PlanOp
                        {
                            Kind = PlanOpKind.AndWithPostings,
                            ParamIndex = matchIndex + t,
                            BitmapLocal = 0,
                            EstimatedCardinality = clauses[0].Cardinality / terms.Count,
                            UseTermSource = true
                        });
                        ops.Add(new PlanOp { Kind = PlanOpKind.CheckEmpty, BitmapLocal = 0 });
                    }
                    matchIndex += terms.Count;
                }
                else
                {
                    ops.Add(new PlanOp
                    {
                        Kind = PlanOpKind.FillFromPostings,
                        ParamIndex = 0,
                        EstimatedCardinality = clauses[0].Cardinality,
                        UseTermSource = IsTermSourceEligibleClause(clauses[0])
                    });
                    matchIndex = 1;
                }
            }
            // Precheck: can all remaining clauses be converted to entry scan predicates?
            // If any clause (In, AllIn, Spatial, Vector, Search, etc.) can't be scanned,
            // we must not emit CheckAndMaybeEntryScan — entry scan would skip them entirely.
            bool allScanEligible = true;
            {
                int dummyL = 0, dummyD = 0, dummyS = 0;
                for (int j = startIndex; j < clauses.Count; j++)
                {
                    if (BuildScanPredicateInfo(clauses[j], ref dummyL, ref dummyD, ref dummyS) == null)
                    {
                        allScanEligible = false;
                        break;
                    }
                }
            }

            for (int i = startIndex; i < clauses.Count; i++)
            {
                // Goto check before each AND step — only if all remaining clauses
                // can be handled by entry scan predicates
                if (allScanEligible)
                {
                    ops.Add(new PlanOp
                    {
                        Kind = PlanOpKind.CheckAndMaybeEntryScan,
                        ParamIndex = matchIndex
                    });
                }

                if (clauses[i].ClauseType == ClauseType.OrGroup && clauses[i].OrSubClauses != null)
                {
                    // OrGroup: OR sub-clauses into bitmap[1], then AND with bitmap[0]
                    var subClauses = clauses[i].OrSubClauses;

                    // Clear bitmap[1] (OR accumulator)
                    ops.Add(new PlanOp { Kind = PlanOpKind.ClearBitmap, BitmapLocal = 1 });

                    // Fill each sub-clause into bitmap[1]
                    for (int s = 0; s < subClauses.Count; s++)
                    {
                        ops.Add(new PlanOp
                        {
                            Kind = PlanOpKind.OrWithPostings,
                            ParamIndex = matchIndex + s,
                            BitmapLocal = 1, // target bitmap[1]
                            EstimatedCardinality = subClauses[s].Cardinality,
                            UseTermSource = IsTermSourceEligibleClause(subClauses[s])
                        });
                    }

                    // AND bitmap[1] into bitmap[0]
                    ops.Add(new PlanOp
                    {
                        Kind = PlanOpKind.AndBitmaps,
                        BitmapLocal = 0,   // target
                        ParamIndex2 = 1    // source (reuse ParamIndex2 for source bitmap)
                    });

                    // Early exit check
                    ops.Add(new PlanOp { Kind = PlanOpKind.CheckEmpty, BitmapLocal = 0 });

                    matchIndex += subClauses.Count;
                }
                else if (clauses[i].ClauseType == ClauseType.In && clauses[i].InTerms != null)
                {
                    // IN in AND chain: OR all terms into bitmap[1], then AND (or ANDNOT for negated) with bitmap[0].
                    // Each IN term is a single-term lookup.
                    var terms = clauses[i].InTerms;
                    ops.Add(new PlanOp { Kind = PlanOpKind.ClearBitmap, BitmapLocal = 1 });
                    for (int t = 0; t < terms.Count; t++)
                    {
                        ops.Add(new PlanOp
                        {
                            Kind = PlanOpKind.OrWithPostings,
                            ParamIndex = matchIndex + t,
                            BitmapLocal = 1,
                            EstimatedCardinality = clauses[i].Cardinality / terms.Count,
                            UseTermSource = true
                        });
                    }
                    // Negated In (NOT IN): subtract the OR'd terms from the result bitmap.
                    var bitmapCombineKind = clauses[i].IsNegated ? PlanOpKind.AndNotBitmaps : PlanOpKind.AndBitmaps;
                    ops.Add(new PlanOp
                    {
                        Kind = bitmapCombineKind,
                        BitmapLocal = 0,
                        ParamIndex2 = 1
                    });
                    if (!clauses[i].IsNegated)
                        ops.Add(new PlanOp { Kind = PlanOpKind.CheckEmpty, BitmapLocal = 0 });
                    matchIndex += terms.Count;
                }
                else if (clauses[i].ClauseType == ClauseType.AllIn && clauses[i].InTerms != null)
                {
                    // AllIn: AND each term's posting list with bitmap[0]. Each term is single-term.
                    var terms = clauses[i].InTerms;
                    for (int t = 0; t < terms.Count; t++)
                    {
                        ops.Add(new PlanOp
                        {
                            Kind = PlanOpKind.AndWithPostings,
                            ParamIndex = matchIndex + t,
                            BitmapLocal = 0,
                            EstimatedCardinality = clauses[i].Cardinality / terms.Count,
                            UseTermSource = true
                        });
                        ops.Add(new PlanOp { Kind = PlanOpKind.CheckEmpty, BitmapLocal = 0 });
                    }
                    matchIndex += terms.Count;
                }
                else
                {
                    // Simple clause: AND or ANDNOT with bitmap[0]
                    var isNegated = clauses[i].IsNegated || clauses[i].ClauseType == ClauseType.NotEquals;
                    var andKind = isNegated ? PlanOpKind.AndNotWithPostings : PlanOpKind.AndWithPostings;
                    ops.Add(new PlanOp
                    {
                        Kind = andKind,
                        ParamIndex = matchIndex,
                        BitmapLocal = 0,
                        EstimatedCardinality = clauses[i].Cardinality,
                        UseTermSource = IsTermSourceEligibleClause(clauses[i])
                    });

                    if (!isNegated)
                        ops.Add(new PlanOp { Kind = PlanOpKind.CheckEmpty, BitmapLocal = 0 });

                    matchIndex++;
                }
            }

            ops.Add(new PlanOp { Kind = PlanOpKind.IterateInto });
        }

        // Pack operand ordering
        int ordering = 0;
        for (int i = 0; i < Math.Min(clauses.Count, 10); i++)
            ordering |= (clauses[i].OriginalIndex & 0x7) << (i * 3);

        // Check if all clauses are negated (first clause after sort is negated)
        bool allNegated = clauses.Count > 0
            && (clauses[0].IsNegated || clauses[0].ClauseType == ClauseType.NotEquals);

        // Build scan predicate infos for entry scan — only for AND chains with simple clauses
        ScanPredicateInfo[] scanPredicateInfos = null;
        if (!isOr && clauses.Count > 1)
        {
            var scanPreds = new List<ScanPredicateInfo>();
            int longIndex = 0, doubleIndex = 0, sliceIndex = 0;

            // Start from 0 when all clauses are negated (allNegated=true): all are ANDNOT
            // operands and AllEntries is the implicit seed, so every clause needs a predicate.
            // Start from 1 in the normal case: clause 0 is the fill seed.
            int scanStart = allNegated ? 0 : 1;
            for (int i = scanStart; i < clauses.Count; i++)
            {
                var pred = BuildScanPredicateInfo(clauses[i], ref longIndex, ref doubleIndex, ref sliceIndex);
                if (pred != null)
                    scanPreds.Add(pred.Value);
            }

            if (scanPreds.Count > 0)
                scanPredicateInfos = scanPreds.ToArray();
        }

        // Compute type signature from scan predicates. The int packs the first 16 kinds
        // (2 bits each). For ≤ 16 predicates this is the exact cache identity. For more,
        // it's a lossy hash and we attach FullKinds for disambiguation in PlanCache.
        int typeSignature = 0;
        byte[] fullKinds = null;
        if (scanPredicateInfos != null)
        {
            int n = scanPredicateInfos.Length;
            int packCount = Math.Min(n, 16);
            for (int i = 0; i < packCount; i++)
                typeSignature |= ((int)scanPredicateInfos[i].ValueType & 0x3) << (i * 2);
            if (n > 16)
            {
                fullKinds = new byte[n];
                for (int i = 0; i < n; i++)
                    fullKinds[i] = (byte)scanPredicateInfos[i].ValueType;
            }
        }

        // EXPLAIN source is now generated lazily by CompiledPlan.ExplainSource on first
        // read (via QueryILEmitter.GenerateExplainSource). The vast majority of plans
        // never get inspected, so the eager build was pure overhead.
        var plan = new QueryPlan
        {
            Ops = ops.ToArray(),
            OperandOrdering = ordering,
            Clauses = clauses.ToArray(),
            AllNegated = allNegated,
            ScanPredicateInfos = scanPredicateInfos,
            TypeSignature = typeSignature,
            FullKinds = fullKinds,
            RequiredBitmaps = needsThreeBitmaps ? 3 : 2
        };
        return plan;
    }

    /// <summary>Extract typed parameter values from clauses for entry scan.
    /// Called per-query at execution time. The values populate the QueryScanContext spans.</summary>
    public static void ExtractScanParameters(QueryPlan plan, IndexSearcher indexSearcher,
        out long[] longParams, out double[] doubleParams, out Voron.Slice[] sliceParams, out long[] fieldRootPages)
    {
        var predicates = plan.ScanPredicateInfos;
        if (predicates == null || predicates.Length == 0)
        {
            longParams = Array.Empty<long>();
            doubleParams = Array.Empty<double>();
            sliceParams = Array.Empty<Voron.Slice>();
            fieldRootPages = Array.Empty<long>();
            return;
        }

        var clauses = plan.Clauses;
        var longs = new List<long>();
        var doubles = new List<double>();
        var slices = new List<Voron.Slice>();
        var roots = new List<long>();

        // Walk predicates and clauses in lock-step (same order as BuildScanPredicateInfo visited them).
        // Using field-name search instead would incorrectly return the first matching clause for
        // every predicate when multiple clauses share the same field (e.g. Name != 'a' AND Name != 'b').
        int scanStart = plan.AllNegated ? 0 : 1;
        int clauseIdx = scanStart;
        foreach (ScanPredicateInfo pred in predicates)
        {
            // Advance to the next eligible clause for this predicate.
            ClauseInfo matchingClause = null;
            while (clauseIdx < (clauses?.Length ?? 0))
            {
                if (clauses[clauseIdx] is ClauseInfo ci)
                {
                    matchingClause = ci;
                    clauseIdx++;
                    break;
                }
                clauseIdx++;
            }

            ExtractParamsFromPredicate(pred, matchingClause, indexSearcher, longs, doubles, slices, roots);
        }

        longParams = longs.Count > 0 ? longs.ToArray() : Array.Empty<long>();
        doubleParams = doubles.Count > 0 ? doubles.ToArray() : Array.Empty<double>();
        sliceParams = slices.Count > 0 ? slices.ToArray() : Array.Empty<Voron.Slice>();
        fieldRootPages = roots.Count > 0 ? roots.ToArray() : Array.Empty<long>();
    }

    private static void ExtractParamsFromPredicate(ScanPredicateInfo pred, ClauseInfo clause,
        IndexSearcher indexSearcher, List<long> longs, List<double> doubles,
        List<Voron.Slice> slices, List<long> roots)
    {
        if (pred.OrBranches != null)
        {
            // Each OrBranch corresponds to a sub-clause of the OrGroup.
            // Pass sub-clauses positionally to avoid the same field-name ambiguity.
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

    /// <summary>
    /// Populate the highlighting terms dictionary from the plan's clauses.
    /// The old CoraxQueryBuilder did this as a side effect during query building.
    /// The bitmap pipeline must do it explicitly after plan building.
    /// </summary>
    public static void PopulateHighlightingTerms(QueryPlan plan, Dictionary<string, CoraxHighlightingTermIndex> highlightingTerms, QueryMetadata metadata)
    {
        if (highlightingTerms == null || plan.Clauses == null)
            return;

        foreach (var clauseObj in plan.Clauses)
        {
            if (clauseObj is not ClauseInfo clause || clause.FieldName == null)
                continue;

            PopulateHighlightingForClause(clause, highlightingTerms, metadata);

            // Also handle OrGroup and AndGroup sub-clauses
            if (clause.ClauseType == ClauseType.OrGroup && clause.OrSubClauses != null)
            {
                foreach (var sub in clause.OrSubClauses)
                    PopulateHighlightingForClause(sub, highlightingTerms, metadata);
            }
            else if (clause.ClauseType == ClauseType.AndGroup && clause.AndSubClauses != null)
            {
                foreach (var sub in clause.AndSubClauses)
                    PopulateHighlightingForClause(sub, highlightingTerms, metadata);
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
            if (existingTerm.Values == null)
                existingTerm.Values = GetHighlightingValues(clause);
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
            ClauseType.Search => clause.TermValue,
            _ => clause.TermValue
        };
    }

    /// <summary>Convert a ClauseInfo to a ScanPredicateInfo for entry scan IL emission.
    /// Returns null for complex clauses that can't be entry-scanned.</summary>
    private static ScanPredicateInfo? BuildScanPredicateInfo(ClauseInfo clause,
        ref int longIndex, ref int doubleIndex, ref int sliceIndex)
    {
        // Complex clauses can't be entry-scanned
        switch (clause.ClauseType)
        {
            case ClauseType.Search:
            case ClauseType.Regex:
            case ClauseType.Spatial:
            case ClauseType.Vector:
            case ClauseType.In:
            case ClauseType.AllIn:
            case ClauseType.Exists:
            case ClauseType.StartsWith:
            case ClauseType.EndsWith:
            case ClauseType.AndGroup: // AND-groups inside OR chains are handled at the bitmap level
                return null;

            case ClauseType.OrGroup:
            {
                if (clause.OrSubClauses == null || clause.OrSubClauses.Count == 0)
                    return null;
                var branches = new List<ScanPredicateInfo>();
                int li = longIndex, di = doubleIndex, si = sliceIndex;
                foreach (var sub in clause.OrSubClauses)
                {
                    var subPred = BuildScanPredicateInfo(sub, ref li, ref di, ref si);
                    if (subPred == null)
                        return null; // Any complex sub-clause → can't entry-scan the whole group
                    branches.Add(subPred.Value);
                }
                longIndex = li; doubleIndex = di; sliceIndex = si;
                return new ScanPredicateInfo
                {
                    FieldName = clause.OrSubClauses[0].FieldName,
                    OrBranches = branches.ToArray()
                };
            }
        }

        // Determine value type and comparison op
        ScanCompareOp compareOp = clause.ClauseType switch
        {
            ClauseType.Equals => ScanCompareOp.Equal,
            ClauseType.NotEquals => ScanCompareOp.NotEqual,
            ClauseType.GreaterThan => ScanCompareOp.GreaterThan,
            ClauseType.GreaterThanOrEqual => ScanCompareOp.GreaterThanOrEqual,
            ClauseType.LessThan => ScanCompareOp.LessThan,
            ClauseType.LessThanOrEqual => ScanCompareOp.LessThanOrEqual,
            ClauseType.Between => ScanCompareOp.Between,
            _ => ScanCompareOp.Equal
        };

        // Strong typing: TermValueType is set by GetTermValue from the parser's literal
        // type (for inline values) or the resolved JSON-blittable runtime type (for params).
        // Switch on it directly — no string round-trip / TryParse fallback. A null TermValue
        // (e.g. "exists" check) falls through to Slice.
        ScanValueType valueType;
        switch (clause.TermValueType)
        {
            case ValueTokenType.Long:
                valueType = ScanValueType.Long;
                break;
            case ValueTokenType.Double:
                valueType = ScanValueType.Double;
                break;
            default:
                // String/True/False/Null/Parameter (when unresolvable) → opaque slice comparison.
                valueType = ScanValueType.Slice;
                break;
        }

        bool isBetween = clause.ClauseType == ClauseType.Between && clause.TermValue2 != null;
        int idx, idx2;
        switch (valueType)
        {
            case ScanValueType.Long:
                idx = longIndex++;
                idx2 = isBetween ? longIndex++ : -1;
                break;
            case ScanValueType.Double:
                idx = doubleIndex++;
                idx2 = isBetween ? doubleIndex++ : -1;
                break;
            default:
                idx = sliceIndex++;
                idx2 = isBetween ? sliceIndex++ : -1;
                break;
        }

        return new ScanPredicateInfo
        {
            FieldName = clause.FieldName,
            ValueType = valueType,
            CompareOp = compareOp,
            ParamIndex = idx,
            ParamIndex2 = idx2
        };
    }

    private static QueryPlan BuildAllEntriesPlan()
    {
        // No bitmap needed — AllEntries already implements IQueryMatch.Fill(),
        // so we iterate it directly without materializing into a bitmap first.
        return new QueryPlan
        {
            Ops = new[] { new PlanOp { Kind = PlanOpKind.DirectIterate, ParamIndex = 0 } },
            Clauses = Array.Empty<ClauseInfo>(),
            IsAllEntries = true
        };
    }

    private static QueryPlan BuildEmptyPlan()
    {
        // Query that always returns 0 results (e.g. false AND X)
        return new QueryPlan
        {
            Ops = Array.Empty<PlanOp>(),
            Clauses = Array.Empty<ClauseInfo>()
        };
    }

    internal enum ClauseType
    {
        Equals,
        NotEquals,
        GreaterThan,
        GreaterThanOrEqual,
        LessThan,
        LessThanOrEqual,
        Between,
        In,
        AllIn,
        Exists,
        StartsWith,
        EndsWith,
        Search,
        Regex,
        Spatial,
        Vector,
        OrGroup,  // A group of OR'd sub-clauses
        AndGroup, // A group of AND'd sub-clauses inside an OR chain
    }

    internal class ClauseInfo
    {
        public string FieldName;
        public string TermValue;
        public ValueTokenType TermValueType; // for type-aware comparison resolution
        public string TermValue2; // for BETWEEN
        public List<string> InTerms; // for IN
        public List<ValueTokenType> InTermTypes; // parallel to InTerms — literal type for each term (avoids wrong long.TryParse for zero-padded string values like "000001")
        public List<ClauseInfo> OrSubClauses;  // for OrGroup
        public bool IsOrChainNotEquals; // Set for NotEquals in OR chains; ResolveMatches creates AllEntries ANDNOT TermQuery
        public List<ClauseInfo> AndSubClauses; // for AndGroup (AND sub-expression inside OR)
        public MethodExpression MethodExpression; // for Spatial, Vector
        public ClauseType ClauseType;
        public long Cardinality = -1;
        public int OriginalIndex;
        public bool IsNegated;
        public bool IsExact;
        public float BoostFactor;

        // AST refs preserved for the master-plan-shape cache (#82 stage α): when we
        // reuse a parsed shape across calls, Refresh re-resolves these against the
        // call's queryParameters without re-walking the AST.
        public QueryExpression FieldExpr;       // FieldExpression for FieldName
        public QueryExpression TermExpr;        // ValueExpression for TermValue
        public QueryExpression TermExpr2;       // ValueExpression for TermValue2 (Between)
        public List<QueryExpression> InTermExprs; // ValueExpressions for InTerms
        public QueryExpression BoostFactorExpr; // ValueExpression for BoostFactor
    }

    /// <summary>
    /// Resolve clause infos to IQueryMatch instances for execution.
    /// Uses existing IndexSearcher methods (TermQuery, etc.) which handle
    /// all the complexity of analyzer application, CompactKey encoding,
    /// posting list resolution, etc.
    /// </summary>
    public static IQueryMatch[] ResolveMatches(QueryPlan plan, IndexSearcher indexSearcher,
        PlanParameters parameters = null, QueryBuilderParameters builderParams = null)
    {
        // All-entries plan with possible post-filter phases
        if (plan.IsAllEntries)
        {
            // If there are spatial/vector post-filters, we need to resolve those too.
            // The AllEntries match is at index 0, post-filter matches follow at their
            // MatchIndex positions (which account for the implicit AllEntries at 0).
            int spatialCount = plan.SpatialFilters?.Length ?? 0;
            int vectorCount = plan.VectorSelects?.Length ?? 0;
            int totalExtra = spatialCount + vectorCount;
            if (totalExtra == 0)
                return new IQueryMatch[] { indexSearcher.AllEntries() };

            var allEntriesMatches = new IQueryMatch[1 + totalExtra];
            allEntriesMatches[0] = indexSearcher.AllEntries();
            // Resolve spatial and vector clauses from the plan's Clauses array.
            // Each clause in plan.Clauses corresponds to a post-filter match at index 1+i.
            for (int i = 0; i < plan.Clauses.Length; i++)
            {
                if (plan.Clauses[i] is ClauseInfo ci)
                    allEntriesMatches[1 + i] = ResolveClause(ci, indexSearcher, parameters, builderParams);
            }
            return allEntriesMatches;
        }

        var clauses = plan.Clauses;
        if (clauses == null || clauses.Length == 0)
            return Array.Empty<IQueryMatch>();

        // Check for standalone NotEquals pattern: plan has Fill(AllEntries) + ANDNOT(term).
        // !plan.AllNegated distinguishes this from the "true AND NOT expr" path (AllNegated=true),
        // which needs normal clause resolution for non-term clauses (startsWith, In, etc.).
        if (clauses.Length == 1 && ((ClauseInfo)clauses[0]).IsNegated && !plan.AllNegated)
        {
            var clause = (ClauseInfo)clauses[0];
            return new IQueryMatch[]
            {
                indexSearcher.AllEntries(),
                indexSearcher.TermQuery(indexSearcher.FieldMetadataBuilder(clause.FieldName), clause.TermValue)
            };
        }

        // Flatten OrGroups, AndGroups, In, and AllIn: each sub-clause/term becomes a separate match.
        int totalMatches = 0;
        for (int i = 0; i < clauses.Length; i++)
        {
            var clause = (ClauseInfo)clauses[i];
            if (clause.ClauseType == ClauseType.OrGroup && clause.OrSubClauses != null)
                totalMatches += clause.OrSubClauses.Count;
            else if (clause.ClauseType == ClauseType.AndGroup && clause.AndSubClauses != null)
                totalMatches += clause.AndSubClauses.Count;
            else if ((clause.ClauseType == ClauseType.AllIn || clause.ClauseType == ClauseType.In) && clause.InTerms != null)
                totalMatches += clause.InTerms.Count;
            else
                totalMatches++;
        }
        int extraSlots = plan.AllNegated ? 1 : 0;
        var matches = new IQueryMatch[totalMatches + extraSlots];
        int matchIdx = 0;
        for (int i = 0; i < clauses.Length; i++)
        {
            var clause = (ClauseInfo)clauses[i];
            if (clause.ClauseType == ClauseType.OrGroup && clause.OrSubClauses != null)
            {
                foreach (var sub in clause.OrSubClauses)
                {
                    var match = ResolveClause(sub, indexSearcher, parameters, builderParams);
                    if (sub.BoostFactor > 0)
                        match = indexSearcher.Boost(match, sub.BoostFactor);
                    matches[matchIdx++] = match;
                }
            }
            else if (clause.ClauseType == ClauseType.AndGroup && clause.AndSubClauses != null)
            {
                // Each AND sub-clause becomes a separate match slot (used by EmitPlan's
                // FillFromPostings + AndWithPostings sequence inside the OR chain).
                foreach (var sub in clause.AndSubClauses)
                {
                    var match = ResolveClause(sub, indexSearcher, parameters, builderParams);
                    if (sub.BoostFactor > 0)
                        match = indexSearcher.Boost(match, sub.BoostFactor);
                    matches[matchIdx++] = match;
                }
            }
            else if ((clause.ClauseType == ClauseType.AllIn || clause.ClauseType == ClauseType.In) && clause.InTerms != null)
            {
                // Each IN/AllIn term becomes a separate match — resolved as typed TermQuery
                for (int t = 0; t < clause.InTerms.Count; t++)
                    matches[matchIdx++] = ResolveInTerm(clause.FieldName, clause.InTermTypes, t, clause.InTerms[t], indexSearcher, parameters, builderParams);
            }
            else
            {
                IQueryMatch match;
                if (clause.IsOrChainNotEquals)
                {
                    // OR-chain NotEquals: De Morgan — NOT(X) in OR = AllEntries ANDNOT X.
                    // Cannot use the raw term posting list (FillBitmapFromTermSource adds entries
                    // WITH X, not entries WITHOUT X). Pre-materialize AllEntries ANDNOT TermQuery.
                    match = CreateNotEqualsOrMatch(clause, indexSearcher, parameters, builderParams);
                }
                else
                {
                    match = ResolveClause(clause, indexSearcher, parameters, builderParams);
                }
                if (clause.BoostFactor > 0)
                    match = indexSearcher.Boost(match, clause.BoostFactor);
                matches[matchIdx++] = match;
            }
        }
        if (plan.AllNegated)
            matches[matchIdx] = indexSearcher.AllEntries();
        return matches;
    }

    /// <summary>
    /// Resolve clause infos to <see cref="TermSource"/> instances for the native
    /// posting-list dispatch path. Parallels <see cref="ResolveMatches"/> — the
    /// returned array uses the same indexing scheme. Slots whose underlying
    /// clause is multi-term / non-term-shaped (Spatial, Vector, Search, Range,
    /// StartsWith, EndsWith, Regex, AllEntries) keep <c>Kind == TermSourceKind.Empty</c>;
    /// only Equals / NotEquals / In / AllIn / OrGroup-of-(Not)Equals slots populate.
    /// The IL emitter consults <see cref="PlanOp.UseTermSource"/> to decide which
    /// array to read.
    /// </summary>
    public static TermSource[] ResolveTermSources(QueryPlan plan, IndexSearcher indexSearcher,
        PlanParameters parameters = null, QueryBuilderParameters builderParams = null)
    {
        // IsAllEntries plans never emit term ops (FillFromPostings / AndWith / etc.) —
        // their match[0] is AllEntries, post-filter slots are spatial/vector. No
        // TermSource population needed.
        if (plan.IsAllEntries)
            return Array.Empty<TermSource>();

        var clauses = plan.Clauses;
        if (clauses == null || clauses.Length == 0)
            return Array.Empty<TermSource>();

        // Standalone NotEquals: matches[0] = AllEntries (NOT a term source),
        // matches[1] = the negated term. Mirror that layout.
        // !plan.AllNegated distinguishes this from the "true AND NOT expr" path (AllNegated=true).
        if (clauses.Length == 1 && ((ClauseInfo)clauses[0]).IsNegated && !plan.AllNegated)
        {
            var sources = new TermSource[2];
            // sources[0] stays Empty — AllEntries goes through DirectSources.
            sources[1] = ResolveSingleTermSource((ClauseInfo)clauses[0], indexSearcher, parameters, builderParams);
            return sources;
        }

        int totalMatches = 0;
        for (int i = 0; i < clauses.Length; i++)
        {
            var clause = (ClauseInfo)clauses[i];
            if (clause.ClauseType == ClauseType.OrGroup && clause.OrSubClauses != null)
                totalMatches += clause.OrSubClauses.Count;
            else if (clause.ClauseType == ClauseType.AndGroup && clause.AndSubClauses != null)
                totalMatches += clause.AndSubClauses.Count;
            else if ((clause.ClauseType == ClauseType.AllIn || clause.ClauseType == ClauseType.In) && clause.InTerms != null)
                totalMatches += clause.InTerms.Count;
            else
                totalMatches++;
        }
        int extraSlots = plan.AllNegated ? 1 : 0;
        var termSources = new TermSource[totalMatches + extraSlots];
        int matchIdx = 0;
        for (int i = 0; i < clauses.Length; i++)
        {
            var clause = (ClauseInfo)clauses[i];
            if (clause.ClauseType == ClauseType.OrGroup && clause.OrSubClauses != null)
            {
                foreach (var sub in clause.OrSubClauses)
                {
                    // Boosting bypasses TermSource — Boost wraps the IQueryMatch and
                    // expects to forward Score(). Keep boosted ops on the IQueryMatch
                    // path so scores are computed correctly.
                    if (sub.BoostFactor > 0)
                    {
                        matchIdx++;
                        continue;
                    }
                    termSources[matchIdx++] = ResolveSingleTermSource(sub, indexSearcher, parameters, builderParams);
                }
            }
            else if (clause.ClauseType == ClauseType.AndGroup && clause.AndSubClauses != null)
            {
                // Each AND sub-clause has its own TermSource slot, mirroring ResolveMatches.
                foreach (var sub in clause.AndSubClauses)
                {
                    if (sub.BoostFactor > 0)
                    {
                        matchIdx++;
                        continue;
                    }
                    termSources[matchIdx++] = ResolveSingleTermSource(sub, indexSearcher, parameters, builderParams);
                }
            }
            else if ((clause.ClauseType == ClauseType.AllIn || clause.ClauseType == ClauseType.In) && clause.InTerms != null)
            {
                for (int t = 0; t < clause.InTerms.Count; t++)
                    termSources[matchIdx++] = ResolveInTermSource(clause, t, indexSearcher, parameters, builderParams);
            }
            else
            {
                if (clause.BoostFactor > 0)
                {
                    matchIdx++;
                    continue;
                }
                termSources[matchIdx++] = ResolveSingleTermSource(clause, indexSearcher, parameters, builderParams);
            }
        }
        // AllNegated extra slot is AllEntries — stays Empty in TermSources.
        return termSources;
    }

    /// <summary>Decide whether a clause type can be expressed as a single
    /// <see cref="TermSource"/>. Boosted clauses go through the IQueryMatch path
    /// even when they're term-shaped, so scoring still works.</summary>
    internal static bool IsTermSourceEligibleClause(ClauseInfo clause)
    {
        if (clause == null)
            return false;
        if (clause.BoostFactor > 0)
            return false;
        return clause.ClauseType is ClauseType.Equals or ClauseType.NotEquals;
    }

    /// <summary>Resolve a single Equals / NotEquals clause to a posting-list ID and
    /// decode it into a <see cref="TermSource"/>. Returns Empty when the clause
    /// is non-term-shaped or the term doesn't exist in the index.</summary>
    private static TermSource ResolveSingleTermSource(ClauseInfo clause, IndexSearcher indexSearcher,
        PlanParameters parameters, QueryBuilderParameters builderParams)
    {
        if (IsTermSourceEligibleClause(clause) == false)
            return default; // Kind == Empty

        FieldMetadata fieldMeta = ResolveFieldMetadata(clause, indexSearcher, parameters, builderParams);

        long postingListId;
        if (clause.TermValueType == ValueTokenType.Long
            && long.TryParse(clause.TermValue, out long eqLong))
        {
            // GetTermPostingListId<long> internally calls GetNumericFieldMetadata<long>;
            // pass the raw field metadata so we don't double-suffix the field name.
            postingListId = indexSearcher.GetTermPostingListId(fieldMeta, eqLong);
        }
        else if (clause.TermValueType == ValueTokenType.Double
            && double.TryParse(clause.TermValue,
                System.Globalization.NumberStyles.Float,
                System.Globalization.CultureInfo.InvariantCulture, out double eqDouble))
        {
            postingListId = indexSearcher.GetTermPostingListId(fieldMeta, eqDouble);
        }
        else
        {
            postingListId = indexSearcher.GetTermPostingListId(fieldMeta, clause.TermValue);
        }

        return DecodePostingListId(postingListId, indexSearcher);
    }

    /// <summary>Resolve a single In/AllIn term to a posting-list ID.
    /// Uses <paramref name="termIndex"/> into <see cref="ClauseInfo.InTerms"/> /
    /// <see cref="ClauseInfo.InTermTypes"/> to pick the correct numeric vs. string
    /// overload — avoids the long.TryParse false-positive on zero-padded string
    /// values like "000001" (parses as 1L but is indexed as the string "000001").</summary>
    private static TermSource ResolveInTermSource(ClauseInfo clause, int termIndex, IndexSearcher indexSearcher,
        PlanParameters parameters, QueryBuilderParameters builderParams)
    {
        string term = clause.InTerms[termIndex];
        ValueTokenType termType = clause.InTermTypes != null && termIndex < clause.InTermTypes.Count
            ? clause.InTermTypes[termIndex]
            : ValueTokenType.String;

        FieldMetadata fieldMeta;
        if (builderParams != null)
            fieldMeta = QueryBuilderHelper.GetFieldMetadata(in builderParams, clause.FieldName, hasBoost: builderParams.HasBoost);
        else
            fieldMeta = indexSearcher.FieldMetadataBuilder(clause.FieldName, hasBoost: parameters?.HasBoost ?? false);

        long postingListId;
        if (termType == ValueTokenType.Long && long.TryParse(term, out long lVal))
        {
            postingListId = indexSearcher.GetTermPostingListId(fieldMeta, lVal);
        }
        else if (termType == ValueTokenType.Double && double.TryParse(term,
            System.Globalization.NumberStyles.Float,
            System.Globalization.CultureInfo.InvariantCulture, out double dVal))
        {
            postingListId = indexSearcher.GetTermPostingListId(fieldMeta, dVal);
        }
        else
        {
            postingListId = indexSearcher.GetTermPostingListId(fieldMeta, term);
        }

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
            return default; // Kind == Empty

        var termType = (global::Corax.Indexing.TermIdMask)postingListId & global::Corax.Indexing.TermIdMask.EnsureIsSingleMask;
        switch (termType)
        {
            case global::Corax.Indexing.TermIdMask.Single:
                return new TermSource
                {
                    Kind = TermSourceKind.Single,
                    SingleEntryId = (long)global::Corax.Utils.EntryIdEncodings.GetContainerId(postingListId),
                };

            case global::Corax.Indexing.TermIdMask.SmallPostingList:
                return new TermSource
                {
                    Kind = TermSourceKind.SmallPostingList,
                    SmallPostingListId = (long)global::Corax.Utils.EntryIdEncodings.GetContainerId(postingListId),
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

            items[i] = HandleVector(builderParams, clause.MethodExpression, false);
        }
        return items;
    }

    /// <summary>Find the ClauseInfo that corresponds to a given match index.
    /// The match index is the flattened position in the resolved IQueryMatch[] array.</summary>
    private static ClauseInfo FindClauseByMatchIndex(QueryPlan plan, int targetMatchIndex)
    {
        if (plan.Clauses == null)
            return null;

        int matchIndex = 0;
        for (int i = 0; i < plan.Clauses.Length; i++)
        {
            if (plan.Clauses[i] is not ClauseInfo clause)
                continue;

            int slotCount;
            if (clause.ClauseType == ClauseType.OrGroup && clause.OrSubClauses != null)
                slotCount = clause.OrSubClauses.Count;
            else if (clause.ClauseType == ClauseType.AndGroup && clause.AndSubClauses != null)
                slotCount = clause.AndSubClauses.Count;
            else if ((clause.ClauseType == ClauseType.AllIn || clause.ClauseType == ClauseType.In) && clause.InTerms != null)
                slotCount = clause.InTerms.Count;
            else
                slotCount = 1;

            if (matchIndex <= targetMatchIndex && targetMatchIndex < matchIndex + slotCount)
                return clause;

            matchIndex += slotCount;
        }

        // Check for AllNegated extra slot
        return null;
    }

    /// <summary>Create a pre-materialized <see cref="BitmapMatch"/> for a NotEquals clause
    /// appearing in an OR chain. OR(NOT X, NOT Y, ...) cannot use the raw term posting list
    /// (FillBitmapFromTermSource would add entries WITH X, not WITHOUT X). Instead, we
    /// pre-compute AllEntries ANDNOT TermQuery(X) into a BitmapMatch so that FillFromMatch
    /// during execution correctly ORs in the set of entries NOT having X.</summary>
    private static IQueryMatch CreateNotEqualsOrMatch(ClauseInfo clause, IndexSearcher indexSearcher,
        PlanParameters parameters, QueryBuilderParameters builderParams)
    {
        FieldMetadata fieldMeta = ResolveFieldMetadata(clause, indexSearcher, parameters, builderParams);

        IQueryMatch termMatch;
        if (clause.TermValueType == ValueTokenType.Long
            && long.TryParse(clause.TermValue, out long lVal))
            termMatch = indexSearcher.TermQuery(fieldMeta, lVal);
        else if (clause.TermValueType == ValueTokenType.Double
            && double.TryParse(clause.TermValue, System.Globalization.NumberStyles.Float,
                System.Globalization.CultureInfo.InvariantCulture, out double dVal))
            termMatch = indexSearcher.TermQuery(fieldMeta, dVal);
        else
            termMatch = indexSearcher.TermQuery(fieldMeta, clause.TermValue);

        // Materialize AllEntries ANDNOT excluded-term into a bitmap.
        // FillFromMatch for the BitmapMatch result fast-paths via IBitmapQueryMatch.
        var bitmapMatch = new BitmapMatch(indexSearcher.Allocator);
        var tempData = new RoaringBitmap(indexSearcher.Allocator);
        QueryPrimitives.FillFromMatch(indexSearcher.AllEntries(), ref bitmapMatch.BitmapState, indexSearcher.Allocator);
        QueryPrimitives.AndNotWithMatch(termMatch, ref bitmapMatch.BitmapState, ref tempData, indexSearcher.Allocator);
        tempData.Dispose();
        return bitmapMatch;
    }

    private static IQueryMatch ResolveClause(ClauseInfo clause, IndexSearcher indexSearcher,
        PlanParameters parameters = null, QueryBuilderParameters builderParams = null)
    {
        // Use proper field metadata from the index schema when available.
        // This ensures correct analyzer application for static index fields.
        // Requires all of: Allocator, Index, IndexFieldsMapping to be non-null.
        // Use proper field metadata from the index schema for simple clauses.
        // Spatial/Vector/Search have their own field resolution paths.
        FieldMetadata fieldMeta;
        bool useBuilderParams = builderParams != null
            && clause.ClauseType != ClauseType.Spatial
            && clause.ClauseType != ClauseType.Vector
            && clause.ClauseType != ClauseType.Search;
        if (useBuilderParams)
        {
            string resolvedFieldName = clause.FieldName;
            // For exact queries on auto-indexes, use the _exact field variant
            if (clause.IsExact && builderParams.Metadata.IsDynamic)
                resolvedFieldName = AutoIndexField.GetExactAutoIndexFieldName(resolvedFieldName);
            fieldMeta = QueryBuilderHelper.GetFieldMetadata(in builderParams, resolvedFieldName, exact: clause.IsExact, hasBoost: builderParams.HasBoost);
        }
        else
        {
            fieldMeta = indexSearcher.FieldMetadataBuilder(clause.FieldName, hasBoost: parameters?.HasBoost ?? false);
        }

        switch (clause.ClauseType)
        {
            case ClauseType.Equals:
            case ClauseType.NotEquals:
            {
                // Use the value type to determine the right TermQuery overload
                IQueryMatch eqMatch;
                if (clause.TermValueType == ValueTokenType.Long
                    && long.TryParse(clause.TermValue, out long eqLong))
                    eqMatch = indexSearcher.TermQuery(fieldMeta, eqLong);
                else if (clause.TermValueType == ValueTokenType.Double
                    && double.TryParse(clause.TermValue,
                        System.Globalization.NumberStyles.Float,
                        System.Globalization.CultureInfo.InvariantCulture, out double eqDouble))
                    eqMatch = indexSearcher.TermQuery(fieldMeta, eqDouble);
                else
                    eqMatch = indexSearcher.TermQuery(fieldMeta, clause.TermValue);
                return eqMatch;
            }

            case ClauseType.GreaterThan:
                if (clause.TermValueType == ValueTokenType.Long
                    && long.TryParse(clause.TermValue, out long gtLong))
                    return indexSearcher.GreaterThanQuery(fieldMeta, gtLong);
                if (clause.TermValueType == ValueTokenType.Double
                    && double.TryParse(clause.TermValue, System.Globalization.NumberStyles.Float,
                    System.Globalization.CultureInfo.InvariantCulture, out double gtDouble))
                    return indexSearcher.GreaterThanQuery(fieldMeta, gtDouble);
                return indexSearcher.GreaterThanQuery(fieldMeta, clause.TermValue);

            case ClauseType.GreaterThanOrEqual:
                if (clause.TermValueType == ValueTokenType.Long
                    && long.TryParse(clause.TermValue, out long gteLong))
                    return indexSearcher.GreatThanOrEqualsQuery(fieldMeta, gteLong);
                if (clause.TermValueType == ValueTokenType.Double
                    && double.TryParse(clause.TermValue, System.Globalization.NumberStyles.Float,
                    System.Globalization.CultureInfo.InvariantCulture, out double gteDouble))
                    return indexSearcher.GreatThanOrEqualsQuery(fieldMeta, gteDouble);
                return indexSearcher.GreatThanOrEqualsQuery(fieldMeta, clause.TermValue);

            case ClauseType.LessThan:
                if (clause.TermValueType == ValueTokenType.Long
                    && long.TryParse(clause.TermValue, out long ltLong))
                    return indexSearcher.LessThanQuery(fieldMeta, ltLong);
                if (clause.TermValueType == ValueTokenType.Double
                    && double.TryParse(clause.TermValue, System.Globalization.NumberStyles.Float,
                    System.Globalization.CultureInfo.InvariantCulture, out double ltDouble))
                    return indexSearcher.LessThanQuery(fieldMeta, ltDouble);
                return indexSearcher.LessThanQuery(fieldMeta, clause.TermValue);

            case ClauseType.LessThanOrEqual:
                if (clause.TermValueType == ValueTokenType.Long
                    && long.TryParse(clause.TermValue, out long lteLong))
                    return indexSearcher.LessThanOrEqualsQuery(fieldMeta, lteLong);
                if (clause.TermValueType == ValueTokenType.Double
                    && double.TryParse(clause.TermValue, System.Globalization.NumberStyles.Float,
                    System.Globalization.CultureInfo.InvariantCulture, out double lteDouble))
                    return indexSearcher.LessThanOrEqualsQuery(fieldMeta, lteDouble);
                return indexSearcher.LessThanOrEqualsQuery(fieldMeta, clause.TermValue);

            case ClauseType.Between:
                if (clause.TermValueType == ValueTokenType.Long
                    && long.TryParse(clause.TermValue, out long btwLowLong)
                    && long.TryParse(clause.TermValue2, out long btwHighLong))
                    return indexSearcher.BetweenQuery(fieldMeta, btwLowLong, btwHighLong);
                if (clause.TermValueType == ValueTokenType.Double
                    && double.TryParse(clause.TermValue, System.Globalization.NumberStyles.Float,
                    System.Globalization.CultureInfo.InvariantCulture, out double btwLowDouble)
                    && double.TryParse(clause.TermValue2, System.Globalization.NumberStyles.Float,
                    System.Globalization.CultureInfo.InvariantCulture, out double btwHighDouble))
                    return indexSearcher.BetweenQuery(fieldMeta, btwLowDouble, btwHighDouble);
                return indexSearcher.BetweenQuery(fieldMeta, clause.TermValue, clause.TermValue2);

            case ClauseType.In:
            case ClauseType.AllIn:
                // In and AllIn sub-terms are expanded into separate matches by ResolveMatches.
                throw new InvalidOperationException(
                    $"{clause.ClauseType} should be expanded by ResolveMatches, not resolved as a single clause.");

            case ClauseType.Exists:
                return indexSearcher.ExistsQuery(fieldMeta);

            case ClauseType.StartsWith:
                return indexSearcher.StartWithQuery(fieldMeta, clause.TermValue);

            case ClauseType.EndsWith:
                return indexSearcher.EndsWithQuery(fieldMeta, clause.TermValue);

            case ClauseType.Search:
            {
                // Search needs proper field metadata with search analyzer
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
                else if (parameters?.Index != null && parameters.IndexFieldsMapping != null)
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

                // Determine SearchQueryOptions based on index version
                var indexDef = builderParams?.Index?.Definition ?? parameters?.Index?.Definition;
                IndexSearcher.SearchQueryOptions searchQueryOptions;
                if (indexDef != null && IndexDefinitionBaseServerSide.IndexVersion.IsCoraxSearchWildcardAdjustmentSupported(indexDef.Version))
                    searchQueryOptions = IndexSearcher.SearchQueryOptions.PhraseQueryWithWildcardAdjustments;
                else if (indexDef != null && indexDef.Version >= IndexDefinitionBaseServerSide.IndexVersion.PhraseQuerySupportInCoraxIndexes)
                    searchQueryOptions = IndexSearcher.SearchQueryOptions.PhraseQuery;
                else
                    searchQueryOptions = IndexSearcher.SearchQueryOptions.Legacy;

                // For wildcard queries with WildcardAdjustments, use Legacy mode
                // which handles wildcard analyzer replacement internally
                if (searchQueryOptions == IndexSearcher.SearchQueryOptions.PhraseQueryWithWildcardAdjustments
                    && clause.TermValue != null && clause.TermValue.Length >= 1
                    && (clause.TermValue[0] == '*' || (clause.TermValue.Length >= 2 && clause.TermValue[^1] == '*')))
                {
                    searchQueryOptions = IndexSearcher.SearchQueryOptions.Legacy;
                }

                // Split search value respecting quoted phrases (same as old CoraxQueryBuilder.GetValues)
                var searchValues = SplitSearchTerms(clause.TermValue);

                return indexSearcher.SearchQuery(searchMeta,
                    searchValues,
                    Constants.Search.Operator.Or,
                    searchQueryOptions);
            }

            case ClauseType.Regex:
                return indexSearcher.RegexQuery(fieldMeta,
                    new System.Text.RegularExpressions.Regex(clause.TermValue));

            case ClauseType.Spatial:
            {
                if (builderParams == null || clause.MethodExpression == null)
                    throw new InvalidOperationException("Spatial resolution requires builder parameters");
                var spatialMethod = QueryMethod.GetMethodType(clause.MethodExpression.Name.Value);
                return HandleSpatial(builderParams, clause.MethodExpression, spatialMethod);
            }

            case ClauseType.Vector:
            {
                if (builderParams == null || clause.MethodExpression == null)
                    throw new InvalidOperationException("Vector resolution requires builder parameters");
                var vectorItem = HandleVector(builderParams, clause.MethodExpression, false);
                // Materialize with null inner — the bitmap provides the candidate set
                return vectorItem.Materialize(null);
            }

            case ClauseType.OrGroup:
                // OrGroup sub-clauses are expanded into separate matches by ResolveMatches.
                // This case should not be reached — OrGroups are handled at the ResolveMatches level.
                throw new InvalidOperationException(
                    "OrGroup should be expanded by ResolveMatches, not resolved as a single clause.");

            case ClauseType.AndGroup:
                // AndGroup sub-clauses are expanded into separate matches by ResolveMatches.
                // This case should not be reached — AndGroups are handled at the ResolveMatches level.
                throw new InvalidOperationException(
                    "AndGroup should be expanded by ResolveMatches, not resolved as a single clause.");

            default:
                throw new InvalidOperationException($"Unexpected ClauseType {clause.ClauseType} in ResolveClause.");
        }
    }

    /// <summary>Resolve a single IN term to a typed TermQuery (long, double, or string).
    /// Uses <paramref name="termTypes"/> at <paramref name="termIndex"/> to pick the correct
    /// numeric vs. string overload — avoids false long.TryParse matches for zero-padded
    /// string values like "000001".</summary>
    private static IQueryMatch ResolveInTerm(string fieldName, List<ValueTokenType> termTypes, int termIndex,
        string term, IndexSearcher indexSearcher,
        PlanParameters parameters, QueryBuilderParameters builderParams)
    {
        ValueTokenType termType = termTypes != null && termIndex < termTypes.Count
            ? termTypes[termIndex]
            : ValueTokenType.String;

        FieldMetadata fieldMeta;
        if (builderParams != null)
            fieldMeta = QueryBuilderHelper.GetFieldMetadata(in builderParams, fieldName, hasBoost: builderParams.HasBoost);
        else
            fieldMeta = indexSearcher.FieldMetadataBuilder(fieldName, hasBoost: parameters?.HasBoost ?? false);

        if (termType == ValueTokenType.Long && long.TryParse(term, out long lVal))
            return indexSearcher.TermQuery(fieldMeta, lVal);
        if (termType == ValueTokenType.Double && double.TryParse(term, System.Globalization.NumberStyles.Float,
            System.Globalization.CultureInfo.InvariantCulture, out double dVal))
            return indexSearcher.TermQuery(fieldMeta, dVal);
        return indexSearcher.TermQuery(fieldMeta, term);
    }

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
                // in case when we've single vector clause and we expose the score, we have to go through
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
                // EntryComparerByScore.Compare is intentionally inverted (returns y.CompareTo(x)),
                // so ascending=true → highest scores first (the default "most relevant first" search engine order).
                // ascending=false → Descending<EntryComparerByScore> → lowest scores first.
                //
                // Parser behaviour: ORDER BY score()          → Ascending=true
                //                   ORDER BY score() ASC      → Ascending=true
                //                   ORDER BY score() DESC     → Ascending=false
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
        // planParams.Metadata.OrderBy is the processed OrderByField[] (QueryMetadata level),
        // not the raw AST Query.OrderBy tuples.
        OrderByField[] orderByFields = planParams.Metadata.OrderBy;
        if (orderByFields == null || orderByFields.Length == 0)
            return match;

        var indexSearcher = planParams.IndexSearcher;
        int takeInt = take > int.MaxValue ? Constants.IndexSearcher.TakeAll : (int)take;

        for (int i = 0; i < orderByFields.Length; i++)
        {
            if (orderByFields[i].OrderingType == OrderByFieldType.Score)
            {
                // ascending=true  → EntryComparerByScore (inverted comparer) → highest scores first
                // ascending=false → Descending<EntryComparerByScore>          → lowest scores first
                var meta = new OrderMetadata(true, MatchCompareFieldType.Score, orderByFields[i].Ascending);
                return indexSearcher.OrderBy(match, meta, nullFirst: false, take: takeInt, token: token);
            }
        }

        return match;
    }

    private static IQueryMatch HandleSpatial(QueryBuilderParameters builderParameters, MethodExpression expression, MethodType spatialMethod)
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

        return builderParameters.IndexSearcher.SpatialQuery(fieldMetadata, distanceErrorPct, shape, spatialField.GetContext(), operation, token: builderParameters.Token);
    }

    private static CoraxVectorItem HandleVector(QueryBuilderParameters builderParameters, MethodExpression me, bool exact)
    {
        var metadata = builderParameters.Metadata;
        IndexField indexField;
        string embeddingsGenerationTaskIdentifier;
        var minimumMatch = builderParameters.Index.Configuration.CoraxVectorSearchDefaultMinimumSimilarity;
        if (me.Arguments.Count > 2)
        {
            var (similarityValue, similiarityValueType) = QueryBuilderHelper.GetValue(builderParameters.Metadata.Query, builderParameters.Metadata, builderParameters.QueryParameters,
                (ValueExpression)me.Arguments[2]);
            minimumMatch = similiarityValueType switch
            {
                ValueTokenType.Null => builderParameters.Index.Configuration.CoraxVectorSearchDefaultMinimumSimilarity,
                ValueTokenType.Long => (long)similarityValue,
                ValueTokenType.Double => (float)(double)similarityValue,
                _ => throw new NotSupportedException("vector.search() minimumMatch must be a float, but was: " + similiarityValueType)
            };
        }

        int numberOfCandidates = builderParameters.Index.Configuration.CoraxVectorDefaultNumberOfCandidatesForQuerying;
        if (me.Arguments.Count > 3)
        {
            var (candidatesValue, candidatesValueType) = QueryBuilderHelper.GetValue(builderParameters.Metadata.Query, builderParameters.Metadata, builderParameters.QueryParameters,
                (ValueExpression)me.Arguments[3]);
            numberOfCandidates = candidatesValueType switch
            {
                ValueTokenType.Long => Convert.ToInt32(candidatesValue),
                ValueTokenType.Double => Convert.ToInt32(candidatesValue),
                ValueTokenType.Null => builderParameters.Index.Configuration.CoraxVectorDefaultNumberOfCandidatesForQuerying,
                _ => throw new NotSupportedException("vector.search() minimumMatch must be a float, but was: " + candidatesValueType)
            };
        }

        var fieldName = metadata.IsDynamic == false
            ? QueryBuilderHelper.ExtractIndexFieldName(metadata.Query, builderParameters.QueryParameters, me.Arguments[0], metadata)
            : metadata.GetVectorFieldName(me, builderParameters.QueryParameters);

        var fieldMetadata = QueryBuilderHelper.GetFieldMetadata(builderParameters, fieldName, hasBoost: builderParameters.HasBoost);
        QueryExpression srcVector = me.Arguments[1];

        if (srcVector is MethodExpression methodValue) // embedding.forDoc(docId) ...
        {
            var supportedMethods = methodValue.Name != ClientConstants.VectorSearch.EmbeddingForDocument
                                   && methodValue.Name != ClientConstants.VectorSearch.EmbeddingForRaw
                                   && methodValue.Name != ClientConstants.VectorSearch.EmbeddingText;

            PortableExceptions.ThrowIf<InvalidDataException>(supportedMethods,
                $"Expected {ClientConstants.VectorSearch.EmbeddingForDocument}() method call, but got: {methodValue.Name}");

            var (methodParameter, valueTokenType) = QueryBuilderHelper.GetValue(metadata.Query, metadata, builderParameters.QueryParameters, (ValueExpression)methodValue.Arguments[0], allowObjectsInParameters: false, allowArraysInParameters: true);

            var method = methodValue.Name.ToString() switch
            {
                ClientConstants.VectorSearch.EmbeddingForDocument => VectorHelpers.MethodVectorValue.ForDocument,
                ClientConstants.VectorSearch.EmbeddingForRaw => VectorHelpers.MethodVectorValue.ForRaw,
                ClientConstants.VectorSearch.EmbeddingText => VectorHelpers.MethodVectorValue.EmbeddingText,
                _ => throw new InvalidDataException(
                    $"Unknown method in value ({methodValue.Name}. Parameter type: {methodParameter.GetType().FullName}, Value: {methodParameter}")
            };

            if (method is not VectorHelpers.MethodVectorValue.EmbeddingText)
            {
                return (method, methodParameter) switch
                {
                    (method: VectorHelpers.MethodVectorValue.ForDocument, string docId) => CoraxVectorItem.BuildForDocVector(builderParameters, fieldMetadata, docId, numberOfCandidates, minimumMatch, exact),
                    (method: VectorHelpers.MethodVectorValue.ForDocument, StringSegment docIdSegment) => CoraxVectorItem.BuildForDocVector(builderParameters, fieldMetadata, docIdSegment.Value, numberOfCandidates, minimumMatch, exact),
                    (method: VectorHelpers.MethodVectorValue.ForRaw, string vectorAsBase64) => CoraxVectorItem.BuildSingleVector(builderParameters, fieldMetadata, GenerateEmbeddings.FromBase64Array(VectorOptions.Default, builderParameters.Allocator, vectorAsBase64, false), numberOfCandidates, minimumMatch, exact),
                    (method: VectorHelpers.MethodVectorValue.ForRaw, StringSegment stringSegmentAsBase64) => CoraxVectorItem.BuildSingleVector(builderParameters, fieldMetadata, GenerateEmbeddings.FromBase64Array(VectorOptions.Default, builderParameters.Allocator, stringSegmentAsBase64.ToString(), false), numberOfCandidates, minimumMatch, exact),
                    (_, BlittableJsonReaderArray { Length: > 0 }) => throw new InvalidDataException("Cannot perform search on empty value."),
                    _ => throw new InvalidQueryException(
                        $"Unknown method in value ({methodValue.Name}. Parameter type: {methodParameter.GetType().FullName}, Value: {methodParameter}")
                };
            }

            var aiTaskMethod = (MethodExpression)methodValue.Arguments[1];
            PortableExceptions.ThrowIfNot<InvalidOperationException>(aiTaskMethod.Name == ClientConstants.VectorSearch.AiTaskMethodName, "Expected to find an AI task method call, but got: " + aiTaskMethod.Name);
            embeddingsGenerationTaskIdentifier = QueryBuilderHelper.GetValue(metadata.Query, metadata, builderParameters.QueryParameters, (ValueExpression)aiTaskMethod.Arguments[0],
                allowObjectsInParameters: false, allowArraysInParameters: false).Value.ToString();
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
            {
                return CoraxVectorItem.BuildSingleVector(builderParameters, fieldMetadata, vector.SingleVector.Value, numberOfCandidates, minimumMatch, exact);
            }

            return CoraxVectorItem.BuildMultiVector(builderParameters, fieldMetadata, vector.MultiVector, numberOfCandidates, minimumMatch, exact);
        }

        var (value, valueType) = QueryBuilderHelper.GetValue(metadata.Query, metadata, builderParameters.QueryParameters, (ValueExpression)srcVector,
            allowObjectsInParameters: false, allowArraysInParameters: true);

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
                return avo.EmbeddingsGenerationTaskIdentifier != null;
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
                if (vectorOptions?.DestinationEmbeddingType is not null)
                    destinationEmbeddingType = vectorOptions!.DestinationEmbeddingType;
                else
                    destinationEmbeddingType = sourceEmbeddingType;
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
}
