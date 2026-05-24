using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Corax.Querying.Planning;
using IndexSearcher = Corax.Querying.IndexSearcher;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Queries;
using Sparrow.Json;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

internal static partial class QueryPlanBuilder
{
    /// <summary>
    /// Per-template context shared across walker steps. Accumulates validation
    /// errors so the user sees every shape problem at once rather than one fix-edit-retry
    /// cycle per issue, threads the parameter/metadata bag walker steps need, and
    /// stores derived facts (e.g. <see cref="WhenCount"/>) walker steps publish back
    /// to the caller for inclusion in the <see cref="PlanTemplate"/>.
    /// </summary>
    private sealed class ResolutionContext
    {
        public readonly List<string> Errors = [];
        public readonly BlittableJsonReaderObject QueryParameters;
        public readonly QueryMetadata Metadata;
        public readonly IndexSearcher IndexSearcher;
        public readonly QueryBuilderParameters BuilderParams;

        /// <summary>Number of top-level WHEN-guarded clauses observed by
        /// <see cref="PlanWalker.RewriteClauses"/>. Drives <see cref="PlanTemplate.WhenCount"/>.</summary>
        public int WhenCount;

        /// <summary>True when the root expression is OR; disables several rewrites
        /// (spatial/vector partitioning, etc.) that only apply to AND queries.</summary>
        public bool IsOr;

        /// <summary>Spatial clauses partitioned out by <see cref="PlanWalker.RewriteClauses"/>
        /// (specifically the GroupCollapse step). Null when no spatial clauses are present
        /// or when the root is OR.</summary>
        public List<ClauseInfo> SpatialClauses;

        /// <summary>Vector clauses partitioned out by <see cref="PlanWalker.RewriteClauses"/>
        /// (specifically the GroupCollapse step). Null when no vector clauses are present
        /// or when the root is OR.</summary>
        public List<ClauseInfo> VectorClauses;

        /// <summary>The clause list currently being built by the materializer. Swapped
        /// in/out by <see cref="QueryPlanBuilder.ParseBinaryExpression"/> when it creates
        /// sub-lists for OrGroup/AndGroup, and by ParseNegated for its inner clause
        /// collection. Callers must save and restore when pushing a sub-scope.</summary>
        public List<ClauseInfo> Clauses;

        /// <summary>Pending boost factor propagations recorded by the materializer.
        /// Each entry pairs a snapshot of clauses inside a boost(...) wrapper with the
        /// factor binding to apply. Drained by <see cref="PlanWalker.BoostPropagate"/>.
        /// Lazily allocated — null when no boost() wrapper has been seen.</summary>
        public List<PendingBoost> PendingBoosts;

        public readonly bool HasBoost;

        // Compound optimization results — set by FindCompoundExactPair / FindCompoundFieldCandidate.
        public int CompoundExactClauseA = -1;
        public int CompoundExactClauseB = -1;
        public bool CompoundExactAFirst;
        public int CompoundFieldDrivingClause = -1;
        public string CompoundFieldSortName;
        public bool CompoundFieldIsMultiSort;

        public ResolutionContext(PlanParameters p)
            : this(p.QueryParameters, p.Metadata, p.IndexSearcher)
        {
            HasBoost = p.HasBoost;
        }

        public ResolutionContext(QueryBuilderParameters b)
            : this(b.QueryParameters, b.Metadata, b.IndexSearcher)
        {
            BuilderParams = b;
            HasBoost = b.HasBoost;
        }

        /// <summary>Construct from raw bag/metadata. Used by sub-expression entry points
        /// (e.g. <see cref="BuildFromSubExpression"/>) that do not have a full
        /// <see cref="PlanParameters"/> available.</summary>
        private ResolutionContext(BlittableJsonReaderObject queryParameters, QueryMetadata metadata, IndexSearcher indexSearcher)
        {
            QueryParameters = queryParameters;
            Metadata = metadata;
            IndexSearcher = indexSearcher;
        }

        public void Report(string error) => Errors.Add(error);

        /// <summary>Record a boost() wrapper for later application by
        /// <see cref="PlanWalker.BoostPropagate"/>. Captures the inner clauses by
        /// reference, so subsequent list manipulation (e.g. GroupCollapse) does not
        /// invalidate the propagation target.</summary>
        public void RecordPendingBoost(ClauseInfo[] innerClauses, ParameterBinding factor)
        {
            PendingBoosts ??= [];
            PendingBoosts.Add(new PendingBoost(innerClauses, factor));
        }
    }

    /// <summary>Snapshot of a boost() wrapper's inner clauses with the factor to apply.
    /// See <see cref="ResolutionContext.PendingBoosts"/>.</summary>
    private readonly record struct PendingBoost(ClauseInfo[] InnerClauses, ParameterBinding Factor);

    /// <summary>
    /// Walker pipeline orchestrator. Runs validation/rewrite steps over the RQL AST
    /// (and later, the materialized ClauseInfo[]) in a fixed sequence.
    ///
    /// Step 4 of #25281 introduces this scaffolding with a single step
    /// (<see cref="AstShapeValidate"/>); follow-up commits add the remaining steps
    /// (AnalyzerRewrite, TimeFieldTicks, InNormalize, AllInNormalize).
    /// </summary>
    private static class PlanWalker
    {
        /// <summary>
        /// Post-materialization phase. Runs rewrite/registration steps over the
        /// materialized <see cref="ClauseInfo"/> list before it is frozen. Receives
        /// a mutable <see cref="List{T}"/> so steps can repartition (GroupCollapse
        /// pulls spatial/vector clauses out into <see cref="ResolutionContext.SpatialClauses"/>
        /// / <see cref="ResolutionContext.VectorClauses"/>). Future commits add
        /// AnalyzerRewrite, TimeFieldTicks, InNormalize, and AllInNormalize.
        /// </summary>
        public static void RewriteClauses(ResolutionContext ctx)
        {
            var clauses = ctx.Clauses;
            BoostPropagate(ctx);
            NotCanonicalize(clauses, ctx);
            BetweenRewriteSentinels(clauses, ctx.IsOr);
            InPreClassify(clauses);
            if (ctx.Metadata.IsDynamic) 
                DynamicFieldNameResolve(clauses);
            GroupCollapse(clauses, ctx);
            WhenRegister(clauses, ctx);
            ThrowIfErrors(ctx);
        }

        public static void ThrowIfErrors(ResolutionContext ctx)
        {
            if (ctx.Errors.Count == 0)
            {
                return;
            }

            string combined = ctx.Errors.Count == 1
                ? ctx.Errors[0]
                : "Query has " + ctx.Errors.Count + " validation errors:" + Environment.NewLine
                    + string.Join(Environment.NewLine, ctx.Errors);
            throw new InvalidQueryException(combined);
        }

        /// <summary>
        /// Count top-level WHEN-guarded clauses and reject templates that exceed
        /// <see cref="PlanTemplate.MaxWhenClauses"/>. The count is published via
        /// <see cref="ResolutionContext.WhenCount"/> so <see cref="ParseTemplate"/>
        /// can copy it onto the resulting <see cref="PlanTemplate"/>.
        ///
        /// The cap is a structural limit (WhenFlags is a 32-bit mask), not a query
        /// shape error, so it throws <see cref="NotSupportedException"/> directly
        /// rather than accumulating into <see cref="ResolutionContext.Errors"/>.
        /// </summary>
        private static void WhenRegister(List<ClauseInfo> clauses, ResolutionContext ctx)
        {
            ctx.WhenCount = 0;
            foreach (var t in clauses)
            {
                if (t.WhenCondition != null) 
                    ctx.WhenCount++;
            }

            if (ctx.WhenCount <= PlanTemplate.MaxWhenClauses) return;
            throw new NotSupportedException(
                $"Query has {ctx.WhenCount} WHEN-guarded clauses; the plan template supports at most " +
                $"{PlanTemplate.MaxWhenClauses}. Split the query into multiple smaller queries.");
        }

        /// <summary>
        /// For IN/AllIn clauses where ALL bindings are literals (no parameters), pre-compute
        /// the dominant type at template time. Sets <see cref="ClauseInfo.AllBindingsAreLiteral"/> and
        /// <see cref="ClauseInfo.InDominantType"/>. At execution time, <see cref="ResolveInFromBindings"/>
        /// skips the dominant-type scan and type-incompatible filtering for these clauses.
        /// Recurses into OrGroup/AndGroup sub-clauses.
        /// </summary>
        private static void InPreClassify(List<ClauseInfo> clauses)
        {
            foreach (var t in clauses)
            {
                InPreClassifyRecursive(t);
            }
        }

        private static void InPreClassifyRecursive(ClauseInfo clause)
        {
            foreach (var t in clause.SubClauses ?? [])
            {
                InPreClassifyRecursive(t);
            }
            
            if (clause.ClauseType is not (ClauseType.In or ClauseType.AllIn) || clause.Bindings is not { Length: not 0 })
                return;

            // Check if all bindings are literals
            foreach (var t in clause.Bindings)
            {
                if (t.Source != BindingSource.Literal)
                    return; // has parameter bindings — can't pre-classify
            }

            // All literal: compute dominant type
            ParamValueType dominant = ParamValueType.Null;
            foreach (var t in clause.Bindings)
            {
                if (t.LiteralValue == null)
                    continue;
                if (dominant == ParamValueType.Null) 
                    dominant = t.LiteralType;
            }
            if (dominant == ParamValueType.Null)
            {
                dominant = ParamValueType.String;
            }

            clause.AllBindingsAreLiteral = true;
            clause.InDominantType = dominant;
        }

        /// <summary>
        /// For OR-rooted templates, mark every top-level negated clause with
        /// <see cref="ClauseInfo.IsOrChainNotEquals"/> = true. 
        ///
        /// Covers NotEquals, NOT IN, NOT AllIn, NOT exists(), NOT startsWith(), etc.
        /// The flag tells <c>CreateNotEqualsOrMatch</c> to pre-materialise
        /// AllEntries ANDNOT(positive form) into a BitmapMatch — required because the
        /// raw posting list / range / tree-scan can't deliver the complement directly,
        /// and the OR chain needs the complement bitmap to OrWith into.
        ///
        /// No-op for AND-rooted templates: <see cref="ClauseInfo.IsOrChainNotEquals"/>
        /// is meaningful only inside an OR chain.
        /// </summary>
        private static void NotCanonicalize(List<ClauseInfo> clauses, ResolutionContext ctx)
        {
            if (ctx.IsOr)
            {
                foreach (var c in clauses)
                {
                    if (c.IsNegated || c.ClauseType == ClauseType.NotEquals)
                    {
                        c.IsOrChainNotEquals = true;
                    }
                }
            }

            // Recurse into nested groups. Negation polarity on a direct child of any OrGroup
            // (top-level or nested) must be materialized as AllEntries ANDNOT(positive); the
            // raw posting list / range / tree-scan can't deliver the complement. Direct
            // children of an AndGroup don't get the flag — AND-rooted negation is handled
            // by the firstIsNegated / AndNotWith path which subtracts the positive form.
            foreach (var c in clauses)
            {
                if (c.SubClauses is not { Count: > 0 } subs)
                    continue;
                bool subIsOr = c.ClauseType == ClauseType.OrGroup;
                foreach (var sub in subs)
                {
                    if (subIsOr && (sub.IsNegated || sub.ClauseType == ClauseType.NotEquals))
                        sub.IsOrChainNotEquals = true;
                }
                NotCanonicalizeRecursive(subs);
            }
        }

        /// <summary>Recursive helper for <see cref="NotCanonicalize"/>. Walks each clause's
        /// SubClauses and flags negated direct children of any OrGroup with IsOrChainNotEquals.</summary>
        private static void NotCanonicalizeRecursive(List<ClauseInfo> clauses)
        {
            foreach (var c in clauses)
            {
                if (c.SubClauses is not { Count: > 0 } subs)
                    continue;
                bool subIsOr = c.ClauseType == ClauseType.OrGroup;
                foreach (var sub in subs)
                {
                    if (subIsOr && (sub.IsNegated || sub.ClauseType == ClauseType.NotEquals))
                        sub.IsOrChainNotEquals = true;
                }
                NotCanonicalizeRecursive(subs);
            }
        }

        /// <summary>
        /// For dynamic (auto-) indexes, pre-resolve field names to their exact or search
        /// variants at template time. On dynamic indexes every exact clause resolves its
        /// field to <c>exact(FieldName)</c> and every search clause to <c>search(FieldName)</c>
        /// (except document-id fields). These string allocations previously happened
        /// per-clause per-execution in ResolveClause / ResolveFieldMetadata / ResolveTermSources.
        /// By rewriting at template time the execution paths can use <see cref="ClauseInfo.FieldName"/>
        /// directly — saving one string allocation per clause per query execution.
        ///
        /// Only runs when <see cref="ResolutionContext.Metadata"/> indicates a dynamic index.
        /// </summary>
        private static void DynamicFieldNameResolve(List<ClauseInfo> clauses)
        {
            foreach (var t in clauses)
            {
                DynamicFieldNameResolveRecursive(t);
            }
        }

        private static void DynamicFieldNameResolveRecursive(ClauseInfo clause)
        {
            foreach (var t in clause.SubClauses ?? [])
            {
                DynamicFieldNameResolveRecursive(t);
            }

            if (clause.FieldName == null || 
                // Spatial and Vector clauses handle their own field resolution — skip them.
                clause.ClauseType is ClauseType.Spatial or ClauseType.Vector)
                return;

            if (clause.ClauseType == ClauseType.Search)
            {
                // search() on document-id field must NOT be wrapped — id() is the document
                // key which is not analyzed. Matches Lucene's HandleSearch guard.
                if (string.Equals(clause.FieldName,
                        Client.Constants.Documents.Indexing.Fields.DocumentIdFieldName,
                        StringComparison.Ordinal) == false)
                {
                    clause.ResolvedFieldName = AutoIndexField.GetSearchAutoIndexFieldName(clause.FieldName);
                }
            }
            else if (clause.IsExact)
            {
                clause.ResolvedFieldName = AutoIndexField.GetExactAutoIndexFieldName(clause.FieldName);
            }
        }

        /// <summary>
        /// Rewrite BETWEEN clauses whose literal bindings carry the client's unbounded-bound
        /// sentinel strings ("*" for low, "NULL" for high) into equivalent range clauses:
        /// <list type="bullet">
        ///   <item>Low sentinel only → <see cref="ClauseType.LessThanOrEqual"/> on the high bound.</item>
        ///   <item>High sentinel only → <see cref="ClauseType.GreaterThanOrEqual"/> on the low bound.</item>
        ///   <item>Both sentinels → clause removed (matches everything; tautological in AND, dominates in OR).</item>
        /// </list>
        ///
        /// Recurses into OrGroup/AndGroup sub-clauses. For both-sentinel sub-clauses inside
        /// a group, the sub-clause is removed from the group's list.
        /// </summary>
        private static void BetweenRewriteSentinels(List<ClauseInfo> clauses, bool isOr)
        {
            RuntimeHelpers.EnsureSufficientExecutionStack();
            if (clauses is null) return;
            for (int i = clauses.Count - 1; i >= 0; i--)
            {
                ClauseInfo it = clauses[i];
                BetweenRewriteSentinels(it.SubClauses, isOr: it.ClauseType == ClauseType.OrGroup);
                    
                // After recursion, remove groups that became empty (tautological OR cleared by a child both-sentinel).
                if (it is { ClauseType: ClauseType.OrGroup, SubClauses.Count: 0 })
                {
                    if (isOr is false)
                    {
                        clauses.RemoveAt(i); // tautological in AND = remove
                        continue;
                    }
                    // tautological propagates up
                    clauses.Clear(); 
                    return;
                }
        
                if (TryRewriteBetweenSentinel(it) == false)
                    continue;
                if (!isOr)
                {
                    clauses.RemoveAt(i); // "everything" in AND = tautological, remove
                    continue;
                }

                clauses.Clear(); // "everything" dominates OR → whole OR is tautological
                return;
            }
        }
        
        /// <summary>Rewrite a single BETWEEN clause with literal sentinel bounds.
        /// Returns true if the caller should remove because both are sentinels.</summary>
        private static bool TryRewriteBetweenSentinel(ClauseInfo clause)
        {
            if (clause.ClauseType != ClauseType.Between || clause.Bindings is not { Length: >= 2 })
                return false;

            bool lowIsSentinel = clause.Bindings[BindingIndex.BetweenLow] is
            {
                LiteralType: ParamValueType.String, 
                LiteralValue: Client.Constants.Documents.Querying.Terms.LeftNullValueOfBetweenQuery
            };

            bool highIsSentinel = clause.Bindings[BindingIndex.BetweenHigh] is
            {
                LiteralType: ParamValueType.String, 
                LiteralValue: Client.Constants.Documents.Querying.Terms.RightNullValueOfBetweenQuery
            };

            return lowIsSentinel &&  highIsSentinel;
        }

        /// <summary>
        /// The materializer records the wrapper at boost() encounter time; the actual
        /// propagation happens here so the rewrite is centralised in the walker rather
        /// than splintered across ParseMethod's recursive descent.
        /// </summary>
        private static void BoostPropagate(ResolutionContext ctx)
        {
            foreach (var pending in ctx.PendingBoosts ?? [])
            {
                foreach (var t in pending.InnerClauses)
                {
                    if (t.ClauseType == ClauseType.Vector)
                        throw new NotSupportedException("Boosting the VectorSearchMatch is not supported yet.");
                    t.Bindings = [..t.Bindings ?? [], pending.Factor];
                    t.HasBoost = true;
                }
            }
        }

        /// <summary>
        /// AND-query post-pass that pulls spatial and vector clauses out of the main
        /// filter list into per-template aux arrays. Spatial and vector clauses are
        /// dispatched on their own paths at execution time (separate IL emission and
        /// per-execution materialization), so they must not be intermixed with the
        /// regular filter chain.
        /// </summary>
        private static void GroupCollapse(List<ClauseInfo> clauses, ResolutionContext ctx)
        {
            if (ctx.IsOr)
                return;

            for (int i = clauses.Count - 1; i >= 0; i--)
            {
                var list = clauses[i].ClauseType switch
                {
                    ClauseType.Spatial => ctx.SpatialClauses ??= [],
                    ClauseType.Vector => ctx.VectorClauses ??= [],
                    _ => null
                };
                if(list is null) continue;
                list.Add(clauses[i]);
                clauses.RemoveAt(i);
            }
        }
    }
}
