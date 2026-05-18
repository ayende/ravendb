using System;
using System.Collections.Generic;
using Corax.Querying.Planning;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
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

        /// <summary>Number of top-level WHEN-guarded clauses observed by
        /// <see cref="PlanWalker.RewriteClauses"/>. Drives <see cref="PlanTemplate.WhenCount"/>.</summary>
        public int WhenCount;

        /// <summary>True when the root expression is OR; disables several rewrites
        /// (spatial/vector partitioning, etc.) that only apply to AND queries.</summary>
        public bool IsOr;

        /// <summary>Spatial clauses partitioned out by <see cref="PlanWalker.RewriteClauses"/>
        /// (specifically the GroupCollapse step). Null when no spatial clauses are present
        /// or when the root is OR.</summary>
        public ClauseInfo[] SpatialClauses;

        /// <summary>Vector clauses partitioned out by <see cref="PlanWalker.RewriteClauses"/>
        /// (specifically the GroupCollapse step). Null when no vector clauses are present
        /// or when the root is OR.</summary>
        public ClauseInfo[] VectorClauses;

        /// <summary>Pending boost factor propagations recorded by the materializer.
        /// Each entry pairs a snapshot of clauses inside a boost(...) wrapper with the
        /// factor binding to apply. Drained by <see cref="PlanWalker.BoostPropagate"/>.
        /// Lazily allocated — null when no boost() wrapper has been seen.</summary>
        public List<PendingBoost> PendingBoosts;

        public ResolutionContext(PlanParameters p)
            : this(p.QueryParameters, p.Metadata)
        {
        }

        /// <summary>Construct from raw bag/metadata. Used by sub-expression entry points
        /// (e.g. <see cref="BuildFromSubExpression"/>) that do not have a full
        /// <see cref="PlanParameters"/> available.</summary>
        public ResolutionContext(BlittableJsonReaderObject queryParameters, QueryMetadata metadata)
        {
            QueryParameters = queryParameters;
            Metadata = metadata;
        }

        public void Report(string error) => Errors.Add(error);

        /// <summary>Record a boost() wrapper for later application by
        /// <see cref="PlanWalker.BoostPropagate"/>. Captures the inner clauses by
        /// reference so subsequent list manipulation (e.g. GroupCollapse) does not
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
        /// Pre-materialization phase. Runs validation steps that need access to the
        /// RQL AST (currently just <see cref="AstShapeValidate"/>). Throws a single
        /// <see cref="InvalidQueryException"/> with every accumulated shape error if
        /// validation fails; otherwise returns control to the caller for
        /// materialization (<see cref="ParseExpression"/>).
        /// </summary>
        public static void ValidateAst(QueryExpression where, ResolutionContext ctx)
        {
            AstShapeValidate(where, ctx);
            ThrowIfErrors(ctx);
        }

        /// <summary>
        /// Post-materialization phase. Runs rewrite/registration steps over the
        /// materialized <see cref="ClauseInfo"/> list before it is frozen. Receives
        /// a mutable <see cref="List{T}"/> so steps can repartition (GroupCollapse
        /// pulls spatial/vector clauses out into <see cref="ResolutionContext.SpatialClauses"/>
        /// / <see cref="ResolutionContext.VectorClauses"/>). Future commits add
        /// AnalyzerRewrite, TimeFieldTicks, InNormalize, and AllInNormalize.
        /// </summary>
        public static void RewriteClauses(List<ClauseInfo> clauses, ResolutionContext ctx)
        {
            BoostPropagate(ctx);
            NotCanonicalize(clauses, ctx);
            BetweenRewriteSentinels(clauses);
            InPreClassify(clauses);
            if (ctx.Metadata.IsDynamic)
                DynamicFieldNameResolve(clauses);
            GroupCollapse(clauses, ctx);
            WhenRegister(clauses, ctx);
            ThrowIfErrors(ctx);
        }

        private static void ThrowIfErrors(ResolutionContext ctx)
        {
            if (ctx.Errors.Count == 0)
                return;

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
            int whenCount = 0;
            for (int i = 0; i < clauses.Count; i++)
            {
                if (clauses[i].WhenCondition != null)
                    whenCount++;
            }
            if (whenCount > PlanTemplate.MaxWhenClauses)
            {
                throw new NotSupportedException(
                    $"Query has {whenCount} WHEN-guarded clauses; the plan template supports at most " +
                    $"{PlanTemplate.MaxWhenClauses}. Split the query into multiple smaller queries.");
            }
            ctx.WhenCount = whenCount;
        }

        /// <summary>
        /// For IN/AllIn clauses where ALL bindings are literals (no parameters), pre-compute
        /// the dominant type at template time. Sets <see cref="ClauseInfo.InAllLiteral"/> and
        /// <see cref="ClauseInfo.InDominantType"/>. At execution time, <see cref="ResolveInFromBindings"/>
        /// skips the dominant-type scan and type-incompatible filtering for these clauses.
        /// Recurses into OrGroup/AndGroup sub-clauses.
        /// </summary>
        private static void InPreClassify(List<ClauseInfo> clauses)
        {
            for (int i = 0; i < clauses.Count; i++)
                InPreClassifyRecursive(clauses[i]);
        }

        private static void InPreClassifyRecursive(ClauseInfo clause)
        {
            if (clause.OrSubClauses != null)
                for (int i = 0; i < clause.OrSubClauses.Count; i++)
                    InPreClassifyRecursive(clause.OrSubClauses[i]);
            if (clause.AndSubClauses != null)
                for (int i = 0; i < clause.AndSubClauses.Count; i++)
                    InPreClassifyRecursive(clause.AndSubClauses[i]);

            if (clause.ClauseType is not (ClauseType.In or ClauseType.AllIn))
                return;
            if (clause.Bindings == null || clause.Bindings.Length == 0)
                return;

            // Check if all bindings are literals
            for (int i = 0; i < clause.Bindings.Length; i++)
            {
                if (clause.Bindings[i].Source != BindingSource.Literal)
                    return; // has parameter bindings — can't pre-classify
            }

            // All literal: compute dominant type
            ParamValueType dominant = ParamValueType.Null;
            for (int i = 0; i < clause.Bindings.Length; i++)
            {
                if (clause.Bindings[i].LiteralValue == null)
                    continue;
                if (dominant == ParamValueType.Null)
                    dominant = clause.Bindings[i].LiteralType;
            }
            if (dominant == ParamValueType.Null)
                dominant = ParamValueType.String;

            clause.InAllLiteral = true;
            clause.InDominantType = dominant;
        }

        /// <summary>
        /// For OR-rooted templates, mark every top-level negated clause with
        /// <see cref="ClauseInfo.IsOrChainNotEquals"/> = true. This was previously a
        /// per-execution decision in <see cref="EmitPlan"/> — observing a negated clause
        /// inside an OR chain, it would <see cref="ClauseInfo.Clone"/> the template
        /// ClauseInfo, flip the flag on the clone, and swap it into the per-execution
        /// list because the template was frozen. Performing the flip here, before
        /// <see cref="FreezeAll"/>, means the template already carries the canonical
        /// value and the per-execution clone is unnecessary.
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
            if (ctx.IsOr == false)
                return;

            for (int i = 0; i < clauses.Count; i++)
            {
                var c = clauses[i];
                if (c.IsNegated || c.ClauseType == ClauseType.NotEquals)
                    c.IsOrChainNotEquals = true;
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
            for (int i = 0; i < clauses.Count; i++)
                DynamicFieldNameResolveRecursive(clauses[i]);
        }

        private static void DynamicFieldNameResolveRecursive(ClauseInfo clause)
        {
            if (clause.OrSubClauses != null)
                for (int i = 0; i < clause.OrSubClauses.Count; i++)
                    DynamicFieldNameResolveRecursive(clause.OrSubClauses[i]);
            if (clause.AndSubClauses != null)
                for (int i = 0; i < clause.AndSubClauses.Count; i++)
                    DynamicFieldNameResolveRecursive(clause.AndSubClauses[i]);

            if (clause.FieldName == null)
                return;

            // Spatial and Vector clauses handle their own field resolution — skip them.
            if (clause.ClauseType is ClauseType.Spatial or ClauseType.Vector)
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
        ///   <item>High sentinel only → negated <see cref="ClauseType.LessThan"/> on the low bound.
        ///     The negation gives ANDNOT semantics at execution time, which preserves the Lucene
        ///     quirk where <c>WhereBetween(field, low, null)</c> includes null-valued docs.</item>
        ///   <item>Both sentinels → clause removed (matches everything; tautological in AND,
        ///     dominates in OR).</item>
        /// </list>
        ///
        /// After this step, sentinel BETWEEN clauses no longer exist at execution time, so
        /// <c>SentinelBetweenBit</c> (bit 31 of OperandOrdering) is unnecessary and the bit
        /// is freed for future use. Sentinel constants are <c>internal</c> to Raven.Client and
        /// only appear as literals in the RQL emitted by the .NET SDK — parameter-bound
        /// sentinels are not supported and will be treated as normal string values.
        ///
        /// Recurses into OrGroup/AndGroup sub-clauses. For both-sentinel sub-clauses inside
        /// a group, the sub-clause is removed from the group's list.
        /// </summary>
        private static void BetweenRewriteSentinels(List<ClauseInfo> clauses)
        {
            for (int i = clauses.Count - 1; i >= 0; i--)
            {
                BetweenRewriteSubClauses(clauses[i]);
                if (TryRewriteBetweenSentinel(clauses[i], out bool bothSentinel))
                {
                    if (bothSentinel)
                        clauses.RemoveAt(i);
                }
            }
        }

        private static void BetweenRewriteSubClauses(ClauseInfo clause)
        {
            if (clause.OrSubClauses != null)
            {
                for (int i = clause.OrSubClauses.Count - 1; i >= 0; i--)
                {
                    BetweenRewriteSubClauses(clause.OrSubClauses[i]);
                    if (TryRewriteBetweenSentinel(clause.OrSubClauses[i], out bool bothSentinel) && bothSentinel)
                        clause.OrSubClauses.RemoveAt(i);
                }
            }
            if (clause.AndSubClauses != null)
            {
                for (int i = clause.AndSubClauses.Count - 1; i >= 0; i--)
                {
                    BetweenRewriteSubClauses(clause.AndSubClauses[i]);
                    if (TryRewriteBetweenSentinel(clause.AndSubClauses[i], out bool bothSentinel) && bothSentinel)
                        clause.AndSubClauses.RemoveAt(i);
                }
            }
        }

        /// <summary>Rewrite a single BETWEEN clause with literal sentinel bounds.
        /// Returns true if the clause was rewritten (caller should remove if bothSentinel).</summary>
        private static bool TryRewriteBetweenSentinel(ClauseInfo clause, out bool bothSentinel)
        {
            bothSentinel = false;
            if (clause.ClauseType != ClauseType.Between || clause.Bindings is not { Length: >= 2 })
                return false;

            bool lowIsSentinel = clause.Bindings[BindingIndex.BetweenLow] is
                { LiteralType: ParamValueType.String, LiteralValue: string ls }
                && ls == Raven.Client.Constants.Documents.Querying.Terms.LeftNullValueOfBetweenQuery;

            bool highIsSentinel = clause.Bindings[BindingIndex.BetweenHigh] is
                { LiteralType: ParamValueType.String, LiteralValue: string hs }
                && hs == Raven.Client.Constants.Documents.Querying.Terms.RightNullValueOfBetweenQuery;

            if (!lowIsSentinel && !highIsSentinel)
                return false;

            if (lowIsSentinel && highIsSentinel)
            {
                bothSentinel = true;
                return true;
            }

            if (lowIsSentinel)
            {
                // BETWEEN '*' AND high → field <= high
                clause.ClauseType = ClauseType.LessThanOrEqual;
                clause.Bindings = [clause.Bindings[BindingIndex.BetweenHigh]];
            }
            else
            {
                // BETWEEN low AND 'NULL' → NOT (field < low)
                // ANDNOT semantics preserve the Lucene quirk: null-valued docs stay.
                clause.ClauseType = ClauseType.LessThan;
                clause.IsNegated = true;
                clause.Bindings = [clause.Bindings[BindingIndex.BetweenLow]];
            }

            return true;
        }

        /// <summary>
        /// Walks <see cref="ResolutionContext.PendingBoosts"/> and applies each recorded
        /// factor binding to its inner clauses by appending the factor to
        /// <see cref="ClauseInfo.Bindings"/> and flipping <see cref="ClauseInfo.HasBoost"/>.
        ///
        /// The materializer records the wrapper at boost() encounter time; the actual
        /// propagation happens here so the rewrite is centralised in the walker rather
        /// than splintered across ParseMethod's recursive descent.
        /// </summary>
        private static void BoostPropagate(ResolutionContext ctx)
        {
            if (ctx.PendingBoosts == null)
                return;

            for (int i = 0; i < ctx.PendingBoosts.Count; i++)
            {
                var pending = ctx.PendingBoosts[i];
                var inner = pending.InnerClauses;
                for (int c = 0; c < inner.Length; c++)
                {
                    if (inner[c].ClauseType == ClauseType.Vector)
                    {
                        throw new NotSupportedException("Boosting the VectorSearchMatch is not supported yet.");
                    }
                    var old = inner[c].Bindings;
                    var extended = new ParameterBinding[(old?.Length ?? 0) + 1];
                    if (old != null) Array.Copy(old, extended, old.Length);
                    extended[^1] = pending.Factor;
                    inner[c].Bindings = extended;
                    inner[c].HasBoost = true;
                }
            }
        }

        /// <summary>
        /// AND-query post-pass that pulls spatial and vector clauses out of the main
        /// filter list into per-template aux arrays. Spatial and vector clauses are
        /// dispatched on their own paths at execution time (separate IL emission and
        /// per-execution materialization), so they must not be intermixed with the
        /// regular filter chain.
        ///
        /// No-op on OR queries (<see cref="ResolutionContext.IsOr"/> = true), which
        /// don't support spatial/vector partitioning today; any spatial or vector
        /// clause in an OR query stays in the main list and follows the standard
        /// dispatch path.
        /// </summary>
        private static void GroupCollapse(List<ClauseInfo> clauses, ResolutionContext ctx)
        {
            if (ctx.IsOr)
                return;

            List<ClauseInfo> spatialList = null;
            List<ClauseInfo> vectorList = null;
            for (int i = clauses.Count - 1; i >= 0; i--)
            {
                switch (clauses[i].ClauseType)
                {
                    case ClauseType.Spatial:
                        spatialList ??= [];
                        spatialList.Add(clauses[i]);
                        clauses.RemoveAt(i);
                        break;
                    case ClauseType.Vector:
                        vectorList ??= [];
                        vectorList.Add(clauses[i]);
                        clauses.RemoveAt(i);
                        break;
                }
            }
            ctx.SpatialClauses = spatialList?.ToArray();
            ctx.VectorClauses = vectorList?.ToArray();
        }

        /// <summary>
        /// Walks the RQL WHERE AST and accumulates shape errors into
        /// <see cref="ResolutionContext.Errors"/>. Mirrors the conditions that
        /// the materialization helpers (ParseComparison, ParseRangeComparison,
        /// ParseBetween, ParseIn, ParseMethod, ParseSearchMethod, ParsePrefixMethod)
        /// would have thrown on directly; reports them as a batch so the user
        /// sees every issue in a single response.
        ///
        /// Unexpected expression types and unexpected operator/method enum values
        /// are intentionally not flagged here — those are internal-invariant
        /// violations and remain <see cref="InvalidOperationException"/> sites in
        /// the materialization helpers.
        /// </summary>
        private static void AstShapeValidate(QueryExpression expr, ResolutionContext ctx)
        {
            switch (expr)
            {
                case BinaryExpression be:
                    ValidateBinary(be, ctx);
                    return;

                case BetweenExpression bw:
                    ValidateBetween(bw, ctx);
                    return;

                case InExpression ie:
                    ValidateIn(ie, ctx);
                    return;

                case NegatedExpression ne:
                    AstShapeValidate(ne.Expression, ctx);
                    return;

                case MethodExpression me:
                    ValidateMethod(me, ctx);
                    return;
            }
        }

        private static void ValidateBinary(BinaryExpression be, ResolutionContext ctx)
        {
            switch (be.Operator)
            {
                case OperatorType.And:
                case OperatorType.Or:
                    AstShapeValidate(be.Left, ctx);
                    AstShapeValidate(be.Right, ctx);
                    return;

                case OperatorType.Equal:
                case OperatorType.NotEqual:
                    if (TryGetFieldName(be.Left, ctx.Metadata, ctx.QueryParameters, out _) == false)
                        ctx.Report($"Comparison left side must be a field expression or id(), but got: {be.Left.Type}");
                    return;

                case OperatorType.LessThan:
                case OperatorType.LessThanEqual:
                case OperatorType.GreaterThan:
                case OperatorType.GreaterThanEqual:
                    if (TryGetFieldName(be.Left, ctx.Metadata, ctx.QueryParameters, out _) == false)
                        ctx.Report($"Range comparison left side must be a field expression or id(), but got: {be.Left.Type}");
                    return;
            }
        }

        private static void ValidateBetween(BetweenExpression between, ResolutionContext ctx)
        {
            if (TryGetFieldName(between.Source, ctx.Metadata, ctx.QueryParameters, out string field) == false)
            {
                ctx.Report($"BETWEEN source must be a field expression or id(), but got: {between.Source.Type}");
                return;
            }

            // Mirror the literal/literal type-mismatch check from ParseBetween. Parameter-typed
            // bindings are validated later, in PopulateParameters, when the actual value is known.
            var minBinding = CreateBinding(between.Min, ctx.QueryParameters);
            var maxBinding = CreateBinding(between.Max, ctx.QueryParameters);
            if (minBinding is { LiteralType: not ParamValueType.Parameter }
                && maxBinding is { LiteralType: not ParamValueType.Parameter }
                && minBinding.LiteralType != maxBinding.LiteralType)
            {
                ctx.Report(
                    $"BETWEEN bounds for field '{field}' have different types: " +
                    $"low is {minBinding.LiteralType}, high is {maxBinding.LiteralType}. Both must be the same type.");
            }
        }

        private static void ValidateIn(InExpression inExpr, ResolutionContext ctx)
        {
            if (TryGetFieldName(inExpr.Source, ctx.Metadata, ctx.QueryParameters, out _) == false)
                ctx.Report($"IN source must be a field expression or id(), but got: {inExpr.Source.Type}");
        }

        private static void ValidateMethod(MethodExpression method, ResolutionContext ctx)
        {
            var methodType = QueryMethod.GetMethodType(method.Name.Value);
            switch (methodType)
            {
                case MethodType.Search:
                    if (method.Arguments.Count < 2)
                    {
                        ctx.Report($"search() requires at least 2 arguments (field, term), but got {method.Arguments.Count}.");
                        return;
                    }
                    if (TryGetFieldName(method.Arguments[0], ctx.Metadata, ctx.QueryParameters, out _) == false)
                        ctx.Report($"search() first argument must be a field name, but got: {method.Arguments[0].Type} ({method.Arguments[0]}).");
                    return;

                case MethodType.StartsWith:
                    ValidatePrefixMethod(method, ClauseType.StartsWith, ctx);
                    return;

                case MethodType.EndsWith:
                    ValidatePrefixMethod(method, ClauseType.EndsWith, ctx);
                    return;

                case MethodType.Exists:
                    if (method.Arguments.Count == 0)
                    {
                        ctx.Report("exists() requires a field argument.");
                        return;
                    }
                    if (TryGetFieldName(method.Arguments[0], ctx.Metadata, ctx.QueryParameters, out _) == false)
                        ctx.Report($"exists() argument must be a field name, but got: {method.Arguments[0].Type} ({method.Arguments[0]}).");
                    return;

                case MethodType.Regex:
                    if (method.Arguments.Count < 2)
                    {
                        ctx.Report($"regex() requires at least 2 arguments (field, pattern), but got {method.Arguments.Count}.");
                        return;
                    }
                    if (TryGetFieldName(method.Arguments[0], ctx.Metadata, ctx.QueryParameters, out _) == false)
                        ctx.Report($"regex() first argument must be a field name, but got: {method.Arguments[0].Type} ({method.Arguments[0]}).");
                    return;

                case MethodType.Exact:
                case MethodType.Boost:
                    if (method.Arguments.Count > 0)
                        AstShapeValidate(method.Arguments[0], ctx);
                    return;

                case MethodType.When:
                    // when(condition, expr) — only the body is materialized into clauses.
                    // The condition is evaluated against query parameters at execution time
                    // and is not part of AST shape validation here.
                    if (method.Arguments.Count == 2)
                        AstShapeValidate(method.Arguments[1], ctx);
                    return;

                case MethodType.Spatial_Within:
                case MethodType.Spatial_Contains:
                case MethodType.Spatial_Disjoint:
                case MethodType.Spatial_Intersects:
                    if (method.Arguments.Count >= 2 && method.Arguments[1] is not MethodExpression)
                        ctx.Report($"Spatial shape argument must be a method expression (spatial.circle or spatial.wkt), but got: {method.Arguments[1].Type}");
                    return;
            }
        }

        private static void ValidatePrefixMethod(MethodExpression method, ClauseType type, ResolutionContext ctx)
        {
            if (method.Arguments.Count < 2)
            {
                ctx.Report($"{type}() requires at least 2 arguments (field, term), but got {method.Arguments.Count}.");
                return;
            }
            if (TryGetFieldName(method.Arguments[0], ctx.Metadata, ctx.QueryParameters, out _) == false)
                ctx.Report($"{type}() first argument must be a field name, but got: {method.Arguments[0].Type} ({method.Arguments[0]}).");
        }
    }
}
