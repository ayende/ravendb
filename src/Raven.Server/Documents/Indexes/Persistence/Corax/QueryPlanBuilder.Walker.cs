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

        public ResolutionContext(PlanParameters p)
        {
            QueryParameters = p.QueryParameters;
            Metadata = p.Metadata;
        }

        public void Report(string error) => Errors.Add(error);
    }

    /// <summary>
    /// Walker pipeline orchestrator. Runs validation/rewrite steps over the RQL AST
    /// (and later, the materialized ClauseInfo[]) in a fixed sequence.
    ///
    /// Step 4 of #25281 introduces this scaffolding with a single step
    /// (<see cref="AstShapeValidate"/>); follow-up commits add the remaining steps
    /// (AnalyzerRewrite, TimeFieldTicks, NotCanonicalize, BetweenToOrWithWhen,
    /// InNormalize, AllInNormalize, WhenRegister, BoostPropagate, GroupCollapse).
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
        /// materialized <see cref="ClauseInfo"/>[] before it is frozen. The first
        /// such step is <see cref="WhenRegister"/>; future commits add
        /// AnalyzerRewrite, TimeFieldTicks, NotCanonicalize, BetweenToOrWithWhen,
        /// InNormalize, AllInNormalize, BoostPropagate, and GroupCollapse.
        /// </summary>
        public static void RewriteClauses(ClauseInfo[] clauses, ResolutionContext ctx)
        {
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
        private static void WhenRegister(ClauseInfo[] clauses, ResolutionContext ctx)
        {
            int whenCount = 0;
            for (int i = 0; i < clauses.Length; i++)
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
