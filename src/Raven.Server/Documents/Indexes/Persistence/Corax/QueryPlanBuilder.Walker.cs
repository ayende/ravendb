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
    private sealed class ResolutionContext
    {
        public readonly List<string> Errors = [];
        public readonly BlittableJsonReaderObject QueryParameters;
        public readonly QueryMetadata Metadata;
        public readonly IndexSearcher IndexSearcher;
        public readonly QueryBuilderParameters BuilderParams;
        public int WhenCount;
        public bool IsOr;
        public List<ClauseInfo> SpatialClauses;
        public List<ClauseInfo> VectorClauses;
        public List<ClauseInfo> Clauses;
        public List<PendingBoost> PendingBoosts;

        public int CompoundExactClauseA = -1;
        public int CompoundExactClauseB = -1;
        public bool CompoundExactAFirst;
        public int CompoundFieldDrivingClause = -1;
        public string CompoundFieldSortName;
        public bool CompoundFieldIsMultiSort;

        public ResolutionContext(PlanParameters p)
            : this(p.QueryParameters, p.Metadata, p.IndexSearcher)
        {
        }

        public ResolutionContext(QueryBuilderParameters b)
            : this(b.QueryParameters, b.Metadata, b.IndexSearcher)
        {
            BuilderParams = b;
        }

        private ResolutionContext(BlittableJsonReaderObject queryParameters, QueryMetadata metadata, IndexSearcher indexSearcher)
        {
            QueryParameters = queryParameters;
            Metadata = metadata;
            IndexSearcher = indexSearcher;
        }

        public void Report(string error) => Errors.Add(error);

        public void RecordPendingBoost(ClauseInfo[] innerClauses, ParameterBinding factor)
        {
            // record boosted clauses, so later we'll have an easier time to propogate the boosted value.
            PendingBoosts ??= [];
            PendingBoosts.Add(new PendingBoost(innerClauses, factor));
        }
    }

    private readonly record struct PendingBoost(ClauseInfo[] InnerClauses, ParameterBinding Factor);

    private static class PlanWalker
    {
        public static void RewriteClauses(ResolutionContext ctx)
        {
            var clauses = ctx.Clauses;
            BoostPropagate(ctx);
            NotCanonicalize(clauses, ctx);
            BetweenRewriteSentinels(clauses, ctx.IsOr);
            if (ctx.Metadata.IsDynamic)
                DynamicFieldNameResolve(clauses);
            GroupCollapse(clauses, ctx);
            WhenRegister(clauses, ctx);
            ThrowIfErrors(ctx);
        }

        public static void ThrowIfErrors(ResolutionContext ctx)
        {
            if (ctx.Errors.Count == 0)
                return;

            string combined = ctx.Errors.Count == 1
                ? ctx.Errors[0]
                : $"Query has {ctx.Errors.Count} validation errors:{Environment.NewLine}{string.Join(Environment.NewLine, ctx.Errors)}";
            throw new InvalidQueryException(combined);
        }

       
        private static void WhenRegister(List<ClauseInfo> clauses, ResolutionContext ctx)
        {
            ctx.WhenCount = 0;
            foreach (var t in clauses)
            {
                if (t.WhenCondition != null) 
                    ctx.WhenCount++;
            }

            if (ctx.WhenCount <= PlanTemplate.MaxWhenClauses) 
                return;
            
            throw new NotSupportedException(
                $"Query has {ctx.WhenCount} WHEN-guarded clauses; the plan template supports at most " +
                $"{PlanTemplate.MaxWhenClauses}. Split the query into multiple smaller queries.");
        }

        private static void NotCanonicalize(List<ClauseInfo> clauses, ResolutionContext ctx)
        {
            foreach (var c in clauses)
            {
                NotCanonizeRecursive(c);
            }

            void NotCanonizeRecursive(ClauseInfo c)
            {
                RuntimeHelpers.EnsureSufficientExecutionStack();
                c.IsOrChainNotEquals |= ctx.IsOr && c.IsNegated;
                bool subIsOr = c.ClauseType == ClauseType.OrGroup;
                foreach (var sub in c.SubClauses ?? [])
                {
                    NotCanonizeRecursive(sub);
                }
            }
        }

        private static void DynamicFieldNameResolve(List<ClauseInfo> clauses)
        {
            foreach (var t in clauses)
            {
                DynamicFieldNameResolveRecursive(t);
            }

            void DynamicFieldNameResolveRecursive(ClauseInfo clause)
            {
                RuntimeHelpers.EnsureSufficientExecutionStack();
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
                    // search() on document-id field must NOT be wrapped — id() is the document key that is not analyzed.
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
        }
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
        
                if (ClauseMatchesAllEntries(it) == false)
                    continue;
                if (isOr is false)
                {
                    clauses.RemoveAt(i); // "everything" in AND = tautological, remove
                    continue;
                }

                clauses.Clear(); // "everything" dominates OR → whole OR is tautological
                return;
            }
            
            bool ClauseMatchesAllEntries(ClauseInfo clause)
            {
                if (clause.ClauseType != ClauseType.Between)
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
        }
        
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
