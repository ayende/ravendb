using System;
using System.Collections.Generic;
using System.Threading;
using Corax.Querying;
using Corax.Querying.Matches;
using Corax.Querying.Planning;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Sparrow.Json;
using Sparrow.Server;
using IndexSearcher = Corax.Querying.IndexSearcher;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

/// <summary>
/// Builds a QueryPlan from a parsed RQL query. Replaces CoraxQueryBuilder
/// for query execution in Corax 2.0.
///
/// Current status: handles basic AND/OR of term equality predicates.
/// TODO: range, between, IN, negation, methods (search, startsWith, etc.),
/// spatial, vector, boost, when, exact.
/// </summary>
internal static class QueryPlanBuilder
{
    public static QueryPlan BuildPlan(
        IndexSearcher indexSearcher,
        QueryMetadata metadata,
        BlittableJsonReaderObject queryParameters,
        CancellationToken token)
    {
        var query = metadata.Query;
        if (query.Where == null)
            return BuildAllEntriesPlan();

        // Parse WHERE clause into flat clause list
        var clauses = new List<ClauseInfo>();
        bool isOr = false;
        ParseWhere(query.Where, clauses, ref isOr, queryParameters);

        if (clauses.Count == 0)
            return BuildAllEntriesPlan();

        // Estimate cardinalities
        foreach (var clause in clauses)
        {
            clause.Cardinality = indexSearcher.NumberOfDocumentsUnderSpecificTerm(
                indexSearcher.FieldMetadataBuilder(clause.FieldName), clause.TermValue);
        }

        // Sort AND operands by ascending cardinality
        if (!isOr)
            clauses.Sort((a, b) => a.Cardinality.CompareTo(b.Cardinality));

        // Build PlanOp array
        var ops = new List<PlanOp>();
        var entryScanPredicates = new List<MultiUnaryItem[]>();

        if (isOr)
        {
            for (int i = 0; i < clauses.Count; i++)
            {
                ops.Add(new PlanOp
                {
                    Kind = i == 0 ? PlanOpKind.FillFromPostings : PlanOpKind.OrWithPostings,
                    FieldId = i,
                    ParamIndex = i,
                    EstimatedCardinality = clauses[i].Cardinality
                });
            }
            ops.Add(new PlanOp { Kind = PlanOpKind.IterateInto });
        }
        else if (clauses.Count == 1)
        {
            ops.Add(new PlanOp
            {
                Kind = PlanOpKind.DirectIterate,
                ParamIndex = 0,
                EstimatedCardinality = clauses[0].Cardinality
            });
        }
        else
        {
            // AND chain: Fill smallest, then AndWith with goto checks
            ops.Add(new PlanOp
            {
                Kind = PlanOpKind.FillFromPostings,
                ParamIndex = 0,
                EstimatedCardinality = clauses[0].Cardinality
            });

            for (int i = 1; i < clauses.Count; i++)
            {
                ops.Add(new PlanOp
                {
                    Kind = PlanOpKind.CheckAndMaybeEntryScan,
                    ParamIndex = i,
                    GotoLabelIndex = entryScanPredicates.Count
                });

                // TODO: build MultiUnaryItem[] from remaining clauses
                entryScanPredicates.Add(Array.Empty<MultiUnaryItem>());

                ops.Add(new PlanOp
                {
                    Kind = PlanOpKind.AndWithPostings,
                    ParamIndex = i,
                    EstimatedCardinality = clauses[i].Cardinality
                });
            }

            ops.Add(new PlanOp { Kind = PlanOpKind.IterateInto });
        }

        // Pack operand ordering
        int ordering = 0;
        for (int i = 0; i < Math.Min(clauses.Count, 10); i++)
            ordering |= (clauses[i].OriginalIndex & 0x7) << (i * 3);

        return new QueryPlan
        {
            Ops = ops.ToArray(),
            EntryScanPredicates = entryScanPredicates.ToArray(),
            OperandOrdering = ordering,
            OperandCount = clauses.Count,
            // Store clause info for execution-time posting list resolution
            Clauses = clauses.ToArray()
        };
    }

    private static void ParseWhere(
        QueryExpression expression,
        List<ClauseInfo> clauses,
        ref bool isOr,
        BlittableJsonReaderObject queryParameters)
    {
        switch (expression)
        {
            case BinaryExpression be:
                switch (be.Operator)
                {
                    case OperatorType.And:
                        ParseWhere(be.Left, clauses, ref isOr, queryParameters);
                        ParseWhere(be.Right, clauses, ref isOr, queryParameters);
                        return;

                    case OperatorType.Or:
                        isOr = true;
                        ParseWhere(be.Left, clauses, ref isOr, queryParameters);
                        ParseWhere(be.Right, clauses, ref isOr, queryParameters);
                        return;

                    case OperatorType.Equal:
                        if (be.Left is FieldExpression field && be.Right is ValueExpression value)
                        {
                            string termValue = value.GetValue(queryParameters)?.ToString();
                            clauses.Add(new ClauseInfo
                            {
                                FieldName = field.FieldValue,
                                TermValue = termValue,
                                OriginalIndex = clauses.Count
                            });
                        }
                        return;

                    // TODO: handle >=, <=, >, < for range queries
                }
                break;

            case TrueExpression:
                // Constant folding: true in AND → skip, true in OR → return all
                break;

            // TODO: BetweenExpression, InExpression, NegatedExpression,
            // MethodExpression (search, startsWith, etc.)
        }
    }

    private static QueryPlan BuildAllEntriesPlan()
    {
        return new QueryPlan
        {
            Ops = new[] { new PlanOp { Kind = PlanOpKind.IterateInto } },
            EntryScanPredicates = Array.Empty<MultiUnaryItem[]>(),
            OperandOrdering = 0,
            OperandCount = 0,
            Clauses = Array.Empty<ClauseInfo>()
        };
    }

    // Internal clause info — stored in QueryPlan for execution-time resolution
    internal class ClauseInfo
    {
        public string FieldName;
        public string TermValue;
        public long Cardinality = -1;
        public int OriginalIndex;
    }
}
