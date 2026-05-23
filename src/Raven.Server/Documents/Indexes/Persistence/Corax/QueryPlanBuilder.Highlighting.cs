using System;
using System.Collections.Generic;
using Corax.Querying.Planning;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Queries;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

/// <summary>
/// Highlighting term extraction from the compiled query plan.
/// Populates <see cref="CoraxHighlightingTermIndex"/> dictionaries that
/// the bitmap pipeline hands off to the highlighter after query execution.
/// </summary>
internal static partial class QueryPlanBuilder
{
    /// <summary>
    /// Populate the highlighting terms dictionary from the plan's clauses.
    /// The old CoraxQueryBuilder did this as a side effect during query building.
    /// The bitmap pipeline must do it explicitly after plan building.
    /// </summary>
    private static void PopulateHighlightingTerms(QueryExecution exec, Dictionary<string, CoraxHighlightingTermIndex> highlightingTerms, QueryMetadata metadata)
    {
        var execs = exec.Executions;
        if (highlightingTerms == null || execs is not { Count: > 0 })
            return;

        for (int ci = 0; ci < execs.Count; ci++)
        {
            var clauseObj = execs[ci].Clause;
            var clauseExec2 = execs[ci];

            // Recurse into sub-clauses before checking FieldName: OrGroup/AndGroup have
            // FieldName==null (they are structural wrappers, not field clauses), so a
            // FieldName-first guard would skip their children entirely.
            switch (clauseObj?.ClauseType)
            {
                case ClauseType.OrGroup or ClauseType.AndGroup when clauseObj.SubClauses is { Count: > 0 }:
                {
                    for (int si = 0; si < clauseObj.SubClauses.Count; si++)
                    {
                        var subExec = clauseExec2?.SubExecutions != null && si < clauseExec2.SubExecutions.Count ? clauseExec2.SubExecutions[si] : null;
                        PopulateHighlightingForClause(clauseObj.SubClauses[si], subExec, highlightingTerms, metadata, exec);
                    }

                    break;
                }
            }

            if (clauseObj?.FieldName == null)
                continue;

            PopulateHighlightingForClause(clauseObj, clauseExec2, highlightingTerms, metadata, exec);
        }
    }

    private static void PopulateHighlightingForClause(ClauseInfo clause, ClauseExecution exec, Dictionary<string, CoraxHighlightingTermIndex> highlightingTerms, QueryMetadata metadata, QueryExecution queryExec)
    {
        string fieldName = clause.FieldName;
        if (fieldName == null)
            return;

        // Skip highlighting for null-valued clauses (e.g. WHERE City == null).
        // Null is not a search term — there's nothing to highlight. Without this
        // guard, the highlighter produces a spurious result for null fields (#4781).
        if (clause.ClauseType is ClauseType.Equals or ClauseType.NotEquals)
        {
            var packed = exec?.PackedParamValue ?? PackedParam.None;
            if (packed.IsNone || (packed is { ValueType: PackedParam.TypeString, Param1: >= 0 } && queryExec.StringValues != null
                                                                                                && packed.Param1 < queryExec.StringValues.Length
                                                                                                && queryExec.StringValues[packed.Param1] == null))
            {
                return;
            }
        }

        if (highlightingTerms.TryGetValue(fieldName, out var existingTerm))
        {
            existingTerm.Values ??= GetHighlightingValues(clause, exec, queryExec);
            return;
        }

        var term = new CoraxHighlightingTermIndex
        {
            FieldName = fieldName,
            Values = GetHighlightingValues(clause, exec, queryExec)
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

    private static object GetHighlightingValues(ClauseInfo clause, ClauseExecution exec, QueryExecution queryExec)
    {
        var packed = exec?.PackedParamValue ?? PackedParam.None;
        if (clause.ClauseType == ClauseType.Between)
        {
            return new Tuple<string, string>(
                FormatValueFromPlan(packed, queryExec),
                FormatValue2FromPlan(packed, queryExec));
        }

        int inTermCount = exec?.InTermCount ?? 0;
        bool hasNullTerm = exec?.HasNullTerm ?? false;
        if (clause.ClauseType is ClauseType.In or ClauseType.AllIn && (inTermCount > 0 || hasNullTerm))
        {
            var p = packed;
            var terms = new List<string>(inTermCount + (hasNullTerm ? 1 : 0));
            for (int t = 0; t < inTermCount; t++)
                terms.Add(FormatValueFromPlan(new PackedParam(p.ValueType, p.Param1 + t), queryExec));
            if (hasNullTerm)
                terms.Add(null);
            return terms;
        }

        return FormatValueFromPlan(packed, queryExec);
    }
}
