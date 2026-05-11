using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Planning;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

/// <summary>
/// Diagnostic / inspection methods for the compiled query plan.
/// Builds the Studio-facing QueryInspectionNode tree from the cached
/// InspectionTemplate plus runtime telemetry (timings, row counts,
/// entry-scan triggers).
/// </summary>
internal static partial class QueryPlanBuilder
{
    /// <summary>Build the inspection graph from the cached template + runtime telemetry.</summary>
    public static QueryInspectionNode BuildInspectionGraph(CompiledPlan compiledPlan, IQueryMatch executedMatch, IQueryMatch sortingWrapper = null)
    {
        long[] timings = null;
        long[] resultCounts = null;
        int entryScanAt = -1;
        long scannedEntries = -1;

        if (executedMatch is CompiledQueryMatch compiled)
        {
            compiled.GetTelemetry(out timings, out resultCounts, out entryScanAt);
            scannedEntries = compiled.Count;
        }

        double tickFreq = Stopwatch.Frequency / 1000.0;
        var template = compiledPlan.InspectionTemplate;
        if (template == null || template.Length == 0)
            return executedMatch.Inspect();

        var rootParams = new Dictionary<string, string>();
        if (scannedEntries >= 0)
            rootParams["ScannedEntries"] = scannedEntries.ToString();
        if (compiledPlan.ExplainSource != null)
            rootParams["Explain"] = compiledPlan.ExplainSource;

        var root = new QueryInspectionNode("CompiledQuery", parameters: rootParams);
        QueryInspectionNode orGroupNode = null;

        for (int i = 0; i < template.Length; i++)
        {
            var t = template[i];

            // For queries like: WHERE (A AND B) OR (C AND D)
            // the plan ops are:
            //   ClearBitmap[2]           <- prepare scratch
            //   SwapBitmaps[0,2]         <- save main, work in scratch  -> InsideAndGroup = true
            //   FillFromPostings(A)      <- first AND-group
            //   AndWithPostings(B)       <-
            //   OrBitmaps[0,2]           <- merge scratch into main     -> InsideAndGroup = false
            //   ClearBitmap[2]
            //   SwapBitmaps[0,2]         <- second AND-group            -> InsideAndGroup = true
            //   FillFromPostings(C)      <-
            //   AndWithPostings(D)       <-
            //   OrBitmaps[0,2]           <- merge                      -> InsideAndGroup = false
            //
            // The inspection tree groups these into:
            //   CompiledQuery
            //     AND-Group
            //       Fill(A)
            //       AND(B)
            //     AND-Group
            //       Fill(C)
            //       AND(D)
            if (t.InsideAndGroup && orGroupNode == null)
                orGroupNode = new QueryInspectionNode("AND-Group");
            else if (!t.InsideAndGroup && orGroupNode != null)
            {
                root.Children.Add(orGroupNode);
                orGroupNode = null;
            }

            var parameters = new Dictionary<string, string>();
            if (t.Dispatch != null) parameters["Dispatch"] = t.Dispatch;
            if (t.FieldName != null) parameters["FieldName"] = t.FieldName;
            if (t.Term != null) parameters["Term"] = t.Term;
            if (t.Term2 != null) parameters["Term2"] = t.Term2;
            if (t.ClauseType != null) parameters["ClauseType"] = t.ClauseType;
            if (t.IsNegated) parameters["Negated"] = "true";
            if (t.Terms != null) parameters["Terms"] = t.Terms;
            if (t.EstimatedCardinality is > 0 and < long.MaxValue)
                parameters["EstimatedRows"] = t.EstimatedCardinality.ToString("N0");

            if (resultCounts != null && i < resultCounts.Length && resultCounts[i] > 0)
                parameters["Count"] = resultCounts[i].ToString();
            if (timings != null && i < timings.Length && timings[i] > 0)
                parameters["Ms"] = (timings[i] / tickFreq).ToString("F3");
            if (i == entryScanAt)
                parameters["EntryScan"] = "triggered";

            var node = new QueryInspectionNode(t.Name, parameters: parameters);
            if (orGroupNode != null) orGroupNode.Children.Add(node);
            else root.Children.Add(node);
        }

        if (orGroupNode != null) root.Children.Add(orGroupNode);

        // Vector/spatial nodes from executed match
        var matchInspection = executedMatch.Inspect();
        AppendVectorNodes(matchInspection, root);

        if (sortingWrapper != null)
        {
            var sortNode = sortingWrapper.Inspect();
            sortNode.Children.Clear();
            sortNode.Children.Add(root);
            return sortNode;
        }

        return root;
    }

    private static void AppendVectorNodes(QueryInspectionNode source, QueryInspectionNode target)
    {
        if (source.Operation.Contains("VectorSearch") || source.Operation.Contains("Spatial"))
        {
            target.Children.Add(source);
            return;
        }
        foreach (var child in source.Children)
            AppendVectorNodes(child, target);
    }

    /// <summary>Build an inspection template from plan ops + clauses. Created once, cached.</summary>
    private static InspectionOp[] BuildInspectionTemplate(QueryExecution plan)
    {
        var ops = plan.Ops;
        if (ops == null || ops.Length == 0) return [];

        var flatClauses = new List<(ClauseInfo Clause, ClauseExecution Exec)>();
        if (plan.Clauses is { Count: > 0 } clauses)
        {
            var execs = plan.Executions;
            for (int ci = 0; ci < clauses.Count; ci++)
            {
                var clause = clauses[ci];
                var exec = execs != null && ci < execs.Length ? execs[ci] : null;
                if (clause.ClauseType == ClauseType.OrGroup && clause.OrSubClauses != null)
                {
                    for (int si = 0; si < clause.OrSubClauses.Count; si++)
                    {
                        var subExec = exec?.OrSubExecutions != null && si < exec.OrSubExecutions.Length ? exec.OrSubExecutions[si] : null;
                        flatClauses.Add((clause.OrSubClauses[si], subExec));
                    }
                }
                else if (clause.ClauseType == ClauseType.AndGroup && clause.AndSubClauses != null)
                {
                    for (int si = 0; si < clause.AndSubClauses.Count; si++)
                    {
                        var subExec = exec?.AndSubExecutions != null && si < exec.AndSubExecutions.Length ? exec.AndSubExecutions[si] : null;
                        flatClauses.Add((clause.AndSubClauses[si], subExec));
                    }
                }
                else if (clause.ClauseType is ClauseType.In or ClauseType.AllIn && exec != null && exec.InTermCount > 0)
                {
                    var p = exec.PackedParamValue;
                    for (int t = 0; t < exec.InTermCount; t++)
                    {
                        var termExec = new ClauseExecution { PackedParamValue = new PackedParam(p.ValueType, p.Param1 + t) };
                        flatClauses.Add((new ClauseInfo
                        {
                            FieldName = clause.FieldName,
                            ClauseType = clause.ClauseType,
                            IsNegated = clause.IsNegated
                        }, termExec));
                    }
                }
                else
                    flatClauses.Add((clause, exec));
            }
        }

        var result = new List<InspectionOp>();
        bool insideAndGroup = false;
        for (int i = 0; i < ops.Length; i++)
        {
            ref PlanOp op = ref ops[i];
            // OR chains like: (A AND B) OR (C AND D)
            // are compiled as:
            //   SwapBitmaps       <- save bitmap[0], start fresh in bitmap[2]
            //   Fill(A)           <- these ops form an AND-group
            //   And(B)            <-
            //   OrBitmaps         <- merge bitmap[2] into bitmap[0]
            //   SwapBitmaps       <- start next AND-group
            //   Fill(C)           <-
            //   And(D)            <-
            //   OrBitmaps         <- merge
            // SwapBitmaps marks the start of an AND-group within the OR chain.
            // OrBitmaps marks the end — it merges the group result back.
            if (op.Kind == PlanOpKind.SwapBitmaps) { insideAndGroup = true; continue; }
            if (op.Kind == PlanOpKind.OrBitmaps) { insideAndGroup = false; continue; }
            if (op.Kind is PlanOpKind.ClearBitmap or PlanOpKind.CheckEmpty or PlanOpKind.RepairAfterLazy or PlanOpKind.IterateInto) continue;

            var inspOp = new InspectionOp
            {
                Name = op.Kind switch
                {
                    PlanOpKind.FillFromPostings or PlanOpKind.DirectIterate => "Fill",
                    PlanOpKind.AndWithPostings => "AND",
                    PlanOpKind.OrWithPostings or PlanOpKind.LazyOrWithPostings => "OR",
                    PlanOpKind.AndNotWithPostings => "ANDNOT",
                    PlanOpKind.AndBitmaps => "AND-Bitmaps",
                    PlanOpKind.AndNotBitmaps => "ANDNOT-Bitmaps",
                    PlanOpKind.CheckAndMaybeEntryScan => "EntryScanCheck",
                    PlanOpKind.OrRange => $"OR-Range({op.ParamIndex2} terms)",
                    PlanOpKind.AndRange => $"AND-Range({op.ParamIndex2} terms)",
                    _ => op.Kind.ToString()
                },
                Dispatch = op.Dispatch switch { MatchDispatch.PostingList => "Term", MatchDispatch.TreeScan => "MultiTerm", _ => "Match" },
                EstimatedCardinality = op.EstimatedCardinality,
                InsideAndGroup = insideAndGroup
            };

            if (op.ParamIndex >= 0 && op.ParamIndex < flatClauses.Count)
            {
                var (clause, exec) = flatClauses[op.ParamIndex];
                if (clause != null)
                {
                    var packed = exec?.PackedParamValue ?? PackedParam.None;
                    int inTermCount = exec?.InTermCount ?? 0;
                    inspOp.FieldName = clause.FieldName;
                    inspOp.Term = FormatValueFromPlan(packed, plan);
                    inspOp.Term2 = FormatValue2FromPlan(packed, plan);
                    inspOp.IsNegated = clause.IsNegated;
                    if (clause.ClauseType != ClauseType.Equals) inspOp.ClauseType = clause.ClauseType.ToString();
                    if (inTermCount > 0)
                    {
                        var p = packed;
                        int displayCount = Math.Min(inTermCount, 5);
                        var displayTerms = new string[displayCount];
                        for (int t = 0; t < displayCount; t++)
                            displayTerms[t] = FormatValueFromPlan(new PackedParam(p.ValueType, p.Param1 + t), plan);
                        inspOp.Terms = string.Join(", ", displayTerms) + (inTermCount > 5 ? $" ... ({inTermCount} total)" : "");
                    }
                }
            }

            result.Add(inspOp);
        }

        return result.ToArray();
    }
}
