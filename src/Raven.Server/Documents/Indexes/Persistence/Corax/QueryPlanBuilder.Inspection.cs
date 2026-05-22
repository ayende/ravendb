using System;
using System.Collections.Generic;
using System.Diagnostics;
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
    /// <summary>Build the inspection graph from the cached template and runtime telemetry.</summary>
    public static QueryInspectionNode BuildInspectionGraph(BuildCompileAndOptimizeResult result)
    {
        long[] timings = null;
        long[] resultCounts = null;
        int entryScanAt = -1;
        long scannedEntries = -1;

        if (result.ExecutedMatch is CompiledQueryMatch compiled)
        {
            compiled.GetTelemetry(out timings, out resultCounts, out entryScanAt);
            scannedEntries = compiled.Count;
        }

        double tickFreq = Stopwatch.Frequency / 1000.0;
        var template = result.CompiledPlan.InspectionTemplate;
        if (template == null || template.Length == 0)
            return result.ExecutedMatch.Inspect();

        var rootParams = new Dictionary<string, string>();
        if (scannedEntries >= 0)
            rootParams["ScannedEntries"] = scannedEntries.ToString();
        rootParams["OptimizationHint"] = result.CompiledPlan.Strategy.ToString();

        var root = new QueryInspectionNode("CompiledQuery", parameters: rootParams);
        QueryInspectionNode orGroupNode = null;

        for (int i = 0; i < template.Length; i++)
        {
            var t = template[i];
            switch (t.InsideAndGroup, orGroupNode)
            {
                case (true ,null):
                    orGroupNode = new QueryInspectionNode("AND-Group");
                    break;
                case (false, not null):
                    root.Children.Add(orGroupNode);
                    orGroupNode = null;
                    break;
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
            (orGroupNode ?? root).Children.Add(node);
        }

        if (orGroupNode != null) 
            root.Children.Add(orGroupNode);

        if (result.CompiledPlan.DecisionTrail is { Entries.Count: > 0 } trail)
        {
            var trailNode = new QueryInspectionNode("DecisionTrail");
            foreach (var entry in trail.Entries)
            {
                var entryParams = new Dictionary<string, string>
                {
                    ["Accepted"] = entry.Accepted.ToString(),
                    ["Reason"] = entry.Reason
                };
                trailNode.Children.Add(new QueryInspectionNode(entry.Optimization, parameters: entryParams));
            }
            root.Children.Add(trailNode);
        }

        // Vector/spatial nodes from executed match
        var matchInspection = result.ExecutedMatch.Inspect();
        AppendPostFilterNodes(matchInspection, root);

        if (result.SortingWrapper == null) 
            return root;
        
        var sortNode = result.SortingWrapper.Inspect();
        sortNode.Children.Clear();
        sortNode.Children.Add(root);
        return sortNode;

    }

    // Covers VectorSearch and Spatial post-filter ops — both expose Inspect() subtrees
    // appended to the compiled-query root.
    private static void AppendPostFilterNodes(QueryInspectionNode source, QueryInspectionNode target)
    {
        if (source.Operation.Contains("VectorSearch") || source.Operation.Contains("Spatial"))
        {
            target.Children.Add(source);
            return;
        }
        foreach (var child in source.Children)
        {
            AppendPostFilterNodes(child, target);
        }
    }

    /// <summary>Build an inspection template from plan ops + clauses. Created once, cached.</summary>
    private static InspectionOp[] BuildInspectionTemplate(PlanOp[] ops, QueryExecution exec)
    {
        if (ops == null || ops.Length == 0) return [];

        var flatClauses = new List<(ClauseInfo Clause, ClauseExecution Exec)>();
        var execs = exec.Executions;
        if (execs is { Length: > 0 })
        {
            for (int ci = 0; ci < execs.Length; ci++)
            {
                var clause = execs[ci].Clause;
                var clauseExec = execs[ci];
                if (clause.ClauseType is ClauseType.OrGroup or ClauseType.AndGroup && clause.SubClauses != null)
                {
                    for (int si = 0; si < clause.SubClauses.Count; si++)
                    {
                        var subExec = clauseExec?.SubExecutions != null && si < clauseExec.SubExecutions.Length ? clauseExec.SubExecutions[si] : null;
                        flatClauses.Add((clause.SubClauses[si], subExec));
                    }
                }
                else if (clause.ClauseType is ClauseType.In or ClauseType.AllIn && clauseExec is { InTermCount: > 0 })
                {
                    var p = clauseExec.PackedParamValue;
                    for (int t = 0; t < clauseExec.InTermCount; t++)
                    {
                        var termExec = new ClauseExecution(clause)
                        {
                            PackedParamValue = new PackedParam(p.ValueType, p.Param1 + t)
                        };
                        flatClauses.Add((new ClauseInfo
                        {
                            FieldName = clause.FieldName,
                            ClauseType = clause.ClauseType,
                            IsNegated = clause.IsNegated
                        }, termExec));
                    }
                }
                else
                {
                    flatClauses.Add((clause, clauseExec));
                }
            }
        }

        var result = new List<InspectionOp>();
        bool insideAndGroup = false;
        for (int i = 0; i < ops.Length; i++)
        {
            ref PlanOp op = ref ops[i];

            if (op.Kind == PlanOpKind.SwapBitmaps)
            {
                insideAndGroup = true; 
                continue;
            }

            if (op.Kind == PlanOpKind.OrBitmaps)
            {
                insideAndGroup = false; 
                continue;
            }
            if (op.Kind is PlanOpKind.ClearBitmap or PlanOpKind.CheckEmpty or PlanOpKind.RepairAfterLazy or PlanOpKind.IterateInto) 
                continue;

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
                var (clause, clauseExec) = flatClauses[op.ParamIndex];
                if (clause != null)
                {
                    var packed = clauseExec?.PackedParamValue ?? PackedParam.None;
                    int inTermCount = clauseExec?.InTermCount ?? 0;
                    inspOp.FieldName = clause.FieldName;
                    inspOp.Term = FormatValueFromPlan(packed, exec);
                    inspOp.Term2 = FormatValue2FromPlan(packed, exec);
                    inspOp.IsNegated = clause.IsNegated;
                    if (clause.ClauseType != ClauseType.Equals) inspOp.ClauseType = clause.ClauseType.ToString();
                    if (inTermCount > 0)
                    {
                        var p = packed;
                        int displayCount = Math.Min(inTermCount, 5);
                        var displayTerms = new string[displayCount];
                        for (int t = 0; t < displayCount; t++)
                            displayTerms[t] = FormatValueFromPlan(new PackedParam(p.ValueType, p.Param1 + t), exec);
                        inspOp.Terms = string.Join(", ", displayTerms) + (inTermCount > 5 ? $" ... ({inTermCount} total)" : "");
                    }
                }
            }

            result.Add(inspOp);
        }

        return result.ToArray();
    }
}
