using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Planning;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

internal static partial class QueryPlanBuilder
{
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

        var exec = result.Execution;
        var flatExecs = BuildFlatClauseExecutions(exec);
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

            // Format values from the current execution's typed arrays (not cached).
            if (t.FlatClauseIndex >= 0 && t.FlatClauseIndex < flatExecs.Count)
            {
                var clauseExec = flatExecs[t.FlatClauseIndex];
                var packed = clauseExec.PackedParamValue;
                int inTermCount = clauseExec.InTermCount;

                var term = FormatValueFromPlan(packed, exec);
                if (term != null) parameters["Term"] = term;
                var term2 = FormatValue2FromPlan(packed, exec);
                if (term2 != null) parameters["Term2"] = term2;

                if (inTermCount > 0)
                {
                    int displayCount = Math.Min(inTermCount, 5);
                    var displayTerms = new string[displayCount];
                    for (int dt = 0; dt < displayCount; dt++)
                        displayTerms[dt] = FormatValueFromPlan(packed.WithTermOffset(dt), exec);
                    parameters["Terms"] = string.Join(", ", displayTerms) + (inTermCount > 5 ? $" ... ({inTermCount} total)" : "");
                }
            }

            if (t.ClauseType != null) parameters["ClauseType"] = t.ClauseType;
            if (t.IsNegated) parameters["Negated"] = "true";

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
        if (result.ExecutedMatch != null)
        {
            var matchInspection = result.ExecutedMatch.Inspect();
            AppendPostFilterNodes(matchInspection, root);
        }

        if (result.SortingWrapper == null) 
            return root;
        
        var sortNode = result.SortingWrapper.Inspect();
        sortNode.Children.Clear();
        sortNode.Children.Add(root);
        return sortNode;

    }

    private static List<ClauseExecution> BuildFlatClauseExecutions(QueryExecution exec)
    {
        var flat = new List<ClauseExecution>();
        foreach (var clauseExecution in exec.Executions)
        {
            BuildFlatClauseExecutionsInternal(flat, clauseExecution);
        }
        return flat;

        static void BuildFlatClauseExecutionsInternal(List<ClauseExecution> list, ClauseExecution clauseExec)
        {
            RuntimeHelpers.EnsureSufficientExecutionStack();
            switch (clauseExec.Clause.ClauseType)
            {
                case ClauseType.OrGroup or ClauseType.AndGroup:
                {
                    foreach (var cur in clauseExec.SubExecutions)
                        BuildFlatClauseExecutionsInternal(list, cur);
                    break;
                }
                case ClauseType.In or ClauseType.AllIn when clauseExec.InTermCount > 0:
                {
                    for (int t = 0; t < clauseExec.InTermCount; t++)
                    {
                        list.Add(new ClauseExecution(clauseExec.Clause)
                        {
                            PackedParamValue = clauseExec.PackedParamValue.WithTermOffset(t)
                        });
                    }
                    break;
                }
                default:
                    list.Add(clauseExec);
                    break;
            }
        }
    }

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

    private static InspectionOp[] BuildInspectionTemplate(PlanOp[] ops, List<ClauseExecution> executions)
    {
        if (ops == null || ops.Length == 0) return [];

        var flatClauses = new List<ClauseInfo>();
        foreach (var clauseExec in executions)
        {
            ExtractFlatClausesInternal(clauseExec);
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

            if (op.Kind == PlanOpKind.LazyOrBitmaps)
            {
                insideAndGroup = false;
                continue;
            }
            if (op.Kind is PlanOpKind.ClearBitmap or PlanOpKind.GotoDoneIfEmpty or PlanOpKind.GotoDone)
                continue;

            var inspOp = new InspectionOp
            {
                Name = op.Kind switch
                {
                    PlanOpKind.FillFromPostingSource or PlanOpKind.FillFromTreeScan or PlanOpKind.FillFromMatch => "Fill",
                    PlanOpKind.AndFromPostingSource or PlanOpKind.AndFromTreeScan or PlanOpKind.AndFromMatch => "AND",
                    PlanOpKind.OrFromPostingSource or PlanOpKind.OrFromTreeScan or PlanOpKind.OrFromMatch => "OR",
                    PlanOpKind.AndNotFromPostingSource or PlanOpKind.AndNotFromTreeScan or PlanOpKind.AndNotFromMatch => "ANDNOT",
                    PlanOpKind.AndBitmaps => "AND-Bitmaps",
                    PlanOpKind.AndNotBitmaps => "ANDNOT-Bitmaps",
                    PlanOpKind.MaybeEntryScan => "EntryScanCheck",
                    PlanOpKind.OrRangeFromPostingSource or PlanOpKind.OrRangeFromMatch => $"OR-Range({op.ParamIndex2} terms)",
                    PlanOpKind.AndRangeFromPostingSource or PlanOpKind.AndRangeFromMatch => $"AND-Range({op.ParamIndex2} terms)",
                    _ => op.Kind.ToString()
                },
                Dispatch = op.Kind switch
                {
                    PlanOpKind.FillFromPostingSource or PlanOpKind.AndFromPostingSource or PlanOpKind.OrFromPostingSource
                        or PlanOpKind.AndNotFromPostingSource or PlanOpKind.OrRangeFromPostingSource or PlanOpKind.AndRangeFromPostingSource => "Term",
                    PlanOpKind.FillFromTreeScan or PlanOpKind.AndFromTreeScan or PlanOpKind.OrFromTreeScan
                        or PlanOpKind.AndNotFromTreeScan => "MultiTerm",
                    _ => "Match"
                },
                EstimatedCardinality = op.EstimatedCardinality,
                InsideAndGroup = insideAndGroup
            };

            if (op.ParamIndex >= 0 && op.ParamIndex < flatClauses.Count)
            {
                inspOp.FlatClauseIndex = op.ParamIndex;
                var clause = flatClauses[op.ParamIndex];
                inspOp.FieldName = clause.FieldName;
                inspOp.IsNegated = clause.IsNegated;
                if (clause.ClauseType != ClauseType.Equals) inspOp.ClauseType = clause.ClauseType.ToString();
            }

            result.Add(inspOp);
        }

        return result.ToArray();

        void ExtractFlatClausesInternal(ClauseExecution clauseExec)
        {
            RuntimeHelpers.EnsureSufficientExecutionStack();
            switch (clauseExec.Clause.ClauseType)
            {
                case ClauseType.OrGroup or ClauseType.AndGroup:
                {
                    foreach (ClauseExecution v in clauseExec.SubExecutions)
                        ExtractFlatClausesInternal(v);
                    break;
                }
                case ClauseType.In or ClauseType.AllIn when clauseExec.InTermCount > 0:
                {
                    for (int t = 0; t < clauseExec.InTermCount; t++)
                    {
                        flatClauses.Add(new ClauseInfo
                        {
                            FieldName = clauseExec.Clause.FieldName,
                            ClauseType = clauseExec.Clause.ClauseType,
                            IsNegated = clauseExec.Clause.IsNegated
                        });
                    }

                    break;
                }
                default:
                    flatClauses.Add(clauseExec.Clause);
                    break;
            }
        }
    }
}
