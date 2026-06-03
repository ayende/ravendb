using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Planning;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

internal static partial class QueryPlanBuilder
{
    /// <summary>
    /// Builds the structural query plan AND overlays the per-op execution timing onto it. This is the
    /// composition used by the live query path (timings requested): <see cref="BuildPlan"/> authors the
    /// timing-independent structure, <see cref="OverlayTimings"/> annotates it with the wall-clock / count
    /// telemetry of the run that just executed.
    /// </summary>
    public static QueryInspectionNode BuildInspectionGraph(CompiledQuery result)
    {
        var plan = BuildPlan(result, out var compiledRoot, out var opNodes);
        if (plan == null)
            return result.ExecutedMatch.Inspect();

        OverlayTimings(result, compiledRoot, opNodes);
        return plan;
    }

    /// <summary>
    /// Authors the structural query plan from the cached <see cref="CompiledPlan.InspectionTemplate"/>, the
    /// decision trail, the live clause values, and the post-filter / sort wrappers. Carries NO timing
    /// (no Ms / Count / EntryScan / ScannedEntries) — those are overlaid separately by <see cref="OverlayTimings"/>,
    /// so the plan is fully formed even when no timing telemetry exists (e.g. the spatial/vector-only path whose
    /// executed match is not a <see cref="CompiledQueryMatch"/>). Returns the outer root (the sort wrapper when
    /// sorting, otherwise the CompiledQuery node), the CompiledQuery node itself via <paramref name="compiledRoot"/>,
    /// and the per-template-op nodes in template order via <paramref name="opNodes"/> so timings can be joined by
    /// op index. Returns null only when there is genuinely no structural template to render.
    /// </summary>
    private static QueryInspectionNode BuildPlan(CompiledQuery result, out QueryInspectionNode compiledRoot, out List<QueryInspectionNode> opNodes)
    {
        compiledRoot = null;
        opNodes = null;

        var template = result.CompiledPlan.InspectionTemplate;
        if (template == null || template.Length == 0)
            return null;

        var exec = result.Execution;
        var flatExecs = BuildFlatClauseExecutions(exec);
        Dictionary<string, string> rootParams = new() 
            {
                // OptimizationHint reflects the strategy that ACTUALLY ran for this execution, not the cached  structural candidacy. 
                // The candidacy (CompiledPlan.Strategy) is decided once at cache-miss time; the bitmap-vs-scan cost gate then re-runs
                // on every execution against the current bound parameters and may fall back to the bitmap pipeline.
                ["OptimizationHint"] = (exec.ActualStrategy != ExecutionStrategy.NotEvaluated
                    ? exec.ActualStrategy
                    : result.CompiledPlan.Strategy).ToString(),
                ["StrategyCandidate"] = result.CompiledPlan.Strategy.ToString()
            };

        var root = new QueryInspectionNode("CompiledQuery", parameters: rootParams);
        compiledRoot = root;
        opNodes = new List<QueryInspectionNode>(template.Length);

        for (int i = 0; i < template.Length; i++)
        {
            var t = template[i];
            var parameters = new Dictionary<string, string>();
            if (t.Dispatch != null) parameters["Dispatch"] = t.Dispatch;
            if (t.FieldName != null) parameters["FieldName"] = t.FieldName;

            // Format values from the current execution's typed arrays (not cached).
            if (t.FlatClauseIndex >= 0 && t.FlatClauseIndex < flatExecs.Count)
            {
                var clauseExec = flatExecs[t.FlatClauseIndex];
                var packed = clauseExec.PackedParamValue;
                int inTermCount = clauseExec.InTermCount;

                var term = FormatValueFromPlan(packed, exec, packed.Param1);
                if (term != null) parameters["Term"] = term;
                var term2 = FormatValueFromPlan(packed, exec, packed.Param2);
                if (term2 != null) parameters["Term2"] = term2;

                if (inTermCount > 0)
                {
                    int displayCount = Math.Min(inTermCount, 5);
                    var displayTerms = new string[displayCount];
                    for (int dt = 0; dt < displayCount; dt++)
                    {
                        PackedParam packed1 = packed.WithTermOffset(dt);
                        displayTerms[dt] = FormatValueFromPlan(packed1, exec, packed1.Param1);
                    }

                    parameters["Terms"] = string.Join(", ", displayTerms) + (inTermCount > 5 ? $" ... ({inTermCount} total)" : "");
                }
            }

            if (t.ClauseType != null) parameters["ClauseType"] = t.ClauseType;
            if (t.IsNegated) parameters["Negated"] = "true";

            if (t.EstimatedCardinality is > 0 and < long.MaxValue)
                parameters["EstimatedRows"] = t.EstimatedCardinality.ToString("N0");

            var node = new QueryInspectionNode(t.Name, parameters: parameters);
            opNodes.Add(node);
            root.Children.Add(node);
        }

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

        // When a tree-scan strategy (FieldSortedScan / CompoundSortedScan) actually executed, the op template rendered
        // above describes only the candidate predicates that fed cost estimation — the bitmap pipeline never
        // ran. Surface the executed scan's OWN structure (driving tree, seek bound, residual predicates, scan
        // counts) so the plan reflects what truly happened instead of hiding it behind the unused bitmap
        // fallback. DirectScanMatchBase.Inspect() carries no vector/spatial children, so AppendPostFilterNodes
        // would drop it — it must be attached explicitly.
        if (result.ExecutedMatch is DirectScanMatchBase directScan)
        {
            root.Children.Add(directScan.Inspect());
        }
        else if (result.ExecutedMatch != null)
        {
            // Vector/spatial post-filter nodes hang off the executed bitmap match.
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

    /// <summary>
    /// Overlays the just-executed run's timing telemetry onto a structural plan produced by <see cref="BuildPlan"/>:
    /// the total scanned-entry count onto the CompiledQuery node, and the per-op wall-clock (Ms), result count, and
    /// entry-scan trigger onto each template-op node by op index. A no-op when the executed match exposed no
    /// telemetry (e.g. spatial/vector-only path), leaving a clean structure-only plan.
    /// </summary>
    private static void OverlayTimings(CompiledQuery result, QueryInspectionNode compiledRoot, List<QueryInspectionNode> opNodes)
    {
        if (result.ExecutedMatch is not CompiledQueryMatch compiled)
            return;

        compiled.GetTelemetry(out var timings, out var resultCounts, out var entryScanAt);

        long scannedEntries = compiled.Count;
        if (scannedEntries >= 0)
            compiledRoot.Parameters["ScannedEntries"] = scannedEntries.ToString();

        double tickFreq = Stopwatch.Frequency / 1000.0;
        for (int i = 0; i < opNodes.Count; i++)
        {
            var parameters = opNodes[i].Parameters;
            if (resultCounts != null && i < resultCounts.Length && resultCounts[i] > 0)
                parameters["Count"] = resultCounts[i].ToString();
            if (timings != null && i < timings.Length && timings[i] > 0)
                parameters["Ms"] = (timings[i] / tickFreq).ToString("F3");
            if (i == entryScanAt)
                parameters["EntryScan"] = "triggered";
        }
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
            if (clauseExec.IsSentinel)
                return; // a collapse sentinel emits no op → no flat entry, keeping FlatClauseIndex aligned with op.ParamIndex
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

    internal static InspectionOp[] BuildInspectionTemplate(PlanOp[] ops, List<ClauseExecution> executions)
    {
        if (ops == null || ops.Length == 0) return [];

        var flatClauses = new List<ClauseInfo>();
        foreach (var clauseExec in executions)
        {
            ExtractFlatClausesInternal(clauseExec);
        }

        var result = new List<InspectionOp>();
        for (int i = 0; i < ops.Length; i++)
        {
            ref PlanOp op = ref ops[i];

            if (op.Kind == PlanOpKind.LazyOrBitmaps)
                continue;
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
                EstimatedCardinality = op.EstimatedCardinality
            };

            // MaybeEntryScan is a control-flow decision (switch to entry-scan vs. stay on the bitmap
            // pipeline based on the running candidate count), not a predicate. Its ParamIndex only marks
            // the leaf cursor position it guards, so attaching FieldName/Term/ClauseType/Negated would
            // misrepresent it as filtering on the clause that happens to sit at that index.
            if (op.Kind != PlanOpKind.MaybeEntryScan && op.ParamIndex >= 0 && op.ParamIndex < flatClauses.Count)
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
            if (clauseExec.IsSentinel)
                return; // a collapse sentinel emits no op → no flat clause, keeping the inspection op cursor aligned
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
