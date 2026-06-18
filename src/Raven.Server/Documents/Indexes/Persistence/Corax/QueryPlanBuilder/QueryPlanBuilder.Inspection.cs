using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Runtime.CompilerServices;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Planning;
using Raven.Server.Documents.Queries;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

internal static partial class QueryPlanBuilder
{
    /// <summary>Build the structural plan (<see cref="BuildPlan"/>) and overlay this run's
    /// execution timing onto it (<see cref="OverlayTimings"/>).</summary>
    public static QueryInspectionNode BuildInspectionGraph(CompiledQuery result)
    {
        var plan = BuildPlan(result, out var compiledRoot, out var opNodes, out var entryScanNode);
        if (plan == null)
        {
            var bypass = result.ExecutedMatch.Inspect();
            if (result.SortingWrapper?.Inspect() is {} bypassSort)
            {
                // Mirror the BuildPlan wrapping so the sort strategy renders as the dataflow tail here too.
                bypassSort.Children.Clear();
                bypassSort.Children.Add(bypass);
                bypass = bypassSort;
            }
            plan = bypass;
        }
        else
        {
            OverlayTimings(result, compiledRoot, opNodes, entryScanNode);
        }
        plan.Parameters["PlanGraphDot"] = QueryPlanGraph.ToGraphviz(plan);
        return plan;
    }

    /// <summary>Author the structural plan from the cached <see cref="CompiledPlan.InspectionTemplate"/>,
    /// decision trail, live clause values, and post-filter/sort wrappers — no timing (overlaid separately by
    /// <see cref="OverlayTimings"/>, so the plan is fully formed even on the spatial/vector-only path). Returns
    /// the outer root (sort wrapper when sorting, else the CompiledQuery node), plus the CompiledQuery node and
    /// per-template-op nodes (joined to timings by op index) via out params. Null when no template to render.</summary>
    private static QueryInspectionNode BuildPlan(CompiledQuery result, out QueryInspectionNode compiledRoot, out List<QueryInspectionNode> opNodes, out QueryInspectionNode entryScanNode)
    {
        compiledRoot = null;
        opNodes = null;
        entryScanNode = null;

        var template = result.Execution.Plan.InspectionTemplate;
        if (template == null || template.Length == 0)
            return null;

        var exec = result.Execution;
        var compiledPlan = result.Execution.Plan;
        var flatExecs = BuildFlatClauseExecutions(exec);
        Dictionary<string, string> rootParams = new()
            {
                // OptimizationHint reflects the strategy that ACTUALLY ran for this execution, not the cached  structural candidacy.
                // The candidacy (CompiledPlan.Strategy) is decided once at cache-miss time; the bitmap-vs-scan cost gate then re-runs
                // on every execution against the current bound parameters and may fall back to the bitmap pipeline.
                ["OptimizationHint"] = (exec.ActualStrategy != ExecutionStrategy.NotEvaluated
                    ? exec.ActualStrategy
                    : compiledPlan.Strategy).ToString(),
                ["StrategyCandidate"] = compiledPlan.Strategy.ToString()
            };

        rootParams["CSharpSourceFormatted"] = compiledPlan.FormattedSource;

        if (compiledPlan.AllNegated)
            rootParams["AllNegated"] = "true";

        var root = new QueryInspectionNode("CompiledQuery", parameters: rootParams);
        compiledRoot = root;
        opNodes = new List<QueryInspectionNode>(template.Length);
        bool hasEntryScanGate = false;

        // When a scan/lookup strategy actually ran, the bitmap op-template below is the UNUSED candidate that only fed cost estimation — the pipeline never executed it. Skip emitting it 
        ExecutionStrategy actualStrategy = exec.ActualStrategy != ExecutionStrategy.NotEvaluated ? exec.ActualStrategy : compiledPlan.Strategy;
        bool scanOrLookupRan = actualStrategy is ExecutionStrategy.CompoundKeyLookup or ExecutionStrategy.CompoundSortedScan or ExecutionStrategy.FieldSortedScan;

        for (int i = 0; scanOrLookupRan == false && i < template.Length; i++)
        {
            var t = template[i];
            var parameters = new Dictionary<string, string>();

            // Physical destination/source slots: the dataflow backbone. A consumer reads DestSlot/SourceSlot to
            // reconstruct which bitmap each op writes (and, for the slot-to-slot merges, which it reads),
            // and can build a flow graph from it. Always emitted for a real op (DestSlot >= 0).
            if (t.DestSlot >= 0) parameters["DestSlot"] = t.DestSlot.ToString();
            if (t.SourceSlot >= 0) parameters["SourceSlot"] = t.SourceSlot.ToString();

            if (t.Dispatch != null) parameters["Dispatch"] = t.Dispatch;
            if (t.FieldName != null) parameters["FieldName"] = t.FieldName;

            // Format values from the current execution's typed arrays (not cached).
            if (t.FlatClauseIndex >= 0 && t.FlatClauseIndex < flatExecs.Count)
            {
                var clauseExec = flatExecs[t.FlatClauseIndex];
                var packed = clauseExec.PackedParamValue;
                int inTermCount = clauseExec.InTermCount;

                // Surface a boosted leaf: this clause's postings are wrapped in a BoostingMatch (IndexSearcher.Boost)
                // that scales their score contribution. The factor feeds the score-ordered ranking (boosting
                // auto-promotes to ORDER BY score()), so showing it on the leaf explains where the ranking weight came from.
                if (clauseExec.Clause.HasBoost || clauseExec.BoostFactor > 0)
                    parameters["Boost"] = clauseExec.BoostFactor.ToString(CultureInfo.InvariantCulture);

                var term = FormatValueFromPlan(packed, exec, packed.Param1);
                if (term != null) parameters["Term"] = term;
                var term2 = FormatValueFromPlan(packed, exec, packed.Param2);
                if (term2 != null) parameters["Term2"] = term2;

                // search(field, "a b c") is one bitmap-pipeline leaf but executes as a match over multiple
                // analyzer-tokenized terms OR/AND-combined (a quoted group is one phrase term) — not the literal
                // string in Term. Surface the tokenized terms + operator. SplitSearchValue mirrors what HandleSearch
                // feeds SearchQuery (split on whitespace, quoted groups kept as one phrase).
                if (clauseExec.Clause.ClauseType == ClauseType.Search && term != null)
                {
                    var searchTerms = QueryBuilderHelper.SplitSearchValue(term).ToList();
                    if (searchTerms.Count > 0)
                    {
                        parameters["SearchTerms"] = string.Join(", ", searchTerms);
                        parameters["SearchTermCount"] = searchTerms.Count.ToString(CultureInfo.InvariantCulture);
                    }
                    parameters["SearchOperator"] = ((global::Corax.Constants.Search.Operator)clauseExec.Clause.SearchOperator).ToString();
                }

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

                // Per-execution estimate (live Cardinality for THIS run's bound parameters), not the cached
                // plan-emit-time value (stale across parameter sets). For range/StartsWith clauses also surface the
                // raw estimator inputs so the reader can see HOW the number was reached. See
                // IndexSearcher.EstimateMatchesInRange / RangeEstimateBreakdown.
                if (clauseExec.Cardinality is > 0 and < long.MaxValue)
                    parameters["EstimatedRows"] = clauseExec.Cardinality.ToString("N0");

                if (clauseExec.RangeEstimate is { } bd)
                {
                    parameters["EstRangeTerms"] = bd.RangeTerms.ToString("N0", CultureInfo.InvariantCulture);
                    parameters["EstSampledTerms"] = bd.SampledTerms.ToString("N0", CultureInfo.InvariantCulture);
                    parameters["EstSampledPostings"] = bd.SampledPostings.ToString("N0", CultureInfo.InvariantCulture);
                    if (bd.IsExact)
                    {
                        parameters["EstExact"] = "true"; // small range: every in-range term was counted, no extrapolation
                    }
                    else
                    {
                        parameters["EstMiddleTerms"] = bd.MiddleTerms.ToString("N0", CultureInfo.InvariantCulture);
                        parameters["EstSampledAvg"] = bd.SampledAvg.ToString("0.###", CultureInfo.InvariantCulture);
                        parameters["EstGlobalAvg"] = bd.GlobalAvg.ToString("0.###", CultureInfo.InvariantCulture);
                        parameters["EstMiddleAvg"] = bd.MiddleAvg.ToString("0.###", CultureInfo.InvariantCulture);
                        parameters["EstBeta"] = bd.Beta.ToString("0.###", CultureInfo.InvariantCulture);
                        parameters["EstCalibrationFactor"] = bd.CalibrationFactor.ToString("0.###", CultureInfo.InvariantCulture);
                    }
                }
            }

            if (t.ClauseType != null) parameters["ClauseType"] = t.ClauseType;
            if (t.IsNegated) parameters["Negated"] = "true";

            var node = new QueryInspectionNode(t.Name, parameters: parameters);
            opNodes.Add(node);
            root.Children.Add(node);

            if (t.IsEntryScanGate)
                hasEntryScanGate = true;
        }

        // Entry-scan tail: a SINGLE node for the shared scan body every EntryScanCheck gate branches to. The gates
        // are read-only cost checks on the slot-0 accumulator; the slot 0 -> slot 1 move and per-entry residual
        // evaluation happen HERE, once, regardless of which gate fired. One tail (vs. hanging the body off a
        // specific check) is the accurate physical picture and avoids mis-attributing "Taken" to a gate that did
        // not fire. Residual children are what the scan evaluates per surviving entry. Rendered whenever the plan
        // has an entry-scan path, independent of whether THIS run took it.
        if (hasEntryScanGate)
        {
            var entryScanParams = new Dictionary<string, string> { ["DestSlot"] = "1", ["SourceSlot"] = "0" };
            entryScanNode = new QueryInspectionNode("EntryScan", parameters: entryScanParams);
            if (compiledPlan.EntryScanSet is { HasPredicates: true } entryScan)
            {
                foreach (var predicate in entryScan.Predicates)
                    entryScanNode.Children.Add(BuildScanPredicateNode(predicate));
            }
            root.Children.Add(entryScanNode);
        }

        if (result.Execution.Plan.DecisionTrail is { Entries.Count: > 0 } trail)
        {
            var trailNode = new QueryInspectionNode("DecisionTrail");
            string candidateName = compiledPlan.Strategy.ToString();
            foreach (var entry in trail.Entries)
            {
                var entryParams = new Dictionary<string, string>
                {
                    ["Accepted"] = entry.Accepted.ToString(),
                    ["Reason"] = entry.Reason
                };
                if (exec.StrategyGateReason != null && entry.Optimization == candidateName)
                    entryParams["PerExecution"] = exec.StrategyGateReason;
                trailNode.Children.Add(new QueryInspectionNode(entry.Optimization, parameters: entryParams));
            }
            root.Children.Add(trailNode);
        }

        // Surface clauses the resolution pass statically collapsed. A sentinel emits no match leaf (it resolves
        // to a bitmap fill/clear, simplified out of the op stream), so these clauses are otherwise invisible in
        // the executed tree. Listing them closes the gap between what the query TEXT asked for and what ran (e.g.
        // a WHEN(false) clause or a contradictory BETWEEN answered without scanning).
        AppendResolvedClauses(exec, root);

        // When a tree-scan strategy (FieldSortedScan / CompoundSortedScan) ran, the op template above describes
        // only the candidate predicates that fed cost estimation — the bitmap pipeline never ran. Surface the
        // executed scan's OWN structure (driving tree, seek bound, residual predicates, scan counts).
        // DirectScanMatchBase.Inspect() carries no vector/spatial children, so AppendPostFilterNodes would drop
        // it — attach explicitly.
        if (result.ExecutedMatch is DirectScanMatchBase directScan)
        {
            var directScanNode = directScan.Inspect();

            // Surface the per-entry residual filter as structured Residual children (mirroring the EntryScan tail).
            var residualSet = result.Execution.Plan.Strategy == ExecutionStrategy.CompoundSortedScan ? result.Execution.Plan.CompoundFieldResidualSet : result.Execution.Plan.DirectScanResidualSet;
            if (residualSet is { HasPredicates: true })
            {
                foreach (var predicate in residualSet.Predicates)
                    directScanNode.Children.Add(BuildScanPredicateNode(predicate));
            }

            root.Children.Add(directScanNode);
        }
        else if (actualStrategy == ExecutionStrategy.CompoundKeyLookup)
        {
            // The two Equals clauses were folded into one composite-key TermQuery on the synthetic compound field, Surface that single lookup as the producer node 
            root.Children.Add(BuildCompoundKeyLookupNode(result, exec, compiledPlan));
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
        if (compiledPlan.Template.SortMetadataTemplate is { ImplicitScore: true })
            sortNode.Parameters["ImplicitScore"] = "auto-promoted from boosting / vector search (no explicit ORDER BY)";

        sortNode.Children.Clear();
        sortNode.Children.Add(root);
        return sortNode;
    }

    /// <summary>Overlay this run's timing telemetry (scanned-entry count, plus per-op wall-clock, result
    /// count, and entry-scan trigger joined by op index) onto a structural plan from <see cref="BuildPlan"/>.
    /// A no-op when the executed match exposed no telemetry (e.g. spatial/vector-only path).</summary>
    private static void OverlayTimings(CompiledQuery result, QueryInspectionNode compiledRoot, List<QueryInspectionNode> opNodes, QueryInspectionNode entryScanNode)
    {
        if (result.ExecutedMatch is not CompiledQueryMatch compiled)
            return;

        compiled.GetTelemetry(out var timings, out var resultCounts, out var entryScanAt);

        // The bitmap pipeline's resolved output: result-bitmap cardinality after the compiled delegate ran. Full
        // count for a plain query; the truncated count for a limit/early-exit query — hence "Output" (what the
        // pipeline produced), not "TotalResults" (which it is NOT under a limit).
        long pipelineOutput = compiled.Count;
        if (pipelineOutput >= 0)
            compiledRoot.Parameters["Output"] = pipelineOutput.ToString("N0");

        // Limit push-down: with a page limit (Limit != int.MaxValue), slot 0 grows only until full, then stops —
        // it does NOT scan the rest. Mark EarlyExit only when output actually reached the limit (output < limit
        // means matches ran out first, nothing skipped). Mirrors CompiledQueryMatch.Confidence (Low == capped).
        if (compiled.Limit != int.MaxValue)
        {
            compiledRoot.Parameters["Limit"] = compiled.Limit.ToString("N0");
            if (pipelineOutput >= compiled.Limit)
                compiledRoot.Parameters["EarlyExit"] = "true";
        }

        // The template is compacted (control-flow ops dropped), but per-op telemetry is recorded against the FULL
        // PlanOp[] index — so join each template node to its timing slot via the original OpIndex, not the node's
        // position in the compacted list (which drifts the moment any op is filtered out).
        var template = result.Execution.Plan.InspectionTemplate;
        double tickFreq = Stopwatch.Frequency / 1000.0;
        for (int i = 0; i < opNodes.Count; i++)
        {
            int opIndex = i < template.Length ? template[i].OpIndex : i;
            var parameters = opNodes[i].Parameters;
            // Per-op count is ComputeCount() on the slot's bitmap right after this op ran, while it may still hold
            // ArrayUnsorted containers whose Cardinality is the RAW appended-entry count (duplicates not yet
            // collapsed; only PrepareForReading sorts+dedups). So this is an upper-bound "OutputWithDups", distinct
            // from the top-level "Output" (compiled.Count) read post-finalization and exact.
            if (resultCounts != null && opIndex >= 0 && opIndex < resultCounts.Length && resultCounts[opIndex] > 0)
                parameters["OutputWithDups"] = resultCounts[opIndex].ToString("N0");
            if (timings != null && opIndex >= 0 && opIndex < timings.Length && timings[opIndex] > 0)
                parameters["Ms"] = (timings[opIndex] / tickFreq).ToString("F3");

            // For an IN/range-expansion op, surface the ACTUAL expanded term-slot count this run (a runtime value
            // the template could not bake), read from the live InRangeCounts the generated code looped on.
            int rangeIdx = i < template.Length ? template[i].RangeCountIndex : -1;
            if (rangeIdx >= 0 && compiled.InRangeCounts != null && rangeIdx < compiled.InRangeCounts.Length)
                parameters["Terms"] = compiled.InRangeCounts[rangeIdx].ToString("N0");
        }

        // Mark whether the entry-scan branch fired this run, on the single EntryScan tail node. entryScanAt >= 0
        // means the cost gate switched off the bitmap pipeline at runtime; its value is the leaf-cursor position
        // at the switch — how many leaf clauses had merged into the slot-0 accumulator first (SwitchedAfterClauses).
        // The scanned/passed counts go here too.
        if (entryScanNode != null)
        {
            var p = entryScanNode.Parameters;
            p["Taken"] = (entryScanAt >= 0).ToString();
            if (entryScanAt >= 0)
            {
                p["SwitchedAfterClauses"] = entryScanAt.ToString();
                if (compiled.EntryScanEntriesScanned > 0)
                    p["EntriesScanned"] = compiled.EntryScanEntriesScanned.ToString("N0");
                if (compiled.EntryScanEntriesPassed > 0)
                    p["EntriesPassed"] = compiled.EntryScanEntriesPassed.ToString("N0");
                if (timings != null && compiled.EntryScanTiming > 0)
                    p["Ms"] = (compiled.EntryScanTiming / tickFreq).ToString("F3");
            }
        }
    }

    /// <summary>Render one entry-scan residual predicate (and, recursively, its AND/OR sub-group children) as
    /// an inspection node, surfacing field/compare/negation so the entry-scan body is legible without reading
    /// the generated C#.</summary>
    private static QueryInspectionNode BuildScanPredicateNode(ScanPredicateInfo predicate)
    {
        RuntimeHelpers.EnsureSufficientExecutionStack();

        if (predicate.SubPredicates != null)
        {
            var groupNode = new QueryInspectionNode(predicate.Group == GroupKind.Or ? "Residual-OrGroup" : "Residual-AndGroup");
            foreach (var sub in predicate.SubPredicates)
                groupNode.Children.Add(BuildScanPredicateNode(sub));
            return groupNode;
        }

        var parameters = new Dictionary<string, string>
        {
            ["FieldName"] = predicate.FieldName,
            ["Compare"] = predicate.CompareOp.ToString(),
            ["ValueType"] = predicate.ValueType.ToString()
        };
        if (predicate.Negated)
            parameters["Negated"] = "true";
        return new QueryInspectionNode("Residual", parameters: parameters);
    }

    /// <summary>
    /// Producer node for a CompoundKeyLookup. The two Equals clauses were folded into one composite-key TermQuery
    /// on the synthetic compound field, so neither component appears in the op stream; this node surfaces the
    /// compound field name, the two component field=value pairs, and the result count. Component ordering follows
    /// <see cref="PlanTemplate.CompoundExactAFirst"/>, the same order <c>ConstructCompoundExact</c> builds the key.
    /// </summary>
    private static QueryInspectionNode BuildCompoundKeyLookupNode(CompiledQuery result, QueryExecution exec, CompiledPlan compiledPlan)
    {
        ClauseExecution eA = exec.CompoundExactFirst;
        ClauseExecution eB = exec.CompoundExactSecond;
        var (first, second) = compiledPlan.Template.CompoundExactAFirst ? (eA, eB) : (eB, eA);

        string firstField = first.Clause.ResolvedFieldName ?? first.Clause.FieldName;
        string secondField = second.Clause.ResolvedFieldName ?? second.Clause.FieldName;
        string firstValue = FormatValueFromPlan(first.PackedParamValue, exec, first.PackedParamValue.Param1);
        string secondValue = FormatValueFromPlan(second.PackedParamValue, exec, second.PackedParamValue.Param1);

        var parameters = new Dictionary<string, string>
        {
            ["Dispatch"] = "CompoundTerm",
            ["FieldName"] = compiledPlan.Template.CompoundExactName,
            ["Components"] = $"{firstField}={firstValue} AND {secondField}={secondValue}"
        };

        // The executed lookup is a TermMatch; reuse its inspection's result count so the node carries the run's
        // cardinality (the only runtime fact here — there is no per-op timing overlay for a non-CompiledQueryMatch).
        if (result.ExecutedMatch?.Inspect() is { Parameters: { } inspected } && inspected.TryGetValue("Count", out string count))
            parameters["Count"] = count;

        return new QueryInspectionNode("CompoundKeyLookup", parameters: parameters);
    }

    /// <summary>For every clause the resolution pass collapsed to a MatchAll / MatchNothing sentinel, append
    /// an informational node under a single "ResolvedClauses" parent reporting the clause's field, its original
    /// clause type (from the never-mutated template <see cref="ClauseInfo"/>), and the static answer —
    /// MatchAll = always true (clause dropped, e.g. WHEN(false)), MatchNothing = always false (a contradiction,
    /// e.g. a self-excluding BETWEEN). Adds nothing when no clause was collapsed.</summary>
    private static void AppendResolvedClauses(QueryExecution exec, QueryInspectionNode root)
    {
        QueryInspectionNode resolvedNode = null;
        foreach (var clauseExec in exec.Executions)
            CollectSentinels(clauseExec, ref resolvedNode);

        if (resolvedNode != null)
            root.Children.Add(resolvedNode);

        static void CollectSentinels(ClauseExecution clauseExec, ref QueryInspectionNode resolvedNode)
        {
            RuntimeHelpers.EnsureSufficientExecutionStack();
            if (clauseExec.SubExecutions != null)
            {
                foreach (var sub in clauseExec.SubExecutions)
                    CollectSentinels(sub, ref resolvedNode);
            }

            if (clauseExec.IsSentinel == false)
                return;

            bool matchAll = clauseExec.ClauseType == ClauseType.MatchAll;
            var clauseParams = new Dictionary<string, string>
            {
                ["FieldName"] = clauseExec.Clause.FieldName,
                ["ClauseType"] = clauseExec.Clause.ClauseType.ToString(),
                ["ResolvedTo"] = matchAll ? "MatchAll" : "MatchNothing",
                ["Answer"] = matchAll ? "always true (clause dropped, not scanned)" : "always false (contradiction, not scanned)"
            };
            resolvedNode ??= new QueryInspectionNode("ResolvedClauses");
            resolvedNode.Children.Add(new QueryInspectionNode("StaticallyResolved", parameters: clauseParams));
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
        if (source.IsPostFilter)
        {
            target.Children.Add(source);
            return;
        }
        foreach (var child in source.Children)
        {
            AppendPostFilterNodes(child, target);
        }
    }

    /// <summary>
    /// Projects the flat <see cref="PlanOp"/> stream into the inspection-op template. The op stream is linear (the
    /// bitmap pipeline combines clauses in sequence), so boolean grouping is rendered as execution order rather
    /// than synthetic OrGroup/AndGroup nodes (which would misrepresent how the engine evaluated). Each op's name
    /// carries its combinator (Fill / AND / OR / ANDNOT); AND-Bitmaps / ANDNOT-Bitmaps mark where two bitmap
    /// sub-results merge (a parenthesised sub-group boundary). Reconstruct grouping from the sequence:
    /// Fill(A), OR(B), AND(C) == (A OR B) AND C.
    /// </summary>
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

            // Pure control-flow ops carry no data destination — they are the early-exit / terminal jumps of
            // the linear op stream and would only add noise to a physical dataflow view.
            if (op.Kind is PlanOpKind.GotoDoneIfEmpty or PlanOpKind.GotoDone)
                continue;

            var inspOp = new InspectionOp
            {
                Name = op.Kind switch
                {
                    PlanOpKind.FillFromPostingSource or PlanOpKind.FillFromTreeScan or PlanOpKind.FillFromMatch => "Fill",
                    PlanOpKind.FillAllEntries => "Fill-AllEntries",
                    PlanOpKind.AndFromPostingSource or PlanOpKind.AndFromTreeScan or PlanOpKind.AndFromMatch => "AND",
                    PlanOpKind.OrFromPostingSource or PlanOpKind.OrFromTreeScan or PlanOpKind.OrFromMatch => "OR",
                    PlanOpKind.AndNotFromPostingSource or PlanOpKind.AndNotFromTreeScan or PlanOpKind.AndNotFromMatch => "ANDNOT",
                    PlanOpKind.AndBitmaps => "AND-Bitmaps",
                    PlanOpKind.AndNotBitmaps => "ANDNOT-Bitmaps",
                    PlanOpKind.LazyOrBitmaps => "OR-Bitmaps",
                    PlanOpKind.ClearBitmap => "Clear",
                    PlanOpKind.MaybeEntryScan => "EntryScanCheck",
                    PlanOpKind.OrRangeFromPostingSource or PlanOpKind.OrRangeFromMatch => "OR-Range",
                    PlanOpKind.AndRangeFromPostingSource or PlanOpKind.AndRangeFromMatch => "AND-Range",
                    _ => op.Kind.ToString()
                },
                Dispatch = op.Kind switch
                {
                    PlanOpKind.FillFromPostingSource or PlanOpKind.AndFromPostingSource or PlanOpKind.OrFromPostingSource
                        or PlanOpKind.AndNotFromPostingSource or PlanOpKind.OrRangeFromPostingSource or PlanOpKind.AndRangeFromPostingSource => "Term",
                    PlanOpKind.FillFromTreeScan or PlanOpKind.AndFromTreeScan or PlanOpKind.OrFromTreeScan
                        or PlanOpKind.AndNotFromTreeScan => "MultiTerm",
                    // MaybeEntryScan is a control-flow branch, not a match dispatch — leave Dispatch unset 
                    PlanOpKind.MaybeEntryScan or PlanOpKind.AndBitmaps or PlanOpKind.AndNotBitmaps
                        or PlanOpKind.LazyOrBitmaps or PlanOpKind.ClearBitmap or PlanOpKind.FillAllEntries => null,
                    _ => "Match"
                },
                OpIndex = i,
                DestSlot = op.BitmapLocal,
                SourceSlot = op.Kind switch
                {
                    PlanOpKind.AndBitmaps or PlanOpKind.AndNotBitmaps or PlanOpKind.LazyOrBitmaps => op.ParamIndex2,
                    _ => -1
                },
                IsEntryScanGate = op.Kind == PlanOpKind.MaybeEntryScan,
                RangeCountIndex = op.Kind switch
                {
                    PlanOpKind.OrRangeFromPostingSource or PlanOpKind.OrRangeFromMatch
                        or PlanOpKind.AndRangeFromPostingSource or PlanOpKind.AndRangeFromMatch => op.ParamIndex2,
                    _ => -1
                }
            };

            // Attach clause metadata only to the LEAF ops that actually read a query clause from the cursor.
            // The slot-algebra ops (AND/ANDNOT/OR-Bitmaps, Clear), the universe fill, and the MaybeEntryScan
            // branch have a default ParamIndex of 0 that does NOT index a clause — attaching flatClauses[0] to
            // them would mislabel the merge/branch as filtering on the first clause.
            if (IsLeafOp(op.Kind) && op.ParamIndex >= 0 && op.ParamIndex < flatClauses.Count)
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

        static bool IsLeafOp(PlanOpKind kind) => kind switch
        {
            PlanOpKind.FillFromPostingSource or PlanOpKind.FillFromTreeScan or PlanOpKind.FillFromMatch
                or PlanOpKind.AndFromPostingSource or PlanOpKind.AndFromTreeScan or PlanOpKind.AndFromMatch
                or PlanOpKind.OrFromPostingSource or PlanOpKind.OrFromTreeScan or PlanOpKind.OrFromMatch
                or PlanOpKind.AndNotFromPostingSource or PlanOpKind.AndNotFromTreeScan or PlanOpKind.AndNotFromMatch
                or PlanOpKind.OrRangeFromPostingSource or PlanOpKind.OrRangeFromMatch
                or PlanOpKind.AndRangeFromPostingSource or PlanOpKind.AndRangeFromMatch => true,
            _ => false
        };

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
