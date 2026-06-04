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
        var compiledPlan = result.CompiledPlan;
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

        // The generated C# is the single most useful artifact for understanding what physically ran — it is the
        // exact mirror of the emitted IL. Surface it on the plan root so the timings payload is self-contained
        // (strategy + decision trail + op stream + the code), instead of only being reachable via the executed
        // match's own Inspect(). Both the raw and Roslyn-formatted forms are carried; consumers pick one.
        if (compiledPlan.Source != null)
        {
            rootParams["CSharpSource"] = compiledPlan.Source;
            rootParams["CSharpSourceFormatted"] = compiledPlan.FormattedSource;
        }
        if (compiledPlan.AllNegated)
            rootParams["AllNegated"] = "true";

        var root = new QueryInspectionNode("CompiledQuery", parameters: rootParams);
        compiledRoot = root;
        opNodes = new List<QueryInspectionNode>(template.Length);
        bool hasEntryScanGate = false;

        for (int i = 0; i < template.Length; i++)
        {
            var t = template[i];
            var parameters = new Dictionary<string, string>();

            // Physical destination/source slots: the dataflow backbone. A consumer reads Slot/FromSlot to
            // reconstruct which bitmap each op writes (and, for the slot-to-slot merges, which it reads),
            // and can build a flow graph from it. Always emitted for a real op (DestSlot >= 0).
            if (t.DestSlot >= 0) parameters["Slot"] = t.DestSlot.ToString();
            if (t.SourceSlot >= 0) parameters["FromSlot"] = t.SourceSlot.ToString();

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

            if (t.Name == "EntryScanCheck")
                hasEntryScanGate = true;
        }

        // Entry-scan tail: a SINGLE node modelling the shared scan body that every EntryScanCheck gate branches
        // to. The gates above are read-only cost checks on the slot-0 accumulator (Slot=0); the actual slot 0 ->
        // slot 1 move and the per-entry residual evaluation happen HERE, once, regardless of which gate fired.
        // Modelling it as one tail (rather than hanging the body off a specific check) is the accurate physical
        // picture and avoids mis-attributing "Taken" to a gate that did not fire (OverlayTimings sets the runtime
        // Taken flag + scanned/passed counts on this node). The Residual children are what the scan evaluates per
        // surviving entry. Rendered whenever the plan has an entry-scan path, independent of whether THIS run took it.
        if (hasEntryScanGate)
        {
            var entryScanParams = new Dictionary<string, string> { ["Slot"] = "1", ["FromSlot"] = "0" };
            var entryScanNode = new QueryInspectionNode("EntryScan", parameters: entryScanParams);
            if (compiledPlan.EntryScanSet is { HasPredicates: true } entryScan)
            {
                foreach (var predicate in entryScan.Predicates)
                    entryScanNode.Children.Add(BuildScanPredicateNode(predicate));
            }
            root.Children.Add(entryScanNode);
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

        // Surface clauses the resolution pass statically collapsed for this run. A sentinel emits no match
        // leaf — it resolves to a bitmap fill/clear and is algebraically simplified out of the op stream — so
        // these clauses are otherwise invisible in the executed tree above. Listing them closes the gap between
        // what the query TEXT asked for and what actually ran: the reader sees that e.g. a WHEN(false) clause or
        // a contradictory BETWEEN was answered without scanning, rather than wondering why a clause vanished.
        AppendResolvedClauses(exec, root);

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

        // The template is compacted (control-flow ops dropped), but per-op telemetry is recorded against the FULL
        // PlanOp[] index — so join each template node to its timing slot via the original OpIndex, not the node's
        // position in the compacted list (which drifts the moment any op is filtered out).
        var template = result.CompiledPlan.InspectionTemplate;
        double tickFreq = Stopwatch.Frequency / 1000.0;
        for (int i = 0; i < opNodes.Count; i++)
        {
            int opIndex = i < template.Length ? template[i].OpIndex : i;
            var parameters = opNodes[i].Parameters;
            if (resultCounts != null && opIndex >= 0 && opIndex < resultCounts.Length && resultCounts[opIndex] > 0)
                parameters["Count"] = resultCounts[opIndex].ToString();
            if (timings != null && opIndex >= 0 && opIndex < timings.Length && timings[opIndex] > 0)
                parameters["Ms"] = (timings[opIndex] / tickFreq).ToString("F3");
        }

        // Mark whether the entry-scan branch actually fired this run, on the single EntryScan tail node (the shared
        // scan body, not a specific gate). EntryScanTakenAtOp >= 0 means the cost gate switched off the bitmap
        // pipeline at runtime; its value is the leaf-cursor position at the switch, i.e. how many leaf clauses had
        // been merged into the slot-0 accumulator before the scan took over (SwitchedAfterClauses). Surfacing the
        // scanned/passed counts here is "where it skipped to the entry scan, and how much it scanned" in one place.
        QueryInspectionNode entryScanNode = null;
        foreach (var child in compiledRoot.Children)
        {
            if (child.Operation == "EntryScan")
            {
                entryScanNode = child;
                break;
            }
        }

        if (entryScanNode != null)
        {
            var p = entryScanNode.Parameters;
            p["Taken"] = (entryScanAt >= 0).ToString();
            if (entryScanAt >= 0)
            {
                p["SwitchedAfterClauses"] = entryScanAt.ToString();
                if (compiled.EntryScanEntriesScanned > 0)
                    p["EntriesScanned"] = compiled.EntryScanEntriesScanned.ToString();
                if (compiled.EntryScanEntriesPassed > 0)
                    p["EntriesPassed"] = compiled.EntryScanEntriesPassed.ToString();
            }
        }
    }

    /// <summary>Renders one entry-scan residual predicate (and, recursively, its AND/OR sub-group children) as an
    /// inspection node. These are the per-entry checks the scan applies once the cost gate switches off the bitmap
    /// pipeline; surfacing field / compare / negation makes the entry-scan body legible without reading the
    /// generated C#.</summary>
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
    /// Walks the runtime clause-execution tree (groups included) and, for every clause the resolution pass
    /// collapsed to a MatchAll / MatchNothing sentinel, appends an informational node under a single
    /// "ResolvedClauses" parent. Each node reports the clause's field, its ORIGINAL clause type (read from the
    /// template <see cref="ClauseInfo"/>, which is never mutated), and the static answer the resolver reached —
    /// MatchAll == "always true" (the clause was dropped, e.g. a WHEN(false) guard), MatchNothing == "always
    /// false" (a contradiction, e.g. a self-excluding BETWEEN). Adds nothing when no clause was collapsed.
    /// </summary>
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

    /// <summary>
    /// Projects the flat <see cref="PlanOp"/> stream into the inspection-op template. The op stream is linear by
    /// design — the bitmap pipeline combines clauses by applying operations in sequence — so boolean grouping is
    /// rendered as that execution order rather than as synthetic OrGroup/AndGroup wrapper nodes (which would
    /// misrepresent how the engine actually evaluated the query). Each op's name carries its combinator
    /// (Fill / AND / OR / ANDNOT), and the AND-Bitmaps / ANDNOT-Bitmaps nodes mark the points where two bitmap
    /// sub-results merge — i.e. a parenthesised sub-group boundary. A reader reconstructs the grouping from the
    /// sequence: Fill(A), OR(B), AND(C) == (A OR B) AND C.
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
                    // Lazy-OR is a real merge of two slots — surfacing it (instead of skipping it, as before) is
                    // what makes a top-level OR / nested-group plan legible: the merge point is no longer invisible.
                    PlanOpKind.LazyOrBitmaps => "OR-Bitmaps",
                    PlanOpKind.ClearBitmap => "Clear",
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
                    // MaybeEntryScan is a control-flow branch, not a match dispatch — leave Dispatch unset so the
                    // EntryScanCheck node is not mislabelled "Match" (which would read as "matched via in-memory match").
                    // The slot-to-slot algebra ops (AND/ANDNOT/OR-Bitmaps), Clear, and Fill-AllEntries have no leaf
                    // dispatch either; they operate on whole bitmaps.
                    PlanOpKind.MaybeEntryScan or PlanOpKind.AndBitmaps or PlanOpKind.AndNotBitmaps
                        or PlanOpKind.LazyOrBitmaps or PlanOpKind.ClearBitmap or PlanOpKind.FillAllEntries => null,
                    _ => "Match"
                },
                EstimatedCardinality = op.EstimatedCardinality,
                OpIndex = i,

                // Physical dataflow annotation: every op writes BitmapLocal; the slot-to-slot merges also read a
                // source slot (ParamIndex2). A consumer threads these to build the graph — slots are nodes, ops are
                // edges into DestSlot. MaybeEntryScan is a read-only GATE on the slot-0 accumulator (it diverts to
                // the entry-scan tail without writing a slot), so it reports slot 0 as its observed destination; the
                // actual slot 0 -> slot 1 move lives on the separate EntryScan tail node (see BuildPlan).
                DestSlot = op.BitmapLocal,
                SourceSlot = op.Kind switch
                {
                    PlanOpKind.AndBitmaps or PlanOpKind.AndNotBitmaps or PlanOpKind.LazyOrBitmaps => op.ParamIndex2,
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
