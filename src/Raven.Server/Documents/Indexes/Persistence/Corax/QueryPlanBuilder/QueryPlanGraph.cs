using System.Collections.Generic;
using Corax.Querying.Matches.Meta;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

/// <summary>
/// Renders a compiled-query <see cref="QueryInspectionNode"/> plan as a Graphviz (DOT) dataflow graph, server-side.
/// The op stream is linear, but the bitmap SLOTS turn it into a graph: every op writes its <c>DestSlot</c>;
/// a non-Fill op that targets a slot consumes whatever last wrote that slot (the running accumulator); the
/// slot-to-slot merges (AND/ANDNOT/OR-Bitmaps) additionally consume their <c>SourceSlot</c>; and the
/// EntryScanCheck branches slot 0 into the entry-scan tail (slot 1). Walking the ops while tracking the last
/// writer per slot reconstructs the edges.
///
/// This is split in two: a BUILD pass populates a <see cref="GraphvizGraph"/> with the structural facts (every
/// node and edge carries its data — dispatch, slot, cardinality, timing, taken-state — in its Data bag), and a
/// STYLE pass (the <c>StyleNode</c>/<c>StyleEdge</c> callbacks handed to <see cref="GraphvizGraph.Render"/>) derives
/// presentation (labels, shapes, the green/grey taken colouring) from that data at render time. Keeping the model
/// and the data separate from the string generation means future changes touch one concern at a time.
/// </summary>
internal static class QueryPlanGraph
{
    // Per-node/per-edge fact keys we set ourselves (beyond the raw inspection parameters copied onto op nodes).
    private const string OperationKey = "Operation";
    private const string FlowKey = "Flow";
    private const string KindKey = "Kind";
    private const string SlotKey = "Slot";
    private const string VariantKey = "Variant";
    private const string FilterKey = "Filter";

    // Synthetic node "operations" for the nodes that are not bitmap ops.
    private const string ResultOp = "Result";
    private const string ResidualNoteOp = "ResidualNote";
    private const string DirectScanOp = "DirectScan";

    // Edge kinds.
    private const string DataflowKind = "dataflow";
    private const string GateKind = "gate";
    private const string BranchKind = "branch";
    private const string ResultKind = "result";
    private const string ResidualKind = "residual";
    private const string SequenceKind = "sequence";

    // Edge/node flow (taken) states. Drive the green/grey colouring at style time.
    private const string FlowOn = "on";           // traversed this run            -> bold green
    private const string FlowOff = "off";         // known NOT taken this run       -> dotted grey
    private const string FlowCandidate = "candidate"; // conditional, did not fire  -> dashed grey
    private const string FlowDashed = "dashed";   // gate with no runtime overlay   -> plain dashed
    private const string FlowInvis = "invis";     // sequencing-only edge           -> invisible
    private const string FlowNone = "none";       // no runtime info                -> plain

    private const string TakenGreen = "#1a7f37";

    /// <summary>Render <paramref name="plan"/> (or the CompiledQuery node within it) as Graphviz DOT text.</summary>
    public static string ToGraphviz(QueryInspectionNode plan)
    {
        QueryInspectionNode compiled = FindNode(plan, "CompiledQuery");
        if (compiled?.Children == null)
            return "digraph QueryPlan { /* no compiled op stream */ }\n";

        // Op nodes are exactly the children that carry a DestSlot (Fill/AND/OR/ANDNOT/*-Bitmaps/Clear/
        // EntryScanCheck/Range). DecisionTrail / ResolvedClauses / Vector / Spatial nodes have no DestSlot and are
        // skipped — they are not part of the bitmap dataflow.
        var ops = new List<QueryInspectionNode>();
        foreach (QueryInspectionNode child in compiled.Children)
        {
            if (child.Parameters != null && child.Parameters.ContainsKey("DestSlot"))
                ops.Add(child);
        }

        // The DirectScan node (present when a tree-scan strategy actually executed) is a child of CompiledQuery with
        // no DestSlot, so it is NOT part of `ops`. Find it separately: when it exists it — not the bitmap pipeline —
        // produced the answer, so we render it as its own node and grey out the unexecuted bitmap exit below.
        QueryInspectionNode directScanNode = null;
        foreach (QueryInspectionNode child in compiled.Children)
        {
            if (child.Operation == DirectScanOp)
            {
                directScanNode = child;
                break;
            }
        }

        // --- Runtime taken-path analysis ------------------------------------------------------------------------
        // Locate the entry-scan tail and the cost gates so every node can carry a taken-state and every edge can be
        // coloured by whether THIS run traversed it. The runtime facts come from OverlayTimings (present only when
        // the query asked for include timings()): the tail's Taken flag, and SwitchedAfterClauses = how many leaf
        // clauses had merged into slot 0 when the scan switched on. The j-th gate (1-indexed in op order) is the one
        // that fires when SwitchedAfterClauses == j, so the fired gate is gateOpIds[switchedAfter-1].
        int entryScanTailId = -1;
        var gateOpIds = new List<int>();
        for (int i = 0; i < ops.Count; i++)
        {
            if (ops[i].Operation == "EntryScan")
                entryScanTailId = i;
            else if (ops[i].Operation == "EntryScanCheck")
                gateOpIds.Add(i);
        }

        bool entryScanTaken = entryScanTailId >= 0
            && ops[entryScanTailId].Parameters != null
            && ops[entryScanTailId].Parameters.TryGetValue("Taken", out string takenVal)
            && takenVal == "True";

        int switchedAfter = -1;
        if (entryScanTaken && ops[entryScanTailId].Parameters.TryGetValue("SwitchedAfterClauses", out string sac))
            int.TryParse(sac, out switchedAfter);
        int firedGateOp = switchedAfter >= 1 && switchedAfter <= gateOpIds.Count ? gateOpIds[switchedAfter - 1] : -1;

        // Did the runtime overlay run at all? Without it we cannot know the taken path, so nodes carry no data_taken
        // and edges fall back to a plain (uncoloured) style rather than falsely claiming a route. With an entry-scan
        // gate the Taken flag is the signal; otherwise any op carrying a runtime Count proves the overlay ran.
        bool hasRuntime = entryScanTailId >= 0
            ? ops[entryScanTailId].Parameters != null && ops[entryScanTailId].Parameters.ContainsKey("Taken")
            : AnyHasCount(ops);

        // An op executed this run iff the run did not switch to the entry scan before reaching it: when the scan
        // fired at firedGateOp, only ops BEFORE that gate built the slot-0 accumulator; the post-switch merges were
        // skipped. When the scan did not fire, every op ran.
        bool OpExecuted(int opIndex) => entryScanTaken == false || firedGateOp < 0 || opIndex < firedGateOp;

        // A cost gate READS the slot-0 accumulator to decide whether to switch to the entry scan. A gate ran this
        // run iff it was reached before (or AT) the switch: with no switch every gate ran and declined; with a switch
        // the gates up to and INCLUDING the one that fired read slot 0 (the fired gate made the call), while gates
        // after it were never reached. So a gate is reached iff gateOp <= firedGateOp (or no switch happened at all).
        bool GateReached(int gateOp) => hasRuntime && (entryScanTaken == false || (firedGateOp >= 0 && gateOp <= firedGateOp));

        // Whether op `i` was on the path the run actually took, mirroring the green/grey edge colouring so a DOT
        // consumer reads the executed path off data_taken: the entry-scan tail uses its own Taken flag; a cost gate
        // uses GateReached (taken if the run reached it); any other op uses OpExecuted.
        bool NodeTaken(int i)
            => ops[i].Operation == "EntryScan" ? entryScanTaken
             : ops[i].Operation == "EntryScanCheck" ? GateReached(i)
             : OpExecuted(i);

        // The flow state of a real dataflow edge whose CONSUMER is op `to`: on = traversed this run; off = skipped
        // (the entry scan replaced these merges); none = no runtime info, so make no claim.
        string DataEdgeFlow(int to) => hasRuntime == false ? FlowNone : OpExecuted(to) ? FlowOn : FlowOff;

        // --- BUILD pass: nodes + edges carrying only facts (Data). Presentation is derived in the STYLE pass. -----
        var g = new GraphvizGraph();
        g.NodeDefaults["shape"] = "box";
        g.NodeDefaults["fontname"] = "monospace";

        for (int i = 0; i < ops.Count; i++)
        {
            Dictionary<string, string> d = g.CreateNode("op" + i);
            d[OperationKey] = ops[i].Operation;
            CopyParameters(ops[i], d);
            // Per-node taken state, for every node, mirroring the edge colouring. The entry-scan tail already carries
            // a Taken param (copied above); for every other node we synthesise it. Skipped when no overlay ran.
            if (hasRuntime && (ops[i].Parameters == null || ops[i].Parameters.ContainsKey("Taken") == false))
                d["Taken"] = NodeTaken(i) ? "True" : "False";
        }

        if (directScanNode != null)
        {
            Dictionary<string, string> d = g.CreateNode("directscan");
            d[OperationKey] = DirectScanOp;
            CopyParameters(directScanNode, d);
        }

        Dictionary<string, string> resultData = g.CreateNode("result");
        resultData[OperationKey] = ResultOp;

        // Edges by slot dataflow. Track the last writer per slot to reconstruct consumers; realEdges records which
        // consecutive op pairs already have a real edge so the invisible sequencing pass can skip them.
        var lastWriter = new Dictionary<int, int>();
        var realEdges = new HashSet<(int From, int To)>();
        for (int i = 0; i < ops.Count; i++)
        {
            QueryInspectionNode op = ops[i];

            // The EntryScan tail is the shared branch TARGET, not part of the linear slot dataflow. Wired after the
            // loop (gate edges from every check, and its slot-1 survivors to the result).
            if (op.Operation == "EntryScan")
                continue;

            // EntryScanCheck is a read-only cost GATE on the slot-0 accumulator: it reads slot 0 and may divert to
            // the entry-scan tail, but writes NO slot. Draw a gate edge from the current slot-0 writer; crucially do
            // NOT make it a writer — that would chain the next op through a phantom slot.
            if (op.Operation == "EntryScanCheck")
            {
                if (lastWriter.TryGetValue(0, out int gateSrc))
                {
                    Dictionary<string, string> e = g.CreateEdge("op" + gateSrc, "op" + i);
                    e[KindKey] = GateKind;
                    e[FlowKey] = hasRuntime == false ? FlowDashed : GateReached(i) ? FlowOn : FlowOff;
                }

                continue;
            }

            int dest = ParseSlot(op, "DestSlot");
            bool isFill = op.Operation is "Fill" or "Fill-AllEntries";

            // A combining op reads the running accumulator already in its destination slot.
            if (isFill == false && lastWriter.TryGetValue(dest, out int destWriter))
            {
                Dictionary<string, string> e = g.CreateEdge("op" + destWriter, "op" + i);
                e[KindKey] = DataflowKind;
                e[SlotKey] = dest.ToString();
                e[FlowKey] = DataEdgeFlow(i);
                realEdges.Add((destWriter, i));
            }

            // A slot-to-slot merge also reads its source slot.
            if (op.Parameters.ContainsKey("SourceSlot"))
            {
                int src = ParseSlot(op, "SourceSlot");
                if (lastWriter.TryGetValue(src, out int srcWriter))
                {
                    Dictionary<string, string> e = g.CreateEdge("op" + srcWriter, "op" + i);
                    e[KindKey] = DataflowKind;
                    e[SlotKey] = src.ToString();
                    e[FlowKey] = DataEdgeFlow(i);
                    realEdges.Add((srcWriter, i));
                }
            }

            lastWriter[dest] = i;
        }

        // Result edge: the bitmap pipeline leaves its answer in slot 0 — UNLESS the entry scan fired this run, in
        // which case the survivors live in slot 1 and the bitmap pipeline's slot-0 result was thrown away; or a
        // tree-scan strategy ran, in which case the bitmap pipeline never produced the answer at all.
        if (lastWriter.TryGetValue(0, out int finalWriter))
        {
            Dictionary<string, string> e = g.CreateEdge("op" + finalWriter, "result");
            e[KindKey] = ResultKind;
            if (directScanNode != null)
            {
                e[VariantKey] = "bitmap-candidate";
                e[FlowKey] = FlowOff;
            }
            else if (entryScanTaken)
            {
                e[VariantKey] = "not-taken";
                e[FlowKey] = FlowOff;
            }
            else if (hasRuntime)
            {
                e[VariantKey] = "bitmap-final";
                e[FlowKey] = FlowOn;
            }
            else
            {
                e[VariantKey] = "bitmap-plain";
                e[FlowKey] = FlowNone;
            }
        }

        // When a tree-scan strategy actually ran, the DirectScan node — not the bitmap pipeline — is the real
        // producer of the answer. Its result edge is on (mirroring the entry-scan TAKEN styling), and its per-entry
        // residual filter (every OTHER clause) hangs off it as one note node.
        if (directScanNode != null)
        {
            Dictionary<string, string> resultEdge = g.CreateEdge("directscan", "result");
            resultEdge[KindKey] = ResultKind;
            resultEdge[VariantKey] = "scan-result";
            resultEdge[FlowKey] = FlowOn;

            string scanFilter = CombinedResidualFilter(directScanNode.Children);
            if (scanFilter != null)
            {
                Dictionary<string, string> noteData = g.CreateNode("res_direct");
                noteData[OperationKey] = ResidualNoteOp;
                noteData[FilterKey] = scanFilter;
                noteData[FlowKey] = FlowOn;

                Dictionary<string, string> noteEdge = g.CreateEdge("directscan", "res_direct");
                noteEdge[KindKey] = ResidualKind;
                noteEdge[FlowKey] = FlowOn;
            }
        }

        // Entry-scan branch: a gate that fires diverts the slot-0 accumulator into the single scan tail, whose slot-1
        // survivors become the answer. Only the gate that actually fired this run (firedGateOp) is on; the other
        // checks — whether they ran and declined, or were never reached — stay candidate, so exactly one switch edge
        // lights up as the route the run took.
        if (entryScanTailId >= 0)
        {
            foreach (int gate in gateOpIds)
            {
                bool isFired = entryScanTaken && gate == firedGateOp;
                Dictionary<string, string> e = g.CreateEdge("op" + gate, "op" + entryScanTailId);
                e[KindKey] = BranchKind;
                e[FlowKey] = isFired ? FlowOn : FlowCandidate;
            }

            Dictionary<string, string> tailResult = g.CreateEdge("op" + entryScanTailId, "result");
            tailResult[KindKey] = ResultKind;
            tailResult[VariantKey] = entryScanTaken ? "entryscan-taken" : "entryscan-iftaken";
            tailResult[FlowKey] = entryScanTaken ? FlowOn : FlowCandidate;

            // Residual predicates: the per-entry checks the scan applies to each slot-0 survivor once a gate switched
            // the pipeline off. One conjunctive note node (every survivor must pass ALL of them). On when the scan
            // fired this run; off otherwise — the bitmap pipeline ignores them.
            string entryFilter = CombinedResidualFilter(ops[entryScanTailId].Children);
            if (entryFilter != null)
            {
                Dictionary<string, string> noteData = g.CreateNode("res_entry");
                noteData[OperationKey] = ResidualNoteOp;
                noteData[FilterKey] = entryFilter;
                noteData[FlowKey] = entryScanTaken ? FlowOn : FlowOff;

                Dictionary<string, string> noteEdge = g.CreateEdge("op" + entryScanTailId, "res_entry");
                noteEdge[KindKey] = ResidualKind;
                noteEdge[FlowKey] = entryScanTaken ? FlowOn : FlowOff;
            }
        }

        // Invisible sequencing edges: pin parallel-looking branches to true execution order. For each consecutive op
        // pair with no real dataflow edge, an invisible edge forces the second to rank below the first. Entry-scan
        // nodes are skipped — their branch edges already express the (conditional) ordering, and chaining through
        // them would imply a false data dependency.
        for (int i = 0; i + 1 < ops.Count; i++)
        {
            if (ops[i].Operation is "EntryScan" or "EntryScanCheck")
                continue;
            if (ops[i + 1].Operation is "EntryScan" or "EntryScanCheck")
                continue;
            if (realEdges.Contains((i, i + 1)))
                continue;
            Dictionary<string, string> e = g.CreateEdge("op" + i, "op" + (i + 1));
            e[KindKey] = SequenceKind;
            e[FlowKey] = FlowInvis;
        }

        return g.Render(StyleNode, StyleEdge);
    }

    // --- STYLE pass: derive presentation (label/shape/style/color) from each element's facts at render time. ------

    private static void StyleNode(GraphvizGraph.Node node)
    {
        node.Data.TryGetValue(OperationKey, out string operation);
        switch (operation)
        {
            case ResultOp:
                node.Attributes["shape"] = "ellipse";
                node.Attributes["label"] = "Result";
                break;

            case ResidualNoteOp:
                node.Attributes["shape"] = "note";
                node.Data.TryGetValue(FlowKey, out string noteFlow);
                node.Attributes["color"] = noteFlow == FlowOn ? TakenGreen : "grey";
                node.Data.TryGetValue(FilterKey, out string filter);
                node.Attributes["label"] = GraphvizGraph.Escape(filter ?? "");
                break;

            case DirectScanOp:
                node.Attributes["style"] = "bold";
                node.Attributes["color"] = TakenGreen;
                node.Attributes["label"] = DirectScanLabel(node.Data);
                break;

            default:
                node.Attributes["label"] = OpLabel(operation, node.Data);
                break;
        }
    }

    private static void StyleEdge(GraphvizGraph.Edge edge)
    {
        edge.Data.TryGetValue(FlowKey, out string flow);
        switch (flow)
        {
            case FlowOn:
                edge.Attributes["style"] = "bold";
                edge.Attributes["color"] = TakenGreen;
                break;
            case FlowOff:
                edge.Attributes["style"] = "dotted";
                edge.Attributes["color"] = "grey";
                break;
            case FlowCandidate:
                edge.Attributes["style"] = "dashed";
                edge.Attributes["color"] = "grey";
                break;
            case FlowDashed:
                edge.Attributes["style"] = "dashed";
                break;
            case FlowInvis:
                edge.Attributes["style"] = "invis";
                break;
        }

        edge.Data.TryGetValue(KindKey, out string kind);
        string label = kind switch
        {
            DataflowKind => "slot " + (edge.Data.TryGetValue(SlotKey, out string slot) ? slot : ""),
            GateKind => "gate slot 0",
            BranchKind => flow == FlowOn ? "switched here" : "candidate switch",
            ResidualKind => "per entry",
            ResultKind => ResultEdgeLabel(edge),
            _ => null
        };
        if (string.IsNullOrEmpty(label) == false)
            edge.Attributes["label"] = GraphvizGraph.Escape(label);
    }

    private static string ResultEdgeLabel(GraphvizGraph.Edge edge)
    {
        edge.Data.TryGetValue(VariantKey, out string variant);
        return variant switch
        {
            "bitmap-candidate" => "(bitmap candidate, not executed)",
            "not-taken" => "(not taken)",
            "scan-result" => "scan result",
            "entryscan-taken" => "entry-scan TAKEN",
            "entryscan-iftaken" => "if entry-scan taken",
            _ => null // bitmap-final / bitmap-plain carry no label
        };
    }

    private static int ParseSlot(QueryInspectionNode op, string key)
        => op.Parameters != null && op.Parameters.TryGetValue(key, out string v) && int.TryParse(v, out int n) ? n : -1;

    private static QueryInspectionNode FindNode(QueryInspectionNode node, string operation)
    {
        if (node == null)
            return null;
        if (node.Operation == operation)
            return node;
        if (node.Children != null)
        {
            foreach (QueryInspectionNode child in node.Children)
            {
                QueryInspectionNode found = FindNode(child, operation);
                if (found != null)
                    return found;
            }
        }

        return null;
    }

    /// <summary>Copy an inspection node's parameters into a node Data bag, skipping the large source/graph blobs that
    /// only live on the root node — they must never bloat a rendered op/scan node.</summary>
    private static void CopyParameters(QueryInspectionNode op, Dictionary<string, string> data)
    {
        if (op.Parameters == null)
            return;
        foreach (KeyValuePair<string, string> kv in op.Parameters)
        {
            if (kv.Key is "CSharpSource" or "CSharpSourceFormatted" or "PlanGraphDot")
                continue;
            if (string.IsNullOrEmpty(kv.Value))
                continue;
            data[kv.Key] = kv.Value;
        }
    }

    /// <summary>Builds the readable, multi-line label for a bitmap op node from its facts. The taken-state is NOT
    /// rendered into the label — it is surfaced as data_taken and via the edge colouring — so the label stays the
    /// structural picture (dispatch, field, term, slot, cardinality, count, timing).</summary>
    private static string OpLabel(string operation, Dictionary<string, string> p)
    {
        var parts = new List<string> { operation };

        // Dispatch (Term / MultiTerm / Match) — how this leaf reaches its postings. Slot-algebra and control-flow
        // ops have no dispatch and so render none.
        if (p.TryGetValue("Dispatch", out string dispatch) && string.IsNullOrEmpty(dispatch) == false)
            parts.Add("[" + dispatch + "]");

        AddIf(p, parts, "FieldName");
        AddIf(p, parts, "ClauseType");
        AddIf(p, parts, "Term");
        AddIf(p, parts, "Term2");
        AddIf(p, parts, "Terms");
        if (p.TryGetValue("Negated", out string neg) && neg == "true")
            parts.Add("NEGATED");
        AddIf(p, parts, "EstimatedRows", "~");
        AddIf(p, parts, "DestSlot", "→slot ");
        AddIf(p, parts, "Count", "count=");
        AddIf(p, parts, "SwitchedAfterClauses", "after=");
        AddIf(p, parts, "EntriesScanned", "scanned=");
        AddIf(p, parts, "EntriesPassed", "passed=");
        if (p.TryGetValue("Ms", out string ms) && string.IsNullOrEmpty(ms) == false)
            parts.Add(ms + " ms");

        for (int i = 0; i < parts.Count; i++)
            parts[i] = GraphvizGraph.Escape(parts[i]);
        return string.Join("\\n", parts);
    }

    /// <summary>Builds the readable label for the executed-tree-scan node from its facts: the driving tree / clause /
    /// seek bound / direction, the per-entry residual predicates, the scan counts, and the per-phase timing.</summary>
    private static string DirectScanLabel(Dictionary<string, string> p)
    {
        var parts = new List<string> { DirectScanOp };
        AddIf(p, parts, "DrivingTree", "tree=");
        AddIf(p, parts, "DrivingClause", "drive=");
        AddIf(p, parts, "SeekBound", "seek=");
        AddIf(p, parts, "TreeDirection", "dir=");
        AddIf(p, parts, "ResidualPredicates", "residuals: ");
        AddIf(p, parts, "TreeEntriesScanned", "scanned=");
        AddIf(p, parts, "EntriesPassedFilter", "passed=");
        AddIf(p, parts, "EntriesRejected", "rejected=");
        AddIf(p, parts, "StoppedAt", "stopped=");
        AddIf(p, parts, "TreeScan_ms", "tree=", " ms");
        AddIf(p, parts, "EntryScans_ms", "entry=", " ms");
        for (int i = 0; i < parts.Count; i++)
            parts[i] = GraphvizGraph.Escape(parts[i]);
        return string.Join("\\n", parts);
    }

    // prefix == "" => render as "Key=value"; otherwise render as "prefix" + value + suffix (e.g. "~1,234").
    private static void AddIf(Dictionary<string, string> p, List<string> into, string key, string prefix = "", string suffix = "")
    {
        if (p.TryGetValue(key, out string val) && string.IsNullOrEmpty(val) == false)
            into.Add(prefix.Length == 0 ? key + "=" + val : prefix + val + suffix);
    }

    /// <summary>True when any op carries a runtime <c>Count</c> parameter, i.e. OverlayTimings ran and the taken path
    /// is knowable. Used for plans with no entry-scan gate, where there is no Taken flag to key off.</summary>
    private static bool AnyHasCount(List<QueryInspectionNode> ops)
    {
        foreach (QueryInspectionNode op in ops)
        {
            if (op.Parameters != null && op.Parameters.ContainsKey("Count"))
                return true;
        }

        return false;
    }

    /// <summary>Joins a scan's Residual children into a single conjunctive filter string — "A AND B AND C" — for one
    /// note node, since every survivor must pass ALL of them. Returns null when there are no residual children.</summary>
    private static string CombinedResidualFilter(List<QueryInspectionNode> children)
    {
        if (children == null)
            return null;
        var tokens = new List<string>();
        foreach (QueryInspectionNode child in children)
        {
            string token = ResidualToken(child);
            if (token != null)
                tokens.Add(token);
        }

        return tokens.Count == 0 ? null : string.Join(" AND ", tokens);
    }

    /// <summary>Renders one residual-predicate node into a compact "Field Compare" token (a leading "!" marks a
    /// negated check, and an AND/OR group is wrapped in parentheses with the matching joiner). Returns null when the
    /// node is not a residual. Recurses into Residual-AndGroup / Residual-OrGroup children.</summary>
    private static string ResidualToken(QueryInspectionNode node)
    {
        if (node.Operation is "Residual-AndGroup" or "Residual-OrGroup")
        {
            string joiner = node.Operation == "Residual-OrGroup" ? " OR " : " AND ";
            var inner = new List<string>();
            if (node.Children != null)
            {
                foreach (QueryInspectionNode sub in node.Children)
                {
                    string token = ResidualToken(sub);
                    if (token != null)
                        inner.Add(token);
                }
            }

            return inner.Count == 0 ? null : "(" + string.Join(joiner, inner) + ")";
        }

        if (node.Operation != "Residual" || node.Parameters == null)
            return null;

        node.Parameters.TryGetValue("FieldName", out string field);
        node.Parameters.TryGetValue("Compare", out string compare);
        bool negated = node.Parameters.TryGetValue("Negated", out string n) && n == "true";
        return (negated ? "!" : "") + field + " " + compare;
    }
}
