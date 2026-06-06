using System.Collections.Generic;
using Corax.Querying.Matches.Meta;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

/// <summary>
///     Renders a compiled-query <see cref="QueryInspectionNode" /> plan as a Graphviz (DOT) dataflow graph.
///     The op stream is linear, but the bitmap SLOTS turn it into a graph: every op writes its <c>DestSlot</c>;
///     a non-Fill op that targets a slot consumes whatever last wrote that slot (the running accumulator); the
///     slot-to-slot merges (AND/ANDNOT/OR-Bitmaps) additionally consume their <c>SourceSlot</c>; and the
///     EntryScanCheck branches slot 0 into the entry-scan tail (slot 1). Walking the ops while tracking the last
///     writer per slot reconstructs the edges.
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
    private const string CompoundLookupOp = "CompoundKeyLookup";

    // Edge kinds.
    private const string DataflowKind = "dataflow";
    private const string GateKind = "gate";
    private const string BranchKind = "branch";
    private const string ResultKind = "result";
    private const string ResidualKind = "residual";
    private const string SequenceKind = "sequence";

    // Edge/node flow (taken) states. Drive the green/grey colouring at style time.
    private const string FlowOn = "on";
    private const string FlowOff = "off";
    private const string FlowCandidate = "candidate";
    private const string FlowDashed = "dashed";
    private const string FlowInvis = "invis";
    private const string FlowNone = "none";

    private const string TakenGreen = "#1a7f37";

    /// <summary>Render <paramref name="plan" /> (or the CompiledQuery node within it) as Graphviz DOT text.</summary>
    public static string ToGraphviz(QueryInspectionNode plan)
    {
        QueryInspectionNode compiled = FindNode(plan, "CompiledQuery");
        if (compiled?.Children == null)
        {
            return "digraph QueryPlan { /* no compiled op stream */ }\n";
        }

        List<QueryInspectionNode> ops = [];
        QueryInspectionNode producerNode = null;
        foreach (QueryInspectionNode child in compiled.Children)
        {
            if (child.Parameters != null && child.Parameters.ContainsKey("DestSlot"))
            {
                ops.Add(child);
            } 
            if (child.Operation is DirectScanOp or CompoundLookupOp)
            {
                producerNode = child;
            }
        }


        int entryScanTailId = -1;
        List<int> gateOpIds = [];
        for (int i = 0; i < ops.Count; i++)
        {
            if (ops[i].Operation == "EntryScan")
            {
                entryScanTailId = i;
            }
            else if (ops[i].Operation == "EntryScanCheck")
            {
                gateOpIds.Add(i);
            }
        }

        bool entryScanTaken = entryScanTailId >= 0
                              && ops[entryScanTailId].Parameters != null
                              && ops[entryScanTailId].Parameters.TryGetValue("Taken", out string takenVal)
                              && takenVal == "True";

        int switchedAfter = -1;
        if (entryScanTaken && ops[entryScanTailId].Parameters.TryGetValue("SwitchedAfterClauses", out string sac))
        {
            int.TryParse(sac, out switchedAfter);
        }

        int firedGateOp = switchedAfter >= 1 && switchedAfter <= gateOpIds.Count ? gateOpIds[switchedAfter - 1] : -1;

        bool hasRuntime = entryScanTailId >= 0
            ? ops[entryScanTailId].Parameters != null && ops[entryScanTailId].Parameters.ContainsKey("Taken")
            : AnyHasCount(ops);

        bool OpExecuted(int opIndex) => !entryScanTaken || firedGateOp < 0 || opIndex < firedGateOp;

        bool GateReached(int gateOp) => hasRuntime && (!entryScanTaken || (firedGateOp >= 0 && gateOp <= firedGateOp));

        bool NodeTaken(int i)
            => ops[i].Operation switch
            {
                "EntryScan" => entryScanTaken,
                "EntryScanCheck" => GateReached(i),
                _ => OpExecuted(i)
            };

        string DataEdgeFlow(int to) => !hasRuntime ? FlowNone : OpExecuted(to) ? FlowOn : FlowOff;

        GraphvizGraph g = new()
        {
            NodeDefaults =
            {
                ["shape"] = "box",
                ["fontname"] = "monospace"
            }
        };

        for (int i = 0; i < ops.Count; i++)
        {
            Dictionary<string, string> d = g.CreateNode("op" + i);
            d[OperationKey] = ops[i].Operation;
            CopyParameters(ops[i], d);
            if (hasRuntime && (ops[i].Parameters == null || !ops[i].Parameters.ContainsKey("Taken")))
            {
                d["Taken"] = NodeTaken(i).ToString();
            }
        }

        if (producerNode != null)
        {
            Dictionary<string, string> d = g.CreateNode("producer");
            d[OperationKey] = producerNode.Operation;
            CopyParameters(producerNode, d);
        }

        Dictionary<string, string> resultData = g.CreateNode("result");
        resultData[OperationKey] = ResultOp;

        Dictionary<int, int> lastWriter = [];
        HashSet<(int From, int To)> realEdges = [];
        for (int i = 0; i < ops.Count; i++)
        {
            QueryInspectionNode op = ops[i];
            switch (op.Operation)
            {
                case "EntryScan":
                    continue;
                case "EntryScanCheck":
                {
                    if (lastWriter.TryGetValue(0, out int gateSrc))
                    {
                        Dictionary<string, string> e = g.CreateEdge("op" + gateSrc, "op" + i);
                        e[KindKey] = GateKind;
                        e[FlowKey] = !hasRuntime ? FlowDashed : GateReached(i) ? FlowOn : FlowOff;
                    }
                    continue;
                }
            }

            int dest = ParseSlot(op, "DestSlot");
            bool isFill = op.Operation is "Fill" or "Fill-AllEntries";

            if (!isFill && lastWriter.TryGetValue(dest, out int destWriter))
            {
                Dictionary<string, string> e = g.CreateEdge("op" + destWriter, "op" + i);
                e[KindKey] = DataflowKind;
                e[SlotKey] = dest.ToString();
                e[FlowKey] = DataEdgeFlow(i);
                realEdges.Add((destWriter, i));
            }

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

        if (lastWriter.TryGetValue(0, out int finalWriter))
        {
            Dictionary<string, string> e = g.CreateEdge("op" + finalWriter, "result");
            e[KindKey] = ResultKind;
            if (entryScanTaken)
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

        if (producerNode != null)
        {
            Dictionary<string, string> resultEdge = g.CreateEdge("producer", "result");
            resultEdge[KindKey] = ResultKind;
            resultEdge[VariantKey] = producerNode.Operation == CompoundLookupOp ? "lookup-result" : "scan-result";
            resultEdge[FlowKey] = FlowOn;

            string scanFilter = CombinedResidualFilter(producerNode.Children);
            if (scanFilter != null)
            {
                Dictionary<string, string> noteData = g.CreateNode("res_producer");
                noteData[OperationKey] = ResidualNoteOp;
                noteData[FilterKey] = scanFilter;
                noteData[FlowKey] = FlowOn;

                Dictionary<string, string> noteEdge = g.CreateEdge("producer", "res_producer");
                noteEdge[KindKey] = ResidualKind;
                noteEdge[FlowKey] = FlowOn;
            }
        }

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

        // Invisible sequencing edges: pin parallel-looking branches to true execution order. An invisible edge forces the second to rank below the first. 
        for (int i = 0; i + 1 < ops.Count; i++)
        {
            if (ops[i].Operation is "EntryScan" or "EntryScanCheck" &&   // Entry-scan nodes are skipped — their branch edges already express the (conditional) ordering.
                ops[i + 1].Operation is "EntryScan" or "EntryScanCheck")
                continue;
            
            if (realEdges.Contains((i, i + 1)))
                continue;

            Dictionary<string, string> e = g.CreateEdge("op" + i, "op" + (i + 1));
            e[KindKey] = SequenceKind;
            e[FlowKey] = FlowInvis;
        }

        return g.Render(StyleNode, StyleEdge);
    }

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

            case CompoundLookupOp:
                node.Attributes["style"] = "bold";
                node.Attributes["color"] = TakenGreen;
                node.Attributes["label"] = CompoundLookupLabel(node.Data);
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
            DataflowKind => "slot " + edge.Data.GetValueOrDefault(SlotKey, ""),
            GateKind => "gate slot 0",
            BranchKind => flow == FlowOn ? "switched here" : "candidate switch",
            ResidualKind => "per entry",
            ResultKind => ResultEdgeLabel(edge),
            _ => null
        };
        if (!string.IsNullOrEmpty(label))
        {
            edge.Attributes["label"] = GraphvizGraph.Escape(label);
        }
    }

    private static string ResultEdgeLabel(GraphvizGraph.Edge edge)
    {
        edge.Data.TryGetValue(VariantKey, out string variant);
        return variant switch
        {
            "not-taken" => "(not taken)",
            "scan-result" => "scan result",
            "lookup-result" => "lookup result",
            "entryscan-taken" => "entry-scan TAKEN",
            "entryscan-iftaken" => "if entry-scan taken",
            _ => null // bitmap-final / bitmap-plain carry no label
        };
    }

    private static int ParseSlot(QueryInspectionNode op, string key)
    {
        return op.Parameters != null && op.Parameters.TryGetValue(key, out string v) && int.TryParse(v, out int n) ? n : -1;
    }

    private static QueryInspectionNode FindNode(QueryInspectionNode node, string operation)
    {
        if (node == null)
            return null;

        if (node.Operation == operation)
            return node;

        foreach (QueryInspectionNode child in node.Children ?? [])
        {
            QueryInspectionNode found = FindNode(child, operation);
            if (found != null)
                return found;
        }

        return null;
    }

    /// <summary>
    ///     Copy an inspection node's parameters into a node Data bag, skipping the large source/graph blobs that
    ///     only live on the root node — they must never bloat a rendered op/scan node.
    /// </summary>
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

    /// <summary>
    ///     Builds the readable, multi-line label for a bitmap op node from its facts. The taken-state is NOT
    ///     rendered into the label — it is surfaced as data_taken and via the edge colouring — so the label stays the
    ///     structural picture (dispatch, field, term, slot, cardinality, count, timing).
    /// </summary>
    private static string OpLabel(string operation, Dictionary<string, string> p)
    {
        List<string> parts = new()
            { operation };

        // Dispatch (Term / MultiTerm / Match) — how this leaf reaches its postings. Slot-algebra and control-flow
        // ops have no dispatch and so render none.
        if (p.TryGetValue("Dispatch", out string dispatch) && !string.IsNullOrEmpty(dispatch))
        {
            parts.Add("[" + dispatch + "]");
        }

        AddIf(p, parts, "FieldName");
        AddIf(p, parts, "ClauseType");
        AddIf(p, parts, "Term");
        AddIf(p, parts, "Term2");
        AddIf(p, parts, "Terms");
        if (p.TryGetValue("Negated", out string neg) && neg == "true")
        {
            parts.Add("NEGATED");
        }

        AddIf(p, parts, "EstimatedRows", "~");
        AddIf(p, parts, "DestSlot", "→slot ");
        AddIf(p, parts, "Count", "count=");
        AddIf(p, parts, "SwitchedAfterClauses", "after=");
        AddIf(p, parts, "EntriesScanned", "scanned=");
        AddIf(p, parts, "EntriesPassed", "passed=");
        if (p.TryGetValue("Ms", out string ms) && !string.IsNullOrEmpty(ms))
        {
            parts.Add(ms + " ms");
        }

        for (int i = 0; i < parts.Count; i++)
        {
            parts[i] = GraphvizGraph.Escape(parts[i]);
        }

        return string.Join("\\n", parts);
    }

    /// <summary>
    ///     Builds the readable label for the executed-tree-scan node from its facts: the driving tree / clause /
    ///     seek bound / direction, the per-entry residual predicates, the scan counts, and the per-phase timing.
    /// </summary>
    private static string DirectScanLabel(Dictionary<string, string> p)
    {
        List<string> parts = new()
            { DirectScanOp };
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
        {
            parts[i] = GraphvizGraph.Escape(parts[i]);
        }

        return string.Join("\\n", parts);
    }

    /// <summary>
    ///     Builds the readable label for a CompoundKeyLookup producer node from its facts: the synthetic
    ///     compound field, the two component field=value pairs the composite key encodes, and the result count.
    /// </summary>
    private static string CompoundLookupLabel(Dictionary<string, string> p)
    {
        List<string> parts = new()
            { CompoundLookupOp };
        if (p.TryGetValue("Dispatch", out string dispatch) && !string.IsNullOrEmpty(dispatch))
        {
            parts.Add("[" + dispatch + "]");
        }

        AddIf(p, parts, "FieldName");
        AddIf(p, parts, "Components", "key: ");
        AddIf(p, parts, "Count", "count=");
        for (int i = 0; i < parts.Count; i++)
        {
            parts[i] = GraphvizGraph.Escape(parts[i]);
        }

        return string.Join("\\n", parts);
    }

    private static void AddIf(Dictionary<string, string> p, List<string> into, string key, string prefix = "", string suffix = "")
    {
        if (p.TryGetValue(key, out string val) && !string.IsNullOrEmpty(val))
        {
            into.Add(prefix.Length == 0 ? key + "=" + val : prefix + val + suffix);
        }
    }

    /// <summary>
    ///     True when any op carries a runtime <c>Count</c> parameter, i.e. OverlayTimings ran and the taken path
    ///     is knowable. Used for plans with no entry-scan gate, where there is no Taken flag to key off.
    /// </summary>
    private static bool AnyHasCount(List<QueryInspectionNode> ops)
    {
        foreach (QueryInspectionNode op in ops)
        {
            if (op.Parameters != null && op.Parameters.ContainsKey("Count"))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    ///     Joins a scan's Residual children into a single conjunctive filter string — "A AND B AND C" — for one
    ///     note node, since every survivor must pass ALL of them. Returns null when there are no residual children.
    /// </summary>
    private static string CombinedResidualFilter(List<QueryInspectionNode> children)
    {
        if (children == null)
        {
            return null;
        }

        List<string> tokens = new();
        foreach (QueryInspectionNode child in children)
        {
            string token = ResidualToken(child);
            if (token != null)
            {
                tokens.Add(token);
            }
        }

        return tokens.Count == 0 ? null : string.Join(" AND ", tokens);
    }

    /// <summary>
    ///     Renders one residual-predicate node into a compact "Field Compare" token (a leading "!" marks a
    ///     negated check, and an AND/OR group is wrapped in parentheses with the matching joiner). Returns null when the
    ///     node is not a residual. Recurses into Residual-AndGroup / Residual-OrGroup children.
    /// </summary>
    private static string ResidualToken(QueryInspectionNode node)
    {
        if (node.Operation is "Residual-AndGroup" or "Residual-OrGroup")
        {
            string joiner = node.Operation == "Residual-OrGroup" ? " OR " : " AND ";
            List<string> inner = [];
            foreach (QueryInspectionNode sub in node.Children ?? [])
            {
                string token = ResidualToken(sub);
                if (token != null)
                {
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
