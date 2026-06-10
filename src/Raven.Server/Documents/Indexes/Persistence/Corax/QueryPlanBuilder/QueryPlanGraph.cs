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
    private const string CandidatesOp = "Candidates";
    private const string AllEntriesOp = "AllEntries";
    private const string PostFilterOp = "PostFilter";
    private const string SortOp = "Sort";
    private const string BoostOp = "Boost";

    // Operation names of the result-shaping wrappers that sit ABOVE the bitmap pipeline (CompiledQuery) in the
    // plan tree. The graph roots at the pipeline, so these are peeled off and rendered as the dataflow tail
    // (candidates → post-filters → sort/boost → Result).
    private static readonly HashSet<string> ResultWrapperOps = ["SortingMatch", "SortingMultiMatch", "BoostingMatch"];

    // Edge kinds.
    private const string DataflowKind = "dataflow";
    private const string GateKind = "gate";
    private const string BranchKind = "branch";
    private const string ResultKind = "result";
    private const string ResidualKind = "residual";
    private const string SequenceKind = "sequence";
    private const string RankKind = "rank";

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
            // plan shape without a CompiledQuery is the spatial/vector all-entries bypass (InstantiateAllEntriesPostFilter)
            if (FindNode(plan, "PostFilterMatch") is {} bypass)
                return RenderAllEntriesBypass(bypass, CollectResultWrappers(plan, bypass));

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

        List<QueryInspectionNode> postFilters = [];
        foreach (QueryInspectionNode child in compiled.Children)
        {
            if (child is { IsPostFilter: true })
                postFilters.Add(child);
        }

        List<QueryInspectionNode> wrappers = CollectResultWrappers(plan, compiled);
        bool hasPostChain = postFilters.Count > 0 || wrappers.Count > 0;

        string bitmapSink = hasPostChain ? "candidates" : "result";


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
                ["shape"] = "box"
            }
        };

        for (int i = 0; i < ops.Count; i++)
        {
            Dictionary<string, string> d = g.CreateNode("op" + i);
            d[OperationKey] = ops[i].Operation;
            CopyParameters(ops[i], d);
            if (hasRuntime && (ops[i].Parameters == null || ops[i].Parameters.ContainsKey("Taken") == false))
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
            Dictionary<string, string> e = g.CreateEdge("op" + finalWriter, bitmapSink);
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
            Dictionary<string, string> resultEdge = g.CreateEdge("producer", bitmapSink);
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

            Dictionary<string, string> tailResult = g.CreateEdge("op" + entryScanTailId, bitmapSink);
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

        if (hasPostChain)
        {
            Dictionary<string, string> candidates = g.CreateNode(bitmapSink);
            candidates[OperationKey] = CandidatesOp;
            BuildPostFilterChain(g, bitmapSink, postFilters, wrappers);
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

            case CandidatesOp:
                node.Attributes["shape"] = "ellipse";
                node.Attributes["style"] = "dashed";
                node.Attributes["label"] = "candidate set\\n(slot 0)";
                break;

            case AllEntriesOp:
                node.Attributes["style"] = "bold";
                node.Attributes["color"] = TakenGreen;
                node.Attributes["label"] = "AllEntries\\n\u2192 slot 0";
                break;

            case PostFilterOp:
                node.Attributes["color"] = TakenGreen;
                node.Attributes["label"] = PostFilterLabel(node.Data);
                break;

            case SortOp:
                node.Attributes["shape"] = "box";
                node.Attributes["style"] = "bold";
                node.Attributes["color"] = TakenGreen;
                node.Attributes["label"] = SortLabel(node.Data);
                break;

            case BoostOp:
                node.Attributes["style"] = "bold";
                node.Attributes["color"] = TakenGreen;
                node.Attributes["label"] = BoostLabel(node.Data);
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
            RankKind => "sort",
            ResultKind => ResultEdgeLabel(edge),
            _ => null
        };
        if (string.IsNullOrEmpty(label) == false)
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

    /// <summary>
    ///     Walks the result-shaping wrappers (sort / boost) that sit between the plan root and the bitmap
    ///     pipeline node <paramref name="pipeline" />, returning them OUTERMOST-FIRST. These are the
    ///     SortingMatch / SortingMultiMatch / BoostingMatch the executor layered on top of the candidate set;
    ///     the graph roots at the pipeline, so without this they would be invisible.
    /// </summary>
    private static List<QueryInspectionNode> CollectResultWrappers(QueryInspectionNode plan, QueryInspectionNode pipeline)
    {
        List<QueryInspectionNode> wrappers = [];
        QueryInspectionNode node = plan;
        while (node != null && node != pipeline && ResultWrapperOps.Contains(node.Operation))
        {
            wrappers.Add(node);
            node = node.Children is { Count: > 0 } ? node.Children[0] : null;
        }

        return wrappers;
    }

    /// <summary>
    ///     Chains the per-entry post-filters and the result-shaping wrappers off the bitmap pipeline output,
    ///     terminating at the Result node: <c>candidates → SpatialMatch → … → Sort/Boost → Result</c>. The
    ///     wrappers arrive outermost-first and are emitted innermost-first (the order data actually flows toward
    ///     the result), so a SortingMatch(BoostingMatch(pipeline)) renders as <c>… → Boost → Sort → Result</c>.
    /// </summary>
    private static void BuildPostFilterChain(GraphvizGraph g, string fromNode,
        List<QueryInspectionNode> postFilters, List<QueryInspectionNode> wrappers)
    {
        string prev = fromNode;
        for (int i = 0; i < postFilters.Count; i++)
        {
            string id = "pf" + i;
            Dictionary<string, string> node = g.CreateNode(id);
            node[OperationKey] = PostFilterOp;
            CopyParameters(postFilters[i], node);
            node["MatchOperation"] = postFilters[i].Operation;

            Dictionary<string, string> e = g.CreateEdge(prev, id);
            e[KindKey] = ResidualKind;
            e[FlowKey] = FlowOn;
            prev = id;
        }

        // Innermost wrapper first: reverse the outermost-first list so the chain matches dataflow order.
        for (int i = wrappers.Count - 1; i >= 0; i--)
        {
            QueryInspectionNode wrapper = wrappers[i];
            bool isBoost = wrapper.Operation == "BoostingMatch";
            string id = (isBoost ? "boost" : "sort") + i;
            Dictionary<string, string> node = g.CreateNode(id);
            node[OperationKey] = isBoost ? BoostOp : SortOp;
            CopyParameters(wrapper, node);
            node["MatchOperation"] = wrapper.Operation;

            Dictionary<string, string> e = g.CreateEdge(prev, id);
            if (isBoost == false)
                e[KindKey] = RankKind; // boost edge carries no label; the factor is on the node
            e[FlowKey] = FlowOn;
            prev = id;
        }

        Dictionary<string, string> resultEdge = g.CreateEdge(prev, "result");
        resultEdge[KindKey] = ResultKind;
        resultEdge[FlowKey] = FlowOn;
    }

    /// <summary>
    ///     Renders the spatial/vector all-entries bypass (InstantiateAllEntriesPostFilter): there is no
    ///     compiled bitmap pipeline, just an implicit full scan feeding a PostFilterMatch. The PostFilterMatch
    ///     children are the spatial/vector filters; they are chained off a synthetic AllEntries source, with
    ///     any sort/boost wrapper that was peeled off the plan root rendered after them.
    /// </summary>
    private static string RenderAllEntriesBypass(QueryInspectionNode postFilter, List<QueryInspectionNode> wrappers)
    {
        GraphvizGraph g = new()
        {
            NodeDefaults =
            {
                ["shape"] = "box"
            }
        };

        Dictionary<string, string> source = g.CreateNode("allentries");
        source[OperationKey] = AllEntriesOp;

        g.CreateNode("result")[OperationKey] = ResultOp;

        List<QueryInspectionNode> postFilters = [];
        foreach (QueryInspectionNode child in postFilter.Children ?? [])
        {
            if (child is { IsPostFilter: true })
                postFilters.Add(child);
        }

        BuildPostFilterChain(g, "allentries", postFilters, wrappers);
        return g.Render(StyleNode, StyleEdge);
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
        if (p.TryGetValue("Dispatch", out string dispatch) && string.IsNullOrEmpty(dispatch) == false)
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

        AddIf(p, parts, "Boost", "boost x");
        AddIf(p, parts, "EstimatedRows", "~");
        AddIf(p, parts, "DestSlot", "→slot ");
        AddIf(p, parts, "Count", "count=");
        AddIf(p, parts, "SwitchedAfterClauses", "after=");
        AddIf(p, parts, "EntriesScanned", "scanned=");
        AddIf(p, parts, "EntriesPassed", "passed=");
        if (p.TryGetValue("Ms", out string ms) && string.IsNullOrEmpty(ms) == false)
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
        if (p.TryGetValue("Dispatch", out string dispatch) && string.IsNullOrEmpty(dispatch) == false)
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

    /// <summary>
    ///     Builds the label for a per-entry post-filter node. The underlying match (stashed as MatchOperation)
    ///     decides the facts shown: a spatial match surfaces its relation (Within / Intersects), field, and tested
    ///     shape; a vector match surfaces its field. The "[And]" / "Multi" variant names flow through verbatim so
    ///     the heading reflects exactly which match ran.
    /// </summary>
    private static string PostFilterLabel(Dictionary<string, string> p)
    {
        string match = p.GetValueOrDefault("MatchOperation", PostFilterOp);
        List<string> parts;
        if (match.Contains("Spatial"))
        {
            p.TryGetValue("SpatialRelation", out string relation);
            parts = new() { string.IsNullOrEmpty(relation) ? match : match + " [" + relation + "]" };
            AddIf(p, parts, "Field", "");
            AddIf(p, parts, "Shape", "");
        }
        else
        {
            parts = new() { match };
            AddIf(p, parts, "FieldName", "");
        }

        for (int i = 0; i < parts.Count; i++)
        {
            parts[i] = GraphvizGraph.Escape(parts[i]);
        }

        return string.Join("\\n", parts);
    }

    /// <summary>
    ///     Builds the label for a sort wrapper node, naming the exact strategy and the sort key(s). A single-field
    ///     <c>SortingMatch</c> renders the materialize-then-sort heap with its field / direction / compare type
    ///     (score and spatial-distance sorts are called out explicitly); a <c>SortingMultiMatch</c> lists every
    ///     ORDER BY field in priority order. (The streaming sorted-scan strategy is NOT a wrapper — it shows up as
    ///     the DirectScan producer node, since the scan itself yields entries already in order.)
    /// </summary>
    private static string SortLabel(Dictionary<string, string> p)
    {
        string match = p.GetValueOrDefault("MatchOperation", SortOp);
        List<string> parts;

        if (match == "SortingMultiMatch")
        {
            parts = new() { match + " [multi-field heap sort]" };
            for (int i = 0; p.ContainsKey("Comparer" + i + "_FieldName"); i++)
            {
                string prefix = "Comparer" + i + "_";
                parts.Add(SortKeyDescription(
                    p.GetValueOrDefault(prefix + "FieldName"),
                    p.GetValueOrDefault(prefix + "Ascending"),
                    p.GetValueOrDefault(prefix + "FieldType")));
            }
        }
        else
        {
            // Single-field SortingMatch.
            p.TryGetValue("FieldType", out string fieldType);
            if (fieldType == "Score")
            {
                bool boosting = p.GetValueOrDefault("IsBoosting") == "True";
                parts = new() { match + " [heap sort]", "rank by score()" + (boosting ? " (boosting)" : "") };
            }
            else if (fieldType == "Spatial")
            {
                parts = new() { match + " [heap sort]", "by distance" };
                AddIf(p, parts, "Point", "from ");
                AddIf(p, parts, "Round", "round ");
                AddIf(p, parts, "Units", "", "");
                parts.Add(SortDirection(p.GetValueOrDefault("Ascending")));
            }
            else
            {
                parts = new()
                {
                    match + " [heap sort]",
                    SortKeyDescription(p.GetValueOrDefault("FieldName"), p.GetValueOrDefault("Ascending"), fieldType)
                };
            }
        }

        // Runtime sort telemetry (set by SortingMatch.Inspect): the actual strategy chosen, how many
        // sort-index entries were streamed (streaming strategy only), and the wall-clock sort time. The
        // sort runs outside the compiled bitmap pipeline, so this is the only place these surface.
        AddIf(p, parts, "Strategy", "via ");
        AddIf(p, parts, "EntriesStreamed", "streamed=");
        AddIf(p, parts, "Candidates", "candidates=");
        if (p.TryGetValue("Ms", out string ms) && string.IsNullOrEmpty(ms) == false)
            parts.Add(ms + " ms");

        for (int i = 0; i < parts.Count; i++)
            parts[i] = GraphvizGraph.Escape(parts[i]);
        return string.Join("\\n", parts);
    }

    private static string SortKeyDescription(string field, string ascending, string fieldType)
    {
        string dir = SortDirection(ascending);
        string type = string.IsNullOrEmpty(fieldType) ? "" : " (" + fieldType + ")";
        return (field ?? "") + " " + dir + type;
    }

    private static string SortDirection(string ascending) => ascending == "False" ? "DESC" : "ASC";

    /// <summary>Builds the label for a boost wrapper node, surfacing the boost factor applied to the inner scores.</summary>
    private static string BoostLabel(Dictionary<string, string> p)
    {
        List<string> parts = new() { p.GetValueOrDefault("MatchOperation", BoostOp) };
        AddIf(p, parts, "BoostFactor", "factor x");
        for (int i = 0; i < parts.Count; i++)
            parts[i] = GraphvizGraph.Escape(parts[i]);
        return string.Join("\\n", parts);
    }

    private static void AddIf(Dictionary<string, string> p, List<string> into, string key, string prefix = "", string suffix = "")
    {
        if (p.TryGetValue(key, out string val) && string.IsNullOrEmpty(val) == false)
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
