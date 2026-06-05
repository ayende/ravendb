using System.Collections.Generic;
using System.Text;
using Corax.Querying.Matches.Meta;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

/// <summary>
/// Renders a compiled-query <see cref="QueryInspectionNode"/> plan as a Graphviz (DOT) dataflow graph, server-side.
/// The op stream is linear, but the bitmap SLOTS turn it into a graph: every op writes its <c>DestSlot</c>;
/// a non-Fill op that targets a slot consumes whatever last wrote that slot (the running accumulator); the
/// slot-to-slot merges (AND/ANDNOT/OR-Bitmaps) additionally consume their <c>SourceSlot</c>; and the
/// EntryScanCheck branches slot 0 into the entry-scan tail (slot 1). Walking the ops while tracking the last
/// writer per slot reconstructs the edges. This runs on the server where the full inspection data lives, so
/// the node labels carry everything the server knows (dispatch, dest slot, cardinality, per-stage timing,
/// entry-scan scan/pass counts, and the residual-predicate set); the resulting DOT is attached to the plan
/// and shipped to the client, so the catalog and any other consumer share one Graphviz implementation.
/// </summary>
internal static class QueryPlanGraph
{
    /// <summary>Render <paramref name="plan"/> (or the CompiledQuery node within it) as Graphviz DOT text.</summary>
    public static string ToGraphviz(QueryInspectionNode plan)
    {
        var sb = new StringBuilder();
        ToGraphviz(plan, sb);
        return sb.ToString();
    }

    /// <summary>Append the Graphviz DOT rendering of <paramref name="plan"/> to <paramref name="sb"/>.</summary>
    public static void ToGraphviz(QueryInspectionNode plan, StringBuilder sb)
    {
        QueryInspectionNode compiled = FindNode(plan, "CompiledQuery");
        if (compiled?.Children == null)
        {
            sb.AppendLine("digraph QueryPlan { /* no compiled op stream */ }");
            return;
        }

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
            if (child.Operation == "DirectScan")
            {
                directScanNode = child;
                break;
            }
        }

        sb.AppendLine("digraph QueryPlan {");
        sb.AppendLine("  rankdir=TB;");
        sb.AppendLine("  node [shape=box, fontname=\"monospace\"];");

        // Node declarations. Every node carries its inspection parameters twice: folded into the human-readable
        // `label`, AND as machine-readable `data_<key>` attributes so any downstream tool (anything parsing the DOT,
        // or a `dot -Tjson` export) can read the structured plan without scraping label text.
        for (int i = 0; i < ops.Count; i++)
        {
            sb.Append("  op").Append(i).Append(" [label=\"").Append(DotLabel(ops[i])).Append('"');
            AppendDataAttributes(sb, ops[i]);
            sb.AppendLine("];");
        }

        if (directScanNode != null)
        {
            sb.Append("  directscan [shape=box, style=bold, color=\"#1a7f37\", label=\"").Append(DirectScanLabel(directScanNode)).Append('"');
            AppendDataAttributes(sb, directScanNode);
            sb.AppendLine("];");
        }

        sb.AppendLine("  result [shape=ellipse, label=\"Result\"];");

        // --- Runtime taken-path analysis ------------------------------------------------------------------------
        // Pre-scan the op list to locate the entry-scan tail and the cost gates BEFORE drawing any edge, so every
        // edge can be coloured by whether THIS run actually traversed it. The runtime facts come from OverlayTimings
        // (present only when the query asked for include timings()): the tail's Taken flag, and SwitchedAfterClauses
        // = how many leaf clauses had merged into slot 0 when the scan switched on. The j-th gate (1-indexed in op
        // order) is the one that fires when SwitchedAfterClauses == j, so the fired gate is gateOpIds[switchedAfter-1].
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

        // Did the runtime overlay run at all? Without it we cannot know the taken path, so edges fall back to a plain
        // (uncoloured) style rather than falsely claiming a route. With an entry-scan gate the Taken flag is the
        // signal; otherwise any op carrying a runtime Count proves the overlay ran.
        bool hasRuntime = entryScanTailId >= 0
            ? ops[entryScanTailId].Parameters != null && ops[entryScanTailId].Parameters.ContainsKey("Taken")
            : AnyHasCount(ops);

        // An op executed this run iff the run did not switch to the entry scan before reaching it: when the scan
        // fired at firedGateOp, only ops BEFORE that gate built the slot-0 accumulator; the post-switch merges were
        // skipped. When the scan did not fire, every op ran.
        bool OpExecuted(int opIndex) => entryScanTaken == false || firedGateOp < 0 || opIndex < firedGateOp;

        // Style prefix for a real dataflow edge whose CONSUMER is op `to`: green+bold = traversed this run; dotted
        // grey = skipped (the entry scan replaced these merges); empty = no runtime info, so make no claim.
        string DataEdgeStyle(int to)
            => hasRuntime == false ? "" : OpExecuted(to) ? "style=bold, color=\"#1a7f37\", " : "style=dotted, color=grey, ";

        // Edges by slot dataflow.
        var lastWriter = new Dictionary<int, int>();
        // Real dataflow edges (srcOp -> destOp), used below to decide where INVISIBLE sequencing edges are
        // needed. The op stream executes top to bottom, but slot dataflow alone makes independent branches —
        // e.g. an OR of two AND-groups built in separate slots — look like they run in parallel. An invisible
        // edge between consecutive ops that have no real edge pins the visual order back to true execution
        // order without implying a (non-existent) data dependency.
        var realEdges = new HashSet<(int From, int To)>();
        for (int i = 0; i < ops.Count; i++)
        {
            QueryInspectionNode op = ops[i];

            // The EntryScan tail is the shared branch TARGET, not part of the linear slot dataflow. Wire it after
            // the loop (gate edges from every check, and its slot-1 survivors to the result).
            if (op.Operation == "EntryScan")
                continue;

            // EntryScanCheck is a read-only cost GATE on the slot-0 accumulator: it reads slot 0 and may divert to
            // the entry-scan tail, but writes NO slot. Draw a dashed gate edge from the current slot-0 writer;
            // crucially do NOT make it a writer — that would chain the next op through a phantom slot.
            if (op.Operation == "EntryScanCheck")
            {
                if (lastWriter.TryGetValue(0, out int gateSrc))
                    sb.Append("  op").Append(gateSrc).Append(" -> op").Append(i)
                      .AppendLine(" [style=dashed, label=\"gate slot 0\"];");
                continue;
            }

            int dest = ParseSlot(op, "DestSlot");
            bool isFill = op.Operation is "Fill" or "Fill-AllEntries";

            // A combining op reads the running accumulator already in its destination slot.
            if (isFill == false && lastWriter.TryGetValue(dest, out int destWriter))
            {
                sb.Append("  op").Append(destWriter).Append(" -> op").Append(i)
                  .Append(" [").Append(DataEdgeStyle(i)).Append("label=\"slot ").Append(dest).AppendLine("\"];");
                realEdges.Add((destWriter, i));
            }

            // A slot-to-slot merge also reads its source slot.
            if (op.Parameters.ContainsKey("SourceSlot"))
            {
                int src = ParseSlot(op, "SourceSlot");
                if (lastWriter.TryGetValue(src, out int srcWriter))
                {
                    sb.Append("  op").Append(srcWriter).Append(" -> op").Append(i)
                      .Append(" [").Append(DataEdgeStyle(i)).Append("label=\"slot ").Append(src).AppendLine("\"];");
                    realEdges.Add((srcWriter, i));
                }
            }

            lastWriter[dest] = i;
        }

        // Result edge: the bitmap pipeline leaves its answer in slot 0 — UNLESS the entry scan fired this run, in
        // which case the survivors live in slot 1 and the bitmap pipeline's slot-0 result was thrown away. The
        // exit the run actually took is drawn green+bold; an exit known NOT to be taken is dotted grey; with no
        // runtime data it stays plain.
        if (lastWriter.TryGetValue(0, out int finalWriter))
        {
            if (directScanNode != null)
                sb.Append("  op").Append(finalWriter).AppendLine(" -> result [style=dotted, color=grey, label=\"(bitmap candidate, not executed)\"];");
            else if (entryScanTaken)
                sb.Append("  op").Append(finalWriter).AppendLine(" -> result [style=dotted, color=grey, label=\"(not taken)\"];");
            else if (hasRuntime)
                sb.Append("  op").Append(finalWriter).AppendLine(" -> result [style=bold, color=\"#1a7f37\"];");
            else
                sb.Append("  op").Append(finalWriter).AppendLine(" -> result;");
        }

        // When a tree-scan strategy actually ran, the DirectScan node — not the bitmap pipeline — is the real
        // producer of the answer. Draw its result edge solid+bold (mirroring the entry-scan TAKEN styling).
        if (directScanNode != null)
            sb.AppendLine("  directscan -> result [style=bold, color=\"#1a7f37\", label=\"scan result\"];");

        // Entry-scan branch: a gate that fires diverts the slot-0 accumulator into the single scan tail, whose
        // slot-1 survivors become the answer. Only the gate that actually fired this run (firedGateOp) is drawn
        // green+bold; the other checks — whether they ran and declined to switch, or were never reached — stay
        // dashed grey, so exactly one "on switch" edge lights up as the route the run took.
        if (entryScanTailId >= 0)
        {
            foreach (int gate in gateOpIds)
            {
                bool isFired = entryScanTaken && gate == firedGateOp;
                string gateStyle = isFired ? "style=bold, color=\"#1a7f37\"" : "style=dashed, color=grey";
                string gateLabel = isFired ? "switched here" : "candidate switch";
                sb.Append("  op").Append(gate).Append(" -> op").Append(entryScanTailId)
                  .Append(" [").Append(gateStyle).Append(", label=\"").Append(gateLabel).AppendLine("\"];");
            }

            string resultLabel = entryScanTaken ? "entry-scan TAKEN" : "if entry-scan taken";
            string resultStyle = entryScanTaken ? "style=bold, color=\"#1a7f37\"" : "style=dashed, color=grey";
            sb.Append("  op").Append(entryScanTailId).Append(" -> result [").Append(resultStyle)
              .Append(", label=\"").Append(resultLabel).AppendLine("\"];");

            // Residual predicates: the per-entry checks the scan applies to each slot-0 survivor once a gate switched
            // the pipeline off. They live on the tail's Residual children; draw each as its own node hanging off the
            // tail so the executed path runs visibly THROUGH them. Bold green when the scan fired this run (every
            // survivor is tested against the whole set), dotted grey otherwise — the bitmap pipeline ignores them.
            if (ops[entryScanTailId].Children != null)
            {
                string resEdgeStyle = entryScanTaken ? "style=bold, color=\"#1a7f37\"" : "style=dotted, color=grey";
                string resNodeColor = entryScanTaken ? "color=\"#1a7f37\"" : "color=grey";
                int r = 0;
                foreach (QueryInspectionNode child in ops[entryScanTailId].Children)
                {
                    string token = ResidualToken(child);
                    if (token == null)
                        continue;
                    sb.Append("  res").Append(r).Append(" [shape=note, ").Append(resNodeColor)
                      .Append(", label=\"").Append(Escape(token)).AppendLine("\"];");
                    sb.Append("  op").Append(entryScanTailId).Append(" -> res").Append(r)
                      .Append(" [").Append(resEdgeStyle).AppendLine(", label=\"per entry\"];");
                    r++;
                }
            }
        }

        // Invisible sequencing edges: pin parallel-looking branches to true execution order (see realEdges
        // above). For each consecutive op pair with no real dataflow edge, an invisible edge forces the
        // second to rank below the first. Entry-scan nodes are skipped — their dashed branch edges already
        // express the (conditional) ordering, and chaining through them would imply a false data dependency.
        for (int i = 0; i + 1 < ops.Count; i++)
        {
            if (ops[i].Operation is "EntryScan" or "EntryScanCheck")
                continue;
            if (ops[i + 1].Operation is "EntryScan" or "EntryScanCheck")
                continue;
            if (realEdges.Contains((i, i + 1)))
                continue;
            sb.Append("  op").Append(i).Append(" -> op").Append(i + 1).AppendLine(" [style=invis];");
        }

        sb.AppendLine("}");
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

    private static string DotLabel(QueryInspectionNode op)
    {
        var parts = new List<string> { op.Operation };
        if (op.Parameters != null)
        {
            // Dispatch (Term / MultiTerm / Match) — how this leaf reaches its postings. The slot-algebra and
            // control-flow ops have no dispatch and so render none.
            if (op.Parameters.TryGetValue("Dispatch", out string dispatch) && string.IsNullOrEmpty(dispatch) == false)
                parts.Add("[" + dispatch + "]");

            // Data-bearing attributes.
            AddIf(op, parts, "FieldName");
            AddIf(op, parts, "ClauseType");
            AddIf(op, parts, "Term");
            AddIf(op, parts, "Term2");
            AddIf(op, parts, "Terms");
            if (op.Parameters.TryGetValue("Negated", out string neg) && neg == "true")
                parts.Add("NEGATED");
            AddIf(op, parts, "EstimatedRows", "~");

            // Physical destination slot, on the node as well as on the edges — the node owns the slot it writes,
            // the edges show which slot each input came from.
            AddIf(op, parts, "DestSlot", "→slot ");

            AddIf(op, parts, "Count", "count=");
            AddIf(op, parts, "Taken", "taken=");
            AddIf(op, parts, "SwitchedAfterClauses", "after=");
            AddIf(op, parts, "EntriesScanned", "scanned=");
            AddIf(op, parts, "EntriesPassed", "passed=");

            // Per-stage wall-clock. OverlayTimings records the elapsed time of each op as the "Ms" parameter
            // (only when the query asked for include timings()), so it is present per node exactly when timing
            // telemetry exists. Render it last so the cost of each stage sits at the bottom of its box.
            if (op.Parameters.TryGetValue("Ms", out string ms) && string.IsNullOrEmpty(ms) == false)
                parts.Add(ms + " ms");
        }

        // Escape for a Graphviz double-quoted label, then join lines with the literal \n that DOT renders as a break.
        for (int i = 0; i < parts.Count; i++)
            parts[i] = Escape(parts[i]);
        return string.Join("\\n", parts);
    }

    // prefix == "" => render as "Key=value"; otherwise render as "prefix" + value + suffix (e.g. "~1,234", "tree=2.3 ms").
    private static void AddIf(QueryInspectionNode n, List<string> into, string key, string prefix = "", string suffix = "")
    {
        if (n.Parameters != null && n.Parameters.TryGetValue(key, out string val) && string.IsNullOrEmpty(val) == false)
            into.Add(prefix.Length == 0 ? key + "=" + val : prefix + val + suffix);
    }

    /// <summary>Escape a string for use inside a Graphviz double-quoted label or attribute value.</summary>
    private static string Escape(string s) => s.Replace("\\", "\\\\").Replace("\"", "\\\"");

    /// <summary>Escape for a DOT attribute value: like <see cref="Escape"/>, plus collapse newlines to spaces so a
    /// multi-line value (e.g. a residual description) cannot break the attribute out of its quotes.</summary>
    private static string EscapeAttr(string s) => Escape(s).Replace("\r", " ").Replace("\n", " ");

    /// <summary>Append every inspection parameter as a machine-readable <c>data_&lt;lowercased key&gt;</c> DOT node
    /// attribute. These ride alongside the human <c>label</c> so a consumer that parses the DOT (or a
    /// <c>dot -Tjson</c> export) gets the full structured plan per node without scraping label text. The big
    /// source/graph blobs only live on the root node (never on a rendered op/scan node), but they are skipped
    /// defensively so a stray one can never bloat the emitted DOT.</summary>
    private static void AppendDataAttributes(StringBuilder sb, QueryInspectionNode op)
    {
        if (op.Parameters == null)
            return;
        foreach (KeyValuePair<string, string> kv in op.Parameters)
        {
            if (kv.Key is "CSharpSource" or "CSharpSourceFormatted" or "PlanGraphDot")
                continue;
            if (string.IsNullOrEmpty(kv.Value))
                continue;
            // Param keys are ASCII identifiers (DestSlot, EntriesScanned, TreeScan_ms, …), so a plain lower-case is a
            // valid DOT attribute name; no further sanitisation is needed.
            sb.Append(", data_").Append(kv.Key.ToLowerInvariant()).Append("=\"").Append(EscapeAttr(kv.Value)).Append('"');
        }
    }

    /// <summary>Builds the readable label for the executed-tree-scan node from a <see cref="DirectScanMatchBase"/>
    /// inspection: the driving tree / clause / seek bound / direction, the per-entry residual predicates, the
    /// scan counts, and the per-phase timing. All of this is also emitted as data attributes (see
    /// <see cref="AppendDataAttributes"/>); the label is just the human-facing subset.</summary>
    private static string DirectScanLabel(QueryInspectionNode node)
    {
        var parts = new List<string> { node.Operation };
        AddIf(node, parts, "DrivingTree", "tree=");
        AddIf(node, parts, "DrivingClause", "drive=");
        AddIf(node, parts, "SeekBound", "seek=");
        AddIf(node, parts, "TreeDirection", "dir=");
        AddIf(node, parts, "ResidualPredicates", "residuals: ");
        AddIf(node, parts, "TreeEntriesScanned", "scanned=");
        AddIf(node, parts, "EntriesPassedFilter", "passed=");
        AddIf(node, parts, "EntriesRejected", "rejected=");
        AddIf(node, parts, "StoppedAt", "stopped=");
        AddIf(node, parts, "TreeScan_ms", "tree=", " ms");
        AddIf(node, parts, "EntryScans_ms", "entry=", " ms");
        for (int i = 0; i < parts.Count; i++)
            parts[i] = Escape(parts[i]);
        return string.Join("\\n", parts);
    }

    /// <summary>True when any op carries a runtime <c>Count</c> parameter, i.e. OverlayTimings ran and the taken
    /// path is knowable. Used for plans with no entry-scan gate, where there is no Taken flag to key off.</summary>
    private static bool AnyHasCount(List<QueryInspectionNode> ops)
    {
        foreach (QueryInspectionNode op in ops)
        {
            if (op.Parameters != null && op.Parameters.ContainsKey("Count"))
                return true;
        }

        return false;
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
