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

        sb.AppendLine("digraph QueryPlan {");
        sb.AppendLine("  rankdir=TB;");
        sb.AppendLine("  node [shape=box, fontname=\"monospace\"];");

        // Node declarations.
        for (int i = 0; i < ops.Count; i++)
            sb.Append("  op").Append(i).Append(" [label=\"").Append(DotLabel(ops[i])).AppendLine("\"];");
        sb.AppendLine("  result [shape=ellipse, label=\"Result\"];");

        // Edges by slot dataflow.
        var lastWriter = new Dictionary<int, int>();
        // Real dataflow edges (srcOp -> destOp), used below to decide where INVISIBLE sequencing edges are
        // needed. The op stream executes top to bottom, but slot dataflow alone makes independent branches —
        // e.g. an OR of two AND-groups built in separate slots — look like they run in parallel. An invisible
        // edge between consecutive ops that have no real edge pins the visual order back to true execution
        // order without implying a (non-existent) data dependency.
        var realEdges = new HashSet<(int From, int To)>();
        var gateOpIds = new List<int>();
        int entryScanTailId = -1;
        for (int i = 0; i < ops.Count; i++)
        {
            QueryInspectionNode op = ops[i];

            // The EntryScan tail is the shared branch TARGET, not part of the linear slot dataflow. Wire it after
            // the loop (dashed edges from every gate, and its slot-1 survivors to the result).
            if (op.Operation == "EntryScan")
            {
                entryScanTailId = i;
                continue;
            }

            // EntryScanCheck is a read-only cost GATE on the slot-0 accumulator: it reads slot 0 and may divert to
            // the entry-scan tail, but writes NO slot. Draw a dashed gate edge from the current slot-0 writer and
            // record it; crucially do NOT make it a writer — that would chain the next op through a phantom slot.
            if (op.Operation == "EntryScanCheck")
            {
                if (lastWriter.TryGetValue(0, out int gateSrc))
                    sb.Append("  op").Append(gateSrc).Append(" -> op").Append(i)
                      .AppendLine(" [style=dashed, label=\"gate slot 0\"];");
                gateOpIds.Add(i);
                continue;
            }

            int dest = ParseSlot(op, "DestSlot");
            bool isFill = op.Operation is "Fill" or "Fill-AllEntries";

            // A combining op reads the running accumulator already in its destination slot.
            if (isFill == false && lastWriter.TryGetValue(dest, out int destWriter))
            {
                sb.Append("  op").Append(destWriter).Append(" -> op").Append(i)
                  .Append(" [label=\"slot ").Append(dest).AppendLine("\"];");
                realEdges.Add((destWriter, i));
            }

            // A slot-to-slot merge also reads its source slot.
            if (op.Parameters.ContainsKey("SourceSlot"))
            {
                int src = ParseSlot(op, "SourceSlot");
                if (lastWriter.TryGetValue(src, out int srcWriter))
                {
                    sb.Append("  op").Append(srcWriter).Append(" -> op").Append(i)
                      .Append(" [label=\"slot ").Append(src).AppendLine("\"];");
                    realEdges.Add((srcWriter, i));
                }
            }

            lastWriter[dest] = i;
        }

        // The entry scan, when present, has two possible runtime exits. Read its Taken flag once so both the
        // result wiring and the node styling below can show WHICH direction this run took.
        bool entryScanTaken = entryScanTailId >= 0
            && ops[entryScanTailId].Parameters != null
            && ops[entryScanTailId].Parameters.TryGetValue("Taken", out string takenVal)
            && takenVal == "True";

        // Result edge: the bitmap pipeline leaves its answer in slot 0 — UNLESS the entry scan fired this run, in
        // which case the survivors live in slot 1 and the bitmap pipeline's slot-0 result was thrown away. Draw the
        // unused exit greyed out so the taken direction is unambiguous from the graph alone.
        if (lastWriter.TryGetValue(0, out int finalWriter))
        {
            if (entryScanTaken)
                sb.Append("  op").Append(finalWriter).AppendLine(" -> result [style=dotted, color=grey, label=\"(not taken)\"];");
            else
                sb.Append("  op").Append(finalWriter).AppendLine(" -> result;");
        }

        // Entry-scan branch: every gate that fires diverts the slot-0 accumulator into the single scan tail, whose
        // slot-1 survivors become the answer instead. The taken path is drawn solid+bold; an untaken one stays
        // dashed/grey so the reader sees at a glance whether the cost gate switched off the bitmap pipeline.
        if (entryScanTailId >= 0)
        {
            string gateStyle = entryScanTaken ? "style=bold, color=\"#1a7f37\"" : "style=dashed, color=grey";
            foreach (int gate in gateOpIds)
                sb.Append("  op").Append(gate).Append(" -> op").Append(entryScanTailId)
                  .Append(" [").Append(gateStyle).AppendLine(", label=\"on switch\"];");

            string resultLabel = entryScanTaken ? "entry-scan TAKEN" : "if entry-scan taken";
            string resultStyle = entryScanTaken ? "style=bold, color=\"#1a7f37\"" : "style=dashed, color=grey";
            sb.Append("  op").Append(entryScanTailId).Append(" -> result [").Append(resultStyle)
              .Append(", label=\"").Append(resultLabel).AppendLine("\"];");
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

            // Residual-predicate summary for the entry-scan tail: the per-entry checks the scan applies once the
            // cost gate switches off the bitmap pipeline. These live on the EntryScan node's Residual children
            // (which carry no DestSlot and so are not rendered as their own graph nodes), so fold them onto the
            // tail's label — otherwise the most interesting part of an entry-scan plan would be invisible.
            string residuals = SummarizeResiduals(op);
            if (residuals != null)
                parts.Add("residuals: " + residuals);

            // Per-stage wall-clock. OverlayTimings records the elapsed time of each op as the "Ms" parameter
            // (only when the query asked for include timings()), so it is present per node exactly when timing
            // telemetry exists. Render it last so the cost of each stage sits at the bottom of its box.
            if (op.Parameters.TryGetValue("Ms", out string ms) && string.IsNullOrEmpty(ms) == false)
                parts.Add(ms + " ms");
        }

        // Escape for a Graphviz double-quoted label, then join lines with the literal \n that DOT renders as a break.
        for (int i = 0; i < parts.Count; i++)
            parts[i] = parts[i].Replace("\\", "\\\\").Replace("\"", "\\\"");
        return string.Join("\\n", parts);

        // prefix == "" => render as "Key=value"; otherwise render as "prefix" + value (e.g. "~1,234", "count=99").
        static void AddIf(QueryInspectionNode n, List<string> into, string key, string prefix = "")
        {
            if (n.Parameters.TryGetValue(key, out string val) && string.IsNullOrEmpty(val) == false)
                into.Add(prefix.Length == 0 ? key + "=" + val : prefix + val);
        }
    }

    /// <summary>Flattens the residual-predicate children of an entry-scan tail node into a compact "Field Compare"
    /// list (a leading "!" marks a negated check, and an AND/OR group is wrapped in parentheses with the matching
    /// joiner). Returns null when the node carries no residual children.</summary>
    private static string SummarizeResiduals(QueryInspectionNode op)
    {
        if (op.Children == null || op.Children.Count == 0)
            return null;

        var parts = new List<string>();
        foreach (QueryInspectionNode child in op.Children)
        {
            string token = ResidualToken(child);
            if (token != null)
                parts.Add(token);
        }

        return parts.Count == 0 ? null : string.Join(", ", parts);

        static string ResidualToken(QueryInspectionNode node)
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
}
