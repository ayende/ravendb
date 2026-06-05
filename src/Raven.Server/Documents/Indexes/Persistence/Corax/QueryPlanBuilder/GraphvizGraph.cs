using System;
using System.Collections.Generic;
using System.Text;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

/// <summary>
/// A small, render-agnostic Graphviz (DOT) document model. An ordered set of nodes and edges, each carrying two
/// property bags:
/// <list type="bullet">
/// <item><b>Data</b> — machine-readable facts about the element (dispatch, slot, cardinality, taken-state, …),
/// emitted as <c>data_&lt;key&gt;</c> attributes. Filled while BUILDING the graph.</item>
/// <item><b>Attributes</b> — presentation only (label, shape, style, color), emitted as raw DOT attributes. Filled
/// by the styler callbacks at RENDER time, derived from each element's Data.</item>
/// </list>
/// This keeps the three concerns apart: building the model (the facts), styling it (the presentation), and
/// serialising it (the DOT text). Callers fill Data via <see cref="CreateNode"/> / <see cref="CreateEdge"/> while
/// walking their domain structure, then pass per-element styler callbacks to <see cref="Render"/>, which derives
/// the Attributes and writes the DOT.
/// </summary>
internal sealed class GraphvizGraph
{
    internal abstract class Element
    {
        /// <summary>Machine-readable facts; serialised as <c>data_&lt;key&gt;</c> attributes (key lower-cased).</summary>
        public readonly Dictionary<string, string> Data = new();

        /// <summary>Presentation attributes (label/shape/style/color/…); serialised as raw DOT attributes. The value
        /// is written verbatim, so a styler that puts a multi-line label here is responsible for its own escaping
        /// and for the literal <c>\n</c> line breaks DOT expects.</summary>
        public readonly Dictionary<string, string> Attributes = new();
    }

    internal sealed class Node : Element
    {
        public readonly string Id;
        internal Node(string id) => Id = id;
    }

    internal sealed class Edge : Element
    {
        public readonly string From;
        public readonly string To;

        internal Edge(string from, string to)
        {
            From = from;
            To = to;
        }
    }

    private readonly List<Node> _nodes = new();
    private readonly List<Edge> _edges = new();

    public string RankDir = "TB";

    /// <summary>Default attributes applied to every node via a DOT <c>node [...]</c> statement.</summary>
    public readonly Dictionary<string, string> NodeDefaults = new();

    public IReadOnlyList<Node> Nodes => _nodes;
    public IReadOnlyList<Edge> Edges => _edges;

    /// <summary>Create a node and return its Data bag for the caller to fill. Order is preserved for rendering.</summary>
    public Dictionary<string, string> CreateNode(string id)
    {
        var node = new Node(id);
        _nodes.Add(node);
        return node.Data;
    }

    /// <summary>Create an edge and return its Data bag for the caller to fill. Order is preserved for rendering.</summary>
    public Dictionary<string, string> CreateEdge(string from, string to)
    {
        var edge = new Edge(from, to);
        _edges.Add(edge);
        return edge.Data;
    }

    /// <summary>Serialise to DOT text. The optional <paramref name="styleNode"/> / <paramref name="styleEdge"/>
    /// callbacks run once per element immediately before it is written, deriving its presentation Attributes from
    /// its Data — so all styling decisions (colours, labels, shapes) live at render time, not in the model or the
    /// build. For each element the presentation Attributes are written first (raw), then every non-empty Data entry
    /// as a <c>data_&lt;key&gt;</c> attribute (escaped).</summary>
    public string Render(Action<Node> styleNode = null, Action<Edge> styleEdge = null)
    {
        var sb = new StringBuilder();
        sb.Append("digraph QueryPlan {\n");
        sb.Append("  rankdir=").Append(RankDir).Append(";\n");

        if (NodeDefaults.Count > 0)
        {
            sb.Append("  node [");
            bool firstDefault = true;
            foreach (KeyValuePair<string, string> kv in NodeDefaults)
                firstDefault = AppendRawAttr(sb, firstDefault, kv.Key, kv.Value);
            sb.Append("];\n");
        }

        foreach (Node node in _nodes)
        {
            styleNode?.Invoke(node);
            sb.Append("  ").Append(node.Id).Append(" [");
            AppendElement(sb, node);
            sb.Append("];\n");
        }

        foreach (Edge edge in _edges)
        {
            styleEdge?.Invoke(edge);
            sb.Append("  ").Append(edge.From).Append(" -> ").Append(edge.To).Append(" [");
            AppendElement(sb, edge);
            sb.Append("];\n");
        }

        sb.Append("}\n");
        return sb.ToString();
    }

    private static void AppendElement(StringBuilder sb, Element e)
    {
        bool first = true;

        // Presentation first, written verbatim — the styler owns escaping (labels carry intentional \n breaks).
        foreach (KeyValuePair<string, string> kv in e.Attributes)
            first = AppendRawAttr(sb, first, kv.Key, kv.Value);

        // Then the data facts, as data_<key>, escaped so a value can never break out of its quotes.
        foreach (KeyValuePair<string, string> kv in e.Data)
        {
            if (string.IsNullOrEmpty(kv.Value))
                continue;
            first = AppendRawAttr(sb, first, "data_" + kv.Key.ToLowerInvariant(), EscapeAttr(kv.Value));
        }
    }

    private static bool AppendRawAttr(StringBuilder sb, bool first, string key, string value)
    {
        if (first == false)
            sb.Append(", ");
        sb.Append(key).Append("=\"").Append(value).Append('"');
        return false;
    }

    /// <summary>Escape a string for use inside a Graphviz double-quoted label or attribute value.</summary>
    public static string Escape(string s) => s.Replace("\\", "\\\\").Replace("\"", "\\\"");

    /// <summary>Escape for a DOT attribute value: like <see cref="Escape"/>, plus collapse newlines to spaces so a
    /// multi-line value (e.g. a residual description) cannot break the attribute out of its quotes.</summary>
    private static string EscapeAttr(string s) => Escape(s).Replace("\r", " ").Replace("\n", " ");
}
