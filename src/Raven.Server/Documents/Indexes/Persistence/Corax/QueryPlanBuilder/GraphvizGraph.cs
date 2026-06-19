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
/// </summary>
internal sealed class GraphvizGraph
{
    internal abstract class Element
    {
        /// <summary>Machine-readable facts; serialized as <c>data_&lt;key&gt;</c> attributes (key lower-cased).</summary>
        public readonly Dictionary<string, string> Data = [];

        /// <summary>Presentation attributes (label/shape/style/color/…); serialized as raw DOT attributes.</summary>
        public readonly Dictionary<string, string> Attributes = [];
    }

    internal sealed class Node(string id) : Element
    {
        public readonly string Id = id;
    }

    internal sealed class Edge(string from, string to) : Element
    {
        public readonly string From = from;
        public readonly string To = to;
    }

    private readonly List<Node> _nodes = [];
    private readonly List<Edge> _edges = [];

    public string RankDir = "TB";

    /// <summary>Default attributes applied to every node via a DOT <c>node [...]</c> statement.</summary>
    public readonly Dictionary<string, string> NodeDefaults = [];

    public IReadOnlyList<Node> Nodes => _nodes;
    public IReadOnlyList<Edge> Edges => _edges;

    public Dictionary<string, string> CreateNode(string id)
    {
        var node = new Node(id);
        _nodes.Add(node);
        return node.Data;
    }

    public Dictionary<string, string> CreateEdge(string from, string to)
    {
        var edge = new Edge(from, to);
        _edges.Add(edge);
        return edge.Data;
    }

    public string Render(Action<Node> styleNode = null, Action<Edge> styleEdge = null)
    {
        var sb = new StringBuilder();
        sb.AppendLine("digraph QueryPlan {");
        sb.AppendLine("  rankdir=").Append(RankDir).Append(";");

        if (NodeDefaults.Count > 0)
        {
            sb.Append("  node [");
            bool firstDefault = true;
            foreach (KeyValuePair<string, string> kv in NodeDefaults)
                firstDefault = AppendRawAttr(sb, firstDefault, kv.Key, kv.Value);
            sb.AppendLine("];");
        }

        foreach (Node node in _nodes)
        {
            styleNode?.Invoke(node);
            sb.Append("  ").Append(node.Id).Append(" [");
            AppendElement(sb, node);
            sb.AppendLine("];");
        }

        foreach (Edge edge in _edges)
        {
            styleEdge?.Invoke(edge);
            sb.Append("  ").Append(edge.From).Append(" -> ").Append(edge.To).Append(" [");
            AppendElement(sb, edge);
            sb.AppendLine("];");
        }

        sb.AppendLine("}");
        return sb.ToString();
    }

    private void AppendElement(StringBuilder sb, Element e)
    {
        bool first = true;
        foreach (KeyValuePair<string, string> kv in e.Attributes)
            first = AppendRawAttr(sb, first, kv.Key, kv.Value);

        foreach (KeyValuePair<string, string> kv in e.Data)
        {
            if (string.IsNullOrEmpty(kv.Value))
                continue;
            first = AppendRawAttr(sb, first, "data_" + SanitizeKey(kv.Key), EscapeAttr(kv.Value));
        }

        // Surface every data_* fact as a single-line hover tooltip (original key names, not the sanitized
        // attribute keys), so the full machine-readable detail is reachable on hover without bloating the
        // visible label. A styler that set its own tooltip wins.
        if (e.Attributes.ContainsKey("tooltip") == false)
        {
            string tooltip = BuildDataTooltip(e.Data);
            if (string.IsNullOrEmpty(tooltip) == false)
                AppendRawAttr(sb, first, "tooltip", EscapeAttr(tooltip));
        }
    }

    /// <summary>Join all non-empty data facts into one line as <c>Key: Value  |  Key: Value</c>, for use as a
    /// Graphviz tooltip. Uses the original (un-sanitized) keys for readability.</summary>
    private static string BuildDataTooltip(Dictionary<string, string> data)
    {
        StringBuilder tip = null;
        foreach (KeyValuePair<string, string> kv in data)
        {
            if (string.IsNullOrEmpty(kv.Value))
                continue;
            tip ??= new StringBuilder();
            if (tip.Length != 0)
                tip.Append("  |  ");
            tip.Append(kv.Key).Append(": ").Append(kv.Value);
        }

        return tip?.ToString();
    }

    private readonly Dictionary<string, string> _sanitizedKeyCache = [];

    private string SanitizeKey(string key) // we may be called with "Number Of Candidates", for example, needs to fix this
    {
        if (string.IsNullOrEmpty(key)) 
            return string.Empty;
        
        if (_sanitizedKeyCache.TryGetValue(key, out string cachedValue))
            return cachedValue;

        string sanitized = string.Create(key.Length, key, static (span, state) =>
        {
            for (int i = 0; i < state.Length; i++)
            {
                char c = state[i];
                span[i] = char.IsLetterOrDigit(c) ? char.ToLowerInvariant(c) : '_';
            }
        });

        _sanitizedKeyCache[key] = sanitized;
        return sanitized;
    }

    private static bool AppendRawAttr(StringBuilder sb, bool first, string key, string value)
    {
        if (first == false)
            sb.Append(", ");
        sb.Append(key).Append("=\"").Append(value).Append('"');
        return false;
    }

    public static string Escape(string s) => s.Replace("\\", "\\\\").Replace("\"", "\\\"");

    private static string EscapeAttr(string s) => Escape(s).Replace("\r", " ").Replace("\n", " ");
}
