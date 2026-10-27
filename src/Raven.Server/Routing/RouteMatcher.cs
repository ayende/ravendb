using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Text;

namespace Raven.Server.Routing
{
    /// <summary>
    /// Finds the route of a request. Routes are ASCII and matched ignoring (ASCII) case, a '*' captures a single
    /// segment (until the next '/') and a route that ends with '$' matches every path that starts with it.
    /// </summary>
    /// <remarks>
    /// PERF: for the routes of the same length we build a decision tree, every node selects the candidates by the character
    /// at a single position until a single candidate remains, which we then compare to the whole path. So a lookup is a few
    /// array reads and a single comparison.
    /// </remarks>
    public sealed class RouteMatcher<T>
    {
        // Position < 0 is a leaf, its value is _values[Value] if the path is Literal (no match when Value < 0).
        // Otherwise, the next node is _children[ChildrenStart + path[Position] - Low], when Low <= path[Position] <= High.
        private readonly struct Node(int position, char low, char high, int childrenStart, int value, string literal)
        {
            public readonly int Position = position;
            public readonly char Low = low;
            public readonly char High = high;
            public readonly int ChildrenStart = childrenStart;
            public readonly int Value = value;
            public readonly string Literal = literal;
        }

        private sealed class Routes
        {
            // the root node of the routes of each length, 0 is the no match node
            public int[] RootByLength = [];

            // the routes that end with '$', longest first
            public (string Prefix, int Value)[] Prefixes = [];
        }

        private sealed class CaptureRoutes
        {
            // the path before the '*'
            public readonly string Prefix;

            // the path after the captured segment
            public readonly Routes Routes;

            // PERF: a prefix of 8 to 16 chars as two (overlapping) vectors of 8 chars with the case bit set for the letters, so
            // comparing it ignoring the case is setting the case bit of the path and two vector comparisons, like the JIT does
            // for a constant
            private readonly bool _vectorized;
            private readonly Vector128<ushort> _first, _firstCaseBits, _last, _lastCaseBits;

            public CaptureRoutes(string prefix, Routes routes)
            {
                Prefix = prefix;
                Routes = routes;

                if (Vector128.IsHardwareAccelerated && prefix.Length >= Vector128<ushort>.Count && prefix.Length <= 2 * Vector128<ushort>.Count)
                {
                    _vectorized = true;
                    (_first, _firstCaseBits) = ToVector(prefix, 0);
                    (_last, _lastCaseBits) = ToVector(prefix, prefix.Length - Vector128<ushort>.Count);
                }
            }

            private static (Vector128<ushort> Value, Vector128<ushort> CaseBits) ToVector(string prefix, int start)
            {
                Span<ushort> value = stackalloc ushort[Vector128<ushort>.Count];
                Span<ushort> caseBits = stackalloc ushort[Vector128<ushort>.Count];
                for (int i = 0; i < value.Length; i++)
                {
                    char c = prefix[start + i];
                    caseBits[i] = (ushort)(char.IsAsciiLetter(c) ? 0x20 : 0);
                    value[i] = (ushort)(c | caseBits[i]);
                }

                return (Vector128.Create<ushort>(value), Vector128.Create<ushort>(caseBits));
            }

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public bool IsPrefixOf(ReadOnlySpan<char> path)
            {
                if (_vectorized == false)
                    return StartsWith(path, Prefix);

                if (path.Length < Prefix.Length)
                    return false;

                ref ushort start = ref Unsafe.As<char, ushort>(ref MemoryMarshal.GetReference(path));
                return (Vector128.LoadUnsafe(ref start) | _firstCaseBits) == _first &&
                       (Vector128.LoadUnsafe(ref start, (nuint)(Prefix.Length - Vector128<ushort>.Count)) | _lastCaseBits) == _last;
            }
        }

        private sealed class MethodRoutes(string method, Routes routes, CaptureRoutes[] captures)
        {
            public readonly string Method = method;
            public readonly Routes Routes = routes;
            public readonly CaptureRoutes[] Captures = captures;
        }

        private readonly MethodRoutes[] _methods;
        private readonly T[] _values;
        private readonly Node[] _nodes;
        private readonly int[] _children;

        public RouteMatcher(IEnumerable<(string Method, string Path, T Value)> routes)
        {
            var builder = new Builder();
            var values = new List<T>();
            var byMethod = new Dictionary<string, (List<(string, int)> Routes, Dictionary<string, List<(string, int)>> Captures)>(StringComparer.OrdinalIgnoreCase);

            foreach (var (method, path, value) in routes)
            {
                EnsureIsAscii(method, path);

                if (byMethod.TryGetValue(method, out var methodRoutes) == false)
                    byMethod[method] = methodRoutes = (new List<(string, int)>(), new Dictionary<string, List<(string, int)>>(StringComparer.OrdinalIgnoreCase));

                values.Add(value);
                int id = values.Count - 1;

                int capture = path.IndexOf('*');
                if (capture < 0)
                {
                    methodRoutes.Routes.Add((path, id));
                    continue;
                }

                if (path.IndexOf('*', capture + 1) >= 0 || (capture + 1 < path.Length && path[capture + 1] != '/'))
                    throw new InvalidOperationException($"A route can capture a single segment, with '*' followed by '/' or at the end, but got: {method} {path}");

                var prefix = path[..capture];
                if (methodRoutes.Captures.TryGetValue(prefix, out var captured) == false)
                    methodRoutes.Captures[prefix] = captured = new List<(string, int)>();

                captured.Add((path[(capture + 1)..], id));
            }

            _methods = byMethod
                .OrderByDescending(x => x.Value.Routes.Count + x.Value.Captures.Sum(c => c.Value.Count))
                .Select(x =>
                {
                    EnsureUnambiguous(x.Key, x.Value.Routes, x.Value.Captures.Keys);

                    return new MethodRoutes(
                        // PERF: Kestrel gives the known methods as the interned literals, so the common case is a reference comparison
                        string.Intern(x.Key.ToUpperInvariant()),
                        builder.Build(x.Key, x.Value.Routes),
                        x.Value.Captures.Select(c => new CaptureRoutes(c.Key, builder.Build(x.Key, c.Value))).ToArray());
                })
                .ToArray();

            _values = values.ToArray();
            _nodes = builder.Nodes.ToArray();
            _children = builder.Children.ToArray();
        }

        public bool TryMatch(string method, string path, out RouteMatch match, out T value)
        {
            path ??= string.Empty;
            match = new RouteMatch
            {
                Url = path,
                Method = method,
                MatchLength = path.Length
            };

            var methodRoutes = FindMethod(method);
            if (methodRoutes != null)
            {
                ReadOnlySpan<char> span = path;

                foreach (var capture in methodRoutes.Captures)
                {
                    if (capture.IsPrefixOf(span) == false)
                        continue;

                    var segment = span.Slice(capture.Prefix.Length);
                    int segmentLength = segment.IndexOf('/');
                    if (segmentLength < 0)
                        segmentLength = segment.Length;

                    int id = Find(capture.Routes, segment.Slice(segmentLength), out int prefixLength);
                    if (id < 0)
                        continue;

                    match.CaptureStart = capture.Prefix.Length;
                    match.CaptureLength = segmentLength;
                    if (prefixLength >= 0)
                        match.MatchLength = capture.Prefix.Length + segmentLength + prefixLength;

                    value = _values[id];
                    return true;
                }

                int routeId = Find(methodRoutes.Routes, span, out int routePrefixLength);
                if (routeId >= 0)
                {
                    if (routePrefixLength >= 0)
                        match.MatchLength = routePrefixLength;

                    value = _values[routeId];
                    return true;
                }
            }

            value = default;
            return false;
        }

        private MethodRoutes FindMethod(string method)
        {
            var methods = _methods;
            foreach (var routes in methods)
            {
                if (ReferenceEquals(routes.Method, method))
                    return routes;
            }

            foreach (var routes in methods)
            {
                if (Ascii.EqualsIgnoreCase(routes.Method, method))
                    return routes;
            }

            return null;
        }

        // returns the index of the value of the route, prefixLength is the length of the matched prefix for a route that ends with '$' (else -1)
        private int Find(Routes routes, ReadOnlySpan<char> path, out int prefixLength)
        {
            prefixLength = -1;

            var rootByLength = routes.RootByLength;
            if ((uint)path.Length < (uint)rootByLength.Length)
            {
                var nodes = _nodes;
                var children = _children;
                int index = rootByLength[path.Length];
                while (true)
                {
                    ref readonly var node = ref nodes[index];
                    if (node.Position < 0)
                    {
                        // PERF: clients send the path as the route was written, so we check the exact match first
                        if (node.Value >= 0 && (path.SequenceEqual(node.Literal) || Ascii.EqualsIgnoreCase(path, node.Literal)))
                            return node.Value;
                        break;
                    }

                    int offset = path[node.Position] - node.Low;
                    if ((uint)offset > (uint)(node.High - node.Low))
                        break;

                    index = children[node.ChildrenStart + offset];
                }
            }

            foreach (var (prefix, value) in routes.Prefixes)
            {
                if (StartsWith(path, prefix))
                {
                    prefixLength = prefix.Length;
                    return value;
                }
            }

            return -1;
        }

        private static bool StartsWith(ReadOnlySpan<char> path, string prefix)
        {
            return path.StartsWith(prefix) ||
                   (path.Length >= prefix.Length && Ascii.EqualsIgnoreCase(path.Slice(0, prefix.Length), prefix));
        }

        private static void EnsureIsAscii(string method, string path)
        {
            if (Ascii.IsValid(method) == false || Ascii.IsValid(path) == false)
                throw new InvalidOperationException($"Cannot use non ASCII chars in routes, but got: {method} {path}");
        }

        private static void EnsureUnambiguous(string method, List<(string Path, int)> routes, IEnumerable<string> capturePrefixes)
        {
            // a path can match a single capture prefix, and a route that starts like a capture prefix would compete with its captures
            var prefixes = capturePrefixes.ToList();
            foreach (var prefix in prefixes)
            {
                foreach (var other in prefixes)
                {
                    if (ReferenceEquals(prefix, other) == false && other.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                        throw new InvalidOperationException($"The routes of {method} capture after both '{prefix}' and '{other}', which is ambiguous");
                }

                foreach (var (path, _) in routes)
                {
                    if (path.TrimEnd('$').StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                        throw new InvalidOperationException($"The route {method} {path} is ambiguous with the routes that capture after '{prefix}'");
                }
            }
        }

        private sealed class Builder
        {
            // node 0 is the no match node
            public readonly List<Node> Nodes = [new Node(position: -1, low: default, high: default, childrenStart: 0, value: -1, literal: null)];
            public readonly List<int> Children = [];

            public Routes Build(string method, List<(string Path, int Value)> routes)
            {
                var exact = new List<(string Path, int Value)>();
                var prefixes = new List<(string Prefix, int Value)>();
                var unique = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                foreach (var (path, value) in routes)
                {
                    int end = path.IndexOf('$');
                    if (end >= 0 && end != path.Length - 1)
                        throw new InvalidOperationException($"A route can have '$' only at its end, but got: {method} {path}");

                    if (unique.Add(path) == false)
                        throw new InvalidOperationException($"A duplicate route found: {method} {path}");

                    if (end >= 0)
                        prefixes.Add((path[..end], value));
                    else
                        exact.Add((path, value));
                }

                var result = new Routes
                {
                    Prefixes = prefixes.OrderByDescending(x => x.Prefix.Length).ToArray()
                };

                if (exact.Count > 0)
                {
                    result.RootByLength = new int[exact.Max(x => x.Path.Length) + 1];
                    foreach (var sameLength in exact.GroupBy(x => x.Path.Length))
                        result.RootByLength[sameLength.Key] = AddNode(sameLength.ToList());
                }

                return result;
            }

            private int AddNode(List<(string Path, int Value)> candidates)
            {
                if (candidates.Count == 1)
                {
                    Nodes.Add(new Node(position: -1, low: default, high: default, childrenStart: 0, value: candidates[0].Value, literal: candidates[0].Path));
                    return Nodes.Count - 1;
                }

                // the position that splits the candidates best, there is always one since they are distinct (ignoring case) and of the same length
                int position = -1;
                List<IGrouping<char, (string Path, int Value)>> groups = null;
                for (int i = 0; i < candidates[0].Path.Length; i++)
                {
                    var byChar = candidates.GroupBy(x => char.ToLowerInvariant(x.Path[i])).ToList();
                    if (groups == null || byChar.Max(g => g.Count()) < groups.Max(g => g.Count()) ||
                        (byChar.Max(g => g.Count()) == groups.Max(g => g.Count()) && byChar.Count > groups.Count))
                    {
                        position = i;
                        groups = byChar;
                    }
                }

                var children = groups.Select(g => (Char: g.Key, Node: AddNode(g.ToList()))).ToList();

                // the children are reachable by both cases of the character
                var entries = children
                    .SelectMany(x => new[] { (Char: x.Char, x.Node), (Char: char.ToUpperInvariant(x.Char), x.Node) })
                    .Distinct()
                    .ToList();

                char low = entries.Min(x => x.Char);
                char high = entries.Max(x => x.Char);
                int childrenStart = Children.Count;
                for (char c = low; c <= high; c++)
                    Children.Add(0);
                foreach (var (c, node) in entries)
                    Children[childrenStart + c - low] = node;

                Nodes.Add(new Node(position, low, high, childrenStart, value: -1, literal: null));
                return Nodes.Count - 1;
            }
        }
    }
}
