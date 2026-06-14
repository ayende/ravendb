using System;
using System.Collections.Generic;
using System.Runtime.Intrinsics;
using Corax.Querying.Planning;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

internal static partial class QueryPlanBuilder
{
    // Node tags for the structural-key bit stream (4 bits each). Every recursive operand position starts with one
    // of these, so the packed stream is self-delimiting and two structurally distinct trees can never collide.
    private const int AstTagNull = 0, AstTagBinary = 1, AstTagNegated = 2, AstTagBetween = 3, AstTagIn = 4,
        AstTagMethod = 5, AstTagField = 6, AstTagValue = 7, AstTagTrue = 8, AstTagUnknown = 9;

    // Structural plan key: a SHA256 digest over a canonical, bit-packed serialization of the query's WHERE +
    // ORDER BY AST, producing a fixed-size 256-bit value so the per-query bucket can be keyed by Vector256<long>.
    // The serialization is value- and parameter-name-agnostic for WHERE operands: literal values are reduced to
    // their type token (Long/Double/String/...) and parameter references are renumbered to first-occurrence
    // ordinals. That collapses pure value variants (price > 5 vs price > 10) and parameter-name variants
    // (name = $p1 vs name = $q) onto a single shared bucket, while every structural distinction the template
    // depends on - operators, field names, IN arity, boolean nesting, negation, method names, literal TYPE, and
    // the full ORDER BY shape - is preserved. Per-parameter runtime types are deliberately NOT in this key: the
    // per-variant CacheKeyHash (see BuildResolver) distinguishes them within the bucket. The bit-packing reuses
    // PlanCacheKeyBuilder, the same allocation-free encoder the inner cache key uses, so there is no intermediate
    // string. A more-like-this base-document sub-expression (WhereOverride) runs through the same walk over the
    // override expression behind a marker bit that keeps its plans in their own namespace (never merging with a
    // regular query).
    private static Vector256<long> ComputeStructuralKey(PlanParameters planParams)
    {
        Span<byte> scratch = stackalloc byte[128];
        var builder = new PlanCacheKeyBuilder(scratch);

        // A WhereOverride is a sub-expression compiled standalone (the MLT base-document query). It is keyed off
        // that expression, not the outer query's WHERE/ORDER BY, and the marker bit partitions its plans from any
        // structurally identical regular query so the two can never share a bucket.
        if (planParams.WhereOverride != null)
        {
            builder.Append(1, 1);
            Dictionary<string, int> overrideOrdinals = new(StringComparer.Ordinal);
            AppendCanonicalExpression(ref builder, planParams.WhereOverride, overrideOrdinals);
        }
        else
        {
            builder.Append(0, 1);
            Dictionary<string, int> paramOrdinals = new(StringComparer.Ordinal);
            AppendCanonicalExpression(ref builder, planParams.Metadata.Query.Where, paramOrdinals);
            AppendCanonicalOrderBy(ref builder, planParams.Metadata.OrderBy);
        }

        return builder.ToHash();
    }

    // Append a string as a presence bit, a length prefix, then its UTF-16 code units. Allocation-free and exact:
    // null, empty, and any two distinct strings each produce a different bit sequence.
    private static void AppendString(ref PlanCacheKeyBuilder builder, string value)
    {
        if (value == null)
        {
            builder.Append(0, 1);
            return;
        }

        builder.Append(1, 1);
        builder.Append(value.Length, 31);
        foreach (char c in value)
            builder.Append(c, 16);
    }

    // Canonical WHERE-clause serialization. Each node emits a fixed-width tag then its delimited contents so
    // structurally distinct trees can never produce the same bit stream; operands are normalized by
    // AppendCanonicalValue so value- and parameter-name variants converge. The DFS order matches the parse walk
    // that assigns value ordinals.
    private static void AppendCanonicalExpression(ref PlanCacheKeyBuilder builder, QueryExpression expr, Dictionary<string, int> paramOrdinals)
    {
        switch (expr)
        {
            case null:
                builder.Append(AstTagNull, 4); // no WHERE clause - distinct from any concrete node
                return;

            case BinaryExpression be:
                // Operator goes right after the tag so a > b and a < b diverge before the operands are even read.
                builder.Append(AstTagBinary, 4);
                builder.Append((int)be.Operator, 8);
                AppendCanonicalExpression(ref builder, be.Left, paramOrdinals);
                AppendCanonicalExpression(ref builder, be.Right, paramOrdinals);
                return;

            case NegatedExpression ne:
                builder.Append(AstTagNegated, 4);
                AppendCanonicalExpression(ref builder, ne.Expression, paramOrdinals);
                return;

            case BetweenExpression bw:
                builder.Append(AstTagBetween, 4);
                AppendCanonicalExpression(ref builder, bw.Source, paramOrdinals);
                AppendCanonicalValue(ref builder, bw.Min, paramOrdinals);
                AppendCanonicalValue(ref builder, bw.Max, paramOrdinals);
                builder.Append(bw.MinInclusive ? 1 : 0, 1);
                builder.Append(bw.MaxInclusive ? 1 : 0, 1);
                return;

            case InExpression ie:
                // IN arity is structural: each value is its own binding, so (a,b) and (a,b,c) are different templates.
                builder.Append(AstTagIn, 4);
                builder.Append(ie.All ? 1 : 0, 1);
                AppendCanonicalExpression(ref builder, ie.Source, paramOrdinals);
                builder.Append(ie.Values.Count, 31);
                foreach (var v in ie.Values)
                    AppendCanonicalExpression(ref builder, v, paramOrdinals);
                return;

            case MethodExpression me:
                // The method name (search/exact/boost/spatial.*/vector.search/...) drives clause shape and the
                // HasBoost / HasVectorSearch flags, so it must be part of the key.
                builder.Append(AstTagMethod, 4);
                AppendString(ref builder, me.Name.Value);
                builder.Append(me.Arguments.Count, 31);
                foreach (var a in me.Arguments)
                    AppendCanonicalExpression(ref builder, a, paramOrdinals);
                return;

            case FieldExpression fe:
                builder.Append(AstTagField, 4);
                AppendString(ref builder, fe.FieldValue);
                return;

            case ValueExpression ve:
                builder.Append(AstTagValue, 4);
                AppendCanonicalValue(ref builder, ve, paramOrdinals);
                return;

            case TrueExpression:
                builder.Append(AstTagTrue, 4);
                return;

            default:
                // Unknown node type: fall back to its full text so an unrecognized shape can never silently
                // collapse with a different one. ExpressionType currently has no member we do not handle above,
                // so this is a forward-compatibility guard, not a live path.
                builder.Append(AstTagUnknown, 4);
                AppendString(ref builder, expr.GetType().Name);
                AppendString(ref builder, expr.ToString());
                return;
        }
    }

    // Canonical operand serialization. A parameter reference becomes P{ordinal}, renumbered by first occurrence so
    // that name-isomorphic queries ($x and $x vs $y and $y) match while genuinely distinct fan-outs ($x and $y) do
    // not - this mirrors the template's deduplicated ParameterSlots. A literal becomes L{typeCode}: the TYPE is kept
    // (the per-variant CacheKeyHash does not see literals, so the bucket must already segregate long/double/string/
    // bool/null), but the value is dropped so value variants collapse.
    private static void AppendCanonicalValue(ref PlanCacheKeyBuilder builder, ValueExpression ve, Dictionary<string, int> paramOrdinals)
    {
        if (ve.Value == ValueTokenType.Parameter)
        {
            string name = ve.Token.Value;
            if (paramOrdinals.TryGetValue(name, out int ordinal) == false)
            {
                ordinal = paramOrdinals.Count;
                paramOrdinals.Add(name, ordinal);
            }

            builder.Append(0, 1); // 0 = parameter operand
            builder.Append(ordinal, 31);
        }
        else
        {
            builder.Append(1, 1); // 1 = literal operand
            builder.Append((int)ve.Value, 8);
        }
    }

    // Canonical ORDER BY serialization. The template bakes the resolved sort shape (field, ordering type,
    // direction, nulls mode, and any literal random seed / distance coordinates), so the structural key must
    // preserve all of it. Argument values are kept verbatim - literal seeds/coords are baked into the template's
    // prebuilt/patch closures and must not be shared across buckets; parameter arguments render by name (the value
    // is resolved at runtime), so parameter-value variants still collapse. A null ORDER BY is kept distinct from an
    // empty one.
    private static void AppendCanonicalOrderBy(ref PlanCacheKeyBuilder builder, OrderByField[] orderBy)
    {
        if (orderBy == null)
        {
            builder.Append(0, 1); // a null ORDER BY is kept distinct from an empty one
            return;
        }

        builder.Append(1, 1);
        builder.Append(orderBy.Length, 31);
        foreach (var field in orderBy)
        {
            AppendString(ref builder, field.Name?.Value);
            builder.Append((int)field.OrderingType, 8);
            builder.Append(field.Ascending ? 1 : 0, 1);
            builder.Append((int)field.NullsOrdering, 8);
            if (field.Method.HasValue)
            {
                builder.Append(1, 1);
                builder.Append((int)field.Method.Value, 8);
            }
            else
            {
                builder.Append(0, 1);
            }

            if (field.Arguments == null)
            {
                builder.Append(0, 1);
            }
            else
            {
                builder.Append(1, 1);
                builder.Append(field.Arguments.Length, 31);
                foreach (var arg in field.Arguments)
                {
                    builder.Append((int)arg.Type, 8);
                    AppendString(ref builder, arg.NameOrValue);
                }
            }
        }
    }
}
