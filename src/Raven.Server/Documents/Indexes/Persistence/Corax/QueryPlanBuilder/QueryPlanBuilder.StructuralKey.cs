using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using Corax.Querying.Planning;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

internal static partial class QueryPlanBuilder
{
    // Node tags for the structural-key bit stream. Every recursive operand position starts with one of these, so
    // the packed stream is self-delimiting and two structurally distinct trees can never collide.
    private enum AstTag
    {
        Null = 0,
        Binary = 1,
        Negated = 2,
        Between = 3,
        In = 4,
        Method = 5,
        Field = 6,
        Value = 7,
        True = 8,
        QuotedField = 9, // a reserved-word field name quoted in the source ('Order'), parsed as a string value
    }

    // Width, in bits, of an AstTag in the packed stream. Holds every member above with room to spare.
    private const int AstTagBits = 4;

    // Width, in bits, of an OperatorType (8 members); 4 bits leaves headroom.
    private const int OperatorBits = 4;

    // Width, in bits, of a ValueTokenType (7 members) - used for literal-type tokens and ORDER BY argument types.
    private const int ValueTokenBits = 4;

    // Width, in bits, of an OrderByFieldType (9 members).
    private const int OrderingTypeBits = 4;

    // Width, in bits, of a NullsOrderingType (3 members).
    private const int NullsOrderingBits = 2;

    // Width, in bits, of a MethodType. The enum churns (currently ~47 members), so keep a wide field (capacity
    // 256): a release-mode overflow would silently truncate the value and collide otherwise-distinct plan keys.
    private const int MethodTypeBits = 8;

    private static void AppendTag(ref PlanCacheKeyBuilder builder, AstTag tag) => builder.Append((int)tag, AstTagBits);

    // Structural plan key: a SHA256 digest over a canonical, bit-packed serialization of the query's WHERE +
    // ORDER BY AST, producing a fixed-size 256-bit value so the per-query bucket can be keyed by Vector256<long>.
    // The serialization is value- and parameter-agnostic for WHERE operands: literal values are reduced to their
    // type token (Long/Double/String/...) and parameter references collapse to a single "is a parameter" marker.
    // That collapses pure value variants (price > 5 vs price > 10) and parameter variants (name = $p1 vs name = $q,
    // and even aliased reuse like a = $x and b = $x vs a = $x and b = $y, since the template never deduplicates
    // bindings - each value leaf is its own slot), while every structural distinction the template depends on -
    // operators, field names, IN arity, boolean nesting, negation, method names, literal TYPE, and the full
    // ORDER BY shape - is preserved. Per-parameter runtime types are deliberately NOT in this key: the per-variant
    // CacheKeyHash (see BuildResolver) distinguishes them within the bucket. The bit-packing reuses
    // PlanCacheKeyBuilder, the same allocation-free encoder the inner cache key uses, so there is no intermediate
    // string.
    [SkipLocalsInit]
    private static Vector256<long> ComputeStructuralKey(PlanParameters planParams)
    {
        Span<byte> scratch = stackalloc byte[256];
        var builder = new PlanCacheKeyBuilder(scratch);

        AppendCanonicalExpression(ref builder, planParams.Metadata.Query.Where);
        AppendCanonicalOrderBy(ref builder, planParams.Metadata.OrderBy);

        return builder.ToHash();
    }

    // Append a string as a presence bit, a length prefix, then its UTF-16 bytes copied directly into the buffer.
    // Allocation-free and exact: null, empty, and any two distinct strings each produce a different bit sequence.
    private static void AppendString(ref PlanCacheKeyBuilder builder, string value)
    {
        if (value == null)
        {
            builder.Append(0, 1);
            return;
        }

        builder.Append(1, 1);
        builder.Append(value.Length, 31);
        builder.Append(MemoryMarshal.AsBytes(value.AsSpan()));
    }

    // Canonical WHERE-clause serialization. Each node emits a fixed-width tag then its delimited contents so
    // structurally distinct trees can never produce the same bit stream; operands are normalized by
    // AppendCanonicalValue so value- and parameter variants converge. The DFS order matches the parse walk
    // that assigns value ordinals.
    private static void AppendCanonicalExpression(ref PlanCacheKeyBuilder builder, QueryExpression expr)
    {
        RuntimeHelpers.EnsureSufficientExecutionStack();
        switch (expr)
        {
            case null:
                AppendTag(ref builder, AstTag.Null); // no WHERE clause - distinct from any concrete node
                return;

            case BinaryExpression be:
                // Operator goes right after the tag so a > b and a < b diverge before the operands are even read.
                AppendTag(ref builder, AstTag.Binary);
                builder.Append((int)be.Operator, OperatorBits);
                if (be.Operator is OperatorType.And or OperatorType.Or)
                {
                    // Boolean connective: both operands are sub-expressions.
                    AppendCanonicalExpression(ref builder, be.Left);
                    AppendCanonicalExpression(ref builder, be.Right);
                }
                else
                {
                    // Comparison: ParseComparison/ParseRangeComparison resolve the left operand as the field
                    // (via TryGetFieldName); the right operand is the value.
                    AppendCanonicalField(ref builder, be.Left);
                    AppendCanonicalExpression(ref builder, be.Right);
                }

                return;

            case NegatedExpression ne:
                AppendTag(ref builder, AstTag.Negated);
                AppendCanonicalExpression(ref builder, ne.Expression);
                return;

            case BetweenExpression bw:
                AppendTag(ref builder, AstTag.Between);
                AppendCanonicalField(ref builder, bw.Source);
                AppendCanonicalValue(ref builder, bw.Min);
                AppendCanonicalValue(ref builder, bw.Max);
                builder.Append(bw.MinInclusive ? 1 : 0, 1);
                builder.Append(bw.MaxInclusive ? 1 : 0, 1);
                return;

            case InExpression ie:
                // IN arity is structural: each value is its own binding, so (a,b) and (a,b,c) are different templates.
                AppendTag(ref builder, AstTag.In);
                builder.Append(ie.All ? 1 : 0, 1);
                AppendCanonicalField(ref builder, ie.Source);
                builder.Append(ie.Values.Count, 31);
                foreach (var v in ie.Values)
                    AppendCanonicalExpression(ref builder, v);
                return;

            case MethodExpression me:
                // The method name (search/exact/boost/spatial.*/vector.search/...) drives clause shape and the
                // HasBoost / HasVectorSearch flags, so it must be part of the key.
                AppendTag(ref builder, AstTag.Method);
                AppendString(ref builder, me.Name.Value);
                builder.Append(me.Arguments.Count, 31);
                for (int i = 0; i < me.Arguments.Count; i++)
                {
                    // arg[0] of a field-first method (search/startsWith/exists/regex/spatial.*/...) is the field;
                    // a quoted reserved word lands here as a string value and must keep its name. Wrapper methods
                    // (exact/boost/when) carry a sub-expression at arg[0], which AppendCanonicalField passes
                    // through unchanged. Treating arg[0] this way only ever splits keys, never merges them.
                    if (i == 0)
                        AppendCanonicalField(ref builder, me.Arguments[i]);
                    else
                        AppendCanonicalExpression(ref builder, me.Arguments[i]);
                }

                return;

            case FieldExpression fe:
                AppendTag(ref builder, AstTag.Field);
                AppendString(ref builder, fe.FieldValue);
                return;

            case ValueExpression ve:
                AppendTag(ref builder, AstTag.Value);
                AppendCanonicalValue(ref builder, ve);
                return;

            case TrueExpression:
                AppendTag(ref builder, AstTag.True);
                return;

            default:
                throw new InvalidOperationException(
                    $"Unsupported query expression node type '{expr.GetType().Name}' in the structural plan key.");
        }
    }

    // Canonical operand serialization. A parameter reference collapses to a single "parameter" marker: which
    // parameter it is (name, or whether the same parameter is reused) is purely value-level and resolved through
    // the per-query slot vector at runtime, so it must not split buckets. A literal becomes L{typeCode}: the TYPE
    // is kept (the per-variant CacheKeyHash does not see literals, so the bucket must already segregate
    // long/double/string/bool/null), but the value is dropped so value variants collapse.
    private static void AppendCanonicalValue(ref PlanCacheKeyBuilder builder, ValueExpression ve)
    {
        if (ve.Value == ValueTokenType.Parameter)
        {
            builder.Append(0, 1); // 0 = parameter operand
        }
        else
        {
            builder.Append(1, 1); // 1 = literal operand
            builder.Append((int)ve.Value, ValueTokenBits);
        }
    }

    // Field-position operand. The template resolves these via TryGetFieldName, which accepts a bare field
    // (FieldExpression) or a quoted reserved-word field that the parser represents as a string ValueExpression
    // (e.g. 'Order'). A quoted field must keep its NAME in the key: collapsing it to a bare type token (as
    // AppendCanonicalValue does for value operands) would let `where 'Order' = $p` and `where 'Group' = $p`
    // share one template and resolve against the wrong field. The distinct QuotedField tag also keeps a quoted
    // field from ever colliding with a same-typed value operand (e.g. a search term). Any other node
    // (FieldExpression, id(), or a wrapped sub-expression) already encodes its identity through the normal walk.
    private static void AppendCanonicalField(ref PlanCacheKeyBuilder builder, QueryExpression expr)
    {
        if (expr is ValueExpression { Value: ValueTokenType.String } ve)
        {
            AppendTag(ref builder, AstTag.QuotedField);
            AppendString(ref builder, ve.Token.Value);
            return;
        }

        AppendCanonicalExpression(ref builder, expr);
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
            builder.Append((int)field.OrderingType, OrderingTypeBits);
            builder.Append(field.Ascending ? 1 : 0, 1);
            builder.Append((int)field.NullsOrdering, NullsOrderingBits);
            if (field.Method.HasValue)
            {
                builder.Append(1, 1);
                builder.Append((int)field.Method.Value, MethodTypeBits);
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
                    builder.Append((int)arg.Type, ValueTokenBits);
                    AppendString(ref builder, arg.NameOrValue);
                }
            }
        }
    }
}
