using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using Corax.Querying.Planning;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using IndexSearcher = Corax.Querying.IndexSearcher;

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
    // ORDER BY AST, as a 256-bit value so the per-query bucket is keyed by Vector256<long>. WHERE operands are
    // value- and parameter-agnostic: literals reduce to their type token, parameter references collapse to one
    // "is a parameter" marker. That collapses value variants (price > 5 vs > 10) and parameter variants (incl.
    // aliased reuse, since each value leaf is its own slot), while every structural distinction the template
    // depends on (operators, field names, IN arity, boolean nesting, negation, method names, literal TYPE, full
    // ORDER BY shape) is preserved. Per-parameter runtime types are NOT in this key — the per-variant CacheKeyHash
    // (BuildResolver) distinguishes them within the bucket. Encoding reuses the allocation-free PlanCacheKeyBuilder.
    //
    // Every field name also folds in the field's index-wide single/multi-valued state (HasMultipleTermsInField).
    // The template bakes single-valued optimizations (straight-line residual IL, sort-key elision), so a field that
    // goes single->multi must select a different bucket and re-plan. The bit is index-wide and monotonic.
    [SkipLocalsInit]
    private static Vector256<long> ComputeStructuralKey(PlanParameters planParams)
    {
        var builder = new PlanCacheKeyBuilder();

        AppendCanonicalExpression(ref builder, planParams.Metadata.Query.Where, planParams.IndexSearcher);
        AppendCanonicalOrderBy(ref builder, planParams.Metadata.OrderBy, planParams.IndexSearcher);

        return builder.ToHash();
    }

    // Append a field name plus the field's single/multi-valued bit (see ComputeStructuralKey). The clause walk only
    // reaches this for genuine field references and quoted reserved-word field names, never for parameters or
    // literals, so the name here is always the indexed field name and matches the write-time MultipleTermsInField key.
    private static void AppendFieldName(ref PlanCacheKeyBuilder builder, string name, IndexSearcher searcher)
    {
        AppendString(ref builder, name);
        builder.Append(name != null && searcher.HasMultipleTermsInField(name) ? 1 : 0, 1);
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
    private static void AppendCanonicalExpression(ref PlanCacheKeyBuilder builder, QueryExpression expr, IndexSearcher searcher, bool exactValues = false)
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
                    AppendCanonicalExpression(ref builder, be.Left, searcher, exactValues);
                    AppendCanonicalExpression(ref builder, be.Right, searcher, exactValues);
                }
                else
                {
                    // Comparison: ParseComparison/ParseRangeComparison resolve the left operand as the field
                    // (via TryGetFieldName); the right operand is the value.
                    AppendCanonicalField(ref builder, be.Left, searcher, exactValues);
                    AppendCanonicalExpression(ref builder, be.Right, searcher, exactValues);
                }

                return;

            case NegatedExpression ne:
                AppendTag(ref builder, AstTag.Negated);
                AppendCanonicalExpression(ref builder, ne.Expression, searcher, exactValues);
                return;

            case BetweenExpression bw:
                AppendTag(ref builder, AstTag.Between);
                AppendCanonicalField(ref builder, bw.Source, searcher, exactValues);
                AppendCanonicalValue(ref builder, bw.Min, exactValues);
                AppendCanonicalValue(ref builder, bw.Max, exactValues);
                builder.Append(bw.MinInclusive ? 1 : 0, 1);
                builder.Append(bw.MaxInclusive ? 1 : 0, 1);
                return;

            case InExpression ie:
                // IN arity is structural: each value is its own binding, so (a,b) and (a,b,c) are different templates.
                AppendTag(ref builder, AstTag.In);
                builder.Append(ie.All ? 1 : 0, 1);
                AppendCanonicalField(ref builder, ie.Source, searcher, exactValues);
                builder.Append(ie.Values.Count, 31);
                foreach (var v in ie.Values)
                    AppendCanonicalExpression(ref builder, v, searcher, exactValues);
                return;

            case MethodExpression me:
            {
                // The method name (search/exact/boost/spatial.*/vector.search/...) drives clause shape and the
                // HasBoost / HasVectorSearch flags, so it must be part of the key.
                AppendTag(ref builder, AstTag.Method);
                AppendString(ref builder, me.Name.Value);
                builder.Append(me.Arguments.Count, 31);

                // when(condition, inner): the condition is constant-folded into the plan at build time and baked
                // into the clause's WhenCondition delegate, so its LITERAL operands are structural — they pick the
                // branch (apply the inner predicate vs collapse to identity). They must split the cache bucket, or
                // when($p == 1.5, X) and when($p == 2.0, X) would share a template and the second would evaluate
                // the first's frozen literal. So the condition (arg[0]) is walked with exact literal values; the
                // inner predicate (arg[1]) keeps normal value collapsing since it is matched per-variant at runtime.
                bool isWhen = QueryMethod.GetMethodType(me.Name.Value, throwIfNoMatch: false) == MethodType.When;

                // Field-first methods resolve arg[0] as their field via TryGetFieldName, so it must go through
                // AppendCanonicalField to preserve a quoted reserved-word field's NAME. Every other argument -
                // and all arguments of the remaining methods (wrappers exact/boost/when, moreLikeThis) - is a
                // normal operand walked below.
                int firstOperand = 0;
                if (me.Arguments.Count > 0 && MethodTakesFieldAsFirstArgument(me.Name.Value))
                {
                    AppendCanonicalField(ref builder, me.Arguments[0], searcher, exactValues);
                    firstOperand = 1;
                }

                for (int i = firstOperand; i < me.Arguments.Count; i++)
                    AppendCanonicalExpression(ref builder, me.Arguments[i], searcher, exactValues || (isWhen && i == 0));

                return;
            }

            case FieldExpression fe:
                AppendTag(ref builder, AstTag.Field);
                AppendFieldName(ref builder, fe.FieldValue, searcher);
                return;

            case ValueExpression ve:
                AppendTag(ref builder, AstTag.Value);
                AppendCanonicalValue(ref builder, ve, exactValues);
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
    private static void AppendCanonicalValue(ref PlanCacheKeyBuilder builder, ValueExpression ve, bool exactValues = false)
    {
        if (ve.Value == ValueTokenType.Parameter)
        {
            builder.Append(0, 1); // 0 = parameter operand
        }
        else
        {
            builder.Append(1, 1); // 1 = literal operand
            builder.Append((int)ve.Value, ValueTokenBits);
            // Inside a constant-folded when() condition the literal VALUE (not just its type) selects the plan
            // branch, so it must be part of the key. Everywhere else the value is dropped so value variants collapse.
            if (exactValues)
                AppendString(ref builder, ve.Token.Value);
        }
    }

    // A field-position operand: the template resolves these via TryGetFieldName, which accepts three concrete
    // shapes, each handled explicitly here so the routing is visible rather than relying on the fall-through walk:
    //   - a bare field (FieldExpression)            -> the same Field tag + name the expression walk emits;
    //   - a quoted reserved-word field ('Order'), which the parser returns as a String ValueExpression
    //     -> a distinct QuotedField tag + name. Blanking it to a type token (as AppendCanonicalValue does for
    //     value operands) would let `where 'Order' = $p` and `where 'Group' = $p` share one template and resolve
    //     against the wrong field; the separate tag also keeps it from colliding with a same-typed search term;
    //   - a wrapper that names the field itself (spatial.point(...) / embedding.*(...) / id())
    //     -> recurse into the normal walk, which encodes that MethodExpression's own identity.
    private static void AppendCanonicalField(ref PlanCacheKeyBuilder builder, QueryExpression expr, IndexSearcher searcher, bool exactValues = false)
    {
        switch (expr)
        {
            case FieldExpression fe:
                AppendTag(ref builder, AstTag.Field);
                AppendFieldName(ref builder, fe.FieldValue, searcher);
                return;

            case ValueExpression { Value: ValueTokenType.String } ve:
                AppendTag(ref builder, AstTag.QuotedField);
                AppendFieldName(ref builder, ve.Token.Value, searcher);
                return;

            default:
                AppendCanonicalExpression(ref builder, expr, searcher, exactValues);
                return;
        }
    }

    // Methods whose first argument is a field reference, resolved through TryGetFieldName in QueryPlanBuilder.
    // Kept in sync with that resolution (MethodHandlers): the wrapper methods exact/boost/when carry a
    // sub-expression at arg[0] and moreLikeThis takes no field, so they are deliberately absent - their
    // arguments are all walked as normal operands. An unknown name returns Unknown and falls through to false.
    private static bool MethodTakesFieldAsFirstArgument(string methodName) =>
        QueryMethod.GetMethodType(methodName, throwIfNoMatch: false) switch
        {
            MethodType.Search
                or MethodType.StartsWith
                or MethodType.EndsWith
                or MethodType.Regex
                or MethodType.Exists
                or MethodType.Spatial_Within
                or MethodType.Spatial_Contains
                or MethodType.Spatial_Disjoint
                or MethodType.Spatial_Intersects
                or MethodType.Vector_Search => true,
            _ => false
        };

    // Canonical ORDER BY serialization. The template bakes the resolved sort shape (field, ordering type,
    // direction, nulls mode, and any literal random seed / distance coordinates), so the structural key must
    // preserve all of it. Argument values are kept verbatim - literal seeds/coords are baked into the template's
    // prebuilt/patch closures and must not be shared across buckets; parameter arguments render by name (the value
    // is resolved at runtime), so parameter-value variants still collapse. A null ORDER BY is kept distinct from an
    // empty one.
    private static void AppendCanonicalOrderBy(ref PlanCacheKeyBuilder builder, OrderByField[] orderBy, IndexSearcher searcher)
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
            AppendFieldName(ref builder, field.Name?.Value, searcher);
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
