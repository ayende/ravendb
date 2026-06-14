using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using Corax.Mappings;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.SortingMatches.Meta;
using Corax.Querying.Planning;
using Corax.Utils;
using Raven.Client.Exceptions;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Sparrow.Binary;
using Sparrow.Json;
using Sparrow.Server;
using Voron;
using Voron.Impl;
using Constants = Corax.Constants;
using RavenConstants = Raven.Client.Constants;
using IndexSearcher = Corax.Querying.IndexSearcher;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

internal static partial class QueryPlanBuilder
{
    public static PlanTemplate BuildTemplate(PlanParameters planParams)
    {
        var planCache = planParams.IndexSearcher.PlanCache;

        // Fast path: the QueryMetadata may already hold the bucket resolved against THIS cache instance, saving the
        // structural-key dictionary lookup. We use a weak ref so we don't pin the plan cache and it can be evicted.
        var metadata = planParams.CacheKeyOverride == null ? planParams.Metadata : null;

        // The per-query slot-binding vector (indexed by ValueOrdinal) carries this query text's literal values /
        // parameter names / deferred expressions; SlotBindingFor redirects each template binding to its slot here.
        // It is a pure function of the query text/AST, so it is memoized on QueryMetadata. We resolve it per-path
        // rather than up front: the warm/existing-bucket paths build it via ExtractSlotBindings, while the cold
        // template-parse path reuses the walk ParseTemplate already does instead of walking the WHERE clause twice.
        ParameterBinding[] slotBindings;

        if (metadata?.CachedPlanMemo is { } memo
            && memo.PlanCacheId == planCache.Id // if we reset the index, we want to re-create the plan
            && memo.Bucket.TryGetTarget(out var memoBucket))
        {
            slotBindings = ResolveSlotBindings(planParams, metadata);
            planParams.SlotBindings = slotBindings;
            planParams.Bucket = memoBucket;
            AssertSlotBindingsMatchTemplate(memoBucket.Template, slotBindings);
            return memoBucket.Template;
        }

        var structuralKey = ComputeStructuralKey(planParams);

        // Bucket already exists for this structural key: reuse its template (and its compiled plan variants).
        if (planCache.GetBucket(structuralKey) is { } existing)
        {
            slotBindings = ResolveSlotBindings(planParams, metadata);
            planParams.SlotBindings = slotBindings;
            planParams.Bucket = existing;
            metadata?.CachedPlanMemo = new QueryMetadata.PlanMemo(planCache.Id, existing);
            AssertSlotBindingsMatchTemplate(existing.Template, slotBindings);
            return existing.Template;
        }

        var template = ParseTemplate(planParams, out var parsedSlotBindings);
        template.SortMetadataTemplate = BuildSortMetadataTemplate(planParams);

        // Reuse the slot bindings ParseTemplate just collected: memoize them on the QueryMetadata (main path) or
        // use them directly (MLT override). This is the only path that walks the WHERE clause, so there is no
        // second walk for the slot vector.
        slotBindings = metadata != null ? (metadata.CachedSlotBindings ??= parsedSlotBindings) : parsedSlotBindings;
        planParams.SlotBindings = slotBindings;
        AssertSlotBindingsMatchTemplate(template, slotBindings);

        // Create (or join a racing thread's) bucket carrying this template. BuildResolver publishes the compiled
        // plan into planParams.Bucket; we return the bucket's template so a lost race uses the winner's instance.
        var bucket = planCache.GetOrAddBucket(structuralKey, template, planParams.CacheKey);
        planParams.Bucket = bucket;
        metadata?.CachedPlanMemo = new QueryMetadata.PlanMemo(planCache.Id, bucket);
        return bucket.Template;
    }

    /// <summary>Resolve the per-query slot-binding vector. Main path (no cache-key override): memoize on the
    /// QueryMetadata, since the vector is a pure function of the query text/AST. Override path (e.g. MLT
    /// sub-expression): build fresh and do not touch the metadata memo.</summary>
    private static ParameterBinding[] ResolveSlotBindings(PlanParameters planParams, QueryMetadata metadata)
    {
        if (metadata == null)
            return ExtractSlotBindings(planParams);

        return metadata.CachedSlotBindings ??= ExtractSlotBindings(planParams);
    }

    [Conditional("DEBUG")]
    private static void AssertSlotBindingsMatchTemplate(PlanTemplate template, ParameterBinding[] slotBindings)
    {
        Debug.Assert(template.ValueOrdinalCount == slotBindings.Length,
            $"Slot-binding vector length ({slotBindings.Length}) must equal the template value-ordinal count " +
            $"({template.ValueOrdinalCount}). Both come from the same canonical WHERE walk, so a mismatch means the " +
            "template parse and the per-query slot-vector parse diverged.");
    }

    // Structural plan key: a SHA256 digest over a canonical serialization of the query's WHERE + ORDER BY AST,
    // packed into a fixed-size 256-bit value so the per-query bucket can be keyed by Vector256<long>. The
    // serialization is value- and parameter-name-agnostic for WHERE operands: literal values are reduced to their
    // type token (Long/Double/String/...) and parameter references are renumbered to first-occurrence ordinals.
    // That collapses pure value variants (price > 5 vs price > 10) and parameter-name variants (name = $p1 vs
    // name = $q) onto a single shared bucket, while every structural distinction the template depends on -
    // operators, field names, IN arity, boolean nesting, negation, method names, literal TYPE, and the full
    // ORDER BY shape - is preserved. Per-parameter runtime types are deliberately NOT in this key: the per-variant
    // CacheKeyHash (see BuildResolver) distinguishes them within the bucket. The MLT/sub-expression path keeps its
    // purpose-built CacheKeyOverride text as the key (it already renders parameters by name, not value).
    private static Vector256<long> ComputeStructuralKey(PlanParameters planParams)
    {
        var sb = new StringBuilder();
        if (planParams.CacheKeyOverride != null)
        {
            sb.Append(planParams.CacheKeyOverride);
        }
        else
        {
            // Namespace the AST-derived format with a control-char prefix so it can never collide with the
            // override text branch above (which is plain query text and never starts with this sentinel).
            sb.Append("\u0001ast\u0001");
            Dictionary<string, int> paramOrdinals = new(StringComparer.Ordinal);
            AppendCanonicalExpression(sb, planParams.Metadata.Query.Where, paramOrdinals);
            AppendCanonicalOrderBy(sb, planParams.Metadata.OrderBy);
        }

        string canonical = sb.ToString();
        int maxBytes = Encoding.UTF8.GetByteCount(canonical);
        byte[] rented = ArrayPool<byte>.Shared.Rent(maxBytes);
        try
        {
            int written = Encoding.UTF8.GetBytes(canonical, rented);
            Span<byte> digest = stackalloc byte[32];
            SHA256.HashData(rented.AsSpan(0, written), digest);
            return Vector256.Create(MemoryMarshal.Cast<byte, long>(digest));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    // Canonical WHERE-clause serialization. Each node emits a tagged, delimited form so structurally distinct
    // trees can never produce the same byte stream; operands are normalized by AppendCanonicalValue so value- and
    // parameter-name variants converge. The DFS order matches the parse walk that assigns value ordinals.
    private static void AppendCanonicalExpression(StringBuilder sb, QueryExpression expr, Dictionary<string, int> paramOrdinals)
    {
        switch (expr)
        {
            case null:
                sb.Append("\u2205"); // no WHERE clause - distinct from any concrete node
                return;

            case BinaryExpression be:
                // Operator goes first so a > b and a < b diverge before the operands are even read.
                sb.Append("B(").Append((int)be.Operator).Append(':');
                AppendCanonicalExpression(sb, be.Left, paramOrdinals);
                sb.Append(',');
                AppendCanonicalExpression(sb, be.Right, paramOrdinals);
                sb.Append(')');
                return;

            case NegatedExpression ne:
                sb.Append("N(");
                AppendCanonicalExpression(sb, ne.Expression, paramOrdinals);
                sb.Append(')');
                return;

            case BetweenExpression bw:
                sb.Append("W(");
                AppendCanonicalExpression(sb, bw.Source, paramOrdinals);
                sb.Append(',');
                AppendCanonicalValue(sb, bw.Min, paramOrdinals);
                sb.Append(',');
                AppendCanonicalValue(sb, bw.Max, paramOrdinals);
                sb.Append(bw.MinInclusive ? '[' : '(').Append(bw.MaxInclusive ? ']' : ')').Append(')');
                return;

            case InExpression ie:
                // IN arity is structural: each value is its own binding, so (a,b) and (a,b,c) are different templates.
                sb.Append("I(").Append(ie.All ? '1' : '0').Append(':');
                AppendCanonicalExpression(sb, ie.Source, paramOrdinals);
                sb.Append('[').Append(ie.Values.Count).Append(':');
                foreach (var v in ie.Values)
                {
                    AppendCanonicalExpression(sb, v, paramOrdinals);
                    sb.Append(',');
                }
                sb.Append("])");
                return;

            case MethodExpression me:
                // The method name (search/exact/boost/spatial.*/vector.search/...) drives clause shape and the
                // HasBoost / HasVectorSearch flags, so it must be part of the key.
                sb.Append("M(").Append(me.Name.Value).Append('[').Append(me.Arguments.Count).Append(':');
                foreach (var a in me.Arguments)
                {
                    AppendCanonicalExpression(sb, a, paramOrdinals);
                    sb.Append(',');
                }
                sb.Append("])");
                return;

            case FieldExpression fe:
                sb.Append("F(").Append(fe.FieldValue).Append(')');
                return;

            case ValueExpression ve:
                AppendCanonicalValue(sb, ve, paramOrdinals);
                return;

            case TrueExpression:
                sb.Append('T');
                return;

            default:
                // Unknown node type: fall back to its full text so an unrecognized shape can never silently
                // collapse with a different one. ExpressionType currently has no member we do not handle above,
                // so this is a forward-compatibility guard, not a live path.
                sb.Append("U(").Append(expr.GetType().Name).Append(':').Append(expr).Append(')');
                return;
        }
    }

    // Canonical operand serialization. A parameter reference becomes P{ordinal}, renumbered by first occurrence so
    // that name-isomorphic queries ($x and $x vs $y and $y) match while genuinely distinct fan-outs ($x and $y) do
    // not - this mirrors the template's deduplicated ParameterSlots. A literal becomes L{typeCode}: the TYPE is kept
    // (the per-variant CacheKeyHash does not see literals, so the bucket must already segregate long/double/string/
    // bool/null), but the value is dropped so value variants collapse.
    private static void AppendCanonicalValue(StringBuilder sb, ValueExpression ve, Dictionary<string, int> paramOrdinals)
    {
        if (ve.Value == ValueTokenType.Parameter)
        {
            string name = ve.Token.Value;
            if (paramOrdinals.TryGetValue(name, out int ordinal) == false)
            {
                ordinal = paramOrdinals.Count;
                paramOrdinals.Add(name, ordinal);
            }
            sb.Append('P').Append(ordinal);
        }
        else
        {
            sb.Append('L').Append((int)ve.Value);
        }
    }

    // Canonical ORDER BY serialization. The template bakes the resolved sort shape (field, ordering type,
    // direction, nulls mode, and any literal random seed / distance coordinates), so the structural key must
    // preserve all of it. Argument values are kept verbatim - literal seeds/coords are baked into the template's
    // prebuilt/patch closures and must not be shared across buckets; parameter arguments render by name (the value
    // is resolved at runtime), so parameter-value variants still collapse. A null ORDER BY is kept distinct from an
    // empty one.
    private static void AppendCanonicalOrderBy(StringBuilder sb, OrderByField[] orderBy)
    {
        sb.Append("O[");
        if (orderBy == null)
        {
            sb.Append('_').Append(']');
            return;
        }

        sb.Append(orderBy.Length).Append(':');
        foreach (var field in orderBy)
        {
            sb.Append(field.Name?.Value)
                .Append('|').Append((int)field.OrderingType)
                .Append('|').Append(field.Ascending ? '1' : '0')
                .Append('|').Append((int)field.NullsOrdering)
                .Append('|').Append(field.Method.HasValue ? ((int)field.Method.Value).ToString() : "_")
                .Append('|');
            if (field.Arguments == null)
            {
                sb.Append('_');
            }
            else
            {
                sb.Append(field.Arguments.Length).Append('(');
                foreach (var arg in field.Arguments)
                {
                    sb.Append((int)arg.Type).Append(':').Append(arg.NameOrValue).Append(',');
                }
                sb.Append(')');
            }
            sb.Append(';');
        }
        sb.Append(']');
    }

    /// <summary>
    /// This gets the query match without any sorting. This is used by callers who care about the results but not the order.
    /// For example, facets, more-like-this, etc.
    /// </summary>
    public static IQueryMatch BuildFilterMatch(
        PlanParameters planParams,
        QueryBuilderParameters builderParameters,
        out QueryExecution exec,
        out CompiledPlan compiledPlanOut,
        Dictionary<string, CoraxHighlightingTermIndex> highlightingTerms,
        bool wantTimings,
        CancellationToken token)
    {
        var indexSearcher = planParams.IndexSearcher;
        var walkerCtx = new ResolutionContext(builderParameters);

        var template = BuildTemplate(planParams);

        (compiledPlanOut, exec) = Build(template, planParams, builderParameters, walkerCtx);
        if (compiledPlanOut == null)
            return TermMatch.CreateEmpty(indexSearcher, indexSearcher.Allocator);

        return InstantiateBitmapPipeline(compiledPlanOut, exec, planParams, builderParameters, walkerCtx, highlightingTerms, wantTimings, token);
    }

    public static CompiledQuery BuildSortedQuery(PlanParameters planParams,
        QueryBuilderParameters builderParameters,
        Dictionary<string, CoraxHighlightingTermIndex> highlightingTerms,
        bool wantTimings,
        CancellationToken token)
    {
        var indexSearcher = planParams.IndexSearcher;
        var walkerCtx = new ResolutionContext(builderParameters);

        var template = BuildTemplate(planParams);

        var (plan, exec) = Build(template, planParams, builderParameters, walkerCtx);
        if (plan == null)
        {
            var emptyMatch = TermMatch.CreateEmpty(indexSearcher, indexSearcher.Allocator);
            return new(emptyMatch, emptyMatch, null, null, null, builderParameters, null);
        }

        var orderByFields = GetSortMetadata(builderParameters, plan.Template);
        // A single vector-search post-filter already streams its HNSW output in similarity-score order. When that
        // score order is exactly what the query asks for (no ORDER BY auto-promoted to score, or an explicit
        // ORDER BY score()), the implicit SortingMatch wrapper is pure overhead — record that here so the vector
        // match streams score order (ApplyPostFilters) and the wrapper is skipped (Instantiate). The order-agnostic
        // BuildFilterMatch path never reaches this, so facets / MLT keep the entry-id-sorted vector output.
        exec.VectorPostFilterProvidesScoreOrder = VectorPostFilterProvidesResultOrder(exec, builderParameters, orderByFields);
        var queryMatch = Instantiate(plan, exec, orderByFields,
            planParams, builderParameters, walkerCtx, highlightingTerms, wantTimings, out var innerMatch, token);
        return new(queryMatch, innerMatch, queryMatch == innerMatch ? null : queryMatch, plan, exec, builderParameters, orderByFields);
    }


    private static (CompiledPlan, QueryExecution) Build(PlanTemplate template, PlanParameters planParams, QueryBuilderParameters builderParameters, ResolutionContext walkerCtx)
    {
        Span<byte> scratch = stackalloc byte[128];
        return new BuildResolver(template, planParams, builderParameters, walkerCtx, scratch).Resolve();
    }

    internal static ClauseExecution CreateExecution(ClauseInfo clause)
    {
        RuntimeHelpers.EnsureSufficientExecutionStack();
        var exec = new ClauseExecution(clause);

        if (clause.SubClauses is null)
            return exec;

        exec.SubExecutions = new List<ClauseExecution>(clause.SubClauses.Count);
        foreach (var it in clause.SubClauses)
        {
            exec.SubExecutions.Add(CreateExecution(it));
        }

        return exec;
    }

    /// <summary>Marker bit OR-ed into a sentinel-bound parameter's FullKinds byte (kind occupies bits 0-1).
    /// Forces a distinct plan-cache entry for parameter-bound BETWEEN sentinels — see ComputeTypeSignature.</summary>
    private const byte SentinelParamMark = 1 << 2;

    /// <summary>Mark a parameter-bound BETWEEN sentinel's slot in the FullKinds carrier, lazily allocating it on first use.
    /// No-op for literal/deferred bounds (ParameterSlot == -1 — the sentinel is encoded in the query text, no marker needed).</summary>
    private static void MarkSentinel(ref byte[] full, int parameterSlotCount, ParameterBinding binding)
    {
        if (binding.ParameterSlot < 0 || parameterSlotCount is 0)
            return;
        full ??= new byte[parameterSlotCount];
        full[binding.ParameterSlot] |= SentinelParamMark;
    }

    /// <summary>Redirect a template binding to its counterpart in the per-query slot vector via the binding's
    /// canonical <see cref="ParameterBinding.ValueOrdinal"/>. The slot binding carries this query text's actual
    /// literal value / parameter name / deferred expression, while the template binding only carries structure.
    /// Returns the binding unchanged when there is no slot vector or no value ordinal (defensive: any binding not
    /// produced by <c>CreateBinding</c>), and is idempotent on bindings already drawn from the slot vector.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static ParameterBinding SlotBindingFor(ParameterBinding binding, ParameterBinding[] slotBindings)
    {
        if (slotBindings != null && binding.ValueOrdinal >= 0)
            return slotBindings[binding.ValueOrdinal];
        return binding;
    }

    internal static void PopulateClauseValues(ClauseExecution exec, ParameterBinding[] slotBindings, BlittableJsonReaderObject queryParameters, ValueWriter writer, QueryBuilderParameters builderParameters,
        int parameterSlotCount, ref byte[] full)
    {
        RuntimeHelpers.EnsureSufficientExecutionStack();
        foreach (var it in exec.SubExecutions ?? [])
        {   // Always recurse into subclauses first (OrGroup/AndGroup have no binding of their own)
            PopulateClauseValues(it, slotBindings, queryParameters, writer, builderParameters, parameterSlotCount, ref full);
        }

        if (exec.Clause is { HasBoost: true, Bindings.Length: > 0 })
        {
            ResolveBoostFactor(exec, slotBindings, queryParameters);
        }

        switch (exec.Clause.ClauseType) // Spatial and vector resolve via their binding array.
        {
            case ClauseType.Spatial when exec.Clause.Bindings is { Length: > 0 }:
                ResolveSpatialFromBindings(exec, slotBindings, queryParameters);
                return;
            case ClauseType.Vector when exec.Clause.Bindings is { Length: > 0 }:
                ResolveVectorFromBindings(exec, slotBindings, queryParameters);
                return;
        }

        if (exec.Clause.Bindings is not { Length: > 0 })
            return;

        var bindings = exec.Clause.Bindings;
        switch (exec.Clause.ClauseType)
        {
            case ClauseType.Between: // BETWEEN: open-range "*"/"NULL" sentinel bounds (literal or parameter-bound) are detected here and rewritten to the equivalent half-open range / match-all leaf.
            {
                var (low, lowType) = ResolveBindingScalar(bindings[BindingIndex.BetweenLow], slotBindings, queryParameters, builderParameters);
                var (high, highType) = ResolveBindingScalar(bindings[BindingIndex.BetweenHigh], slotBindings, queryParameters, builderParameters);
                bool lowIsSentinel = low is RavenConstants.Documents.Querying.Terms.LeftNullValueOfBetweenQuery;
                bool highIsSentinel = high is RavenConstants.Documents.Querying.Terms.RightNullValueOfBetweenQuery;
                switch (lowIsSentinel, highIsSentinel)
                {
                    case (true, true):
                        exec.SentinelRewriteType = ClauseType.Exists;
                        MarkSentinel(ref full, parameterSlotCount, bindings[BindingIndex.BetweenLow]);
                        MarkSentinel(ref full, parameterSlotCount, bindings[BindingIndex.BetweenHigh]);
                        return;
                    case (true, false):
                        exec.SentinelRewriteType = ClauseType.LessThanOrEqual;
                        MarkSentinel(ref full, parameterSlotCount, bindings[BindingIndex.BetweenLow]);
                        exec.TermValueType = highType;
                        exec.PackedParamValue = writer.Add(high, ToValueTokenType(highType));
                        return;
                    case (false, true):
                        exec.SentinelRewriteType = ClauseType.GreaterThanOrEqual;
                        MarkSentinel(ref full, parameterSlotCount, bindings[BindingIndex.BetweenHigh]);
                        exec.TermValueType = lowType;
                        exec.PackedParamValue = writer.Add(low, ToValueTokenType(lowType));
                        return;
                    case (false, false):
                        exec.TermValueType = lowType;
                        exec.PackedParamValue = writer.AddPair(low, high, ToValueTokenType(lowType));
                        return;
                }
            }
            case ClauseType.In or ClauseType.AllIn:
                Span<ParameterBinding> inBindings =  bindings;
                if(exec.Clause.HasBoost)
                {   // Boosted clauses store the boost factor in the trailing binding (read by ResolveBoostFactor via Bindings[^1]); exclude it from the IN-term walk.
                    inBindings = inBindings[..^1];
                }
                ResolveInFromBindings(exec, slotBindings, queryParameters, writer, inBindings, builderParameters);
                break;
            default: // Simple clause (Equals, Range, Search, Regex, etc.): single value at Bindings[0]
                var (value, valueType) = ResolveBindingScalar(bindings[BindingIndex.Value], slotBindings, queryParameters, builderParameters);
                if (value == null && exec.Clause.ClauseType is ClauseType.StartsWith or ClauseType.EndsWith or ClauseType.Search or ClauseType.Regex)
                {
                    throw new InvalidQueryException(  // reject null (matches Lucene behavior).
                        $"Method {exec.Clause.ClauseType}() expects to get an argument of type String while it got Null");
                }

                exec.TermValueType = valueType;
                exec.PackedParamValue = writer.Add(value, ToValueTokenType(valueType));
                break;
        }
    }


    private static void EmitInTerms(ClauseExecution exec, ValueWriter writer, ParamValueType dominantType, List<object> values, bool hasNullTerm)
    {
        var (packedType, startIdx) = writer.ResolveInSlot(dominantType);
        var dominantTokenType = ToValueTokenType(dominantType);

        int written = 0;
        for (int i = 0; i < values.Count; i++)
        {
            // Mixed-type IN: (IN [long, "Shalom"]). Silently drop it instead of throwing, Matches Lucene's behavior.
            if (writer.TryAdd(values[i], dominantTokenType) is null)
                continue;
            written++;
        }

        exec.PackedParamValue = new PackedParam(packedType, startIdx);
        exec.InTermCount = written;
        exec.HasNullTerm = hasNullTerm;
    }

    private static (object Value, ParamValueType Type) ResolveBindingScalar(ParameterBinding binding, ParameterBinding[] slotBindings, BlittableJsonReaderObject queryParameters, QueryBuilderParameters builderParameters)
    {
        // The template binding supplies only structure plus its canonical ValueOrdinal. The value for THIS query's
        // text lives in the per-query slot vector at that ordinal, so value/name/param variants can share one
        // shared template. Redirect to the slot binding before reading any value. (Already-slot bindings are
        // idempotent under this lookup, since slotBindings[b.ValueOrdinal] == b.)
        binding = SlotBindingFor(binding, slotBindings);
        switch (binding.Source)
        {
            case BindingSource.Literal:
                return (binding.LiteralValue, binding.LiteralType);

            case BindingSource.DeferredMethod:
            {
                var value = binding.DeferredExpression(builderParameters, queryParameters);
                if (value == null)
                    return (null, ParamValueType.Null);
                var (val, valType) = ResolveParameterValue(value);
                return (val, ToParamValueType(valType));
            }

            case BindingSource.QueryParameter:
            default:
                if (queryParameters == null) // query text references $param but no parameters were supplied
                    QueryBuilderHelper.ThrowParametersWereNotProvided(builderParameters?.Metadata?.QueryText);

                if (queryParameters.TryGet(binding.ParameterName, out object raw) == false) // referenced parameter is absent from the supplied set
                    QueryBuilderHelper.ThrowParameterValueWasNotProvided(binding.ParameterName, builderParameters?.Metadata?.QueryText, queryParameters);

                if (raw == null) // explicit null value is allowed (matches null terms)
                    return (null, ParamValueType.Null);

                var (paramVal, paramType) = ResolveParameterValue(raw);
                return (paramVal, ToParamValueType(paramType));
        }
    }

    private static void ResolveBoostFactor(ClauseExecution exec, ParameterBinding[] slotBindings, BlittableJsonReaderObject queryParameters)
    {
        var (boostVal, boostType) = ResolveBindingScalar(exec.Clause.Bindings[^1], slotBindings, queryParameters, builderParameters: null);
        if (boostVal == null) return;

        exec.BoostFactor = boostType switch
        {
            ParamValueType.Double => (float)(double)boostVal,
            _ => boostType switch
            {
                ParamValueType.Long => (long)boostVal,
                _ when float.TryParse(boostVal.ToString(), NumberStyles.Float, CultureInfo.InvariantCulture, out float parsed) => parsed,
                _ => 1f
            }
        };
    }

    /// <summary>
    /// Foo BETWEEN $x AND $y - where $x > $y - returns nothing, this collapses the clause to a
    /// MatchNothing sentinel so the plan emitter bakes an empty bitmap for it.
    /// </summary>
    internal static void PropagateBetweenContradiction(ClauseExecution exec, ValueWriter writer)
    {
        var p = exec.PackedParamValue;
        if (exec.Clause.ClauseType != ClauseType.Between || p.Param2 is PackedParam.NoParamValue)
            return;

        bool contradictory = p.ValueType switch
        {
            PackedParam.TypeLong => writer.GetLong(p.Param1) > writer.GetLong(p.Param2),
            PackedParam.TypeDouble => writer.GetDouble(p.Param1) > writer.GetDouble(p.Param2),
            _ => false // for strings, we have to consider analyzers, so we can't tell
        };
        if (!contradictory)
            return;

        exec.MarkAsSentinel(ClauseType.MatchNothing, 0);
    }

    private static IQueryMatch InstantiateBitmapPipeline(
        CompiledPlan compiledPlan,
        QueryExecution exec,
        PlanParameters planParams,
        QueryBuilderParameters builderParameters,
        ResolutionContext walkerCtx,
        Dictionary<string, CoraxHighlightingTermIndex> highlightingTerms,
        bool wantTimings,
        CancellationToken token)
    {
        var indexSearcher = planParams.IndexSearcher;

        // Spatial / Vector queries with no other clauses ( WHERE spatial.within() / WHERE vector.search() )
        // use a dedicated code path to avoid AllEntries + post-filters
        if (exec is { IsAllEntries: true, HasSpatialOrVector: true })
            return InstantiateAllEntriesPostFilter(exec, builderParameters, walkerCtx, wantTimings);

        var (resolvedMatches, leaves) = ResolveAllSlots(exec, walkerCtx, planParams.HasBoost);

        if (highlightingTerms != null)
            PopulateHighlightingTerms(exec, highlightingTerms, planParams.Metadata);

        var compiledMatch = new CompiledQueryMatch(
            compiledPlan, exec, compiledPlan.RequiredBitmaps, compiledPlan.OpCount, resolvedMatches, leaves,
            indexSearcher, planParams.Allocator, wantTimings, token)
        {
            InRangeCounts = exec.InRangeCounts,
            Cardinalities = exec.Cardinalities,
        };

        if (exec.Plan.EntryScanSet is { HasPredicates: true })
        {
            exec.PopulateScanParams = () => ScanParamExtractor.Extract(exec, indexSearcher, walkerCtx, exec.Plan.EntryScanSet);
        }

        IQueryMatch[] spatialMatches = null;
        if (exec.SpatialFilters is { Length: > 0 })
        {
            spatialMatches = new IQueryMatch[exec.SpatialFilters.Length];
            for (int sf = 0; sf < exec.SpatialFilters.Length; sf++)
                spatialMatches[sf] = resolvedMatches[exec.SpatialFilters[sf].MatchIndex];
        }

        return ApplyPostFilters(compiledMatch, spatialMatches, exec, builderParameters, wantTimings);
    }

    private static IQueryMatch ApplyPostFilters(
        IQueryMatch source, IQueryMatch[] spatialMatches,
        QueryExecution exec, QueryBuilderParameters builderParameters, bool wantTimings)
    {
        IQueryMatch result = source;

        if (spatialMatches is { Length: > 0 })
        {
            // These spatial matches were lifted to top-level post-filters by the planner (AND context). Record the
            // role on each one so inspection reports it as a post-filter rather than re-deriving it from the type
            // (a spatial leaf inside an OR is NOT a post-filter — see IPostFilterMatch).
            foreach (var spatialMatch in spatialMatches)
            {
                if (spatialMatch is IPostFilterMatch postFilter)
                    postFilter.IsPostFilter = true;
            }

            result = result is null
                ? new PostFilterMatch(spatialMatches[0], spatialMatches.Length is 1 ? [] : spatialMatches[1..], wantTimings)
                : new PostFilterMatch(result, spatialMatches, wantTimings);
        }

        if (exec.VectorSelects is { Length: > 0 })
        {
            foreach (var item in ResolveVectorItems(exec, builderParameters))
            {
                result = item.Materialize(result, isPostFilter: true, streamScoreOrder: exec.VectorPostFilterProvidesScoreOrder);
            }
        }

        return result;
    }

    /// <summary>
    /// Bypass path for queries with no real WHERE clauses — only spatial filters and/or  vector selects. 
    /// </summary>
    private static IQueryMatch InstantiateAllEntriesPostFilter(QueryExecution exec, QueryBuilderParameters builderParameters, ResolutionContext walkerCtx, bool wantTimings)
    {
        // No real WHERE clause, so the spatial clauses aren't in resolvedMatches — resolve them directly.
        IQueryMatch[] spatialMatches = null;
        if (exec.SpatialFilters is { Length: > 0 })
        {
            spatialMatches = new IQueryMatch[exec.SpatialFilters.Length];
            for (int i = 0; i < exec.SpatialFilters.Length; i++)
                spatialMatches[i] = ResolveClause(exec.SpatialFilters[i].Exec, exec, walkerCtx);
        }

        return ApplyPostFilters(source: null, spatialMatches, exec, builderParameters, wantTimings);
    }


    
    public static IQueryMatch BuildQueryForMoreLikeThis(QueryBuilderParameters builderParams, QueryExpression expression)
    {
        const string moreLikeThisCacheKeyPrefix = "$mlt$:";

        return BuildFilterMatch(new PlanParameters
        {
            IndexSearcher = builderParams.IndexSearcher,
            Metadata = builderParams.Query.Metadata,
            QueryParameters = builderParams.QueryParameters,
            Index = builderParams.Index,
            IndexFieldsMapping = builderParams.IndexFieldsMapping,
            Allocator = builderParams.Allocator,
            HasDynamics = builderParams.HasDynamics,
            DynamicFields = builderParams.DynamicFields,
            HasBoost = builderParams.HasBoost,
            WhereOverride = expression,
            // The cache key must capture the expression STRUCTURE (parameter names like $p0), not the
            // bound values: the compiled plan reads its operands from QueryParameters by name at
            // instantiation, so two MLT queries whose base-document expression differs only in the bound
            // value (e.g. id() = 'users/1' vs id() = 'users/2') legitimately share a plan, while two
            // queries that resolve to the same value but reference different parameter names (e.g.
            // id() = $p1 with options vs id() = $p0 without) must NOT share — otherwise the cached plan
            // reads the wrong parameter slot. GetTextWithAlias(parent: null) renders parameters as $pN.
            CacheKeyOverride = moreLikeThisCacheKeyPrefix + expression.GetTextWithAlias(parent: null),
        }, builderParams, out _, out _, highlightingTerms: null, wantTimings: false, builderParams.Token);
    }

    
    private static bool TryCreateCompoundExactMatch(ref InstCtx ctx, out string rejectReason)
    {
        // The only thing still unknown is value-dependent — a bound parameter can resolve to "none" (null/missing), which has no composite-key encoding.
        if (ctx.Exec.CompoundExactFirst.PackedParamValue.IsNone || 
            ctx.Exec.CompoundExactSecond.PackedParamValue.IsNone)
        {
            rejectReason = "a compound-pair value resolved to none and has no composite-key encoding";
            return false;
        }

        rejectReason = null;
        return true;
    }

    private static IQueryMatch ConstructCompoundExact(ref InstCtx ctx)
    {
        var indexSearcher = ctx.PlanParams.IndexSearcher;
        var eA = ctx.Exec.CompoundExactFirst;
        var eB = ctx.Exec.CompoundExactSecond;

        var (firstField, secondField, firstExec, secondExec) = ctx.Exec.Plan.Template.CompoundExactAFirst
            ? (eA.Clause.ResolvedFieldName ?? eA.Clause.FieldName, eB.Clause.ResolvedFieldName ?? eB.Clause.FieldName, eA, eB)
            : (eB.Clause.ResolvedFieldName ?? eB.Clause.FieldName, eA.Clause.ResolvedFieldName ?? eA.Clause.FieldName, eB, eA);
        
        if (TryGetCompoundFieldEncoding(ref ctx, firstField, firstExec.PackedParamValue, firstExec.PackedParamValue.Param1, out var enc1) == false || 
            TryGetCompoundFieldEncoding(ref ctx, secondField, secondExec.PackedParamValue, secondExec.PackedParamValue.Param1, out var enc2) == false)
            return null;

        int totalLen = enc1.Size + enc2.Size + 1;
        if (totalLen > Constants.Terms.MaxLength) 
            return null;

        ctx.PlanParams.Allocator.Allocate(totalLen, out ByteString keyBuf);
        var keySpan = keyBuf.ToSpan();
        WriteCompoundFieldEncoding(keySpan.Slice(0, enc1.Size), enc1, ctx.Exec);
        WriteCompoundFieldEncoding(keySpan.Slice(enc1.Size, enc2.Size), enc2, ctx.Exec);
        keySpan[totalLen - 1] = (byte)enc1.Size;

        var compoundFieldMeta = indexSearcher.FieldMetadataBuilder(ctx.Exec.Plan.Template.CompoundExactName, hasBoost: false);

        return indexSearcher.TermQuery(compoundFieldMeta, new Slice(keyBuf));
    }

    private static bool TryCreateCompoundFieldMatch(ref InstCtx ctx, out string rejectReason)
    {
        if (ctx.Exec.CompoundFieldDrivingClause is null || ctx.Exec.Plan.Template.CompoundFieldSortName is null)
        {
            rejectReason = "no compound-field candidate identified at template time";
            return false;
        }

        if (ctx.Exec.Plan.AllNegated)
        {
            rejectReason = "all clauses are negated";
            return false;
        }

        var driving = ctx.Exec.CompoundFieldDrivingClause;
        var field2Range = ctx.Exec.CompoundFieldField2Range;
        var execs = ctx.Exec.Executions;
        for (int i = 0; i < execs.Count; i++)
        {
            if (ReferenceEquals(execs[i], driving) || ReferenceEquals(execs[i], field2Range))
                continue;
            if (IsClauseBoosted(execs[i]))
            {
                rejectReason = "boosted clause found";
                return false;
            }
        }

        if (ctx.Exec.Plan.CompoundFieldResidualSet is null)
        {
            rejectReason = "scan predicate info is null";
            return false;
        }

        rejectReason = null;
        return true;
    }

    private static Slice BuildField1Prefix(ref InstCtx ctx, string field1Name, PackedParam packed, out string field1ValueStrForIntrospection)
    {
        var indexSearcher = ctx.PlanParams.IndexSearcher;
        switch (packed.ValueType)
        {
            case PackedParam.TypeString:
            {
                field1ValueStrForIntrospection = ctx.Exec.StringValues[packed.Param1];
                var field1Meta = QueryBuilderHelper.GetFieldMetadata(in ctx.BuilderParams, field1Name, hasBoost: false);
                return ctx.Exec.GetAnalyzedSlice(indexSearcher, field1Meta, packed.Param1);
            }
            case PackedParam.TypeLong:
            {
                // skip the ToString allocation unless this is an inspected query.
                field1ValueStrForIntrospection = ctx.WantTimings ? ctx.Exec.LongValues[packed.Param1].ToString() : null;
                ctx.PlanParams.Allocator.Allocate(sizeof(long), out ByteString buf);
                EncodeNumericValue(buf.ToSpan(), PackedParam.TypeLong, packed.Param1, ctx.Exec);
                return new Slice(buf);
            }
            case PackedParam.TypeDouble:
            {
                field1ValueStrForIntrospection = ctx.WantTimings ? ctx.Exec.DoubleValues[packed.Param1].ToString(CultureInfo.InvariantCulture) : null;
                ctx.PlanParams.Allocator.Allocate(sizeof(long), out ByteString buf);
                EncodeNumericValue(buf.ToSpan(), PackedParam.TypeDouble, packed.Param1, ctx.Exec);
                return new Slice(buf);
            }
            default:
                field1ValueStrForIntrospection = null;
                return default;
        }
    }

    private static bool TryCreateSimpleFieldDirectScan(ref InstCtx ctx, out string rejectReason)
    {
        if (ctx.OrderByFields is not { Length: not 0 })
        {
            rejectReason = "no ORDER BY fields";
            return false;
        }

        if (ctx.OrderByFields.Length > 2)
        {
            rejectReason = "ORDER BY has too many fields (max 2 for direct scan)";
            return false;
        }

        bool hasTieBreak = ctx.OrderByFields.Length == 2;
        if (hasTieBreak)
        {
            var tieBreakType = ctx.OrderByFields[1].FieldType;
            if (tieBreakType is not (MatchCompareFieldType.Integer or MatchCompareFieldType.Floating or MatchCompareFieldType.Sequence))
            {
                rejectReason = "tie-break field type isn't numeric or string";
                return false;
            }
        }

        var execs = ctx.Exec.Executions;
        bool isFullScan = execs is not { Count: not 0 };

        if (isFullScan)
        {
            if (ctx.Exec.Plan.AllNegated)
            {
                rejectReason = "all clauses are negated";
                return false;
            }
            if (ctx.OrderByFields[0].MayHaveMissingEntries)
            {
                rejectReason = "sort field may have missing entries";
                return false;
            }
            if (ctx.OrderByFields[0].FieldType is not (MatchCompareFieldType.Sequence or MatchCompareFieldType.Integer or MatchCompareFieldType.Floating))
            {
                rejectReason = "full-scan sort field type is not numeric or string";
                return false;
            }
            rejectReason = null;
            return true;
        }

        if (ctx.Exec.SortDrivingClause is null)
        {
            rejectReason = "no range/equals clause on sort field (or WHEN eliminated the candidate)";
            return false;
        }

        if (ctx.Exec.Plan.DirectScanResidualSet is null)
        {
            rejectReason = "non-scannable residual clause";
            return false;
        }

        rejectReason = null;
        return true;
    }

    private static bool ResolveNullFirst(in OrderMetadata orderByField, NullsSortMode indexDefault, bool forward)
    {
        bool nullIsSmallest = (orderByField.NullsSortMode ?? indexDefault) == NullsSortMode.NullsSmallest;
        return forward ? nullIsSmallest : nullIsSmallest is false;
    }

    /// <summary>
    /// Resolves the entry budget for a sorted index-only scan. Normally the driving match yields entries already in
    /// ORDER BY order, so the first <c>take</c> (= pageSize + start) survivors ARE the answer and the scan can stop
    /// early. Two situations break that assumption and require streaming the whole sorted tree (TakeAll):
    /// <list type="bullet">
    /// <item>A server-side <c>filter</c> clause is applied AFTER the index produces results, so an entry the tree
    /// yields is only a *candidate* — the index must keep streaming until the filter has accepted enough (bounded
    /// server-side by FilterLimit), else filtered+sorted queries truncate before reaching matching documents.</item>
    /// <item>The client requested statistics (<c>SkipStatistics == false</c>) or this is a count query: the read
    /// operation needs the exact <c>TotalResults</c>, which it derives by draining the match. Early-stopping at
    /// <c>take</c> would report only the page-sized prefix as the total. For a no-residual full scan this drain
    /// reads no entries (it just enumerates ids), matching the old SortingMatch behaviour.</item>
    /// </list>
    /// </summary>
    private static int ResolveSortedScanTake(QueryBuilderParameters builderParams)
    {
        if (builderParams?.Metadata?.Query?.Filter != null)
            return Constants.IndexSearcher.TakeAll;

        if (builderParams?.Query is { IsCountQuery: true } or { SkipStatistics: false })
            return Constants.IndexSearcher.TakeAll;

        return builderParams?.Take ?? Constants.IndexSearcher.TakeAll;
    }

    /// <summary>
    /// The page size the residual-DirectScan cost model is allowed to assume. A residual scan only
    /// early-terminates at the page boundary when the executor's take is page-bounded; <see cref="ResolveSortedScanTake"/>
    /// returns <c>TakeAll</c> whenever a total-result count must be reported (<c>SkipStatistics == false</c>, the
    /// default), the query is a count query, or a post-filter is present. In those cases the scan enumerates every
    /// matching entry — doing a stored-entry read per entry — so the page no longer bounds the work. Modelling the
    /// page bound there would let the cost gate price a handful of reads when the scan actually reads the whole
    /// driving tree, so report the full matching set (<see cref="long.MaxValue"/>, clamped to the driving
    /// cardinality by the caller) instead.
    /// </summary>
    private static long ResolveEffectiveScanPageSize(QueryBuilderParameters builderParams)
    {
        return ResolveSortedScanTake(builderParams) == Constants.IndexSearcher.TakeAll
            ? long.MaxValue
            : builderParams.Query.PageSize;
    }

    private static string DescribeUnboundedScanTake(QueryBuilderParameters builderParams)
    {
        return builderParams switch
        {
            { Metadata.Query.Filter: not null } => "post-filter present", 
            { Query.IsCountQuery: true } => "count query",
            { Query.SkipStatistics: false } => "statistics requested (SkipStatistics=false, requires count)",
            _ => null
        };
    }

    private static IQueryMatch BuildSortedDrivingWithTieBreakMatch(InstCtx ctx, ITermsProvider provider, LowLevelTransaction llt, NullsSortMode indexDefaultNullsSortMode,
        IndexSearcher indexSearcher, bool nullFirst)
    {
        bool secondaryNullIsSmallest = (ctx.OrderByFields[1].NullsSortMode ?? indexDefaultNullsSortMode) == NullsSortMode.NullsSmallest;
        int take = ResolveSortedScanTake(ctx.BuilderParams);
        return new SortedDrivingWithTieBreakMatch(
            provider, llt, ctx.PlanParams.Allocator, indexSearcher,
            ctx.OrderByFields[0].Field, ctx.OrderByFields[1].Field,
            ctx.OrderByFields[1].FieldType, secondaryDescending: !ctx.OrderByFields[1].Ascending,
            nullFirst: nullFirst, nullIsSmallest: secondaryNullIsSmallest,
            take: take);
    }

    private static (IQueryMatch[], LeafResolveInfo[]) ResolveAllSlots(QueryExecution exec, ResolutionContext walkerCtx, bool planHasBoost)
    {
        Debug.Assert((exec.IsAllEntries && exec.HasSpatialOrVector) is false);

        if (exec.IsAllEntries) // nothing to do here
            return ( [walkerCtx.IndexSearcher.AllEntries()], [new LeafResolveInfo { Kind = LeafResolveKind.PreResolved }]);

        if (exec.Executions is not { Count: > 0 })
            return ([], []);

        var matchList = new List<IQueryMatch>();
        var leafList = new List<LeafResolveInfo>();
        foreach (var clauseExec in exec.Executions)
        {
            ResolveLeafIntoAll(walkerCtx, clauseExec, exec, planHasBoost, matchList, leafList);
        }

        return (matchList.ToArray(), leafList.ToArray());
    }

    private static IQueryMatch ResolveSentinelRewrittenBetween(ClauseExecution exec, FieldMetadata fieldMeta,
        IndexSearcher indexSearcher, QueryExecution queryExec)
    {
        if (exec.SentinelRewriteType == ClauseType.Exists)
            return indexSearcher.AllEntries();
        if (exec.SentinelRewriteType == ClauseType.LessThanOrEqual)
            return exec.PackedParamValue.RangeQuery(ClauseType.LessThanOrEqual, fieldMeta, indexSearcher, queryExec);

        Debug.Assert(exec.SentinelRewriteType == ClauseType.GreaterThanOrEqual);
        IQueryMatch rangeMatch = exec.PackedParamValue.RangeQuery(ClauseType.GreaterThanOrEqual, fieldMeta, indexSearcher, queryExec);
        if (indexSearcher.TryGetPostingListForNull(in fieldMeta, out _) is false) 
            return rangeMatch;
        
        // BETWEEN low AND 'NULL' must include null-valued docs (Lucene parity)
        return new LazyOrMatch(indexSearcher.Allocator, rangeMatch, indexSearcher.TermQuery(fieldMeta, null));
    }

    private static IQueryMatch ResolveInTerm(ClauseExecution exec, int termIndex, QueryExecution queryExec, ResolutionContext walkerCtx)
    {
        FieldMetadata fieldMeta = ResolveFieldMetadata(exec.Clause, walkerCtx);
        var termPacked = exec.PackedParamValue.WithTermOffset(termIndex);
        return termPacked.TermQuery(fieldMeta, walkerCtx.IndexSearcher, queryExec);
    }

    internal static FieldMetadata ResolveFieldMetadata(ClauseInfo clause, ResolutionContext walkerCtx)
    {
        var builderParams = walkerCtx.BuilderParams;
        string resolvedFieldName = clause.ResolvedFieldName ?? clause.FieldName;
        bool forceSearchAnalyzer = builderParams.HasDynamics
                                   && !clause.IsExact
                                   && clause.ClauseType != ClauseType.Search
                                   && builderParams.Index.Configuration.UseSearchAnalyzerForDynamicFieldsIfNotSetExplicitlyInSearchQuery;
        
        return QueryBuilderHelper.GetFieldMetadata(in builderParams, resolvedFieldName, exact: clause.IsExact,
            hasBoost: builderParams.HasBoost, forceDefaultSearchAnalyzer: forceSearchAnalyzer);
    }

    private static bool IsClauseBoosted(ClauseExecution exec)
        => exec.Clause.HasBoost || exec.BoostFactor > 0;

    private static void EncodeNumericValue(Span<byte> dest, int valueType, int paramIdx, QueryExecution exec)
    {
        long raw = valueType == PackedParam.TypeDouble
            ? Bits.DoubleToSortableLong(exec.DoubleValues[paramIdx])
            : exec.LongValues[paramIdx];
        // Must produce byte-for-byte the same key the indexer wrote for this value. The compound-field
        // indexer (CoraxDocumentConverterBase.AppendLong) stores `BitConverter.TryWriteBytes(buf, SwapBytes(l))`
        // — a little-endian write of the byte-swapped value, i.e. the big-endian (sortable) byte order of `l`.
        // Mirror that exactly; a big-endian write here would re-swap the bytes and the seek would never match
        // the indexed key (numeric compound members would silently return zero rows).
        BinaryPrimitives.WriteInt64LittleEndian(dest, Bits.SwapBytes(raw));
    }

    private struct CompoundFieldEncoding
    {
        public PackedParam Packed;
        public Slice Analyzed;
        public int SourceSlot;
        public int Size;
    }

    private static bool TryGetCompoundFieldEncoding(ref InstCtx ctx, string fieldName, PackedParam packed, int paramSlot, out CompoundFieldEncoding encoding)
    {
        encoding = default;
        encoding.Packed = packed;
        encoding.SourceSlot = paramSlot;

        switch (packed.ValueType)
        {
            case PackedParam.TypeString:
            {
                var meta = QueryBuilderHelper.GetFieldMetadata(in ctx.BuilderParams, fieldName, hasBoost: false);
                encoding.Analyzed = ctx.Exec.GetAnalyzedSlice(ctx.PlanParams.IndexSearcher, meta, paramSlot);
                encoding.Size = encoding.Analyzed.Size;
                return encoding.Size <= byte.MaxValue;
            }
            case PackedParam.TypeLong or PackedParam.TypeDouble:
                encoding.Size = sizeof(long);
                return true;
            default:
                return false;
        }
    }
    
    private static void WriteCompoundFieldEncoding(Span<byte> dest, CompoundFieldEncoding encoding, QueryExecution exec)
    {
        if (encoding.Packed.ValueType == PackedParam.TypeString)
        {
            encoding.Analyzed.CopyTo(dest);
            return;
        }
        EncodeNumericValue(dest, encoding.Packed.ValueType, encoding.SourceSlot, exec);
    }

    /// <summary>TreeScan-eligible: multi-term clauses with a direct ITermsProvider (StartsWith,
    /// EndsWith, Exists, Regex, ranges, BETWEEN). Boosted clauses go through QueryMatch for scoring.
    /// Sentinel-rewritten BETWEEN is handled by GetDispatch, not here, because it needs the
    /// per-execution SentinelRewriteType.</summary>
    internal static bool IsTreeScanEligibleClause(ClauseInfo clause)
    {
        if (clause.HasBoost)
            return false;

        return clause.ClauseType is ClauseType.StartsWith or ClauseType.EndsWith
            or ClauseType.Exists or ClauseType.Regex
            or ClauseType.GreaterThan or ClauseType.GreaterThanOrEqual
            or ClauseType.LessThan or ClauseType.LessThanOrEqual
            or ClauseType.Between;
    }

    /// <summary>Resolve the <see cref="MatchDispatch"/> mode for a clause execution at plan-build time.
    /// Equals / NotEquals (unboosted) → <c>PostingList</c>. Multi-term (unboosted) → <c>TreeScan</c>.
    /// All other clause types → <c>QueryMatch</c>. A sentinel-rewritten BETWEEN ("*"/"NULL" bounds)
    /// always takes the QueryMatch path: ResolveSentinelRewrittenBetween reads SentinelRewriteType at
    /// resolve time and may fold in the null posting list, so it cannot be expressed as a plain TreeScan.</summary>
    internal static MatchDispatch GetDispatch(ClauseExecution exec)
    {
        var clause = exec.Clause;
        if (clause is { HasBoost: false, ClauseType: ClauseType.Equals or ClauseType.NotEquals })
            return MatchDispatch.PostingList;

        if (exec.SentinelRewriteType != null)
            return MatchDispatch.QueryMatch;

        if (IsTreeScanEligibleClause(clause))
            return MatchDispatch.TreeScan;

        return MatchDispatch.QueryMatch;
    }

    private static string FormatValueFromPlan(PackedParam packed, QueryExecution exec, int idx)
    {
        if (idx is PackedParam.NoParamValue)
            return null;
        // An IN clause with all-null terms records InTermCount=0 and writes no values
        // to the typed arrays, but the packed Param1 still points at the (empty) slot.
        // Bounds-check before indexing — return null to indicate "no displayable value".
        return packed.ValueType switch
        {
            PackedParam.TypeLong => idx < exec.LongValues.Length ? exec.LongValues[idx].ToString() : null,
            PackedParam.TypeDouble => idx < exec.DoubleValues.Length ? exec.DoubleValues[idx].ToString(CultureInfo.InvariantCulture) : null,
            _ => idx < exec.StringValues.Length ? exec.StringValues[idx] : null
        };
    }
}
