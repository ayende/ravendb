using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Primitives;
using Voron.Data.RoaringBitmaps;
using Corax.Querying.Matches.SortingMatches.Meta;
using Corax.Querying.Planning;
using Corax.Mappings;
using Corax.Utils;
using Raven.Client.Documents.Indexes.Vector;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Corax;
using VectorOptions = Raven.Client.Documents.Indexes.Vector.VectorOptions;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;
using Raven.Server.Documents.Indexes.Persistence.Corax.QueryOptimizer;
using Raven.Server.Documents.Indexes.VectorSearch;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Spatial4n.Shapes;
using Sparrow;
using Sparrow.Json;
using Constants = Corax.Constants;
using RavenConstants = Raven.Client.Constants;
using SpatialUnits = Raven.Client.Documents.Indexes.Spatial.SpatialUnits;
using IndexSearcher = Corax.Querying.IndexSearcher;
using SpatialRelation = Corax.Utils.Spatial.SpatialRelation;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

/// <summary>
/// Per-execution resolution: compiles plans, resolves matches and term sources
/// from clause metadata, extracts typed scan parameters, handles highlighting,
/// sorting, and spatial/vector materialization.
///
/// Methods here run once per query execution (not cached).
/// </summary>
internal static partial class QueryPlanBuilder
{
    /// <summary>Clause count at or below which we use stackalloc for the
    /// indices buffer; larger counts fall back to ArrayPool.</summary>
    private const int StackallocClauseThreshold = 32;

    /// <summary>Sort comparer for clause cardinality ordering. Negated clauses
    /// sort last (must run after the positive set is established); ties broken
    /// by ascending cardinality so the cheapest clause runs first.
    /// Struct comparer to keep the sort path JIT-inlineable.</summary>
    private readonly struct ClauseCardinalityComparer(List<ClauseInfo> clauses, List<ClauseExecution> execList) : IComparer<int>
    {
        public int Compare(int a, int b)
        {
            bool aNeg = clauses[a].IsNegated || clauses[a].ClauseType == ClauseType.NotEquals;
            bool bNeg = clauses[b].IsNegated || clauses[b].ClauseType == ClauseType.NotEquals;
            if (aNeg != bNeg)
                return aNeg ? 1 : -1;
            return execList[a].Cardinality.CompareTo(execList[b].Cardinality);
        }
    }

    /// <summary>Maximum number of ORDER BY fields supported by Corax.</summary>
    private const int MaxSortFields = 16;

    // ── Entry points ─────────────────────────────────────────────────────

    /// <summary>
    /// Plan → compile → resolve pipeline. On cache hit (template exists for this query text),
    /// skips AST parsing — re-resolves parameter values from the blittable, re-estimates
    /// cardinality, re-sorts, and it looks up the compiled delegate by ordering.
    /// On cache miss, parses the AST into a template and caches it.
    /// Both paths then: populate → sort → emit → compile (if needed) → resolve.
    /// </summary>
    public static IQueryMatch BuildAndCompile(
        PlanParameters planParams,
        QueryBuilderParameters builderParameters,
        out QueryExecution plan,
        out CompiledPlan compiledPlanOut,
        Dictionary<string, CoraxHighlightingTermIndex> highlightingTerms,
        bool wantTimings,
        CancellationToken token)
    {
        var indexSearcher = planParams.IndexSearcher;

        // Phase 1: structural template (cached per queryText).
        var template = BuildTemplate(planParams);

        // Phase 2: parameter resolution, plan emission, IL compile (with cache miss handling).
        compiledPlanOut = Build(template, planParams, builderParameters, out plan, wantTimings);
        if (compiledPlanOut == null)
            return TermMatch.CreateEmpty(indexSearcher, indexSearcher.Allocator);

        // Phase 3: live binding (resolved matches, term sources, spatial/vector/highlighting wrappers).
        return Instantiate(compiledPlanOut, plan, planParams, builderParameters, highlightingTerms, wantTimings, token);
    }

    /// <summary>
    /// Full pipeline: build, compile, optimize, and apply ORDER BY — returns the final
    /// <see cref="IQueryMatch"/> ready for result iteration. Encapsulates the optimization
    /// dispatch (compound-exact, compound-field, direct-scan) that previously lived in
    /// <c>CoraxIndexReadOperation</c>. The <see cref="InstantiateHint"/> on the compiled plan
    /// caches which optimization succeeded on the first execution; subsequent cache-hit
    /// executions skip the Try* chain entirely.
    /// </summary>
    /// <param name="orderByFieldsOut">Resolved ORDER BY metadata, or null if no ORDER BY.
    /// Needed by the caller for result streaming (sort data transfer).</param>
    /// <param name="hasEmptySorts">True if any ORDER BY field had zero indexed terms
    /// (sharded scenario — caller must handle empty-sort placeholders).</param>
    /// <param name="innerMatchOut">The unwrapped inner match — same as the return value when no
    /// ORDER BY wrapper is applied (CompoundExact / DirectScan / no-ORDER-BY paths), or the
    /// pre-wrap match (compoundMatch or the original CompiledQueryMatch) when CompoundField /
    /// BitmapSort wraps with a SortingMatch. The caller uses this to dispose the IL-emitted
    /// compiled match deterministically and to build the inspection graph with the SortingMatch
    /// wrapper visible as the root.</param>
    public static IQueryMatch BuildCompileAndOptimize(
        PlanParameters planParams,
        QueryBuilderParameters builderParameters,
        out QueryExecution plan,
        out CompiledPlan compiledPlanOut,
        out OrderMetadata[] orderByFieldsOut,
        out bool hasEmptySorts,
        out IQueryMatch innerMatchOut,
        Dictionary<string, CoraxHighlightingTermIndex> highlightingTerms,
        bool wantTimings,
        CancellationToken token)
    {
        var queryMatch = BuildAndCompile(planParams, builderParameters, out plan, out compiledPlanOut,
            highlightingTerms, wantTimings, token);
        var disposable = queryMatch as IDisposable;
        innerMatchOut = queryMatch;

        orderByFieldsOut = GetSortMetadata(builderParameters, out hasEmptySorts);

        // Phase 5 (#4814 + #4830): split Discovery (compile-miss) from Construction
        // (per-execution). On a hint hit, dispatch straight to Construct* and skip
        // the Try*-chain discovery. On miss, run the full Try* chain — each Try*
        // performs discovery and, if it wins, calls its Construct* internally.
        var hint = compiledPlanOut?.Hint ?? InstantiateHint.NotEvaluated;
        if (hint != InstantiateHint.NotEvaluated)
        {
            // ── Fast path: cached hint, dispatch to Construct directly ──
            IQueryMatch built = null;
            switch (hint)
            {
                case InstantiateHint.CompoundExact:
                    if (plan != null)
                        built = ConstructCompoundExact(plan, planParams);
                    if (built != null)
                    {
                        disposable?.Dispose();
                        innerMatchOut = built;
                        return built;
                    }
                    break;
                case InstantiateHint.CompoundField:
                    if (plan != null && orderByFieldsOut != null)
                    {
                        int f2 = FindCompoundFieldField2Range(plan.Clauses, plan.CompoundFieldDrivingClause, plan.CompoundFieldSortName);
                        // Cost facts (entriesToScan, bitmapCost) are diagnostic-only inside
                        // Construct (used for the Reason string). Pass zeros — the cliff bit
                        // in the cache key already segregates cost buckets.
                        built = ConstructCompoundField(plan, orderByFieldsOut, planParams, builderParameters,
                            compiledPlanOut, f2, entriesToScan: 0, bitmapCost: 0);
                    }
                    if (built != null)
                    {
                        disposable?.Dispose();
                        innerMatchOut = built;
                        return OrderBy(builderParameters, built, orderByFieldsOut, hasEmptySorts);
                    }
                    break;
                case InstantiateHint.DirectScan:
                    if (plan != null && orderByFieldsOut != null && orderByFieldsOut.Length <= 2)
                    {
                        var clauses = plan.Clauses;
                        bool isFullScan = clauses == null || clauses.Count == 0;
                        bool hasTieBreak = orderByFieldsOut.Length == 2;
                        // Re-resolve drivingIdx (cheap structural lookup; cached at
                        // template time via SortDrivingClauseIndex). Skip cost gating —
                        // hint cache encodes the cost decision via cliff bit in Ordering.
                        int drivingIdx = -1;
                        if (isFullScan == false)
                        {
                            drivingIdx = plan.SortDrivingClauseIndex;
                            if (drivingIdx >= 0 && (plan.Executions[drivingIdx] is { BoostFactor: > 0 } ||
                                                    plan.Executions[drivingIdx].PackedParamValue.IsNone))
                                drivingIdx = -1;
                        }
                        if (isFullScan || drivingIdx >= 0)
                            built = ConstructDirectScan(plan, orderByFieldsOut, planParams, builderParameters,
                                compiledPlanOut, drivingIdx, isFullScan, hasTieBreak,
                                entriesToScan: 0, bitmapCost: 0);
                    }
                    if (built != null)
                    {
                        disposable?.Dispose();
                        innerMatchOut = built;
                        return built;
                    }
                    break;
            }

            // Cached hint failed at construction time (e.g. per-execution byte length
            // overflow). Fall through to the bitmap+sort fallback. The cached hint stays
            // valid for the next execution; this one had unlucky parameter values.
            if (orderByFieldsOut != null)
            {
                if (queryMatch is global::Corax.Querying.Matches.CompiledQueryMatch seekMatch)
                    TrySetSortSeekHint(seekMatch, plan, orderByFieldsOut);
                queryMatch = OrderBy(builderParameters, queryMatch, orderByFieldsOut, hasEmptySorts);
            }
            return queryMatch;
        }

        // ── Slow path: compile-miss, run full Try* chain to discover the winner ──
        bool needsFullChain = true;
        var resultHint = InstantiateHint.None;
        var trail = new PlanDecisionTrail();

        if (plan == null)
            trail.Record("CompoundExact", false, "no plan available");
        else if ((plan.OptimizationFlags & PlanOptFlags.CompoundExactCandidate) == 0)
            trail.Record("CompoundExact", false, "template has no compound-exact candidate");
        else if (TryCreateCompoundExactMatch(plan, planParams, builderParameters, out var compoundExact, out var ceReason))
        {
            disposable?.Dispose();
            queryMatch = compoundExact;
            innerMatchOut = compoundExact;
            resultHint = InstantiateHint.CompoundExact;
            needsFullChain = false;
            trail.Record("CompoundExact", true, "compound exact-term lookup");
        }
        else
            trail.Record("CompoundExact", false, ceReason ?? "rejected");

        if (orderByFieldsOut != null)
        {
            if (needsFullChain)
            {
                if (plan == null)
                    trail.Record("CompoundField", false, "no plan available");
                else if ((plan.OptimizationFlags & PlanOptFlags.DirectScanCandidate) == 0)
                    trail.Record("CompoundField", false, "template has no direct-scan candidate");
                else if (TryCreateCompoundFieldMatch(plan, orderByFieldsOut, planParams, builderParameters, compiledPlanOut, out var compoundMatch, out var cfReason))
                {
                    disposable?.Dispose();
                    innerMatchOut = compoundMatch;
                    queryMatch = OrderBy(builderParameters, compoundMatch, orderByFieldsOut, hasEmptySorts);
                    resultHint = InstantiateHint.CompoundField;
                    needsFullChain = false;
                    trail.Record("CompoundField", true, "compound tree scan with ORDER BY");
                }
                else
                    trail.Record("CompoundField", false, cfReason ?? "rejected");
            }

            if (needsFullChain)
            {
                if (plan == null)
                    trail.Record("DirectScan", false, "no plan available");
                else if ((plan.OptimizationFlags & PlanOptFlags.DirectScanCandidate) == 0)
                    trail.Record("DirectScan", false, "template has no direct-scan candidate");
                else if (TryCreateSimpleFieldDirectScan(plan, orderByFieldsOut, planParams, builderParameters, compiledPlanOut, out var directMatch, out var dsReason))
                {
                    disposable?.Dispose();
                    queryMatch = directMatch;
                    innerMatchOut = directMatch;
                    resultHint = InstantiateHint.DirectScan;
                    needsFullChain = false;
                    trail.Record("DirectScan", true, "direct tree scan on sort field");
                }
                else
                    trail.Record("DirectScan", false, dsReason ?? "rejected");
            }

            if (needsFullChain)
            {
                if (queryMatch is global::Corax.Querying.Matches.CompiledQueryMatch seekMatch)
                    TrySetSortSeekHint(seekMatch, plan, orderByFieldsOut);
                // innerMatchOut already references the pre-wrap match (the CompiledQueryMatch
                // produced by BuildAndCompile). The OrderBy wrapper here doesn't replace the
                // inner — the SortingMatch keeps it alive and the caller disposes it via
                // innerMatchOut after the inspection graph is built.
                queryMatch = OrderBy(builderParameters, queryMatch, orderByFieldsOut, hasEmptySorts);
                resultHint = InstantiateHint.None;
                trail.Record("BitmapSort", true, "bitmap pipeline with SortingMatch fallback");
            }
        }
        else
            trail.Record("NoOrderBy", true, "no ORDER BY");

        if (compiledPlanOut != null)
        {
            compiledPlanOut.Hint = resultHint;
            compiledPlanOut.DecisionTrail = trail;
        }

        return queryMatch;
    }

    /// <summary>
    /// Phase 1: build (or fetch from the plan cache) the structural template for a query
    /// text. Captures field names, clause types, parameter bindings, and literal values.
    /// No cardinality estimation, no parameter values, no IL.
    ///
    /// Safe to call without a live transaction once the schema is known —
    /// cmpxchg()/now()/today() bindings store a DeferredExpression that resolves per
    /// execution, so the template itself remains parameter-independent.
    /// </summary>
    public static PlanTemplate BuildTemplate(PlanParameters planParams)
    {
        var queryText = planParams.Metadata.Query.QueryText;
        var planCache = planParams.IndexSearcher.PlanCache;
        return planCache.TryGetTemplate(queryText) ?? ParseTemplate(planParams);
    }

    /// <summary>
    /// Phase 2: bind parameter values to the structural template, estimate cardinality,
    /// sort clauses, emit plan ops, and look up or compile the IL delegate via the plan cache.
    ///
    /// Returns the (cached or newly compiled) <see cref="CompiledPlan"/> together with the
    /// per-execution <see cref="QueryExecution"/> via <paramref name="plan"/>.
    /// Returns <c>null</c> when the plan reduces to an empty match without spatial/vector
    /// post-filters (e.g. an empty IN clause inside an AND chain) — the caller must produce
    /// an explicit empty result rather than caching this shape under the wrong key.
    /// </summary>
    [SkipLocalsInit] // stackalloc int[StackallocClauseThreshold] is immediately overwritten by InitializeIndices
    private static CompiledPlan Build(
        PlanTemplate template,
        PlanParameters planParams,
        QueryBuilderParameters builderParameters,
        out QueryExecution plan,
        bool wantTimings)
    {
        _ = wantTimings; // IL emission always produces both timed and untimed delegates today.
        var indexSearcher = planParams.IndexSearcher;
        var queryText = planParams.Metadata.Query.QueryText;
        var planCache = indexSearcher.PlanCache;

        // Step 2: Build the per-execution clause/exec lists from the template, evaluating
        // WHEN clauses against bound parameters as we go. Surviving clauses are added to
        // the working lists; non-survivors are skipped.
        //
        // WhenFlags layout: bit `i` = "the i-th WHEN clause in template traversal order
        // evaluated true." This is a stable, parameter-independent ordinal (PlanTemplate
        // construction enforced MaxWhenClauses=32 already). Plans built from the same
        // queryText but different WHEN-survival subsets must end up with different
        // cache keys — e.g. [Attach==true, Number!=1] sorted Attach-first gives
        // ordering=1, which collides with the single-clause [Attach==true] survivor
        // (also ord=1). WhenFlags joins (Ordering, TypeSignature, FullKinds) so each
        // survival pattern gets its own cached compiled plan.
        var clauses = new List<ClauseInfo>(template.Clauses.Length);
        var execList = new List<ClauseExecution>(template.Clauses.Length);
        int whenFlags = 0;
        if (template.WhenCount == 0)
        {
            // Fast path: no WHEN clauses anywhere in the template — skip the per-clause
            // WhenCondition null check. Common case for non-conditional queries.
            for (int ti = 0; ti < template.Clauses.Length; ti++)
            {
                var cached = template.Clauses[ti];
                clauses.Add(cached);
                execList.Add(CreateExecution(cached));
            }
        }
        else
        {
            int whenBit = 0;
            for (int ti = 0; ti < template.Clauses.Length; ti++)
            {
                var cached = template.Clauses[ti];
                if (cached.WhenCondition != null)
                {
                    if (cached.WhenCondition(planParams.QueryParameters) == false)
                    {
                        whenBit++;
                        continue;
                    }
                    whenFlags |= 1 << whenBit;
                    whenBit++;
                }
                clauses.Add(cached);
                execList.Add(CreateExecution(cached));
            }
        }

        // Step 3: Populate parameter values into typed arrays
        var writer = new ValueWriter();
        for (int ci = 0; ci < clauses.Count; ci++)
            PopulateClauseValues(clauses[ci], execList[ci], planParams.QueryParameters, writer, builderParameters);

        // Step 3b: Constant propagation — simplify trivially-false/simple clauses
        bool isOr = template.IsOr;
        for (int ci = clauses.Count - 1; ci >= 0; ci--)
        {
            var c = clauses[ci];
            var e = execList[ci];

            // Contradictory BETWEEN: low > high → clause matches nothing
            if (c.ClauseType == ClauseType.Between && e.PackedParamValue.IsNone == false)
            {
                var p = e.PackedParamValue;
                if (p.Param2 != PackedParam.NoParamValue)
                {
                    bool contradictory = p.ValueType switch
                    {
                        PackedParam.TypeLong => writer.GetLongs()[p.Param1] > writer.GetLongs()[p.Param2],
                        PackedParam.TypeDouble => writer.GetDoubles()[p.Param1] > writer.GetDoubles()[p.Param2],
                        // Strings: skip — the per-field analyzer (e.g. LowerCaseKeyword on
                        // auto-indexes) rewrites both bounds at BetweenQuery time. A raw-bounds
                        // ordinal compare here would mis-mark e.g. ('m', 'Maciej') as contradictory
                        // before 'Maciej' is normalized to 'maciej'. See RavenDB-23642.
                        _ => false
                    };
                    if (contradictory)
                    {
                        // Mark with zero cardinality. EmitPlan handles:
                        // AND chain: zero-cardinality → empty result.
                        // OR chain: remove the clause (contributes nothing).
                        e.Cardinality = 0;
                        e.InTermCount = 0;
                        e.HasNullTerm = false;
                        // Clone before mutating: c references the frozen template clause,
                        // shared across executions. Mutating it in place would poison cached
                        // plans (RavenDB-17423 pattern).
                        c = c.Clone();
                        c.ClauseType = ClauseType.In; // Reuse empty-IN elimination in EmitPlan
                        clauses[ci] = c;
                    }
                }
            }

            // Note: we intentionally do NOT rewrite single-value IN → Equals here.
            // The plan cache is keyed by (queryText, OperandOrdering), but parameter
            // cardinality changes between executions of the same queryText (e.g.
            // `Identifier.In($ids)` with $ids growing from 1 to N elements). If the
            // first execution rewrote to Equals and cached an Equals-shaped IL, the
            // second execution with N>1 terms would reuse the cached Equals delegate
            // and silently drop the extra terms — see RavenDB-17423. Keeping the IN
            // shape uniform across cardinalities makes EmitInOps emit a single
            // OrRange op whose term count is read from InRangeCounts at runtime, so
            // the same compiled delegate handles any IN size.
            //
            // Null-only IN (InTermCount=0, HasNullTerm=true): already optimized —
            // EmitInOps skips OrRange (no non-null terms) and emits only the null-term
            // OrWithPostings op. No conversion needed.
        }

        // Step 4: Estimate cardinality (needs populated values)
        for (int ci = 0; ci < clauses.Count; ci++)
        {
            if (execList[ci].Cardinality < 0)
                execList[ci].Cardinality = EstimateCardinality(clauses[ci], execList[ci], indexSearcher, writer, planParams, builderParameters);
        }

        // Step 5: Sort operands by cardinality (sort clauses and executions in lockstep)
        ClauseExecution[] executions;
        if (!isOr)
        {
            // Build an index array, sort it by cardinality, then reorder both
            // collections via the resulting permutation. Indices buffer is
            // stackalloc for small clause counts, ArrayPool for larger; both
            // paths share the same Span<int>.Sort with a struct comparer.
            int n = clauses.Count;
            Span<int> stackIndices = stackalloc int[StackallocClauseThreshold];
            int[] rented = null;
            scoped Span<int> indices;
            if (n <= StackallocClauseThreshold)
            {
                indices = stackIndices.Slice(0, n);
            }
            else
            {
                // Rent padded to a multiple of 8 for RoaringBitmap.InitializeIndices
                // (AVX2 Vector256 stores write 8 ints at a time).
                int paddedLen = RoaringBitmap.PadToVector256Width(n);
                rented = ArrayPool<int>.Shared.Rent(paddedLen);
                indices = rented.AsSpan(0, paddedLen);
            }

            RoaringBitmap.InitializeIndices(indices, n);
            indices.Sort(new ClauseCardinalityComparer(clauses, execList));

            // Apply the forward permutation (indices[sorted_pos] = original_pos) to both
            // clauses and execList in-place via cycle decomposition — O(n) time, O(1) space.
            // Each cycle: save the start element, rotate the chain, place saved at cycle end.
            // Entries where indices[j]==j (fixed points, already-settled cycles) are skipped.
            Span<ClauseInfo> clausesSpan = CollectionsMarshal.AsSpan(clauses);
            Span<ClauseExecution> execSpan = CollectionsMarshal.AsSpan(execList);
            for (int start = 0; start < n; start++)
            {
                if (indices[start] == start) continue;
                int j = start;
                ClauseInfo savedClause = clausesSpan[j];
                ClauseExecution savedExec = execSpan[j];
                while (indices[j] != start)
                {
                    int next = indices[j];
                    clausesSpan[j] = clausesSpan[next];
                    execSpan[j] = execSpan[next];
                    indices[j] = j; // mark settled
                    j = next;
                }
                clausesSpan[j] = savedClause;
                execSpan[j] = savedExec;
                indices[j] = j; // mark settled
            }

            if (rented != null)
                ArrayPool<int>.Shared.Return(rented);

            executions = execList.ToArray();
        }
        else
        {
            executions = execList.ToArray();
            // Move AndGroup clauses to the front (preserving relative order)
            // using in-place shifts on both the clauses list and executions array.
            int insertPos = 0;
            for (int j = 0; j < clauses.Count; j++)
            {
                if (clauses[j].ClauseType == ClauseType.AndGroup)
                {
                    if (j != insertPos)
                    {
                        ClauseInfo ag = clauses[j];
                        ClauseExecution agExec = executions[j];
                        // Shift elements [insertPos..j-1] right by one in both collections.
                        clauses.RemoveAt(j);
                        clauses.Insert(insertPos, ag);
                        Array.Copy(executions, insertPos, executions, insertPos + 1, j - insertPos);
                        executions[insertPos] = agExec;
                    }
                    insertPos++;
                }
            }
        }

        // Step 6: Emit plan ops + attach spatial/vector post-filters.
        // clauses.Count == 0 can happen either because the template is AllEntries (no WHERE)
        // or because every WHERE clause was eliminated by a false WHEN condition — both reduce
        // to "match all entries".
        if (clauses.Count == 0)
        {
            plan = BuildAllEntriesPlan();
            plan.Executions = executions;
        }
        else
        {
            // Extract per-clause cardinalities and value types for EmitPlan.
            // EmitPlan uses these for annotations (EstimatedCardinality, scan predicate types)
            // but NOT for shape decisions.
            var emitCardinalities = new long[executions.Length];
            var emitTermTypes = new ParamValueType[executions.Length];
            for (int ei = 0; ei < executions.Length; ei++)
            {
                emitCardinalities[ei] = executions[ei].Cardinality;
                emitTermTypes[ei] = executions[ei].TermValueType;
            }
            plan = EmitPlan(clauses, emitCardinalities, emitTermTypes, isOr, writer.GetStrings(), executions);
            plan.Executions = executions;

            // Fixup InRangeCounts from actual runtime InTermCount / HasNullTerm.
            // EmitPlan uses Bindings.Length (structural) for range counts, but runtime
            // InTermCount may differ when a single parameter binding expands to an array.
            // For AllIn, the null-term slot must be excluded when HasNullTerm is false
            // (ANDing with an empty PostingSource clears the bitmap).
            if (plan.InRangeCounts != null)
            {
                int rangeIdx = 0;
                // OR chain: every In/AllIn clause emits one range entry
                // AND chain: seed In/AllIn at position 0 emits one entry, each non-seed In/AllIn emits one
                for (int ci = 0; ci < clauses.Count && rangeIdx < plan.InRangeCounts.Length; ci++)
                {
                    var cl = clauses[ci];
                    if (cl.ClauseType == ClauseType.In)
                    {
                        // IN: range = InTermCount (OR with empty null-slot is no-op)
                        plan.InRangeCounts[rangeIdx] = executions[ci].InTermCount;
                        rangeIdx++;
                    }
                    else if (cl.ClauseType == ClauseType.AllIn)
                    {
                        int inCount = executions[ci].InTermCount;
                        bool hasNull = executions[ci].HasNullTerm;
                        if (ci == 0 && !isOr)
                        {
                            // Seed AllIn (EmitAllInOps): exclude null-slot when !HasNullTerm
                            plan.InRangeCounts[rangeIdx] = Math.Max(0, inCount - 1 + (hasNull ? 1 : 0));
                        }
                        else if (isOr)
                        {
                            // OR chain uses EmitInOps for AllIn too — range = InTermCount
                            plan.InRangeCounts[rangeIdx] = inCount;
                        }
                        else
                        {
                            // Non-seed AND AllIn: range = InTermCount (direct AndRange)
                            plan.InRangeCounts[rangeIdx] = inCount;
                        }
                        rangeIdx++;
                    }
                }
            }

            // Build scan predicate infos for entry scan — done here (not in EmitPlan)
            // because OrGroup/AndGroup subclauses need actual resolved types from executions.
            if (isOr == false && clauses.Count > 1)
            {
                int longIndex = 0, doubleIndex = 0, sliceIndex = 0;
                int scanStart = plan.AllNegated ? 0 : 1;
                int maxPreds = clauses.Count - scanStart;
                var scanPreds = new ScanPredicateInfo[maxPreds];
                int scanPredCount = 0;
                for (int si2 = scanStart; si2 < clauses.Count; si2++)
                {
                    var pred = BuildScanPredicateInfo(clauses[si2], executions[si2], ref longIndex, ref doubleIndex, ref sliceIndex);
                    if (pred != null)
                        scanPreds[scanPredCount++] = pred.Value;
                }
                if (scanPredCount > 0)
                {
                    if (scanPredCount < maxPreds)
                        Array.Resize(ref scanPreds, scanPredCount);
                    plan.ScanPredicateInfos = scanPreds;
                    (plan.TypeSignature, plan.FullKinds) = GetTypeSignature(plan.ScanPredicateInfos, writer.GetStrings());
                }
            }
        }
        // Empty-IN short-circuit: EmitPlan returns Ops=[] for an AND chain containing
        // an empty IN clause (e.g. `Names in ()`), and the resulting QueryExecution has
        // the default OperandOrdering=0 and TypeSignature=0. That cache key collides
        // with single-clause "default" plans (e.g. a one-term Equals after constant
        // propagation), so a subsequent execution against the same queryText would
        // receive the cached empty IL and produce zero results for a real query.
        // Return an explicit empty match here without touching the cache. Bail only
        // when there are no spatial/vector post-filters — those phases still need to
        // run (AND with empty is empty for spatial, but vector with a null filter
        // would otherwise return unfiltered top-K).
        if (plan.Ops is { Length: 0 } && plan.IsAllEntries == false
            && template.SpatialClauses == null && template.VectorClauses == null)
        {
            // Caller (BuildAndCompile facade) converts null into TermMatch.CreateEmpty.
            return null;
        }

        if (template.SpatialClauses != null || template.VectorClauses != null)
        {
            ClauseInfo[] spatialArr = null;
            ClauseInfo[] vectorArr = null;
            ClauseExecution[] spatialExecs = null;
            ClauseExecution[] vectorExecs = null;
            if (template.SpatialClauses != null)
            {
                int sLen = template.SpatialClauses.Length;
                spatialArr = new ClauseInfo[sLen];
                spatialExecs = new ClauseExecution[sLen];
                for (int si = 0; si < sLen; si++)
                {
                    var sc = template.SpatialClauses[si];
                    var scExec = new ClauseExecution();
                    PopulateClauseValues(sc, scExec, planParams.QueryParameters, writer, builderParameters);
                    spatialArr[si] = sc;
                    spatialExecs[si] = scExec;
                }
            }
            if (template.VectorClauses != null)
            {
                int vLen = template.VectorClauses.Length;
                vectorArr = new ClauseInfo[vLen];
                vectorExecs = new ClauseExecution[vLen];
                for (int vi = 0; vi < vLen; vi++)
                {
                    var vc = template.VectorClauses[vi];
                    var vcExec = new ClauseExecution();
                    PopulateClauseValues(vc, vcExec, planParams.QueryParameters, writer, builderParameters);
                    vectorArr[vi] = vc;
                    vectorExecs[vi] = vcExec;
                }
            }
            AttachPostFilterPhases(plan, spatialArr, spatialExecs, vectorArr, vectorExecs);
        }

        // Store typed arrays once after all clauses (including spatial/vector) are populated.
        plan.LongValues = writer.GetLongs();
        plan.DoubleValues = writer.GetDoubles();
        plan.StringValues = writer.GetStrings();

        // Step 7: Boost handling
        if (planParams.HasBoost)
        {
            var ops = plan.Ops;
            if (ops != null)
                for (int i = 0; i < ops.Length; i++)
                    ops[i].Dispatch = MatchDispatch.QueryMatch;
            plan.OperandOrdering |= QueryExecution.HasBoostBit;
        }

        // Step 8: Look up or compile the delegate for this ordering.
        // WhenFlags (set above when building clauses/execList from template) is a
        // first-class part of the cache key alongside Ordering / TypeSignature / FullKinds.
        plan.WhenFlags = whenFlags;
        plan.OptimizationFlags = template.OptimizationFlags;

        // Remap template-position optimization indices to post-sort runtime indices.
        // After WHEN elimination + cardinality sort, clauses are reordered; OriginalIndex
        // records each clause's template position. Single pass over the clause list.
        plan.SortDrivingClauseIndex = -1;
        plan.CompoundExactClauseA = -1;
        plan.CompoundExactClauseB = -1;
        plan.CompoundExactAFirst = template.CompoundExactAFirst;
        plan.CompoundFieldDrivingClause = -1;
        plan.CompoundFieldSortName = template.CompoundFieldSortName;
        plan.CompoundFieldIsMultiSort = template.CompoundFieldIsMultiSort;
        {
            int needSort = template.SortDrivingClauseIndex >= 0 ? 1 : 0;
            int needExactA = template.CompoundExactClauseA >= 0 ? 1 : 0;
            int needExactB = template.CompoundExactClauseB >= 0 ? 1 : 0;
            int needField = template.CompoundFieldDrivingClause >= 0 ? 1 : 0;
            int remaining = needSort + needExactA + needExactB + needField;
            for (int i = 0; i < clauses.Count && remaining > 0; i++)
            {
                int origIdx = clauses[i].OriginalIndex;
                if (needSort > 0 && origIdx == template.SortDrivingClauseIndex)
                {
                    plan.SortDrivingClauseIndex = i;
                    remaining--;
                }
                if (needExactA > 0 && origIdx == template.CompoundExactClauseA)
                {
                    plan.CompoundExactClauseA = i;
                    remaining--;
                }
                else if (needExactB > 0 && origIdx == template.CompoundExactClauseB)
                {
                    plan.CompoundExactClauseB = i;
                    remaining--;
                }
                if (needField > 0 && origIdx == template.CompoundFieldDrivingClause)
                {
                    plan.CompoundFieldDrivingClause = i;
                    remaining--;
                }
            }
        }

        // Cardinality cliff bucket: if the sort-driving clause's cardinality is within
        // the SortedDrivingWithTieBreakMatch.MaxGroupSize cap, set bit 31. This produces
        // different compiled plans (and optimization hints) for under/over cliff cardinality.
        if (plan.SortDrivingClauseIndex >= 0 && plan.SortDrivingClauseIndex < executions.Length)
        {
            long drivingCard = executions[plan.SortDrivingClauseIndex].Cardinality;
            if (drivingCard >= 0 && drivingCard <= global::Corax.Querying.Matches.SortedDrivingWithTieBreakMatch.MaxGroupSize)
                plan.OperandOrdering |= QueryExecution.CardinalityCliffBit;
        }

        var compiledPlan = planCache.Get(queryText, plan.OperandOrdering, plan.TypeSignature, plan.FullKinds, plan.WhenFlags);
        if (compiledPlan == null)
        {
            compiledPlan = new CompiledPlan
            {
                CompiledDelegate = QueryIlEmitter.EmitDelegate(plan, out var explainText, emitTimings: false),
                CompiledTimedDelegate = QueryIlEmitter.EmitDelegate(plan, out _, emitTimings: true),
                CompiledEntryPredicate = ResidualScanIlEmitter.EmitDelegate(plan.ScanPredicateInfos, out var scanExplain),

                ExplainSource = explainText + "\n" + scanExplain,
                CSharpSource = QueryIlEmitter.EmitCSharpSource(plan) + "\n" + ResidualScanIlEmitter.EmitCSharpSource(plan.ScanPredicateInfos),
                Ordering = plan.OperandOrdering,
                TypeSignature = plan.TypeSignature,
                FullKinds = plan.FullKinds,
                WhenFlags = plan.WhenFlags,
                InspectionTemplate = BuildInspectionTemplate(plan)
            };
            planCache.Add(queryText, compiledPlan, template);
        }

        return compiledPlan;
    }

    /// <summary>
    /// Phase 3: bind a cached/compiled plan to a live execution. Resolves matches and term
    /// sources from the live transaction, extracts scan parameters, optionally populates
    /// highlighting terms, builds the <see cref="CompiledQueryMatch"/>, then applies the
    /// mandatory spatial post-filter and vector-select wrappers.
    ///
    /// The wrappers are correctness, not optional decoration — vector selects produce
    /// unfiltered top-K without the filter source they're given here.
    /// </summary>
    private static IQueryMatch Instantiate(
        CompiledPlan compiledPlan,
        QueryExecution plan,
        PlanParameters planParams,
        QueryBuilderParameters builderParameters,
        Dictionary<string, CoraxHighlightingTermIndex> highlightingTerms,
        bool wantTimings,
        CancellationToken token)
    {
        var indexSearcher = planParams.IndexSearcher;
        var resolvedMatches = ResolveMatches(plan, indexSearcher, planParams, builderParameters);
        var termSources = ResolveTermSources(plan, indexSearcher, planParams, builderParameters);
        var termsProviders = ResolveTermsProviders(plan, indexSearcher, planParams, builderParameters);
        ExtractScanParameters(plan, indexSearcher,
            out var longParams, out var doubleParams, out var sliceParams, out var fieldRootPages);

        if (highlightingTerms != null)
            PopulateHighlightingTerms(plan, highlightingTerms, planParams.Metadata);

        var compiledMatch = new CompiledQueryMatch(
            compiledPlan, plan.RequiredBitmaps, plan.Ops?.Length ?? 0, resolvedMatches, termSources, termsProviders,
            indexSearcher, planParams.Allocator, wantTimings, token)
        {
            InRangeCounts = plan.InRangeCounts,
            ScanPredicateInfos = plan.ScanPredicateInfos,
            ScanLongParams = longParams,
            ScanDoubleParams = doubleParams,
            ScanSliceParams = sliceParams,
            ScanFieldRootPages = fieldRootPages
        };
        IQueryMatch result = compiledMatch;

        // Spatial post-filter phase: AND each spatial match with the candidate bitmap.
        if (plan.SpatialFilters is { Length: > 0 })
        {
            var spatialFilters = new IQueryMatch[plan.SpatialFilters.Length];
            for (int sf = 0; sf < plan.SpatialFilters.Length; sf++)
                spatialFilters[sf] = resolvedMatches[plan.SpatialFilters[sf].MatchIndex];
            result = new PostFilterMatch(result, spatialFilters);
        }

        // Vector select phase: each vector wraps the bitmap so far as its filter source.
        if (plan.VectorSelects is { Length: > 0 } && builderParameters != null)
        {
            var vectorItems = ResolveVectorItems(plan, builderParameters);
            bool hasActualFilter = !plan.IsAllEntries || plan.SpatialFilters is { Length: > 0 };
            IQueryMatch vectorFilter = hasActualFilter ? result : null;
            foreach (var item in vectorItems)
                result = item.Materialize(vectorFilter);
        }

        return result;
    }

    /// <summary>
    /// Build a query match from a sub-expression (e.g. the inner BinaryExpression
    /// of a moreLikeThis clause). Parses and resolves the expression directly
    /// without going through the full plan/compile pipeline.
    /// </summary>
    public static IQueryMatch BuildFromSubExpression(QueryBuilderParameters builderParams, QueryExpression expression)
    {
        var indexSearcher = builderParams.IndexSearcher;
        var clauses = new List<ClauseInfo>();
        bool hasMixed = false;
        // Sub-expression entry point: run the same walker phases as ParseTemplate
        // (ValidateAst pre-materialize, RewriteClauses post-materialize) so boost()
        // and other deferred rewrites apply consistently here too.
        var walkerCtx = new ResolutionContext(builderParams.QueryParameters, builderParams.Metadata);
        PlanWalker.ValidateAst(expression, walkerCtx);
        ParseExpression(expression, indexSearcher, clauses, builderParams.QueryParameters,
            builderParams.Metadata, walkerCtx, ref hasMixed);
        PlanWalker.RewriteClauses(clauses, walkerCtx);

        if (clauses.Count == 0)
            return indexSearcher.AllEntries();

        // Populate parameters for the sub-expression clauses
        var writer = new ValueWriter();
        var subExecs = new ClauseExecution[clauses.Count];
        for (int ci = 0; ci < clauses.Count; ci++)
        {
            subExecs[ci] = CreateExecution(clauses[ci]);
            PopulateClauseValues(clauses[ci], subExecs[ci], builderParams.QueryParameters, writer, builderParams);
        }

        var subPlan = new QueryExecution
        {
            LongValues = writer.GetLongs(),
            DoubleValues = writer.GetDoubles(),
            StringValues = writer.GetStrings(),
            Executions = subExecs
        };

        if (clauses.Count == 1)
            return ResolveClause(clauses[0], subExecs[0], indexSearcher, subPlan, parameters: null, builderParams: builderParams);

        // Multiple clauses (AND chain) — resolve each and AND them via bitmap.
        // RoaringBitmap is passed as `ref` to AndWithMatch, so using var is not legal here;
        // use try/finally to guarantee disposal.
        var bitmap = new BitmapMatch(indexSearcher.Allocator);
        var temp = new RoaringBitmap(indexSearcher.Allocator);
        try
        {
            bool first = true;
            for (int ci2 = 0; ci2 < clauses.Count; ci2++)
            {
                var clause = clauses[ci2];
                var match = ResolveClause(clause, subExecs[ci2], indexSearcher, subPlan, parameters: null, builderParams: builderParams);
                if (first)
                {
                    QueryPrimitives.OrWithMatch(match, ref bitmap.BitmapState);
                    first = false;
                }
                else
                {
                    QueryPrimitives.AndWithMatch(match, ref bitmap.BitmapState, ref temp);
                }
            }
        }
        finally
        {
            temp.Dispose();
        }
        return bitmap;
    }

    // ── Template caching ──────────────────────────────────────────────

    /// <summary>Create a ClauseExecution for a clause, including sub-executions for OrGroup/AndGroup.</summary>
    private static ClauseExecution CreateExecution(ClauseInfo clause)
    {
        var exec = new ClauseExecution();
        if (clause.OrSubClauses is { Count: > 0 })
        {
            exec.OrSubExecutions = new ClauseExecution[clause.OrSubClauses.Count];
            for (int i = 0; i < clause.OrSubClauses.Count; i++)
                exec.OrSubExecutions[i] = CreateExecution(clause.OrSubClauses[i]);
        }
        if (clause.AndSubClauses is { Count: > 0 })
        {
            exec.AndSubExecutions = new ClauseExecution[clause.AndSubClauses.Count];
            for (int i = 0; i < clause.AndSubClauses.Count; i++)
                exec.AndSubExecutions[i] = CreateExecution(clause.AndSubClauses[i]);
        }
        return exec;
    }

    /// <summary>Resolve a single clause's parameter value using its cached binding.
    /// Called for each clause during parameter population (both first execution and cache hit).
    /// The optional builderParameters is needed to resolve deferred method expressions (cmpxchg, now, today).</summary>
    private static void PopulateClauseValues(ClauseInfo clause, ClauseExecution exec, BlittableJsonReaderObject queryParameters, ValueWriter writer, QueryBuilderParameters builderParameters)
    {
        RuntimeHelpers.EnsureSufficientExecutionStack();
        // Always recurse into subclauses first (OrGroup/AndGroup have no binding of their own)
        if (clause.OrSubClauses != null && exec.OrSubExecutions != null)
            for (int si = 0; si < clause.OrSubClauses.Count; si++)
                PopulateClauseValues(clause.OrSubClauses[si], exec.OrSubExecutions[si], queryParameters, writer, builderParameters);
        if (clause.AndSubClauses != null && exec.AndSubExecutions != null)
            for (int si = 0; si < clause.AndSubClauses.Count; si++)
                PopulateClauseValues(clause.AndSubClauses[si], exec.AndSubExecutions[si], queryParameters, writer, builderParameters);

        // Resolve boost factor if this clause is boosted
        if (clause.HasBoost && clause.Bindings is { Length: > 0 })
        {
            ResolveBoostFactor(clause, exec, queryParameters);
        }

        switch (clause.ClauseType)
        {
            // Spatial and vector resolve via their binding array.
            case ClauseType.Spatial when clause.Bindings is { Length: > 0 }:
                ResolveSpatialFromBindings(clause, exec, queryParameters);
                return;
            case ClauseType.Vector when clause.Bindings is { Length: > 0 }:
                ResolveVectorFromBindings(clause, exec, queryParameters);
                return;
        }

        var bindings = clause.Bindings;
        if (bindings == null || bindings.Length == 0)
            return;

        switch (clause.ClauseType)
        {
            // BETWEEN: Literal sentinel bounds are rewritten at template time.
            // Parameter-bound sentinels are detected here at execution time.
            case ClauseType.Between:
            {
                var (low, lowType) = ResolveBindingScalar(bindings[BindingIndex.BetweenLow], queryParameters, builderParameters);
                var (high, highType) = ResolveBindingScalar(bindings[BindingIndex.BetweenHigh], queryParameters, builderParameters);
                bool lowIsSentinel = low is string lowStr && lowStr == RavenConstants.Documents.Querying.Terms.LeftNullValueOfBetweenQuery;
                bool highIsSentinel = high is string highStr && highStr == RavenConstants.Documents.Querying.Terms.RightNullValueOfBetweenQuery;
                if (lowIsSentinel && highIsSentinel) { exec.SentinelRewriteType = ClauseType.Exists; return; }
                if (lowIsSentinel) { exec.SentinelRewriteType = ClauseType.LessThanOrEqual; exec.TermValueType = highType; exec.PackedParamValue = writer.Add(high, ToValueTokenType(highType)); return; }
                if (highIsSentinel) { exec.SentinelRewriteType = ClauseType.LessThan; exec.SentinelRewriteNegated = true; exec.TermValueType = lowType; exec.PackedParamValue = writer.Add(low, ToValueTokenType(lowType)); return; }
                exec.TermValueType = lowType;
                exec.PackedParamValue = writer.AddPair(low, high, ToValueTokenType(lowType));
                return;
            }
            case ClauseType.In or ClauseType.AllIn:
                // IN/AllIn: each binding is a term (literal or parameter, possibly array-expanding)
                ResolveInFromBindings(clause, exec, queryParameters, writer, bindings);
                break;
            default:
                // Simple clause (Equals, Range, Search, Regex, etc.): single value at Bindings[0]
                var (value, valueType) = ResolveBindingScalar(bindings[BindingIndex.Value], queryParameters, builderParameters);
                // startsWith/endsWith/search/regex require a String argument — reject Null (matches Lucene behavior).
                if (value == null && clause.ClauseType is ClauseType.StartsWith or ClauseType.EndsWith or ClauseType.Search or ClauseType.Regex)
                {
                    string methodName = clause.ClauseType switch
                    {
                        ClauseType.StartsWith => "startsWith",
                        ClauseType.EndsWith => "endsWith",
                        ClauseType.Search => "search",
                        ClauseType.Regex => "regex",
                        _ => clause.ClauseType.ToString()
                    };
                    throw new Raven.Client.Exceptions.InvalidQueryException(
                        $"Method {methodName}() expects to get an argument of type String while it got Null");
                }
                exec.TermValueType = valueType;
                exec.PackedParamValue = writer.Add(value, ToValueTokenType(valueType));
                break;
        }
    }

    private static void ResolveBoostFactor(ClauseInfo clause, ClauseExecution exec, BlittableJsonReaderObject queryParameters)
    {
        var (boostVal, boostType) = ResolveBindingScalar(clause.Bindings[^1], queryParameters, builderParameters: null);
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

    private static void ResolveInFromBindings(ClauseInfo clause, ClauseExecution exec, BlittableJsonReaderObject queryParameters, ValueWriter writer, ParameterBinding[] bindings)
    {
        // Pre-size lists from bindings.Length (covers all-literal and scalar-parameter
        // cases without growth). Array-expansion parameters grow the backing array
        // automatically.
        var resolvedValues = new List<object>(bindings.Length);
        var termTypes = new List<ParamValueType>(bindings.Length);
        bool hasNullTerm = false;

        for (int bi = 0; bi < bindings.Length; bi++)
        {
            var it = bindings[bi];
            switch (it.Source)
            {
                case BindingSource.Literal:
                    resolvedValues.Add(it.LiteralValue);
                    termTypes.Add(it.LiteralType);
                    if (it.LiteralValue == null)
                        hasNullTerm = true;
                    break;

                case BindingSource.QueryParameter:
                {
                    // Parameter — resolve from blittable. May be scalar or array.
                    object inRaw = null;
                    queryParameters?.TryGet(it.ParameterName, out inRaw);
                    if (inRaw is BlittableJsonReaderArray arr)
                    {
                        foreach (var elem in arr)
                        {
                            var (elemVal, elemType) = ResolveInValue(elem, ValueTokenType.Parameter);
                            resolvedValues.Add(elemVal);
                            termTypes.Add(ToParamValueType(elemType));
                            if (elemVal == null)
                                hasNullTerm = true;
                        }
                    }
                    else if (inRaw != null)
                    {
                        var (singleVal, singleType) = ResolveInValue(inRaw, ValueTokenType.Parameter);
                        resolvedValues.Add(singleVal);
                        termTypes.Add(ToParamValueType(singleType));
                    }
                    else
                    {
                        resolvedValues.Add(null);
                        termTypes.Add(ParamValueType.Null);
                        hasNullTerm = true;
                    }
                    break;
                }

                case BindingSource.DeferredMethod:
                    // Deferred bindings (cmpxchg, now, today) shouldn't appear in IN lists,
                    // but handle gracefully: resolve as null.
                    resolvedValues.Add(null);
                    termTypes.Add(ParamValueType.Null);
                    hasNullTerm = true;
                    break;
            }
        }

        int count = resolvedValues.Count;

        // Determine dominant type. For all-literal IN clauses, this was pre-computed
        // at template time by InPreClassify — skip the per-execution scan.
        ParamValueType dominantType;
        if (clause.InAllLiteral)
        {
            dominantType = clause.InDominantType;
        }
        else
        {
            dominantType = ParamValueType.Null;
            for (int i = 0; i < count; i++)
            {
                if (resolvedValues[i] == null) continue;
                if (dominantType == ParamValueType.Null)
                    dominantType = termTypes[i];
            }
            if (dominantType == ParamValueType.Null)
                dominantType = ParamValueType.String;
        }

        int packedType = dominantType switch
        {
            ParamValueType.Long => PackedParam.TypeLong,
            ParamValueType.Double => PackedParam.TypeDouble,
            _ => PackedParam.TypeString
        };
        int startIdx = packedType switch
        {
            PackedParam.TypeLong => writer.LongCount,
            PackedParam.TypeDouble => writer.DoubleCount,
            _ => writer.StringCount
        };
        // Only store non-null values in the typed array. Null terms are handled
        // separately via HasNullTerm — one null-term lookup covers all nulls.
        //
        // Skip values whose individual type can't be coerced to the dominant type.
        // Example: IN(DateTime, "Shalom") on a DateTime-indexed field — the dominant
        // type is Long (DateTime.Ticks); "Shalom" can never match a long-indexed term,
        // so dropping it produces the correct empty/partial result (matches Lucene).
        // Without this guard, Convert.ToInt64("Shalom") would throw FormatException.
        int nonNullCount = 0;
        for (int i = 0; i < count; i++)
        {
            if (resolvedValues[i] == null) continue;
            if (termTypes[i] != dominantType && IsTypeIncompatible(termTypes[i], dominantType))
                continue;
            writer.Add(resolvedValues[i], ToValueTokenType(dominantType));
            nonNullCount++;
        }

        exec.PackedParamValue = new PackedParam(packedType, startIdx);
        exec.InTermCount = nonNullCount;
        exec.HasNullTerm = hasNullTerm;
    }

    /// <summary>Resolve spatial parameters from cached bindings (no MethodExpression dependency).</summary>
    private static void ResolveSpatialFromBindings(ClauseInfo clause, ClauseExecution exec, BlittableJsonReaderObject queryParameters)
    {
        var bindings = clause.Bindings;
        var sp = new SpatialParams();

        // [0] = distanceErrorPct
        if (bindings.Length > 0 && bindings[BindingIndex.SpatialDistErrPct] != null)
        {
            var (depVal, _) = ResolveBindingScalar(bindings[BindingIndex.SpatialDistErrPct], queryParameters, builderParameters: null);
            sp.DistanceErrorPct = depVal != null ? Convert.ToDouble(depVal) : -1;
        }

        // Shape type determined by the number of bindings:
        // circle has 5 (distErrPct, radius, lat, lng, units), WKT has 3 (distErrPct, wkt, units)
        if (bindings.Length >= BindingIndex.SpatialCircleBindingCount - 1) // circle: at least distErrPct + radius + lat + lng
        {
            sp.ShapeType = SpatialShapeType.Circle;
            var (r, _) = ResolveBindingScalar(bindings[BindingIndex.SpatialRadius], queryParameters, builderParameters: null);
            var (lat, _) = ResolveBindingScalar(bindings[BindingIndex.SpatialLatitude], queryParameters, builderParameters: null);
            var (lng, _) = ResolveBindingScalar(bindings[BindingIndex.SpatialLongitude], queryParameters, builderParameters: null);
            sp.CircleRadius = Convert.ToDouble(r);
            sp.CircleLatitude = Convert.ToDouble(lat);
            sp.CircleLongitude = Convert.ToDouble(lng);
            if (bindings.Length > BindingIndex.SpatialUnits && bindings[BindingIndex.SpatialUnits] != null)
            {
                var (u, _) = ResolveBindingScalar(bindings[BindingIndex.SpatialUnits], queryParameters, builderParameters: null);
                if (u != null && Enum.TryParse(typeof(SpatialUnits), u.ToString(), true, out var su))
                    sp.Units = (SpatialUnits)su == SpatialUnits.Kilometers
                            ? global::Corax.Utils.Spatial.SpatialUnits.Kilometers
                            : global::Corax.Utils.Spatial.SpatialUnits.Miles;
            }
        }
        else // WKT: distErrPct, wkt, [units]
        {
            sp.ShapeType = SpatialShapeType.Wkt;
            if (bindings.Length > BindingIndex.SpatialWkt && bindings[BindingIndex.SpatialWkt] != null)
            {
                var (wkt, _) = ResolveBindingScalar(bindings[BindingIndex.SpatialWkt], queryParameters, builderParameters: null);
                sp.Wkt = wkt?.ToString();
                if (bindings.Length > BindingIndex.SpatialWktUnits && bindings[BindingIndex.SpatialWktUnits] != null)
                {
                    var (u, _) = ResolveBindingScalar(bindings[BindingIndex.SpatialWktUnits], queryParameters, builderParameters: null);
                    if (u != null && Enum.TryParse(typeof(SpatialUnits), u.ToString(), true, out var su))
                        sp.Units = (SpatialUnits)su == SpatialUnits.Kilometers
                            ? global::Corax.Utils.Spatial.SpatialUnits.Kilometers
                            : global::Corax.Utils.Spatial.SpatialUnits.Miles;
                }
            }
        }

        exec.Spatial = sp;
    }

    /// <summary>Resolve vector parameters from cached bindings (no MethodExpression dependency).</summary>
    private static void ResolveVectorFromBindings(ClauseInfo clause, ClauseExecution exec, BlittableJsonReaderObject queryParameters)
    {
        var bindings = clause.Bindings;
        var vec = new VectorParams { Method = clause.VectorMethod };

        // [1]=minimumMatch, [2]=numberOfCandidates, [3]=aiTask
        if (bindings.Length > BindingIndex.VectorMinMatch && bindings[BindingIndex.VectorMinMatch] != null)
        {
            var (simVal, simType) = ResolveBindingScalar(bindings[BindingIndex.VectorMinMatch], queryParameters, builderParameters: null);
            if (simVal != null && simType != ParamValueType.Null)
                vec.MinimumMatch = simType == ParamValueType.Double ? (float)(double)simVal
                    : simType == ParamValueType.Long ? (long)simVal : -1;
        }
        if (bindings.Length > BindingIndex.VectorCandidates && bindings[BindingIndex.VectorCandidates] != null)
        {
            var (candVal, candType) = ResolveBindingScalar(bindings[BindingIndex.VectorCandidates], queryParameters, builderParameters: null);
            if (candVal != null && candType != ParamValueType.Null)
                vec.NumberOfCandidates = Convert.ToInt32(candVal);
        }
        if (bindings.Length > BindingIndex.VectorAiTask && bindings[BindingIndex.VectorAiTask] != null)
        {
            var (taskVal, _) = ResolveBindingScalar(bindings[BindingIndex.VectorAiTask], queryParameters, builderParameters: null);
            vec.AiTaskName = taskVal?.ToString();
        }

        // [0]=vector value (may be scalar, array, or blittable object)
        if (bindings.Length > BindingIndex.VectorValue && bindings[BindingIndex.VectorValue] != null)
        {
            var (val, valType) = ResolveBindingRaw(bindings[BindingIndex.VectorValue], queryParameters);
            vec.ResolvedValue = val;
            vec.ResolvedValueType = valType;
            // For scalar parameters, resolve the native type
            if (valType == ParamValueType.Parameter && val is not (BlittableJsonReaderArray or BlittableJsonReaderObject))
            {
                var (resolved, resolvedType) = ResolveParameterValue(val);
                vec.ResolvedValue = resolved;
                vec.ResolvedValueType = ToParamValueType(resolvedType);
            }
        }

        exec.Vector = vec;
    }

    /// <summary>Look up a binding's value from the blittable. Returns the RAW value —
    /// callers must check for arrays/objects before calling ResolveParameterValue.</summary>
    private static (object Value, ParamValueType Type) ResolveBindingRaw(ParameterBinding binding, BlittableJsonReaderObject queryParameters)
    {
        if (binding.LiteralType != ParamValueType.Parameter)
            return (binding.LiteralValue, binding.LiteralType);
        if (queryParameters != null && queryParameters.TryGet(binding.ParameterName, out object raw) && raw != null)
            return (raw, ParamValueType.Parameter); // raw from blittable — caller decides how to interpret
        return (null, ParamValueType.Null);
    }

    /// <summary>Resolve a binding to a scalar value. Asserts the result is not an array/object.
    /// For parameters that might be arrays, use ResolveBindingRaw and handle arrays first.
    /// The optional builderParameters is needed to resolve deferred method expressions (cmpxchg, now, today).</summary>
    private static (object Value, ParamValueType Type) ResolveBindingScalar(ParameterBinding binding, BlittableJsonReaderObject queryParameters, QueryBuilderParameters builderParameters)
    {
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
                if (queryParameters != null && queryParameters.TryGet(binding.ParameterName, out object raw) && raw != null)
                {
                    var (val, type) = ResolveParameterValue(raw);
                    return (val, ToParamValueType(type));
                }
                return (null, ParamValueType.Null);
        }
    }

    // ── Typed dispatch helpers ───────────────────────────────────────────

    /// <summary>Create a TermQuery using the pre-resolved typed value from the plan's arrays.</summary>
    private static IQueryMatch TermQueryFromParam(PackedParam packed, FieldMetadata fieldMeta,
        IndexSearcher indexSearcher, QueryExecution plan)
    {
        int idx = packed.Param1;
        return packed.ValueType switch
        {
            PackedParam.TypeLong => indexSearcher.TermQuery(fieldMeta, plan.LongValues[idx]),
            PackedParam.TypeDouble => indexSearcher.TermQuery(fieldMeta, plan.DoubleValues[idx]),
            _ => indexSearcher.TermQuery(fieldMeta, plan.StringValues[idx])
        };
    }

    /// <summary>Get a posting-list ID using the pre-resolved typed value.</summary>
    private static long GetTermPostingListIdFromParam(PackedParam packed, FieldMetadata fieldMeta,
        IndexSearcher indexSearcher, QueryExecution plan)
    {
        int idx = packed.Param1;
        return packed.ValueType switch
        {
            PackedParam.TypeLong => indexSearcher.GetTermPostingListId(fieldMeta, plan.LongValues[idx]),
            PackedParam.TypeDouble => indexSearcher.GetTermPostingListId(fieldMeta, plan.DoubleValues[idx]),
            _ => indexSearcher.GetTermPostingListId(fieldMeta, plan.StringValues[idx])
        };
    }

    // ── Match resolution ─────────────────────────────────────────────────

    /// <summary>
    /// Resolve clause infos to IQueryMatch instances for execution.
    /// Uses existing IndexSearcher methods (TermQuery, etc.) which handle
    /// all the complexity of analyzer application, CompactKey encoding,
    /// posting list resolution, etc.
    /// </summary>
    private static IQueryMatch[] ResolveMatches(QueryExecution plan, IndexSearcher indexSearcher,
        PlanParameters parameters, QueryBuilderParameters builderParams)
    {
        var clauses = plan.Clauses ?? [];
        // IsAllEntries + spatial/vector occurs when the query's only predicates are
        // vector.search() or spatial clauses with no other WHERE terms. GroupCollapse
        // partitions them into SpatialClauses/VectorClauses, leaving the main clause
        // list empty → IsAllEntries=true. The AllEntries bitmap feeds the post-filter
        // phases (spatial AND, then vector select with null filter for unfiltered top-K).
        if (plan.IsAllEntries)
        {
            int spatialCount = plan.SpatialFilters?.Length ?? 0;
            int vectorCount = plan.VectorSelects?.Length ?? 0;
            int totalExtra = spatialCount + vectorCount;
            if (totalExtra == 0)
                return [indexSearcher.AllEntries()];

            var allEntriesMatches = new IQueryMatch[1 + totalExtra];
            allEntriesMatches[0] = indexSearcher.AllEntries();
            int matchOfs = 1;
            if (plan.SpatialFilters != null)
            {
                for (int i = 0; i < plan.SpatialFilters.Length; i++)
                    allEntriesMatches[matchOfs++] = ResolveClause(plan.SpatialFilters[i].Clause, plan.SpatialFilters[i].Exec ?? new ClauseExecution(), indexSearcher, plan, parameters, builderParams);
            }
            if (plan.VectorSelects != null)
            {
                for (int i = 0; i < plan.VectorSelects.Length; i++)
                    allEntriesMatches[matchOfs++] = ResolveClause(plan.VectorSelects[i].Clause, plan.VectorSelects[i].Exec ?? new ClauseExecution(), indexSearcher, plan, parameters, builderParams);
            }
            return allEntriesMatches;
        }

        if (clauses.Count == 0)
            return [];

        var execs = plan.Executions;

        // Standalone NotEquals pattern: Fill(AllEntries) + ANDNOT(term).
        if (clauses.Count == 1 && clauses[0].IsNegated && !plan.AllNegated)
        {
            var clause = clauses[0];
            var exec0 = execs[0];
            return
            [
                indexSearcher.AllEntries(),
                TermQueryFromParam(exec0.PackedParamValue, indexSearcher.FieldMetadataBuilder(clause.FieldName), indexSearcher, plan)
            ];
        }

        var matches = new IQueryMatch[CountMatchSlots(clauses, execs, plan.IsAllEntries, plan.AllNegated)];
        int matchIdx = 0;
        for (int ci = 0; ci < clauses.Count; ci++)
        {
            ClauseInfo clause = clauses[ci];
            ClauseExecution exec = execs[ci];
            switch (clause.ClauseType)
            {
                case ClauseType.OrGroup when clause.OrSubClauses is { Count: > 0 }:
                {
                    for (int si = 0; si < clause.OrSubClauses.Count; si++)
                    {
                        var sub = clause.OrSubClauses[si];
                        var subExec = exec.OrSubExecutions[si];
                        var match = ResolveClause(sub, subExec, indexSearcher, plan, parameters, builderParams);
                        if (subExec.BoostFactor > 0)
                            match = indexSearcher.Boost(match, subExec.BoostFactor);
                        matches[matchIdx++] = match;
                    }

                    break;
                }
                case ClauseType.AndGroup when clause.AndSubClauses is { Count : > 0 }:
                {
                    for (int si = 0; si < clause.AndSubClauses.Count; si++)
                    {
                        var sub = clause.AndSubClauses[si];
                        var subExec = exec.AndSubExecutions[si];
                        var match = ResolveClause(sub, subExec, indexSearcher, plan, parameters, builderParams);
                        if (subExec.BoostFactor > 0)
                            match = indexSearcher.Boost(match, subExec.BoostFactor);
                        matches[matchIdx++] = match;
                    }

                    break;
                }
                case ClauseType.AllIn or ClauseType.In:
                {
                    for (int t = 0; t < exec.InTermCount; t++)
                        matches[matchIdx++] = ResolveInTerm(clause, exec, t, indexSearcher, plan, parameters, builderParams);
                    // Always allocate the null-term slot (plan structure is parameter-independent).
                    // When HasNullTerm is false, fill with a TermQuery(null) that resolves to an
                    // empty posting list — the OR with an empty match is a no-op.
                    {
                        FieldMetadata nullMeta = ResolveFieldMetadata(clause, indexSearcher, parameters, builderParams);
                        matches[matchIdx++] = exec.HasNullTerm
                            ? indexSearcher.TermQuery(nullMeta, null)
                            : TermMatch.CreateEmpty(indexSearcher, indexSearcher.Allocator);
                    }
                    break;
                }
                default:
                {
                    IQueryMatch match = clause.IsOrChainNotEquals switch
                    {
                        true => CreateNotEqualsOrMatch(clause, exec, indexSearcher, plan, parameters, builderParams),
                        false => ResolveClause(clause, exec, indexSearcher, plan, parameters, builderParams)
                    };
                    if (exec.BoostFactor > 0)
                        match = indexSearcher.Boost(match, exec.BoostFactor);
                    matches[matchIdx++] = match;
                    break;
                }
            }
        }

        if (plan.AllNegated)
            matches[matchIdx] = indexSearcher.AllEntries();
        return matches;
    }

    private static IQueryMatch ResolveRangeClauseWithDirection(ClauseInfo clause, ClauseExecution exec,
        IndexSearcher indexSearcher, QueryExecution plan, PlanParameters parameters, QueryBuilderParameters builderParams, bool forward)
    {
        FieldMetadata fieldMeta = ResolveFieldMetadata(clause, indexSearcher, parameters, builderParams);
        var packed = exec.PackedParamValue;

        return clause.ClauseType switch
        {
            ClauseType.GreaterThan => packed.ValueType switch
            {
                PackedParam.TypeLong => indexSearcher.GreaterThanQuery(fieldMeta, plan.LongValues[packed.Param1], forward),
                PackedParam.TypeDouble => indexSearcher.GreaterThanQuery(fieldMeta, plan.DoubleValues[packed.Param1], forward),
                _ => indexSearcher.GreaterThanQuery(fieldMeta, plan.StringValues[packed.Param1], forward)
            },
            ClauseType.GreaterThanOrEqual => packed.ValueType switch
            {
                PackedParam.TypeLong => indexSearcher.GreaterThanOrEqualsQuery(fieldMeta, plan.LongValues[packed.Param1], forward),
                PackedParam.TypeDouble => indexSearcher.GreaterThanOrEqualsQuery(fieldMeta, plan.DoubleValues[packed.Param1], forward),
                _ => indexSearcher.GreaterThanOrEqualsQuery(fieldMeta, plan.StringValues[packed.Param1], forward)
            },
            ClauseType.LessThan => packed.ValueType switch
            {
                PackedParam.TypeLong => indexSearcher.LessThanQuery(fieldMeta, plan.LongValues[packed.Param1], forward),
                PackedParam.TypeDouble => indexSearcher.LessThanQuery(fieldMeta, plan.DoubleValues[packed.Param1], forward),
                _ => indexSearcher.LessThanQuery(fieldMeta, plan.StringValues[packed.Param1], forward)
            },
            ClauseType.LessThanOrEqual => packed.ValueType switch
            {
                PackedParam.TypeLong => indexSearcher.LessThanOrEqualsQuery(fieldMeta, plan.LongValues[packed.Param1], forward),
                PackedParam.TypeDouble => indexSearcher.LessThanOrEqualsQuery(fieldMeta, plan.DoubleValues[packed.Param1], forward),
                _ => indexSearcher.LessThanOrEqualsQuery(fieldMeta, plan.StringValues[packed.Param1], forward)
            },
            ClauseType.Between when exec.SentinelRewriteType != null =>
                ResolveSentinelRewrittenBetween(exec, fieldMeta, indexSearcher, plan),
            ClauseType.Between => ResolveBetweenWithDirection(clause, exec, fieldMeta, indexSearcher, plan, forward),
            _ => ResolveClause(clause, exec, indexSearcher, plan, parameters, builderParams) // fallback
        };
    }

    /// <summary>Resolve a BETWEEN clause for sort-driving (TermsProviderMatch) paths.
    /// Sentinel bounds are rewritten at template time, so remaining BETWEEN clauses
    /// always have genuine bounds.</summary>
    private static IQueryMatch ResolveBetweenWithDirection(ClauseInfo clause, ClauseExecution exec, FieldMetadata fieldMeta,
        IndexSearcher indexSearcher, QueryExecution plan, bool forward)
    {
        var packed = exec.PackedParamValue;
        return packed.ValueType switch
        {
            PackedParam.TypeLong => indexSearcher.BetweenQuery(fieldMeta, plan.LongValues[packed.Param1], plan.LongValues[packed.Param2], forward: forward),
            PackedParam.TypeDouble => indexSearcher.BetweenQuery(fieldMeta, plan.DoubleValues[packed.Param1], plan.DoubleValues[packed.Param2], forward: forward),
            _ => indexSearcher.BetweenQuery(fieldMeta, plan.StringValues[packed.Param1], plan.StringValues[packed.Param2], forward: forward)
        };
    }

    private static IQueryMatch ResolveSentinelRewrittenBetween(ClauseExecution exec, FieldMetadata fieldMeta,
        IndexSearcher indexSearcher, QueryExecution plan)
    {
        if (exec.SentinelRewriteType == ClauseType.Exists)
            return indexSearcher.AllEntries();
        var packed = exec.PackedParamValue;
        int idx = packed.Param1;
        if (exec.SentinelRewriteType == ClauseType.LessThanOrEqual)
        {
            return packed.ValueType switch
            {
                PackedParam.TypeLong => indexSearcher.LessThanOrEqualsQuery(fieldMeta, plan.LongValues[idx]),
                PackedParam.TypeDouble => indexSearcher.LessThanOrEqualsQuery(fieldMeta, plan.DoubleValues[idx]),
                _ => indexSearcher.LessThanOrEqualsQuery(fieldMeta, plan.StringValues[idx])
            };
        }
        Debug.Assert(exec.SentinelRewriteType == ClauseType.LessThan && exec.SentinelRewriteNegated);
        IQueryMatch lessThanMatch = packed.ValueType switch
        {
            PackedParam.TypeLong => indexSearcher.LessThanQuery(fieldMeta, plan.LongValues[idx]),
            PackedParam.TypeDouble => indexSearcher.LessThanQuery(fieldMeta, plan.DoubleValues[idx]),
            _ => indexSearcher.LessThanQuery(fieldMeta, plan.StringValues[idx])
        };
        var bitmap = new BitmapMatch(indexSearcher.Allocator);
        var temp = new RoaringBitmap(indexSearcher.Allocator);
        QueryPrimitives.OrWithMatch(indexSearcher.AllEntries(), ref bitmap.BitmapState);
        QueryPrimitives.AndNotWithMatch(lessThanMatch, ref bitmap.BitmapState, ref temp);
        temp.Dispose();
        return bitmap;
    }

    /// <summary>Converts an Equals clause into a BetweenQuery(low==high==value) so
    /// it produces a TermsProviderMatch that SortedDrivingMatch can walk in sort order.</summary>
    private static IQueryMatch ResolveEqualsClauseWithDirection(ClauseInfo clause, ClauseExecution exec,
        IndexSearcher indexSearcher, QueryExecution plan, PlanParameters parameters, QueryBuilderParameters builderParams, bool forward)
    {
        FieldMetadata fieldMeta = ResolveFieldMetadata(clause, indexSearcher, parameters, builderParams);
        var packed = exec.PackedParamValue;
        return packed.ValueType switch
        {
            PackedParam.TypeLong => indexSearcher.BetweenQuery(fieldMeta, plan.LongValues[packed.Param1], plan.LongValues[packed.Param1],
                UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThanOrEqual, forward: forward),
            PackedParam.TypeDouble => indexSearcher.BetweenQuery(fieldMeta, plan.DoubleValues[packed.Param1], plan.DoubleValues[packed.Param1],
                UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThanOrEqual, forward: forward),
            _ => indexSearcher.BetweenQuery(fieldMeta, plan.StringValues[packed.Param1], plan.StringValues[packed.Param1],
                UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThanOrEqual, forward: forward)
        };
    }

    private static IQueryMatch ResolveClause(ClauseInfo clause, ClauseExecution exec, IndexSearcher indexSearcher,
        QueryExecution plan, PlanParameters parameters, QueryBuilderParameters builderParams)
    {
        if (clause.ClauseType == ClauseType.OrGroup && clause.OrSubClauses != null)
        {
            var bm = new BitmapMatch(indexSearcher.Allocator);
            var temp = new RoaringBitmap(indexSearcher.Allocator);
            for (int si = 0; si < clause.OrSubClauses.Count; si++)
            {
                var subExec = exec.OrSubExecutions[si];
                var subMatch = ResolveClause(clause.OrSubClauses[si], subExec, indexSearcher, plan, parameters, builderParams);
                QueryPrimitives.OrWithMatch(subMatch, ref bm.BitmapState);
            }
            temp.Dispose();
            return bm;
        }
        if (clause.ClauseType == ClauseType.AndGroup && clause.AndSubClauses != null)
        {
            var bm = new BitmapMatch(indexSearcher.Allocator);
            var temp = new RoaringBitmap(indexSearcher.Allocator);
            bool first = true;
            for (int si = 0; si < clause.AndSubClauses.Count; si++)
            {
                var sub = clause.AndSubClauses[si];
                var subExec = exec.AndSubExecutions[si];
                var subMatch = ResolveClause(sub, subExec, indexSearcher, plan, parameters, builderParams);
                if (first)
                {
                    QueryPrimitives.OrWithMatch(subMatch, ref bm.BitmapState);
                    first = false;
                }
                else if (sub.IsNegated)
                    QueryPrimitives.AndNotWithMatch(subMatch, ref bm.BitmapState, ref temp);
                else
                    QueryPrimitives.AndWithMatch(subMatch, ref bm.BitmapState, ref temp);
            }
            temp.Dispose();
            return bm;
        }

        // Spatial/Vector/Search have their own field resolution paths.
        FieldMetadata fieldMeta = default;
        bool needsFieldMeta = clause.ClauseType != ClauseType.Spatial
            && clause.ClauseType != ClauseType.Vector
            && clause.ClauseType != ClauseType.Search;
        if (needsFieldMeta)
        {
            if (builderParams != null)
            {
                // Dynamic field name variants (exact/search) are pre-resolved by the
                // DynamicFieldNameResolve walker step at template time — no per-execution
                // string allocation.
                string resolvedFieldName = clause.ResolvedFieldName ?? clause.FieldName;
                fieldMeta = QueryBuilderHelper.GetFieldMetadata(in builderParams, resolvedFieldName, exact: clause.IsExact, hasBoost: builderParams.HasBoost);
            }
            else
            {
                fieldMeta = indexSearcher.FieldMetadataBuilder(clause.FieldName, hasBoost: parameters?.HasBoost ?? false);
            }
        }

        var packed = exec.PackedParamValue;

        switch (clause.ClauseType)
        {
            case ClauseType.Equals:
            case ClauseType.NotEquals:
                return TermQueryFromParam(packed, fieldMeta, indexSearcher, plan);

            case ClauseType.GreaterThan:
            {
                int idx = packed.Param1;
                return packed.ValueType switch
                {
                    PackedParam.TypeLong => indexSearcher.GreaterThanQuery(fieldMeta, plan.LongValues[idx]),
                    PackedParam.TypeDouble => indexSearcher.GreaterThanQuery(fieldMeta, plan.DoubleValues[idx]),
                    _ => indexSearcher.GreaterThanQuery(fieldMeta, plan.StringValues[idx])
                };
            }

            case ClauseType.GreaterThanOrEqual:
            {
                int idx = packed.Param1;
                return packed.ValueType switch
                {
                    PackedParam.TypeLong => indexSearcher.GreaterThanOrEqualsQuery(fieldMeta, plan.LongValues[idx]),
                    PackedParam.TypeDouble => indexSearcher.GreaterThanOrEqualsQuery(fieldMeta, plan.DoubleValues[idx]),
                    _ => indexSearcher.GreaterThanOrEqualsQuery(fieldMeta, plan.StringValues[idx])
                };
            }

            case ClauseType.LessThan:
            {
                int idx = packed.Param1;
                return packed.ValueType switch
                {
                    PackedParam.TypeLong => indexSearcher.LessThanQuery(fieldMeta, plan.LongValues[idx]),
                    PackedParam.TypeDouble => indexSearcher.LessThanQuery(fieldMeta, plan.DoubleValues[idx]),
                    _ => indexSearcher.LessThanQuery(fieldMeta, plan.StringValues[idx])
                };
            }

            case ClauseType.LessThanOrEqual:
            {
                int idx = packed.Param1;
                return packed.ValueType switch
                {
                    PackedParam.TypeLong => indexSearcher.LessThanOrEqualsQuery(fieldMeta, plan.LongValues[idx]),
                    PackedParam.TypeDouble => indexSearcher.LessThanOrEqualsQuery(fieldMeta, plan.DoubleValues[idx]),
                    _ => indexSearcher.LessThanOrEqualsQuery(fieldMeta, plan.StringValues[idx])
                };
            }

            case ClauseType.Between:
            {
                if (exec.SentinelRewriteType != null)
                    return ResolveSentinelRewrittenBetween(exec, fieldMeta, indexSearcher, plan);
                int idx1 = packed.Param1;
                int idx2 = packed.Param2;
                return packed.ValueType switch
                {
                    PackedParam.TypeLong => indexSearcher.BetweenQuery(fieldMeta, plan.LongValues[idx1], plan.LongValues[idx2]),
                    PackedParam.TypeDouble => indexSearcher.BetweenQuery(fieldMeta, plan.DoubleValues[idx1], plan.DoubleValues[idx2]),
                    _ => indexSearcher.BetweenQuery(fieldMeta, plan.StringValues[idx1], plan.StringValues[idx2])
                };
            }

            case ClauseType.In:
            case ClauseType.AllIn:
            {
                // IN/AllIn inside an AndGroup reaches here as a single clause.
                // Expand each term into a TermQuery and merge via bitmap, same as the
                // top-level plan does with FillFromPostings/OrWithPostings/AndWithPostings.
                if (exec.InTermCount == 0)
                    return indexSearcher.EmptyMatch();
                var bm = new BitmapMatch(indexSearcher.Allocator);
                var temp = new RoaringBitmap(indexSearcher.Allocator);
                for (int t = 0; t < exec.InTermCount; t++)
                {
                    var termMatch = ResolveInTerm(clause, exec, t, indexSearcher, plan, parameters, builderParams);
                    if (clause.ClauseType == ClauseType.AllIn && t > 0)
                        QueryPrimitives.AndWithMatch(termMatch, ref bm.BitmapState, ref temp);
                    else
                        QueryPrimitives.OrWithMatch(termMatch, ref bm.BitmapState);
                }
                temp.Dispose();
                return bm;
            }

            case ClauseType.Exists:
                return indexSearcher.ExistsQuery(fieldMeta);

            case ClauseType.StartsWith:
                return indexSearcher.StartWithQuery(fieldMeta, plan.StringValues[packed.Param1]);

            case ClauseType.EndsWith:
                return indexSearcher.EndsWithQuery(fieldMeta, plan.StringValues[packed.Param1]);

            case ClauseType.Search:
            {
                FieldMetadata searchMeta;
                // Dynamic field name variants (search(FieldName) for auto-indexes) are
                // pre-resolved by the DynamicFieldNameResolve walker step at template time.
                string searchFieldName = clause.ResolvedFieldName ?? clause.FieldName;
                if (builderParams != null)
                {
                    bool forceSearch = builderParams.HasDynamics
                        && (builderParams.Index?.Configuration?.UseSearchAnalyzerForDynamicFieldsIfNotSetExplicitlyInSearchQuery ?? false);
                    searchMeta = QueryBuilderHelper.GetFieldMetadata(
                        builderParams.Allocator, searchFieldName, builderParams.Index,
                        builderParams.IndexFieldsMapping, builderParams.FieldsToFetch,
                        builderParams.HasDynamics, builderParams.DynamicFields,
                        handleSearch: true, hasBoost: builderParams.HasBoost,
                        forceDefaultSearchAnalyzer: forceSearch);
                }
                else if (parameters is { Index: not null, IndexFieldsMapping: not null })
                {
                    bool forceSearch = parameters.HasDynamics
                        && (parameters.Index?.Configuration?.UseSearchAnalyzerForDynamicFieldsIfNotSetExplicitlyInSearchQuery ?? false);
                    searchMeta = QueryBuilderHelper.GetFieldMetadata(
                        parameters.Allocator, searchFieldName, parameters.Index,
                        parameters.IndexFieldsMapping, parameters.FieldsToFetch,
                        parameters.HasDynamics, parameters.DynamicFields,
                        handleSearch: true, hasBoost: parameters.HasBoost,
                        forceDefaultSearchAnalyzer: forceSearch);
                }
                else
                {
                    searchMeta = fieldMeta;
                }

                var indexDef = builderParams?.Index?.Definition ?? parameters?.Index?.Definition;
                IndexSearcher.SearchQueryOptions searchQueryOptions;
                if (indexDef != null && IndexDefinitionBaseServerSide.IndexVersion.IsCoraxSearchWildcardAdjustmentSupported(indexDef.Version))
                    searchQueryOptions = IndexSearcher.SearchQueryOptions.PhraseQueryWithWildcardAdjustments;
                else if (indexDef is { Version: >= IndexDefinitionBaseServerSide.IndexVersion.PhraseQuerySupportInCoraxIndexes })
                    searchQueryOptions = IndexSearcher.SearchQueryOptions.PhraseQuery;
                else
                    searchQueryOptions = IndexSearcher.SearchQueryOptions.Legacy;

                var searchTerm = plan.StringValues[packed.Param1];
                if (searchQueryOptions == IndexSearcher.SearchQueryOptions.PhraseQueryWithWildcardAdjustments
                    && searchTerm is { Length: >= 1 }
                    && (searchTerm[0] == '*' || (searchTerm.Length >= 2 && searchTerm[^1] == '*')))
                {
                    searchMeta = ReplaceAnalyzerForWildcardQueries(searchMeta, builderParams, parameters);
                }

                var searchValues = QueryBuilderHelper.SplitSearchValue(searchTerm);

                return indexSearcher.SearchQuery(searchMeta,
                    searchValues,
                    (Constants.Search.Operator)clause.SearchOperator,
                    searchQueryOptions);
            }

            case ClauseType.Regex:
                return indexSearcher.RegexQuery(fieldMeta,
                    new System.Text.RegularExpressions.Regex(plan.StringValues[packed.Param1]));

            case ClauseType.Spatial:
            {
                if (builderParams == null)
                    throw new InvalidOperationException("Spatial resolution requires builder parameters");
                return HandleSpatial(builderParams, clause, exec, clause.SpatialMethodType);
            }

            case ClauseType.Vector:
            {
                if (builderParams == null)
                    throw new InvalidOperationException("Vector resolution requires builder parameters");
                var vectorItem = HandleVector(builderParams, clause, exec, false);
                return vectorItem.Materialize(null);
            }

            case ClauseType.OrGroup:
                throw new InvalidOperationException(
                    "OrGroup should be expanded by ResolveMatches, not resolved as a single clause.");

            case ClauseType.AndGroup:
                throw new InvalidOperationException(
                    "AndGroup should be expanded by ResolveMatches, not resolved as a single clause.");

            default:
                throw new InvalidOperationException($"Unexpected ClauseType {clause.ClauseType} in ResolveClause.");
        }
    }

    /// <summary>Resolve a single IN term to a typed TermQuery.
    /// IN terms are stored contiguously: PackedParamValue.Param1 = start index, InTermCount = count.
    /// Only non-null terms are in the typed array. Null is handled separately via HasNullTerm.</summary>
    private static IQueryMatch ResolveInTerm(ClauseInfo clause, ClauseExecution exec, int termIndex,
        IndexSearcher indexSearcher, QueryExecution plan,
        PlanParameters parameters, QueryBuilderParameters builderParams)
    {
        // Use ResolveFieldMetadata to pick up the exact/search field name variant
        // for dynamic indexes (#4777 fix).
        FieldMetadata fieldMeta = ResolveFieldMetadata(clause, indexSearcher, parameters, builderParams);

        var p = exec.PackedParamValue;
        int idx = p.Param1 + termIndex;
        var termPacked = new PackedParam(p.ValueType, idx);
        return TermQueryFromParam(termPacked, fieldMeta, indexSearcher, plan);
    }

    /// <summary>Create a pre-materialized <see cref="BitmapMatch"/> for a negated clause
    /// appearing in an OR chain. OR(NOT X, NOT Y, ...) cannot use the raw term posting list
    /// (FillBitmapFromPostingSource would add entries WITH X, not WITHOUT X). Instead, we
    /// pre-compute AllEntries ANDNOT (positive form) into a BitmapMatch so that OrWithMatch
    /// during execution correctly ORs in the set of entries NOT matching the positive predicate.
    /// Handles NOT EQUALS (single term), NOT EXISTS (ExistsQuery), and single-term NOT IN/AllIn.</summary>
    private static IQueryMatch CreateNotEqualsOrMatch(ClauseInfo clause, ClauseExecution exec, IndexSearcher indexSearcher,
        QueryExecution plan, PlanParameters parameters, QueryBuilderParameters builderParams)
    {
        // Resolve the positive form of the match. For IN/AllIn clauses, ResolveClause
        // handles multi-term expansion correctly. For simple Equals/NotEquals, the
        // single-term TermQueryFromParam suffices. EXISTS clauses carry no PackedParam.
        IQueryMatch termMatch;
        if (clause.ClauseType is ClauseType.In or ClauseType.AllIn)
        {
            termMatch = ResolveClause(clause, exec, indexSearcher, plan, parameters, builderParams);
        }
        else if (clause.ClauseType == ClauseType.Exists)
        {
            FieldMetadata fieldMeta = ResolveFieldMetadata(clause, indexSearcher, parameters, builderParams);
            termMatch = indexSearcher.ExistsQuery(fieldMeta);
        }
        else
        {
            FieldMetadata fieldMeta = ResolveFieldMetadata(clause, indexSearcher, parameters, builderParams);
            termMatch = TermQueryFromParam(exec.PackedParamValue, fieldMeta, indexSearcher, plan);
        }

        var bitmapMatch = new BitmapMatch(indexSearcher.Allocator);
        var tempData = new RoaringBitmap(indexSearcher.Allocator);
        QueryPrimitives.OrWithMatch(indexSearcher.AllEntries(), ref bitmapMatch.BitmapState);
        QueryPrimitives.AndNotWithMatch(termMatch, ref bitmapMatch.BitmapState, ref tempData);
        tempData.Dispose();
        return bitmapMatch;
    }

    // ── Term-source resolution ───────────────────────────────────────────

    /// <summary>
    /// Resolve clause infos to <see cref="PostingSource"/> instances for the native
    /// posting-list dispatch path. Parallels <see cref="ResolveMatches"/> — the
    /// returned array uses the same indexing scheme. Slots whose underlying
    /// clause is multi-term / non-term-shaped (Spatial, Vector, Search, Range,
    /// StartsWith, EndsWith, Regex, AllEntries) keep <c>Kind == PostingSourceKind.Empty</c>;
    /// only Equals / NotEquals / In / AllIn / OrGroup-of-(Not)Equals slots populate.
    /// The IL emitter consults <see cref="PlanOp.Dispatch"/> to decide which
    /// array to read.
    /// </summary>
    private static PostingSource[] ResolveTermSources(QueryExecution plan, IndexSearcher indexSearcher,
        PlanParameters parameters, QueryBuilderParameters builderParams)
    {
        // IsAllEntries plans never emit term ops (FillFromPostings / AndWith / etc.) —
        // their match[0] is AllEntries, post-filter slots are spatial/vector. No
        // PostingSource population is needed.
        if (plan.IsAllEntries)
            return [];

        if (plan.Clauses is not { Count: > 0 } clauses)
            return [];

        var execs = plan.Executions;

        // Standalone NotEquals: matches[0] = AllEntries (NOT a term source),
        // matches[1] = the negated term. Mirror that layout.
        if (clauses.Count == 1 && clauses[0].IsNegated && !plan.AllNegated)
        {
            var sources = new PostingSource[2];
            sources[1] = ResolveSingleTermSource(clauses[0], execs[0], indexSearcher, plan, parameters, builderParams);
            return sources;
        }

        var termSources = new PostingSource[CountMatchSlots(clauses, execs, plan.IsAllEntries, plan.AllNegated)];
        int matchIdx = 0;
        for (int ci = 0; ci < clauses.Count; ci++)
        {
            ClauseInfo clause = clauses[ci];
            ClauseExecution exec = execs[ci];
            switch (clause.ClauseType)
            {
                case ClauseType.OrGroup when clause.OrSubClauses is { Count: > 0 }:
                {
                    for (int si = 0; si < clause.OrSubClauses.Count; si++)
                    {
                        var sub = clause.OrSubClauses[si];
                        var subExec = exec.OrSubExecutions[si];
                        if (subExec.BoostFactor > 0)
                        {
                            matchIdx++;
                            continue;
                        }
                        termSources[matchIdx++] = ResolveSingleTermSource(sub, subExec, indexSearcher, plan, parameters, builderParams);
                    }

                    break;
                }
                case ClauseType.AndGroup when clause.AndSubClauses is { Count: > 0 }:
                {
                    for (int si = 0; si < clause.AndSubClauses.Count; si++)
                    {
                        var sub = clause.AndSubClauses[si];
                        var subExec = exec.AndSubExecutions[si];
                        if (subExec.BoostFactor > 0)
                        {
                            matchIdx++;
                            continue;
                        }
                        termSources[matchIdx++] = ResolveSingleTermSource(sub, subExec, indexSearcher, plan, parameters, builderParams);
                    }

                    break;
                }
                case ClauseType.AllIn or ClauseType.In:
                {
                    for (int t = 0; t < exec.InTermCount; t++)
                        termSources[matchIdx++] = ResolveInTermSource(clause, exec, t, indexSearcher, plan, parameters, builderParams);
                    // Null-term slot: resolve via the null posting list so that the PostingList
                    // dispatch path (used by EmitInOps/EmitAllInOps) can read it.  When HasNullTerm
                    // is false the slot stays Empty and the compiled OR/AND step is a no-op.
                    if (exec.HasNullTerm)
                    {
                        FieldMetadata nullMeta = ResolveFieldMetadata(clause, indexSearcher, parameters, builderParams);
                        if (indexSearcher.TryGetPostingListForNull(in nullMeta, out long nullPlId))
                            termSources[matchIdx] = DecodePostingListId(nullPlId, indexSearcher);
                    }
                    matchIdx++;
                    break;
                }
                default:
                {
                    if (exec.BoostFactor > 0)
                    {
                        matchIdx++;
                        continue;
                    }
                    termSources[matchIdx++] = ResolveSingleTermSource(clause, exec, indexSearcher, plan, parameters, builderParams);
                    break;
                }
            }
        }
        // AllNegated extra slot is AllEntries — stays Empty in TermSources.
        return termSources;
    }

    /// <summary>Resolve TreeScan-eligible clauses to ITermsProvider instances for direct
    /// tree-scan dispatch in the compiled pipeline. Slot indexing is parallel to
    /// ResolveMatches/ResolveTermSources. Returns null if no TreeScan clauses exist.</summary>
    private static ITermsProvider[] ResolveTermsProviders(QueryExecution plan, IndexSearcher indexSearcher,
        PlanParameters parameters, QueryBuilderParameters builderParams)
    {
        if (plan.IsAllEntries || plan.Clauses is not { Count: > 0 } clauses)
            return null;

        var execs = plan.Executions;
        bool hasAnyTreeScan = false;

        // Quick check: do we have any TreeScan clauses at all?
        for (int ci = 0; ci < clauses.Count; ci++)
        {
            var clause = clauses[ci];
            var exec = execs != null && ci < execs.Length ? execs[ci] : null;

            if (IsTreeScanEligibleClause(clause))
            {
                hasAnyTreeScan = true;
                break;
            }

            // Check subclauses
            if (clause.OrSubClauses != null)
            {
                for (int si = 0; si < clause.OrSubClauses.Count; si++)
                {
                    var subExec = exec?.OrSubExecutions?[si];
                    if (IsTreeScanEligibleClause(clause.OrSubClauses[si]))
                    {
                        hasAnyTreeScan = true;
                        break;
                    }
                }
            }
            if (hasAnyTreeScan) break;

            if (clause.AndSubClauses != null)
            {
                for (int si = 0; si < clause.AndSubClauses.Count; si++)
                {
                    var subExec = exec?.AndSubExecutions?[si];
                    if (IsTreeScanEligibleClause(clause.AndSubClauses[si]))
                    {
                        hasAnyTreeScan = true;
                        break;
                    }
                }
            }
            if (hasAnyTreeScan) break;
        }

        if (!hasAnyTreeScan)
            return null;

        int totalSlots = CountMatchSlots(clauses, execs, plan.IsAllEntries, plan.AllNegated);
        var providers = new ITermsProvider[totalSlots];
        int matchIdx = 0;

        for (int ci = 0; ci < clauses.Count; ci++)
        {
            ClauseInfo clause = clauses[ci];
            ClauseExecution exec = execs != null && ci < execs.Length ? execs[ci] : null;

            switch (clause.ClauseType)
            {
                case ClauseType.OrGroup when clause.OrSubClauses is { Count: > 0 }:
                    for (int si = 0; si < clause.OrSubClauses.Count; si++)
                    {
                        var sub = clause.OrSubClauses[si];
                        var subExec = exec?.OrSubExecutions?[si];
                        providers[matchIdx] = ResolveSingleTermsProvider(sub, subExec, indexSearcher, plan, parameters, builderParams);
                        matchIdx++;
                    }
                    break;

                case ClauseType.AndGroup when clause.AndSubClauses is { Count: > 0 }:
                    for (int si = 0; si < clause.AndSubClauses.Count; si++)
                    {
                        var sub = clause.AndSubClauses[si];
                        var subExec = exec?.AndSubExecutions?[si];
                        providers[matchIdx] = ResolveSingleTermsProvider(sub, subExec, indexSearcher, plan, parameters, builderParams);
                        matchIdx++;
                    }
                    break;

                case ClauseType.AllIn or ClauseType.In:
                    // IN terms use PostingList dispatch, not TreeScan. +1 for null-term slot (always allocated).
                    matchIdx += (exec?.InTermCount ?? 0) + 1;
                    break;

                default:
                    providers[matchIdx] = ResolveSingleTermsProvider(clause, exec, indexSearcher, plan, parameters, builderParams);
                    matchIdx++;
                    break;
            }
        }

        return providers;
    }

    /// <summary>Resolve a single TreeScan-eligible clause to its raw ITermsProvider.
    /// Returns null for non-TreeScan clauses or when the field doesn't exist in the
    /// index (factory method returned TermMatch.Empty instead of TermsProviderMatch).
    /// Null slots cause the IL to fall through to the QueryMatch dispatch path.</summary>
    private static ITermsProvider ResolveSingleTermsProvider(ClauseInfo clause, ClauseExecution exec,
        IndexSearcher indexSearcher, QueryExecution plan, PlanParameters parameters, QueryBuilderParameters builderParams)
    {
        if (IsTreeScanEligibleClause(clause) == false)
            return null;

        // Create the match via the existing factory methods, then extract the provider.
        // The factory methods handle all complexity (analyzer, CompactKey, tree lookup).
        var match = ResolveClause(clause, exec ?? new ClauseExecution(), indexSearcher, plan, parameters, builderParams);
        if (match is TermsProviderMatch tpm)
            return tpm.Provider;

        // Factory returned something other than TermsProviderMatch (e.g. TermMatch.Empty
        // when the field doesn't exist). Return an empty provider so the IL's TreeScan
        // dispatch gets a valid (no-op) provider instead of null.
        return EmptyTermsProviderInstance.Instance;
    }

    /// <summary>Resolve a single Equals / NotEquals clause to a posting-list ID and
    /// decode it into a <see cref="PostingSource"/>. Returns Empty when the clause
    /// is non-term-shaped or the term doesn't exist in the index.</summary>
    private static PostingSource ResolveSingleTermSource(ClauseInfo clause, ClauseExecution exec, IndexSearcher indexSearcher,
        QueryExecution plan, PlanParameters parameters, QueryBuilderParameters builderParams)
    {
        if (IsTermSourceEligibleClause(clause) == false)
            return default; // Kind == Empty

        FieldMetadata fieldMeta = ResolveFieldMetadata(clause, indexSearcher, parameters, builderParams);
        long postingListId = GetTermPostingListIdFromParam(exec.PackedParamValue, fieldMeta, indexSearcher, plan);
        return DecodePostingListId(postingListId, indexSearcher);
    }

    /// <summary>Resolve a single In/AllIn term to a posting-list ID.
    /// Uses <paramref name="termIndex"/> into <see cref="ClauseInfo.InTerms"/> /
    /// <see cref="ClauseInfo.InTermTypes"/> to pick the correct numeric vs. string
    /// overload — avoids the long.TryParse false-positive on zero-padded string
    /// values like "000001" (parses as 1L but is indexed as the string "000001").</summary>
    private static PostingSource ResolveInTermSource(ClauseInfo clause, ClauseExecution exec, int termIndex, IndexSearcher indexSearcher,
        QueryExecution plan, PlanParameters parameters, QueryBuilderParameters builderParams)
    {
        // Use ResolveFieldMetadata to pick up the exact/search field name variant (#4777 fix).
        FieldMetadata fieldMeta = ResolveFieldMetadata(clause, indexSearcher, parameters, builderParams);

        var p = exec.PackedParamValue;
        int idx = p.Param1 + termIndex;
        var termPacked = new PackedParam(p.ValueType, idx);
        long postingListId = GetTermPostingListIdFromParam(termPacked, fieldMeta, indexSearcher, plan);
        return DecodePostingListId(postingListId, indexSearcher);
    }

    /// <summary>Resolve field metadata for a term-source clause. Mirrors the
    /// non-Spatial/Vector/Search branch of <see cref="ResolveClause"/>.</summary>
    private static FieldMetadata ResolveFieldMetadata(ClauseInfo clause, IndexSearcher indexSearcher,
        PlanParameters parameters, QueryBuilderParameters builderParams)
    {
        if (builderParams != null)
        {
            // Dynamic field name variants are pre-resolved by DynamicFieldNameResolve at template time.
            string resolvedFieldName = clause.ResolvedFieldName ?? clause.FieldName;
            // When forceDefaultSearchAnalyzer is enabled for indexes with dynamic fields (CreateField),
            // non-exact non-search clauses should use the search analyzer (#4778 fix).
            bool forceSearchAnalyzer = builderParams.HasDynamics
                && clause.IsExact == false
                && clause.ClauseType != ClauseType.Search
                && (builderParams.Index?.Configuration?.UseSearchAnalyzerForDynamicFieldsIfNotSetExplicitlyInSearchQuery ?? false);
            return QueryBuilderHelper.GetFieldMetadata(in builderParams, resolvedFieldName, exact: clause.IsExact,
                hasBoost: builderParams.HasBoost, forceDefaultSearchAnalyzer: forceSearchAnalyzer);
        }

        return indexSearcher.FieldMetadataBuilder(clause.FieldName, hasBoost: parameters?.HasBoost ?? false);
    }

    /// <summary>Decode a raw posting-list ID (with TermIdMask bits) into a
    /// <see cref="PostingSource"/>. Returns Empty when the term doesn't exist (-1).
    /// For PostingList kind, opens a fresh iterator on the underlying set.</summary>
    private static PostingSource DecodePostingListId(long postingListId, IndexSearcher indexSearcher)
    {
        if (postingListId == -1)
        {
            return default; // Kind == Empty
        }

        var termType = (global::Corax.Indexing.TermIdMask)postingListId & global::Corax.Indexing.TermIdMask.EnsureIsSingleMask;
        switch (termType)
        {
            case global::Corax.Indexing.TermIdMask.Single:
                return new PostingSource
                {
                    Kind = PostingSourceKind.Single,
                    SingleEntryId = (long)EntryIdEncodings.GetContainerId(postingListId),
                };

            case global::Corax.Indexing.TermIdMask.SmallPostingList:
                return new PostingSource
                {
                    Kind = PostingSourceKind.SmallPostingList,
                    SmallPostingListId = (long)EntryIdEncodings.GetContainerId(postingListId),
                };

            case global::Corax.Indexing.TermIdMask.PostingList:
            {
                var postingList = indexSearcher.GetPostingList(postingListId);
                return new PostingSource
                {
                    Kind = PostingSourceKind.PostingList,
                    LargeIterator = postingList.Iterate(),
                };
            }

            default:
                return default;
        }
    }

    // ── Scan parameter extraction ────────────────────────────────────────

    /// <summary>Extract typed parameter values from clauses for entry scan.
    /// Called per-query at execution time. The values populate the CompiledQueryMatch arrays.</summary>
    private static void ExtractScanParameters(QueryExecution plan, IndexSearcher indexSearcher,
        out long[] longParams, out double[] doubleParams, out Voron.Slice[] sliceParams, out long[] fieldRootPages)
    {
        var predicates = plan.ScanPredicateInfos;
        if (predicates == null || predicates.Length == 0)
        {
            longParams = [];
            doubleParams = [];
            sliceParams = [];
            fieldRootPages = [];
            return;
        }

        var longs = new List<long>();
        var doubles = new List<double>();
        var slices = new List<Voron.Slice>();
        var roots = new List<long>();

        // Walk predicates and clauses in lock-step. BuildScanPredicateInfo skips non-eligible
        // clauses (Search, In, AllIn, Exists, StartsWith, EndsWith, Regex, Spatial, Vector,
        // AndGroup), so we must skip them here too to keep the 1:1 positional mapping.
        int scanStart = plan.AllNegated ? 0 : 1;
        int clauseIdx = scanStart;
        var clauses = plan.Clauses;
        var execs = plan.Executions;
        int dummyL = 0, dummyD = 0, dummyS = 0;
        foreach (ScanPredicateInfo pred in predicates)
        {
            // Advance past clauses that BuildScanPredicateInfo would have skipped (returned null).
            while (clauseIdx < clauses.Count &&
                   BuildScanPredicateInfo(clauses[clauseIdx], execs != null && clauseIdx < execs.Length ? execs[clauseIdx] : null,
                       ref dummyL, ref dummyD, ref dummyS) == null)
            {
                clauseIdx++;
            }

            ClauseInfo matchingClause = clauseIdx < clauses.Count ? clauses[clauseIdx] : null;
            ClauseExecution matchingExec = execs != null && clauseIdx < execs.Length ? execs[clauseIdx] : null;
            clauseIdx++;
            ExtractParamsFromPredicate(pred, matchingClause, matchingExec, indexSearcher, plan, longs, doubles, slices, roots);
        }

        longParams = longs.Count > 0 ? longs.ToArray() : [];
        doubleParams = doubles.Count > 0 ? doubles.ToArray() : [];
        sliceParams = slices.Count > 0 ? slices.ToArray() : [];
        fieldRootPages = roots.Count > 0 ? roots.ToArray() : [];
    }

    private static void ExtractParamsFromPredicate(ScanPredicateInfo pred, ClauseInfo clause, ClauseExecution exec,
        IndexSearcher indexSearcher, QueryExecution plan, List<long> longs, List<double> doubles,
        List<Voron.Slice> slices, List<long> roots)
    {
        if (pred.SubPredicates != null)
        {
            // Each OrBranch corresponds to a subclause of the OrGroup.
            // Pass subclauses positionally to avoid the same field-name ambiguity.
            List<ClauseInfo> subClauses = clause?.OrSubClauses;
            ClauseExecution[] subExecs = exec?.OrSubExecutions;
            for (int b = 0; b < pred.SubPredicates.Length; b++)
            {
                ClauseInfo subClause = (subClauses != null && b < subClauses.Count) ? subClauses[b] : null;
                ClauseExecution subExec = (subExecs != null && b < subExecs.Length) ? subExecs[b] : null;
                ExtractParamsFromPredicate(pred.SubPredicates[b], subClause, subExec, indexSearcher, plan, longs, doubles, slices, roots);
            }
            return;
        }

        // Resolve field root page
        roots.Add(indexSearcher.FieldCache.GetLookupRootPage(pred.FieldName));

        if (clause == null || exec == null)
            return;

        // Read pre-resolved typed values from the plan's arrays via packed param.
        var packed = exec.PackedParamValue;
        if (packed.IsNone)
            return;
        int idx1 = packed.Param1;
        int idx2 = packed.Param2;
        bool hasBetween = idx2 != PackedParam.NoParamValue;

        switch (pred.ValueType)
        {
            case ScanValueType.Long:
                longs.Add(plan.LongValues[idx1]);
                if (hasBetween)
                    longs.Add(plan.LongValues[idx2]);
                break;
            case ScanValueType.Double:
                doubles.Add(plan.DoubleValues[idx1]);
                if (hasBetween)
                    doubles.Add(plan.DoubleValues[idx2]);
                break;
            case ScanValueType.Slice:
            case ScanValueType.SliceLong:
                var fieldMeta = indexSearcher.FieldMetadataBuilder(clause.FieldName);
                slices.Add(indexSearcher.EncodeAndApplyAnalyzer(fieldMeta, plan.StringValues[idx1]));
                if (hasBetween)
                    slices.Add(indexSearcher.EncodeAndApplyAnalyzer(fieldMeta, plan.StringValues[idx2]));
                break;
        }
    }

    // ── Highlighting ─────────────────────────────────────────────────────

    /// <summary>
    /// Populate the highlighting terms dictionary from the plan's clauses.
    /// The old CoraxQueryBuilder did this as a side effect during query building.
    /// The bitmap pipeline must do it explicitly after plan building.
    /// </summary>
    private static void PopulateHighlightingTerms(QueryExecution plan, Dictionary<string, CoraxHighlightingTermIndex> highlightingTerms, QueryMetadata metadata)
    {
        if (highlightingTerms == null || plan.Clauses is not { Count: > 0 } clauses)
            return;

        var execs = plan.Executions;
        for (int ci = 0; ci < clauses.Count; ci++)
        {
            var clauseObj = clauses[ci];
            var exec = execs != null && ci < execs.Length ? execs[ci] : null;

            // Recurse into sub-clauses before checking FieldName: OrGroup/AndGroup have
            // FieldName==null (they are structural wrappers, not field clauses), so a
            // FieldName-first guard would skip their children entirely.
            switch (clauseObj?.ClauseType)
            {
                case ClauseType.OrGroup when clauseObj.OrSubClauses is { Count: > 0 }:
                {
                    for (int si = 0; si < clauseObj.OrSubClauses.Count; si++)
                    {
                        var subExec = exec?.OrSubExecutions != null && si < exec.OrSubExecutions.Length ? exec.OrSubExecutions[si] : null;
                        PopulateHighlightingForClause(clauseObj.OrSubClauses[si], subExec, highlightingTerms, metadata, plan);
                    }
                    break;
                }
                case ClauseType.AndGroup when clauseObj.AndSubClauses is { Count: > 0 }:
                {
                    for (int si = 0; si < clauseObj.AndSubClauses.Count; si++)
                    {
                        var subExec = exec?.AndSubExecutions != null && si < exec.AndSubExecutions.Length ? exec.AndSubExecutions[si] : null;
                        PopulateHighlightingForClause(clauseObj.AndSubClauses[si], subExec, highlightingTerms, metadata, plan);
                    }
                    break;
                }
            }

            if (clauseObj?.FieldName == null)
                continue;

            PopulateHighlightingForClause(clauseObj, exec, highlightingTerms, metadata, plan);
        }
    }

    private static void PopulateHighlightingForClause(ClauseInfo clause, ClauseExecution exec, Dictionary<string, CoraxHighlightingTermIndex> highlightingTerms, QueryMetadata metadata, QueryExecution plan)
    {
        string fieldName = clause.FieldName;
        if (fieldName == null)
            return;

        // Skip highlighting for null-valued clauses (e.g. WHERE City == null).
        // Null is not a search term — there's nothing to highlight. Without this
        // guard, the highlighter produces a spurious result for null fields (#4781).
        if (clause.ClauseType is ClauseType.Equals or ClauseType.NotEquals)
        {
            var packed = exec?.PackedParamValue ?? PackedParam.None;
            if (packed.IsNone || (packed.ValueType == PackedParam.TypeString
                && packed.Param1 >= 0 && plan.StringValues != null
                && packed.Param1 < plan.StringValues.Length
                && plan.StringValues[packed.Param1] == null))
            {
                return;
            }
        }

        if (highlightingTerms.TryGetValue(fieldName, out var existingTerm))
        {
            existingTerm.Values ??= GetHighlightingValues(clause, exec, plan);
            return;
        }

        var term = new CoraxHighlightingTermIndex
        {
            FieldName = fieldName,
            Values = GetHighlightingValues(clause, exec, plan)
        };

        if (metadata.IsDynamic && clause.ClauseType == ClauseType.Search)
            term.DynamicFieldName = AutoIndexField.GetSearchAutoIndexFieldName(fieldName);
        else if (metadata.IsDynamic && clause.IsExact)
            term.DynamicFieldName = AutoIndexField.GetExactAutoIndexFieldName(fieldName);

        highlightingTerms[fieldName] = term;

        // For dynamic indexes, also add the dynamic field name variant
        if (term.DynamicFieldName != null)
            highlightingTerms[term.DynamicFieldName] = term;
    }

    private static object GetHighlightingValues(ClauseInfo clause, ClauseExecution exec, QueryExecution plan)
    {
        var packed = exec?.PackedParamValue ?? PackedParam.None;
        if (clause.ClauseType == ClauseType.Between)
        {
            return new Tuple<string, string>(
                FormatValueFromPlan(packed, plan),
                FormatValue2FromPlan(packed, plan));
        }

        int inTermCount = exec?.InTermCount ?? 0;
        bool hasNullTerm = exec?.HasNullTerm ?? false;
        if (clause.ClauseType is ClauseType.In or ClauseType.AllIn && (inTermCount > 0 || hasNullTerm))
        {
            var p = packed;
            var terms = new List<string>(inTermCount + (hasNullTerm ? 1 : 0));
            for (int t = 0; t < inTermCount; t++)
                terms.Add(FormatValueFromPlan(new PackedParam(p.ValueType, p.Param1 + t), plan));
            if (hasNullTerm)
                terms.Add(null);
            return terms;
        }

        return FormatValueFromPlan(packed, plan);
    }

    // ── Vector / Spatial resolution ──────────────────────────────────────

    /// <summary>
    /// Resolve vector select operations from the plan into CoraxVectorItem instances.
    /// These are NOT materialized yet — the caller materializes them with the bitmap-producing
    /// match as the filterQuery. Returns null if the plan has no vectors.
    /// </summary>
    private static CoraxVectorItem[] ResolveVectorItems(QueryExecution plan, QueryBuilderParameters builderParams)
    {
        if (plan.VectorSelects == null || plan.VectorSelects.Length == 0)
            return null;

        var items = new CoraxVectorItem[plan.VectorSelects.Length];
        for (int i = 0; i < plan.VectorSelects.Length; i++)
        {
            var clause = plan.VectorSelects[i].Clause;
            var exec = plan.VectorSelects[i].Exec;
            if (clause == null || clause.ClauseType != ClauseType.Vector || builderParams == null)
                throw new InvalidOperationException("Vector select references an invalid clause at index " + i);

            items[i] = HandleVector(builderParams, clause, exec, false);
        }
        return items;
    }

    private static IQueryMatch HandleSpatial(QueryBuilderParameters builderParameters, ClauseInfo clause, ClauseExecution exec, SpatialOperationType spatialMethod)
    {
        var index = builderParameters.Index;
        var allocator = builderParameters.Allocator;

        // Field name was pre-resolved during parsing.
        string fieldName = clause.FieldName
            ?? throw new InvalidOperationException("Spatial clause has no pre-resolved field name.");

        var fieldMetadata = QueryBuilderHelper.GetFieldMetadata(allocator, fieldName, index, builderParameters.IndexFieldsMapping,
            builderParameters.FieldsToFetch, builderParameters.HasDynamics, builderParameters.DynamicFields, hasBoost: builderParameters.HasBoost);

        var sp = exec.Spatial;
        var distanceErrorPct = sp.DistanceErrorPct >= 0
            ? sp.DistanceErrorPct
            : RavenConstants.Documents.Indexing.Spatial.DefaultDistanceErrorPct;

        var spatialField = builderParameters.Factories.GetSpatialFieldFactory(fieldName);

        // Build shape from pre-resolved parameters — no GetValue calls
        IShape shape;
        SpatialUnits? units = sp.Units.HasValue ? (SpatialUnits)sp.Units.Value : null;
        if (sp.ShapeType == SpatialShapeType.Circle)
        {
            shape = spatialField.ReadCircle(sp.CircleRadius, sp.CircleLatitude, sp.CircleLongitude, units);
        }
        else if (sp.Wkt != null)
        {
            shape = spatialField.ReadShape(sp.Wkt, units);
        }
        else
        {
            throw new InvalidOperationException("Spatial clause has no pre-resolved shape parameters.");
        }

        return builderParameters.IndexSearcher.SpatialQuery(fieldMetadata, distanceErrorPct, shape, spatialField.GetContext(), (SpatialRelation)spatialMethod, token: builderParameters.Token);
    }

    private static CoraxVectorItem HandleVector(QueryBuilderParameters builderParameters, ClauseInfo clause, ClauseExecution exec, bool exact)
    {
        IndexField indexField;
        string embeddingsGenerationTaskIdentifier;

        var vec = exec.Vector;
        var minimumMatch = vec.MinimumMatch >= 0
            ? vec.MinimumMatch
            : builderParameters.Index.Configuration.CoraxVectorSearchDefaultMinimumSimilarity;

        int numberOfCandidates = vec.NumberOfCandidates >= 0
            ? vec.NumberOfCandidates
            : builderParameters.Index.Configuration.CoraxVectorDefaultNumberOfCandidatesForQuerying;

        var fieldName = clause.FieldName
            ?? throw new InvalidOperationException("Vector clause has no pre-resolved field name.");

        var fieldMetadata = QueryBuilderHelper.GetFieldMetadata(builderParameters, fieldName, hasBoost: builderParameters.HasBoost);

        // Use pre-resolved vector value and method kind from parsing
        object methodParameter = vec.ResolvedValue;
        ValueTokenType valueTokenType = ToValueTokenType(vec.ResolvedValueType);

        if (vec.Method != VectorSourceKind.Inline)
        {
            var method = vec.Method switch
            {
                VectorSourceKind.FromDocument => VectorHelpers.MethodVectorValue.ForDocument,
                VectorSourceKind.FromText => VectorHelpers.MethodVectorValue.EmbeddingText,
                _ => throw new InvalidDataException($"Unknown vector source kind: {vec.Method}")
            };

            if (method is not VectorHelpers.MethodVectorValue.EmbeddingText)
            {
                return (method, methodParameter) switch
                {
                    (method: VectorHelpers.MethodVectorValue.ForDocument, string docId) => CoraxVectorItem.BuildForDocVector(builderParameters, fieldMetadata, docId, numberOfCandidates, minimumMatch, exact),
                    (method: VectorHelpers.MethodVectorValue.ForDocument, StringSegment docIdSegment) => CoraxVectorItem.BuildForDocVector(builderParameters, fieldMetadata, docIdSegment.Value, numberOfCandidates, minimumMatch, exact),
                    (method: VectorHelpers.MethodVectorValue.ForRaw, string vectorAsBase64) => CoraxVectorItem.BuildSingleVector(builderParameters, fieldMetadata, GenerateEmbeddings.FromBase64Array(VectorOptions.Default, builderParameters.Allocator, vectorAsBase64), numberOfCandidates, minimumMatch, exact),
                    (method: VectorHelpers.MethodVectorValue.ForRaw, StringSegment stringSegmentAsBase64) => CoraxVectorItem.BuildSingleVector(builderParameters, fieldMetadata, GenerateEmbeddings.FromBase64Array(VectorOptions.Default, builderParameters.Allocator, stringSegmentAsBase64.ToString()), numberOfCandidates, minimumMatch, exact),
                    (_, BlittableJsonReaderArray { Length: > 0 }) => throw new InvalidDataException("Cannot perform search on empty value."),
                    _ => throw new InvalidQueryException(
                        $"Unknown method in value ({vec.Method}. Parameter type: {methodParameter?.GetType().FullName}, Value: {methodParameter}")
                };
            }

            embeddingsGenerationTaskIdentifier = vec.AiTaskName;
            var vectorOptions = VectorHelpers.GetExplicitVectorOptions(builderParameters, fieldName, out indexField);
            if (vectorOptions != null)
            {
                vectorOptions = new VectorOptions()
                {
                    DestinationEmbeddingType = vectorOptions.DestinationEmbeddingType,
                    Dimensions = vectorOptions.Dimensions,
                    SourceEmbeddingType = VectorEmbeddingType.Text,
                    NumberOfCandidatesForIndexing = vectorOptions.NumberOfCandidatesForIndexing,
                    NumberOfEdges = vectorOptions.NumberOfEdges
                };
            }

            var vector = VectorHelpers.GetEmbeddingsForQueryParameter(builderParameters, valueTokenType, methodParameter, embeddingsGenerationTaskIdentifier, vectorOptions, fieldName);

            if (vector.SingleVector != null)
                return CoraxVectorItem.BuildSingleVector(builderParameters, fieldMetadata, vector.SingleVector.Value, numberOfCandidates, minimumMatch, exact);

            return CoraxVectorItem.BuildMultiVector(builderParameters, fieldMetadata, vector.MultiVector, numberOfCandidates, minimumMatch, exact);
        }

        // Direct value (not a method call) — use pre-resolved value
        var value = methodParameter;
        var valueType = valueTokenType;

        (VectorValue? SingleVector, VectorValue[] MultiVector) transformedEmbeddings = (null, null);
        int numberOfDimensions;
        if (VectorHelpers.TryRetrieveEmbeddingsGenerationTaskIdentifier(builderParameters, fieldName, out embeddingsGenerationTaskIdentifier))
        {
            var vectorOptions = VectorHelpers.GetExplicitVectorOptions(builderParameters, fieldName, out indexField);
            transformedEmbeddings = VectorHelpers.GetEmbeddingsForQueryParameter(builderParameters, valueType, value, embeddingsGenerationTaskIdentifier, vectorOptions, fieldName);
        }
        else
        {
            VectorOptions vectorOptions = VectorHelpers.GetOptions(builderParameters, fieldName, out indexField);

            if (builderParameters.Index.IndexFieldsPersistence.TryReadNumberOfDimensions(fieldName, out numberOfDimensions) == false)
                return CoraxVectorItem.BuildEmpty(builderParameters); // no vector indexed
            if (vectorOptions.SourceEmbeddingType is VectorEmbeddingType.Text)
            {
                transformedEmbeddings = VectorHelpers.GetVectorValueForTextualInput(builderParameters, vectorOptions, valueType, value);
            }
            else
            {
                switch (value)
                {
                    case string s:
                        transformedEmbeddings.SingleVector = GenerateEmbeddings.FromBase64Array(vectorOptions, builderParameters.Allocator, s);
                        break;
                    case StringSegment stringSegment:
                        transformedEmbeddings.SingleVector = GenerateEmbeddings.FromBase64Array(vectorOptions, builderParameters.Allocator, stringSegment.ToString());
                        break;
                    case BlittableJsonReaderObject bjro:
                        transformedEmbeddings.SingleVector = VectorHelpers.GetVectorValueFromRavenVector(builderParameters, bjro, vectorOptions);
                        break;
                    case BlittableJsonReaderArray { Length: > 0 } bjra:
                    {
                        var isRavenVector = bjra[0] is BlittableJsonReaderObject;
                        var isStringArray = bjra[0] is string or StringSegment or LazyStringValue;
                        var isArray = bjra[0] is BlittableJsonReaderArray;

                        if (isRavenVector == false && isStringArray == false && isArray == false)
                        {
                            transformedEmbeddings.SingleVector = VectorHelpers.GetVectorValueFromNumericalBlittableArray(builderParameters, bjra, vectorOptions);
                        }
                        else
                        {
                            var embeddings = new VectorValue[bjra.Length];
                            for (int i = 0; i < bjra.Length; ++i)
                            {
                                if (isRavenVector)
                                    embeddings[i] = VectorHelpers.GetVectorValueFromRavenVector(builderParameters, (BlittableJsonReaderObject)bjra[i], vectorOptions);
                                else if (isStringArray)
                                    embeddings[i] = GenerateEmbeddings.FromBase64Array(vectorOptions, builderParameters.Allocator, bjra[i].ToString());
                                else
                                    embeddings[i] = VectorHelpers.GetVectorValueFromNumericalBlittableArray(builderParameters, (BlittableJsonReaderArray)bjra[i],
                                        vectorOptions);
                            }

                            transformedEmbeddings.MultiVector = embeddings;
                        }

                        break;
                    }
                    default:
                        PortableExceptions.Throw<InvalidDataException>("We expected to get vector(s), however got: " + value.GetType().Name);
                        break;
                }
            }
        }

        if (builderParameters.Index.IndexFieldsPersistence.TryReadNumberOfDimensions(fieldName, out numberOfDimensions) == false)
            return CoraxVectorItem.BuildEmpty(builderParameters); // no vector indexed

        if (transformedEmbeddings.SingleVector != null)
        {
            var singleVector = transformedEmbeddings.SingleVector.Value;

            if (indexField != null)
                AssertDimensions(singleVector);
            return CoraxVectorItem.BuildSingleVector(builderParameters, fieldMetadata, singleVector, numberOfCandidates, minimumMatch, exact);
        }

        if (transformedEmbeddings.MultiVector != null)
        {
            var multiVector = transformedEmbeddings.MultiVector;

            if (indexField != null)
            {
                foreach (var vector in multiVector)
                    AssertDimensions(vector);
            }
            return CoraxVectorItem.BuildMultiVector(builderParameters, fieldMetadata, multiVector, numberOfCandidates, minimumMatch, exact);
        }

        throw new InvalidDataException("Expected to get single or multiple embeddings of VectorValue type but none was provided");

        void AssertDimensions(in VectorValue vector)
        {
            if (numberOfDimensions != vector.Length)
            {
                using (vector)
                    VectorHelpers.ThrowDifferentNumberOfDimensions(indexField, fieldName, vector, numberOfDimensions);
            }
        }
    }

    private static class VectorHelpers
    {
        public enum MethodVectorValue
        {
            ForDocument,
            ForRaw,
            EmbeddingText
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool TryRetrieveEmbeddingsGenerationTaskIdentifier(QueryBuilderParameters builderParameters, in string fieldName, out string embeddingsGenerationTaskIdentifier)
        {
            var existsInPersistence =
                builderParameters.Index.IndexFieldsPersistence.TryReadEmbeddingsGenerationTaskIdentifier(fieldName, out embeddingsGenerationTaskIdentifier);

            if (builderParameters.Metadata.IsDynamic == false)
                return existsInPersistence;

            if (((builderParameters.FieldsToFetch != null && builderParameters.FieldsToFetch.IndexFields.TryGetValue(fieldName, out var indexField)) || (builderParameters.Index.Definition.IndexFields.TryGetValue(fieldName, out indexField))) && indexField.Vector is AutoVectorOptions avo)
            {
                embeddingsGenerationTaskIdentifier = avo.EmbeddingsGenerationTaskIdentifier;
                return string.IsNullOrEmpty(avo.EmbeddingsGenerationTaskIdentifier) == false;
            }

            embeddingsGenerationTaskIdentifier = null;
            return false;
        }

        internal static (VectorValue? SingleVector, VectorValue[] MultiVector) GetVectorValueForTextualInput(QueryBuilderParameters parameters, VectorOptions vectorOptions, ValueTokenType valueType, object value)
        {
            if (valueType is ValueTokenType.String)
                return (GenerateEmbeddings.FromText(parameters.Allocator, vectorOptions, value.ToString()), null);

            if (valueType is not ValueTokenType.Parameter)
                PortableExceptions.Throw<InvalidDataException>($"Cannot use vector.search() on a text field with a non-string value. Got {valueType}");

            if (value is BlittableJsonReaderArray valueAsList)
            {
                var embeddings = new VectorValue[valueAsList.Length];
                for (var i = 0; i < valueAsList.Length; ++i)
                    embeddings[i] = GenerateEmbeddings.FromText(parameters.Allocator, vectorOptions, valueAsList[i].ToString());

                return (null, embeddings);
            }

            PortableExceptions.Throw<InvalidDataException>($"Cannot use vector.search() on a text field with a non-string value(s). Got {valueType}");
            return (null, null);
        }

        internal static VectorValue GetVectorValueFromRavenVector(QueryBuilderParameters parameters, BlittableJsonReaderObject json, VectorOptions vectorOptions)
        {
            var vectorObjectFound = json.TryGetMember(Sparrow.Global.Constants.Naming.VectorPropertyName, out var vectorObject);
            PortableExceptions.ThrowIfNot<InvalidDataException>(vectorObjectFound, "Cannot find vector property in the object.");

            var vectorReader = (BlittableJsonReaderVector)vectorObject;
            return QueryBuilderHelper.GetVectorValueFromBlittableJsonVectorReader(parameters.Allocator, vectorOptions, vectorReader);
        }

        internal static VectorValue GetVectorValueFromNumericalBlittableArray(QueryBuilderParameters parameters, BlittableJsonReaderArray array, VectorOptions vectorOptions)
        {
            var bytesUsed = array.Length * (vectorOptions.SourceEmbeddingType is VectorEmbeddingType.Single ? sizeof(float) : 1);
            var memScope = parameters.Allocator.Allocate(bytesUsed, out Memory<byte> mem);
            ref var floatRef = ref MemoryMarshal.GetReference(MemoryMarshal.Cast<byte, float>(mem.Span));
            ref var sbyteRef = ref MemoryMarshal.GetReference(MemoryMarshal.Cast<byte, sbyte>(mem.Span));
            ref var byteRef = ref MemoryMarshal.GetReference(mem.Span);

            for (int i = 0; i < array.Length; ++i)
            {
                switch (vectorOptions.SourceEmbeddingType)
                {
                    case VectorEmbeddingType.Single:
                        Unsafe.Add(ref floatRef, i) = array.GetByIndex<float>(i);
                        break;
                    case VectorEmbeddingType.Int8:
                        Unsafe.Add(ref sbyteRef, i) = array.GetByIndex<sbyte>(i);
                        break;
                    default:
                        Unsafe.AddByteOffset(ref byteRef, i) = array.GetByIndex<byte>(i);
                        break;
                }
            }

            return GenerateEmbeddings.FromArray(parameters.Allocator, memScope, mem, vectorOptions, bytesUsed);
        }

        internal static VectorOptions GetExplicitVectorOptions(QueryBuilderParameters builderParameters, in string fieldName, out IndexField indexField)
        {
            if ((builderParameters.FieldsToFetch != null && builderParameters.FieldsToFetch.IndexFields.TryGetValue(fieldName, out indexField)) == false
                && (builderParameters.Index.Definition.IndexFields.TryGetValue(fieldName, out indexField)) == false)
                PortableExceptions.Throw<InvalidDataException>($"Cannot find `{fieldName}` field in the index.");

            return indexField.Vector;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static VectorOptions GetOptions(QueryBuilderParameters builderParameters, in string fieldName, out IndexField indexField)
        {
            if ((builderParameters.FieldsToFetch != null && builderParameters.FieldsToFetch.IndexFields.TryGetValue(fieldName, out indexField)) == false
                && (builderParameters.Index.Definition.IndexFields.TryGetValue(fieldName, out indexField)) == false)
                PortableExceptions.Throw<InvalidDataException>($"Cannot find `{fieldName}` field in the index.");

            // VectorOptions can be null when a user does not specify the configuration.
            // In such cases, we will choose the input depending on the value type (similar to how we handle it during indexing).
            if (indexField.Vector != null)
                return indexField.Vector;

            builderParameters.Index.IndexFieldsPersistence.TryReadVectorSourceEmbeddingType(fieldName, out var vectorSourceEmbeddingType);

            var defaultVectorOptions = vectorSourceEmbeddingType switch
            {
                VectorEmbeddingType.Single => VectorOptions.Default,
                VectorEmbeddingType.Text => VectorOptions.DefaultText,
                _ => throw new InvalidDataException(
                    $"Unknown vector source embedding type: {vectorSourceEmbeddingType}. Implicit configuration support only single and text vector source embedding types.")
            };

            indexField.Vector = defaultVectorOptions;

            return defaultVectorOptions;
        }

        internal static void ThrowDifferentNumberOfDimensions(in IndexField indexField, in string fieldName, in VectorValue transformedEmbedding,
            in int numberOfDimensions)
        {
            var (storedDimensions, inputDimensions) = indexField.Vector.DestinationEmbeddingType switch
            {
                VectorEmbeddingType.Single => (numberOfDimensions / sizeof(float), transformedEmbedding.Length / sizeof(float)),
                VectorEmbeddingType.Int8 => (numberOfDimensions - sizeof(float), transformedEmbedding.Length - sizeof(float)),
                VectorEmbeddingType.Binary => (numberOfDimensions, transformedEmbedding.Length),
                _ => throw new InvalidDataException($"Unexpected embedding type - {numberOfDimensions}.")
            };

            PortableExceptions.Throw<InvalidDataException>(
                $"Vector field `{fieldName}` has {storedDimensions} dimensions, but the vector passed to vector.search() has {inputDimensions} dimensions.");
        }

        internal static (VectorValue? SingleVector, VectorValue[] MultiVector) GetEmbeddingsForQueryParameter(QueryBuilderParameters builderParameters, ValueTokenType valueType,
            object value,
            string embeddingsGenerationTaskIdentifier, VectorOptions vectorOptions, string fieldName)
        {
            var database = builderParameters.Index.DocumentDatabase;

            var embeddingsTaskId = new EmbeddingsGenerationTaskIdentifier(embeddingsGenerationTaskIdentifier);

            var embeddingsGenerator = database.EmbeddingsGeneratorQueries;

            var sourceEmbeddingType = embeddingsGenerator.GetQuantizationOf(embeddingsTaskId);

            // Quantized dynamic field indicates that the task generated embeddings with different quantization than requested in the index
            // In this case we want to use quantization defined in dynamic field (which was set in CurrentIndexingScope.GetLoadVectorField)
            VectorEmbeddingType destinationEmbeddingType;
            if (builderParameters.Metadata.IsDynamic)
            {
                destinationEmbeddingType = sourceEmbeddingType is not VectorEmbeddingType.Single ? 
                    sourceEmbeddingType : 
                    vectorOptions!.DestinationEmbeddingType;
            }
            else
            {
                destinationEmbeddingType = vectorOptions?.DestinationEmbeddingType ?? sourceEmbeddingType;
            }

            ReadOnlyMemory<ReadOnlyMemory<byte>> embeddingValues;

            switch (valueType)
            {
                case ValueTokenType.String:
                    embeddingValues = embeddingsGenerator
                        .GetEmbeddingsForQuery(builderParameters.DocumentsContext, embeddingsTaskId, value.ToString());
                    break;
                case ValueTokenType.Parameter:
                {
                    if (value is not BlittableJsonReaderArray bjra)
                        throw new InvalidQueryException($"Expected array as parameter of vector.search({fieldName}) method, got '{value.GetType().FullName}' type instead.");

                    var values = new string[bjra.Length];

                    for (var i = 0; i < values.Length; i++)
                        values[i] = bjra[i].ToString();

                    embeddingValues = embeddingsGenerator
                        .GetEmbeddingsForQuery(builderParameters.DocumentsContext, embeddingsTaskId, values);
                    break;
                }
                default:
                    throw new NotSupportedException($"Unexpected value type provided as parameter to vector.search({fieldName}) method. Got '{value.GetType().FullName}' type.");
            }

            var queryingVectorOption = new VectorOptions
            {
                SourceEmbeddingType = sourceEmbeddingType,
                DestinationEmbeddingType = destinationEmbeddingType
            };

            if (embeddingValues.Length == 1)
            {
                var embeddingValue = embeddingValues.Span[0];

                return (GenerateEmbeddings.FromArray(builderParameters.Allocator, embeddingValue.Span, queryingVectorOption), null);
            }
            else
            {
                var vectorValues = new VectorValue[embeddingValues.Length];

                for (int i = 0; i < embeddingValues.Length; i++)
                {
                    var embeddingValue = embeddingValues.Span[i];

                    vectorValues[i] = GenerateEmbeddings.FromArray(builderParameters.Allocator, embeddingValue.Span, queryingVectorOption);
                }

                return (null, vectorValues);
            }
        }
    }

    // ── Compound field merging (WHERE only, no ORDER BY) ──────────────────

    // ── Compound field exact match (no ORDER BY) ─────────────────────────

    public static bool TryCreateCompoundExactMatch(
        QueryExecution plan, PlanParameters planParams, QueryBuilderParameters builderParams,
        out IQueryMatch compoundMatch, out string rejectReason)
    {
        rejectReason = null;
        bool result = TryCreateCompoundExactMatch(plan, planParams, builderParams, out compoundMatch);
        if (!result)
        {
            if (plan.Clauses == null || plan.Clauses.Count < 2)
                rejectReason = $"fewer than 2 clauses ({plan.Clauses?.Count ?? 0})";
            else if (plan.AllNegated)
                rejectReason = "all clauses are negated";
            else if (planParams.Index == null)
                rejectReason = "no index available";
            else if (plan.CompoundExactClauseA < 0 || plan.CompoundExactClauseB < 0)
                rejectReason = "no compound-exact clause pair identified at template time";
            else
                rejectReason = "composite key encoding failed or exceeded max term length";
        }
        return result;
    }

    /// <summary>Check if two Equals clauses on (field1, field2) match a compound field.
    /// If so, build a single TermQuery on the compound tree with the composite key.
    /// One tree lookup instead of two posting list intersections.</summary>
    public static bool TryCreateCompoundExactMatch(
        QueryExecution plan, PlanParameters planParams, QueryBuilderParameters builderParams,
        out IQueryMatch compoundMatch)
    {
        compoundMatch = null;
        // Discovery: structural rejection. All structural facts (CompoundExactClauseA/B,
        // CompoundExactAFirst) were pre-classified at template time; this method only
        // confirms the runtime state is compatible.
        if (plan.Clauses == null || plan.Clauses.Count < 2 || plan.AllNegated)
            return false;
        if (planParams.Index == null)
            return false;

        int idxA = plan.CompoundExactClauseA;
        int idxB = plan.CompoundExactClauseB;
        if (idxA < 0 || idxB < 0 || idxA >= plan.Clauses.Count || idxB >= plan.Clauses.Count)
            return false;

        var eA = plan.Executions[idxA];
        var eB = plan.Executions[idxB];
        if (eA.BoostFactor > 0 || eA.PackedParamValue.IsNone)
            return false;
        if (eB.BoostFactor > 0 || eB.PackedParamValue.IsNone)
            return false;

        compoundMatch = ConstructCompoundExact(plan, planParams);
        return compoundMatch != null;
    }

    /// <summary>Phase 5 bake: construction-only path for the CompoundExact hint.
    /// Assumes structural discovery has already validated this optimization applies
    /// (called either right after <see cref="TryCreateCompoundExactMatch"/>'s checks pass
    /// on compile-miss, or directly on cache-hit when <c>plan.Hint == InstantiateHint.CompoundExact</c>).
    /// Returns null when a per-execution byte-length check fails — the caller must fall
    /// back to the next optimization (or bitmap). No cost gates here — those are encoded
    /// in the plan-cache key (cardinality cliff bit 31 of Ordering).</summary>
    private static IQueryMatch ConstructCompoundExact(QueryExecution plan, PlanParameters planParams)
    {
        var clauses = plan.Clauses;
        var execs = plan.Executions;
        var indexSearcher = planParams.IndexSearcher;
        int idxA = plan.CompoundExactClauseA;
        int idxB = plan.CompoundExactClauseB;
        var eA = execs[idxA];
        var eB = execs[idxB];

        string firstField, secondField;
        ClauseExecution firstExec, secondExec;
        if (plan.CompoundExactAFirst)
        {
            firstField = clauses[idxA].ResolvedFieldName ?? clauses[idxA].FieldName;
            secondField = clauses[idxB].ResolvedFieldName ?? clauses[idxB].FieldName;
            firstExec = eA; secondExec = eB;
        }
        else
        {
            firstField = clauses[idxB].ResolvedFieldName ?? clauses[idxB].FieldName;
            secondField = clauses[idxA].ResolvedFieldName ?? clauses[idxA].FieldName;
            firstExec = eB; secondExec = eA;
        }

        byte[] field1Bytes = BuildCompoundFieldBytes(firstField, firstExec, indexSearcher, plan);
        if (field1Bytes == null || field1Bytes.Length > byte.MaxValue) return null;

        byte[] field2Bytes = BuildCompoundFieldBytes(secondField, secondExec, indexSearcher, plan);
        if (field2Bytes == null) return null;

        int totalLen = field1Bytes.Length + field2Bytes.Length + 1;
        if (totalLen > Constants.Terms.MaxLength) return null;

        var compositeKey = new byte[totalLen];
        field1Bytes.CopyTo(compositeKey, 0);
        field2Bytes.CopyTo(compositeKey.AsSpan(field1Bytes.Length));
        compositeKey[^1] = (byte)field1Bytes.Length;

        var compoundFieldName = $"compound({firstField},{secondField})";
        var compoundFieldMeta = indexSearcher.FieldMetadataBuilder(compoundFieldName, hasBoost: false);
        Voron.Slice.From(planParams.Allocator, compositeKey, out var keySlice);

        return indexSearcher.TermQuery(compoundFieldMeta, keySlice);
    }

    private static byte[] BuildCompoundFieldBytes(string fieldName, ClauseExecution exec,
        IndexSearcher indexSearcher, QueryExecution plan)
    {
        var p = exec.PackedParamValue;
        if (p.ValueType == PackedParam.TypeString)
        {
            var meta = indexSearcher.FieldMetadataBuilder(fieldName, hasBoost: false);
            var analyzed = indexSearcher.EncodeAndApplyAnalyzer(meta, plan.StringValues[p.Param1]);
            if (analyzed.Size > byte.MaxValue) return null;
            var bytes = new byte[analyzed.Size];
            analyzed.CopyTo(bytes);
            return bytes;
        }
        if (p.ValueType == PackedParam.TypeLong)
        {
            var bytes = new byte[sizeof(long)];
            System.Buffers.Binary.BinaryPrimitives.WriteInt64BigEndian(
                bytes, Sparrow.Binary.Bits.SwapBytes(plan.LongValues[p.Param1]));
            return bytes;
        }
        if (p.ValueType == PackedParam.TypeDouble)
        {
            var bytes = new byte[sizeof(long)];
            long sortable = Sparrow.Binary.Bits.DoubleToSortableLong(plan.DoubleValues[p.Param1]);
            System.Buffers.Binary.BinaryPrimitives.WriteInt64BigEndian(
                bytes, Sparrow.Binary.Bits.SwapBytes(sortable));
            return bytes;
        }
        return null;
    }

    // ── Compound field optimization (with ORDER BY) ─────────────────────

    /// <summary>Check if WHERE + ORDER BY can be served by a compound tree scan.
    /// Condition: an Equals clause on field1, ORDER BY on field2 (or field1, or both),
    public static bool TryCreateCompoundFieldMatch(
        QueryExecution plan, OrderMetadata[] orderByFields,
        PlanParameters planParams, QueryBuilderParameters builderParams,
        CompiledPlan compiledPlan, out IQueryMatch compoundMatch, out string rejectReason)
    {
        rejectReason = null;
        bool result = TryCreateCompoundFieldMatch(plan, orderByFields, planParams, builderParams, compiledPlan, out compoundMatch);
        if (!result)
        {
            if (plan.CompoundFieldDrivingClause < 0 || plan.CompoundFieldSortName == null)
                rejectReason = "no compound-field candidate identified at template time";
            else if (plan.AllNegated)
                rejectReason = "all clauses are negated";
            else
                rejectReason = "cost check failed (bitmap is cheaper), non-scannable residual, or prefix too long";
        }
        return result;
    }

    /// compound(field1, field2) exists in the index, and any residual clauses are
    /// entry-scan eligible.
    /// Returns a DirectScanMatch wrapping a compound tree StartsWith with optional
    /// residual predicate checking.</summary>
    public static bool TryCreateCompoundFieldMatch(
        QueryExecution plan, OrderMetadata[] orderByFields,
        PlanParameters planParams, QueryBuilderParameters builderParams,
        CompiledPlan compiledPlan, out IQueryMatch compoundMatch)
    {
        compoundMatch = null;

        // Discovery: structural rejection + cost gate. Structural facts
        // (CompoundFieldDrivingClause, CompoundFieldSortName) were pre-classified at
        // template time. This method runs cost estimation and residual-scannability
        // checks that are deterministic for the cache key (cardinality cliff bit 31
        // in Ordering segregates cliff buckets, so cost outcome is stable per key).
        int drivingClauseIdx = plan.CompoundFieldDrivingClause;
        string sortFieldName = plan.CompoundFieldSortName;
        if (drivingClauseIdx < 0 || sortFieldName == null)
            return false;
        if (plan.Clauses == null || drivingClauseIdx >= plan.Clauses.Count || plan.AllNegated)
            return false;

        var clauses = plan.Clauses;
        var execs = plan.Executions;
        var indexSearcher = planParams.IndexSearcher;

        var drivingExec = execs[drivingClauseIdx];
        if (drivingExec.PackedParamValue.IsNone)
            return false;

        // Find optional field2 range narrowing clause (structural — same for all
        // executions of this template).
        int field2RangeIdx = FindCompoundFieldField2Range(clauses, drivingClauseIdx, sortFieldName);

        // Residual scannability + cost check
        int longIdx = 0, doubleIdx = 0, sliceIdx = 0;
        long bitmapCost = 0;
        int residualCount = 0;
        for (int i = 0; i < clauses.Count; i++)
        {
            bitmapCost += execs[i].Cardinality > 0 ? execs[i].Cardinality : indexSearcher.NumberOfEntries;
            if (i == drivingClauseIdx || i == field2RangeIdx)
                continue;
            if (clauses[i].HasBoost || (execs[i] is { BoostFactor: > 0 }))
                return false;
            var pred = BuildScanPredicateInfo(clauses[i], execs[i], ref longIdx, ref doubleIdx, ref sliceIdx);
            if (pred == null)
                return false;
            residualCount++;
        }

        long drivingCardinality = drivingExec.Cardinality > 0 ? drivingExec.Cardinality : indexSearcher.NumberOfEntries;
        long entriesToScan = drivingCardinality;
        if (residualCount > 0)
        {
            long minResidualCardinality = long.MaxValue;
            for (int i = 0; i < clauses.Count; i++)
            {
                if (i == drivingClauseIdx) continue;
                long card = execs[i].Cardinality > 0 ? execs[i].Cardinality : indexSearcher.NumberOfEntries;
                if (card < minResidualCardinality)
                    minResidualCardinality = card;
            }
            if (minResidualCardinality > 0 && minResidualCardinality < indexSearcher.NumberOfEntries)
            {
                double passRate = (double)minResidualCardinality / indexSearcher.NumberOfEntries;
                if (passRate > 0)
                    entriesToScan = (long)(drivingCardinality / passRate);
            }
        }

        long directCost = entriesToScan > long.MaxValue / QueryPrimitives.EntryScanCostMultiplier ? long.MaxValue : entriesToScan * QueryPrimitives.EntryScanCostMultiplier;
        if (directCost >= bitmapCost || entriesToScan > QueryPrimitives.EntryScanCountThreshold)
            return false;

        compoundMatch = ConstructCompoundField(plan, orderByFields, planParams, builderParams, compiledPlan,
            field2RangeIdx, entriesToScan, bitmapCost);
        return compoundMatch != null;
    }

    /// <summary>Locate an optional GT/GTE/LT/LTE/Between clause on the sort field
    /// that can narrow the compound prefix scan. Structural — same for all executions
    /// of a given template, but cheap enough to recompute on each Construct call
    /// rather than threading another field through QueryExecution.</summary>
    private static int FindCompoundFieldField2Range(List<ClauseInfo> clauses, int drivingClauseIdx, string sortFieldName)
    {
        for (int i = 0; i < clauses.Count; i++)
        {
            if (i == drivingClauseIdx) continue;
            if (clauses[i].FieldName != sortFieldName) continue;
            if (clauses[i].ClauseType is ClauseType.GreaterThan or ClauseType.GreaterThanOrEqual
                or ClauseType.LessThan or ClauseType.LessThanOrEqual or ClauseType.Between)
                return i;
        }
        return -1;
    }

    /// <summary>Phase 5 bake: construction-only path for the CompoundField hint.
    /// Caller has either run TryCreateCompoundFieldMatch's discovery (compile-miss)
    /// or read the cached InstantiateHint and is dispatching directly.
    /// Returns null on per-execution failure (e.g. analyzed prefix exceeds 255 bytes);
    /// caller falls back to the next optimization or bitmap.</summary>
    private static IQueryMatch ConstructCompoundField(
        QueryExecution plan, OrderMetadata[] orderByFields,
        PlanParameters planParams, QueryBuilderParameters builderParams, CompiledPlan compiledPlan,
        int field2RangeIdx, long entriesToScan, long bitmapCost)
    {
        var clauses = plan.Clauses;
        var execs = plan.Executions;
        var indexSearcher = planParams.IndexSearcher;
        var allocator = planParams.Allocator;
        int drivingClauseIdx = plan.CompoundFieldDrivingClause;
        string sortFieldName = plan.CompoundFieldSortName;

        var drivingClause = clauses[drivingClauseIdx];
        var drivingExec = execs[drivingClauseIdx];
        var packed = drivingExec.PackedParamValue;

        // Rebuild residual predicates (Construct rebuilds; the structural shape is
        // identical to what discovery just walked, so List growth is bounded).
        var residualPreds = new List<ScanPredicateInfo>();
        int rLongIdx = 0, rDoubleIdx = 0, rSliceIdx = 0;
        for (int i = 0; i < clauses.Count; i++)
        {
            if (i == drivingClauseIdx || i == field2RangeIdx)
                continue;
            var pred = BuildScanPredicateInfo(clauses[i], execs[i], ref rLongIdx, ref rDoubleIdx, ref rSliceIdx);
            if (pred == null)
                return null;
            residualPreds.Add(pred.Value);
        }

        string field1Name = drivingClause.FieldName;
        var compoundFieldName = $"compound({field1Name},{sortFieldName})";
        var compoundFieldMeta = indexSearcher.FieldMetadataBuilder(compoundFieldName, hasBoost: false);

        // Build the prefix bytes for field1's value.
        // String: analyzed via field1's analyzer. Numeric: Bits.SwapBytes big-endian encoding.
        Voron.Slice analyzedPrefix;
        string field1ValueStr;
        switch (packed.ValueType)
        {
            case PackedParam.TypeString:
            {
                field1ValueStr = plan.StringValues[packed.Param1];
                var field1Meta = builderParams != null
                    ? QueryBuilderHelper.GetFieldMetadata(in builderParams, field1Name, hasBoost: false)
                    : indexSearcher.FieldMetadataBuilder(field1Name, hasBoost: false);
                analyzedPrefix = indexSearcher.EncodeAndApplyAnalyzer(field1Meta, field1ValueStr);
                break;
            }
            case PackedParam.TypeLong:
            {
                long longVal = plan.LongValues[packed.Param1];
                field1ValueStr = longVal.ToString();
                var bytes = new byte[sizeof(long)];
                System.Buffers.Binary.BinaryPrimitives.WriteInt64BigEndian(bytes, Sparrow.Binary.Bits.SwapBytes(longVal));
                Voron.Slice.From(allocator, bytes, out analyzedPrefix);
                break;
            }
            case PackedParam.TypeDouble:
            {
                double dblVal = plan.DoubleValues[packed.Param1];
                field1ValueStr = dblVal.ToString(CultureInfo.InvariantCulture);
                long sortable = Sparrow.Binary.Bits.DoubleToSortableLong(dblVal);
                var bytes = new byte[sizeof(long)];
                System.Buffers.Binary.BinaryPrimitives.WriteInt64BigEndian(bytes, Sparrow.Binary.Bits.SwapBytes(sortable));
                Voron.Slice.From(allocator, bytes, out analyzedPrefix);
                break;
            }
            default:
                return null;
        }

        // Compound key trailing byte stores field1 length as a single byte.
        // If the analyzed prefix exceeds 255 bytes, the compound key format can't represent it.
        // Fall back to the bitmap pipeline which queries individual fields normally.
        if (analyzedPrefix.Size > byte.MaxValue)
            return null;

        IQueryMatch drivingMatch = null;
        if (field2RangeIdx >= 0)
        {
            // Compound range: build composite low/high keys incorporating the field2 bound
            var field2Exec = execs[field2RangeIdx];
            var field2Clause = clauses[field2RangeIdx];
            var field2Packed = field2Exec.PackedParamValue;

            if (field2Packed.IsNone == false)
            {
                // Encode field2 bound value into bytes (same encoding as indexing).
                // Long/Double: Bits.SwapBytes big-endian. String: analyze with field2's analyzer.
                byte[] field2Bytes = null;
                byte[] field2HighBytes = null;
                bool usePrefix = false;

                if (field2Packed.ValueType == PackedParam.TypeLong)
                {
                    field2Bytes = new byte[sizeof(long)];
                    System.Buffers.Binary.BinaryPrimitives.WriteInt64BigEndian(
                        field2Bytes, Sparrow.Binary.Bits.SwapBytes(plan.LongValues[field2Packed.Param1]));
                    if (field2Clause.ClauseType == ClauseType.Between)
                    {
                        field2HighBytes = new byte[sizeof(long)];
                        System.Buffers.Binary.BinaryPrimitives.WriteInt64BigEndian(
                            field2HighBytes, Sparrow.Binary.Bits.SwapBytes(plan.LongValues[field2Packed.Param2]));
                    }
                }
                else if (field2Packed.ValueType == PackedParam.TypeDouble)
                {
                    field2Bytes = new byte[sizeof(long)];
                    long sortable = Sparrow.Binary.Bits.DoubleToSortableLong(plan.DoubleValues[field2Packed.Param1]);
                    System.Buffers.Binary.BinaryPrimitives.WriteInt64BigEndian(
                        field2Bytes, Sparrow.Binary.Bits.SwapBytes(sortable));
                    if (field2Clause.ClauseType == ClauseType.Between)
                    {
                        field2HighBytes = new byte[sizeof(long)];
                        long highSortable = Sparrow.Binary.Bits.DoubleToSortableLong(plan.DoubleValues[field2Packed.Param2]);
                        System.Buffers.Binary.BinaryPrimitives.WriteInt64BigEndian(
                            field2HighBytes, Sparrow.Binary.Bits.SwapBytes(highSortable));
                    }
                }
                else if (field2Packed.ValueType == PackedParam.TypeString)
                {
                    // Analyze field2's value with the sort field's analyzer (same as indexing)
                    var field2Meta = builderParams != null
                        ? QueryBuilderHelper.GetFieldMetadata(in builderParams, sortFieldName, hasBoost: false)
                        : indexSearcher.FieldMetadataBuilder(sortFieldName, hasBoost: false);
                    var analyzed = indexSearcher.EncodeAndApplyAnalyzer(field2Meta, plan.StringValues[field2Packed.Param1]);
                    if (analyzed.Size > byte.MaxValue)
                        usePrefix = true;
                    else
                    {
                        field2Bytes = new byte[analyzed.Size];
                        analyzed.CopyTo(field2Bytes);
                        if (field2Clause.ClauseType == ClauseType.Between)
                        {
                            var analyzedHigh = indexSearcher.EncodeAndApplyAnalyzer(field2Meta, plan.StringValues[field2Packed.Param2]);
                            if (analyzedHigh.Size > byte.MaxValue)
                                usePrefix = true;
                            else
                            {
                                field2HighBytes = new byte[analyzedHigh.Size];
                                analyzedHigh.CopyTo(field2HighBytes);
                            }
                        }
                    }
                }
                else
                {
                    usePrefix = true;
                }

                if (usePrefix || field2Bytes == null)
                {
                    // Field2 value too long or unsupported type — fall back to prefix-only
                    drivingMatch = indexSearcher.StartWithQuery(compoundFieldMeta, analyzedPrefix,
                        isNegated: false, forward: orderByFields[0].Ascending,
                        validatePostfixLen: true);
                }
                else
                {

                    // Build low and high composite keys
                    int prefixLen = analyzedPrefix.Size;
                    int field2Len = field2Bytes.Length;
                    int keyLen = prefixLen + field2Len + 1; // +1 for field1 length byte
                    int highField2Len = field2HighBytes?.Length ?? field2Len;
                    int highKeyLen = prefixLen + highField2Len + 1;

                    // Check total key length against max
                    if (keyLen > Constants.Terms.MaxLength || highKeyLen > Constants.Terms.MaxLength)
                    {
                        drivingMatch = indexSearcher.StartWithQuery(compoundFieldMeta, analyzedPrefix,
                            isNegated: false, forward: orderByFields[0].Ascending, validatePostfixLen: true);
                        goto DrivingMatchReady;
                    }

                    byte[] lowKeyBytes = new byte[keyLen];
                    byte[] highKeyBytes = new byte[highKeyLen];

                    analyzedPrefix.CopyTo(lowKeyBytes);
                    analyzedPrefix.CopyTo(highKeyBytes);

                    // Low key: either the field2 bound or min value (0x00s)
                    // High key: either the field2 bound or max value (0xFFs)
                    bool isGt = field2Clause.ClauseType is ClauseType.GreaterThan or ClauseType.GreaterThanOrEqual;
                    if (isGt || field2Clause.ClauseType == ClauseType.Between)
                    {
                        field2Bytes.CopyTo(lowKeyBytes.AsSpan(prefixLen));
                    }
                    // else: low = field1 prefix + 0x00s (already zeroed)

                    if (field2Clause.ClauseType is ClauseType.LessThan or ClauseType.LessThanOrEqual || field2Clause.ClauseType == ClauseType.Between)
                    {
                        var highBytes = field2HighBytes ?? field2Bytes;
                        highBytes.CopyTo(highKeyBytes.AsSpan(prefixLen));
                    }
                    else
                    {
                        // GT/GTE: high = field1 prefix + 0xFF...FF
                        highKeyBytes.AsSpan(prefixLen, highField2Len).Fill(0xFF);
                    }

                    // Trailing field1 length byte
                    lowKeyBytes[^1] = (byte)prefixLen;
                    highKeyBytes[^1] = (byte)prefixLen;

                    Voron.Slice.From(allocator, lowKeyBytes, out var lowSlice);
                    Voron.Slice.From(allocator, highKeyBytes, out var highSlice);

                    drivingMatch = indexSearcher.RangeBuilder<global::Corax.Querying.Matches.Meta.Range.Inclusive, global::Corax.Querying.Matches.Meta.Range.Inclusive>(
                        compoundFieldMeta, lowSlice, highSlice,
                        forward: orderByFields[0].Ascending, CancellationToken.None);
                }
            }
        }
        else
        {
            // Pure prefix scan (no field2 constraint)
            drivingMatch = indexSearcher.StartWithQuery(compoundFieldMeta, analyzedPrefix,
                isNegated: false, forward: orderByFields[0].Ascending,
                validatePostfixLen: true);
        }
        DrivingMatchReady:

        // Extract scan parameters for residual predicates
        ScanPredicateInfo[] residualArray = residualPreds.Count > 0 ? residualPreds.ToArray() : null;
        long[] longParams = null;
        double[] doubleParams = null;
        Voron.Slice[] sliceParams = null;
        long[] fieldRootPages = null;

        if (residualArray != null)
        {
            var longs = new List<long>();
            var doubles = new List<double>();
            var slices = new List<Voron.Slice>();
            var roots = new List<long>();

            int residualIdx = 0;
            for (int i = 0; i < clauses.Count; i++)
            {
                if (i == drivingClauseIdx || i == field2RangeIdx) continue;
                var matchingExec = execs[i];

                // Add field root page
                roots.Add(indexSearcher.FieldCache.GetLookupRootPage(clauses[i].FieldName));

                var predPacked = matchingExec.PackedParamValue;
                if (predPacked.IsNone) { residualIdx++; continue; }

                int idx1 = predPacked.Param1;
                int idx2 = predPacked.Param2;
                bool hasBetween = idx2 != PackedParam.NoParamValue;

                switch (residualArray[residualIdx].ValueType)
                {
                    case ScanValueType.Long:
                        longs.Add(plan.LongValues[idx1]);
                        if (hasBetween) longs.Add(plan.LongValues[idx2]);
                        break;
                    case ScanValueType.Double:
                        doubles.Add(plan.DoubleValues[idx1]);
                        if (hasBetween) doubles.Add(plan.DoubleValues[idx2]);
                        break;
                    case ScanValueType.Slice:
            case ScanValueType.SliceLong:
                    {
                        Voron.Slice.From(allocator, plan.StringValues[idx1], out var s1);
                        slices.Add(s1);
                        if (hasBetween)
                        {
                            Voron.Slice.From(allocator, plan.StringValues[idx2], out var s2);
                            slices.Add(s2);
                        }
                        break;
                    }
                }
                residualIdx++;
            }

            longParams = longs.Count > 0 ? longs.ToArray() : null;
            doubleParams = doubles.Count > 0 ? doubles.ToArray() : null;
            sliceParams = slices.Count > 0 ? slices.ToArray() : null;
            fieldRootPages = roots.Count > 0 ? roots.ToArray() : null;
        }

        var directScan = BuildDirectScan(
            indexSearcher, drivingMatch, longParams, doubleParams, sliceParams, fieldRootPages,
            compiledPlan.CompiledEntryPredicate, residualArray);
        directScan.DrivingTreeName = compoundFieldName;
        directScan.DrivingClause = $"{field1Name} = '{field1ValueStr}'";
        directScan.SeekBound = $"'{field1ValueStr}' (prefix, validatePostfixLen)";
        directScan.Direction = orderByFields[0].Ascending ? "Forward" : "Backward";
        directScan.ResidualDescription = residualArray != null
            ? string.Join(", ", residualPreds.ConvertAll(p => $"{p.FieldName} {p.CompareOp}"))
            : null;
        directScan.Reason = $"entries_to_scan({entriesToScan}) × {QueryPrimitives.EntryScanCostMultiplier} < bitmap_cost({bitmapCost})";

        return directScan;
    }

    // ── Simple field direct scan ──────────────────────────────────────────

    /// <summary>Check if a range clause on the ORDER BY field can be served by a direct
    public static bool TryCreateSimpleFieldDirectScan(
        QueryExecution plan, OrderMetadata[] orderByFields,
        PlanParameters planParams, QueryBuilderParameters builderParams,
        CompiledPlan compiledPlan, out IQueryMatch directMatch, out string rejectReason)
    {
        rejectReason = null;
        bool result = TryCreateSimpleFieldDirectScan(plan, orderByFields, planParams, builderParams, compiledPlan, out directMatch);
        if (!result)
        {
            if (orderByFields == null || orderByFields.Length == 0)
                rejectReason = "no ORDER BY fields";
            else if (orderByFields.Length > 2)
                rejectReason = $"ORDER BY has {orderByFields.Length} fields (max 2 for direct scan)";
            else if (orderByFields.Length == 2 && orderByFields[1].FieldType is not (MatchCompareFieldType.Integer or MatchCompareFieldType.Floating))
                rejectReason = $"tie-break field type {orderByFields[1].FieldType} is not numeric";
            else if (plan.Clauses is { Count: > 0 } && plan.SortDrivingClauseIndex < 0)
                rejectReason = $"no range/equals clause on sort field '{orderByFields[0].Field.FieldName}'";
            else
                rejectReason = "cost check failed (bitmap is cheaper), non-scannable residual, or cardinality too high for tie-break";
        }
        return result;
    }

    /// tree scan instead of the bitmap pipeline. The range query already walks the tree
    /// in sort order, so no SortingMatch wrapper is needed.</summary>
    public static bool TryCreateSimpleFieldDirectScan(
        QueryExecution plan, OrderMetadata[] orderByFields,
        PlanParameters planParams, QueryBuilderParameters builderParams,
        CompiledPlan compiledPlan, out IQueryMatch directMatch)
    {
        directMatch = null;

        // Discovery: ORDER BY shape, sort-driving clause selection, residual
        // scannability + cost check. Per Phase 5, all per-execution rebuilds of
        // the live match are routed through ConstructDirectScan; this method
        // only validates the runtime state is compatible and then delegates.
        if (orderByFields == null || orderByFields.Length == 0)
            return false;

        if (orderByFields.Length > 2)
            return false;

        bool hasTieBreak = orderByFields.Length == 2;
        if (hasTieBreak)
        {
            var tieBreakType = orderByFields[1].FieldType;
            if (tieBreakType is not (MatchCompareFieldType.Integer or MatchCompareFieldType.Floating or MatchCompareFieldType.Sequence))
                return false;
        }

        var indexSearcher = planParams.IndexSearcher;
        string sortFieldName = orderByFields[0].Field.FieldName.ToString();
        bool forward = orderByFields[0].Ascending;
        var sortFieldType = orderByFields[0].FieldType;

        var clauses = plan.Clauses;
        var execs = plan.Executions;
        bool isFullScan = clauses == null || clauses.Count == 0;

        if (isFullScan && plan.AllNegated)
            return false;

        // ── Discovery: drivingIdx + cost gate ──
        int drivingIdx = -1;
        long entriesToScan = 0, bitmapCost = 0;
        if (isFullScan == false)
        {
            // SortDrivingClauseIndex pre-identified at template time and remapped to
            // post-sort index during Build — skip the per-execution clause scan.
            drivingIdx = plan.SortDrivingClauseIndex;
            if (drivingIdx == -1)
            {
                // Fallback: template didn't identify a candidate (e.g. WHEN eliminated the
                // clause, or sort field didn't match any template clause).
                for (int i = 0; i < clauses.Count; i++)
                {
                    if (clauses[i].FieldName != sortFieldName)
                        continue;
                    if (clauses[i].ClauseType is not (ClauseType.GreaterThan or ClauseType.GreaterThanOrEqual
                        or ClauseType.LessThan or ClauseType.LessThanOrEqual or ClauseType.Between
                        or ClauseType.Equals))
                        continue;
                    if (clauses[i].IsNegated)
                        continue;
                    if (clauses[i].HasBoost || (execs[i] is { BoostFactor: > 0 }))
                        continue;
                    drivingIdx = i;
                    break;
                }
            }
            else if (execs[drivingIdx] is { BoostFactor: > 0 })
            {
                drivingIdx = -1;
            }

            if (drivingIdx == -1)
                return false;

            if (execs[drivingIdx].PackedParamValue.IsNone)
                return false;

            // Residual scannability + bitmap cost summation in one pass.
            int rlongIdx = 0, rdoubleIdx = 0, rsliceIdx = 0;
            int residualCount = 0;
            for (int i = 0; i < clauses.Count; i++)
            {
                bitmapCost += execs[i].Cardinality > 0 ? execs[i].Cardinality : indexSearcher.NumberOfEntries;
                if (i == drivingIdx) continue;
                if (clauses[i].HasBoost || (execs[i] is { BoostFactor: > 0 }))
                    return false;
                var pred = BuildScanPredicateInfo(clauses[i], execs[i], ref rlongIdx, ref rdoubleIdx, ref rsliceIdx);
                if (pred == null)
                    return false;
                residualCount++;
            }

            long drivingCard = execs[drivingIdx].Cardinality > 0 ? execs[drivingIdx].Cardinality : indexSearcher.NumberOfEntries;
            entriesToScan = drivingCard;
            if (residualCount > 0)
            {
                long minResidual = long.MaxValue;
                for (int i = 0; i < clauses.Count; i++)
                {
                    if (i == drivingIdx) continue;
                    long c = execs[i].Cardinality > 0 ? execs[i].Cardinality : indexSearcher.NumberOfEntries;
                    if (c < minResidual) minResidual = c;
                }
                if (minResidual > 0 && minResidual < indexSearcher.NumberOfEntries)
                {
                    double passRate = (double)minResidual / indexSearcher.NumberOfEntries;
                    if (passRate > 0) entriesToScan = (long)(drivingCard / passRate);
                }
            }

            long directCost = entriesToScan > long.MaxValue / QueryPrimitives.EntryScanCostMultiplier ? long.MaxValue : entriesToScan * QueryPrimitives.EntryScanCostMultiplier;
            if (directCost >= bitmapCost || entriesToScan > QueryPrimitives.EntryScanCountThreshold)
                return false;
        }
        else
        {
            // Full-scan structural eligibility checks (would-cause-empty paths).
            if (orderByFields[0].MayHaveMissingEntries)
                return false;
            if (sortFieldType is not (MatchCompareFieldType.Sequence or MatchCompareFieldType.Integer or MatchCompareFieldType.Floating))
                return false;
        }

        directMatch = ConstructDirectScan(plan, orderByFields, planParams, builderParams, compiledPlan,
            drivingIdx, isFullScan, hasTieBreak, entriesToScan, bitmapCost);
        return directMatch != null;
    }

    /// <summary>Phase 5 bake: construction-only path for the DirectScan hint.
    /// Discovery (clause selection, cost gate, residual scannability) already passed
    /// in either TryCreateSimpleFieldDirectScan or by virtue of a cached
    /// <see cref="InstantiateHint.DirectScan"/>. Returns null when a per-execution
    /// runtime check fails (e.g. driving match resolution returns non-TermsProviderMatch
    /// or tie-break group cap exceeded by current parameter cardinality).</summary>
    private static IQueryMatch ConstructDirectScan(
        QueryExecution plan, OrderMetadata[] orderByFields,
        PlanParameters planParams, QueryBuilderParameters builderParams, CompiledPlan compiledPlan,
        int drivingIdx, bool isFullScan, bool hasTieBreak,
        long entriesToScan, long bitmapCost)
    {
        var indexSearcher = planParams.IndexSearcher;
        string sortFieldName = orderByFields[0].Field.FieldName.ToString();
        bool forward = orderByFields[0].Ascending;
        var sortFieldType = orderByFields[0].FieldType;
        var clauses = plan.Clauses;
        var execs = plan.Executions;

        ITermsProvider provider;
        Voron.Impl.LowLevelTransaction llt;
        long drivingCardinality;
        string drivingClauseDescription;

        if (isFullScan)
        {
            var fieldMeta = orderByFields[0].Field;
            IQueryMatch fullScanMatch;
            if (sortFieldType == MatchCompareFieldType.Integer)
                fullScanMatch = indexSearcher.BetweenQuery(fieldMeta, long.MinValue, long.MaxValue, forward: forward);
            else if (sortFieldType == MatchCompareFieldType.Floating)
                fullScanMatch = indexSearcher.BetweenQuery(fieldMeta, double.MinValue, double.MaxValue, forward: forward);
            else
                fullScanMatch = indexSearcher.ExistsQuery(fieldMeta, forward: forward);
            if (fullScanMatch is not TermsProviderMatch tpm)
                return null;
            provider = tpm.Provider;
            llt = tpm.Llt;
            drivingCardinality = 0;
            drivingClauseDescription = $"{sortFieldName} [all]";
        }
        else
        {
            var drivingClause = clauses[drivingIdx];
            var drivingExec = execs[drivingIdx];

            TermsProviderMatch tpm;
            if (drivingClause.ClauseType == ClauseType.Equals)
            {
                var eqMatch = ResolveEqualsClauseWithDirection(drivingClause, drivingExec, indexSearcher, plan, planParams, builderParams, forward);
                if (eqMatch is not TermsProviderMatch eq)
                    return null;
                tpm = eq;
            }
            else
            {
                var match = ResolveRangeClauseWithDirection(drivingClause, drivingExec, indexSearcher, plan, planParams, builderParams, forward);
                if (match is not TermsProviderMatch m)
                    return null;
                tpm = m;
            }
            provider = tpm.Provider;
            llt = tpm.Llt;
            drivingCardinality = drivingExec.Cardinality > 0 ? drivingExec.Cardinality : indexSearcher.NumberOfEntries;
            drivingClauseDescription = $"{drivingClause.FieldName} {drivingClause.ClauseType}";
        }

        // Rebuild residual predicates structurally (same shape as discovery's pre-check).
        int longIdx = 0, doubleIdx = 0, sliceIdx = 0;
        var residualPreds = new List<ScanPredicateInfo>();
        if (isFullScan == false)
        {
            for (int i = 0; i < clauses.Count; i++)
            {
                if (i == drivingIdx) continue;
                var pred = BuildScanPredicateInfo(clauses[i], execs[i], ref longIdx, ref doubleIdx, ref sliceIdx);
                if (pred == null)
                    return null;
                residualPreds.Add(pred.Value);
            }
        }

        // ── Create the driving match ──
        // BetweenQuery and StartWithQuery don't include nulls in their term output,
        // so SortedDrivingMatch must drain them itself (respecting nullFirst direction).
        bool nullIsSmallest = (orderByFields[0].NullsSortMode ?? builderParams.Index.Configuration.NullsSortMode) == NullsSortMode.NullsSmallest;
        bool nullFirst = forward ? nullIsSmallest : !nullIsSmallest;
        bool drainNulls = true;
        IQueryMatch drivingMatch;
        if (hasTieBreak)
        {
            // Per-term group cap: bail if any single primary term could exceed the group
            // buffer cap. Conservative gate: total driving cardinality must fit.
            if (drivingCardinality > SortedDrivingWithTieBreakMatch.MaxGroupSize)
                return null;
            // Secondary field uses its own NullsSortMode — distinct from the primary field's.
            bool secondaryNullIsSmallest = (orderByFields[1].NullsSortMode ?? builderParams.Index.Configuration.NullsSortMode) == NullsSortMode.NullsSmallest;
            drivingMatch = new SortedDrivingWithTieBreakMatch(
                provider, llt, planParams.Allocator, indexSearcher,
                orderByFields[0].Field, orderByFields[1].Field,
                orderByFields[1].FieldType, secondaryDescending: orderByFields[1].Ascending == false,
                nullFirst: nullFirst, nullIsSmallest: secondaryNullIsSmallest, drainNulls: drainNulls);
        }
        else
        {
            drivingMatch = new SortedDrivingMatch(provider, llt, planParams.Allocator,
                indexSearcher, orderByFields[0].Field, nullFirst, drainNulls);
        }

        // ── Residual scan parameters ──
        ScanPredicateInfo[] residualArray = residualPreds.Count > 0 ? residualPreds.ToArray() : null;
        long[] longParams = null;
        double[] doubleParams = null;
        Voron.Slice[] sliceParams = null;
        long[] fieldRootPages = null;

        if (residualArray != null && clauses != null)
        {
            var longs = new List<long>();
            var doubles = new List<double>();
            var slices = new List<Voron.Slice>();
            var roots = new List<long>();

            int residualIdx = 0;
            for (int i = 0; i < clauses.Count; i++)
            {
                if (i == drivingIdx) continue;
                roots.Add(indexSearcher.FieldCache.GetLookupRootPage(clauses[i].FieldName));
                var predPacked = execs[i].PackedParamValue;
                if (predPacked.IsNone) { residualIdx++; continue; }
                int idx1 = predPacked.Param1;
                int idx2 = predPacked.Param2;
                bool hasBetween = idx2 != PackedParam.NoParamValue;
                switch (residualArray[residualIdx].ValueType)
                {
                    case ScanValueType.Long:
                        longs.Add(plan.LongValues[idx1]);
                        if (hasBetween) longs.Add(plan.LongValues[idx2]);
                        break;
                    case ScanValueType.Double:
                        doubles.Add(plan.DoubleValues[idx1]);
                        if (hasBetween) doubles.Add(plan.DoubleValues[idx2]);
                        break;
                    case ScanValueType.Slice:
            case ScanValueType.SliceLong:
                        Voron.Slice.From(planParams.Allocator, plan.StringValues[idx1], out var s1);
                        slices.Add(s1);
                        if (hasBetween) { Voron.Slice.From(planParams.Allocator, plan.StringValues[idx2], out var s2); slices.Add(s2); }
                        break;
                }
                residualIdx++;
            }
            longParams = longs.Count > 0 ? longs.ToArray() : null;
            doubleParams = doubles.Count > 0 ? doubles.ToArray() : null;
            sliceParams = slices.Count > 0 ? slices.ToArray() : null;
            fieldRootPages = roots.Count > 0 ? roots.ToArray() : null;
        }

        var ds = BuildDirectScan(
            indexSearcher, drivingMatch, longParams, doubleParams, sliceParams, fieldRootPages,
            compiledPlan.CompiledEntryPredicate, residualArray);
        ds.DrivingTreeName = sortFieldName;
        ds.DrivingClause = drivingClauseDescription;
        ds.Direction = orderByFields[0].Ascending ? "Forward" : "Backward";
        ds.ResidualDescription = residualArray != null
            ? string.Join(", ", residualPreds.ConvertAll(p => $"{p.FieldName} {p.CompareOp}"))
            : null;
        ds.Reason = isFullScan
            ? "full index-only scan (no WHERE clause)"
            : $"entries_to_scan({entriesToScan}) × {QueryPrimitives.EntryScanCostMultiplier} < bitmap_cost({bitmapCost})";
        return ds;
    }

    // ── Sort seek hint ────────────────────────────────────────────────────

    /// <summary>If the first clause is a range predicate on the same field as the first
    /// ORDER BY field, set a seek hint on the CompiledQueryMatch so SortedIndexReader
    /// can skip walking irrelevant tree terms.</summary>
    public static void TrySetSortSeekHint(CompiledQueryMatch match,
        QueryExecution plan, OrderMetadata[] orderByFields)
    {
        if (orderByFields == null || orderByFields.Length == 0)
            return;

        var clauses = plan.Clauses;
        var execs = plan.Executions;
        if (clauses == null || clauses.Count == 0 || execs == null || execs.Length == 0)
            return;

        // Only consider the first ORDER BY field
        var sortField = orderByFields[0].Field.FieldName;

        // Find a range clause on the same field (scan all clauses, not just first — the sort-eligible
        // clause may not be the cheapest and thus not clause[0]).
        for (int i = 0; i < clauses.Count; i++)
        {
            var clause = clauses[i];
            var exec = execs[i];

            if (clause.FieldName != sortField.ToString())
                continue;

            // Range clauses on the sort field — supports long, double, and string
            if (clause.ClauseType is not (ClauseType.GreaterThan or ClauseType.GreaterThanOrEqual
                or ClauseType.LessThan or ClauseType.LessThanOrEqual or ClauseType.Between))
                continue;

            var packed = exec.PackedParamValue;
            if (packed.IsNone)
                continue;

            // For ascending order: seek to the lower bound (GT/GTE value)
            // For descending order: seek to the upper bound (LT/LTE value)
            bool ascending = orderByFields[0].Ascending;
            object seekValue = null;
            bool inclusive = false;

            if (ascending && clause.ClauseType is ClauseType.GreaterThan or ClauseType.GreaterThanOrEqual)
            {
                seekValue = packed.ValueType switch
                {
                    PackedParam.TypeLong => plan.LongValues[packed.Param1],
                    PackedParam.TypeDouble => plan.DoubleValues[packed.Param1],
                    PackedParam.TypeString => plan.StringValues[packed.Param1],
                    _ => null
                };
                inclusive = clause.ClauseType == ClauseType.GreaterThanOrEqual;
            }
            else if (ascending == false && clause.ClauseType is ClauseType.LessThan or ClauseType.LessThanOrEqual)
            {
                seekValue = packed.ValueType switch
                {
                    PackedParam.TypeLong => plan.LongValues[packed.Param1],
                    PackedParam.TypeDouble => plan.DoubleValues[packed.Param1],
                    PackedParam.TypeString => plan.StringValues[packed.Param1],
                    _ => null
                };
                inclusive = clause.ClauseType == ClauseType.LessThanOrEqual;
            }
            else if (clause.ClauseType == ClauseType.Between)
            {
                // Between: seek to the lower bound for ASC, upper bound for DESC
                int idx = ascending ? packed.Param1 : packed.Param2;
                seekValue = packed.ValueType switch
                {
                    PackedParam.TypeLong => plan.LongValues[idx],
                    PackedParam.TypeDouble => plan.DoubleValues[idx],
                    PackedParam.TypeString => plan.StringValues[idx],
                    _ => null
                };
                inclusive = true; // Between is always inclusive on both sides
            }

            if (seekValue != null)
            {
                match.SortHint = new SortHint(clause.FieldName, seekValue, inclusive);
                return;
            }
        }
    }

    // ── Sorting / Ordering ───────────────────────────────────────────────

    public static OrderMetadata[] GetSortMetadata(QueryBuilderParameters builderParameters, out bool hasEmpty)
    {
        hasEmpty = false;
        var query = builderParameters.Query;
        var index = builderParameters.Index;
        var getSpatialField = builderParameters.Factories?.GetSpatialFieldFactory;
        var indexMapping = builderParameters.IndexFieldsMapping;
        var queryMapping = builderParameters.FieldsToFetch;
        var allocator = builderParameters.Allocator;
        if (query.PageSize == 0) // no need to sort when counting only
        {
            return null;
        }

        var orderByFields = query.Metadata.OrderBy;

        if (orderByFields == null)
        {
            if (builderParameters.HasBoost && (
                    index.Configuration.OrderByScoreAutomaticallyWhenBoostingIsInvolved
                    || index.Configuration.CoraxVectorSearchOrderByScoreAutomatically))
            {
                if (builderParameters.Metadata.HasVectorSearch == false)
                    builderParameters.IndexReadOperation?.AssertCanOrderByScoreAutomaticallyWhenBoostingOrVectorSearchIsInvolved();

                return [new OrderMetadata(true, MatchCompareFieldType.Score)];
            }

            return null;
        }

        if (orderByFields.Length > MaxSortFields)
            throw new InvalidOperationException($"Corax does not support ordering by more than {MaxSortFields} properties.");

        int sortIndex = 0;
        var sortArray = new OrderMetadata[MaxSortFields];

        foreach (var field in orderByFields)
        {
            var nullsSortMode = (field.NullsOrdering, field.Ascending) switch
            {
                (NullsOrderingType.First, Ascending: true) => NullsSortMode.NullsSmallest,
                (NullsOrderingType.First, Ascending: false) => NullsSortMode.NullsLargest,
                (NullsOrderingType.Last, Ascending: true) => NullsSortMode.NullsLargest,
                (NullsOrderingType.Last, Ascending: false) => NullsSortMode.NullsSmallest,
                _ => (NullsSortMode?)null
            };

            if (field.OrderingType == OrderByFieldType.Random)
            {
                var seed = field.Arguments is { Length: > 0 } ?
                    (int)Hashing.XXHash32.CalculateRaw(field.Arguments[0].NameOrValue) :
                    Random.Shared.Next();
                sortArray[sortIndex++] = new OrderMetadata(seed);
                continue;
            }

            if (field.OrderingType == OrderByFieldType.Score)
            {
                // EntryComparerByScore.Compare is intentionally inverted (returns y.CompareTo(x)),
                // so ascending=true -> highest scores first (the default "most relevant first" search engine order).
                // ascending=false -> Descending<EntryComparerByScore> -> lowest scores first.
                sortArray[sortIndex++] = new OrderMetadata(true, MatchCompareFieldType.Score, field.Ascending);

                continue;
            }

            var fieldMetadata = QueryBuilderHelper.GetFieldIdForOrderBy(allocator, field.Name, index, builderParameters.HasDynamics,
                builderParameters.DynamicFields, indexMapping, queryMapping, false);

            bool fieldIsEmpty = builderParameters.IndexSearcher.GetTermAmountInField(fieldMetadata) == 0;
            if (fieldIsEmpty)
            {
                if (builderParameters.IndexReadOperation.IsSharded == false)
                    continue;
                hasEmpty = true;
            }

            if (field.OrderingType == OrderByFieldType.Distance)
            {
                var spatialField = getSpatialField(field.Name);

                int lastArgument;
                IPoint point;
                switch (field.Method)
                {
                    case MethodType.Spatial_Circle:
                        var cLatitude = field.Arguments[1].GetDouble(query.QueryParameters);
                        var cLongitude = field.Arguments[2].GetDouble(query.QueryParameters);
                        lastArgument = 2;
                        point = spatialField.ReadPoint(cLatitude, cLongitude).Center;
                        break;
                    case MethodType.Spatial_Wkt:
                        var wkt = field.Arguments[0].GetString(query.QueryParameters);
                        SpatialUnits? spatialUnits = null;
                        lastArgument = 1;
                        if (field.Arguments.Length > 1)
                        {
                            spatialUnits = Enum.Parse<SpatialUnits>(field.Arguments[1].GetString(query.QueryParameters), ignoreCase: true);
                            lastArgument = 2;
                        }

                        point = spatialField.ReadShape(wkt, spatialUnits).Center;
                        break;
                    case MethodType.Spatial_Point:
                        var pLatitude = field.Arguments[0].GetDouble(query.QueryParameters);
                        var pLongitude = field.Arguments[1].GetDouble(query.QueryParameters);
                        lastArgument = 2;
                        point = spatialField.ReadPoint(pLatitude, pLongitude).Center;
                        break;
                    default:
                        throw new ArgumentOutOfRangeException();
                }

                var roundTo = field.Arguments.Length > lastArgument
                    ? field.Arguments[lastArgument].GetDouble(query.QueryParameters)
                    : 0D;

                sortArray[sortIndex++] = new OrderMetadata(fieldMetadata, field.Ascending, MatchCompareFieldType.Spatial, point, roundTo,
                    spatialField.Units is SpatialUnits.Kilometers
                        ? global::Corax.Utils.Spatial.SpatialUnits.Kilometers
                        : global::Corax.Utils.Spatial.SpatialUnits.Miles, fieldIsEmpty, nullsSortMode);
                continue;
            }

            var orderingType = field.OrderingType;
            if (orderingType is OrderByFieldType.Implicit && index.Configuration.OrderByTicksAutomaticallyWhenDatesAreInvolved && index.IndexFieldsPersistence.HasTimeValues(field.Name.Value))
                orderingType = OrderByFieldType.Long;

            var metadataField = QueryBuilderHelper.GetFieldIdForOrderBy(allocator, field.Name.Value, index, builderParameters.HasDynamics,
                builderParameters.DynamicFields,
                indexMapping, queryMapping, false);
            // Dynamic CreateField fields: no IndexFieldsMapping entry, FieldId == DynamicField (-2).
            // Such fields are written per-document only when the index function emits CreateField;
            // docs that don't emit the field have NO entry (not even a NonExisting marker) in the
            // field's tree, so StreamAndIntersect (which walks tree + null/nonExisting lists) would
            // silently drop them. Route through ExtractAndSort instead — see SortingMatch.Fill.
            bool mayHaveMissingEntries = metadataField.FieldId == global::Corax.Constants.IndexWriter.DynamicField;
            OrderMetadata? temporaryOrder = null;
            switch (orderingType)
            {
                case OrderByFieldType.Custom:
                    throw new NotSupportedInCoraxException($"{nameof(Corax)} doesn't support Custom OrderBy.");
                case OrderByFieldType.AlphaNumeric:
                    sortArray[sortIndex++] = new OrderMetadata(metadataField, field.Ascending, MatchCompareFieldType.Alphanumeric, fieldIsEmpty, nullsSortMode, mayHaveMissingEntries);
                    continue;
                case OrderByFieldType.Long:
                    temporaryOrder = new OrderMetadata(metadataField, field.Ascending, MatchCompareFieldType.Integer, fieldIsEmpty, nullsSortMode, mayHaveMissingEntries);
                    break;
                case OrderByFieldType.Double:
                    temporaryOrder = new OrderMetadata(metadataField, field.Ascending, MatchCompareFieldType.Floating, fieldIsEmpty, nullsSortMode, mayHaveMissingEntries);
                    break;
            }

            sortArray[sortIndex++] = temporaryOrder ?? new OrderMetadata(metadataField, field.Ascending, MatchCompareFieldType.Sequence, fieldIsEmpty, nullsSortMode, mayHaveMissingEntries);
        }

        return sortArray[0..sortIndex];
    }

    public static IQueryMatch OrderBy(QueryBuilderParameters builderParameters, IQueryMatch match, in OrderMetadata[] orderMetadataSource, bool hasEmptySortingMatches)
    {
        RuntimeHelpers.EnsureSufficientExecutionStack();
        var indexSearcher = builderParameters.IndexSearcher;
        var take = builderParameters.Take;
        OrderMetadata[] orderMetadata = null;
        if (hasEmptySortingMatches == false)
            orderMetadata = orderMetadataSource;
        else
        {
            var currentIdx = 0;
            foreach (var orderMetadataItem in orderMetadataSource)
            {
                if (orderMetadataItem.FieldHasNoTerms)
                    continue;

                orderMetadata ??= new OrderMetadata[orderMetadataSource.Length];
                orderMetadata[currentIdx++] = orderMetadataItem;
            }

            orderMetadata = currentIdx == 0 ? [] : orderMetadata![..currentIdx];
        }

        switch (orderMetadata.Length)
        {
            case 0:
                return match;
            case 1:
                return indexSearcher.OrderBy(match, orderMetadata[0], builderParameters.Index.Configuration.NullsSortMode, take, builderParameters.Token);
            default:
                return indexSearcher.OrderBy(match, orderMetadata, builderParameters.Index.Configuration.NullsSortMode, take, builderParameters.Token);
        }
    }

    /// <summary>Apply ORDER BY from plan metadata when a full <see cref="QueryBuilderParameters"/> is not
    /// available (e.g., direct tests). Handles <c>ORDER BY score()</c> only — callers that need
    /// field / spatial / alphanumeric sorts must use the full
    /// <see cref="OrderBy(QueryBuilderParameters,IQueryMatch,in OrderMetadata[],bool)"/> overload.</summary>
    public static IQueryMatch ApplyScoreOrdering(PlanParameters planParams, IQueryMatch match, long take, CancellationToken token = default)
    {
        OrderByField[] orderByFields = planParams.Metadata.OrderBy;
        if (orderByFields == null || orderByFields.Length == 0)
            return match;

        var indexSearcher = planParams.IndexSearcher;
        int takeInt = take > int.MaxValue ? Constants.IndexSearcher.TakeAll : (int)take;

        for (int i = 0; i < orderByFields.Length; i++)
        {
            if (orderByFields[i].OrderingType == OrderByFieldType.Score)
            {
                var meta = new OrderMetadata(true, MatchCompareFieldType.Score, orderByFields[i].Ascending);
                return indexSearcher.OrderBy(match, meta, NullsSortMode.NullsLargest, take: takeInt, token: token);
            }
        }

        return match;
    }

    // ── Search helpers (used by ResolveClause for Search clause type) ────

    /// <summary>
    /// Replaces the search analyzer with an appropriate wildcard analyzer.
    /// LuceneAnalyzerAdapter wrapping KeywordAnalyzer has IsExactAnalyzer=false
    /// (because LuceneAnalyzerAdapter passes NoTransformers), so the generic
    /// CreateWildcardAnalyzer in the Legacy path would incorrectly lowercase the term.
    /// This matches the old CoraxQueryBuilder.ReplaceAnalyzerForWildcardQueries logic.
    /// </summary>
    private static FieldMetadata ReplaceAnalyzerForWildcardQueries(
        FieldMetadata searchMeta,
        QueryBuilderParameters builderParams,
        PlanParameters parameters)
    {
        var result = searchMeta;
        var indexFieldsMapping = builderParams?.IndexFieldsMapping ?? parameters?.IndexFieldsMapping;

        if (searchMeta.IsDynamic && indexFieldsMapping != null)
            result = searchMeta.ChangeAnalyzer(searchMeta.Mode, indexFieldsMapping.SearchAnalyzer(searchMeta.FieldName.ToString()));

        if (searchMeta.Analyzer is Lucene.LuceneAnalyzerAdapter laa && indexFieldsMapping != null)
        {
            global::Corax.Analyzers.Analyzer replacementAnalyzer = laa.Analyzer switch
            {
                global::Lucene.Net.Analysis.KeywordAnalyzer => indexFieldsMapping.ExactAnalyzer(searchMeta.FieldName.ToString()),
                Lucene.Analyzers.RavenStandardAnalyzer
                    or Lucene.Analyzers.NGramAnalyzer => indexFieldsMapping.DefaultAnalyzer,
                global::Lucene.Net.Analysis.Standard.StandardAnalyzer when laa.Analyzer.GetType() == typeof(global::Lucene.Net.Analysis.Standard.StandardAnalyzer)
                    => indexFieldsMapping.DefaultAnalyzer,
                Lucene.Analyzers.LowerCaseKeywordAnalyzer
                    or Lucene.Analyzers.Collation.CollationAnalyzer => indexFieldsMapping.DefaultAnalyzer,
                _ => null
            };

            if (replacementAnalyzer != null)
                result = searchMeta.ChangeAnalyzer(global::Corax.FieldIndexingMode.Search, replacementAnalyzer);
        }

        return result;
    }

    /// <summary>Create the appropriate DirectScan match based on whether residual predicates exist.</summary>
    private static DirectScanMatchBase BuildDirectScan(
        IndexSearcher searcher, IQueryMatch drivingMatch,
        long[] longParams, double[] doubleParams, Voron.Slice[] sliceParams, long[] fieldRootPages,
        ResidualScanIlEmitter.ResidualScanPredicate residualDelegate,
        ScanPredicateInfo[] residualArray)
    {
        if (residualArray == null) 
            return new DirectScanSimpleMatch(searcher, drivingMatch, take: -1);
        
        return new DirectScanFilteredMatch(
            searcher, drivingMatch, longParams, doubleParams, sliceParams, fieldRootPages,
            take: -1, precompiledDelegate: residualDelegate);
    }

    /// <summary>Singleton no-op ITermsProvider for TreeScan slots where the field doesn't exist.
    /// FillPostingListIds returns 0 immediately, so the bitmap op is a no-op.</summary>
    private sealed class EmptyTermsProviderInstance : ITermsProvider
    {
        public static readonly EmptyTermsProviderInstance Instance = new();
        public int FillPostingListIds(Span<long> postingListIds) => 0;
        public void Reset() { }
        public bool Next(out TermMatch term) { term = default; return false; }
        public QueryInspectionNode Inspect() => new("EmptyTermsProvider");
    }
}
