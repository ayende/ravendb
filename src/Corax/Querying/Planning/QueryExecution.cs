using System.Collections.Generic;

namespace Corax.Querying.Planning;

public class QueryExecution
{
    public PlanOp[] Ops;

    /// <summary>
    /// Cache-key disambiguator for <see cref="PlanCache"/>. Two query executions with the same
    /// queryText but different shapes must produce different OperandOrdering values to avoid
    /// reusing compiled IL across incompatible plans.
    ///
    /// Bit layout (low → high):
    /// <list type="table">
    ///   <listheader><term>Bits</term><description>Meaning</description></listheader>
    ///   <item><term>0..29</term><description>Clause-ordering encoding. Up to 10 clauses × 3 bits each;
    ///     slot i holds <c>clauses[i].OriginalIndex &amp; 0x7</c> shifted by <c>i*3</c>. Captures the
    ///     post-cardinality-sort order so a query whose clauses get reordered into a different sequence
    ///     gets a distinct cache key.</description></item>
    ///   <item><term>30</term><description>HasBoost flag. Set when any clause has boost(); forces
    ///     every op to QueryMatch dispatch (so scores are accumulated).</description></item>
    ///   <item><term>31</term><description>SentinelBetween flag. Set when any execution carries
    ///     BetweenLowUnbounded / BetweenHighUnbounded — those rewrites force QueryMatch dispatch
    ///     (IsTreeScanEligibleClause rejects sentinel BETWEEN) and must not collide with cached
    ///     TreeScan IL for the same queryText. Set in QueryPlanBuilder.Resolution.cs ~L304. This is
    ///     the int's sign bit, but PlanCache compares orderings bitwise (Vector256.Equals / scalar !=),
    ///     never as a signed magnitude, so setting bit 31 is safe — it just makes the value negative.</description></item>
    /// </list>
    /// </summary>
    public int OperandOrdering;

    /// <summary>Bit 30 of <see cref="OperandOrdering"/>. Set when any clause carries a boost factor.</summary>
    public const int HasBoostBit = 1 << 30;

    /// <summary>Bit 31 of <see cref="OperandOrdering"/>. Set when any execution carries
    /// BetweenLowUnbounded / BetweenHighUnbounded sentinel values.</summary>
    public const int SentinelBetweenBit = 1 << 31;

    /// <summary>Clause list from the query plan builder — structural template data.</summary>
    public List<ClauseInfo> Clauses;

    /// <summary>Per-execution state parallel to Clauses — parameter values, cardinality, etc.</summary>
    public ClauseExecution[] Executions;
    public bool IsAllEntries;
    public bool AllNegated;

    /// <summary>Typed parameter values for clause resolution. Populated during plan building
    /// from resolved query parameters and literal values. Each clause's PackedParam field
    /// encodes (type, index) pairs pointing into these arrays, so resolution never has to
    /// reparse strings back to their native types.</summary>
    public long[] LongValues;
    public double[] DoubleValues;
    public string[] StringValues;

    /// <summary>Spatial operations to apply after the bitmap filter phase builds the candidate bitmap.
    /// Each spatial match is ANDed with the candidate set.</summary>
    public SpatialFilterOp[] SpatialFilters;

    /// <summary>Vector operations to apply after spatial filtering.
    /// The bitmap-producing CompiledQueryMatch is passed as the filterQuery to VectorSearchMatch.</summary>
    public VectorSearchOp[] VectorSelects;

    /// <summary>Packed parameter type signature from ScanPredicateInfos.
    /// 2 bits per predicate (0=long, 1=double, 2=string) for the FIRST 16 predicates.
    /// For ≤ 16 predicates this is the exact identity. For more, it acts as a lossy
    /// hash and <see cref="FullKinds"/> carries the disambiguator.</summary>
    public int TypeSignature;

    /// <summary>Full per-predicate kind vector. Populated only when there are more than
    /// 16 typed scan predicates. Null in the common case, so PlanCache lookups stay
    /// branch-free on the hot path. When non-null, PlanCache.Add walks the slot chain
    /// (CompiledPlan.Next) and SequenceEqual-compares this vs. existing FullKinds to
    /// disambiguate plans whose <see cref="TypeSignature"/> ints collide.</summary>
    public byte[] FullKinds;

    /// <summary>Number of bitmaps this plan needs at execution time.
    /// Slot 0 = main result, slot 1 = scratch for AND-with-postings / AND-NOT and OR-group
    /// accumulation. Plans with multiple AndGroups inside an OR chain use slot 2 as a
    /// save slot during the swap-build-or pattern, so the RequiredBitmaps field is set to 3 for those.
    /// Default is 2 (covers all non-multi-AndGroup plans).</summary>
    public int RequiredBitmaps = 2;

    /// <summary>Metadata for entry scan predicates. Used by the IL emitter to generate
    /// direct comparison calls. Null if no entry scan is possible.</summary>
    public ScanPredicateInfo[] ScanPredicateInfos;

    /// <summary>Per-execution term counts for OrRange/AndRange ops. Each range op's
    /// ParamIndex2 is an index into this array. The IL reads the count at runtime,
    /// so the same compiled delegate handles different IN parameter array sizes.</summary>
    public int[] InRangeCounts;
}
