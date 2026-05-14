using System.Collections.Generic;

namespace Corax.Querying.Planning;

public class QueryExecution
{
    public PlanOp[] Ops;
    public int OperandOrdering;

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
