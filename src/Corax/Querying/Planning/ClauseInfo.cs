using System;
using System.Collections.Generic;
using Sparrow.Json;

namespace Corax.Querying.Planning;

/// <summary>
/// Intermediate representation of a single WHERE predicate, between the RQL AST
/// and the PlanOp[] execution plan.
///
/// Why not reuse the RQL AST directly?
/// - RQL AST exists in the Raven.Server project, not accessible here
/// - The AST is a recursive tree (AND(AND(A,B),C)); ClauseInfo is a flat list suitable for
///   plan emission. Mixed AND/OR trees are flattened into OrGroup/AndGroup sub-lists.
/// - Field names are resolved (alias substitution, id() expansion, quoted-name handling).
/// - Parameter values are resolved from the blittable and stored as native types in the
///   plan's typed arrays (LongValues, DoubleValues, StringValues). PackedParam encodes
///   (type, index), so resolution never reparses strings.
/// - A clause type is classified into a flat enum — downstream code switches on one value
///   instead of pattern-matching AST node types and method names.
/// - Planning annotations (Cardinality, IsExact, BoostFactor, IsNegated) are attached per
///   clause for operand reordering, dispatch classification, and entry-scan eligibility.
///
/// <para><b>Freeze contract:</b> ClauseInfo is frozen at the end of template construction.
/// After freezing, property writes throw <see cref="InvalidOperationException"/>.
/// Per-execution mutable state lives on <see cref="ClauseExecution"/>.</para>
/// </summary>
public sealed class ClauseInfo
{
    public string FieldName { get; init; }

    /// <summary>Pre-resolved dynamic-index field name variant (e.g. <c>exact(Name)</c> or
    /// <c>search(Name)</c>). Set by the DynamicFieldNameResolve walker step for auto-indexes.
    /// Null for static indexes and non-exact/non-search clauses. When set, execution-time
    /// field metadata lookups use this instead of <see cref="FieldName"/> — saving one string
    /// allocation per clause per query execution.</summary>
    public string ResolvedFieldName
    {
        get;
        set { ThrowIfFrozen(); field = value; }
    }

    public ClauseType ClauseType
    {
        get;
        set { ThrowIfFrozen(); field = value; }
    }

    public int OriginalIndex { get; init; }

    public bool IsNegated
    {
        get;
        set { ThrowIfFrozen(); field = value; }
    }

    public bool IsExact
    {
        get;
        set { ThrowIfFrozen(); field = value; }
    }

    /// <summary>for Search (AND=1/OR=0)</summary>
    public int SearchOperator { get; init; }

    public SpatialOperationType SpatialMethodType { get; init; }

    public VectorSourceKind VectorMethod { get; init; }

    /// <summary>Set for any negated clause appearing in an OR chain — NotEquals,
    /// NOT IN, NOT AllIn, NOT exists(), NOT startsWith(), etc.
    /// Example: `WHERE Name != 'a' OR Age = 25` or `WHERE NOT exists(Tags) OR Score &gt; 10`.
    /// The complement set cannot be delivered by the raw posting list / range / tree-scan
    /// (which would produce the POSITIVE form). Instead, ResolveMatches pre-materializes
    /// AllEntries ANDNOT(positive form) into a BitmapMatch via CreateNotEqualsOrMatch,
    /// so OrWithMatch during execution correctly ORs in the complement set. The slot
    /// is always dispatched via QueryMatch, regardless of the underlying clause type.</summary>
    public bool IsOrChainNotEquals
    {
        get;
        set { ThrowIfFrozen(); field = value; }
    }

    /// <summary>Sub-clauses for OrGroup / AndGroup nodes. Mutually exclusive with other
    /// group-type usage — a clause is either OrGroup or AndGroup (never both), determined
    /// by <see cref="ClauseType"/>.</summary>
    public List<ClauseInfo> SubClauses
    {
        get;
        set { ThrowIfFrozen(); field = value; }
    }

    /// <summary>Parameter bindings indexed by <see cref="BindingIndex"/> constants.
    /// If <see cref="HasBoost"/> is true, the last entry is the boost factor binding.</summary>
    public ParameterBinding[] Bindings
    {
        get;
        set { ThrowIfFrozen(); field = value; }
    }

    /// <summary>For IN/AllIn clauses: true when ALL bindings are literals (no parameters).
    /// When set, the dominant type and type-incompatible filtering are pre-computed at
    /// template time, skipping per-execution work in ResolveInFromBindings.</summary>
    public bool AllBindingsAreLiteral
    {
        get;
        set { ThrowIfFrozen(); field = value; }
    }

    /// <summary>Pre-computed dominant type for all-literal IN/AllIn clauses. Only valid
    /// when <see cref="AllBindingsAreLiteral"/> is true. The dominant type determines which typed
    /// array (Long/Double/String) receives the resolved values.</summary>
    public ParamValueType InDominantType
    {
        get;
        set { ThrowIfFrozen(); field = value; }
    }

    /// <summary>True if this clause is wrapped in boost(). When set, Bindings[^1] is the
    /// boost factor binding and exec.BoostFactor is resolved from it per-execution.</summary>
    public bool HasBoost
    {
        get;
        set { ThrowIfFrozen(); field = value; }
    }

    /// <summary>Optional WHEN condition delegate. Null when no WHEN wraps this clause.
    /// Created at ParseTemplate time as a closure over the parsed condition expression.
    /// Evaluated per-execution in BuildAndCompile: called with the query's BlittableJsonReaderObject
    /// parameters; returns true to keep the clause, false to eliminate it.</summary>
    public Func<BlittableJsonReaderObject, bool> WhenCondition
    {
        get;
        set { ThrowIfFrozen(); field = value; }
    }

    /// <summary>True once <see cref="Freeze"/> has been called. Frozen instances reject
    /// all property mutations with <see cref="InvalidOperationException"/>.</summary>
    public bool IsFrozen { get; private set; }

    /// <summary>Mark this ClauseInfo as part of an immutable plan-cache template. After
    /// freezing, any property write throws. Idempotent — calling Freeze() on an already
    /// frozen instance is a no-op. Sub-clauses (<see cref="SubClauses"/>) are NOT
    /// auto-frozen; callers must freeze each sub-clause individually.</summary>
    public void Freeze() => IsFrozen = true;

    private void ThrowIfFrozen()
    {
        if (IsFrozen)
            throw new InvalidOperationException(
                "ClauseInfo is frozen (plan-cache template). " +
                "Per-execution mutable state belongs on ClauseExecution.");
    }
}
