using System;
using System.Collections.Generic;
using Sparrow;
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
/// <para><b>Freeze contract:</b> ClauseInfo participates in the plan-cache template. Once
/// <see cref="Freeze"/> has been called (at the end of plan-template construction), any
/// subsequent attempt to mutate a property throws <see cref="InvalidOperationException"/>.
/// This catches a regression class where post-build code rewrites a shared template in
/// place — different executions of the same cached plan would then see inconsistent state.
/// The fix in such cases is to <see cref="Clone"/> the ClauseInfo (clones are un-frozen)
/// and mutate the copy. See the RavenDB_17423 fix history for the original bug pattern.</para>
/// </summary>
public sealed class ClauseInfo
{
    private bool _frozen;

    private string _fieldName;
    private ClauseType _clauseType;
    private int _originalIndex;
    private bool _isNegated;
    private bool _isExact;
    private int _searchOperator;
    private SpatialOperationType _spatialMethodType;
    private VectorSourceKind _vectorMethod;
    private bool _isOrChainNotEquals;
    private List<ClauseInfo> _orSubClauses;
    private List<ClauseInfo> _andSubClauses;
    private ParameterBinding[] _bindings;
    private bool _hasBoost;
    private Func<BlittableJsonReaderObject, bool> _whenCondition;

    public string FieldName
    {
        get => _fieldName;
        set { ThrowIfFrozen(); _fieldName = value; }
    }

    public ClauseType ClauseType
    {
        get => _clauseType;
        set { ThrowIfFrozen(); _clauseType = value; }
    }

    public int OriginalIndex
    {
        get => _originalIndex;
        set { ThrowIfFrozen(); _originalIndex = value; }
    }

    public bool IsNegated
    {
        get => _isNegated;
        set { ThrowIfFrozen(); _isNegated = value; }
    }

    public bool IsExact
    {
        get => _isExact;
        set { ThrowIfFrozen(); _isExact = value; }
    }

    /// <summary>for Search (AND=1/OR=0)</summary>
    public int SearchOperator
    {
        get => _searchOperator;
        set { ThrowIfFrozen(); _searchOperator = value; }
    }

    public SpatialOperationType SpatialMethodType
    {
        get => _spatialMethodType;
        set { ThrowIfFrozen(); _spatialMethodType = value; }
    }

    public VectorSourceKind VectorMethod
    {
        get => _vectorMethod;
        set { ThrowIfFrozen(); _vectorMethod = value; }
    }

    /// <summary>Set for any negated clause appearing in an OR chain — NotEquals,
    /// NOT IN, NOT AllIn, NOT exists(), NOT startsWith(), etc.
    /// Example: `WHERE Name != 'a' OR Age = 25` or `WHERE NOT exists(Tags) OR Score &gt; 10`.
    /// The complement set cannot be delivered by the raw posting list / range / tree-scan
    /// (which would produce the POSITIVE form). Instead, ResolveMatches pre-materializes
    /// AllEntries ANDNOT(positive form) into a BitmapMatch via CreateNotEqualsOrMatch,
    /// so FillFromMatch during execution correctly ORs in the complement set. The slot
    /// is always dispatched via QueryMatch, regardless of the underlying clause type.</summary>
    public bool IsOrChainNotEquals
    {
        get => _isOrChainNotEquals;
        set { ThrowIfFrozen(); _isOrChainNotEquals = value; }
    }

    public List<ClauseInfo> OrSubClauses
    {
        get => _orSubClauses;
        set { ThrowIfFrozen(); _orSubClauses = value; }
    }

    public List<ClauseInfo> AndSubClauses
    {
        get => _andSubClauses;
        set { ThrowIfFrozen(); _andSubClauses = value; }
    }

    /// <summary>Parameter bindings indexed by <see cref="BindingIndex"/> constants.
    /// If <see cref="HasBoost"/> is true, the last entry is the boost factor binding.</summary>
    public ParameterBinding[] Bindings
    {
        get => _bindings;
        set { ThrowIfFrozen(); _bindings = value; }
    }

    /// <summary>True if this clause is wrapped in boost(). When set, Bindings[^1] is the
    /// boost factor binding and exec.BoostFactor is resolved from it per-execution.</summary>
    public bool HasBoost
    {
        get => _hasBoost;
        set { ThrowIfFrozen(); _hasBoost = value; }
    }

    /// <summary>Optional WHEN condition delegate. Null when no WHEN wraps this clause.
    /// Created at ParseTemplate time as a closure over the parsed condition expression.
    /// Evaluated per-execution in BuildAndCompile: called with the query's BlittableJsonReaderObject
    /// parameters; returns true to keep the clause, false to eliminate it.</summary>
    public Func<BlittableJsonReaderObject, bool> WhenCondition
    {
        get => _whenCondition;
        set { ThrowIfFrozen(); _whenCondition = value; }
    }

    /// <summary>True once <see cref="Freeze"/> has been called. Frozen instances reject
    /// all property mutations with <see cref="InvalidOperationException"/>.</summary>
    public bool IsFrozen => _frozen;

    /// <summary>Mark this ClauseInfo as part of an immutable plan-cache template. After
    /// freezing, any property write throws. Idempotent — calling Freeze() on an already
    /// frozen instance is a no-op. Sub-clauses (OrSubClauses, AndSubClauses) are NOT
    /// auto-frozen; callers must freeze each sub-clause individually.</summary>
    public void Freeze() => _frozen = true;

    /// <summary>Create a mutable (un-frozen) copy of this ClauseInfo. Used by per-execution
    /// rewrite paths that need to override a field for a single query without disturbing
    /// the shared template. Shallow-copies reference fields (Bindings array, OrSub/AndSub
    /// lists, WhenCondition delegate); the copy may share those references with the original.
    /// If a caller mutates the array/list contents (not just the reference), it must clone
    /// those first too.</summary>
    public ClauseInfo Clone()
    {
        return new ClauseInfo
        {
            _fieldName = _fieldName,
            _clauseType = _clauseType,
            _originalIndex = _originalIndex,
            _isNegated = _isNegated,
            _isExact = _isExact,
            _searchOperator = _searchOperator,
            _spatialMethodType = _spatialMethodType,
            _vectorMethod = _vectorMethod,
            _isOrChainNotEquals = _isOrChainNotEquals,
            _orSubClauses = _orSubClauses,
            _andSubClauses = _andSubClauses,
            _bindings = _bindings,
            _hasBoost = _hasBoost,
            _whenCondition = _whenCondition,
            // _frozen intentionally left false — clones are mutable
        };
    }

    private void ThrowIfFrozen()
    {
        if (_frozen)
            throw new InvalidOperationException(
                "ClauseInfo is frozen as part of the plan-cache template. Mutations would corrupt " +
                "cached plans shared across executions. Use Clone() to get a mutable copy.");
    }
}
