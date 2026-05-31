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
/// <para>ClauseInfo is the param-independent template, shared by reference across all
/// executions of a cached plan. Per-execution mutable state lives on
/// <see cref="ClauseExecution"/>; rewrites that vary by parameter values must clone the
/// ClauseInfo rather than mutating it in place.</para>
/// </summary>
public sealed class ClauseInfo
{
    public string FieldName { get; init; }

    /// <summary>Pre-resolved dynamic-index field name variant (e.g. <c>exact(Name)</c> or
    /// <c>search(Name)</c>). Set by the DynamicFieldNameResolve walker step for auto-indexes.
    /// Null for static indexes and non-exact/non-search clauses. When set, execution-time
    /// field metadata lookups use this instead of <see cref="FieldName"/> — saving one string
    /// allocation per clause per query execution.</summary>
    public string ResolvedFieldName { get; set; }

    public ClauseType ClauseType { get; set; }

    public int OriginalIndex { get; init; }

    public bool IsNegated { get; set; }

    public bool IsExact { get; set; }

    /// <summary>for Search (AND=1/OR=0)</summary>
    public int SearchOperator { get; init; }

    public SpatialOperationType SpatialMethodType { get; init; }

    public VectorSourceKind VectorMethod { get; init; }

    /// <summary>Set for any negated clause appearing in an OR chain — NotEquals,
    /// NOT IN, NOT AllIn, NOT exists(), NOT startsWith(), etc.
    /// Example: `WHERE Name != 'a' OR Age = 25` or `WHERE NOT exists(Tags) OR Score &gt; 10`.
    /// The complement set cannot be delivered by the raw posting list / range / tree-scan
    /// (which would produce the POSITIVE form). Instead, the IL emitter builds the complement
    /// at execution time via FillAllEntries + AndNot(positive form), so OrWithMatch correctly
    /// ORs in the set of entries NOT matching the positive predicate. Boost is intentionally
    /// ignored on such clauses (matches Lucene — there is no match to score).</summary>
    public bool IsOrChainNotEquals { get; set; }

    /// <summary>Sub-clauses for OrGroup / AndGroup nodes. Mutually exclusive with other
    /// group-type usage — a clause is either OrGroup or AndGroup (never both), determined
    /// by <see cref="ClauseType"/>.</summary>
    public List<ClauseInfo> SubClauses { get; set; }

    /// <summary>Parameter bindings indexed by <see cref="BindingIndex"/> constants.
    /// If <see cref="HasBoost"/> is true, the last entry is the boost factor binding.</summary>
    public ParameterBinding[] Bindings { get; set; }

    /// <summary>True if this clause is wrapped in boost(). When set, Bindings[^1] is the
    /// boost factor binding and exec.BoostFactor is resolved from it per-execution.</summary>
    public bool HasBoost { get; set; }

    /// <summary>Optional WHEN condition delegate. Null when no WHEN wraps this clause.
    /// Created at ParseTemplate time as a closure over the parsed condition expression.
    /// Evaluated per-execution in BuildAndCompile: called with the query's BlittableJsonReaderObject
    /// parameters; returns true to keep the clause, false to eliminate it.</summary>
    public Func<BlittableJsonReaderObject, bool> WhenCondition { get; set; }
}
