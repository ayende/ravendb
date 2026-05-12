using System.Collections.Generic;

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
/// </summary>
/// <summary>
/// Structural template for a single WHERE predicate. Immutable after the first parse,
/// cached on ClauseTemplate, shared across all executions of the same query text.
/// Per-execution data lives in the parallel <see cref="ClauseExecution"/> array.
/// </summary>
public sealed class ClauseInfo
{
    public string FieldName;
    public ClauseType ClauseType;
    public int OriginalIndex;
    public bool IsNegated;
    public bool IsExact;
    public int SearchOperator; // for Search (AND=1/OR=0)
    public SpatialOperationType SpatialMethodType;
    public VectorSourceKind VectorMethod;

    /// <summary>Set for NotEquals clauses appearing in OR chains.
    /// Example: `WHERE Name != 'a' OR Age = 25`
    /// The NOT(Name='a') term cannot use the raw posting list (which contains entries
    /// WITH 'a', not entries WITHOUT 'a'). Instead, ResolveMatches pre-materializes
    /// AllEntries ANDNOT TermQuery('a') into a BitmapMatch, so FillFromMatch during
    /// execution correctly ORs in the complement set.</summary>
    public bool IsOrChainNotEquals;

    public List<ClauseInfo> OrSubClauses;
    public List<ClauseInfo> AndSubClauses;

    /// <summary>Parameter bindings indexed by <see cref="BindingIndex"/> constants.
    /// If <see cref="HasBoost"/> is true, the last entry is the boost factor binding.</summary>
    public ParameterBinding[] Bindings;

    /// <summary>True if this clause is wrapped in boost(). When set, Bindings[^1] is the
    /// boost factor binding and exec.BoostFactor is resolved from it per-execution.</summary>
    public bool HasBoost;

    /// <summary>Index into ClauseTemplate.WhenConditions, or -1 if no WHEN wraps this clause.
    /// The condition is evaluated per-execution in PopulateClauseValues.</summary>
    public int WhenConditionIndex = -1;
}
