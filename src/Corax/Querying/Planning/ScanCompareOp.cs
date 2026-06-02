namespace Corax.Querying.Planning;

public enum ScanCompareOp : byte
{
    Equal,
    NotEqual,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
    Between,
    /// <summary>Field exists — reader.FindNext succeeds. No value comparison.</summary>
    Exists,
    /// <summary>StartsWith — reader term starts with the param slice. Iterate all terms for multi-value fields.</summary>
    StartsWith,
    /// <summary>EndsWith — reader term ends with the param slice.</summary>
    EndsWith,
    /// <summary>IN — the field has at least one term equal to one of the IN value set
    /// (OR semantics). Values live in a per-execution set, indexed positionally by the
    /// IN-leaf walk order (see <see cref="QueryExecution.ResidualInSets"/>).</summary>
    In,
    /// <summary>ALL IN — the (multi-valued) field contains every value in the set
    /// (set-containment). Same per-execution value-set storage as <see cref="In"/>.</summary>
    AllIn,
    /// <summary>Always-true placeholder for a <see cref="ClauseType.MatchAll"/> sentinel
    /// (x ∧ ALL = x; x ∨ ALL = ALL). It carries no field or value. As a TOP-LEVEL clause the
    /// residual-set builders drop it (the clause stays scan-eligible). As a GROUP sub-predicate it is
    /// kept in the tree and <see cref="ResidualScanIlEmitter"/> bakes it as a no-op (AND branch passes;
    /// OR branch short-circuits the group to passed). It consumes no fieldRootPage slot.</summary>
    AlwaysTrue,
    /// <summary>Always-false placeholder for a <see cref="ClauseType.MatchNothing"/> sentinel
    /// (x ∧ ∅ = ∅; x ∨ ∅ = x). It carries no field or value. As a TOP-LEVEL clause the builders
    /// collapse it to a disqualifier so the bitmap pipeline empties the whole AND. As a GROUP
    /// sub-predicate it is kept and <see cref="ResidualScanIlEmitter"/> bakes it as an unconditional
    /// fail (AND group fails; OR branch falls through to the next). It consumes no fieldRootPage
    /// slot.</summary>
    AlwaysFalse,
}