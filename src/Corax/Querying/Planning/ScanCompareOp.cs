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
}