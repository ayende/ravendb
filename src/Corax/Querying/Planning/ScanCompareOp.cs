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
}