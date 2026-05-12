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
}