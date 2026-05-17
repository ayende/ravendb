namespace Corax.Querying.Planning;

public enum ScanValueType : byte
{
    Long,    // reader.CurrentLong vs ctx.ResidualLongParams[i]
    Double,  // reader.CurrentDouble vs ctx.ResidualDoubleParams[i]
    Slice,   // IL-emitted byte-sequence comparison
}