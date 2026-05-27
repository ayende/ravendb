using Voron;

namespace Corax.Querying.Planning;

/// <summary>
/// Concrete state passed to the residual-scan delegate emitted by
/// <see cref="ResidualScanIlEmitter"/>. The emitted IL loads fields directly via
/// <c>Ldfld</c> on a managed pointer to this struct — no virtcall, no interface dispatch.
///
/// Both the entry-scan path (<see cref="Matches.CompiledQueryMatch"/>) and the direct-scan
/// path (<see cref="Matches.DirectScanFilteredMatch"/>) embed this struct as a field and pass
/// it by <c>ref</c> to the delegate.
/// </summary>
public struct ResidualParams
{
    public long[] Longs;
    public double[] Doubles;
    public Slice[] Slices;
    public long[] FieldRootPages;
}
