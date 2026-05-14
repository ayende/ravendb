using Voron;

namespace Corax.Querying.Planning;

/// <summary>Common interface for types that provide the parameter arrays and field root
/// pages needed by the emitted residual-scan predicate delegates. Both
/// <see cref="Matches.CompiledQueryMatch"/> and <see cref="Matches.DirectScanMatch"/>
/// implement this so a single <see cref="ResidualScanIlEmitter"/> can emit IL against
/// either context type.</summary>
public interface IPredicateEvaluationContext
{
    long[] ResidualLongParams { get; }
    double[] ResidualDoubleParams { get; }
    Slice[] ResidualSliceParams { get; }
    long[] ResidualFieldRootPages { get; }
}
