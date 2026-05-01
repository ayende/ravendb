namespace Corax.Querying.Planning;

public sealed class CompiledPlan
{
    public QueryILEmitter.CompiledExecuteDelegate CompiledDelegate { get; init; }
    public string ExplainSource { get; init; }
    public int Ordering { get; init; }

    /// <summary>Packed parameter type signature (2 bits per param: 0=long, 1=double, 2=string).
    /// Different types produce different IL (different comparison instructions).</summary>
    public int TypeSignature { get; init; }
}
