namespace Corax.Querying.Planning;

public sealed class CompiledPlan
{
    public QueryILEmitter.CompiledExecuteDelegate CompiledDelegate { get; init; }
    public string ExplainSource { get; init; }
    public int Ordering { get; init; }
}
