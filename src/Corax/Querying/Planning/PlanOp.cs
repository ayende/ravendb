namespace Corax.Querying.Planning;

public struct PlanOp
{
    public PlanOpKind Kind;
    public int ParamIndex;
    public int ParamIndex2;
    public int BitmapLocal;
    public long EstimatedCardinality;

    /// <summary>When true, suppress the empty-check early exit after
    /// <see cref="PlanOpKind.AndFromPostingSource"/>. Used for AND sub-chains inside
    /// an OR accumulator where an empty intermediate result is not a reason to
    /// abort the whole expression.</summary>
    public bool SkipEarlyExit;
}
