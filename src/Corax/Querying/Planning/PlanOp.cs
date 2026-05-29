namespace Corax.Querying.Planning;

public struct PlanOp
{
    public PlanOpKind Kind;
    public int ParamIndex;
    public int ParamIndex2;
    public int BitmapLocal;
    public long EstimatedCardinality;

    /// <summary>When true, suppress the empty-check early exit after an AND. Used for AND inside an OR, where we can't just abort</summary>
    public bool SkipEarlyExit;
}
