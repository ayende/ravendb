namespace Corax.Querying.Planning;

public sealed class InspectionOp
{
    public string Name;
    public string Dispatch;
    public string FieldName;
    public string ClauseType;
    public bool IsNegated;
    public long EstimatedCardinality;

    public int FlatClauseIndex = -1;
}
