namespace Corax.Querying.Planning;

public struct PlanOp
{
    public PlanOpKind Kind;
    public int ParamIndex;
    public int ParamIndex2;
    public int BitmapLocal;
    public long EstimatedCardinality;

    /// <summary>Controls how <see cref="ParamIndex"/> is resolved at execution time
    /// for term ops (Fill/And/Or/AndNot WithPostings):
    /// <list type="bullet">
    /// <item><see cref="MatchDispatch.QueryMatch"/> — <c>ctx.ResolvedMatches[ParamIndex]</c>
    ///   (IQueryMatch interface dispatch; vector, spatial, search, boosted clauses).</item>
    /// <item><see cref="MatchDispatch.PostingList"/> — <c>ctx.PostingSources[ParamIndex]</c>
    ///   (native posting-list dispatch; Equals / NotEquals / In / AllIn).</item>
    /// <item><see cref="MatchDispatch.TreeScan"/> — <c>ctx.TermsProviders[ParamIndex]</c>
    ///   (multi-term bitmap fill; StartsWith / EndsWith / Contains / Exists / Regex / ranges).</item>
    /// </list></summary>
    public MatchDispatch Dispatch;

    /// <summary>When true, suppress the empty-check early exit after
    /// <see cref="PlanOpKind.AndWithPostings"/>. Used for AND sub-chains inside
    /// an OR accumulator where an empty intermediate result is not a reason to
    /// abort the whole expression.</summary>
    public bool SkipEarlyExit;
}
