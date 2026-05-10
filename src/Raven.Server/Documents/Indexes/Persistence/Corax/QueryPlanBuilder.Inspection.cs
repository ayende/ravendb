using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Planning;

namespace Raven.Server.Documents.Indexes.Persistence.Corax;

/// <summary>
/// Diagnostic / inspection methods for the compiled query plan.
/// Builds the Studio-facing QueryInspectionNode tree from the cached
/// InspectionTemplate plus runtime telemetry (timings, row counts,
/// entry-scan triggers).
/// </summary>
internal static partial class QueryPlanBuilder
{
    /// <summary>Build the inspection graph from the cached template + runtime telemetry.</summary>
    public static QueryInspectionNode BuildInspectionGraph(CompiledPlan compiledPlan, IQueryMatch executedMatch, IQueryMatch sortingWrapper = null)
    {
        long[] timings = null;
        long[] resultCounts = null;
        int entryScanAt = -1;
        long scannedEntries = -1;

        if (executedMatch is CompiledQueryMatch compiled)
        {
            compiled.GetTelemetry(out timings, out resultCounts, out entryScanAt);
            scannedEntries = compiled.Count;
        }

        double tickFreq = Stopwatch.Frequency / 1000.0;
        var template = compiledPlan.InspectionTemplate;
        if (template == null || template.Length == 0)
            return executedMatch.Inspect();

        var rootParams = new Dictionary<string, string>();
        if (scannedEntries >= 0)
            rootParams["ScannedEntries"] = scannedEntries.ToString();
        if (compiledPlan.ExplainSource != null)
            rootParams["Explain"] = compiledPlan.ExplainSource;

        var root = new QueryInspectionNode("CompiledQuery", parameters: rootParams);
        QueryInspectionNode orGroupNode = null;

        for (int i = 0; i < template.Length; i++)
        {
            var t = template[i];

            // For queries like: WHERE (A AND B) OR (C AND D)
            // the plan ops are:
            //   ClearBitmap[2]           <- prepare scratch
            //   SwapBitmaps[0,2]         <- save main, work in scratch  -> InsideAndGroup = true
            //   FillFromPostings(A)      <- first AND-group
            //   AndWithPostings(B)       <-
            //   OrBitmaps[0,2]           <- merge scratch into main     -> InsideAndGroup = false
            //   ClearBitmap[2]
            //   SwapBitmaps[0,2]         <- second AND-group            -> InsideAndGroup = true
            //   FillFromPostings(C)      <-
            //   AndWithPostings(D)       <-
            //   OrBitmaps[0,2]           <- merge                      -> InsideAndGroup = false
            //
            // The inspection tree groups these into:
            //   CompiledQuery
            //     AND-Group
            //       Fill(A)
            //       AND(B)
            //     AND-Group
            //       Fill(C)
            //       AND(D)
            if (t.InsideAndGroup && orGroupNode == null)
                orGroupNode = new QueryInspectionNode("AND-Group");
            else if (!t.InsideAndGroup && orGroupNode != null)
            {
                root.Children.Add(orGroupNode);
                orGroupNode = null;
            }

            var parameters = new Dictionary<string, string>();
            if (t.Dispatch != null) parameters["Dispatch"] = t.Dispatch;
            if (t.FieldName != null) parameters["FieldName"] = t.FieldName;
            if (t.Term != null) parameters["Term"] = t.Term;
            if (t.Term2 != null) parameters["Term2"] = t.Term2;
            if (t.ClauseType != null) parameters["ClauseType"] = t.ClauseType;
            if (t.IsNegated) parameters["Negated"] = "true";
            if (t.Terms != null) parameters["Terms"] = t.Terms;
            if (t.EstimatedCardinality is > 0 and < long.MaxValue)
                parameters["EstimatedRows"] = t.EstimatedCardinality.ToString("N0");

            if (resultCounts != null && i < resultCounts.Length && resultCounts[i] > 0)
                parameters["Count"] = resultCounts[i].ToString();
            if (timings != null && i < timings.Length && timings[i] > 0)
                parameters["Ms"] = (timings[i] / tickFreq).ToString("F3");
            if (i == entryScanAt)
                parameters["EntryScan"] = "triggered";

            var node = new QueryInspectionNode(t.Name, parameters: parameters);
            if (orGroupNode != null) orGroupNode.Children.Add(node);
            else root.Children.Add(node);
        }

        if (orGroupNode != null) root.Children.Add(orGroupNode);

        // Vector/spatial nodes from executed match
        var matchInspection = executedMatch.Inspect();
        AppendVectorNodes(matchInspection, root);

        if (sortingWrapper != null)
        {
            var sortNode = sortingWrapper.Inspect();
            sortNode.Children.Clear();
            sortNode.Children.Add(root);
            return sortNode;
        }

        return root;
    }

    private static void AppendVectorNodes(QueryInspectionNode source, QueryInspectionNode target)
    {
        if (source.Operation.Contains("VectorSearch") || source.Operation.Contains("Spatial"))
        {
            target.Children.Add(source);
            return;
        }
        foreach (var child in source.Children)
            AppendVectorNodes(child, target);
    }

    /// <summary>Build an inspection template from plan ops + clauses. Created once, cached.</summary>
    private static InspectionOp[] BuildInspectionTemplate(QueryPlan plan)
    {
        var ops = plan.Ops;
        if (ops == null || ops.Length == 0) return [];

        var flatClauses = new List<ClauseInfo>();
        if (plan.QueryBuilderPlanState is List<ClauseInfo> clauses)
        {
            foreach (var clause in clauses)
            {
                if (clause.ClauseType == ClauseType.OrGroup && clause.OrSubClauses != null)
                    foreach (var sub in clause.OrSubClauses) flatClauses.Add(sub);
                else if (clause.ClauseType == ClauseType.AndGroup && clause.AndSubClauses != null)
                    foreach (var sub in clause.AndSubClauses) flatClauses.Add(sub);
                else if (clause.ClauseType is ClauseType.In or ClauseType.AllIn && clause.InTerms != null)
                {
                    // Create synthetic clauses for each IN term for display.
                    // PackedParamValue points to the contiguous range; offset per term.
                    var p = clause.PackedParamValue;
                    for (int t = 0; t < clause.InTerms.Count; t++)
                    {
                        flatClauses.Add(new ClauseInfo
                        {
                            FieldName = clause.FieldName,
                            PackedParamValue = new PackedParam(p.ValueType, p.Param1 + t),
                            ClauseType = clause.ClauseType,
                            IsNegated = clause.IsNegated
                        });
                    }
                }
                else
                    flatClauses.Add(clause);
            }
        }

        var result = new List<InspectionOp>();
        bool insideAndGroup = false;
        for (int i = 0; i < ops.Length; i++)
        {
            ref PlanOp op = ref ops[i];
            // OR chains like: (A AND B) OR (C AND D)
            // are compiled as:
            //   SwapBitmaps       <- save bitmap[0], start fresh in bitmap[2]
            //   Fill(A)           <- these ops form an AND-group
            //   And(B)            <-
            //   OrBitmaps         <- merge bitmap[2] into bitmap[0]
            //   SwapBitmaps       <- start next AND-group
            //   Fill(C)           <-
            //   And(D)            <-
            //   OrBitmaps         <- merge
            // SwapBitmaps marks the start of an AND-group within the OR chain.
            // OrBitmaps marks the end — it merges the group result back.
            if (op.Kind == PlanOpKind.SwapBitmaps) { insideAndGroup = true; continue; }
            if (op.Kind == PlanOpKind.OrBitmaps) { insideAndGroup = false; continue; }
            if (op.Kind is PlanOpKind.ClearBitmap or PlanOpKind.CheckEmpty or PlanOpKind.RepairAfterLazy or PlanOpKind.IterateInto) continue;

            var inspOp = new InspectionOp
            {
                Name = op.Kind switch
                {
                    PlanOpKind.FillFromPostings or PlanOpKind.DirectIterate => "Fill",
                    PlanOpKind.AndWithPostings => "AND",
                    PlanOpKind.OrWithPostings or PlanOpKind.LazyOrWithPostings => "OR",
                    PlanOpKind.AndNotWithPostings => "ANDNOT",
                    PlanOpKind.AndBitmaps => "AND-Bitmaps",
                    PlanOpKind.AndNotBitmaps => "ANDNOT-Bitmaps",
                    PlanOpKind.CheckAndMaybeEntryScan => "EntryScanCheck",
                    _ => op.Kind.ToString()
                },
                Dispatch = op.Dispatch switch { MatchDispatch.TermSource => "Term", MatchDispatch.TermsProvider => "MultiTerm", _ => "Match" },
                EstimatedCardinality = op.EstimatedCardinality,
                InsideAndGroup = insideAndGroup
            };

            if (op.ParamIndex >= 0 && op.ParamIndex < flatClauses.Count)
            {
                var clause = flatClauses[op.ParamIndex];
                if (clause != null)
                {
                    inspOp.FieldName = clause.FieldName;
                    inspOp.Term = FormatValueFromPlan(clause.PackedParamValue, plan);
                    inspOp.Term2 = FormatValue2FromPlan(clause.PackedParamValue, plan);
                    inspOp.IsNegated = clause.IsNegated;
                    if (clause.ClauseType != ClauseType.Equals) inspOp.ClauseType = clause.ClauseType.ToString();
                    if (clause.InTerms is { Count: > 0 })
                        inspOp.Terms = string.Join(", ", clause.InTerms.Take(5)) + (clause.InTerms.Count > 5 ? $" ... ({clause.InTerms.Count} total)" : "");
                }
            }

            result.Add(inspOp);
        }

        return result.ToArray();
    }
}
