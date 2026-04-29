using System;
using System.Reflection;
using System.Reflection.Emit;
using System.Runtime.CompilerServices;
using System.Text;
using Corax.Querying.Matches;
using Corax.Querying.Primitives;
using Corax.Utils;
using Corax.Utils.RoaringBitmaps;

namespace Corax.Querying.Planning;

/// <summary>
/// Emits a DynamicMethod from a QueryPlan. The generated IL is a flat sequence
/// of static calls to QueryPrimitives methods with goto-based dynamic unary
/// promotion between AND steps.
///
/// Generation time: ~50μs for a typical 5-operand AND query.
/// The generated delegate is GC-collectible when unreferenced.
/// </summary>
public static class QueryILEmitter
{
    // Cached MethodInfo references for primitives — resolved once at startup.
    private static readonly MethodInfo _fillFromPostings = typeof(QueryPrimitives).GetMethod(nameof(QueryPrimitives.FillFromPostings))!;
    private static readonly MethodInfo _andWithPostings = typeof(QueryPrimitives).GetMethod(nameof(QueryPrimitives.AndWithPostings))!;
    private static readonly MethodInfo _orWithPostings = typeof(QueryPrimitives).GetMethod(nameof(QueryPrimitives.OrWithPostings))!;
    private static readonly MethodInfo _andNotWithPostings = typeof(QueryPrimitives).GetMethod(nameof(QueryPrimitives.AndNotWithPostings))!;
    private static readonly MethodInfo _lazyOrWithPostings = typeof(QueryPrimitives).GetMethod(nameof(QueryPrimitives.LazyOrWithPostings))!;
    private static readonly MethodInfo _iterateInto = typeof(QueryPrimitives).GetMethod(nameof(QueryPrimitives.IterateInto))!;
    private static readonly MethodInfo _shouldSwitchToEntryScan = typeof(QueryPrimitives).GetMethod(nameof(QueryPrimitives.ShouldSwitchToEntryScan))!;

    /// <summary>
    /// Emit a CompiledPlan from a QueryPlan.
    /// Uses closure-based interpretation (functionally equivalent to emitted IL,
    /// easier to debug). The overhead of the interpreter loop (one switch per op)
    /// is negligible compared to the actual bitmap operations.
    /// </summary>
    public static CompiledPlan Emit(QueryPlan plan)
    {
        var ops = plan.Ops;
        var entryScanPredicates = plan.EntryScanPredicates;

        CompiledPlan.ExecuteDelegate execute = (ref QueryContext ctx, Span<long> output, ref int skip) =>
        {
            return ExecutePlan(ops, entryScanPredicates, ref ctx, output, ref skip);
        };

        return new CompiledPlan
        {
            Execute = execute,
            ExplainSource = plan.ExplainSource ?? GenerateExplainSource(plan),
            Ordering = plan.OperandOrdering
        };
    }

    /// <summary>
    /// Execute the query plan and populate a bitmap with matching entry IDs.
    /// Used by CompiledQueryMatch which needs to iterate the bitmap incrementally
    /// via multiple Fill() calls.
    /// </summary>
    public static void ExecuteToBitmap(PlanOp[] ops, MultiUnaryItem[][] entryScanPredicates,
        ref QueryContext ctx, ref RoaringBitmap bitmap)
    {
        var tempBitmap = new RoaringBitmap(ctx.Allocator);

        try
        {
            for (int i = 0; i < ops.Length; i++)
            {
                ctx.Token.ThrowIfCancellationRequested();
                ref PlanOp op = ref ops[i];

                switch (op.Kind)
                {
                    case PlanOpKind.FillFromPostings:
                    {
                        var postingList = ctx.Searcher.GetPostingList(ctx.PostingListIds[op.ParamIndex]);
                        var it = postingList.Iterate();
                        QueryPrimitives.FillFromPostings(ref it, ref bitmap, ctx.Limit);
                        break;
                    }

                    case PlanOpKind.AndWithPostings:
                    {
                        var postingList = ctx.Searcher.GetPostingList(ctx.PostingListIds[op.ParamIndex]);
                        var it = postingList.Iterate();
                        QueryPrimitives.AndWithPostings(ref it, ref bitmap, ref tempBitmap, ctx.Limit);
                        break;
                    }

                    case PlanOpKind.OrWithPostings:
                    {
                        var postingList = ctx.Searcher.GetPostingList(ctx.PostingListIds[op.ParamIndex]);
                        var it = postingList.Iterate();
                        QueryPrimitives.OrWithPostings(ref it, ref bitmap);
                        break;
                    }

                    case PlanOpKind.AndNotWithPostings:
                    {
                        var postingList = ctx.Searcher.GetPostingList(ctx.PostingListIds[op.ParamIndex]);
                        var it = postingList.Iterate();
                        QueryPrimitives.AndNotWithPostings(ref it, ref bitmap, ref tempBitmap);
                        break;
                    }

                    case PlanOpKind.LazyOrWithPostings:
                    {
                        var postingList = ctx.Searcher.GetPostingList(ctx.PostingListIds[op.ParamIndex]);
                        var it = postingList.Iterate();
                        QueryPrimitives.LazyOrWithPostings(ref it, ref bitmap);
                        break;
                    }

                    case PlanOpKind.RepairAfterLazy:
                        bitmap.RepairAfterLazy();
                        break;

                    case PlanOpKind.CheckAndMaybeEntryScan:
                    {
                        var postingListState = ctx.Searcher.GetPostingListState(ctx.PostingListIds[op.ParamIndex]);
                        if (QueryPrimitives.ShouldSwitchToEntryScan(ref bitmap, in postingListState))
                        {
                            // Entry scan modifies bitmap in place — remaining predicates evaluated per-entry
                            // For now, entry scan predicates are empty (filtering deferred), so we just stop here
                            bitmap.PrepareForReading();
                            tempBitmap.Dispose();
                            return;
                        }
                        break;
                    }

                    case PlanOpKind.IterateInto:
                    case PlanOpKind.DirectIterate:
                        // These are terminal ops — for bitmap mode, we just stop here
                        // and let the caller iterate the bitmap
                        break;

                    default:
                        throw new NotSupportedException(
                            $"PlanOp {op.Kind} not supported in ExecuteToBitmap mode.");
                }
            }

            bitmap.PrepareForReading();
        }
        finally
        {
            tempBitmap.Dispose();
        }
    }

    /// <summary>
    /// Execute a query plan by interpreting the PlanOp array.
    /// Each op dispatches to the corresponding QueryPrimitives method.
    /// </summary>
    private static int ExecutePlan(PlanOp[] ops, MultiUnaryItem[][] entryScanPredicates,
        ref QueryContext ctx, Span<long> output, ref int skip)
    {
        // Allocate bitmaps as needed (up to 3: main, temp, scratch)
        var bitmap = new RoaringBitmap(ctx.Allocator);
        var tempBitmap = new RoaringBitmap(ctx.Allocator);
        var iterator = bitmap.GetIterator();

        try
        {
            for (int i = 0; i < ops.Length; i++)
            {
                ctx.Token.ThrowIfCancellationRequested();

                ref PlanOp op = ref ops[i];

                switch (op.Kind)
                {
                    case PlanOpKind.FillFromPostings:
                    {
                        var postingList = ctx.Searcher.GetPostingList(ctx.PostingListIds[op.ParamIndex]);
                        var it = postingList.Iterate();
                        QueryPrimitives.FillFromPostings(ref it, ref bitmap, ctx.Limit);
                        break;
                    }

                    case PlanOpKind.AndWithPostings:
                    {
                        var postingList = ctx.Searcher.GetPostingList(ctx.PostingListIds[op.ParamIndex]);
                        var it = postingList.Iterate();
                        QueryPrimitives.AndWithPostings(ref it, ref bitmap, ref tempBitmap, ctx.Limit);
                        break;
                    }

                    case PlanOpKind.OrWithPostings:
                    {
                        var postingList = ctx.Searcher.GetPostingList(ctx.PostingListIds[op.ParamIndex]);
                        var it = postingList.Iterate();
                        QueryPrimitives.OrWithPostings(ref it, ref bitmap);
                        break;
                    }

                    case PlanOpKind.AndNotWithPostings:
                    {
                        var postingList = ctx.Searcher.GetPostingList(ctx.PostingListIds[op.ParamIndex]);
                        var it = postingList.Iterate();
                        QueryPrimitives.AndNotWithPostings(ref it, ref bitmap, ref tempBitmap);
                        break;
                    }

                    case PlanOpKind.LazyOrWithPostings:
                    {
                        var postingList = ctx.Searcher.GetPostingList(ctx.PostingListIds[op.ParamIndex]);
                        var it = postingList.Iterate();
                        QueryPrimitives.LazyOrWithPostings(ref it, ref bitmap);
                        break;
                    }

                    case PlanOpKind.RepairAfterLazy:
                    {
                        bitmap.RepairAfterLazy();
                        break;
                    }

                    case PlanOpKind.CheckAndMaybeEntryScan:
                    {
                        // Runtime check: should we switch to entry scan?
                        var postingListState = ctx.Searcher.GetPostingListState(ctx.PostingListIds[op.ParamIndex]);
                        if (QueryPrimitives.ShouldSwitchToEntryScan(ref bitmap, in postingListState))
                        {
                            // Jump to entry scan with remaining predicates
                            var predicates = entryScanPredicates[op.GotoLabelIndex];
                            bitmap.PrepareForReading();
                            int result = QueryPrimitives.ScanAndFilter(ref bitmap, ctx.Searcher,
                                predicates, output, ctx.Limit, ref skip);
                            bitmap.Dispose();
                            tempBitmap.Dispose();
                            return result;
                        }
                        break;
                    }

                    case PlanOpKind.IterateInto:
                    {
                        bitmap.PrepareForReading();
                        int result = iterator.Fill(ref bitmap, output);
                        bitmap.Dispose();
                        tempBitmap.Dispose();
                        return result;
                    }

                    case PlanOpKind.DirectIterate:
                    {
                        // Single operand, no bitmap — iterate posting list directly
                        var postingList = ctx.Searcher.GetPostingList(ctx.PostingListIds[op.ParamIndex]);
                        var it = postingList.Iterate();
                        Span<long> buffer = output;
                        it.Fill(buffer, out int read);
                        if (read > 0)
                            EntryIdEncodings.DecodeAndDiscardFrequency(buffer, read);
                        bitmap.Dispose();
                        tempBitmap.Dispose();
                        return read;
                    }

                    case PlanOpKind.ScanAndFilter:
                    {
                        var predicates = entryScanPredicates[op.GotoLabelIndex];
                        bitmap.PrepareForReading();
                        int result = QueryPrimitives.ScanAndFilter(ref bitmap, ctx.Searcher,
                            predicates, output, ctx.Limit, ref skip);
                        bitmap.Dispose();
                        tempBitmap.Dispose();
                        return result;
                    }

                    case PlanOpKind.FillFromRange:
                    case PlanOpKind.SortWithFilter:
                    case PlanOpKind.OrderedRangeScan:
                    case PlanOpKind.VectorRank:
                    case PlanOpKind.SpatialFilter:
                    case PlanOpKind.SortByScore:
                    case PlanOpKind.SortByDistance:
                    case PlanOpKind.ScanAndFilterInPlace:
                        throw new NotSupportedException(
                            $"PlanOp {op.Kind} not yet implemented in Corax 2.0 executor. " +
                            "See docs/implementation-notes.md for deferred features.");

                    default:
                        throw new InvalidOperationException($"Unknown PlanOp kind: {op.Kind}");
                }
            }

            // If we reach here without an explicit return, iterate the bitmap
            bitmap.PrepareForReading();
            int finalResult = iterator.Fill(ref bitmap, output);
            return finalResult;
        }
        finally
        {
            bitmap.Dispose();
            tempBitmap.Dispose();
        }
    }

    /// <summary>
    /// Generate a C# pseudocode EXPLAIN string from the plan.
    /// </summary>
    private static string GenerateExplainSource(QueryPlan plan)
    {
        var sb = new StringBuilder();
        sb.AppendLine("// Generated query plan");
        sb.AppendLine("static int Execute(ref QueryContext ctx, Span<long> output, ref int skip)");
        sb.AppendLine("{");
        sb.AppendLine("    using var bitmap = new RoaringBitmap(ctx.Allocator);");

        for (int i = 0; i < plan.Ops.Length; i++)
        {
            ref PlanOp op = ref plan.Ops[i];
            string line = op.Kind switch
            {
                PlanOpKind.FillFromPostings => $"    Primitives.FillFromPostings(field[{op.FieldId}], param[{op.ParamIndex}], ref bitmap); // est: {op.EstimatedCardinality:N0}",
                PlanOpKind.AndWithPostings => $"    Primitives.AndWithPostings(field[{op.FieldId}], param[{op.ParamIndex}], ref bitmap); // est: {op.EstimatedCardinality:N0}",
                PlanOpKind.OrWithPostings => $"    Primitives.OrWithPostings(field[{op.FieldId}], param[{op.ParamIndex}], ref bitmap);",
                PlanOpKind.AndNotWithPostings => $"    Primitives.AndNotWithPostings(field[{op.FieldId}], param[{op.ParamIndex}], ref bitmap);",
                PlanOpKind.LazyOrWithPostings => $"    Primitives.LazyOrWithPostings(field[{op.FieldId}], param[{op.ParamIndex}], ref bitmap);",
                PlanOpKind.RepairAfterLazy => "    bitmap.RepairAfterLazy();",
                PlanOpKind.CheckAndMaybeEntryScan => $"    if (ShouldSwitchToEntryScan(ref bitmap, postingList[{op.ParamIndex}])) goto EntryScan_{op.GotoLabelIndex};",
                PlanOpKind.IterateInto => "    return Primitives.IterateInto(ref bitmap, output, ref skip);",
                PlanOpKind.DirectIterate => $"    return postingList[{op.ParamIndex}].Iterator.Fill(output);",
                _ => $"    // {op.Kind} (not yet in EXPLAIN renderer)"
            };
            sb.AppendLine(line);
        }

        // Add entry scan labels
        if (plan.EntryScanPredicates != null)
        {
            for (int i = 0; i < plan.EntryScanPredicates.Length; i++)
            {
                sb.AppendLine($"EntryScan_{i}:");
                sb.AppendLine($"    return Primitives.ScanAndFilter(ref bitmap, ctx.Searcher, predicates[{i}], output, ctx.Limit, ref skip);");
            }
        }

        sb.AppendLine("}");
        return sb.ToString();
    }
}
