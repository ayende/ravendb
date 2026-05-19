using System;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;
using System.Threading;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Primitives;
using Corax.Utils;
using Voron;
using Voron.Data.CompactTrees;
using Voron.Data.RoaringBitmaps;

namespace Corax.Querying.Planning;

public static class QueryIlEmitter
{
    public delegate void CompiledExecuteDelegate(CompiledQueryMatch ctx);

    // Span<long>
    private static readonly ConstructorInfo SpanCtor = typeof(Span<long>).GetConstructor([typeof(void*), typeof(int)])!;

    public static CompiledExecuteDelegate EmitDelegate(QueryExecution plan, out string explainSource, out string csharpSource, bool emitTimings = true)
    {
        var ops = plan.Ops;
        if (ops == null || ops.Length == 0)
        {
            explainSource = "// Empty plan";
            csharpSource = "// Empty plan.\n";
            return EmptyExecute;
        }

        // EXPLAIN pseudocode built alongside IL emission.
        var explain = new StringBuilder();
        explain.AppendLine("// Compiled query — pseudocode mirroring the emitted IL.");

        // C# source built alongside IL emission so the two backends cannot drift.
        var cs = new StringBuilder();
        cs.AppendLine("static void CompiledQuery(CompiledQueryMatch ctx)");
        cs.AppendLine("{");
        EmitCSharpBoundsCheckPreamble(cs, ops);
        // Runtime cursor over ctx.ResolvedMatches / ctx.TermSources / ctx.TermsProviders.
        // Each consuming op uses cursor as the slot index and then advances it; OrRange /
        // AndRange advance by ctx.InRangeCounts[rangeIdx].
        cs.AppendLine("    int cursor = 0;");

        var dm = new DynamicMethod(
            "CompiledQuery",
            typeof(void),
            [typeof(CompiledQueryMatch)],
            typeof(CompiledQueryMatch).Module,
            skipVisibility: true)
            {
                InitLocals = false
            };

        var il = dm.GetILGenerator();

        // Locals
        var bufferLocal = il.DeclareLocal(typeof(Span<long>));    // 0: Fill buffer
        var readLocal = il.DeclareLocal(typeof(int));              // 1: read count
        var startTickLocal = il.DeclareLocal(typeof(long));        // 2: timing start tick
        // Runtime cursor over the resolved-source array slot space (ctx.ResolvedMatches /
        // ctx.TermSources / ctx.TermsProviders, depending on op.Dispatch). Each consuming
        // op uses cursor as the slot index and then advances it; OrRange / AndRange advance
        // by ctx.InRangeCounts[rangeIdx]. Replaces the previously-baked op.ParamIndex so the
        // plan stays structural while slot indices follow the runtime IN expansion.
        var cursorVar = il.DeclareLocal(typeof(int));              // 3: runtime slot cursor

        // Top-level labels: keep IL-only Label + manual C# label emission so the C# output
        // continues to use literal "Done:" / "EntryScan:" names (rather than the numbered
        // form DualEmit.DefineLabelPair would produce). QueryILEmitter never pushes to a
        // CsStack, so the MarkLabel stack-balance assert is not load-bearing here.
        var doneLabel = il.DefineLabel();
        var entryScanLabel = il.DefineLabel();
        bool hasEntryScan = false;
        bool needsLazyRepair = false;
        int entryScanOpIndex = -1;

        // stackalloc long[FillBufferSize]
        IlEmitterShared.EmitLdcI4(il, QueryPrimitives.FillBufferSize);
        il.Emit(OpCodes.Conv_U);
        il.Emit(OpCodes.Sizeof, typeof(long));
        il.Emit(OpCodes.Mul_Ovf_Un);
        il.Emit(OpCodes.Localloc);
        IlEmitterShared.EmitLdcI4(il, QueryPrimitives.FillBufferSize);
        il.Emit(OpCodes.Newobj, SpanCtor);
        il.Emit(OpCodes.Stloc, bufferLocal);

        // Initialize cursor = 0 (DynamicMethod has InitLocals = false).
        il.Emit(OpCodes.Ldc_I4_0);
        il.Emit(OpCodes.Stloc, cursorVar);

        // Bounds-check elimination preamble: touch the max bitmap index so the JIT
        // can hoist bitmap-slot length checks. Source-array touches are skipped —
        // the cursor is runtime-driven, so we have no compile-time upper bound.
        EmitBoundsCheckPreamble(il, ops);

        for (int i = 0; i < ops.Length; i++)
        {
            ref PlanOp op = ref ops[i];

            // Timing: record start tick before each op (skipped for untimed delegate)
            if (emitTimings)
            {
                EmitTimingStart(il, startTickLocal);
                cs.AppendLine($"    long startTick_{i} = Stopwatch.GetTimestamp();");
            }

            // Resolve dispatch once per op — used by IL, EXPLAIN, and C# emission.
            var (src, fillMethod, andMethod, orMethod, andNotMethod) = op.Dispatch switch
            {
                MatchDispatch.PostingList => (
                    $"ctx.TermSources[{op.ParamIndex}]",
                    IlEmitterShared.CtxFillFromPostingSource, IlEmitterShared.CtxAndFromPostingSource, IlEmitterShared.CtxOrFillFromPostingSource, IlEmitterShared.CtxAndNotFromPostingSource),
                MatchDispatch.TreeScan => (
                    $"ctx.TermsProviders[{op.ParamIndex}]",
                    IlEmitterShared.CtxFillFromTreeScan, IlEmitterShared.CtxAndFromTreeScan, IlEmitterShared.CtxOrFillFromTreeScan, IlEmitterShared.CtxAndNotFromTreeScan),
                _ => (
                    $"ctx.ResolvedMatches[{op.ParamIndex}]",
                    IlEmitterShared.CtxOrWithMatch, IlEmitterShared.CtxAndFromMatch, (MethodInfo)IlEmitterShared.CtxOrWithMatchSlot, IlEmitterShared.CtxAndNotFromMatch)
            };

            var (fillName, andName, orName, andNotName) = op.Dispatch switch
            {
                MatchDispatch.PostingList => (
                    "QueryPrimitives.CtxFillFromPostingSource",
                    "QueryPrimitives.CtxAndFromPostingSource",
                    "QueryPrimitives.CtxOrFillFromPostingSource",
                    "QueryPrimitives.CtxAndNotFromPostingSource"),
                MatchDispatch.TreeScan => (
                    "QueryPrimitives.CtxFillFromTreeScan",
                    "QueryPrimitives.CtxAndFromTreeScan",
                    "QueryPrimitives.CtxOrFillFromTreeScan",
                    "QueryPrimitives.CtxAndNotFromTreeScan"),
                _ => (
                    "QueryPrimitives.CtxOrWithMatch",
                    "QueryPrimitives.CtxAndFromMatch",
                    "QueryPrimitives.CtxOrWithMatchSlot",
                    "QueryPrimitives.CtxAndNotFromMatch")
            };

            switch (op.Kind)
            {
                case PlanOpKind.FillFromPostings:
                case PlanOpKind.DirectIterate:
                    EmitCancellationCheck(il);
                    il.Emit(OpCodes.Ldarg_0);
                    il.Emit(OpCodes.Ldloc, cursorVar);
                    il.Emit(OpCodes.Call, fillMethod);
                    EmitCursorAdvance(il, cursorVar);
                    explain.AppendLine($"QueryPrimitives.CtxFill(ctx, paramIndex: cursor);  // bitmap[0] ← {src.Replace($"[{op.ParamIndex}]", "[cursor]")}; cursor++");
                    cs.AppendLine("    ctx.Token.ThrowIfCancellationRequested();");
                    cs.AppendLine($"    {fillName}(ctx, cursor); cursor++;");
                    break;

                case PlanOpKind.FillAllEntries:
                    EmitCancellationCheck(il);
                    il.Emit(OpCodes.Ldarg_0);
                    il.Emit(OpCodes.Call, IlEmitterShared.CtxFillAllEntries);
                    explain.AppendLine("QueryPrimitives.CtxFillAllEntries(ctx);  // bitmap[0] ← AllEntries");
                    cs.AppendLine("    ctx.Token.ThrowIfCancellationRequested();");
                    cs.AppendLine("    QueryPrimitives.CtxFillAllEntries(ctx);");
                    break;

                case PlanOpKind.AndWithPostings:
                    EmitCancellationCheck(il);
                    il.Emit(OpCodes.Ldarg_0);
                    il.Emit(OpCodes.Ldloc, cursorVar);
                    il.Emit(OpCodes.Call, andMethod);
                    EmitCursorAdvance(il, cursorVar);
                    explain.AppendLine($"QueryPrimitives.CtxAnd(ctx, paramIndex: cursor);  // bitmap[0] &= {src.Replace($"[{op.ParamIndex}]", "[cursor]")}; cursor++");
                    cs.AppendLine("    ctx.Token.ThrowIfCancellationRequested();");
                    cs.AppendLine($"    {andName}(ctx, cursor); cursor++;");
                    if (!op.SkipEarlyExit)
                    {
                        EmitLoadBitmapRef(il, 0);
                        il.Emit(OpCodes.Call, IlEmitterShared.IsEmptyGetter);
                        il.Emit(OpCodes.Brtrue, doneLabel);
                        explain.AppendLine("if (bitmap[0].IsEmpty) goto Done;");
                        cs.AppendLine("    if (ctx.Bitmaps[0].IsEmpty) goto Done;");
                    }
                    break;

                case PlanOpKind.OrWithPostings:
                case PlanOpKind.LazyOrWithPostings:
                    EmitCancellationCheck(il);
                    il.Emit(OpCodes.Ldarg_0);
                    il.Emit(OpCodes.Ldloc, cursorVar);
                    il.Emit(OpCodes.Ldc_I4, op.BitmapLocal);
                    il.Emit(OpCodes.Call, orMethod);
                    EmitCursorAdvance(il, cursorVar);
                    explain.AppendLine($"QueryPrimitives.CtxOr(ctx, paramIndex: cursor, bitmapSlot: {op.BitmapLocal});  // bitmap[{op.BitmapLocal}] |= {src.Replace($"[{op.ParamIndex}]", "[cursor]")}; cursor++");
                    cs.AppendLine("    ctx.Token.ThrowIfCancellationRequested();");
                    cs.AppendLine($"    {orName}(ctx, cursor, {op.BitmapLocal}); cursor++;");
                    if (op.BitmapLocal == 0)
                    {
                        EmitLoadBitmapRef(il, 0);
                        il.Emit(OpCodes.Call, IlEmitterShared.CountGetter);
                        il.Emit(OpCodes.Conv_I8);
                        EmitLoadLimit(il);
                        il.Emit(OpCodes.Bge, doneLabel);
                        explain.AppendLine("if (bitmap[0].Count >= limit) goto Done;");
                        cs.AppendLine("    if ((long)ctx.Bitmaps[0].Count >= ctx.Limit) goto Done;");
                    }
                    break;

                case PlanOpKind.ClearBitmap:
                    EmitLoadBitmapRef(il, op.BitmapLocal);
                    il.Emit(OpCodes.Call, IlEmitterShared.Clear);
                    explain.AppendLine($"bitmap[{op.BitmapLocal}].Clear();");
                    cs.AppendLine($"    ctx.Bitmaps[{op.BitmapLocal}].Clear();");
                    break;

                case PlanOpKind.AndBitmaps:
                    EmitLoadBitmapRef(il, op.BitmapLocal);
                    EmitLoadBitmapRef(il, op.ParamIndex2);
                    il.Emit(OpCodes.Call, IlEmitterShared.AndWith);
                    explain.AppendLine($"bitmap[{op.BitmapLocal}].AndWith(bitmap[{op.ParamIndex2}]);  // bitmap[{op.BitmapLocal}] &= bitmap[{op.ParamIndex2}]");
                    cs.AppendLine($"    ctx.Bitmaps[{op.BitmapLocal}].AndWith(ref ctx.Bitmaps[{op.ParamIndex2}]);");
                    break;

                case PlanOpKind.AndNotBitmaps:
                    EmitLoadBitmapRef(il, op.BitmapLocal);
                    EmitLoadBitmapRef(il, op.ParamIndex2);
                    il.Emit(OpCodes.Call, IlEmitterShared.AndNotWith);
                    explain.AppendLine($"bitmap[{op.BitmapLocal}].AndNotWith(bitmap[{op.ParamIndex2}]);  // bitmap[{op.BitmapLocal}] &= ~bitmap[{op.ParamIndex2}]");
                    cs.AppendLine($"    ctx.Bitmaps[{op.BitmapLocal}].AndNotWith(ref ctx.Bitmaps[{op.ParamIndex2}]);");
                    break;

                case PlanOpKind.OrBitmaps:
                    EmitLoadBitmapRef(il, op.BitmapLocal);
                    EmitLoadBitmapRef(il, op.ParamIndex2);
                    il.Emit(OpCodes.Call, IlEmitterShared.LazyOrWith);
                    needsLazyRepair = true;
                    explain.AppendLine($"bitmap[{op.BitmapLocal}].LazyOrWith(bitmap[{op.ParamIndex2}]);  // lazy OR (cardinality repaired later)");
                    cs.AppendLine($"    ctx.Bitmaps[{op.BitmapLocal}].LazyOrWith(ref ctx.Bitmaps[{op.ParamIndex2}]);");
                    break;

                case PlanOpKind.SwapBitmaps:
                    EmitLoadBitmapRef(il, op.BitmapLocal);
                    EmitLoadBitmapRef(il, op.ParamIndex2);
                    il.Emit(OpCodes.Call, IlEmitterShared.SwapContents);
                    explain.AppendLine($"bitmap[{op.BitmapLocal}].SwapContents(bitmap[{op.ParamIndex2}]);");
                    cs.AppendLine($"    ctx.Bitmaps[{op.BitmapLocal}].SwapContents(ref ctx.Bitmaps[{op.ParamIndex2}]);");
                    break;

                case PlanOpKind.CheckEmpty:
                    EmitLoadBitmapRef(il, op.BitmapLocal);
                    il.Emit(OpCodes.Call, IlEmitterShared.IsEmptyGetter);
                    il.Emit(OpCodes.Brtrue, doneLabel);
                    explain.AppendLine($"if (bitmap[{op.BitmapLocal}].IsEmpty) goto Done;");
                    cs.AppendLine($"    if (ctx.Bitmaps[{op.BitmapLocal}].IsEmpty) goto Done;");
                    break;

                case PlanOpKind.AndNotWithPostings:
                    EmitCancellationCheck(il);
                    il.Emit(OpCodes.Ldarg_0);
                    il.Emit(OpCodes.Ldloc, cursorVar);
                    il.Emit(OpCodes.Call, andNotMethod);
                    EmitCursorAdvance(il, cursorVar);
                    explain.AppendLine($"QueryPrimitives.CtxAndNot(ctx, paramIndex: cursor);  // bitmap[0] &= ~{src.Replace($"[{op.ParamIndex}]", "[cursor]")}; cursor++");
                    cs.AppendLine("    ctx.Token.ThrowIfCancellationRequested();");
                    cs.AppendLine($"    {andNotName}(ctx, cursor); cursor++;");
                    break;

                case PlanOpKind.RepairAfterLazy:
                    EmitLoadBitmapRef(il, 0);
                    il.Emit(OpCodes.Call, IlEmitterShared.RepairAfterLazy);
                    explain.AppendLine("bitmap[0].RepairAfterLazy();  // fix lazy cardinalities from LazyOrWith");
                    cs.AppendLine("    ctx.Bitmaps[0].RepairAfterLazy();");
                    break;

                case PlanOpKind.CheckAndMaybeEntryScan:
                {
                    hasEntryScan = true;
                    entryScanOpIndex = i;

                    EmitLoadBitmapRef(il, 0);
                    il.Emit(OpCodes.Call, IlEmitterShared.CountGetter);
                    il.Emit(OpCodes.Conv_I8);
                    // Compare against the match the next consumer op will read (cursor, not cursor-1):
                    // CheckAndMaybeEntryScan is emitted BEFORE the next AND step, so cursor still
                    // points at that step's slot.
                    EmitLoadMatchFromCursor(il, cursorVar);
                    il.Emit(OpCodes.Callvirt, IlEmitterShared.MatchCountGetter);
                    il.Emit(OpCodes.Call, IlEmitterShared.ShouldSwitchToEntryScan);
                    il.Emit(OpCodes.Brtrue, entryScanLabel);
                    explain.AppendLine($"if (QueryPrimitives.ShouldSwitchToEntryScan(bitmap[0].Count, ctx.ResolvedMatches[cursor].Count))");
                    explain.AppendLine($"    goto EntryScan;  // bitmap[0] is small, walk entries instead of decoding posting list");
                    cs.AppendLine("    if (QueryPrimitives.ShouldSwitchToEntryScan((long)ctx.Bitmaps[0].Count, ctx.ResolvedMatches[cursor].Count))");
                    cs.AppendLine("        goto EntryScan;");
                    break;
                }

                case PlanOpKind.OrRange:
                {
                    int rangeIdx = op.ParamIndex2;
                    var loopVar = il.DeclareLocal(typeof(int));
                    var endVar = il.DeclareLocal(typeof(int));
                    var loopCheck = il.DefineLabel();
                    var loopBody = il.DefineLabel();

                    // endVar = cursor + ctx.InRangeCounts[rangeIdx]
                    il.Emit(OpCodes.Ldloc, cursorVar);
                    il.Emit(OpCodes.Ldarg_0);
                    il.Emit(OpCodes.Ldfld, IlEmitterShared.CtxInRangeCounts);
                    IlEmitterShared.EmitLdcI4(il, rangeIdx);
                    il.Emit(OpCodes.Ldelem_I4);
                    il.Emit(OpCodes.Add);
                    il.Emit(OpCodes.Stloc, endVar);

                    EmitRangeEndIndexTouch(il, op.Dispatch, cursorVar, endVar);

                    il.Emit(OpCodes.Ldloc, cursorVar);
                    il.Emit(OpCodes.Stloc, loopVar);
                    il.Emit(OpCodes.Br, loopCheck);

                    il.MarkLabel(loopBody);
                    EmitCancellationCheck(il);
                    il.Emit(OpCodes.Ldarg_0);
                    il.Emit(OpCodes.Ldloc, loopVar);
                    il.Emit(OpCodes.Ldc_I4, op.BitmapLocal);
                    il.Emit(OpCodes.Call, orMethod);

                    il.Emit(OpCodes.Ldloc, loopVar);
                    il.Emit(OpCodes.Ldc_I4_1);
                    il.Emit(OpCodes.Add);
                    il.Emit(OpCodes.Stloc, loopVar);

                    il.MarkLabel(loopCheck);
                    il.Emit(OpCodes.Ldloc, loopVar);
                    il.Emit(OpCodes.Ldloc, endVar);
                    il.Emit(OpCodes.Blt, loopBody);

                    // cursor = endVar (advance past all consumed slots in this range)
                    il.Emit(OpCodes.Ldloc, endVar);
                    il.Emit(OpCodes.Stloc, cursorVar);

                    explain.AppendLine($"// Range OR: ctx.InRangeCounts[{rangeIdx}] terms starting at cursor");
                    explain.AppendLine($"for (int j = cursor; j < cursor + ctx.InRangeCounts[{rangeIdx}]; j++)");
                    explain.AppendLine($"    QueryPrimitives.CtxOr(ctx, j, bitmapSlot: {op.BitmapLocal});  // bitmap[{op.BitmapLocal}] |= ResolvedMatches[j]");
                    explain.AppendLine($"cursor += ctx.InRangeCounts[{rangeIdx}];");

                    cs.AppendLine("    {");
                    cs.AppendLine($"        int end_{i} = cursor + ctx.InRangeCounts[{rangeIdx}];");
                    cs.AppendLine($"        for (int j_{i} = cursor; j_{i} < end_{i}; j_{i}++) {{ ctx.Token.ThrowIfCancellationRequested(); {orName}(ctx, j_{i}, {op.BitmapLocal}); }}");
                    cs.AppendLine($"        cursor = end_{i};");
                    cs.AppendLine("    }");
                    break;
                }

                case PlanOpKind.AndRange:
                {
                    int rangeIdx = op.ParamIndex2;
                    var loopVar = il.DeclareLocal(typeof(int));
                    var endVar = il.DeclareLocal(typeof(int));
                    var loopCheck = il.DefineLabel();
                    var loopBody = il.DefineLabel();

                    // endVar = cursor + ctx.InRangeCounts[rangeIdx]
                    il.Emit(OpCodes.Ldloc, cursorVar);
                    il.Emit(OpCodes.Ldarg_0);
                    il.Emit(OpCodes.Ldfld, IlEmitterShared.CtxInRangeCounts);
                    IlEmitterShared.EmitLdcI4(il, rangeIdx);
                    il.Emit(OpCodes.Ldelem_I4);
                    il.Emit(OpCodes.Add);
                    il.Emit(OpCodes.Stloc, endVar);

                    EmitRangeEndIndexTouch(il, op.Dispatch, cursorVar, endVar);

                    il.Emit(OpCodes.Ldloc, cursorVar);
                    il.Emit(OpCodes.Stloc, loopVar);
                    il.Emit(OpCodes.Br, loopCheck);

                    il.MarkLabel(loopBody);
                    EmitCancellationCheck(il);
                    il.Emit(OpCodes.Ldarg_0);
                    il.Emit(OpCodes.Ldloc, loopVar);
                    il.Emit(OpCodes.Call, andMethod);

                    if (!op.SkipEarlyExit)
                    {
                        EmitLoadBitmapRef(il, 0);
                        il.Emit(OpCodes.Call, IlEmitterShared.IsEmptyGetter);
                        il.Emit(OpCodes.Brtrue, doneLabel);
                    }

                    il.Emit(OpCodes.Ldloc, loopVar);
                    il.Emit(OpCodes.Ldc_I4_1);
                    il.Emit(OpCodes.Add);
                    il.Emit(OpCodes.Stloc, loopVar);

                    il.MarkLabel(loopCheck);
                    il.Emit(OpCodes.Ldloc, loopVar);
                    il.Emit(OpCodes.Ldloc, endVar);
                    il.Emit(OpCodes.Blt, loopBody);

                    // cursor = endVar (advance past all consumed slots in this range)
                    il.Emit(OpCodes.Ldloc, endVar);
                    il.Emit(OpCodes.Stloc, cursorVar);

                    explain.AppendLine($"// Range AND: ctx.InRangeCounts[{rangeIdx}] terms starting at cursor");
                    explain.AppendLine($"for (int j = cursor; j < cursor + ctx.InRangeCounts[{rangeIdx}]; j++)");
                    explain.AppendLine($"{{");
                    explain.AppendLine($"    QueryPrimitives.CtxAnd(ctx, j);  // bitmap[0] &= ResolvedMatches[j]");
                    if (!op.SkipEarlyExit)
                    {
                        explain.AppendLine($"    if (bitmap[0].IsEmpty) goto Done;");
                    }
                    explain.AppendLine($"}}");
                    explain.AppendLine($"cursor += ctx.InRangeCounts[{rangeIdx}];");

                    cs.AppendLine("    {");
                    cs.AppendLine($"        int end_{i} = cursor + ctx.InRangeCounts[{rangeIdx}];");
                    cs.AppendLine($"        for (int j_{i} = cursor; j_{i} < end_{i}; j_{i}++) {{ ctx.Token.ThrowIfCancellationRequested(); {andName}(ctx, j_{i});");
                    if (!op.SkipEarlyExit)
                        cs.AppendLine($"            if (ctx.Bitmaps[0].IsEmpty) goto Done;");
                    cs.AppendLine("        }");
                    cs.AppendLine($"        cursor = end_{i};");
                    cs.AppendLine("    }");
                    break;
                }

                case PlanOpKind.IterateInto:
                    il.Emit(OpCodes.Br, doneLabel);
                    explain.AppendLine("goto Done;  // result ready in bitmap[0]");
                    cs.AppendLine("    goto Done;");
                    break;
            }

            // Timing: record elapsed time and result count after each op (skipped for untimed delegate)
            if (emitTimings)
            {
                EmitTimingEnd(il, i, startTickLocal);
                cs.AppendLine($"    CompiledQueryHelper.RecordTiming(ctx, {i}, startTick_{i});");
                cs.AppendLine($"    CompiledQueryHelper.RecordResultCount(ctx, {i});");
                cs.AppendLine();
            }
        }

        il.MarkLabel(doneLabel);
        cs.AppendLine("Done:");
        if (needsLazyRepair)
        {
            EmitLoadBitmapRef(il, 0);
            il.Emit(OpCodes.Call, IlEmitterShared.RepairAfterLazy);
            explain.AppendLine("bitmap[0].RepairAfterLazy();  // fix lazy cardinalities before returning");
            cs.AppendLine("    ctx.Bitmaps[0].RepairAfterLazy();");
        }
        il.Emit(OpCodes.Ret);
        cs.AppendLine("    return;");

        if (hasEntryScan)
        {
            il.MarkLabel(entryScanLabel);
            explain.AppendLine();
            explain.AppendLine("EntryScan:");
            explain.AppendLine("// Per-entry scan path: walk bitmap[0] entries, check residual predicates,");
            explain.AppendLine("// compact survivors into bitmap[1], then swap bitmap[0] ← bitmap[1].");

            cs.AppendLine();
            cs.AppendLine("EntryScan:");
            cs.AppendLine($"    ctx.EntryScanTakenAtOp = {entryScanOpIndex};");
            cs.AppendLine("    CompiledQueryHelper.RunEntryScan(ctx, ref ctx.Bitmaps[0], ref ctx.Bitmaps[1]);");
            cs.AppendLine("    ctx.Bitmaps[0].SwapContents(ref ctx.Bitmaps[1]);");
            cs.AppendLine("    ctx.Bitmaps[1].Clear();");
            cs.AppendLine("    return;");

            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Ldc_I4, entryScanOpIndex);
            il.Emit(OpCodes.Stfld, IlEmitterShared.CtxEntryScanTakenAtOp);

            il.Emit(OpCodes.Ldarg_0);
            EmitLoadBitmapRef(il, 0);
            EmitLoadBitmapRef(il, 1);
            il.Emit(OpCodes.Call, IlEmitterShared.RunEntryScanMethod);

            EmitLoadBitmapRef(il, 0);
            EmitLoadBitmapRef(il, 1);
            il.Emit(OpCodes.Call, IlEmitterShared.SwapContents);
            EmitLoadBitmapRef(il, 1);
            il.Emit(OpCodes.Call, IlEmitterShared.Clear);
            il.Emit(OpCodes.Ret);
        }
        else
        {
            il.MarkLabel(entryScanLabel);
            il.Emit(OpCodes.Ret);
        }

        cs.AppendLine("}");

        explainSource = explain.ToString();
        csharpSource = cs.ToString();
        return (CompiledExecuteDelegate)dm.CreateDelegate(typeof(CompiledExecuteDelegate));
    }

    private static void EmitLoadBitmapRef(ILGenerator il, int slot)
    {
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Ldfld, IlEmitterShared.CtxBitmaps);  // RoaringBitmap[]
        IlEmitterShared.EmitLdcI4(il, slot);
        il.Emit(OpCodes.Ldelema, typeof(RoaringBitmap)); // ref RoaringBitmap
    }

    private static void EmitLoadMatch(ILGenerator il, int index)
    {
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Ldfld, IlEmitterShared.CtxResolvedMatches); // IQueryMatch[]
        IlEmitterShared.EmitLdcI4(il, index);
        il.Emit(OpCodes.Ldelem_Ref);                  // IQueryMatch
    }

    private static void EmitLoadLimit(ILGenerator il)
    {
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Ldfld, IlEmitterShared.CtxLimit);
    }

    /// <summary>Emit: startTick = Stopwatch.GetTimestamp()</summary>
    private static void EmitTimingStart(ILGenerator il, LocalBuilder startTickLocal)
    {
        il.Emit(OpCodes.Call, IlEmitterShared.GetTimestamp);
        il.Emit(OpCodes.Stloc, startTickLocal);
    }

    /// <summary>Emit: IlEmitterShared.RecordTiming(ref ctx, opIndex, startTick); IlEmitterShared.RecordResultCount(ref ctx, opIndex);</summary>
    private static void EmitTimingEnd(ILGenerator il, int opIndex, LocalBuilder startTickLocal)
    {
        il.Emit(OpCodes.Ldarg_0);         // ref ctx
        IlEmitterShared.EmitLdcI4(il, opIndex);           // opIndex
        il.Emit(OpCodes.Ldloc, startTickLocal); // startTick
        il.Emit(OpCodes.Call, IlEmitterShared.RecordTiming);

        il.Emit(OpCodes.Ldarg_0);
        IlEmitterShared.EmitLdcI4(il, opIndex);
        il.Emit(OpCodes.Call, IlEmitterShared.RecordResultCount);
    }

    private static void EmitCancellationCheck(ILGenerator il)
    {
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Ldflda, IlEmitterShared.CtxToken);
        il.Emit(OpCodes.Call, IlEmitterShared.ThrowIfCancelled);
    }

    /// <summary>Emit array-length validation hints at the start of the delegate.
    /// Touch the maximum bitmap slot so the JIT can elide bitmap bounds checks
    /// inside the body. Source arrays (TermSources / TermsProviders / ResolvedMatches)
    /// are indexed by the runtime cursor (see <c>cursorVar</c>), not by a static
    /// op.ParamIndex, so we don't know a compile-time upper bound for them. The
    /// per-iteration ldelem still performs the bounds check at runtime.</summary>
    private static void EmitBoundsCheckPreamble(ILGenerator il, PlanOp[] ops)
    {
        int maxBitmapSlot = -1;

        for (int i = 0; i < ops.Length; i++)
        {
            ref PlanOp op = ref ops[i];

            // Bitmap slots
            if (op.BitmapLocal > maxBitmapSlot) maxBitmapSlot = op.BitmapLocal;
            if (op.Kind is PlanOpKind.AndBitmaps or PlanOpKind.AndNotBitmaps or PlanOpKind.OrBitmaps or PlanOpKind.SwapBitmaps)
            {
                if (op.ParamIndex2 > maxBitmapSlot) maxBitmapSlot = op.ParamIndex2;
            }
            // FillFromPostings / FillAllEntries always use bitmap[0]; And/AndNot use [0] and [1]
            if (op.Kind is PlanOpKind.FillFromPostings or PlanOpKind.DirectIterate or PlanOpKind.FillAllEntries)
            {
                if (0 > maxBitmapSlot) maxBitmapSlot = 0;
            }
            if (op.Kind is PlanOpKind.AndWithPostings or PlanOpKind.AndNotWithPostings)
            {
                if (1 > maxBitmapSlot) maxBitmapSlot = 1;
            }
        }

        // Touch _bitmaps[maxBitmapSlot] (RoaringBitmap is a value type, use ldelema)
        if (maxBitmapSlot >= 0)
        {
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Ldfld, IlEmitterShared.CtxBitmaps);
            IlEmitterShared.EmitLdcI4(il, maxBitmapSlot);
            il.Emit(OpCodes.Ldelema, typeof(RoaringBitmap));
            il.Emit(OpCodes.Pop);
        }
    }

    /// <summary>C# mirror of <see cref="EmitBoundsCheckPreamble"/>: emit the textual
    /// bounds-check touch and a trailing blank line. Same upper-bound computation;
    /// only bitmap-slot indices are static so only those are emitted.</summary>
    private static void EmitCSharpBoundsCheckPreamble(StringBuilder sb, PlanOp[] ops)
    {
        int maxBitmapSlot = -1;
        for (int i = 0; i < ops.Length; i++)
        {
            ref PlanOp op = ref ops[i];
            if (op.BitmapLocal > maxBitmapSlot) maxBitmapSlot = op.BitmapLocal;
            if (op.Kind is PlanOpKind.AndBitmaps or PlanOpKind.AndNotBitmaps or PlanOpKind.OrBitmaps or PlanOpKind.SwapBitmaps && op.ParamIndex2 > maxBitmapSlot)
                maxBitmapSlot = op.ParamIndex2;
            if (op.Kind is PlanOpKind.FillFromPostings or PlanOpKind.DirectIterate && 0 > maxBitmapSlot)
                maxBitmapSlot = 0;
            if (op.Kind is PlanOpKind.AndWithPostings or PlanOpKind.AndNotWithPostings && 1 > maxBitmapSlot)
                maxBitmapSlot = 1;
        }
        if (maxBitmapSlot >= 0)
            sb.AppendLine($"    _ = ref ctx.Bitmaps[{maxBitmapSlot}];");
        sb.AppendLine();
    }

    private static void EmptyExecute(CompiledQueryMatch ctx) { }

    /// <summary>Emit a bounds-check hint immediately after computing <paramref name="endVar"/>
    /// (the exclusive upper bound of an OrRange / AndRange loop). Touches the source array
    /// at <c>endVar - 1</c> so the JIT proves the array is at least <c>endVar</c> long;
    /// per-iteration <c>ldelem</c> calls in the loop body can then elide bounds checks.
    /// Guarded by <c>endVar != start</c> because when the runtime range count is zero the
    /// static <paramref name="start"/> may be past the array length (see
    /// EmitBoundsCheckPreamble for the explanation of why OrRange/AndRange are skipped there).
    /// </summary>
    private static void EmitRangeEndIndexTouch(ILGenerator il, MatchDispatch dispatch, LocalBuilder cursorVar, LocalBuilder endVar)
    {
        FieldInfo arrayField;
        OpCode loadOp;
        Type elementType;
        switch (dispatch)
        {
            case MatchDispatch.PostingList:
                arrayField = IlEmitterShared.CtxTermSources;
                loadOp = OpCodes.Ldelema;
                elementType = typeof(PostingSource);
                break;
            case MatchDispatch.TreeScan:
                arrayField = IlEmitterShared.CtxTermsProviders;
                loadOp = OpCodes.Ldelem_Ref;
                elementType = null;
                break;
            default:
                arrayField = IlEmitterShared.CtxResolvedMatches;
                loadOp = OpCodes.Ldelem_Ref;
                elementType = null;
                break;
        }

        var skipTouch = il.DefineLabel();
        il.Emit(OpCodes.Ldloc, endVar);
        il.Emit(OpCodes.Ldloc, cursorVar);
        il.Emit(OpCodes.Beq, skipTouch);

        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Ldfld, arrayField);
        il.Emit(OpCodes.Ldloc, endVar);
        il.Emit(OpCodes.Ldc_I4_1);
        il.Emit(OpCodes.Sub);
        if (elementType != null)
            il.Emit(loadOp, elementType);
        else
            il.Emit(loadOp);
        il.Emit(OpCodes.Pop);

        il.MarkLabel(skipTouch);
    }

    /// <summary>Emit: cursor = cursor + 1.</summary>
    private static void EmitCursorAdvance(ILGenerator il, LocalBuilder cursorVar)
    {
        il.Emit(OpCodes.Ldloc, cursorVar);
        il.Emit(OpCodes.Ldc_I4_1);
        il.Emit(OpCodes.Add);
        il.Emit(OpCodes.Stloc, cursorVar);
    }

    /// <summary>Emit: push ctx.ResolvedMatches[cursor] onto the stack.</summary>
    private static void EmitLoadMatchFromCursor(ILGenerator il, LocalBuilder cursorVar)
    {
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Ldfld, IlEmitterShared.CtxResolvedMatches);
        il.Emit(OpCodes.Ldloc, cursorVar);
        il.Emit(OpCodes.Ldelem_Ref);
    }
}
