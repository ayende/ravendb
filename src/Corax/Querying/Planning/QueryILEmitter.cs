using System;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;
using Corax.Querying.Matches;
using Corax.Querying.Primitives;

namespace Corax.Querying.Planning;

public static class QueryIlEmitter
{
    public delegate void CompiledExecuteDelegate(CompiledQueryMatch ctx);

    // Span<long>
    private static readonly ConstructorInfo SpanCtor = typeof(Span<long>).GetConstructor([typeof(void*), typeof(int)])!;

    public static CompiledExecuteDelegate EmitDelegate(PlanOp[] ops, out string csharpSource, bool emitTimings = true)
    {
        if (ops == null || ops.Length == 0)
        {
            csharpSource = "// Empty plan.\n";
            return EmptyExecute;
        }

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
        var cs = new StringBuilder();
        var d = new DualEmit(il, cs);

        // Register arguments.
        var ctxIdx = d.RegisterArg("ctx");

        // C# function header.
        d.CsLine("static void CompiledQuery(CompiledQueryMatch ctx)");
        d.CsLine("{");

        // Locals
        var bufferLocal = d.DeclareLocal(typeof(Span<long>), "buffer");
        var readLocal = d.DeclareLocal(typeof(int), "read");
        var startTickLocal = d.DeclareLocal(typeof(long), "startTick");
        var cursorVar = d.DeclareLocal(typeof(int), "cursor");

        // Labels
        var doneLabel = d.DefineNamedLabel("Done");
        var entryScanLabel = d.DefineNamedLabel("EntryScan");
        bool hasEntryScan = false;
        bool needsLazyRepair = false;
        int entryScanOpIndex = -1;

        // stackalloc long[FillBufferSize]
        EmitStackAlloc(ref d, bufferLocal);

        // cursor = 0
        d.StoreLocalConst(cursorVar, 0);

        // Bounds-check elimination preamble
        EmitBoundsCheckPreamble(ref d, ops);

        for (int i = 0; i < ops.Length; i++)
        {
            ref PlanOp op = ref ops[i];

            // Timing: record start tick before each op
            if (emitTimings)
                EmitTimingStart(ref d, startTickLocal, i);

            switch (op.Kind)
            {
                case PlanOpKind.FillFromPostingSource:
                    d.EmitCancelledCursorSlotCall(cursorVar, IlEmitterShared.CtxFillFromPostingSource, "QueryPrimitives.CtxFillFromPostingSource", op.BitmapLocal);
                    break;

                case PlanOpKind.FillFromTreeScan:
                    d.EmitCancelledCursorSlotCall(cursorVar, IlEmitterShared.CtxFillFromTreeScan, "QueryPrimitives.CtxFillFromTreeScan", op.BitmapLocal);
                    break;

                case PlanOpKind.FillFromMatch:
                    d.EmitCancelledCursorSlotCall(cursorVar, IlEmitterShared.CtxFillFromMatch, "QueryPrimitives.CtxFillFromMatch", op.BitmapLocal);
                    break;

                case PlanOpKind.FillAllEntries:
                    d.EmitFillAllEntries();
                    break;

                case PlanOpKind.AndFromPostingSource:
                    d.EmitCancelledCursorSlotCall(cursorVar, IlEmitterShared.CtxAndFromPostingSource, "QueryPrimitives.CtxAndFromPostingSource", op.BitmapLocal);
                    if (!op.SkipEarlyExit)
                        d.EmitBitmapEmptyGoto(op.BitmapLocal, doneLabel.Il, doneLabel.Name);
                    break;

                case PlanOpKind.AndFromTreeScan:
                    d.EmitCancelledCursorSlotCall(cursorVar, IlEmitterShared.CtxAndFromTreeScan, "QueryPrimitives.CtxAndFromTreeScan", op.BitmapLocal);
                    if (!op.SkipEarlyExit)
                        d.EmitBitmapEmptyGoto(op.BitmapLocal, doneLabel.Il, doneLabel.Name);
                    break;

                case PlanOpKind.AndFromMatch:
                    d.EmitCancelledCursorSlotCall(cursorVar, IlEmitterShared.CtxAndFromMatch, "QueryPrimitives.CtxAndFromMatch", op.BitmapLocal);
                    if (!op.SkipEarlyExit)
                        d.EmitBitmapEmptyGoto(op.BitmapLocal, doneLabel.Il, doneLabel.Name);
                    break;

                case PlanOpKind.OrFromPostingSource:
                    d.EmitCancelledCursorSlotCall(cursorVar, IlEmitterShared.CtxOrFillFromPostingSource, "QueryPrimitives.CtxOrFillFromPostingSource", op.BitmapLocal);
                    if (op.BitmapLocal == 0)
                        d.EmitLimitReachedGoto(doneLabel.Il, doneLabel.Name);
                    break;

                case PlanOpKind.OrFromTreeScan:
                    d.EmitCancelledCursorSlotCall(cursorVar, IlEmitterShared.CtxOrFillFromTreeScan, "QueryPrimitives.CtxOrFillFromTreeScan", op.BitmapLocal);
                    if (op.BitmapLocal == 0)
                        d.EmitLimitReachedGoto(doneLabel.Il, doneLabel.Name);
                    break;

                case PlanOpKind.OrFromMatch:
                    d.EmitCancelledCursorSlotCall(cursorVar, IlEmitterShared.CtxOrWithMatchSlot, "QueryPrimitives.CtxOrWithMatchSlot", op.BitmapLocal);
                    if (op.BitmapLocal == 0)
                        d.EmitLimitReachedGoto(doneLabel.Il, doneLabel.Name);
                    break;

                case PlanOpKind.AndNotFromPostingSource:
                    d.EmitCancelledCursorSlotCall(cursorVar, IlEmitterShared.CtxAndNotFromPostingSource, "QueryPrimitives.CtxAndNotFromPostingSource", op.BitmapLocal);
                    break;

                case PlanOpKind.AndNotFromTreeScan:
                    d.EmitCancelledCursorSlotCall(cursorVar, IlEmitterShared.CtxAndNotFromTreeScan, "QueryPrimitives.CtxAndNotFromTreeScan", op.BitmapLocal);
                    break;

                case PlanOpKind.AndNotFromMatch:
                    d.EmitCancelledCursorSlotCall(cursorVar, IlEmitterShared.CtxAndNotFromMatch, "QueryPrimitives.CtxAndNotFromMatch", op.BitmapLocal);
                    break;

                case PlanOpKind.ClearBitmap:
                    d.EmitBitmapUnaryCall(op.BitmapLocal, IlEmitterShared.Clear, "Clear");
                    break;

                case PlanOpKind.AndBitmaps:
                    d.EmitBitmapBinaryOp(op.BitmapLocal, op.ParamIndex2, IlEmitterShared.AndWith, "AndWith");
                    break;

                case PlanOpKind.AndNotBitmaps:
                    d.EmitBitmapBinaryOp(op.BitmapLocal, op.ParamIndex2, IlEmitterShared.AndNotWith, "AndNotWith");
                    break;

                case PlanOpKind.LazyOrBitmaps:
                    d.EmitBitmapBinaryOp(op.BitmapLocal, op.ParamIndex2, IlEmitterShared.LazyOrWith, "LazyOrWith");
                    needsLazyRepair = true;
                    break;

                case PlanOpKind.SwapBitmaps:
                    d.EmitBitmapBinaryOp(op.BitmapLocal, op.ParamIndex2, IlEmitterShared.SwapContents, "SwapContents");
                    break;

                case PlanOpKind.GotoDoneIfEmpty:
                    d.EmitBitmapEmptyGoto(op.BitmapLocal, doneLabel.Il, doneLabel.Name);
                    break;

                case PlanOpKind.MaybeEntryScan:
                {
                    hasEntryScan = true;
                    entryScanOpIndex = i;
                    EmitEntryScanCheck(ref d, cursorVar, entryScanLabel);
                    break;
                }

                case PlanOpKind.OrRangeFromPostingSource:
                    EmitRangeLoop(ref d, cursorVar, op.ParamIndex2, op.BitmapLocal, MatchDispatch.PostingList,
                        IlEmitterShared.CtxOrFillFromPostingSource, "QueryPrimitives.CtxOrFillFromPostingSource", i,
                        earlyExit: false, skipEarlyExit: false, doneLabel);
                    break;

                case PlanOpKind.OrRangeFromMatch:
                    EmitRangeLoop(ref d, cursorVar, op.ParamIndex2, op.BitmapLocal, MatchDispatch.QueryMatch,
                        IlEmitterShared.CtxOrWithMatchSlot, "QueryPrimitives.CtxOrWithMatchSlot", i,
                        earlyExit: false, skipEarlyExit: false, doneLabel);
                    break;

                case PlanOpKind.AndRangeFromPostingSource:
                    EmitRangeLoop(ref d, cursorVar, op.ParamIndex2, op.BitmapLocal, MatchDispatch.PostingList,
                        IlEmitterShared.CtxAndFromPostingSource, "QueryPrimitives.CtxAndFromPostingSource", i,
                        earlyExit: true, skipEarlyExit: op.SkipEarlyExit, doneLabel);
                    break;

                case PlanOpKind.AndRangeFromMatch:
                    EmitRangeLoop(ref d, cursorVar, op.ParamIndex2, op.BitmapLocal, MatchDispatch.QueryMatch,
                        IlEmitterShared.CtxAndFromMatch, "QueryPrimitives.CtxAndFromMatch", i,
                        earlyExit: true, skipEarlyExit: op.SkipEarlyExit, doneLabel);
                    break;

                case PlanOpKind.GotoDone:
                    d.EmitGotoDone(doneLabel.Il, doneLabel.Name);
                    break;
            }

            // Timing: record elapsed time and result count after each op
            if (emitTimings)
                EmitTimingEnd(ref d, i, startTickLocal);
        }

        // Done label
        d.MarkLabel(doneLabel);
        if (needsLazyRepair)
            d.EmitBitmapUnaryCall(0, IlEmitterShared.RepairAfterLazy, "RepairAfterLazy");
        d.EmitRetVoid();

        // EntryScan tail
        if (hasEntryScan)
            EmitEntryScanTail(ref d, entryScanLabel, entryScanOpIndex);
        else
        {
            // Dead label — must be marked even if unreachable (IL verifier requires it).
            d.Il.MarkLabel(entryScanLabel.Il);
            d.Il.Emit(OpCodes.Ret);
        }

        d.CsLine("}");

        csharpSource = cs.ToString();
        return (CompiledExecuteDelegate)dm.CreateDelegate(typeof(CompiledExecuteDelegate));
    }

    /// <summary>stackalloc long[FillBufferSize] → bufferLocal</summary>
    private static void EmitStackAlloc(ref DualEmit d, LocalBuilder bufferLocal)
    {
        IlEmitterShared.EmitLdcI4(d.Il, QueryPrimitives.FillBufferSize);
        d.Il.Emit(OpCodes.Conv_U);
        d.Il.Emit(OpCodes.Sizeof, typeof(long));
        d.Il.Emit(OpCodes.Mul_Ovf_Un);
        d.Il.Emit(OpCodes.Localloc);
        IlEmitterShared.EmitLdcI4(d.Il, QueryPrimitives.FillBufferSize);
        d.Il.Emit(OpCodes.Newobj, SpanCtor);
        d.Il.Emit(OpCodes.Stloc, bufferLocal);
        d.CsLine($"Span<long> {d.GetLocalName(bufferLocal)} = stackalloc long[{QueryPrimitives.FillBufferSize}];");
    }

    /// <summary>Emit bounds-check preamble: touch max bitmap slot so JIT hoists length checks.</summary>
    private static void EmitBoundsCheckPreamble(ref DualEmit d, PlanOp[] ops)
    {
        int maxBitmapSlot = ComputeMaxBitmapSlot(ops);

        if (maxBitmapSlot >= 0)
        {
            d.IlLoadBitmapRef(maxBitmapSlot);
            d.Il.Emit(OpCodes.Pop);
            d.CsLine($"_ = ref ctx.Bitmaps[{maxBitmapSlot}];");
        }
        d.CsLine("");
    }

    private static int ComputeMaxBitmapSlot(PlanOp[] ops)
    {
        int max = -1;
        for (int i = 0; i < ops.Length; i++)
        {
            ref PlanOp op = ref ops[i];
            if (op.BitmapLocal > max) max = op.BitmapLocal;
            if (op.Kind is PlanOpKind.AndBitmaps or PlanOpKind.AndNotBitmaps or PlanOpKind.LazyOrBitmaps or PlanOpKind.SwapBitmaps
                && op.ParamIndex2 > max) max = op.ParamIndex2;
            if (op.Kind is PlanOpKind.FillFromPostingSource or PlanOpKind.FillFromTreeScan or PlanOpKind.FillFromMatch or PlanOpKind.FillAllEntries && 0 > max) max = 0;
            if (op.Kind is PlanOpKind.AndFromPostingSource or PlanOpKind.AndFromTreeScan or PlanOpKind.AndFromMatch
                    or PlanOpKind.AndNotFromPostingSource or PlanOpKind.AndNotFromTreeScan or PlanOpKind.AndNotFromMatch && 1 > max) max = 1;
        }
        return max;
    }

    /// <summary>startTick = Stopwatch.GetTimestamp()</summary>
    private static void EmitTimingStart(ref DualEmit d, LocalBuilder startTickLocal, int opIndex)
    {
        d.Il.Emit(OpCodes.Call, IlEmitterShared.GetTimestamp);
        d.Il.Emit(OpCodes.Stloc, startTickLocal);
        d.CsLine($"long startTick_{opIndex} = Stopwatch.GetTimestamp();");
    }

    /// <summary>RecordTiming + RecordResultCount</summary>
    private static void EmitTimingEnd(ref DualEmit d, int opIndex, LocalBuilder startTickLocal)
    {
        d.Il.Emit(OpCodes.Ldarg_0);
        IlEmitterShared.EmitLdcI4(d.Il, opIndex);
        d.Il.Emit(OpCodes.Ldloc, startTickLocal);
        d.Il.Emit(OpCodes.Call, IlEmitterShared.RecordTiming);

        d.Il.Emit(OpCodes.Ldarg_0);
        IlEmitterShared.EmitLdcI4(d.Il, opIndex);
        d.Il.Emit(OpCodes.Call, IlEmitterShared.RecordResultCount);

        d.CsLine($"CompiledQueryHelper.RecordTiming(ctx, {opIndex}, startTick_{opIndex});");
        d.CsLine($"CompiledQueryHelper.RecordResultCount(ctx, {opIndex});");
        d.CsLine("");
    }

    /// <summary>if (ShouldSwitchToEntryScan(bitmaps[0].Count, ctx.Cardinalities[cursor])) goto EntryScan.</summary>
    private static void EmitEntryScanCheck(ref DualEmit d, LocalBuilder cursorVar, LabelPair entryScanLabel)
    {
        d.IlLoadBitmapRef(0);
        d.Il.Emit(OpCodes.Call, IlEmitterShared.CountGetter);
        d.Il.Emit(OpCodes.Conv_I8);

        // ctx.Cardinalities[cursor]
        d.Il.Emit(OpCodes.Ldarg_0);
        d.Il.Emit(OpCodes.Ldfld, IlEmitterShared.CtxCardinalities);
        d.Il.Emit(OpCodes.Ldloc, cursorVar);
        d.Il.Emit(OpCodes.Ldelem_I8);

        d.Il.Emit(OpCodes.Call, IlEmitterShared.ShouldSwitchToEntryScan);
        d.Il.Emit(OpCodes.Brtrue, entryScanLabel.Il);

        d.CsLine($"if (QueryPrimitives.ShouldSwitchToEntryScan((long)ctx.Bitmaps[0].Count, ctx.Cardinalities[{d.GetLocalName(cursorVar)}]))");
        d.CsLine($"    goto {entryScanLabel.Name};");
    }

    /// <summary>Emit the OR/AND range loop over IN-expanded term slots.</summary>
    private static void EmitRangeLoop(ref DualEmit d, LocalBuilder cursorVar, int rangeIdx, int bitmapLocal,
        MatchDispatch dispatch, MethodInfo method, string methodName, int opIndex,
        bool earlyExit, bool skipEarlyExit, LabelPair doneLabel)
    {
        var loopVar = d.DeclareLocal(typeof(int), $"j_{opIndex}");
        var endVar = d.DeclareLocal(typeof(int), $"end_{opIndex}");
        var loopCheck = d.DefineLabelPair($"rangeCheck_{opIndex}");
        var loopBody = d.DefineLabelPair($"rangeBody_{opIndex}");

        // endVar = cursor + ctx.InRangeCounts[rangeIdx]
        d.Il.Emit(OpCodes.Ldloc, cursorVar);
        d.Il.Emit(OpCodes.Ldarg_0);
        d.Il.Emit(OpCodes.Ldfld, IlEmitterShared.CtxInRangeCounts);
        IlEmitterShared.EmitLdcI4(d.Il, rangeIdx);
        d.Il.Emit(OpCodes.Ldelem_I4);
        d.Il.Emit(OpCodes.Add);
        d.Il.Emit(OpCodes.Stloc, endVar);
        d.CsLine($"{d.GetLocalName(endVar)} = {d.GetLocalName(cursorVar)} + ctx.InRangeCounts[{rangeIdx}];");

        // Bounds-check hint: touch source[endVar-1]
        EmitRangeEndIndexTouch(d.Il, dispatch, cursorVar, endVar);

        // loopVar = cursor
        d.Il.Emit(OpCodes.Ldloc, cursorVar);
        d.Il.Emit(OpCodes.Stloc, loopVar);
        d.CsLine($"{d.GetLocalName(loopVar)} = {d.GetLocalName(cursorVar)};");

        d.GotoAlways(loopCheck);

        // Loop body
        d.MarkLabel(loopBody);
        d.IlCancellationCheck();

        // Both AND and OR primitives take the destination slot explicitly: ctx.Method(ctx, loopVar, bitmapLocal).
        d.Il.Emit(OpCodes.Ldarg_0);
        d.Il.Emit(OpCodes.Ldloc, loopVar);
        d.Il.Emit(OpCodes.Ldc_I4, bitmapLocal);
        d.Il.Emit(OpCodes.Call, method);
        d.CsLine($"{methodName}(ctx, {d.GetLocalName(loopVar)}, {bitmapLocal});");

        // AND short-circuits once the destination is empty (the intersection can only shrink).
        if (earlyExit && !skipEarlyExit)
        {
            d.EmitBitmapEmptyGoto(bitmapLocal, doneLabel.Il, doneLabel.Name);
        }

        // loopVar++
        d.IncrementLocal(loopVar);

        // Loop check: if (loopVar < endVar) goto loopBody
        d.MarkLabel(loopCheck);
        d.LoadLocal(loopVar);
        d.LoadLocal(endVar);
        d.BranchLT(loopBody);

        // cursor = endVar
        d.Il.Emit(OpCodes.Ldloc, endVar);
        d.Il.Emit(OpCodes.Stloc, cursorVar);
        d.CsLine($"{d.GetLocalName(cursorVar)} = {d.GetLocalName(endVar)};");
    }

    /// <summary>EntryScan tail: set ctx.EntryScanTakenAtOp, run entry scan, swap bitmaps, return.</summary>
    private static void EmitEntryScanTail(ref DualEmit d, LabelPair entryScanLabel, int entryScanOpIndex)
    {
        d.MarkLabel(entryScanLabel);

        // ctx.EntryScanTakenAtOp = entryScanOpIndex
        d.Il.Emit(OpCodes.Ldarg_0);
        d.Il.Emit(OpCodes.Ldc_I4, entryScanOpIndex);
        d.Il.Emit(OpCodes.Stfld, IlEmitterShared.CtxEntryScanTakenAtOp);
        d.CsLine($"ctx.EntryScanTakenAtOp = {entryScanOpIndex};");

        // CompiledQueryHelper.RunEntryScan(ctx, ref bitmaps[0], ref bitmaps[1])
        d.Il.Emit(OpCodes.Ldarg_0);
        d.IlLoadBitmapRef(0);
        d.IlLoadBitmapRef(1);
        d.Il.Emit(OpCodes.Call, IlEmitterShared.RunEntryScanMethod);
        d.CsLine("CompiledQueryHelper.RunEntryScan(ctx, ref ctx.Bitmaps[0], ref ctx.Bitmaps[1]);");

        // bitmaps[0].SwapContents(ref bitmaps[1])
        d.EmitBitmapBinaryOp(0, 1, IlEmitterShared.SwapContents, "SwapContents");
        // bitmaps[1].Clear()
        d.EmitBitmapUnaryCall(1, IlEmitterShared.Clear, "Clear");

        d.EmitRetVoid();
    }

    /// <summary>Emit a bounds-check hint immediately after computing endVar
    /// (the exclusive upper bound of an OrRange / AndRange loop). Touches the source array
    /// at endVar - 1 so the JIT proves the array is at least endVar long.</summary>
    private static void EmitRangeEndIndexTouch(ILGenerator il, MatchDispatch dispatch, LocalBuilder cursorVar, LocalBuilder endVar)
    {
        FieldInfo arrayField;
        OpCode loadOp;
        Type elementType;
        switch (dispatch)
        {
            case MatchDispatch.PostingList:
                arrayField = IlEmitterShared.CtxLeaves;
                loadOp = OpCodes.Ldelema;
                elementType = typeof(LeafResolveInfo);
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

    private static void EmptyExecute(CompiledQueryMatch ctx) { }
}
