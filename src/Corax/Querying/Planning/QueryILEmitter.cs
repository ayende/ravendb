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

        int bitmapCount = CountBitmaps(ops);
        d.CsLine($"// Uses {bitmapCount} result bitmaps");
        d.CsLine("""
                 [SkipLocalsInit]
                 static void CompiledQuery(CompiledQueryMatch ctx)
                 {
                 """);

        // Calling convention for the QueryPrimitives.Ctx*From*(ctx, cursor, slot) helpers below:
        //   'cursor' is the leaf cursor — it picks WHICH match we read from (the posting source / tree
        //   scan / query match resolved for ctx.Leaves[cursor] / ctx.ResolvedMatches[cursor], i.e. the
        //   field+term of that clause). 'slot' is only the DESTINATION bitmap (ctx.Bitmaps[slot]); slot 0
        //   is the live result accumulator. The cursor auto-advances by one after each such call.

        // Locals
        var bufferLocal = d.DeclareLocal(typeof(Span<long>), "buffer");
        var startTickLocal = d.DeclareLocal(typeof(long), "startTick");
        var cursorVar = d.DeclareLocal(typeof(int), "cursor");

        // Labels
        var doneLabel = d.DefineNamedLabel("Done");
        var entryScanLabel = d.DefineNamedLabel("EntryScan");
        bool hasEntryScan = false;
        bool needsLazyRepair = false;

        // stackalloc long[FillBufferSize]
        EmitStackAlloc(ref d, bufferLocal);

        // cursor = 0
        d.StoreLocalConst(cursorVar, 0);

        // The op stream ends in a run of pure control-flow-to-Done ops: the terminal GotoDone the
        // PlanEmitter always appends, plus any GotoDoneIfEmpty empty-checks that immediately precede it
        // (an AND followed only by `if empty goto Done; Done:` jumps to a label control already falls
        // through to). Everything from this index onward is dead — its branch, and the cursor advance of
        // the last real op, would just be `goto Done; Done:` / a dead store. lastEffectiveIndex is the
        // first op of that trailing dead run, so `i >= lastEffectiveIndex` flags both the last real op
        // (suppress its cursor advance / early exit) and the dead tail ops (suppress them entirely).
        int lastEffectiveIndex = ops.Length;
        while (lastEffectiveIndex > 0 &&
               ops[lastEffectiveIndex - 1].Kind is PlanOpKind.GotoDone or PlanOpKind.GotoDoneIfEmpty)
        {
            lastEffectiveIndex--;
        }
        lastEffectiveIndex--; // step onto the last real op itself

        for (int i = 0; i < ops.Length; i++)
        {
            ref PlanOp op = ref ops[i];

            bool isLastEffectiveOp = i >= lastEffectiveIndex;
            bool emitGoToEmpty = !op.SkipEarlyExit && !isLastEffectiveOp;
            bool emitGotoLimitReached = op.BitmapLocal == 0 && !isLastEffectiveOp;

            // The cursor advance after the last cursor-consuming op is a dead store — control falls
            // straight to Done and nothing reads cursor there (the entry-scan tail captures cursor at
            // its own branch point). Suppress it so the emitted IL/C# don't carry a trailing cursor++.
            bool advanceCursor = !isLastEffectiveOp;

            // A seed Fill into slot 0 can short-circuit on the limit exactly like the OR ops themselves:
            // once slot 0 only ever GROWS from here on (a union — whether an incremental OR-into-slot-0
            // or a LazyOrBitmaps merge of another operand built in its own slot, as in
            // `City = $c OR not exists(Name)`), slot 0 is a subset of the final result, so when it
            // already holds Limit matches the remaining leaves cannot change which docs an unordered
            // query returns. Gated on NO later op narrowing slot 0 (an AND/ANDNOT intersection), so
            // pipelines that keep intersecting slot 0 — e.g. the seed of `(City and Age) or ...` — never
            // get it while slot 0 is still being built down.
            bool seedFillGrowsOnly = op.BitmapLocal == 0 && !isLastEffectiveOp && !LaterOpNarrowsSlot0(ops, i + 1);

            // Build-time clause label (e.g. "Name [Equals]") for the C# mirror; no IL effect. Staged so it
            // trails the op's primary call line (see DualEmit.CsCall) instead of floating above the
            // cancellation check on its own line.
            if (op.DebugLabel != null)
                d.SetPendingComment(op.DebugLabel);

            // Timing: record start tick before each op
            if (emitTimings)
                EmitTimingStart(ref d, startTickLocal, i);

            switch (op.Kind)
            {
                case PlanOpKind.FillFromPostingSource:
                    d.EmitCancelledCursorSlotCall(cursorVar, IlEmitterShared.CtxFillFromPostingSource, "QueryPrimitives.CtxFillFromPostingSource", op.BitmapLocal, advanceCursor);
                    if (seedFillGrowsOnly)
                        d.EmitLimitReachedGoto(doneLabel.Il, doneLabel.Name);
                    break;

                case PlanOpKind.FillFromTreeScan:
                    d.EmitCancelledCursorSlotCall(cursorVar, IlEmitterShared.CtxFillFromTreeScan, "QueryPrimitives.CtxFillFromTreeScan", op.BitmapLocal, advanceCursor);
                    if (seedFillGrowsOnly)
                        d.EmitLimitReachedGoto(doneLabel.Il, doneLabel.Name);
                    break;

                case PlanOpKind.FillFromMatch:
                    d.EmitCancelledCursorSlotCall(cursorVar, IlEmitterShared.CtxFillFromMatch, "QueryPrimitives.CtxFillFromMatch", op.BitmapLocal, advanceCursor);
                    if (seedFillGrowsOnly)
                        d.EmitLimitReachedGoto(doneLabel.Il, doneLabel.Name);
                    break;

                case PlanOpKind.FillAllEntries:
                    d.EmitFillAllEntries(op.BitmapLocal);
                    break;

                case PlanOpKind.AndFromPostingSource:
                    d.EmitCancelledCursorSlotCall(cursorVar, IlEmitterShared.CtxAndFromPostingSource, "QueryPrimitives.CtxAndFromPostingSource", op.BitmapLocal, advanceCursor);
                    if (emitGoToEmpty)
                        d.EmitBitmapEmptyGoto(op.BitmapLocal, doneLabel.Il, doneLabel.Name);
                    break;

                case PlanOpKind.AndFromTreeScan:
                    d.EmitCancelledCursorSlotCall(cursorVar, IlEmitterShared.CtxAndFromTreeScan, "QueryPrimitives.CtxAndFromTreeScan", op.BitmapLocal, advanceCursor);
                    if (emitGoToEmpty)
                        d.EmitBitmapEmptyGoto(op.BitmapLocal, doneLabel.Il, doneLabel.Name);
                    break;

                case PlanOpKind.AndFromMatch:
                    d.EmitCancelledCursorSlotCall(cursorVar, IlEmitterShared.CtxAndFromMatch, "QueryPrimitives.CtxAndFromMatch", op.BitmapLocal, advanceCursor);
                    if (emitGoToEmpty)
                        d.EmitBitmapEmptyGoto(op.BitmapLocal, doneLabel.Il, doneLabel.Name);
                    break;

                case PlanOpKind.OrFromPostingSource:
                    d.EmitCancelledCursorSlotCall(cursorVar, IlEmitterShared.CtxOrFillFromPostingSource, "QueryPrimitives.CtxOrFillFromPostingSource", op.BitmapLocal, advanceCursor);
                    if (emitGotoLimitReached)
                        d.EmitLimitReachedGoto(doneLabel.Il, doneLabel.Name);
                    break;

                case PlanOpKind.OrFromTreeScan:
                    d.EmitCancelledCursorSlotCall(cursorVar, IlEmitterShared.CtxOrFillFromTreeScan, "QueryPrimitives.CtxOrFillFromTreeScan", op.BitmapLocal, advanceCursor);
                    if (emitGotoLimitReached)
                        d.EmitLimitReachedGoto(doneLabel.Il, doneLabel.Name);
                    break;

                case PlanOpKind.OrFromMatch:
                    d.EmitCancelledCursorSlotCall(cursorVar, IlEmitterShared.CtxOrWithMatchSlot, "QueryPrimitives.CtxOrWithMatchSlot", op.BitmapLocal, advanceCursor);
                    if (emitGotoLimitReached)
                        d.EmitLimitReachedGoto(doneLabel.Il, doneLabel.Name);
                    break;

                case PlanOpKind.AndNotFromPostingSource:
                    d.EmitCancelledCursorSlotCall(cursorVar, IlEmitterShared.CtxAndNotFromPostingSource, "QueryPrimitives.CtxAndNotFromPostingSource", op.BitmapLocal, advanceCursor);
                    break;

                case PlanOpKind.AndNotFromTreeScan:
                    d.EmitCancelledCursorSlotCall(cursorVar, IlEmitterShared.CtxAndNotFromTreeScan, "QueryPrimitives.CtxAndNotFromTreeScan", op.BitmapLocal, advanceCursor);
                    break;

                case PlanOpKind.AndNotFromMatch:
                    d.EmitCancelledCursorSlotCall(cursorVar, IlEmitterShared.CtxAndNotFromMatch, "QueryPrimitives.CtxAndNotFromMatch", op.BitmapLocal, advanceCursor);
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

                case PlanOpKind.GotoDoneIfEmpty:
                    // Dead when terminal: `if (empty) goto Done;` falls straight through to the Done label
                    // that immediately follows, so the branch is a no-op regardless of emptiness.
                    if (!isLastEffectiveOp)
                        d.EmitBitmapEmptyGoto(op.BitmapLocal, doneLabel.Il, doneLabel.Name);
                    break;

                case PlanOpKind.MaybeEntryScan:
                {
                    hasEntryScan = true;
                    EmitEntryScanCheck(ref d, cursorVar, entryScanLabel);
                    break;
                }

                case PlanOpKind.OrRangeFromPostingSource:
                    EmitRangeLoop(ref d, cursorVar, op.ParamIndex2, op.BitmapLocal,
                        IlEmitterShared.CtxOrFillFromPostingSource, "QueryPrimitives.CtxOrFillFromPostingSource", i,
                        earlyExit: false, skipEarlyExit: false, doneLabel);
                    break;

                case PlanOpKind.OrRangeFromMatch:
                    EmitRangeLoop(ref d, cursorVar, op.ParamIndex2, op.BitmapLocal,
                        IlEmitterShared.CtxOrWithMatchSlot, "QueryPrimitives.CtxOrWithMatchSlot", i,
                        earlyExit: false, skipEarlyExit: false, doneLabel);
                    break;

                case PlanOpKind.AndRangeFromPostingSource:
                    EmitRangeLoop(ref d, cursorVar, op.ParamIndex2, op.BitmapLocal,
                        IlEmitterShared.CtxAndFromPostingSource, "QueryPrimitives.CtxAndFromPostingSource", i,
                        earlyExit: true, skipEarlyExit: op.SkipEarlyExit, doneLabel);
                    break;

                case PlanOpKind.AndRangeFromMatch:
                    EmitRangeLoop(ref d, cursorVar, op.ParamIndex2, op.BitmapLocal,
                        IlEmitterShared.CtxAndFromMatch, "QueryPrimitives.CtxAndFromMatch", i,
                        earlyExit: true, skipEarlyExit: op.SkipEarlyExit, doneLabel);
                    break;

                case PlanOpKind.GotoDone:
                    // Falls straight through to the Done label when terminal — emitting the branch
                    // would be dead IL. Only emit when something real follows.
                    if (!isLastEffectiveOp)
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
            EmitEntryScanTail(ref d, entryScanLabel, cursorVar);
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

    /// <summary>Emit the OR/AND range loop over IN-expanded term slots. The IL is a canonical
    /// check-first loop (init; goto check; body; check: cond → body); the C# mirror is rendered
    /// as an equivalent <c>for</c> loop.</summary>
    private static void EmitRangeLoop(ref DualEmit d, LocalBuilder cursorVar, int rangeIdx, int bitmapLocal,
        MethodInfo method, string methodName, int opIndex,
        bool earlyExit, bool skipEarlyExit, LabelPair doneLabel)
    {
        var loopVar = d.DeclareLocal(typeof(int), $"j_{opIndex}");
        var endVar = d.DeclareLocal(typeof(int), $"end_{opIndex}");
        var loopCheck = d.DefineLabelPair($"rangeCheck_{opIndex}");
        var loopBody = d.DefineLabelPair($"rangeBody_{opIndex}");

        // endVar = cursor + ctx.InRangeCounts[rangeIdx] (dual: drives the for-loop bound below)
        d.Il.Emit(OpCodes.Ldloc, cursorVar);
        d.Il.Emit(OpCodes.Ldarg_0);
        d.Il.Emit(OpCodes.Ldfld, IlEmitterShared.CtxInRangeCounts);
        IlEmitterShared.EmitLdcI4(d.Il, rangeIdx);
        d.Il.Emit(OpCodes.Ldelem_I4);
        d.Il.Emit(OpCodes.Add);
        d.Il.Emit(OpCodes.Stloc, endVar);
        d.CsLine($"{d.GetLocalName(endVar)} = {d.GetLocalName(cursorVar)} + ctx.InRangeCounts[{rangeIdx}];");

        // IL loop init: loopVar = cursor; goto check.   (IL only — the for-header carries this in C#)
        d.Il.Emit(OpCodes.Ldloc, cursorVar);
        d.Il.Emit(OpCodes.Stloc, loopVar);
        d.Il.Emit(OpCodes.Br, loopCheck.Il);

        // C# for-header + open brace (C# only).
        d.CsLine($"for ({d.GetLocalName(loopVar)} = {d.GetLocalName(cursorVar)}; {d.GetLocalName(loopVar)} < {d.GetLocalName(endVar)}; {d.GetLocalName(loopVar)}++)");
        d.CsLine("{");

        // Loop body (dual statements land inside the braces).
        d.Il.MarkLabel(loopBody.Il);
        d.IlCancellationCheck();

        // Both AND and OR primitives take the destination slot explicitly: ctx.Method(ctx, loopVar, bitmapLocal).
        d.Il.Emit(OpCodes.Ldarg_0);
        d.Il.Emit(OpCodes.Ldloc, loopVar);
        d.Il.Emit(OpCodes.Ldc_I4, bitmapLocal);
        d.Il.Emit(OpCodes.Call, method);
        d.CsCall($"{methodName}(ctx, {d.GetLocalName(loopVar)}, {bitmapLocal});");

        // AND short-circuits once the destination is empty (the intersection can only shrink).
        if (earlyExit && !skipEarlyExit)
        {
            d.EmitBitmapEmptyGoto(bitmapLocal, doneLabel.Il, doneLabel.Name);
        }

        // IL loopVar++ (IL only — the for-header carries this in C#).
        d.Il.Emit(OpCodes.Ldloc, loopVar);
        d.Il.Emit(OpCodes.Ldc_I4_1);
        d.Il.Emit(OpCodes.Add);
        d.Il.Emit(OpCodes.Stloc, loopVar);

        // C# close brace (C# only).
        d.CsLine("}");

        // IL loop check: if (loopVar < endVar) goto loopBody.   (IL only)
        d.Il.MarkLabel(loopCheck.Il);
        d.Il.Emit(OpCodes.Ldloc, loopVar);
        d.Il.Emit(OpCodes.Ldloc, endVar);
        d.Il.Emit(OpCodes.Blt, loopBody.Il);

        // cursor = endVar   (dual)
        d.Il.Emit(OpCodes.Ldloc, endVar);
        d.Il.Emit(OpCodes.Stloc, cursorVar);
        d.CsLine($"{d.GetLocalName(cursorVar)} = {d.GetLocalName(endVar)};");
    }

    /// <summary>EntryScan tail: set ctx.EntryScanTakenAtOp, run entry scan, return. Survivors are left in
    /// slot 1 (RunEntryScan source 0 -> target 1).</summary>
    private static void EmitEntryScanTail(ref DualEmit d, LabelPair entryScanLabel, LocalBuilder cursorVar)
    {
        d.MarkLabel(entryScanLabel);

        // ctx.EntryScanTakenAtOp = cursor
        d.Il.Emit(OpCodes.Ldarg_0);
        d.Il.Emit(OpCodes.Ldloc, cursorVar);
        d.Il.Emit(OpCodes.Stfld, IlEmitterShared.CtxEntryScanTakenAtOp);
        d.CsLine($"ctx.EntryScanTakenAtOp = {d.GetLocalName(cursorVar)};");

        // CompiledQueryHelper.RunEntryScan(ctx, ref bitmaps[0], ref bitmaps[1])
        d.Il.Emit(OpCodes.Ldarg_0);
        d.IlLoadBitmapRef(0);
        d.IlLoadBitmapRef(1);
        d.Il.Emit(OpCodes.Call, IlEmitterShared.RunEntryScanMethod);
        d.CsLine("CompiledQueryHelper.RunEntryScan(ctx, ref ctx.Bitmaps[0], ref ctx.Bitmaps[1]);");

        // Execute() reads the result from slot 1 because EntryScanTakenAtOp is now set, and disposes slot 0.
        d.EmitRetVoid();
    }

    /// <summary>Highest destination/source bitmap slot referenced by any op, +1. For the
    /// bitmap-to-bitmap ops (AndBitmaps / AndNotBitmaps / LazyOrBitmaps) ParamIndex2 is a
    /// source SLOT and counts; for the range ops it is an InRangeCounts index and does not.
    /// MaybeEntryScan stages survivors into slot 1, so any plan with an entry-scan needs ≥2.</summary>
    private static int CountBitmaps(PlanOp[] ops)
    {
        int maxSlot = 0;
        for (int i = 0; i < ops.Length; i++)
        {
            ref var op = ref ops[i];
            var curSlot = Math.Max(op.Kind switch
            {
                PlanOpKind.AndBitmaps or PlanOpKind.AndNotBitmaps or PlanOpKind.LazyOrBitmaps => op.BitmapLocal,
                PlanOpKind.MaybeEntryScan => 1,
                _ => -1
            }, op.BitmapLocal);
            maxSlot = Math.Max(curSlot, maxSlot);
        }
        return maxSlot + 1;
    }
    
    /// <summary>True if any op from <paramref name="from"/> onward narrows slot 0 — an AND/ANDNOT
    /// intersection into slot 0, or an entry scan (which reanchors the result into slot 1). When this
    /// is false the slot-0 accumulator only ever grows (unions), so it is already a subset of the final
    /// unordered result and a Limit short-circuit at the current point is safe.</summary>
    private static bool LaterOpNarrowsSlot0(PlanOp[] ops, int from)
    {
        for (int k = from; k < ops.Length; k++)
        {
            ref PlanOp op = ref ops[k];

            // Entry scan moves the result from slot 0 into slot 1; stopping early on slot 0's count
            // would skip the scan that actually produces the answer.
            if (op.Kind == PlanOpKind.MaybeEntryScan)
                return true;

            if (op.BitmapLocal != 0)
                continue;

            if (op.Kind is PlanOpKind.AndFromPostingSource or PlanOpKind.AndFromTreeScan or PlanOpKind.AndFromMatch
                or PlanOpKind.AndNotFromPostingSource or PlanOpKind.AndNotFromTreeScan or PlanOpKind.AndNotFromMatch
                or PlanOpKind.AndBitmaps or PlanOpKind.AndNotBitmaps
                or PlanOpKind.AndRangeFromPostingSource or PlanOpKind.AndRangeFromMatch)
                return true;
        }

        return false;
    }

    private static void EmptyExecute(CompiledQueryMatch ctx) { }
}
