using System.Reflection;
using System.Reflection.Emit;
using Voron.Data.RoaringBitmaps;

namespace Corax.Querying.Planning;

/// <summary>Statement-level emission helpers used by <see cref="QueryIlEmitter"/>.
/// Each helper emits one IL sequence AND the matching C# line(s) so the two
/// backends cannot drift — same drift-prevention discipline as the value-stack
/// primitives in <see cref="DualEmit"/> but at statement granularity (no operand
/// stack involvement; nothing is pushed or popped on CsStack).</summary>
internal ref partial struct DualEmit
{
    // ── Internal IL building blocks ──────────────────────────────────────

    /// <summary>Push <c>ref ctx.Bitmaps[slot]</c> onto the IL eval stack.</summary>
    private void IlLoadBitmapRef(int slot)
    {
        IL.Emit(OpCodes.Ldarg_0);
        IL.Emit(OpCodes.Ldfld, IlEmitterShared.CtxBitmaps);
        IlEmitterShared.EmitLdcI4(IL, slot);
        IL.Emit(OpCodes.Ldelema, typeof(RoaringBitmap));
    }

    /// <summary>Emit a cancellation token check at the IL level + the matching C# line.</summary>
    private void IlCancellationCheck()
    {
        IL.Emit(OpCodes.Ldarg_0);
        IL.Emit(OpCodes.Ldflda, IlEmitterShared.CtxToken);
        IL.Emit(OpCodes.Call, IlEmitterShared.ThrowIfCancelled);
    }

    /// <summary>Emit <c>cursor = cursor + 1</c> at the IL level (no C# — the caller
    /// typically inlines <c>cursor++</c> into the same C# line as the dispatched call).</summary>
    private void IlAdvanceCursor(LocalBuilder cursorVar)
    {
        IL.Emit(OpCodes.Ldloc, cursorVar);
        IL.Emit(OpCodes.Ldc_I4_1);
        IL.Emit(OpCodes.Add);
        IL.Emit(OpCodes.Stloc, cursorVar);
    }

    // ── Bitmap binary ops (AndWith / AndNotWith / LazyOrWith / SwapContents) ──

    /// <summary>Emit <c>ctx.Bitmaps[target].{op}(ref ctx.Bitmaps[source])</c> at both
    /// backends. <paramref name="csOp"/> is the method name to render in C# (e.g.
    /// <c>"AndWith"</c>); <paramref name="ilMethod"/> is the bound MethodInfo.</summary>
    public void EmitBitmapBinaryOp(int target, int source, MethodInfo ilMethod, string csOp)
    {
        IlLoadBitmapRef(target);
        IlLoadBitmapRef(source);
        IL.Emit(OpCodes.Call, ilMethod);
        CsLine($"ctx.Bitmaps[{target}].{csOp}(ref ctx.Bitmaps[{source}]);");
    }

    /// <summary>Emit <c>ctx.Bitmaps[slot].{op}()</c> at both backends (e.g. Clear,
    /// RepairAfterLazy).</summary>
    public void EmitBitmapUnaryCall(int slot, MethodInfo ilMethod, string csOp)
    {
        IlLoadBitmapRef(slot);
        IL.Emit(OpCodes.Call, ilMethod);
        CsLine($"ctx.Bitmaps[{slot}].{csOp}();");
    }

    /// <summary>Emit <c>if (ctx.Bitmaps[slot].IsEmpty) goto Done</c>.
    /// Uses the caller-managed <paramref name="doneIlLabel"/> + literal C# label name
    /// <paramref name="doneCsName"/> (typically <c>"Done"</c>) so the C# output keeps
    /// readable label names rather than the numbered form <see cref="LabelPair"/> produces.</summary>
    public void EmitBitmapEmptyGoto(int slot, Label doneIlLabel, string doneCsName)
    {
        IlLoadBitmapRef(slot);
        IL.Emit(OpCodes.Call, IlEmitterShared.IsEmptyGetter);
        IL.Emit(OpCodes.Brtrue, doneIlLabel);
        CsLine($"if (ctx.Bitmaps[{slot}].IsEmpty) goto {doneCsName};");
    }

    /// <summary>Emit <c>if ((long)ctx.Bitmaps[0].Count &gt;= ctx.Limit) goto Done</c>.
    /// Used after each OR-into-bitmap[0] to short-circuit when the limit is reached.</summary>
    public void EmitLimitReachedGoto(Label doneIlLabel, string doneCsName)
    {
        IlLoadBitmapRef(0);
        IL.Emit(OpCodes.Call, IlEmitterShared.CountGetter);
        IL.Emit(OpCodes.Conv_I8);
        IL.Emit(OpCodes.Ldarg_0);
        IL.Emit(OpCodes.Ldfld, IlEmitterShared.CtxLimit);
        IL.Emit(OpCodes.Bge, doneIlLabel);
        CsLine($"if ((long)ctx.Bitmaps[0].Count >= ctx.Limit) goto {doneCsName};");
    }

    // ── Cursor-dispatched posting/match operations ───────────────────────

    /// <summary>Emit a cursor-advancing single-arg dispatch — used by FillFromPostings,
    /// AndWithPostings, AndNotWithPostings. The IL form is
    /// <c>{method}(ctx, cursor); cursor++;</c>.</summary>
    public void EmitCancelledCursorCall(LocalBuilder cursorVar, MethodInfo ilMethod, string csMethodName)
    {
        IlCancellationCheck();
        IL.Emit(OpCodes.Ldarg_0);
        IL.Emit(OpCodes.Ldloc, cursorVar);
        IL.Emit(OpCodes.Call, ilMethod);
        IlAdvanceCursor(cursorVar);
        CsLine("ctx.Token.ThrowIfCancellationRequested();");
        CsLine($"{csMethodName}(ctx, cursor); cursor++;");
    }

    /// <summary>Emit a cursor-advancing two-arg dispatch (OR-with-postings into a
    /// specific bitmap slot). The IL form is <c>{method}(ctx, cursor, bitmapSlot); cursor++;</c>.</summary>
    public void EmitCancelledCursorOrCall(LocalBuilder cursorVar, MethodInfo ilMethod, string csMethodName, int bitmapSlot)
    {
        IlCancellationCheck();
        IL.Emit(OpCodes.Ldarg_0);
        IL.Emit(OpCodes.Ldloc, cursorVar);
        IL.Emit(OpCodes.Ldc_I4, bitmapSlot);
        IL.Emit(OpCodes.Call, ilMethod);
        IlAdvanceCursor(cursorVar);
        CsLine("ctx.Token.ThrowIfCancellationRequested();");
        CsLine($"{csMethodName}(ctx, cursor, {bitmapSlot}); cursor++;");
    }

    /// <summary>Emit <c>QueryPrimitives.CtxFillAllEntries(ctx)</c>.</summary>
    public void EmitFillAllEntries()
    {
        IlCancellationCheck();
        IL.Emit(OpCodes.Ldarg_0);
        IL.Emit(OpCodes.Call, IlEmitterShared.CtxFillAllEntries);
        CsLine("ctx.Token.ThrowIfCancellationRequested();");
        CsLine("QueryPrimitives.CtxFillAllEntries(ctx);");
    }

    /// <summary>Emit an unconditional <c>goto Done</c> at both backends.</summary>
    public void EmitGotoDone(Label doneIlLabel, string doneCsName)
    {
        IL.Emit(OpCodes.Br, doneIlLabel);
        CsLine($"goto {doneCsName};");
    }
}
