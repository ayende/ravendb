using System.Reflection;
using System.Reflection.Emit;
using System.Runtime.InteropServices;
using Voron.Data.RoaringBitmaps;

namespace Corax.Querying.Planning;

/// <summary>Statement-level emission helpers used by <see cref="QueryIlEmitter"/>.
/// Each helper emits one IL sequence AND the matching C# line(s) so the two
/// backends cannot drift — same drift-prevention discipline as the value-stack
/// primitives in <see cref="DualEmit"/> but at statement granularity (no operand
/// stack involvement; nothing is pushed or popped on CsStack).</summary>
[StructLayout(LayoutKind.Auto)]
internal ref partial struct DualEmit
{
    public void IlLoadBitmapRef(int slot)
    {
        Il.Emit(OpCodes.Ldarg_0);
        Il.Emit(OpCodes.Ldfld, IlEmitterShared.CtxBitmaps);
        IlEmitterShared.EmitLdcI4(Il, slot);
        Il.Emit(OpCodes.Ldelema, typeof(RoaringBitmap));
    }

    public void IlCancellationCheck()
    {
        Il.Emit(OpCodes.Ldarg_0);
        Il.Emit(OpCodes.Ldflda, IlEmitterShared.CtxToken);
        Il.Emit(OpCodes.Call, IlEmitterShared.ThrowIfCancelled);
        CsLine("ctx.Token.ThrowIfCancellationRequested();");
    }

    private void IlAdvanceCursor(LocalBuilder cursorVar)
    {
        Il.Emit(OpCodes.Ldloc, cursorVar);
        Il.Emit(OpCodes.Ldc_I4_1);
        Il.Emit(OpCodes.Add);
        Il.Emit(OpCodes.Stloc, cursorVar);
        CsLine("cursor++;");
    }

    public void EmitBitmapBinaryOp(int target, int source, MethodInfo ilMethod, string csOp)
    {
        IlLoadBitmapRef(target);
        IlLoadBitmapRef(source);
        Il.Emit(OpCodes.Call, ilMethod);
        CsLine($"ctx.Bitmaps[{target}].{csOp}(ref ctx.Bitmaps[{source}]);");
    }

    public void EmitBitmapUnaryCall(int slot, MethodInfo ilMethod, string csOp)
    {
        IlLoadBitmapRef(slot);
        Il.Emit(OpCodes.Call, ilMethod);
        CsLine($"ctx.Bitmaps[{slot}].{csOp}();");
    }

    public void EmitBitmapEmptyGoto(int slot, Label doneIlLabel, string doneCsName)
    {
        IlLoadBitmapRef(slot);
        Il.Emit(OpCodes.Call, IlEmitterShared.IsEmptyGetter);
        Il.Emit(OpCodes.Brtrue, doneIlLabel);
        CsLine($"if (ctx.Bitmaps[{slot}].IsEmpty) goto {doneCsName};");
    }

    public void EmitLimitReachedGoto(Label doneIlLabel, string doneCsName)
    {
        IlLoadBitmapRef(0);
        Il.Emit(OpCodes.Call, IlEmitterShared.CountGetter);
        Il.Emit(OpCodes.Ldarg_0);
        Il.Emit(OpCodes.Ldfld, IlEmitterShared.CtxLimit);
        Il.Emit(OpCodes.Bge, doneIlLabel);
        CsLine($"if (ctx.Bitmaps[0].Count >= ctx.Limit) goto {doneCsName};");
    }

    public void EmitCancelledCursorSlotCall(LocalBuilder cursorVar, MethodInfo ilMethod, string csMethodName, int bitmapSlot)
    {
        IlCancellationCheck();
        Il.Emit(OpCodes.Ldarg_0);
        Il.Emit(OpCodes.Ldloc, cursorVar);
        Il.Emit(OpCodes.Ldc_I4, bitmapSlot);
        Il.Emit(OpCodes.Call, ilMethod);
        CsLine($"{csMethodName}(ctx, cursor, bitmapSlot: {bitmapSlot});");
        IlAdvanceCursor(cursorVar);
    }

    public void EmitFillAllEntries(int bitmapSlot)
    {
        IlCancellationCheck();
        Il.Emit(OpCodes.Ldarg_0);
        IlEmitterShared.EmitLdcI4(Il, bitmapSlot);
        Il.Emit(OpCodes.Call, IlEmitterShared.CtxFillAllEntries);
        CsLine($"QueryPrimitives.CtxFillAllEntries(ctx, {bitmapSlot});");
    }

    public void EmitGotoDone(Label doneIlLabel, string doneCsName)
    {
        Il.Emit(OpCodes.Br, doneIlLabel);
        CsLine($"goto {doneCsName};");
    }
}
