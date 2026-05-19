using System.Collections.Generic;
using System.Diagnostics;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;
using Voron;

namespace Corax.Querying.Planning;

internal readonly record struct LabelPair(Label Il, string Name);

/// <summary>
/// Dual-backend emission helper. Every primitive emits one IL operation AND
/// the matching effect on a textual C# operand stack — value producers push a
/// textual fragment; control-flow primitives pop fragments and write a C#
/// statement to the <see cref="cs"/> buffer.
///
/// The C# operand stack is parallel to the IL evaluation stack:
/// every IL "push" corresponds to a textual fragment push, every IL "pop"
/// to a textual fragment pop.
/// </summary>
internal ref partial struct DualEmit(ILGenerator il, StringBuilder cs)
{
    public readonly ILGenerator Il = il;
    public readonly Stack<string> CsStack = new();
    private readonly Dictionary<LocalBuilder, string> _locals = new();
    private readonly Dictionary<byte, string> _args = new();
    private int _labelCounter = 0;
    private int _tempCounter = 0;

    // ── C# output ────────────────────────────────────────────────────

    public void CsLine(string line) => cs.AppendLine(line);

    // ── Labels ───────────────────────────────────────────────────────

    public LabelPair DefineLabelPair(string prefix) => new(Il.DefineLabel(), $"{prefix}_{_labelCounter++}");

    /// <summary>Define a label with an exact name (no counter suffix).
    /// Use for well-known labels like "Done" or "EntryScan".</summary>
    public LabelPair DefineNamedLabel(string exactName) => new(Il.DefineLabel(), exactName);

    public void MarkLabel(LabelPair l)
    {
        Il.MarkLabel(l.Il);
        cs.Append(l.Name);
        cs.AppendLine(":;");
        Debug.Assert(CsStack.Count == 0,
            $"DualEmit: C# operand stack not empty at label {l.Name}: [{string.Join(", ", CsStack)}]");
    }

    // ── Temps ────────────────────────────────────────────────────────

    private string NewTempName(string hint) => $"{hint}_{_tempCounter++}";

    public string DeclareTempBool(string hint)
    {
        var name = NewTempName(hint);
        CsLine($"bool {name};");
        return name;
    }

    public void PushTempName(string name) => CsStack.Push(name);

    // ── Local variable management ────────────────────────────────────

    /// <summary>Declare an IL local and track its C# name. Does not emit a C# declaration —
    /// the first store or explicit CsLine handles that.</summary>
    public LocalBuilder DeclareLocal(System.Type type, string csName)
    {
        var local = Il.DeclareLocal(type);
        _locals[local] = csName;
        return local;
    }

    /// <summary>Declare a by-ref IL local and track its C# name.</summary>
    public LocalBuilder DeclareLocalRef(System.Type type, string csName)
    {
        var local = Il.DeclareLocal(type.MakeByRefType());
        _locals[local] = csName;
        return local;
    }

    /// <summary>Get the tracked C# name for a local.</summary>
    public string GetLocalName(LocalBuilder local) => _locals[local];

    /// <summary>Push the local's value onto both stacks.</summary>
    public void LoadLocal(LocalBuilder local)
    {
        Il.Emit(OpCodes.Ldloc, local);
        CsStack.Push(_locals[local]);
    }

    /// <summary>Pop the top of both stacks into the local.</summary>
    public void StoreLocal(LocalBuilder local)
    {
        Il.Emit(OpCodes.Stloc, local);
        var val = CsStack.Pop();
        CsLine($"{_locals[local]} = {val};");
    }

    /// <summary>Store an integer constant into a local.</summary>
    public void StoreLocalConst(LocalBuilder local, int value)
    {
        IlEmitterShared.EmitLdcI4(Il, value);
        Il.Emit(OpCodes.Stloc, local);
        CsLine($"{_locals[local]} = {value};");
    }

    /// <summary>Increment an integer local by 1.</summary>
    public void IncrementLocal(LocalBuilder local)
    {
        Il.Emit(OpCodes.Ldloc, local);
        Il.Emit(OpCodes.Ldc_I4_1);
        Il.Emit(OpCodes.Add);
        Il.Emit(OpCodes.Stloc, local);
        CsLine($"{_locals[local]}++;");
    }

    // ── Argument management ──────────────────────────────────────────

    /// <summary>Register a C# name for a method argument (no IL emitted).</summary>
    public void RegisterArg(byte index, string csName) => _args[index] = csName;

    /// <summary>Get the tracked C# name for an argument.</summary>
    public string GetArgName(byte index) => _args[index];

    /// <summary>Push an argument value onto both stacks.</summary>
    public void LoadArg(byte index)
    {
        Il.Emit(OpCodes.Ldarg, (int)index);
        CsStack.Push(_args[index]);
    }

    /// <summary>Push the address of a Span argument for calling instance methods like .Length or indexer.
    /// CsStack receives the arg name (without ref — Span method calls don't need it in C#).</summary>
    public void LoadArgAddress(byte index)
    {
        Il.Emit(OpCodes.Ldarga_S, index);
        CsStack.Push(_args[index]);
    }

    // ── Return ───────────────────────────────────────────────────────

    /// <summary>Return void.</summary>
    public void EmitRetVoid()
    {
        Il.Emit(OpCodes.Ret);
        CsLine("return;");
    }

    /// <summary>Return the value currently on top of both stacks.</summary>
    public void EmitReturn()
    {
        Il.Emit(OpCodes.Ret);
        var val = CsStack.Pop();
        CsLine($"return {val};");
    }

    public void PushConstBool(bool v)
    {
        Il.Emit(v ? OpCodes.Ldc_I4_1 : OpCodes.Ldc_I4_0);
        CsStack.Push(v ? "true" : "false");
    }

    public void PushConstInt(int v)
    {
        IlEmitterShared.EmitLdcI4(Il, v);
        CsStack.Push(v.ToString());
    }

    public void LoadReaderCurrentLong(LocalBuilder readerRef)
    {
        Il.Emit(OpCodes.Ldloc, readerRef);
        Il.Emit(OpCodes.Ldfld, IlEmitterShared.ReaderCurrentLong);
        CsStack.Push("reader.CurrentLong");
    }

    public void LoadReaderCurrentDouble(LocalBuilder readerRef)
    {
        Il.Emit(OpCodes.Ldloc, readerRef);
        Il.Emit(OpCodes.Ldfld, IlEmitterShared.ReaderCurrentDouble);
        CsStack.Push("reader.CurrentDouble");
    }

    public void LoadReaderDecodedSlice(LocalBuilder readerRef)
    {
        Il.Emit(OpCodes.Ldloc, readerRef);
        Il.Emit(OpCodes.Ldfld, IlEmitterShared.ReaderCurrent);
        Il.Emit(OpCodes.Callvirt, IlEmitterShared.CompactKeyDecoded);
        CsStack.Push("reader.Current.Decoded()");
    }

    public void LoadLongParam(int idx)
    {
        Il.Emit(OpCodes.Ldarg_0);
        Il.Emit(OpCodes.Callvirt, IlEmitterShared.CtxLongParams);
        IlEmitterShared.EmitLdcI4(Il, idx);
        Il.Emit(OpCodes.Ldelem_I8);
        CsStack.Push($"ctx.ResidualLongParams[{idx}]");
    }

    public void LoadDoubleParam(int idx)
    {
        Il.Emit(OpCodes.Ldarg_0);
        Il.Emit(OpCodes.Callvirt, IlEmitterShared.CtxDoubleParams);
        IlEmitterShared.EmitLdcI4(Il, idx);
        Il.Emit(OpCodes.Ldelem_R8);
        CsStack.Push($"ctx.ResidualDoubleParams[{idx}]");
    }

    public void LoadSliceSpan(int idx)
    {
        Il.Emit(OpCodes.Ldarg_0);
        Il.Emit(OpCodes.Callvirt, IlEmitterShared.CtxSliceParams);
        IlEmitterShared.EmitLdcI4(Il, idx);
        Il.Emit(OpCodes.Ldelema, typeof(Slice));
        Il.Emit(OpCodes.Call, IlEmitterShared.SliceAsReadOnlySpan);
        CsStack.Push($"ctx.ResidualSliceParams[{idx}].AsReadOnlySpan()");
    }

    public void LoadFieldRootPage(int rootIdx)
    {
        Il.Emit(OpCodes.Ldarg_0);
        Il.Emit(OpCodes.Callvirt, IlEmitterShared.CtxFieldRootPages);
        IlEmitterShared.EmitLdcI4(Il, rootIdx);
        Il.Emit(OpCodes.Ldelem_I8);
        CsStack.Push($"ctx.ResidualFieldRootPages[{rootIdx}]");
    }

    public void Ceq()
    {
        Il.Emit(OpCodes.Ceq);
        var b = CsStack.Pop(); var a = CsStack.Pop();
        CsStack.Push($"({a} == {b})");
    }

    public void Clt()
    {
        Il.Emit(OpCodes.Clt);
        var b = CsStack.Pop(); var a = CsStack.Pop();
        CsStack.Push($"({a} < {b})");
    }

    public void Cgt()
    {
        Il.Emit(OpCodes.Cgt);
        var b = CsStack.Pop(); var a = CsStack.Pop();
        CsStack.Push($"({a} > {b})");
    }

    public void LogicalNot()
    {
        Il.Emit(OpCodes.Ldc_I4_0);
        Il.Emit(OpCodes.Ceq);
        var a = CsStack.Pop();
        CsStack.Push($"{a} is false");
    }

    public void CallReturning(MethodInfo method, int arity, string csTemplate)
    {
        Il.Emit(OpCodes.Call, method);
        var args = new string[arity];
        for (int i = arity - 1; i >= 0; i--) args[i] = CsStack.Pop();
        CsStack.Push(string.Format(csTemplate, args));
    }

    // --- Conditional branches: pop fragments, write a C# if/goto ---

    public void BranchLT(LabelPair l)
    {
        Il.Emit(OpCodes.Blt, l.Il);
        var b = CsStack.Pop(); var a = CsStack.Pop();
        CsLine($"if ({a} < {b}) goto {l.Name};");
    }

    public void BranchLTUnsigned(LabelPair l)
    {
        Il.Emit(OpCodes.Blt_Un, l.Il);
        var b = CsStack.Pop(); var a = CsStack.Pop();
        CsLine($"if ({a} < {b}) goto {l.Name};");
    }

    public void BranchGT(LabelPair l)
    {
        Il.Emit(OpCodes.Bgt, l.Il);
        var b = CsStack.Pop(); var a = CsStack.Pop();
        CsLine($"if ({a} > {b}) goto {l.Name};");
    }

    public void BranchGTUnsigned(LabelPair l)
    {
        Il.Emit(OpCodes.Bgt_Un, l.Il);
        var b = CsStack.Pop(); var a = CsStack.Pop();
        CsLine($"if ({a} > {b}) goto {l.Name};");
    }

    public void GotoAlways(LabelPair l)
    {
        Il.Emit(OpCodes.Br, l.Il);
        CsLine($"goto {l.Name};");
    }
}
