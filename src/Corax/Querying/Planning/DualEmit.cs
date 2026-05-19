using System.Collections.Generic;
using System.Diagnostics;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;
using Voron;

namespace Corax.Querying.Planning;

/// <summary>
/// Bound pair of an IL <see cref="Label"/> and its mirroring C# label name.
/// Defined together so a missing C# branch instantly mismatches an IL one.
/// </summary>
internal readonly struct LabelPair(Label il, string name)
{
    public readonly Label IL = il;
    public readonly string Name = name;
}

/// <summary>
/// Dual-backend emission helper. Every primitive emits one IL operation AND
/// the matching effect on a textual C# operand stack — value producers push a
/// textual fragment; control-flow primitives pop fragments and write a C#
/// statement to the <see cref="Cs"/> buffer. This guarantees the IL and C#
/// outputs cannot drift: forgetting to wire up one backend in a new primitive
/// imbalances the C# operand stack and is caught by the assert in
/// <see cref="MarkLabel"/> / <see cref="VerifyEmpty"/> / <see cref="Ret"/>.
///
/// The C# operand stack is parallel to the IL evaluation stack:
/// every IL "push" corresponds to a textual fragment push, every IL "pop"
/// to a textual fragment pop.
/// </summary>
internal ref struct DualEmit
{
    public ILGenerator IL;
    public StringBuilder Cs;
    public Stack<string> CsStack;
    public int LabelCounter;
    public int TempCounter;
    public string Indent;

    public DualEmit(ILGenerator il, StringBuilder cs, string indent = "    ")
    {
        IL = il;
        Cs = cs;
        CsStack = new Stack<string>();
        LabelCounter = 0;
        TempCounter = 0;
        Indent = indent;
    }

    // --- Free-form C# text emission (used by statement-level helpers) ---

    public void CsLine(string line)
    {
        Cs.Append(Indent);
        Cs.AppendLine(line);
    }

    public void CsRaw(string text) => Cs.Append(text);
    public void CsRawLine(string text) => Cs.AppendLine(text);

    // --- Labels & temps ---

    public LabelPair DefineLabelPair(string prefix)
    {
        var name = $"{prefix}_{LabelCounter++}";
        return new LabelPair(IL.DefineLabel(), name);
    }

    public void MarkLabel(LabelPair l)
    {
        IL.MarkLabel(l.IL);
        Cs.Append(Indent);
        Cs.Append(l.Name);
        Cs.AppendLine(":;");
        // Cs operand stack must be empty when a label is reached: C# labels are not
        // expression-stack-aware. If a primitive pushed a fragment that the IL
        // consumed implicitly via control flow, that's a wiring bug.
        Debug.Assert(CsStack.Count == 0,
            $"DualEmit: C# operand stack not empty at label {l.Name}: [{string.Join(", ", CsStack)}]");
    }

    public string NewTempName(string hint) => $"{hint}_{TempCounter++}";

    public string DeclareTempBool(string hint)
    {
        var name = NewTempName(hint);
        CsLine($"bool {name};");
        return name;
    }

    /// <summary>Pop the top of the C# operand stack and assign it to <paramref name="name"/>.
    /// IL is a no-op (caller already produced an IL value); this only flushes the textual
    /// fragment into an assignment so subsequent C# can refer to the temp.</summary>
    public void AssignTemp(string name)
    {
        var v = CsStack.Pop();
        CsLine($"{name} = {v};");
    }

    /// <summary>Push a temp's name onto the C# operand stack (no IL effect).
    /// Used after a join point where the IL value lives on the IL eval stack and
    /// the C# equivalent lives in a previously-declared temp.</summary>
    public void PushTempName(string name) => CsStack.Push(name);

    // --- Constants ---

    public void PushConstBool(bool v)
    {
        IL.Emit(v ? OpCodes.Ldc_I4_1 : OpCodes.Ldc_I4_0);
        CsStack.Push(v ? "true" : "false");
    }

    public void PushConstInt(int v)
    {
        IlEmitterShared.EmitLdcI4(IL, v);
        CsStack.Push(v.ToString());
    }

    // --- Loaders for the residual-scan emitter (push value AND fragment) ---

    public void LoadReaderCurrentLong(LocalBuilder readerRef)
    {
        IL.Emit(OpCodes.Ldloc, readerRef);
        IL.Emit(OpCodes.Ldfld, IlEmitterShared.ReaderCurrentLong);
        CsStack.Push("reader.CurrentLong");
    }

    public void LoadReaderCurrentDouble(LocalBuilder readerRef)
    {
        IL.Emit(OpCodes.Ldloc, readerRef);
        IL.Emit(OpCodes.Ldfld, IlEmitterShared.ReaderCurrentDouble);
        CsStack.Push("reader.CurrentDouble");
    }

    public void LoadReaderDecodedSlice(LocalBuilder readerRef)
    {
        IL.Emit(OpCodes.Ldloc, readerRef);
        IL.Emit(OpCodes.Ldfld, IlEmitterShared.ReaderCurrent);
        IL.Emit(OpCodes.Callvirt, IlEmitterShared.CompactKeyDecoded);
        CsStack.Push("reader.Current.Decoded()");
    }

    public void LoadLongParam(int idx)
    {
        IL.Emit(OpCodes.Ldarg_0);
        IL.Emit(OpCodes.Callvirt, IlEmitterShared.CtxLongParams);
        IlEmitterShared.EmitLdcI4(IL, idx);
        IL.Emit(OpCodes.Ldelem_I8);
        CsStack.Push($"ctx.ResidualLongParams[{idx}]");
    }

    public void LoadDoubleParam(int idx)
    {
        IL.Emit(OpCodes.Ldarg_0);
        IL.Emit(OpCodes.Callvirt, IlEmitterShared.CtxDoubleParams);
        IlEmitterShared.EmitLdcI4(IL, idx);
        IL.Emit(OpCodes.Ldelem_R8);
        CsStack.Push($"ctx.ResidualDoubleParams[{idx}]");
    }

    /// <summary>Push <c>ctx.ResidualSliceParams[idx].AsReadOnlySpan()</c> onto both stacks.</summary>
    public void LoadSliceSpan(int idx)
    {
        IL.Emit(OpCodes.Ldarg_0);
        IL.Emit(OpCodes.Callvirt, IlEmitterShared.CtxSliceParams);
        IlEmitterShared.EmitLdcI4(IL, idx);
        IL.Emit(OpCodes.Ldelema, typeof(Slice));
        IL.Emit(OpCodes.Call, IlEmitterShared.SliceAsReadOnlySpan);
        CsStack.Push($"ctx.ResidualSliceParams[{idx}].AsReadOnlySpan()");
    }

    /// <summary>Push the field-root-page long for the given residual index.</summary>
    public void LoadFieldRootPage(int rootIdx)
    {
        IL.Emit(OpCodes.Ldarg_0);
        IL.Emit(OpCodes.Callvirt, IlEmitterShared.CtxFieldRootPages);
        IlEmitterShared.EmitLdcI4(IL, rootIdx);
        IL.Emit(OpCodes.Ldelem_I8);
        CsStack.Push($"ctx.ResidualFieldRootPages[{rootIdx}]");
    }

    // --- Pure compares: pop 2, push 1 (no C# statement, expression composes) ---

    public void Ceq()
    {
        IL.Emit(OpCodes.Ceq);
        var b = CsStack.Pop(); var a = CsStack.Pop();
        CsStack.Push($"({a} == {b})");
    }

    public void Clt()
    {
        IL.Emit(OpCodes.Clt);
        var b = CsStack.Pop(); var a = CsStack.Pop();
        CsStack.Push($"({a} < {b})");
    }

    public void Cgt()
    {
        IL.Emit(OpCodes.Cgt);
        var b = CsStack.Pop(); var a = CsStack.Pop();
        CsStack.Push($"({a} > {b})");
    }

    /// <summary>Compose <c>!(top)</c>: pop and re-push the negation.
    /// IL equivalent is <c>ldc.i4.0 ceq</c> (a 0/1 boolean flip).</summary>
    public void LogicalNot()
    {
        IL.Emit(OpCodes.Ldc_I4_0);
        IL.Emit(OpCodes.Ceq);
        var a = CsStack.Pop();
        CsStack.Push($"!{a}");
    }

    // --- Calls returning a value: caller supplies method + C# expression template ---

    /// <summary>Emit a call to <paramref name="method"/> that consumes <paramref name="arity"/>
    /// stack slots and returns one value. The C# expression is built from the popped fragments
    /// via <paramref name="csTemplate"/> — a printf-style template where {0}..{N-1} are the
    /// arguments in stack order (top of stack is the highest index).</summary>
    public void CallReturning(MethodInfo method, int arity, string csTemplate, OpCode opcode = default)
    {
        var op = opcode == default ? OpCodes.Call : opcode;
        IL.Emit(op, method);
        var args = new string[arity];
        for (int i = arity - 1; i >= 0; i--) args[i] = CsStack.Pop();
        CsStack.Push(string.Format(csTemplate, args));
    }

    // --- Conditional branches: pop fragments, write a C# if/goto ---

    public void BranchLT(LabelPair l)
    {
        IL.Emit(OpCodes.Blt, l.IL);
        var b = CsStack.Pop(); var a = CsStack.Pop();
        CsLine($"if ({a} < {b}) goto {l.Name};");
    }

    public void BranchLTUnsigned(LabelPair l)
    {
        IL.Emit(OpCodes.Blt_Un, l.IL);
        var b = CsStack.Pop(); var a = CsStack.Pop();
        CsLine($"if ({a} < {b}) goto {l.Name};");
    }

    public void BranchGT(LabelPair l)
    {
        IL.Emit(OpCodes.Bgt, l.IL);
        var b = CsStack.Pop(); var a = CsStack.Pop();
        CsLine($"if ({a} > {b}) goto {l.Name};");
    }

    public void BranchGTUnsigned(LabelPair l)
    {
        IL.Emit(OpCodes.Bgt_Un, l.IL);
        var b = CsStack.Pop(); var a = CsStack.Pop();
        CsLine($"if ({a} > {b}) goto {l.Name};");
    }

    public void BranchGE(LabelPair l)
    {
        IL.Emit(OpCodes.Bge, l.IL);
        var b = CsStack.Pop(); var a = CsStack.Pop();
        CsLine($"if ({a} >= {b}) goto {l.Name};");
    }

    public void BranchEq(LabelPair l)
    {
        IL.Emit(OpCodes.Beq, l.IL);
        var b = CsStack.Pop(); var a = CsStack.Pop();
        CsLine($"if ({a} == {b}) goto {l.Name};");
    }

    public void BranchTrue(LabelPair l)
    {
        IL.Emit(OpCodes.Brtrue, l.IL);
        var a = CsStack.Pop();
        CsLine($"if ({a}) goto {l.Name};");
    }

    public void BranchFalse(LabelPair l)
    {
        IL.Emit(OpCodes.Brfalse, l.IL);
        var a = CsStack.Pop();
        CsLine($"if (!{a}) goto {l.Name};");
    }

    public void GotoAlways(LabelPair l)
    {
        IL.Emit(OpCodes.Br, l.IL);
        CsLine($"goto {l.Name};");
    }

    /// <summary>Discard a value from the C# stack with no IL counterpart — used by
    /// primitives that compose into expressions consumed implicitly by a later
    /// statement (e.g. <c>CallReturning</c> producing a span used by the next call).</summary>
    public void DiscardTop() => CsStack.Pop();

    /// <summary>Throw if any operand fragment is left on the C# stack — invoke before
    /// a Ret or before exiting an emit region that should have balanced both stacks.</summary>
    [Conditional("DEBUG")]
    public void VerifyEmpty(string where)
    {
        Debug.Assert(CsStack.Count == 0,
            $"DualEmit: C# operand stack not empty at {where}: [{string.Join(", ", CsStack)}]");
    }
}
