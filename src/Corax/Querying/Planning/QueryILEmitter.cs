using System;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;
using System.Threading;
using Corax.Querying.Matches.Meta;
using Corax.Utils.RoaringBitmaps;

namespace Corax.Querying.Planning;

/// <summary>
/// Emits a DynamicMethod from a QueryPlan. The generated IL is a flat sequence
/// of direct calls to bitmap operations, with real branch instructions for the
/// goto pattern (CheckAndMaybeEntryScan).
///
/// The JIT compiles the emitted IL to native code, enabling inlining of small
/// methods (IsEmpty, ThrowIfCancellationRequested) and optimal register allocation
/// across the entire query execution path.
///
/// Generation time: ~50μs for a typical 5-operand AND query.
/// The generated delegate is GC-collectible when unreferenced.
/// </summary>
public static class QueryILEmitter
{
    /// <summary>
    /// Delegate for the compiled bitmap-fill function.
    /// Called by CompiledQueryMatch.Execute() to populate the bitmap.
    /// </summary>
    /// <param name="resolvedMatches">IQueryMatch instances for each clause</param>
    /// <param name="bitmap">Main accumulator bitmap (populated by this delegate)</param>
    /// <param name="tempBitmap">Scratch bitmap for AND/ANDNOT operations</param>
    /// <param name="token">Cancellation token checked between ops</param>
    public delegate void CompiledExecuteDelegate(
        IQueryMatch[] resolvedMatches,
        ref RoaringBitmap bitmap,
        ref RoaringBitmap tempBitmap,
        CancellationToken token);

    private const int FillBufferSize = 4096;

    // Cached MethodInfo for methods called by emitted IL.
    // Resolved once at class load, used by every EmitDelegate call.
    private static readonly MethodInfo s_fillMethod =
        typeof(IQueryMatch).GetMethod(nameof(IQueryMatch.Fill))!;
    private static readonly MethodInfo s_addRange =
        typeof(RoaringBitmap).GetMethod(nameof(RoaringBitmap.AddRange))!;
    private static readonly MethodInfo s_andWith =
        typeof(RoaringBitmap).GetMethod(nameof(RoaringBitmap.AndWith))!;
    private static readonly MethodInfo s_andNotWith =
        typeof(RoaringBitmap).GetMethod(nameof(RoaringBitmap.AndNotWith))!;
    private static readonly MethodInfo s_clear =
        typeof(RoaringBitmap).GetMethod(nameof(RoaringBitmap.Clear))!;
    private static readonly MethodInfo s_isEmptyGetter =
        typeof(RoaringBitmap).GetProperty(nameof(RoaringBitmap.IsEmpty))!.GetGetMethod()!;
    private static readonly MethodInfo s_repairAfterLazy =
        typeof(RoaringBitmap).GetMethod(nameof(RoaringBitmap.RepairAfterLazy))!;
    private static readonly MethodInfo s_bitmapCountGetter =
        typeof(RoaringBitmap).GetProperty(nameof(RoaringBitmap.Count))!.GetGetMethod()!;
    private static readonly MethodInfo s_matchCountGetter =
        typeof(IQueryMatch).GetProperty(nameof(IQueryMatch.Count))!.GetGetMethod()!;
    private static readonly MethodInfo s_throwIfCancelled =
        typeof(CancellationToken).GetMethod(nameof(CancellationToken.ThrowIfCancellationRequested))!;
    private static readonly ConstructorInfo s_spanCtor =
        typeof(Span<long>).GetConstructor(new[] { typeof(void*), typeof(int) })!;
    private static readonly MethodInfo s_spanSlice =
        typeof(Span<long>).GetMethod(nameof(Span<long>.Slice), new[] { typeof(int), typeof(int) })!;
    private static readonly MethodInfo s_spanToReadOnly =
        typeof(Span<long>).GetMethod("op_Implicit", new[] { typeof(Span<long>) })!;

    /// <summary>
    /// Emit a compiled delegate from a QueryPlan.
    /// The emitted IL is a flat sequence of bitmap operations with:
    /// - stackalloc buffer (localloc, no zero-init)
    /// - inline Fill loops (callvirt IQueryMatch.Fill + AddRange)
    /// - real brtrue branches for goto pattern
    /// - early-exit on empty bitmap after AND
    /// </summary>
    public static CompiledExecuteDelegate EmitDelegate(QueryPlan plan)
    {
        var ops = plan.Ops;
        if (ops == null || ops.Length == 0)
            return EmptyExecute;

        var dm = new DynamicMethod(
            "CompiledQuery",
            typeof(void),
            new[]
            {
                typeof(IQueryMatch[]),                     // arg0: resolvedMatches
                typeof(RoaringBitmap).MakeByRefType(),     // arg1: ref bitmap
                typeof(RoaringBitmap).MakeByRefType(),     // arg2: ref tempBitmap
                typeof(CancellationToken)                  // arg3: token
            },
            typeof(QueryILEmitter).Module,
            skipVisibility: true);

        dm.InitLocals = false; // SkipLocalsInit — localloc is uninitialized anyway

        var il = dm.GetILGenerator();

        // Locals:
        //   0: Span<long> buffer
        //   1: int read
        var bufferLocal = il.DeclareLocal(typeof(Span<long>));
        var readLocal = il.DeclareLocal(typeof(int));

        var doneLabel = il.DefineLabel();

        // === Span<long> buffer = stackalloc long[FillBufferSize] ===
        EmitLdcI4(il, FillBufferSize);
        il.Emit(OpCodes.Conv_U);
        il.Emit(OpCodes.Sizeof, typeof(long));
        il.Emit(OpCodes.Mul_Ovf_Un);
        il.Emit(OpCodes.Localloc);
        EmitLdcI4(il, FillBufferSize);
        il.Emit(OpCodes.Newobj, s_spanCtor);
        il.Emit(OpCodes.Stloc, bufferLocal);

        // === Emit ops ===
        for (int i = 0; i < ops.Length; i++)
        {
            ref PlanOp op = ref ops[i];

            switch (op.Kind)
            {
                case PlanOpKind.FillFromPostings:
                case PlanOpKind.DirectIterate:
                {
                    // token.ThrowIfCancellationRequested()
                    EmitCancellationCheck(il);
                    // Fill loop: materialize matches[paramIndex] into bitmap
                    EmitFillLoop(il, op.ParamIndex, bufferLocal, readLocal, isMainBitmap: true);
                    break;
                }

                case PlanOpKind.AndWithPostings:
                {
                    EmitCancellationCheck(il);

                    // tempBitmap.Clear()
                    il.Emit(OpCodes.Ldarg_2);
                    il.Emit(OpCodes.Call, s_clear);

                    // Fill loop into tempBitmap
                    EmitFillLoop(il, op.ParamIndex, bufferLocal, readLocal, isMainBitmap: false);

                    // bitmap.AndWith(ref tempBitmap)
                    il.Emit(OpCodes.Ldarg_1);
                    il.Emit(OpCodes.Ldarg_2);
                    il.Emit(OpCodes.Call, s_andWith);

                    // if (bitmap.IsEmpty) goto done
                    il.Emit(OpCodes.Ldarg_1);
                    il.Emit(OpCodes.Call, s_isEmptyGetter);
                    il.Emit(OpCodes.Brtrue, doneLabel);
                    break;
                }

                case PlanOpKind.OrWithPostings:
                case PlanOpKind.LazyOrWithPostings:
                {
                    EmitCancellationCheck(il);
                    // Fill loop directly into main bitmap (OR is idempotent)
                    EmitFillLoop(il, op.ParamIndex, bufferLocal, readLocal, isMainBitmap: true);
                    break;
                }

                case PlanOpKind.AndNotWithPostings:
                {
                    EmitCancellationCheck(il);

                    // tempBitmap.Clear()
                    il.Emit(OpCodes.Ldarg_2);
                    il.Emit(OpCodes.Call, s_clear);

                    // Fill loop into tempBitmap
                    EmitFillLoop(il, op.ParamIndex, bufferLocal, readLocal, isMainBitmap: false);

                    // bitmap.AndNotWith(ref tempBitmap)
                    il.Emit(OpCodes.Ldarg_1);
                    il.Emit(OpCodes.Ldarg_2);
                    il.Emit(OpCodes.Call, s_andNotWith);
                    break;
                }

                case PlanOpKind.RepairAfterLazy:
                {
                    il.Emit(OpCodes.Ldarg_1);
                    il.Emit(OpCodes.Call, s_repairAfterLazy);
                    break;
                }

                case PlanOpKind.CheckAndMaybeEntryScan:
                {
                    // Runtime check: if bitmap is small relative to the next operand's
                    // cardinality, skip the remaining AND steps — the bitmap is already
                    // small enough to iterate directly. This is the dynamic unary promotion.
                    //
                    // if (bitmap.Count * 64 < matches[paramIndex].Count && bitmap.Count < 32000)
                    //     goto done;  // skip remaining ANDs

                    var skipCheck = il.DefineLabel();

                    // bitmap.Count
                    il.Emit(OpCodes.Ldarg_1);               // ref bitmap
                    il.Emit(OpCodes.Call, s_bitmapCountGetter);
                    il.Emit(OpCodes.Conv_I8);                // ensure long

                    // Check bitmap.Count < 32000 first (cheap check)
                    il.Emit(OpCodes.Ldc_I8, 32000L);
                    il.Emit(OpCodes.Bge, skipCheck);         // if bitmap.Count >= 32K, skip (too large for entry scan)

                    // bitmap.Count * 64
                    il.Emit(OpCodes.Ldarg_1);
                    il.Emit(OpCodes.Call, s_bitmapCountGetter);
                    il.Emit(OpCodes.Conv_I8);
                    il.Emit(OpCodes.Ldc_I4, 64);
                    il.Emit(OpCodes.Conv_I8);
                    il.Emit(OpCodes.Mul);

                    // matches[paramIndex].Count
                    il.Emit(OpCodes.Ldarg_0);               // matches array
                    EmitLdcI4(il, op.ParamIndex);
                    il.Emit(OpCodes.Ldelem_Ref);
                    il.Emit(OpCodes.Callvirt, s_matchCountGetter);

                    // if (bitmap.Count * 64 < match.Count) goto done
                    il.Emit(OpCodes.Blt, doneLabel);

                    il.MarkLabel(skipCheck);
                    break;
                }

                case PlanOpKind.IterateInto:
                    il.Emit(OpCodes.Br, doneLabel);
                    break;
            }
        }

        il.MarkLabel(doneLabel);
        il.Emit(OpCodes.Ret);

        return (CompiledExecuteDelegate)dm.CreateDelegate(typeof(CompiledExecuteDelegate));
    }

    /// <summary>
    /// Emit the Fill loop pattern:
    ///   while ((read = matches[paramIndex].Fill(buffer)) > 0)
    ///       targetBitmap.AddRange(buffer.Slice(0, read));
    /// </summary>
    private static void EmitFillLoop(ILGenerator il, int paramIndex,
        LocalBuilder bufferLocal, LocalBuilder readLocal, bool isMainBitmap)
    {
        var loopStart = il.DefineLabel();
        var loopEnd = il.DefineLabel();

        il.MarkLabel(loopStart);

        // read = matches[paramIndex].Fill(buffer)
        il.Emit(OpCodes.Ldarg_0);                // matches array
        EmitLdcI4(il, paramIndex);                // index
        il.Emit(OpCodes.Ldelem_Ref);              // matches[paramIndex]
        il.Emit(OpCodes.Ldloc, bufferLocal);      // buffer
        il.Emit(OpCodes.Callvirt, s_fillMethod);  // .Fill(buffer)
        il.Emit(OpCodes.Stloc, readLocal);        // read = result

        // if (read <= 0) break
        il.Emit(OpCodes.Ldloc, readLocal);
        il.Emit(OpCodes.Ldc_I4_0);
        il.Emit(OpCodes.Ble, loopEnd);

        // targetBitmap.AddRange(buffer.Slice(0, read))
        il.Emit(isMainBitmap ? OpCodes.Ldarg_1 : OpCodes.Ldarg_2); // ref bitmap or ref tempBitmap
        il.Emit(OpCodes.Ldloca_S, bufferLocal);   // &buffer (for instance method call on Span)
        il.Emit(OpCodes.Ldc_I4_0);                // start = 0
        il.Emit(OpCodes.Ldloc, readLocal);         // length = read
        il.Emit(OpCodes.Call, s_spanSlice);        // buffer.Slice(0, read) → Span<long>
        il.Emit(OpCodes.Call, s_spanToReadOnly);   // implicit → ReadOnlySpan<long>
        il.Emit(OpCodes.Call, s_addRange);         // .AddRange(ReadOnlySpan<long>)

        il.Emit(OpCodes.Br, loopStart);

        il.MarkLabel(loopEnd);
    }

    /// <summary>Emit token.ThrowIfCancellationRequested()</summary>
    private static void EmitCancellationCheck(ILGenerator il)
    {
        il.Emit(OpCodes.Ldarga_S, (byte)3);       // &token (arg3)
        il.Emit(OpCodes.Call, s_throwIfCancelled);
    }

    private static void EmitLdcI4(ILGenerator il, int value)
    {
        switch (value)
        {
            case 0: il.Emit(OpCodes.Ldc_I4_0); break;
            case 1: il.Emit(OpCodes.Ldc_I4_1); break;
            case 2: il.Emit(OpCodes.Ldc_I4_2); break;
            case 3: il.Emit(OpCodes.Ldc_I4_3); break;
            case 4: il.Emit(OpCodes.Ldc_I4_4); break;
            case 5: il.Emit(OpCodes.Ldc_I4_5); break;
            case 6: il.Emit(OpCodes.Ldc_I4_6); break;
            case 7: il.Emit(OpCodes.Ldc_I4_7); break;
            case 8: il.Emit(OpCodes.Ldc_I4_8); break;
            default:
                if (value >= -128 && value <= 127)
                    il.Emit(OpCodes.Ldc_I4_S, (sbyte)value);
                else
                    il.Emit(OpCodes.Ldc_I4, value);
                break;
        }
    }

    /// <summary>No-op delegate for empty plans.</summary>
    private static void EmptyExecute(
        IQueryMatch[] resolvedMatches,
        ref RoaringBitmap bitmap,
        ref RoaringBitmap tempBitmap,
        CancellationToken token)
    {
    }

    /// <summary>
    /// Generate EXPLAIN pseudocode from the plan.
    /// Shows the C# equivalent of what the emitted IL does.
    /// </summary>
    public static string GenerateExplainSource(QueryPlan plan)
    {
        if (plan.Ops == null || plan.Ops.Length == 0)
            return "// Empty plan — no ops";

        var sb = new StringBuilder();
        sb.AppendLine("// Compiled query (DynamicMethod IL)");
        sb.AppendLine("Span<long> buffer = stackalloc long[4096];");
        sb.AppendLine("int read;");

        for (int i = 0; i < plan.Ops.Length; i++)
        {
            ref PlanOp op = ref plan.Ops[i];
            switch (op.Kind)
            {
                case PlanOpKind.FillFromPostings:
                case PlanOpKind.DirectIterate:
                    sb.AppendLine($"while ((read = matches[{op.ParamIndex}].Fill(buffer)) > 0) bitmap.AddRange(buffer[..read]); // est {op.EstimatedCardinality:N0}");
                    break;
                case PlanOpKind.AndWithPostings:
                    sb.AppendLine($"tempBitmap.Clear();");
                    sb.AppendLine($"while ((read = matches[{op.ParamIndex}].Fill(buffer)) > 0) tempBitmap.AddRange(buffer[..read]);");
                    sb.AppendLine($"bitmap.AndWith(ref tempBitmap); if (bitmap.IsEmpty) return; // est {op.EstimatedCardinality:N0}");
                    break;
                case PlanOpKind.OrWithPostings:
                    sb.AppendLine($"while ((read = matches[{op.ParamIndex}].Fill(buffer)) > 0) bitmap.AddRange(buffer[..read]);");
                    break;
                case PlanOpKind.LazyOrWithPostings:
                    sb.AppendLine($"while ((read = matches[{op.ParamIndex}].Fill(buffer)) > 0) bitmap.AddRange(buffer[..read]); // lazy");
                    break;
                case PlanOpKind.AndNotWithPostings:
                    sb.AppendLine($"tempBitmap.Clear();");
                    sb.AppendLine($"while ((read = matches[{op.ParamIndex}].Fill(buffer)) > 0) tempBitmap.AddRange(buffer[..read]);");
                    sb.AppendLine($"bitmap.AndNotWith(ref tempBitmap);");
                    break;
                case PlanOpKind.RepairAfterLazy:
                    sb.AppendLine("bitmap.RepairAfterLazy();");
                    break;
                case PlanOpKind.CheckAndMaybeEntryScan:
                    sb.AppendLine($"// if (ShouldSwitchToEntryScan(bitmap, matches[{op.ParamIndex}])) goto entryScan_{op.GotoLabelIndex};");
                    break;
                case PlanOpKind.IterateInto:
                    sb.AppendLine("return; // bitmap ready for iteration");
                    break;
                default:
                    sb.AppendLine($"// {op.Kind}");
                    break;
            }
        }

        return sb.ToString();
    }
}
