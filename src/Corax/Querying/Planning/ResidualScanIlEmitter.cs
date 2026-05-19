using System;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;
using Corax.Querying.Matches;
using Corax.Utils;
using Voron;
using Voron.Data.CompactTrees;

namespace Corax.Querying.Planning;

/// <summary>
/// Emits IL for residual-predicate evaluation delegates used by both the entry-scan
/// path (CompiledQueryMatch) and the direct-scan path (DirectScanMatch). A single
/// delegate works against <see cref="IPredicateEvaluationContext"/>, which both
/// context types implement. Per-predicate value type, compare op, AND/OR sub-groups,
/// and fieldRootPages indexing are baked into IL at emit time.
///
/// The delegate ALWAYS evaluates ALL predicates baked into the IL, regardless of
/// which path calls it. In the direct-scan case, the driving-clause predicates
/// (already satisfied by the tree scan) are re-evaluated — this is redundant but
/// harmless, and it eliminates the need for a separate delegate-per-path or a
/// runtime predicate-count parameter. The extra cost is negligible: a handful
/// of FindNext + compare operations per entry against already-cached stored fields.
/// </summary>
public static class ResidualScanIlEmitter
{
    /// <summary>Compiled predicate that filters and compacts a batch of entry IDs in-place.</summary>
    public delegate int ResidualScanPredicate(
        IPredicateEvaluationContext ctx,
        Span<EntryTermsReader> readers,
        Span<long> entryIds,
        Span<int> originalIndexes);



    /// <summary>Emit a residual-scan delegate that evaluates <paramref name="predicates"/>
    /// against each reader in the batch. Passing entry IDs and (optionally) original indexes
    /// are compacted to the front of their spans. Returns the count of survivors.
    /// The emitted IL always evaluates ALL predicates against every entry.
    /// <paramref name="explainSource"/> receives a human-readable pseudocode description
    /// of the predicates, matching the format used by <see cref="QueryIlEmitter.EmitDelegate"/>.</summary>
    public static ResidualScanPredicate EmitDelegate(ScanPredicateInfo[] predicates, out string explainSource)
    {
        if (predicates == null || predicates.Length == 0)
        {
            explainSource = "// No residual predicates";
            return null;
        }

        var sb = new System.Text.StringBuilder();
        sb.AppendLine("// Entry scan — residual predicate evaluation (emitted IL).");
        for (int p = 0; p < predicates.Length; p++)
        {
            ref readonly var pred = ref predicates[p];
            sb.Append($"//   [{p}] Field '{pred.FieldName}' {pred.ValueType} {pred.CompareOp}");
            if (pred.SubPredicates != null)
                sb.Append($" ({pred.Group} group, {pred.SubPredicates.Length} branches)");
            sb.AppendLine($" at rootPage=[rootIdx]");
        }
        explainSource = sb.ToString();

        var dm = new DynamicMethod(
            "ResidualScan",
            typeof(int),
            [typeof(IPredicateEvaluationContext), typeof(Span<EntryTermsReader>), typeof(Span<long>), typeof(Span<int>)],
            typeof(IPredicateEvaluationContext).Module,
            skipVisibility: true)
        {
            InitLocals = false
        };

        var il = dm.GetILGenerator();

        var iLocal = il.DeclareLocal(typeof(int));
        var writeIdxLocal = il.DeclareLocal(typeof(int));
        var lengthLocal = il.DeclareLocal(typeof(int));
        var readerRefLocal = il.DeclareLocal(typeof(EntryTermsReader).MakeByRefType());
        var origIdxLengthLocal = il.DeclareLocal(typeof(int));

        il.Emit(OpCodes.Ldarga_S, (byte)2);
        il.Emit(OpCodes.Call, IlEmitterShared.SpanLongLength);
        il.Emit(OpCodes.Stloc, lengthLocal);

        il.Emit(OpCodes.Ldarga_S, (byte)3);
        il.Emit(OpCodes.Call, IlEmitterShared.SpanIntLength);
        il.Emit(OpCodes.Stloc, origIdxLengthLocal);

        il.Emit(OpCodes.Ldc_I4_0);
        il.Emit(OpCodes.Stloc, writeIdxLocal);

        il.Emit(OpCodes.Ldc_I4_0);
        il.Emit(OpCodes.Stloc, iLocal);

        var loopCheck = il.DefineLabel();
        var loopBody = il.DefineLabel();
        var loopIncrement = il.DefineLabel();
        var failLabel = il.DefineLabel();

        il.Emit(OpCodes.Br, loopCheck);

        il.MarkLabel(loopBody);

        il.Emit(OpCodes.Ldarga_S, (byte)1);
        il.Emit(OpCodes.Ldloc, iLocal);
        il.Emit(OpCodes.Call, IlEmitterShared.SpanEntryTermsReaderGetItem);
        il.Emit(OpCodes.Stloc, readerRefLocal);

        int rootIdx = 0;
        for (int p = 0; p < predicates.Length; p++)
        {
            EmitPredicate(il, in predicates[p], failLabel, ref rootIdx, readerRefLocal);
        }

        // All passed: entryIds[writeIdx] = entryIds[i]
        il.Emit(OpCodes.Ldarga_S, (byte)2);
        il.Emit(OpCodes.Ldloc, writeIdxLocal);
        il.Emit(OpCodes.Call, IlEmitterShared.SpanLongGetItem);
        il.Emit(OpCodes.Ldarga_S, (byte)2);
        il.Emit(OpCodes.Ldloc, iLocal);
        il.Emit(OpCodes.Call, IlEmitterShared.SpanLongGetItem);
        il.Emit(OpCodes.Ldind_I8);
        il.Emit(OpCodes.Stind_I8);

        // originalIndexes compaction (only if span is non-empty — caller decides)
        il.Emit(OpCodes.Ldarga_S, (byte)3);
        il.Emit(OpCodes.Call, IlEmitterShared.SpanIntLength);
        var noOrigIdx = il.DefineLabel();
        il.Emit(OpCodes.Ldc_I4_0);
        il.Emit(OpCodes.Ceq);
        il.Emit(OpCodes.Brtrue, noOrigIdx);

        // originalIndexes[writeIdx] = originalIndexes[i]
        il.Emit(OpCodes.Ldarga_S, (byte)3);
        il.Emit(OpCodes.Ldloc, writeIdxLocal);
        il.Emit(OpCodes.Call, IlEmitterShared.SpanIntGetItem);
        il.Emit(OpCodes.Ldarga_S, (byte)3);
        il.Emit(OpCodes.Ldloc, iLocal);
        il.Emit(OpCodes.Call, IlEmitterShared.SpanIntGetItem);
        il.Emit(OpCodes.Ldind_I4);
        il.Emit(OpCodes.Stind_I4);

        il.MarkLabel(noOrigIdx);

        il.Emit(OpCodes.Ldloc, writeIdxLocal);
        il.Emit(OpCodes.Ldc_I4_1);
        il.Emit(OpCodes.Add);
        il.Emit(OpCodes.Stloc, writeIdxLocal);
        il.Emit(OpCodes.Br, loopIncrement);

        il.MarkLabel(failLabel);

        il.MarkLabel(loopIncrement);
        il.Emit(OpCodes.Ldloc, iLocal);
        il.Emit(OpCodes.Ldc_I4_1);
        il.Emit(OpCodes.Add);
        il.Emit(OpCodes.Stloc, iLocal);

        il.MarkLabel(loopCheck);
        il.Emit(OpCodes.Ldloc, iLocal);
        il.Emit(OpCodes.Ldloc, lengthLocal);
        il.Emit(OpCodes.Blt, loopBody);

        il.Emit(OpCodes.Ldloc, writeIdxLocal);
        il.Emit(OpCodes.Ret);

        return (ResidualScanPredicate)dm.CreateDelegate(typeof(ResidualScanPredicate));
    }

    private static void EmitPredicate(
        ILGenerator il,
        in ScanPredicateInfo pred,
        Label failLabel,
        ref int rootIdx,
        LocalBuilder readerRefLocal)
    {
        if (pred.SubPredicates != null)
        {
            if (pred.Group == GroupKind.Or)
            {
                var groupPassed = il.DefineLabel();
                for (int b = 0; b < pred.SubPredicates.Length; b++)
                {
                    var nextSub = il.DefineLabel();
                    EmitLeafPredicate(il, in pred.SubPredicates[b], nextSub, rootIdx, readerRefLocal);
                    il.Emit(OpCodes.Br, groupPassed);
                    il.MarkLabel(nextSub);
                    rootIdx++;
                }
                il.Emit(OpCodes.Br, failLabel);
                il.MarkLabel(groupPassed);
            }
            else
            {
                for (int b = 0; b < pred.SubPredicates.Length; b++)
                {
                    EmitLeafPredicate(il, in pred.SubPredicates[b], failLabel, rootIdx, readerRefLocal);
                    rootIdx++;
                }
            }
            return;
        }

        EmitLeafPredicate(il, in pred, failLabel, rootIdx, readerRefLocal);
        rootIdx++;
    }

    private static void EmitLeafPredicate(
        ILGenerator il,
        in ScanPredicateInfo pred,
        Label failLabel,
        int rootIdx,
        LocalBuilder readerRefLocal)
    {
        var nextPredicate = il.DefineLabel();
        var foundLabel = il.DefineLabel();

        il.Emit(OpCodes.Ldloc, readerRefLocal);
        il.Emit(OpCodes.Call, IlEmitterShared.ReaderReset);

        // FindNext(ctx.ResidualFieldRootPages[rootIdx])
        il.Emit(OpCodes.Ldloc, readerRefLocal);
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Callvirt, IlEmitterShared.CtxFieldRootPages);
        IlEmitterShared.EmitLdcI4(il, rootIdx);
        il.Emit(OpCodes.Ldelem_I8);
        il.Emit(OpCodes.Call, IlEmitterShared.ReaderFindNext);

        switch (pred.CompareOp)
        {
            case ScanCompareOp.Exists:
                il.Emit(OpCodes.Brfalse, failLabel);
                il.Emit(OpCodes.Br, nextPredicate);
                break;

            case ScanCompareOp.NotEqual:
                il.Emit(OpCodes.Brtrue, foundLabel);
                il.Emit(OpCodes.Br, nextPredicate);
                il.MarkLabel(foundLabel);
                EmitTypedComparison(il, in pred, rootIdx, readerRefLocal);
                il.Emit(OpCodes.Brtrue, failLabel);
                break;

            default:
                il.Emit(OpCodes.Brfalse, failLabel);
                EmitTypedComparison(il, in pred, rootIdx, readerRefLocal);
                il.Emit(OpCodes.Brfalse, failLabel);
                break;
        }

        il.MarkLabel(nextPredicate);
    }

    private static void EmitTypedComparison(
        ILGenerator il,
        in ScanPredicateInfo pred,
        int rootIdx,
        LocalBuilder readerRefLocal)
    {
        _ = rootIdx;

        if (pred.CompareOp == ScanCompareOp.StartsWith)
        {
            // Multi-term: iterate ALL field terms — correct for both entry-scan and direct-scan
            il.Emit(OpCodes.Ldloc, readerRefLocal);
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Callvirt, IlEmitterShared.CtxFieldRootPages);
            IlEmitterShared.EmitLdcI4(il, rootIdx);
            il.Emit(OpCodes.Ldelem_I8);
            EmitLoadSliceSpan(il, pred.ParamIndex);
            il.Emit(OpCodes.Call, IlEmitterShared.CheckFieldTermStartsWith);
            return;
        }

        if (pred.CompareOp == ScanCompareOp.EndsWith)
        {
            il.Emit(OpCodes.Ldloc, readerRefLocal);
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Callvirt, IlEmitterShared.CtxFieldRootPages);
            IlEmitterShared.EmitLdcI4(il, rootIdx);
            il.Emit(OpCodes.Ldelem_I8);
            EmitLoadSliceSpan(il, pred.ParamIndex);
            il.Emit(OpCodes.Call, IlEmitterShared.CheckFieldTermEndsWith);
            return;
        }

        switch (pred.ValueType)
        {
            case ScanValueType.Long:
            {
                if (pred.CompareOp == ScanCompareOp.Between)
                {
                    var fail = il.DefineLabel();
                    var done = il.DefineLabel();
                    EmitLoadReaderCurrentLong(il, readerRefLocal);
                    EmitLoadLongParam(il, pred.ParamIndex);
                    il.Emit(OpCodes.Blt, fail);
                    EmitLoadReaderCurrentLong(il, readerRefLocal);
                    EmitLoadLongParam(il, pred.ParamIndex2);
                    il.Emit(OpCodes.Bgt, fail);
                    il.Emit(OpCodes.Ldc_I4_1);
                    il.Emit(OpCodes.Br, done);
                    il.MarkLabel(fail);
                    il.Emit(OpCodes.Ldc_I4_0);
                    il.MarkLabel(done);
                    break;
                }
                EmitLoadReaderCurrentLong(il, readerRefLocal);
                EmitLoadLongParam(il, pred.ParamIndex);
                EmitNumericCompareOp(il, pred.CompareOp);
                break;
            }

            case ScanValueType.Double:
            {
                if (pred.CompareOp == ScanCompareOp.Between)
                {
                    var fail = il.DefineLabel();
                    var done = il.DefineLabel();
                    EmitLoadReaderCurrentDouble(il, readerRefLocal);
                    EmitLoadDoubleParam(il, pred.ParamIndex);
                    il.Emit(OpCodes.Blt_Un, fail);
                    EmitLoadReaderCurrentDouble(il, readerRefLocal);
                    EmitLoadDoubleParam(il, pred.ParamIndex2);
                    il.Emit(OpCodes.Bgt_Un, fail);
                    il.Emit(OpCodes.Ldc_I4_1);
                    il.Emit(OpCodes.Br, done);
                    il.MarkLabel(fail);
                    il.Emit(OpCodes.Ldc_I4_0);
                    il.MarkLabel(done);
                    break;
                }
                EmitLoadReaderCurrentDouble(il, readerRefLocal);
                EmitLoadDoubleParam(il, pred.ParamIndex);
                EmitNumericCompareOp(il, pred.CompareOp);
                break;
            }

            case ScanValueType.Slice:
            case ScanValueType.SliceLong:
            {
                if (pred.CompareOp == ScanCompareOp.Between)
                {
                    var fail = il.DefineLabel();
                    var done = il.DefineLabel();
                    EmitLoadReaderDecodedSlice(il, readerRefLocal);
                    EmitLoadSliceSpan(il, pred.ParamIndex);
                    il.Emit(OpCodes.Call, IlEmitterShared.SequenceCompareTo);
                    il.Emit(OpCodes.Ldc_I4_0);
                    il.Emit(OpCodes.Blt, fail);
                    EmitLoadReaderDecodedSlice(il, readerRefLocal);
                    EmitLoadSliceSpan(il, pred.ParamIndex2);
                    il.Emit(OpCodes.Call, IlEmitterShared.SequenceCompareTo);
                    il.Emit(OpCodes.Ldc_I4_0);
                    il.Emit(OpCodes.Bgt, fail);
                    il.Emit(OpCodes.Ldc_I4_1);
                    il.Emit(OpCodes.Br, done);
                    il.MarkLabel(fail);
                    il.Emit(OpCodes.Ldc_I4_0);
                    il.MarkLabel(done);
                    break;
                }
                if (pred.CompareOp == ScanCompareOp.Equal || pred.CompareOp == ScanCompareOp.NotEqual)
                {
                    EmitLoadReaderDecodedSlice(il, readerRefLocal);
                    EmitLoadSliceSpan(il, pred.ParamIndex);
                    il.Emit(OpCodes.Call, IlEmitterShared.SequenceEqual);
                    break;
                }
                EmitLoadReaderDecodedSlice(il, readerRefLocal);
                EmitLoadSliceSpan(il, pred.ParamIndex);
                il.Emit(OpCodes.Call, IlEmitterShared.SequenceCompareTo);
                il.Emit(OpCodes.Ldc_I4_0);
                EmitNumericCompareOp(il, pred.CompareOp);
                break;
            }

            default:
                il.Emit(OpCodes.Ldc_I4_0);
                break;
        }
    }

    private static void EmitNumericCompareOp(ILGenerator il, ScanCompareOp op)
    {
        switch (op)
        {
            case ScanCompareOp.Equal:
            case ScanCompareOp.NotEqual:
                il.Emit(OpCodes.Ceq);
                break;
            case ScanCompareOp.GreaterThan:
                il.Emit(OpCodes.Cgt);
                break;
            case ScanCompareOp.GreaterThanOrEqual:
                il.Emit(OpCodes.Clt);
                il.Emit(OpCodes.Ldc_I4_0);
                il.Emit(OpCodes.Ceq);
                break;
            case ScanCompareOp.LessThan:
                il.Emit(OpCodes.Clt);
                break;
            case ScanCompareOp.LessThanOrEqual:
                il.Emit(OpCodes.Cgt);
                il.Emit(OpCodes.Ldc_I4_0);
                il.Emit(OpCodes.Ceq);
                break;
            default:
                il.Emit(OpCodes.Pop);
                il.Emit(OpCodes.Pop);
                il.Emit(OpCodes.Ldc_I4_0);
                break;
        }
    }

    private static void EmitLoadReaderCurrentLong(ILGenerator il, LocalBuilder readerRefLocal)
    {
        il.Emit(OpCodes.Ldloc, readerRefLocal);
        il.Emit(OpCodes.Ldfld, IlEmitterShared.ReaderCurrentLong);
    }

    private static void EmitLoadReaderCurrentDouble(ILGenerator il, LocalBuilder readerRefLocal)
    {
        il.Emit(OpCodes.Ldloc, readerRefLocal);
        il.Emit(OpCodes.Ldfld, IlEmitterShared.ReaderCurrentDouble);
    }

    private static void EmitLoadReaderDecodedSlice(ILGenerator il, LocalBuilder readerRefLocal)
    {
        il.Emit(OpCodes.Ldloc, readerRefLocal);
        il.Emit(OpCodes.Ldfld, IlEmitterShared.ReaderCurrent);
        il.Emit(OpCodes.Callvirt, IlEmitterShared.CompactKeyDecoded);
    }

    private static void EmitLoadLongParam(ILGenerator il, int index)
    {
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Callvirt, IlEmitterShared.CtxLongParams);
        IlEmitterShared.EmitLdcI4(il, index);
        il.Emit(OpCodes.Ldelem_I8);
    }

    private static void EmitLoadDoubleParam(ILGenerator il, int index)
    {
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Callvirt, IlEmitterShared.CtxDoubleParams);
        IlEmitterShared.EmitLdcI4(il, index);
        il.Emit(OpCodes.Ldelem_R8);
    }

    private static void EmitLoadSliceSpan(ILGenerator il, int paramIndex)
    {
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Callvirt, IlEmitterShared.CtxSliceParams);
        IlEmitterShared.EmitLdcI4(il, paramIndex);
        il.Emit(OpCodes.Ldelema, typeof(Slice));
        il.Emit(OpCodes.Call, IlEmitterShared.SliceAsReadOnlySpan);
    }


    public static string EmitCSharpSource(ScanPredicateInfo[] predicates) {
        if (predicates == null || predicates.Length == 0) return "// No residual predicates.\n";
        var sb = new StringBuilder(); sb.AppendLine("static int ResidualScan(IPredicateEvaluationContext ctx, Span<EntryTermsReader> readers, Span<long> entryIds, Span<int> originalIndexes)"); sb.AppendLine("{"); sb.AppendLine("    int length = entryIds.Length;"); sb.AppendLine("    int writeIdx = 0;"); sb.AppendLine("    for (int i = 0; i < length; i++)"); sb.AppendLine("    {"); sb.AppendLine("        ref EntryTermsReader reader = ref readers[i];");
        int rootIdx = 0; for (int p = 0; p < predicates.Length; p++) { sb.AppendLine(); EmitCSharpPredicate(sb, in predicates[p], ref rootIdx, p); }
        sb.AppendLine(); sb.AppendLine("        entryIds[writeIdx] = entryIds[i];"); sb.AppendLine("        if (originalIndexes.Length != 0) originalIndexes[writeIdx] = originalIndexes[i];"); sb.AppendLine("        writeIdx++; continue;"); sb.AppendLine("        rejected:;"); sb.AppendLine("    }"); sb.AppendLine("    return writeIdx;"); sb.AppendLine("}"); return sb.ToString(); }
    private static void EmitCSharpPredicate(StringBuilder sb, in ScanPredicateInfo pred, ref int rootIdx, int pIdx) { if (pred.SubPredicates != null) { if (pred.Group == GroupKind.Or) { sb.AppendLine($"        // OR group ({pIdx})"); sb.AppendLine("        {"); for (int b = 0; b < pred.SubPredicates.Length; b++) { EmitCSharpLeafBranch(sb, in pred.SubPredicates[b], rootIdx, $"gp_{pIdx}"); rootIdx++; } sb.AppendLine("            goto rejected;"); sb.AppendLine($"            gp_{pIdx}:;"); sb.AppendLine("        }"); } else { for (int b = 0; b < pred.SubPredicates.Length; b++) { EmitCSharpLeafPredicate(sb, in pred.SubPredicates[b], rootIdx); rootIdx++; } } return; } EmitCSharpLeafPredicate(sb, in pred, rootIdx); rootIdx++; }
    private static void EmitCSharpLeafBranch(StringBuilder sb, in ScanPredicateInfo pred, int ri, string pl) { sb.AppendLine("            reader.Reset();"); switch (pred.CompareOp) { case ScanCompareOp.Exists: sb.AppendLine($"            if (reader.FindNext(ctx.ResidualFieldRootPages[{ri}])) goto {pl};"); break; case ScanCompareOp.StartsWith: sb.AppendLine($"            if (CompiledQueryHelper.CheckFieldTermStartsWith(ref reader, ctx.ResidualFieldRootPages[{ri}], ctx.ResidualSliceParams[{pred.ParamIndex}].AsReadOnlySpan())) goto {pl};"); break; case ScanCompareOp.EndsWith: sb.AppendLine($"            if (CompiledQueryHelper.CheckFieldTermEndsWith(ref reader, ctx.ResidualFieldRootPages[{ri}], ctx.ResidualSliceParams[{pred.ParamIndex}].AsReadOnlySpan())) goto {pl};"); break; default: sb.AppendLine($"            if (reader.FindNext(ctx.ResidualFieldRootPages[{ri}]) && {EmitCSharpComparison(in pred)}) goto {pl};"); break; } }
    private static void EmitCSharpLeafPredicate(StringBuilder sb, in ScanPredicateInfo pred, int ri) { sb.AppendLine("        reader.Reset();"); switch (pred.CompareOp) { case ScanCompareOp.Exists: sb.AppendLine($"        if (!reader.FindNext(ctx.ResidualFieldRootPages[{ri}])) goto rejected;"); break; case ScanCompareOp.StartsWith: sb.AppendLine($"        if (!CompiledQueryHelper.CheckFieldTermStartsWith(ref reader, ctx.ResidualFieldRootPages[{ri}], ctx.ResidualSliceParams[{pred.ParamIndex}].AsReadOnlySpan())) goto rejected;"); break; case ScanCompareOp.EndsWith: sb.AppendLine($"        if (!CompiledQueryHelper.CheckFieldTermEndsWith(ref reader, ctx.ResidualFieldRootPages[{ri}], ctx.ResidualSliceParams[{pred.ParamIndex}].AsReadOnlySpan())) goto rejected;"); break; default: sb.AppendLine($"        if (!reader.FindNext(ctx.ResidualFieldRootPages[{ri}])) goto rejected;"); sb.AppendLine($"        if (!({EmitCSharpComparison(in pred)})) goto rejected;"); break; } }
    private static string EmitCSharpComparison(in ScanPredicateInfo pred) { return pred.ValueType switch { ScanValueType.Long => pred.CompareOp switch { ScanCompareOp.Equal or ScanCompareOp.NotEqual => $"reader.CurrentLong == ctx.ResidualLongParams[{pred.ParamIndex}]", ScanCompareOp.GreaterThan => $"reader.CurrentLong > ctx.ResidualLongParams[{pred.ParamIndex}]", ScanCompareOp.Between => $"reader.CurrentLong >= ctx.ResidualLongParams[{pred.ParamIndex}] && reader.CurrentLong <= ctx.ResidualLongParams[{pred.ParamIndex2}]", _ => "false" }, ScanValueType.Double => pred.CompareOp switch { ScanCompareOp.Equal or ScanCompareOp.NotEqual => $"reader.CurrentDouble == ctx.ResidualDoubleParams[{pred.ParamIndex}]", ScanCompareOp.GreaterThan => $"reader.CurrentDouble > ctx.ResidualDoubleParams[{pred.ParamIndex}]", ScanCompareOp.Between => $"reader.CurrentDouble >= ctx.ResidualDoubleParams[{pred.ParamIndex}] && reader.CurrentDouble <= ctx.ResidualDoubleParams[{pred.ParamIndex2}]", _ => "false" }, ScanValueType.Slice or ScanValueType.SliceLong => pred.CompareOp switch { ScanCompareOp.Equal or ScanCompareOp.NotEqual => $"reader.Current.Decoded().SequenceEqual(ctx.ResidualSliceParams[{pred.ParamIndex}].AsReadOnlySpan())", ScanCompareOp.Between => $"reader.Current.Decoded().SequenceCompareTo(ctx.ResidualSliceParams[{pred.ParamIndex}].AsReadOnlySpan()) >= 0 && reader.Current.Decoded().SequenceCompareTo(ctx.ResidualSliceParams[{pred.ParamIndex2}].AsReadOnlySpan()) <= 0", _ => "false" }, _ => "false" }; }
}
