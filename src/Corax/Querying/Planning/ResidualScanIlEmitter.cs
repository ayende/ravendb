using System;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
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
/// </summary>
public static class ResidualScanIlEmitter
{
    public delegate int ResidualScanPredicate(
        IPredicateEvaluationContext ctx,
        Span<EntryTermsReader> readers,
        Span<long> entryIds,
        Span<int> originalIndexes);

    private const BindingFlags AnyInstance =
        BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;

    // IPredicateEvaluationContext interface property getters
    private static readonly MethodInfo CtxLongParams =
        typeof(IPredicateEvaluationContext).GetProperty(nameof(IPredicateEvaluationContext.ResidualLongParams)).GetGetMethod();
    private static readonly MethodInfo CtxDoubleParams =
        typeof(IPredicateEvaluationContext).GetProperty(nameof(IPredicateEvaluationContext.ResidualDoubleParams)).GetGetMethod();
    private static readonly MethodInfo CtxSliceParams =
        typeof(IPredicateEvaluationContext).GetProperty(nameof(IPredicateEvaluationContext.ResidualSliceParams)).GetGetMethod();
    private static readonly MethodInfo CtxFieldRootPages =
        typeof(IPredicateEvaluationContext).GetProperty(nameof(IPredicateEvaluationContext.ResidualFieldRootPages)).GetGetMethod();

    // EntryTermsReader members
    private static readonly MethodInfo ReaderReset =
        typeof(EntryTermsReader).GetMethod(nameof(EntryTermsReader.Reset));
    private static readonly MethodInfo ReaderFindNext =
        typeof(EntryTermsReader).GetMethod(nameof(EntryTermsReader.FindNext));
    private static readonly FieldInfo ReaderCurrentLong =
        typeof(EntryTermsReader).GetField(nameof(EntryTermsReader.CurrentLong));
    private static readonly FieldInfo ReaderCurrentDouble =
        typeof(EntryTermsReader).GetField(nameof(EntryTermsReader.CurrentDouble));
    private static readonly FieldInfo ReaderCurrent =
        typeof(EntryTermsReader).GetField(nameof(EntryTermsReader.Current));

    private static readonly MethodInfo CompactKeyDecoded =
        typeof(CompactKey).GetMethod(nameof(CompactKey.Decoded), Type.EmptyTypes);

    private static readonly MethodInfo SliceAsReadOnlySpan =
        typeof(Slice).GetMethod(nameof(Slice.AsReadOnlySpan));

    private static readonly MethodInfo SliceStartsWithHelper =
        typeof(CompiledQueryHelper).GetMethod(nameof(CompiledQueryHelper.SliceStartsWith));
    private static readonly MethodInfo SliceEndsWithHelper =
        typeof(CompiledQueryHelper).GetMethod(nameof(CompiledQueryHelper.SliceEndsWith));

    private static readonly MethodInfo CheckFieldTermStartsWith =
        typeof(CompiledQueryHelper).GetMethod(nameof(CompiledQueryHelper.CheckFieldTermStartsWith));
    private static readonly MethodInfo CheckFieldTermEndsWith =
        typeof(CompiledQueryHelper).GetMethod(nameof(CompiledQueryHelper.CheckFieldTermEndsWith));

    private static readonly MethodInfo SpanByteSequenceEqual = typeof(System.MemoryExtensions)
        .GetMethods(BindingFlags.Public | BindingFlags.Static)
        .First(m => m.Name == nameof(System.MemoryExtensions.SequenceEqual)
                    && m.IsGenericMethodDefinition
                    && m.GetParameters().Length == 2
                    && m.GetParameters()[0].ParameterType.GetGenericTypeDefinition() == typeof(ReadOnlySpan<>)
                    && m.GetParameters()[1].ParameterType.GetGenericTypeDefinition() == typeof(ReadOnlySpan<>))
        .MakeGenericMethod(typeof(byte));

    private static readonly MethodInfo SpanByteSequenceCompareTo = typeof(System.MemoryExtensions)
        .GetMethods(BindingFlags.Public | BindingFlags.Static)
        .First(m => m.Name == nameof(System.MemoryExtensions.SequenceCompareTo)
                    && m.IsGenericMethodDefinition
                    && m.GetParameters().Length == 2)
        .MakeGenericMethod(typeof(byte));

    /// <summary>Emit a residual-scan delegate that evaluates <paramref name="predicates"/>
    /// against each reader in the batch. Passing entry IDs and (optionally) original indexes
    /// are compacted to the front of their spans. Returns the count of survivors.
    /// When <paramref name="multiValueStartsWith"/> is true, StartsWith/EndsWith compare
    /// against ALL field terms (DirectScanMatch semantics); when false, they compare against
    /// the current decoded term only (CompiledQueryMatch semantics).</summary>
    public static ResidualScanPredicate EmitDelegate(ScanPredicateInfo[] predicates, bool multiValueStartsWith = false)
    {
        if (predicates == null || predicates.Length == 0)
            return null;

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
        il.Emit(OpCodes.Call, typeof(Span<long>).GetMethod("get_Length"));
        il.Emit(OpCodes.Stloc, lengthLocal);

        il.Emit(OpCodes.Ldarga_S, (byte)3);
        il.Emit(OpCodes.Call, typeof(Span<int>).GetMethod("get_Length"));
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
        il.Emit(OpCodes.Call, typeof(Span<EntryTermsReader>).GetMethod("get_Item", [typeof(int)]));
        il.Emit(OpCodes.Stloc, readerRefLocal);

        int rootIdx = 0;
        for (int p = 0; p < predicates.Length; p++)
        {
            EmitPredicate(il, in predicates[p], failLabel, ref rootIdx, readerRefLocal, multiValueStartsWith);
        }

        // All passed: entryIds[writeIdx] = entryIds[i]
        il.Emit(OpCodes.Ldarga_S, (byte)2);
        il.Emit(OpCodes.Ldloc, writeIdxLocal);
        il.Emit(OpCodes.Call, typeof(Span<long>).GetMethod("get_Item", [typeof(int)]));
        il.Emit(OpCodes.Ldarga_S, (byte)2);
        il.Emit(OpCodes.Ldloc, iLocal);
        il.Emit(OpCodes.Call, typeof(Span<long>).GetMethod("get_Item", [typeof(int)]));
        il.Emit(OpCodes.Ldind_I8);
        il.Emit(OpCodes.Stind_I8);

        // originalIndexes compaction (only if span is non-empty — caller decides)
        il.Emit(OpCodes.Ldarga_S, (byte)3);
        il.Emit(OpCodes.Call, typeof(Span<int>).GetMethod("get_Length"));
        var noOrigIdx = il.DefineLabel();
        il.Emit(OpCodes.Ldc_I4_0);
        il.Emit(OpCodes.Ceq);
        il.Emit(OpCodes.Brtrue, noOrigIdx);

        // originalIndexes[writeIdx] = originalIndexes[i]
        il.Emit(OpCodes.Ldarga_S, (byte)3);
        il.Emit(OpCodes.Ldloc, writeIdxLocal);
        il.Emit(OpCodes.Call, typeof(Span<int>).GetMethod("get_Item", [typeof(int)]));
        il.Emit(OpCodes.Ldarga_S, (byte)3);
        il.Emit(OpCodes.Ldloc, iLocal);
        il.Emit(OpCodes.Call, typeof(Span<int>).GetMethod("get_Item", [typeof(int)]));
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
        LocalBuilder readerRefLocal,
        bool multiValueStartsWith = false)
    {
        if (pred.SubPredicates != null)
        {
            if (pred.Group == GroupKind.Or)
            {
                var groupPassed = il.DefineLabel();
                for (int b = 0; b < pred.SubPredicates.Length; b++)
                {
                    var nextSub = il.DefineLabel();
                    EmitLeafPredicate(il, in pred.SubPredicates[b], nextSub, rootIdx, readerRefLocal, multiValueStartsWith);
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
                    EmitLeafPredicate(il, in pred.SubPredicates[b], failLabel, rootIdx, readerRefLocal, multiValueStartsWith);
                    rootIdx++;
                }
            }
            return;
        }

        EmitLeafPredicate(il, in pred, failLabel, rootIdx, readerRefLocal, multiValueStartsWith);
        rootIdx++;
    }

    private static void EmitLeafPredicate(
        ILGenerator il,
        in ScanPredicateInfo pred,
        Label failLabel,
        int rootIdx,
        LocalBuilder readerRefLocal,
        bool multiValueStartsWith = false)
    {
        var nextPredicate = il.DefineLabel();
        var foundLabel = il.DefineLabel();

        il.Emit(OpCodes.Ldloc, readerRefLocal);
        il.Emit(OpCodes.Call, ReaderReset);

        // FindNext(ctx.ResidualFieldRootPages[rootIdx])
        il.Emit(OpCodes.Ldloc, readerRefLocal);
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Callvirt, CtxFieldRootPages);
        EmitLdcI4(il, rootIdx);
        il.Emit(OpCodes.Ldelem_I8);
        il.Emit(OpCodes.Call, ReaderFindNext);

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
                EmitTypedComparison(il, in pred, rootIdx, readerRefLocal, multiValueStartsWith);
                il.Emit(OpCodes.Brtrue, failLabel);
                break;

            default:
                il.Emit(OpCodes.Brfalse, failLabel);
                EmitTypedComparison(il, in pred, rootIdx, readerRefLocal, multiValueStartsWith);
                il.Emit(OpCodes.Brfalse, failLabel);
                break;
        }

        il.MarkLabel(nextPredicate);
    }

    private static void EmitTypedComparison(
        ILGenerator il,
        in ScanPredicateInfo pred,
        int rootIdx,
        LocalBuilder readerRefLocal,
        bool multiValueStartsWith = false)
    {
        _ = rootIdx;

        if (pred.CompareOp == ScanCompareOp.StartsWith)
        {
            if (multiValueStartsWith)
            {
                // Multi-value: iterate ALL terms for the field (DirectScanMatch semantics)
                il.Emit(OpCodes.Ldloc, readerRefLocal);
                il.Emit(OpCodes.Ldarg_0);
                il.Emit(OpCodes.Callvirt, CtxFieldRootPages);
                EmitLdcI4(il, rootIdx);
                il.Emit(OpCodes.Ldelem_I8);
                EmitLoadSliceSpan(il, pred.ParamIndex);
                il.Emit(OpCodes.Call, CheckFieldTermStartsWith);
            }
            else
            {
                // Single-term: compare against current decoded term (CompiledQueryMatch semantics)
                EmitLoadReaderDecodedSlice(il, readerRefLocal);
                EmitLoadSliceSpan(il, pred.ParamIndex);
                il.Emit(OpCodes.Call, SliceStartsWithHelper);
            }
            return;
        }

        if (pred.CompareOp == ScanCompareOp.EndsWith)
        {
            if (multiValueStartsWith)
            {
                il.Emit(OpCodes.Ldloc, readerRefLocal);
                il.Emit(OpCodes.Ldarg_0);
                il.Emit(OpCodes.Callvirt, CtxFieldRootPages);
                EmitLdcI4(il, rootIdx);
                il.Emit(OpCodes.Ldelem_I8);
                EmitLoadSliceSpan(il, pred.ParamIndex);
                il.Emit(OpCodes.Call, CheckFieldTermEndsWith);
            }
            else
            {
                EmitLoadReaderDecodedSlice(il, readerRefLocal);
                EmitLoadSliceSpan(il, pred.ParamIndex);
                il.Emit(OpCodes.Call, SliceEndsWithHelper);
            }
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
            {
                if (pred.CompareOp == ScanCompareOp.Between)
                {
                    var fail = il.DefineLabel();
                    var done = il.DefineLabel();
                    EmitLoadReaderDecodedSlice(il, readerRefLocal);
                    EmitLoadSliceSpan(il, pred.ParamIndex);
                    il.Emit(OpCodes.Call, SpanByteSequenceCompareTo);
                    il.Emit(OpCodes.Ldc_I4_0);
                    il.Emit(OpCodes.Blt, fail);
                    EmitLoadReaderDecodedSlice(il, readerRefLocal);
                    EmitLoadSliceSpan(il, pred.ParamIndex2);
                    il.Emit(OpCodes.Call, SpanByteSequenceCompareTo);
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
                    il.Emit(OpCodes.Call, SpanByteSequenceEqual);
                    break;
                }
                EmitLoadReaderDecodedSlice(il, readerRefLocal);
                EmitLoadSliceSpan(il, pred.ParamIndex);
                il.Emit(OpCodes.Call, SpanByteSequenceCompareTo);
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
        il.Emit(OpCodes.Ldfld, ReaderCurrentLong);
    }

    private static void EmitLoadReaderCurrentDouble(ILGenerator il, LocalBuilder readerRefLocal)
    {
        il.Emit(OpCodes.Ldloc, readerRefLocal);
        il.Emit(OpCodes.Ldfld, ReaderCurrentDouble);
    }

    private static void EmitLoadReaderDecodedSlice(ILGenerator il, LocalBuilder readerRefLocal)
    {
        il.Emit(OpCodes.Ldloc, readerRefLocal);
        il.Emit(OpCodes.Ldfld, ReaderCurrent);
        il.Emit(OpCodes.Callvirt, CompactKeyDecoded);
    }

    private static void EmitLoadLongParam(ILGenerator il, int index)
    {
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Callvirt, CtxLongParams);
        EmitLdcI4(il, index);
        il.Emit(OpCodes.Ldelem_I8);
    }

    private static void EmitLoadDoubleParam(ILGenerator il, int index)
    {
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Callvirt, CtxDoubleParams);
        EmitLdcI4(il, index);
        il.Emit(OpCodes.Ldelem_R8);
    }

    private static void EmitLoadSliceSpan(ILGenerator il, int paramIndex)
    {
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Callvirt, CtxSliceParams);
        EmitLdcI4(il, paramIndex);
        il.Emit(OpCodes.Ldelema, typeof(Slice));
        il.Emit(OpCodes.Call, SliceAsReadOnlySpan);
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
                if (value is >= -128 and <= 127)
                    il.Emit(OpCodes.Ldc_I4_S, (sbyte)value);
                else
                    il.Emit(OpCodes.Ldc_I4, value);
                break;
        }
    }
}
