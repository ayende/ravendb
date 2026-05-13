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
/// Emits the residual-predicate scan delegate used by DirectScanMatch. The delegate
/// inlines per-predicate comparisons at IL emit time: each predicate's value type and
/// compare op are baked into specialized IL, eliminating the runtime switch+loop.
/// </summary>
public static class DirectScanIlEmitter
{
    private const BindingFlags AnyInstance =
        BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;

    // DirectScanMatch private fields
    private static readonly FieldInfo SelfLongParams =
        typeof(DirectScanMatch).GetField("_longParams", AnyInstance)!;
    private static readonly FieldInfo SelfDoubleParams =
        typeof(DirectScanMatch).GetField("_doubleParams", AnyInstance)!;
    private static readonly FieldInfo SelfSliceParams =
        typeof(DirectScanMatch).GetField("_sliceParams", AnyInstance)!;
    private static readonly FieldInfo SelfFieldRootPages =
        typeof(DirectScanMatch).GetField("_fieldRootPages", AnyInstance)!;

    // EntryTermsReader
    private static readonly MethodInfo ReaderReset =
        typeof(EntryTermsReader).GetMethod(nameof(EntryTermsReader.Reset))!;
    private static readonly MethodInfo ReaderFindNext =
        typeof(EntryTermsReader).GetMethod(nameof(EntryTermsReader.FindNext))!;
    private static readonly FieldInfo ReaderCurrentLong =
        typeof(EntryTermsReader).GetField(nameof(EntryTermsReader.CurrentLong))!;
    private static readonly FieldInfo ReaderCurrentDouble =
        typeof(EntryTermsReader).GetField(nameof(EntryTermsReader.CurrentDouble))!;
    private static readonly FieldInfo ReaderCurrent =
        typeof(EntryTermsReader).GetField(nameof(EntryTermsReader.Current))!;

    private static readonly MethodInfo CompactKeyDecoded =
        typeof(CompactKey).GetMethod(nameof(CompactKey.Decoded), Type.EmptyTypes)!;

    private static readonly MethodInfo SliceAsReadOnlySpan =
        typeof(Slice).GetMethod(nameof(Slice.AsReadOnlySpan))!;

    // Span<T> accessors
    private static readonly MethodInfo SpanReaderGetItem =
        typeof(Span<EntryTermsReader>).GetMethod("get_Item", new[] { typeof(int) })!;
    private static readonly MethodInfo SpanLongGetItem =
        typeof(Span<long>).GetMethod("get_Item", new[] { typeof(int) })!;
    private static readonly MethodInfo SpanIntGetItem =
        typeof(Span<int>).GetMethod("get_Item", new[] { typeof(int) })!;
    private static readonly MethodInfo SpanLongGetLength =
        typeof(Span<long>).GetMethod("get_Length")!;

    // Multi-value field-iterating helpers (loop over all terms — non-trivial,
    // not worth inlining in IL). Switch-based Compare helpers were removed in favor
    // of emitting raw IL ops directly.
    private static readonly MethodInfo CheckFieldTermStartsWith =
        typeof(CompiledQueryHelper).GetMethod(nameof(CompiledQueryHelper.CheckFieldTermStartsWith))!;
    private static readonly MethodInfo CheckFieldTermEndsWith =
        typeof(CompiledQueryHelper).GetMethod(nameof(CompiledQueryHelper.CheckFieldTermEndsWith))!;

    // ReadOnlySpan<byte> comparison primitives — emitted as direct IL calls, not switched on.
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

    /// <summary>
    /// Emits a delegate that walks <paramref name="predicates"/> against each reader/entryId
    /// pair, compacts surviving entries to the front of <c>entryIds</c> and <c>originalIndexes</c>,
    /// and returns the count of survivors. Per-predicate comparisons are inlined at emit time.
    /// </summary>
    public static DirectScanMatch.CompiledResidualScan EmitResidualScanDelegate(ScanPredicateInfo[] predicates)
    {
        var dm = new DynamicMethod(
            "DirectScanResidual",
            typeof(int),
            [typeof(DirectScanMatch), typeof(Span<EntryTermsReader>), typeof(Span<long>), typeof(Span<int>)],
            typeof(DirectScanMatch).Module,
            skipVisibility: true)
        {
            InitLocals = false
        };

        var il = dm.GetILGenerator();

        var iLocal = il.DeclareLocal(typeof(int));
        var writeIdxLocal = il.DeclareLocal(typeof(int));
        var lengthLocal = il.DeclareLocal(typeof(int));
        var readerRefLocal = il.DeclareLocal(typeof(EntryTermsReader).MakeByRefType());

        var loopBody = il.DefineLabel();
        var loopCheck = il.DefineLabel();
        var loopIncrement = il.DefineLabel();
        var matchedLabel = il.DefineLabel();

        // length = entryIds.Length
        il.Emit(OpCodes.Ldarga_S, (byte)2);
        il.Emit(OpCodes.Call, SpanLongGetLength);
        il.Emit(OpCodes.Stloc, lengthLocal);

        // writeIdx = 0
        il.Emit(OpCodes.Ldc_I4_0);
        il.Emit(OpCodes.Stloc, writeIdxLocal);

        // i = 0
        il.Emit(OpCodes.Ldc_I4_0);
        il.Emit(OpCodes.Stloc, iLocal);

        il.Emit(OpCodes.Br, loopCheck);

        il.MarkLabel(loopBody);

        // readerRef = ref readers[i]
        il.Emit(OpCodes.Ldarga_S, (byte)1);
        il.Emit(OpCodes.Ldloc, iLocal);
        il.Emit(OpCodes.Call, SpanReaderGetItem);
        il.Emit(OpCodes.Stloc, readerRefLocal);

        // Evaluate all predicates. Any failure → jump to loopIncrement (skip compaction).
        for (int p = 0; p < predicates.Length; p++)
        {
            EmitPredicate(il, in predicates[p], readerRefLocal, loopIncrement);
        }

        // All predicates passed: compact entryIds[writeIdx] = entryIds[i]; same for originalIndexes.
        il.MarkLabel(matchedLabel);

        // entryIds[writeIdx] = entryIds[i]
        il.Emit(OpCodes.Ldarga_S, (byte)2);
        il.Emit(OpCodes.Ldloc, writeIdxLocal);
        il.Emit(OpCodes.Call, SpanLongGetItem);            // ref long (dest)
        il.Emit(OpCodes.Ldarga_S, (byte)2);
        il.Emit(OpCodes.Ldloc, iLocal);
        il.Emit(OpCodes.Call, SpanLongGetItem);            // ref long (src)
        il.Emit(OpCodes.Ldind_I8);                         // long
        il.Emit(OpCodes.Stind_I8);                         // *dest = value

        // originalIndexes[writeIdx] = originalIndexes[i]
        il.Emit(OpCodes.Ldarga_S, (byte)3);
        il.Emit(OpCodes.Ldloc, writeIdxLocal);
        il.Emit(OpCodes.Call, SpanIntGetItem);             // ref int (dest)
        il.Emit(OpCodes.Ldarga_S, (byte)3);
        il.Emit(OpCodes.Ldloc, iLocal);
        il.Emit(OpCodes.Call, SpanIntGetItem);             // ref int (src)
        il.Emit(OpCodes.Ldind_I4);
        il.Emit(OpCodes.Stind_I4);

        // writeIdx++
        il.Emit(OpCodes.Ldloc, writeIdxLocal);
        il.Emit(OpCodes.Ldc_I4_1);
        il.Emit(OpCodes.Add);
        il.Emit(OpCodes.Stloc, writeIdxLocal);

        // i++ (fall-through point for failed predicate)
        il.MarkLabel(loopIncrement);
        il.Emit(OpCodes.Ldloc, iLocal);
        il.Emit(OpCodes.Ldc_I4_1);
        il.Emit(OpCodes.Add);
        il.Emit(OpCodes.Stloc, iLocal);

        // while (i < length)
        il.MarkLabel(loopCheck);
        il.Emit(OpCodes.Ldloc, iLocal);
        il.Emit(OpCodes.Ldloc, lengthLocal);
        il.Emit(OpCodes.Blt, loopBody);

        // return writeIdx
        il.Emit(OpCodes.Ldloc, writeIdxLocal);
        il.Emit(OpCodes.Ret);

        return (DirectScanMatch.CompiledResidualScan)dm.CreateDelegate(typeof(DirectScanMatch.CompiledResidualScan));
    }

    /// <summary>
    /// Emit IL for one predicate. On failure, jumps to <paramref name="failLabel"/>.
    /// Layout: Reset(); found = FindNext(fieldRoot); branch on (found, op) and inline the
    /// type-specific comparison via CompiledQueryHelper.
    /// </summary>
    private static void EmitPredicate(
        ILGenerator il,
        in ScanPredicateInfo pred,
        LocalBuilder readerRefLocal,
        Label failLabel)
    {
        var nextPredicate = il.DefineLabel();
        var foundLabel = il.DefineLabel();

        // reader.Reset()
        il.Emit(OpCodes.Ldloc, readerRefLocal);
        il.Emit(OpCodes.Call, ReaderReset);

        // found = reader.FindNext(_fieldRootPages[ParamIndex])
        il.Emit(OpCodes.Ldloc, readerRefLocal);
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Ldfld, SelfFieldRootPages);
        EmitLdcI4(il, pred.ParamIndex);
        il.Emit(OpCodes.Ldelem_I8);
        il.Emit(OpCodes.Call, ReaderFindNext);
        // stack: found (bool)

        switch (pred.CompareOp)
        {
            case ScanCompareOp.Exists:
                // passed = found
                il.Emit(OpCodes.Brfalse, failLabel);
                il.Emit(OpCodes.Br, nextPredicate);
                break;

            case ScanCompareOp.NotEqual:
                // !found → continue (pass). found → evaluate; if compare returns true, the
                // term equals the param → predicate fails.
                il.Emit(OpCodes.Brtrue, foundLabel);
                il.Emit(OpCodes.Br, nextPredicate);
                il.MarkLabel(foundLabel);
                EmitTypedComparison(il, in pred, readerRefLocal);
                // stack: comparisonResult (true means EQUAL, so NotEqual fails)
                il.Emit(OpCodes.Brtrue, failLabel);
                break;

            default:
                // All others: !found → fail. found → evaluate; comparison must return true.
                il.Emit(OpCodes.Brfalse, failLabel);
                EmitTypedComparison(il, in pred, readerRefLocal);
                il.Emit(OpCodes.Brfalse, failLabel);
                break;
        }

        il.MarkLabel(nextPredicate);
    }

    /// <summary>
    /// Push the result of the predicate comparison (bool) onto the stack. Caller branches.
    /// </summary>
    private static void EmitTypedComparison(
        ILGenerator il,
        in ScanPredicateInfo pred,
        LocalBuilder readerRefLocal)
    {
        // StartsWith / EndsWith iterate ALL terms for the field via the helper. They do
        // their own Reset+FindNext loop, so the outer FindNext result is discarded.
        if (pred.CompareOp == ScanCompareOp.StartsWith)
        {
            il.Emit(OpCodes.Ldloc, readerRefLocal);
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Ldfld, SelfFieldRootPages);
            EmitLdcI4(il, pred.ParamIndex);
            il.Emit(OpCodes.Ldelem_I8);
            EmitLoadSliceSpan(il, pred.ParamIndex);
            il.Emit(OpCodes.Call, CheckFieldTermStartsWith);
            return;
        }

        if (pred.CompareOp == ScanCompareOp.EndsWith)
        {
            il.Emit(OpCodes.Ldloc, readerRefLocal);
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Ldfld, SelfFieldRootPages);
            EmitLdcI4(il, pred.ParamIndex);
            il.Emit(OpCodes.Ldelem_I8);
            EmitLoadSliceSpan(il, pred.ParamIndex);
            il.Emit(OpCodes.Call, CheckFieldTermEndsWith);
            return;
        }

        switch (pred.ValueType)
        {
            case ScanValueType.Long:
            {
                if (pred.CompareOp == ScanCompareOp.Between)
                {
                    // low <= actual && actual <= high
                    // Eval as: !(actual < low) && !(actual > high)
                    var lowFail = il.DefineLabel();
                    var done = il.DefineLabel();

                    // actual < low ?
                    EmitLoadReaderCurrentLong(il, readerRefLocal);
                    EmitLoadLongParam(il, pred.ParamIndex);
                    il.Emit(OpCodes.Blt, lowFail);

                    // actual > high ?
                    EmitLoadReaderCurrentLong(il, readerRefLocal);
                    EmitLoadLongParam(il, pred.ParamIndex2);
                    il.Emit(OpCodes.Bgt, lowFail);

                    il.Emit(OpCodes.Ldc_I4_1);
                    il.Emit(OpCodes.Br, done);
                    il.MarkLabel(lowFail);
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
                    il.Emit(OpCodes.Blt_Un, fail);  // NaN treated as fail

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

                    // actual.SequenceCompareTo(low) < 0 → fail
                    EmitLoadReaderDecodedSlice(il, readerRefLocal);
                    EmitLoadSliceSpan(il, pred.ParamIndex);
                    il.Emit(OpCodes.Call, SpanByteSequenceCompareTo);
                    il.Emit(OpCodes.Ldc_I4_0);
                    il.Emit(OpCodes.Blt, fail);

                    // actual.SequenceCompareTo(high) > 0 → fail
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
                    // Equality uses SequenceEqual; caller inverts on NotEqual.
                    EmitLoadReaderDecodedSlice(il, readerRefLocal);
                    EmitLoadSliceSpan(il, pred.ParamIndex);
                    il.Emit(OpCodes.Call, SpanByteSequenceEqual);
                    break;
                }

                // Comparisons: SequenceCompareTo + compare-to-zero
                EmitLoadReaderDecodedSlice(il, readerRefLocal);
                EmitLoadSliceSpan(il, pred.ParamIndex);
                il.Emit(OpCodes.Call, SpanByteSequenceCompareTo);
                il.Emit(OpCodes.Ldc_I4_0);
                EmitNumericCompareOp(il, pred.CompareOp);
                break;
            }

            default:
                // Unknown type: push false so the predicate fails.
                il.Emit(OpCodes.Ldc_I4_0);
                break;
        }
    }

    /// <summary>Emit a comparison op against the two values currently on the stack
    /// (a, b). The caller-provided op may be inverted by EmitPredicate (for NotEqual),
    /// so we treat Equal/NotEqual identically here — both leave (a == b) on the stack.</summary>
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
                // !(a < b)
                il.Emit(OpCodes.Clt);
                il.Emit(OpCodes.Ldc_I4_0);
                il.Emit(OpCodes.Ceq);
                break;
            case ScanCompareOp.LessThan:
                il.Emit(OpCodes.Clt);
                break;
            case ScanCompareOp.LessThanOrEqual:
                // !(a > b)
                il.Emit(OpCodes.Cgt);
                il.Emit(OpCodes.Ldc_I4_0);
                il.Emit(OpCodes.Ceq);
                break;
            default:
                // Drop the two operands and produce false.
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
        il.Emit(OpCodes.Ldfld, SelfLongParams);
        EmitLdcI4(il, index);
        il.Emit(OpCodes.Ldelem_I8);
    }

    private static void EmitLoadDoubleParam(ILGenerator il, int index)
    {
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Ldfld, SelfDoubleParams);
        EmitLdcI4(il, index);
        il.Emit(OpCodes.Ldelem_R8);
    }

    private static void EmitLoadSliceSpan(ILGenerator il, int paramIndex)
    {
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Ldfld, SelfSliceParams);
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
