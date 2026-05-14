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
/// Emits the per-entry residual-predicate evaluator delegate used by the entry-scan
/// fallback path. Per-CompiledPlan: the ScanPredicateInfo array is fixed for a given
/// query shape, so all predicate dispatch (value type, compare op, AND/OR sub-groups,
/// fieldRootPages indexing) is baked into IL once and reused for every execution.
/// This replaces the runtime switches that used to live in CompiledQueryHelper.
/// </summary>
public static class EntryScanIlEmitter
{
    public delegate bool CompiledEntryPredicate(CompiledQueryMatch ctx, ref EntryTermsReader reader);

    private const BindingFlags AnyInstance =
        BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;

    // CompiledQueryMatch fields the emitted IL reads
    private static readonly FieldInfo CtxScanLongParams =
        typeof(CompiledQueryMatch).GetField(nameof(CompiledQueryMatch.ScanLongParams), AnyInstance)!;
    private static readonly FieldInfo CtxScanDoubleParams =
        typeof(CompiledQueryMatch).GetField(nameof(CompiledQueryMatch.ScanDoubleParams), AnyInstance)!;
    private static readonly FieldInfo CtxScanSliceParams =
        typeof(CompiledQueryMatch).GetField(nameof(CompiledQueryMatch.ScanSliceParams), AnyInstance)!;
    private static readonly FieldInfo CtxScanFieldRootPages =
        typeof(CompiledQueryMatch).GetField(nameof(CompiledQueryMatch.ScanFieldRootPages), AnyInstance)!;

    // EntryTermsReader members
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

    // Single-term StartsWith / EndsWith helpers (compare against current term only,
    // matching the legacy EvaluateSinglePredicate behaviour).
    private static readonly MethodInfo SliceStartsWithHelper =
        typeof(CompiledQueryHelper).GetMethod(nameof(CompiledQueryHelper.SliceStartsWith))!;
    private static readonly MethodInfo SliceEndsWithHelper =
        typeof(CompiledQueryHelper).GetMethod(nameof(CompiledQueryHelper.SliceEndsWith))!;

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
    /// Build a per-entry predicate delegate that evaluates all predicates in
    /// <paramref name="predicates"/> against a single entry. Returns true when
    /// every top-level predicate passes (with AND-group semantics across the array,
    /// and per-group AND/OR semantics within sub-predicates).
    ///
    /// Returns null when there are no predicates (entry scan is a no-op).
    /// </summary>
    public static CompiledEntryPredicate EmitDelegate(ScanPredicateInfo[] predicates)
    {
        if (predicates == null || predicates.Length == 0)
            return null;

        var dm = new DynamicMethod(
            "EntryScanResidual",
            typeof(bool),
            [typeof(CompiledQueryMatch), typeof(EntryTermsReader).MakeByRefType()],
            typeof(CompiledQueryMatch).Module,
            skipVisibility: true)
        {
            InitLocals = false
        };

        var il = dm.GetILGenerator();

        var passLabel = il.DefineLabel();
        var failLabel = il.DefineLabel();

        // Walk predicates; each emits to either fall through to next or branch to failLabel.
        int rootIdx = 0;
        for (int p = 0; p < predicates.Length; p++)
        {
            EmitPredicate(il, in predicates[p], failLabel, ref rootIdx);
        }

        // All predicates passed → return true
        il.Emit(OpCodes.Ldc_I4_1);
        il.Emit(OpCodes.Br, passLabel);

        il.MarkLabel(failLabel);
        il.Emit(OpCodes.Ldc_I4_0);

        il.MarkLabel(passLabel);
        il.Emit(OpCodes.Ret);

        return (CompiledEntryPredicate)dm.CreateDelegate(typeof(CompiledEntryPredicate));
    }

    /// <summary>
    /// Emit IL for one top-level predicate. Leaf predicates use a sequential
    /// <paramref name="rootIdx"/> into <c>ctx.ScanFieldRootPages</c>; the index
    /// is advanced by one per leaf. AND/OR group predicates recurse on
    /// sub-predicates and advance <paramref name="rootIdx"/> by the group size.
    /// </summary>
    private static void EmitPredicate(
        ILGenerator il,
        in ScanPredicateInfo pred,
        Label failLabel,
        ref int rootIdx)
    {
        if (pred.SubPredicates != null)
        {
            if (pred.Group == GroupKind.Or)
            {
                // OR group: pass if ANY sub passes. Try each sub; if it passes, jump to
                // groupPassed; if it fails, fall through to next sub. If all fail, jump
                // to failLabel.
                var groupPassed = il.DefineLabel();
                for (int b = 0; b < pred.SubPredicates.Length; b++)
                {
                    var nextSub = il.DefineLabel();
                    EmitLeafPredicate(il, in pred.SubPredicates[b], nextSub, rootIdx);
                    // sub passed → entire OR group passes
                    il.Emit(OpCodes.Br, groupPassed);
                    il.MarkLabel(nextSub);
                    rootIdx++;
                }
                // All subs failed
                il.Emit(OpCodes.Br, failLabel);
                il.MarkLabel(groupPassed);
            }
            else
            {
                // AND group: pass only if ALL subs pass. Emit each sub sequentially with
                // the outer failLabel as failure target.
                for (int b = 0; b < pred.SubPredicates.Length; b++)
                {
                    EmitLeafPredicate(il, in pred.SubPredicates[b], failLabel, rootIdx);
                    rootIdx++;
                }
            }
            return;
        }

        EmitLeafPredicate(il, in pred, failLabel, rootIdx);
        rootIdx++;
    }

    /// <summary>
    /// Emit a single leaf predicate. Layout: Reset(); found = FindNext(rootPage);
    /// branch on (found, op) and inline the type-specific comparison.
    /// On failure, branches to <paramref name="failLabel"/>.
    /// </summary>
    private static void EmitLeafPredicate(
        ILGenerator il,
        in ScanPredicateInfo pred,
        Label failLabel,
        int rootIdx)
    {
        var nextPredicate = il.DefineLabel();
        var foundLabel = il.DefineLabel();

        // reader.Reset()
        il.Emit(OpCodes.Ldarg_1);
        il.Emit(OpCodes.Call, ReaderReset);

        // found = reader.FindNext(ctx.ScanFieldRootPages[rootIdx])
        il.Emit(OpCodes.Ldarg_1);
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Ldfld, CtxScanFieldRootPages);
        EmitLdcI4(il, rootIdx);
        il.Emit(OpCodes.Ldelem_I8);
        il.Emit(OpCodes.Call, ReaderFindNext);
        // stack: found (bool)

        switch (pred.CompareOp)
        {
            case ScanCompareOp.Exists:
                il.Emit(OpCodes.Brfalse, failLabel);
                il.Emit(OpCodes.Br, nextPredicate);
                break;

            case ScanCompareOp.NotEqual:
                // !found → continue (pass). found → evaluate; if compare returns true
                // (term equals param), the predicate fails.
                il.Emit(OpCodes.Brtrue, foundLabel);
                il.Emit(OpCodes.Br, nextPredicate);
                il.MarkLabel(foundLabel);
                EmitTypedComparison(il, in pred, rootIdx);
                il.Emit(OpCodes.Brtrue, failLabel);
                break;

            default:
                il.Emit(OpCodes.Brfalse, failLabel);
                EmitTypedComparison(il, in pred, rootIdx);
                il.Emit(OpCodes.Brfalse, failLabel);
                break;
        }

        il.MarkLabel(nextPredicate);
    }

    /// <summary>
    /// Push the result of the predicate comparison (bool) onto the stack. Caller branches.
    /// Argument 1 (<c>ref EntryTermsReader</c>) must be the active reader.
    /// </summary>
    private static void EmitTypedComparison(ILGenerator il, in ScanPredicateInfo pred, int rootIdx)
    {
        _ = rootIdx; // only consumed by the multi-value helpers above (none currently used)

        // StartsWith / EndsWith compare against the CURRENT term (matches legacy
        // EvaluateSinglePredicate; the outer Reset+FindNext has positioned the reader
        // at the first term for this field).
        if (pred.CompareOp == ScanCompareOp.StartsWith)
        {
            EmitLoadReaderDecodedSlice(il);
            EmitLoadSliceSpan(il, pred.ParamIndex);
            il.Emit(OpCodes.Call, SliceStartsWithHelper);
            return;
        }

        if (pred.CompareOp == ScanCompareOp.EndsWith)
        {
            EmitLoadReaderDecodedSlice(il);
            EmitLoadSliceSpan(il, pred.ParamIndex);
            il.Emit(OpCodes.Call, SliceEndsWithHelper);
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

                    EmitLoadReaderCurrentLong(il);
                    EmitLoadLongParam(il, pred.ParamIndex);
                    il.Emit(OpCodes.Blt, fail);

                    EmitLoadReaderCurrentLong(il);
                    EmitLoadLongParam(il, pred.ParamIndex2);
                    il.Emit(OpCodes.Bgt, fail);

                    il.Emit(OpCodes.Ldc_I4_1);
                    il.Emit(OpCodes.Br, done);
                    il.MarkLabel(fail);
                    il.Emit(OpCodes.Ldc_I4_0);
                    il.MarkLabel(done);
                    break;
                }

                EmitLoadReaderCurrentLong(il);
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

                    EmitLoadReaderCurrentDouble(il);
                    EmitLoadDoubleParam(il, pred.ParamIndex);
                    il.Emit(OpCodes.Blt_Un, fail);  // NaN-safe (NaN treated as fail)

                    EmitLoadReaderCurrentDouble(il);
                    EmitLoadDoubleParam(il, pred.ParamIndex2);
                    il.Emit(OpCodes.Bgt_Un, fail);

                    il.Emit(OpCodes.Ldc_I4_1);
                    il.Emit(OpCodes.Br, done);
                    il.MarkLabel(fail);
                    il.Emit(OpCodes.Ldc_I4_0);
                    il.MarkLabel(done);
                    break;
                }

                EmitLoadReaderCurrentDouble(il);
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

                    EmitLoadReaderDecodedSlice(il);
                    EmitLoadSliceSpan(il, pred.ParamIndex);
                    il.Emit(OpCodes.Call, SpanByteSequenceCompareTo);
                    il.Emit(OpCodes.Ldc_I4_0);
                    il.Emit(OpCodes.Blt, fail);

                    EmitLoadReaderDecodedSlice(il);
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
                    // Equality uses SequenceEqual; EmitLeafPredicate inverts on NotEqual.
                    EmitLoadReaderDecodedSlice(il);
                    EmitLoadSliceSpan(il, pred.ParamIndex);
                    il.Emit(OpCodes.Call, SpanByteSequenceEqual);
                    break;
                }

                // Ordered comparisons: SequenceCompareTo + compare-to-zero
                EmitLoadReaderDecodedSlice(il);
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

    /// <summary>Emit a comparison op against the two values currently on the stack
    /// (a, b). Equal/NotEqual produce (a == b) — EmitLeafPredicate's NotEqual branch
    /// inverts the bool externally.</summary>
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
                il.Emit(OpCodes.Pop);
                il.Emit(OpCodes.Pop);
                il.Emit(OpCodes.Ldc_I4_0);
                break;
        }
    }

    private static void EmitLoadReaderCurrentLong(ILGenerator il)
    {
        il.Emit(OpCodes.Ldarg_1);
        il.Emit(OpCodes.Ldfld, ReaderCurrentLong);
    }

    private static void EmitLoadReaderCurrentDouble(ILGenerator il)
    {
        il.Emit(OpCodes.Ldarg_1);
        il.Emit(OpCodes.Ldfld, ReaderCurrentDouble);
    }

    private static void EmitLoadReaderDecodedSlice(ILGenerator il)
    {
        il.Emit(OpCodes.Ldarg_1);
        il.Emit(OpCodes.Ldfld, ReaderCurrent);
        il.Emit(OpCodes.Callvirt, CompactKeyDecoded);
    }

    private static void EmitLoadLongParam(ILGenerator il, int index)
    {
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Ldfld, CtxScanLongParams);
        EmitLdcI4(il, index);
        il.Emit(OpCodes.Ldelem_I8);
    }

    private static void EmitLoadDoubleParam(ILGenerator il, int index)
    {
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Ldfld, CtxScanDoubleParams);
        EmitLdcI4(il, index);
        il.Emit(OpCodes.Ldelem_R8);
    }

    private static void EmitLoadSliceSpan(ILGenerator il, int paramIndex)
    {
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Ldfld, CtxScanSliceParams);
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
