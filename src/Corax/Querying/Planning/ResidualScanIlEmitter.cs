using System;
using System.Collections.Generic;
using System.Reflection;
using System.Reflection.Emit;
using System.Runtime.InteropServices;
using System.Text;
using Corax.Querying.Matches;
using Corax.Utils;
using Voron;

namespace Corax.Querying.Planning;

/// <summary>
/// Emits IL for residual-predicate evaluation delegates used by both the entry-scan
/// path (CompiledQueryMatch) and the direct-scan path (DirectScanMatch). A single
/// delegate works against a <c>ref</c> to <see cref="ResidualParams"/>, which both
/// context types embed as a field. Per-predicate value type, compare op, AND/OR
/// sub-groups, and fieldRootPages indexing are baked into IL at emit time. Loads
/// against the residual arrays use plain <c>Ldfld</c> on the byref struct — no
/// interface dispatch, fully JIT-inlineable.
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
    public delegate int ResidualScanPredicate(
        QueryExecution exec,
        Span<EntryTermsReader> readers,
        Span<long> entryIds,
        Span<int> originalIndexes);

    /// <summary>Emit a residual-scan delegate that evaluates <paramref name="predicates"/>
    /// against each reader in the batch. Passing entry IDs and (optionally) original indexes
    /// are compacted to the front of their spans. Returns the count of survivors.
    /// The emitted IL always evaluates ALL predicates against every entry.
    /// <paramref name="csharpSource"/> receives the C# equivalent of the emitted IL,
    /// generated side-by-side via <see cref="DualEmit"/>.</summary>
    public static ResidualScanPredicate EmitDelegate(Span<ScanPredicateInfo> predicates, out string csharpSource)
    {
        if (predicates.IsEmpty)
        {
            csharpSource = "// No residual predicates.\n";
            return null;
        }

        var dm = new DynamicMethod(
            "ResidualScan",
            typeof(int),
            [typeof(QueryExecution), typeof(Span<EntryTermsReader>), typeof(Span<long>), typeof(Span<int>)],
            typeof(QueryExecution).Module,
            skipVisibility: true)
        {
            InitLocals = false
        };

        var il = dm.GetILGenerator();
        var cs = new StringBuilder();
        var d = new DualEmit(il, cs);

        // Register arguments for C# name tracking. exec (arg 0) is used implicitly by
        // LoadLongParam/LoadDoubleParam/LoadSliceSpan/LoadFieldRootPage, which emit Ldarg_0.
        _ = d.RegisterArg("exec");
        var readersIdx = d.RegisterArg("readers");
        var entryIdsIdx =  d.RegisterArg("entryIds");
        var originalIndexesIdx = d.RegisterArg("originalIndexes");

        // C# function signature (no IL equivalent).
        d.CsLine("static int ResidualScan(QueryExecution exec, Span<EntryTermsReader> readers, Span<long> entryIds, Span<int> originalIndexes)");
        d.CsLine("{");

        // Locals
        var iLocal = d.DeclareLocal(typeof(int), "i");
        var writeIdxLocal = d.DeclareLocal(typeof(int), "writeIdx");
        var lengthLocal = d.DeclareLocal(typeof(int), "length");
        var readerRefLocal = d.DeclareLocalRef(typeof(EntryTermsReader), "reader");
        var origIdxLengthLocal = d.DeclareLocal(typeof(int), "origIdxLength");

        // length = entryIds.Length
        EmitSpanLengthToLocal(ref d, entryIdsIdx, IlEmitterShared.SpanLongLength, lengthLocal);
        // origIdxLength = originalIndexes.Length
        EmitSpanLengthToLocal(ref d, originalIndexesIdx, IlEmitterShared.SpanIntLength, origIdxLengthLocal);
        // writeIdx = 0; i = 0
        d.StoreLocalConst(writeIdxLocal, 0);
        d.StoreLocalConst(iLocal, 0);

        // Loop structure
        var loopCheck = d.DefineLabelPair("loopCheck");
        var loopBody = d.DefineLabelPair("loopBody");
        var loopIncrement = d.DefineLabelPair("loopInc");
        var rejected = d.DefineNamedLabel("rejected");

        d.GotoAlways(loopCheck);

        d.MarkLabel(loopBody);

        // ref reader = ref readers[i]
        EmitSpanGetItemRef(ref d, readersIdx, IlEmitterShared.SpanEntryTermsReaderGetItem, iLocal, readerRefLocal);

        // Per-predicate emission (already through DualEmit).
        int rootIdx = 0;
        int inSetIdx = 0;
        for (int p = 0; p < predicates.Length; p++)
        {
            d.CsLine("");
            EmitPredicate(ref d, in predicates[p], rejected.Il, ref rootIdx, ref inSetIdx, readerRefLocal, p);
        }

        // All passed: entryIds[writeIdx] = entryIds[i]
        EmitSpanElementCopy(ref d, entryIdsIdx, IlEmitterShared.SpanLongGetItem, writeIdxLocal, iLocal, OpCodes.Ldind_I8, OpCodes.Stind_I8);

        // if (originalIndexes.Length == 0) skip copy
        var noOrigIdx = d.DefineLabelPair("noOrigIdx");
        EmitSpanLengthBranchIfZero(ref d, originalIndexesIdx, IlEmitterShared.SpanIntLength, noOrigIdx);

        // originalIndexes[writeIdx] = originalIndexes[i]
        EmitSpanElementCopy(ref d, originalIndexesIdx, IlEmitterShared.SpanIntGetItem, writeIdxLocal, iLocal, OpCodes.Ldind_I4, OpCodes.Stind_I4);

        d.MarkLabel(noOrigIdx);

        // writeIdx++
        d.IncrementLocal(writeIdxLocal);
        d.GotoAlways(loopIncrement);

        // rejected:
        d.MarkLabel(rejected);

        // i++
        d.MarkLabel(loopIncrement);
        d.IncrementLocal(iLocal);

        // loop check: if (i < length) goto loopBody
        d.MarkLabel(loopCheck);
        d.LoadLocal(iLocal);
        d.LoadLocal(lengthLocal);
        d.BranchLT(loopBody);

        // return writeIdx
        d.LoadLocal(writeIdxLocal);
        d.EmitReturn();

        d.CsLine("}");
        csharpSource = cs.ToString();

        return (ResidualScanPredicate)dm.CreateDelegate(typeof(ResidualScanPredicate));
    }

    /// <summary>target = arg.Length</summary>
    private static void EmitSpanLengthToLocal(ref DualEmit d, byte argIdx, MethodInfo lengthGetter, LocalBuilder target)
    {
        d.LoadArgAddress(argIdx);
        d.Il.Emit(OpCodes.Call, lengthGetter);
        d.Il.Emit(OpCodes.Stloc, target);
        var argName = d.CsStack.Pop();
        d.CsLine($"{d.GetLocalName(target)} = {argName}.Length;");
    }

    /// <summary>ref dest = ref arg[index]</summary>
    private static void EmitSpanGetItemRef(ref DualEmit d, byte argIdx, MethodInfo getItem,
        LocalBuilder indexLocal, LocalBuilder destRef)
    {
        d.Il.Emit(OpCodes.Ldarga_S, argIdx);
        d.Il.Emit(OpCodes.Ldloc, indexLocal);
        d.Il.Emit(OpCodes.Call, getItem);
        d.Il.Emit(OpCodes.Stloc, destRef);
        d.CsLine($"ref var {d.GetLocalName(destRef)} = ref {d.GetArgName(argIdx)}[{d.GetLocalName(indexLocal)}];");
    }

    /// <summary>arg[destIdx] = arg[srcIdx]</summary>
    private static void EmitSpanElementCopy(ref DualEmit d, byte argIdx, MethodInfo getItem,
        LocalBuilder destIdx, LocalBuilder srcIdx, OpCode loadIndirect, OpCode storeIndirect)
    {
        // &arg[destIdx]
        d.Il.Emit(OpCodes.Ldarga_S, argIdx);
        d.Il.Emit(OpCodes.Ldloc, destIdx);
        d.Il.Emit(OpCodes.Call, getItem);
        // arg[srcIdx] value
        d.Il.Emit(OpCodes.Ldarga_S, argIdx);
        d.Il.Emit(OpCodes.Ldloc, srcIdx);
        d.Il.Emit(OpCodes.Call, getItem);
        d.Il.Emit(loadIndirect);
        // store
        d.Il.Emit(storeIndirect);

        string argName = d.GetArgName(argIdx);
        d.CsLine($"{argName}[{d.GetLocalName(destIdx)}] = {argName}[{d.GetLocalName(srcIdx)}];");
    }

    /// <summary>if (arg.Length == 0) goto target</summary>
    private static void EmitSpanLengthBranchIfZero(ref DualEmit d, byte argIdx, MethodInfo lengthGetter, LabelPair target)
    {
        d.Il.Emit(OpCodes.Ldarga_S, argIdx);
        d.Il.Emit(OpCodes.Call, lengthGetter);
        d.Il.Emit(OpCodes.Ldc_I4_0);
        d.Il.Emit(OpCodes.Ceq);
        d.Il.Emit(OpCodes.Brtrue, target.Il);
        d.CsLine($"if ({d.GetArgName(argIdx)}.Length == 0) goto {target.Name};");
    }

    /// <summary>Emit one top-level predicate (leaf, AND group, or OR group). The OR group
    /// short-circuits to a per-group "passed" label after any branch succeeds; falling off
    /// the end means every branch failed and routes to <paramref name="failLabel"/>.</summary>
    private static void EmitPredicate(
        ref DualEmit d,
        in ScanPredicateInfo pred,
        Label failLabel,
        ref int rootIdx,
        ref int inSetIdx,
        LocalBuilder readerRefLocal,
        int pIdx)
    {
        if (pred.SubPredicates != null)
        {
            if (pred.Group == GroupKind.Or)
            {
                var groupPassed = d.DefineLabelPair($"gp_{pIdx}");
                for (int b = 0; b < pred.SubPredicates.Length; b++)
                {
                    var nextSub = d.DefineLabelPair("nextBranch");
                    EmitLeafPredicate(ref d, in pred.SubPredicates[b], nextSub.Il, nextSub.Name, rootIdx, ref inSetIdx, readerRefLocal);
                    // Branch succeeded — skip remaining alternatives.
                    d.GotoAlways(groupPassed);
                    d.MarkLabel(nextSub);
                    rootIdx++;
                }
                // All branches fell through → group fails.
                d.Il.Emit(OpCodes.Br, failLabel);
                d.CsLine("goto rejected;");
                d.MarkLabel(groupPassed);
            }
            else
            {
                for (int b = 0; b < pred.SubPredicates.Length; b++)
                {
                    EmitLeafPredicate(ref d, in pred.SubPredicates[b], failLabel, "rejected", rootIdx, ref inSetIdx, readerRefLocal);
                    rootIdx++;
                }
            }
            return;
        }

        EmitLeafPredicate(ref d, in pred, failLabel, "rejected", rootIdx, ref inSetIdx, readerRefLocal);
        rootIdx++;
    }

    /// <summary>Emit FindNext + per-op comparison for one leaf predicate. A failure routes
    /// to (<paramref name="failIl"/>, <paramref name="failName"/>) — either the global "rejected"
    /// label (top-level/AND group) or the OR-branch "nextBranch" label.</summary>
    private static void EmitLeafPredicate(
        ref DualEmit d,
        in ScanPredicateInfo pred,
        Label failIl,
        string failName,
        int rootIdx,
        ref int inSetIdx,
        LocalBuilder readerRefLocal)
    {
        // reader.Reset();
        d.Il.Emit(OpCodes.Ldloc, readerRefLocal);
        d.Il.Emit(OpCodes.Call, IlEmitterShared.ReaderReset);
        d.CsLine("reader.Reset();");

        // IN / ALL IN iterate ALL terms for the field against a per-execution value set
        // (CompiledQueryHelper does the loop + null handling), mirroring StartsWith/EndsWith.
        if (pred.CompareOp == ScanCompareOp.In || pred.CompareOp == ScanCompareOp.AllIn)
        {
            bool allIn = pred.CompareOp == ScanCompareOp.AllIn;
            var (helper, helperName) = pred.ValueType switch
            {
                ScanValueType.Long => allIn
                    ? (IlEmitterShared.CheckFieldTermAllInLong, "CompiledQueryHelper.CheckFieldTermAllInLong")
                    : (IlEmitterShared.CheckFieldTermInLong, "CompiledQueryHelper.CheckFieldTermInLong"),
                ScanValueType.Double => allIn
                    ? (IlEmitterShared.CheckFieldTermAllInDouble, "CompiledQueryHelper.CheckFieldTermAllInDouble")
                    : (IlEmitterShared.CheckFieldTermInDouble, "CompiledQueryHelper.CheckFieldTermInDouble"),
                _ => allIn
                    ? (IlEmitterShared.CheckFieldTermAllInSlice, "CompiledQueryHelper.CheckFieldTermAllInSlice")
                    : (IlEmitterShared.CheckFieldTermInSlice, "CompiledQueryHelper.CheckFieldTermInSlice"),
            };

            // ref reader, fieldRootPage, values[], includeNull
            d.Il.Emit(OpCodes.Ldloc, readerRefLocal);
            d.CsStack.Push("ref reader");
            d.LoadFieldRootPage(rootIdx);
            d.LoadInValueArray(inSetIdx, pred.ValueType);
            d.LoadInHasNull(inSetIdx);
            d.CallReturning(helper, arity: 4, csTemplate: helperName + "({0}, {1}, {2}, {3})");
            // Positive IN/ALL IN: fail when membership is false. Negated (NOT IN / NOT ALL IN):
            // fail when membership is true — a missing/null field has membership false, so it
            // passes, matching the bitmap AndNot complement.
            EmitBranch(ref d, failOnTrue: pred.Negated, failIl, failName);
            inSetIdx++;
            return;
        }

        // StartsWith / EndsWith are full-field scans rather than positioning calls — they
        // wrap FindNext internally, so they replace the usual FindNext+compare sequence.
        if (pred.CompareOp == ScanCompareOp.StartsWith || pred.CompareOp == ScanCompareOp.EndsWith)
        {
            var helper = pred.CompareOp == ScanCompareOp.StartsWith
                ? IlEmitterShared.CheckFieldTermStartsWith
                : IlEmitterShared.CheckFieldTermEndsWith;
            var helperName = pred.CompareOp == ScanCompareOp.StartsWith
                ? "CompiledQueryHelper.CheckFieldTermStartsWith"
                : "CompiledQueryHelper.CheckFieldTermEndsWith";

            // ref reader, fieldRootPage, paramSpan
            d.Il.Emit(OpCodes.Ldloc, readerRefLocal);
            d.CsStack.Push("ref reader");
            d.LoadFieldRootPage(rootIdx);
            d.LoadSliceSpan(pred.ParamIndex);
            d.CallReturning(helper, arity: 3, csTemplate: helperName + "({0}, {1}, {2})");
            // Fail if helper returned false.
            EmitBranchFalse(ref d, failIl, failName);
            return;
        }

        // FindNext(ctx.ResidualFieldRootPages[rootIdx]) leaves a bool on both stacks.
        d.Il.Emit(OpCodes.Ldloc, readerRefLocal);
        d.CsStack.Push("reader");
        d.LoadFieldRootPage(rootIdx);
        d.CallReturning(IlEmitterShared.ReaderFindNext, arity: 2, csTemplate: "{0}.FindNext({1})");

        switch (pred.CompareOp)
        {
            case ScanCompareOp.Exists:
                // Predicate succeeds iff FindNext returned true.
                EmitBranchFalse(ref d, failIl, failName);
                break;

            case ScanCompareOp.NotEqual:
            {
                // NotEqual semantics: if the field is absent (FindNext == false), the
                // predicate PASSES (nothing to be unequal to ≠ violation). If found, then
                // it fails iff the value compares equal.
                var notFoundIl = d.Il.DefineLabel();
                var foundFragment = d.CsStack.Pop();
                d.Il.Emit(OpCodes.Brfalse, notFoundIl);
                d.CsLine($"if ({foundFragment})");
                d.CsLine("{");
                EmitTypedComparison(ref d, in pred, readerRefLocal);
                // FAIL if comparison is true (value equals).
                EmitBranchTrue(ref d, failIl, failName);
                d.CsLine("}");
                d.Il.MarkLabel(notFoundIl);
                break;
            }

            default:
            {
                // FindNext must return true.
                EmitBranchFalse(ref d, failIl, failName);
                // And the typed comparison must hold.
                EmitTypedComparison(ref d, in pred, readerRefLocal);
                EmitBranchFalse(ref d, failIl, failName);
                break;
            }
        }
    }

    /// <summary>Emit the comparison portion of a leaf predicate. On entry both stacks are
    /// balanced; on exit both stacks have one extra bool (top of IL stack + C# fragment).
    /// Equality/relational comparisons compose via the DualEmit stack machine; BETWEEN
    /// uses a tiny diamond materializing 1/0 onto the stacks.</summary>
    private static void EmitTypedComparison(
        ref DualEmit d,
        in ScanPredicateInfo pred,
        LocalBuilder readerRefLocal)
    {
        switch (pred.ValueType)
        {
            case ScanValueType.Long:
                if (pred.CompareOp == ScanCompareOp.Between)
                {
                    EmitLongBetween(ref d, readerRefLocal, pred.ParamIndex, pred.ParamIndex2);
                    break;
                }
                d.LoadReaderCurrentLong(readerRefLocal);
                d.LoadLongParam(pred.ParamIndex);
                EmitNumericCompareOp(ref d, pred.CompareOp);
                break;

            case ScanValueType.Double:
                if (pred.CompareOp == ScanCompareOp.Between)
                {
                    EmitDoubleBetween(ref d, readerRefLocal, pred.ParamIndex, pred.ParamIndex2);
                    break;
                }
                d.LoadReaderCurrentDouble(readerRefLocal);
                d.LoadDoubleParam(pred.ParamIndex);
                EmitNumericCompareOp(ref d, pred.CompareOp);
                break;

            case ScanValueType.Slice:
            case ScanValueType.SliceLong:
            {
                if (pred.CompareOp == ScanCompareOp.Between)
                {
                    // Diamond mirroring the long/double Between using SequenceCompareTo(a,b) vs 0.
                    var fail = d.DefineLabelPair("sliceBetweenFail");
                    var done = d.DefineLabelPair("sliceBetweenDone");

                    // a.SequenceCompareTo(low) < 0 → fail
                    d.LoadReaderDecodedSlice(readerRefLocal);
                    d.LoadSliceSpan(pred.ParamIndex);
                    d.CallReturning(IlEmitterShared.SequenceCompareTo, arity: 2, csTemplate: "{0}.SequenceCompareTo({1})");
                    d.PushConstInt(0);
                    d.BranchLT(fail);

                    // a.SequenceCompareTo(high) > 0 → fail
                    d.LoadReaderDecodedSlice(readerRefLocal);
                    d.LoadSliceSpan(pred.ParamIndex2);
                    d.CallReturning(IlEmitterShared.SequenceCompareTo, arity: 2, csTemplate: "{0}.SequenceCompareTo({1})");
                    d.PushConstInt(0);
                    d.BranchGT(fail);

                    EmitBetweenTail(ref d, fail, done);
                    break;
                }
                if (pred.CompareOp == ScanCompareOp.Equal || pred.CompareOp == ScanCompareOp.NotEqual)
                {
                    d.LoadReaderDecodedSlice(readerRefLocal);
                    d.LoadSliceSpan(pred.ParamIndex);
                    d.CallReturning(IlEmitterShared.SequenceEqual, arity: 2, csTemplate: "{0}.SequenceEqual({1})");
                    break;
                }
                // Relational: compare SequenceCompareTo result against 0 using the same op.
                d.LoadReaderDecodedSlice(readerRefLocal);
                d.LoadSliceSpan(pred.ParamIndex);
                d.CallReturning(IlEmitterShared.SequenceCompareTo, arity: 2, csTemplate: "{0}.SequenceCompareTo({1})");
                d.PushConstInt(0);
                EmitNumericCompareOp(ref d, pred.CompareOp);
                break;
            }

            default:
                // Unknown value type — emit a constant false so the predicate always rejects.
                d.PushConstBool(false);
                break;
        }
    }

    /// <summary>Pop two numerics from both stacks and push 1/0 representing the compare result.</summary>
    private static void EmitNumericCompareOp(ref DualEmit d, ScanCompareOp op)
    {
        switch (op)
        {
            case ScanCompareOp.Equal:
            case ScanCompareOp.NotEqual:
                // Caller decides whether to invert (NotEqual branches on TRUE-equal → fail).
                d.Ceq();
                break;
            case ScanCompareOp.GreaterThan:
                d.Cgt();
                break;
            case ScanCompareOp.GreaterThanOrEqual:
                // !(a < b)
                d.Clt();
                d.LogicalNot();
                break;
            case ScanCompareOp.LessThan:
                d.Clt();
                break;
            case ScanCompareOp.LessThanOrEqual:
                // !(a > b)
                d.Cgt();
                d.LogicalNot();
                break;
            default:
                // Unknown compare → constant false. Discard the two operands first.
                d.Il.Emit(OpCodes.Pop);
                d.Il.Emit(OpCodes.Pop);
                d.CsStack.Pop();
                d.CsStack.Pop();
                d.PushConstBool(false);
                break;
        }
    }

    /// <summary>BETWEEN diamond over <c>reader.CurrentLong</c> and two long params.
    /// Materializes 1/0 on the IL stack mirrored by a bool temp on the C# stack.</summary>
    private static void EmitLongBetween(ref DualEmit d, LocalBuilder readerRefLocal, int loIdx, int hiIdx)
    {
        var fail = d.DefineLabelPair("betweenFail");
        var done = d.DefineLabelPair("betweenDone");

        d.LoadReaderCurrentLong(readerRefLocal);
        d.LoadLongParam(loIdx);
        d.BranchLT(fail);

        d.LoadReaderCurrentLong(readerRefLocal);
        d.LoadLongParam(hiIdx);
        d.BranchGT(fail);

        EmitBetweenTail(ref d, fail, done);
    }

    /// <summary>BETWEEN diamond over <c>reader.CurrentDouble</c> and two double params.
    /// Uses unsigned float comparisons (Blt_Un / Bgt_Un) — matches the original IL.</summary>
    private static void EmitDoubleBetween(ref DualEmit d, LocalBuilder readerRefLocal, int loIdx, int hiIdx)
    {
        var fail = d.DefineLabelPair("betweenFail");
        var done = d.DefineLabelPair("betweenDone");

        d.LoadReaderCurrentDouble(readerRefLocal);
        d.LoadDoubleParam(loIdx);
        d.BranchLTUnsigned(fail);

        d.LoadReaderCurrentDouble(readerRefLocal);
        d.LoadDoubleParam(hiIdx);
        d.BranchGTUnsigned(fail);

        EmitBetweenTail(ref d, fail, done);
    }

    /// <summary>Shared tail for BETWEEN diamonds: materialize the 1/0 result via a temp,
    /// emit the fail/done labels, and leave a bool fragment on the C# stack.</summary>
    private static void EmitBetweenTail(ref DualEmit d, LabelPair fail, LabelPair done)
    {
        var tmp = d.DeclareTempBool("between");
        d.Il.Emit(OpCodes.Ldc_I4_1);
        d.CsLine($"{tmp} = true;");
        d.GotoAlways(done);

        d.MarkLabel(fail);
        d.Il.Emit(OpCodes.Ldc_I4_0);
        d.CsLine($"{tmp} = false;");

        d.MarkLabel(done);
        d.PushTempName(tmp);
    }

    /// <summary>Pop the top of both stacks and branch to (<paramref name="ilLabel"/>,
    /// <paramref name="csName"/>) when the value is false. The C# label name may be
    /// the literal "rejected" or a DualEmit-generated branch name, so we accept the
    /// raw string rather than a LabelPair.</summary>
    private static void EmitBranchFalse(ref DualEmit d, Label ilLabel, string csName) => EmitBranch(ref d, failOnTrue: false, ilLabel, csName);

    private static void EmitBranchTrue(ref DualEmit d, Label ilLabel, string csName) => EmitBranch(ref d, failOnTrue: true, ilLabel, csName);

    /// <summary>Pop the top of both stacks and branch to the fail target. When
    /// <paramref name="failOnTrue"/> is set the branch fires on a true value (negated leaf:
    /// fail when membership holds); otherwise it fires on false (the positive default).</summary>
    private static void EmitBranch(ref DualEmit d, bool failOnTrue, Label ilLabel, string csName)
    {
        d.Il.Emit(failOnTrue ? OpCodes.Brtrue : OpCodes.Brfalse, ilLabel);
        var a = d.CsStack.Pop();
        d.CsLine(failOnTrue ? $"if ({a}) goto {csName};" : $"if (!{a}) goto {csName};");
    }
}
