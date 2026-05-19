using System;
using System.Reflection.Emit;
using System.Text;
using Corax.Querying.Matches;
using Corax.Utils;
using Voron;

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
///
/// IL and the diagnostic C# source are emitted in lockstep through <see cref="DualEmit"/>:
/// each predicate primitive pushes to both the IL evaluation stack and a parallel
/// textual operand stack, so the two backends cannot drift.
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
    /// <paramref name="csharpSource"/> receives the C# equivalent of the emitted IL,
    /// generated side-by-side via <see cref="DualEmit"/>.</summary>
    public static ResidualScanPredicate EmitDelegate(ScanPredicateInfo[] predicates, out string csharpSource)
    {
        if (predicates == null || predicates.Length == 0)
        {
            csharpSource = "// No residual predicates.\n";
            return null;
        }

        var cs = new StringBuilder();
        cs.AppendLine("static int ResidualScan(IPredicateEvaluationContext ctx, Span<EntryTermsReader> readers, Span<long> entryIds, Span<int> originalIndexes)");
        cs.AppendLine("{");
        cs.AppendLine("    int length = entryIds.Length;");
        cs.AppendLine("    int writeIdx = 0;");
        cs.AppendLine("    for (int i = 0; i < length; i++)");
        cs.AppendLine("    {");
        cs.AppendLine("        ref EntryTermsReader reader = ref readers[i];");

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
        // The "rejected" label is shared by every predicate's fail path. Use a raw IL
        // label here and emit the C# label literally so the generated source keeps a
        // clean "rejected:;" name (rather than the numbered form DualEmit would produce).
        // Per-predicate intermediate labels still go through DualEmit.
        var failLabel = il.DefineLabel();

        il.Emit(OpCodes.Br, loopCheck);

        il.MarkLabel(loopBody);

        il.Emit(OpCodes.Ldarga_S, (byte)1);
        il.Emit(OpCodes.Ldloc, iLocal);
        il.Emit(OpCodes.Call, IlEmitterShared.SpanEntryTermsReaderGetItem);
        il.Emit(OpCodes.Stloc, readerRefLocal);

        // DualEmit drives per-predicate emission: every primitive pushes IL + C# fragment
        // so the two outputs cannot drift. Indent matches the C# loop body opened above.
        var d = new DualEmit(il, cs, indent: "        ");
        int rootIdx = 0;
        for (int p = 0; p < predicates.Length; p++)
        {
            cs.AppendLine();
            EmitPredicate(ref d, in predicates[p], failLabel, ref rootIdx, readerRefLocal, p);
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

        // Mirror of the post-loop epilogue.
        cs.AppendLine();
        cs.AppendLine("        entryIds[writeIdx] = entryIds[i];");
        cs.AppendLine("        if (originalIndexes.Length != 0) originalIndexes[writeIdx] = originalIndexes[i];");
        cs.AppendLine("        writeIdx++; continue;");
        cs.AppendLine("        rejected:;");
        cs.AppendLine("    }");
        cs.AppendLine("    return writeIdx;");
        cs.AppendLine("}");
        csharpSource = cs.ToString();

        return (ResidualScanPredicate)dm.CreateDelegate(typeof(ResidualScanPredicate));
    }

    /// <summary>Emit one top-level predicate (leaf, AND group, or OR group). The OR group
    /// short-circuits to a per-group "passed" label after any branch succeeds; falling off
    /// the end means every branch failed and routes to <paramref name="failLabel"/>.</summary>
    private static void EmitPredicate(
        ref DualEmit d,
        in ScanPredicateInfo pred,
        Label failLabel,
        ref int rootIdx,
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
                    EmitLeafPredicate(ref d, in pred.SubPredicates[b], nextSub.IL, nextSub.Name, rootIdx, readerRefLocal);
                    // Branch succeeded — skip remaining alternatives.
                    d.GotoAlways(groupPassed);
                    d.MarkLabel(nextSub);
                    rootIdx++;
                }
                // All branches fell through → group fails.
                d.IL.Emit(OpCodes.Br, failLabel);
                d.CsLine("goto rejected;");
                d.MarkLabel(groupPassed);
            }
            else
            {
                for (int b = 0; b < pred.SubPredicates.Length; b++)
                {
                    EmitLeafPredicate(ref d, in pred.SubPredicates[b], failLabel, "rejected", rootIdx, readerRefLocal);
                    rootIdx++;
                }
            }
            return;
        }

        EmitLeafPredicate(ref d, in pred, failLabel, "rejected", rootIdx, readerRefLocal);
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
        LocalBuilder readerRefLocal)
    {
        // reader.Reset();
        d.IL.Emit(OpCodes.Ldloc, readerRefLocal);
        d.IL.Emit(OpCodes.Call, IlEmitterShared.ReaderReset);
        d.CsLine("reader.Reset();");

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
            d.IL.Emit(OpCodes.Ldloc, readerRefLocal);
            d.CsStack.Push("ref reader");
            d.LoadFieldRootPage(rootIdx);
            d.LoadSliceSpan(pred.ParamIndex);
            d.CallReturning(helper, arity: 3, csTemplate: helperName + "({0}, {1}, {2})");
            // Fail if helper returned false.
            EmitBranchFalse(ref d, failIl, failName);
            return;
        }

        // FindNext(ctx.ResidualFieldRootPages[rootIdx]) leaves a bool on both stacks.
        d.IL.Emit(OpCodes.Ldloc, readerRefLocal);
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
                // C# mirror: emit as nested `if (FindNext) { if (compare) goto fail; }`
                // matching the IL exactly. (The previous textual emitter emitted
                // `if (!FindNext) goto rejected;` here — wrong for NotEqual; C# is
                // diagnostic-only, but the lockstep emitter now mirrors the real IL.)
                var notFoundIl = d.IL.DefineLabel();
                // The DualEmit branch helpers would write IF (!found) goto notFound; here
                // we want a `goto skip` C# but no jump back — so we open a C# `if (found)`
                // block by hand around the equality compare.
                var foundFragment = d.CsStack.Pop();
                d.IL.Emit(OpCodes.Brfalse, notFoundIl);
                d.CsLine($"if ({foundFragment})");
                d.CsLine("{");
                var prev = d.Indent;
                d.Indent = prev + "    ";
                EmitTypedComparison(ref d, in pred, readerRefLocal);
                // FAIL if comparison is true (value equals).
                EmitBranchTrue(ref d, failIl, failName);
                d.Indent = prev;
                d.CsLine("}");
                d.IL.MarkLabel(notFoundIl);
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
                d.IL.Emit(OpCodes.Pop);
                d.IL.Emit(OpCodes.Pop);
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
        d.IL.Emit(OpCodes.Ldc_I4_1);
        d.CsLine($"{tmp} = true;");
        d.GotoAlways(done);

        d.MarkLabel(fail);
        d.IL.Emit(OpCodes.Ldc_I4_0);
        d.CsLine($"{tmp} = false;");

        d.MarkLabel(done);
        d.PushTempName(tmp);
    }

    /// <summary>Pop the top of both stacks and branch to (<paramref name="ilLabel"/>,
    /// <paramref name="csName"/>) when the value is false. The C# label name may be
    /// the literal "rejected" or a DualEmit-generated branch name, so we accept the
    /// raw string rather than a LabelPair.</summary>
    private static void EmitBranchFalse(ref DualEmit d, Label ilLabel, string csName)
    {
        d.IL.Emit(OpCodes.Brfalse, ilLabel);
        var a = d.CsStack.Pop();
        d.CsLine($"if (!{a}) goto {csName};");
    }

    private static void EmitBranchTrue(ref DualEmit d, Label ilLabel, string csName)
    {
        d.IL.Emit(OpCodes.Brtrue, ilLabel);
        var a = d.CsStack.Pop();
        d.CsLine($"if ({a}) goto {csName};");
    }
}
