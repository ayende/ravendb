using System;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;
using System.Threading;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Primitives;
using Corax.Utils;
using Voron;
using Voron.Data.CompactTrees;
using Voron.Data.RoaringBitmaps;

namespace Corax.Querying.Planning;

public static class QueryIlEmitter
{
    public delegate void CompiledExecuteDelegate(CompiledQueryMatch ctx);

    // CompiledQueryMatch fields (accessed by emitted IL)
    private static readonly FieldInfo CtxBitmaps = typeof(CompiledQueryMatch).GetField(nameof(CompiledQueryMatch.Bitmaps));
    private static readonly FieldInfo CtxTermSources = typeof(CompiledQueryMatch).GetField(nameof(CompiledQueryMatch.PostingSources));
    private static readonly FieldInfo CtxLimit = typeof(CompiledQueryMatch).GetField(nameof(CompiledQueryMatch.Limit));

    // Timing helpers
    private static readonly MethodInfo GetTimestamp =
        typeof(Stopwatch).GetMethod(nameof(Stopwatch.GetTimestamp))!;
    private static readonly MethodInfo RecordTiming =
        typeof(CompiledQueryHelper).GetMethod(nameof(CompiledQueryHelper.RecordTiming))!;
    private static readonly MethodInfo RecordResultCount =
        typeof(CompiledQueryHelper).GetMethod(nameof(CompiledQueryHelper.RecordResultCount))!;
    private static readonly MethodInfo RunEntryScanMethod =
        typeof(CompiledQueryHelper).GetMethod(nameof(CompiledQueryHelper.RunEntryScan))!;

    // IQueryMatch
    private static readonly MethodInfo MatchCountGetter = typeof(IQueryMatch).GetProperty(nameof(IQueryMatch.Count))!.GetGetMethod()!;

    // RoaringBitmap — methods called directly by emitted IL
    private static readonly MethodInfo AndWith =
        typeof(RoaringBitmap).GetMethod(nameof(RoaringBitmap.AndWith),
            [typeof(RoaringBitmap).MakeByRefType()])!;
    private static readonly MethodInfo OrWith =
        typeof(RoaringBitmap).GetMethod(nameof(RoaringBitmap.OrWith),
            [typeof(RoaringBitmap).MakeByRefType()])!;
    private static readonly MethodInfo LazyOrWith =
        typeof(RoaringBitmap).GetMethod(nameof(RoaringBitmap.LazyOrWith),
            System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Public,
            [typeof(RoaringBitmap).MakeByRefType()])!;
    private static readonly MethodInfo AndNotWith =
        typeof(RoaringBitmap).GetMethod(nameof(RoaringBitmap.AndNotWith),
            [typeof(RoaringBitmap).MakeByRefType()])!;
    private static readonly MethodInfo Clear =
        typeof(RoaringBitmap).GetMethod(nameof(RoaringBitmap.Clear), Type.EmptyTypes)!;
    private static readonly MethodInfo IsEmptyGetter = typeof(RoaringBitmap).GetProperty(nameof(RoaringBitmap.IsEmpty))!.GetGetMethod()!;
    private static readonly MethodInfo CountGetter = typeof(RoaringBitmap).GetProperty(nameof(RoaringBitmap.Count))!.GetGetMethod()!;
    private static readonly MethodInfo RepairAfterLazy =
        typeof(RoaringBitmap).GetMethod(nameof(RoaringBitmap.RepairAfterLazy), Type.EmptyTypes)!;
    private static readonly MethodInfo SwapContents =
        typeof(RoaringBitmap).GetMethod(nameof(RoaringBitmap.SwapContents),
            [typeof(RoaringBitmap).MakeByRefType()])!;

    // CancellationToken
    private static readonly MethodInfo ThrowIfCancelled = typeof(CancellationToken).GetMethod(nameof(CancellationToken.ThrowIfCancellationRequested))!;

    // Span<long>
    private static readonly ConstructorInfo SpanCtor = typeof(Span<long>).GetConstructor([typeof(void*), typeof(int)])!;
    // IndexSearcher — for entry scan
    private static readonly MethodInfo GetEntryTermsReader =
        typeof(IndexSearcher).GetMethod(nameof(IndexSearcher.GetEntryTermsReader),
            [typeof(long), typeof(Page).MakeByRefType(), typeof(CompactKey)])!;

    // CompiledQueryMatch typed parameter arrays
    private static readonly FieldInfo CtxResolvedMatches =
        typeof(CompiledQueryMatch).GetField(nameof(CompiledQueryMatch.ResolvedMatches));
    private static readonly FieldInfo CtxInRangeCounts =
        typeof(CompiledQueryMatch).GetField(nameof(CompiledQueryMatch.InRangeCounts));
    private static readonly FieldInfo CtxTermsProviders =
        typeof(CompiledQueryMatch).GetField(nameof(CompiledQueryMatch.TermsProviders));
    private static readonly FieldInfo CtxEntryScanTakenAtOp =
        typeof(CompiledQueryMatch).GetField(nameof(CompiledQueryMatch.EntryScanTakenAtOp));
    private static readonly FieldInfo CtxToken =
        typeof(CompiledQueryMatch).GetField(nameof(CompiledQueryMatch.Token));

    // CompactKey.Decoded() → ReadOnlySpan<byte>
    private static readonly MethodInfo CompactKeyDecoded =
        typeof(CompactKey).GetMethod(nameof(CompactKey.Decoded), Type.EmptyTypes)!;

    // Ctx-based entry points — take ref CompiledQueryMatch, IL just pushes ldarg.0 + int constants
    private static readonly MethodInfo CtxFillFromPostingSource = typeof(QueryPrimitives).GetMethod(nameof(QueryPrimitives.CtxFillFromPostingSource))!;
    private static readonly MethodInfo CtxFillFromTreeScan = typeof(QueryPrimitives).GetMethod(nameof(QueryPrimitives.CtxFillFromTreeScan))!;
    private static readonly MethodInfo CtxFillFromMatch = typeof(QueryPrimitives).GetMethod(nameof(QueryPrimitives.CtxFillFromMatch))!;
    private static readonly MethodInfo CtxOrFillFromPostingSource = typeof(QueryPrimitives).GetMethod(nameof(QueryPrimitives.CtxOrFillFromPostingSource))!;
    private static readonly MethodInfo CtxOrFillFromTreeScan = typeof(QueryPrimitives).GetMethod(nameof(QueryPrimitives.CtxOrFillFromTreeScan))!;
    private static readonly MethodInfo CtxOrFillFromMatch = typeof(QueryPrimitives).GetMethod(nameof(QueryPrimitives.CtxOrFillFromMatch))!;
    private static readonly MethodInfo CtxAndFromPostingSource = typeof(QueryPrimitives).GetMethod(nameof(QueryPrimitives.CtxAndFromPostingSource))!;
    private static readonly MethodInfo CtxAndFromTreeScan = typeof(QueryPrimitives).GetMethod(nameof(QueryPrimitives.CtxAndFromTreeScan))!;
    private static readonly MethodInfo CtxAndFromMatch = typeof(QueryPrimitives).GetMethod(nameof(QueryPrimitives.CtxAndFromMatch))!;
    private static readonly MethodInfo CtxAndNotFromPostingSource = typeof(QueryPrimitives).GetMethod(nameof(QueryPrimitives.CtxAndNotFromPostingSource))!;
    private static readonly MethodInfo CtxAndNotFromTreeScan = typeof(QueryPrimitives).GetMethod(nameof(QueryPrimitives.CtxAndNotFromTreeScan))!;
    private static readonly MethodInfo CtxAndNotFromMatch = typeof(QueryPrimitives).GetMethod(nameof(QueryPrimitives.CtxAndNotFromMatch))!;

    // Slice.AsReadOnlySpan() is used by EmitSliceComparison to load parameter values.
    // With arrays, we use ldelema to get ref Slice, then call AsReadOnlySpan.

    // MemoryExtensions.SequenceCompareTo<byte>(ReadOnlySpan<byte>, ReadOnlySpan<byte>)
    private static readonly MethodInfo SequenceCompareTo =
        typeof(MemoryExtensions).GetMethods()
            .First(m => m.Name == nameof(MemoryExtensions.SequenceCompareTo) && m.IsGenericMethod
                && m.GetParameters().Length == 2)
            .MakeGenericMethod(typeof(byte));

    // MemoryExtensions.SequenceEqual<byte>(ReadOnlySpan<byte>, ReadOnlySpan<byte>)
    private static readonly MethodInfo SequenceEqual =
        typeof(MemoryExtensions).GetMethods()
            .First(m => m.Name == nameof(MemoryExtensions.SequenceEqual) && m.IsGenericMethod
                && m.GetParameters().Length == 2)
            .MakeGenericMethod(typeof(byte));

    // Entry-scan cost heuristics — called from emitted IL so thresholds stay in one place
    private static readonly MethodInfo ShouldSwitchToEntryScan =
        typeof(QueryPrimitives).GetMethod(
            nameof(QueryPrimitives.ShouldSwitchToEntryScan),
            [typeof(long), typeof(long)])!;

    public static CompiledExecuteDelegate EmitDelegate(QueryExecution plan, out string explainSource, bool emitTimings = true)
    {
        var ops = plan.Ops;
        if (ops == null || ops.Length == 0)
        {
            explainSource = "// Empty plan";
            return EmptyExecute;
        }

        // EXPLAIN pseudocode built alongside IL emission.
        var explain = new StringBuilder();
        explain.AppendLine("// Compiled query — pseudocode mirroring the emitted IL.");

        var dm = new DynamicMethod(
            "CompiledQuery",
            typeof(void),
            [typeof(CompiledQueryMatch)],
            typeof(CompiledQueryMatch).Module,
            skipVisibility: true)
            {
                InitLocals = false
            };

        var il = dm.GetILGenerator();

        // Locals
        var bufferLocal = il.DeclareLocal(typeof(Span<long>));    // 0: Fill buffer
        var readLocal = il.DeclareLocal(typeof(int));              // 1: read count
        var startTickLocal = il.DeclareLocal(typeof(long));        // 2: timing start tick

        var doneLabel = il.DefineLabel();
        var entryScanLabel = il.DefineLabel();
        bool hasEntryScan = false;
        bool needsLazyRepair = false;
        int entryScanOpIndex = -1;

        // stackalloc long[FillBufferSize]
        IlEmitterShared.EmitLdcI4(il, QueryPrimitives.FillBufferSize);
        il.Emit(OpCodes.Conv_U);
        il.Emit(OpCodes.Sizeof, typeof(long));
        il.Emit(OpCodes.Mul_Ovf_Un);
        il.Emit(OpCodes.Localloc);
        IlEmitterShared.EmitLdcI4(il, QueryPrimitives.FillBufferSize);
        il.Emit(OpCodes.Newobj, SpanCtor);
        il.Emit(OpCodes.Stloc, bufferLocal);

        // Bounds-check elimination preamble: touch the max index of each array
        // so the JIT can hoist the length checks out of loops.
        EmitBoundsCheckPreamble(il, ops);

        for (int i = 0; i < ops.Length; i++)
        {
            ref PlanOp op = ref ops[i];

            // Timing: record start tick before each op (skipped for untimed delegate)
            if (emitTimings)
                EmitTimingStart(il, startTickLocal);

            // Resolve dispatch once per op — used by both IL emission and EXPLAIN.
            var (src, fillMethod, andMethod, orMethod, andNotMethod) = op.Dispatch switch
            {
                MatchDispatch.PostingList => (
                    $"ctx.TermSources[{op.ParamIndex}]",
                    CtxFillFromPostingSource, CtxAndFromPostingSource, CtxOrFillFromPostingSource, CtxAndNotFromPostingSource),
                MatchDispatch.TreeScan => (
                    $"ctx.TermsProviders[{op.ParamIndex}]",
                    CtxFillFromTreeScan, CtxAndFromTreeScan, CtxOrFillFromTreeScan, CtxAndNotFromTreeScan),
                _ => (
                    $"ctx.ResolvedMatches[{op.ParamIndex}]",
                    CtxFillFromMatch, CtxAndFromMatch, (MethodInfo)CtxOrFillFromMatch, CtxAndNotFromMatch)
            };

            switch (op.Kind)
            {
                case PlanOpKind.FillFromPostings:
                case PlanOpKind.DirectIterate:
                    EmitCancellationCheck(il);
                    il.Emit(OpCodes.Ldarg_0);
                    il.Emit(OpCodes.Ldc_I4, op.ParamIndex);
                    il.Emit(OpCodes.Call, fillMethod);
                    explain.AppendLine($"Fill(bitmap[0], {src});");
                    break;

                case PlanOpKind.AndWithPostings:
                    EmitCancellationCheck(il);
                    il.Emit(OpCodes.Ldarg_0);
                    il.Emit(OpCodes.Ldc_I4, op.ParamIndex);
                    il.Emit(OpCodes.Call, andMethod);
                    explain.AppendLine($"And(bitmap[0], {src});");
                    if (!op.SkipEarlyExit)
                    {
                        EmitLoadBitmapRef(il, 0);
                        il.Emit(OpCodes.Call, IsEmptyGetter);
                        il.Emit(OpCodes.Brtrue, doneLabel);
                        explain.AppendLine("if (bitmap[0].IsEmpty) return;");
                    }
                    break;

                case PlanOpKind.OrWithPostings:
                case PlanOpKind.LazyOrWithPostings:
                    EmitCancellationCheck(il);
                    il.Emit(OpCodes.Ldarg_0);
                    il.Emit(OpCodes.Ldc_I4, op.ParamIndex);
                    il.Emit(OpCodes.Ldc_I4, op.BitmapLocal);
                    il.Emit(OpCodes.Call, orMethod);
                    explain.AppendLine($"Or(bitmap[{op.BitmapLocal}], {src});");
                    if (op.BitmapLocal == 0)
                    {
                        EmitLoadBitmapRef(il, 0);
                        il.Emit(OpCodes.Call, CountGetter);
                        il.Emit(OpCodes.Conv_I8);
                        EmitLoadLimit(il);
                        il.Emit(OpCodes.Bge, doneLabel);
                        explain.AppendLine("if (bitmap[0].Count >= limit) return;");
                    }
                    break;

                case PlanOpKind.ClearBitmap:
                    EmitLoadBitmapRef(il, op.BitmapLocal);
                    il.Emit(OpCodes.Call, Clear);
                    explain.AppendLine($"bitmap[{op.BitmapLocal}].Clear();");
                    break;

                case PlanOpKind.AndBitmaps:
                    EmitLoadBitmapRef(il, op.BitmapLocal);
                    EmitLoadBitmapRef(il, op.ParamIndex2);
                    il.Emit(OpCodes.Call, AndWith);
                    explain.AppendLine($"bitmap[{op.BitmapLocal}].AndWith(bitmap[{op.ParamIndex2}]);");
                    break;

                case PlanOpKind.AndNotBitmaps:
                    EmitLoadBitmapRef(il, op.BitmapLocal);
                    EmitLoadBitmapRef(il, op.ParamIndex2);
                    il.Emit(OpCodes.Call, AndNotWith);
                    explain.AppendLine($"bitmap[{op.BitmapLocal}].AndNotWith(bitmap[{op.ParamIndex2}]);");
                    break;

                case PlanOpKind.OrBitmaps:
                    // Use LazyOrWith to defer popcount repair. Containers are stolen/merged
                    // immediately but cardinality is left as -1. A single RepairAfterLazy
                    // before the done label fixes all lazy cardinalities in one pass.
                    EmitLoadBitmapRef(il, op.BitmapLocal);
                    EmitLoadBitmapRef(il, op.ParamIndex2);
                    il.Emit(OpCodes.Call, LazyOrWith);
                    needsLazyRepair = true;
                    explain.AppendLine($"bitmap[{op.BitmapLocal}].LazyOrWith(bitmap[{op.ParamIndex2}]);");
                    // Skip Count-based limit check — Count is unreliable with lazy cardinality.
                    // The limit will be checked after RepairAfterLazy.
                    break;

                case PlanOpKind.SwapBitmaps:
                    EmitLoadBitmapRef(il, op.BitmapLocal);
                    EmitLoadBitmapRef(il, op.ParamIndex2);
                    il.Emit(OpCodes.Call, SwapContents);
                    explain.AppendLine($"Swap(bitmap[{op.BitmapLocal}], bitmap[{op.ParamIndex2}]);");
                    break;

                case PlanOpKind.CheckEmpty:
                    EmitLoadBitmapRef(il, op.BitmapLocal);
                    il.Emit(OpCodes.Call, IsEmptyGetter);
                    il.Emit(OpCodes.Brtrue, doneLabel);
                    explain.AppendLine($"if (bitmap[{op.BitmapLocal}].IsEmpty) return;");
                    break;

                case PlanOpKind.AndNotWithPostings:
                    EmitCancellationCheck(il);
                    il.Emit(OpCodes.Ldarg_0);
                    il.Emit(OpCodes.Ldc_I4, op.ParamIndex);
                    il.Emit(OpCodes.Call, andNotMethod);
                    explain.AppendLine($"AndNot(bitmap[0], {src});");
                    break;

                case PlanOpKind.RepairAfterLazy:
                    EmitLoadBitmapRef(il, 0);
                    il.Emit(OpCodes.Call, RepairAfterLazy);
                    explain.AppendLine("bitmap[0].RepairAfterLazy();");
                    break;

                case PlanOpKind.CheckAndMaybeEntryScan:
                {
                    hasEntryScan = true;
                    entryScanOpIndex = i;

                    EmitLoadBitmapRef(il, 0);
                    il.Emit(OpCodes.Call, CountGetter);
                    il.Emit(OpCodes.Conv_I8);
                    EmitLoadMatch(il, op.ParamIndex);
                    il.Emit(OpCodes.Callvirt, MatchCountGetter);
                    il.Emit(OpCodes.Call, ShouldSwitchToEntryScan);
                    il.Emit(OpCodes.Brtrue, entryScanLabel);
                    explain.AppendLine($"if (ShouldSwitchToEntryScan(bitmap[0].Count, {src}.Count)) goto EntryScan;");
                    break;
                }

                case PlanOpKind.OrRange:
                {
                    // for (int j = start; j < start + ctx.InRangeCounts[rangeIdx]; j++) Or(ctx, j, bitmapLocal);
                    // Count is read at runtime from ctx.InRangeCounts so the same compiled
                    // delegate handles different IN parameter array sizes.
                    int start = op.ParamIndex;
                    int rangeIdx = op.ParamIndex2; // index into InRangeCounts
                    var loopVar = il.DeclareLocal(typeof(int));
                    var endVar = il.DeclareLocal(typeof(int));
                    var loopCheck = il.DefineLabel();
                    var loopBody = il.DefineLabel();

                    // endVar = start + ctx.InRangeCounts[rangeIdx]
                    il.Emit(OpCodes.Ldc_I4, start);
                    il.Emit(OpCodes.Ldarg_0);
                    il.Emit(OpCodes.Ldfld, CtxInRangeCounts);
                    IlEmitterShared.EmitLdcI4(il, rangeIdx);
                    il.Emit(OpCodes.Ldelem_I4);
                    il.Emit(OpCodes.Add);
                    il.Emit(OpCodes.Stloc, endVar);

                    il.Emit(OpCodes.Ldc_I4, start);
                    il.Emit(OpCodes.Stloc, loopVar);
                    il.Emit(OpCodes.Br, loopCheck);

                    il.MarkLabel(loopBody);
                    EmitCancellationCheck(il);
                    il.Emit(OpCodes.Ldarg_0);
                    il.Emit(OpCodes.Ldloc, loopVar);
                    il.Emit(OpCodes.Ldc_I4, op.BitmapLocal);
                    il.Emit(OpCodes.Call, orMethod);

                    il.Emit(OpCodes.Ldloc, loopVar);
                    il.Emit(OpCodes.Ldc_I4_1);
                    il.Emit(OpCodes.Add);
                    il.Emit(OpCodes.Stloc, loopVar);

                    il.MarkLabel(loopCheck);
                    il.Emit(OpCodes.Ldloc, loopVar);
                    il.Emit(OpCodes.Ldloc, endVar);
                    il.Emit(OpCodes.Blt, loopBody);

                    explain.AppendLine($"for (i = {start}..{start}+InRangeCounts[{rangeIdx}]) Or(bitmap[{op.BitmapLocal}], {src}[i]);");
                    break;
                }

                case PlanOpKind.AndRange:
                {
                    // for (int j = start; j < start + ctx.InRangeCounts[rangeIdx]; j++) { And(ctx, j); if empty → done; }
                    int start = op.ParamIndex;
                    int rangeIdx = op.ParamIndex2; // index into InRangeCounts
                    var loopVar = il.DeclareLocal(typeof(int));
                    var endVar = il.DeclareLocal(typeof(int));
                    var loopCheck = il.DefineLabel();
                    var loopBody = il.DefineLabel();

                    // endVar = start + ctx.InRangeCounts[rangeIdx]
                    il.Emit(OpCodes.Ldc_I4, start);
                    il.Emit(OpCodes.Ldarg_0);
                    il.Emit(OpCodes.Ldfld, CtxInRangeCounts);
                    IlEmitterShared.EmitLdcI4(il, rangeIdx);
                    il.Emit(OpCodes.Ldelem_I4);
                    il.Emit(OpCodes.Add);
                    il.Emit(OpCodes.Stloc, endVar);

                    il.Emit(OpCodes.Ldc_I4, start);
                    il.Emit(OpCodes.Stloc, loopVar);
                    il.Emit(OpCodes.Br, loopCheck);

                    il.MarkLabel(loopBody);
                    EmitCancellationCheck(il);
                    il.Emit(OpCodes.Ldarg_0);
                    il.Emit(OpCodes.Ldloc, loopVar);
                    il.Emit(OpCodes.Call, andMethod);

                    // Early exit if bitmap is empty
                    if (!op.SkipEarlyExit)
                    {
                        EmitLoadBitmapRef(il, 0);
                        il.Emit(OpCodes.Call, IsEmptyGetter);
                        il.Emit(OpCodes.Brtrue, doneLabel);
                    }

                    il.Emit(OpCodes.Ldloc, loopVar);
                    il.Emit(OpCodes.Ldc_I4_1);
                    il.Emit(OpCodes.Add);
                    il.Emit(OpCodes.Stloc, loopVar);

                    il.MarkLabel(loopCheck);
                    il.Emit(OpCodes.Ldloc, loopVar);
                    il.Emit(OpCodes.Ldloc, endVar);
                    il.Emit(OpCodes.Blt, loopBody);

                    explain.AppendLine($"AndRange(bitmap[0], {src}[{start}..{start}+InRangeCounts[{rangeIdx}]]);");
                    break;
                }

                case PlanOpKind.IterateInto:
                    il.Emit(OpCodes.Br, doneLabel);
                    explain.AppendLine("return; // result in bitmap[0]");
                    break;
            }

            // Timing: record elapsed time and result count after each op (skipped for untimed delegate)
            if (emitTimings)
                EmitTimingEnd(il, i, startTickLocal);
        }

        il.MarkLabel(doneLabel);
        if (needsLazyRepair)
        {
            // Fix lazy cardinalities from LazyOrWith before the bitmap is read.
            EmitLoadBitmapRef(il, 0);
            il.Emit(OpCodes.Call, RepairAfterLazy);
            explain.AppendLine("bitmap[0].RepairAfterLazy();");
        }
        il.Emit(OpCodes.Ret);

        // Entry scan label — if any CheckAndMaybeEntryScan was emitted
        if (hasEntryScan)
        {
            il.MarkLabel(entryScanLabel);
            // Record which op triggered the entry scan
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Ldc_I4, entryScanOpIndex);
            il.Emit(OpCodes.Stfld, CtxEntryScanTakenAtOp);

            // Call CompiledQueryHelper.RunEntryScan(ctx, ref bitmap[0], ref bitmap[1]).
            // Predicate dispatch lives in ctx.CompiledEntryPredicate (IL-emitted per plan).
            il.Emit(OpCodes.Ldarg_0);                  // ctx
            EmitLoadBitmapRef(il, 0);                   // ref bitmap[0]
            EmitLoadBitmapRef(il, 1);                   // ref bitmap[1]
            il.Emit(OpCodes.Call, RunEntryScanMethod);

            // Swap bitmap[0] and bitmap[1], clear bitmap[1]
            EmitLoadBitmapRef(il, 0);
            EmitLoadBitmapRef(il, 1);
            il.Emit(OpCodes.Call, SwapContents);
            EmitLoadBitmapRef(il, 1);
            il.Emit(OpCodes.Call, Clear);
            il.Emit(OpCodes.Ret);
        }
        else
        {
            il.MarkLabel(entryScanLabel);
            il.Emit(OpCodes.Ret);
        }

        if (hasEntryScan)
            explain.AppendLine("EntryScan: // walks bitmap[0] re-checking per-entry predicates");

        explainSource = explain.ToString();
        return (CompiledExecuteDelegate)dm.CreateDelegate(typeof(CompiledExecuteDelegate));
    }

    private static void EmitLoadBitmapRef(ILGenerator il, int slot)
    {
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Ldfld, CtxBitmaps);  // RoaringBitmap[]
        IlEmitterShared.EmitLdcI4(il, slot);
        il.Emit(OpCodes.Ldelema, typeof(RoaringBitmap)); // ref RoaringBitmap
    }

    private static void EmitLoadMatch(ILGenerator il, int index)
    {
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Ldfld, CtxResolvedMatches); // IQueryMatch[]
        IlEmitterShared.EmitLdcI4(il, index);
        il.Emit(OpCodes.Ldelem_Ref);                  // IQueryMatch
    }

    private static void EmitLoadLimit(ILGenerator il)
    {
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Ldfld, CtxLimit);
    }

    /// <summary>Emit: startTick = Stopwatch.GetTimestamp()</summary>
    private static void EmitTimingStart(ILGenerator il, LocalBuilder startTickLocal)
    {
        il.Emit(OpCodes.Call, GetTimestamp);
        il.Emit(OpCodes.Stloc, startTickLocal);
    }

    /// <summary>Emit: RecordTiming(ref ctx, opIndex, startTick); RecordResultCount(ref ctx, opIndex);</summary>
    private static void EmitTimingEnd(ILGenerator il, int opIndex, LocalBuilder startTickLocal)
    {
        il.Emit(OpCodes.Ldarg_0);         // ref ctx
        IlEmitterShared.EmitLdcI4(il, opIndex);           // opIndex
        il.Emit(OpCodes.Ldloc, startTickLocal); // startTick
        il.Emit(OpCodes.Call, RecordTiming);

        il.Emit(OpCodes.Ldarg_0);
        IlEmitterShared.EmitLdcI4(il, opIndex);
        il.Emit(OpCodes.Call, RecordResultCount);
    }

    private static void EmitCancellationCheck(ILGenerator il)
    {
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Ldflda, CtxToken);
        il.Emit(OpCodes.Call, ThrowIfCancelled);
    }

    /// <summary>Emit array-length validation hints at the start of the delegate.
    /// Touch the maximum index of each array that will be accessed, so the JIT
    /// can hoist bounds checks out of loops.</summary>
    private static void EmitBoundsCheckPreamble(ILGenerator il, PlanOp[] ops)
    {
        int maxBitmapSlot = -1;
        int maxMatchIndex = -1;
        int maxTermSourceIndex = -1;
        int maxTermsProviderIndex = -1;

        for (int i = 0; i < ops.Length; i++)
        {
            ref PlanOp op = ref ops[i];

            // Bitmap slots
            if (op.BitmapLocal > maxBitmapSlot) maxBitmapSlot = op.BitmapLocal;
            if (op.Kind is PlanOpKind.AndBitmaps or PlanOpKind.AndNotBitmaps or PlanOpKind.OrBitmaps or PlanOpKind.SwapBitmaps)
            {
                if (op.ParamIndex2 > maxBitmapSlot) maxBitmapSlot = op.ParamIndex2;
            }
            // FillFromPostings always uses bitmap[0]; And/AndNot use [0] and [1]
            if (op.Kind is PlanOpKind.FillFromPostings or PlanOpKind.DirectIterate)
            {
                if (0 > maxBitmapSlot) maxBitmapSlot = 0;
            }
            if (op.Kind is PlanOpKind.AndWithPostings or PlanOpKind.AndNotWithPostings)
            {
                if (1 > maxBitmapSlot) maxBitmapSlot = 1;
            }

            // Source arrays
            switch (op.Dispatch)
            {
                case MatchDispatch.QueryMatch:
                    if (op.ParamIndex > maxMatchIndex) maxMatchIndex = op.ParamIndex;
                    break;
                case MatchDispatch.PostingList:
                    if (op.ParamIndex > maxTermSourceIndex) maxTermSourceIndex = op.ParamIndex;
                    break;
                case MatchDispatch.TreeScan:
                    if (op.ParamIndex > maxTermsProviderIndex) maxTermsProviderIndex = op.ParamIndex;
                    break;
            }

            // CheckAndMaybeEntryScan uses DirectSources for the match count check
            if (op.Kind == PlanOpKind.CheckAndMaybeEntryScan && op.ParamIndex > maxMatchIndex)
                maxMatchIndex = op.ParamIndex;
        }

        // Touch _bitmaps[maxBitmapSlot] (RoaringBitmap is a value type, use ldelema)
        if (maxBitmapSlot >= 0)
        {
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Ldfld, CtxBitmaps);
            IlEmitterShared.EmitLdcI4(il, maxBitmapSlot);
            il.Emit(OpCodes.Ldelema, typeof(RoaringBitmap));
            il.Emit(OpCodes.Pop);
        }

        // Touch _resolvedMatches[maxMatchIndex]
        if (maxMatchIndex >= 0)
        {
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Ldfld, CtxResolvedMatches);
            IlEmitterShared.EmitLdcI4(il, maxMatchIndex);
            il.Emit(OpCodes.Ldelem_Ref);
            il.Emit(OpCodes.Pop);
        }

        // Touch _termSources[maxTermSourceIndex]
        if (maxTermSourceIndex >= 0)
        {
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Ldfld, CtxTermSources);
            IlEmitterShared.EmitLdcI4(il, maxTermSourceIndex);
            il.Emit(OpCodes.Ldelema, typeof(PostingSource));
            il.Emit(OpCodes.Pop);
        }

        // Touch _termProviders[maxTermsProviderIndex]
        if (maxTermsProviderIndex >= 0)
        {
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Ldfld, CtxTermsProviders);
            IlEmitterShared.EmitLdcI4(il, maxTermsProviderIndex);
            il.Emit(OpCodes.Ldelem_Ref);
            il.Emit(OpCodes.Pop);
        }
    }

    private static void EmptyExecute(CompiledQueryMatch ctx) { }

}