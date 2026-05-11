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
    private static readonly FieldInfo CtxLlt = typeof(CompiledQueryMatch).GetField(nameof(CompiledQueryMatch.Llt));
    private static readonly FieldInfo CtxSearcher = typeof(CompiledQueryMatch).GetField(nameof(CompiledQueryMatch.Searcher));
    private static readonly FieldInfo CtxToken = typeof(CompiledQueryMatch).GetField(nameof(CompiledQueryMatch.Token));
    private static readonly FieldInfo CtxEntryScanTakenAtOp = typeof(CompiledQueryMatch).GetField(nameof(CompiledQueryMatch.EntryScanTakenAtOp));
    private static readonly FieldInfo CtxLimit = typeof(CompiledQueryMatch).GetField(nameof(CompiledQueryMatch.Limit));

    // Timing helpers
    private static readonly MethodInfo GetTimestamp =
        typeof(Stopwatch).GetMethod(nameof(Stopwatch.GetTimestamp))!;
    private static readonly MethodInfo RecordTiming =
        typeof(CompiledQueryHelper).GetMethod(nameof(CompiledQueryHelper.RecordTiming))!;
    private static readonly MethodInfo RecordResultCount =
        typeof(CompiledQueryHelper).GetMethod(nameof(CompiledQueryHelper.RecordResultCount))!;

    // IQueryMatch
    private static readonly MethodInfo MatchCountGetter = typeof(IQueryMatch).GetProperty(nameof(IQueryMatch.Count))!.GetGetMethod()!;

    // RoaringBitmap — methods called directly by emitted IL
    private static readonly MethodInfo AndWith =
        typeof(RoaringBitmap).GetMethod(nameof(RoaringBitmap.AndWith),
            [typeof(RoaringBitmap).MakeByRefType()])!;
    private static readonly MethodInfo OrWith =
        typeof(RoaringBitmap).GetMethod(nameof(RoaringBitmap.OrWith),
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
    private static readonly MethodInfo BitmapCountGetter = typeof(RoaringBitmap).GetProperty(nameof(RoaringBitmap.Count))!.GetGetMethod()!;
    private static readonly MethodInfo SwapContents =
        typeof(RoaringBitmap).GetMethod(nameof(RoaringBitmap.SwapContents),
            [typeof(RoaringBitmap).MakeByRefType()])!;
    private static readonly MethodInfo PrepareForReading =
        typeof(RoaringBitmap).GetMethod(nameof(RoaringBitmap.PrepareForReading), Type.EmptyTypes)!;
    private static readonly MethodInfo GetIterator =
        typeof(RoaringBitmap).GetMethod(nameof(RoaringBitmap.GetIterator), Type.EmptyTypes)!;
    private static readonly MethodInfo BitmapAdd =
        typeof(RoaringBitmap).GetMethod(nameof(RoaringBitmap.Add), [typeof(long)])!;

    // RoaringBitmapIterator
    private static readonly MethodInfo IterFill =
        typeof(RoaringBitmapIterator).GetMethod(nameof(RoaringBitmapIterator.Fill),
            [typeof(RoaringBitmap).MakeByRefType(), typeof(Span<long>)])!;
    private static readonly MethodInfo IterDispose = typeof(RoaringBitmapIterator).GetMethod(nameof(RoaringBitmapIterator.Dispose))!;

    // CancellationToken
    private static readonly MethodInfo ThrowIfCancelled = typeof(CancellationToken).GetMethod(nameof(CancellationToken.ThrowIfCancellationRequested))!;

    // Span<long>
    private static readonly ConstructorInfo SpanCtor = typeof(Span<long>).GetConstructor([typeof(void*), typeof(int)])!;
    private static readonly MethodInfo SpanLongIndexer = typeof(Span<long>).GetProperty("Item")!.GetGetMethod()!;

    // IndexSearcher — for entry scan
    private static readonly MethodInfo GetEntryTermsReader =
        typeof(IndexSearcher).GetMethod(nameof(IndexSearcher.GetEntryTermsReader),
            [typeof(long), typeof(Page).MakeByRefType(), typeof(CompactKey)])!;

    // EntryTermsReader
    private static readonly MethodInfo ReaderReset = typeof(EntryTermsReader).GetMethod(nameof(EntryTermsReader.Reset))!;
    private static readonly MethodInfo ReaderFindNext = typeof(EntryTermsReader).GetMethod(nameof(EntryTermsReader.FindNext))!;

    // EntryTermsReader numeric fields — for direct comparison
    private static readonly FieldInfo ReaderCurrentLong = typeof(EntryTermsReader).GetField(nameof(EntryTermsReader.CurrentLong))!;
    private static readonly FieldInfo ReaderCurrentDouble = typeof(EntryTermsReader).GetField(nameof(EntryTermsReader.CurrentDouble))!;

    // CompiledQueryMatch typed parameter arrays
    private static readonly FieldInfo CtxFieldRootPages =
        typeof(CompiledQueryMatch).GetField(nameof(CompiledQueryMatch.FieldRootPages));
    private static readonly FieldInfo CtxLongParams =
        typeof(CompiledQueryMatch).GetField(nameof(CompiledQueryMatch.LongParams));
    private static readonly FieldInfo CtxDoubleParams =
        typeof(CompiledQueryMatch).GetField(nameof(CompiledQueryMatch.DoubleParams));
    private static readonly FieldInfo CtxSliceParams =
        typeof(CompiledQueryMatch).GetField(nameof(CompiledQueryMatch.SliceParams));
    private static readonly FieldInfo CtxResolvedMatches =
        typeof(CompiledQueryMatch).GetField(nameof(CompiledQueryMatch.ResolvedMatches));
    private static readonly FieldInfo CtxTermsProviders =
        typeof(CompiledQueryMatch).GetField(nameof(CompiledQueryMatch.TermsProviders));

    // CompactKey.Decoded() → ReadOnlySpan<byte>
    private static readonly FieldInfo ReaderCurrent =
        typeof(EntryTermsReader).GetField(nameof(EntryTermsReader.Current))!;
    private static readonly MethodInfo CompactKeyDecoded =
        typeof(CompactKey).GetMethod(nameof(CompactKey.Decoded), Type.EmptyTypes)!;

    // Slice.AsReadOnlySpan() → ReadOnlySpan<byte>
    private static readonly MethodInfo SliceAsReadOnlySpan =
        typeof(Slice).GetMethod(nameof(Slice.AsReadOnlySpan))!;

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

    public static CompiledExecuteDelegate EmitDelegate(QueryExecution plan, out string explainSource)
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
        int entryScanOpIndex = -1;

        // stackalloc long[FillBufferSize]
        EmitLdcI4(il, QueryPrimitives.FillBufferSize);
        il.Emit(OpCodes.Conv_U);
        il.Emit(OpCodes.Sizeof, typeof(long));
        il.Emit(OpCodes.Mul_Ovf_Un);
        il.Emit(OpCodes.Localloc);
        EmitLdcI4(il, QueryPrimitives.FillBufferSize);
        il.Emit(OpCodes.Newobj, SpanCtor);
        il.Emit(OpCodes.Stloc, bufferLocal);

        // Bounds-check elimination preamble: touch the max index of each array
        // so the JIT can hoist the length checks out of loops.
        EmitBoundsCheckPreamble(il, ops);

        for (int i = 0; i < ops.Length; i++)
        {
            ref PlanOp op = ref ops[i];

            // Timing: record start tick before each op
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
                    EmitLoadBitmapRef(il, op.BitmapLocal);
                    EmitLoadBitmapRef(il, op.ParamIndex2);
                    il.Emit(OpCodes.Call, OrWith);
                    explain.AppendLine($"bitmap[{op.BitmapLocal}].OrWith(bitmap[{op.ParamIndex2}]);");

                    EmitLoadBitmapRef(il, op.BitmapLocal);
                    il.Emit(OpCodes.Call, CountGetter);
                    il.Emit(OpCodes.Conv_I8);
                    EmitLoadLimit(il);
                    il.Emit(OpCodes.Bge, doneLabel);
                    explain.AppendLine($"if (bitmap[{op.BitmapLocal}].Count >= limit) return;");
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
                    il.Emit(OpCodes.Call, BitmapCountGetter);
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
                    // for (int j = start; j < start+count; j++) Or(ctx, j, bitmapLocal);
                    // Or on an empty bitmap acts as Fill — no separate Fill needed.
                    int start = op.ParamIndex;
                    int count = op.ParamIndex2;
                    var loopVar = il.DeclareLocal(typeof(int));
                    var loopCheck = il.DefineLabel();
                    var loopBody = il.DefineLabel();

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
                    il.Emit(OpCodes.Ldc_I4, start + count);
                    il.Emit(OpCodes.Blt, loopBody);

                    explain.AppendLine($"for (i = {start}..{start + count}) Or(bitmap[{op.BitmapLocal}], {src}[i]);");
                    break;
                }

                case PlanOpKind.AndRange:
                {
                    // Emit a loop: for (int j = start; j < start + count; j++) { And(ctx, j); if empty → done; }
                    int start = op.ParamIndex;
                    int count = op.ParamIndex2;
                    var loopVar = il.DeclareLocal(typeof(int));
                    var loopCheck = il.DefineLabel();
                    var loopBody = il.DefineLabel();

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
                    il.Emit(OpCodes.Ldc_I4, start + count);
                    il.Emit(OpCodes.Blt, loopBody);

                    explain.AppendLine($"AndRange(bitmap[0], {src}[{start}..{start + count}]);");
                    break;
                }

                case PlanOpKind.IterateInto:
                    il.Emit(OpCodes.Br, doneLabel);
                    explain.AppendLine("return; // result in bitmap[0]");
                    break;
            }

            // Timing: record elapsed time and result count after each op
            EmitTimingEnd(il, i, startTickLocal);
        }

        il.MarkLabel(doneLabel);
        il.Emit(OpCodes.Ret);

        // Entry scan label — if any CheckAndMaybeEntryScan was emitted
        if (hasEntryScan)
        {
            il.MarkLabel(entryScanLabel);
            // Record which op triggered the entry scan
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Ldc_I4, entryScanOpIndex);
            il.Emit(OpCodes.Stfld, CtxEntryScanTakenAtOp);
            EmitEntryScan(il, plan, readLocal);
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

    /// <summary>Emit the entry scan: iterate bitmap entries, check predicates per entry,
    /// collect matches into TempBitmap, swap bitmaps.
    /// The predicate checks are emitted as direct IL — no generic loop, no type switch.
    /// Each predicate's comparison kind (Numerical/Literal) is baked at emitting time.
    /// The actual comparison values come from the typed parameter arrays (LongParams, DoubleParams, SliceParams).</summary>
    private static void EmitEntryScan(ILGenerator il, QueryExecution plan, LocalBuilder readLocal)
    {
        var predicates = plan.ScanPredicateInfos;
        if (predicates == null || predicates.Length == 0)
        {
            // No predicates — nothing to scan, bitmap is the result
            return;
        }

        // Locals for the entry scan loop
        var batchLocal = il.DeclareLocal(typeof(Span<long>));        // scan batch buffer
        var iterLocal = il.DeclareLocal(typeof(RoaringBitmapIterator));
        var pageLocal = il.DeclareLocal(typeof(Page));               // lastPage
        var readerLocal = il.DeclareLocal(typeof(EntryTermsReader));
        var iLocal = il.DeclareLocal(typeof(int));                   // loop index i
        var entryIdLocal = il.DeclareLocal(typeof(long));

        // PrepareForReading + GetIterator
        EmitLoadBitmapRef(il, 0);
        il.Emit(OpCodes.Call, PrepareForReading);
        EmitLoadBitmapRef(il, 0);
        il.Emit(OpCodes.Call, GetIterator);
        il.Emit(OpCodes.Stloc, iterLocal);

        // TempBitmap.Clear()
        EmitLoadBitmapRef(il, 1);
        il.Emit(OpCodes.Call, Clear);

        // Allocate scan batch: stackalloc long[EntryScanBatchSize]
        EmitLdcI4(il, QueryPrimitives.EntryScanBatchSize);
        il.Emit(OpCodes.Conv_U);
        il.Emit(OpCodes.Sizeof, typeof(long));
        il.Emit(OpCodes.Mul_Ovf_Un);
        il.Emit(OpCodes.Localloc);
        EmitLdcI4(il, QueryPrimitives.EntryScanBatchSize);
        il.Emit(OpCodes.Newobj, SpanCtor);
        il.Emit(OpCodes.Stloc, batchLocal);

        // Outer loop: while ((read = iter.Fill(ref bitmap, batch)) > 0)
        var outerLoopStart = il.DefineLabel();
        var outerLoopEnd = il.DefineLabel();

        il.MarkLabel(outerLoopStart);
        il.Emit(OpCodes.Ldloca, iterLocal);
        EmitLoadBitmapRef(il, 0);
        il.Emit(OpCodes.Ldloc, batchLocal);
        il.Emit(OpCodes.Call, IterFill);
        il.Emit(OpCodes.Stloc, readLocal);
        il.Emit(OpCodes.Ldloc, readLocal);
        il.Emit(OpCodes.Ldc_I4_0);
        il.Emit(OpCodes.Ble, outerLoopEnd);

        // Inner loop: for (i = 0; i < read; i++)
        var innerLoopStart = il.DefineLabel();
        var innerLoopEnd = il.DefineLabel();
        var nextEntry = il.DefineLabel();

        il.Emit(OpCodes.Ldc_I4_0);
        il.Emit(OpCodes.Stloc, iLocal);

        il.MarkLabel(innerLoopStart);
        il.Emit(OpCodes.Ldloc, iLocal);
        il.Emit(OpCodes.Ldloc, readLocal);
        il.Emit(OpCodes.Bge, innerLoopEnd);

        // entryId = batch[i]
        il.Emit(OpCodes.Ldloca, batchLocal);
        il.Emit(OpCodes.Ldloc, iLocal);
        il.Emit(OpCodes.Call, SpanLongIndexer);
        il.Emit(OpCodes.Ldind_I8);
        il.Emit(OpCodes.Stloc, entryIdLocal);

        // reader = searcher.GetEntryTermsReader(entryId, ref lastPage, null)
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Ldfld, CtxSearcher);
        il.Emit(OpCodes.Ldloc, entryIdLocal);
        il.Emit(OpCodes.Ldloca, pageLocal);
        il.Emit(OpCodes.Ldnull);                  // CompactKey key = null
        il.Emit(OpCodes.Call, GetEntryTermsReader);
        il.Emit(OpCodes.Stloc, readerLocal);

        // Emit each predicate check — comparison kind baked at emit time.
        // Numeric: direct field access + comparison instruction (no delegate).
        // Slice: SequenceCompareTo / SequenceEqual — direct byte comparison.
        // OR groups: branch on first matching sub-predicate.
        int fieldRootIndex = 0;
        for (int p = 0; p < predicates.Length; p++)
        {
            ref var pred = ref predicates[p];

            if (pred.OrBranches is { Length: > 0 })
            {
                // OR group: succeed on first matching branch
                var orPassed = il.DefineLabel();

                for (int b = 0; b < pred.OrBranches.Length; b++)
                {
                    ref var branch = ref pred.OrBranches[b];
                    var tryNextBranch = (b < pred.OrBranches.Length - 1)
                        ? il.DefineLabel()
                        : nextEntry; // last branch fails → entry fails

                    il.Emit(OpCodes.Ldloca, readerLocal);
                    il.Emit(OpCodes.Call, ReaderReset);

                    // FindNext(fieldRootPages[fieldRootIndex])
                    il.Emit(OpCodes.Ldloca, readerLocal);
                    il.Emit(OpCodes.Ldarg_0);
                    il.Emit(OpCodes.Ldfld, CtxFieldRootPages); // long[]
                    EmitLdcI4(il, fieldRootIndex);
                    il.Emit(OpCodes.Ldelem_I8);                  // long
                    il.Emit(OpCodes.Call, ReaderFindNext);
                    il.Emit(OpCodes.Brfalse, tryNextBranch);

                    EmitSingleComparison(il, branch, readerLocal);
                    il.Emit(OpCodes.Brtrue, orPassed); // match → OR succeeds

                    if (b < pred.OrBranches.Length - 1)
                        il.MarkLabel(tryNextBranch);

                    fieldRootIndex++;
                }

                il.MarkLabel(orPassed);
            }
            else
            {
                // Simple AND predicate
                il.Emit(OpCodes.Ldloca, readerLocal);
                il.Emit(OpCodes.Call, ReaderReset);

                il.Emit(OpCodes.Ldloca, readerLocal);
                il.Emit(OpCodes.Ldarg_0);
                il.Emit(OpCodes.Ldfld, CtxFieldRootPages); // long[]
                EmitLdcI4(il, fieldRootIndex);
                il.Emit(OpCodes.Ldelem_I8);                  // long
                il.Emit(OpCodes.Call, ReaderFindNext);

                if (pred.CompareOp == ScanCompareOp.NotEqual)
                {
                    // NotEquals: field not found → predicate passes (entry doesn't have the value)
                    var fieldFound = il.DefineLabel();
                    var predPassed = il.DefineLabel();
                    il.Emit(OpCodes.Brtrue, fieldFound);
                    il.Emit(OpCodes.Br, predPassed); // skip comparison, predicate passes
                    il.MarkLabel(fieldFound);
                    EmitSingleComparison(il, pred, readerLocal);
                    il.Emit(OpCodes.Brfalse, nextEntry);
                    il.MarkLabel(predPassed);
                }
                else
                {
                    il.Emit(OpCodes.Brfalse, nextEntry); // field not found → predicate fails
                    EmitSingleComparison(il, pred, readerLocal);
                    il.Emit(OpCodes.Brfalse, nextEntry);
                }

                fieldRootIndex++;
            }
        }

        // All predicates passed — add to TempBitmap
        EmitLoadBitmapRef(il, 1);
        il.Emit(OpCodes.Ldloc, entryIdLocal);
        il.Emit(OpCodes.Call, BitmapAdd);

        // nextEntry: i++, continue
        il.MarkLabel(nextEntry);
        il.Emit(OpCodes.Ldloc, iLocal);
        il.Emit(OpCodes.Ldc_I4_1);
        il.Emit(OpCodes.Add);
        il.Emit(OpCodes.Stloc, iLocal);
        il.Emit(OpCodes.Br, innerLoopStart);

        il.MarkLabel(innerLoopEnd);
        il.Emit(OpCodes.Br, outerLoopStart);

        il.MarkLabel(outerLoopEnd);

        // Dispose iterator
        il.Emit(OpCodes.Ldloca, iterLocal);
        il.Emit(OpCodes.Call, IterDispose);

        // Swap bitmaps: Bitmap.SwapContents(ref TempBitmap)
        EmitLoadBitmapRef(il, 0);
        EmitLoadBitmapRef(il, 1);
        il.Emit(OpCodes.Call, SwapContents);

        // Clear the now-unused TempBitmap
        EmitLoadBitmapRef(il, 1);
        il.Emit(OpCodes.Call, Clear);
    }

    /// <summary>Emit a single predicate comparison — dispatches to Long/Double/Slice.</summary>
    private static void EmitSingleComparison(ILGenerator il, in ScanPredicateInfo pred, LocalBuilder readerLocal)
    {
        switch (pred.ValueType)
        {
            case ScanValueType.Long:
                EmitLongComparison(il, pred, readerLocal);
                break;
            case ScanValueType.Double:
                EmitDoubleComparison(il, pred, readerLocal);
                break;
            case ScanValueType.Slice:
                EmitSliceComparison(il, pred, readerLocal);
                break;
        }
    }

    /// <summary>Emit: reader.CurrentLong [op] ctx.LongParams[paramIndex] → bool on stack.
    /// For Between: reader.CurrentLong >= LongParams[paramIndex] AND reader.CurrentLong &lt;= LongParams[paramIndex2].
    /// IL Cgt/Clt on longs use signed comparison, so long.MinValue &lt; long.MaxValue holds.
    /// Between uses Blt/Bgt (signed branch) for the same reason — no overflow concern.</summary>
    private static void EmitLongComparison(ILGenerator il, in ScanPredicateInfo pred, LocalBuilder readerLocal)
    {
        if (pred.CompareOp == ScanCompareOp.Between)
        {
            // reader.CurrentLong >= LongParams[p1] AND reader.CurrentLong <= LongParams[p2]
            var betweenFail = il.DefineLabel();
            var betweenDone = il.DefineLabel();

            // reader.CurrentLong >= LongParams[p1]
            il.Emit(OpCodes.Ldloca, readerLocal);
            il.Emit(OpCodes.Ldfld, ReaderCurrentLong);
            EmitLoadLongParam(il, pred.ParamIndex);
            il.Emit(OpCodes.Blt, betweenFail);

            // reader.CurrentLong <= LongParams[p2]
            il.Emit(OpCodes.Ldloca, readerLocal);
            il.Emit(OpCodes.Ldfld, ReaderCurrentLong);
            EmitLoadLongParam(il, pred.ParamIndex2);
            il.Emit(OpCodes.Bgt, betweenFail);

            il.Emit(OpCodes.Ldc_I4_1);
            il.Emit(OpCodes.Br, betweenDone);
            il.MarkLabel(betweenFail);
            il.Emit(OpCodes.Ldc_I4_0);
            il.MarkLabel(betweenDone);
            return;
        }

        // Load reader.CurrentLong
        il.Emit(OpCodes.Ldloca, readerLocal);
        il.Emit(OpCodes.Ldfld, ReaderCurrentLong);

        // Load ctx.LongParams[paramIndex]
        EmitLoadLongParam(il, pred.ParamIndex);

        // Emit comparison → push 1 or 0
        EmitCompareOp(il, pred.CompareOp);
    }

    /// <summary>Emit: reader.CurrentDouble [op] ctx.DoubleParams[paramIndex] → bool on stack.</summary>
    private static void EmitDoubleComparison(ILGenerator il, in ScanPredicateInfo pred, LocalBuilder readerLocal)
    {
        if (pred.CompareOp == ScanCompareOp.Between)
        {
            var betweenFail = il.DefineLabel();
            var betweenDone = il.DefineLabel();

            il.Emit(OpCodes.Ldloca, readerLocal);
            il.Emit(OpCodes.Ldfld, ReaderCurrentDouble);
            EmitLoadDoubleParam(il, pred.ParamIndex);
            il.Emit(OpCodes.Blt_Un, betweenFail);

            il.Emit(OpCodes.Ldloca, readerLocal);
            il.Emit(OpCodes.Ldfld, ReaderCurrentDouble);
            EmitLoadDoubleParam(il, pred.ParamIndex2);
            il.Emit(OpCodes.Bgt_Un, betweenFail);

            il.Emit(OpCodes.Ldc_I4_1);
            il.Emit(OpCodes.Br, betweenDone);
            il.MarkLabel(betweenFail);
            il.Emit(OpCodes.Ldc_I4_0);
            il.MarkLabel(betweenDone);
            return;
        }

        il.Emit(OpCodes.Ldloca, readerLocal);
        il.Emit(OpCodes.Ldfld, ReaderCurrentDouble);
        EmitLoadDoubleParam(il, pred.ParamIndex);
        EmitCompareOp(il, pred.CompareOp);
    }

    /// <summary>Emit: reader.Current.Decoded() [op] ctx.SliceParams[paramIndex].AsReadOnlySpan() → bool on stack.
    /// Direct byte comparison — inlined IL, no delegate indirection.
    /// For Equals: SequenceEqual. For ordered: SequenceCompareTo [op] 0.</summary>
    private static void EmitSliceComparison(ILGenerator il, in ScanPredicateInfo pred, LocalBuilder readerLocal)
    {
        // decoded = reader.Current.Decoded()
        il.Emit(OpCodes.Ldloca, readerLocal);
        il.Emit(OpCodes.Ldfld, ReaderCurrent);    // CompactKey
        il.Emit(OpCodes.Callvirt, CompactKeyDecoded); // ReadOnlySpan<byte>

        // expected = ctx.SliceParams[paramIndex].AsReadOnlySpan()
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Ldfld, CtxSliceParams);    // Slice[]
        EmitLdcI4(il, pred.ParamIndex);
        il.Emit(OpCodes.Ldelema, typeof(Slice)); // ref Slice
        il.Emit(OpCodes.Call, SliceAsReadOnlySpan); // ReadOnlySpan<byte>

        if (pred.CompareOp == ScanCompareOp.Between)
        {
            // Between: decoded >= low AND decoded <= high
            // Stack has: decoded, low (from the default load above)
            // Need to do two comparisons
            var betweenFail = il.DefineLabel();
            var betweenDone = il.DefineLabel();

            // First: decoded.SequenceCompareTo(low) >= 0
            il.Emit(OpCodes.Call, SequenceCompareTo);
            il.Emit(OpCodes.Ldc_I4_0);
            il.Emit(OpCodes.Blt, betweenFail);

            // Second: decoded.SequenceCompareTo(high) <= 0
            // Re-load decoded
            il.Emit(OpCodes.Ldloca, readerLocal);
            il.Emit(OpCodes.Ldfld, ReaderCurrent);
            il.Emit(OpCodes.Callvirt, CompactKeyDecoded);
            // Load high
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Ldfld, CtxSliceParams);    // Slice[]
            EmitLdcI4(il, pred.ParamIndex2);
            il.Emit(OpCodes.Ldelema, typeof(Slice)); // ref Slice
            il.Emit(OpCodes.Call, SliceAsReadOnlySpan);
            il.Emit(OpCodes.Call, SequenceCompareTo);
            il.Emit(OpCodes.Ldc_I4_0);
            il.Emit(OpCodes.Bgt, betweenFail);

            il.Emit(OpCodes.Ldc_I4_1);
            il.Emit(OpCodes.Br, betweenDone);
            il.MarkLabel(betweenFail);
            il.Emit(OpCodes.Ldc_I4_0);
            il.MarkLabel(betweenDone);
        }
        else if (pred.CompareOp == ScanCompareOp.Equal)
        {
            // SequenceEqual — returns bool directly
            il.Emit(OpCodes.Call, SequenceEqual);
        }
        else if (pred.CompareOp == ScanCompareOp.NotEqual)
        {
            il.Emit(OpCodes.Call, SequenceEqual);
            il.Emit(OpCodes.Ldc_I4_0);
            il.Emit(OpCodes.Ceq); // negate
        }
        else
        {
            // SequenceCompareTo → int, then compare to 0
            il.Emit(OpCodes.Call, SequenceCompareTo);
            il.Emit(OpCodes.Ldc_I4_0);
            EmitIntCompareOp(il, pred.CompareOp);
        }
    }

    /// <summary>Emit int comparison for SequenceCompareTo result vs 0.</summary>
    private static void EmitIntCompareOp(ILGenerator il, ScanCompareOp op)
    {
        switch (op)
        {
            case ScanCompareOp.GreaterThan:
                il.Emit(OpCodes.Cgt);
                break;
            case ScanCompareOp.GreaterThanOrEqual:
                il.Emit(OpCodes.Clt);
                il.Emit(OpCodes.Ldc_I4_0);
                il.Emit(OpCodes.Ceq); // !(result < 0) = result >= 0
                break;
            case ScanCompareOp.LessThan:
                il.Emit(OpCodes.Clt);
                break;
            case ScanCompareOp.LessThanOrEqual:
                il.Emit(OpCodes.Cgt);
                il.Emit(OpCodes.Ldc_I4_0);
                il.Emit(OpCodes.Ceq); // !(result > 0) = result <= 0
                break;
        }
    }

    private static void EmitLoadLongParam(ILGenerator il, int index)
    {
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Ldfld, CtxLongParams); // long[]
        EmitLdcI4(il, index);
        il.Emit(OpCodes.Ldelem_I8);              // long
    }

    private static void EmitLoadDoubleParam(ILGenerator il, int index)
    {
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Ldfld, CtxDoubleParams); // double[]
        EmitLdcI4(il, index);
        il.Emit(OpCodes.Ldelem_R8);                // double
    }

    /// <summary>Emit comparison op. Stack has [value, comparand]. Pushes 1 or 0.</summary>
    private static void EmitCompareOp(ILGenerator il, ScanCompareOp op)
    {
        switch (op)
        {
            case ScanCompareOp.Equal:
                il.Emit(OpCodes.Ceq);
                break;
            case ScanCompareOp.NotEqual:
                il.Emit(OpCodes.Ceq);
                il.Emit(OpCodes.Ldc_I4_0);
                il.Emit(OpCodes.Ceq); // negate
                break;
            case ScanCompareOp.GreaterThan:
                il.Emit(OpCodes.Cgt);
                break;
            case ScanCompareOp.GreaterThanOrEqual:
                il.Emit(OpCodes.Clt);
                il.Emit(OpCodes.Ldc_I4_0);
                il.Emit(OpCodes.Ceq); // !(a < b) = a >= b
                break;
            case ScanCompareOp.LessThan:
                il.Emit(OpCodes.Clt);
                break;
            case ScanCompareOp.LessThanOrEqual:
                il.Emit(OpCodes.Cgt);
                il.Emit(OpCodes.Ldc_I4_0);
                il.Emit(OpCodes.Ceq); // !(a > b) = a <= b
                break;
        }
    }

    private static void EmitLoadBitmapRef(ILGenerator il, int slot)
    {
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Ldfld, CtxBitmaps);  // RoaringBitmap[]
        EmitLdcI4(il, slot);
        il.Emit(OpCodes.Ldelema, typeof(RoaringBitmap)); // ref RoaringBitmap
    }

    private static void EmitLoadMatch(ILGenerator il, int index)
    {
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Ldfld, CtxResolvedMatches); // IQueryMatch[]
        EmitLdcI4(il, index);
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
        EmitLdcI4(il, opIndex);           // opIndex
        il.Emit(OpCodes.Ldloc, startTickLocal); // startTick
        il.Emit(OpCodes.Call, RecordTiming);

        il.Emit(OpCodes.Ldarg_0);
        EmitLdcI4(il, opIndex);
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
            EmitLdcI4(il, maxBitmapSlot);
            il.Emit(OpCodes.Ldelema, typeof(RoaringBitmap));
            il.Emit(OpCodes.Pop);
        }

        // Touch _resolvedMatches[maxMatchIndex]
        if (maxMatchIndex >= 0)
        {
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Ldfld, CtxResolvedMatches);
            EmitLdcI4(il, maxMatchIndex);
            il.Emit(OpCodes.Ldelem_Ref);
            il.Emit(OpCodes.Pop);
        }

        // Touch _termSources[maxTermSourceIndex]
        if (maxTermSourceIndex >= 0)
        {
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Ldfld, CtxTermSources);
            EmitLdcI4(il, maxTermSourceIndex);
            il.Emit(OpCodes.Ldelema, typeof(PostingSource));
            il.Emit(OpCodes.Pop);
        }

        // Touch _termProviders[maxTermsProviderIndex]
        if (maxTermsProviderIndex >= 0)
        {
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Ldfld, CtxTermsProviders);
            EmitLdcI4(il, maxTermsProviderIndex);
            il.Emit(OpCodes.Ldelem_Ref);
            il.Emit(OpCodes.Pop);
        }
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

    private static void EmptyExecute(CompiledQueryMatch ctx) { }

}