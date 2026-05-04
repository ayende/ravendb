using System;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;
using System.Threading;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Primitives;
using Corax.Utils;
using Voron.Data.RoaringBitmaps;
using Voron;

namespace Corax.Querying.Planning;

public static class QueryILEmitter
{
    public delegate void CompiledExecuteDelegate(ref QueryScanContext ctx);

    private const int FillBufferSize = 4096;
    private const int ScanBatchSize = 256;

    // QueryScanContext fields
    private static readonly FieldInfo s_ctxBitmaps = typeof(QueryScanContext).GetField(nameof(QueryScanContext.Bitmaps))!;
    private static readonly FieldInfo s_ctxDirectSources = typeof(QueryScanContext).GetField(nameof(QueryScanContext.DirectSources))!;
    private static readonly FieldInfo s_ctxTermSources = typeof(QueryScanContext).GetField(nameof(QueryScanContext.TermSources))!;
    private static readonly FieldInfo s_ctxLlt = typeof(QueryScanContext).GetField(nameof(QueryScanContext.Llt))!;
    private static readonly FieldInfo s_ctxSearcher = typeof(QueryScanContext).GetField(nameof(QueryScanContext.Searcher))!;
    private static readonly FieldInfo s_ctxToken = typeof(QueryScanContext).GetField(nameof(QueryScanContext.Token))!;
    private static readonly FieldInfo s_ctxEntryScanTakenAtOp = typeof(QueryScanContext).GetField(nameof(QueryScanContext.EntryScanTakenAtOp))!;

    // Timing helpers
    private static readonly MethodInfo s_getTimestamp =
        typeof(System.Diagnostics.Stopwatch).GetMethod(nameof(System.Diagnostics.Stopwatch.GetTimestamp))!;
    private static readonly MethodInfo s_recordTiming =
        typeof(EntryScanHelper).GetMethod(nameof(EntryScanHelper.RecordTiming))!;
    private static readonly MethodInfo s_recordResultCount =
        typeof(EntryScanHelper).GetMethod(nameof(EntryScanHelper.RecordResultCount))!;

    // Span<RoaringBitmapData> indexer — returns ref RoaringBitmapData
    private static readonly MethodInfo s_bitmapSpanIndexer =
        typeof(Span<RoaringBitmapData>).GetProperty("Item")!.GetGetMethod()!;

    // IQueryMatch
    private static readonly MethodInfo s_matchCountGetter = typeof(IQueryMatch).GetProperty(nameof(IQueryMatch.Count))!.GetGetMethod()!;

    // Span<IQueryMatch> indexer
    private static readonly MethodInfo s_matchSpanIndexer = typeof(Span<IQueryMatch>).GetProperty("Item")!.GetGetMethod()!;

    // RoaringBitmapData — methods called directly by emitted IL
    private static readonly MethodInfo s_andWith =
        typeof(RoaringBitmapData).GetMethod(nameof(RoaringBitmapData.AndWith),
            new[] { typeof(RoaringBitmapData).MakeByRefType(), typeof(Sparrow.Server.ByteStringContext) })!;
    private static readonly MethodInfo s_andNotWith =
        typeof(RoaringBitmapData).GetMethod(nameof(RoaringBitmapData.AndNotWith),
            new[] { typeof(RoaringBitmapData).MakeByRefType(), typeof(Sparrow.Server.ByteStringContext) })!;
    private static readonly MethodInfo s_clear =
        typeof(RoaringBitmapData).GetMethod(nameof(RoaringBitmapData.Clear),
            new[] { typeof(Sparrow.Server.ByteStringContext) })!;
    private static readonly MethodInfo s_isEmptyGetter = typeof(RoaringBitmapData).GetProperty(nameof(RoaringBitmapData.IsEmpty))!.GetGetMethod()!;
    private static readonly MethodInfo s_repairAfterLazy =
        typeof(RoaringBitmapData).GetMethod(nameof(RoaringBitmapData.RepairAfterLazy),
            new[] { typeof(Sparrow.Server.ByteStringContext) })!;
    private static readonly MethodInfo s_bitmapCountGetter = typeof(RoaringBitmapData).GetProperty(nameof(RoaringBitmapData.Count))!.GetGetMethod()!;
    private static readonly MethodInfo s_swapContents =
        typeof(RoaringBitmapData).GetMethod(nameof(RoaringBitmapData.SwapContents),
            new[] { typeof(RoaringBitmapData).MakeByRefType() })!;
    private static readonly MethodInfo s_prepareForReading =
        typeof(RoaringBitmapData).GetMethod(nameof(RoaringBitmapData.PrepareForReading),
            new[] { typeof(Sparrow.Server.ByteStringContext) })!;
    private static readonly MethodInfo s_getIterator =
        typeof(RoaringBitmapData).GetMethod(nameof(RoaringBitmapData.GetIterator),
            new[] { typeof(Sparrow.Server.ByteStringContext) })!;
    private static readonly MethodInfo s_bitmapAdd =
        typeof(RoaringBitmapData).GetMethod(nameof(RoaringBitmapData.Add),
            new[] { typeof(long), typeof(Sparrow.Server.ByteStringContext) })!;

    // IndexSearcher.Allocator — for passing ByteStringContext to RoaringBitmapData methods
    private static readonly MethodInfo s_searcherAllocatorGetter =
        typeof(IndexSearcher).GetProperty(nameof(IndexSearcher.Allocator))!.GetGetMethod()!;

    // RoaringBitmapIterator
    private static readonly MethodInfo s_iterFill =
        typeof(RoaringBitmapIterator).GetMethod(nameof(RoaringBitmapIterator.Fill),
            new[] { typeof(RoaringBitmapData).MakeByRefType(), typeof(System.Span<long>) })!;
    private static readonly MethodInfo s_iterDispose = typeof(RoaringBitmapIterator).GetMethod(nameof(RoaringBitmapIterator.Dispose))!;

    // CancellationToken
    private static readonly MethodInfo s_throwIfCancelled = typeof(CancellationToken).GetMethod(nameof(CancellationToken.ThrowIfCancellationRequested))!;

    // Span<long>
    private static readonly ConstructorInfo s_spanCtor = typeof(Span<long>).GetConstructor(new[] { typeof(void*), typeof(int) })!;
    private static readonly MethodInfo s_spanLongIndexer = typeof(Span<long>).GetProperty("Item")!.GetGetMethod()!;

    // IndexSearcher — for entry scan
    private static readonly MethodInfo s_getEntryTermsReader =
        typeof(IndexSearcher).GetMethod(nameof(IndexSearcher.GetEntryTermsReader),
            new[] { typeof(long), typeof(Page).MakeByRefType(), typeof(Voron.Data.CompactTrees.CompactKey) })!;

    // EntryTermsReader
    private static readonly MethodInfo s_readerReset = typeof(EntryTermsReader).GetMethod(nameof(EntryTermsReader.Reset))!;
    private static readonly MethodInfo s_readerFindNext = typeof(EntryTermsReader).GetMethod(nameof(EntryTermsReader.FindNext))!;

    // EntryTermsReader numeric fields — for direct comparison
    private static readonly FieldInfo s_readerCurrentLong = typeof(EntryTermsReader).GetField(nameof(EntryTermsReader.CurrentLong))!;
    private static readonly FieldInfo s_readerCurrentDouble = typeof(EntryTermsReader).GetField(nameof(EntryTermsReader.CurrentDouble))!;

    // QueryScanContext typed parameter spans
    private static readonly FieldInfo s_ctxFieldRootPages =
        typeof(QueryScanContext).GetField(nameof(QueryScanContext.FieldRootPages))!;
    private static readonly FieldInfo s_ctxLongParams =
        typeof(QueryScanContext).GetField(nameof(QueryScanContext.LongParams))!;
    private static readonly FieldInfo s_ctxDoubleParams =
        typeof(QueryScanContext).GetField(nameof(QueryScanContext.DoubleParams))!;
    private static readonly FieldInfo s_ctxSliceParams =
        typeof(QueryScanContext).GetField(nameof(QueryScanContext.SliceParams))!;

    // CompactKey.Decoded() → ReadOnlySpan<byte>
    private static readonly FieldInfo s_readerCurrent =
        typeof(EntryTermsReader).GetField(nameof(EntryTermsReader.Current))!;
    private static readonly MethodInfo s_compactKeyDecoded =
        typeof(Voron.Data.CompactTrees.CompactKey).GetMethod("Decoded", Type.EmptyTypes)!;

    // Slice.AsReadOnlySpan() → ReadOnlySpan<byte>
    private static readonly MethodInfo s_sliceAsReadOnlySpan =
        typeof(Voron.Slice).GetMethod(nameof(Voron.Slice.AsReadOnlySpan))!;

    // QueryPrimitives — called instead of inline IL Fill loops
    private static readonly MethodInfo s_fillFromMatch =
        typeof(Corax.Querying.Primitives.QueryPrimitives).GetMethod(nameof(Primitives.QueryPrimitives.FillFromMatch))!;
    private static readonly MethodInfo s_andWithMatch =
        typeof(Corax.Querying.Primitives.QueryPrimitives).GetMethod(nameof(Primitives.QueryPrimitives.AndWithMatch))!;
    private static readonly MethodInfo s_andNotWithMatch =
        typeof(Corax.Querying.Primitives.QueryPrimitives).GetMethod(nameof(Primitives.QueryPrimitives.AndNotWithMatch))!;

    // Native posting-list dispatch — bypasses IQueryMatch wrapper
    private static readonly MethodInfo s_fillBitmapFromTermSource =
        typeof(Corax.Querying.Primitives.QueryPrimitives).GetMethod(nameof(Primitives.QueryPrimitives.FillBitmapFromTermSource))!;
    private static readonly MethodInfo s_andWithTermSource =
        typeof(Corax.Querying.Primitives.QueryPrimitives).GetMethod(nameof(Primitives.QueryPrimitives.AndWithTermSource))!;
    private static readonly MethodInfo s_andNotWithTermSource =
        typeof(Corax.Querying.Primitives.QueryPrimitives).GetMethod(nameof(Primitives.QueryPrimitives.AndNotWithTermSource))!;

    // Span<TermSource> indexer
    private static readonly MethodInfo s_termSourceSpanIndexer =
        typeof(Span<TermSource>).GetProperty("Item")!.GetGetMethod()!;

    // Span<Slice> indexer
    private static readonly MethodInfo s_spanSliceIndexer =
        typeof(Span<Voron.Slice>).GetProperty("Item")!.GetGetMethod()!;

    // MemoryExtensions.SequenceCompareTo<byte>(ReadOnlySpan<byte>, ReadOnlySpan<byte>)
    private static readonly MethodInfo s_sequenceCompareTo =
        typeof(MemoryExtensions).GetMethods()
            .First(m => m.Name == nameof(MemoryExtensions.SequenceCompareTo) && m.IsGenericMethod
                && m.GetParameters().Length == 2)
            .MakeGenericMethod(typeof(byte));

    // MemoryExtensions.SequenceEqual<byte>(ReadOnlySpan<byte>, ReadOnlySpan<byte>)
    private static readonly MethodInfo s_sequenceEqual =
        typeof(MemoryExtensions).GetMethods()
            .First(m => m.Name == nameof(MemoryExtensions.SequenceEqual) && m.IsGenericMethod
                && m.GetParameters().Length == 2)
            .MakeGenericMethod(typeof(byte));

    // Span<double> indexer
    private static readonly MethodInfo s_spanDoubleIndexer =
        typeof(Span<double>).GetProperty("Item")!.GetGetMethod()!;

    // Entry-scan cost heuristics — called from emitted IL so thresholds stay in one place
    private static readonly MethodInfo s_shouldSwitchToEntryScan =
        typeof(Corax.Querying.Primitives.QueryPrimitives).GetMethod(
            nameof(Primitives.QueryPrimitives.ShouldSwitchToEntryScan),
            new[] { typeof(long), typeof(long) })!;

    public static CompiledExecuteDelegate EmitDelegate(QueryPlan plan)
    {
        var ops = plan.Ops;
        if (ops == null || ops.Length == 0)
            return EmptyExecute;

        var dm = new DynamicMethod(
            "CompiledQuery",
            typeof(void),
            new[] { typeof(QueryScanContext).MakeByRefType() },
            typeof(QueryILEmitter).Module,
            skipVisibility: true);

        dm.InitLocals = false;

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
        EmitLdcI4(il, FillBufferSize);
        il.Emit(OpCodes.Conv_U);
        il.Emit(OpCodes.Sizeof, typeof(long));
        il.Emit(OpCodes.Mul_Ovf_Un);
        il.Emit(OpCodes.Localloc);
        EmitLdcI4(il, FillBufferSize);
        il.Emit(OpCodes.Newobj, s_spanCtor);
        il.Emit(OpCodes.Stloc, bufferLocal);

        for (int i = 0; i < ops.Length; i++)
        {
            ref PlanOp op = ref ops[i];

            // Timing: record start tick before each op
            EmitTimingStart(il, startTickLocal);

            switch (op.Kind)
            {
                case PlanOpKind.FillFromPostings:
                case PlanOpKind.DirectIterate:
                    EmitCancellationCheck(il);
                    if (op.UseTermSource)
                    {
                        // QueryPrimitives.FillBitmapFromTermSource(ref TermSources[i], llt, ref bitmap[0], allocator)
                        EmitLoadTermSourceRef(il, op.ParamIndex);
                        EmitLoadLlt(il);
                        EmitLoadBitmapRef(il, 0);
                        EmitLoadAllocator(il);
                        il.Emit(OpCodes.Call, s_fillBitmapFromTermSource);
                    }
                    else
                    {
                        // QueryPrimitives.FillFromMatch(match, ref bitmap[0], allocator)
                        EmitLoadMatch(il, op.ParamIndex);
                        EmitLoadBitmapRef(il, 0);
                        EmitLoadAllocator(il);
                        il.Emit(OpCodes.Call, s_fillFromMatch);
                    }
                    break;

                case PlanOpKind.AndWithPostings:
                    EmitCancellationCheck(il);
                    if (op.UseTermSource)
                    {
                        // QueryPrimitives.AndWithTermSource(ref TermSources[i], llt, ref bitmap[0], ref bitmap[1], allocator)
                        EmitLoadTermSourceRef(il, op.ParamIndex);
                        EmitLoadLlt(il);
                        EmitLoadBitmapRef(il, 0);
                        EmitLoadBitmapRef(il, 1);
                        EmitLoadAllocator(il);
                        il.Emit(OpCodes.Call, s_andWithTermSource);
                    }
                    else
                    {
                        // QueryPrimitives.AndWithMatch(match, ref bitmap[0], ref bitmap[1], allocator)
                        EmitLoadMatch(il, op.ParamIndex);
                        EmitLoadBitmapRef(il, 0);
                        EmitLoadBitmapRef(il, 1);
                        EmitLoadAllocator(il);
                        il.Emit(OpCodes.Call, s_andWithMatch);
                    }
                    EmitLoadBitmapRef(il, 0);
                    il.Emit(OpCodes.Call, s_isEmptyGetter);
                    il.Emit(OpCodes.Brtrue, doneLabel);
                    break;

                case PlanOpKind.OrWithPostings:
                case PlanOpKind.LazyOrWithPostings:
                    EmitCancellationCheck(il);
                    if (op.UseTermSource)
                    {
                        // QueryPrimitives.FillBitmapFromTermSource(ref TermSources[i], llt, ref bitmap[slot], allocator)
                        EmitLoadTermSourceRef(il, op.ParamIndex);
                        EmitLoadLlt(il);
                        EmitLoadBitmapRef(il, op.BitmapLocal);
                        EmitLoadAllocator(il);
                        il.Emit(OpCodes.Call, s_fillBitmapFromTermSource);
                    }
                    else
                    {
                        // QueryPrimitives.FillFromMatch(match, ref bitmap[slot], allocator)
                        EmitLoadMatch(il, op.ParamIndex);
                        EmitLoadBitmapRef(il, op.BitmapLocal);
                        EmitLoadAllocator(il);
                        il.Emit(OpCodes.Call, s_fillFromMatch);
                    }
                    break;

                case PlanOpKind.ClearBitmap:
                    EmitLoadBitmapRef(il, op.BitmapLocal);
                    EmitLoadAllocator(il);
                    il.Emit(OpCodes.Call, s_clear);
                    break;

                case PlanOpKind.AndBitmaps:
                    EmitLoadBitmapRef(il, op.BitmapLocal);   // target
                    EmitLoadBitmapRef(il, op.ParamIndex2);    // source
                    EmitLoadAllocator(il);
                    il.Emit(OpCodes.Call, s_andWith);
                    break;

                case PlanOpKind.CheckEmpty:
                    EmitLoadBitmapRef(il, op.BitmapLocal);
                    il.Emit(OpCodes.Call, s_isEmptyGetter);
                    il.Emit(OpCodes.Brtrue, doneLabel);
                    break;

                case PlanOpKind.AndNotWithPostings:
                    EmitCancellationCheck(il);
                    if (op.UseTermSource)
                    {
                        // QueryPrimitives.AndNotWithTermSource(ref TermSources[i], llt, ref bitmap[0], ref bitmap[1], allocator)
                        EmitLoadTermSourceRef(il, op.ParamIndex);
                        EmitLoadLlt(il);
                        EmitLoadBitmapRef(il, 0);
                        EmitLoadBitmapRef(il, 1);
                        EmitLoadAllocator(il);
                        il.Emit(OpCodes.Call, s_andNotWithTermSource);
                    }
                    else
                    {
                        // QueryPrimitives.AndNotWithMatch(match, ref bitmap[0], ref bitmap[1], allocator)
                        EmitLoadMatch(il, op.ParamIndex);
                        EmitLoadBitmapRef(il, 0);
                        EmitLoadBitmapRef(il, 1);
                        EmitLoadAllocator(il);
                        il.Emit(OpCodes.Call, s_andNotWithMatch);
                    }
                    break;

                case PlanOpKind.RepairAfterLazy:
                    EmitLoadBitmapRef(il, 0);
                    EmitLoadAllocator(il);
                    il.Emit(OpCodes.Call, s_repairAfterLazy);
                    break;

                case PlanOpKind.CheckAndMaybeEntryScan:
                {
                    hasEntryScan = true;
                    entryScanOpIndex = i;

                    // if (QueryPrimitives.ShouldSwitchToEntryScan(bitmap.Count, match.Count)) goto entryScan
                    EmitLoadBitmapRef(il, 0);
                    il.Emit(OpCodes.Call, s_bitmapCountGetter);
                    il.Emit(OpCodes.Conv_I8);
                    EmitLoadMatch(il, op.ParamIndex);
                    il.Emit(OpCodes.Callvirt, s_matchCountGetter);
                    il.Emit(OpCodes.Call, s_shouldSwitchToEntryScan);
                    il.Emit(OpCodes.Brtrue, entryScanLabel);

                    break;
                }

                case PlanOpKind.IterateInto:
                    il.Emit(OpCodes.Br, doneLabel);
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
            il.Emit(OpCodes.Stfld, s_ctxEntryScanTakenAtOp);
            EmitEntryScan(il, plan, bufferLocal, readLocal);
            il.Emit(OpCodes.Ret);
        }
        else
        {
            il.MarkLabel(entryScanLabel);
            il.Emit(OpCodes.Ret);
        }

        return (CompiledExecuteDelegate)dm.CreateDelegate(typeof(CompiledExecuteDelegate));
    }

    /// <summary>Emit the entry scan: iterate bitmap entries, check predicates per entry,
    /// collect matches into TempBitmap, swap bitmaps.
    /// The predicate checks are emitted as direct IL — no generic loop, no type switch.
    /// Each predicate's comparison kind (Numerical/Literal) is baked at emit time.
    /// The actual comparison values come from MultiUnaryItem structs in the context span.</summary>
    private static void EmitEntryScan(ILGenerator il, QueryPlan plan, LocalBuilder bufferLocal, LocalBuilder readLocal)
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
        EmitLoadAllocator(il);
        il.Emit(OpCodes.Call, s_prepareForReading);
        EmitLoadBitmapRef(il, 0);
        EmitLoadAllocator(il);
        il.Emit(OpCodes.Call, s_getIterator);
        il.Emit(OpCodes.Stloc, iterLocal);

        // TempBitmap.Clear()
        EmitLoadBitmapRef(il, 1);
        EmitLoadAllocator(il);
        il.Emit(OpCodes.Call, s_clear);

        // Allocate scan batch: stackalloc long[256]
        EmitLdcI4(il, ScanBatchSize);
        il.Emit(OpCodes.Conv_U);
        il.Emit(OpCodes.Sizeof, typeof(long));
        il.Emit(OpCodes.Mul_Ovf_Un);
        il.Emit(OpCodes.Localloc);
        EmitLdcI4(il, ScanBatchSize);
        il.Emit(OpCodes.Newobj, s_spanCtor);
        il.Emit(OpCodes.Stloc, batchLocal);

        // Outer loop: while ((read = iter.Fill(ref bitmap, batch)) > 0)
        var outerLoopStart = il.DefineLabel();
        var outerLoopEnd = il.DefineLabel();

        il.MarkLabel(outerLoopStart);
        il.Emit(OpCodes.Ldloca, iterLocal);
        EmitLoadBitmapRef(il, 0);
        il.Emit(OpCodes.Ldloc, batchLocal);
        il.Emit(OpCodes.Call, s_iterFill);
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
        il.Emit(OpCodes.Call, s_spanLongIndexer);
        il.Emit(OpCodes.Ldind_I8);
        il.Emit(OpCodes.Stloc, entryIdLocal);

        // reader = searcher.GetEntryTermsReader(entryId, ref lastPage, null)
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Ldfld, s_ctxSearcher);
        il.Emit(OpCodes.Ldloc, entryIdLocal);
        il.Emit(OpCodes.Ldloca, pageLocal);
        il.Emit(OpCodes.Ldnull);                  // CompactKey key = null
        il.Emit(OpCodes.Call, s_getEntryTermsReader);
        il.Emit(OpCodes.Stloc, readerLocal);

        // Emit each predicate check — comparison kind baked at emit time.
        // Numeric: direct field access + comparison instruction (no delegate).
        // Slice: SequenceCompareTo / SequenceEqual — direct byte comparison.
        // OR groups: branch on first matching sub-predicate.
        int fieldRootIndex = 0;
        for (int p = 0; p < predicates.Length; p++)
        {
            ref var pred = ref predicates[p];

            if (pred.OrBranches != null && pred.OrBranches.Length > 0)
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
                    il.Emit(OpCodes.Call, s_readerReset);

                    // FindNext(fieldRootPages[fieldRootIndex])
                    il.Emit(OpCodes.Ldloca, readerLocal);
                    il.Emit(OpCodes.Ldarg_0);
                    il.Emit(OpCodes.Ldflda, s_ctxFieldRootPages);
                    EmitLdcI4(il, fieldRootIndex);
                    il.Emit(OpCodes.Call, s_spanLongIndexer);
                    il.Emit(OpCodes.Ldind_I8);
                    il.Emit(OpCodes.Call, s_readerFindNext);
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
                il.Emit(OpCodes.Call, s_readerReset);

                il.Emit(OpCodes.Ldloca, readerLocal);
                il.Emit(OpCodes.Ldarg_0);
                il.Emit(OpCodes.Ldflda, s_ctxFieldRootPages);
                EmitLdcI4(il, fieldRootIndex);
                il.Emit(OpCodes.Call, s_spanLongIndexer);
                il.Emit(OpCodes.Ldind_I8);
                il.Emit(OpCodes.Call, s_readerFindNext);

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
        EmitLoadAllocator(il);
        il.Emit(OpCodes.Call, s_bitmapAdd);

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
        il.Emit(OpCodes.Call, s_iterDispose);

        // Swap bitmaps: Bitmap.SwapContents(ref TempBitmap)
        EmitLoadBitmapRef(il, 0);
        EmitLoadBitmapRef(il, 1);
        il.Emit(OpCodes.Call, s_swapContents);

        // Clear the now-unused TempBitmap
        EmitLoadBitmapRef(il, 1);
        EmitLoadAllocator(il);
        il.Emit(OpCodes.Call, s_clear);
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
    /// For Between: reader.CurrentLong >= LongParams[paramIndex] AND reader.CurrentLong <= LongParams[paramIndex2].</summary>
    private static void EmitLongComparison(ILGenerator il, in ScanPredicateInfo pred, LocalBuilder readerLocal)
    {
        if (pred.CompareOp == ScanCompareOp.Between)
        {
            // reader.CurrentLong >= LongParams[p1] AND reader.CurrentLong <= LongParams[p2]
            var betweenFail = il.DefineLabel();
            var betweenDone = il.DefineLabel();

            // reader.CurrentLong >= LongParams[p1]
            il.Emit(OpCodes.Ldloca, readerLocal);
            il.Emit(OpCodes.Ldfld, s_readerCurrentLong);
            EmitLoadLongParam(il, pred.ParamIndex);
            il.Emit(OpCodes.Blt, betweenFail);

            // reader.CurrentLong <= LongParams[p2]
            il.Emit(OpCodes.Ldloca, readerLocal);
            il.Emit(OpCodes.Ldfld, s_readerCurrentLong);
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
        il.Emit(OpCodes.Ldfld, s_readerCurrentLong);

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
            il.Emit(OpCodes.Ldfld, s_readerCurrentDouble);
            EmitLoadDoubleParam(il, pred.ParamIndex);
            il.Emit(OpCodes.Blt_Un, betweenFail);

            il.Emit(OpCodes.Ldloca, readerLocal);
            il.Emit(OpCodes.Ldfld, s_readerCurrentDouble);
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
        il.Emit(OpCodes.Ldfld, s_readerCurrentDouble);
        EmitLoadDoubleParam(il, pred.ParamIndex);
        EmitCompareOp(il, pred.CompareOp);
    }

    /// <summary>Emit: reader.Current.Decoded() [op] ctx.SliceParams[paramIndex].AsReadOnlySpan() → bool on stack.
    /// Direct byte comparison — no MultiUnaryItem, no delegate indirection.
    /// For Equals: SequenceEqual. For ordered: SequenceCompareTo [op] 0.</summary>
    private static void EmitSliceComparison(ILGenerator il, in ScanPredicateInfo pred, LocalBuilder readerLocal)
    {
        // decoded = reader.Current.Decoded()
        il.Emit(OpCodes.Ldloca, readerLocal);
        il.Emit(OpCodes.Ldfld, s_readerCurrent);    // CompactKey
        il.Emit(OpCodes.Callvirt, s_compactKeyDecoded); // ReadOnlySpan<byte>

        // expected = ctx.SliceParams[paramIndex].AsReadOnlySpan()
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Ldflda, s_ctxSliceParams);
        EmitLdcI4(il, pred.ParamIndex);
        il.Emit(OpCodes.Call, s_spanSliceIndexer);    // ref Slice
        il.Emit(OpCodes.Call, s_sliceAsReadOnlySpan); // ReadOnlySpan<byte>

        if (pred.CompareOp == ScanCompareOp.Between)
        {
            // Between: decoded >= low AND decoded <= high
            // Stack has: decoded, low (from the default load above)
            // Need to do two comparisons
            var betweenFail = il.DefineLabel();
            var betweenDone = il.DefineLabel();

            // First: decoded.SequenceCompareTo(low) >= 0
            il.Emit(OpCodes.Call, s_sequenceCompareTo);
            il.Emit(OpCodes.Ldc_I4_0);
            il.Emit(OpCodes.Blt, betweenFail);

            // Second: decoded.SequenceCompareTo(high) <= 0
            // Re-load decoded
            il.Emit(OpCodes.Ldloca, readerLocal);
            il.Emit(OpCodes.Ldfld, s_readerCurrent);
            il.Emit(OpCodes.Callvirt, s_compactKeyDecoded);
            // Load high
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Ldflda, s_ctxSliceParams);
            EmitLdcI4(il, pred.ParamIndex2);
            il.Emit(OpCodes.Call, s_spanSliceIndexer);
            il.Emit(OpCodes.Call, s_sliceAsReadOnlySpan);
            il.Emit(OpCodes.Call, s_sequenceCompareTo);
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
            il.Emit(OpCodes.Call, s_sequenceEqual);
        }
        else if (pred.CompareOp == ScanCompareOp.NotEqual)
        {
            il.Emit(OpCodes.Call, s_sequenceEqual);
            il.Emit(OpCodes.Ldc_I4_0);
            il.Emit(OpCodes.Ceq); // negate
        }
        else
        {
            // SequenceCompareTo → int, then compare to 0
            il.Emit(OpCodes.Call, s_sequenceCompareTo);
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

    /// <summary>Load ctx.LongParams[index] → long on stack.</summary>
    private static void EmitLoadLongParam(ILGenerator il, int index)
    {
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Ldflda, s_ctxLongParams);
        EmitLdcI4(il, index);
        il.Emit(OpCodes.Call, s_spanLongIndexer);
        il.Emit(OpCodes.Ldind_I8);
    }

    /// <summary>Load ctx.DoubleParams[index] → double on stack.</summary>
    private static void EmitLoadDoubleParam(ILGenerator il, int index)
    {
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Ldflda, s_ctxDoubleParams);
        EmitLdcI4(il, index);
        il.Emit(OpCodes.Call, s_spanDoubleIndexer);
        il.Emit(OpCodes.Ldind_R8);
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

    /// <summary>Load ref to bitmap data from ctx.Bitmaps[slot].
    /// Span indexer returns ref RoaringBitmapData.</summary>
    private static void EmitLoadBitmapRef(ILGenerator il, int slot)
    {
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Ldflda, s_ctxBitmaps);
        EmitLdcI4(il, slot);
        il.Emit(OpCodes.Call, s_bitmapSpanIndexer); // ref RoaringBitmapData
    }

    /// <summary>Load ctx.Searcher.Allocator — ByteStringContext for methods needing allocation.</summary>
    private static void EmitLoadAllocator(ILGenerator il)
    {
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Ldfld, s_ctxSearcher);
        il.Emit(OpCodes.Callvirt, s_searcherAllocatorGetter);
    }

    /// <summary>Load ctx.Matches[index] — returns IQueryMatch on the stack.</summary>
    private static void EmitLoadMatch(ILGenerator il, int index)
    {
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Ldflda, s_ctxDirectSources);
        EmitLdcI4(il, index);
        il.Emit(OpCodes.Call, s_matchSpanIndexer);
        il.Emit(OpCodes.Ldind_Ref);
    }

    /// <summary>Load &amp;ctx.TermSources[index] — pushes a managed pointer (ref TermSource)
    /// suitable for the FillBitmapFromTermSource / AndWithTermSource / AndNotWithTermSource
    /// primitives. Span indexer returns ref T directly.</summary>
    private static void EmitLoadTermSourceRef(ILGenerator il, int index)
    {
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Ldflda, s_ctxTermSources);
        EmitLdcI4(il, index);
        il.Emit(OpCodes.Call, s_termSourceSpanIndexer);
    }

    /// <summary>Load ctx.Llt — pushes a LowLevelTransaction reference on the stack.</summary>
    private static void EmitLoadLlt(ILGenerator il)
    {
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Ldfld, s_ctxLlt);
    }

    /// <summary>Emit: startTick = Stopwatch.GetTimestamp()</summary>
    private static void EmitTimingStart(ILGenerator il, LocalBuilder startTickLocal)
    {
        il.Emit(OpCodes.Call, s_getTimestamp);
        il.Emit(OpCodes.Stloc, startTickLocal);
    }

    /// <summary>Emit: RecordTiming(ref ctx, opIndex, startTick); RecordResultCount(ref ctx, opIndex);</summary>
    private static void EmitTimingEnd(ILGenerator il, int opIndex, LocalBuilder startTickLocal)
    {
        il.Emit(OpCodes.Ldarg_0);         // ref ctx
        EmitLdcI4(il, opIndex);           // opIndex
        il.Emit(OpCodes.Ldloc, startTickLocal); // startTick
        il.Emit(OpCodes.Call, s_recordTiming);

        il.Emit(OpCodes.Ldarg_0);
        EmitLdcI4(il, opIndex);
        il.Emit(OpCodes.Call, s_recordResultCount);
    }

    private static void EmitCancellationCheck(ILGenerator il)
    {
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Ldflda, s_ctxToken);
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

    private static void EmptyExecute(ref QueryScanContext ctx) { }

    public static string GenerateExplainSource(QueryPlan plan)
    {
        if (plan.Ops == null || plan.Ops.Length == 0)
            return "// Empty plan";

        var sb = new StringBuilder();
        sb.AppendLine("// Compiled query (DynamicMethod) — pseudocode mirroring the emitted IL.");
        sb.AppendLine("// Mutating ops dispatch through Corax.Querying.Primitives.QueryPrimitives,");
        sb.AppendLine("// which picks bitmap-borrow / posting-list / generic-Fill paths at runtime.");

        for (int i = 0; i < plan.Ops.Length; i++)
        {
            ref PlanOp op = ref plan.Ops[i];
            string src = op.UseTermSource ? $"ctx.TermSources[{op.ParamIndex}]" : $"ctx.DirectSources[{op.ParamIndex}]";
            switch (op.Kind)
            {
                case PlanOpKind.FillFromPostings:
                case PlanOpKind.DirectIterate:
                    if (op.UseTermSource)
                        sb.AppendLine($"QueryPrimitives.FillBitmapFromTermSource(ref {src}, ctx.Llt, ref ctx.Bitmaps[0]);");
                    else
                        sb.AppendLine($"QueryPrimitives.FillFromMatch({src}, ref ctx.Bitmaps[0]);");
                    break;
                case PlanOpKind.AndWithPostings:
                    if (op.UseTermSource)
                        sb.AppendLine($"QueryPrimitives.AndWithTermSource(ref {src}, ctx.Llt, ref ctx.Bitmaps[0], ref ctx.Bitmaps[1]);");
                    else
                        sb.AppendLine($"QueryPrimitives.AndWithMatch({src}, ref ctx.Bitmaps[0], ref ctx.Bitmaps[1]);");
                    sb.AppendLine("if (ctx.Bitmaps[0].IsEmpty) return;");
                    break;
                case PlanOpKind.OrWithPostings:
                case PlanOpKind.LazyOrWithPostings:
                    if (op.UseTermSource)
                        sb.AppendLine($"QueryPrimitives.FillBitmapFromTermSource(ref {src}, ctx.Llt, ref ctx.Bitmaps[{op.BitmapLocal}]);");
                    else
                        sb.AppendLine($"QueryPrimitives.FillFromMatch({src}, ref ctx.Bitmaps[{op.BitmapLocal}]);");
                    break;
                case PlanOpKind.ClearBitmap:
                    sb.AppendLine($"ctx.Bitmaps[{op.BitmapLocal}].Clear();");
                    break;
                case PlanOpKind.AndBitmaps:
                    sb.AppendLine($"ctx.Bitmaps[{op.BitmapLocal}].AndWith(ref ctx.Bitmaps[{op.ParamIndex2}]);");
                    break;
                case PlanOpKind.CheckEmpty:
                    sb.AppendLine($"if (ctx.Bitmaps[{op.BitmapLocal}].IsEmpty) return;");
                    break;
                case PlanOpKind.AndNotWithPostings:
                    if (op.UseTermSource)
                        sb.AppendLine($"QueryPrimitives.AndNotWithTermSource(ref {src}, ctx.Llt, ref ctx.Bitmaps[0], ref ctx.Bitmaps[1]);");
                    else
                        sb.AppendLine($"QueryPrimitives.AndNotWithMatch({src}, ref ctx.Bitmaps[0], ref ctx.Bitmaps[1]);");
                    break;
                case PlanOpKind.RepairAfterLazy:
                    sb.AppendLine("ctx.Bitmaps[0].RepairAfterLazy();");
                    break;
                case PlanOpKind.CheckAndMaybeEntryScan:
                    sb.AppendLine($"if (ctx.Bitmaps[0].Count < 32000 && ctx.Bitmaps[0].Count * 64 < ctx.DirectSources[{op.ParamIndex}].Count) goto EntryScan;");
                    break;
                case PlanOpKind.IterateInto:
                    sb.AppendLine("return; // result is in ctx.Bitmaps[0]; caller iterates via QueryPrimitives.IterateInto");
                    break;
                default:
                    sb.AppendLine($"// {op.Kind}");
                    break;
            }
        }
        sb.AppendLine("EntryScan: // emitted IL walks ctx.Bitmaps[0] and re-checks per-entry predicates");
        return sb.ToString();
    }
}

/// <summary>
/// Helper for entry scan execution. Called by the emitted IL when the dynamic
/// unary promotion check fires. Iterates bitmap entries, evaluates MultiUnaryItem
/// predicates per entry, collects matches into TempBitmap, swaps.
/// Then ANDs remaining complex matchers (regex/vector/spatial) against the reduced bitmap.
/// </summary>
public static class EntryScanHelper
{
    [System.Runtime.CompilerServices.SkipLocalsInit]
    public static void Execute(ref QueryScanContext ctx)
    {
        // Entry scan is now emitted directly as IL in EmitEntryScan.
        // This helper exists only as a fallback when ScanPredicateInfos are not populated.
    }

    /// <summary>Record timing for a plan op. Called by emitted IL.</summary>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    public static void RecordTiming(ref QueryScanContext ctx, int opIndex, long startTick)
    {
        if (opIndex < ctx.Timings.Length)
            ctx.Timings[opIndex] = System.Diagnostics.Stopwatch.GetTimestamp() - startTick;
    }

    /// <summary>Record bitmap result count after a plan op. Called by emitted IL.</summary>
    [System.Runtime.CompilerServices.MethodImpl(System.Runtime.CompilerServices.MethodImplOptions.AggressiveInlining)]
    public static void RecordResultCount(ref QueryScanContext ctx, int opIndex)
    {
        if (opIndex < ctx.ResultCounts.Length)
            ctx.ResultCounts[opIndex] = ctx.Bitmaps[0].Count;
    }
}
