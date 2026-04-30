using System;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;
using System.Threading;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Utils;
using Corax.Utils.RoaringBitmaps;
using Voron;

namespace Corax.Querying.Planning;

public static class QueryILEmitter
{
    public delegate void CompiledExecuteDelegate(ref QueryScanContext ctx);

    private const int FillBufferSize = 4096;
    private const int ScanBatchSize = 256;

    // QueryScanContext fields
    private static readonly FieldInfo s_ctxBitmap = typeof(QueryScanContext).GetField(nameof(QueryScanContext.Bitmap))!;
    private static readonly FieldInfo s_ctxTempBitmap = typeof(QueryScanContext).GetField(nameof(QueryScanContext.TempBitmap))!;
    private static readonly FieldInfo s_ctxMatches = typeof(QueryScanContext).GetField(nameof(QueryScanContext.Matches))!;
    private static readonly FieldInfo s_ctxScanPredicates = typeof(QueryScanContext).GetField(nameof(QueryScanContext.ScanPredicates))!;
    private static readonly FieldInfo s_ctxSearcher = typeof(QueryScanContext).GetField(nameof(QueryScanContext.Searcher))!;
    private static readonly FieldInfo s_ctxToken = typeof(QueryScanContext).GetField(nameof(QueryScanContext.Token))!;

    // IQueryMatch
    private static readonly MethodInfo s_fillMethod = typeof(IQueryMatch).GetMethod(nameof(IQueryMatch.Fill))!;
    private static readonly MethodInfo s_matchCountGetter = typeof(IQueryMatch).GetProperty(nameof(IQueryMatch.Count))!.GetGetMethod()!;

    // Span<IQueryMatch> indexer
    private static readonly MethodInfo s_matchSpanIndexer = typeof(Span<IQueryMatch>).GetProperty("Item")!.GetGetMethod()!;

    // RoaringBitmap
    private static readonly MethodInfo s_addRange = typeof(RoaringBitmap).GetMethod(nameof(RoaringBitmap.AddRange))!;
    private static readonly MethodInfo s_andWith = typeof(RoaringBitmap).GetMethod(nameof(RoaringBitmap.AndWith))!;
    private static readonly MethodInfo s_andNotWith = typeof(RoaringBitmap).GetMethod(nameof(RoaringBitmap.AndNotWith))!;
    private static readonly MethodInfo s_clear = typeof(RoaringBitmap).GetMethod(nameof(RoaringBitmap.Clear))!;
    private static readonly MethodInfo s_isEmptyGetter = typeof(RoaringBitmap).GetProperty(nameof(RoaringBitmap.IsEmpty))!.GetGetMethod()!;
    private static readonly MethodInfo s_repairAfterLazy = typeof(RoaringBitmap).GetMethod(nameof(RoaringBitmap.RepairAfterLazy))!;
    private static readonly MethodInfo s_bitmapCountGetter = typeof(RoaringBitmap).GetProperty(nameof(RoaringBitmap.Count))!.GetGetMethod()!;
    private static readonly MethodInfo s_swapContents = typeof(RoaringBitmap).GetMethod(nameof(RoaringBitmap.SwapContents))!;
    private static readonly MethodInfo s_prepareForReading = typeof(RoaringBitmap).GetMethod(nameof(RoaringBitmap.PrepareForReading))!;
    private static readonly MethodInfo s_getIterator = typeof(RoaringBitmap).GetMethod(nameof(RoaringBitmap.GetIterator))!;
    private static readonly MethodInfo s_bitmapAdd = typeof(RoaringBitmap).GetMethod(nameof(RoaringBitmap.Add))!;

    // RoaringBitmapIterator
    private static readonly MethodInfo s_iterFill = typeof(RoaringBitmapIterator).GetMethod(nameof(RoaringBitmapIterator.Fill))!;
    private static readonly MethodInfo s_iterDispose = typeof(RoaringBitmapIterator).GetMethod(nameof(RoaringBitmapIterator.Dispose))!;

    // CancellationToken
    private static readonly MethodInfo s_throwIfCancelled = typeof(CancellationToken).GetMethod(nameof(CancellationToken.ThrowIfCancellationRequested))!;

    // Span<long>
    private static readonly ConstructorInfo s_spanCtor = typeof(Span<long>).GetConstructor(new[] { typeof(void*), typeof(int) })!;
    private static readonly MethodInfo s_spanSlice = typeof(Span<long>).GetMethod(nameof(Span<long>.Slice), new[] { typeof(int), typeof(int) })!;
    private static readonly MethodInfo s_spanToReadOnly = typeof(Span<long>).GetMethod("op_Implicit", new[] { typeof(Span<long>) })!;
    private static readonly MethodInfo s_spanLongIndexer = typeof(Span<long>).GetProperty("Item")!.GetGetMethod()!;

    // IndexSearcher — for entry scan
    private static readonly MethodInfo s_getEntryTermsReader =
        typeof(IndexSearcher).GetMethod(nameof(IndexSearcher.GetEntryTermsReader), new[] { typeof(long), typeof(Page).MakeByRefType() })!;

    // EntryTermsReader
    private static readonly MethodInfo s_readerReset = typeof(EntryTermsReader).GetMethod(nameof(EntryTermsReader.Reset))!;
    private static readonly MethodInfo s_readerFindNext = typeof(EntryTermsReader).GetMethod(nameof(EntryTermsReader.FindNext))!;

    // MultiUnaryItem comparison
    private static readonly MethodInfo s_compareNumerical = typeof(MultiUnaryItem).GetMethod(nameof(MultiUnaryItem.CompareNumerical))!;
    private static readonly MethodInfo s_compareLiteral = typeof(MultiUnaryItem).GetMethod(nameof(MultiUnaryItem.CompareLiteral))!;

    // FieldCache
    private static readonly MethodInfo s_getLookupRootPage =
        typeof(Utils.FieldsCache).GetMethod(nameof(Utils.FieldsCache.GetLookupRootPage), new[] { typeof(string) })!;
    private static readonly PropertyInfo s_fieldCache =
        typeof(IndexSearcher).GetProperty(nameof(IndexSearcher.FieldCache))!;

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

        var doneLabel = il.DefineLabel();
        var entryScanLabel = il.DefineLabel();
        bool hasEntryScan = false;

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

            switch (op.Kind)
            {
                case PlanOpKind.FillFromPostings:
                case PlanOpKind.DirectIterate:
                    EmitCancellationCheck(il);
                    EmitFillLoop(il, op.ParamIndex, bufferLocal, readLocal, useBitmap: true);
                    break;

                case PlanOpKind.AndWithPostings:
                    EmitCancellationCheck(il);
                    EmitLoadBitmapRef(il, s_ctxTempBitmap);
                    il.Emit(OpCodes.Call, s_clear);
                    EmitFillLoop(il, op.ParamIndex, bufferLocal, readLocal, useBitmap: false);
                    EmitLoadBitmapRef(il, s_ctxBitmap);
                    EmitLoadBitmapRef(il, s_ctxTempBitmap);
                    il.Emit(OpCodes.Call, s_andWith);
                    EmitLoadBitmapRef(il, s_ctxBitmap);
                    il.Emit(OpCodes.Call, s_isEmptyGetter);
                    il.Emit(OpCodes.Brtrue, doneLabel);
                    break;

                case PlanOpKind.OrWithPostings:
                case PlanOpKind.LazyOrWithPostings:
                    EmitCancellationCheck(il);
                    EmitFillLoop(il, op.ParamIndex, bufferLocal, readLocal, useBitmap: true);
                    break;

                case PlanOpKind.AndNotWithPostings:
                    EmitCancellationCheck(il);
                    EmitLoadBitmapRef(il, s_ctxTempBitmap);
                    il.Emit(OpCodes.Call, s_clear);
                    EmitFillLoop(il, op.ParamIndex, bufferLocal, readLocal, useBitmap: false);
                    EmitLoadBitmapRef(il, s_ctxBitmap);
                    EmitLoadBitmapRef(il, s_ctxTempBitmap);
                    il.Emit(OpCodes.Call, s_andNotWith);
                    break;

                case PlanOpKind.RepairAfterLazy:
                    EmitLoadBitmapRef(il, s_ctxBitmap);
                    il.Emit(OpCodes.Call, s_repairAfterLazy);
                    break;

                case PlanOpKind.CheckAndMaybeEntryScan:
                {
                    hasEntryScan = true;
                    var skipCheck = il.DefineLabel();

                    // if (bitmap.Count >= 32000) skip
                    EmitLoadBitmapRef(il, s_ctxBitmap);
                    il.Emit(OpCodes.Call, s_bitmapCountGetter);
                    il.Emit(OpCodes.Conv_I8);
                    il.Emit(OpCodes.Ldc_I8, 32000L);
                    il.Emit(OpCodes.Bge, skipCheck);

                    // if (bitmap.Count * 64 < matches[paramIndex].Count) goto entryScan
                    EmitLoadBitmapRef(il, s_ctxBitmap);
                    il.Emit(OpCodes.Call, s_bitmapCountGetter);
                    il.Emit(OpCodes.Conv_I8);
                    il.Emit(OpCodes.Ldc_I4, 64);
                    il.Emit(OpCodes.Conv_I8);
                    il.Emit(OpCodes.Mul);
                    EmitLoadMatch(il, op.ParamIndex);
                    il.Emit(OpCodes.Callvirt, s_matchCountGetter);
                    il.Emit(OpCodes.Blt, entryScanLabel);

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

        // Entry scan label — if any CheckAndMaybeEntryScan was emitted
        if (hasEntryScan)
        {
            il.MarkLabel(entryScanLabel);
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

    // Span<MultiUnaryItem> indexer
    private static readonly MethodInfo s_predicateSpanIndexer =
        typeof(Span<MultiUnaryItem>).GetProperty("Item")!.GetGetMethod()!;

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

        // Field root page locals — one per predicate, resolved once
        var fieldRootLocals = new LocalBuilder[predicates.Length];
        for (int p = 0; p < predicates.Length; p++)
            fieldRootLocals[p] = il.DeclareLocal(typeof(long));

        // PrepareForReading + GetIterator
        EmitLoadBitmapRef(il, s_ctxBitmap);
        il.Emit(OpCodes.Call, s_prepareForReading);
        EmitLoadBitmapRef(il, s_ctxBitmap);
        il.Emit(OpCodes.Call, s_getIterator);
        il.Emit(OpCodes.Stloc, iterLocal);

        // TempBitmap.Clear()
        EmitLoadBitmapRef(il, s_ctxTempBitmap);
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

        // Resolve field root pages once
        for (int p = 0; p < predicates.Length; p++)
        {
            // fieldRootLocals[p] = ctx.Searcher.FieldCache.GetLookupRootPage("FieldName")
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Ldfld, s_ctxSearcher);
            il.Emit(OpCodes.Callvirt, s_fieldCache.GetGetMethod()!);
            il.Emit(OpCodes.Ldstr, predicates[p].FieldName);
            il.Emit(OpCodes.Callvirt, s_getLookupRootPage);
            il.Emit(OpCodes.Stloc, fieldRootLocals[p]);
        }

        // Outer loop: while ((read = iter.Fill(ref bitmap, batch)) > 0)
        var outerLoopStart = il.DefineLabel();
        var outerLoopEnd = il.DefineLabel();

        il.MarkLabel(outerLoopStart);
        il.Emit(OpCodes.Ldloca, iterLocal);
        EmitLoadBitmapRef(il, s_ctxBitmap);
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

        // reader = searcher.GetEntryTermsReader(entryId, ref lastPage)
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Ldfld, s_ctxSearcher);
        il.Emit(OpCodes.Ldloc, entryIdLocal);
        il.Emit(OpCodes.Ldloca, pageLocal);
        il.Emit(OpCodes.Call, s_getEntryTermsReader);
        il.Emit(OpCodes.Stloc, readerLocal);

        // Emit each predicate check — direct call, no loop, no type switch
        for (int p = 0; p < predicates.Length; p++)
        {
            ref var pred = ref predicates[p];

            // reader.Reset()
            il.Emit(OpCodes.Ldloca, readerLocal);
            il.Emit(OpCodes.Call, s_readerReset);

            // if (!reader.FindNext(fieldRoot)) goto nextEntry
            il.Emit(OpCodes.Ldloca, readerLocal);
            il.Emit(OpCodes.Ldloc, fieldRootLocals[p]);
            il.Emit(OpCodes.Call, s_readerFindNext);
            il.Emit(OpCodes.Brfalse, nextEntry);

            // Call the right comparison method (baked at emit time)
            // scanPredicates[p].CompareNumerical(reader) or .CompareLiteral(reader)
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Ldflda, s_ctxScanPredicates);
            EmitLdcI4(il, pred.PredicateIndex);
            il.Emit(OpCodes.Call, s_predicateSpanIndexer);  // ref MultiUnaryItem
            il.Emit(OpCodes.Ldloc, readerLocal);
            il.Emit(OpCodes.Call, pred.CompareKind == ScanCompareKind.Numerical
                ? s_compareNumerical
                : s_compareLiteral);
            il.Emit(OpCodes.Brfalse, nextEntry);
        }

        // All predicates passed — add to TempBitmap
        EmitLoadBitmapRef(il, s_ctxTempBitmap);
        il.Emit(OpCodes.Ldloc, entryIdLocal);
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
        EmitLoadBitmapRef(il, s_ctxBitmap);
        EmitLoadBitmapRef(il, s_ctxTempBitmap);
        il.Emit(OpCodes.Call, s_swapContents);

        // Clear the now-unused TempBitmap
        EmitLoadBitmapRef(il, s_ctxTempBitmap);
        il.Emit(OpCodes.Call, s_clear);
    }

    private static void EmitFillLoop(ILGenerator il, int paramIndex,
        LocalBuilder bufferLocal, LocalBuilder readLocal, bool useBitmap)
    {
        var loopStart = il.DefineLabel();
        var loopEnd = il.DefineLabel();

        il.MarkLabel(loopStart);

        // read = ctx.Matches[paramIndex].Fill(buffer)
        EmitLoadMatch(il, paramIndex);
        il.Emit(OpCodes.Ldloc, bufferLocal);
        il.Emit(OpCodes.Callvirt, s_fillMethod);
        il.Emit(OpCodes.Stloc, readLocal);
        il.Emit(OpCodes.Ldloc, readLocal);
        il.Emit(OpCodes.Ldc_I4_0);
        il.Emit(OpCodes.Ble, loopEnd);

        // bitmap.AddRange(buffer.Slice(0, read))
        EmitLoadBitmapRef(il, useBitmap ? s_ctxBitmap : s_ctxTempBitmap);
        il.Emit(OpCodes.Ldloca_S, bufferLocal);
        il.Emit(OpCodes.Ldc_I4_0);
        il.Emit(OpCodes.Ldloc, readLocal);
        il.Emit(OpCodes.Call, s_spanSlice);
        il.Emit(OpCodes.Call, s_spanToReadOnly);
        il.Emit(OpCodes.Call, s_addRange);

        il.Emit(OpCodes.Br, loopStart);
        il.MarkLabel(loopEnd);
    }

    /// <summary>Load ref to bitmap from ctx. ctx is arg0 (ref QueryScanContext).
    /// ldfld on a ref field gives us the byref (RoaringBitmap&amp;).</summary>
    private static void EmitLoadBitmapRef(ILGenerator il, FieldInfo bitmapField)
    {
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Ldfld, bitmapField);
    }

    /// <summary>Load ctx.Matches[index] — returns IQueryMatch on the stack.</summary>
    private static void EmitLoadMatch(ILGenerator il, int index)
    {
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Ldflda, s_ctxMatches);
        EmitLdcI4(il, index);
        il.Emit(OpCodes.Call, s_matchSpanIndexer);
        il.Emit(OpCodes.Ldind_Ref);
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
        sb.AppendLine("// Compiled query (DynamicMethod)");
        sb.AppendLine("Span<long> buffer = stackalloc long[4096];");

        for (int i = 0; i < plan.Ops.Length; i++)
        {
            ref PlanOp op = ref plan.Ops[i];
            switch (op.Kind)
            {
                case PlanOpKind.FillFromPostings:
                case PlanOpKind.DirectIterate:
                    sb.AppendLine($"while ((read = ctx.Matches[{op.ParamIndex}].Fill(buffer)) > 0) ctx.Bitmap.AddRange(buffer[..read]);");
                    break;
                case PlanOpKind.AndWithPostings:
                    sb.AppendLine($"ctx.TempBitmap.Clear();");
                    sb.AppendLine($"while ((read = ctx.Matches[{op.ParamIndex}].Fill(buffer)) > 0) ctx.TempBitmap.AddRange(buffer[..read]);");
                    sb.AppendLine($"ctx.Bitmap.AndWith(ref ctx.TempBitmap); if (ctx.Bitmap.IsEmpty) return;");
                    break;
                case PlanOpKind.OrWithPostings:
                case PlanOpKind.LazyOrWithPostings:
                    sb.AppendLine($"while ((read = ctx.Matches[{op.ParamIndex}].Fill(buffer)) > 0) ctx.Bitmap.AddRange(buffer[..read]);");
                    break;
                case PlanOpKind.AndNotWithPostings:
                    sb.AppendLine($"ctx.TempBitmap.Clear();");
                    sb.AppendLine($"while ((read = ctx.Matches[{op.ParamIndex}].Fill(buffer)) > 0) ctx.TempBitmap.AddRange(buffer[..read]);");
                    sb.AppendLine($"ctx.Bitmap.AndNotWith(ref ctx.TempBitmap);");
                    break;
                case PlanOpKind.RepairAfterLazy:
                    sb.AppendLine("ctx.Bitmap.RepairAfterLazy();");
                    break;
                case PlanOpKind.CheckAndMaybeEntryScan:
                    sb.AppendLine($"if (ctx.Bitmap.Count < 32000 && ctx.Bitmap.Count * 64 < ctx.Matches[{op.ParamIndex}].Count) goto EntryScan;");
                    break;
                case PlanOpKind.IterateInto:
                    sb.AppendLine("return;");
                    break;
                default:
                    sb.AppendLine($"// {op.Kind}");
                    break;
            }
        }
        sb.AppendLine("EntryScan: EntryScanHelper.Execute(ref ctx);");
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
        var predicates = ctx.ScanPredicates;
        if (predicates.IsEmpty)
            return; // No predicates — bitmap is the result as-is

        ctx.Bitmap.PrepareForReading();
        var iter = ctx.Bitmap.GetIterator();
        ctx.TempBitmap.Clear();

        try
        {
            Span<long> batch = stackalloc long[256];
            Page lastPage = default;

            int read;
            while ((read = iter.Fill(ref ctx.Bitmap, batch)) > 0)
            {
                for (int i = 0; i < read; i++)
                {
                    long entryId = batch[i];
                    var reader = ctx.Searcher.GetEntryTermsReader(entryId, ref lastPage);
                    bool entryMatches = true;

                    for (int p = 0; p < predicates.Length; p++)
                    {
                        ref var predicate = ref predicates[p];
                        long fieldRootPage = ctx.Searcher.FieldCache.GetLookupRootPage(
                            predicate.Binding.FieldName);

                        bool accepted = predicate.Mode == MultiUnaryItem.UnaryMode.All;
                        reader.Reset();

                        while (reader.FindNext(fieldRootPage))
                        {
                            bool cmpResult = predicate.Type switch
                            {
                                MultiUnaryItem.DataType.Slice => predicate.CompareLiteral(reader),
                                MultiUnaryItem.DataType.Long => predicate.CompareNumerical(reader),
                                MultiUnaryItem.DataType.Double => predicate.CompareNumerical(reader),
                                _ => throw new ArgumentOutOfRangeException()
                            };

                            if (predicate.Mode == MultiUnaryItem.UnaryMode.All && !cmpResult)
                            {
                                accepted = false;
                                break;
                            }

                            if (predicate.Mode == MultiUnaryItem.UnaryMode.Any && cmpResult)
                            {
                                accepted = true;
                                break;
                            }
                        }

                        if (!accepted)
                        {
                            entryMatches = false;
                            break;
                        }
                    }

                    if (entryMatches)
                        ctx.TempBitmap.Add(entryId);
                }
            }

            // Swap: TempBitmap has filtered results → becomes main bitmap
            ctx.Bitmap.SwapContents(ref ctx.TempBitmap);
            ctx.TempBitmap.Clear();
        }
        finally
        {
            iter.Dispose();
        }
    }
}
