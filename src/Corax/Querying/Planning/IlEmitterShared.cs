using System;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Threading;
using Corax.Querying;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Primitives;
using Corax.Utils;
using Sparrow;
using Voron;
using Voron.Data.CompactTrees;
using Voron.Data.RoaringBitmaps;

namespace Corax.Querying.Planning;

/// <summary>Shared IL emission helpers and reflection members used by both <see cref="QueryILEmitter"/>
/// and <see cref="ResidualScanIlEmitter"/>.</summary>
public static class IlEmitterShared
{
    // --- From QueryILEmitter ---

    // CompiledQueryMatch fields (accessed by emitted IL)
    public static readonly FieldInfo CtxBitmaps = typeof(CompiledQueryMatch).GetField(nameof(CompiledQueryMatch.Bitmaps));
    public static readonly FieldInfo CtxTermSources = typeof(CompiledQueryMatch).GetField(nameof(CompiledQueryMatch.PostingSources));
    public static readonly FieldInfo CtxLimit = typeof(CompiledQueryMatch).GetField(nameof(CompiledQueryMatch.Limit));

    // Timing helpers
    public static readonly MethodInfo GetTimestamp =
        typeof(Stopwatch).GetMethod(nameof(Stopwatch.GetTimestamp))!;
    public static readonly MethodInfo RecordTiming =
        typeof(CompiledQueryHelper).GetMethod(nameof(CompiledQueryHelper.RecordTiming))!;
    public static readonly MethodInfo RecordResultCount =
        typeof(CompiledQueryHelper).GetMethod(nameof(CompiledQueryHelper.RecordResultCount))!;
    public static readonly MethodInfo RunEntryScanMethod =
        typeof(CompiledQueryHelper).GetMethod(nameof(CompiledQueryHelper.RunEntryScan))!;

    // IQueryMatch
    public static readonly MethodInfo MatchCountGetter = typeof(IQueryMatch).GetProperty(nameof(IQueryMatch.Count))!.GetGetMethod()!;

    // RoaringBitmap — methods called directly by emitted IL
    public static readonly MethodInfo AndWith =
        typeof(RoaringBitmap).GetMethod(nameof(RoaringBitmap.AndWith),
            [typeof(RoaringBitmap).MakeByRefType()])!;
    public static readonly MethodInfo LazyOrWith =
        typeof(RoaringBitmap).GetMethod(nameof(RoaringBitmap.LazyOrWith),
            System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Public,
            [typeof(RoaringBitmap).MakeByRefType()])!;
    public static readonly MethodInfo AndNotWith =
        typeof(RoaringBitmap).GetMethod(nameof(RoaringBitmap.AndNotWith),
            [typeof(RoaringBitmap).MakeByRefType()])!;
    public static readonly MethodInfo Clear =
        typeof(RoaringBitmap).GetMethod(nameof(RoaringBitmap.Clear), Type.EmptyTypes)!;
    public static readonly MethodInfo IsEmptyGetter = typeof(RoaringBitmap).GetProperty(nameof(RoaringBitmap.IsEmpty))!.GetGetMethod()!;
    public static readonly MethodInfo CountGetter = typeof(RoaringBitmap).GetProperty(nameof(RoaringBitmap.Count))!.GetGetMethod()!;
    public static readonly MethodInfo RepairAfterLazy =
        typeof(RoaringBitmap).GetMethod(nameof(RoaringBitmap.RepairAfterLazy), Type.EmptyTypes)!;
    public static readonly MethodInfo SwapContents =
        typeof(RoaringBitmap).GetMethod(nameof(RoaringBitmap.SwapContents),
            [typeof(RoaringBitmap).MakeByRefType()])!;

    // CancellationToken
    public static readonly MethodInfo ThrowIfCancelled = typeof(CancellationToken).GetMethod(nameof(CancellationToken.ThrowIfCancellationRequested))!;

    // IndexSearcher — for entry scan

    // CompiledQueryMatch typed parameter arrays
    public static readonly FieldInfo CtxResolvedMatches =
        typeof(CompiledQueryMatch).GetField(nameof(CompiledQueryMatch.ResolvedMatches));
    public static readonly FieldInfo CtxInRangeCounts =
        typeof(CompiledQueryMatch).GetField(nameof(CompiledQueryMatch.InRangeCounts));
    public static readonly FieldInfo CtxTermsProviders =
        typeof(CompiledQueryMatch).GetField(nameof(CompiledQueryMatch.TermsProviders));
    public static readonly FieldInfo CtxEntryScanTakenAtOp =
        typeof(CompiledQueryMatch).GetField(nameof(CompiledQueryMatch.EntryScanTakenAtOp));
    public static readonly FieldInfo CtxToken =
        typeof(CompiledQueryMatch).GetField(nameof(CompiledQueryMatch.Token));

    // CompactKey.Decoded() → ReadOnlySpan<byte>
    public static readonly MethodInfo CompactKeyDecoded =
        typeof(CompactKey).GetMethod(nameof(CompactKey.Decoded), Type.EmptyTypes)!;

    // Ctx-based entry points — take ref CompiledQueryMatch, IL just pushes ldarg.0 + int constants
    public static readonly MethodInfo CtxFillFromPostingSource = typeof(QueryPrimitives).GetMethod(nameof(QueryPrimitives.CtxFillFromPostingSource))!;
    public static readonly MethodInfo CtxFillAllEntries = typeof(QueryPrimitives).GetMethod(nameof(QueryPrimitives.CtxFillAllEntries))!;
    public static readonly MethodInfo CtxFillFromTreeScan = typeof(QueryPrimitives).GetMethod(nameof(QueryPrimitives.CtxFillFromTreeScan))!;
    public static readonly MethodInfo CtxOrWithMatch = typeof(QueryPrimitives).GetMethod(nameof(QueryPrimitives.CtxOrWithMatch))!;
    public static readonly MethodInfo CtxOrFillFromPostingSource = typeof(QueryPrimitives).GetMethod(nameof(QueryPrimitives.CtxOrFillFromPostingSource))!;
    public static readonly MethodInfo CtxOrFillFromTreeScan = typeof(QueryPrimitives).GetMethod(nameof(QueryPrimitives.CtxOrFillFromTreeScan))!;
    public static readonly MethodInfo CtxOrWithMatchSlot = typeof(QueryPrimitives).GetMethod(nameof(QueryPrimitives.CtxOrWithMatchSlot))!;
    public static readonly MethodInfo CtxAndFromPostingSource = typeof(QueryPrimitives).GetMethod(nameof(QueryPrimitives.CtxAndFromPostingSource))!;
    public static readonly MethodInfo CtxAndFromTreeScan = typeof(QueryPrimitives).GetMethod(nameof(QueryPrimitives.CtxAndFromTreeScan))!;
    public static readonly MethodInfo CtxAndFromMatch = typeof(QueryPrimitives).GetMethod(nameof(QueryPrimitives.CtxAndFromMatch))!;
    public static readonly MethodInfo CtxAndNotFromPostingSource = typeof(QueryPrimitives).GetMethod(nameof(QueryPrimitives.CtxAndNotFromPostingSource))!;
    public static readonly MethodInfo CtxAndNotFromTreeScan = typeof(QueryPrimitives).GetMethod(nameof(QueryPrimitives.CtxAndNotFromTreeScan))!;
    public static readonly MethodInfo CtxAndNotFromMatch = typeof(QueryPrimitives).GetMethod(nameof(QueryPrimitives.CtxAndNotFromMatch))!;

    // ReadOnlySpan<T0> where T0 is the 0th generic method parameter — used to resolve
    // MemoryExtensions overloads that take (ReadOnlySpan<T>, ReadOnlySpan<T>).
    private static readonly Type ReadOnlySpanOfT0 = typeof(ReadOnlySpan<>).MakeGenericType(Type.MakeGenericMethodParameter(0));

    // MemoryExtensions.SequenceCompareTo<T>(ReadOnlySpan<T>, ReadOnlySpan<T>)
    public static readonly MethodInfo SequenceCompareTo = typeof(MemoryExtensions)
        .GetMethod(nameof(MemoryExtensions.SequenceCompareTo), 1,
            BindingFlags.Public | BindingFlags.Static, null,
            [ReadOnlySpanOfT0, ReadOnlySpanOfT0], null)!
        .MakeGenericMethod(typeof(byte));

    // MemoryExtensions.SequenceEqual<T>(ReadOnlySpan<T>, ReadOnlySpan<T>)
    public static readonly MethodInfo SequenceEqual = typeof(MemoryExtensions)
        .GetMethod(nameof(MemoryExtensions.SequenceEqual), 1,
            BindingFlags.Public | BindingFlags.Static, null,
            [ReadOnlySpanOfT0, ReadOnlySpanOfT0], null)!
        .MakeGenericMethod(typeof(byte));

    // Entry-scan cost heuristics — called from emitted IL so thresholds stay in one place
    public static readonly MethodInfo ShouldSwitchToEntryScan =
        typeof(QueryPrimitives).GetMethod(
            nameof(QueryPrimitives.ShouldSwitchToEntryScan),
            [typeof(long), typeof(long)])!;

    // --- From ResidualScanIlEmitter ---

    // IPredicateEvaluationContext interface property getters
    public static readonly MethodInfo CtxLongParams =
        typeof(IPredicateEvaluationContext).GetProperty(nameof(IPredicateEvaluationContext.ResidualLongParams)).GetGetMethod();
    public static readonly MethodInfo CtxDoubleParams =
        typeof(IPredicateEvaluationContext).GetProperty(nameof(IPredicateEvaluationContext.ResidualDoubleParams)).GetGetMethod();
    public static readonly MethodInfo CtxSliceParams =
        typeof(IPredicateEvaluationContext).GetProperty(nameof(IPredicateEvaluationContext.ResidualSliceParams)).GetGetMethod();
    public static readonly MethodInfo CtxFieldRootPages =
        typeof(IPredicateEvaluationContext).GetProperty(nameof(IPredicateEvaluationContext.ResidualFieldRootPages)).GetGetMethod();

    // EntryTermsReader members
    public static readonly MethodInfo ReaderReset =
        typeof(EntryTermsReader).GetMethod(nameof(EntryTermsReader.Reset));
    public static readonly MethodInfo ReaderFindNext =
        typeof(EntryTermsReader).GetMethod(nameof(EntryTermsReader.FindNext));
    public static readonly FieldInfo ReaderCurrentLong =
        typeof(EntryTermsReader).GetField(nameof(EntryTermsReader.CurrentLong));
    public static readonly FieldInfo ReaderCurrentDouble =
        typeof(EntryTermsReader).GetField(nameof(EntryTermsReader.CurrentDouble));
    public static readonly FieldInfo ReaderCurrent =
        typeof(EntryTermsReader).GetField(nameof(EntryTermsReader.Current));

    // Span<T> reflection handles (used by emitted IL for array access/length in the delegate)
    public static readonly MethodInfo SpanLongLength =
        typeof(Span<long>).GetMethod("get_Length")!;
    public static readonly MethodInfo SpanIntLength =
        typeof(Span<int>).GetMethod("get_Length")!;
    public static readonly MethodInfo SpanLongGetItem =
        typeof(Span<long>).GetMethod("get_Item", [typeof(int)])!;
    public static readonly MethodInfo SpanIntGetItem =
        typeof(Span<int>).GetMethod("get_Item", [typeof(int)])!;
    public static readonly MethodInfo SpanEntryTermsReaderGetItem =
        typeof(Span<EntryTermsReader>).GetMethod("get_Item", [typeof(int)])!;

    public static readonly MethodInfo SliceAsReadOnlySpan =
        typeof(Slice).GetMethod(nameof(Slice.AsReadOnlySpan));

    public static readonly MethodInfo CheckFieldTermStartsWith =
        typeof(CompiledQueryHelper).GetMethod(nameof(CompiledQueryHelper.CheckFieldTermStartsWith));
    public static readonly MethodInfo CheckFieldTermEndsWith =
        typeof(CompiledQueryHelper).GetMethod(nameof(CompiledQueryHelper.CheckFieldTermEndsWith));

    /// <summary>Emit the most compact Ldc_I4 opcode for the given value.</summary>
    public static void EmitLdcI4(ILGenerator il, int value)
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
