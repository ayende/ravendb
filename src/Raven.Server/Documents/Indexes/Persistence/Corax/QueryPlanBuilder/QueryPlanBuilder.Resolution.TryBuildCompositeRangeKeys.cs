using System;
using Corax.Querying.Planning;
using Sparrow.Server;
using Voron;
using Constants = Corax.Constants;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

internal static partial class QueryPlanBuilder
{
    private static bool TryBuildCompositeRangeKeys(ref InstCtx ctx, Slice analyzedPrefix, string sortFieldName,
        ClauseExecution field2Exec, out Slice lowSlice, out Slice highSlice)
    {
        lowSlice = default;
        highSlice = default;

        var field2Packed = field2Exec.PackedParamValue;
        if (field2Packed.IsNone)
            return false;

        if (TryGetCompoundFieldEncoding(ref ctx, sortFieldName, field2Packed, field2Packed.Param1, out var encLow) == false)
            return false;
        
        CompoundFieldEncoding encHigh = default;
        if (field2Exec.Clause.ClauseType is ClauseType.Between && 
            TryGetCompoundFieldEncoding(ref ctx, sortFieldName, field2Packed, field2Packed.Param2, out encHigh) == false)
            return false;

        // The open side of a one-sided range carries NO encoding (null) so WriteCompositeRangeKey fills it
        // with the open-fill byte (0x00 low / 0xFF high). The encodings are nullable on purpose: a
        // non-nullable struct would make the `is { }` test in WriteCompositeRangeKey always true, so the
        // open bound would never get filled and the range would collapse.
        var (lowEnc, highEnc, lowSuffixSize, highSuffixSize) = field2Exec.Clause.ClauseType switch
        {
            // e.g. WHERE field1 = X AND field2 BETWEEN Y AND Z ORDER BY field1, field2
            ClauseType.Between => ((CompoundFieldEncoding?)encLow, (CompoundFieldEncoding?)encHigh, encLow.Size, encHigh.Size),
            // e.g. WHERE field1 = X AND field2 > Y (or >=) ORDER BY field1, field2 — high bound is open (fill 0xFF)
            ClauseType.GreaterThan or ClauseType.GreaterThanOrEqual => ((CompoundFieldEncoding?)encLow, null, encLow.Size, encLow.Size),
            // e.g. WHERE field1 = X AND field2 < Y (or <=) ORDER BY field1, field2 — low bound is open (fill 0x00)
            ClauseType.LessThan or ClauseType.LessThanOrEqual =>  (null, (CompoundFieldEncoding?)encLow, encLow.Size, encLow.Size),
            // Fall back to a prefix-only scan, will fail the length check
            _ => (null, null, Constants.Terms.MaxLength, Constants.Terms.MaxLength)
        };

        if (analyzedPrefix.Size + lowSuffixSize + 1 > Constants.Terms.MaxLength ||
            analyzedPrefix.Size + highSuffixSize + 1 > Constants.Terms.MaxLength)
            return false;

        lowSlice = WriteCompositeRangeKey(ref ctx, analyzedPrefix, lowSuffixSize, in lowEnc, openFill: 0x00);
        highSlice = WriteCompositeRangeKey(ref ctx, analyzedPrefix, highSuffixSize, in highEnc, openFill: 0xFF);
        return true;
        
        static Slice WriteCompositeRangeKey(ref InstCtx ctx, Slice analyzedPrefix, int suffixSize, in CompoundFieldEncoding? suffixEncoding, byte openFill)
        {
            int len = analyzedPrefix.Size + suffixSize + 1;
            ctx.PlanParams.Allocator.Allocate(len, out ByteString buf);
            Span<byte> span = buf.ToSpan();
            analyzedPrefix.CopyTo(span);

            Span<byte> suffix = span.Slice(analyzedPrefix.Size, suffixSize);
            if (suffixEncoding is { } enc)
                WriteCompoundFieldEncoding(suffix, enc, ctx.Exec, CompoundNumericXorMask(ref ctx));
            else
                suffix.Fill(openFill);

            span[len - 1] = (byte)analyzedPrefix.Size;
            return new Slice(buf);
        }
    }
}
