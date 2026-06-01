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

        var (lowEnc, highEnc, lowSuffixSize, highSuffixSize) = field2Exec.Clause.ClauseType switch
        {
            // e.g. WHERE field1 = X AND field2 BETWEEN Y AND Z ORDER BY field1, field2
            ClauseType.Between => (encLow, encHigh, encLow.Size, encHigh.Size),
            // e.g. WHERE field1 = X AND field2 > Y (or >=) ORDER BY field1, field2
            ClauseType.GreaterThan or ClauseType.GreaterThanOrEqual => (encLow, default, encLow.Size, encLow.Size),
            // e.g. WHERE field1 = X AND field2 < Y (or <=) ORDER BY field1, field2
            ClauseType.LessThan or ClauseType.LessThanOrEqual =>  (default, encLow, encLow.Size, encLow.Size),
            // Fall back to a prefix-only scan, will fail the length check
            _ => (default(CompoundFieldEncoding), default(CompoundFieldEncoding), Constants.Terms.MaxLength, Constants.Terms.MaxLength)
        };

        if (analyzedPrefix.Size + lowSuffixSize + 1 > Constants.Terms.MaxLength ||
            analyzedPrefix.Size + highSuffixSize + 1 > Constants.Terms.MaxLength)
            return false;

        lowSlice = WriteCompositeRangeKey(ref ctx, analyzedPrefix, lowSuffixSize, in lowEnc, openFill: 0x00);
        highSlice = WriteCompositeRangeKey(ref ctx, analyzedPrefix, highSuffixSize, in highEnc, openFill: 0xFF);
        return true;
        
        static Slice WriteCompositeRangeKey(ref InstCtx ctx, Slice analyzedPrefix, int suffixSize, in CompoundFieldEncoding  suffixEncoding, byte openFill)
        {
            int len = analyzedPrefix.Size + suffixSize + 1;
            ctx.PlanParams.Allocator.Allocate(len, out ByteString buf);
            Span<byte> span = buf.ToSpan();
            analyzedPrefix.CopyTo(span);

            Span<byte> suffix = span.Slice(analyzedPrefix.Size, suffixSize);
            if (suffixEncoding is { } enc)
                WriteCompoundFieldEncoding(suffix, enc, ctx.Exec);
            else
                suffix.Fill(openFill);

            span[len - 1] = (byte)analyzedPrefix.Size;
            return new Slice(buf);
        }
    }
}
