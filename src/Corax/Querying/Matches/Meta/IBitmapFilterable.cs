using Corax.Querying.Planning;

namespace Corax.Querying.Matches.Meta;

/// <summary>
/// Implemented by matches that can filter a bitmap in-place.
/// When the entry scan produces a small bitmap, the emitted IL calls
/// FilterBitmap directly instead of filling the full posting list.
/// The match inspects Bitmaps[0].Count and decides its strategy:
///   small → scan each entry, check condition
///   large → fill posting list into temp, AND with bitmap
/// </summary>
public interface IBitmapFilterable
{
    void FilterBitmap(ref QueryScanContext ctx);
}
