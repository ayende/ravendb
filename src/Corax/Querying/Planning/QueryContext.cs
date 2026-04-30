using System;
using System.Threading;
using Corax.Querying.Matches;
using Corax.Utils.RoaringBitmaps;

namespace Corax.Querying.Planning;

/// <summary>
/// Execution context passed to compiled query delegates.
/// Single argument — avoids signature changes as features are added.
/// </summary>
public ref struct QueryScanContext
{
    public ref RoaringBitmap Bitmap;
    public ref RoaringBitmap TempBitmap;
    public IndexSearcher Searcher;
    public Span<Matches.Meta.IQueryMatch> Matches;

    /// <summary>Pre-created MultiUnaryItem predicates for string/slice comparisons
    /// in entry scan. Numeric comparisons use LongParams/DoubleParams directly.</summary>
    public Span<MultiUnaryItem> ScanPredicates;

    /// <summary>Pre-resolved field root pages for entry scan predicates.</summary>
    public Span<long> FieldRootPages;

    /// <summary>Typed parameter values for entry scan comparisons.
    /// The emitted IL reads by index (baked constant) and compares directly
    /// against reader.CurrentLong / reader.CurrentDouble / reader.Current.Decoded().</summary>
    public Span<long> LongParams;
    public Span<double> DoubleParams;

    /// <summary>Pre-encoded comparison Slices for string predicates.
    /// Created per-query via searcher.EncodeAndApplyAnalyzer(). The emitted IL
    /// calls Slice.AsReadOnlySpan() + SequenceCompareTo directly — no MultiUnaryItem.</summary>
    public Span<Voron.Slice> SliceParams;

    public CancellationToken Token;
}
