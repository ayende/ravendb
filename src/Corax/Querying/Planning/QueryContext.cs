using System;
using System.Threading;
using Corax.Querying.Matches.Meta;
using Voron.Data.RoaringBitmaps;
using Voron;

namespace Corax.Querying.Planning;

/// <summary>
/// Execution context passed to compiled query delegates.
/// Single argument — avoids signature changes as features are added.
/// </summary>
public ref struct QueryScanContext
{
    /// <summary>Bitmap pool. [0] = main result, [1..N] = scratch.
    /// Size is statically determined by the plan's nesting depth.</summary>
    public Span<RoaringBitmap> Bitmaps;

    public IndexSearcher Searcher;

    /// <summary>Direct sources — matches that produce entry IDs via Fill().
    /// TermMatch, MultiTermMatch, AllEntriesMatch, SpatialMatch, VectorSearchMatch.</summary>
    public Span<IQueryMatch> DirectSources;

    /// <summary>Term providers — iterate CompactTree terms, each yielding a posting list.
    /// The emitted IL iterates providers in a batch loop, OR-ing each term's
    /// posting list directly into the bitmap. Reserved for future use when
    /// MultiTermMatch internals are refactored.</summary>
    public Span<ITermProvider> TermProviders;

    /// <summary>Pre-resolved field root pages for entry scan predicates.</summary>
    public Span<long> FieldRootPages;

    /// <summary>Typed parameter values for entry scan comparisons.</summary>
    public Span<long> LongParams;
    public Span<double> DoubleParams;
    public Span<Slice> SliceParams;

    public CancellationToken Token;

    /// <summary>Per-op timing in Stopwatch ticks. One slot per PlanOp.
    /// Emitted IL writes Stopwatch.GetTimestamp() before/after each clause.
    /// null/empty if timings not requested.</summary>
    public Span<long> Timings;

    /// <summary>Bitmap count after each op. Tracks cardinality reduction through the plan.</summary>
    public Span<long> ResultCounts;

    /// <summary>Op index where entry scan was triggered, or -1 if not taken.
    /// Set by emitted IL when CheckAndMaybeEntryScan fires.</summary>
    public int EntryScanTakenAtOp;
}
