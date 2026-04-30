using System;
using System.Threading;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Utils.RoaringBitmaps;
using Voron;

namespace Corax.Querying.Planning;

/// <summary>
/// Execution context passed to compiled query delegates.
/// Single argument — avoids signature changes as features are added.
/// </summary>
public ref struct QueryScanContext
{
    /// <summary>Bitmap pool. [0] = main result, [1..N] = scratch.
    /// Size is statically determined by the plan's nesting depth.
    /// The emitter bakes bitmap indices as constants in the IL.</summary>
    public Span<RoaringBitmap> Bitmaps;

    public IndexSearcher Searcher;
    public Span<IQueryMatch> Matches;

    /// <summary>Pre-resolved field root pages for entry scan predicates.</summary>
    public Span<long> FieldRootPages;

    /// <summary>Typed parameter values for entry scan comparisons.</summary>
    public Span<long> LongParams;
    public Span<double> DoubleParams;
    public Span<Slice> SliceParams;

    public CancellationToken Token;
}
