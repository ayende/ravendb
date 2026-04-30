using System;
using System.Threading;
using Corax.Querying.Matches;
using Corax.Utils.RoaringBitmaps;

namespace Corax.Querying.Planning;

/// <summary>
/// Execution context passed to compiled query delegates.
/// Ref struct — lives on the stack, holds refs to bitmaps and spans of parameters.
/// Single argument to the delegate, avoiding signature changes as features are added.
/// </summary>
public ref struct QueryScanContext
{
    public ref RoaringBitmap Bitmap;
    public ref RoaringBitmap TempBitmap;
    public IndexSearcher Searcher;
    public Span<Matches.Meta.IQueryMatch> Matches;

    /// <summary>Pre-created MultiUnaryItem predicates for entry scan.
    /// Each group corresponds to an AND operand. OrGroups have multiple items.
    /// Created per-query from parameter values. The emitted IL calls
    /// CompareNumerical/CompareLiteral directly — the boolean structure is baked in IL.</summary>
    public Span<MultiUnaryItem> ScanPredicates;

    /// <summary>Number of scan predicates that are "simple" (entry-scannable).
    /// Complex clauses (regex, vector, spatial) have indices >= SimplePredicateCount
    /// in the Matches span and are AND'd via bitmap after entry scan.</summary>
    public int SimplePredicateCount;

    public CancellationToken Token;
}
