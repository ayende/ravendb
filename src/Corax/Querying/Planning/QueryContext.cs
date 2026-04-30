using System.Threading;
using Corax.Utils;
using Sparrow.Server;
using Voron;

namespace Corax.Querying.Planning;

/// <summary>
/// Runtime state bag passed to compiled query functions.
/// Contains the IndexSearcher, allocator, parameters, and sort metadata.
/// Field IDs are resolved at plan time and baked into the IL as constants.
/// </summary>
public ref struct QueryContext
{
    /// <summary>The Corax index searcher for this query.</summary>
    public IndexSearcher Searcher;

    /// <summary>Allocator for bitmap and temp allocations.</summary>
    public ByteStringContext Allocator;

    /// <summary>Sort specification (may be null for unsorted queries).</summary>
    public OrderMetadata[] OrderBy;

    /// <summary>Maximum results to return. int.MaxValue if no LIMIT.</summary>
    public int Limit;

    /// <summary>Cancellation token for long-running queries.</summary>
    public CancellationToken Token;

    /// <summary>Resolved parameter values as Slices. Indexed by ParamIndex in PlanOps.
    /// Populated by the planner before execution.</summary>
    public Slice[] ParamSlices;

    /// <summary>Resolved posting list container IDs. Indexed by ParamIndex in PlanOps.</summary>
    public long[] PostingListIds;
}
