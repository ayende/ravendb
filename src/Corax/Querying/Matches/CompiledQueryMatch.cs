using System;
using System.Collections.Generic;
using System.Threading;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Planning;
using Corax.Utils.RoaringBitmaps;
using Sparrow.Server;

namespace Corax.Querying.Matches;

/// <summary>
/// Bridges the Corax 2.0 compiled execution path into the existing IQueryMatch interface.
/// On first Fill() call, invokes the compiled DynamicMethod delegate to populate a
/// RoaringBitmap, then iterates it across subsequent Fill() calls.
/// </summary>
public unsafe struct CompiledQueryMatch : IQueryMatch, IDisposable
{
    private readonly QueryILEmitter.CompiledExecuteDelegate _compiledDelegate;
    private readonly IQueryMatch[] _resolvedMatches;
    private readonly string _explainSource;
    private readonly ByteStringContext _allocator;
    private readonly long _limit;
    private readonly CancellationToken _token;

    private RoaringBitmap _bitmap;
    private RoaringBitmapIterator _iterator;
    private bool _executed;
    private long _count;

    public CompiledQueryMatch(QueryPlan plan, CompiledPlan compiledPlan,
        IQueryMatch[] resolvedMatches,
        IndexSearcher searcher, ByteStringContext allocator, long limit, CancellationToken token)
    {
        _compiledDelegate = compiledPlan.CompiledDelegate;
        _resolvedMatches = resolvedMatches;
        _explainSource = compiledPlan.ExplainSource;
        _allocator = allocator;
        _limit = limit;
        _token = token;
        _bitmap = new RoaringBitmap(allocator);
        _iterator = default;
        _executed = false;
        _count = -1;
    }

    public long Count
    {
        get
        {
            if (!_executed) Execute();
            return _count;
        }
    }

    public QueryCountConfidence Confidence => _executed ? QueryCountConfidence.High : QueryCountConfidence.Normal;

    public bool IsBoosting
    {
        get
        {
            if (_resolvedMatches == null)
                return false;
            for (int i = 0; i < _resolvedMatches.Length; i++)
            {
                if (_resolvedMatches[i].IsBoosting)
                    return true;
            }
            return false;
        }
    }

    public DuplicatesOccurrence DuplicatesOccurrenceStatus => DuplicatesOccurrence.NotPossible;

    public int Fill(Span<long> matches)
    {
        if (!_executed) Execute();
        return _iterator.Fill(ref _bitmap, matches);
    }

    public int AndWith(Span<long> buffer, int matches)
    {
        if (!_executed) Execute();

        // Buffer is sorted. Group by container key and skip entire containers
        // that don't exist in the bitmap. For containers that do exist, use
        // per-entry Contains (which is O(1) within a container).
        int kept = 0;
        int i = 0;
        while (i < matches)
        {
            long containerKey = buffer[i] >> RoaringBitmap.ContainerKeyShift;
            int containerSlot = _bitmap.GetSlotForKey(containerKey);

            if (containerSlot < 0)
            {
                // Container doesn't exist — skip all entries in this container range
                long nextContainerStart = (containerKey + 1) << RoaringBitmap.ContainerKeyShift;
                while (i < matches && buffer[i] < nextContainerStart)
                    i++;
                continue;
            }

            // Container exists — check individual entries until we leave this container
            long containerEnd = (containerKey + 1) << RoaringBitmap.ContainerKeyShift;
            while (i < matches && buffer[i] < containerEnd)
            {
                if (_bitmap.Contains(buffer[i]))
                    buffer[kept++] = buffer[i];
                i++;
            }
        }
        return kept;
    }

    public void Score(Span<long> matches, Span<float> scores, float boostFactor)
    {
        if (_resolvedMatches == null)
            return;

        for (int i = 0; i < _resolvedMatches.Length; i++)
        {
            _resolvedMatches[i].Score(matches, scores, boostFactor);
        }
    }

    public QueryInspectionNode Inspect()
    {
        return new QueryInspectionNode(
            nameof(CompiledQueryMatch),
            parameters: new Dictionary<string, string>
            {
                ["Explain"] = _explainSource ?? "N/A"
            });
    }

    public SkipSortingResult AttemptToSkipSorting() => SkipSortingResult.ResultsNativelySorted;

    private void Execute()
    {
        if (_executed) return;
        _executed = true;

        var tempBitmap = new RoaringBitmap(_allocator);
        try
        {
            // Single call — the DynamicMethod delegate IS the query
            _compiledDelegate(_resolvedMatches, ref _bitmap, ref tempBitmap, _token);

            _bitmap.PrepareForReading();
            _count = _bitmap.Count;
            _iterator = _bitmap.GetIterator();
        }
        finally
        {
            tempBitmap.Dispose();
        }
    }

    public void Dispose()
    {
        _iterator.Dispose();
        _bitmap.Dispose();
    }
}
