using System;
using System.Collections.Generic;
using System.Threading;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Planning;
using Corax.Querying.Primitives;
using Corax.Utils;
using Corax.Utils.RoaringBitmaps;
using Sparrow.Server;

namespace Corax.Querying.Matches;

/// <summary>
/// Bridges the Corax 2.0 compiled execution path into the existing IQueryMatch interface.
/// On first Fill() call, executes the plan using IQueryMatch instances resolved by
/// the QueryPlanBuilder, materializes results into a bitmap, then iterates it across
/// subsequent Fill() calls.
/// </summary>
public unsafe struct CompiledQueryMatch : IQueryMatch, IDisposable
{
    private readonly PlanOp[] _ops;
    private readonly IQueryMatch[] _resolvedMatches;
    private readonly string _explainSource;
    private readonly IndexSearcher _searcher;
    private readonly ByteStringContext _allocator;
    private readonly int _limit;
    private readonly CancellationToken _token;

    private RoaringBitmap _bitmap;
    private RoaringBitmapIterator _iterator;
    private bool _executed;
    private long _count;

    public CompiledQueryMatch(QueryPlan plan, IQueryMatch[] resolvedMatches,
        IndexSearcher searcher, ByteStringContext allocator, int limit, CancellationToken token)
    {
        _ops = plan.Ops;
        _resolvedMatches = resolvedMatches;
        _explainSource = plan.ExplainSource;
        _searcher = searcher;
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
    public bool IsBoosting => false;
    public DuplicatesOccurrence DuplicatesOccurrenceStatus => DuplicatesOccurrence.NotPossible;

    public int Fill(Span<long> matches)
    {
        if (!_executed) Execute();
        return _iterator.Fill(ref _bitmap, matches);
    }

    public int AndWith(Span<long> buffer, int matches)
    {
        if (!_executed) Execute();
        int kept = 0;
        for (int i = 0; i < matches; i++)
        {
            if (_bitmap.Contains(buffer[i]))
                buffer[kept++] = buffer[i];
        }
        return kept;
    }

    public void Score(Span<long> matches, Span<float> scores, float boostFactor)
    {
        // Scoring handled separately in compiled path.
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

        if (_ops == null || _ops.Length == 0)
        {
            _count = 0;
            return;
        }

        var tempBitmap = new RoaringBitmap(_allocator);
        Span<long> buffer = stackalloc long[4096];

        try
        {
            for (int i = 0; i < _ops.Length; i++)
            {
                _token.ThrowIfCancellationRequested();
                ref PlanOp op = ref _ops[i];

                switch (op.Kind)
                {
                    case PlanOpKind.FillFromPostings:
                    case PlanOpKind.DirectIterate:
                    {
                        // Materialize the resolved match into the bitmap
                        var match = _resolvedMatches[op.ParamIndex];
                        MaterializeIntoBitmap(ref match, ref _bitmap, buffer);
                        break;
                    }

                    case PlanOpKind.AndWithPostings:
                    {
                        // Materialize into temp, AND with main
                        tempBitmap.Clear();
                        var match = _resolvedMatches[op.ParamIndex];
                        MaterializeIntoBitmap(ref match, ref tempBitmap, buffer);
                        _bitmap.AndWith(ref tempBitmap);
                        break;
                    }

                    case PlanOpKind.OrWithPostings:
                    case PlanOpKind.LazyOrWithPostings:
                    {
                        // Materialize into temp, OR with main
                        tempBitmap.Clear();
                        var match = _resolvedMatches[op.ParamIndex];
                        MaterializeIntoBitmap(ref match, ref tempBitmap, buffer);
                        _bitmap.OrWith(ref tempBitmap);
                        break;
                    }

                    case PlanOpKind.AndNotWithPostings:
                    {
                        // Materialize into temp, ANDNOT with main
                        tempBitmap.Clear();
                        var match = _resolvedMatches[op.ParamIndex];
                        MaterializeIntoBitmap(ref match, ref tempBitmap, buffer);
                        _bitmap.AndNotWith(ref tempBitmap);
                        break;
                    }

                    case PlanOpKind.RepairAfterLazy:
                        _bitmap.RepairAfterLazy();
                        break;

                    case PlanOpKind.CheckAndMaybeEntryScan:
                        // Skip for now — goto not yet functional (empty predicates)
                        break;

                    case PlanOpKind.IterateInto:
                        // Terminal — stop processing ops, bitmap is ready
                        goto DoneProcessing;

                    default:
                        throw new NotSupportedException(
                            $"PlanOp {op.Kind} not yet supported in CompiledQueryMatch.");
                }
            }

            DoneProcessing:
            _bitmap.PrepareForReading();
            _count = _bitmap.Cardinality;
            _iterator = _bitmap.GetIterator();
        }
        finally
        {
            tempBitmap.Dispose();
        }
    }

    private static void MaterializeIntoBitmap(ref IQueryMatch match, ref RoaringBitmap bitmap, Span<long> buffer)
    {
        int read;
        while ((read = match.Fill(buffer)) > 0)
        {
            bitmap.AddRange(buffer.Slice(0, read));
        }
    }

    public void Dispose()
    {
        _bitmap.Dispose();
    }
}
