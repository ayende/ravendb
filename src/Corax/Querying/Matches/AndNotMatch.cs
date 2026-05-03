using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using Corax.Querying.Matches.Meta;
using Sparrow.Server;
using Voron.Data.RoaringBitmaps;

namespace Corax.Querying.Matches
{
    [DebuggerDisplay("{DebugView,nq}")]
    public struct AndNotMatch<TInner, TOuter> : IQueryMatch, IDisposable
    where TInner : IQueryMatch
    where TOuter : IQueryMatch
    {
        private TInner _inner;
        private TOuter _outer;

        private long _totalResults;
        private QueryCountConfidence _confidence;
        private readonly CancellationToken _token;
        public DuplicatesOccurrence DuplicatesOccurrenceStatus => DuplicatesOccurrence.Possible;

        public bool IsBoosting => _inner.IsBoosting || _outer.IsBoosting;
        public long Count => _totalResults;

        private readonly ByteStringContext _context;

        private RoaringBitmapData _bitmapData;
        private RoaringBitmapIterator _iterator;
        private bool _materialized;
        private bool _isAndWithBuffer;

        private const int FillScratchSize = 4096;

        public SkipSortingResult AttemptToSkipSorting() => SkipSortingResult.ResultsNativelySorted;

        public QueryCountConfidence Confidence => _confidence;

        private AndNotMatch(ByteStringContext context,
            in TInner inner, in TOuter outer,
            long totalResults, QueryCountConfidence confidence, CancellationToken token)
        {
            _totalResults = totalResults;

            _inner = inner;
            _outer = outer;
            _confidence = confidence;
            _token = token;

            _context = context;
            _bitmapData = default;
            _iterator = default;
            _materialized = false;
            _isAndWithBuffer = false;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int Fill(Span<long> matches)
        {
            if (_isAndWithBuffer)
                throw new InvalidOperationException($"We cannot execute `{nameof(Fill)}` after initiating a `{nameof(AndWith)}` operation.");

            if (_materialized == false)
                Materialize();

            return _iterator.Fill(ref _bitmapData, matches);
        }

        private void Materialize()
        {
            _token.ThrowIfCancellationRequested();

            // Drain inner into the result bitmap.
            FillBitmapFromMatch(ref _inner, ref _bitmapData, _context);
            RoaringBitmap main = new(ref _bitmapData, _context);
            main.PrepareForReading();

            // Drain outer into a scratch bitmap and subtract it.
            RoaringBitmapData outerData = default;
            try
            {
                FillBitmapFromMatch(ref _outer, ref outerData, _context);
                RoaringBitmap outer = new(ref outerData, _context);
                outer.PrepareForReading();

                _token.ThrowIfCancellationRequested();
                main.AndNotWith(ref outerData);
                main.PrepareForReading();
            }
            finally
            {
                outerData.Dispose(_context);
            }

            _totalResults = _bitmapData.Count;
            _confidence = QueryCountConfidence.High;
            _iterator = _bitmapData.GetIterator(_context);
            _materialized = true;
        }

        private void FillBitmapFromMatch<TMatch>(ref TMatch match, ref RoaringBitmapData bitmapData, ByteStringContext context)
            where TMatch : IQueryMatch
        {
            RoaringBitmap bitmap = new(ref bitmapData, context);
            Span<long> scratch = stackalloc long[FillScratchSize];
            int read;
            while ((read = match.Fill(scratch)) > 0)
            {
                _token.ThrowIfCancellationRequested();
                // Successive Fill batches are not guaranteed to be ascending across calls; add per-element.
                for (int i = 0; i < read; i++)
                    bitmap.Add(scratch[i]);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public int AndWith(Span<long> buffer, int matches)
        {
            if (_isAndWithBuffer == false)
            {
                if (_materialized == false)
                    Materialize();
                _isAndWithBuffer = true;
            }

            // Filter the incoming buffer against the materialized result set.
            int kept = 0;
            for (int i = 0; i < matches; i++)
            {
                if (_bitmapData.Contains(buffer[i]))
                    buffer[kept++] = buffer[i];
            }
            return kept;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Score(Span<long> matches, Span<float> scores, float boostFactor)
        {
            _inner.Score(matches, scores, boostFactor);
        }

        public QueryInspectionNode Inspect()
        {
            return new QueryInspectionNode($"{nameof(AndNotMatch)} [AndNot]",
                children: new List<QueryInspectionNode> { _inner.Inspect(), _outer.Inspect() },
                parameters: new Dictionary<string, string>()
                {
                    { Constants.QueryInspectionNode.IsBoosting, IsBoosting.ToString() },
                    { Constants.QueryInspectionNode.Count, Count.ToString() },
                    { Constants.QueryInspectionNode.CountConfidence, Confidence.ToString() }
                });
        }

        string DebugView => Inspect().ToString();

        public void Dispose()
        {
            _iterator.Dispose();
            _bitmapData.Dispose(_context);
        }


        public static AndNotMatch<TInner, TOuter> Create(IndexSearcher searcher, in TInner inner, in TOuter outer, in CancellationToken token)
        {
            // Estimate Confidence values.
            QueryCountConfidence confidence;
            if (inner.Count < outer.Count / 2)
                confidence = inner.Confidence;
            else if (outer.Count < inner.Count / 2)
                confidence = outer.Confidence;
            else
                confidence = inner.Confidence.Min(outer.Confidence);

            return new AndNotMatch<TInner, TOuter>(searcher.Allocator, in inner, in outer, inner.Count, confidence, token);
        }
    }
}
