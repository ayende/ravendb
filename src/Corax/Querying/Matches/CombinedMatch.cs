using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Corax.Querying.Matches.Meta;

namespace Corax.Querying.Matches;

/// <summary>
/// Lightweight replacement for BinaryMatch. Combines two IQueryMatch instances
/// with OR or AND semantics during Fill(). Used internally by SearchQuery and
/// InQuery for combining intermediate results.
/// </summary>
public struct CombinedMatch : IQueryMatch
{
    private IQueryMatch _left;
    private IQueryMatch _right;
    private readonly bool _isOr;
    private long[] _leftBuffer;
    private long[] _rightBuffer;
    private bool _leftDone;
    private bool _rightDone;
    private long[] _leftRemaining;
    private int _leftRemainingCount;
    private long[] _rightRemaining;
    private int _rightRemainingCount;
    private long[] _mergeBuffer;
    private const int BufferSize = 4096;

    private CombinedMatch(IQueryMatch left, IQueryMatch right, bool isOr)
    {
        _left = left;
        _right = right;
        _isOr = isOr;
        _leftBuffer = new long[BufferSize];
        _rightBuffer = new long[BufferSize];
        _leftRemaining = Array.Empty<long>();
        _rightRemaining = Array.Empty<long>();
        _mergeBuffer = new long[BufferSize * 2];
    }

    public static CombinedMatch Or(IQueryMatch left, IQueryMatch right) => new(left, right, isOr: true);
    public static CombinedMatch And(IQueryMatch left, IQueryMatch right) => new(left, right, isOr: false);

    public long Count => _isOr
        ? Math.Min(_left.Count + _right.Count, long.MaxValue)
        : Math.Min(_left.Count, _right.Count);

    public QueryCountConfidence Confidence => _left.Confidence.Min(_right.Confidence);
    public bool IsBoosting => _left.IsBoosting || _right.IsBoosting;
    public DuplicatesOccurrence DuplicatesOccurrenceStatus => DuplicatesOccurrence.Possible;

    public SkipSortingResult AttemptToSkipSorting() => SkipSortingResult.SortingIsRequired;

    public int Fill(Span<long> matches)
    {
        if (_isOr)
            return FillOr(matches);
        return FillAnd(matches);
    }

    private int FillOr(Span<long> output)
    {
        // Fill from both sides, merge
        Span<long> leftBuf = _leftBuffer;
        Span<long> rightBuf = _rightBuffer;

        int leftCount = _leftRemainingCount;
        Span<long> leftSpan;
        if (leftCount > 0)
        {
            leftSpan = _leftRemaining.AsSpan(0, leftCount);
        }
        else if (!_leftDone)
        {
            leftCount = _left.Fill(leftBuf);
            if (leftCount == 0) _leftDone = true;
            leftSpan = leftBuf[..leftCount];
        }
        else
        {
            leftSpan = Span<long>.Empty;
        }

        int rightCount = _rightRemainingCount;
        Span<long> rightSpan;
        if (rightCount > 0)
        {
            rightSpan = _rightRemaining.AsSpan(0, rightCount);
        }
        else if (!_rightDone)
        {
            rightCount = _right.Fill(rightBuf);
            if (rightCount == 0) _rightDone = true;
            rightSpan = rightBuf[..rightCount];
        }
        else
        {
            rightSpan = Span<long>.Empty;
        }

        if (leftSpan.Length == 0 && rightSpan.Length == 0)
            return 0;

        if (leftSpan.Length == 0)
        {
            int toCopy = Math.Min(rightSpan.Length, output.Length);
            rightSpan[..toCopy].CopyTo(output);
            SaveRemaining(rightSpan[toCopy..], ref _rightRemaining, ref _rightRemainingCount);
            _leftRemainingCount = 0;
            return toCopy;
        }

        if (rightSpan.Length == 0)
        {
            int toCopy = Math.Min(leftSpan.Length, output.Length);
            leftSpan[..toCopy].CopyTo(output);
            SaveRemaining(leftSpan[toCopy..], ref _leftRemaining, ref _leftRemainingCount);
            _rightRemainingCount = 0;
            return toCopy;
        }

        // MergeHelper.Or does not respect destination length — merge into a temp buffer
        // large enough for the full merge, then copy what fits to output.
        int mergeSize = leftSpan.Length + rightSpan.Length;
        if (_mergeBuffer.Length < mergeSize)
            _mergeBuffer = new long[mergeSize];
        int merged = MergeHelper.Or(_mergeBuffer.AsSpan(0, mergeSize), leftSpan, rightSpan);

        int copyCount = Math.Min(merged, output.Length);
        _mergeBuffer.AsSpan(0, copyCount).CopyTo(output);

        // Save any remaining merged entries for the next Fill call
        SaveRemaining(_mergeBuffer.AsSpan(copyCount, merged - copyCount), ref _leftRemaining, ref _leftRemainingCount);
        _rightRemainingCount = 0;
        return copyCount;
    }

    private int FillAnd(Span<long> output)
    {
        // AND of two ascending-sorted streams. Each Fill on an inner match returns
        // the next batch of ascending entries; we must intersect them while preserving
        // the side whose tail has not yet been consumed (its higher entries may match
        // future entries on the other side). Loops until we either produce results or
        // one side is permanently exhausted.
        while (true)
        {
            if (EnsureBuffered(_left, ref _leftBuffer, ref _leftRemaining, ref _leftRemainingCount, ref _leftDone) == false)
                return 0;
            if (EnsureBuffered(_right, ref _rightBuffer, ref _rightRemaining, ref _rightRemainingCount, ref _rightDone) == false)
                return 0;

            Span<long> leftSpan = _leftRemaining.AsSpan(0, _leftRemainingCount);
            Span<long> rightSpan = _rightRemaining.AsSpan(0, _rightRemainingCount);

            long leftMax = leftSpan[^1];
            long rightMax = rightSpan[^1];

            int result = MergeHelper.And(output, leftSpan, rightSpan);

            // Drop everything up to the lower of the two maxes from the side that owns it,
            // because the other side has not yet seen entries past its own max. Keep the
            // higher-max tail so it can match future entries from the side that lags.
            if (leftMax <= rightMax)
            {
                _leftRemainingCount = 0;
                TrimBelowOrEqual(ref _rightRemaining, ref _rightRemainingCount, leftMax);
            }
            else
            {
                _rightRemainingCount = 0;
                TrimBelowOrEqual(ref _leftRemaining, ref _leftRemainingCount, rightMax);
            }

            if (result > 0)
                return result;

            // Empty intersection on this batch; refill and try again unless the side
            // we need to advance is already exhausted.
        }
    }

    private static bool EnsureBuffered(IQueryMatch match, ref long[] scratch, ref long[] saved, ref int savedCount, ref bool done)
    {
        if (savedCount > 0)
            return true;
        if (done)
            return false;

        int filled = match.Fill(scratch);
        if (filled == 0)
        {
            done = true;
            return false;
        }

        if (saved.Length < filled)
            saved = new long[filled];
        scratch.AsSpan(0, filled).CopyTo(saved);
        savedCount = filled;
        return true;
    }

    private static void TrimBelowOrEqual(ref long[] buffer, ref int count, long threshold)
    {
        // Remove entries <= threshold from the front of buffer (entries are ascending).
        int i = 0;
        while (i < count && buffer[i] <= threshold)
            i++;
        if (i == 0)
            return;
        if (i >= count)
        {
            count = 0;
            return;
        }
        int remaining = count - i;
        Array.Copy(buffer, i, buffer, 0, remaining);
        count = remaining;
    }

    private static void SaveRemaining(Span<long> remaining, ref long[] buffer, ref int count)
    {
        if (remaining.Length == 0)
        {
            count = 0;
            return;
        }
        if (buffer.Length < remaining.Length)
            buffer = new long[remaining.Length];
        remaining.CopyTo(buffer);
        count = remaining.Length;
    }

    public int AndWith(Span<long> buffer, int matches)
    {
        throw new NotSupportedException($"{nameof(CombinedMatch)} does not support AndWith.");
    }

    public void Score(Span<long> matches, Span<float> scores, float boostFactor)
    {
        _left.Score(matches, scores, boostFactor);
        _right.Score(matches, scores, boostFactor);
    }

    public QueryInspectionNode Inspect()
    {
        return new QueryInspectionNode(_isOr ? "Or" : "And",
            children: new List<QueryInspectionNode> { _left.Inspect(), _right.Inspect() },
            parameters: new Dictionary<string, string>
            {
                { "Count", Count.ToString() },
                { "IsBoosting", IsBoosting.ToString() }
            });
    }
}
