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
        var mergeBuffer = new long[mergeSize];
        int merged = MergeHelper.Or(mergeBuffer.AsSpan(0, mergeSize), leftSpan, rightSpan);

        int copyCount = Math.Min(merged, output.Length);
        mergeBuffer.AsSpan(0, copyCount).CopyTo(output);

        // Save any remaining merged entries for the next Fill call
        SaveRemaining(mergeBuffer.AsSpan(copyCount, merged - copyCount), ref _leftRemaining, ref _leftRemainingCount);
        _rightRemainingCount = 0;
        return copyCount;
    }

    private int FillAnd(Span<long> output)
    {
        Span<long> leftBuf = _leftBuffer;
        Span<long> rightBuf = _rightBuffer;

        int leftCount = _leftRemainingCount > 0 ? _leftRemainingCount : (!_leftDone ? _left.Fill(leftBuf) : 0);
        if (leftCount == 0 && _leftRemainingCount == 0) { _leftDone = true; return 0; }
        Span<long> leftSpan = _leftRemainingCount > 0 ? _leftRemaining.AsSpan(0, _leftRemainingCount) : leftBuf[..leftCount];

        int rightCount = _rightRemainingCount > 0 ? _rightRemainingCount : (!_rightDone ? _right.Fill(rightBuf) : 0);
        if (rightCount == 0 && _rightRemainingCount == 0) { _rightDone = true; return 0; }
        Span<long> rightSpan = _rightRemainingCount > 0 ? _rightRemaining.AsSpan(0, _rightRemainingCount) : rightBuf[..rightCount];

        int result = MergeHelper.And(output, leftSpan, rightSpan);
        _leftRemainingCount = 0;
        _rightRemainingCount = 0;
        return result;
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
