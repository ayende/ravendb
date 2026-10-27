using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;
using Sparrow.Binary;
using Voron.Global;

namespace Voron.Impl.Paging;

/// <summary>
/// The per data file state of <see cref="PrefetchPolicy"/>: the hits of every segment in the current epoch and when
/// each segment was last prefetched. It is updated on every page access without synchronization: a lost update only
/// changes when a prefetch is issued.
/// </summary>
internal sealed class SegmentPrefetchTable
{
    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    private struct Segment
    {
        public ushort Epoch;        // the epoch Hits belong to
        public ushort PrefetchedAt; // PrefetchPolicy.NowSeconds of the last prefetch, 0 = never
        public byte Hits;
    }

    [StructLayout(LayoutKind.Explicit, Size = 64)]
    private struct PaddedCounter
    {
        [FieldOffset(0)]
        public long Value;
    }

    // a counter line per processor (a power of two, so a mask picks it), capped so a server with thousands of data files
    // does not spend much memory on it
    private static readonly int Stripes = Math.Min(64, Bits.PowerOf2(Environment.ProcessorCount));

    private readonly PrefetchPolicy _policy;
    private readonly int _segmentShift;
    private readonly PaddedCounter[] _accesses = new PaddedCounter[Stripes]; // striped, so readers on many cores do not share a line
    private Segment[] _segments;

    public readonly long SegmentSize;
    public DevicePrefetchGate Gate;

    public SegmentPrefetchTable(PrefetchPolicy policy, long segmentSize, long fileSize)
    {
        _policy = policy;
        _segmentShift = Bits.MostSignificantBit(segmentSize);
        SegmentSize = 1L << _segmentShift;
        _segments = new Segment[Math.Max(1, (fileSize >> _segmentShift) + 1)];
        policy.Register(this);
    }

    public long Accesses
    {
        get
        {
            long sum = 0;
            for (var i = 0; i < Stripes; i++)
                sum += Volatile.Read(ref _accesses[i].Value);
            return sum;
        }
    }

    public void Unregister() => _policy.Unregister(this);

    /// <summary>
    /// Counts an access to <paramref name="pageNumber"/>. Returns true with the range to read ahead when its segment
    /// reached the policy's threshold (and the next segment too when the access pattern is sequential).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool ShouldPrefetchSegment(long pageNumber, out long offset, out long bytes)
    {
        offset = 0;
        bytes = 0;
        var index = (pageNumber * Constants.Storage.PageSize) >> _segmentShift;
        var segments = _segments;
        if (index >= segments.Length)
            segments = Grow(index);

        _accesses[Environment.CurrentManagedThreadId & (Stripes - 1)].Value++;

        ref var segment = ref segments[index];
        var policy = _policy;
        var epoch = (ushort)policy.Epoch;
        var now = (ushort)policy.NowSeconds;
        if (segment.Epoch != epoch)
        {
            segment.Epoch = epoch;
            segment.Hits = 0;
        }

        if (segment.PrefetchedAt != 0 && (ushort)(now - segment.PrefetchedAt) < PrefetchPolicy.RearmSeconds)
            return false;

        var hits = segment.Hits;
        if (hits < byte.MaxValue)
            segment.Hits = ++hits;
        if (hits < policy.Threshold)
            return false;

        return Unlikely(segments, index, now, out offset, out bytes);
    }

    private bool Unlikely(Segment[] segments, long index, ushort now, out long offset, out long bytes)
    {
        offset = index << _segmentShift;
        bytes = SegmentSize;

        // the previous segment was read ahead a moment ago: a sequential pattern, read the next one as well
        var sequential = index > 0 && PrefetchedWithin(segments[index - 1], now, PrefetchPolicy.SequentialSeconds) && index + 1 < segments.Length;
        if (sequential)
            bytes += SegmentSize;

        if (Gate != null && Gate.TryAdmit(bytes) == false)
            return false;

        segments[index].PrefetchedAt = now;
        if (sequential)
            segments[index + 1].PrefetchedAt = now;
        return true;
    }

    private static bool PrefetchedWithin(in Segment segment, ushort now, int seconds) =>
        segment.PrefetchedAt != 0 && (ushort)(now - segment.PrefetchedAt) <= seconds;

    private Segment[] Grow(long index)
    {
        var current = _segments;
        if (index < current.Length)
            return current;
        var grown = new Segment[Math.Max(index + 1, current.Length * 2)];
        Array.Copy(current, grown, current.Length);
        _segments = grown; // a racing reader may keep counting in the old array for a moment
        return grown;
    }
}
