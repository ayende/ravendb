using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using Corax.Mappings;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.TermsProviders;
using Voron;
using Voron.Data.CompactTrees;
using Voron.Data.Lookups;
using Range = Corax.Querying.Matches.Meta.Range;

namespace Corax.Querying;

public partial class IndexSearcher
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IAggregationProvider TextualAggregation(in FieldMetadata field, bool forward = true, in CancellationToken token = default)
    {
        var compactTree = _fieldsTree?.CompactTreeFor(field.FieldName);
        if (compactTree is null)
            return new EmptyAggregationProvider();
        
        return forward
            ? new ExistsTermsProvider<Lookup<CompactTree.CompactKeyLookup>.ForwardIterator>(this, compactTree, field, forAggregation: true)
            : new ExistsTermsProvider<Lookup<CompactTree.CompactKeyLookup>.BackwardIterator>(this, compactTree, field, forAggregation: true);
    }

    public IAggregationProvider LowAggregationBuilder<TValue>(in FieldMetadata field, TValue value, UnaryMatchOperation operation, bool forward)
    {
        Debug.Assert(value is double or string, "value is double or string");
        Debug.Assert(operation is UnaryMatchOperation.LessThan or UnaryMatchOperation.LessThanOrEqual);
        
        return value switch
        {
            double d => BetweenAggregation(field, double.MinValue, d, UnaryMatchOperation.GreaterThanOrEqual, rightSide: operation,
                forward),
            string s => BetweenAggregation(field, Slices.BeforeAllKeys, EncodeAndApplyAnalyzer(default, s), UnaryMatchOperation.GreaterThanOrEqual,
                operation, forward),
            _ => throw new ArgumentOutOfRangeException(nameof(value), value, null)
        };
    }

    public IAggregationProvider GreaterAggregationBuilder<TValue>(in FieldMetadata field, TValue value, UnaryMatchOperation operation, bool forward)
    {
        Debug.Assert(operation is UnaryMatchOperation.GreaterThan or UnaryMatchOperation.GreaterThanOrEqual);
        Debug.Assert(value is double or string, "value is double or string");
        
        return value switch
        {
            double d => BetweenAggregation(field, d, double.MaxValue, operation, rightSide: UnaryMatchOperation.LessThanOrEqual,
                forward),
            string s => BetweenAggregation(field, EncodeAndApplyAnalyzer(default, s), Slices.AfterAllKeys, operation,
                UnaryMatchOperation.LessThanOrEqual, forward),
            _ => throw new ArgumentOutOfRangeException(nameof(value), value, null)
        };
    }
    
    public IAggregationProvider BetweenAggregation<TValue>(in FieldMetadata field, TValue low, TValue high,
        UnaryMatchOperation leftSide = UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation rightSide = UnaryMatchOperation.LessThanOrEqual, bool forward = true)
    {
        Debug.Assert(low is double or long or string or Slice, "value is double, long, string or Slice");

        // Map the (low, high) inclusivity pair to the compile-time range markers once. The value-type
        // fan-out (double / long / string / Slice) lives in the generic builder this dispatches to, so the
        // marker mapping is no longer duplicated per value type.
        return (leftSide, rightSide) switch
        {
            // (x, y)
            (UnaryMatchOperation.GreaterThan, UnaryMatchOperation.LessThan) =>
                AggregationRangeBuilder<TValue, Range.Exclusive, Range.Exclusive>(field, low, high, forward),
            //<x, y)
            (UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThan) =>
                AggregationRangeBuilder<TValue, Range.Inclusive, Range.Exclusive>(field, low, high, forward),
            //<x, y>
            (UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThanOrEqual) =>
                AggregationRangeBuilder<TValue, Range.Inclusive, Range.Inclusive>(field, low, high, forward),
            //(x, y>
            (UnaryMatchOperation.GreaterThan, UnaryMatchOperation.LessThanOrEqual) =>
                AggregationRangeBuilder<TValue, Range.Exclusive, Range.Inclusive>(field, low, high, forward),
            _ => throw new ArgumentOutOfRangeException($"Unknown operation at {nameof(BetweenQuery)}.")
        };
    }

    // Two-ended probe + combiner. Cheaply estimates how many *documents* match a range without scanning it: it samples
    // the posting-count distribution at the bottom and top of the range, gets a sub-linear estimate of the in-range
    // term count, and extrapolates the unscanned middle assuming a similar per-term density. Open bounds are estimated
    // directly (the term-count descent walks to the edge of the tree), so every range yields a concrete, non-negative
    // estimate capped at NumberOfEntries.
    private const int RangeBottomSample = 512;
    private const int RangeTopSample = 256;

    public long EstimateMatchesInRange<TValue>(in FieldMetadata field, TValue low, TValue high,
        UnaryMatchOperation leftSide = UnaryMatchOperation.GreaterThanOrEqual,
        UnaryMatchOperation rightSide = UnaryMatchOperation.LessThanOrEqual)
    {
        var forward = BetweenAggregation(field, low, high, leftSide, rightSide, forward: true);

        long terms = forward.EstimateTermCountInRange();
        if (terms == 0)
            return 0;

        // Scan the bottom of the range. If we never hit the cap, we have walked every in-range term: the count is exact.
        RangePostingStats bottom = forward.CountPostingsInRange(RangeBottomSample);
        if (bottom.Terms < RangeBottomSample)
            return Math.Min(bottom.Postings, NumberOfEntries);

        // Cap the top sample so it cannot overlap the bottom sample (matters only for ranges barely above the cap).
        int topCap = (int)Math.Min(RangeTopSample, Math.Max(0, terms - bottom.Terms));
        if (topCap == 0)
            return Math.Min(bottom.Postings, NumberOfEntries);

        var backward = BetweenAggregation(field, low, high, leftSide, rightSide, forward: false);
        RangePostingStats top = backward.CountPostingsInRange(topCap);

        long sampledTerms = bottom.Terms + top.Terms;
        long sampledPostings = bottom.Postings + top.Postings;
        long middleTerms = Math.Max(0, terms - sampledTerms);

        double sampledAvg = (double)sampledPostings / sampledTerms;

        // Whale guard: a dense ("whale") term hiding in the unscanned middle would be invisible to the edge samples.
        // The global average postings-per-term (total docs / total terms, both O(1)) is a floor that lifts the estimate
        // when the sampled edges look unusually sparse relative to the field as a whole.
        long totalTerms = forward.TotalTermCount();
        double globalAvg = totalTerms > 0 ? (double)NumberOfEntries / totalTerms : sampledAvg;
        double middleAvg = Math.Max(sampledAvg, globalAvg);

        long estimate = sampledPostings + (long)(middleTerms * middleAvg);
        return Math.Min(estimate, NumberOfEntries);
    }

    // StartsWith(prefix) matches exactly the contiguous byte-range [encodedPrefix, successor(encodedPrefix)). The prefix
    // is analyzer-encoded the same way stored terms are, and the CompactTree sorts lexicographically on those bytes, so
    // every prefix match is one block. The exclusive upper bound is the encoded prefix with its last non-0xFF byte
    // incremented and trailing 0xFF bytes dropped; if every byte is 0xFF (or the prefix is empty) no finite successor
    // exists and the range runs to the end of the tree. Reuses the range estimator so StartsWith costs the same two
    // descents as a bounded range instead of falling back to the whole-index size.
    public long EstimateStartsWith(in FieldMetadata field, string prefix)
    {
        Slice encodedPrefix = EncodeAndApplyAnalyzer(field, prefix);
        ReadOnlySpan<byte> prefixBytes = encodedPrefix.AsReadOnlySpan();

        int len = prefixBytes.Length;
        while (len > 0 && prefixBytes[len - 1] == 0xFF)
            len--;

        if (len == 0)
        {
            // empty prefix or all-0xFF carry: no finite successor, so the match set runs to the end of the tree
            return EstimateMatchesInRange(field, encodedPrefix, Slices.AfterAllKeys,
                UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThanOrEqual);
        }

        using var _ = Allocator.Allocate(len, out Span<byte> successor);
        prefixBytes.Slice(0, len).CopyTo(successor);
        successor[len - 1]++;

        using var __ = Slice.From(Allocator, successor, out Slice high);
        return EstimateMatchesInRange(field, encodedPrefix, high,
            UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThan);
    }

    private IAggregationProvider AggregationRangeBuilder<TLow, THigh>(in FieldMetadata field, Slice low, Slice high, bool forward)
        where TLow : struct, Range.Marker
        where THigh : struct, Range.Marker
    {
        var terms = _fieldsTree?.CompactTreeFor(field.FieldName);
        if (terms == null)
            return new EmptyAggregationProvider();

        return forward switch
        {
            true => new TermsRangeProvider<Lookup<CompactTree.CompactKeyLookup>.ForwardIterator, TLow, THigh>(this, terms, field, low, high),
            false => new TermsRangeProvider<Lookup<CompactTree.CompactKeyLookup>.BackwardIterator, TLow, THigh>(this, terms, field, low, high)
        };
    }


    // Single place the runtime value type fans out to its lookup-key/term-type pair. Numeric values (double,
    // long) go through their dedicated numeric lookup; string/Slice share the textual builder (strings are
    // analyzer-encoded to a Slice first). longs are kept as longs so full precision is preserved end to end.
    private IAggregationProvider AggregationRangeBuilder<TValue, TLow, THigh>(in FieldMetadata field, TValue low, TValue high, bool forward)
        where TLow : struct, Range.Marker
        where THigh : struct, Range.Marker
    {
        if (typeof(TValue) == typeof(double))
            return AggregationRangeBuilder<DoubleLookupKey, double, TLow, THigh>(field, new((double)(object)low), new((double)(object)high), forward);

        if (typeof(TValue) == typeof(long))
            return AggregationRangeBuilder<Int64LookupKey, long, TLow, THigh>(field, new((long)(object)low), new((long)(object)high), forward);

        if (typeof(TValue) == typeof(string))
            return AggregationRangeBuilder<TLow, THigh>(field, EncodeAndApplyAnalyzer(default, (string)(object)low), EncodeAndApplyAnalyzer(default, (string)(object)high), forward);

        if (typeof(TValue) == typeof(Slice))
            return AggregationRangeBuilder<TLow, THigh>(field, (Slice)(object)low, (Slice)(object)high, forward);

        throw new ArgumentException($"{typeof(TValue)} is not supported in {nameof(BetweenQuery)}");
    }


    private IAggregationProvider AggregationRangeBuilder<TLookupKey, TTermType, TLow, THigh>(FieldMetadata field, TLookupKey low, TLookupKey high, bool forward)
        where TLow : struct, Range.Marker
        where THigh : struct, Range.Marker
        where TLookupKey : struct, ILookupKey
    {
        field = field.GetNumericFieldMetadata<TTermType>(Allocator);
        var set = _fieldsTree?.LookupFor<TLookupKey>(field.FieldName);
        if (set is null || set.NumberOfEntries == 0)
            return new EmptyAggregationProvider();

        return forward switch
        {
            true => new TermsNumericRangeProvider<Lookup<TLookupKey>.ForwardIterator, TLow, THigh, TLookupKey>(this, set, field, low, high),
            false => new TermsNumericRangeProvider<Lookup<TLookupKey>.BackwardIterator, TLow, THigh, TLookupKey>(this, set, field, low, high)
        };
    }
}
