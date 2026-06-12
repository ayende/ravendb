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
        Debug.Assert(low is double or string or Slice, "value is double or string or Slice");
        
        if (typeof(TValue) == typeof(double))
        {
            return (leftSide, rightSide) switch
            {
                // (x, y)
                (UnaryMatchOperation.GreaterThan, UnaryMatchOperation.LessThan) => AggregationRangeBuilder<Range.Exclusive, Range.Exclusive>(field, (double)(object)low,
                    (double)(object)high, forward),

                //<x, y)
                (UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThan) => AggregationRangeBuilder<Range.Inclusive, Range.Exclusive>(field,
                    (double)(object)low,
                    (double)(object)high, forward),

                //<x, y>
                (UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThanOrEqual) => AggregationRangeBuilder<Range.Inclusive, Range.Inclusive>(field,
                    (double)(object)low, (double)(object)high, forward),

                //(x, y>
                (UnaryMatchOperation.GreaterThan, UnaryMatchOperation.LessThanOrEqual) => AggregationRangeBuilder<Range.Exclusive, Range.Inclusive>(field,
                    (double)(object)low,
                    (double)(object)high, forward),
                _ => throw new ArgumentOutOfRangeException($"Unknown operation at {nameof(BetweenQuery)}.")
            };
        }

        if (typeof(TValue) == typeof(string) || typeof(TValue) == typeof(Slice))
        {
            Slice leftValue;
            Slice rightValue;

            if (typeof(string) == typeof(TValue))
            {
                leftValue = EncodeAndApplyAnalyzer(default, (string)(object)low);
                rightValue = EncodeAndApplyAnalyzer(default, (string)(object)high);
            }
            else
            {
                leftValue = (Slice)(object)low;
                rightValue = (Slice)(object)high;
            }

            return (leftSide, rightSide) switch
            {
                // (x, y)
                (UnaryMatchOperation.GreaterThan, UnaryMatchOperation.LessThan) => AggregationRangeBuilder<Range.Exclusive, Range.Exclusive>(field,
                    leftValue, rightValue, forward),

                //<x, y)
                (UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThan) => AggregationRangeBuilder<Range.Inclusive, Range.Exclusive>(field,
                    leftValue, rightValue, forward),

                //<x, y>
                (UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThanOrEqual) => AggregationRangeBuilder<Range.Inclusive, Range.Inclusive>(
                    field, leftValue, rightValue, forward),

                //(x, y>
                (UnaryMatchOperation.GreaterThan, UnaryMatchOperation.LessThanOrEqual) => AggregationRangeBuilder<Range.Exclusive, Range.Inclusive>(field,
                    leftValue, rightValue, forward),

                _ => throw new ArgumentOutOfRangeException($"Unknown operation at {nameof(BetweenQuery)}.")
            };
        }

        throw new ArgumentException($"{typeof(TValue)} is not supported in {nameof(BetweenQuery)}");
    }

    // Two-ended probe + combiner. Cheaply estimates how many *documents* match a range without scanning it: it samples
    // the posting-count distribution at the bottom and top of the range, gets a sub-linear estimate of the in-range
    // term count, and extrapolates the unscanned middle assuming a similar per-term density. Returns -1 when the range
    // can't be estimated cheaply (e.g. an open-ended textual bound) so the caller can fall back to a coarser bound.
    private const int RangeBottomSample = 512;
    private const int RangeTopSample = 256;

    public long EstimateMatchesInRange<TValue>(in FieldMetadata field, TValue low, TValue high,
        UnaryMatchOperation leftSide = UnaryMatchOperation.GreaterThanOrEqual,
        UnaryMatchOperation rightSide = UnaryMatchOperation.LessThanOrEqual)
    {
        var forward = BetweenAggregation(field, low, high, leftSide, rightSide, forward: true);

        long terms = forward.EstimateTermCountInRange();
        if (terms < 0)
            return -1; // not cheaply estimable -> caller decides on a fallback
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

    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private IAggregationProvider AggregationRangeBuilder<TLow, THigh>(FieldMetadata field, double low, double high, bool forward)
        where TLow : struct, Range.Marker
        where THigh : struct, Range.Marker
        => AggregationRangeBuilder<DoubleLookupKey, double, TLow, THigh>(field, new(low), new(high), forward);


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
