using System;
using System.Runtime.CompilerServices;
using System.Threading;
using Corax.Mappings;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.TermsProviders;
using Voron;
using Voron.Data.Lookups;
using static Voron.Data.CompactTrees.CompactTree;
using Range = Corax.Querying.Matches.Meta.Range;

namespace Corax.Querying;

public partial class IndexSearcher
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IQueryMatch BetweenQuery<TValue>(in FieldMetadata field, TValue low, TValue high, UnaryMatchOperation leftSide = UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation rightSide = UnaryMatchOperation.LessThanOrEqual, bool forward = true, long maxNumberOfTerms = long.MaxValue, CancellationToken token = default) {
        if (typeof(TValue) == typeof(long))
        {
            return (leftSide, rightSide) switch
            {
                // (x, y)
                (UnaryMatchOperation.GreaterThan, UnaryMatchOperation.LessThan) => RangeBuilder<Range.Exclusive, Range.Exclusive>(field, (long)(object)low, (long)(object)high, forward, token: token),

                //<x, y)
                (UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThan) => RangeBuilder<Range.Inclusive, Range.Exclusive>(field, (long)(object)low, (long)(object)high, forward, token: token),

                //<x, y>
                (UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThanOrEqual) => RangeBuilder<Range.Inclusive, Range.Inclusive>(field, (long)(object)low, (long)(object)high, forward, token: token),

                //(x, y>
                (UnaryMatchOperation.GreaterThan, UnaryMatchOperation.LessThanOrEqual) => RangeBuilder<Range.Exclusive, Range.Inclusive>(field, (long)(object)low, (long)(object)high, forward, token: token),
                _ => throw new ArgumentOutOfRangeException($"Unknown operation at {nameof(BetweenQuery)}.")
            };
        }

        if (typeof(TValue) == typeof(double))
        {
            return (leftSide, rightSide) switch
            {
                // (x, y)
                (UnaryMatchOperation.GreaterThan, UnaryMatchOperation.LessThan) => RangeBuilder<Range.Exclusive, Range.Exclusive>(field, (double)(object)low, (double)(object)high, forward, token: token),

                //<x, y)
                (UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThan) => RangeBuilder<Range.Inclusive, Range.Exclusive>(field, (double)(object)low, (double)(object)high, forward, token: token),

                //<x, y>
                (UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThanOrEqual) => RangeBuilder<Range.Inclusive, Range.Inclusive>(field, (double)(object)low, (double)(object)high, forward, token: token),

                //(x, y>
                (UnaryMatchOperation.GreaterThan, UnaryMatchOperation.LessThanOrEqual) => RangeBuilder<Range.Exclusive, Range.Inclusive>(field, (double)(object)low, (double)(object)high, forward, token: token),
                _ => throw new ArgumentOutOfRangeException($"Unknown operation at {nameof(BetweenQuery)}.")

            };
        }

        if (typeof(TValue) == typeof(string))
        {
            var leftValue = EncodeAndApplyAnalyzer(field, (string)(object)low);
            var rightValue = EncodeAndApplyAnalyzer(field, (string)(object)high);

            return (leftSide, rightSide) switch
            {
                // (x, y)
                (UnaryMatchOperation.GreaterThan, UnaryMatchOperation.LessThan) => RangeBuilder<Range.Exclusive, Range.Exclusive>(field,
                    leftValue, rightValue, forward, token: token),

                //<x, y)
                (UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThan) => RangeBuilder<Range.Inclusive, Range.Exclusive>(field,
                    leftValue, rightValue, forward, token: token),

                //<x, y>
                (UnaryMatchOperation.GreaterThanOrEqual, UnaryMatchOperation.LessThanOrEqual) => RangeBuilder<Range.Inclusive, Range.Inclusive>(
                    field, leftValue, rightValue, forward, token: token),

                //(x, y>
                (UnaryMatchOperation.GreaterThan, UnaryMatchOperation.LessThanOrEqual) => RangeBuilder<Range.Exclusive, Range.Inclusive>(field,
                    leftValue, rightValue, forward, token: token),
                _ => throw new ArgumentOutOfRangeException($"Unknown operation at {nameof(BetweenQuery)}.")
            };
        }

        throw new ArgumentException($"{typeof(TValue)} is not supported in {nameof(BetweenQuery)}");
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IQueryMatch GreaterThanQuery<TValue>(in FieldMetadata field, TValue value, bool forward = true, long maxNumberOfTerms = long.MaxValue, CancellationToken token = default)
    {
        return GreatBuilder<Range.Exclusive, Range.Inclusive, TValue>(field, value, forward, token: token);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IQueryMatch GreaterThanOrEqualsQuery<TValue>(in FieldMetadata field, TValue value, bool forward = true, long maxNumberOfTerms = long.MaxValue, CancellationToken token = default)
    {
        return GreatBuilder<Range.Inclusive, Range.Inclusive, TValue>(field, value, forward, token: token);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private IQueryMatch GreatBuilder<TLeftRange, TRightRange, TValue>(in FieldMetadata field, TValue value, bool forward = true, CancellationToken token = default)
        where TLeftRange : struct, Range.Marker
        where TRightRange : struct, Range.Marker
    {
        if (typeof(TValue) == typeof(long))
        {
            return RangeBuilder<TLeftRange, TRightRange>(field, (long)(object)value, long.MaxValue, forward, token: token);
        }

        if (typeof(TValue) == typeof(double))
            return RangeBuilder<TLeftRange, TRightRange>(field, (double)(object)value, double.MaxValue, forward, token: token);
        if (typeof(TValue) == typeof(string))
        {
            var sliceValue = EncodeAndApplyAnalyzer(field, (string)(object)value);
            return RangeBuilder<TLeftRange, TRightRange>(field, sliceValue, Slices.AfterAllKeys, forward, token: token);
        }

        throw new ArgumentException("Range queries are supporting strings, longs or doubles only");
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IQueryMatch LessThanOrEqualsQuery<TValue>(in FieldMetadata field, TValue value, bool forward = true, long maxNumberOfTerms = long.MaxValue, CancellationToken token = default)
        => LessBuilder<Range.Inclusive, Range.Inclusive, TValue>(field, value, forward, token: token);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IQueryMatch LessThanQuery<TValue>(in FieldMetadata field, TValue value, bool forward = true, long maxNumberOfTerms = long.MaxValue, CancellationToken token = default)
        => LessBuilder<Range.Inclusive, Range.Exclusive, TValue>(field, value, forward, token: token);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private IQueryMatch LessBuilder<TLeftRange, TRightRange, TValue>(in FieldMetadata field, TValue value,
        bool forward, CancellationToken token)
        where TLeftRange : struct, Range.Marker
        where TRightRange : struct, Range.Marker
    {
        if (typeof(TValue) == typeof(long))
            return RangeBuilder<TLeftRange, TRightRange>(field, long.MinValue, (long)(object)value, forward, token: token);

        if (typeof(TValue) == typeof(double))
            return RangeBuilder<TLeftRange, TRightRange>(field, double.MinValue, (double)(object)value, forward, token: token);

        if (typeof(TValue) == typeof(string))
        {
            var sliceValue = EncodeAndApplyAnalyzer(field, (string)(object)value);
            return RangeBuilder<TLeftRange, TRightRange>(field, Slices.BeforeAllKeys, sliceValue, forward, token: token);
        }

        throw new ArgumentException("Range queries are supporting strings, longs or doubles only");
    }

    // ── Slice-overloads for pre-analyzed terms ──────────────────────────
    // These exist so callers that already hold an analyzer-encoded slice (e.g. via the
    // per-execution analyzed-slice cache on QueryExecution) can skip the analyzer pass that
    // the string-typed generic builders run internally. They use distinct names ("...Slice")
    // because the existing generic builders accept TValue=Slice, which would otherwise make
    // the calls ambiguous at use sites.

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IQueryMatch GreaterThanQuerySlice(in FieldMetadata field, Slice value, bool forward = true, CancellationToken token = default)
        => RangeBuilder<Range.Exclusive, Range.Inclusive>(field, value, Slices.AfterAllKeys, forward, token);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IQueryMatch GreaterThanOrEqualsQuerySlice(in FieldMetadata field, Slice value, bool forward = true, CancellationToken token = default)
        => RangeBuilder<Range.Inclusive, Range.Inclusive>(field, value, Slices.AfterAllKeys, forward, token);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IQueryMatch LessThanQuerySlice(in FieldMetadata field, Slice value, bool forward = true, CancellationToken token = default)
        => RangeBuilder<Range.Inclusive, Range.Exclusive>(field, Slices.BeforeAllKeys, value, forward, token);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IQueryMatch LessThanOrEqualsQuerySlice(in FieldMetadata field, Slice value, bool forward = true, CancellationToken token = default)
        => RangeBuilder<Range.Inclusive, Range.Inclusive>(field, Slices.BeforeAllKeys, value, forward, token);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IQueryMatch BetweenQuerySlice(in FieldMetadata field, Slice low, Slice high, bool forward = true, CancellationToken token = default)
        => RangeBuilder<Range.Inclusive, Range.Inclusive>(field, low, high, forward, token);

    public IQueryMatch RangeBuilder<TLow, THigh>(in FieldMetadata field, Slice low, Slice high, bool forward, CancellationToken token)
        where TLow : struct, Range.Marker
        where THigh : struct, Range.Marker
    {
        var terms = _fieldsTree?.CompactTreeFor(field.FieldName);
        if (terms == null)
            return TermMatch.CreateEmpty(this, _transaction.Allocator);

        ITermsProvider provider = forward
            ? new TermsRangeProvider<Lookup<CompactKeyLookup>.ForwardIterator, TLow, THigh>(this, terms, field, low, high)
            : (ITermsProvider)new TermsRangeProvider<Lookup<CompactKeyLookup>.BackwardIterator, TLow, THigh>(this, terms, field, low, high);

        return new TermsProviderMatch(provider, _transaction.LowLevelTransaction, _transaction.Allocator);
    }

    private IQueryMatch RangeBuilder<TLow, THigh>(FieldMetadata field, long low, long high, bool forward, CancellationToken token)
        where TLow : struct, Range.Marker
        where THigh : struct, Range.Marker
    {
        field = field.GetNumericFieldMetadata<long>(Allocator);
        var set = _fieldsTree?.LookupFor<Int64LookupKey>(field.FieldName);
        if (set == null)
            return TermMatch.CreateEmpty(this, _transaction.Allocator);

        ITermsProvider provider = forward
            ? new TermsNumericRangeProvider<Lookup<Int64LookupKey>.ForwardIterator, TLow, THigh, Int64LookupKey>(this, set, field, low, high)
            : (ITermsProvider)new TermsNumericRangeProvider<Lookup<Int64LookupKey>.BackwardIterator, TLow, THigh, Int64LookupKey>(this, set, field, low, high);

        return new TermsProviderMatch(provider, _transaction.LowLevelTransaction, _transaction.Allocator);
    }

    private IQueryMatch RangeBuilder<TLow, THigh>(FieldMetadata field, double low, double high, bool forward, CancellationToken token)
        where TLow : struct, Range.Marker
        where THigh : struct, Range.Marker
    {
        field = field.GetNumericFieldMetadata<double>(Allocator);
        var set = _fieldsTree?.LookupFor<DoubleLookupKey>(field.FieldName);
        if (set == null)
            return TermMatch.CreateEmpty(this, _transaction.Allocator);

        ITermsProvider provider = forward
            ? new TermsNumericRangeProvider<Lookup<DoubleLookupKey>.ForwardIterator, TLow, THigh, DoubleLookupKey>(this, set, field, low, high)
            : (ITermsProvider)new TermsNumericRangeProvider<Lookup<DoubleLookupKey>.BackwardIterator, TLow, THigh, DoubleLookupKey>(this, set, field, low, high);

        return new TermsProviderMatch(provider, _transaction.LowLevelTransaction, _transaction.Allocator);
    }
}
