using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading;
using Corax.Mappings;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.TermProviders;
using Voron;
using Voron.Data.Lookups;
using static Voron.Data.CompactTrees.CompactTree;

namespace Corax.Querying;

public partial class IndexSearcher
{
    /// <summary>
    /// Test API only
    /// </summary>
    public IQueryMatch StartWithQuery(string field, string startWith, bool isNegated = false, bool hasBoost = false, bool forward = true) => StartWithQuery(FieldMetadataBuilder(field, hasBoost: hasBoost), EncodeAndApplyAnalyzer(default, startWith), isNegated, forward);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IQueryMatch StartWithQuery(in FieldMetadata field, string startWith, bool isNegated = false, bool forward = true, bool streamingEnabled = false, in CancellationToken token = default)
    {
        return (forward, isNegated) switch
        {
            (true, false) => TermProviderMatchBuilder<StartsWithTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>>(field, startWith, streamingEnabled, token),
            (false, false) => TermProviderMatchBuilder<StartsWithTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>>(field, startWith, streamingEnabled, token),
            (true, true) => TermProviderMatchBuilder<NotStartsWithTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>>(field, startWith, streamingEnabled, token),
            (false, true) => TermProviderMatchBuilder<NotStartsWithTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>>(field, startWith, streamingEnabled, token)
        };
    }

    public IQueryMatch StartWithQuery(in FieldMetadata field, Slice startWith, bool isNegated = false, bool forward = true, bool streamingEnabled = false, bool validatePostfixLen = false, in CancellationToken token = default)
    {
        return (forward, isNegated) switch
        {
            (true, false) => TermProviderMatchBuilder<StartsWithTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>>(field, startWith, streamingEnabled, validatePostfixLen, token),
            (false, false) => TermProviderMatchBuilder<StartsWithTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>>(field, startWith, streamingEnabled, validatePostfixLen, token),
            (true, true) => TermProviderMatchBuilder<NotStartsWithTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>>(field, startWith, streamingEnabled, validatePostfixLen, token),
            (false, true) => TermProviderMatchBuilder<NotStartsWithTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>>(field, startWith, streamingEnabled, validatePostfixLen, token)
        };
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IQueryMatch EndsWithQuery(in FieldMetadata field, string endsWith, bool isNegated = false, bool forward = true, bool streamingEnabled = false, in CancellationToken token = default)
    {
        return (forward, isNegated) switch
        {
            (true, false) => TermProviderMatchBuilder<EndsWithTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>>(field, endsWith, streamingEnabled, token),
            (false, false) => TermProviderMatchBuilder<EndsWithTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>>(field, endsWith, streamingEnabled, token),
            (true, true) => TermProviderMatchBuilder<NotEndsWithTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>>(field, endsWith, streamingEnabled, token),
            (false, true) => TermProviderMatchBuilder<NotEndsWithTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>>(field, endsWith, streamingEnabled, token)
        };
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IQueryMatch EndsWithQuery(in FieldMetadata field, Slice endsWith, bool isNegated = false, bool forward = true, bool streamingEnabled = false, in CancellationToken token = default)
    {
        return (forward, isNegated) switch
        {
            (true, false) => TermProviderMatchBuilder<EndsWithTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>>(field, endsWith, streamingEnabled, validatePostfixLen: false, token: token),
            (false, false) => TermProviderMatchBuilder<EndsWithTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>>(field, endsWith, streamingEnabled, validatePostfixLen: false, token: token),
            (true, true) => TermProviderMatchBuilder<NotEndsWithTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>>(field, endsWith, streamingEnabled, validatePostfixLen: false, token: token),
            (false, true) => TermProviderMatchBuilder<NotEndsWithTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>>(field, endsWith, streamingEnabled, validatePostfixLen: false, token: token)
        };
    }

    public IQueryMatch ContainsQuery(in FieldMetadata field, string containsTerm, bool isNegated = false, bool forward = true, in CancellationToken token = default) => ContainsQuery(field, (Slice)EncodeAndApplyAnalyzer(field, containsTerm), isNegated, forward, token);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IQueryMatch ContainsQuery(in FieldMetadata field, Slice containsTerm, bool isNegated = false, bool forward = true, in CancellationToken token = default)
    {
        return (forward, isNegated) switch
        {
            (true, false) => TermProviderMatchBuilder<ContainsTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>>(field, containsTerm, token: token),
            (false, false) => TermProviderMatchBuilder<ContainsTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>>(field, containsTerm, token: token),
            (true, true) => TermProviderMatchBuilder<NotContainsTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>>(field, containsTerm, token: token),
            (false, true) => TermProviderMatchBuilder<NotContainsTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>>(field, containsTerm, token: token)
        };
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public IQueryMatch ExistsQuery(in FieldMetadata field, bool forward = true, bool streamingEnabled = false, in CancellationToken token = default)
    {
        return forward
            ? TermProviderMatchBuilder<ExistsTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>>(field, default(Slice), streamingEnabled: streamingEnabled, token: token)
            : TermProviderMatchBuilder<ExistsTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>>(field, default(Slice), streamingEnabled: streamingEnabled, token: token);
    }

    public IQueryMatch RegexQuery(in FieldMetadata field, Regex regex, bool forward = true, bool streamingEnabled = false, in CancellationToken token = default)
    {
        var terms = _fieldsTree?.CompactTreeFor(field.FieldName);
        if (terms == null)
            return TermMatch.CreateEmpty(this, _transaction.Allocator);

        ITermProvider provider = forward
            ? new RegexTermProvider<Lookup<CompactKeyLookup>.ForwardIterator>(this, terms, field, regex)
            : (ITermProvider)new RegexTermProvider<Lookup<CompactKeyLookup>.BackwardIterator>(this, terms, field, regex);

        return new TermProviderMatch(provider, _transaction.LowLevelTransaction, _transaction.Allocator);
    }

    /// <summary>
    /// Creates an IN query match for a set of string terms on the given field.
    /// Equivalent to: WHERE field IN (terms[0], terms[1], ...).
    /// </summary>
    public IQueryMatch InQuery(string fieldName, List<string> terms)
    {
        FieldMetadata field = FieldMetadataBuilder(fieldName);
        return InQuery(field, terms);
    }

    public IQueryMatch InQuery(in FieldMetadata field, List<string> terms)
    {
        ITermProvider provider = new InTermProvider<string>(this, field, terms);
        return new TermProviderMatch(provider, _transaction.LowLevelTransaction, _transaction.Allocator);
    }
}
