using System;
using System.Diagnostics;
using System.Threading;
using Corax.Mappings;
using Corax.Querying.Matches;
using Corax.Querying.Matches.Meta;
using Corax.Querying.Matches.TermsProviders;
using Voron;
using Voron.Data.CompactTrees;
using Voron.Data.Lookups;
using static Voron.Data.CompactTrees.CompactTree;

namespace Corax.Querying;

public partial class IndexSearcher
{
    private IQueryMatch TermsProviderMatchBuilder<TTermsProvider>(in FieldMetadata field, Slice term, bool validatePostfixLen = false, in CancellationToken token = default)
        where TTermsProvider : struct, ITermsProvider
    {
        var terms = _fieldsTree?.CompactTreeFor(field.FieldName);
        if (terms == null)
            return TermMatch.CreateEmpty(this, _transaction.Allocator);

        CompactKey termKey;
        if (term.Size != 0)
        {
            termKey = _fieldsTree.Llt.AcquireCompactKey();
            termKey.Set(term.AsReadOnlySpan());
            termKey.ChangeDictionary(terms.DictionaryId);
        }
        else
        {
            termKey = null;
        }

        ITermsProvider provider = GetMultiTermMatchProvider<TTermsProvider>(field, terms, termKey, seekTerm: null, validatePostfixLen, token);
        return new TermsProviderMatch(provider, _transaction.LowLevelTransaction, _transaction.Allocator);
    }

    private IQueryMatch TermsProviderMatchBuilder<TTermsProvider>(in FieldMetadata field, string term, CancellationToken token)
        where TTermsProvider : struct, ITermsProvider
    {
        var terms = _fieldsTree?.CompactTreeFor(field.FieldName);
        if (terms == null)
            return TermMatch.CreateEmpty(this, _transaction.Allocator);

        var slicedTerm = EncodeAndApplyAnalyzer(field, term);
        var termKey = _fieldsTree.Llt.AcquireCompactKey();
        termKey.Set(slicedTerm.AsReadOnlySpan());

        ITermsProvider provider = GetMultiTermMatchProvider<TTermsProvider>(field, terms, termKey, seekTerm: null, validatePostfixLen: false, token: token);
        return new TermsProviderMatch(provider, _transaction.LowLevelTransaction, _transaction.Allocator);
    }

    private TTermsProvider GetMultiTermMatchProvider<TTermsProvider>(in FieldMetadata field, CompactTree termTree, CompactKey term, CompactKey seekTerm, bool validatePostfixLen, CancellationToken token)
        where TTermsProvider : struct, ITermsProvider
    {
        if (typeof(TTermsProvider) == typeof(StartsWithTermsProvider<Lookup<CompactKeyLookup>.ForwardIterator>))
            return (TTermsProvider)(object)new StartsWithTermsProvider<Lookup<CompactKeyLookup>.ForwardIterator>(this, termTree, field, term, seekTerm, validatePostfixLen, token);

        if (typeof(TTermsProvider) == typeof(StartsWithTermsProvider<Lookup<CompactKeyLookup>.BackwardIterator>))
            return (TTermsProvider)(object)new StartsWithTermsProvider<Lookup<CompactKeyLookup>.BackwardIterator>(this, termTree, field, term, seekTerm, validatePostfixLen, token);

        if (typeof(TTermsProvider) == typeof(NotStartsWithTermsProvider<Lookup<CompactKeyLookup>.ForwardIterator>))
            return (TTermsProvider)(object)new NotStartsWithTermsProvider<Lookup<CompactKeyLookup>.ForwardIterator>(this, termTree, field, term, validatePostfixLen, token);

        if (typeof(TTermsProvider) == typeof(NotStartsWithTermsProvider<Lookup<CompactKeyLookup>.BackwardIterator>))
            return (TTermsProvider)(object)new NotStartsWithTermsProvider<Lookup<CompactKeyLookup>.BackwardIterator>(this, termTree, field, term, validatePostfixLen, token);

        Debug.Assert(validatePostfixLen == false, "Not supported for the rest of this");

        if (typeof(TTermsProvider) == typeof(EndsWithTermsProvider<Lookup<CompactKeyLookup>.ForwardIterator>))
            return (TTermsProvider)(object)new EndsWithTermsProvider<Lookup<CompactKeyLookup>.ForwardIterator>(this, termTree, field, term);

        if (typeof(TTermsProvider) == typeof(EndsWithTermsProvider<Lookup<CompactKeyLookup>.BackwardIterator>))
            return (TTermsProvider)(object)new EndsWithTermsProvider<Lookup<CompactKeyLookup>.BackwardIterator>(this, termTree, field, term);

        if (typeof(TTermsProvider) == typeof(NotEndsWithTermsProvider<Lookup<CompactKeyLookup>.ForwardIterator>))
            return (TTermsProvider)(object)new NotEndsWithTermsProvider<Lookup<CompactKeyLookup>.ForwardIterator>(this, termTree, field, term);

        if (typeof(TTermsProvider) == typeof(NotEndsWithTermsProvider<Lookup<CompactKeyLookup>.BackwardIterator>))
            return (TTermsProvider)(object)new NotEndsWithTermsProvider<Lookup<CompactKeyLookup>.BackwardIterator>(this, termTree, field, term);

        if (typeof(TTermsProvider) == typeof(ContainsTermsProvider<Lookup<CompactKeyLookup>.ForwardIterator>))
            return (TTermsProvider)(object)new ContainsTermsProvider<Lookup<CompactKeyLookup>.ForwardIterator>(this, termTree, field, term);

        if (typeof(TTermsProvider) == typeof(ContainsTermsProvider<Lookup<CompactKeyLookup>.BackwardIterator>))
            return (TTermsProvider)(object)new ContainsTermsProvider<Lookup<CompactKeyLookup>.BackwardIterator>(this, termTree, field, term);

        if (typeof(TTermsProvider) == typeof(NotContainsTermsProvider<Lookup<CompactKeyLookup>.ForwardIterator>))
            return (TTermsProvider)(object)new NotContainsTermsProvider<Lookup<CompactKeyLookup>.ForwardIterator>(this, termTree, field, term);

        if (typeof(TTermsProvider) == typeof(NotContainsTermsProvider<Lookup<CompactKeyLookup>.BackwardIterator>))
            return (TTermsProvider)(object)new NotContainsTermsProvider<Lookup<CompactKeyLookup>.BackwardIterator>(this, termTree, field, term);

        if (typeof(TTermsProvider) == typeof(ExistsTermsProvider<Lookup<CompactKeyLookup>.ForwardIterator>))
            return (TTermsProvider)(object)new ExistsTermsProvider<Lookup<CompactKeyLookup>.ForwardIterator>(this, termTree, field);

        if (typeof(TTermsProvider) == typeof(ExistsTermsProvider<Lookup<CompactKeyLookup>.BackwardIterator>))
            return (TTermsProvider)(object)new ExistsTermsProvider<Lookup<CompactKeyLookup>.BackwardIterator>(this, termTree, field);

        throw new NotSupportedException($"{nameof(TTermsProvider)}: {typeof(TTermsProvider)} is not supported. ");
    }
}
