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
    private IQueryMatch TermsProviderMatchBuilder<TTermsProvider>(in FieldMetadata field, Slice term, bool streamingEnabled = false, bool validatePostfixLen = false, in CancellationToken token = default)
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

        CompactKey seekKey = null;
        if (TryRewriteTermWhenPerformingBackwardStreaming<TTermsProvider>(streamingEnabled, term, out var seekTerm))
        {
            seekKey = _fieldsTree.Llt.AcquireCompactKey();
            seekKey.Set(seekTerm.AsReadOnlySpan());
            seekKey.ChangeDictionary(terms.DictionaryId);
        }

        ITermsProvider provider = GetMultiTermMatchProvider<TTermsProvider>(field, terms, termKey, seekKey, validatePostfixLen, token);
        return new TermsProviderMatch(provider, _transaction.LowLevelTransaction, _transaction.Allocator);
    }

    private IQueryMatch TermsProviderMatchBuilder<TTermsProvider>(in FieldMetadata field, string term, bool streamingEnabled, CancellationToken token)
        where TTermsProvider : struct, ITermsProvider
    {
        var terms = _fieldsTree?.CompactTreeFor(field.FieldName);
        if (terms == null)
            return TermMatch.CreateEmpty(this, _transaction.Allocator);

        var slicedTerm = EncodeAndApplyAnalyzer(field, term);
        var termKey = _fieldsTree.Llt.AcquireCompactKey();
        termKey.Set(slicedTerm.AsReadOnlySpan());

        CompactKey seekKey = null;
        if (TryRewriteTermWhenPerformingBackwardStreaming<TTermsProvider>(streamingEnabled, slicedTerm, out var seekTerm))
        {
            seekKey = _fieldsTree.Llt.AcquireCompactKey();
            seekKey.Set(seekTerm.AsReadOnlySpan());
        }

        ITermsProvider provider = GetMultiTermMatchProvider<TTermsProvider>(field, terms, termKey, seekKey, validatePostfixLen: false, token: token);
        return new TermsProviderMatch(provider, _transaction.LowLevelTransaction, _transaction.Allocator);
    }

    private bool TryRewriteTermWhenPerformingBackwardStreaming<TTermsProvider>(bool streamingEnabled, Slice termSlice, out Slice termForSeek)
        where TTermsProvider : struct, ITermsProvider
    {
        var shouldRewrite = typeof(TTermsProvider) == typeof(StartsWithTermsProvider<Lookup<CompactKeyLookup>.BackwardIterator>);

        if (streamingEnabled == false || shouldRewrite == false || termSlice.Size == 0)
        {
            termForSeek = default;
            return false;
        }

        var originalTerm = termSlice.AsSpan();

        if (originalTerm[^1] < byte.MaxValue)
        {
            Slice.From(Allocator, termSlice.AsSpan(), out termForSeek);
            //When we have eg startsWith("ab") we have to seek into "ac"
            termForSeek.AsSpan()[^1]++;
            return true;
        }

        if (originalTerm.Length >= 2)
        {
            //Lets scan
            int idX = originalTerm.Length - 2;
            for (; idX >= 0; idX--)
            {
                if (originalTerm[idX] < byte.MaxValue)
                    break;
            }

            if (idX == 0 && originalTerm[idX] == byte.MaxValue)
                goto AfterAllKeys;

            using (Slice.From(Allocator, originalTerm, out Slice temporarySlice))
            {
                temporarySlice[idX]++;
                temporarySlice[idX + 1] = 1;

                //We accept leaking here since it's will be released after query execution.
                Slice.From(Allocator, temporarySlice.AsSpan().Slice(idX + 1), out termForSeek);
                return true;
            }
        }

        AfterAllKeys:
        //Super rare case when we have prefix [255][255] prefix that means we can go to the end of tree, isn't?
        //[255] chain, we can go to the end of the tree then ;-)
        termForSeek = Slices.AfterAllKeys;
        return true;
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
