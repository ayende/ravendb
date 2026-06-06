using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;
using Corax.Mappings;
using Corax.Querying.Matches.Meta;
using Voron.Data.CompactTrees;
using Voron.Data.Lookups;

namespace Corax.Querying.Matches.TermsProviders;

public struct RegexTermsProvider<TLookupIterator> : ITermsProvider
    where TLookupIterator : struct, ILookupIterator
{
    private readonly CompactTree _tree;
    private readonly Querying.IndexSearcher _searcher;
    private readonly FieldMetadata _field;
    private readonly Regex _regex;

    private CompactTree.Iterator<TLookupIterator> _iterator;

    public RegexTermsProvider(Querying.IndexSearcher searcher, CompactTree tree, in FieldMetadata field, Regex regex)
    {
        _searcher = searcher;
        _regex = regex;
        _tree = tree;
        _iterator = tree.Iterate<TLookupIterator>();
        _iterator.Reset();
        _field = field;
    }

    public int FillPostingListIds(Span<long> postingListIds)
    {
        int count = 0;

        using var scope = new CompactKeyCacheScope(_searcher.Transaction.LowLevelTransaction);
        var compactKey = scope.Key;

        while (count < postingListIds.Length)
        {
            if (_iterator.MoveNext(compactKey, out long postingListId, out _) == false)
                break;

            var key = compactKey.Decoded();
            if (_regex.IsMatch(Encoding.UTF8.GetString(key)) == false)
                continue;

            postingListIds[count++] = postingListId;
        }

        return count;
    }

    public void Reset()
    {
        _iterator = _tree.Iterate<TLookupIterator>();
        _iterator.Reset();
    }

    public bool Next(out TermMatch term)
    {
        while (_iterator.MoveNext(out var compactKey, out _, out _))
        {
            var key = compactKey.Decoded();
            if (_regex.IsMatch(Encoding.UTF8.GetString(key)) == false)
                continue;

            term = _searcher.TermQuery(_field, compactKey, _tree);
            return true;
        }

        term = TermMatch.CreateEmpty(_searcher, _searcher.Allocator);
        return false;
    }

    public QueryInspectionNode Inspect()
    {
        return new QueryInspectionNode($"{nameof(RegexTermsProvider<TLookupIterator>)}",
            parameters: new Dictionary<string, string>()
            {
                { Constants.QueryInspectionNode.FieldName, _field.FieldName.ToString() },
                { Constants.QueryInspectionNode.Term, _regex.ToString()}
            });
    }
}
