using System;
using System.Buffers;
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

        // Decode each term's UTF-8 bytes into a reusable pooled char buffer and match the regex against the
        // span directly, instead of allocating a fresh string per term via Encoding.UTF8.GetString. The buffer
        // is rented once and grown only if a later term needs more room (see ToChars), then returned at the end.
        char[] buffer = null;
        try
        {
            while (count < postingListIds.Length)
            {
                if (_iterator.MoveNext(compactKey, out long postingListId, out _) == false)
                    break;

                var key = compactKey.Decoded();
                if (_regex.IsMatch(ToChars(key, ref buffer)) == false)
                    continue;

                postingListIds[count++] = postingListId;
            }
        }
        finally
        {
            if (buffer != null)
                ArrayPool<char>.Shared.Return(buffer);
        }

        return count;
    }

    // Decode the term's UTF-8 bytes into a pooled char buffer (no per-term string allocation) and return the
    // written slice. The buffer is sized with GetMaxCharCount, an O(1) upper bound on the char count, rather
    // than GetCharCount, which would re-scan every term's bytes just to size the buffer.
    private static ReadOnlySpan<char> ToChars(ReadOnlySpan<byte> key, ref char[] buffer)
    {
        int maxChars = Encoding.UTF8.GetMaxCharCount(key.Length);
        if (buffer == null || buffer.Length < maxChars)
        {
            if (buffer != null)
                ArrayPool<char>.Shared.Return(buffer);
            buffer = ArrayPool<char>.Shared.Rent(maxChars);
        }

        int written = Encoding.UTF8.GetChars(key, buffer);
        return buffer.AsSpan(0, written);
    }

    public void Reset()
    {
        _iterator = _tree.Iterate<TLookupIterator>();
        _iterator.Reset();
    }

    public bool Next(out TermMatch term)
    {
        // Same allocation-free decode as FillPostingListIds. There is no Dispose hook on ITermsProvider and Next
        // yields one term per call, so the buffer is rented/returned within the call - this still spares the
        // per-term string allocation for every non-matching term the loop skips before it finds a hit.
        char[] buffer = null;
        try
        {
            while (_iterator.MoveNext(out var compactKey, out _, out _))
            {
                var key = compactKey.Decoded();
                if (_regex.IsMatch(ToChars(key, ref buffer)) == false)
                    continue;

                term = _searcher.TermQuery(_field, compactKey, _tree);
                return true;
            }
        }
        finally
        {
            if (buffer != null)
                ArrayPool<char>.Shared.Return(buffer);
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
