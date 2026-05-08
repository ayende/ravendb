using System;
using System.Collections.Generic;
using Corax.Mappings;
using Corax.Querying.Matches.Meta;
using Voron.Data.CompactTrees;
using Voron.Data.Lookups;

namespace Corax.Querying.Matches.TermsProviders
{
    public struct EndsWithTermsProvider<TLookupIterator> : ITermsProvider
        where TLookupIterator : struct, ILookupIterator
    {
        private readonly CompactTree _tree;
        private readonly IndexSearcher _searcher;
        private readonly FieldMetadata _field;

        private readonly CompactKey _endsWith;

        private CompactTree.Iterator<TLookupIterator> _iterator;

        public EndsWithTermsProvider(IndexSearcher searcher, CompactTree tree, in FieldMetadata field, CompactKey endsWith)
        {
            _tree = tree;
            _searcher = searcher;
            _field = field;
            _iterator = tree.Iterate<TLookupIterator>();
            _iterator.Reset();
            _endsWith = endsWith;
        }

        public bool IsFillSupported => false;

        public int Fill(Span<long> containers)
        {
            throw new NotImplementedException();
        }

        public int FillPostingListIds(Span<long> postingListIds)
        {
            var suffix = _endsWith.Decoded();
            int count = 0;

            using var scope = new CompactKeyCacheScope(_searcher.Transaction.LowLevelTransaction);
            var key = scope.Key;

            while (count < postingListIds.Length)
            {
                if (!_iterator.MoveNext(key, out long postingListId, out _))
                    break;

                if (!key.Decoded().EndsWith(suffix))
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
            var suffix = _endsWith.Decoded();
            while (_iterator.MoveNext(out var key, out _, out _))
            {
                var termSlice = key.Decoded();
                if (!termSlice.EndsWith(suffix))
                {
                    continue;
                }

                term = _searcher.TermQuery(_field, key, _tree);
                return true;
            }

            term = TermMatch.CreateEmpty(_searcher, _searcher.Allocator);
            return false;
        }

        public QueryInspectionNode Inspect()
        {
            return new QueryInspectionNode($"{nameof(EndsWithTermsProvider<TLookupIterator>)}",
                parameters: new Dictionary<string, string>
                {
                    { Constants.QueryInspectionNode.FieldName, _field.ToString() },
                    { Constants.QueryInspectionNode.Suffix, _endsWith.ToString()}
                });
        }
    }
}
