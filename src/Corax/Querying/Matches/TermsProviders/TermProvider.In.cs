using System;
using System.Collections.Generic;
using System.Diagnostics;
using Corax.Mappings;
using Corax.Querying.Matches.Meta;

namespace Corax.Querying.Matches.TermsProviders
{
    [DebuggerDisplay("{DebugView,nq}")]
    public struct InTermsProvider : ITermsProvider
    {
        private readonly IndexSearcher _searcher;
        private readonly List<string> _terms;
        private int _termIndex;
        private readonly FieldMetadata _field;

        public InTermsProvider(IndexSearcher searcher, in FieldMetadata field, List<string> terms)
        {
            _field = field;
            _searcher = searcher;
            _terms = terms;
            _termIndex = -1;
        }

        public int FillPostingListIds(Span<long> postingListIds)
        {
            int count = 0;

            while (count < postingListIds.Length && _termIndex + 1 < _terms.Count)
            {
                _termIndex++;

                long containerId = _searcher.GetTermPostingListId(_field, _terms[_termIndex]);

                if (containerId != -1)
                    postingListIds[count++] = containerId;
            }

            return count;
        }

        public void Reset() => _termIndex = -1;

        public bool Next(out TermMatch term)
        {
            _termIndex++;
            if (_termIndex >= _terms.Count)
            {
                term = TermMatch.CreateEmpty(_searcher, _searcher.Allocator);
                return false;
            }

            term = _searcher.TermQuery(_field, _terms[_termIndex]);
            return true;
        }

        public QueryInspectionNode Inspect()
        {
            return new QueryInspectionNode(nameof(InTermsProvider),
                            parameters: new Dictionary<string, string>()
                            {
                                { Constants.QueryInspectionNode.FieldName, _field.FieldName.ToString() },
                                { Constants.QueryInspectionNode.Term, string.Join(",", _terms)}
                            });
        }

        string DebugView => Inspect().ToString();
    }
}
