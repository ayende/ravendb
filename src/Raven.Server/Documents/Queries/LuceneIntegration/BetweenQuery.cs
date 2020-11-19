using System;
using System.Buffers;
using System.Collections.Generic;
using Lucene.Net.Index;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Lucene.Net.Util;

namespace Raven.Server.Documents.Queries.LuceneIntegration
{
    public class BetweenQuery<T> : Query
    {
        public readonly string Field;
        public readonly T Start, End;
        public readonly bool StartInclusive, EndInclusive;

        public BetweenQuery(string field, T start,  bool startInclusive, T end,bool endInclusive)
        {
            Field = field;
            Start = start;
            End = end;
            StartInclusive = startInclusive;
            EndInclusive = endInclusive;
        }

        public override Weight CreateWeight(Searcher searcher, IState state)
        {
            return new BetweenQueryWeight(this, searcher);
        }

        public override string ToString(string field)
        {
            return Field + " between " +  (StartInclusive== false ? "after " : "" ) + Start + " and " + (EndInclusive ? "before " : "") + End;
        }

        private class BetweenQueryWeight : Weight
        {
            private readonly BetweenQuery<T> _parent;
            private readonly Searcher _searcher;
            private float _queryWeight = 1.0f;

            public BetweenQueryWeight(BetweenQuery<T> parent, Searcher searcher)
            {
                _parent = parent;
                _searcher = searcher;
            }
            
            public override void Normalize(float norm)
            {
                _queryWeight *= norm;
            }

            public override Scorer Scorer(IndexReader reader, bool scoreDocsInOrder, bool topScorer, IState state)
            {
                Similarity similarity = _parent.GetSimilarity(_searcher);

                var aggregator = new TermsAggregator(_parent, reader, state);

                if (typeof(T) == typeof(long))
                {
                    NumericUtils.SplitLongRange(aggregator, 4, (long)(object)_parent.Start, (long)(object)_parent.End);
                }
                else
                {
                    aggregator.AddRange(_parent.Start.ToString(), _parent.End.ToString());
                }

                return new EagerBetweenScorer(_parent, aggregator.TermsInRange, reader, similarity, state);
            }

            public override Lucene.Net.Search.Explanation Explain(IndexReader reader, int doc, IState state)
            {
                var result = new ComplexExplanation {Description = _parent.ToString(), Value = _queryWeight};
                result.AddDetail(new Lucene.Net.Search.Explanation(_queryWeight, "queryWeight"));
                return result;
            }

            public override float GetSumOfSquaredWeights()
            {
                return _queryWeight * _queryWeight;
            }
            
            
            public override Query Query => _parent;
            public override float Value => _queryWeight;
        }

        class TermsAggregator : NumericUtils.LongRangeBuilder
        {
            private readonly BetweenQuery<T> _parent;
            private readonly IndexReader _reader;
            private readonly IState _state;

            public TermsAggregator(BetweenQuery<T> parent, IndexReader reader, IState state)
            {
                _parent = parent;
                _reader = reader;
                _state = state;
            }

            public readonly HashSet<string> TermsInRange = new HashSet<string>();

            public override void AddRange( string min, string max)
            {
                Console.WriteLine($"{min} -- > { max}");
                var terms = _reader.Terms(new Term(_parent.Field, min), _state);
                if (terms.Term == null || terms.Term.Field != _parent.Field)
                {
                    terms.Dispose();
                    return;
                }

                if (string.CompareOrdinal(terms.Term.Text, min) == 0 && _parent.StartInclusive == false)
                {
                    if (terms.Next(_state) == false)
                        return;
                }

                do
                {
                    TermsInRange.Add(terms.Term.Text);

                    if (terms.Next(_state) == false || terms.Term.Field != _parent.Field)
                        return;

                    if (max != null)
                    {
                        int compareOrdinal = string.CompareOrdinal(max, terms.Term.Text);
                        if (compareOrdinal > 0)
                            return;
                        if (compareOrdinal == 0 && _parent.EndInclusive == false)
                            return;
                    }
                } while (true);
            }
            
        }


        private  class EagerBetweenScorer : Scorer
        {
            private readonly FastBitArray _docs;
            private IEnumerator<int> _enum;

            public EagerBetweenScorer(BetweenQuery<T> parent, HashSet<string> terms, IndexReader reader, Similarity similarity, IState state) : base(similarity)
            {
                _docs = new FastBitArray(reader.MaxDoc);
                var docs = ArrayPool<int>.Shared.Rent(1024 * 16);
                var freq = ArrayPool<int>.Shared.Rent(1024 * 16);
                try
                {
                    using var termsDocs = reader.TermDocs( state);
                    foreach (string term in terms)
                    {
                        termsDocs.Seek(new Term(parent.Field, term),state);
                        while (true)
                        {
                            int read = termsDocs.Read(docs, freq, state);
                            if (read == 0) break;
                            
                            for (int i = 0; i < read; i++)
                            {
                                _docs.Set(docs[i]);
                            }
                        }
                    }

                }
                finally
                {
                    ArrayPool<int>.Shared.Return(docs);
                    ArrayPool<int>.Shared.Return(freq);
                }
                _enum = _docs.Iterate(0).GetEnumerator();
            }

            public override int DocID()
            {
                return _enum?.Current ?? NO_MORE_DOCS;
            }

            public override int NextDoc(IState state)
            {
                if (_enum?.MoveNext() == true)
                    return _enum.Current;
                _enum?.Dispose();
                _enum = null;
                _docs.Dispose();
                return NO_MORE_DOCS;
            }

            public override int Advance(int target, IState state)
            {
                if (_docs.Disposed) 
                    return NO_MORE_DOCS;
                
                _enum?.Dispose();
                _enum = _docs.Iterate(target).GetEnumerator();
                return NextDoc(state);
            }

            public override float Score(IState state)
            {
                return 1.0f;
            }
        }
    }

    
}
