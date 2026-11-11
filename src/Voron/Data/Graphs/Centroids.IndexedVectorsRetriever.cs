using System;
using Voron.Data.Lookups;
using Voron.Impl;

namespace Voron.Data.Graphs;

public partial class Centroids
{
    public class IndexedVectorsRetriever : IIndexedTermsRetriever
    {
        private readonly LowLevelTransaction _llt;
        private readonly Options _options;
        private readonly bool _isEmpty;
        private Lookup<Int64LookupKey>.ForwardIterator _iterator;

        public IndexedVectorsRetriever(LowLevelTransaction llt, string name)
        {
            _llt = llt;
            using var _ = Slice.From(llt.Allocator, name, out var slice);
            if (TryReadOptions(llt, slice, out _options, out var tree) == false)
            {
                _isEmpty = true;
                return;
            }

            _iterator = tree.LookupFor<Int64LookupKey>(NodePostingsSlice).Iterate();
            _iterator.Seek<Int64LookupKey>(long.MinValue);
        }

        public bool GetNextTerm(out ReadOnlySpan<byte> term)
        {
            if (_isEmpty || _iterator.MoveNext(out Int64LookupKey vectorId, out _, out _) == false)
            {
                term = [];
                return false;
            }

            term = Hnsw.ReadVector(_llt, vectorId.Value, _options.VectorSizeBytes).ToReadOnlySpan();
            return true;
        }

        public ConvertTo Type => ConvertTo.Base64;

        public void Dispose()
        {
        }
    }
}
