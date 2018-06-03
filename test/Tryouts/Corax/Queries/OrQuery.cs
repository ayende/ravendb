using Ewah;
using Raven.Server.ServerWide.Context;

namespace Tryouts.Corax.Queries
{
    public class OrQuery : Query
    {
        private readonly Query _left, _right;

        public OrQuery(TransactionOperationContext context, IndexReader reader, Query left, Query right) : base(context, reader)
        {
            _left = left;
            _right = right;

        }

        public override EwahCompressedBitArray Run()
        {
            return _left.Run().Or(_right.Run());
        }
    }
}
