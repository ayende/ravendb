using Ewah;
using Raven.Server.ServerWide.Context;
using Tryouts.Corax.Queries;

namespace Tryouts.Corax
{
    public class AndQuery : Query
    {
        private readonly Query _left, _right;

        public AndQuery(TransactionOperationContext context, IndexReader reader, Query left, Query right) : base(context, reader)
        {
            _left = left;
            _right = right;
        }

        public override EwahCompressedBitArray Run()
        {
            return _left.Run().And(_right.Run());
        }
    }
}
