using Raven.Server.ServerWide.Context;
using Tryouts.Corax.Queries;

namespace Tryouts.Corax
{
    public class XorQuery : Query
    {
        private readonly Query _left, _right;

        public XorQuery(TransactionOperationContext context, IndexReader reader, Query left, Query right) : base(context, reader)
        {
            _left = left;
            _right = right;
        }


        public override void Run(out PackedBitmapReader results)
        {
            _left.Run(out var leftResults);
            try
            {
                _right.Run(out var rightResults);
                try
                {
                    PackedBitmapReader.Xor(Context, ref leftResults, ref rightResults, out results);
                }
                finally
                {
                    rightResults.Dispose();
                }
            }
            finally
            {
                leftResults.Dispose();
            }
        }
    }
}
