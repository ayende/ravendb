using Raven.Server.ServerWide.Context;
using Voron.Data.PostingList;

namespace Tryouts.Corax.Queries
{
    public class TermQuery : Query
    {
        public readonly string Field;
        public readonly string Term;
        private readonly PostingListReader _postingListReader;

        public TermQuery(TransactionOperationContext context, IndexReader reader, string field, string term) : base(context, reader)
        {
            Field = field;
            Term = term;
            _postingListReader = PostingListReader.Create(context.Transaction.InnerTransaction, Field, Term);
        }

        public override void Run(out PackedBitmapReader results)
        {
            using (var builder = new PackedBitmapBuilder(Context))
            {
                while (_postingListReader.ReadNext(out var val))
                {
                    builder.Set((ulong)val);
                }
                builder.Complete(out results);
            }
        }
    }
}
