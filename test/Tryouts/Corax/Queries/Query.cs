using Raven.Server.ServerWide.Context;

namespace Tryouts.Corax.Queries
{
    public abstract class Query
    {
        public readonly TransactionOperationContext Context;
        protected readonly IndexReader Reader;

        public Query(TransactionOperationContext context, IndexReader reader)
        {
            Context = context;
            Reader = reader;
        }

        public abstract void Run(out PackedBitmapReader results);

    }
}
