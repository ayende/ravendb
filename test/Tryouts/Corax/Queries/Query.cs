using Raven.Server.ServerWide.Context;

namespace Tryouts.Corax.Queries
{
    public abstract class Query
    {
        public readonly TransactionOperationContext Context;
        protected readonly IndexReader Reader;

        protected Query(IndexReader reader)
        {
            Context = reader.Context;
            Reader = reader;
        }

        public abstract void Run(out PackedBitmapReader results);

    }
}
