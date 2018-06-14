using System;
using Raven.Server.ServerWide.Context;

namespace Tryouts.Corax.Queries
{
    public abstract class Query
    {
        public readonly TransactionOperationContext Context;
        protected readonly IndexReader Reader;

        protected Query(IndexReader reader)
        {
            if(reader.Context?.Transaction == null)
                ThrowNoActiveTransaction();
            Context = reader.Context;
            Reader = reader;
        }

        private static void ThrowNoActiveTransaction()
        {
            throw new ArgumentException("Cannot initialize query outside of BeginReading() scope.");
        }

        public abstract void Run(out PackedBitmapReader results);

    }
}
