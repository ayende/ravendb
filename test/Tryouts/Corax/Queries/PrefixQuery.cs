using System;
using Ewah;
using Raven.Server.ServerWide.Context;
using Voron;
using Voron.Data.PostingList;

namespace Tryouts.Corax.Queries
{
    public class PrefixQuery : Query
    {
        public readonly string Field, Prefix;

        public PrefixQuery(TransactionOperationContext context, IndexReader reader, string field, string prefix) : base(context, reader)
        {
            Field = field;
            Prefix = prefix;
        }

        public override EwahCompressedBitArray Run()
        {
            var bitmap = new EwahCompressedBitArray();
//            var table = Context.Transaction.InnerTransaction.OpenTable(PostingList.PostingListSchema, Field);
//            if (table == null)
                return bitmap;

            
//            using (Slice.From(Context.Allocator, "S:" + Prefix, out var prefixSlice))
//            {
//                foreach (var item in table.SeekByPrimaryKeyPrefix(prefixSlice, Slices.Empty, 0))
//                {
//                    
//                }
//                
//            }
        }
    }
}
