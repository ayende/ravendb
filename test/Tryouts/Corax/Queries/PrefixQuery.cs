using System;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Voron;
using Voron.Data.PostingList;
using Voron.Data.Tables;

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

        protected unsafe ByteStringContext<ByteStringMemoryCache>.InternalScope TermPrefix(out Slice slice)
        {
            using (Slice.From(Context.Allocator, Prefix, out var prefixSlice))
            {
                ByteStringContext<ByteStringMemoryCache>.InternalScope scope = Context.Allocator.Allocate(prefixSlice.Size + (sizeof(byte) * 2), out ByteString prefixBuffer);
                prefixBuffer.Ptr[0] = (byte)'M'; // metrics for this term
                prefixBuffer.Ptr[1] = (byte)':';
                prefixSlice.CopyTo(prefixBuffer.Ptr + 2);

                slice = new Slice(prefixBuffer);
                return scope;
            }
        }

        public override unsafe void Run(out PackedBitmapReader results)
        {
            using (Slice.From(Context.Allocator, Field, out var fieldSlice))
            {
                var table = Context.Transaction.InnerTransaction.OpenTable(PostingList.PostingListSchema, fieldSlice);
                // because we need to run over individual terms, we run over the metrics, where we have a single
                // entry per term, instear of the possible many posting list blocks.

                using (TermPrefix(out var prefix))
                {
                    results = new PackedBitmapReader(); // empty
                    var plr = new PostingListReader(Context.Transaction.InnerTransaction, fieldSlice);
                    foreach (var item in table.SeekByPrimaryKeyPrefix(prefix, Slices.Empty, 0))
                    {
                        using (Context.Allocator.Allocate(item.Key.Size-2, out var keyBuffer))
                        {
                            Memory.Copy(keyBuffer.Ptr, item.Key.Content.Ptr + 2, item.Key.Size - 2);
                            plr.Reset(new Slice(keyBuffer));
                            using (var builder = new PackedBitmapBuilder(Context))
                            {
                                while (plr.ReadNext(out var v))
                                {
                                    builder.Set((ulong)v);
                                }
                                builder.Complete(out var current);
                                var writerToDispose = results.InnerWriter;
                                try
                                {
                                    PackedBitmapReader.Or(Context, ref results, ref current, out results);
                                }
                                finally
                                {
                                    current.Dispose();
                                    writerToDispose.Dispose();
                                }
                            }

                        }
                    }
                }
            }
        }
    }
}
