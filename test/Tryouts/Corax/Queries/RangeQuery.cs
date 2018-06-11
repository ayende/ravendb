using System;
using Sparrow;
using Tryouts.Corax.Bitmaps;
using Voron;
using Voron.Data.PostingList;

namespace Tryouts.Corax.Queries
{
    public class RangeQuery : Query
    {
        public readonly string Field, Min, Max;

        public RangeQuery(IndexReader reader, string field, string min, string max) : base(reader)
        {
            Field = field;
            Min = min;
            Max = max;
        }

        protected unsafe ByteStringContext<ByteStringMemoryCache>.InternalScope TermSlice(string value, out Slice slice)
        {
            using (Slice.From(Context.Allocator, value, out var buffer))
            {
                ByteStringContext<ByteStringMemoryCache>.InternalScope scope = Context.Allocator.Allocate(buffer.Size + (sizeof(byte) * 2), out ByteString prefixBuffer);
                prefixBuffer.Ptr[0] = (byte)'M'; // metrics for this term
                prefixBuffer.Ptr[1] = (byte)':';
                buffer.CopyTo(prefixBuffer.Ptr + 2);

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

                using (TermSlice(Min, out var minSlice))
                using (TermSlice(Max, out var maxSlice))
                using (var builder = new PackedBitmapMultiSequenceBuilder(Context))
                {
                    var plr = new PostingListReader(Context.Transaction.InnerTransaction, fieldSlice);
                    foreach (var item in table.SeekByPrimaryKeyForKeyOnly(minSlice, Slices.Empty))
                    {
                        var size = Math.Min(item.Size, maxSlice.Size);
                        var cmp = Memory.Compare(item.Content.Ptr, maxSlice.Content.Ptr, size);

                        if (cmp > 0 || cmp == 0 && maxSlice.Size <= item.Size)
                            break;// too big, done

                        using (Context.Allocator.Allocate(item.Size - 2, out var keyBuffer))
                        {
                            Memory.Copy(keyBuffer.Ptr, item.Content.Ptr + 2, item.Size - 2);
                            plr.Reset(new Slice(keyBuffer));
                            while (plr.ReadNext(out var v))
                            {
                                builder.Set((ulong)v);
                            }
                        }
                    }
                    builder.Complete(out results);
                }
            }
        }
    }
}
