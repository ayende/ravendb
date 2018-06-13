using System;
using System.Diagnostics;
using Sparrow;
using Sparrow.Binary;
using Voron.Data.Tables;
using Voron.Impl;

namespace Voron.Data.PostingList
{
    /// <summary>
    ///     A posting list is a data structure that allow us to hold
    ///     in Voron all the relevant documents matching a particular term.
    ///     The posting list has the following operations:
    ///     * Append a number (must be larger than all previous numbers)
    ///     * Delete a number
    ///     * Iterate over the values in the posting list
    ///     A posting list is implemented using a set of blocks, stored inside
    ///     raw data section, starting from 32 bytes and growing up to 1 KB
    /// </summary>
    public abstract unsafe class PostingList
    {
        protected const int MaximumBufferSize = 1024;

        // The table holds:
        // M:term- -> metrics
        // S:term:start -> raw entries
        public static readonly TableSchema PostingListSchema;
        protected readonly Table Table;
        protected Slice Term;

        public readonly Transaction Tx;
        protected PostingListBuffer Buffer;
        public long NumberOfEntries;

        static PostingList()
        {
            PostingListSchema = new TableSchema()
                .DefineKey(new TableSchema.SchemaIndexDef
                {
                    IsGlobal = false,
                    StartIndex = 0,
                    Count = 1,
                });
        }

        protected PostingList(Transaction tx, Slice field, Slice term)
        {
            Tx = tx;
            Table = Tx.OpenTable(PostingListSchema, field);
            if (Table == null)
                return;


            if (term.Size != 0)
                SetTerm(term);
        }

        protected void SetTerm(Slice term)
        {
            Term = term;
            using (TermStats(out var key))
            {
                if (Table.ReadByKey(key, out var tvr) == false)
                    return;

                NumberOfEntries = *(long*)tvr.Read(1, out int size);
                Debug.Assert(size == sizeof(long));
            }
        }

        protected ByteStringContext<ByteStringMemoryCache>.InternalScope BuildId(long num, out Slice slice)
        {
            ByteStringContext<ByteStringMemoryCache>.InternalScope scope = Tx.Allocator.Allocate(Term.Size + (sizeof(byte) * 3) + sizeof(long),
                out ByteString prefixBuffer);
            prefixBuffer.Ptr[0] = (byte)'S'; // string
            prefixBuffer.Ptr[1] = (byte)':';
            Term.CopyTo(prefixBuffer.Ptr + 2);
            prefixBuffer.Ptr[Term.Size + 2] = (byte)':';
            *(long*)(prefixBuffer.Ptr + sizeof(byte) + sizeof(byte) + Term.Size + sizeof(byte)) = Bits.SwapBytes(num);

            slice = new Slice(prefixBuffer);
            return scope;
        }

        protected ByteStringContext<ByteStringMemoryCache>.InternalScope TermStats(out Slice slice)
        {
            ByteStringContext<ByteStringMemoryCache>.InternalScope scope = Tx.Allocator.Allocate(Term.Size + (sizeof(byte) * 2), out ByteString prefixBuffer);
            prefixBuffer.Ptr[0] = (byte)'M'; // metrics for this term
            prefixBuffer.Ptr[1] = (byte)':';
            Term.CopyTo(prefixBuffer.Ptr + 2);

            slice = new Slice(prefixBuffer);
            return scope;
        }

        protected bool GetIdForBlockFor(long num, bool preferEarlier, out TableValueReader tvr)
        {
            using (BuildId(num, out Slice key))
            using (Slice.External(Tx.Allocator, key.Content.Ptr, key.Content.Length - sizeof(long), out var termPrefix))
            {
                if (Table.ReadByKey(key, out tvr))
                    return true;
                if (preferEarlier && Table.SeekOneBeforePrimaryKeyPrefix(key, termPrefix, out tvr))
                    return true;
                return Table.SeekOnePrimaryKeyPrefix(key, termPrefix, out tvr);
            }
        }

        public (long SizeInBytes, int Blocks) GetSize()
        {
            using (BuildId(0, out Slice key))
            using (Slice.External(Tx.Allocator, key.Content.Ptr, key.Content.Length - sizeof(long), out var termPrefix))
            {
                long sum = 0L;
                int blocks = 0;
                foreach (var item in Table.SeekByPrimaryKeyPrefix(termPrefix, Slices.Empty, 0))
                {
                    sum += item.Key.Content.Length + item.Value.Reader.Size;
                    blocks++;
                }

                return (sum, blocks);
            }
        }
    }
}
