using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices.ComTypes;
using Sparrow.Binary;
using Voron.Data.Tables;
using Voron.Impl;

namespace Voron.Data.PostingList
{
    public unsafe class PostingListWriter : PostingList, IDisposable
    {
        public static PostingListWriter Create(Transaction tx, string field, string term)
        {
            Slice.From(tx.Allocator, field, out var fieldSlice);
            Slice.From(tx.Allocator, term, out var termSlice);
            PostingListSchema.Create(tx, fieldSlice, 2);// we always use the minimum size and grow from there

            return new PostingListWriter(tx, fieldSlice, termSlice);
        }

        private readonly List<long> _deletes = new List<long>();
        
        public PostingListWriter(Transaction tx, Slice field, Slice term) : base(tx, field, term)
        {
        }

        public void Append(long num)
        {
            NumberOfEntries++;
            if (Buffer.Size == 0)
                LoadBufferFirstTime();

            if (Buffer.TryAppend(num))
                return;

            FlushBuffer(done: false);
            Buffer.Last = Buffer.Start = num;
            if (Buffer.TryAppend(num) == false)
                ThrowImpossibleToWriteToNewBuffer();
        }

        private void LoadBufferFirstTime()
        {
            if (GetIdForBlockFor(long.MaxValue, out var tvr) == false)
            {
                Buffer.Start = 0;
                Buffer.Size = 64;
                Buffer.Used = 0;
                Buffer.HasModifications = false;
                Buffer.Scope = Tx.Allocator.Allocate(64, out Buffer.Buffer);
            }
            else
            {
                LoadPersistedBuffer(in tvr);
            }
        }

        private void LoadPersistedBuffer(in TableValueReader tvr)
        {
            var key = tvr.Read(0, out int size);
            Buffer.Start = Bits.SwapBytes(*(long*)(key + size - sizeof(long)));
            byte* data = tvr.Read(1, out size);
            Buffer.Used = size;
            Buffer.HasModifications = false;
            Buffer.Size = Math.Max(64, Math.Min(MaximumBufferSize, Bits.NextPowerOf2(size + 1)));
            Debug.Assert(Buffer.Used <= Buffer.Size);
            Buffer.Scope = Tx.Allocator.Allocate(Buffer.Size, out Buffer.Buffer);
            Unsafe.CopyBlock(Buffer.Buffer.Ptr, data, (uint)Buffer.Used);
            Buffer.Last = Buffer.ComputeLast();
        }

        private static void ThrowImpossibleToWriteToNewBuffer()
        {
            throw new InvalidOperationException("Failed to write to newly allocated buffer, something is badly wrong");
        }

        private void FlushBuffer(bool done)
        {
            _deletes.Sort();
            // processing in reverse order to maybe start from the current
            // buffer and to optimize number of memmove we need to run
            for (int i = _deletes.Count - 1; i >= 0; i--)
            {
                var toDel = _deletes[i];
                if (Buffer.Start >= toDel && Buffer.Last <= toDel)
                {
                    if (Buffer.Delete(toDel))
                        NumberOfEntries--;
                    continue;
                }
                // need to load another buffer, let's save the current one
                StoreCurrentBuffer();

                if (GetIdForBlockFor(toDel,out var tvr))
                {
                    LoadPersistedBuffer(in tvr);
                    if (Buffer.Delete(toDel))
                        NumberOfEntries--;
                    continue;
                }

                // no such entry, and not even a buffer for that range?
                // let's mark it invalid for the next runs
                Buffer.HasModifications = false;
                Buffer.Start = Buffer.Last = -1;
            }
            _deletes.Clear();
            StoreCurrentBuffer();
            if (done) // update number of entries
            {
                using (Table.Allocate(out TableValueBuilder tvb))
                using (TermStats(out var key))
                {
                    if(NumberOfEntries == 0)
                    {
                        Table.DeleteByKey(key);
                    }
                    else
                    {
                        tvb.Add(key);
                        tvb.Add(NumberOfEntries);
                        Table.Set(tvb);
                    }
                }
            }

            Buffer.Used = 0;
            Buffer.HasModifications = false;

            if (Buffer.Size < MaximumBufferSize && done == false)
            {
                Buffer.Scope.Dispose();
                Buffer.Size = Math.Max(64, Math.Min(MaximumBufferSize, Bits.NextPowerOf2(Buffer.Size + 1)));
                Buffer.Scope = Tx.Allocator.Allocate(Buffer.Size, out Buffer.Buffer);
            }
        }

        private void StoreCurrentBuffer()
        {
            if (Buffer.HasModifications)
            {
                using (Table.Allocate(out TableValueBuilder tvb))
                using (BuildId(Buffer.Start, out var key))
                {
                    if(Buffer.Used == 0)
                    {
                        Table.DeleteByKey(key);
                    }
                    else
                    {
#if VALIDATE
                        Debug.Assert(Buffer.Last == Buffer.ComputeLast());
#endif

                        tvb.Add(key);
                        tvb.Add(Buffer.Buffer.Ptr, Buffer.Used);
                        Table.Set(tvb);
                    }
                }
                Buffer.HasModifications = false;
            }
        }

        public void Dispose()
        {
            FlushBuffer(done: true);
            Buffer.Scope.Dispose();
            Buffer = default;
        }

        public void Delete(long val)
        {
            _deletes.Add(val);
        }
    }
}
