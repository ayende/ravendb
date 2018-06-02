using System;
using Sparrow.Binary;
using Voron.Impl;

namespace Voron.Data.PostingList
{
    public unsafe class PostingListReader : PostingList
    {
        private byte* _buffer;
        private byte* _end;

        private long _last;

        public static PostingListReader Create(Transaction tx, string field, string term)
        {
            Slice.From(tx.Allocator, field, out var fieldSlice);
            Slice.From(tx.Allocator, term, out var termSlice);
            return new PostingListReader(tx, fieldSlice, termSlice);
        }

        public PostingListReader(Transaction tx, Slice field, Slice term) : base(tx, field, term)
        {
            if (Table == null)
                return; // no table means there is no field for this value
            ReadBlock(0);
        }

        private void ReadBlock(long start)
        {
            if (GetIdForBlockFor(start, out var tvr) == false)
            {
                _buffer = null;
                _end = null;
                return;
            }

            var key = tvr.Read(0, out int size);
            var blockStart = Bits.SwapBytes(*(long*)(key + size - sizeof(long)));
            _last = blockStart;
            _buffer = tvr.Read(1, out size);
            _end = _buffer + size;
        }


        public bool ReadNext(out long val)
        {
            if (_buffer == null)
            {
                val = 0;
                return false;
            }

            if (_buffer >= _end)
            {
                return TryFindInNextBlock(out val);
            }

            long delta = PostingListBuffer.ReadVariableSizeLong(ref _buffer);
            _last += delta;
            val = _last;
            return true;
        }

        private bool TryFindInNextBlock(out long val)
        {
            var nextValue = _last + 1;
            ReadBlock(nextValue);
            if (_last < nextValue)
            {
                // couldn't find a value, done
                _buffer = null;
                _end = null;
                _last = 0;
                val = 0;
                return false;
            }
            return ReadNext(out val);
        }

        public void Seek(long val)
        {
            ReadBlock(val);
            if (_buffer == null)
                return;
            while (_buffer < _end)
            {
                var prevBuffer = _buffer;
                long current = PostingListBuffer.ReadVariableSizeLong(ref _buffer) + _last;
                if (current >= val)
                {
                    _buffer = prevBuffer;// next read will get it again
                    return;
                }
                _last = current;
            }
            // next read will jump to the next block, if there is one
        }
    }
}
