using System;
using System.Collections.Generic;
using System.Text;
using Voron.Data.PostingList;

namespace Tryouts.Corax
{
    public unsafe struct EntryReader
    {
        private readonly byte* _ptr;
        private readonly int _size;

        public EntryReader(byte* ptr, int size)
        {
            _ptr = ptr;
            _size = size;
        }


        public List<long> GetTermsFor(long fieldId)
        {
            var range = FindRangeForField(fieldId);
            var end = range.Ptr + range.Size;
            var list = new List<long>();
            while (range.Ptr < end)
            {
                list.Add(PostingListBuffer.ReadVariableSizeLong(ref range.Ptr));
            }
            return list;
        }

        public override string ToString()
        {
            var sb = new StringBuilder();
            var ptr = _ptr;
            var end = _ptr + _size;
            while (ptr < end)
            {
                var actualfieldId = PostingListBuffer.ReadVariableSizeLong(ref ptr);
                var size = PostingListBuffer.ReadVariableSizeLong(ref ptr);
                sb.Append(actualfieldId).Append(":\t");
                var fieldEnd = ptr + size;
                while (ptr < fieldEnd)
                {
                    var entry = PostingListBuffer.ReadVariableSizeLong(ref ptr);
                    sb.Append(entry).Append(", ");
                }
            }

            return sb.ToString();
        }

        private struct TermsRange
        {
            public byte* Ptr;
            public int Size;
        }

        private TermsRange FindRangeForField(long fieldId)
        {
            var ptr = _ptr;
            var end = _ptr + _size;
            while(ptr < end)
            {
                var actualfieldId = PostingListBuffer.ReadVariableSizeLong(ref ptr);
                var size = PostingListBuffer.ReadVariableSizeLong(ref ptr);
                if (actualfieldId == fieldId)
                    return new TermsRange { Ptr = ptr, Size = (int)size };
                ptr += size;
            }
            return new TermsRange();
        }


        private static void ThrowInvalidOffsetSize()
        {
            throw new ArgumentOutOfRangeException("Invalid offset size for index entry");
        }
    }
}