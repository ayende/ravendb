using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using Raven.Server.ServerWide.Context;
using Sparrow.Binary;
using Sparrow.Json;
using Voron;
using Voron.Data.PostingList;
using Voron.Data.Tables;

namespace Tryouts
{
    public class IndexReader
    {
        private readonly TransactionContextPool _pool;

        public IndexReader(TransactionContextPool pool)
        {
            _pool = pool;
        }

        public IEnumerable<(long Id, string ExternalId)> Query(Query q)
        {
            var entriesTable = q.Context.Transaction.InnerTransaction.OpenTable(IndexBuilder.EntriesTableSchema, "Entries");
            while (q.ReadNext(out var entryId))
            {
                var externalId = GetExternalId(q.Context, entriesTable, entryId);
                yield return (entryId, externalId);
            }
        }

        private unsafe long GetStringId(TransactionOperationContext context, Table stringsTable, string key)
        {
            using (Slice.From(context.Allocator, key, out var slice))
            {
                if (stringsTable.ReadByKey(slice, out var tvr) != false)
                {
                    long stringId = *(long*)tvr.Read(1, out var size);
                    Debug.Assert(size == sizeof(long));
                    return stringId;
                }

                return -1;
            }
        }

        public unsafe string[] GetTerms(TransactionOperationContext context, long id, string field)
        {
            var entriesTable = context.Transaction.InnerTransaction.OpenTable(IndexBuilder.EntriesTableSchema, "Entries");
            var stringsTable = context.Transaction.InnerTransaction.OpenTable(IndexBuilder.StringsTableSchema, "Strings");
            long revId = Bits.SwapBytes(id);
            using (Slice.From(context.Allocator, (byte*)&revId, sizeof(long), out var key))
            {
                if (entriesTable.ReadByKey(key, out var tvr) == false)
                {
                    return Array.Empty<string>();
                }

                var entry = tvr.Read(1, out var size);
                var reader = new EntryReader(entry, size);

                var stringId = GetStringId(context, stringsTable, field);

                var termIds = reader.GetTermsFor(stringId);
                var terms = new string[termIds.Length];
                for (int i = 0; i < termIds.Length; i++)
                {
                    var curTerm = termIds[i];
                    using (Slice.From(context.Allocator, (byte*)&curTerm, sizeof(long), out key))
                    {
                        var tvh = stringsTable.SeekOneForwardFromPrefix(IndexBuilder.StringsTableSchema.Indexes[IndexBuilder.IdToString], key);
                        if (tvh == null)
                            ThrowInvalidMissingStringId(termIds[i]);

                        terms[i] = Encoding.UTF8.GetString(tvh.Reader.Read(0, out size), size);
                    }
                }

                return terms;
            }
        }

        private static unsafe void ThrowInvalidMissingStringId(long id)
        {
            throw new InvalidOperationException("Missing string whose id is: " + id);
        }

        private static unsafe string GetExternalId(TransactionOperationContext context, Table entriesTable, long entryId)
        {
            long revId = Bits.SwapBytes(entryId);
            using (Slice.From(context.Allocator, (byte*)&revId, sizeof(long), out var key))
            {
                if (entriesTable.ReadByKey(key, out var tvr) == false)
                {
                    ThrowBadData();
                }

                return Encoding.UTF8.GetString(tvr.Read(2, out var size), size);
            }
        }


        private static void ThrowBadData()
        {
            throw new InvalidDataException("Missing entry that appears in posting list?");
        }
    }

    public unsafe struct EntryReader
    {
        private readonly byte* _ptr;
        private readonly byte* _arrayStart;
        private readonly int _size;
        private readonly int _count;
        private readonly byte _offsetSize;

        public EntryReader(byte* ptr, int size)
        {
            _ptr = ptr;
            _size = size;

            _offsetSize = _ptr[_size - 1];
            _count = BlittableJsonReaderBase.ReadVariableSizeIntInReverse(_ptr, _size - 2, out byte offset);
            var arrayOffset = BlittableJsonReaderBase.ReadVariableSizeIntInReverse(_ptr, _size - 2 - offset, out offset);
            _arrayStart = _ptr + arrayOffset;
        }

        [StructLayout(LayoutKind.Sequential, Pack = 1, Size = 3)]
        private struct ByteTriple
        {
            public byte Start, Size, FieldIdOffset;
        }

        [StructLayout(LayoutKind.Sequential, Pack = 1, Size = 6)]
        private struct UShortTriple
        {
            public ushort Start, Size, FieldIdOffset;
        }

        [StructLayout(LayoutKind.Sequential, Pack = 1, Size = 12)]
        private struct IntTriple
        {
            public int Start, Size, FieldIdOffset;
        }

        public long[] GetTermsFor(long fieldId)
        {
            var (Start, Size) = FindRangeForField(fieldId);
            if (Size == 0)
                return Array.Empty<long>();
            var count = Size / _offsetSize;
            var terms = new long[count]; // TODO: avoid this allocation, is this even called often enough?
            for (int i = 0; i < count; i++)
            {
                switch (_offsetSize)
                {
                    case 1:
                        terms[i] = (_ptr + Start)[i];
                        break;
                    case 2:
                        terms[i] = ((short*)(_ptr + Start))[i];
                        break;
                    case 4:
                        terms[i] = ((int*)(_ptr + Start))[i];
                        break;
                    default:
                        ThrowInvalidOffsetSize();
                        break;
                }
            }

            return terms;
        }

        private (int Start, int Size) FindRangeForField(long fieldId)
        {
            // TODO: do binary search here? The values are sorted by field id
            byte* ptr;
            for (int i = 0; i < _count; i++)
            {
                switch (_offsetSize)
                {
                    case 1:
                        ptr = ((ByteTriple*)_arrayStart)[i].FieldIdOffset + _ptr;
                        if (PostingListBuffer.ReadVariableSizeLong(ref ptr) == fieldId)
                            return (((ByteTriple*)_arrayStart)[i].Start, ((ByteTriple*)_arrayStart)[i].Size);
                        break;
                    case 2:
                        ptr = ((UShortTriple*)_arrayStart)[i].FieldIdOffset + _ptr;
                        if (PostingListBuffer.ReadVariableSizeLong(ref ptr) == fieldId)
                            return (((UShortTriple*)_arrayStart)[i].Start, ((UShortTriple*)_arrayStart)[i].Size);
                        break;
                    case 4:
                        ptr = ((IntTriple*)_arrayStart)[i].FieldIdOffset + _ptr;
                        if (PostingListBuffer.ReadVariableSizeLong(ref ptr) == fieldId)
                            return (((IntTriple*)_arrayStart)[i].Start, ((IntTriple*)_arrayStart)[i].Size);
                        break;
                    default:
                        ThrowInvalidOffsetSize();
                        return (0, 0); // never hit
                }
            }

            return (0, 0); // not found
        }


        private static void ThrowInvalidOffsetSize()
        {
            throw new ArgumentOutOfRangeException("Invalid offset size for index entry");
        }
    }

    public abstract class Query
    {
        public readonly TransactionOperationContext Context;
        protected readonly IndexReader Reader;

        public Query(TransactionOperationContext context, IndexReader reader)
        {
            Context = context;
            Reader = reader;
        }

        public abstract bool ReadNext(out long output);

        public abstract bool Seek(long value, out long output);
    }

    public class TermQuery : Query
    {
        public readonly string Field;
        public readonly string Term;
        private readonly PostingListReader _postingListReader;

        public TermQuery(TransactionOperationContext context, IndexReader reader, string field, string term) : base(context, reader)
        {
            Field = field;
            Term = term;
            _postingListReader = PostingListReader.Create(context.Transaction.InnerTransaction, Field, Term);
        }

        public override bool ReadNext(out long output)
        {
            return _postingListReader.ReadNext(out output);
        }

        public override bool Seek(long value, out long output)
        {
            _postingListReader.Seek(value);
            return _postingListReader.ReadNext(out output);
        }
    }

    public class AndQuery : Query
    {
        private Query _left, _right;
        private long _leftVal = -1, _rightVal = -2;
        private bool _done;

        public AndQuery(TransactionOperationContext context, IndexReader reader, Query left, Query right) : base(context, reader)
        {
            _left = left;
            _right = right;

            _done |= _left.ReadNext(out _leftVal) == false;
            _done |= _right.ReadNext(out _rightVal) == false;
        }

        public override bool ReadNext(out long output)
        {
            while (_done == false)
            {
                if (_leftVal == _rightVal)
                {
                    output = _leftVal;
                    _done |= _left.ReadNext(out _leftVal) == false;
                    _done |= _right.ReadNext(out _rightVal) == false;
                    return true;
                }

                if (_leftVal > _rightVal)
                {
                    _done |= _right.Seek(_leftVal, out _rightVal) == false;
                }
                else // if (_rightVal > _leftVal)
                {
                    _done |= _left.Seek(_rightVal, out _leftVal) == false;
                }
            }

            output = -1;
            return false;
        }

        public override bool Seek(long value, out long output)
        {
            _done |= _left.Seek(value, out _leftVal) == false;
            _done |= _right.Seek(value, out _rightVal) == false;
            return ReadNext(out output);
        }
    }
}
