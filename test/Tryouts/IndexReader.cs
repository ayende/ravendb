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
                var terms = new string[termIds.Count];
                for (int i = 0; i < termIds.Count; i++)
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

    public class PrefixQuery : Query
    {
        public readonly string Field, Prefix;

        public PrefixQuery(TransactionOperationContext context, IndexReader reader, string field, string prefix) : base(context, reader)
        {
            Field = field;
            Prefix = prefix;

            var table = context.Transaction.InnerTransaction.OpenTable(PostingList.PostingListSchema, field);
            if (table == null)
                return;

            // TODO: avoid this allocation
            using (Slice.From(context.Allocator, "S:" + prefix, out var prefixSlice))
            {
                foreach (var item in table.SeekByPrimaryKeyPrefix(prefixSlice, Slices.Empty, 0))
                {
//                    item.Value.Reader
                }
                
            }
        }

        public override bool ReadNext(out long output)
        {
            throw new NotImplementedException();

            
        }

        public override bool Seek(long value, out long output)
        {
            throw new NotImplementedException();
        }
    }

    public class OrQuery : Query
    {
        private readonly Query _left, _right;
        private long _leftVal, _rightVal;
        private bool _leftDone, _rightDone;

        public OrQuery(TransactionOperationContext context, IndexReader reader, Query left, Query right) : base(context, reader)
        {
            _left = left;
            _right = right;

            _leftDone = _left.ReadNext(out _leftVal) == false;
            _rightDone = _right.ReadNext(out _rightVal) == false;
        }

        public override bool ReadNext(out long output)
        {
            while (_leftDone == false || _rightDone == false)
            {
                if (_leftDone == false && (_leftVal < _rightVal || _rightDone))
                {
                    output = _leftVal;
                    _leftDone = _left.ReadNext(out _leftVal) == false;
                    return true;
                }

                if (_rightDone == false && (_leftVal > _rightVal || _leftDone))
                {
                    output = _rightVal;
                    _rightDone = _right.ReadNext(out _rightVal) == false;
                    return true;
                }

                output = _leftVal;
                _leftDone = _left.ReadNext(out _leftVal) == false;
                _rightDone = _right.ReadNext(out _rightVal) == false;
                return true;
            }

            output = -1;
            return false;
        }

        public override bool Seek(long value, out long output)
        {
            _leftDone |= _left.Seek(value, out _leftVal) == false;
            _rightDone |= _right.Seek(value, out _rightVal) == false;
            return ReadNext(out output);
        }
    }
    
    public class AndQuery : Query
    {
        private readonly Query _left, _right;
        private long _leftVal, _rightVal;
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
