using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Json;
using Tryouts.Corax.Queries;
using Voron;
using Voron.Data.Tables;

namespace Tryouts.Corax
{
    public class IndexReader
    {
        private readonly ITransactionContextPool _pool;
        internal TransactionOperationContext Context;
        private Table _stringsTable;
        private Table _entriesTable;

        public IndexReader(ITransactionContextPool pool)
        {
            _pool = pool;
        }

        public IDisposable BeginReading()
        {
            var dispose = _pool.AllocateOperationContext(out Context);
        
            Context.OpenReadTransaction();
            _stringsTable = Context.Transaction.InnerTransaction.OpenTable(IndexBuilder.StringsTableSchema, "Strings");
            _entriesTable = Context.Transaction.InnerTransaction.OpenTable(IndexBuilder.EntriesTableSchema, "Entries");

            return dispose;
        }

        public IEnumerable<(long Id, LazyStringValue ExternalId)> Query(Query q)
        {
            if(_entriesTable == null) //no entries were written yet, so OpenTable will return null
                yield break;

            q.Run(out var results);
            while (results.MoveNext())
            {
                var entryId = (long)results.Current;
                var externalId = GetExternalId(Context, _entriesTable, entryId);
                yield return (entryId, externalId);
            }
        }

        internal static unsafe long GetStringId(TransactionOperationContext context, Table stringsTable, string key)
        {
            using (Slice.From(context.Allocator, key, out var slice))
            {
                if (stringsTable.ReadByKey(slice, out var tvr))
                {
                    var stringId = *(long*)tvr.Read(1, out var size);
                    Debug.Assert(size == sizeof(long));
                    return stringId;
                }

                return -1;
            }
        }
        
        public unsafe int GetTermFreq(string term)
        {
            if (_stringsTable == null && _stringsTable == null) //no entries were written yet, so OpenTable will return null
                return 0;

            using (Slice.From(Context.Allocator, term, out var slice))
            {
                if (_stringsTable.ReadByKey(slice, out var tvr) == false) 
                    return 0;

                var freq = *(int*)tvr.Read(2, out var size);
                Debug.Assert(size == sizeof(int));

                return freq;
            }
        }

        public IEnumerable<LazyStringValue> GetTerms(long id, string field)
        {
            if (_entriesTable == null && _stringsTable == null) //no entries were written yet, so OpenTable will return null
                yield break;

            var revId = Bits.SwapBytes(id);
            using (GetSliceFromLong(revId,out var key))
            {
                // ReSharper disable once PossibleNullReferenceException
                if (_entriesTable.ReadByKey(key, out var tvr) == false)
                    yield break;

                var reader = GetReaderForEntry(tvr);
                var stringId = GetStringId(Context, _stringsTable, field);
                var termIds = reader.GetTermsFor(stringId);

                for (int i = 0; i < termIds.Count; i++)
                {
                    var curTerm = termIds[i];
                    using (GetSliceFromLong(curTerm, out key))
                    {
                        var tvh = _stringsTable.SeekOneForwardFromPrefix(
                            IndexBuilder.StringsTableSchema.Indexes[IndexBuilder.IdToString], key);

                        if (tvh == null)
                            ThrowInvalidMissingStringId(termIds[i]);

                        yield return GetTermValue(tvh);
                    }
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe LazyStringValue GetTermValue(Table.TableValueHolder tvh) => 
            Context.AllocateStringValue(null, tvh.Reader.Read(0, out var size), size);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private unsafe IDisposable GetSliceFromLong(long value, out Slice slice) => 
            Slice.From(Context.Allocator, (byte*)&value, sizeof(long), out slice);

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static unsafe EntryReader GetReaderForEntry(TableValueReader tvr) =>
            new EntryReader(tvr.Read(1, out var size), size);

        private static void ThrowInvalidMissingStringId(long id) => 
            throw new InvalidOperationException("Missing string whose id is: " + id);

        internal static unsafe LazyStringValue GetExternalId(TransactionOperationContext context, Table entriesTable, long entryId)
        {
            var revId = Bits.SwapBytes(entryId);
            using (Slice.From(context.Allocator, (byte*)&revId, sizeof(long), out var key))
            {
                if (entriesTable.ReadByKey(key, out var tvr) == false)
                {
                    ThrowBadData();
                }

                return context.AllocateStringValue(null, tvr.Read(2, out var size), size);
            }
        }

        private static void ThrowBadData()
        {
            throw new InvalidDataException("Missing entry that appears in posting list?");
        }
    }
}
