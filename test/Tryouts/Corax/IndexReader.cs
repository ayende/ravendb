using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using Raven.Server.ServerWide.Context;
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

        public unsafe string[] GetTerms(long id, string field)
        {
            if (_entriesTable == null && _stringsTable == null) //no entries were written yet, so OpenTable will return null
                return Array.Empty<string>();

            var revId = Bits.SwapBytes(id);
            using (Slice.From(Context.Allocator, (byte*)&revId, sizeof(long), out var key))
            {
                // ReSharper disable once PossibleNullReferenceException
                if (_entriesTable.ReadByKey(key, out var tvr) == false)
                {
                    return Array.Empty<string>();
                }

                var entry = tvr.Read(1, out var size);
                var reader = new EntryReader(entry, size);

                var stringId = GetStringId(Context, _stringsTable, field);

                var termIds = reader.GetTermsFor(stringId);
                var terms = new string[termIds.Count];
                for (int i = 0; i < termIds.Count; i++)
                {
                    var curTerm = termIds[i];
                    using (Slice.From(Context.Allocator, (byte*)&curTerm, sizeof(long), out key))
                    {
                        var tvh = _stringsTable.SeekOneForwardFromPrefix(IndexBuilder.StringsTableSchema.Indexes[IndexBuilder.IdToString], key);
                        if (tvh == null)
                            ThrowInvalidMissingStringId(termIds[i]);

                        terms[i] = Encoding.UTF8.GetString(tvh.Reader.Read(0, out size), size);
                    }
                }
                return terms;
            }
        }

        private static void ThrowInvalidMissingStringId(long id)
        {
            throw new InvalidOperationException("Missing string whose id is: " + id);
        }

        internal static unsafe LazyStringValue GetExternalId(TransactionOperationContext context, Table entriesTable, long entryId)
        {
            var revId = Bits.SwapBytes(entryId);
            using (Slice.From(context.Allocator, (byte*)&revId, sizeof(long), out var key))
            {
                if (entriesTable.ReadByKey(key, out var tvr) == false)
                {
                    ThrowBadData();
                }
                
                return new LazyStringValue(null,tvr.Read(2, out var size), size, context);
            }
        }

        private static void ThrowBadData()
        {
            throw new InvalidDataException("Missing entry that appears in posting list?");
        }
    }
}
