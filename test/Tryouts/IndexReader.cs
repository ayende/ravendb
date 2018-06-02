using System.Collections.Generic;
using System.IO;
using System.Text;
using Lucene.Net.Search;
using Lucene.Net.Spatial.Util;
using Raven.Client.Documents.Linq;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Voron;
using Voron.Data.PostingList;
using Voron.Data.Tables;
using Bits = Sparrow.Binary.Bits;

namespace Tryouts
{
    public class IndexReader
    {
        private readonly TransactionContextPool _pool;

        public IndexReader(TransactionContextPool pool)
        {
            _pool = pool;
        }

        public IEnumerable<string> Query(string field, string term)
        {
            using (_pool.AllocateOperationContext(out TransactionOperationContext context))
            using(var tx = context.OpenReadTransaction())
            {
                var entriesTable = tx.InnerTransaction.OpenTable(IndexBuilder.EntriesTableSchema,"Entries");
                var reader = PostingListReader.Create(tx.InnerTransaction, field, term);
                while (reader.ReadNext(out var entryId))
                {
                    var externalId = GetExternalId(context, entriesTable, entryId);
                    yield return externalId;
                }
            }
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

                return Encoding.UTF8.GetString(tvr.Read(2, out var size1), size1);
            }
        }


        private static void ThrowBadData()
        {
            throw new InvalidDataException("Missing entry that appears in posting list?");
        }
    }
}
