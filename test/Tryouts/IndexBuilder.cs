using System;
using System.Collections.Generic;
using System.Diagnostics;
using Raven.Client.Documents.Linq;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using SlowTests.MailingList;
using Sparrow.Json;
using Voron;
using Voron.Data.PostingList;
using Voron.Data.Tables;
using Voron.Impl;
using Bits = Sparrow.Binary.Bits;

namespace Tryouts
{
    /// <summary>
    /// Index builder builds the index :-)
    /// More specifically, it manages all the state related to a single index
    /// We expect a single index per Voron env.
    ///
    /// In particular, the following states are tracked:
    /// * Entry Id - int64 sequence
    /// * Fields - the named fields that were indexed
    /// * Field Terms - a table per field that contain the posting lists for each term
    /// * Entries - a table for each entry body
    ///
    /// This is a single threaded class which can be reused for index batches.
    ///
    /// This class does _not_ handle any analysis, this is left for the caller
    /// </summary>
    public class IndexBuilder
    {
        public static readonly TableSchema EntriesTableSchema;

        static IndexBuilder()
        {
            EntriesTableSchema = new TableSchema()
                .DefineKey(new TableSchema.SchemaIndexDef
                {
                    Count = 1,
                    StartIndex = 0,
                    IsGlobal = false
                }); 
        }
        
        private readonly TransactionContextPool _pool;
        TransactionOperationContext _context;

        public IndexBuilder(TransactionContextPool pool)
        {
            _pool = pool;
        }

        private long _lastEntryId;
        private long _currentEntryId;
        private readonly Dictionary<string, Dictionary<string, PostingListWriter>> _fields = new Dictionary<string, Dictionary<string, PostingListWriter>>();
        private RavenTransaction _tx;
        private Table _entriesTable;
        private readonly Dictionary<string, List<string>> _values = new Dictionary<string, List<string>>();
        private string _externalId;

        // TODO: Cache to avoid allocations
        // Queue<PostingListWriter>
        // Queue<List<object>>
        // readonly Queue<Dictionary<string, PostingListWriter>> 

        public IDisposable BeginIndexing()
        {
            //TODO: Use proper slices instead of strings
            
            var dispose = _pool.AllocateOperationContext(out _context);
            _tx = _context.OpenWriteTransaction();
            EntriesTableSchema.Create(_tx.InnerTransaction, "Entries", 32);

            _entriesTable = _tx.InnerTransaction.OpenTable(EntriesTableSchema, "Entries");
            
            var tree = _tx.InnerTransaction.CreateTree("Config");
            var entryId = tree.Read("LastEntryId");
            _lastEntryId = (entryId?.Reader)?.ReadLittleEndianInt64() ?? 0;
            return dispose;
        }

        public long NewEntry(string externalId)
        {
            _externalId = externalId;
            _currentEntryId = ++_lastEntryId;

            return _currentEntryId;
        }

        public unsafe void DeleteEntry(long id)
        {
            long revId = Bits.SwapBytes(id);
            using (Slice.From(_context.Allocator, (byte*)&revId, sizeof(long), out var key))
            {
                if (_entriesTable.ReadByKey(key, out var tvr) == false)
                    return;

                var entry = new BlittableJsonReaderObject(tvr.Read(1, out var size), size, _context);
                
                BlittableJsonReaderObject.PropertyDetails props = default;
                for (int i = 0; i < entry.Count; i++)
                {
                    entry.GetPropertyByIndex(i, ref props, addObjectToCache: false);
                    var values = (BlittableJsonReaderArray)props.Value;
                    for (int j = 0; j < values.Length; j++)
                    {
                        var value = (string)values[i];
                        GetPostingListWriter(props.Name, value).Delete(id);
                    }
                }
                _entriesTable.Delete(tvr.Id);

            }
        }

        private static void ThrowInvalidEntry()
        {
            throw new InvalidOperationException("Could not get 'id()' field from entry");
        }

        public unsafe void FinishEntry()
        {
            using (var writer = new ManualBlittableJsonDocumentBuilder<UnmanagedWriteBuffer>(_context))
            {
                writer.Reset(BlittableJsonDocumentBuilder.UsageMode.None);
                writer.StartWriteObjectDocument();
                writer.StartWriteObject();
                writer.WritePropertyName("entry()");
                writer.WriteValue(_currentEntryId);
                
                writer.WritePropertyName("id()");
                writer.WriteValue(_externalId);

                // TODO: use short ids for each of the property names instead of the values
                foreach (var (name, values) in _values)
                {   
                    
                    writer.WritePropertyName(name);
                    writer.StartWriteArray();
                    foreach (var value in values)
                    {
                        writer.WriteValue(value);
                    }
                    writer.WriteArrayEnd();
                }
                writer.WriteObjectEnd();
                writer.FinalizeDocument();

                var entry = writer.CreateReader();

                using (_entriesTable.Allocate(out var tvb))
                 using(Slice.From(_context.Allocator, _externalId, out var externalIdSlice))
                {
                    tvb.Add(Bits.SwapBytes(_currentEntryId));
                    tvb.Add(entry.BasePointer, entry.Size);
                    tvb.Add(externalIdSlice);
                    _entriesTable.Insert(tvb);
                }
            }

            _values.Clear();
        }

        public void FlushState()
        {
            foreach (var (_, terms) in _fields)
            {
                foreach (var (_, postingList) in terms)
                {
                    postingList.Dispose();
                }
            }

            _fields.Clear();
        }

        // TODO: Need to handle lazy string, lazy compressed string, number, etc
        public void Term(string name, string value)
        {
            if (value == null)
                return;
            GetPostingListWriter(name, value).Append(_currentEntryId);
            AddTermToEntryBody(name, value);
        }

        private void AddTermToEntryBody(string name, string value)
        {
            if (_values.TryGetValue(name, out var list) == false)
            {
                list = new List<string>();
                _values[name] = list;
            }

            list.Add(value);
        }

        private PostingListWriter GetPostingListWriter(string name, string value)
        {
            if (_fields.TryGetValue(name, out var fieldPostings) == false)
            {
                fieldPostings = new Dictionary<string, PostingListWriter>();
                _fields[name] = fieldPostings;
            }

            if (fieldPostings.TryGetValue(value, out var postingList) == false)
            {
                postingList = PostingListWriter.Create(_tx.InnerTransaction, name, value);
                fieldPostings[value] = postingList;
            }

            return postingList;
        }

        public void CompleteIndexing()
        {
            FlushState();
             
            var tree = _tx.InnerTransaction.CreateTree("Config");
            tree.Add("LastEntryId", _lastEntryId);

            _tx.Commit();
        }
    }
}
