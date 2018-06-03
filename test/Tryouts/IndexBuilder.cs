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
        public static readonly TableSchema StringsTableSchema;
        public static readonly Slice IdToString;
        static IndexBuilder()
        {
            Slice.From(StorageEnvironment.LabelsContext, "IdToString", out IdToString);

            EntriesTableSchema = new TableSchema()
                .DefineKey(new TableSchema.SchemaIndexDef
                {
                    Count = 1,
                    StartIndex = 0,
                    IsGlobal = false
                });
            StringsTableSchema = new TableSchema()
                .DefineKey(new TableSchema.SchemaIndexDef
                {
                    Count = 1,
                    StartIndex = 0,
                    IsGlobal = false
                })
                .DefineIndex(new TableSchema.SchemaIndexDef
                {
                    Count = 1,
                    StartIndex = 1,
                    IsGlobal = false,
                    Name = IdToString
                });
        }

        private readonly TransactionContextPool _pool;
        TransactionOperationContext _context;

        public IndexBuilder(TransactionContextPool pool)
        {
            _pool = pool;
        }

        private int _lastEntrySize = 64;
        private long _lastStringId;
        private long _lastEntryId;
        private long _currentEntryId;
        private readonly Dictionary<string, long> _stringIdCache = new Dictionary<string, long>();
        private readonly Dictionary<string, Dictionary<string, PostingListWriter>> _fields = new Dictionary<string, Dictionary<string, PostingListWriter>>();
        private RavenTransaction _tx;
        private Table _stringsTable;
        private Table _entriesTable;
        private readonly Dictionary<long, HashSet<long>> _values = new Dictionary<long, HashSet<long>>();
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
            StringsTableSchema.Create(_tx.InnerTransaction, "Strings", 32);
            _stringsTable = _tx.InnerTransaction.OpenTable(StringsTableSchema, "Strings");
            _entriesTable = _tx.InnerTransaction.OpenTable(EntriesTableSchema, "Entries");

            var tree = _tx.InnerTransaction.CreateTree("Config");
            _lastEntryId = (tree.Read("LastEntryId")?.Reader)?.ReadLittleEndianInt64() ?? 0;
            _lastStringId = (tree.Read("LastStringId")?.Reader)?.ReadLittleEndianInt64() ?? 0;
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

        private int MinOffsetSize(int offset)
        {
            if (offset < byte.MaxValue)
                return 1;
            if (offset < ushort.MaxValue)
                return 2;
            return 4;
        }

        public unsafe void FinishEntry()
        {
            var buffer = stackalloc byte[9];
            using (var writer = new UnmanagedWriteBuffer(_context, _context.GetMemory(_lastEntrySize)))
            {
                int size;
                foreach (var (fieldId, terms) in _values)
                {
                    using (var temp = new UnmanagedWriteBuffer(_context, _context.GetMemory(_lastEntrySize)))
                    {
                        foreach (var term in terms)
                        {
                            size = PostingListBuffer.WriteVariableSizeLong(term, buffer);
                            temp.Write(buffer, size);
                        }

                        size = PostingListBuffer.WriteVariableSizeLong(fieldId, buffer);
                        writer.Write(buffer, size);

                        temp.EnsureSingleChunk(out var termsBuf, out int termsSize);

                        size = PostingListBuffer.WriteVariableSizeLong(termsSize, buffer);
                        writer.Write(buffer, size);

                        writer.Write(termsBuf, termsSize);
                    }
                }
                writer.EnsureSingleChunk(out var output, out int entrySize);

                _lastEntrySize = Math.Max(Bits.NextPowerOf2(entrySize), _lastEntrySize);

                using (_entriesTable.Allocate(out var tvb))
                using (Slice.From(_context.Allocator, _externalId, out var externalIdSlice))
                {
                    tvb.Add(Bits.SwapBytes(_currentEntryId));
                    tvb.Add(output, entrySize);
                    tvb.Add(externalIdSlice);
                    _entriesTable.Insert(tvb);
                }
            }

            _values.Clear();
        }


        private static void ThrowInvalidOffsetSize()
        {
            throw new InvalidOperationException("Invalid offset size for index entry on write");
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
        public void Term(string field, string term)
        {
            if (term == null)
                return;
            GetPostingListWriter(field, term).Append(_currentEntryId);
            AddTermToEntryBody(field, term);
        }

        private void AddTermToEntryBody(string field, string term)
        {
            var fieldId = GetStringId(field, cache: true);
            if (_values.TryGetValue(fieldId, out var list) == false)
            {
                list = new HashSet<long>();
                _values[fieldId] = list;
            }

            list.Add(GetStringId(term, cache: false));
        }

        private PostingListWriter GetPostingListWriter(string field, string term)
        {
            if (_fields.TryGetValue(field, out var fieldPostings) == false)
            {
                fieldPostings = new Dictionary<string, PostingListWriter>();
                _fields[field] = fieldPostings;
            }

            if (fieldPostings.TryGetValue(term, out var postingList) == false)
            {
                postingList = PostingListWriter.Create(_tx.InnerTransaction, field, term);
                fieldPostings[term] = postingList;
            }

            return postingList;
        }

        private unsafe long GetStringId(string key, bool cache)
        {
            if (cache && _stringIdCache.TryGetValue(key, out var val))
                return val;

            using (Slice.From(_context.Allocator, key, out var slice))
            {
                if (_stringsTable.ReadByKey(slice, out var tvr) != false)
                {
                    long stringId = *(long*)tvr.Read(1, out var size);
                    Debug.Assert(size == sizeof(long));
                    if (cache)
                        _stringIdCache[key] = stringId;
                    return stringId;
                }

                using (_stringsTable.Allocate(out var tvb))
                {
                    var stringId = ++_lastStringId;
                    tvb.Add(slice);
                    tvb.Add(stringId);
                    _stringsTable.Insert(tvb);
                    if (cache)
                        _stringIdCache[key] = stringId;
                    return stringId;
                }
            }
        }

        public void CompleteIndexing()
        {
            FlushState();

            var tree = _tx.InnerTransaction.CreateTree("Config");
            tree.Add("LastEntryId", _lastEntryId);
            tree.Add("LastStringId", _lastStringId);

            _tx.Commit();
        }
    }
}
