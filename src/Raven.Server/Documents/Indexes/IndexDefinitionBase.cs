﻿using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexing;
using Raven.Server.ServerWide.Context;

using Sparrow.Json;

using Voron;
using Sparrow;

namespace Raven.Server.Documents.Indexes
{
    public abstract class IndexDefinitionBase
    {
        protected const string MetadataFileName = "metadata";

        protected static readonly Slice DefinitionSlice = Slice.From(StorageEnvironment.LabelsContext, "Definition", ByteStringType.Immutable); 

        private int? _cachedHashCode;

        protected IndexDefinitionBase(string name, string[] collections, IndexLockMode lockMode, IndexField[] mapFields)
        {
            Name = name;
            Collections = collections;
            MapFields = mapFields.ToDictionary(x => x.Name, x => x, StringComparer.Ordinal);
            LockMode = lockMode;
        }

        public string Name { get; }

        public string[] Collections { get; }

        public Dictionary<string, IndexField> MapFields { get; }

        public IndexLockMode LockMode { get; set; }

        public void Persist(TransactionOperationContext context, StorageEnvironmentOptions options)
        {
            if (options is StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)
            {
                using (var stream = File.Open(Path.Combine(options.BasePath, MetadataFileName), FileMode.Create))
                using (var writer = new StreamWriter(stream, Encoding.UTF8))
                {
                    writer.Write(Name);
                    writer.Flush();
                }
            }

            var tree = context.Transaction.InnerTransaction.CreateTree("Definition");
            using (var stream = new MemoryStream())
            using (var writer = new BlittableJsonTextWriter(context, stream))
            {
                Persist(context, writer);

                writer.Flush();

                stream.Position = 0;
                tree.Add(DefinitionSlice, Slice.From(context.Allocator, stream.ToArray()));
            }
        }

        public void Persist(JsonOperationContext context, BlittableJsonTextWriter writer)
        {
            writer.WriteStartObject();

            writer.WritePropertyName((nameof(Name)));
            writer.WriteString(Name);
            writer.WriteComma();

            writer.WritePropertyName((nameof(Collections)));
            writer.WriteStartArray();
            var isFirst = true;
            foreach (var collection in Collections)
            {
                if (isFirst == false)
                    writer.WriteComma();

                isFirst = false;
                writer.WriteString((collection));
            }

            writer.WriteEndArray();
            writer.WriteComma();
            writer.WritePropertyName((nameof(LockMode)));
            writer.WriteInteger((int)LockMode);
            writer.WriteComma();

            PersistFields(context, writer);

            writer.WriteEndObject();
        }

        protected abstract void PersistFields(JsonOperationContext context, BlittableJsonTextWriter writer);

        protected void PersistMapFields(JsonOperationContext context, BlittableJsonTextWriter writer)
        {
            writer.WritePropertyName((nameof(MapFields)));
            writer.WriteStartArray();
            var first = true;
            foreach (var field in MapFields.Values)
            {
                if (first == false)
                    writer.WriteComma();

                writer.WriteStartObject();

                writer.WritePropertyName((nameof(field.Name)));
                writer.WriteString((field.Name));
                writer.WriteComma();

                writer.WritePropertyName((nameof(field.Highlighted)));
                writer.WriteBool(field.Highlighted);
                writer.WriteComma();

                writer.WritePropertyName((nameof(field.SortOption)));
                writer.WriteInteger((int)(field.SortOption ?? SortOptions.None));
                writer.WriteComma();

                writer.WritePropertyName((nameof(field.MapReduceOperation)));
                writer.WriteInteger((int)(field.MapReduceOperation));

                writer.WriteEndObject();

                first = false;
            }
            writer.WriteEndArray();
        }

        public IndexDefinition ConvertToIndexDefinition(Index index)
        {
            var indexDefinition = CreateIndexDefinition() ?? new IndexDefinition();
            indexDefinition.Name = index.Name;
            indexDefinition.IndexId = index.IndexId;
            indexDefinition.Type = index.Type;
            indexDefinition.LockMode = LockMode;

            indexDefinition.IndexVersion = -1; // TODO [ppekrol]

            return indexDefinition;
        }

        protected abstract IndexDefinition CreateIndexDefinition();

        public bool ContainsField(string field)
        {
            if (field.EndsWith("_Range"))
                field = field.Substring(0, field.Length - 6);

            return MapFields.ContainsKey(field);
        }

        public IndexField GetField(string field)
        {
            if (field.EndsWith("_Range"))
                field = field.Substring(0, field.Length - 6);

            return MapFields[field];
        }

        public bool TryGetField(string field, out IndexField value)
        {
            if (field.EndsWith("_Range"))
                field = field.Substring(0, field.Length - 6);

            return MapFields.TryGetValue(field, out value);
        }

        public abstract bool Equals(IndexDefinitionBase indexDefinition, bool ignoreFormatting, bool ignoreMaxIndexOutputs);

        public abstract bool Equals(IndexDefinition indexDefinition, bool ignoreFormatting, bool ignoreMaxIndexOutputs);

        public override int GetHashCode()
        {
            if (_cachedHashCode != null)
                return _cachedHashCode.Value;

            unchecked
            {
                var hashCode = MapFields?.GetDictionaryHashCode() ?? 0;
                hashCode = (hashCode * 397) ^ (Name?.GetHashCode() ?? 0);
                hashCode = (hashCode * 397) ^ (Collections?.GetEnumerableHashCode() ?? 0);

                _cachedHashCode = hashCode;

                return hashCode;
            }
        }

        public static string TryReadNameFromMetadataFile(DirectoryInfo directory)
        {
            var metadataFile = Path.Combine(directory.FullName, MetadataFileName);
            if (File.Exists(metadataFile) == false)
                return null;

            var name = File.ReadAllText(metadataFile, Encoding.UTF8);
            if (string.IsNullOrWhiteSpace(name))
                return null;

            return name;
        }

        protected static string ReadName(BlittableJsonReaderObject reader)
        {
            string name;
            if (reader.TryGet(nameof(Name), out name) == false || string.IsNullOrWhiteSpace(name))
                throw new InvalidOperationException("No persisted name");

            return name;
        }

        protected static string[] ReadCollections(BlittableJsonReaderObject reader)
        {
            BlittableJsonReaderArray jsonArray;
            if (reader.TryGet(nameof(Collections), out jsonArray) == false || jsonArray.Length == 0)
                throw new InvalidOperationException("No persisted collections");

            var result = new string[jsonArray.Length];
            for (var i = 0; i < jsonArray.Length; i++)
                result[i] = jsonArray.GetStringByIndex(i);

            return result;
        }

        protected static IndexLockMode ReadLockMode(BlittableJsonReaderObject reader)
        {
            int lockModeAsInt;
            if (reader.TryGet(nameof(LockMode), out lockModeAsInt) == false)
                throw new InvalidOperationException("No persisted lock mode");

            return (IndexLockMode)lockModeAsInt;
        }

        protected static IndexField[] ReadMapFields(BlittableJsonReaderObject reader)
        {
            BlittableJsonReaderArray jsonArray;
            if (reader.TryGet(nameof(MapFields), out jsonArray) == false)
                throw new InvalidOperationException("No persisted lock mode");

            var fields = new IndexField[jsonArray.Length];
            for (var i = 0; i < jsonArray.Length; i++)
            {
                var json = jsonArray.GetByIndex<BlittableJsonReaderObject>(i);

                string name;
                json.TryGet(nameof(IndexField.Name), out name);

                bool highlighted;
                json.TryGet(nameof(IndexField.Highlighted), out highlighted);

                int sortOptionAsInt;
                json.TryGet(nameof(IndexField.SortOption), out sortOptionAsInt);

                var field = new IndexField
                {
                    Name = name,
                    Highlighted = highlighted,
                    Storage = FieldStorage.No,
                    SortOption = (SortOptions?)sortOptionAsInt,
                    Indexing = FieldIndexing.Default
                };

                fields[i] = field;
            }

            return fields;
        }

        protected Dictionary<string, IndexFieldOptions> ConvertFields(Dictionary<string, IndexField> fields)
        {
            return fields.ToDictionary(
                x => x.Key,
                x => new IndexFieldOptions
                {
                    Sort = x.Value.SortOption,
                    TermVector = x.Value.Highlighted ? FieldTermVector.WithPositionsAndOffsets : (FieldTermVector?)null,
                    Analyzer = x.Value.Analyzer,
                    Indexing = x.Value.Indexing,
                    Storage = x.Value.Storage
                });
        }
    }
}