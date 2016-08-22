using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexing;
using Raven.Client.Smuggler;
using Raven.Server.Documents;
using Raven.Server.Documents.Versioning;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Smuggler.Documents.Data;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Smuggler.Documents
{
    public class SmugglerImporter
    {
        private readonly DocumentDatabase _database;

        public DatabaseItemType OperateOnTypes;

        public SmugglerImporter(DocumentDatabase database)
        {
            _database = database;
            _batchPutCommand = new MergedBatchPutCommand(_database, 0);
            OperateOnTypes = DatabaseItemType.Indexes | DatabaseItemType.Transformers
                             | DatabaseItemType.Documents | DatabaseItemType.RevisionDocuments |
                             DatabaseItemType.Identities;
        }

        private MergedBatchPutCommand _batchPutCommand;
        private MergedBatchPutCommand _prevCommand;
        private Task _prevCommandTask;

        public async Task<ImportResult> Import(DocumentsOperationContext context, Stream stream)
        {
            var result = new ImportResult();

            var state = new JsonParserState();

            using (var parser = new UnmanagedJsonParser(context, state, "fileName"))
            {
                var operateOnType = "__top_start_object";
                var buildVersion = 0L;
                var identities = new Dictionary<string, long>();
                VersioningStorage versioningStorage = null;

                var buffer = context.GetParsingBuffer();
                while (true)
                {
                    if (parser.Read() == false)
                    {
                        var read = await stream.ReadAsync(buffer, 0, buffer.Length);
                        if (read == 0)
                        {
                            if (state.CurrentTokenType != JsonParserToken.EndObject)
                                throw new EndOfStreamException("Stream ended without reaching end of json content");
                            break;
                        }
                        parser.SetBuffer(buffer, read);
                        continue;
                    }

                    switch (state.CurrentTokenType)
                    {
                        case JsonParserToken.String:
                            unsafe
                            {
                                operateOnType =
                                    new LazyStringValue(null, state.StringBuffer, state.StringSize, context).ToString();
                            }
                            break;
                        case JsonParserToken.Integer:
                            switch (operateOnType)
                            {
                                case "BuildVersion":
                                    buildVersion = state.Long;
                                    break;
                            }
                            break;
                        case JsonParserToken.StartObject:
                            if (operateOnType == "__top_start_object")
                            {
                                operateOnType = null;
                                break;
                            }

                            var builder = new BlittableJsonDocumentBuilder(context,
                                BlittableJsonDocumentBuilder.UsageMode.ToDisk, "ImportObject", parser, state);
                            builder.ReadNestedObject();
                            while (builder.Read() == false)
                            {
                                var read = await stream.ReadAsync(buffer, 0, buffer.Length);
                                if (read == 0)
                                    throw new EndOfStreamException("Stream ended without reaching end of json content");
                                parser.SetBuffer(buffer, read);
                            }
                            builder.FinalizeDocument();

                            if (operateOnType == "Docs" && OperateOnTypes.HasFlag(DatabaseItemType.Documents))
                            {
                                result.DocumentsCount++;
                                _batchPutCommand.Add(builder.CreateReader());
                                await HandleBatchOfDocuments(context, parser, buildVersion);
                            }
                            else if (operateOnType == "RevisionDocuments" &&
                                     OperateOnTypes.HasFlag(DatabaseItemType.RevisionDocuments))
                            {
                                if (versioningStorage == null)
                                    break;

                                result.RevisionDocumentsCount++;
                                _batchPutCommand.Add(builder.CreateReader());
                                await HandleBatchOfDocuments(context, parser, buildVersion);
                            }
                            else
                            {
                                using (builder)
                                {
                                    switch (operateOnType)
                                    {
                                        case "Attachments":
                                            result.Warnings.Add(
                                                "Attachments are not supported anymore. Use RavenFS isntead. Skipping.");
                                            break;
                                        case "Indexes":
                                            if (OperateOnTypes.HasFlag(DatabaseItemType.Indexes))
                                            {
                                                result.IndexesCount++;

                                                try
                                                {
                                                    using (var reader = builder.CreateReader())
                                                    {
                                                        IndexDefinition indexDefinition;
                                                        if (buildVersion == 0) // pre 4.0 support
                                                        {
                                                            indexDefinition = ReadLegacyIndexDefinition(reader);
                                                            if (string.Equals(indexDefinition.Name, "Raven/DocumentsByEntityName", StringComparison.OrdinalIgnoreCase)) // skipping not needed old default index
                                                                continue;
                                                        }
                                                        else if (buildVersion >= 40000 && buildVersion <= 44999)
                                                        {
                                                            indexDefinition = JsonDeserializationServer.IndexDefinition(reader);
                                                        }
                                                        else
                                                            throw new NotSupportedException($"We do not support importing indexes from '{buildVersion}' build.");

                                                        _database.IndexStore.CreateIndex(indexDefinition);
                                                    }
                                                }
                                                catch (Exception e)
                                                {
                                                    result.Warnings.Add($"Could not import index. Message: {e.Message}");
                                                }
                                            }
                                            break;
                                        case "Transformers":
                                            if (OperateOnTypes.HasFlag(DatabaseItemType.Transformers))
                                            {
                                                result.TransformersCount++;

                                                try
                                                {
                                                    using (var reader = builder.CreateReader())
                                                    {
                                                        TransformerDefinition transformerDefinition;
                                                        if (buildVersion == 0) // pre 4.0 support
                                                        {
                                                            transformerDefinition = ReadLegacyTransformerDefinition(reader);
                                                        }
                                                        else if (buildVersion >= 40000 && buildVersion <= 44999)
                                                        {
                                                            transformerDefinition = JsonDeserializationServer.TransformerDefinition(reader);
                                                        }
                                                        else
                                                            throw new NotSupportedException($"We do not support importing transformers from '{buildVersion}' build.");

                                                        _database.TransformerStore.CreateTransformer(transformerDefinition);
                                                    }
                                                }
                                                catch (Exception e)
                                                {
                                                    result.Warnings.Add($"Could not import transformer. Message: {e.Message}");
                                                }
                                            }
                                            break;
                                        case "Identities":
                                            if (OperateOnTypes.HasFlag(DatabaseItemType.Identities))
                                            {
                                                result.IdentitiesCount++;

                                                using (var reader = builder.CreateReader())
                                                {
                                                    try
                                                    {
                                                        string identityKey, identityValueString;
                                                        long identityValue;
                                                        if (reader.TryGet("Key", out identityKey) == false || reader.TryGet("Value", out identityValueString) == false || long.TryParse(identityValueString, out identityValue) == false)
                                                        {
                                                            result.Warnings.Add($"Cannot import the following identity: '{reader}'. Skipping.");
                                                        }
                                                        else
                                                        {
                                                            identities[identityKey] = identityValue;
                                                        }
                                                    }
                                                    catch (Exception e)
                                                    {
                                                        result.Warnings.Add($"Cannot import the following identity: '{reader}'. Error: {e}. Skipping.");
                                                    }
                                                }
                                            }
                                            break;
                                        default:
                                            result.Warnings.Add(
                                                $"The following type is not recognized: '{operateOnType}'. Skipping.");
                                            break;
                                    }
                                }
                            }
                            break;
                        case JsonParserToken.EndArray:
                            switch (operateOnType)
                            {
                                case "Docs":
                                    await FinishBatchOfDocuments();
                                    _batchPutCommand = new MergedBatchPutCommand(_database, buildVersion);

                                    // We are taking a reference here since the documents import can activate or disable the versioning.
                                    // We holad a local copy because the user can disable the bundle during the import process, exteranly.
                                    // In this case we want to continue to import the revisions documents.
                                    versioningStorage = _database.BundleLoader.VersioningStorage;
                                    _batchPutCommand.IsRevision = true;
                                    break;
                                case "RevisionDocuments":
                                    await FinishBatchOfDocuments();
                                    break;
                                case "Identities":
                                    if (identities.Count > 0)
                                    {
                                        using (var tx = context.OpenWriteTransaction())
                                        {
                                            _database.DocumentsStorage.UpdateIdentities(context, identities);
                                            tx.Commit();
                                        }
                                    }
                                    identities = null;
                                    break;
                            }
                            break;
                    }
                }
            }

            return result;
        }

        private static TransformerDefinition ReadLegacyTransformerDefinition(BlittableJsonReaderObject reader)
        {
            string name;
            if (reader.TryGet("name", out name) == false)
                throw new InvalidOperationException("Could not read legacy index definition.");

            BlittableJsonReaderObject definition;
            if (reader.TryGet("definition", out definition) == false)
                throw new InvalidOperationException("Could not read legacy index definition.");

            var transformerDefinition = JsonDeserializationServer.TransformerDefinition(definition);
            transformerDefinition.Name = name;

            return transformerDefinition;
        }

        private static IndexDefinition ReadLegacyIndexDefinition(BlittableJsonReaderObject reader)
        {
            string name;
            if (reader.TryGet("name", out name) == false)
                throw new InvalidOperationException("Could not read legacy index definition.");

            BlittableJsonReaderObject definition;
            if (reader.TryGet("definition", out definition) == false)
                throw new InvalidOperationException("Could not read legacy index definition.");

            var legacyIndexDefinition = JsonDeserializationServer.LegacyIndexDefinition(definition);

            var indexDefinition = new IndexDefinition
            {
                IndexId = legacyIndexDefinition.IndexId,
                IndexVersion = legacyIndexDefinition.IndexVersion,
                LockMode = legacyIndexDefinition.LockMode,
                Maps = legacyIndexDefinition.Maps,
                MaxIndexOutputsPerDocument = legacyIndexDefinition.MaxIndexOutputsPerDocument,
                Name = name,
                Reduce = legacyIndexDefinition.Reduce
            };

            foreach (var kvp in legacyIndexDefinition.Analyzers)
            {
                if (indexDefinition.Fields.ContainsKey(kvp.Key) == false)
                    indexDefinition.Fields[kvp.Key] = new IndexFieldOptions();

                indexDefinition.Fields[kvp.Key].Analyzer = kvp.Value;
            }

            foreach (var kvp in legacyIndexDefinition.Indexes)
            {
                if (indexDefinition.Fields.ContainsKey(kvp.Key) == false)
                    indexDefinition.Fields[kvp.Key] = new IndexFieldOptions();

                indexDefinition.Fields[kvp.Key].Indexing = kvp.Value;
            }

            foreach (var kvp in legacyIndexDefinition.SortOptions)
            {
                if (indexDefinition.Fields.ContainsKey(kvp.Key) == false)
                    indexDefinition.Fields[kvp.Key] = new IndexFieldOptions();

                SortOptions sortOptions;
                switch (kvp.Value)
                {
                    case LegacyIndexDefinition.LegacySortOptions.None:
                        sortOptions = SortOptions.None;
                        break;
                    case LegacyIndexDefinition.LegacySortOptions.String:
                        sortOptions = SortOptions.String;
                        break;
                    case LegacyIndexDefinition.LegacySortOptions.Short:
                    case LegacyIndexDefinition.LegacySortOptions.Long:
                    case LegacyIndexDefinition.LegacySortOptions.Int:
                    case LegacyIndexDefinition.LegacySortOptions.Byte:
                        sortOptions = SortOptions.NumericDefault;
                        break;
                    case LegacyIndexDefinition.LegacySortOptions.Float:
                    case LegacyIndexDefinition.LegacySortOptions.Double:
                        sortOptions = SortOptions.NumericDouble;
                        break;
                    case LegacyIndexDefinition.LegacySortOptions.Custom:
                        throw new NotImplementedException(kvp.Value.ToString());
                    case LegacyIndexDefinition.LegacySortOptions.StringVal:
                        sortOptions = SortOptions.StringVal;
                        break;
                    default:
                        throw new NotSupportedException(kvp.Value.ToString());
                }

                indexDefinition.Fields[kvp.Key].Sort = sortOptions;
            }

            foreach (var kvp in legacyIndexDefinition.SpatialIndexes)
            {
                if (indexDefinition.Fields.ContainsKey(kvp.Key) == false)
                    indexDefinition.Fields[kvp.Key] = new IndexFieldOptions();

                indexDefinition.Fields[kvp.Key].Spatial = kvp.Value;
            }

            foreach (var kvp in legacyIndexDefinition.Stores)
            {
                if (indexDefinition.Fields.ContainsKey(kvp.Key) == false)
                    indexDefinition.Fields[kvp.Key] = new IndexFieldOptions();

                indexDefinition.Fields[kvp.Key].Storage = kvp.Value;
            }

            foreach (var kvp in legacyIndexDefinition.TermVectors)
            {
                if (indexDefinition.Fields.ContainsKey(kvp.Key) == false)
                    indexDefinition.Fields[kvp.Key] = new IndexFieldOptions();

                indexDefinition.Fields[kvp.Key].TermVector = kvp.Value;
            }

            foreach (var field in legacyIndexDefinition.SuggestionsOptions)
            {
                if (indexDefinition.Fields.ContainsKey(field) == false)
                    indexDefinition.Fields[field] = new IndexFieldOptions();

                indexDefinition.Fields[field].Suggestions = true;
            }

            return indexDefinition;
        }

        private async Task FinishBatchOfDocuments()
        {
            if (_prevCommand != null)
            {
                using (_prevCommand)
                {
                    await _prevCommandTask;
                }
                _prevCommand = null;
            }

            if (_batchPutCommand.Documents.Count > 0)
            {
                using (_batchPutCommand)
                {
                    await _database.TxMerger.Enqueue(_batchPutCommand);
                }
            }
            _batchPutCommand = null;
        }

        private async Task HandleBatchOfDocuments(DocumentsOperationContext context, UnmanagedJsonParser parser, long buildVersion)
        {
            if (_batchPutCommand.Documents.Count >= 16)
            {
                if (_prevCommand != null)
                {
                    using (_prevCommand)
                    {
                        await _prevCommandTask;
                        ResetContextAndParser(context, parser);
                    }
                }
                _prevCommandTask = _database.TxMerger.Enqueue(_batchPutCommand);
                _prevCommand = _batchPutCommand;
                _batchPutCommand = new MergedBatchPutCommand(_database, buildVersion);
            }
        }

        private static void ResetContextAndParser(DocumentsOperationContext context, UnmanagedJsonParser parser)
        {
            parser.ResetStream();
            context.Reset();
            parser.SetStream();
        }

        private class MergedBatchPutCommand : TransactionOperationsMerger.MergedTransactionCommand, IDisposable
        {
            public bool IsRevision;

            private readonly DocumentDatabase _database;
            private readonly long _buildVersion;

            public readonly List<BlittableJsonReaderObject> Documents = new List<BlittableJsonReaderObject>();
            private readonly IDisposable _resetContext;
            private readonly DocumentsOperationContext _context;

            public MergedBatchPutCommand(DocumentDatabase database, long buildVersion)
            {
                _database = database;
                _buildVersion = buildVersion;
                _resetContext = _database.DocumentsStorage.ContextPool.AllocateOperationContext(out _context);
            }

            public override void Execute(DocumentsOperationContext context, RavenTransaction tx)
            {
                foreach (var document in Documents)
                {
                    BlittableJsonReaderObject metadata;
                    if (document.TryGet(Constants.Metadata, out metadata) == false)
                        throw new InvalidOperationException("A document must have a metadata");
                    // We are using the id term here and not key in order to be backward compatiable with old export files.
                    string key;
                    if (metadata.TryGet(Constants.MetadataDocId, out key) == false)
                        throw new InvalidOperationException("Document's metadata must include the document's key.");

                    DynamicJsonValue mutatedMetadata;
                    metadata.Modifications = mutatedMetadata = new DynamicJsonValue(metadata);
                    mutatedMetadata.Remove(Constants.MetadataDocId);
                    mutatedMetadata.Remove(Constants.MetadataEtagId);

                    if (IsRevision)
                    {
                        long etag;
                        if (metadata.TryGet(Constants.MetadataEtagId, out etag) == false)
                            throw new InvalidOperationException("Document's metadata must include the document's key.");

                        _database.BundleLoader.VersioningStorage.PutDirect(context, key, etag, document);
                    }
                    else if (_buildVersion < 4000 && key.Contains("/revisions/"))
                    {
                        long etag;
                        if (metadata.TryGet(Constants.MetadataEtagId, out etag) == false)
                            throw new InvalidOperationException("Document's metadata must include the document's key.");

                        var endIndex = key.IndexOf("/revisions/", StringComparison.OrdinalIgnoreCase);
                        key = key.Substring(0, endIndex);

                        _database.BundleLoader.VersioningStorage.PutDirect(context, key, etag, document);
                    }
                    else
                    {
                        _database.DocumentsStorage.Put(context, key, null, document);
                    }
                }
            }

            public void Dispose()
            {
                _resetContext.Dispose();
            }

            public unsafe void Add(BlittableJsonReaderObject doc)
            {
                var mem = _context.GetMemory(doc.Size);
                Memory.Copy((byte*)mem.Address, doc.BasePointer, doc.Size);
                Documents.Add(new BlittableJsonReaderObject((byte*)mem.Address, doc.Size, _context));
            }
        }
    }
}