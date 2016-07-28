using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Smuggler;
using Raven.Server.Documents;
using Raven.Server.Documents.Versioning;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Smuggler
{
    public class DatabaseDataImporter
    {
        private readonly DocumentDatabase _database;

        public DatabaseItemType OperateOnTypes;

        public DatabaseDataImporter(DocumentDatabase database)
        {
            _database = database;
            _batchPutCommand = new MergedBatchPutCommand(_database);
            OperateOnTypes = DatabaseItemType.Indexes | DatabaseItemType.Transformers
                | DatabaseItemType.Documents | DatabaseItemType.RevisionDocuments | DatabaseItemType.Identities;
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
                string operateOnType = "__top_start_object";
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
                                operateOnType = new LazyStringValue(null, state.StringBuffer, state.StringSize, context).ToString();
                            }
                            break;
                        case JsonParserToken.Integer:
                            switch (operateOnType)
                            {
                                case "BuildVersion":
                                    _batchPutCommand.BuildVersion = state.Long;
                                    break;
                            }
                            break;
                        case JsonParserToken.StartObject:
                            if (operateOnType == "__top_start_object")
                            {
                                operateOnType = null;
                                break;
                            }

                            var builder = new BlittableJsonDocumentBuilder(context, BlittableJsonDocumentBuilder.UsageMode.ToDisk, "ImportObject", parser, state);
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
                                _batchPutCommand.Add(builder);
                                await HandleBatchOfDocuments();
                            }
                            else if (operateOnType == "RevisionDocuments" && OperateOnTypes.HasFlag(DatabaseItemType.RevisionDocuments))
                            {
                                if (versioningStorage == null)
                                    break;

                                result.RevisionDocumentsCount++;
                                _batchPutCommand.Add(builder);
                                await HandleBatchOfDocuments();
                            }
                            else
                            {
                                using (builder)
                                {
                                    switch (operateOnType)
                                    {
                                        case "Attachments":
                                            result.Warnings.Add("Attachments are not supported anymore. Use RavenFS isntead. Skipping.");
                                            break;
                                        case "Indexes":
                                            if (OperateOnTypes.HasFlag(DatabaseItemType.Indexes))
                                            {
                                                result.IndexesCount++;

                                                using (var reader = builder.CreateReader())
                                                {
                                                    /*   var index = new IndexDefinition();
                                                       string name;
                                                       if (reader.TryGet("Name", out name) == false)
                                                       {
                                                           result.Warnings.Add($"Cannot import the following index as it does not contain a name: '{reader}'. Skipping.");
                                                       }
                                                       index.Name = name;
                                                       _database.IndexStore.CreateIndex(index);*/
                                                }
                                            }
                                            break;
                                        case "Transformers":
                                            if (OperateOnTypes.HasFlag(DatabaseItemType.Transformers))
                                            {
                                                result.TransformersCount++;

                                                using (var reader = builder.CreateReader())
                                                {
                                                    /* var transformerDefinition = new TransformerDefinition();
                                                     // TODO: Import
                                                     _database.TransformerStore.CreateTransformer(transformerDefinition);*/
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
                                                        if (reader.TryGet("Key", out identityKey) == false ||
                                                            reader.TryGet("Value", out identityValueString) == false ||
                                                            long.TryParse(identityValueString, out identityValue) == false)
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
                                            result.Warnings.Add($"The following type is not recognized: '{operateOnType}'. Skipping.");
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
                                    _batchPutCommand = new MergedBatchPutCommand(_database);

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

            if (_batchPutCommand.Count > 0)
            {
                using (_batchPutCommand)
                {
                    await _database.TxMerger.Enqueue(_batchPutCommand);
                }
            }
            _batchPutCommand = null;
        }

        private async Task HandleBatchOfDocuments()
        {
            if (_batchPutCommand.Count >= 16)
            {
                if (_prevCommand != null)
                {
                    using (_prevCommand)
                    {
                        await _prevCommandTask;
                    }
                }
                _prevCommandTask = _database.TxMerger.Enqueue(_batchPutCommand);
                _prevCommand = _batchPutCommand;
                _batchPutCommand = new MergedBatchPutCommand(_database);
            }
        }

        private class MergedBatchPutCommand : TransactionOperationsMerger.MergedTransactionCommand, IDisposable
        {
            public long BuildVersion;
            public bool IsRevision;

            private readonly DocumentDatabase _database;
            private readonly List<BlittableJsonDocumentBuilder> _buildersToDispose = new List<BlittableJsonDocumentBuilder>();
            private readonly List<BlittableJsonReaderObject> _documents = new List<BlittableJsonReaderObject>();

            public MergedBatchPutCommand(DocumentDatabase database)
            {
                _database = database;
            }

            public int Count
            {
                [MethodImpl(MethodImplOptions.AggressiveInlining)]
                get { return _documents.Count; }
            }


            public override void Execute(DocumentsOperationContext context, RavenTransaction tx)
            {
                foreach (var document in _documents)
                {
                    BlittableJsonReaderObject metadata;
                    if (document.TryGet(Constants.Metadata.MetadataId, out metadata) == false)
                        throw new InvalidOperationException("A document must have a metadata");

                    string id;
                    if (metadata.TryGet(Constants.Metadata.MetadataDocId, out id) == false)
                        throw new InvalidOperationException("Document's metadata must include the document's id.");

                    DynamicJsonValue mutatedMetadata;
                    metadata.Modifications = mutatedMetadata = new DynamicJsonValue(metadata);
                    mutatedMetadata.Remove(Constants.Metadata.MetadataDocId);
                    mutatedMetadata.Remove(Constants.Metadata.MetadataEtagId);

                    if (IsRevision)
                    {
                        long etag;
                        if (metadata.TryGet(Constants.Metadata.MetadataEtagId, out etag) == false)
                            throw new InvalidOperationException("Document's metadata must include the document's key.");

                        _database.BundleLoader.VersioningStorage.PutDirect(context, id, etag, document);
                    }
                    else if (BuildVersion < 4000 && id.Contains("/revisions/"))
                    {
                        long etag;
                        if (metadata.TryGet(Constants.Metadata.MetadataEtagId, out etag) == false)
                            throw new InvalidOperationException("Document's metadata must include the document's key.");

                        var endIndex = id.IndexOf("/revisions/", StringComparison.OrdinalIgnoreCase);
                        id = id.Substring(0, endIndex);

                        _database.BundleLoader.VersioningStorage.PutDirect(context, id, etag, document);
                    }
                    else
                    {
                        _database.DocumentsStorage.Put(context, id, null, document);
                    }
                }
            }

            public void Dispose()
            {
                foreach (var documentBuilder in _buildersToDispose)
                {
                    documentBuilder.Dispose();
                }
                foreach (var documentBuilder in _documents)
                {
                    documentBuilder.Dispose();
                }
            }

            public void Add(BlittableJsonDocumentBuilder documentBuilder)
            {
                _buildersToDispose.Add(documentBuilder);
                var reader = documentBuilder.CreateReader();
                _documents.Add(reader);
            }
        }
    }
}