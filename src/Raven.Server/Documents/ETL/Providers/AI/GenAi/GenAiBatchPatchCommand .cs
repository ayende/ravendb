using System;
using System.Collections.Generic;
using Raven.Client;
using Raven.Server.Documents.Handlers.Batches;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using PatchRequest = Raven.Server.Documents.Patch.PatchRequest;

namespace Raven.Server.Documents.ETL.Providers.AI.GenAi;

internal sealed class GenAiBatchPatchCommand : PatchDocumentCommandBase
{
    private readonly List<GenAiResultItem> _items;
    private readonly PatchRequest _patchRequest;
    private readonly string _taskName;

    public GenAiBatchPatchCommand(
        DocumentsOperationContext context,
        List<GenAiResultItem> items,
        PatchRequest patchRequest,
        string taskName)
        : base(
              context,
              skipPatchIfChangeVectorMismatch: false,
              patch: default,
              patchIfMissing: default,
              createIfMissing: null,
              isTest: false,
              debugMode: false,
              collectResultsNeeded: true,
              returnDocument: false)
    {
        _items = items;
        _patchRequest = patchRequest;
        _taskName = taskName;
        _database = context.DocumentDatabase;
    }

    protected override long ExecuteCmd(DocumentsOperationContext context)
    {
        var hashes = new Dictionary<string, (BlittableJsonReaderObject Doc, DynamicJsonArray Hashes)>();

        using (_database.Scripts.GetScriptRunner(_patchRequest, readOnly: false, out var runner))
        {
            foreach (var item in _items)
            {
                if (hashes.TryGetValue(item.DocId, out var tuple) == false)
                {
                    tuple = (null, new DynamicJsonArray());
                }
                tuple.Hashes.Add(item.ContextOutput.AiHash);

                if (item.ModelOutput is null)
                    continue; 

                _patch = (_patchRequest, CreatePatchArgs(context, item));

                var patchResult = ExecuteOnDocument(context, item.DocId, expectedChangeVector: null, runner, runIfMissing: null);
                tuple.Doc = patchResult.ModifiedDocument;
                hashes[item.DocId] = tuple;
            }
        }

        // update metadata for each doc in same transaction
        foreach (var kvp in hashes)
        {
            var doc = kvp.Value.Doc;
            if (doc == null)
                continue; // document was deleted?

            UpdateHashesInMetadata(kvp.Key, doc, _taskName, kvp.Value.Hashes, context);
        }

        return _items.Count;
    }

    private static BlittableJsonReaderObject CreatePatchArgs(DocumentsOperationContext context, GenAiResultItem item)
    {
        var djv = new DynamicJsonValue
        {
            ["output"] = item.ModelOutput.Output,
            ["input"] = item.ContextOutput.Context
        };

        return context.ReadObject(djv, item.DocId);
    }

    internal static BlittableJsonReaderObject UpdateHashesInMetadata(string id, BlittableJsonReaderObject doc, string taskName, DynamicJsonArray allHashes, DocumentsOperationContext context)
    {
        if (doc.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false)
        {
            // no metadata at all (shouldn't happen)

            doc.Modifications = new DynamicJsonValue(doc)
            {
                [Constants.Documents.Metadata.Key] = new DynamicJsonValue
                {
                    [GenAiTask.GenAiHashesMetadataKey] = new DynamicJsonValue
                    {
                        [taskName] = allHashes
                    }
                }
            };
        }

        else if (metadata.TryGet(GenAiTask.GenAiHashesMetadataKey, out BlittableJsonReaderObject hashes) == false)
        {
            // no hashes section

            metadata.Modifications = new DynamicJsonValue(metadata)
            {
                [GenAiTask.GenAiHashesMetadataKey] = new DynamicJsonValue
                {
                    [taskName] = allHashes
                }
            };
            doc.Modifications = new DynamicJsonValue(doc)
            {
                [Constants.Documents.Metadata.Key] = metadata
            };
        }

        else
        {
            // we already have the hashes section, need to modify it

            hashes.Modifications = new DynamicJsonValue(hashes)
            {
                [taskName] = allHashes
            };

            metadata.Modifications = new DynamicJsonValue(metadata)
            {
                [GenAiTask.GenAiHashesMetadataKey] = hashes
            };

            doc.Modifications = new DynamicJsonValue(doc)
            {
                [Constants.Documents.Metadata.Key] = metadata
            };
        }

        using (var old = doc)
        {
            doc = context.ReadObject(doc, id);
        }

        context.DocumentDatabase.DocumentsStorage.Put(context, id, expectedChangeVector: null, doc);

        return doc;
    }

    public override string HandleReply(DynamicJsonArray reply, HashSet<string> modifiedCollections)
    {
        // TODO

        reply?.Add(new DynamicJsonValue
        {
            [nameof(BatchRequestParser.CommandData.Type)] = "GenAiBatchPATCH"
        });

        return null;
    }

    public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, DocumentMergedTransactionCommand> ToDto(DocumentsOperationContext context)
    {
        throw new NotSupportedException("Replay not supported for GenAiBatchPatchCommand");
    }
}

