using System;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Context;
using Raven.Server.TrafficWatch;
using Sparrow.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using PatchRequest = Raven.Server.Documents.Patch.PatchRequest;
using System.Runtime.CompilerServices;
using System.Threading;
using Voron.Data.RawData;
using Voron.Global;
using Voron.Impl;
using Voron.Impl.Paging;

namespace Raven.Server.Documents.Handlers.Processors.Documents;

internal sealed class DocumentHandlerProcessorForPatch : AbstractDocumentHandlerProcessorForPatch<DocumentHandler, DocumentsOperationContext>
{
    public DocumentHandlerProcessorForPatch([NotNull] DocumentHandler requestHandler) : base(requestHandler)
    {
    }

    // The merger applies every write on one thread, and patching reads the document first. A document that
    // is not in memory would page-fault on that thread and stall every write queued behind it. Reading it
    // here moves the page fault to the request thread, where it overlaps with other requests.
    // The update also removes the document's current etag from the etag indexes; for a document that was not
    // written recently, those entries sit on cold pages, so we seek to them as well.
    // A script that only assigns arguments to fields needs no script engine: we build the patched document here,
    // and the merger only writes it (if the document did not change in between).
    private void PrepareOutsideOfMerger(DocumentsOperationContext context, string id, PatchDocumentCommand command, SimpleAssignmentPatch simplePatch, BlittableJsonReaderObject args)
    {
        var storage = RequestHandler.Database.DocumentsStorage;
        using (context.OpenReadTransaction())
        using (var document = storage.Get(context, id, simplePatch == null ? DocumentFields.Data : DocumentFields.All, throwOnConflict: false))
        {
            if (document == null)
                return;

            foreach (var _ in storage.GetDocumentsFrom(context, document.Etag, start: 0, take: 1, DocumentFields.Id))
                break;

            var collection = CollectionName.GetCollectionName(document.Data);
            foreach (var _ in storage.GetDocumentsFrom(context, collection, document.Etag, start: 0, take: 1, DocumentFields.Id))
                break;

            // The merger copies every page it modifies in full, but reading an entry faults in only the 4 KB around it
            // (random access advice), so we touch the rest of the document's page, and the section header page that a
            // relocation frees the old entry into.
            var llt = context.Transaction.InnerTransaction.LowLevelTransaction;
            var documentPage = document.StorageId / Constants.Storage.PageSize;
            var sectionPage = document.StorageId % Constants.Storage.PageSize != 0 ? RawDataSection.GetSectionPageNumber(llt, document.StorageId) : -1;
            if (Pager.PrefetchRingEnabled)
            {
                // fire and forget: the read races the merger instead of holding this request
                llt.DataPager.PrefetchPagesAsync(documentPage);
                if (sectionPage >= 0)
                    llt.DataPager.PrefetchPagesAsync(sectionPage);
            }
            else
            {
                TouchPage(llt, documentPage);
                if (sectionPage >= 0)
                    TouchPage(llt, sectionPage);
            }

            if (simplePatch == null || document.Data == null)
                return;

            if (simplePatch.TryApply(context, document.Id, document.Data, args, out var modified) == false)
                return;

            if (modified == null)
                command.CompleteAsNotModified();
            else
                command.SetPrepared(document.Id, document.ChangeVector, document.Flags, modified);
        }
    }

    private static unsafe void TouchPage(LowLevelTransaction llt, long pageNumber)
    {
        var page = llt.GetPage(pageNumber);
        var size = page.IsOverflow ? Paging.GetNumberOfOverflowPages(page.OverflowSize) * Constants.Storage.PageSize : Constants.Storage.PageSize;
        for (var offset = 0; offset < size; offset += 4096)
            Volatile.Read(ref page.Pointer[offset]);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    protected override async ValueTask HandleDocumentPatchAsync(string id, string cv, BlittableJsonReaderObject patchRequest, bool skipPatchIfChangeVectorMismatch, bool debugMode,
        bool isTest, DocumentsOperationContext context)
    {
        var changeVector = context.GetLazyString(cv);

        patchRequest.TryGet("Patch", out BlittableJsonReaderObject patchCmd);
        patchRequest.TryGet("PatchIfMissing", out BlittableJsonReaderObject patchIfMissingCmd);
        patchRequest.TryGet("CreateIfMissing", out BlittableJsonReaderObject createIfMissingCmd);

        var patch = PatchRequest.Parse(patchCmd, out var patchArgs);

        PatchRequest patchIfMissing = null;
        BlittableJsonReaderObject patchIfMissingArgs = null;

        if (patchIfMissingCmd != null)
            patchIfMissing = PatchRequest.Parse(patchIfMissingCmd, out patchIfMissingArgs);

        var command = new PatchDocumentCommand(context,
            id,
            changeVector,
            skipPatchIfChangeVectorMismatch,
            (patch, patchArgs),
            (patchIfMissing, patchIfMissingArgs),
            createIfMissingCmd,
            RequestHandler.Database.IdentityPartsSeparator,
            isTest,
            debugMode,
            true,
            returnDocument: false
        );

        if (isTest == false)
        {
            var simplePatch = debugMode == false && changeVector == null && patchIfMissing == null && createIfMissingCmd == null
                ? SimpleAssignmentPatch.TryGet(patch)
                : null;

            if (simplePatch != null)
                command.SetSimplePatch(simplePatch);
            PrepareOutsideOfMerger(context, id, command, simplePatch, patchArgs);

            if (command.PatchResult == null)
                await RequestHandler.Database.TxMerger.Enqueue(command);
        }
        else
        {
            // PutDocument requires the write access to the docs storage
            // testing patching is rare enough not to optimize it
            using (context.OpenWriteTransaction())
            {
                command.Execute(context, null);
            }
        }

        switch (command.PatchResult.Status)
        {
            case PatchStatus.DocumentDoesNotExist:
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return;

            case PatchStatus.Created:
                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;
                break;

            case PatchStatus.Skipped:
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                return;

            case PatchStatus.Patched:
            case PatchStatus.NotModified:
                HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;
                break;

            default:
                throw new ArgumentOutOfRangeException();
        }

        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
        {
            writer.WriteStartObject();

            writer.WritePropertyName(nameof(command.PatchResult.Status));
            writer.WriteString(command.PatchResult.Status.ToString());
            writer.WriteComma();

            writer.WritePropertyName(nameof(command.PatchResult.ModifiedDocument));
            writer.WriteObject(command.PatchResult.ModifiedDocument);

            if (debugMode)
            {
                writer.WriteComma();
                writer.WritePropertyName(nameof(command.PatchResult.OriginalDocument));
                if (isTest)
                    writer.WriteObject(command.PatchResult.OriginalDocument);
                else
                    writer.WriteNull();

                writer.WriteComma();

                writer.WritePropertyName(nameof(command.PatchResult.Debug));

                context.Write(writer, new DynamicJsonValue
                {
                    ["Info"] = new DynamicJsonArray(command.DebugOutput),
                    ["Actions"] = command.DebugActions
                });
            }

            switch (command.PatchResult.Status)
            {
                case PatchStatus.Created:
                case PatchStatus.Patched:

                    writer.WriteComma();

                    writer.WritePropertyName(nameof(command.PatchResult.LastModified));
                    writer.WriteString(command.PatchResult.LastModified.GetDefaultRavenFormat());
                    writer.WriteComma();

                    writer.WritePropertyName(nameof(command.PatchResult.ChangeVector));
                    writer.WriteString(command.PatchResult.ChangeVector);
                    writer.WriteComma();

                    writer.WritePropertyName(nameof(command.PatchResult.Collection));
                    writer.WriteString(command.PatchResult.Collection);
                    break;
            }

            writer.WriteEndObject();

            if (TrafficWatchManager.HasRegisteredClients)
                RequestHandler.AddStringToHttpContext(writer.ToString(), TrafficWatchChangeType.Documents);
        }
    }
}
