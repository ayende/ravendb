using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.ETL;
using Raven.Server.Documents.AI.AiGen;
using Raven.Server.Documents.ETL.Metrics;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;
using Raven.Server.Documents.ETL.Providers.AI.Enumerators;
using Raven.Server.Documents.ETL.Providers.AI.GenAi.Test;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Utils;
using Encoding = System.Text.Encoding;
using PatchRequest = Raven.Server.Documents.Patch.PatchRequest;

#pragma warning disable SKEXP0001

namespace Raven.Server.Documents.ETL.Providers.AI.GenAi;

public sealed class GenAiTask : EtlProcess<AiEtlItem, GenAiScriptResult, GenAiConfiguration, AiConnectionString,
    GenAiStatsScope, GenAiPerformanceOperation>
{
    private const string GenAiTaskTag = "AI/Gen";

    private int _fallbackCounter = 0;
    private readonly ChatCompletionClient _chatCompletionClient;


    public GenAiTask(Transformation transformation, GenAiConfiguration configuration, DocumentDatabase database, ServerStore serverStore)
        : base(transformation, configuration, database, serverStore, GenAiTaskTag)
    {
        Metrics = new EtlMetricsCountersManager();
        _chatCompletionClient = GetClient(configuration);
    }

    private static ChatCompletionClient GetClient(GenAiConfiguration cfg)
    {
        var connectorType = cfg.Connection.GetActiveProvider();
        var (uri, model, apiKey) = connectorType switch
        {
            AiConnectorType.Ollama => (cfg.Connection.OllamaSettings.Uri, cfg.Connection.OllamaSettings.Model, (string)null),
            AiConnectorType.OpenAi => (cfg.Connection.OpenAiSettings.Endpoint, cfg.Connection.OpenAiSettings.Model, null),
            _ => throw new NotSupportedException(connectorType.ToString())
        };
        return new ChatCompletionClient(new Uri(uri), model, apiKey, cfg.JsonSchema);
    }

    public override EtlType EtlType => EtlType.GenAi;
    public override bool ShouldTrackCounters() => false;
    public override bool ShouldTrackTimeSeries() => false;

    protected override bool ShouldTrackAttachmentTombstones() => false;

    protected override IEnumerator<AiEtlItem> ConvertDocsEnumerator(DocumentsOperationContext context, IEnumerator<Document> docs, string collection)
    {
        return new DocumentsToEmbeddingsGenerationItems(docs, collection);
    }

    protected override IEnumerator<AiEtlItem> ConvertTombstonesEnumerator(DocumentsOperationContext context, IEnumerator<Tombstone> tombstones, string collection,
        bool trackAttachments)
    {
        return new TombstonesToEmbeddingsGenerationItems(tombstones, collection);
    }

    protected override IEnumerator<AiEtlItem> ConvertAttachmentTombstonesEnumerator(DocumentsOperationContext context, IEnumerator<Tombstone> tombstones,
        List<string> collections)
    {
        throw new NotSupportedException($"{nameof(ConvertAttachmentTombstonesEnumerator)} is not supported for {nameof(EmbeddingsGenerationTask)}");
    }

    protected override IEnumerator<AiEtlItem> ConvertCountersEnumerator(DocumentsOperationContext context, IEnumerator<CounterGroupDetail> counters,
        string collection)
    {
        throw new NotSupportedException($"{nameof(ConvertCountersEnumerator)} is not supported for {nameof(EmbeddingsGenerationTask)}");
    }

    protected override IEnumerator<AiEtlItem> ConvertTimeSeriesEnumerator(DocumentsOperationContext context, IEnumerator<TimeSeriesSegmentEntry> timeSeries,
        string collection)
    {
        throw new NotSupportedException($"{nameof(ConvertTimeSeriesEnumerator)} is not supported for {nameof(EmbeddingsGenerationTask)}");
    }

    protected override IEnumerator<AiEtlItem> ConvertTimeSeriesDeletedRangeEnumerator(DocumentsOperationContext context,
        IEnumerator<TimeSeriesDeletedRangeItem> timeSeries, string collection)
    {
        throw new NotSupportedException($"{nameof(ConvertTimeSeriesDeletedRangeEnumerator)} is not supported for {nameof(EmbeddingsGenerationTask)}");
    }

    protected override EtlTransformer<AiEtlItem, GenAiScriptResult, GenAiStatsScope, GenAiPerformanceOperation>
        GetTransformer(DocumentsOperationContext context)
    {
        return new GenAiScriptTransformer(Database, context, Transformation, null, Configuration);
    }

    protected override string LoadFailureMessage => $"Failed to generate embeddings in '{Configuration.Name}' task. Going to do the retry in {FallbackTime} (failure #{_fallbackCounter}).";

    protected override void EnterFallbackMode(Exception e, DateTime? lastErrorTime)
    {
        // rate limits in embeddings are usually expressed as requests per minute or tokens per minute
        // and they are reset on each full minute ticks, so we'll wait until the next full minute to retry
        // at a minimum
        int secondsToWaitToNextMinute = 60 - DateTime.UtcNow.Second;

        var secondsToWait = ++_fallbackCounter switch
        {
            // first - we'll wait for the next minute each time - 5 minutes total 
            < 5 => secondsToWaitToNextMinute,
            // then - we'll wait for ~two minutes - 10 minutes total
            < 10 => secondsToWaitToNextMinute + 60,
            // then - we'll wait for three minutes - 30 minutes
            < 20 => secondsToWaitToNextMinute + 120,
            // finally - we'll use log2 minutes - so at 20+ failures, wait 5 minutes - 1 hour total
            // then after 32 failures, wait 6 minutes each time - 3.2 hours, etc...
            _ => secondsToWaitToNextMinute + ((int)Math.Log2(_fallbackCounter) * 60)
        };

        double max = Database.Configuration.Ai.EmbeddingsGenerationMaxFallbackTime.AsTimeSpan.TotalSeconds;
        FallbackTime = TimeSpan.FromSeconds(Math.Min(secondsToWait, max));
    }

    protected override int LoadInternal(IEnumerable<GenAiScriptResult> items, DocumentsOperationContext context, GenAiStatsScope scope)
    {
        var results = SendToModel(items, context, out var exceptions);

        if (results.Count is not 0)
        {
            List<Task> patches = [];
            PatchRequest req = new(Configuration.Update, PatchRequestType.AiGen);

            foreach (var item in results)
            {
                if (item?.ModelOutput is null)
                    continue;

                var dvj = new DynamicJsonValue { ["output"] = item.ModelOutput, ["aiHash"] = item.AiHash, ["input"] = item.Context, };
                var args = context.ReadObject(dvj, item.DocId);
                var cmd = new PatchDocumentCommand(
                    context: context,
                    id: item.DocId,
                    expectedChangeVector: null,
                    skipPatchIfChangeVectorMismatch: false,
                    patch: (req, args),
                    patchIfMissing: default,
                    createIfMissing: null,
                    identityPartsSeparator: Database.IdentityPartsSeparator,
                    isTest: Configuration.TestMode,
                    debugMode: false,
                    collectResultsNeeded: false,
                    returnDocument: false,
                    ignoreMaxStepsForScript: false);
                patches.Add(Database.TxMerger.Enqueue(cmd));
            }

            try
            {
                Task.WaitAll(patches.OfType<Task>().ToArray()); // TODO: yuck
            }
            catch (Exception e)
            {
                if (exceptions is null)
                    throw;
                exceptions.Add(e);
            }
        }
        if (exceptions is not null)
            throw new AggregateException(exceptions);

        return results.Count;
    }

    protected override GenAiStatsScope CreateScope(EtlRunStats stats)
    {
        return new GenAiStatsScope(stats);
    }

    protected override string StatsAggregatorTag => "Embeddings Generation";

    protected override bool ShouldFilterOutHiLoDocument()
    {
        return true;
    }

    public GenAiTestScriptResult RunTest(Document document, IEnumerable<GenAiScriptResult> records, DocumentsOperationContext context)
    {
        // we close the read tx in SendToModel, so we need to clone this document
        using (var old = document)
        {
            document = document.Clone(context);
        }

        var results = SendToModel(records, context, out List<Exception> exceptions);
        
        using var _ = context.OpenWriteTransaction();
        BlittableJsonReaderObject outputDocument = null;
        if (results.Count is not 0)
        {
            PatchRequest req = new(Configuration.Update, PatchRequestType.AiGen);

            PatchDocumentCommand lastPatch = null;
            foreach (var item in results)
            {
                if (item?.ModelOutput is null)
                    continue;

                var dvj = new DynamicJsonValue { ["output"] = item.ModelOutput, ["aiHash"] = item.AiHash, ["input"] = item.Context };
                var args = context.ReadObject(dvj, document.Id);
                var cmd = lastPatch = new PatchDocumentCommand(
                    context: context,
                    id: document.Id,
                    expectedChangeVector: null,
                    skipPatchIfChangeVectorMismatch: false,
                    patch: (req, args),
                    patchIfMissing: default,
                    createIfMissing: null,
                    identityPartsSeparator: Database.IdentityPartsSeparator,
                    isTest: false,
                    debugMode: false,
                    collectResultsNeeded: true,
                    returnDocument: false,
                    ignoreMaxStepsForScript: false);

                cmd.Execute(context, recordingState: null);

                item.DebugActions = cmd.DebugActions;
                item.DebugOutput = cmd.DebugOutput;
            }

            outputDocument = lastPatch?.PatchResult.ModifiedDocument;
        }

        if (exceptions is not null)
            throw new AggregateException(exceptions);

        return new GenAiTestScriptResult
        {
            InputDocument = document.Data,
            Results = results,
            OutputDocument = outputDocument,
            TransformationErrors = Statistics.TransformationErrorsInCurrentBatch.Errors.ToList()
        };
    }

    private List<GenAiResultItem> SendToModel(IEnumerable<GenAiScriptResult> records, DocumentsOperationContext context, out List<Exception> exceptions)
    {
        context.CloseTransaction();

        exceptions = null;
        List<Task<(string Result, string Usage)>> tasks = [];
        List<GenAiResultItem> results = [];

        foreach (var item in records)
        {
            var singleResult = new GenAiResultItem { Context = item.Context, DocId = item.DocumentId};
            results.Add(singleResult);

            string json = item.Context.ToString();
            string hash = AttachmentsStorageHelper.CalculateHash(MemoryMarshal.AsBytes(json.AsSpan()));
            if (item.AiHash == hash)
            {
                singleResult.IsCached = true;
                continue; // no change, can skip
            }

            singleResult.AiHash = hash;
            tasks.Add(_chatCompletionClient.CompleteAsync(Configuration.Prompt, json));
        }

        try
        {
            // TODO: Yuck
            Task.WaitAll(tasks.OfType<Task>().ToArray());
        }
        catch
        {
            // we'll handle that later
        }

        for (int index = 0; index < tasks.Count; index++)
        {
            var task = tasks[index];
            var item = results[index];
            if (task.IsCompletedSuccessfully is false)
            {
                exceptions ??= [];
                exceptions.Add(task.Exception);

                continue; // so we won't try to save it 
            }

            (string result, string usage) = task.Result;
            // TODO: report usage

            //TODO: REALLY YUCKY!
            var stream = new ReadOnlyMemoryStream<byte>(Encoding.UTF8.GetBytes(result));
            item.ModelOutput = context.ReadForMemoryAsync(stream, item.DocId).GetAwaiter().GetResult();
            stream.Dispose();

            if (Configuration.TestMode == false) 
                continue;

            stream = new ReadOnlyMemoryStream<byte>(Encoding.UTF8.GetBytes(usage));
            item.Usage = context.ReadForMemoryAsync(stream, item.DocId).GetAwaiter().GetResult();
            stream.Dispose();
        }

        return results;
    }
}
