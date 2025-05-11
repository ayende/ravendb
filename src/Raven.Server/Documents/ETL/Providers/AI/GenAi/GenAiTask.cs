using System;
using System.Collections.Generic;
using System.Linq;
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
using Raven.Server.Documents.ETL.Test;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
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

    internal const string GenAiHashesMetadataKey = "@genAiHashes";

    private const string TestDocumentId = "GenAi/TestDocument";
    private const string GenAiTaskTag = "AI/Gen";
    private int _fallbackCounter = 0;
    private ChatCompletionClient _chatCompletionClient;

    public GenAiTask(Transformation transformation, GenAiConfiguration configuration, DocumentDatabase database, ServerStore serverStore)
        : base(transformation, configuration, database, serverStore, GenAiTaskTag)
    {
        Metrics = new EtlMetricsCountersManager();

        if (configuration.TestMode == false)
            _chatCompletionClient = GetClient(configuration);
    }

    private static ChatCompletionClient GetClient(GenAiConfiguration cfg)
    {
        var connectorType = cfg.Connection.GetActiveProvider();
        var (uri, model, apiKey) = connectorType switch
        {
            AiConnectorType.Ollama => (cfg.Connection.OllamaSettings.Uri, cfg.Connection.OllamaSettings.Model, (string)null),
            AiConnectorType.OpenAi => (cfg.Connection.OpenAiSettings.Endpoint, cfg.Connection.OpenAiSettings.Model, cfg.Connection.OpenAiSettings.ApiKey),
            _ => throw new NotSupportedException(connectorType.ToString())
        };

        if (string.IsNullOrWhiteSpace(cfg.JsonSchema))
        {
            cfg.JsonSchema = ChatCompletionClient.GetSchemaFor(cfg.SampleObject);
        }

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
        var results = PrepareItemsBeforeSendingToModel(items);
        if (results.Count is 0)
            return 0;

        var exceptions = SendToModel(results, context);

        ApplyUpdateScript(context, results);

        if (exceptions is not null)
            throw new AggregateException(exceptions);

        return results.Count;
    }

    private void ApplyUpdateScript(DocumentsOperationContext context, List<GenAiResultItem> results)
    {
        PatchRequest req = new(Configuration.Update, PatchRequestType.AiGen);
        var cmd = new GenAiBatchPatchCommand(context, results, req, Configuration.Name);

        Database.TxMerger.Enqueue(cmd).GetAwaiter().GetResult();
    }

    protected override GenAiStatsScope CreateScope(EtlRunStats stats)
    {
        return new GenAiStatsScope(stats);
    }

    protected override string StatsAggregatorTag => "Generative AI";

    protected override bool ShouldFilterOutHiLoDocument()
    {
        return true;
    }

    private List<Exception> SendToModel(List<GenAiResultItem> items, DocumentsOperationContext context)
    {
        context.CloseTransaction();

        List<Exception> exceptions = null;
        List<Task<(string Result, string Usage)>> tasks = [];

        foreach (var item in items)
        {
            if (item.ContextOutput.IsCached)
                continue; // no change, can skip
            
            string json = item.ContextOutput.Context.ToString();
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
            var item = items[index];
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
            item.ModelOutput = new ModelOutput
            {
                Output = context.ReadForMemoryAsync(stream, item.DocId).GetAwaiter().GetResult()
            };

            stream.Dispose();

            if (Configuration.TestMode == false)
                continue;

            stream = new ReadOnlyMemoryStream<byte>(Encoding.UTF8.GetBytes(usage));
            item.ModelOutput.Usage = context.ReadForMemoryAsync(stream, item.DocId).GetAwaiter().GetResult();
            stream.Dispose();
        }

        return exceptions;
    }

    public TestEtlScriptResult RunTest(TestGenAiScript testGenAiScript, DocumentsOperationContext context)
    {
        List<GenAiResultItem> items;
        List<Exception> exceptions = null;
        BlittableJsonReaderObject outputDocument = null;

        Document document;
        if (testGenAiScript.Document != null)
        {
            document = new Document
            {
                Data = testGenAiScript.Document, 
                ChangeVector = ChangeVectorUtils.NewChangeVector(context.DocumentDatabase.ServerStore.NodeTag, long.MaxValue, context.DocumentDatabase.DbBase64Id),
                Id = context.GetLazyString(TestDocumentId)
            };
        }
        else
        {
            if (string.IsNullOrEmpty(testGenAiScript.DocumentId))
                throw new InvalidOperationException("Document or DocumentId must be provided to run GenAI test");

            context.OpenReadTransaction();
            document = context.DocumentDatabase.DocumentsStorage.Get(context, testGenAiScript.DocumentId)?.Clone(context);
            if (document == null)
                throw new InvalidOperationException($"Document {testGenAiScript.DocumentId} does not exist");
        }

        switch (testGenAiScript.TestStage)
        {
            case TestStage.CreateContextObjects:
                if (context.HasTransaction == false)
                    context.OpenReadTransaction();

                var aiEtlItem = new AiEtlItem(document, Configuration.Collection);
                var transformedResults = Transform([aiEtlItem], context, new GenAiStatsScope(new EtlRunStats()), new EtlProcessState());
                items = PrepareItemsBeforeSendingToModel(transformedResults);

                context.CloseTransaction();
                break;
            case TestStage.SendToModel:
                _chatCompletionClient ??= GetClient(Configuration);
                items = testGenAiScript.Input;
                exceptions = SendToModel(items, context);
                break;
            case TestStage.ApplyUpdateScript:
                {
                    context.CloseTransaction();
                    using var _ = context.OpenWriteTransaction();

                    items = testGenAiScript.Input;
                    PatchRequest req = new(Configuration.Update, PatchRequestType.AiGen);
                    PatchDocumentCommand lastPatch = null;
                    var hashes = new DynamicJsonArray();

                    if (testGenAiScript.Document != null)
                    {
                        // the document that was provided as input does not exist (we gave it a dummy id),
                        // so it needs to be written to storage before the patch.
                        // the write-tx is not commited so this won't be persisted.

                        if (document.Data.HasParent)
                        {
                            using (var old = document.Data)
                            {
                                document.Data = document.Data.Clone(context);
                            }
                        }

                        context.DocumentDatabase.DocumentsStorage.Put(context, document.Id, expectedChangeVector: null, document.Data);
                    }

                    foreach (var item in items)
                    {
                        hashes.Add(item.ContextOutput.AiHash);

                        if (item?.ModelOutput is null)
                            continue;

                        var dvj = new DynamicJsonValue
                        {
                            ["output"] = item.ModelOutput.Output,
                            ["input"] = item.ContextOutput.Context
                        };

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

                    if (lastPatch?.PatchResult?.ModifiedDocument != null)
                        outputDocument = GenAiBatchPatchCommand.UpdateHashesInMetadata(document.Id, lastPatch.PatchResult.ModifiedDocument, Configuration.Name, hashes, context);

                    break;
                }

            default:
                throw new InvalidOperationException("Unknown TestStage type : " + testGenAiScript.TestStage.GetType());
        }

        if (exceptions is not null)
            throw new AggregateException(exceptions);

        return new GenAiTestScriptResult
        {
            InputDocument = document.Data,
            Results = items,
            OutputDocument = outputDocument,
            TransformationErrors = Statistics.TransformationErrorsInCurrentBatch.Errors.ToList()
        };
    }

    private static List<GenAiResultItem> PrepareItemsBeforeSendingToModel(IEnumerable<GenAiScriptResult> items)
    {
        // TODO we can do this in the transform phase 

        var results = new List<GenAiResultItem>();

        foreach (var scriptResult in items)
        {
            var item = new GenAiResultItem
            {
                DocId = scriptResult.DocumentId,

                ContextOutput = new ContextOutput
                {
                    Context = scriptResult.Context,
                    IsCached = scriptResult.IsCached,
                    AiHash = scriptResult.AiHash
                }
            };

            results.Add(item);
        }

        return results;
    }
}
