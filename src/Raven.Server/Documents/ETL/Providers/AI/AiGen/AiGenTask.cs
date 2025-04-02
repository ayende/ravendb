using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.SemanticKernel.Embeddings;
using Parquet.Meta;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.ETL;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.AI.AiGen;
using Raven.Server.Documents.ETL.Metrics;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings.Stats;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings.Test;
using Raven.Server.Documents.ETL.Providers.AI.Enumerators;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Server.Utils;
using Encoding = System.Text.Encoding;

#pragma warning disable SKEXP0001

namespace Raven.Server.Documents.ETL.Providers.AI.AiGen;

public sealed class AiGenTask : EtlProcess<AiEtlItem, AiGenScriptResult, GenAiConfiguration, AiConnectionString,
    AiGenStatsScope, AiGenPerformanceOperation>
{
    private const string EmbeddingsTaskTag = "AI/Embeddings Generation";

    private int _fallbackCounter = 0;
    private ChatCompletionClient _chatCompletionClient;


    public AiGenTask(Transformation transformation, GenAiConfiguration configuration, DocumentDatabase database, ServerStore serverStore)
        : base(transformation, configuration, database, serverStore, EmbeddingsTaskTag)
    {
        Metrics = new EtlMetricsCountersManager();
        _chatCompletionClient = GetClient(configuration);
    }

    private static ChatCompletionClient GetClient(GenAiConfiguration cfg)
    {
         var (uri, model, apiKey) = cfg.Connection.GetActiveProvider() switch
        {
            AiConnectorType.Ollama => (cfg.Connection.OllamaSettings.Uri, cfg.Connection.OllamaSettings.Model, (string)null),
            AiConnectorType.OpenAi => (cfg.Connection.OpenAiSettings.Endpoint, cfg.Connection.OpenAiSettings.Model, null),
            _ => throw new NotSupportedException(cfg.Connection.GetActiveProvider().ToString())
        };
        return new ChatCompletionClient(new Uri(uri), model, apiKey, cfg.JsonSchema);
    }

    public override EtlType EtlType => EtlType.AiGen;
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

    protected override EtlTransformer<AiEtlItem, AiGenScriptResult, AiGenStatsScope, AiGenPerformanceOperation>
        GetTransformer(DocumentsOperationContext context)
    {
        return new AiGenScriptTransformer(Database, context, Transformation, null, Configuration);
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

    protected override int LoadInternal(IEnumerable<AiGenScriptResult> items, DocumentsOperationContext context, AiGenStatsScope scope)
    {
        int count = 0;

        List<Task<(string Result, string Usage)>> tasks = [];
        List<AiGenScriptResult> matchingItems = [];
        
        foreach (var item in items)
        {
            string json = item.Context.ToString();
            string hash = AttachmentsStorageHelper.CalculateHash(MemoryMarshal.AsBytes(json.AsSpan()));
            if(item.AiHash == hash)
                continue; // no change, can skip
            tasks.Add(_chatCompletionClient.CompleteAsync(Configuration.Prompt, json));
            matchingItems.Add(item with { AiHash = hash });
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

        List<Exception> exceptions = null;
        for (int index = 0; index < tasks.Count; index++)
        {
            count++;
            Task<(string Result, string Usage)> task = tasks[index];
            if (task.IsCompletedSuccessfully is false)
            {
                exceptions ??= [];
                exceptions.Add(task.Exception);
                matchingItems[index] = null; // so we won't try to save it 
                continue;
            }

            (string result, string usage) = task.Result;
            // TODO: report usage

            //TODO: REALLY YUCKY!
            var stream = new ReadOnlyMemoryStream<byte>(Encoding.UTF8.GetBytes(result));
            matchingItems[index].Output = context.ReadForMemoryAsync(stream, matchingItems[index].DocumentId).GetAwaiter().GetResult();
        }

        if (matchingItems.Count is not 0)
        {
            List<Task> patches = [];
            PatchRequest req = new(Configuration.Update, PatchRequestType.AiGen);
            foreach (var item in matchingItems)
            {
                if(item is null) continue;
                
                var dvj = new DynamicJsonValue { ["output"] = item.Output, ["aiHash"] = item.AiHash, ["input"] = item.Context, };
                var args = context.ReadObject(dvj, item.DocumentId);
                var cmd = new PatchDocumentCommand(
                    context: context,
                    id: item.DocumentId,
                    expectedChangeVector: null,
                    skipPatchIfChangeVectorMismatch: false,
                    patch: (req, args),
                    patchIfMissing: default,
                    createIfMissing: null,
                    identityPartsSeparator: Database.IdentityPartsSeparator,
                    isTest: false,
                    debugMode: false,
                    collectResultsNeeded: false,
                    returnDocument: false,
                    ignoreMaxStepsForScript: false);
                patches.Add(
                    Database.TxMerger.Enqueue(cmd)
                    );
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
        if(exceptions is not null)
            throw new AggregateException(exceptions);

        return count;
    }

    public class AiGenUpdateScript(List<AiGenScriptResult> work, string update) : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>
    {
        protected override long ExecuteCmd(DocumentsOperationContext context)
        {
            var patch = new PatchRequest(update, PatchRequestType.AiGen);
            context.DocumentDatabase.Scripts.GetScriptRunner(patch, true, out var script);

            int count = 0;
            foreach (AiGenScriptResult item in work)
            {
                count++;
                script.Run(context, context, "execute", [item.DocumentId, item.Context, item.AiHash, item.Output]);
            }

            return count;
        }

        public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>> ToDto(DocumentsOperationContext context)
        {
            throw new NotImplementedException();
        }
    }

    protected override AiGenStatsScope CreateScope(EtlRunStats stats)
    {
        return new AiGenStatsScope(stats);
    }

    protected override string StatsAggregatorTag => "Embeddings Generation";

    protected override bool ShouldFilterOutHiLoDocument()
    {
        return true;
    }

    public AiGenTestScriptResult RunTest(IEnumerable<AiGenScriptResult> records, DocumentsOperationContext context)
    {
        return null;
    }
}
