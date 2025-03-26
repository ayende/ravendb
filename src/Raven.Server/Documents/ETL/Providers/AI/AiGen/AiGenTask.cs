using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Microsoft.SemanticKernel.Embeddings;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.ETL;
using Raven.Server.Documents.AI;
using Raven.Server.Documents.ETL.Metrics;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings.Stats;
using Raven.Server.Documents.ETL.Providers.AI.Embeddings.Test;
using Raven.Server.Documents.ETL.Providers.AI.Enumerators;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Replication.ReplicationItems;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Server.Utils;

#pragma warning disable SKEXP0001

namespace Raven.Server.Documents.ETL.Providers.AI.AiGen;

public sealed class AiGenTask : EtlProcess<AiEtlItem, AiGenScriptResult, AiGenConfiguration, AiConnectionString,
    AiGenStatsScope, AiGenPerformanceOperation>
{
    private const string EmbeddingsTaskTag = "AI/Embeddings Generation";

    private int _fallbackCounter = 0;


    public AiGenTask(Transformation transformation, AiGenConfiguration configuration, DocumentDatabase database, ServerStore serverStore)
        : base(transformation, configuration, database, serverStore, EmbeddingsTaskTag)
    {
        Metrics = new EtlMetricsCountersManager();
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
        if (items is not EmbeddingsGenerationScriptRun embeddingsScriptRun)
        {
            Debug.Assert(items != null && items!.GetType()!.FullName!.StartsWith("System.Linq.EmptyPartition"),
                $"items != null && items!.GetType()!.FullName!.StartsWith('System.Linq.EmptyPartition'): {items!.GetType()!.FullName!}");
            return 0;
        }

        int count = 0;
         

        return count;
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
