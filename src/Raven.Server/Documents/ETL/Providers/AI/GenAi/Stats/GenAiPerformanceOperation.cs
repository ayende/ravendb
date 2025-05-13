using System;
using Raven.Server.Documents.ETL.Stats;

namespace Raven.Server.Documents.ETL.Providers.AI.GenAi.Stats;

public sealed class GenAiPerformanceOperation(TimeSpan duration) : EtlPerformanceOperation(duration)
{
    public int NumberOfContextObjects { get; set; }

    public int TotalSentToModel { get; set; }

    public int TotalCachedContexts { get; set; }

    public int TotalTokensUsed { get; set; }

    public int PromptTokensUsed { get; set; }

    public int CompletionTokensUsed { get; set; }
}
