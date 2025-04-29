using System.Collections.Generic;
using Raven.Client.Documents.Operations.AI;
using Raven.Server.Documents.ETL.Test;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.AI.GenAi.Test
{
    public sealed class TestGenAiScript : TestEtlScript<GenAiConfiguration, AiConnectionString>
    {
        public List<GenAiResultItem> Input { get; set; }

        public TestStage TestStage { get; set; }

        public BlittableJsonReaderObject Document { get; set; }
    }

    public enum TestStage
    {
        CreateContextObjects,
        SendToModel,
        ApplyUpdateScript
    }
}
